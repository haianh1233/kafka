/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Time: Created - TASK-WS1.05

package kafka.server.http.ws;

import kafka.server.http.HttpRequestTranslator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

/**
 * Owns routing metadata (exchanges, queues, bindings) backed by the internal
 * {@code __ws_routing_metadata} compacted Kafka topic.
 *
 * <p>Each mutation follows the same pattern as {@code GroupCoordinator}:
 * <ol>
 *   <li>Serialize to JSON and publish the record (delegated to a pluggable
 *       {@link BiConsumer}{@code <String, byte[]>} for testability).</li>
 *   <li>Apply the change to the in-memory cache so reads see it immediately.</li>
 * </ol>
 *
 * <p>On broker startup the replay loop streams the topic from offset 0 to the
 * high watermark and delivers every record to {@link #applyRecord(String, byte[])},
 * rebuilding the cache. A {@code null} value is treated as a tombstone and removes
 * the entity from the cache.
 *
 * <p>The cache uses {@link ConcurrentHashMap} and {@link CopyOnWriteArrayList}
 * so that concurrent writes and high-read traffic from the routing engine do
 * not require external locking.
 *
 * // Time: Created - TASK-WS1.05
 */
public class WsRoutingMetadataManager {

    private static final Logger log = LoggerFactory.getLogger(WsRoutingMetadataManager.class);
    private static final ObjectMapper MAPPER = HttpRequestTranslator.MAPPER;

    static final String EXCHANGE_PREFIX = "exchange:";
    static final String QUEUE_PREFIX = "queue:";
    static final String BINDING_PREFIX = "binding:";

    private final WsConfigs wsConfigs;
    private final BiConsumer<String, byte[]> recordWriter;
    private final AtomicBoolean replayComplete = new AtomicBoolean(false);

    // vhost:name -> metadata (exchanges)
    private final ConcurrentHashMap<String, ExchangeMetadata> exchanges = new ConcurrentHashMap<>();
    // vhost:name -> metadata (queues)
    private final ConcurrentHashMap<String, QueueMetadata> queues = new ConcurrentHashMap<>();
    // vhost:exchange -> list of bindings
    private final ConcurrentHashMap<String, CopyOnWriteArrayList<BindingMetadata>> bindings = new ConcurrentHashMap<>();

    public WsRoutingMetadataManager(WsConfigs wsConfigs, BiConsumer<String, byte[]> recordWriter) {
        this.wsConfigs = Objects.requireNonNull(wsConfigs, "wsConfigs");
        this.recordWriter = Objects.requireNonNull(recordWriter, "recordWriter");
    }

    // ------------------------------------------------------------------
    // Write methods — persist then update cache.
    // ------------------------------------------------------------------

    public void writeExchange(ExchangeMetadata exchange) {
        Objects.requireNonNull(exchange, "exchange");
        String key = exchangeKey(exchange.vhost(), exchange.name());
        byte[] value = serializeExchange(exchange);
        recordWriter.accept(key, value);
        exchanges.put(cacheKey(exchange.vhost(), exchange.name()), exchange);
    }

    public void deleteExchange(String vhost, String name) {
        Objects.requireNonNull(vhost, "vhost");
        Objects.requireNonNull(name, "name");
        String key = exchangeKey(vhost, name);
        recordWriter.accept(key, null);
        exchanges.remove(cacheKey(vhost, name));
        // Also drop any bindings that lived on this exchange.
        bindings.remove(cacheKey(vhost, name));
    }

    public void writeQueue(QueueMetadata queue) {
        Objects.requireNonNull(queue, "queue");
        String key = queueKey(queue.vhost(), queue.name());
        byte[] value = serializeQueue(queue);
        recordWriter.accept(key, value);
        queues.put(cacheKey(queue.vhost(), queue.name()), queue);
    }

    public void deleteQueue(String vhost, String name) {
        Objects.requireNonNull(vhost, "vhost");
        Objects.requireNonNull(name, "name");
        String key = queueKey(vhost, name);
        recordWriter.accept(key, null);
        queues.remove(cacheKey(vhost, name));
    }

    public void writeBinding(BindingMetadata binding) {
        Objects.requireNonNull(binding, "binding");
        String key = bindingKey(binding.vhost(), binding.exchange(), binding.queue(),
            binding.routingKey(), binding.arguments());
        byte[] value = serializeBinding(binding);
        recordWriter.accept(key, value);
        addBindingToCache(binding);
    }

    public void deleteBinding(String vhost, String exchange, String queue,
                              String routingKey, Map<String, String> arguments) {
        Objects.requireNonNull(vhost, "vhost");
        Objects.requireNonNull(exchange, "exchange");
        Objects.requireNonNull(queue, "queue");
        Objects.requireNonNull(routingKey, "routingKey");
        String key = bindingKey(vhost, exchange, queue, routingKey, arguments);
        recordWriter.accept(key, null);
        removeBindingFromCache(vhost, exchange, queue, routingKey, arguments);
    }

    // ------------------------------------------------------------------
    // Read methods — all served from the in-memory cache.
    // ------------------------------------------------------------------

    public ExchangeMetadata getExchange(String vhost, String name) {
        return exchanges.get(cacheKey(vhost, name));
    }

    public QueueMetadata getQueue(String vhost, String name) {
        return queues.get(cacheKey(vhost, name));
    }

    public List<BindingMetadata> getBindings(String vhost, String exchange) {
        CopyOnWriteArrayList<BindingMetadata> list = bindings.get(cacheKey(vhost, exchange));
        return list == null ? Collections.emptyList() : Collections.unmodifiableList(list);
    }

    public Collection<ExchangeMetadata> listExchanges(String vhost) {
        Objects.requireNonNull(vhost, "vhost");
        List<ExchangeMetadata> result = new ArrayList<>();
        for (ExchangeMetadata ex : exchanges.values()) {
            if (vhost.equals(ex.vhost())) {
                result.add(ex);
            }
        }
        return Collections.unmodifiableList(result);
    }

    public Collection<QueueMetadata> listQueues(String vhost) {
        Objects.requireNonNull(vhost, "vhost");
        List<QueueMetadata> result = new ArrayList<>();
        for (QueueMetadata q : queues.values()) {
            if (vhost.equals(q.vhost())) {
                result.add(q);
            }
        }
        return Collections.unmodifiableList(result);
    }

    public int exchangeCount(String vhost) {
        Objects.requireNonNull(vhost, "vhost");
        int count = 0;
        for (ExchangeMetadata ex : exchanges.values()) {
            if (vhost.equals(ex.vhost())) count++;
        }
        return count;
    }

    public int queueCount(String vhost) {
        Objects.requireNonNull(vhost, "vhost");
        int count = 0;
        for (QueueMetadata q : queues.values()) {
            if (vhost.equals(q.vhost())) count++;
        }
        return count;
    }

    public int bindingCount(String vhost, String exchange) {
        CopyOnWriteArrayList<BindingMetadata> list = bindings.get(cacheKey(vhost, exchange));
        return list == null ? 0 : list.size();
    }

    // ------------------------------------------------------------------
    // Replay — applyRecord is called for every record read at startup.
    // A null value is a tombstone.
    // ------------------------------------------------------------------

    public void applyRecord(String key, byte[] value) {
        Objects.requireNonNull(key, "key");
        try {
            if (key.startsWith(EXCHANGE_PREFIX)) {
                applyExchangeRecord(key, value);
            } else if (key.startsWith(QUEUE_PREFIX)) {
                applyQueueRecord(key, value);
            } else if (key.startsWith(BINDING_PREFIX)) {
                applyBindingRecord(key, value);
            } else {
                log.warn("Ignoring routing metadata record with unknown key prefix: {}", key);
            }
        } catch (Exception e) {
            log.error("Failed to apply routing metadata record key={}: {}", key, e.getMessage(), e);
        }
    }

    public void markReplayComplete() {
        replayComplete.set(true);
    }

    public boolean isReplayComplete() {
        return replayComplete.get();
    }

    public WsConfigs wsConfigs() {
        return wsConfigs;
    }

    // ------------------------------------------------------------------
    // Key formatting.
    // ------------------------------------------------------------------

    static String exchangeKey(String vhost, String name) {
        return EXCHANGE_PREFIX + vhost + ":" + name;
    }

    static String queueKey(String vhost, String name) {
        return QUEUE_PREFIX + vhost + ":" + name;
    }

    static String bindingKey(String vhost, String exchange, String queue,
                             String routingKey, Map<String, String> arguments) {
        return BINDING_PREFIX + vhost + ":" + exchange + ":" + queue + ":"
            + routingKey + ":" + argsHash(arguments);
    }

    /**
     * Deterministic hash of a binding's arguments map: sort by key, then hash the
     * canonical string form. Two maps with the same entries (in any iteration order)
     * produce the same hash and hence the same binding key.
     */
    static int argsHash(Map<String, String> arguments) {
        if (arguments == null || arguments.isEmpty()) return 0;
        TreeMap<String, String> sorted = new TreeMap<>(arguments);
        return Objects.hash(sorted.toString());
    }

    // ------------------------------------------------------------------
    // Internal helpers.
    // ------------------------------------------------------------------

    private static String cacheKey(String vhost, String name) {
        return vhost + ":" + name;
    }

    private void addBindingToCache(BindingMetadata binding) {
        String cacheKey = cacheKey(binding.vhost(), binding.exchange());
        bindings.compute(cacheKey, (k, existing) -> {
            CopyOnWriteArrayList<BindingMetadata> list =
                existing == null ? new CopyOnWriteArrayList<>() : existing;
            // Replace any existing binding with the same identity (same queue, routing key,
            // arguments) so re-publishing the same binding is idempotent.
            list.removeIf(b -> sameBindingIdentity(b, binding));
            list.add(binding);
            return list;
        });
    }

    private void removeBindingFromCache(String vhost, String exchange, String queue,
                                        String routingKey, Map<String, String> arguments) {
        String cacheKey = cacheKey(vhost, exchange);
        Map<String, String> normalizedArgs = arguments == null ? Collections.emptyMap() : arguments;
        bindings.computeIfPresent(cacheKey, (k, list) -> {
            list.removeIf(b -> b.queue().equals(queue)
                && b.routingKey().equals(routingKey)
                && argsHash(b.arguments()) == argsHash(normalizedArgs));
            return list.isEmpty() ? null : list;
        });
    }

    private static boolean sameBindingIdentity(BindingMetadata a, BindingMetadata b) {
        return a.queue().equals(b.queue())
            && a.routingKey().equals(b.routingKey())
            && argsHash(a.arguments()) == argsHash(b.arguments());
    }

    // ------------------------------------------------------------------
    // Serialization.
    // ------------------------------------------------------------------

    private static byte[] serializeExchange(ExchangeMetadata ex) {
        ObjectNode node = MAPPER.createObjectNode();
        node.put("name", ex.name());
        node.put("vhost", ex.vhost());
        node.put("type", ex.type());
        node.put("durable", ex.durable());
        node.put("autoDelete", ex.autoDelete());
        node.put("internal", ex.internal());
        node.set("arguments", toJsonArguments(ex.arguments()));
        return toBytes(node);
    }

    private static byte[] serializeQueue(QueueMetadata q) {
        ObjectNode node = MAPPER.createObjectNode();
        node.put("name", q.name());
        node.put("vhost", q.vhost());
        node.put("durable", q.durable());
        node.put("exclusive", q.exclusive());
        node.put("autoDelete", q.autoDelete());
        node.set("arguments", toJsonArguments(q.arguments()));
        return toBytes(node);
    }

    private static byte[] serializeBinding(BindingMetadata b) {
        ObjectNode node = MAPPER.createObjectNode();
        node.put("vhost", b.vhost());
        node.put("exchange", b.exchange());
        node.put("queue", b.queue());
        node.put("routingKey", b.routingKey());
        node.set("arguments", toJsonArguments(b.arguments()));
        return toBytes(node);
    }

    private static ObjectNode toJsonArguments(Map<String, String> arguments) {
        ObjectNode args = MAPPER.createObjectNode();
        if (arguments != null) {
            for (Map.Entry<String, String> e : arguments.entrySet()) {
                args.put(e.getKey(), e.getValue());
            }
        }
        return args;
    }

    private static byte[] toBytes(ObjectNode node) {
        try {
            return MAPPER.writeValueAsBytes(node);
        } catch (Exception e) {
            // Should never happen for the well-formed ObjectNodes we build above.
            throw new IllegalStateException("Failed to serialize routing metadata", e);
        }
    }

    private void applyExchangeRecord(String key, byte[] value) throws Exception {
        // Key format: exchange:{vhost}:{name}
        String[] parts = splitKeyParts(key.substring(EXCHANGE_PREFIX.length()), 2);
        if (parts == null) {
            log.warn("Malformed exchange metadata key: {}", key);
            return;
        }
        String vhost = parts[0];
        String name = parts[1];
        if (value == null) {
            exchanges.remove(cacheKey(vhost, name));
            bindings.remove(cacheKey(vhost, name));
            return;
        }
        JsonNode node = MAPPER.readTree(new String(value, StandardCharsets.UTF_8));
        ExchangeMetadata ex = new ExchangeMetadata(
            optString(node, "name", name),
            optString(node, "vhost", vhost),
            optString(node, "type", "direct"),
            node.path("durable").asBoolean(false),
            node.path("autoDelete").asBoolean(false),
            node.path("internal").asBoolean(false),
            readArguments(node.path("arguments"))
        );
        exchanges.put(cacheKey(ex.vhost(), ex.name()), ex);
    }

    private void applyQueueRecord(String key, byte[] value) throws Exception {
        String[] parts = splitKeyParts(key.substring(QUEUE_PREFIX.length()), 2);
        if (parts == null) {
            log.warn("Malformed queue metadata key: {}", key);
            return;
        }
        String vhost = parts[0];
        String name = parts[1];
        if (value == null) {
            queues.remove(cacheKey(vhost, name));
            return;
        }
        JsonNode node = MAPPER.readTree(new String(value, StandardCharsets.UTF_8));
        QueueMetadata q = new QueueMetadata(
            optString(node, "name", name),
            optString(node, "vhost", vhost),
            node.path("durable").asBoolean(false),
            node.path("exclusive").asBoolean(false),
            node.path("autoDelete").asBoolean(false),
            readArguments(node.path("arguments"))
        );
        queues.put(cacheKey(q.vhost(), q.name()), q);
    }

    private void applyBindingRecord(String key, byte[] value) throws Exception {
        // Key format: binding:{vhost}:{exchange}:{queue}:{routingKey}:{argsHash}
        String remainder = key.substring(BINDING_PREFIX.length());
        // Keys are always exactly 5 segments separated by ':', but the routing key itself
        // may contain ':'. Split from the left for the first three segments (vhost, exchange,
        // queue) and then peel the argsHash off the right; the middle is the routing key.
        int firstColon = remainder.indexOf(':');
        if (firstColon < 0) {
            log.warn("Malformed binding metadata key: {}", key);
            return;
        }
        String vhost = remainder.substring(0, firstColon);
        String rest = remainder.substring(firstColon + 1);
        int secondColon = rest.indexOf(':');
        if (secondColon < 0) {
            log.warn("Malformed binding metadata key: {}", key);
            return;
        }
        String exchange = rest.substring(0, secondColon);
        String afterExchange = rest.substring(secondColon + 1);
        int thirdColon = afterExchange.indexOf(':');
        if (thirdColon < 0) {
            log.warn("Malformed binding metadata key: {}", key);
            return;
        }
        String queue = afterExchange.substring(0, thirdColon);
        String afterQueue = afterExchange.substring(thirdColon + 1);
        int lastColon = afterQueue.lastIndexOf(':');
        if (lastColon < 0) {
            log.warn("Malformed binding metadata key: {}", key);
            return;
        }
        String routingKey = afterQueue.substring(0, lastColon);
        // argsHash segment is unused for lookup — the value payload restores the real args.

        if (value == null) {
            // For tombstones we have to match by (queue, routingKey, argsHash-from-key) since
            // we don't have the argument map in the payload.
            String argsHashFromKey = afterQueue.substring(lastColon + 1);
            final int hashVal;
            try {
                hashVal = Integer.parseInt(argsHashFromKey);
            } catch (NumberFormatException nfe) {
                log.warn("Malformed binding metadata key (bad argsHash): {}", key);
                return;
            }
            String cacheKey = cacheKey(vhost, exchange);
            bindings.computeIfPresent(cacheKey, (k, list) -> {
                list.removeIf(b -> b.queue().equals(queue)
                    && b.routingKey().equals(routingKey)
                    && argsHash(b.arguments()) == hashVal);
                return list.isEmpty() ? null : list;
            });
            return;
        }

        JsonNode node = MAPPER.readTree(new String(value, StandardCharsets.UTF_8));
        BindingMetadata b = new BindingMetadata(
            optString(node, "vhost", vhost),
            optString(node, "exchange", exchange),
            optString(node, "queue", queue),
            optString(node, "routingKey", routingKey),
            readArguments(node.path("arguments"))
        );
        addBindingToCache(b);
    }

    private static String[] splitKeyParts(String remainder, int expectedParts) {
        if (expectedParts != 2) return null;
        int colon = remainder.indexOf(':');
        if (colon < 0) return null;
        return new String[]{remainder.substring(0, colon), remainder.substring(colon + 1)};
    }

    private static String optString(JsonNode node, String field, String fallback) {
        JsonNode v = node.path(field);
        return v.isMissingNode() || v.isNull() ? fallback : v.asText();
    }

    private static Map<String, String> readArguments(JsonNode node) {
        if (node == null || node.isMissingNode() || node.isNull() || !node.isObject()) {
            return Collections.emptyMap();
        }
        Map<String, String> out = new HashMap<>();
        Iterator<String> names = node.fieldNames();
        while (names.hasNext()) {
            String name = names.next();
            out.put(name, node.path(name).asText());
        }
        return out;
    }
}
