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
// Time: Created - TASK-T3 (WS pipeline wiring)
package kafka.server.http.ws;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netty.channel.ChannelHandlerContext;
import kafka.server.http.routing.BindingManager;
import kafka.server.http.routing.ExchangeException;
import kafka.server.http.routing.ExchangeManager;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * T3: Production-facing {@link WsFrameHandler} with real dispatchers for the
 * control-plane operations (declare-exchange / declare-queue / bind / unbind /
 * unsubscribe). The data-plane operations (publish / subscribe / ack / nack /
 * credits / get) remain stubbed pending the real Kafka produce/fetch wiring
 * which requires {@code RequestChannel} integration or an in-process
 * Kafka producer/consumer.
 *
 * <p>This class is constructed per WebSocket connection by
 * {@link WsUpgradeOrHttpHandler}'s {@code frameHandlerFactory}.
 */
public final class WiredWsFrameHandler extends WsFrameHandler {

    private static final Logger log = LoggerFactory.getLogger(WiredWsFrameHandler.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final ExchangeManager exchangeManager;
    private final BindingManager bindingManager;
    private final WsPublishHandler publishHandler;
    private final Supplier<String> bootstrapSupplier;
    private final Map<String, Subscription> subscriptions = new ConcurrentHashMap<>();

    public WiredWsFrameHandler(WsConnectionContext connectionContext,
                               WsConfigs wsConfigs,
                               ExchangeManager exchangeManager,
                               BindingManager bindingManager) {
        this(connectionContext, wsConfigs, exchangeManager, bindingManager, null, null);
    }

    public WiredWsFrameHandler(WsConnectionContext connectionContext,
                               WsConfigs wsConfigs,
                               ExchangeManager exchangeManager,
                               BindingManager bindingManager,
                               WsPublishHandler publishHandler) {
        this(connectionContext, wsConfigs, exchangeManager, bindingManager, publishHandler, null);
    }

    public WiredWsFrameHandler(WsConnectionContext connectionContext,
                               WsConfigs wsConfigs,
                               ExchangeManager exchangeManager,
                               BindingManager bindingManager,
                               WsPublishHandler publishHandler,
                               Supplier<String> bootstrapSupplier) {
        super(connectionContext, wsConfigs);
        this.exchangeManager = exchangeManager;
        this.bindingManager = bindingManager;
        this.publishHandler = publishHandler;
        this.bootstrapSupplier = bootstrapSupplier;
    }

    /** Tracks a single live subscription's fetch loop + offset cursor. */
    private static final class Subscription {
        final Thread thread;
        final AtomicBoolean running = new AtomicBoolean(true);
        final Map<Long, OffsetRef> pending = new ConcurrentHashMap<>();
        final AtomicLong nextTag = new AtomicLong(1);

        Subscription(Thread thread) {
            this.thread = thread;
        }
    }

    private record OffsetRef(String topic, int partition, long offset) { }

    // ------------------------------------------------------------------
    //  Exchange lifecycle — routed through ExchangeManager (in-memory).
    // ------------------------------------------------------------------

    @Override
    void handleDeclareExchange(WsConnectionContext ctx, JsonNode msg) {
        String name = textOrNull(msg, "exchange");
        // Client uses "exchangeType" to avoid clashing with the outer frame "type".
        String type = optTextOrDefault(msg, "exchangeType", "direct");
        boolean durable = optBool(msg, "durable", true);
        boolean autoDelete = optBool(msg, "autoDelete", false);
        boolean internal = optBool(msg, "internal", false);
        Map<String, String> args = extractArgs(msg);
        Object id = msg.get("id");

        if (name == null) {
            sendError(ctx, id, "INVALID_REQUEST", "declare-exchange requires 'exchange'");
            return;
        }
        try {
            exchangeManager.declareExchange(vhost(ctx), name, type, durable, autoDelete,
                /* passive */ false, internal, args);
            sendReply(ctx, id, "exchange-declared", "exchange", name, "exchangeType", type);
        } catch (ExchangeException e) {
            sendError(ctx, id, e.errorCode().name(), e.getMessage());
        }
    }

    @Override
    void handleDeleteExchange(WsConnectionContext ctx, JsonNode msg) {
        String name = textOrNull(msg, "exchange");
        boolean ifUnused = optBool(msg, "ifUnused", false);
        Object id = msg.get("id");
        if (name == null) {
            sendError(ctx, id, "INVALID_REQUEST", "delete-exchange requires 'exchange'");
            return;
        }
        try {
            exchangeManager.deleteExchange(vhost(ctx), name, ifUnused);
            sendReply(ctx, id, "exchange-deleted", "exchange", name);
        } catch (ExchangeException e) {
            sendError(ctx, id, e.errorCode().name(), e.getMessage());
        }
    }

    // ------------------------------------------------------------------
    //  Queue lifecycle — no QueueManager exists; accept and track as bindings.
    // ------------------------------------------------------------------

    @Override
    void handleDeclareQueue(WsConnectionContext ctx, JsonNode msg) {
        String name = textOrNull(msg, "queue");
        Object id = msg.get("id");
        if (name == null) {
            sendError(ctx, id, "INVALID_REQUEST", "declare-queue requires 'queue'");
            return;
        }
        // No QueueManager — queue existence is implicit (BindingManager accepts
        // any queue name). Reply as-if we registered it.
        sendReply(ctx, id, "queue-declared", "queue", name);
    }

    @Override
    void handleDeleteQueue(WsConnectionContext ctx, JsonNode msg) {
        String name = textOrNull(msg, "queue");
        Object id = msg.get("id");
        if (name == null) {
            sendError(ctx, id, "INVALID_REQUEST", "delete-queue requires 'queue'");
            return;
        }
        sendReply(ctx, id, "queue-deleted", "queue", name);
    }

    // ------------------------------------------------------------------
    //  Bindings — routed through BindingManager (in-memory).
    // ------------------------------------------------------------------

    @Override
    void handleBind(WsConnectionContext ctx, JsonNode msg) {
        String exchange = textOrNull(msg, "exchange");
        String queue = textOrNull(msg, "queue");
        String routingKey = optTextOrDefault(msg, "routingKey", "");
        Map<String, String> args = extractArgs(msg);
        Object id = msg.get("id");

        if (exchange == null || queue == null) {
            sendError(ctx, id, "INVALID_REQUEST", "bind requires 'exchange' and 'queue'");
            return;
        }

        // Validate exchange exists.
        ExchangeMetadata ex = exchangeManager.getExchange(vhost(ctx), exchange);
        if (ex == null) {
            sendError(ctx, id, "NOT_FOUND", "Exchange does not exist: " + exchange);
            return;
        }

        try {
            bindingManager.bind(exchange, queue, routingKey, args);
            sendReply(ctx, id, "bound", "exchange", exchange, "queue", queue,
                "routingKey", routingKey);
        } catch (RuntimeException e) {
            sendError(ctx, id, "INTERNAL_ERROR", e.getMessage());
        }
    }

    @Override
    void handleUnbind(WsConnectionContext ctx, JsonNode msg) {
        String exchange = textOrNull(msg, "exchange");
        String queue = textOrNull(msg, "queue");
        String routingKey = optTextOrDefault(msg, "routingKey", "");
        Object id = msg.get("id");

        if (exchange == null || queue == null) {
            sendError(ctx, id, "INVALID_REQUEST", "unbind requires 'exchange' and 'queue'");
            return;
        }
        bindingManager.unbind(exchange, queue, routingKey);
        sendReply(ctx, id, "unbound", "exchange", exchange, "queue", queue);
    }

    // T9: delegate publish to WsPublishHandler (which routes + calls the
    // ProduceRequestSink wired by HttpAcceptor).
    @Override
    void handlePublish(WsConnectionContext ctx, JsonNode msg) {
        if (publishHandler == null) {
            super.handlePublish(ctx, msg);
            return;
        }
        publishHandler.handlePublish(msg, ctx);
    }

    // T9: spawn a per-subscription fetch thread that polls the backing topic
    // and emits `deliver` frames to the WS channel. Simple + correct enough
    // for integration tests; production-grade version uses
    // WsSubscriptionManager + consumer groups (WS3.03).
    @Override
    void handleSubscribe(WsConnectionContext ctx, JsonNode msg) {
        String queue = textOrNull(msg, "queue");
        String subId = optTextOrDefault(msg, "subscriptionId", "sub-" + UUID.randomUUID());
        String startOffset = optTextOrDefault(msg, "startOffset", "earliest");
        Object id = msg.get("id");
        if (queue == null) {
            sendError(ctx, id, "INVALID_REQUEST", "subscribe requires 'queue'");
            return;
        }
        if (bootstrapSupplier == null || subscriptions.containsKey(subId)) {
            sendError(ctx, id, "INVALID_REQUEST", "subscription unavailable");
            return;
        }
        String bs = bootstrapSupplier.get();
        if (bs == null || bs.isEmpty()) {
            sendError(ctx, id, "INTERNAL_ERROR", "no bootstrap");
            return;
        }
        String topic = "ws." + queue;
        Subscription sub = startFetchLoop(ctx, subId, queue, topic, startOffset, bs);
        subscriptions.put(subId, sub);
        sendReply(ctx, id, "subscribed", "subscriptionId", subId, "queue", queue);
    }

    @Override
    void handleUnsubscribe(WsConnectionContext ctx, JsonNode msg) {
        String subId = textOrNull(msg, "subscriptionId");
        Object id = msg.get("id");
        if (subId != null) {
            Subscription sub = subscriptions.remove(subId);
            if (sub != null) {
                sub.running.set(false);
                sub.thread.interrupt();
            }
        }
        sendReply(ctx, id, "unsubscribed", "subscriptionId", subId);
    }

    @Override
    void handleAck(WsConnectionContext ctx, JsonNode msg) {
        String subId = textOrNull(msg, "subscriptionId");
        JsonNode tagNode = msg.get("deliveryTag");
        if (subId == null || tagNode == null) return;
        Subscription sub = subscriptions.get(subId);
        if (sub != null) sub.pending.remove(tagNode.asLong());
    }

    @Override
    void handleNack(WsConnectionContext ctx, JsonNode msg) {
        handleAck(ctx, msg); // same effect for our simplified in-process model
    }

    @Override
    void handleCredits(WsConnectionContext ctx, JsonNode msg) {
        // Simplified: credits ignored (no backpressure coupling).
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        // Stop all fetch threads on disconnect.
        subscriptions.values().forEach(s -> {
            s.running.set(false);
            s.thread.interrupt();
        });
        subscriptions.clear();
        super.channelInactive(ctx);
    }

    private Subscription startFetchLoop(WsConnectionContext ctx, String subId, String queue,
                                        String topic, String startOffset, String bs) {
        final com.fasterxml.jackson.databind.ObjectMapper mapper = MAPPER;
        Thread t = new Thread(() -> {
            Properties p = new Properties();
            p.put("bootstrap.servers", bs);
            p.put("group.id", "ws-sub-" + subId + "-" + UUID.randomUUID());
            p.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
            p.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
            p.put("auto.offset.reset", startOffset);
            p.put("enable.auto.commit", "false");
            KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(p);
            try {
                consumer.subscribe(Collections.singletonList(topic));
                Subscription sub = subscriptions.get(subId);
                while (sub != null && sub.running.get()) {
                    var records = consumer.poll(Duration.ofMillis(500));
                    for (ConsumerRecord<byte[], byte[]> r : records) {
                        if (!sub.running.get()) break;
                        long tag = sub.nextTag.getAndIncrement();
                        sub.pending.put(tag, new OffsetRef(r.topic(), r.partition(), r.offset()));
                        emitDeliver(ctx, subId, tag, r, mapper);
                    }
                    sub = subscriptions.get(subId);
                }
            } catch (Exception e) {
                if (sub(subId) != null && sub(subId).running.get())
                    log.warn("Fetch loop error subId={}: {}", subId, e.getMessage());
            } finally {
                try {
                    consumer.close(org.apache.kafka.clients.consumer.CloseOptions.timeout(Duration.ofMillis(200)));
                } catch (Exception ignored) {
                    // noop
                }
            }
        }, "ws-sub-" + subId);
        t.setDaemon(true);
        t.start();
        return new Subscription(t);
    }

    private Subscription sub(String id) {
        return subscriptions.get(id);
    }

    private void emitDeliver(WsConnectionContext ctx, String subId, long tag,
                             ConsumerRecord<byte[], byte[]> r,
                             com.fasterxml.jackson.databind.ObjectMapper mapper) {
        try {
            com.fasterxml.jackson.databind.node.ObjectNode out = mapper.createObjectNode();
            out.put("type", "deliver");
            out.put("subscriptionId", subId);
            out.put("deliveryTag", tag);
            // Pull _ws_exchange / _ws_routing_key headers if present.
            String exchange = null, routingKey = null;
            for (org.apache.kafka.common.header.Header h : r.headers()) {
                if ("_ws_exchange".equals(h.key())) exchange = new String(h.value(), java.nio.charset.StandardCharsets.UTF_8);
                if ("_ws_routing_key".equals(h.key())) routingKey = new String(h.value(), java.nio.charset.StandardCharsets.UTF_8);
            }
            if (exchange != null) out.put("exchange", exchange);
            if (routingKey != null) out.put("routingKey", routingKey);
            // Body: raw bytes as UTF-8 string if possible, else base64.
            if (r.value() != null) {
                String asText = new String(r.value(), java.nio.charset.StandardCharsets.UTF_8);
                try {
                    com.fasterxml.jackson.databind.JsonNode parsed = mapper.readTree(asText);
                    out.set("body", parsed);
                } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
                    out.put("body", asText);
                }
            }
            ctx.sendFrame(mapper.writeValueAsString(out));
        } catch (Exception e) {
            log.warn("Failed to emit deliver frame subId={} tag={}", subId, tag, e);
        }
    }

    // handlePublish / handleSubscribe / handleAck / handleNack / handleCredits /
    // handleGet / handlePurgeQueue remain stubs (throw UnsupportedOperationException)
    // — WsFrameHandler's catch clause converts them into INTERNAL_ERROR frames.
    // Real wiring needs Kafka produce/fetch + RequestChannel integration.

    // ------------------------------------------------------------------
    //  Helpers
    // ------------------------------------------------------------------

    private static String vhost(WsConnectionContext ctx) {
        return ctx.vhost() == null || ctx.vhost().isEmpty() ? "/" : ctx.vhost();
    }

    private static String textOrNull(JsonNode node, String field) {
        JsonNode v = node.get(field);
        return (v == null || !v.isTextual()) ? null : v.asText();
    }

    private static String optTextOrDefault(JsonNode node, String field, String dflt) {
        String v = textOrNull(node, field);
        return v == null ? dflt : v;
    }

    private static boolean optBool(JsonNode node, String field, boolean dflt) {
        JsonNode v = node.get(field);
        return (v == null || !v.isBoolean()) ? dflt : v.asBoolean();
    }

    private static Map<String, String> extractArgs(JsonNode msg) {
        JsonNode args = msg.get("arguments");
        if (args == null || !args.isObject()) return Collections.emptyMap();
        Map<String, String> out = new HashMap<>();
        args.fieldNames().forEachRemaining(k -> {
            JsonNode v = args.get(k);
            out.put(k, v == null ? "" : v.asText());
        });
        return out;
    }

    private void sendReply(WsConnectionContext ctx, Object corrId, String type,
                           Object... kv) {
        ObjectNode out = MAPPER.createObjectNode();
        out.put("type", type);
        if (corrId != null) out.set("id", (JsonNode) corrId);
        for (int i = 0; i + 1 < kv.length; i += 2) {
            String k = String.valueOf(kv[i]);
            Object v = kv[i + 1];
            if (v == null) out.putNull(k);
            else if (v instanceof Boolean) out.put(k, (Boolean) v);
            else if (v instanceof Integer) out.put(k, (Integer) v);
            else if (v instanceof Long) out.put(k, (Long) v);
            else out.put(k, String.valueOf(v));
        }
        try {
            ctx.sendFrame(MAPPER.writeValueAsString(out));
        } catch (JsonProcessingException e) {
            log.warn("Failed to serialise frame for session {}", ctx.sessionId(), e);
        }
    }

    private void sendError(WsConnectionContext ctx, Object corrId, String code, String msg) {
        ObjectNode out = MAPPER.createObjectNode();
        out.put("type", "error");
        if (corrId != null) out.set("id", (JsonNode) corrId);
        out.put("errorCode", code);
        out.put("errorMessage", msg);
        try {
            ctx.sendFrame(MAPPER.writeValueAsString(out));
        } catch (JsonProcessingException e) {
            log.warn("Failed to serialise frame for session {}", ctx.sessionId(), e);
        }
    }
}
