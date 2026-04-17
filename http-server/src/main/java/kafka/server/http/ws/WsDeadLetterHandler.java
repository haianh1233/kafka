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

// Time: Created - TASK-WS4.01

package kafka.server.http.ws;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import kafka.server.http.routing.RoutingEngine;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Handles dead-letter exchange (DLX) routing for NACK'd messages.
 *
 * <p>When a consumer NACKs with {@code requeue=false} and the source queue has
 * an {@code x-dead-letter-exchange} configured, the broker:
 * <ol>
 *   <li>Looks up the queue's DLX configuration via the injected
 *       {@link QueueResolver}.</li>
 *   <li>Builds a copy of the original record headers with an updated
 *       {@code x-death} array — the new entry (queue, reason, exchange,
 *       routing-keys, count, time) is prepended to any existing entries so the
 *       newest hop appears first (RabbitMQ convention).</li>
 *   <li>Routes the message through the DLX via {@link RoutingEngine#route}.
 *       This is a full-fidelity exchange resolution: fanout DLX may produce to
 *       multiple DLQ topics, headers/topic/direct DLXs are fully supported.</li>
 *   <li>Hands the per-queue records off to the injected {@link DlxProduceSink};
 *       the returned futures are aggregated so the outer
 *       {@link CompletableFuture} only completes once <i>every</i> DLX produce
 *       has been acknowledged. If any produce fails, the outer future fails too
 *       — this is the atomicity invariant the {@link WsAckHandler} relies on
 *       to keep the original offset uncommitted.</li>
 * </ol>
 *
 * <h3>Atomicity invariant (spec §X)</h3>
 * <p>The future returned by {@link #deadLetter} <b>must</b> complete (success
 * or failure) only after the underlying DLX produce(s) have been acknowledged
 * by the broker. Callers are required to use that future as the gate before
 * advancing the original offset's commit watermark — see
 * {@link WsAckHandler#handleNack} for the production wiring.
 *
 * <h3>Misconfiguration handling</h3>
 * <ul>
 *   <li>Queue not registered, or registered without {@code x-dead-letter-exchange}
 *       → returns a successfully-completed future and produces nothing. The
 *       message is intentionally discarded; the client explicitly rejected it.</li>
 *   <li>{@code x-dead-letter-exchange} points to an unknown exchange → the
 *       returned future completes exceptionally with
 *       {@link IllegalArgumentException}. The caller MUST treat this as a
 *       redelivery trigger to avoid silent message loss on misconfiguration.</li>
 *   <li>DLX resolves to zero queues (no bindings or no match) → returns a
 *       successfully-completed future. By analogy with mandatory-return
 *       semantics for the publish path, an unrouted DLX is "best effort done"
 *       and the original offset is allowed to advance.</li>
 *   <li>Per-DLQ produce failure → the outer future fails. Original offset must
 *       NOT be committed; the caller treats it as a transient redelivery.</li>
 * </ul>
 *
 * <h3>{@code x-death} accumulation</h3>
 * <p>If the inbound headers already carry an {@code x-death} array (i.e., the
 * message was already dead-lettered once), the new entry is prepended and the
 * full history is preserved. A malformed prior {@code x-death} value is
 * dropped silently and the new entry becomes the sole element — this avoids
 * crashing the consumer pipeline on hand-crafted bad headers.
 *
 * <h3>Thread-safety</h3>
 * <p>The handler is stateless beyond its injected dependencies. Concurrent
 * {@link #deadLetter} invocations are safe; ordering across concurrent calls
 * is not guaranteed (each call's atomicity is local to its returned future).
 */
public final class WsDeadLetterHandler {

    private static final Logger log = LoggerFactory.getLogger(WsDeadLetterHandler.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** Queue argument: name of the dead-letter exchange to publish to. */
    public static final String ARG_DLX_EXCHANGE = "x-dead-letter-exchange";

    /** Queue argument: routing key to use when publishing to the DLX. */
    public static final String ARG_DLX_ROUTING_KEY = "x-dead-letter-routing-key";

    /** Header name for the {@code x-death} JSON array of dead-letter hop entries. */
    public static final String HDR_X_DEATH = "x-death";

    // --- x-death entry field names ---
    private static final String F_QUEUE = "queue";
    private static final String F_REASON = "reason";
    private static final String F_COUNT = "count";
    private static final String F_EXCHANGE = "exchange";
    private static final String F_ROUTING_KEYS = "routing-keys";
    private static final String F_TIME = "time";

    private final RoutingEngine routingEngine;
    private final QueueResolver queueResolver;
    private final Function<String, String> queueToTopicFn;
    private final DlxProduceSink sink;

    /**
     * @param routingEngine    routes the dead-letter publish through the
     *                         configured DLX (full exchange-binding resolution)
     * @param queueResolver    looks up queue metadata by {@code (vhost, name)};
     *                         returns {@code null} for unknown queues
     * @param queueToTopicFn   queue name → Kafka topic; mirrors the convention
     *                         used by {@link WsPublishHandler}
     * @param sink             per-queue DLX produce hand-off; returns a future
     *                         that completes when the broker acks
     */
    public WsDeadLetterHandler(RoutingEngine routingEngine,
                                QueueResolver queueResolver,
                                Function<String, String> queueToTopicFn,
                                DlxProduceSink sink) {
        this.routingEngine = Objects.requireNonNull(routingEngine, "routingEngine");
        this.queueResolver = Objects.requireNonNull(queueResolver, "queueResolver");
        this.queueToTopicFn = Objects.requireNonNull(queueToTopicFn, "queueToTopicFn");
        this.sink = Objects.requireNonNull(sink, "sink");
    }

    /**
     * Dead-letter a NACK'd record.
     *
     * <p>Returns a future that completes when DLX produce is fully acknowledged
     * (or fails). The caller MUST await this future before advancing the
     * original offset's commit watermark.
     *
     * <p>If the source queue has no DLX configured (or is unknown) the future
     * completes successfully without producing anything — the original message
     * is then safe to drop.
     *
     * @param recordKey      original record key bytes (may be {@code null})
     * @param recordValue    original record value bytes (may be {@code null})
     * @param recordHeaders  original record headers (must not be {@code null})
     * @param vhost          virtual host identifier (must not be {@code null})
     * @param queueName      source queue name (must not be {@code null})
     * @param reason         dead-letter reason ({@code "rejected"},
     *                       {@code "expired"}, {@code "max-retries-exceeded"})
     * @return future completing when DLX produce acks; failed future on routing
     *         or produce error
     */
    public CompletableFuture<Void> deadLetter(byte[] recordKey,
                                              byte[] recordValue,
                                              Headers recordHeaders,
                                              String vhost,
                                              String queueName,
                                              String reason) {
        Objects.requireNonNull(recordHeaders, "recordHeaders");
        Objects.requireNonNull(vhost, "vhost");
        Objects.requireNonNull(queueName, "queueName");
        Objects.requireNonNull(reason, "reason");

        DlxConfig cfg = resolveDlxConfig(vhost, queueName, recordHeaders);
        if (cfg == null) {
            return CompletableFuture.completedFuture(null);
        }

        Set<String> targetQueues = routeOrFail(cfg);
        if (targetQueues == null) {
            // Routing exception was wrapped into a CompletionException; surface it.
            return cfg.failure;
        }
        if (targetQueues.isEmpty()) {
            log.debug("DLX '{}' routed to no queues for source queue '{}' (rk='{}'); "
                + "original message will be dropped", cfg.dlxExchange, queueName, cfg.dlxRoutingKey);
            return CompletableFuture.completedFuture(null);
        }

        List<Header> dlxHeaders = buildHeaders(recordHeaders, queueName, reason,
            cfg.originalExchange, cfg.originalRoutingKey);
        byte[] dlxKeyBytes = cfg.dlxRoutingKey.isEmpty()
            ? null
            : cfg.dlxRoutingKey.getBytes(StandardCharsets.UTF_8);

        return fanOutDlx(targetQueues, dlxKeyBytes, recordValue, dlxHeaders);
    }

    /** Per-call DLX configuration: queue arguments + resolved routing values. */
    private static final class DlxConfig {
        final String dlxExchange;
        final String dlxRoutingKey;
        final String originalExchange;
        final String originalRoutingKey;
        // Set to a failed future when routing throws, so caller can return it.
        CompletableFuture<Void> failure;

        DlxConfig(String dlxExchange, String dlxRoutingKey,
                  String originalExchange, String originalRoutingKey) {
            this.dlxExchange = dlxExchange;
            this.dlxRoutingKey = dlxRoutingKey;
            this.originalExchange = originalExchange;
            this.originalRoutingKey = originalRoutingKey;
        }
    }

    /**
     * Resolves the DLX configuration for a queue and returns {@code null} when
     * no DLX is configured (the caller should treat this as "discard, commit").
     */
    private DlxConfig resolveDlxConfig(String vhost, String queueName, Headers recordHeaders) {
        QueueMetadata queue = queueResolver.lookup(vhost, queueName);
        String dlxExchange = queue == null ? null : queue.arguments().get(ARG_DLX_EXCHANGE);
        if (dlxExchange == null || dlxExchange.isEmpty()) {
            log.debug("NACK requeue=false for queue '{}' but no x-dead-letter-exchange configured; "
                + "message will be discarded", queueName);
            return null;
        }
        String configuredRk = queue.arguments().get(ARG_DLX_ROUTING_KEY);
        String originalRk = headerValue(recordHeaders, WsMessageSerializer.HDR_ROUTING_KEY, "");
        String originalExchange = headerValue(recordHeaders, WsMessageSerializer.HDR_EXCHANGE, "");
        String dlxRk = (configuredRk == null || configuredRk.isEmpty()) ? originalRk : configuredRk;
        return new DlxConfig(dlxExchange, dlxRk, originalExchange, originalRk);
    }

    /**
     * Runs the DLX through the routing engine; on failure stashes a failed
     * future on {@code cfg} and returns {@code null} so the caller surfaces it.
     */
    private Set<String> routeOrFail(DlxConfig cfg) {
        try {
            return routingEngine.route(cfg.dlxExchange, cfg.dlxRoutingKey, Map.of());
        } catch (RuntimeException e) {
            log.warn("DLX '{}' resolution failed: {}", cfg.dlxExchange, e.toString());
            CompletableFuture<Void> failed = new CompletableFuture<>();
            failed.completeExceptionally(e);
            cfg.failure = failed;
            return null;
        }
    }

    /**
     * Fans the DLX produce out across every matched destination queue and
     * aggregates the per-queue futures. The outer future only completes once
     * every produce has acked (atomicity invariant); any failure short-circuits
     * the outer future to a failed state.
     */
    private CompletableFuture<Void> fanOutDlx(Set<String> targetQueues,
                                              byte[] keyBytes,
                                              byte[] valueBytes,
                                              List<Header> headers) {
        List<CompletableFuture<Void>> perQueue = new ArrayList<>(targetQueues.size());
        for (String dlq : targetQueues) {
            CompletableFuture<Void> f = enqueueOne(dlq, keyBytes, valueBytes, headers);
            if (f != null) {
                perQueue.add(f);
            }
        }
        if (perQueue.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        return CompletableFuture.allOf(perQueue.toArray(new CompletableFuture<?>[0]));
    }

    /**
     * Resolves the queue → topic mapping and dispatches a single produce; any
     * synchronous failure is converted to a failed future so the aggregate
     * still surfaces it.
     */
    private CompletableFuture<Void> enqueueOne(String dlq, byte[] keyBytes, byte[] valueBytes,
                                               List<Header> headers) {
        String topic = queueToTopicFn.apply(dlq);
        if (topic == null || topic.isEmpty()) {
            log.warn("DLX queue '{}' resolved to null/empty topic; skipping", dlq);
            return null;
        }
        try {
            CompletableFuture<Void> f = sink.enqueue(topic, keyBytes, valueBytes, headers);
            return f == null ? CompletableFuture.completedFuture(null) : f;
        } catch (RuntimeException e) {
            CompletableFuture<Void> failed = new CompletableFuture<>();
            failed.completeExceptionally(e);
            return failed;
        }
    }

    // ------------------------------------------------------------------
    //  Header building
    // ------------------------------------------------------------------

    /**
     * Builds the header list for the DLX-published record: copies all original
     * headers except {@code x-death}, then appends the rebuilt
     * (newest-first) {@code x-death} array.
     */
    private List<Header> buildHeaders(Headers original, String queueName, String reason,
                                      String exchange, String routingKey) {
        // LinkedHashMap preserves original ordering and gives us O(1) override of x-death.
        Map<String, byte[]> merged = new LinkedHashMap<>();
        for (Header h : original) {
            // Skip the prior x-death; we rebuild it below to guarantee newest-first.
            if (HDR_X_DEATH.equals(h.key())) {
                continue;
            }
            merged.put(h.key(), h.value());
        }

        ArrayNode xDeath = buildXDeathArray(original, queueName, reason, exchange, routingKey);
        try {
            merged.put(HDR_X_DEATH, MAPPER.writeValueAsBytes(xDeath));
        } catch (JsonProcessingException e) {
            // Should be impossible — we built this ArrayNode from primitive types.
            throw new IllegalStateException("Failed to serialize x-death header", e);
        }

        List<Header> out = new ArrayList<>(merged.size());
        for (Map.Entry<String, byte[]> e : merged.entrySet()) {
            out.add(new RecordHeader(e.getKey(), e.getValue()));
        }
        return Collections.unmodifiableList(out);
    }

    /**
     * Constructs the new {@code x-death} JSON array: prepends the new entry to
     * any pre-existing array (newest-first). Malformed prior arrays are dropped
     * silently and the new entry becomes the sole element.
     *
     * <p>Package-private for direct unit testing.
     */
    ArrayNode buildXDeathArray(Headers existingHeaders, String queueName, String reason,
                                String exchange, String routingKey) {
        ArrayNode out = MAPPER.createArrayNode();
        ObjectNode entry = MAPPER.createObjectNode();
        entry.put(F_QUEUE, queueName);
        entry.put(F_REASON, reason);
        entry.put(F_COUNT, 1);
        entry.put(F_EXCHANGE, exchange == null ? "" : exchange);
        ArrayNode rks = entry.putArray(F_ROUTING_KEYS);
        rks.add(routingKey == null ? "" : routingKey);
        // Seconds since epoch — matches RabbitMQ's x-death.time field.
        entry.put(F_TIME, System.currentTimeMillis() / 1000L);
        out.add(entry);

        Header existing = lastHeader(existingHeaders, HDR_X_DEATH);
        if (existing == null) {
            return out;
        }
        try {
            JsonNode prior = MAPPER.readTree(existing.value());
            if (prior.isArray()) {
                for (JsonNode node : prior) {
                    out.add(node);
                }
            }
        } catch (Exception e) {
            log.debug("Malformed prior x-death header dropped (replaced with fresh entry): {}", e.toString());
        }
        return out;
    }

    // ------------------------------------------------------------------
    //  Header utilities
    // ------------------------------------------------------------------

    private static String headerValue(Headers headers, String key, String fallback) {
        Header h = lastHeader(headers, key);
        if (h == null || h.value() == null) {
            return fallback;
        }
        return new String(h.value(), StandardCharsets.UTF_8);
    }

    private static Header lastHeader(Headers headers, String key) {
        Header last = null;
        for (Header h : headers) {
            if (key.equals(h.key())) {
                last = h;
            }
        }
        return last;
    }

    // ------------------------------------------------------------------
    //  Injected dependencies
    // ------------------------------------------------------------------

    /**
     * Resolves queue metadata by {@code (vhost, queueName)}. Returns
     * {@code null} when the queue is not registered. Production wiring will
     * delegate to {@link WsRoutingMetadataManager#getQueue} via a
     * {@code BiFunction} adapter; tests pass a lambda over an in-memory map.
     */
    @FunctionalInterface
    public interface QueueResolver extends BiFunction<String, String, QueueMetadata> {
        @Override
        QueueMetadata apply(String vhost, String queueName);

        default QueueMetadata lookup(String vhost, String queueName) {
            return apply(vhost, queueName);
        }
    }

    /**
     * Per-queue DLX produce hand-off. Production wiring builds a
     * {@code RequestChannel.Request} carrying a single-record produce request
     * and returns a future that completes when the broker's
     * {@code ProduceResponse} arrives. Tests typically capture the call in
     * memory and return a pre-completed future.
     */
    @FunctionalInterface
    public interface DlxProduceSink {
        /**
         * Enqueue a DLX produce request.
         *
         * @param topic    Kafka topic for the destination DLQ (typically
         *                 {@code "ws." + dlqName})
         * @param key      record key bytes ({@code null} → round-robin partition)
         * @param value    record value bytes (the original payload, untouched)
         * @param headers  rebuilt headers including the appended {@code x-death}
         *                 entry
         * @return future completing when the produce is acknowledged by the
         *         broker; failed future on produce error
         */
        CompletableFuture<Void> enqueue(String topic, byte[] key, byte[] value, List<Header> headers);
    }
}
