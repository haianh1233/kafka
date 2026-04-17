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
// Time: Created - TASK-WS1.11
// Time: Update - TASK-WS3.01 - recordPending via WsPublisherConfirmTracker
// Time: Update - TASK-WS3.02 - enhanced mandatory return semantics
// Time: Update - TASK-WS3.08 - added metrics recording
package kafka.server.http.ws;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import kafka.server.http.HttpRequestTranslator;
import kafka.server.http.routing.ExchangeManager;
import kafka.server.http.routing.RoutingEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/**
 * Handles WebSocket {@code publish} frames end-to-end: frame parsing →
 * exchange existence check → routing → per-queue Kafka record construction →
 * hand-off to an injected {@link ProduceRequestSink}.
 *
 * <p>The sink abstracts the eventual integration with the Kafka request pipeline
 * ({@code RequestChannel.tryEnqueue} in production). Tests capture produce
 * calls directly through the sink. Queue-to-topic resolution is likewise
 * supplied as a {@link Function} placeholder until {@code QueueManager} exists
 * (WS-later); the default topology maps {@code queue} → {@code ws.<queue>}.
 *
 * <h3>Error handling (all surface as frames on the WS channel)</h3>
 * <ul>
 *   <li>Missing {@code exchange}/{@code routingKey}/{@code message} → {@code error}
 *       frame with {@code INVALID_REQUEST}.</li>
 *   <li>Unknown exchange + {@code mandatory=true} → {@code returned} frame
 *       (design doc §5.7: missing exchange is treated as "unroutable").</li>
 *   <li>Unknown exchange + {@code mandatory=false} → silent drop.</li>
 *   <li>Internal exchange ({@code internal=true}) → {@code error} frame with
 *       {@code ACCESS_REFUSED}. Takes precedence over mandatory: no returned
 *       frame, no published confirm (TASK-WS3.02).</li>
 *   <li>Routing engine threw (e.g. unsupported exchange type) → {@code error}
 *       frame with {@code INTERNAL_ERROR}.</li>
 *   <li>No matching queues + {@code mandatory=true} → {@code returned} frame
 *       (AMQP reply code 312 / {@code NO_ROUTE}).</li>
 *   <li>No matching queues + {@code mandatory=false} → silent drop.</li>
 *   <li>Any unroutable case + publisher confirms enabled → additionally emits
 *       a {@code published} confirm (AMQP: mandatory return does not negate
 *       basic.ack).</li>
 * </ul>
 *
 * <h3>Threading</h3>
 * <p>{@link #handlePublish} is called from the Netty event loop by
 * {@link WsFrameHandler}. The sink is responsible for non-blocking hand-off
 * (e.g., {@code RequestChannel.tryEnqueue}); publisher-confirm completion is
 * wired by {@link kafka.server.http.HttpProcessor#handleWsResponse} via the
 * {@code wsPublishId} request-local property — this handler stamps that
 * property via the sink's {@code publishId} argument.</p>
 *
 * <h3>Limitations (see task file Limitations section)</h3>
 * <ul>
 *   <li>{@code QueueManager} not yet built; the queue→topic function is an
 *       injected placeholder. Production wiring should substitute a real
 *       resolver.</li>
 *   <li>Publisher-confirm correlation is only stamped here; the full async
 *       callback (emitting {@code published} / {@code publish-failed} frames) is
 *       owned by {@link kafka.server.http.HttpProcessor} and the sink.</li>
 *   <li>Timeouts, retries, and dead-lettering are deferred to Phase 3/4.</li>
 * </ul>
 */
public final class WsPublishHandler {

    private static final Logger log = LoggerFactory.getLogger(WsPublishHandler.class);
    private static final ObjectMapper MAPPER = HttpRequestTranslator.MAPPER;

    // --- Frame field names ---
    private static final String FIELD_EXCHANGE = "exchange";
    private static final String FIELD_ROUTING_KEY = "routingKey";
    private static final String FIELD_MESSAGE = "message";
    private static final String FIELD_MANDATORY = "mandatory";
    private static final String FIELD_PUBLISH_ID = "publishId";
    private static final String FIELD_HEADERS = "headers";

    // --- Error codes (string tokens mirrored elsewhere in the WS layer) ---
    private static final String ERR_INVALID = "INVALID_REQUEST";
    private static final String ERR_INTERNAL = "INTERNAL_ERROR";
    /** TASK-WS3.02: reject attempts to publish to an {@code internal=true} exchange. */
    private static final String ERR_ACCESS_REFUSED = "ACCESS_REFUSED";

    /** AMQP 0.9.1 NO_ROUTE reply code (returned-frame replyCode). */
    private static final int AMQP_NO_ROUTE = 312;

    // --- Dependencies ---
    private final ExchangeManager exchangeManager;
    private final RoutingEngine routingEngine;
    private final WsMessageSerializer messageSerializer;
    private final Function<String, String> queueToTopicFn;
    private final ProduceRequestSink sink;
    private final WsMetrics metrics;

    /**
     * Convenience constructor for tests that do not care about metrics. Delegates
     * to the main constructor with {@code metrics = null} — the handler treats a
     * null metrics instance as a silent no-op on every record call.
     */
    public WsPublishHandler(ExchangeManager exchangeManager,
                            RoutingEngine routingEngine,
                            WsMessageSerializer messageSerializer,
                            Function<String, String> queueToTopicFn,
                            ProduceRequestSink sink) {
        this(exchangeManager, routingEngine, messageSerializer, queueToTopicFn, sink, null);
    }

    /**
     * @param exchangeManager    used to verify the target exchange exists in the
     *                           caller's vhost
     * @param routingEngine      resolves {@code (exchange, routingKey, headers)}
     *                           → matched queue names
     * @param messageSerializer  converts the publish frame's {@code message}
     *                           object into Kafka record components
     * @param queueToTopicFn     queue name → Kafka topic name; placeholder until
     *                           {@code QueueManager} exists
     * @param sink               hand-off for per-queue produce hand-off (injected
     *                           to keep the handler testable without a running
     *                           {@code RequestChannel})
     * @param metrics            WS metrics sink (may be {@code null} — useful for
     *                           tests; production wiring injects a real instance)
     */
    public WsPublishHandler(ExchangeManager exchangeManager,
                            RoutingEngine routingEngine,
                            WsMessageSerializer messageSerializer,
                            Function<String, String> queueToTopicFn,
                            ProduceRequestSink sink,
                            WsMetrics metrics) {
        this.exchangeManager = Objects.requireNonNull(exchangeManager, "exchangeManager");
        this.routingEngine = Objects.requireNonNull(routingEngine, "routingEngine");
        this.messageSerializer = Objects.requireNonNull(messageSerializer, "messageSerializer");
        this.queueToTopicFn = Objects.requireNonNull(queueToTopicFn, "queueToTopicFn");
        this.sink = Objects.requireNonNull(sink, "sink");
        this.metrics = metrics; // nullable by design — see Javadoc
    }

    // ------------------------------------------------------------------
    //  Entry point
    // ------------------------------------------------------------------

    /**
     * Processes a publish frame.
     *
     * @param publishFrame the parsed publish JSON frame (must not be {@code null})
     * @param ctx          the WebSocket connection context (vhost + principal +
     *                     channel for emitting response frames)
     */
    public void handlePublish(JsonNode publishFrame, WsConnectionContext ctx) {
        Objects.requireNonNull(publishFrame, "publishFrame");
        Objects.requireNonNull(ctx, "ctx");

        Long publishId = extractPublishId(publishFrame);
        ParsedPublish parsed = parsePublishFrame(publishFrame, ctx, publishId);
        if (parsed == null) {
            return; // error frame already emitted
        }

        String vhost = ctx.vhost();
        ExchangeMetadata exchange = exchangeManager.getExchange(vhost, parsed.exchange);

        // TASK-WS3.02: Non-existent exchange behaviour differs from WS1.11:
        //   - mandatory=true  → emit returned frame (NO_ROUTE)
        //   - mandatory=false → silent drop (no error frame)
        // In both cases, if confirms are enabled the message is still acknowledged
        // as accepted via a published confirm (design doc §5.7 parity with
        // unroutable-but-known-exchange behaviour).
        if (exchange == null) {
            handleUnrouted(ctx, publishId, parsed);
            return;
        }

        // TASK-WS3.02: Internal exchanges (`internal=true`) cannot be published to
        // by clients. This check runs BEFORE routing and supersedes the mandatory
        // flag — the message is rejected, not returned. Produces no published
        // confirm because the broker never accepted the message.
        if (exchange.internal()) {
            emitError(ctx, publishId, ERR_ACCESS_REFUSED,
                "Cannot publish to internal exchange '" + parsed.exchange + "'");
            return;
        }

        Set<String> matchedQueues = runRoute(parsed, ctx, publishId);
        if (matchedQueues == null) {
            return; // routing threw — error already emitted
        }
        if (matchedQueues.isEmpty()) {
            handleUnrouted(ctx, publishId, parsed);
            return;
        }

        WsMessageSerializer.SerializedMessage serialized = serializeOrError(parsed, ctx, publishId, vhost);
        if (serialized == null) {
            return;
        }

        fanOut(matchedQueues, serialized, ctx, publishId);
    }

    /**
     * TASK-WS3.02: centralises the "message was accepted but could not be routed"
     * path, shared between "exchange missing" and "routing returned empty set".
     *
     * <p>Emission matrix:</p>
     * <pre>
     *   mandatory | confirms | frames emitted
     *   ----------+----------+---------------------------------------
     *   false     | false    | (silent)
     *   false     | true     | published
     *   true      | false    | returned
     *   true      | true     | returned + published
     * </pre>
     *
     * <p>The {@code published} confirm reflects AMQP semantics: a mandatory
     * return does not negate the basic.ack. The broker accepted responsibility
     * for the message; it just had no queue to deliver it to.</p>
     */
    private void handleUnrouted(WsConnectionContext ctx, Long publishId, ParsedPublish parsed) {
        if (parsed.mandatory) {
            emitReturned(ctx, publishId, parsed.exchange, parsed.routingKey, parsed.message);
            if (metrics != null) {
                metrics.mandatoryReturnRate.mark();
            }
        }
        emitPublishedIfConfirmsEnabled(ctx, publishId);
    }

    /**
     * Record + immediately confirm the publishId so the client sees a
     * {@code published} frame, if confirms are enabled and the client stamped
     * a publishId on this publish. Safe to call with {@code null} publishId
     * (no-op) or when confirms are disabled (no-op).
     */
    private static void emitPublishedIfConfirmsEnabled(WsConnectionContext ctx, Long publishId) {
        if (publishId == null || !ctx.isPublishConfirmsEnabled()) {
            return;
        }
        WsPublisherConfirmTracker tracker = ctx.confirmTracker();
        tracker.recordPending(publishId);
        tracker.confirmSuccess(publishId);
    }

    /** Fully parsed + validated publish frame. */
    private record ParsedPublish(String exchange, String routingKey, JsonNode message,
                                 boolean mandatory) { }

    /**
     * Extracts and validates the required fields from the raw frame. Emits an
     * error frame and returns {@code null} on the first missing field.
     */
    private ParsedPublish parsePublishFrame(JsonNode frame, WsConnectionContext ctx, Long publishId) {
        String exchange = textOrNull(frame, FIELD_EXCHANGE);
        if (exchange == null) {
            emitError(ctx, publishId, ERR_INVALID, "Missing required field: exchange");
            return null;
        }
        String routingKey = textOrNull(frame, FIELD_ROUTING_KEY);
        if (routingKey == null) {
            emitError(ctx, publishId, ERR_INVALID, "Missing required field: routingKey");
            return null;
        }
        JsonNode message = frame.get(FIELD_MESSAGE);
        if (message == null || message.isNull() || !message.isObject()) {
            emitError(ctx, publishId, ERR_INVALID, "Missing required field: message");
            return null;
        }
        boolean mandatory = frame.hasNonNull(FIELD_MANDATORY)
            && frame.get(FIELD_MANDATORY).asBoolean(false);
        return new ParsedPublish(exchange, routingKey, message, mandatory);
    }

    /**
     * Runs the routing engine, translating runtime exceptions (e.g., unsupported
     * exchange type) into an error frame. Returns {@code null} if an error was
     * already emitted.
     */
    private Set<String> runRoute(ParsedPublish parsed, WsConnectionContext ctx, Long publishId) {
        Map<String, String> userHeaders = extractUserHeaders(parsed.message);
        try {
            return routingEngine.route(parsed.exchange, parsed.routingKey, userHeaders);
        } catch (RuntimeException e) {
            log.warn("Routing failed for exchange={} rk={} publishId={}: {}",
                parsed.exchange, parsed.routingKey, publishId, e.toString());
            emitError(ctx, publishId, ERR_INTERNAL, "Routing failed: " + e.getMessage());
            return null;
        }
    }

    /**
     * Serializes the message body + headers once; emits an error frame and
     * returns {@code null} on failure.
     */
    private WsMessageSerializer.SerializedMessage serializeOrError(
            ParsedPublish parsed, WsConnectionContext ctx, Long publishId, String vhost) {
        try {
            return messageSerializer.serialize(parsed.exchange, parsed.routingKey, parsed.message, vhost);
        } catch (RuntimeException e) {
            log.warn("Serialization failed for exchange={} rk={} publishId={}: {}",
                parsed.exchange, parsed.routingKey, publishId, e.toString());
            emitError(ctx, publishId, ERR_INVALID, "Message serialization failed: " + e.getMessage());
            return null;
        }
    }

    /** Pushes the serialized record to the sink once per matched queue. */
    private void fanOut(Set<String> matchedQueues,
                        WsMessageSerializer.SerializedMessage serialized,
                        WsConnectionContext ctx, Long publishId) {
        boolean confirmsEnabled = ctx.isPublishConfirmsEnabled();
        // If confirms are enabled and the client stamped a publishId, record it
        // as pending so the produce-completion callback can emit the confirm.
        // No-op when confirmsEnabled is false or publishId is null. Recording
        // happens before enqueue to avoid a race where the response fires before
        // the pending slot is populated.
        if (confirmsEnabled && publishId != null) {
            ctx.confirmTracker().recordPending(publishId);
        }
        for (String queue : matchedQueues) {
            String topic = queueToTopicFn.apply(queue);
            if (topic == null || topic.isEmpty()) {
                log.warn("Queue '{}' resolved to null/empty topic; skipping", queue);
                continue;
            }
            try {
                sink.enqueue(topic, serialized, publishId, confirmsEnabled, ctx);
                if (metrics != null) {
                    metrics.publishRate.mark();
                }
            } catch (RuntimeException e) {
                log.warn("Produce sink rejected publish for queue={} topic={}: {}",
                    queue, topic, e.toString());
                emitError(ctx, publishId, ERR_INTERNAL,
                    "Broker queue saturated for topic '" + topic + "'");
                // Continue — other queues may still succeed.
            }
        }
    }

    // ------------------------------------------------------------------
    //  Parsing helpers
    // ------------------------------------------------------------------

    private static Long extractPublishId(JsonNode frame) {
        JsonNode n = frame.get(FIELD_PUBLISH_ID);
        if (n == null || n.isNull()) {
            return null;
        }
        if (n.isNumber()) {
            return n.asLong();
        }
        // Non-numeric publishId is tolerated — we just cannot echo it.
        return null;
    }

    private static String textOrNull(JsonNode node, String field) {
        JsonNode child = node.get(field);
        if (child == null || child.isNull() || !child.isTextual()) {
            return null;
        }
        String s = child.asText();
        return s.isEmpty() ? null : s;
    }

    /**
     * Extracts user-supplied {@code message.headers} (AMQP application headers)
     * into a {@code Map<String,String>} suitable for the headers-exchange matcher.
     * Non-string values are stringified via {@link JsonNode#asText()}. Returns
     * an empty immutable map when {@code headers} is absent.
     */
    private static Map<String, String> extractUserHeaders(JsonNode message) {
        JsonNode headers = message.get(FIELD_HEADERS);
        if (headers == null || headers.isNull() || !headers.isObject()) {
            return Map.of();
        }
        Map<String, String> out = new HashMap<>();
        Iterator<String> names = headers.fieldNames();
        while (names.hasNext()) {
            String name = names.next();
            JsonNode v = headers.get(name);
            if (v == null || v.isNull()) {
                continue;
            }
            out.put(name, v.asText());
        }
        return out;
    }

    // ------------------------------------------------------------------
    //  Frame emitters
    // ------------------------------------------------------------------

    private void emitError(WsConnectionContext ctx, Long publishId,
                           String errorCode, String errorMessage) {
        ObjectNode frame = MAPPER.createObjectNode();
        frame.put("type", "error");
        frame.put("errorCode", errorCode);
        frame.put("errorMessage", errorMessage);
        if (publishId != null) {
            frame.put("publishId", publishId.longValue());
        }
        sendFrame(ctx, frame);
        if (metrics != null) {
            metrics.errorRate.mark();
        }
    }

    private static void emitReturned(WsConnectionContext ctx, Long publishId,
                                     String exchange, String routingKey,
                                     JsonNode message) {
        ObjectNode frame = MAPPER.createObjectNode();
        frame.put("type", "returned");
        frame.put("exchange", exchange);
        frame.put("routingKey", routingKey);
        frame.put("replyCode", AMQP_NO_ROUTE);
        frame.put("replyText", "NO_ROUTE");
        frame.set("message", message);
        if (publishId != null) {
            frame.put("publishId", publishId.longValue());
        }
        sendFrame(ctx, frame);
    }

    private static void sendFrame(WsConnectionContext ctx, ObjectNode frame) {
        try {
            ctx.sendFrame(MAPPER.writeValueAsString(frame));
        } catch (JsonProcessingException e) {
            // Should be impossible for the simple ObjectNodes we build here;
            // log and continue rather than throwing on the Netty event loop.
            log.warn("Failed to serialize WS frame to JSON (dropping)", e);
        }
    }

    // ------------------------------------------------------------------
    //  Sink abstraction
    // ------------------------------------------------------------------

    /**
     * Sink for per-queue produce hand-off. Typical production wiring builds a
     * {@code RequestChannel.Request} carrying a {@code ProduceRequest} for the
     * single-partition batch and calls {@code requestChannel.tryEnqueue(req)},
     * stamping the {@code publishId} onto the request's
     * {@code requestLocalProperties} under the {@code wsPublishId} key so that
     * {@link kafka.server.http.HttpProcessor} can emit the correct confirm
     * frame when the response arrives.
     *
     * <p>Tests typically use a simple capturing lambda.
     */
    @FunctionalInterface
    public interface ProduceRequestSink {

        /**
         * Invoked once per matched queue with the serialized record components.
         *
         * @param topic           Kafka topic name derived from the queue name
         * @param serialized      key/value/headers produced by
         *                        {@link WsMessageSerializer}
         * @param publishId       client-assigned correlation id ({@code null} when
         *                        the client omitted it)
         * @param confirmsEnabled whether publisher confirms are active on the
         *                        originating WS connection; the sink is
         *                        responsible for wiring the response path
         * @param ctx             the originating connection, supplied so the sink
         *                        can attach its channel id to the
         *                        {@code RequestChannel.Request}
         */
        void enqueue(String topic,
                     WsMessageSerializer.SerializedMessage serialized,
                     Long publishId,
                     boolean confirmsEnabled,
                     WsConnectionContext ctx);
    }
}
