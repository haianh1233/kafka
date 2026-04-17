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
// Time: Created - TASK-WS2.08
package kafka.server.http.rest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import kafka.server.http.routing.RoutingEngine;
import kafka.server.http.ws.WsMessageSerializer;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * REST handlers for message operations: publish, get, ack, nack.
 *
 * <p>Complements the REST CRUD endpoints introduced in WS2.06/WS2.07 by letting
 * HTTP-only clients participate in the exchange routing model without opening a
 * WebSocket.
 *
 * <h3>Endpoints</h3>
 * <ul>
 *   <li>{@code POST /v1/exchanges/{exchange}/publish} — route a message through
 *       the exchange/binding layer. Returns per-queue offsets for every queue
 *       that matched. With {@code mandatory: true}, an unroutable message
 *       surfaces as {@code replyCode: 312 / NO_ROUTE} (still HTTP 200).</li>
 *   <li>{@code POST /v1/queues/{queue}/get} — pull up to {@code count} messages
 *       (capped at {@value #MAX_GET_COUNT}). With {@code ackMode: "manual"},
 *       the response includes a server-generated {@code sessionId} that the
 *       client must pass back in the subsequent ack/nack call.</li>
 *   <li>{@code POST /v1/queues/{queue}/ack} — commit delivery tags.</li>
 *   <li>{@code POST /v1/queues/{queue}/nack} — reject delivery tags with an
 *       optional requeue flag.</li>
 * </ul>
 *
 * <h3>Delivery-tag tracking</h3>
 * <p>Because REST is stateless, the handler maintains an in-memory
 * {@link ConcurrentHashMap} of short-lived tracking sessions keyed by
 * {@code sessionId}. Each session holds a map of {@code deliveryTag → {topic,
 * partition, offset}} populated on {@code /get} with {@code ackMode=manual}
 * and drained by ack/nack calls. Sessions are best-effort: no expiry thread is
 * wired here; tests rely on session entries remaining live for the duration
 * of a single JUnit run. See Limitations in the task file.
 *
 * // Time: Created - TASK-WS2.08
 */
public final class MessageRestHandler {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int MAX_GET_COUNT = 100;

    /** AMQP 0.9.1 NO_ROUTE reply code. */
    private static final int AMQP_NO_ROUTE = 312;

    // --- Dependencies (all optional — null-guarded at call sites) ---

    private final RoutingEngine routingEngine;
    private final WsMessageSerializer messageSerializer;
    /** queue → Kafka topic; {@code null}-safe wrapper supplied below. */
    private final Function<String, String> queueToTopicFn;
    private final PublishSink publishSink;
    private final GetSink getSink;
    private final AckSink ackSink;

    // --- Session tracking for manual ack ---

    private final Map<String, Map<Long, DeliveryRef>> sessions = new ConcurrentHashMap<>();
    private final AtomicLong deliveryTagCounter = new AtomicLong(1);

    public MessageRestHandler(RoutingEngine routingEngine,
                              WsMessageSerializer messageSerializer,
                              Function<String, String> queueToTopicFn,
                              PublishSink publishSink,
                              GetSink getSink,
                              AckSink ackSink) {
        this.routingEngine = Objects.requireNonNull(routingEngine, "routingEngine");
        this.messageSerializer = Objects.requireNonNull(messageSerializer, "messageSerializer");
        this.queueToTopicFn = Objects.requireNonNull(queueToTopicFn, "queueToTopicFn");
        this.publishSink = Objects.requireNonNull(publishSink, "publishSink");
        this.getSink = Objects.requireNonNull(getSink, "getSink");
        this.ackSink = Objects.requireNonNull(ackSink, "ackSink");
    }

    // ------------------------------------------------------------------
    // publish
    // ------------------------------------------------------------------

    private static final String[] OPTIONAL_AMQP_PROPS = {
        "deliveryMode", "correlationId", "replyTo", "messageId",
        "contentEncoding", "priority", "type", "userId", "appId", "expiration"
    };

    /**
     * {@code POST /v1/exchanges/{exchange}/publish}.
     *
     * <p>Body: {@code { routingKey, headers, body, contentType, mandatory }}
     * (routingKey may also be supplied via the {@code X-Routing-Key} header,
     * which wins when both are present).
     */
    public FullHttpResponse handlePublish(String exchangeName, String vhost,
                                          FullHttpRequest request) {
        Objects.requireNonNull(exchangeName, "exchangeName");
        Objects.requireNonNull(vhost, "vhost");
        Objects.requireNonNull(request, "request");

        JsonNode body;
        try {
            body = readBody(request);
        } catch (Exception e) {
            return errorResponse(HttpResponseStatus.BAD_REQUEST,
                "Malformed JSON body: " + e.getMessage());
        }

        String routingKey = resolveRoutingKey(request, body);
        boolean mandatory = boolField(body, "mandatory", false);
        ObjectNode messageNode = buildMessageNode(body);
        Map<String, String> userHeaders = readHeaderMap(body);

        Set<String> queues;
        try {
            queues = routingEngine.route(exchangeName, routingKey, userHeaders);
        } catch (IllegalArgumentException e) {
            return errorResponse(HttpResponseStatus.NOT_FOUND, e.getMessage());
        } catch (RuntimeException e) {
            return errorResponse(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                "Routing failed: " + e.getMessage());
        }

        if (queues == null || queues.isEmpty()) {
            return unroutableResponse(mandatory);
        }

        WsMessageSerializer.SerializedMessage serialized;
        try {
            serialized = messageSerializer.serialize(exchangeName, routingKey, messageNode, vhost);
        } catch (RuntimeException e) {
            return errorResponse(HttpResponseStatus.BAD_REQUEST,
                "Serialization failed: " + e.getMessage());
        }

        return fanOutPublish(queues, serialized);
    }

    /** Header wins over JSON body. Returns empty string when neither is set. */
    private static String resolveRoutingKey(FullHttpRequest request, JsonNode body) {
        String hdrRk = request.headers().get("X-Routing-Key");
        if (hdrRk != null && !hdrRk.isEmpty()) {
            return hdrRk;
        }
        return stringField(body, "routingKey", "");
    }

    /**
     * Builds the synthetic "message" node passed to {@link WsMessageSerializer},
     * mirroring the REST body fields onto the WS publish frame shape.
     */
    private static ObjectNode buildMessageNode(JsonNode body) {
        ObjectNode msg = MAPPER.createObjectNode();
        copyField(body, msg, "body");
        JsonNode headers = body.get("headers");
        if (headers != null && headers.isObject()) {
            msg.set("headers", headers);
        }
        copyField(body, msg, "contentType");
        for (String prop : OPTIONAL_AMQP_PROPS) {
            copyField(body, msg, prop);
        }
        return msg;
    }

    private static void copyField(JsonNode src, ObjectNode dst, String field) {
        if (src.has(field)) {
            dst.set(field, src.get(field));
        }
    }

    private FullHttpResponse unroutableResponse(boolean mandatory) {
        ObjectNode node = MAPPER.createObjectNode();
        node.put("routed", false);
        node.putArray("queues");
        if (mandatory) {
            node.put("replyCode", AMQP_NO_ROUTE);
            node.put("replyText", "NO_ROUTE");
        }
        return jsonResponse(HttpResponseStatus.OK, node);
    }

    private FullHttpResponse fanOutPublish(Set<String> queues,
                                           WsMessageSerializer.SerializedMessage serialized) {
        List<QueueOffset> offsets = new ArrayList<>();
        for (String queue : queues) {
            String topic = queueToTopicFn.apply(queue);
            if (topic == null || topic.isEmpty()) {
                continue;
            }
            try {
                QueueOffset o = publishSink.publish(queue, topic, serialized);
                if (o != null) {
                    offsets.add(o);
                }
            } catch (RuntimeException e) {
                return errorResponse(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                    "Produce failed for queue '" + queue + "': " + e.getMessage());
            }
        }
        ObjectNode node = MAPPER.createObjectNode();
        node.put("routed", true);
        ArrayNode qArr = node.putArray("queues");
        ArrayNode oArr = node.putArray("offsets");
        for (QueueOffset o : offsets) {
            qArr.add(o.queue());
            ObjectNode entry = oArr.addObject();
            entry.put("queue", o.queue());
            entry.put("partition", o.partition());
            entry.put("offset", o.offset());
        }
        return jsonResponse(HttpResponseStatus.OK, node);
    }

    // ------------------------------------------------------------------
    // get
    // ------------------------------------------------------------------

    /**
     * {@code POST /v1/queues/{queue}/get}.
     *
     * <p>Body: {@code { count, ackMode, encoding }}. Count defaults to 1 and is
     * silently capped at {@value #MAX_GET_COUNT}.
     */
    public FullHttpResponse handleGet(String queueName, String vhost,
                                      FullHttpRequest request) {
        Objects.requireNonNull(queueName, "queueName");
        Objects.requireNonNull(vhost, "vhost");
        Objects.requireNonNull(request, "request");

        JsonNode body;
        try {
            body = readBody(request);
        } catch (Exception e) {
            return errorResponse(HttpResponseStatus.BAD_REQUEST,
                "Malformed JSON body: " + e.getMessage());
        }

        int count = clampCount(intField(body, "count", 1));
        String ackMode = stringField(body, "ackMode", "auto");
        String encoding = stringField(body, "encoding", "auto");

        String topic = queueToTopicFn.apply(queueName);
        if (topic == null || topic.isEmpty()) {
            return errorResponse(HttpResponseStatus.NOT_FOUND,
                "Queue '" + queueName + "' is not mapped to a topic");
        }

        List<FetchedMessage> messages;
        try {
            List<FetchedMessage> fetched = getSink.fetch(queueName, topic, count);
            messages = fetched == null ? Collections.emptyList() : fetched;
        } catch (RuntimeException e) {
            return errorResponse(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                "Fetch failed: " + e.getMessage());
        }

        return buildGetResponse(queueName, topic, ackMode, encoding, messages);
    }

    private static int clampCount(int count) {
        if (count < 1) return 1;
        if (count > MAX_GET_COUNT) return MAX_GET_COUNT;
        return count;
    }

    private FullHttpResponse buildGetResponse(String queueName, String topic,
                                              String ackMode, String encoding,
                                              List<FetchedMessage> messages) {
        boolean manual = "manual".equals(ackMode);
        String sessionId = null;
        Map<Long, DeliveryRef> sessionMap = null;
        if (manual && !messages.isEmpty()) {
            sessionId = UUID.randomUUID().toString();
            sessionMap = new ConcurrentHashMap<>();
            sessions.put(sessionId, sessionMap);
        }

        ObjectNode root = MAPPER.createObjectNode();
        if (sessionId != null) {
            root.put("sessionId", sessionId);
        }
        ArrayNode arr = root.putArray("messages");

        for (FetchedMessage m : messages) {
            long tag = deliveryTagCounter.getAndIncrement();
            arr.add(renderMessage(queueName, m, tag, encoding));
            dispatchAckMode(queueName, topic, ackMode, sessionMap, m, tag);
        }
        return jsonResponse(HttpResponseStatus.OK, root);
    }

    private static ObjectNode renderMessage(String queueName, FetchedMessage m,
                                            long tag, String encoding) {
        ObjectNode msg = MAPPER.createObjectNode();
        msg.put("deliveryTag", tag);
        msg.put("queue", queueName);
        msg.put("partition", m.partition());
        msg.put("offset", m.offset());
        if (m.key() != null) {
            msg.put("key", new String(m.key(), StandardCharsets.UTF_8));
        }
        if (m.value() != null) {
            renderBody(msg, m.value(), encoding);
        }
        if (m.headers() != null && !m.headers().isEmpty()) {
            ObjectNode h = msg.putObject("headers");
            for (Map.Entry<String, String> e : m.headers().entrySet()) {
                h.put(e.getKey(), e.getValue());
            }
        }
        return msg;
    }

    private static void renderBody(ObjectNode msg, byte[] value, String encoding) {
        if ("base64".equalsIgnoreCase(encoding)) {
            msg.put("body", java.util.Base64.getEncoder().encodeToString(value));
            msg.put("encoding", "base64");
        } else {
            msg.put("body", new String(value, StandardCharsets.UTF_8));
            msg.put("encoding", "utf8");
        }
    }

    private void dispatchAckMode(String queueName, String topic, String ackMode,
                                 Map<Long, DeliveryRef> sessionMap,
                                 FetchedMessage m, long tag) {
        if ("manual".equals(ackMode) && sessionMap != null) {
            sessionMap.put(tag, new DeliveryRef(topic, m.partition(), m.offset()));
        } else if ("auto".equals(ackMode)) {
            try {
                ackSink.commit(queueName, topic, m.partition(), m.offset());
            } catch (RuntimeException ignore) {
                // Auto-ack failures are swallowed — the get succeeds.
            }
        }
        // "reject-requeue": no commit, no tracking — message stays on queue.
    }

    // ------------------------------------------------------------------
    // ack / nack
    // ------------------------------------------------------------------

    /**
     * {@code POST /v1/queues/{queue}/ack}.
     *
     * <p>Body: {@code { sessionId, deliveryTag, multiple }}.
     */
    public FullHttpResponse handleAck(String queueName, String vhost,
                                      FullHttpRequest request) {
        return handleAckOrNack(queueName, vhost, request, /* ack */ true);
    }

    /**
     * {@code POST /v1/queues/{queue}/nack}.
     *
     * <p>Body: {@code { sessionId, deliveryTag, multiple, requeue }}. When
     * {@code requeue=true}, the offset is <b>not</b> committed; the message
     * stays available on the queue.
     */
    public FullHttpResponse handleNack(String queueName, String vhost,
                                       FullHttpRequest request) {
        return handleAckOrNack(queueName, vhost, request, /* ack */ false);
    }

    /** Parsed ack/nack request. */
    private record AckArgs(String sessionId, long deliveryTag, boolean multiple, boolean requeue) { }

    private FullHttpResponse handleAckOrNack(String queueName, String vhost,
                                             FullHttpRequest request, boolean isAck) {
        Objects.requireNonNull(queueName, "queueName");
        Objects.requireNonNull(vhost, "vhost");
        Objects.requireNonNull(request, "request");

        JsonNode body;
        try {
            body = readBody(request);
        } catch (Exception e) {
            return errorResponse(HttpResponseStatus.BAD_REQUEST,
                "Malformed JSON body: " + e.getMessage());
        }

        AckArgs args = parseAckArgs(body, isAck);
        FullHttpResponse validation = validateAckArgs(args);
        if (validation != null) return validation;

        Map<Long, DeliveryRef> sessionMap = sessions.get(args.sessionId());
        if (sessionMap == null) {
            return errorResponse(HttpResponseStatus.NOT_FOUND,
                "Unknown session: " + args.sessionId());
        }

        List<Long> targets = selectTargets(sessionMap, args);
        if (targets == null) {
            return errorResponse(HttpResponseStatus.NOT_FOUND,
                "Unknown deliveryTag for session: " + args.deliveryTag());
        }

        FullHttpResponse err = applyAckDecisions(queueName, sessionMap, targets, isAck, args.requeue());
        if (err != null) return err;

        // Clean up empty session — best-effort.
        if (sessionMap.isEmpty()) {
            sessions.remove(args.sessionId(), sessionMap);
        }

        ObjectNode node = MAPPER.createObjectNode();
        node.put("ok", true);
        node.put("count", targets.size());
        return jsonResponse(HttpResponseStatus.OK, node);
    }

    private static AckArgs parseAckArgs(JsonNode body, boolean isAck) {
        String sessionId = stringField(body, "sessionId", null);
        long tag = body.has("deliveryTag") ? body.get("deliveryTag").asLong() : -1L;
        boolean multiple = boolField(body, "multiple", false);
        boolean requeue = boolField(body, "requeue", isAck);
        return new AckArgs(sessionId, tag, multiple, requeue);
    }

    private FullHttpResponse validateAckArgs(AckArgs args) {
        if (args.sessionId() == null || args.sessionId().isEmpty()) {
            return errorResponse(HttpResponseStatus.BAD_REQUEST,
                "Missing required field: sessionId");
        }
        if (args.deliveryTag() < 0) {
            return errorResponse(HttpResponseStatus.BAD_REQUEST,
                "Missing required field: deliveryTag");
        }
        return null;
    }

    private static List<Long> selectTargets(Map<Long, DeliveryRef> sessionMap, AckArgs args) {
        List<Long> targets = new ArrayList<>();
        if (args.multiple()) {
            for (Long t : sessionMap.keySet()) {
                if (t <= args.deliveryTag()) {
                    targets.add(t);
                }
            }
            return targets;
        }
        if (!sessionMap.containsKey(args.deliveryTag())) {
            return null;
        }
        targets.add(args.deliveryTag());
        return targets;
    }

    /**
     * Commits (or skips committing) every selected tag. Returns a non-null
     * {@link FullHttpResponse} if the ack sink threw — the caller short-circuits.
     */
    private FullHttpResponse applyAckDecisions(String queueName,
                                               Map<Long, DeliveryRef> sessionMap,
                                               List<Long> targets, boolean isAck,
                                               boolean requeue) {
        for (Long tag : targets) {
            DeliveryRef ref = sessionMap.remove(tag);
            if (ref == null) continue;
            if (!shouldCommit(isAck, requeue)) continue;
            try {
                ackSink.commit(queueName, ref.topic(), ref.partition(), ref.offset());
            } catch (RuntimeException e) {
                String kind = isAck ? "Ack" : "Nack";
                return errorResponse(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                    kind + " commit failed: " + e.getMessage());
            }
        }
        return null;
    }

    /**
     * ACK always commits. NACK commits only when {@code requeue=false} (advance
     * past the poisoned message); {@code requeue=true} leaves the offset so the
     * next fetch replays the message.
     */
    private static boolean shouldCommit(boolean isAck, boolean requeue) {
        return isAck || !requeue;
    }

    // ------------------------------------------------------------------
    // Public auxiliary types
    // ------------------------------------------------------------------

    /** Per-queue offset result returned by {@link PublishSink}. */
    public record QueueOffset(String queue, int partition, long offset) { }

    /** Delivery reference held inside a session map. */
    private record DeliveryRef(String topic, int partition, long offset) { }

    /** Message pulled by {@link GetSink}. Headers may be {@code null}. */
    public record FetchedMessage(int partition, long offset, byte[] key, byte[] value,
                                 Map<String, String> headers) { }

    /**
     * Publishes a serialized record to the Kafka topic backing the given queue
     * and returns the offset that Kafka assigned. May return {@code null} if
     * the publish produced no meaningful offset (e.g., topic missing).
     */
    @FunctionalInterface
    public interface PublishSink {
        QueueOffset publish(String queue, String topic,
                            WsMessageSerializer.SerializedMessage serialized);
    }

    /**
     * Fetches up to {@code count} messages from {@code topic}. Implementations
     * may block up to a short timeout; the REST endpoint is expected to return
     * an empty array if no message arrives in time.
     */
    @FunctionalInterface
    public interface GetSink {
        List<FetchedMessage> fetch(String queue, String topic, int count);
    }

    /** Commits a single (topic, partition, offset) triple for the given queue. */
    @FunctionalInterface
    public interface AckSink {
        void commit(String queue, String topic, int partition, long offset);
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    private static JsonNode readBody(FullHttpRequest request) throws Exception {
        if (request.content() == null || !request.content().isReadable()) {
            return MAPPER.createObjectNode();
        }
        byte[] bytes = new byte[request.content().readableBytes()];
        request.content().getBytes(request.content().readerIndex(), bytes);
        if (bytes.length == 0) {
            return MAPPER.createObjectNode();
        }
        return MAPPER.readTree(bytes);
    }

    private static String stringField(JsonNode node, String field, String dflt) {
        if (node == null || !node.hasNonNull(field)) return dflt;
        return node.get(field).asText();
    }

    private static boolean boolField(JsonNode node, String field, boolean dflt) {
        if (node == null || !node.hasNonNull(field)) return dflt;
        return node.get(field).asBoolean(dflt);
    }

    private static int intField(JsonNode node, String field, int dflt) {
        if (node == null || !node.hasNonNull(field)) return dflt;
        return node.get(field).asInt(dflt);
    }

    private static Map<String, String> readHeaderMap(JsonNode body) {
        Map<String, String> headers = new LinkedHashMap<>();
        if (body == null) return headers;
        JsonNode hn = body.get("headers");
        if (hn == null || !hn.isObject()) return headers;
        Iterator<String> names = hn.fieldNames();
        while (names.hasNext()) {
            String name = names.next();
            JsonNode v = hn.get(name);
            if (v == null || v.isNull()) continue;
            headers.put(name, v.asText());
        }
        return headers;
    }

    private FullHttpResponse errorResponse(HttpResponseStatus status, String message) {
        ObjectNode node = MAPPER.createObjectNode();
        node.put("errorCode", -1);
        node.put("errorMessage", message);
        return jsonResponse(status, node);
    }

    private FullHttpResponse jsonResponse(HttpResponseStatus status, JsonNode body) {
        byte[] bytes = body.toString().getBytes(StandardCharsets.UTF_8);
        FullHttpResponse response = new DefaultFullHttpResponse(
            HttpVersion.HTTP_1_1, status, Unpooled.wrappedBuffer(bytes));
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json");
        response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, bytes.length);
        return response;
    }

    /** Snapshot size of live sessions — for tests/metrics. */
    public int activeSessionCount() {
        return sessions.size();
    }

    /**
     * Exposed for tests: records a session + delivery tag mapping directly,
     * bypassing the get endpoint. Not intended for production use.
     */
    void recordDeliveryForTest(String sessionId, long tag, String topic, int partition, long offset) {
        sessions.computeIfAbsent(sessionId, k -> new ConcurrentHashMap<>())
            .put(tag, new DeliveryRef(topic, partition, offset));
    }

    /** Exposed for tests — true if the session currently tracks any tags. */
    boolean hasSession(String sessionId) {
        return sessions.containsKey(sessionId);
    }

    /** Max count clamp, exposed for test assertions. */
    public static int maxGetCount() {
        return MAX_GET_COUNT;
    }

    /** Convenience for tests — empty/immutable headers list passthrough. */
    static Map<String, String> emptyHeaders() {
        return Collections.emptyMap();
    }
}
