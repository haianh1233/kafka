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
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import kafka.server.http.routing.Binding;
import kafka.server.http.routing.E2EBinding;
import kafka.server.http.routing.RoutingEngine;
import kafka.server.http.ws.WsMessageSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * // Time: Created - TASK-WS2.08
 */
class MessageRestHandlerTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String VHOST = "/";

    // --- Test doubles ---

    /** Routing engine configured with per-test bindings. */
    private final Map<String, String> exchangeTypes = new HashMap<>();
    private final Map<String, List<Binding>> exchangeBindings = new HashMap<>();
    private RoutingEngine routingEngine;

    /** Captured publish offsets per queue — returned in iteration order. */
    private final Map<String, AtomicLong> nextOffsets = new LinkedHashMap<>();
    private final List<CapturedPublish> publishes = new ArrayList<>();

    /** Messages to serve from /get per queue. */
    private final Map<String, List<MessageRestHandler.FetchedMessage>> fetchable =
        new ConcurrentHashMap<>();

    /** Captured ack commits. */
    private final List<CapturedCommit> commits = new ArrayList<>();

    private MessageRestHandler handler;

    private static final class CapturedPublish {
        final String queue;
        final String topic;
        final WsMessageSerializer.SerializedMessage serialized;

        CapturedPublish(String queue, String topic, WsMessageSerializer.SerializedMessage s) {
            this.queue = queue;
            this.topic = topic;
            this.serialized = s;
        }
    }

    private static final class CapturedCommit {
        final String queue;
        final String topic;
        final int partition;
        final long offset;

        CapturedCommit(String queue, String topic, int partition, long offset) {
            this.queue = queue;
            this.topic = topic;
            this.partition = partition;
            this.offset = offset;
        }
    }

    @BeforeEach
    void setUp() {
        routingEngine = new RoutingEngine(
            exchangeTypes::get,
            name -> exchangeBindings.getOrDefault(name, Collections.emptyList()),
            name -> Collections.<E2EBinding>emptyList());

        handler = new MessageRestHandler(
            routingEngine,
            new WsMessageSerializer(),
            // queue -> topic
            q -> "ws." + q,
            // PublishSink
            (queue, topic, serialized) -> {
                publishes.add(new CapturedPublish(queue, topic, serialized));
                long off = nextOffsets
                    .computeIfAbsent(queue, k -> new AtomicLong(0))
                    .getAndIncrement();
                return new MessageRestHandler.QueueOffset(queue, 0, off);
            },
            // GetSink
            (queue, topic, count) -> {
                List<MessageRestHandler.FetchedMessage> avail = fetchable.get(queue);
                if (avail == null || avail.isEmpty()) return List.of();
                int take = Math.min(count, avail.size());
                List<MessageRestHandler.FetchedMessage> out = new ArrayList<>(take);
                for (int i = 0; i < take; i++) {
                    out.add(avail.remove(0));
                }
                return out;
            },
            // AckSink
            (queue, topic, partition, offset) ->
                commits.add(new CapturedCommit(queue, topic, partition, offset))
        );
    }

    // ==================================================================
    // publish
    // ==================================================================

    @Test
    void publish_routedToMultipleQueues_returnsOffsetsPerQueue() throws Exception {
        exchangeTypes.put("ex1", "fanout");
        exchangeBindings.put("ex1", List.of(
            new Binding("ex1", "order-events", "", Collections.emptyMap()),
            new Binding("ex1", "audit-log", "", Collections.emptyMap())));

        FullHttpResponse resp = handler.handlePublish("ex1", VHOST,
            requestBody("{\"routingKey\":\"orders.created\",\"body\":\"hello\"}", null));

        assertEquals(HttpResponseStatus.OK, resp.status());
        JsonNode body = parseBody(resp);
        assertTrue(body.get("routed").asBoolean());
        assertEquals(2, body.get("queues").size());
        assertEquals(2, body.get("offsets").size());
        // Each offset entry has queue/partition/offset.
        JsonNode first = body.get("offsets").get(0);
        assertTrue(first.has("queue"));
        assertTrue(first.has("partition"));
        assertTrue(first.has("offset"));
        // publishes captured = 2 (one per queue).
        assertEquals(2, publishes.size());
    }

    @Test
    void publish_mandatory_noRoute_returnsReplyCode312() throws Exception {
        exchangeTypes.put("ex1", "direct");
        // no bindings — routing yields empty.
        FullHttpResponse resp = handler.handlePublish("ex1", VHOST,
            requestBody("{\"routingKey\":\"orders\",\"body\":\"x\",\"mandatory\":true}", null));

        assertEquals(HttpResponseStatus.OK, resp.status());
        JsonNode body = parseBody(resp);
        assertFalse(body.get("routed").asBoolean());
        assertEquals(0, body.get("queues").size());
        assertEquals(312, body.get("replyCode").asInt());
        assertEquals("NO_ROUTE", body.get("replyText").asText());
    }

    @Test
    void publish_nonMandatory_noRoute_returnsRoutedFalse() throws Exception {
        exchangeTypes.put("ex1", "direct");
        FullHttpResponse resp = handler.handlePublish("ex1", VHOST,
            requestBody("{\"routingKey\":\"x\",\"body\":\"y\"}", null));
        assertEquals(HttpResponseStatus.OK, resp.status());
        JsonNode body = parseBody(resp);
        assertFalse(body.get("routed").asBoolean());
        // No replyCode when non-mandatory.
        assertFalse(body.has("replyCode"));
    }

    @Test
    void publish_xRoutingKeyHeader_overridesBody() throws Exception {
        exchangeTypes.put("ex1", "direct");
        exchangeBindings.put("ex1", List.of(
            new Binding("ex1", "q1", "from-header", Collections.emptyMap())));

        Map<String, String> headers = new HashMap<>();
        headers.put("X-Routing-Key", "from-header");
        FullHttpResponse resp = handler.handlePublish("ex1", VHOST,
            requestBody("{\"routingKey\":\"from-body\",\"body\":\"x\"}", headers));

        assertEquals(HttpResponseStatus.OK, resp.status());
        JsonNode body = parseBody(resp);
        assertTrue(body.get("routed").asBoolean());
        assertEquals(1, publishes.size());
        assertEquals("q1", publishes.get(0).queue);
    }

    @Test
    void publish_unknownExchange_returns404() throws Exception {
        // exchangeTypes empty → exchangeTypeFn returns null → IllegalArgumentException.
        FullHttpResponse resp = handler.handlePublish("ghost", VHOST,
            requestBody("{\"routingKey\":\"x\",\"body\":\"y\"}", null));
        assertEquals(HttpResponseStatus.NOT_FOUND, resp.status());
    }

    @Test
    void publish_malformedJson_returns400() {
        FullHttpRequest req = new DefaultFullHttpRequest(
            HttpVersion.HTTP_1_1, HttpMethod.POST, "/v1/exchanges/ex1/publish",
            Unpooled.wrappedBuffer("not-json".getBytes(StandardCharsets.UTF_8)));
        FullHttpResponse resp = handler.handlePublish("ex1", VHOST, req);
        assertEquals(HttpResponseStatus.BAD_REQUEST, resp.status());
    }

    // ==================================================================
    // get
    // ==================================================================

    @Test
    void get_returnsMessages() throws Exception {
        fetchable.put("orders", new ArrayList<>(List.of(
            new MessageRestHandler.FetchedMessage(
                0, 42L,
                "k".getBytes(StandardCharsets.UTF_8),
                "hello".getBytes(StandardCharsets.UTF_8),
                null))));

        FullHttpResponse resp = handler.handleGet("orders", VHOST,
            requestBody("{\"count\":1}", null));

        assertEquals(HttpResponseStatus.OK, resp.status());
        JsonNode body = parseBody(resp);
        assertEquals(1, body.get("messages").size());
        JsonNode msg = body.get("messages").get(0);
        assertEquals("orders", msg.get("queue").asText());
        assertEquals(0, msg.get("partition").asInt());
        assertEquals(42L, msg.get("offset").asLong());
        assertEquals("hello", msg.get("body").asText());
    }

    @Test
    void get_emptyQueue_returnsEmptyArray() throws Exception {
        FullHttpResponse resp = handler.handleGet("orders", VHOST,
            requestBody("{\"count\":5}", null));
        assertEquals(HttpResponseStatus.OK, resp.status());
        JsonNode body = parseBody(resp);
        assertNotNull(body.get("messages"));
        assertEquals(0, body.get("messages").size());
    }

    @Test
    void get_countExceedsMax_cappedAt100() throws Exception {
        // Seed 150 messages; expect only up to 100 pulled.
        List<MessageRestHandler.FetchedMessage> seeded = new ArrayList<>();
        for (int i = 0; i < 150; i++) {
            seeded.add(new MessageRestHandler.FetchedMessage(
                0, i,
                null, ("m" + i).getBytes(StandardCharsets.UTF_8), null));
        }
        fetchable.put("orders", seeded);

        FullHttpResponse resp = handler.handleGet("orders", VHOST,
            requestBody("{\"count\":1000}", null));
        assertEquals(HttpResponseStatus.OK, resp.status());
        JsonNode body = parseBody(resp);
        assertEquals(100, body.get("messages").size());
        // fetchable should still have 50 left since sink was asked for 100.
        assertEquals(50, fetchable.get("orders").size());
    }

    @Test
    void get_autoAckMode_commitsEachMessage() throws Exception {
        fetchable.put("orders", new ArrayList<>(List.of(
            new MessageRestHandler.FetchedMessage(
                0, 1L, null, "a".getBytes(StandardCharsets.UTF_8), null),
            new MessageRestHandler.FetchedMessage(
                0, 2L, null, "b".getBytes(StandardCharsets.UTF_8), null))));

        FullHttpResponse resp = handler.handleGet("orders", VHOST,
            requestBody("{\"count\":2,\"ackMode\":\"auto\"}", null));
        assertEquals(HttpResponseStatus.OK, resp.status());
        assertEquals(2, commits.size(), "auto-ack must commit each message");
        assertEquals(1L, commits.get(0).offset);
        assertEquals(2L, commits.get(1).offset);
    }

    @Test
    void get_manualAckMode_returnsSessionId_doesNotAutoCommit() throws Exception {
        fetchable.put("orders", new ArrayList<>(List.of(
            new MessageRestHandler.FetchedMessage(
                0, 7L, null, "a".getBytes(StandardCharsets.UTF_8), null))));

        FullHttpResponse resp = handler.handleGet("orders", VHOST,
            requestBody("{\"count\":1,\"ackMode\":\"manual\"}", null));
        assertEquals(HttpResponseStatus.OK, resp.status());
        JsonNode body = parseBody(resp);
        assertTrue(body.has("sessionId"));
        assertTrue(handler.hasSession(body.get("sessionId").asText()));
        // No auto-ack.
        assertEquals(0, commits.size());
    }

    @Test
    void get_rejectRequeueMode_doesNotCommit() throws Exception {
        fetchable.put("orders", new ArrayList<>(List.of(
            new MessageRestHandler.FetchedMessage(
                0, 1L, null, "a".getBytes(StandardCharsets.UTF_8), null))));

        FullHttpResponse resp = handler.handleGet("orders", VHOST,
            requestBody("{\"count\":1,\"ackMode\":\"reject-requeue\"}", null));
        assertEquals(HttpResponseStatus.OK, resp.status());
        assertEquals(0, commits.size());
    }

    @Test
    void get_base64Encoding_usesBase64() throws Exception {
        fetchable.put("orders", new ArrayList<>(List.of(
            new MessageRestHandler.FetchedMessage(
                0, 1L, null, new byte[]{1, 2, 3, (byte) 0xff}, null))));

        FullHttpResponse resp = handler.handleGet("orders", VHOST,
            requestBody("{\"count\":1,\"encoding\":\"base64\"}", null));
        JsonNode body = parseBody(resp);
        JsonNode m = body.get("messages").get(0);
        assertEquals("base64", m.get("encoding").asText());
        assertEquals("AQID/w==", m.get("body").asText());
    }

    // ==================================================================
    // ack
    // ==================================================================

    @Test
    void ack_commitsOffset() throws Exception {
        // Seed a session + delivery tag directly.
        handler.recordDeliveryForTest("sess-1", 42L, "ws.orders", 0, 99L);

        FullHttpResponse resp = handler.handleAck("orders", VHOST,
            requestBody("{\"sessionId\":\"sess-1\",\"deliveryTag\":42}", null));

        assertEquals(HttpResponseStatus.OK, resp.status());
        assertEquals(1, commits.size());
        CapturedCommit c = commits.get(0);
        assertEquals("orders", c.queue);
        assertEquals("ws.orders", c.topic);
        assertEquals(0, c.partition);
        assertEquals(99L, c.offset);
    }

    @Test
    void ack_multiple_commitsAllTagsUpTo() throws Exception {
        handler.recordDeliveryForTest("s", 1L, "ws.q", 0, 1L);
        handler.recordDeliveryForTest("s", 2L, "ws.q", 0, 2L);
        handler.recordDeliveryForTest("s", 3L, "ws.q", 0, 3L);
        handler.recordDeliveryForTest("s", 4L, "ws.q", 0, 4L); // not acked

        FullHttpResponse resp = handler.handleAck("q", VHOST,
            requestBody("{\"sessionId\":\"s\",\"deliveryTag\":3,\"multiple\":true}", null));

        assertEquals(HttpResponseStatus.OK, resp.status());
        assertEquals(3, commits.size(), "multiple ack covers tags <=3");
    }

    @Test
    void ack_unknownSession_returns404() throws Exception {
        FullHttpResponse resp = handler.handleAck("q", VHOST,
            requestBody("{\"sessionId\":\"missing\",\"deliveryTag\":1}", null));
        assertEquals(HttpResponseStatus.NOT_FOUND, resp.status());
    }

    @Test
    void ack_unknownDeliveryTag_returns404() throws Exception {
        handler.recordDeliveryForTest("s", 1L, "ws.q", 0, 1L);
        FullHttpResponse resp = handler.handleAck("q", VHOST,
            requestBody("{\"sessionId\":\"s\",\"deliveryTag\":999}", null));
        assertEquals(HttpResponseStatus.NOT_FOUND, resp.status());
    }

    // ==================================================================
    // nack
    // ==================================================================

    @Test
    void nack_requeueTrue_doesNotCommit() throws Exception {
        handler.recordDeliveryForTest("s", 10L, "ws.q", 0, 200L);
        FullHttpResponse resp = handler.handleNack("q", VHOST,
            requestBody("{\"sessionId\":\"s\",\"deliveryTag\":10,\"requeue\":true}", null));
        assertEquals(HttpResponseStatus.OK, resp.status());
        assertEquals(0, commits.size(), "requeue=true must NOT commit");
    }

    @Test
    void nack_requeueFalse_commitsToAdvancePastPoisoned() throws Exception {
        handler.recordDeliveryForTest("s", 10L, "ws.q", 0, 200L);
        FullHttpResponse resp = handler.handleNack("q", VHOST,
            requestBody("{\"sessionId\":\"s\",\"deliveryTag\":10,\"requeue\":false}", null));
        assertEquals(HttpResponseStatus.OK, resp.status());
        assertEquals(1, commits.size(), "requeue=false advances the commit watermark");
    }

    @Test
    void nack_unknownSession_returns404() throws Exception {
        FullHttpResponse resp = handler.handleNack("q", VHOST,
            requestBody("{\"sessionId\":\"missing\",\"deliveryTag\":1,\"requeue\":true}",
                null));
        assertEquals(HttpResponseStatus.NOT_FOUND, resp.status());
    }

    @Test
    void ack_missingSessionId_returns400() throws Exception {
        FullHttpResponse resp = handler.handleAck("q", VHOST,
            requestBody("{\"deliveryTag\":1}", null));
        assertEquals(HttpResponseStatus.BAD_REQUEST, resp.status());
    }

    @Test
    void ack_missingDeliveryTag_returns400() throws Exception {
        FullHttpResponse resp = handler.handleAck("q", VHOST,
            requestBody("{\"sessionId\":\"s\"}", null));
        assertEquals(HttpResponseStatus.BAD_REQUEST, resp.status());
    }

    // ==================================================================
    // helpers
    // ==================================================================

    private static FullHttpRequest requestBody(String json, Map<String, String> headers) {
        byte[] bytes = json == null ? new byte[0] : json.getBytes(StandardCharsets.UTF_8);
        FullHttpRequest req = new DefaultFullHttpRequest(
            HttpVersion.HTTP_1_1, HttpMethod.POST, "/v1/any",
            Unpooled.wrappedBuffer(bytes));
        if (headers != null) {
            for (Map.Entry<String, String> e : headers.entrySet()) {
                req.headers().set(e.getKey(), e.getValue());
            }
        }
        return req;
    }

    private static JsonNode parseBody(FullHttpResponse resp) throws Exception {
        byte[] bytes = new byte[resp.content().readableBytes()];
        resp.content().getBytes(resp.content().readerIndex(), bytes);
        if (bytes.length == 0) return MAPPER.readTree("{}");
        return MAPPER.readTree(bytes);
    }

    // Silence unused-import warnings for assertions we use conditionally.
    @SuppressWarnings("unused")
    private static void unused() {
        assertNull(null);
    }
}
