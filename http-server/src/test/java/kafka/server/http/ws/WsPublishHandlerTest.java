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
package kafka.server.http.ws;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import kafka.server.http.HttpRequestTranslator;
import kafka.server.http.routing.ExchangeManager;
import kafka.server.http.routing.RoutingEngine;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link WsPublishHandler}.
 *
 * <p>Drives the handler with a captured {@link WsConnectionContext} bound to a
 * Mockito-mocked {@link ChannelHandlerContext} so we can assert on every frame
 * written back to the client.
 *
 * // Time: Created - TASK-WS1.11
 */
class WsPublishHandlerTest {

    private static final ObjectMapper MAPPER = HttpRequestTranslator.MAPPER;

    private ExchangeManager exchangeManager;
    private RoutingEngine routingEngine;
    private WsMessageSerializer messageSerializer;
    private List<CapturedProduce> captured;
    private WsPublishHandler.ProduceRequestSink sink;
    private WsPublishHandler handler;

    private ChannelHandlerContext mockChannelCtx;
    private WsConnectionContext connCtx;
    private final List<String> writtenFrames = new ArrayList<>();

    /** Captured shape of a sink.enqueue() call. */
    private static final class CapturedProduce {
        final String topic;
        final byte[] key;
        final byte[] value;
        final int headerCount;
        final Long publishId;
        final boolean confirmsEnabled;

        CapturedProduce(String topic, byte[] key, byte[] value, int headerCount,
                        Long publishId, boolean confirmsEnabled) {
            this.topic = topic;
            this.key = key;
            this.value = value;
            this.headerCount = headerCount;
            this.publishId = publishId;
            this.confirmsEnabled = confirmsEnabled;
        }
    }

    @BeforeEach
    void setUp() {
        exchangeManager = mock(ExchangeManager.class);
        routingEngine = mock(RoutingEngine.class);
        messageSerializer = new WsMessageSerializer();

        captured = new ArrayList<>();
        sink = (topic, serialized, publishId, confirmsEnabled, ctx) ->
            captured.add(new CapturedProduce(
                topic, serialized.key(), serialized.value(),
                serialized.headers().size(), publishId, confirmsEnabled));

        handler = new WsPublishHandler(
            exchangeManager,
            routingEngine,
            messageSerializer,
            queue -> "ws." + queue,
            sink);

        // Build a WS connection context backed by a mocked Netty channel.
        mockChannelCtx = mock(ChannelHandlerContext.class);
        Channel mockChannel = mock(Channel.class);
        when(mockChannelCtx.channel()).thenReturn(mockChannel);
        when(mockChannel.isActive()).thenReturn(true);

        ChannelFuture future = mock(ChannelFuture.class);
        when(mockChannelCtx.writeAndFlush(any())).thenAnswer(inv -> {
            Object arg = inv.getArgument(0);
            if (arg instanceof TextWebSocketFrame) {
                TextWebSocketFrame f = (TextWebSocketFrame) arg;
                writtenFrames.add(f.text());
                f.release();
            }
            return future;
        });

        KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "alice");
        InetSocketAddress remote = new InetSocketAddress("127.0.0.1", 12345);
        connCtx = new WsConnectionContext(
            "ws-1-test0001", principal, "/", mockChannelCtx, remote);
    }

    // ------------------------------------------------------------------
    //  Constructor validation
    // ------------------------------------------------------------------

    @Test
    void constructor_nullExchangeManager_throws() {
        assertThrows(NullPointerException.class, () ->
            new WsPublishHandler(null, routingEngine, messageSerializer,
                q -> "ws." + q, sink));
    }

    @Test
    void constructor_nullRoutingEngine_throws() {
        assertThrows(NullPointerException.class, () ->
            new WsPublishHandler(exchangeManager, null, messageSerializer,
                q -> "ws." + q, sink));
    }

    @Test
    void constructor_nullSerializer_throws() {
        assertThrows(NullPointerException.class, () ->
            new WsPublishHandler(exchangeManager, routingEngine, null,
                q -> "ws." + q, sink));
    }

    @Test
    void constructor_nullQueueToTopicFn_throws() {
        assertThrows(NullPointerException.class, () ->
            new WsPublishHandler(exchangeManager, routingEngine, messageSerializer,
                null, sink));
    }

    @Test
    void constructor_nullSink_throws() {
        assertThrows(NullPointerException.class, () ->
            new WsPublishHandler(exchangeManager, routingEngine, messageSerializer,
                q -> "ws." + q, null));
    }

    // ------------------------------------------------------------------
    //  Happy path — publish to valid exchange with matching queues
    // ------------------------------------------------------------------

    @Test
    void handlePublish_validExchange_singleQueue_enqueuesOneProduce() {
        when(exchangeManager.getExchange("/", "events"))
            .thenReturn(new ExchangeMetadata("events", "/", "direct", true, false, false, java.util.Map.of()));
        when(routingEngine.route(eq("events"), eq("order.created"), any()))
            .thenReturn(Set.of("order-events"));

        handler.handlePublish(buildPublishFrame("events", "order.created", "hello", 42L, false), connCtx);

        assertEquals(1, captured.size(), "exactly one ProduceRequest enqueued");
        CapturedProduce c = captured.get(0);
        assertEquals("ws.order-events", c.topic);
        assertArraysEqualString("order.created", c.key);
        assertArraysEqualString("hello", c.value);
        assertTrue(c.headerCount >= 3, "expected _ws_exchange + _ws_routing_key + _ws_vhost headers at minimum");
        assertEquals(Long.valueOf(42L), c.publishId);
        // No confirm frame yet — confirms disabled by default on a fresh context.
        assertFalse(c.confirmsEnabled);
        assertTrue(writtenFrames.isEmpty(), "no sync frame expected when confirms disabled");
    }

    @Test
    void handlePublish_validExchange_multipleQueues_enqueuesOnePerQueue() {
        when(exchangeManager.getExchange("/", "fan"))
            .thenReturn(new ExchangeMetadata("fan", "/", "direct", true, false, false, java.util.Map.of()));
        when(routingEngine.route(eq("fan"), eq("broadcast"), any()))
            .thenReturn(new java.util.LinkedHashSet<>(List.of("q1", "q2", "q3")));

        handler.handlePublish(buildPublishFrame("fan", "broadcast", "msg", 7L, false), connCtx);

        assertEquals(3, captured.size(), "one produce per matched queue");
        List<String> topics = new ArrayList<>();
        for (CapturedProduce c : captured) {
            topics.add(c.topic);
        }
        assertTrue(topics.contains("ws.q1"));
        assertTrue(topics.contains("ws.q2"));
        assertTrue(topics.contains("ws.q3"));
    }

    @Test
    void handlePublish_confirmsEnabled_propagatesFlagToSink() {
        connCtx.enablePublishConfirms();

        when(exchangeManager.getExchange("/", "events"))
            .thenReturn(new ExchangeMetadata("events", "/", "direct", true, false, false, java.util.Map.of()));
        when(routingEngine.route(eq("events"), eq("k"), any()))
            .thenReturn(Set.of("q"));

        handler.handlePublish(buildPublishFrame("events", "k", "payload", 99L, false), connCtx);

        assertEquals(1, captured.size());
        assertTrue(captured.get(0).confirmsEnabled,
            "sink should be told confirms are enabled for this connection");
        assertEquals(Long.valueOf(99L), captured.get(0).publishId);
    }

    // ------------------------------------------------------------------
    //  Empty route handling (mandatory vs non-mandatory)
    // ------------------------------------------------------------------

    @Test
    void handlePublish_emptyRoute_nonMandatory_silentDrop() {
        when(exchangeManager.getExchange("/", "events"))
            .thenReturn(new ExchangeMetadata("events", "/", "direct", true, false, false, java.util.Map.of()));
        when(routingEngine.route(eq("events"), eq("nowhere"), any()))
            .thenReturn(Set.of());

        handler.handlePublish(buildPublishFrame("events", "nowhere", "x", 1L, false), connCtx);

        assertTrue(captured.isEmpty(), "nothing enqueued when no queues match");
        assertTrue(writtenFrames.isEmpty(), "non-mandatory = silent drop (no frame)");
    }

    @Test
    void handlePublish_emptyRoute_mandatory_emitsReturnedFrame() {
        when(exchangeManager.getExchange("/", "events"))
            .thenReturn(new ExchangeMetadata("events", "/", "direct", true, false, false, java.util.Map.of()));
        when(routingEngine.route(eq("events"), eq("nowhere"), any()))
            .thenReturn(Set.of());

        handler.handlePublish(buildPublishFrame("events", "nowhere", "x", 5L, true), connCtx);

        assertTrue(captured.isEmpty(), "no enqueue for unrouted message");
        assertEquals(1, writtenFrames.size(), "exactly one returned frame");
        String frame = writtenFrames.get(0);
        assertTrue(frame.contains("\"type\":\"returned\""), "frame=" + frame);
        assertTrue(frame.contains("\"exchange\":\"events\""), "frame=" + frame);
        assertTrue(frame.contains("\"routingKey\":\"nowhere\""), "frame=" + frame);
        assertTrue(frame.contains("\"replyCode\":312"), "frame=" + frame);
        assertTrue(frame.contains("NO_ROUTE"), "frame=" + frame);
        assertTrue(frame.contains("\"publishId\":5"), "publishId echoed: frame=" + frame);
    }

    // ------------------------------------------------------------------
    //  Unknown exchange
    // ------------------------------------------------------------------

    @Test
    void handlePublish_unknownExchange_emitsErrorFrame() {
        when(exchangeManager.getExchange("/", "ghost")).thenReturn(null);

        handler.handlePublish(buildPublishFrame("ghost", "k", "x", 3L, false), connCtx);

        assertTrue(captured.isEmpty());
        assertEquals(1, writtenFrames.size(), "one error frame expected");
        String frame = writtenFrames.get(0);
        assertTrue(frame.contains("\"type\":\"error\""), "frame=" + frame);
        assertTrue(frame.contains("NOT_FOUND"), "frame=" + frame);
        assertTrue(frame.contains("\"publishId\":3"), "publishId echoed: frame=" + frame);
        // And routing engine must not be called.
        verify(routingEngine, never()).route(any(), any(), any());
    }

    // ------------------------------------------------------------------
    //  Malformed frames
    // ------------------------------------------------------------------

    @Test
    void handlePublish_missingExchange_emitsErrorFrame() {
        ObjectNode frame = MAPPER.createObjectNode();
        frame.put("type", "publish");
        frame.put("routingKey", "k");
        frame.put("publishId", 1);
        frame.putObject("message").put("body", "x");

        handler.handlePublish(frame, connCtx);

        assertTrue(captured.isEmpty());
        assertEquals(1, writtenFrames.size());
        String out = writtenFrames.get(0);
        assertTrue(out.contains("\"type\":\"error\""), "out=" + out);
        assertTrue(out.contains("exchange"), "out=" + out);
        verify(exchangeManager, never()).getExchange(any(), any());
    }

    @Test
    void handlePublish_missingRoutingKey_emitsErrorFrame() {
        ObjectNode frame = MAPPER.createObjectNode();
        frame.put("type", "publish");
        frame.put("exchange", "events");
        frame.put("publishId", 1);
        frame.putObject("message").put("body", "x");

        handler.handlePublish(frame, connCtx);

        assertTrue(captured.isEmpty());
        assertEquals(1, writtenFrames.size());
        String out = writtenFrames.get(0);
        assertTrue(out.contains("\"type\":\"error\""), "out=" + out);
        assertTrue(out.contains("routingKey"), "out=" + out);
    }

    @Test
    void handlePublish_missingMessage_emitsErrorFrame() {
        ObjectNode frame = MAPPER.createObjectNode();
        frame.put("type", "publish");
        frame.put("exchange", "events");
        frame.put("routingKey", "k");
        frame.put("publishId", 1);
        // no message field at all

        handler.handlePublish(frame, connCtx);

        assertTrue(captured.isEmpty());
        assertEquals(1, writtenFrames.size());
        String out = writtenFrames.get(0);
        assertTrue(out.contains("\"type\":\"error\""), "out=" + out);
        assertTrue(out.contains("message"), "out=" + out);
    }

    // ------------------------------------------------------------------
    //  Routing engine error propagation
    // ------------------------------------------------------------------

    @Test
    void handlePublish_routingEngineThrows_emitsErrorFrame() {
        when(exchangeManager.getExchange("/", "bad"))
            .thenReturn(new ExchangeMetadata("bad", "/", "topic", true, false, false, java.util.Map.of()));
        when(routingEngine.route(eq("bad"), any(), any()))
            .thenThrow(new UnsupportedOperationException("Topic not supported in Phase 1"));

        handler.handlePublish(buildPublishFrame("bad", "k", "x", 11L, false), connCtx);

        assertTrue(captured.isEmpty());
        assertEquals(1, writtenFrames.size());
        String out = writtenFrames.get(0);
        assertTrue(out.contains("\"type\":\"error\""), "out=" + out);
        assertTrue(out.contains("\"publishId\":11"), "out=" + out);
    }

    // ------------------------------------------------------------------
    //  Headers propagation to RoutingEngine
    // ------------------------------------------------------------------

    @Test
    void handlePublish_passesUserHeadersToRoutingEngine() {
        when(exchangeManager.getExchange("/", "hx"))
            .thenReturn(new ExchangeMetadata("hx", "/", "direct", true, false, false, java.util.Map.of()));
        when(routingEngine.route(eq("hx"), eq("k"), any()))
            .thenReturn(Set.of("q"));

        ObjectNode frame = buildPublishFrame("hx", "k", "p", 1L, false);
        ObjectNode appHeaders = frame.with("message").putObject("headers");
        appHeaders.put("x-trace-id", "abc-123");
        appHeaders.put("x-origin", "svc-a");

        AtomicReference<java.util.Map<String, String>> capturedHeaders = new AtomicReference<>();
        when(routingEngine.route(eq("hx"), eq("k"), any())).thenAnswer(inv -> {
            @SuppressWarnings("unchecked")
            java.util.Map<String, String> h = (java.util.Map<String, String>) inv.getArgument(2);
            capturedHeaders.set(h);
            return Set.of("q");
        });

        handler.handlePublish(frame, connCtx);

        java.util.Map<String, String> h = capturedHeaders.get();
        assertNotNull(h, "routing engine should have been called");
        assertEquals("abc-123", h.get("x-trace-id"));
        assertEquals("svc-a", h.get("x-origin"));
    }

    // ------------------------------------------------------------------
    //  Helpers
    // ------------------------------------------------------------------

    private ObjectNode buildPublishFrame(String exchange, String routingKey,
                                         String body, long publishId, boolean mandatory) {
        ObjectNode frame = MAPPER.createObjectNode();
        frame.put("type", "publish");
        frame.put("exchange", exchange);
        frame.put("routingKey", routingKey);
        frame.put("mandatory", mandatory);
        frame.put("publishId", publishId);
        ObjectNode message = frame.putObject("message");
        message.put("body", body);
        return frame;
    }

    private static void assertArraysEqualString(String expected, byte[] actual) {
        assertEquals(expected, new String(actual, StandardCharsets.UTF_8));
    }
}
