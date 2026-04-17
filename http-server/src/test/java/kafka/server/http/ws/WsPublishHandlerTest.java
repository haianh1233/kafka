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
// Time: Update - TASK-WS3.02 - enhanced mandatory return semantics
// Time: Update - TASK-WS4.05 - integrated dedup cache
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
import static org.mockito.Mockito.atMostOnce;
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
    //  Unknown exchange — TASK-WS3.02 changed these semantics:
    //  non-existent exchange is treated as unroutable (not an error).
    // ------------------------------------------------------------------

    @Test
    void handlePublish_unknownExchange_nonMandatory_silentDrop() {
        // Design doc §5.7 / TASK-WS3.02: missing exchange + mandatory=false =>
        // silent drop, no error frame, no returned frame. Routing is NOT invoked.
        when(exchangeManager.getExchange("/", "ghost")).thenReturn(null);

        handler.handlePublish(buildPublishFrame("ghost", "k", "x", 3L, false), connCtx);

        assertTrue(captured.isEmpty());
        assertTrue(writtenFrames.isEmpty(),
            "non-existent exchange + mandatory=false must be silent; got: " + writtenFrames);
        verify(routingEngine, never()).route(any(), any(), any());
    }

    @Test
    void handlePublish_unknownExchange_mandatory_emitsReturnedFrame() {
        // Design doc §5.7 / TASK-WS3.02: missing exchange + mandatory=true =>
        // returned frame (replyCode 312 / NO_ROUTE).
        when(exchangeManager.getExchange("/", "ghost")).thenReturn(null);

        handler.handlePublish(buildPublishFrame("ghost", "k", "x", 3L, true), connCtx);

        assertTrue(captured.isEmpty());
        assertEquals(1, writtenFrames.size(), "one returned frame expected");
        String frame = writtenFrames.get(0);
        assertTrue(frame.contains("\"type\":\"returned\""), "frame=" + frame);
        assertTrue(frame.contains("\"replyCode\":312"), "frame=" + frame);
        assertTrue(frame.contains("\"publishId\":3"), "publishId echoed: frame=" + frame);
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
    //  Publisher confirms — TASK-WS3.01
    // ------------------------------------------------------------------

    @Test
    void handlePublish_confirmsEnabled_recordsPublishIdAsPending() {
        connCtx.enablePublishConfirms();
        when(exchangeManager.getExchange("/", "events"))
            .thenReturn(new ExchangeMetadata("events", "/", "direct", true, false, false, java.util.Map.of()));
        when(routingEngine.route(eq("events"), eq("k"), any()))
            .thenReturn(Set.of("q"));

        assertEquals(0, connCtx.confirmTracker().pendingCount());
        handler.handlePublish(buildPublishFrame("events", "k", "payload", 101L, false), connCtx);

        assertEquals(1, connCtx.confirmTracker().pendingCount(),
            "publishId should be recorded as pending for confirm");
    }

    @Test
    void handlePublish_confirmsDisabled_doesNotRecordPending() {
        // confirms not enabled on ctx
        when(exchangeManager.getExchange("/", "events"))
            .thenReturn(new ExchangeMetadata("events", "/", "direct", true, false, false, java.util.Map.of()));
        when(routingEngine.route(eq("events"), eq("k"), any()))
            .thenReturn(Set.of("q"));

        handler.handlePublish(buildPublishFrame("events", "k", "payload", 42L, false), connCtx);

        assertEquals(0, connCtx.confirmTracker().pendingCount(),
            "nothing should be recorded when confirms are disabled");
    }

    @Test
    void handlePublish_multiplePublishes_recordMonotonic() {
        connCtx.enablePublishConfirms();
        when(exchangeManager.getExchange("/", "events"))
            .thenReturn(new ExchangeMetadata("events", "/", "direct", true, false, false, java.util.Map.of()));
        when(routingEngine.route(eq("events"), eq("k"), any()))
            .thenReturn(Set.of("q"));

        handler.handlePublish(buildPublishFrame("events", "k", "m1", 1L, false), connCtx);
        handler.handlePublish(buildPublishFrame("events", "k", "m2", 2L, false), connCtx);
        handler.handlePublish(buildPublishFrame("events", "k", "m3", 3L, false), connCtx);

        assertEquals(3, connCtx.confirmTracker().pendingCount());
        // Verify the captured publishIds are strictly increasing (monotonic).
        assertEquals(3, captured.size());
        assertEquals(Long.valueOf(1L), captured.get(0).publishId);
        assertEquals(Long.valueOf(2L), captured.get(1).publishId);
        assertEquals(Long.valueOf(3L), captured.get(2).publishId);
    }

    @Test
    void handlePublish_multiQueueFanout_recordsPendingOnce() {
        // Multi-queue: publishId should be recorded once, not per-queue. That
        // way a single "published" confirm covers the whole fanout.
        connCtx.enablePublishConfirms();
        when(exchangeManager.getExchange("/", "fan"))
            .thenReturn(new ExchangeMetadata("fan", "/", "direct", true, false, false, java.util.Map.of()));
        when(routingEngine.route(eq("fan"), eq("b"), any()))
            .thenReturn(new java.util.LinkedHashSet<>(List.of("q1", "q2", "q3")));

        handler.handlePublish(buildPublishFrame("fan", "b", "msg", 55L, false), connCtx);

        assertEquals(3, captured.size(), "sink still sees one enqueue per matched queue");
        assertEquals(1, connCtx.confirmTracker().pendingCount(),
            "tracker should record publishId exactly once even with fanout");
    }

    @Test
    void handlePublish_confirmsEnabled_confirmSuccess_emitsPublishedFrame() throws Exception {
        // End-to-end tracker integration: after a successful produce callback
        // calls confirmSuccess via the tracker, a "published" frame lands on
        // the WS channel.
        connCtx.enablePublishConfirms();
        when(exchangeManager.getExchange("/", "events"))
            .thenReturn(new ExchangeMetadata("events", "/", "direct", true, false, false, java.util.Map.of()));
        when(routingEngine.route(eq("events"), eq("k"), any()))
            .thenReturn(Set.of("q"));

        handler.handlePublish(buildPublishFrame("events", "k", "payload", 77L, false), connCtx);
        assertEquals(0, writtenFrames.size(), "no frames emitted yet — produce not complete");

        // Simulate the produce callback.
        connCtx.confirmTracker().confirmSuccess(77L);

        assertEquals(1, writtenFrames.size());
        String frame = writtenFrames.get(0);
        assertTrue(frame.contains("\"type\":\"published\""), "frame=" + frame);
        assertTrue(frame.contains("\"publishId\":77"), "frame=" + frame);
    }

    @Test
    void handlePublish_confirmsEnabled_confirmFailure_emitsPublishFailedFrame() {
        connCtx.enablePublishConfirms();
        when(exchangeManager.getExchange("/", "events"))
            .thenReturn(new ExchangeMetadata("events", "/", "direct", true, false, false, java.util.Map.of()));
        when(routingEngine.route(eq("events"), eq("k"), any()))
            .thenReturn(Set.of("q"));

        handler.handlePublish(buildPublishFrame("events", "k", "payload", 88L, false), connCtx);
        connCtx.confirmTracker().confirmFailure(88L, "NOT_ENOUGH_REPLICAS", "ISR below min");

        assertEquals(1, writtenFrames.size());
        String frame = writtenFrames.get(0);
        assertTrue(frame.contains("\"type\":\"publish-failed\""), "frame=" + frame);
        assertTrue(frame.contains("\"publishId\":88"), "frame=" + frame);
        assertTrue(frame.contains("NOT_ENOUGH_REPLICAS"), "frame=" + frame);
        assertTrue(frame.contains("ISR below min"), "frame=" + frame);
    }

    // ------------------------------------------------------------------
    //  Deduplication — TASK-WS4.05
    // ------------------------------------------------------------------

    @Test
    void handlePublish_dedupHit_silentlyDropsAndConfirms() {
        // Build a handler with dedup enabled.
        WsDeduplicationCache dedup = new WsDeduplicationCache(100, 60_000);
        WsPublishHandler dedupHandler = new WsPublishHandler(
            exchangeManager, routingEngine, messageSerializer,
            queue -> "ws." + queue, sink, null, dedup);

        connCtx.enablePublishConfirms();
        when(exchangeManager.getExchange("/", "events"))
            .thenReturn(new ExchangeMetadata("events", "/", "direct", true, false, false, java.util.Map.of()));
        when(routingEngine.route(eq("events"), eq("k"), any()))
            .thenReturn(Set.of("q"));

        // First publish: routes + produces normally.
        dedupHandler.handlePublish(buildPublishFrameWithMessageId(
            "events", "k", "payload", 1L, false, "msg-A"), connCtx);
        assertEquals(1, captured.size(), "first publish enqueued");
        // First publish triggers a "published" confirm via the success path
        // (recordPending in fanOut + confirmSuccess later — only recordPending
        // fires synchronously, so no frame yet).
        assertEquals(0, writtenFrames.size(), "no confirm frame until produce completes");

        // Second publish — same (exchange, messageId) → duplicate hit.
        dedupHandler.handlePublish(buildPublishFrameWithMessageId(
            "events", "k", "payload", 2L, false, "msg-A"), connCtx);

        assertEquals(1, captured.size(),
            "duplicate must NOT enqueue another produce");
        // Routing not re-invoked for the duplicate.
        verify(routingEngine, atMostOnce()).route(eq("events"), eq("k"), any());
        assertEquals(1, writtenFrames.size(),
            "duplicate gets a synchronous published confirm");
        String frame = writtenFrames.get(0);
        assertTrue(frame.contains("\"type\":\"published\""), "frame=" + frame);
        assertTrue(frame.contains("\"publishId\":2"), "duplicate's publishId echoed: " + frame);
    }

    @Test
    void handlePublish_dedupMiss_admitsAndAddsToCache() {
        WsDeduplicationCache dedup = new WsDeduplicationCache(100, 60_000);
        WsPublishHandler dedupHandler = new WsPublishHandler(
            exchangeManager, routingEngine, messageSerializer,
            queue -> "ws." + queue, sink, null, dedup);

        when(exchangeManager.getExchange("/", "events"))
            .thenReturn(new ExchangeMetadata("events", "/", "direct", true, false, false, java.util.Map.of()));
        when(routingEngine.route(eq("events"), eq("k"), any()))
            .thenReturn(Set.of("q"));

        dedupHandler.handlePublish(buildPublishFrameWithMessageId(
            "events", "k", "payload", 1L, false, "msg-fresh"), connCtx);

        assertEquals(1, captured.size(), "miss admits the publish");
        WsDeduplicationCache.CacheStats stats = dedup.stats();
        assertEquals(0, stats.hits());
        assertEquals(1, stats.misses());
        assertEquals(1, stats.size(), "messageId recorded in cache");
    }

    @Test
    void handlePublish_dedupDisabled_noCacheInteraction() {
        // Default handler in setUp has dedupCache=null. Publishing the same
        // messageId twice must enqueue twice (dedup is off).
        when(exchangeManager.getExchange("/", "events"))
            .thenReturn(new ExchangeMetadata("events", "/", "direct", true, false, false, java.util.Map.of()));
        when(routingEngine.route(eq("events"), eq("k"), any()))
            .thenReturn(Set.of("q"));

        handler.handlePublish(buildPublishFrameWithMessageId(
            "events", "k", "p1", 1L, false, "same-id"), connCtx);
        handler.handlePublish(buildPublishFrameWithMessageId(
            "events", "k", "p2", 2L, false, "same-id"), connCtx);

        assertEquals(2, captured.size(),
            "dedup disabled → both publishes go through");
    }

    @Test
    void handlePublish_dedup_perExchangeNamespace() {
        // Same messageId on different exchanges = NOT a duplicate.
        WsDeduplicationCache dedup = new WsDeduplicationCache(100, 60_000);
        WsPublishHandler dedupHandler = new WsPublishHandler(
            exchangeManager, routingEngine, messageSerializer,
            queue -> "ws." + queue, sink, null, dedup);

        when(exchangeManager.getExchange("/", "events"))
            .thenReturn(new ExchangeMetadata("events", "/", "direct", true, false, false, java.util.Map.of()));
        when(exchangeManager.getExchange("/", "orders"))
            .thenReturn(new ExchangeMetadata("orders", "/", "direct", true, false, false, java.util.Map.of()));
        when(routingEngine.route(eq("events"), any(), any())).thenReturn(Set.of("q-events"));
        when(routingEngine.route(eq("orders"), any(), any())).thenReturn(Set.of("q-orders"));

        dedupHandler.handlePublish(buildPublishFrameWithMessageId(
            "events", "k", "p1", 1L, false, "shared-id"), connCtx);
        dedupHandler.handlePublish(buildPublishFrameWithMessageId(
            "orders", "k", "p2", 2L, false, "shared-id"), connCtx);

        assertEquals(2, captured.size(),
            "same messageId on different exchanges should both enqueue");
    }

    @Test
    void handlePublish_dedup_missingMessageId_skipsCache() {
        // Frames without message.messageId bypass dedup entirely (publisher
        // opt-out). Both publishes should go through.
        WsDeduplicationCache dedup = new WsDeduplicationCache(100, 60_000);
        WsPublishHandler dedupHandler = new WsPublishHandler(
            exchangeManager, routingEngine, messageSerializer,
            queue -> "ws." + queue, sink, null, dedup);

        when(exchangeManager.getExchange("/", "events"))
            .thenReturn(new ExchangeMetadata("events", "/", "direct", true, false, false, java.util.Map.of()));
        when(routingEngine.route(eq("events"), eq("k"), any()))
            .thenReturn(Set.of("q"));

        // Standard frame builder produces no messageId field.
        handler.handlePublish(buildPublishFrame("events", "k", "p1", 1L, false), connCtx);
        dedupHandler.handlePublish(buildPublishFrame("events", "k", "p1", 2L, false), connCtx);
        dedupHandler.handlePublish(buildPublishFrame("events", "k", "p2", 3L, false), connCtx);

        // captured: 1 from default handler + 2 from dedup handler (no dedup).
        assertEquals(3, captured.size());
        assertEquals(0, dedup.stats().size(),
            "messageId-less publishes must not occupy cache slots");
    }

    @Test
    void handlePublish_dedupHit_confirmsDisabled_noFrameEmitted() {
        // When confirms are disabled, a duplicate hit drops silently with no
        // frame emission.
        WsDeduplicationCache dedup = new WsDeduplicationCache(100, 60_000);
        WsPublishHandler dedupHandler = new WsPublishHandler(
            exchangeManager, routingEngine, messageSerializer,
            queue -> "ws." + queue, sink, null, dedup);

        // confirms NOT enabled
        when(exchangeManager.getExchange("/", "events"))
            .thenReturn(new ExchangeMetadata("events", "/", "direct", true, false, false, java.util.Map.of()));
        when(routingEngine.route(eq("events"), eq("k"), any()))
            .thenReturn(Set.of("q"));

        dedupHandler.handlePublish(buildPublishFrameWithMessageId(
            "events", "k", "p", 1L, false, "id-1"), connCtx);
        dedupHandler.handlePublish(buildPublishFrameWithMessageId(
            "events", "k", "p", 2L, false, "id-1"), connCtx);

        assertEquals(1, captured.size(), "duplicate suppressed");
        assertTrue(writtenFrames.isEmpty(),
            "confirms disabled → no frame on dedup hit; got: " + writtenFrames);
    }

    @Test
    void handlePublish_dedup_doesNotRunOnUnknownExchange() {
        // Unknown exchange → unrouted path runs; dedup must NOT be consulted
        // (cache should remain empty so a future create-then-publish is admitted).
        WsDeduplicationCache dedup = new WsDeduplicationCache(100, 60_000);
        WsPublishHandler dedupHandler = new WsPublishHandler(
            exchangeManager, routingEngine, messageSerializer,
            queue -> "ws." + queue, sink, null, dedup);

        when(exchangeManager.getExchange("/", "ghost")).thenReturn(null);

        dedupHandler.handlePublish(buildPublishFrameWithMessageId(
            "ghost", "k", "p", 1L, false, "id-A"), connCtx);

        assertEquals(0, dedup.stats().size(),
            "no exchange → no dedup record");
        assertEquals(0, dedup.stats().misses());
    }

    @Test
    void handlePublish_dedup_doesNotRunOnInternalExchange() {
        // Internal exchange → ACCESS_REFUSED error before dedup runs.
        WsDeduplicationCache dedup = new WsDeduplicationCache(100, 60_000);
        WsPublishHandler dedupHandler = new WsPublishHandler(
            exchangeManager, routingEngine, messageSerializer,
            queue -> "ws." + queue, sink, null, dedup);

        when(exchangeManager.getExchange("/", "amq.internal"))
            .thenReturn(new ExchangeMetadata("amq.internal", "/", "direct", true, false, true, java.util.Map.of()));

        dedupHandler.handlePublish(buildPublishFrameWithMessageId(
            "amq.internal", "k", "p", 1L, false, "id-A"), connCtx);

        assertEquals(0, dedup.stats().size(),
            "internal exchange rejection must not pollute the dedup cache");
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

    /** TASK-WS4.05 — variant carrying {@code message.messageId} for dedup tests. */
    private ObjectNode buildPublishFrameWithMessageId(String exchange, String routingKey,
                                                      String body, long publishId,
                                                      boolean mandatory, String messageId) {
        ObjectNode frame = buildPublishFrame(exchange, routingKey, body, publishId, mandatory);
        ((ObjectNode) frame.get("message")).put("messageId", messageId);
        return frame;
    }

    private static void assertArraysEqualString(String expected, byte[] actual) {
        assertEquals(expected, new String(actual, StandardCharsets.UTF_8));
    }
}
