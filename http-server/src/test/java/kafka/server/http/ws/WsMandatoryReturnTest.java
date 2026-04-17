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
// Time: Created - TASK-WS3.02 - mandatory return semantics (enhanced)
package kafka.server.http.ws;

import com.fasterxml.jackson.databind.JsonNode;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Focused tests for TASK-WS3.02 mandatory message return semantics.
 *
 * <p>These tests complement {@link WsPublishHandlerTest} — they verify:
 * <ul>
 *   <li>returned frame shape (exchange, routingKey, replyCode 312 / NO_ROUTE, full message body)</li>
 *   <li>non-mandatory + no route = silent drop</li>
 *   <li>internal exchange → ACCESS_REFUSED error frame (never routed, never returned)</li>
 *   <li>non-existent exchange: mandatory returns, non-mandatory drops</li>
 *   <li>alternate exchange routing short-circuits the return</li>
 *   <li>confirms + mandatory + no route → BOTH returned AND published emitted (AMQP parity)</li>
 * </ul>
 *
 * // Time: Created - TASK-WS3.02
 */
class WsMandatoryReturnTest {

    private static final ObjectMapper MAPPER = HttpRequestTranslator.MAPPER;

    private ExchangeManager exchangeManager;
    private RoutingEngine routingEngine;
    private WsMessageSerializer messageSerializer;
    private List<String> capturedTopics;
    private WsPublishHandler.ProduceRequestSink sink;
    private WsPublishHandler handler;

    private ChannelHandlerContext mockChannelCtx;
    private WsConnectionContext connCtx;
    private final List<String> writtenFrames = new ArrayList<>();

    @BeforeEach
    void setUp() {
        exchangeManager = mock(ExchangeManager.class);
        routingEngine = mock(RoutingEngine.class);
        messageSerializer = new WsMessageSerializer();

        capturedTopics = new ArrayList<>();
        sink = (topic, serialized, publishId, confirmsEnabled, ctx) ->
            capturedTopics.add(topic);

        handler = new WsPublishHandler(
            exchangeManager,
            routingEngine,
            messageSerializer,
            queue -> "ws." + queue,
            sink);

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
    //  Core returned frame shape
    // ------------------------------------------------------------------

    @Test
    void mandatory_noRoute_returnsMessage() throws Exception {
        givenExchange("events", /*internal*/ false);
        when(routingEngine.route(eq("events"), eq("order.xxx"), any())).thenReturn(Set.of());

        handler.handlePublish(
            buildPublishFrame("events", "order.xxx", "payload", 42L, true),
            connCtx);

        assertTrue(capturedTopics.isEmpty(), "nothing enqueued");
        assertEquals(1, writtenFrames.size(), "one returned frame");
        JsonNode frame = MAPPER.readTree(writtenFrames.get(0));
        assertEquals("returned", frame.get("type").asText());
        assertEquals("events", frame.get("exchange").asText());
        assertEquals("order.xxx", frame.get("routingKey").asText());
        assertEquals(312, frame.get("replyCode").asInt());
        assertEquals("NO_ROUTE", frame.get("replyText").asText());
    }

    @Test
    void mandatory_returnedFrame_includesOriginalBody() throws Exception {
        givenExchange("events", false);
        when(routingEngine.route(eq("events"), eq("nowhere"), any())).thenReturn(Set.of());

        ObjectNode publishFrame = buildPublishFrame("events", "nowhere", "payload-body", 1L, true);
        // enrich message with extra fields to ensure full passthrough
        ObjectNode msg = (ObjectNode) publishFrame.get("message");
        msg.put("contentType", "application/json");
        msg.put("correlationId", "abc-123");
        msg.putObject("headers").put("x-trace", "trace-1");

        handler.handlePublish(publishFrame, connCtx);

        assertEquals(1, writtenFrames.size());
        JsonNode frame = MAPPER.readTree(writtenFrames.get(0));
        JsonNode returnedMsg = frame.get("message");
        assertNotNull(returnedMsg, "returned frame must embed the original message object");
        assertEquals("payload-body", returnedMsg.get("body").asText());
        assertEquals("application/json", returnedMsg.get("contentType").asText());
        assertEquals("abc-123", returnedMsg.get("correlationId").asText());
        assertEquals("trace-1", returnedMsg.get("headers").get("x-trace").asText());
    }

    // ------------------------------------------------------------------
    //  Non-mandatory silent drop
    // ------------------------------------------------------------------

    @Test
    void nonMandatory_noRoute_silentDrop() {
        givenExchange("events", false);
        when(routingEngine.route(eq("events"), eq("lost"), any())).thenReturn(Set.of());

        handler.handlePublish(
            buildPublishFrame("events", "lost", "x", 1L, false),
            connCtx);

        assertTrue(capturedTopics.isEmpty(), "no produce issued");
        assertTrue(writtenFrames.isEmpty(),
            "non-mandatory + no route must be silent — no frames of any kind");
    }

    // ------------------------------------------------------------------
    //  Alternate exchange short-circuits the return
    // ------------------------------------------------------------------

    @Test
    void mandatory_withAlternateExchange_noReturn() {
        // RoutingEngine owns the alternate-exchange fallback. When the
        // engine returns a non-empty set, the handler treats it as a normal
        // routed publish: no returned frame, sink is invoked.
        givenExchange("events", false);
        when(routingEngine.route(eq("events"), eq("k"), any()))
            .thenReturn(Set.of("alt-bound-queue"));

        handler.handlePublish(
            buildPublishFrame("events", "k", "body", 9L, true),
            connCtx);

        assertEquals(1, capturedTopics.size(),
            "alternate-exchange match should flow to the sink like any other");
        assertEquals("ws.alt-bound-queue", capturedTopics.get(0));
        assertTrue(writtenFrames.isEmpty(),
            "no returned frame when alternate routing succeeded");
    }

    // ------------------------------------------------------------------
    //  Non-existent exchange
    // ------------------------------------------------------------------

    @Test
    void mandatory_nonExistentExchange_returns() throws Exception {
        // Design doc §5.7: non-existent exchange + mandatory=true => returned frame.
        when(exchangeManager.getExchange("/", "ghost")).thenReturn(null);

        handler.handlePublish(
            buildPublishFrame("ghost", "rk", "body", 7L, true),
            connCtx);

        assertTrue(capturedTopics.isEmpty());
        assertEquals(1, writtenFrames.size(), "one returned frame");
        JsonNode frame = MAPPER.readTree(writtenFrames.get(0));
        assertEquals("returned", frame.get("type").asText(),
            "non-existent exchange with mandatory=true must emit returned, not error");
        assertEquals("ghost", frame.get("exchange").asText());
        assertEquals("rk", frame.get("routingKey").asText());
        assertEquals(312, frame.get("replyCode").asInt());
        assertEquals("NO_ROUTE", frame.get("replyText").asText());
        // Routing engine must NOT be consulted for a missing exchange.
        verify(routingEngine, never()).route(any(), any(), any());
    }

    @Test
    void nonMandatory_nonExistentExchange_silentDrop() {
        // Design doc §5.7: non-existent exchange + mandatory=false => silent drop.
        // This is a BEHAVIOR CHANGE vs. WS1.11 which surfaced NOT_FOUND.
        when(exchangeManager.getExchange("/", "ghost")).thenReturn(null);

        handler.handlePublish(
            buildPublishFrame("ghost", "rk", "body", 7L, false),
            connCtx);

        assertTrue(capturedTopics.isEmpty());
        assertTrue(writtenFrames.isEmpty(),
            "non-existent exchange + mandatory=false must be silent");
        verify(routingEngine, never()).route(any(), any(), any());
    }

    // ------------------------------------------------------------------
    //  Internal exchange — ACCESS_REFUSED error frame
    // ------------------------------------------------------------------

    @Test
    void internalExchange_rejected() throws Exception {
        // Internal exchange (internal=true) must never accept a publish.
        givenExchange("amq.rabbitmq.trace", /*internal*/ true);

        handler.handlePublish(
            buildPublishFrame("amq.rabbitmq.trace", "k", "body", 3L, false),
            connCtx);

        assertTrue(capturedTopics.isEmpty(), "internal exchange must not route");
        assertEquals(1, writtenFrames.size(), "one error frame expected");
        JsonNode frame = MAPPER.readTree(writtenFrames.get(0));
        assertEquals("error", frame.get("type").asText());
        assertEquals("ACCESS_REFUSED", frame.get("errorCode").asText());
        assertTrue(frame.get("errorMessage").asText().contains("amq.rabbitmq.trace"),
            "error message should reference the exchange name; was: "
                + frame.get("errorMessage").asText());
        assertEquals(3L, frame.get("publishId").asLong(), "publishId echoed");
        verify(routingEngine, never()).route(any(), any(), any());
    }

    @Test
    void internalExchange_mandatoryIgnored_noReturnedFrame() throws Exception {
        // Even with mandatory=true, internal exchange must be rejected with
        // ACCESS_REFUSED — not a returned frame.
        givenExchange("amq.internal", true);

        handler.handlePublish(
            buildPublishFrame("amq.internal", "k", "body", 4L, true),
            connCtx);

        assertEquals(1, writtenFrames.size());
        JsonNode frame = MAPPER.readTree(writtenFrames.get(0));
        assertEquals("error", frame.get("type").asText(),
            "internal exchange must NOT produce a returned frame, even with mandatory=true");
        assertEquals("ACCESS_REFUSED", frame.get("errorCode").asText());
    }

    // ------------------------------------------------------------------
    //  Confirms + mandatory + no route
    // ------------------------------------------------------------------

    @Test
    void mandatory_withConfirms_sendsBothReturnAndPublished() throws Exception {
        // AMQP semantics: mandatory return DOES NOT prevent basic.ack.
        // When confirms are enabled, a mandatory + unroutable publish must
        // emit BOTH the returned frame AND a published confirm.
        connCtx.enablePublishConfirms();
        givenExchange("events", false);
        when(routingEngine.route(eq("events"), eq("lost"), any())).thenReturn(Set.of());

        handler.handlePublish(
            buildPublishFrame("events", "lost", "body", 777L, true),
            connCtx);

        assertTrue(capturedTopics.isEmpty());
        assertEquals(2, writtenFrames.size(),
            "expected BOTH returned + published frames; got " + writtenFrames);

        // Collect frames by type (order not strictly specified).
        String returnedFrame = null, publishedFrame = null;
        for (String f : writtenFrames) {
            JsonNode n = MAPPER.readTree(f);
            if ("returned".equals(n.get("type").asText())) returnedFrame = f;
            else if ("published".equals(n.get("type").asText())) publishedFrame = f;
        }
        assertNotNull(returnedFrame, "returned frame missing");
        assertNotNull(publishedFrame, "published frame missing");

        JsonNode ret = MAPPER.readTree(returnedFrame);
        assertEquals(312, ret.get("replyCode").asInt());
        assertEquals(777L, ret.get("publishId").asLong());

        JsonNode pub = MAPPER.readTree(publishedFrame);
        assertEquals(777L, pub.get("publishId").asLong());

        // Tracker should have no pending entries left: the success was consumed.
        assertEquals(0, connCtx.confirmTracker().pendingCount(),
            "published confirm should have drained the pending slot");
    }

    @Test
    void nonMandatory_withConfirms_noRoute_sendsOnlyPublished() throws Exception {
        // Same as above but mandatory=false. We still record + emit published
        // (message accepted by the broker; just not routed). NO returned frame.
        connCtx.enablePublishConfirms();
        givenExchange("events", false);
        when(routingEngine.route(eq("events"), eq("lost"), any())).thenReturn(Set.of());

        handler.handlePublish(
            buildPublishFrame("events", "lost", "body", 42L, false),
            connCtx);

        assertTrue(capturedTopics.isEmpty());
        assertEquals(1, writtenFrames.size());
        JsonNode frame = MAPPER.readTree(writtenFrames.get(0));
        assertEquals("published", frame.get("type").asText(),
            "non-mandatory + no route + confirms should emit only published");
        assertEquals(42L, frame.get("publishId").asLong());
        assertEquals(0, connCtx.confirmTracker().pendingCount());
    }

    @Test
    void nonMandatory_withConfirms_nonExistentExchange_sendsOnlyPublished() throws Exception {
        // Silent drop when exchange doesn't exist — but confirms + non-mandatory
        // still acknowledges the message was accepted. No error, no returned.
        connCtx.enablePublishConfirms();
        when(exchangeManager.getExchange("/", "ghost")).thenReturn(null);

        handler.handlePublish(
            buildPublishFrame("ghost", "k", "body", 5L, false),
            connCtx);

        assertEquals(1, writtenFrames.size());
        JsonNode frame = MAPPER.readTree(writtenFrames.get(0));
        assertEquals("published", frame.get("type").asText());
        assertEquals(5L, frame.get("publishId").asLong());
    }

    @Test
    void mandatory_withConfirms_nonExistentExchange_sendsBothReturnAndPublished() throws Exception {
        connCtx.enablePublishConfirms();
        when(exchangeManager.getExchange("/", "ghost")).thenReturn(null);

        handler.handlePublish(
            buildPublishFrame("ghost", "rk", "body", 6L, true),
            connCtx);

        assertEquals(2, writtenFrames.size(),
            "non-existent + mandatory + confirms => both returned and published");
        int returnedCount = 0, publishedCount = 0;
        for (String f : writtenFrames) {
            String type = MAPPER.readTree(f).get("type").asText();
            if ("returned".equals(type)) returnedCount++;
            else if ("published".equals(type)) publishedCount++;
        }
        assertEquals(1, returnedCount);
        assertEquals(1, publishedCount);
    }

    @Test
    void internalExchange_withConfirms_noPublishedFrame() throws Exception {
        // ACCESS_REFUSED is a publish rejection — the message was NOT accepted,
        // so no published frame is emitted even when confirms are enabled.
        connCtx.enablePublishConfirms();
        givenExchange("amq.internal", true);

        handler.handlePublish(
            buildPublishFrame("amq.internal", "k", "body", 10L, false),
            connCtx);

        assertEquals(1, writtenFrames.size(), "only the error frame");
        JsonNode frame = MAPPER.readTree(writtenFrames.get(0));
        assertEquals("error", frame.get("type").asText());
        assertEquals("ACCESS_REFUSED", frame.get("errorCode").asText());
        assertFalse(writtenFrames.get(0).contains("\"published\""),
            "rejection must not emit a published confirm");
    }

    // ------------------------------------------------------------------
    //  Helpers
    // ------------------------------------------------------------------

    private void givenExchange(String name, boolean internal) {
        when(exchangeManager.getExchange("/", name))
            .thenReturn(new ExchangeMetadata(
                name, "/", "direct", true, false, internal, java.util.Map.of()));
    }

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
}
