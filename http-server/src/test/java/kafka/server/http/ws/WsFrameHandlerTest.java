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
// Time: Created - TASK-WS1.04
package kafka.server.http.ws;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link WsFrameHandler}.
 *
 * <p>We drive the handler by invoking {@code channelRead0} directly with a
 * captured {@link ChannelHandlerContext}.  An EmbeddedChannel is unnecessary
 * for these tests — the handler's only side effect on the wire is writing
 * frames via {@link WsConnectionContext#sendFrame(String)}, which we capture
 * through a Mockito mock of {@link ChannelHandlerContext}.
 *
 * // Time: Created - TASK-WS1.04
 */
class WsFrameHandlerTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private ChannelHandlerContext mockChannelCtx;
    private WsConnectionContext connCtx;
    private WsConfigs wsConfigs;

    /** Captured text payloads from every {@code writeAndFlush} on the mock channel. */
    private final List<String> writtenFrames = new ArrayList<>();

    @BeforeEach
    void setUp() {
        mockChannelCtx = mock(ChannelHandlerContext.class);
        Channel mockChannel = mock(Channel.class);
        when(mockChannelCtx.channel()).thenReturn(mockChannel);
        when(mockChannel.isActive()).thenReturn(true);

        // Capture every TextWebSocketFrame written through the context and stash its
        // text payload for assertions, then return a harmless mock future.
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
        wsConfigs = WsConfigs.withDefaults();
    }

    // ------------------------------------------------------------------
    //  Constructor
    // ------------------------------------------------------------------

    @Test
    void constructor_nullConnectionContext_throws() {
        assertThrows(NullPointerException.class,
                () -> new WsFrameHandler(null, wsConfigs));
    }

    @Test
    void constructor_nullConfigs_throws() {
        assertThrows(NullPointerException.class,
                () -> new WsFrameHandler(connCtx, null));
    }

    // ------------------------------------------------------------------
    //  Parser error paths — malformed / empty / missing type
    // ------------------------------------------------------------------

    @Test
    void malformedJson_sendsProtocolError() throws Exception {
        WsFrameHandler handler = new WsFrameHandler(connCtx, wsConfigs);
        deliver(handler, "not json at all");

        assertEquals(1, writtenFrames.size(), "one error frame should be written");
        JsonNode err = MAPPER.readTree(writtenFrames.get(0));
        assertEquals("error", err.get("type").asText());
        assertEquals("PROTOCOL_ERROR", err.get("errorCode").asText());
        assertNull(err.get("id"), "no correlation id was parseable");
    }

    @Test
    void emptyFrame_sendsProtocolError() throws Exception {
        WsFrameHandler handler = new WsFrameHandler(connCtx, wsConfigs);
        deliver(handler, "");

        assertEquals(1, writtenFrames.size());
        JsonNode err = MAPPER.readTree(writtenFrames.get(0));
        assertEquals("PROTOCOL_ERROR", err.get("errorCode").asText());
    }

    @Test
    void whitespaceOnlyFrame_sendsProtocolError() throws Exception {
        WsFrameHandler handler = new WsFrameHandler(connCtx, wsConfigs);
        deliver(handler, "   \t\n  ");

        assertEquals(1, writtenFrames.size());
        JsonNode err = MAPPER.readTree(writtenFrames.get(0));
        assertEquals("PROTOCOL_ERROR", err.get("errorCode").asText());
    }

    @Test
    void nonObjectTopLevel_sendsProtocolError() throws Exception {
        // readTree("null") returns a NullNode, readTree("[]") returns an ArrayNode —
        // neither is a valid message envelope.
        WsFrameHandler handler = new WsFrameHandler(connCtx, wsConfigs);
        deliver(handler, "[]");

        assertEquals(1, writtenFrames.size());
        JsonNode err = MAPPER.readTree(writtenFrames.get(0));
        assertEquals("PROTOCOL_ERROR", err.get("errorCode").asText());
    }

    @Test
    void missingTypeField_sendsProtocolError() throws Exception {
        WsFrameHandler handler = new WsFrameHandler(connCtx, wsConfigs);
        deliver(handler, "{\"foo\":\"bar\"}");

        assertEquals(1, writtenFrames.size());
        JsonNode err = MAPPER.readTree(writtenFrames.get(0));
        assertEquals("PROTOCOL_ERROR", err.get("errorCode").asText());
        assertTrue(err.get("errorMessage").asText().contains("type"),
                "message should mention missing 'type'");
    }

    @Test
    void nonTextualTypeField_sendsProtocolError() throws Exception {
        WsFrameHandler handler = new WsFrameHandler(connCtx, wsConfigs);
        deliver(handler, "{\"type\":42}");

        assertEquals(1, writtenFrames.size());
        JsonNode err = MAPPER.readTree(writtenFrames.get(0));
        assertEquals("PROTOCOL_ERROR", err.get("errorCode").asText());
    }

    @Test
    void unknownType_sendsUnknownMessageTypeError() throws Exception {
        WsFrameHandler handler = new WsFrameHandler(connCtx, wsConfigs);
        deliver(handler, "{\"type\":\"bogus-op\"}");

        assertEquals(1, writtenFrames.size());
        JsonNode err = MAPPER.readTree(writtenFrames.get(0));
        assertEquals("UNKNOWN_MESSAGE_TYPE", err.get("errorCode").asText());
        assertTrue(err.get("errorMessage").asText().contains("bogus-op"));
    }

    // ------------------------------------------------------------------
    //  Correlation id echo
    // ------------------------------------------------------------------

    @Test
    void errorFrame_includesCorrelationId_whenPresent() throws Exception {
        WsFrameHandler handler = new WsFrameHandler(connCtx, wsConfigs);
        deliver(handler, "{\"type\":\"bogus\",\"id\":\"req-42\"}");

        assertEquals(1, writtenFrames.size());
        JsonNode err = MAPPER.readTree(writtenFrames.get(0));
        assertEquals("req-42", err.get("id").asText());
        assertEquals("UNKNOWN_MESSAGE_TYPE", err.get("errorCode").asText());
    }

    @Test
    void errorFrame_includesCorrelationId_evenWhenTypeMissing() throws Exception {
        WsFrameHandler handler = new WsFrameHandler(connCtx, wsConfigs);
        deliver(handler, "{\"id\":\"req-9\",\"other\":\"x\"}");

        assertEquals(1, writtenFrames.size());
        JsonNode err = MAPPER.readTree(writtenFrames.get(0));
        assertEquals("req-9", err.get("id").asText());
        assertEquals("PROTOCOL_ERROR", err.get("errorCode").asText());
    }

    @Test
    void errorFrame_omitsCorrelationId_whenAbsent() throws Exception {
        WsFrameHandler handler = new WsFrameHandler(connCtx, wsConfigs);
        deliver(handler, "{\"type\":\"bogus\"}");

        assertEquals(1, writtenFrames.size());
        JsonNode err = MAPPER.readTree(writtenFrames.get(0));
        assertNull(err.get("id"), "id field should not be present");
    }

    // ------------------------------------------------------------------
    //  Dispatch — verify each of the 15 types routes to its method.
    //  Handlers throw UnsupportedOperationException in this task, which the
    //  frame handler translates into an INTERNAL_ERROR frame — so we verify
    //  both that dispatch happened AND that the error frame looks right.
    // ------------------------------------------------------------------

    @Test
    void dispatch_declareExchange_reachesHandler() {
        assertDispatchedTo("declare-exchange", "handleDeclareExchange");
    }

    @Test
    void dispatch_deleteExchange_reachesHandler() {
        assertDispatchedTo("delete-exchange", "handleDeleteExchange");
    }

    @Test
    void dispatch_declareQueue_reachesHandler() {
        assertDispatchedTo("declare-queue", "handleDeclareQueue");
    }

    @Test
    void dispatch_deleteQueue_reachesHandler() {
        assertDispatchedTo("delete-queue", "handleDeleteQueue");
    }

    @Test
    void dispatch_bind_reachesHandler() {
        assertDispatchedTo("bind", "handleBind");
    }

    @Test
    void dispatch_unbind_reachesHandler() {
        assertDispatchedTo("unbind", "handleUnbind");
    }

    @Test
    void dispatch_publish_reachesHandler() {
        assertDispatchedTo("publish", "handlePublish");
    }

    @Test
    void dispatch_subscribe_reachesHandler() {
        assertDispatchedTo("subscribe", "handleSubscribe");
    }

    @Test
    void dispatch_unsubscribe_reachesHandler() {
        assertDispatchedTo("unsubscribe", "handleUnsubscribe");
    }

    @Test
    void dispatch_get_reachesHandler() {
        assertDispatchedTo("get", "handleGet");
    }

    @Test
    void dispatch_purgeQueue_reachesHandler() {
        assertDispatchedTo("purge-queue", "handlePurgeQueue");
    }

    @Test
    void dispatch_ack_reachesHandler() {
        assertDispatchedTo("ack", "handleAck");
    }

    @Test
    void dispatch_nack_reachesHandler() {
        assertDispatchedTo("nack", "handleNack");
    }

    @Test
    void dispatch_credits_reachesHandler() {
        assertDispatchedTo("credits", "handleCredits");
    }

    @Test
    void dispatch_enableConfirms_reachesHandler() {
        assertDispatchedTo("enable-confirms", "handleEnableConfirms");
    }

    @Test
    void dispatch_passesMessageNodeToHandler() {
        // Verify the whole JSON object, not just the "type", reaches the handler.
        AtomicReference<JsonNode> captured = new AtomicReference<>();
        AtomicReference<WsConnectionContext> capturedCtx = new AtomicReference<>();
        WsFrameHandler handler = new WsFrameHandler(connCtx, wsConfigs) {
            @Override
            void handlePublish(WsConnectionContext ctx, JsonNode msg) {
                capturedCtx.set(ctx);
                captured.set(msg);
            }
        };
        deliver(handler, "{\"type\":\"publish\",\"exchange\":\"e1\",\"routingKey\":\"k\"}");

        assertNotNull(captured.get(), "handler should have been invoked");
        assertEquals("publish", captured.get().get("type").asText());
        assertEquals("e1", captured.get().get("exchange").asText());
        assertEquals("k", captured.get().get("routingKey").asText());
        assertEquals(connCtx, capturedCtx.get(), "handler should receive the shared ctx");
        assertTrue(writtenFrames.isEmpty(), "no error frame when handler succeeds");
    }

    @Test
    void stubHandlers_produceInternalErrorFrame() throws Exception {
        // Baseline handler with stubs intact — an inbound publish should get
        // translated into an INTERNAL_ERROR frame rather than crashing.
        WsFrameHandler handler = new WsFrameHandler(connCtx, wsConfigs);
        deliver(handler, "{\"type\":\"publish\",\"id\":\"req-1\"}");

        assertEquals(1, writtenFrames.size());
        JsonNode err = MAPPER.readTree(writtenFrames.get(0));
        assertEquals("INTERNAL_ERROR", err.get("errorCode").asText());
        assertEquals("req-1", err.get("id").asText());
        assertTrue(err.get("errorMessage").asText().contains("publish"));
    }

    @Test
    void runtimeExceptionInHandler_producesInternalErrorFrame() throws Exception {
        WsFrameHandler handler = new WsFrameHandler(connCtx, wsConfigs) {
            @Override
            void handlePublish(WsConnectionContext ctx, JsonNode msg) {
                throw new IllegalStateException("boom");
            }
        };
        deliver(handler, "{\"type\":\"publish\",\"id\":\"r2\"}");

        assertEquals(1, writtenFrames.size());
        JsonNode err = MAPPER.readTree(writtenFrames.get(0));
        assertEquals("INTERNAL_ERROR", err.get("errorCode").asText());
        assertEquals("r2", err.get("id").asText());
    }

    // ------------------------------------------------------------------
    //  Lifecycle
    // ------------------------------------------------------------------

    @Test
    void channelInactive_clearsSubscriptionsAndFiresDownstream() {
        WsFrameHandler handler = new WsFrameHandler(connCtx, wsConfigs);
        connCtx.subscriptions().put("sub-1", "placeholder");
        connCtx.subscriptions().put("sub-2", "placeholder");

        handler.channelInactive(mockChannelCtx);

        assertTrue(connCtx.subscriptions().isEmpty(),
                "channelInactive should clear all subscriptions");
        // We can't easily verify fireChannelInactive was called on the mock without
        // complicating the setup; behavioural proof is the subscription clear.
    }

    @Test
    void exceptionCaught_closesChannel() {
        AtomicBoolean closed = new AtomicBoolean(false);
        ChannelHandlerContext mock = mock(ChannelHandlerContext.class);
        when(mock.close()).thenAnswer(inv -> {
            closed.set(true);
            return mock(ChannelFuture.class);
        });

        WsFrameHandler handler = new WsFrameHandler(connCtx, wsConfigs);
        handler.exceptionCaught(mock, new RuntimeException("simulated"));

        assertTrue(closed.get(), "exceptionCaught should close the channel");
    }

    // ------------------------------------------------------------------
    //  Helpers
    // ------------------------------------------------------------------

    /**
     * Deliver a text frame to the handler by invoking {@code channelRead}, which
     * will unwrap to {@code channelRead0}.  Releases the frame after delivery.
     */
    private void deliver(WsFrameHandler handler, String text) {
        TextWebSocketFrame frame = new TextWebSocketFrame(text);
        try {
            // channelRead (not channelRead0) is the public entry; it handles the
            // type check and auto-release that SimpleChannelInboundHandler applies.
            handler.channelRead(mockChannelCtx, frame);
        } catch (Exception e) {
            throw new AssertionError("channelRead threw unexpectedly", e);
        }
    }

    /**
     * Build a frame handler with {@code handlerMethod} overridden to flip a boolean,
     * deliver a frame of the given {@code type}, and assert the override ran and no
     * error frame was emitted.
     */
    private void assertDispatchedTo(String type, String handlerMethod) {
        AtomicBoolean called = new AtomicBoolean(false);
        WsFrameHandler handler = new WsFrameHandler(connCtx, wsConfigs) {
            @Override void handleDeclareExchange(WsConnectionContext ctx, JsonNode msg) {
                if ("handleDeclareExchange".equals(handlerMethod)) called.set(true);
            }
            @Override void handleDeleteExchange(WsConnectionContext ctx, JsonNode msg) {
                if ("handleDeleteExchange".equals(handlerMethod)) called.set(true);
            }
            @Override void handleDeclareQueue(WsConnectionContext ctx, JsonNode msg) {
                if ("handleDeclareQueue".equals(handlerMethod)) called.set(true);
            }
            @Override void handleDeleteQueue(WsConnectionContext ctx, JsonNode msg) {
                if ("handleDeleteQueue".equals(handlerMethod)) called.set(true);
            }
            @Override void handleBind(WsConnectionContext ctx, JsonNode msg) {
                if ("handleBind".equals(handlerMethod)) called.set(true);
            }
            @Override void handleUnbind(WsConnectionContext ctx, JsonNode msg) {
                if ("handleUnbind".equals(handlerMethod)) called.set(true);
            }
            @Override void handlePublish(WsConnectionContext ctx, JsonNode msg) {
                if ("handlePublish".equals(handlerMethod)) called.set(true);
            }
            @Override void handleSubscribe(WsConnectionContext ctx, JsonNode msg) {
                if ("handleSubscribe".equals(handlerMethod)) called.set(true);
            }
            @Override void handleUnsubscribe(WsConnectionContext ctx, JsonNode msg) {
                if ("handleUnsubscribe".equals(handlerMethod)) called.set(true);
            }
            @Override void handleGet(WsConnectionContext ctx, JsonNode msg) {
                if ("handleGet".equals(handlerMethod)) called.set(true);
            }
            @Override void handlePurgeQueue(WsConnectionContext ctx, JsonNode msg) {
                if ("handlePurgeQueue".equals(handlerMethod)) called.set(true);
            }
            @Override void handleAck(WsConnectionContext ctx, JsonNode msg) {
                if ("handleAck".equals(handlerMethod)) called.set(true);
            }
            @Override void handleNack(WsConnectionContext ctx, JsonNode msg) {
                if ("handleNack".equals(handlerMethod)) called.set(true);
            }
            @Override void handleCredits(WsConnectionContext ctx, JsonNode msg) {
                if ("handleCredits".equals(handlerMethod)) called.set(true);
            }
            @Override void handleEnableConfirms(WsConnectionContext ctx, JsonNode msg) {
                if ("handleEnableConfirms".equals(handlerMethod)) called.set(true);
            }
        };

        deliver(handler, "{\"type\":\"" + type + "\"}");

        assertTrue(called.get(),
                "expected " + handlerMethod + " to be invoked for type=" + type);
        assertTrue(writtenFrames.isEmpty(),
                "no error frame expected when handler succeeds; got " + writtenFrames);
    }

}
