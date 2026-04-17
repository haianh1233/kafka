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
// Time: Created - TASK-WS1.12
package kafka.server.http;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelId;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import kafka.network.RequestChannel;
import kafka.server.http.ws.WsConnectionContext;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.requests.RequestHeader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link HttpProcessor} WebSocket extension (TASK-WS1.12).
 *
 * <p>Kept separate from {@link HttpProcessorTest} so the original HTTP tests
 * stay untouched — any regression in the HTTP path must reproduce there.</p>
 *
 * // Time: Created - TASK-WS1.12
 */
class HttpProcessorWsTest {

    private HttpProcessor processor;

    @BeforeEach
    void setUp() {
        processor = new HttpProcessor(200);
    }

    @AfterEach
    void tearDown() {
        processor.close();
    }

    // --- Registration API ---

    @Test
    void registerWsConnection_addsToMap() {
        WsConnectionContext wsCtx = mockWsContext();
        processor.registerWsConnection("ws-1", wsCtx);
        assertTrue(processor.isWsConnection("ws-1"));
        assertEquals(1, processor.wsConnectionCount());
    }

    @Test
    void unregisterWsConnection_removesFromMap() {
        WsConnectionContext wsCtx = mockWsContext();
        processor.registerWsConnection("ws-1", wsCtx);
        processor.unregisterWsConnection("ws-1");
        assertFalse(processor.isWsConnection("ws-1"));
        assertEquals(0, processor.wsConnectionCount());
    }

    @Test
    void unregisterWsConnection_unknownId_isNoOp() {
        processor.unregisterWsConnection("never-registered");
        assertEquals(0, processor.wsConnectionCount());
    }

    @Test
    void wsConnectionCount_reflectsRegistrations() {
        assertEquals(0, processor.wsConnectionCount());
        processor.registerWsConnection("ws-a", mockWsContext());
        processor.registerWsConnection("ws-b", mockWsContext());
        assertEquals(2, processor.wsConnectionCount());
    }

    @Test
    void isWsConnection_falseForHttpConnection() {
        processor.registerChannel("http-1", mockActiveCtx("http-1"));
        assertFalse(processor.isWsConnection("http-1"));
    }

    @Test
    void registerWsConnection_nullId_throwsNPE() {
        assertThrows(NullPointerException.class,
            () -> processor.registerWsConnection(null, mockWsContext()));
    }

    @Test
    void registerWsConnection_nullCtx_throwsNPE() {
        assertThrows(NullPointerException.class,
            () -> processor.registerWsConnection("conn-1", null));
    }

    // --- processResponses WS routing ---

    @Test
    void processResponses_wsProduceWithConfirmsEnabled_writesConfirmFrame() {
        EmbeddedWsHarness harness = newHarness("ws-produce", /*confirmsEnabled*/ true);
        processor.registerWsConnection("ws-produce", harness.ctx);

        // Build a successful ProduceResponse
        ProduceResponse produceResp = successProduceResponse();
        RequestChannel.SendResponse sendResp = mockSendResponseWithProduce(
            "ws-produce", produceResp, /*publishId*/ 42L);

        processor.enqueueResponse(sendResp);
        processor.processResponses();

        ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
        verify(harness.channel).writeAndFlush(captor.capture());
        Object written = captor.getValue();
        assertTrue(written instanceof TextWebSocketFrame,
            "Expected TextWebSocketFrame but got " + written.getClass());
        String json = ((TextWebSocketFrame) written).text();
        assertTrue(json.contains("\"type\":\"published\""), "json=" + json);
        assertTrue(json.contains("\"publishId\":42"), "json=" + json);

        // No HTTP response should have been written for a WS connection.
        verify(harness.channel, never()).writeAndFlush(any(FullHttpResponse.class));
    }

    @Test
    void processResponses_wsProduceWithConfirmsDisabled_writesNoFrame() {
        EmbeddedWsHarness harness = newHarness("ws-silent", /*confirmsEnabled*/ false);
        processor.registerWsConnection("ws-silent", harness.ctx);

        ProduceResponse produceResp = successProduceResponse();
        RequestChannel.SendResponse sendResp = mockSendResponseWithProduce(
            "ws-silent", produceResp, 1L);

        processor.enqueueResponse(sendResp);
        processor.processResponses();

        verify(harness.channel, never()).writeAndFlush(any());
    }

    @Test
    void processResponses_wsProduceFailure_writesPublishFailedFrame() {
        EmbeddedWsHarness harness = newHarness("ws-fail", /*confirmsEnabled*/ true);
        processor.registerWsConnection("ws-fail", harness.ctx);

        ProduceResponse produceResp = failedProduceResponse(
            Errors.NOT_LEADER_OR_FOLLOWER, "no leader");
        RequestChannel.SendResponse sendResp = mockSendResponseWithProduce(
            "ws-fail", produceResp, 7L);

        processor.enqueueResponse(sendResp);
        processor.processResponses();

        ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
        verify(harness.channel).writeAndFlush(captor.capture());
        String json = ((TextWebSocketFrame) captor.getValue()).text();
        assertTrue(json.contains("\"type\":\"publish-failed\""), "json=" + json);
        assertTrue(json.contains("\"publishId\":7"), "json=" + json);
        assertTrue(json.contains("\"errorCode\":" + Errors.NOT_LEADER_OR_FOLLOWER.code()),
            "json=" + json);
    }

    @Test
    void processResponses_httpSendResponse_routesToHttpHandler_whenWsMapDoesNotContainId() {
        // Regression guard: a connection NOT registered as WS must flow through
        // the original HTTP path — the existing HttpProcessorTest already covers
        // this, but we re-check here to detect accidental mis-routing.
        ChannelHandlerContext ctx = mockActiveCtx("http-1");
        processor.registerChannel("http-1", ctx);

        RequestChannel.SendResponse sendResp = mockSendResponse("http-1");
        processor.enqueueResponse(sendResp);
        processor.processResponses();

        verify(ctx).writeAndFlush(any(FullHttpResponse.class));
        assertEquals(0, processor.channelCount(),
            "HTTP channel should be removed after one-shot response (HTTP 1.1 behaviour)");
    }

    @Test
    void processResponses_closeResponse_removesWsConnection_andClosesChannel() {
        EmbeddedWsHarness harness = newHarness("ws-close", /*confirmsEnabled*/ false);
        processor.registerWsConnection("ws-close", harness.ctx);

        RequestChannel.CloseConnectionResponse closeResp = mockCloseConnectionResponse("ws-close");
        processor.enqueueResponse(closeResp);
        processor.processResponses();

        assertEquals(0, processor.wsConnectionCount(),
            "WS connection entry must be removed after CloseConnectionResponse");
        // A CloseWebSocketFrame is written to the Netty channel.
        ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
        verify(harness.channel).writeAndFlush(captor.capture());
        assertTrue(captor.getValue() instanceof CloseWebSocketFrame,
            "Expected CloseWebSocketFrame but got " + captor.getValue().getClass());
    }

    @Test
    void processResponses_throttlingResponse_writesThrottleFrame() {
        EmbeddedWsHarness harness = newHarness("ws-throttle", /*confirmsEnabled*/ false);
        processor.registerWsConnection("ws-throttle", harness.ctx);

        RequestChannel.StartThrottlingResponse response =
            mockStartThrottlingResponse("ws-throttle", 3000);
        processor.enqueueResponse(response);
        processor.processResponses();

        ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
        verify(harness.channel).writeAndFlush(captor.capture());
        assertTrue(captor.getValue() instanceof TextWebSocketFrame);
        String json = ((TextWebSocketFrame) captor.getValue()).text();
        assertTrue(json.contains("\"type\":\"throttled\""), "json=" + json);
        assertTrue(json.contains("\"throttleTimeMs\":3000"), "json=" + json);
    }

    @Test
    void processResponses_noOpResponse_forWsConnection_writesNothing() {
        EmbeddedWsHarness harness = newHarness("ws-noop", /*confirmsEnabled*/ true);
        processor.registerWsConnection("ws-noop", harness.ctx);

        RequestChannel.NoOpResponse noop = mockNoOpResponse("ws-noop");
        processor.enqueueResponse(noop);
        processor.processResponses();

        verify(harness.channel, never()).writeAndFlush(any());
    }

    @Test
    void processResponses_unknownSessionId_fallsBackToHttpPath() {
        // When a response arrives for a connectionId that is NOT in the WS map,
        // routing must fall through to the HTTP path (no WS frame written).
        ChannelHandlerContext httpCtx = mockActiveCtx("stale");
        processor.registerChannel("stale", httpCtx);
        // Note: we do NOT register "stale" as a WS connection.

        RequestChannel.SendResponse resp = mockSendResponse("stale");
        processor.enqueueResponse(resp);
        processor.processResponses();

        verify(httpCtx).writeAndFlush(any(FullHttpResponse.class));
    }

    // --- Mixed routing ---

    @Test
    void processResponses_mixedConnections_routesEachCorrectly() {
        ChannelHandlerContext httpCtx = mockActiveCtx("http-x");
        processor.registerChannel("http-x", httpCtx);

        EmbeddedWsHarness wsHarness = newHarness("ws-x", /*confirmsEnabled*/ true);
        processor.registerWsConnection("ws-x", wsHarness.ctx);

        processor.enqueueResponse(mockSendResponse("http-x"));
        processor.enqueueResponse(
            mockSendResponseWithProduce("ws-x", successProduceResponse(), 1L));

        processor.processResponses();

        verify(httpCtx).writeAndFlush(any(FullHttpResponse.class));
        verify(wsHarness.channel).writeAndFlush(any(TextWebSocketFrame.class));
    }

    // --- Lifecycle ---

    @Test
    void close_clearsWsConnections() {
        processor.registerWsConnection("ws-a", mockWsContext());
        processor.registerWsConnection("ws-b", mockWsContext());
        assertEquals(2, processor.wsConnectionCount());
        processor.close();
        assertEquals(0, processor.wsConnectionCount());
    }

    @Test
    void concurrentRegisterUnregister_doesNotCorruptMap() throws InterruptedException {
        final int threads = 8;
        final int perThread = 500;
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        Thread[] workers = new Thread[threads];
        for (int t = 0; t < threads; t++) {
            final int tid = t;
            workers[t] = new Thread(() -> {
                try {
                    start.await();
                    for (int i = 0; i < perThread; i++) {
                        String id = "ws-" + tid + "-" + i;
                        processor.registerWsConnection(id, mockWsContext());
                        processor.unregisterWsConnection(id);
                    }
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
            workers[t].start();
        }
        start.countDown();
        assertTrue(done.await(30, TimeUnit.SECONDS),
            "Concurrent register/unregister did not complete in time");
        assertEquals(0, processor.wsConnectionCount());
    }

    // --- Helpers ---

    private WsConnectionContext mockWsContext() {
        WsConnectionContext wsCtx = mock(WsConnectionContext.class);
        when(wsCtx.isActive()).thenReturn(true);
        return wsCtx;
    }

    /**
     * An EmbeddedWsHarness couples a real {@link WsConnectionContext} with a
     * mocked Netty channel so that {@link WsConnectionContext#sendFrame(String)}
     * and {@link WsConnectionContext#close(int, String)} invocations are
     * observable via {@code verify(channel).writeAndFlush(...)}.
     */
    private static final class EmbeddedWsHarness {
        final WsConnectionContext ctx;
        final ChannelHandlerContext channel;

        EmbeddedWsHarness(WsConnectionContext ctx, ChannelHandlerContext channel) {
            this.ctx = ctx;
            this.channel = channel;
        }
    }

    private EmbeddedWsHarness newHarness(String sessionId, boolean confirmsEnabled) {
        ChannelHandlerContext channel = mock(ChannelHandlerContext.class);
        Channel ch = mock(Channel.class);
        ChannelFuture writeFuture = mock(ChannelFuture.class);

        when(channel.channel()).thenReturn(ch);
        when(ch.isActive()).thenReturn(true);
        when(channel.writeAndFlush(any())).thenReturn(writeFuture);
        when(writeFuture.addListener(any())).thenReturn(writeFuture);

        WsConnectionContext wsCtx = new WsConnectionContext(
            sessionId,
            new org.apache.kafka.common.security.auth.KafkaPrincipal(
                org.apache.kafka.common.security.auth.KafkaPrincipal.USER_TYPE, "test-user"),
            "/",
            channel,
            new java.net.InetSocketAddress("127.0.0.1", 1234));
        if (confirmsEnabled) {
            wsCtx.enablePublishConfirms();
        }
        return new EmbeddedWsHarness(wsCtx, channel);
    }

    private ChannelHandlerContext mockActiveCtx(String connectionId) {
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        Channel channel = mock(Channel.class);
        ChannelId channelId = mock(ChannelId.class);
        ChannelFuture closeFuture = mock(ChannelFuture.class);
        ChannelFuture writeFuture = mock(ChannelFuture.class);

        when(ctx.channel()).thenReturn(channel);
        when(channel.isActive()).thenReturn(true);
        when(channel.id()).thenReturn(channelId);
        when(channelId.asLongText()).thenReturn(connectionId);
        when(channel.closeFuture()).thenReturn(closeFuture);
        when(ctx.writeAndFlush(any())).thenReturn(writeFuture);
        when(writeFuture.addListener(any())).thenReturn(writeFuture);

        return ctx;
    }

    private RequestChannel.Request mockRequest(String connectionId) {
        RequestChannel.Request request = mock(RequestChannel.Request.class);
        RequestContext context = mock(RequestContext.class);
        RequestHeader header = mock(RequestHeader.class);

        when(request.context()).thenReturn(context);
        when(context.connectionId()).thenReturn(connectionId);
        when(request.header()).thenReturn(header);
        when(header.apiKey()).thenReturn(ApiKeys.PRODUCE);
        when(request.apiThrottleTimeMs()).thenReturn(0L);
        when(request.requestLocalProperties())
            .thenReturn(new ConcurrentHashMap<>());
        return request;
    }

    private RequestChannel.SendResponse mockSendResponse(String connectionId) {
        RequestChannel.Request request = mockRequest(connectionId);
        RequestChannel.SendResponse sendResponse = mock(RequestChannel.SendResponse.class);

        when(sendResponse.request()).thenReturn(request);
        when(sendResponse.responseLog()).thenReturn(scala.Option.apply(null));

        Send send = mock(Send.class);
        when(sendResponse.responseSend()).thenReturn(send);
        when(send.size()).thenReturn(100L);
        return sendResponse;
    }

    private RequestChannel.SendResponse mockSendResponseWithProduce(String connectionId,
                                                                    ProduceResponse produceResp,
                                                                    long publishId) {
        RequestChannel.Request request = mockRequest(connectionId);
        ConcurrentHashMap<String, Object> props = new ConcurrentHashMap<>();
        props.put("httpAbstractResponse", (AbstractResponse) produceResp);
        props.put("wsPublishId", publishId);
        when(request.requestLocalProperties()).thenReturn(props);

        RequestChannel.SendResponse sendResponse = mock(RequestChannel.SendResponse.class);
        when(sendResponse.request()).thenReturn(request);
        when(sendResponse.responseLog()).thenReturn(scala.Option.apply(null));
        Send send = mock(Send.class);
        when(sendResponse.responseSend()).thenReturn(send);
        when(send.size()).thenReturn(50L);
        return sendResponse;
    }

    private RequestChannel.CloseConnectionResponse mockCloseConnectionResponse(String connectionId) {
        RequestChannel.Request request = mockRequest(connectionId);
        RequestChannel.CloseConnectionResponse response =
            mock(RequestChannel.CloseConnectionResponse.class);
        when(response.request()).thenReturn(request);
        return response;
    }

    private RequestChannel.StartThrottlingResponse mockStartThrottlingResponse(String connectionId,
                                                                                long throttleTimeMs) {
        RequestChannel.Request request = mockRequest(connectionId);
        when(request.apiThrottleTimeMs()).thenReturn(throttleTimeMs);
        RequestChannel.StartThrottlingResponse response =
            mock(RequestChannel.StartThrottlingResponse.class);
        when(response.request()).thenReturn(request);
        return response;
    }

    private RequestChannel.NoOpResponse mockNoOpResponse(String connectionId) {
        RequestChannel.Request request = mockRequest(connectionId);
        RequestChannel.NoOpResponse response = mock(RequestChannel.NoOpResponse.class);
        when(response.request()).thenReturn(request);
        return response;
    }

    private ProduceResponse successProduceResponse() {
        ProduceResponseData.PartitionProduceResponse part =
            new ProduceResponseData.PartitionProduceResponse()
                .setIndex(0)
                .setErrorCode(Errors.NONE.code())
                .setBaseOffset(100L)
                .setLogAppendTimeMs(-1L)
                .setLogStartOffset(0L);
        ProduceResponseData.TopicProduceResponse topic =
            new ProduceResponseData.TopicProduceResponse()
                .setName("t1")
                .setPartitionResponses(List.of(part));
        ProduceResponseData data = new ProduceResponseData()
            .setResponses(new ProduceResponseData.TopicProduceResponseCollection(
                List.of(topic).iterator()))
            .setThrottleTimeMs(0);
        ProduceResponse response = new ProduceResponse(data);
        assertNotNull(response);
        return response;
    }

    private ProduceResponse failedProduceResponse(Errors err, String message) {
        ProduceResponseData.PartitionProduceResponse part =
            new ProduceResponseData.PartitionProduceResponse()
                .setIndex(0)
                .setErrorCode(err.code())
                .setErrorMessage(message)
                .setBaseOffset(-1L)
                .setLogAppendTimeMs(-1L)
                .setLogStartOffset(-1L);
        ProduceResponseData.TopicProduceResponse topic =
            new ProduceResponseData.TopicProduceResponse()
                .setName("t1")
                .setPartitionResponses(List.of(part));
        ProduceResponseData data = new ProduceResponseData()
            .setResponses(new ProduceResponseData.TopicProduceResponseCollection(
                List.of(topic).iterator()))
            .setThrottleTimeMs(0);
        return new ProduceResponse(data);
    }
}
