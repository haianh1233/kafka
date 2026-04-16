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
package kafka.server.http;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelId;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import kafka.network.RequestChannel;
import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.requests.RequestHeader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link HttpProcessor} — response routing bridge between RequestChannel and Netty.
 *
 * // Time: Created - TASK-B.04
 */
class HttpProcessorTest {

    private HttpProcessor processor;

    @BeforeEach
    void setUp() {
        processor = new HttpProcessor(100);  // processor ID = 100
    }

    @AfterEach
    void tearDown() {
        processor.close();
    }

    @Test
    void id_returnsConfiguredId() {
        assertEquals(100, processor.id());
    }

    @Test
    void registerChannel_addsToMap() {
        ChannelHandlerContext ctx = mockActiveCtx("conn-1");
        processor.registerChannel("conn-1", ctx);
        assertEquals(1, processor.channelCount());
    }

    @Test
    void registerChannel_autoRemovesOnClose() {
        // Capture the close listener so we can trigger it manually
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        Channel channel = mock(Channel.class);
        ChannelId channelId = mock(ChannelId.class);
        ChannelFuture closeFuture = mock(ChannelFuture.class);

        when(ctx.channel()).thenReturn(channel);
        when(channel.isActive()).thenReturn(true);
        when(channel.id()).thenReturn(channelId);
        when(channelId.asLongText()).thenReturn("conn-close-test");
        when(channel.closeFuture()).thenReturn(closeFuture);

        // Capture the listener added to closeFuture
        ArgumentCaptor<ChannelFutureListener> listenerCaptor =
            ArgumentCaptor.forClass(ChannelFutureListener.class);

        processor.registerChannel("conn-close-test", ctx);
        assertEquals(1, processor.channelCount());

        verify(closeFuture).addListener(listenerCaptor.capture());

        // Trigger the close listener
        try {
            listenerCaptor.getValue().operationComplete(closeFuture);
        } catch (Exception e) {
            fail("Close listener threw exception: " + e.getMessage());
        }

        // Channel should be auto-removed
        assertEquals(0, processor.channelCount());
    }

    @Test
    void registerChannel_rejectsNullConnectionId() {
        ChannelHandlerContext ctx = mockActiveCtx("test");
        assertThrows(NullPointerException.class, () -> processor.registerChannel(null, ctx));
    }

    @Test
    void registerChannel_rejectsNullCtx() {
        assertThrows(NullPointerException.class, () -> processor.registerChannel("conn-1", null));
    }

    @Test
    void enqueueResponse_isNonBlocking() {
        RequestChannel.Response response = mockSendResponse("conn-enqueue");
        processor.enqueueResponse(response);
        assertEquals(1, processor.responseQueueSize());
    }

    @Test
    void enqueueResponse_rejectsNull() {
        assertThrows(NullPointerException.class, () -> processor.enqueueResponse(null));
    }

    @Test
    void processResponses_sendResponse_writesToChannel() {
        // Register channel
        ChannelHandlerContext ctx = mockActiveCtx("conn-send");
        processor.registerChannel("conn-send", ctx);

        // Enqueue SendResponse
        RequestChannel.Response response = mockSendResponse("conn-send");
        processor.enqueueResponse(response);

        // Process
        processor.processResponses();

        // Verify writeAndFlush was called
        verify(ctx).writeAndFlush(any(FullHttpResponse.class));

        // Verify channel is removed from map after response sent (HTTP 1.1 one-shot)
        assertEquals(0, processor.channelCount());
    }

    @Test
    void processResponses_closeConnectionResponse_closesChannel() {
        // Register channel
        ChannelHandlerContext ctx = mockActiveCtx("conn-close");
        processor.registerChannel("conn-close", ctx);

        // Enqueue CloseConnectionResponse
        RequestChannel.CloseConnectionResponse response = mockCloseConnectionResponse("conn-close");
        processor.enqueueResponse(response);

        // Process
        processor.processResponses();

        // Verify ctx.close() was called
        verify(ctx).close();

        // Channel should be removed
        assertEquals(0, processor.channelCount());
    }

    @Test
    void processResponses_startThrottling_sends429() {
        // Register channel
        ChannelHandlerContext ctx = mockActiveCtx("conn-throttle");
        ChannelFuture writeFuture = mock(ChannelFuture.class);
        when(ctx.writeAndFlush(any())).thenReturn(writeFuture);
        processor.registerChannel("conn-throttle", ctx);

        // Enqueue StartThrottlingResponse with 5000ms throttle time
        RequestChannel.StartThrottlingResponse response = mockStartThrottlingResponse("conn-throttle", 5000);
        processor.enqueueResponse(response);

        // Process
        processor.processResponses();

        // Capture the HTTP response written
        ArgumentCaptor<FullHttpResponse> responseCaptor =
            ArgumentCaptor.forClass(FullHttpResponse.class);
        verify(ctx).writeAndFlush(responseCaptor.capture());

        FullHttpResponse httpResponse = responseCaptor.getValue();
        assertEquals(429, httpResponse.status().code());

        // Retry-After: ceil(5000/1000) = 5
        String retryAfter = httpResponse.headers().get(HttpHeaderNames.RETRY_AFTER);
        assertEquals("5", retryAfter);

        // Verify JSON body contains error info
        String body = httpResponse.content().toString(java.nio.charset.StandardCharsets.UTF_8);
        assertTrue(body.contains("\"errorCode\":89"));
        assertTrue(body.contains("THROTTLING_QUOTA_EXCEEDED"));
        assertTrue(body.contains("\"throttleTimeMs\":5000"));

        // Release the response buffer
        httpResponse.release();

        // Channel should be removed
        assertEquals(0, processor.channelCount());
    }

    @Test
    void processResponses_startThrottling_retryAfterRoundsUp() {
        // Test ceiling behavior: 1500ms -> ceil(1.5) = 2 seconds
        ChannelHandlerContext ctx = mockActiveCtx("conn-throttle-ceil");
        ChannelFuture writeFuture = mock(ChannelFuture.class);
        when(ctx.writeAndFlush(any())).thenReturn(writeFuture);
        processor.registerChannel("conn-throttle-ceil", ctx);

        RequestChannel.StartThrottlingResponse response = mockStartThrottlingResponse("conn-throttle-ceil", 1500);
        processor.enqueueResponse(response);

        processor.processResponses();

        ArgumentCaptor<FullHttpResponse> responseCaptor =
            ArgumentCaptor.forClass(FullHttpResponse.class);
        verify(ctx).writeAndFlush(responseCaptor.capture());

        FullHttpResponse httpResponse = responseCaptor.getValue();
        // Retry-After: ceil(1500/1000) = 2
        String retryAfter = httpResponse.headers().get(HttpHeaderNames.RETRY_AFTER);
        assertEquals("2", retryAfter);
        httpResponse.release();
    }

    @Test
    void processResponses_startThrottling_retryAfterMinimumIsOne() {
        // Test minimum: 100ms -> ceil(0.1) = 1 (min is 1)
        ChannelHandlerContext ctx = mockActiveCtx("conn-throttle-min");
        ChannelFuture writeFuture = mock(ChannelFuture.class);
        when(ctx.writeAndFlush(any())).thenReturn(writeFuture);
        processor.registerChannel("conn-throttle-min", ctx);

        RequestChannel.StartThrottlingResponse response = mockStartThrottlingResponse("conn-throttle-min", 100);
        processor.enqueueResponse(response);

        processor.processResponses();

        ArgumentCaptor<FullHttpResponse> responseCaptor =
            ArgumentCaptor.forClass(FullHttpResponse.class);
        verify(ctx).writeAndFlush(responseCaptor.capture());

        FullHttpResponse httpResponse = responseCaptor.getValue();
        // Retry-After: max(1, ceil(100/1000)) = max(1, 1) = 1
        String retryAfter = httpResponse.headers().get(HttpHeaderNames.RETRY_AFTER);
        assertEquals("1", retryAfter);
        httpResponse.release();
    }

    @Test
    void processResponses_endThrottling_isNoOp() {
        // Register channel
        ChannelHandlerContext ctx = mockActiveCtx("conn-end-throttle");
        processor.registerChannel("conn-end-throttle", ctx);

        // Enqueue EndThrottlingResponse
        RequestChannel.EndThrottlingResponse response = mockEndThrottlingResponse("conn-end-throttle");
        processor.enqueueResponse(response);

        // Process
        processor.processResponses();

        // No writeAndFlush, no close
        verify(ctx, never()).writeAndFlush(any());
        verify(ctx, never()).close();

        // Channel should still be in the map (EndThrottling doesn't remove it)
        assertEquals(1, processor.channelCount());
    }

    @Test
    void processResponses_channelAlreadyClosed_silentlyDropped() {
        // Register channel, then mark as inactive
        ChannelHandlerContext ctx = mockInactiveCtx("conn-dead");
        processor.registerChannel("conn-dead", ctx);

        // Enqueue SendResponse
        RequestChannel.Response response = mockSendResponse("conn-dead");
        processor.enqueueResponse(response);

        // Process — should not throw
        assertDoesNotThrow(() -> processor.processResponses());

        // No writeAndFlush since channel is inactive
        verify(ctx, never()).writeAndFlush(any());
    }

    @Test
    void processResponses_channelNotRegistered_silentlyDropped() {
        // Enqueue response for an unregistered connection
        RequestChannel.Response response = mockSendResponse("conn-unregistered");
        processor.enqueueResponse(response);

        // Process — should not throw
        assertDoesNotThrow(() -> processor.processResponses());
    }

    @Test
    void processResponses_exceptionInOneResponse_doesNotBlockOthers() {
        // Register two channels
        ChannelHandlerContext ctx1 = mockActiveCtx("conn-bad");
        ChannelHandlerContext ctx2 = mockActiveCtx("conn-good");
        processor.registerChannel("conn-bad", ctx1);
        processor.registerChannel("conn-good", ctx2);

        // First channel throws on writeAndFlush
        when(ctx1.writeAndFlush(any())).thenThrow(new RuntimeException("Write failed"));

        // Enqueue two SendResponses
        processor.enqueueResponse(mockSendResponse("conn-bad"));
        processor.enqueueResponse(mockSendResponse("conn-good"));

        // Process — should not throw
        assertDoesNotThrow(() -> processor.processResponses());

        // Second response should still be delivered
        verify(ctx2).writeAndFlush(any(FullHttpResponse.class));
    }

    @Test
    void startDrainer_processesQueuedResponses() throws InterruptedException {
        // Register channel
        ChannelHandlerContext ctx = mockActiveCtx("conn-drainer");
        ChannelFuture writeFuture = mock(ChannelFuture.class);
        when(ctx.writeAndFlush(any())).thenReturn(writeFuture);
        processor.registerChannel("conn-drainer", ctx);

        // Enqueue response
        processor.enqueueResponse(mockSendResponse("conn-drainer"));

        // Start drainer
        processor.startDrainer();

        // Wait for the drainer to process (should happen within a few poll intervals)
        boolean delivered = false;
        for (int i = 0; i < 50; i++) {
            Thread.sleep(20);
            if (processor.responseQueueSize() == 0) {
                delivered = true;
                break;
            }
        }
        assertTrue(delivered, "Drainer should have processed the response within 1 second");

        // Verify response was written
        verify(ctx, timeout(1000)).writeAndFlush(any(FullHttpResponse.class));
    }

    @Test
    void close_stopsDrainerThread() throws InterruptedException {
        // Start drainer
        processor.startDrainer();

        // Give thread time to start
        Thread.sleep(50);

        // Close should stop the drainer
        processor.close();

        // Verify queue and channels are cleared
        assertEquals(0, processor.responseQueueSize());
        assertEquals(0, processor.channelCount());
    }

    @Test
    void close_isIdempotent() {
        processor.startDrainer();
        assertDoesNotThrow(() -> {
            processor.close();
            processor.close();
            processor.close();
        });
    }

    @Test
    void close_worksWithoutStartingDrainer() {
        // close() should be safe even if startDrainer() was never called
        assertDoesNotThrow(() -> processor.close());
    }

    @Test
    void responseQueueSize_reflectsEnqueuedResponses() {
        assertEquals(0, processor.responseQueueSize());

        processor.enqueueResponse(mockSendResponse("conn-1"));
        assertEquals(1, processor.responseQueueSize());

        processor.enqueueResponse(mockSendResponse("conn-2"));
        assertEquals(2, processor.responseQueueSize());
    }

    @Test
    void channelCount_reflectsRegisteredChannels() {
        assertEquals(0, processor.channelCount());

        processor.registerChannel("conn-1", mockActiveCtx("conn-1"));
        assertEquals(1, processor.channelCount());

        processor.registerChannel("conn-2", mockActiveCtx("conn-2"));
        assertEquals(2, processor.channelCount());
    }

    // --- Mock helpers ---

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

        return ctx;
    }

    private ChannelHandlerContext mockInactiveCtx(String connectionId) {
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        Channel channel = mock(Channel.class);
        ChannelId channelId = mock(ChannelId.class);
        ChannelFuture closeFuture = mock(ChannelFuture.class);

        when(ctx.channel()).thenReturn(channel);
        when(channel.isActive()).thenReturn(false);
        when(channel.id()).thenReturn(channelId);
        when(channelId.asLongText()).thenReturn(connectionId);
        when(channel.closeFuture()).thenReturn(closeFuture);

        return ctx;
    }

    /**
     * Creates a mock RequestChannel.Request with the given connectionId.
     */
    private RequestChannel.Request mockRequest(String connectionId) {
        RequestChannel.Request request = mock(RequestChannel.Request.class);
        RequestContext context = mock(RequestContext.class);
        RequestHeader header = mock(RequestHeader.class);

        when(request.context()).thenReturn(context);
        when(context.connectionId()).thenReturn(connectionId);
        when(request.header()).thenReturn(header);
        when(header.apiKey()).thenReturn(ApiKeys.METADATA);
        when(request.apiThrottleTimeMs()).thenReturn(0L);

        return request;
    }

    /**
     * Creates a mock SendResponse for the given connectionId.
     */
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

    /**
     * Creates a mock CloseConnectionResponse for the given connectionId.
     */
    private RequestChannel.CloseConnectionResponse mockCloseConnectionResponse(String connectionId) {
        RequestChannel.Request request = mockRequest(connectionId);
        RequestChannel.CloseConnectionResponse response = mock(RequestChannel.CloseConnectionResponse.class);
        when(response.request()).thenReturn(request);
        return response;
    }

    /**
     * Creates a mock StartThrottlingResponse for the given connectionId.
     */
    private RequestChannel.StartThrottlingResponse mockStartThrottlingResponse(String connectionId, long throttleTimeMs) {
        RequestChannel.Request request = mockRequest(connectionId);
        when(request.apiThrottleTimeMs()).thenReturn(throttleTimeMs);
        RequestChannel.StartThrottlingResponse response = mock(RequestChannel.StartThrottlingResponse.class);
        when(response.request()).thenReturn(request);
        return response;
    }

    /**
     * Creates a mock EndThrottlingResponse for the given connectionId.
     */
    private RequestChannel.EndThrottlingResponse mockEndThrottlingResponse(String connectionId) {
        RequestChannel.Request request = mockRequest(connectionId);
        RequestChannel.EndThrottlingResponse response = mock(RequestChannel.EndThrottlingResponse.class);
        when(response.request()).thenReturn(request);
        return response;
    }
}
