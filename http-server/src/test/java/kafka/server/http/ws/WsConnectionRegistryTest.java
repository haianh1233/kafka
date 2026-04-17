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
// Time: Created - TASK-WS3.07
package kafka.server.http.ws;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link WsConnectionRegistry}, covering the original
 * register/unregister/get surface plus the {@code drain(timeoutMs)} method
 * added in TASK-WS3.07.
 *
 * // Time: Created - TASK-WS3.07
 */
class WsConnectionRegistryTest {

    private WsConnectionRegistry registry;
    private KafkaPrincipal principal;
    private InetSocketAddress remote;

    @BeforeEach
    void setUp() {
        registry = new WsConnectionRegistry();
        principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "testuser");
        remote = new InetSocketAddress("127.0.0.1", 12345);
    }

    // ------------------------------------------------------------------
    //  Basic register / unregister / lookup
    // ------------------------------------------------------------------

    @Test
    void register_storesContext() {
        WsConnectionContext ctx = newCtx("ws-1");
        registry.register("ws-1", ctx);
        assertSame(ctx, registry.get("ws-1"));
        assertTrue(registry.contains("ws-1"));
        assertEquals(1, registry.size());
    }

    @Test
    void unregister_removesContext() {
        WsConnectionContext ctx = newCtx("ws-1");
        registry.register("ws-1", ctx);
        assertSame(ctx, registry.unregister("ws-1"));
        assertFalse(registry.contains("ws-1"));
        assertEquals(0, registry.size());
    }

    @Test
    void get_unknownId_returnsNull() {
        assertNull(registry.get("missing"));
    }

    @Test
    void unregister_nullId_returnsNull() {
        assertNull(registry.unregister(null));
    }

    @Test
    void contexts_reflectsSnapshot() {
        registry.register("a", newCtx("ws-a"));
        registry.register("b", newCtx("ws-b"));
        assertEquals(2, registry.contexts().size());
        assertEquals(2, registry.connectionIds().size());
    }

    // ------------------------------------------------------------------
    //  Drain — TASK-WS3.07
    // ------------------------------------------------------------------

    @Test
    void drain_noConnections_returnsTrueImmediately() {
        long start = System.currentTimeMillis();
        assertTrue(registry.drain(1000));
        long elapsed = System.currentTimeMillis() - start;
        assertTrue(elapsed < 500, "drain on empty registry should return quickly, elapsed=" + elapsed);
    }

    @Test
    void drain_sendsCloseFrame1001_toAllConnections() {
        MockCtx ctx1 = registerMock("a");
        MockCtx ctx2 = registerMock("b");
        MockCtx ctx3 = registerMock("c");

        // Simulate channels closing promptly after the close frame is written.
        ctx1.onCloseFrame(() -> registry.unregister("a"));
        ctx2.onCloseFrame(() -> registry.unregister("b"));
        ctx3.onCloseFrame(() -> registry.unregister("c"));

        assertTrue(registry.drain(2000));

        assertCloseFrame(ctx1, 1001, "server shutting down");
        assertCloseFrame(ctx2, 1001, "server shutting down");
        assertCloseFrame(ctx3, 1001, "server shutting down");
    }

    @Test
    void drain_returnsFalseOnTimeout_whenConnectionsDoNotClear() {
        // Register connections but do NOT unregister them after close —
        // simulates a misbehaving client that does not drop the socket.
        MockCtx ctx1 = registerMock("a");
        MockCtx ctx2 = registerMock("b");

        long start = System.currentTimeMillis();
        assertFalse(registry.drain(200),
                "drain must return false when connections remain past timeout");
        long elapsed = System.currentTimeMillis() - start;

        assertTrue(elapsed >= 200, "must wait at least the timeout, elapsed=" + elapsed);
        assertTrue(elapsed < 2000, "must not wait significantly past timeout, elapsed=" + elapsed);

        // Close frame was still delivered to both connections.
        assertCloseFrame(ctx1, 1001, "server shutting down");
        assertCloseFrame(ctx2, 1001, "server shutting down");
    }

    @Test
    void drain_isIdempotent() {
        MockCtx ctx1 = registerMock("a");
        ctx1.onCloseFrame(() -> registry.unregister("a"));

        assertTrue(registry.drain(1000));
        // Second call on an empty registry should be a no-op and return true.
        assertTrue(registry.drain(1000));
        // Close frame was written exactly once (only first drain found the ctx).
        verify(ctx1.channel, times(1)).writeAndFlush(any(CloseWebSocketFrame.class));
    }

    @Test
    void drain_closeFrame_usesCode1001_notNormalClosure() {
        MockCtx ctx = registerMock("a");
        ctx.onCloseFrame(() -> registry.unregister("a"));
        registry.drain(1000);

        ArgumentCaptor<CloseWebSocketFrame> captor = ArgumentCaptor.forClass(CloseWebSocketFrame.class);
        verify(ctx.channel, atLeastOnce()).writeAndFlush(captor.capture());
        CloseWebSocketFrame frame = captor.getValue();
        assertEquals(1001, frame.statusCode(),
                "close frame must use 1001 (Going Away), not 1000 (Normal Closure)");
        frame.release();
    }

    // ------------------------------------------------------------------
    //  Helpers
    // ------------------------------------------------------------------

    private WsConnectionContext newCtx(String sessionId) {
        ChannelHandlerContext ch = mock(ChannelHandlerContext.class);
        Channel ntty = mock(Channel.class);
        when(ch.channel()).thenReturn(ntty);
        when(ntty.isActive()).thenReturn(true);
        ChannelFuture fut = mock(ChannelFuture.class);
        when(ch.writeAndFlush(any())).thenReturn(fut);
        return new WsConnectionContext(sessionId, principal, "/", ch, remote);
    }

    /** Registers a context whose outbound writeAndFlush can be hooked in tests. */
    private MockCtx registerMock(String id) {
        MockCtx m = new MockCtx(principal, remote);
        registry.register(id, m.context);
        return m;
    }

    private static void assertCloseFrame(MockCtx ctx, int expectedCode, String expectedReason) {
        ArgumentCaptor<CloseWebSocketFrame> captor = ArgumentCaptor.forClass(CloseWebSocketFrame.class);
        verify(ctx.channel, atLeastOnce()).writeAndFlush(captor.capture());
        // The last value captured is the one used by drain (there's only one).
        List<CloseWebSocketFrame> all = captor.getAllValues();
        CloseWebSocketFrame frame = all.get(all.size() - 1);
        assertEquals(expectedCode, frame.statusCode());
        assertEquals(expectedReason, frame.reasonText());
        assertNotNull(frame);
        frame.release();
    }

    /** A mock ChannelHandlerContext wrapper whose {@code writeAndFlush} can fire a hook. */
    private static final class MockCtx {
        final ChannelHandlerContext channel;
        final WsConnectionContext context;
        private final AtomicBoolean hookFired = new AtomicBoolean(false);
        private volatile Runnable onCloseHook;

        MockCtx(KafkaPrincipal principal, InetSocketAddress remote) {
            this.channel = mock(ChannelHandlerContext.class);
            Channel ntty = mock(Channel.class);
            when(this.channel.channel()).thenReturn(ntty);
            when(ntty.isActive()).thenReturn(true);

            ChannelFuture fut = mock(ChannelFuture.class);
            // Mock addListener to return the future itself — WsConnectionContext.close
            // chains addListener(ChannelFutureListener.CLOSE) onto the write future.
            when(fut.addListener(any())).thenReturn(fut);
            when(this.channel.writeAndFlush(any())).thenAnswer(invocation -> {
                Object arg = invocation.getArgument(0);
                if (arg instanceof CloseWebSocketFrame && hookFired.compareAndSet(false, true)) {
                    Runnable r = onCloseHook;
                    if (r != null) r.run();
                }
                return fut;
            });

            this.context = new WsConnectionContext(
                    "ws-mock", principal, "/", this.channel, remote);
        }

        void onCloseFrame(Runnable r) {
            this.onCloseHook = r;
        }
    }
}
