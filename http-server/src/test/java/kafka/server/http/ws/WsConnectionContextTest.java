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
// Time: Created - TASK-WS1.02
// Time: Update - TASK-WS3.04 - added exclusive consumer semantics
package kafka.server.http.ws;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * // Time: Created - TASK-WS1.02
 */
class WsConnectionContextTest {

    private WsConnectionContext ctx;
    private ChannelHandlerContext mockChannel;
    private Channel mockNettyChannel;
    private KafkaPrincipal principal;
    private InetSocketAddress remoteAddress;

    @BeforeEach
    void setUp() {
        mockChannel = mock(ChannelHandlerContext.class);
        mockNettyChannel = mock(Channel.class);
        when(mockChannel.channel()).thenReturn(mockNettyChannel);
        when(mockNettyChannel.isActive()).thenReturn(true);
        ChannelFuture mockFuture = mock(ChannelFuture.class);
        when(mockChannel.writeAndFlush(any())).thenReturn(mockFuture);

        principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "testuser");
        remoteAddress = new InetSocketAddress("127.0.0.1", 54321);

        ctx = new WsConnectionContext(
            "ws-1-abc12345",
            principal,
            "/",
            mockChannel,
            remoteAddress);
    }

    // --- Immutable accessors ---

    @Test
    void constructor_setsImmutableFields() {
        assertEquals("ws-1-abc12345", ctx.sessionId());
        assertEquals("testuser", ctx.principal().getName());
        assertEquals("/", ctx.vhost());
        assertSame(mockChannel, ctx.channel());
        assertEquals(remoteAddress, ctx.remoteAddress());
        assertEquals(54321, ctx.remoteAddress().getPort());
    }

    @Test
    void connectTime_setAtConstruction() {
        Instant before = Instant.now();
        WsConnectionContext c = new WsConnectionContext(
            "ws-2-x", principal, "/", mockChannel, remoteAddress);
        Instant after = Instant.now();
        Instant t = c.connectTime();
        assertNotNull(t);
        // connectTime should be between before and after (allowing equality)
        assertTrue(!t.isBefore(before), "connectTime should not be before test start");
        assertTrue(!t.isAfter(after), "connectTime should not be after test end");
    }

    @Test
    void principal_returnsSameInstance() {
        assertSame(principal, ctx.principal());
    }

    // --- Subscriptions map ---

    @Test
    void subscriptions_startsEmpty() {
        assertTrue(ctx.subscriptions().isEmpty());
    }

    @Test
    void subscriptions_isConcurrentHashMap() {
        ConcurrentHashMap<String, Object> subs = ctx.subscriptions();
        assertNotNull(subs);
    }

    @Test
    void subscriptions_addRemove() {
        ctx.subscriptions().put("sub-1", "placeholder1");
        ctx.subscriptions().put("sub-2", "placeholder2");
        assertEquals(2, ctx.subscriptions().size());
        ctx.subscriptions().remove("sub-1");
        assertEquals(1, ctx.subscriptions().size());
        assertEquals("placeholder2", ctx.subscriptions().get("sub-2"));
    }

    @Test
    void subscriptions_concurrentAdd() {
        ctx.subscriptions().put("sub-1", "placeholder");
        ctx.subscriptions().put("sub-2", "placeholder");
        assertEquals(2, ctx.subscriptions().size());
    }

    @Test
    void subscriptions_threadSafety_concurrentAddRemove() throws InterruptedException {
        final int threads = 8;
        final int opsPerThread = 500;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        AtomicInteger failures = new AtomicInteger();

        try {
            for (int t = 0; t < threads; t++) {
                final int threadId = t;
                pool.submit(() -> {
                    try {
                        start.await();
                        for (int i = 0; i < opsPerThread; i++) {
                            String key = "t" + threadId + "-k" + i;
                            ctx.subscriptions().put(key, "v");
                            ctx.subscriptions().remove(key);
                        }
                    } catch (Throwable ex) {
                        failures.incrementAndGet();
                    } finally {
                        done.countDown();
                    }
                });
            }
            start.countDown();
            assertTrue(done.await(15, TimeUnit.SECONDS), "workers did not finish in time");
            assertEquals(0, failures.get(), "no thread should have thrown");
            assertTrue(ctx.subscriptions().isEmpty(), "all puts should have been removed");
        } finally {
            pool.shutdownNow();
        }
    }

    // --- Publish confirms ---

    @Test
    void publishConfirms_defaultDisabled() {
        assertFalse(ctx.isPublishConfirmsEnabled());
    }

    @Test
    void enablePublishConfirms_setsFlag() {
        ctx.enablePublishConfirms();
        assertTrue(ctx.isPublishConfirmsEnabled());
    }

    @Test
    void enablePublishConfirms_idempotent() {
        ctx.enablePublishConfirms();
        ctx.enablePublishConfirms();
        ctx.enablePublishConfirms();
        assertTrue(ctx.isPublishConfirmsEnabled());
    }

    // --- Null checks ---

    @Test
    void nullSessionId_throwsNPE() {
        assertThrows(NullPointerException.class, () -> new WsConnectionContext(
            null, principal, "/", mockChannel, remoteAddress));
    }

    @Test
    void nullPrincipal_throwsNPE() {
        assertThrows(NullPointerException.class, () -> new WsConnectionContext(
            "ws-1-x", null, "/", mockChannel, remoteAddress));
    }

    @Test
    void nullVhost_throwsNPE() {
        assertThrows(NullPointerException.class, () -> new WsConnectionContext(
            "ws-1-x", principal, null, mockChannel, remoteAddress));
    }

    @Test
    void nullChannel_throwsNPE() {
        assertThrows(NullPointerException.class, () -> new WsConnectionContext(
            "ws-1-x", principal, "/", null, remoteAddress));
    }

    @Test
    void nullRemoteAddress_throwsNPE() {
        assertThrows(NullPointerException.class, () -> new WsConnectionContext(
            "ws-1-x", principal, "/", mockChannel, null));
    }

    // --- Channel operations ---

    @Test
    void sendFrame_writesTextWebSocketFrame() {
        ctx.sendFrame("{\"type\":\"connected\"}");
        ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
        verify(mockChannel, times(1)).writeAndFlush(captor.capture());
        Object written = captor.getValue();
        assertTrue(written instanceof TextWebSocketFrame,
            "expected TextWebSocketFrame but got " + written.getClass());
        TextWebSocketFrame frame = (TextWebSocketFrame) written;
        assertEquals("{\"type\":\"connected\"}", frame.text());
        frame.release();
    }

    @Test
    void close_writesCloseFrameAndClosesChannel() {
        ChannelFuture future = mock(ChannelFuture.class);
        when(mockChannel.writeAndFlush(any(CloseWebSocketFrame.class))).thenReturn(future);

        ctx.close(1000, "normal");

        ArgumentCaptor<CloseWebSocketFrame> captor = ArgumentCaptor.forClass(CloseWebSocketFrame.class);
        verify(mockChannel, times(1)).writeAndFlush(captor.capture());
        CloseWebSocketFrame frame = captor.getValue();
        assertEquals(1000, frame.statusCode());
        assertEquals("normal", frame.reasonText());
        frame.release();
    }

    @Test
    void isActive_delegatesToChannel() {
        when(mockNettyChannel.isActive()).thenReturn(true);
        assertTrue(ctx.isActive());

        when(mockNettyChannel.isActive()).thenReturn(false);
        assertFalse(ctx.isActive());
    }

    // --- TASK-WS3.04: exclusive consumer wiring ---

    @Test
    void exclusiveConsumerManager_unsetByDefault() {
        assertNull(ctx.exclusiveConsumerManager());
    }

    @Test
    void onClose_withoutExclusiveManager_returnsEmptyList() {
        List<String> released = ctx.onClose();
        assertNotNull(released);
        assertTrue(released.isEmpty());
    }

    @Test
    void onClose_releasesConnectionLocks_viaExclusiveManager() {
        ExclusiveConsumerManager excl = new ExclusiveConsumerManager();
        WsConnectionContext c = new WsConnectionContext(
            "ws-3-close", principal, "/", mockChannel, remoteAddress, excl);

        assertTrue(excl.tryAcquireExclusive("q1", "ws-3-close"));
        assertTrue(excl.tryAcquireExclusive("q2", "ws-3-close"));
        // Different connection's lock must not be affected.
        assertTrue(excl.tryAcquireExclusive("q-other", "other-conn"));

        List<String> released = c.onClose();
        assertEquals(2, released.size());
        assertTrue(released.contains("q1"));
        assertTrue(released.contains("q2"));
        assertFalse(excl.isExclusivelyLocked("q1"));
        assertFalse(excl.isExclusivelyLocked("q2"));
        assertTrue(excl.isLockedBy("q-other", "other-conn"));
    }

    @Test
    void onClose_noLocks_returnsEmptyList() {
        ExclusiveConsumerManager excl = new ExclusiveConsumerManager();
        WsConnectionContext c = new WsConnectionContext(
            "ws-3-empty", principal, "/", mockChannel, remoteAddress, excl);
        List<String> released = c.onClose();
        assertTrue(released.isEmpty());
    }

    @Test
    void sixArgConstructor_storesExclusiveManager() {
        ExclusiveConsumerManager excl = new ExclusiveConsumerManager();
        WsConnectionContext c = new WsConnectionContext(
            "ws-3-ctor", principal, "/", mockChannel, remoteAddress, excl);
        assertSame(excl, c.exclusiveConsumerManager());
    }
}
