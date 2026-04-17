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

// Time: Created - TASK-WS1.15

package kafka.server.http.ws;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;

import org.apache.kafka.common.TopicPartition;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.ArgumentCaptor;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link WsConsumerFetchLoop} — per-subscription fetch loop structure.
 *
 * <p>Because TASK-WS1.15 is structural (actual fetch integration arrives with WS1.16),
 * the tests verify:
 * <ul>
 *   <li>Loop lifecycle ({@code stop}, {@code isActive})</li>
 *   <li>Direct {@code deliverRecord} path: tag assignment, frame writing,
 *       credit consumption, offset advance</li>
 *   <li>noAck mode skips tag tracking</li>
 *   <li>Cancellation exits the loop promptly</li>
 * </ul>
 */
class WsConsumerFetchLoopTest {

    private Channel channel;
    private WsCreditManager creditManager;
    private WsDeliveryTagTracker tagTracker;
    private ExecutorService executor;
    private final TopicPartition tp0 = new TopicPartition("ws.orders", 0);

    @BeforeEach
    void setUp() {
        channel = mock(Channel.class);
        ChannelFuture future = mock(ChannelFuture.class);
        when(channel.isWritable()).thenReturn(true);
        when(channel.isOpen()).thenReturn(true);
        when(channel.writeAndFlush(any())).thenReturn(future);
        creditManager = new WsCreditManager(10, channel);
        tagTracker = new WsDeliveryTagTracker();
        executor = Executors.newSingleThreadExecutor();
    }

    @AfterEach
    void tearDown() {
        executor.shutdownNow();
    }

    // ---- Construction ----

    @Test
    void constructor_rejectsNulls() {
        assertThrows(NullPointerException.class, () -> new WsConsumerFetchLoop(
            null, "ws.orders", Map.of(tp0, 0L), creditManager, tagTracker, channel, false));
        assertThrows(NullPointerException.class, () -> new WsConsumerFetchLoop(
            "sub-1", null, Map.of(tp0, 0L), creditManager, tagTracker, channel, false));
        assertThrows(NullPointerException.class, () -> new WsConsumerFetchLoop(
            "sub-1", "ws.orders", null, creditManager, tagTracker, channel, false));
        assertThrows(NullPointerException.class, () -> new WsConsumerFetchLoop(
            "sub-1", "ws.orders", Map.of(tp0, 0L), null, tagTracker, channel, false));
        assertThrows(NullPointerException.class, () -> new WsConsumerFetchLoop(
            "sub-1", "ws.orders", Map.of(tp0, 0L), creditManager, null, channel, false));
        assertThrows(NullPointerException.class, () -> new WsConsumerFetchLoop(
            "sub-1", "ws.orders", Map.of(tp0, 0L), creditManager, tagTracker, null, false));
    }

    @Test
    void constructor_initializesActive() {
        WsConsumerFetchLoop loop = new WsConsumerFetchLoop(
            "sub-1", "ws.orders", Map.of(tp0, 0L), creditManager, tagTracker, channel, false);
        assertTrue(loop.isActive());
    }

    @Test
    void constructor_copiesStartOffsets() {
        WsConsumerFetchLoop loop = new WsConsumerFetchLoop(
            "sub-1", "ws.orders", Map.of(tp0, 42L), creditManager, tagTracker, channel, false);
        assertEquals(42L, loop.currentOffsets().get(tp0));
    }

    // ---- stop() ----

    @Test
    void stop_setsInactive() {
        WsConsumerFetchLoop loop = new WsConsumerFetchLoop(
            "sub-1", "ws.orders", Map.of(tp0, 0L), creditManager, tagTracker, channel, false);
        assertTrue(loop.isActive());
        loop.stop();
        assertFalse(loop.isActive());
    }

    @Test
    void stop_isIdempotent() {
        WsConsumerFetchLoop loop = new WsConsumerFetchLoop(
            "sub-1", "ws.orders", Map.of(tp0, 0L), creditManager, tagTracker, channel, false);
        loop.stop();
        loop.stop();
        assertFalse(loop.isActive());
    }

    // ---- deliverRecord() ----

    @Test
    void deliverRecord_assignsTag_andWritesFrame() {
        WsConsumerFetchLoop loop = new WsConsumerFetchLoop(
            "sub-1", "ws.orders", Map.of(tp0, 0L), creditManager, tagTracker, channel, false);

        loop.deliverRecord(tp0, 5L, "orders.ex", "rk.created", "{\"id\":1}", false);

        // Tag tracker should have one pending delivery with tag 1
        assertEquals(1, tagTracker.pendingCount());
        // Credit consumed
        assertEquals(9, creditManager.available());
        // Offset advanced to next (5 + 1)
        assertEquals(6L, loop.currentOffsets().get(tp0));

        ArgumentCaptor<TextWebSocketFrame> captor = ArgumentCaptor.forClass(TextWebSocketFrame.class);
        verify(channel).writeAndFlush(captor.capture());
        String frame = captor.getValue().text();
        assertTrue(frame.contains("\"type\":\"deliver\""), frame);
        assertTrue(frame.contains("\"subscriptionId\":\"sub-1\""), frame);
        assertTrue(frame.contains("\"deliveryTag\":1"), frame);
        assertTrue(frame.contains("\"redelivered\":false"), frame);
        assertTrue(frame.contains("\"exchange\":\"orders.ex\""), frame);
        assertTrue(frame.contains("\"routingKey\":\"rk.created\""), frame);
        assertTrue(frame.contains("\"partition\":0"), frame);
        assertTrue(frame.contains("\"offset\":5"), frame);
        assertTrue(frame.contains("\"message\":{\"id\":1}"), frame);
    }

    @Test
    void deliverRecord_noAck_skipsTagAssignment() {
        WsConsumerFetchLoop loop = new WsConsumerFetchLoop(
            "sub-1", "ws.orders", Map.of(tp0, 0L), creditManager, tagTracker, channel, true);

        loop.deliverRecord(tp0, 10L, "ex", "rk", "{}", false);

        // Tag tracker should have NO pending deliveries in noAck mode
        assertEquals(0, tagTracker.pendingCount());
        // Credit still consumed
        assertEquals(9, creditManager.available());
        // Offset still advances
        assertEquals(11L, loop.currentOffsets().get(tp0));

        ArgumentCaptor<TextWebSocketFrame> captor = ArgumentCaptor.forClass(TextWebSocketFrame.class);
        verify(channel).writeAndFlush(captor.capture());
        String frame = captor.getValue().text();
        assertTrue(frame.contains("\"deliveryTag\":0"), frame);
    }

    @Test
    void deliverRecord_multipleRecords_assignsMonotonicTags() {
        WsConsumerFetchLoop loop = new WsConsumerFetchLoop(
            "sub-1", "ws.orders", Map.of(tp0, 0L), creditManager, tagTracker, channel, false);

        loop.deliverRecord(tp0, 0L, "ex", "rk", "{\"n\":0}", false);
        loop.deliverRecord(tp0, 1L, "ex", "rk", "{\"n\":1}", false);
        loop.deliverRecord(tp0, 2L, "ex", "rk", "{\"n\":2}", false);

        assertEquals(3, tagTracker.pendingCount());
        assertEquals(7, creditManager.available());
        assertEquals(3L, loop.currentOffsets().get(tp0));

        verify(channel, atLeastOnce()).writeAndFlush(any(TextWebSocketFrame.class));
    }

    @Test
    void deliverRecord_redeliveredFlag_propagatesToFrame() {
        WsConsumerFetchLoop loop = new WsConsumerFetchLoop(
            "sub-1", "ws.orders", Map.of(tp0, 0L), creditManager, tagTracker, channel, false);

        loop.deliverRecord(tp0, 0L, "ex", "rk", "{}", true);

        ArgumentCaptor<TextWebSocketFrame> captor = ArgumentCaptor.forClass(TextWebSocketFrame.class);
        verify(channel).writeAndFlush(captor.capture());
        assertTrue(captor.getValue().text().contains("\"redelivered\":true"));
    }

    // ---- run() loop lifecycle ----

    @Test
    @Timeout(5)
    void run_exitsPromptlyOnStop() throws Exception {
        // Zero credits so the loop will sit in awaitCredits until stop signal.
        WsCreditManager zeroCredits = new WsCreditManager(0, channel);
        WsConsumerFetchLoop loop = new WsConsumerFetchLoop(
            "sub-1", "ws.orders", Map.of(tp0, 0L), zeroCredits, tagTracker, channel, false);

        executor.submit(loop);
        // Give the loop time to start and park in awaitCredits.
        Thread.sleep(100);
        assertTrue(loop.isActive());

        loop.stop();
        executor.shutdown();
        assertTrue(executor.awaitTermination(2, TimeUnit.SECONDS),
            "loop must exit promptly after stop()");
        assertFalse(loop.isActive());
    }

    @Test
    @Timeout(5)
    void run_exitsWhenChannelClosed() throws Exception {
        WsCreditManager zeroCredits = new WsCreditManager(0, channel);
        AtomicBoolean openFlag = new AtomicBoolean(true);
        when(channel.isOpen()).thenAnswer(inv -> openFlag.get());

        WsConsumerFetchLoop loop = new WsConsumerFetchLoop(
            "sub-1", "ws.orders", Map.of(tp0, 0L), zeroCredits, tagTracker, channel, false);

        executor.submit(loop);
        Thread.sleep(100);
        openFlag.set(false);

        executor.shutdown();
        assertTrue(executor.awaitTermination(2, TimeUnit.SECONDS),
            "loop must exit when channel closes");
    }

    @Test
    @Timeout(5)
    void run_doesNotBusySpin_whenNoCredits() throws Exception {
        // With zero credits, the loop should park in awaitCredits — no CPU burn.
        // We assert that no deliverRecord side effects happen during a 200ms window
        // (no tag assignments, no writeAndFlush).
        WsCreditManager zeroCredits = new WsCreditManager(0, channel);
        WsConsumerFetchLoop loop = new WsConsumerFetchLoop(
            "sub-1", "ws.orders", Map.of(tp0, 0L), zeroCredits, tagTracker, channel, false);

        executor.submit(loop);
        Thread.sleep(200);
        loop.stop();
        executor.shutdown();
        assertTrue(executor.awaitTermination(2, TimeUnit.SECONDS));

        assertEquals(0, tagTracker.pendingCount());
        verify(channel, never()).writeAndFlush(any());
    }

    // ---- currentOffsets snapshot ----

    @Test
    void currentOffsets_returnsImmutableSnapshot() {
        WsConsumerFetchLoop loop = new WsConsumerFetchLoop(
            "sub-1", "ws.orders", Map.of(tp0, 7L), creditManager, tagTracker, channel, false);
        Map<TopicPartition, Long> snap = loop.currentOffsets();
        assertNotNull(snap);
        assertThrows(UnsupportedOperationException.class, () -> snap.put(tp0, 99L));
    }
}
