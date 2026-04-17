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
// Time: Update - TASK-WS3.03 - added competing consumer / group integration

package kafka.server.http.ws;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;

import org.apache.kafka.common.TopicPartition;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link WsSubscriptionManager} — per-connection subscription lifecycle.
 */
class WsSubscriptionManagerTest {

    private WsSubscriptionManager manager;
    private ExecutorService executor;
    private Channel channel;
    private final TopicPartition tp0 = new TopicPartition("ws.orders", 0);
    private final TopicPartition tp1 = new TopicPartition("ws.events", 0);

    @BeforeEach
    void setUp() {
        executor = Executors.newFixedThreadPool(4);
        channel = mock(Channel.class);
        ChannelFuture future = mock(ChannelFuture.class);
        when(channel.isWritable()).thenReturn(true);
        when(channel.isOpen()).thenReturn(true);
        when(channel.writeAndFlush(any())).thenReturn(future);
        manager = new WsSubscriptionManager(executor);
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        manager.cancelAll();
        executor.shutdownNow();
        executor.awaitTermination(2, TimeUnit.SECONDS);
    }

    // ---- Construction ----

    @Test
    void constructor_rejectsNullExecutor() {
        assertThrows(NullPointerException.class, () -> new WsSubscriptionManager(null));
    }

    // ---- subscribe ----

    @Test
    void subscribe_createsSubscription() {
        manager.subscribe("sub-1", "orders", "ws.orders",
            Set.of(tp0), Map.of(tp0, 0L), 100, false, channel);

        assertEquals(1, manager.activeCount());
        SubscriptionContext ctx = manager.getSubscription("sub-1");
        assertNotNull(ctx);
        assertEquals("sub-1", ctx.subscriptionId());
        assertEquals("orders", ctx.queueName());
        assertEquals("ws.orders", ctx.topic());
        assertNotNull(ctx.deliveryTagTracker());
        assertNotNull(ctx.creditManager());
        assertNotNull(ctx.fetchLoop());
        assertTrue(ctx.fetchLoop().isActive());
        assertEquals(100, ctx.creditManager().available());
    }

    @Test
    void subscribe_duplicateId_throwsIllegalState() {
        manager.subscribe("sub-1", "orders", "ws.orders",
            Set.of(tp0), Map.of(tp0, 0L), 100, false, channel);
        assertThrows(IllegalStateException.class, () ->
            manager.subscribe("sub-1", "orders", "ws.orders",
                Set.of(tp0), Map.of(tp0, 0L), 100, false, channel));
    }

    @Test
    void subscribe_multipleSubscriptions_allTracked() {
        manager.subscribe("sub-1", "orders", "ws.orders",
            Set.of(tp0), Map.of(tp0, 0L), 100, false, channel);
        manager.subscribe("sub-2", "events", "ws.events",
            Set.of(tp1), Map.of(tp1, 0L), 50, false, channel);

        assertEquals(2, manager.activeCount());
        assertNotNull(manager.getSubscription("sub-1"));
        assertNotNull(manager.getSubscription("sub-2"));
    }

    @Test
    void subscribe_nullIdsRejected() {
        assertThrows(NullPointerException.class, () -> manager.subscribe(
            null, "orders", "ws.orders", Set.of(tp0), Map.of(tp0, 0L), 10, false, channel));
        assertThrows(NullPointerException.class, () -> manager.subscribe(
            "sub-1", null, "ws.orders", Set.of(tp0), Map.of(tp0, 0L), 10, false, channel));
        assertThrows(NullPointerException.class, () -> manager.subscribe(
            "sub-1", "orders", null, Set.of(tp0), Map.of(tp0, 0L), 10, false, channel));
        assertThrows(NullPointerException.class, () -> manager.subscribe(
            "sub-1", "orders", "ws.orders", Set.of(tp0), Map.of(tp0, 0L), 10, false, null));
    }

    @Test
    void subscribe_schedulesFetchLoop() throws InterruptedException {
        // Verify that subscribe actually dispatches the fetch loop runnable onto the executor.
        AtomicInteger executedCount = new AtomicInteger();
        CountDownLatch latch = new CountDownLatch(1);
        ThreadPoolExecutor tracking = new ThreadPoolExecutor(
            1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>()) {
            @Override
            public void execute(Runnable command) {
                executedCount.incrementAndGet();
                super.execute(() -> {
                    latch.countDown();
                    command.run();
                });
            }
        };
        WsSubscriptionManager localMgr = new WsSubscriptionManager(tracking);
        try {
            localMgr.subscribe("sub-x", "q", "ws.q",
                Set.of(tp0), Map.of(tp0, 0L), 0, false, channel);
            assertTrue(latch.await(1, TimeUnit.SECONDS), "fetch loop should be scheduled");
            assertEquals(1, executedCount.get());
        } finally {
            localMgr.cancelAll();
            tracking.shutdownNow();
            tracking.awaitTermination(2, TimeUnit.SECONDS);
        }
    }

    // ---- unsubscribe ----

    @Test
    @Timeout(5)
    void unsubscribe_stopsAndRemoves() throws InterruptedException {
        manager.subscribe("sub-1", "orders", "ws.orders",
            Set.of(tp0), Map.of(tp0, 0L), 100, false, channel);
        SubscriptionContext ctx = manager.getSubscription("sub-1");

        manager.unsubscribe("sub-1");

        assertEquals(0, manager.activeCount());
        assertNull(manager.getSubscription("sub-1"));
        // Give loop a moment to notice stop
        Thread.sleep(100);
        assertFalse(ctx.fetchLoop().isActive());
    }

    @Test
    void unsubscribe_unknownId_returnsEmptyMap() {
        Map<TopicPartition, Long> offsets = manager.unsubscribe("nonexistent");
        assertTrue(offsets.isEmpty());
    }

    @Test
    void unsubscribe_returnsCommittableOffsets() {
        manager.subscribe("sub-1", "orders", "ws.orders",
            Set.of(tp0), Map.of(tp0, 0L), 10, false, channel);
        SubscriptionContext ctx = manager.getSubscription("sub-1");

        // Simulate two deliveries and ack them, so there are committable offsets to return.
        ctx.fetchLoop().deliverRecord(tp0, 0L, "ex", "rk", "{}", false);
        ctx.fetchLoop().deliverRecord(tp0, 1L, "ex", "rk", "{}", false);
        ctx.deliveryTagTracker().ack(1L);
        ctx.deliveryTagTracker().ack(2L);

        Map<TopicPartition, Long> offsets = manager.unsubscribe("sub-1");
        assertEquals(2L, offsets.get(tp0), "next commit offset should be 2 (offset+1 of last acked 1)");
    }

    @Test
    void unsubscribe_afterUnsubscribe_idempotent() {
        manager.subscribe("sub-1", "orders", "ws.orders",
            Set.of(tp0), Map.of(tp0, 0L), 10, false, channel);
        manager.unsubscribe("sub-1");
        // Second call should be safe and return empty.
        assertTrue(manager.unsubscribe("sub-1").isEmpty());
    }

    // ---- grantCredits ----

    @Test
    void grantCredits_forwardsToCreditManager() {
        manager.subscribe("sub-1", "orders", "ws.orders",
            Set.of(tp0), Map.of(tp0, 0L), 10, false, channel);
        manager.grantCredits("sub-1", 50);
        assertEquals(60, manager.getSubscription("sub-1").creditManager().available());
    }

    @Test
    void grantCredits_unknownSubscription_noOp() {
        // No exception
        manager.grantCredits("nonexistent", 5);
        assertEquals(0, manager.activeCount());
    }

    // ---- cancelAll ----

    @Test
    @Timeout(5)
    void cancelAll_stopsAllSubscriptions() throws InterruptedException {
        manager.subscribe("sub-1", "orders", "ws.orders",
            Set.of(tp0), Map.of(tp0, 0L), 100, false, channel);
        manager.subscribe("sub-2", "events", "ws.events",
            Set.of(tp1), Map.of(tp1, 0L), 50, false, channel);

        SubscriptionContext a = manager.getSubscription("sub-1");
        SubscriptionContext b = manager.getSubscription("sub-2");

        manager.cancelAll();
        Thread.sleep(100);

        assertEquals(0, manager.activeCount());
        assertFalse(a.fetchLoop().isActive());
        assertFalse(b.fetchLoop().isActive());
    }

    @Test
    void cancelAll_returnsCommittableOffsetsForAll() {
        manager.subscribe("sub-1", "orders", "ws.orders",
            Set.of(tp0), Map.of(tp0, 0L), 10, false, channel);
        manager.subscribe("sub-2", "events", "ws.events",
            Set.of(tp1), Map.of(tp1, 0L), 10, false, channel);

        SubscriptionContext a = manager.getSubscription("sub-1");
        SubscriptionContext b = manager.getSubscription("sub-2");

        a.fetchLoop().deliverRecord(tp0, 3L, "ex", "rk", "{}", false);
        a.deliveryTagTracker().ack(1L);
        b.fetchLoop().deliverRecord(tp1, 7L, "ex", "rk", "{}", false);
        b.deliveryTagTracker().ack(1L);

        Map<TopicPartition, Long> all = manager.cancelAll();
        assertEquals(4L, all.get(tp0));
        assertEquals(8L, all.get(tp1));
    }

    @Test
    void cancelAll_emptyManager_returnsEmptyMap() {
        assertTrue(manager.cancelAll().isEmpty());
    }

    // ---- Concurrency ----

    @Test
    @Timeout(10)
    void concurrent_subscribeUnsubscribe_maintainsConsistency() throws Exception {
        int threads = 8;
        int perThread = 20;
        ExecutorService workers = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);

        try {
            for (int t = 0; t < threads; t++) {
                final int tid = t;
                workers.submit(() -> {
                    try {
                        start.await();
                        for (int i = 0; i < perThread; i++) {
                            String id = "sub-" + tid + "-" + i;
                            manager.subscribe(id, "q", "ws.q",
                                Set.of(tp0), Map.of(tp0, 0L), 10, false, channel);
                            manager.grantCredits(id, 5);
                            manager.unsubscribe(id);
                        }
                    } catch (Exception ignored) {
                    } finally {
                        done.countDown();
                    }
                });
            }
            start.countDown();
            assertTrue(done.await(5, TimeUnit.SECONDS), "workers should complete");
            assertEquals(0, manager.activeCount(), "all subscriptions should be unsubscribed");
        } finally {
            workers.shutdownNow();
            workers.awaitTermination(2, TimeUnit.SECONDS);
        }
    }

    // ---- TASK-WS3.03: group coordinator wiring ----

    @Test
    void constructor_withGroupCoordinator_storesReference() {
        WsConsumerGroupCoordinator coord = new WsConsumerGroupCoordinator();
        WsSubscriptionManager mgr = new WsSubscriptionManager(executor, coord);
        assertNotNull(mgr.groupCoordinator());
    }

    @Test
    void constructor_withoutGroupCoordinator_returnsNull() {
        assertNull(manager.groupCoordinator());
    }

    @Test
    void invalidateRevokedPartitions_dropsTagsForAffectedSubscription() {
        manager.subscribe("sub-1", "orders", "ws.orders",
            Set.of(tp0), Map.of(tp0, 0L), 10, false, channel);
        SubscriptionContext ctx = manager.getSubscription("sub-1");

        ctx.fetchLoop().deliverRecord(tp0, 0L, "ex", "rk", "{}", false);
        ctx.fetchLoop().deliverRecord(tp0, 1L, "ex", "rk", "{}", false);
        assertEquals(2, ctx.deliveryTagTracker().pendingCount());

        int dropped = manager.invalidateRevokedPartitions("sub-1", Set.of(tp0));
        assertEquals(2, dropped);
        assertEquals(0, ctx.deliveryTagTracker().pendingCount());
    }

    @Test
    void invalidateRevokedPartitions_unknownSubscription_returnsZero() {
        assertEquals(0, manager.invalidateRevokedPartitions("nope", Set.of(tp0)));
    }

    @Test
    void invalidateRevokedPartitions_emptyOrNull_returnsZero() {
        manager.subscribe("sub-1", "orders", "ws.orders",
            Set.of(tp0), Map.of(tp0, 0L), 10, false, channel);
        assertEquals(0, manager.invalidateRevokedPartitions("sub-1", null));
        assertEquals(0, manager.invalidateRevokedPartitions("sub-1", Set.of()));
    }

    @Test
    void activeCount_reflectsCurrent() {
        assertEquals(0, manager.activeCount());
        manager.subscribe("sub-1", "q", "ws.q",
            Set.of(tp0), Map.of(tp0, 0L), 10, false, channel);
        assertEquals(1, manager.activeCount());
        manager.subscribe("sub-2", "q", "ws.q",
            new HashSet<>(Set.of(tp1)), new HashMap<>(Map.of(tp1, 0L)), 10, false, channel);
        assertEquals(2, manager.activeCount());
        manager.unsubscribe("sub-1");
        assertEquals(1, manager.activeCount());
    }
}
