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

// Time: Created - TASK-WS1.16
// Time: Update - TASK-WS4.01 - added DeadLetterHook integration tests

package kafka.server.http.ws;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;

import org.apache.kafka.common.TopicPartition;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
 * Unit tests for {@link WsAckHandler} — ACK/NACK handling and offset commit batching.
 *
 * <p>The production class wires {@link WsSubscriptionManager} (for tag tracker lookup) to an
 * injected {@link WsAckHandler.OffsetCommitSink} that stands in for the eventual
 * {@code OffsetCommit} RequestChannel integration. Tests therefore capture sink
 * invocations in-memory instead of asserting on the real commit pipeline.
 */
class WsAckHandlerTest {

    private WsSubscriptionManager subscriptionManager;
    private ExecutorService executor;
    private Channel channel;
    private RecordingSink sink;
    private WsAckHandler handler;

    private final TopicPartition tp0 = new TopicPartition("ws.orders", 0);

    @BeforeEach
    void setUp() {
        executor = Executors.newSingleThreadExecutor();
        channel = mock(Channel.class);
        ChannelFuture future = mock(ChannelFuture.class);
        when(channel.isWritable()).thenReturn(true);
        when(channel.isOpen()).thenReturn(true);
        when(channel.writeAndFlush(any())).thenReturn(future);

        subscriptionManager = new WsSubscriptionManager(executor);
        subscriptionManager.subscribe("sub-1", "orders", "ws.orders",
            Set.of(tp0), Map.of(tp0, 0L), 0, false, channel);

        sink = new RecordingSink();
        // commitIntervalMs = 0 → flush-on-each-ack mode; easier to test without relying on
        // scheduled executor timing in individual cases.
        handler = new WsAckHandler(subscriptionManager, 0L, sink);
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        handler.stop();
        subscriptionManager.cancelAll();
        executor.shutdownNow();
        executor.awaitTermination(2, TimeUnit.SECONDS);
    }

    // ---- Construction ----

    @Test
    void constructor_rejectsNullArgs() {
        assertThrows(NullPointerException.class, () -> new WsAckHandler(null, 0L, sink));
        assertThrows(NullPointerException.class, () -> new WsAckHandler(subscriptionManager, 0L, null));
    }

    // ---- handleAck ----

    @Test
    void handleAck_singleTag_succeedsAndEmitsCommit() {
        SubscriptionContext ctx = subscriptionManager.getSubscription("sub-1");
        long tag = ctx.deliveryTagTracker().assign(tp0, 100L);

        String error = handler.handleAck("sub-1", tag, false);

        assertNull(error);
        // committable offset = offset + 1 per Kafka convention
        assertEquals(List.of(Map.of(tp0, 101L)), sink.commits());
    }

    @Test
    void handleAck_unknownSubscription_returnsError() {
        String error = handler.handleAck("nonexistent", 1L, false);

        assertNotNull(error);
        assertTrue(error.contains("PRECONDITION_FAILED"),
            "expected PRECONDITION_FAILED, got: " + error);
        assertTrue(sink.commits().isEmpty(), "no commit must happen for unknown sub");
    }

    @Test
    void handleAck_unknownTag_returnsPreconditionFailed() {
        // Subscription exists but no tag has been assigned for tag=42.
        String error = handler.handleAck("sub-1", 42L, false);

        assertNotNull(error);
        assertTrue(error.contains("PRECONDITION_FAILED"),
            "expected PRECONDITION_FAILED, got: " + error);
        assertTrue(sink.commits().isEmpty(), "no commit for unknown tag");
    }

    @Test
    void handleAck_doubleAck_isIdempotentNoExtraCommit() {
        SubscriptionContext ctx = subscriptionManager.getSubscription("sub-1");
        long tag = ctx.deliveryTagTracker().assign(tp0, 100L);

        assertNull(handler.handleAck("sub-1", tag, false));
        // Second ack of same tag: the tracker returns false (already consumed), no new commit.
        String second = handler.handleAck("sub-1", tag, false);
        // Double-ack is documented as idempotent-success (§5.9). An error is acceptable only
        // if it is NOT raised as PRECONDITION_FAILED to the client for the first normal ack.
        // Implementation choice: return null (success-noop) to keep client state simple.
        assertNull(second);
        assertEquals(1, sink.commits().size(), "second ack must not emit another commit");
    }

    @Test
    void handleAck_multipleTrue_acksRangeAndEmitsWatermark() {
        SubscriptionContext ctx = subscriptionManager.getSubscription("sub-1");
        long t1 = ctx.deliveryTagTracker().assign(tp0, 100L);
        long t2 = ctx.deliveryTagTracker().assign(tp0, 101L);
        long t3 = ctx.deliveryTagTracker().assign(tp0, 102L);
        assertEquals(1L, t1);
        assertEquals(2L, t2);
        assertEquals(3L, t3);

        String error = handler.handleAck("sub-1", 2L, true);

        assertNull(error);
        // After multiple=2: offsets 100 and 101 are acked → watermark 102.
        assertEquals(List.of(Map.of(tp0, 102L)), sink.commits());
        assertEquals(1, ctx.deliveryTagTracker().pendingCount());
    }

    @Test
    void handleAck_afterUnsubscribe_isNoop() {
        SubscriptionContext ctx = subscriptionManager.getSubscription("sub-1");
        long tag = ctx.deliveryTagTracker().assign(tp0, 100L);
        subscriptionManager.unsubscribe("sub-1");

        String error = handler.handleAck("sub-1", tag, false);

        // After unsubscribe, subscription is gone → PRECONDITION_FAILED.
        assertNotNull(error);
        assertTrue(error.contains("PRECONDITION_FAILED"));
        assertTrue(sink.commits().isEmpty());
    }

    // ---- handleNack ----

    @Test
    void handleNack_withRequeue_triggersRedeliveryNoCommit() {
        SubscriptionContext ctx = subscriptionManager.getSubscription("sub-1");
        long tag = ctx.deliveryTagTracker().assign(tp0, 100L);

        String error = handler.handleNack("sub-1", tag, true, false);

        assertNull(error);
        // NACK+requeue must NOT advance commit watermark.
        assertTrue(sink.commits().isEmpty(),
            "requeue must not produce a commit; got " + sink.commits());
    }

    @Test
    void handleNack_withoutRequeue_allowsCommit() {
        SubscriptionContext ctx = subscriptionManager.getSubscription("sub-1");
        long tag = ctx.deliveryTagTracker().assign(tp0, 100L);

        String error = handler.handleNack("sub-1", tag, false, false);

        assertNull(error);
        // NACK+discard (DLX path is deferred to WS4.01) allows commit to advance.
        assertEquals(List.of(Map.of(tp0, 101L)), sink.commits());
    }

    @Test
    void handleNack_unknownSubscription_returnsError() {
        String error = handler.handleNack("nonexistent", 1L, true, false);

        assertNotNull(error);
        assertTrue(error.contains("PRECONDITION_FAILED"));
    }

    @Test
    void handleNack_unknownTag_returnsPreconditionFailed() {
        String error = handler.handleNack("sub-1", 42L, true, false);

        assertNotNull(error);
        assertTrue(error.contains("PRECONDITION_FAILED"));
    }

    // ---- DLX hook integration (TASK-WS4.01) ----

    @Test
    void handleNack_noRequeue_withDlxHook_holdsCommitUntilHookCompletes() throws Exception {
        // Hook future left in-flight: commit watermark MUST NOT advance.
        java.util.concurrent.CompletableFuture<Void> hookFuture =
            new java.util.concurrent.CompletableFuture<>();
        RecordingHook hook = new RecordingHook(hookFuture);
        rebuildHandlerWithHook(hook);

        SubscriptionContext ctx = subscriptionManager.getSubscription("sub-1");
        long tag = ctx.deliveryTagTracker().assign(tp0, 500L);

        String error = handler.handleNack("sub-1", tag, false, false);

        assertNull(error);
        assertEquals(1, hook.calls.size(), "hook called exactly once");
        assertEquals("orders", hook.calls.get(0).queueName);
        assertEquals(tp0, hook.calls.get(0).pd.topicPartition());
        assertEquals(500L, hook.calls.get(0).pd.offset());
        // Hook in-flight ⇒ tracker tag still PENDING ⇒ no commit emitted.
        assertTrue(sink.commits().isEmpty(),
            "commit must not advance while DLX hook is pending; got " + sink.commits());

        // Now complete the hook successfully — commit should advance.
        hookFuture.complete(null);
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (sink.commits().isEmpty() && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertEquals(List.of(Map.of(tp0, 501L)), sink.commits(),
            "commit watermark must advance after DLX produce completes");
    }

    @Test
    void handleNack_noRequeue_withDlxHookFailure_keepsOffsetUncommitted() throws Exception {
        // Hook future fails: tag must transition to NACKED_REQUEUE so the offset stays put.
        java.util.concurrent.CompletableFuture<Void> hookFuture =
            new java.util.concurrent.CompletableFuture<>();
        RecordingHook hook = new RecordingHook(hookFuture);
        rebuildHandlerWithHook(hook);

        SubscriptionContext ctx = subscriptionManager.getSubscription("sub-1");
        long tag = ctx.deliveryTagTracker().assign(tp0, 500L);

        assertNull(handler.handleNack("sub-1", tag, false, false));
        assertTrue(sink.commits().isEmpty());

        hookFuture.completeExceptionally(new RuntimeException("kafka down"));
        // Give the async whenComplete a moment.
        Thread.sleep(100);
        // Tracker is now NACKED_REQUEUE → no commit.
        assertTrue(sink.commits().isEmpty(),
            "DLX failure must NOT advance commit; got " + sink.commits());
    }

    @Test
    void handleNack_noRequeue_unknownTag_doesNotInvokeDlxHook() {
        RecordingHook hook = new RecordingHook(java.util.concurrent.CompletableFuture.completedFuture(null));
        rebuildHandlerWithHook(hook);

        String error = handler.handleNack("sub-1", 42L, false, false);

        assertNotNull(error);
        assertTrue(error.contains("PRECONDITION_FAILED"));
        assertTrue(hook.calls.isEmpty(), "hook must not be called for unknown tag");
    }

    @Test
    void handleNack_withRequeue_withDlxHook_doesNotInvokeHook() {
        // requeue=true bypasses DLX entirely.
        RecordingHook hook = new RecordingHook(java.util.concurrent.CompletableFuture.completedFuture(null));
        rebuildHandlerWithHook(hook);

        SubscriptionContext ctx = subscriptionManager.getSubscription("sub-1");
        long tag = ctx.deliveryTagTracker().assign(tp0, 100L);

        assertNull(handler.handleNack("sub-1", tag, true, false));
        assertTrue(hook.calls.isEmpty(), "requeue path must not call DLX hook");
        assertTrue(sink.commits().isEmpty(), "requeue must not commit");
    }

    @Test
    void handleNack_noRequeue_withSyncHookThrow_keepsOffsetUncommitted() throws Exception {
        // Hook throws synchronously: must be treated as DLX failure (requeue).
        WsAckHandler.DeadLetterHook throwingHook = (q, pd) -> {
            throw new RuntimeException("boom");
        };
        rebuildHandlerWithHook(throwingHook);

        SubscriptionContext ctx = subscriptionManager.getSubscription("sub-1");
        long tag = ctx.deliveryTagTracker().assign(tp0, 100L);

        assertNull(handler.handleNack("sub-1", tag, false, false));
        Thread.sleep(50);
        assertTrue(sink.commits().isEmpty(),
            "sync hook throw is treated as DLX failure → no commit; got " + sink.commits());
    }

    private void rebuildHandlerWithHook(WsAckHandler.DeadLetterHook hook) {
        handler.stop();
        sink.clear();
        handler = new WsAckHandler(subscriptionManager, 0L, sink, null, hook);
    }

    /** Captures hook invocations and replays a caller-supplied future. */
    private static final class RecordingHook implements WsAckHandler.DeadLetterHook {
        record Call(String queueName, WsDeliveryTagTracker.PendingDelivery pd) { }
        final List<Call> calls = new CopyOnWriteArrayList<>();
        private final java.util.concurrent.CompletableFuture<Void> future;
        RecordingHook(java.util.concurrent.CompletableFuture<Void> future) {
            this.future = future;
        }
        @Override
        public java.util.concurrent.CompletableFuture<Void> deadLetter(
                String queueName, WsDeliveryTagTracker.PendingDelivery pd) {
            calls.add(new Call(queueName, pd));
            return future;
        }
    }

    // ---- Batched commit timer ----

    @Test
    @Timeout(5)
    void batchedCommit_flushesOnInterval() throws Exception {
        // Rebuild handler with positive commit interval so commits are deferred.
        handler.stop();
        sink.clear();
        handler = new WsAckHandler(subscriptionManager, 50L, sink);
        handler.start();

        SubscriptionContext ctx = subscriptionManager.getSubscription("sub-1");
        long tag = ctx.deliveryTagTracker().assign(tp0, 200L);

        String error = handler.handleAck("sub-1", tag, false);
        assertNull(error);

        // With batched mode, no immediate commit expected.
        assertTrue(sink.commits().isEmpty(),
            "commit should be deferred to the scheduled flush");

        // Wait up to 2 seconds for the scheduled flusher to fire.
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (sink.commits().isEmpty() && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        assertFalse(sink.commits().isEmpty(),
            "expected periodic commit within 2s");
        assertEquals(Map.of(tp0, 201L), sink.commits().get(0));
    }

    @Test
    void startAndStop_areIdempotent() {
        WsAckHandler h = new WsAckHandler(subscriptionManager, 100L, new RecordingSink());
        h.start();
        h.start();  // second start is a no-op
        h.stop();
        h.stop();   // second stop is a no-op
    }

    @Test
    void stop_flushesPendingCommits() throws InterruptedException {
        handler.stop();
        sink.clear();

        // Batched mode so ack doesn't commit immediately.
        WsAckHandler batched = new WsAckHandler(subscriptionManager, 5_000L, sink);
        batched.start();

        SubscriptionContext ctx = subscriptionManager.getSubscription("sub-1");
        long tag = ctx.deliveryTagTracker().assign(tp0, 300L);
        assertNull(batched.handleAck("sub-1", tag, false));
        assertTrue(sink.commits().isEmpty(), "deferred, so no commit yet");

        // stop() MUST flush remaining committable offsets.
        batched.stop();
        assertEquals(1, sink.commits().size());
        assertEquals(Map.of(tp0, 301L), sink.commits().get(0));
    }

    // ---- Concurrency ----

    @Test
    @Timeout(10)
    void concurrentAcks_doNotCorruptCommitState() throws Exception {
        // Pre-assign many tags, ack them concurrently, and assert the final commit watermark.
        int n = 500;
        SubscriptionContext ctx = subscriptionManager.getSubscription("sub-1");
        long[] tags = new long[n];
        for (int i = 0; i < n; i++) {
            tags[i] = ctx.deliveryTagTracker().assign(tp0, i);
        }

        int threads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger index = new AtomicInteger(0);
        try {
            for (int t = 0; t < threads; t++) {
                pool.submit(() -> {
                    try {
                        start.await();
                        int i;
                        while ((i = index.getAndIncrement()) < n) {
                            handler.handleAck("sub-1", tags[i], false);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
            }
            start.countDown();
        } finally {
            pool.shutdown();
            assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));
        }

        // All tags acked → watermark should be offset n (last offset was n-1, commit = n).
        // Multiple commits may have been emitted, but the final watermark must be n.
        List<Map<TopicPartition, Long>> all = sink.commits();
        assertFalse(all.isEmpty());
        long finalOffset = all.stream()
            .map(m -> m.getOrDefault(tp0, -1L))
            .max(Long::compare)
            .orElse(-1L);
        assertEquals((long) n, finalOffset);
    }

    /**
     * In-memory recorder for {@link WsAckHandler.OffsetCommitSink} invocations.
     */
    private static final class RecordingSink implements WsAckHandler.OffsetCommitSink {
        private final List<Map<TopicPartition, Long>> calls = new CopyOnWriteArrayList<>();

        @Override
        public void commit(String subscriptionId, Map<TopicPartition, Long> offsets) {
            // Copy defensively so the caller is free to mutate / reuse the map.
            calls.add(Map.copyOf(offsets));
        }

        List<Map<TopicPartition, Long>> commits() {
            return new ArrayList<>(calls);
        }

        void clear() {
            calls.clear();
        }
    }
}
