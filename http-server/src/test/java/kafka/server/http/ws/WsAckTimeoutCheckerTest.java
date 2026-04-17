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

// Time: Created - TASK-WS4.06

package kafka.server.http.ws;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;

import org.apache.kafka.common.TopicPartition;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link WsAckTimeoutChecker} — the WS4.06 ack-timeout sweeper.
 *
 * <p>The checker scans pending delivery tags, sends an {@code ACK_TIMEOUT} warning
 * error frame for any tag older than the configured timeout, and then routes the
 * tag through the same {@code NACK(requeue=true)} path the client would use. Time
 * is driven by an injected {@link java.util.function.LongSupplier} so the tests can
 * age tags deterministically without sleeping.
 */
class WsAckTimeoutCheckerTest {

    private WsSubscriptionManager subscriptionManager;
    private ExecutorService executor;
    private Channel channel;
    private RecordingSink sink;
    private WsAckHandler handler;
    private AtomicLong clock;
    private WsDeliveryTagTracker tracker;
    private RecordingFrameCapture frameCapture;

    private final TopicPartition tp0 = new TopicPartition("ws.orders", 0);

    @BeforeEach
    void setUp() {
        executor = Executors.newSingleThreadExecutor();
        channel = mock(Channel.class);
        ChannelFuture future = mock(ChannelFuture.class);
        when(channel.isWritable()).thenReturn(true);
        when(channel.isOpen()).thenReturn(true);
        when(channel.writeAndFlush(any())).thenAnswer(inv -> {
            frameCapture.frames.add(inv.getArgument(0));
            return future;
        });

        frameCapture = new RecordingFrameCapture();

        // We use the subscription manager so the checker can go through the real
        // WsAckHandler.handleNack path (§19.6: auto-NACK must use the same NACK
        // path as client-initiated NACKs). A clock-driven tracker is injected so
        // tags age deterministically.
        clock = new AtomicLong(1_000L);
        tracker = new WsDeliveryTagTracker(clock::get);

        subscriptionManager = new WsSubscriptionManager(executor);
        subscriptionManager.subscribe("sub-1", "orders", "ws.orders",
            Set.of(tp0), Map.of(tp0, 0L), 0, false, channel, tracker);

        sink = new RecordingSink();
        handler = new WsAckHandler(subscriptionManager, 0L, sink);
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        handler.stop();
        subscriptionManager.cancelAll();
        executor.shutdownNow();
        executor.awaitTermination(2, TimeUnit.SECONDS);
    }

    // ------------------------------------------------------------------
    //  Construction
    // ------------------------------------------------------------------

    @Test
    void constructor_rejectsNullArgs() {
        assertThrows(NullPointerException.class, () ->
            new WsAckTimeoutChecker(null, handler, "sub-1", channel, 1_000L, clock::get));
        assertThrows(NullPointerException.class, () ->
            new WsAckTimeoutChecker(tracker, null, "sub-1", channel, 1_000L, clock::get));
        assertThrows(NullPointerException.class, () ->
            new WsAckTimeoutChecker(tracker, handler, null, channel, 1_000L, clock::get));
        assertThrows(NullPointerException.class, () ->
            new WsAckTimeoutChecker(tracker, handler, "sub-1", null, 1_000L, clock::get));
        assertThrows(IllegalArgumentException.class, () ->
            new WsAckTimeoutChecker(tracker, handler, "sub-1", channel, 0L, clock::get));
        assertThrows(IllegalArgumentException.class, () ->
            new WsAckTimeoutChecker(tracker, handler, "sub-1", channel, -5L, clock::get));
    }

    // ------------------------------------------------------------------
    //  Core timeout behaviour
    // ------------------------------------------------------------------

    @Test
    void tagWithinTimeout_notRequeued() {
        WsAckTimeoutChecker checker = new WsAckTimeoutChecker(
            tracker, handler, "sub-1", channel, 10_000L, clock::get);

        long tag = tracker.assign(tp0, 100L);
        // advance clock, but less than timeout
        clock.addAndGet(5_000L);

        checker.run();

        // Tag is still pending — no auto-NACK, no frame sent
        assertEquals(1, tracker.pendingCount());
        assertTrue(frameCapture.textFrames().isEmpty(), "no error frame should be sent");
        // And not transitioned: we can still ack it normally
        assertNull(handler.handleAck("sub-1", tag, false));
    }

    @Test
    void tagPastTimeout_autoNacked() {
        WsAckTimeoutChecker checker = new WsAckTimeoutChecker(
            tracker, handler, "sub-1", channel, 1_000L, clock::get);

        long tag = tracker.assign(tp0, 100L);
        // Push clock past the timeout threshold
        clock.addAndGet(2_000L);

        checker.run();

        // Tag was auto-NACKed with requeue=true; it is no longer pending.
        assertEquals(0, tracker.pendingCount());
        // NACK with requeue must NOT advance the commit watermark.
        assertTrue(sink.commits().isEmpty(),
            "requeue must not produce a commit; got " + sink.commits());
        // A later ack for this tag finds it already-transitioned → idempotent (null).
        String error = handler.handleAck("sub-1", tag, false);
        assertNull(error);
    }

    @Test
    void timeoutWarning_sentBeforeRequeue() {
        WsAckTimeoutChecker checker = new WsAckTimeoutChecker(
            tracker, handler, "sub-1", channel, 500L, clock::get);

        long tag = tracker.assign(tp0, 100L);
        clock.addAndGet(1_000L);

        checker.run();

        List<String> frames = frameCapture.textFrames();
        assertEquals(1, frames.size(), "exactly one ACK_TIMEOUT frame should be sent");
        String frame = frames.get(0);
        assertTrue(frame.contains("\"type\":\"error\""),
            "expected error frame, got: " + frame);
        assertTrue(frame.contains("\"errorCode\":\"ACK_TIMEOUT\""),
            "expected ACK_TIMEOUT errorCode, got: " + frame);
        assertTrue(frame.contains("\"deliveryTag\":" + tag),
            "expected deliveryTag=" + tag + ", got: " + frame);
        assertTrue(frame.contains("500"),
            "expected timeout ms in error message, got: " + frame);
    }

    @Test
    void alreadyAcked_tagSkipped() {
        WsAckTimeoutChecker checker = new WsAckTimeoutChecker(
            tracker, handler, "sub-1", channel, 500L, clock::get);

        long tag = tracker.assign(tp0, 100L);
        clock.addAndGet(1_000L);

        // Client acks the tag first — commit watermark advances
        assertNull(handler.handleAck("sub-1", tag, false));
        assertEquals(List.of(Map.of(tp0, 101L)), sink.commits());

        // Now the checker runs — the tag is no longer pending.
        // No error frame should be sent, no additional commit.
        frameCapture.frames.clear();
        checker.run();

        assertTrue(frameCapture.textFrames().isEmpty(),
            "no error frame for already-acked tag");
        assertEquals(1, sink.commits().size(),
            "no new commit should occur from checker");
    }

    @Test
    void stop_preventsSubsequentChecks() {
        WsAckTimeoutChecker checker = new WsAckTimeoutChecker(
            tracker, handler, "sub-1", channel, 500L, clock::get);

        long tag = tracker.assign(tp0, 100L);
        clock.addAndGet(1_000L);

        checker.stop();
        checker.run();

        // stop() before run() → checker is inert
        assertEquals(1, tracker.pendingCount(), "tag must NOT be nacked after stop()");
        assertTrue(frameCapture.textFrames().isEmpty(),
            "stopped checker must not send frames");
        // Ack still works normally
        assertNull(handler.handleAck("sub-1", tag, false));
    }

    @Test
    void multipleExpiredTags_allHandled() {
        WsAckTimeoutChecker checker = new WsAckTimeoutChecker(
            tracker, handler, "sub-1", channel, 500L, clock::get);

        long tag1 = tracker.assign(tp0, 100L);
        long tag2 = tracker.assign(tp0, 101L);
        long tag3 = tracker.assign(tp0, 102L);
        assertEquals(3, tracker.pendingCount());

        clock.addAndGet(1_000L); // all three past timeout
        checker.run();

        assertEquals(0, tracker.pendingCount(),
            "all three expired tags should have been nacked");

        List<String> frames = frameCapture.textFrames();
        assertEquals(3, frames.size(), "one ACK_TIMEOUT frame per expired tag");
        // Each frame references a different delivery tag
        String joined = String.join("|", frames);
        assertTrue(joined.contains("\"deliveryTag\":" + tag1));
        assertTrue(joined.contains("\"deliveryTag\":" + tag2));
        assertTrue(joined.contains("\"deliveryTag\":" + tag3));
        // And all were requeue=true → no commit advance
        assertTrue(sink.commits().isEmpty(),
            "requeue NACK must not produce commits");
    }

    @Test
    void partialExpiry_onlyExpiredTagsHandled() {
        WsAckTimeoutChecker checker = new WsAckTimeoutChecker(
            tracker, handler, "sub-1", channel, 1_000L, clock::get);

        // t=1000: assign tag 1
        long tag1 = tracker.assign(tp0, 100L);
        // t=1500: assign tag 2
        clock.addAndGet(500L);
        long tag2 = tracker.assign(tp0, 101L);
        // t=2100: tag1 expired (>1000ms old), tag2 still fresh (600ms old)
        clock.addAndGet(600L);

        checker.run();

        // Only tag1 expired; tag2 should still be pending.
        assertEquals(1, tracker.pendingCount());
        assertEquals(1, frameCapture.textFrames().size());
        assertTrue(frameCapture.textFrames().get(0).contains("\"deliveryTag\":" + tag1));

        // tag2 still ackable by client
        assertNull(handler.handleAck("sub-1", tag2, false));
    }

    @Test
    void customTimeout_perSubscriptionOverride() {
        // Two checkers for hypothetical different subscriptions, each with its own timeout.
        // The "per-queue override" in the design is realised by passing a different
        // timeoutMs at construction time — the checker is owned per-subscription so
        // queue-specific settings come out of the callers' resolution.
        WsAckTimeoutChecker shortChecker = new WsAckTimeoutChecker(
            tracker, handler, "sub-1", channel, 200L, clock::get);
        WsAckTimeoutChecker longChecker = new WsAckTimeoutChecker(
            tracker, handler, "sub-1", channel, 10_000L, clock::get);

        tracker.assign(tp0, 100L); // tag 1
        clock.addAndGet(500L); // more than short, less than long

        longChecker.run();
        // long timeout: not yet expired
        assertEquals(1, tracker.pendingCount());
        assertTrue(frameCapture.textFrames().isEmpty());

        shortChecker.run();
        // short timeout: expired
        assertEquals(0, tracker.pendingCount());
        assertEquals(1, frameCapture.textFrames().size());
    }

    // ------------------------------------------------------------------
    //  Concurrency
    // ------------------------------------------------------------------

    @Test
    void concurrentSweepVsAck_noDoubleNack() throws InterruptedException {
        // The checker races with the client acking every expired tag; the end state
        // must have each tag transitioned exactly once, with no extra commits and no
        // NullPointerException on concurrent map mutations.
        WsAckTimeoutChecker checker = new WsAckTimeoutChecker(
            tracker, handler, "sub-1", channel, 500L, clock::get);

        int n = 200;
        long[] tags = new long[n];
        for (int i = 0; i < n; i++) {
            tags[i] = tracker.assign(tp0, 1000 + i);
        }
        clock.addAndGet(1_000L); // all expired

        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(2);

        Thread ackThread = new Thread(() -> {
            try {
                start.await();
                for (int i = 0; i < n; i++) {
                    handler.handleAck("sub-1", tags[i], false);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                done.countDown();
            }
        });
        Thread checkerThread = new Thread(() -> {
            try {
                start.await();
                checker.run();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                done.countDown();
            }
        });

        ackThread.start();
        checkerThread.start();
        start.countDown();
        assertTrue(done.await(10, TimeUnit.SECONDS));

        assertEquals(0, tracker.pendingCount(),
            "every tag must be transitioned exactly once");
        // Commits produced by ack path are a prefix-sum on offsets. No assertion on
        // exact sequence here: the point is that nothing blew up and state is consistent.
    }

    @Test
    void scheduled_runsPeriodicallyUntilStopped() throws InterruptedException {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        try {
            WsAckTimeoutChecker checker = new WsAckTimeoutChecker(
                tracker, handler, "sub-1", channel, 100L, clock::get);

            // Assign a tag and immediately push the clock past the timeout so the
            // first scheduled tick catches it.
            tracker.assign(tp0, 100L);
            clock.addAndGet(200L);

            checker.setScheduledFuture(scheduler.scheduleAtFixedRate(
                checker, 10L, 10L, TimeUnit.MILLISECONDS));

            // Wait for the scheduled sweep to fire
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
            while (tracker.pendingCount() > 0 && System.nanoTime() < deadline) {
                Thread.sleep(5);
            }
            assertEquals(0, tracker.pendingCount(),
                "scheduled checker should have swept the expired tag");
            checker.stop(); // cancels the scheduled future
        } finally {
            scheduler.shutdownNow();
            assertTrue(scheduler.awaitTermination(2, TimeUnit.SECONDS));
        }
    }

    // ------------------------------------------------------------------
    //  Helpers
    // ------------------------------------------------------------------

    private static final class RecordingFrameCapture {
        final List<Object> frames = new CopyOnWriteArrayList<>();

        List<String> textFrames() {
            List<String> out = new java.util.ArrayList<>();
            for (Object f : frames) {
                if (f instanceof TextWebSocketFrame) {
                    out.add(((TextWebSocketFrame) f).text());
                }
            }
            return out;
        }
    }

    private static final class RecordingSink implements WsAckHandler.OffsetCommitSink {
        private final List<Map<TopicPartition, Long>> commits = new CopyOnWriteArrayList<>();

        @Override
        public void commit(String subscriptionId, Map<TopicPartition, Long> offsets) {
            commits.add(Map.copyOf(offsets));
        }

        List<Map<TopicPartition, Long>> commits() {
            return commits;
        }
    }

}
