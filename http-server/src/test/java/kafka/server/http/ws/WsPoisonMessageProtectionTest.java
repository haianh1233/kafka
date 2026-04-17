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

// Time: Created - TASK-WS4.04

package kafka.server.http.ws;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link WsPoisonMessageProtection} — per-message delivery count
 * tracking and auto-dead-letter when redelivery exceeds the configured
 * {@code ws.max.redelivery.count} threshold (or the per-queue {@code x-max-retries}
 * override).
 *
 * <p>The delivery count is persisted on each Kafka record as the
 * {@code _ws_delivery_count} header. First delivery carries count {@code 1}; the
 * count is incremented immediately before each redelivery. Once the count equals
 * or exceeds the threshold, a NACK with {@code requeue=true} is routed to the
 * DLX with {@code reason="max-retries-exceeded"} instead of being requeued —
 * this bounds the redelivery loop and prevents a single poison payload from
 * wedging the queue forever (design doc §12.6).
 */
class WsPoisonMessageProtectionTest {

    private Map<String, QueueMetadata> queues;
    private WsPoisonMessageProtection poison;

    @BeforeEach
    void setUp() {
        queues = new HashMap<>();
        poison = new WsPoisonMessageProtection(
            (vhost, queueName) -> queues.get(queueKey(vhost, queueName)),
            /* defaultMaxRetries */ 3);
    }

    // ---- Header helpers ----

    @Test
    void firstDelivery_countIsOne() {
        // A record with no prior _ws_delivery_count header represents the first delivery.
        // readDeliveryCount returns 0 (no prior hops); incrementDeliveryCount stamps 1.
        Headers empty = new RecordHeaders();
        assertEquals(0, WsPoisonMessageProtection.readDeliveryCount(empty));

        List<Header> incremented = WsPoisonMessageProtection.incrementDeliveryCount(empty);
        assertEquals(1, readCountFromList(incremented));
    }

    @Test
    void deliveryCount_incrementsOnRedelivery() {
        // Header already carries count=1 → next delivery becomes 2.
        Headers headers = new RecordHeaders();
        headers.add(WsMessageSerializer.HDR_DELIVERY_COUNT,
            "1".getBytes(StandardCharsets.UTF_8));
        assertEquals(1, WsPoisonMessageProtection.readDeliveryCount(headers));

        List<Header> next = WsPoisonMessageProtection.incrementDeliveryCount(headers);
        assertEquals(2, readCountFromList(next));
    }

    @Test
    void deliveryCountHeader_persistsAcrossRedeliveries() {
        // Increment N times and verify the count monotonically rises.
        Headers headers = new RecordHeaders();
        int count = 0;
        for (int i = 1; i <= 5; i++) {
            List<Header> next = WsPoisonMessageProtection.incrementDeliveryCount(headers);
            count = readCountFromList(next);
            assertEquals(i, count);
            // Rebuild headers for next iteration — simulates what the broker would
            // persist and re-read on each fetch.
            headers = new RecordHeaders();
            for (Header h : next) {
                headers.add(h);
            }
        }
        assertEquals(5, count);
    }

    @Test
    void incrementDeliveryCount_preservesOtherHeaders() {
        Headers headers = new RecordHeaders();
        headers.add(WsMessageSerializer.HDR_ROUTING_KEY, "orders".getBytes(StandardCharsets.UTF_8));
        headers.add(WsMessageSerializer.HDR_DELIVERY_COUNT, "2".getBytes(StandardCharsets.UTF_8));
        headers.add(WsMessageSerializer.HDR_EXCHANGE, "ex".getBytes(StandardCharsets.UTF_8));

        List<Header> next = WsPoisonMessageProtection.incrementDeliveryCount(headers);

        // Count was bumped.
        assertEquals(3, readCountFromList(next));
        // Other headers survived unchanged.
        assertEquals("orders", findHeaderValue(next, WsMessageSerializer.HDR_ROUTING_KEY));
        assertEquals("ex", findHeaderValue(next, WsMessageSerializer.HDR_EXCHANGE));
        // Delivery-count header appears exactly once (we don't append — we replace).
        long occurrences = next.stream()
            .filter(h -> h.key().equals(WsMessageSerializer.HDR_DELIVERY_COUNT))
            .count();
        assertEquals(1, occurrences, "delivery count header must be replaced, not duplicated");
    }

    @Test
    void readDeliveryCount_handlesMalformedValue() {
        Headers headers = new RecordHeaders();
        headers.add(WsMessageSerializer.HDR_DELIVERY_COUNT, "not a number".getBytes(StandardCharsets.UTF_8));

        // Malformed → treated as 0 so redelivery can still proceed (don't crash on bad headers).
        assertEquals(0, WsPoisonMessageProtection.readDeliveryCount(headers));
    }

    // ---- Threshold logic ----

    @Test
    void maxRetries_notExceeded_returnsFalse() {
        registerQueue("/", "orders", Map.of());  // no override → global default=3
        // Count 1, 2 → below threshold 3.
        assertFalse(poison.exceedsMaxRetries("/", "orders", 1));
        assertFalse(poison.exceedsMaxRetries("/", "orders", 2));
    }

    @Test
    void maxRetries_exceeded_returnsTrue() {
        registerQueue("/", "orders", Map.of());  // no override → global default=3
        assertTrue(poison.exceedsMaxRetries("/", "orders", 3),
            "count == max must trigger auto-DLX");
        assertTrue(poison.exceedsMaxRetries("/", "orders", 99));
    }

    @Test
    void perQueueMaxRetries_overridesGlobal() {
        // Per-queue x-max-retries=1 wins over global default of 3.
        Map<String, String> args = new HashMap<>();
        args.put(WsPoisonMessageProtection.ARG_MAX_RETRIES, "1");
        registerQueue("/", "orders", args);

        assertFalse(poison.exceedsMaxRetries("/", "orders", 0));
        assertTrue(poison.exceedsMaxRetries("/", "orders", 1),
            "per-queue override of 1 must fire on first retry");
    }

    @Test
    void perQueueMaxRetries_malformed_fallsBackToGlobal() {
        // Bad x-max-retries value must not crash the ack path.
        Map<String, String> args = new HashMap<>();
        args.put(WsPoisonMessageProtection.ARG_MAX_RETRIES, "not-a-number");
        registerQueue("/", "orders", args);

        // Falls back to global default (3).
        assertFalse(poison.exceedsMaxRetries("/", "orders", 2));
        assertTrue(poison.exceedsMaxRetries("/", "orders", 3));
    }

    @Test
    void unknownQueue_usesGlobalDefault() {
        // No queue registered → should still honour the global default.
        assertFalse(poison.exceedsMaxRetries("/", "ghost", 2));
        assertTrue(poison.exceedsMaxRetries("/", "ghost", 3));
    }

    // ---- Auto-DLX integration ----

    @Test
    void maxRetries_exceeded_autoDlx() throws Exception {
        RecordingDlx dlx = new RecordingDlx();
        // Record carries _ws_delivery_count=5 (past the threshold of 3).
        Headers headers = withCount(5);
        Headers original = copyHeaders(headers);

        boolean dlxed = poison.maybeDeadLetter(
            "k".getBytes(StandardCharsets.UTF_8),
            "v".getBytes(StandardCharsets.UTF_8),
            headers,
            "/", "orders", dlx);

        assertTrue(dlxed, "count >= max must auto-DLX");
        assertEquals(1, dlx.calls.size(), "exactly one DLX call expected");
        // Reason is "max-retries-exceeded", NOT "rejected".
        assertEquals("max-retries-exceeded", dlx.calls.get(0).reason);
        // Headers passed to the DLX handler must preserve the delivery count so the
        // eventual DLQ consumer can see how many retries elapsed.
        assertEquals(5, WsPoisonMessageProtection.readDeliveryCount(original));
    }

    @Test
    void maxRetries_notExceeded_requeued() throws Exception {
        RecordingDlx dlx = new RecordingDlx();
        // Count 2 < threshold 3 → normal requeue, no DLX call.
        Headers headers = withCount(2);

        boolean dlxed = poison.maybeDeadLetter(
            "k".getBytes(StandardCharsets.UTF_8),
            "v".getBytes(StandardCharsets.UTF_8),
            headers,
            "/", "orders", dlx);

        assertFalse(dlxed, "below threshold must requeue, not DLX");
        assertTrue(dlx.calls.isEmpty(), "no DLX call expected");
    }

    @Test
    void autoDlx_reasonIsMaxRetriesExceeded() throws Exception {
        // Explicit reason-string assertion — spec is clear.
        RecordingDlx dlx = new RecordingDlx();
        Headers headers = withCount(10);
        poison.maybeDeadLetter(
            "k".getBytes(StandardCharsets.UTF_8),
            "v".getBytes(StandardCharsets.UTF_8),
            headers,
            "/", "orders", dlx);
        assertEquals("max-retries-exceeded", dlx.calls.get(0).reason);
    }

    @Test
    void noDlx_maxRetriesExceeded_discards() throws Exception {
        // DLX raises NO_ROUTE — poison protection must still claim the message as handled
        // so the caller commits the offset (message lost, warning logged).
        FailingDlx dlx = new FailingDlx(new IllegalStateException("no DLX configured"));
        Headers headers = withCount(7);

        boolean dlxed = poison.maybeDeadLetter(
            "k".getBytes(StandardCharsets.UTF_8),
            "v".getBytes(StandardCharsets.UTF_8),
            headers,
            "/", "orders", dlx);

        // Returns true (poison threshold was reached) even though DLX failed — the commit
        // advances so the message doesn't bounce forever.
        assertTrue(dlxed, "threshold exceeded must claim the message even without a DLX");
    }

    // ---- Constructor / null guards ----

    @Test
    void constructor_rejectsNullArgs() {
        assertThrows(NullPointerException.class,
            () -> new WsPoisonMessageProtection(null, 3));
        assertThrows(IllegalArgumentException.class,
            () -> new WsPoisonMessageProtection((v, q) -> null, 0),
            "defaultMaxRetries must be >= 1");
        assertThrows(IllegalArgumentException.class,
            () -> new WsPoisonMessageProtection((v, q) -> null, -1));
    }

    // ---- AckHandler PoisonGate integration (requeue=true threshold crossing) ----

    @Test
    void ackHandler_requeueWithCountExceeded_autoDlxesAndCommits() throws Exception {
        // Wire WsAckHandler with a PoisonGate. On NACK(requeue=true) where the
        // delivery count for that tag is past the threshold, the gate must:
        //   1. Invoke the DLX path with reason="max-retries-exceeded".
        //   2. Allow the commit watermark to advance (message handled).
        RecordingDlx dlx = new RecordingDlx();
        Map<Long, Headers> tagHeaders = new HashMap<>();
        registerQueue("/", "orders", Map.of());

        WsAckHandler.PoisonGate gate = (queueName, pd) -> {
            Headers h = tagHeaders.get(pd.offset());
            if (h == null) {
                return false;
            }
            int count = WsPoisonMessageProtection.readDeliveryCount(h);
            if (!poison.exceedsMaxRetries("/", queueName, count)) {
                return false;
            }
            // exceeded → auto-DLX
            poison.maybeDeadLetter(null, null, h, "/", queueName, dlx);
            return true;
        };

        var subMgr = new WsSubscriptionManager(java.util.concurrent.Executors.newSingleThreadExecutor());
        var chan = org.mockito.Mockito.mock(io.netty.channel.Channel.class);
        var f = org.mockito.Mockito.mock(io.netty.channel.ChannelFuture.class);
        org.mockito.Mockito.when(chan.isOpen()).thenReturn(true);
        org.mockito.Mockito.when(chan.isWritable()).thenReturn(true);
        org.mockito.Mockito.when(chan.writeAndFlush(org.mockito.ArgumentMatchers.any())).thenReturn(f);
        var tp0 = new org.apache.kafka.common.TopicPartition("ws.orders", 0);
        subMgr.subscribe("sub-1", "orders", "ws.orders",
            java.util.Set.of(tp0), Map.of(tp0, 0L), 0, false, chan);

        List<Map<org.apache.kafka.common.TopicPartition, Long>> commits = new java.util.concurrent.CopyOnWriteArrayList<>();
        WsAckHandler.OffsetCommitSink sink = (subId, offsets) -> commits.add(Map.copyOf(offsets));
        WsAckHandler handler = new WsAckHandler(subMgr, 0L, sink, null, null, gate);
        try {
            SubscriptionContext ctx = subMgr.getSubscription("sub-1");
            long tag = ctx.deliveryTagTracker().assign(tp0, 200L);
            // Stash headers for that tag — count=3 (>= max of 3) means "exceeded".
            tagHeaders.put(200L, withCount(3));

            String error = handler.handleNack("sub-1", tag, /* requeue */ true, false);
            assertNull(error);

            // Poison gate auto-DLX'd → offset committed (next offset = 201).
            assertEquals(1, commits.size(), "offset must commit after auto-DLX");
            assertEquals(201L, commits.get(0).get(tp0));
            // DLX called with max-retries-exceeded.
            assertEquals(1, dlx.calls.size());
            assertEquals("max-retries-exceeded", dlx.calls.get(0).reason);
        } finally {
            handler.stop();
            subMgr.cancelAll();
        }
    }

    @Test
    void ackHandler_requeueWithCountBelowMax_followsNormalRequeue() throws Exception {
        // Gate returns false → normal NACK(requeue=true) semantics: offset NOT committed.
        registerQueue("/", "orders", Map.of());
        WsAckHandler.PoisonGate gate = (queueName, pd) -> false;

        var subMgr = new WsSubscriptionManager(java.util.concurrent.Executors.newSingleThreadExecutor());
        var chan = org.mockito.Mockito.mock(io.netty.channel.Channel.class);
        var f = org.mockito.Mockito.mock(io.netty.channel.ChannelFuture.class);
        org.mockito.Mockito.when(chan.isOpen()).thenReturn(true);
        org.mockito.Mockito.when(chan.isWritable()).thenReturn(true);
        org.mockito.Mockito.when(chan.writeAndFlush(org.mockito.ArgumentMatchers.any())).thenReturn(f);
        var tp0 = new org.apache.kafka.common.TopicPartition("ws.orders", 0);
        subMgr.subscribe("sub-1", "orders", "ws.orders",
            java.util.Set.of(tp0), Map.of(tp0, 0L), 0, false, chan);

        List<Map<org.apache.kafka.common.TopicPartition, Long>> commits = new java.util.concurrent.CopyOnWriteArrayList<>();
        WsAckHandler.OffsetCommitSink sink = (subId, offsets) -> commits.add(Map.copyOf(offsets));
        WsAckHandler handler = new WsAckHandler(subMgr, 0L, sink, null, null, gate);
        try {
            SubscriptionContext ctx = subMgr.getSubscription("sub-1");
            long tag = ctx.deliveryTagTracker().assign(tp0, 200L);
            assertNull(handler.handleNack("sub-1", tag, true, false));
            assertTrue(commits.isEmpty(), "normal requeue must not commit");
        } finally {
            handler.stop();
            subMgr.cancelAll();
        }
    }

    // ---- Helpers ----

    private static String queueKey(String vhost, String queueName) {
        return vhost + ":" + queueName;
    }

    private void registerQueue(String vhost, String name, Map<String, String> args) {
        QueueMetadata q = new QueueMetadata(name, vhost, true, false, false, args);
        queues.put(queueKey(vhost, name), q);
    }

    private static Headers withCount(int count) {
        Headers h = new RecordHeaders();
        h.add(WsMessageSerializer.HDR_DELIVERY_COUNT,
            Integer.toString(count).getBytes(StandardCharsets.UTF_8));
        return h;
    }

    private static Headers copyHeaders(Headers in) {
        Headers out = new RecordHeaders();
        for (Header h : in) {
            out.add(new RecordHeader(h.key(), h.value()));
        }
        return out;
    }

    private static int readCountFromList(List<Header> headers) {
        for (Header h : headers) {
            if (h.key().equals(WsMessageSerializer.HDR_DELIVERY_COUNT)) {
                return Integer.parseInt(new String(h.value(), StandardCharsets.UTF_8));
            }
        }
        throw new AssertionError("no delivery count header");
    }

    private static String findHeaderValue(List<Header> headers, String key) {
        for (Header h : headers) {
            if (h.key().equals(key)) {
                return new String(h.value(), StandardCharsets.UTF_8);
            }
        }
        return null;
    }

    // ---- Fake DLX sinks ----

    /** Records every {@link WsPoisonMessageProtection.DeadLetterSink} invocation. */
    private static final class RecordingDlx implements WsPoisonMessageProtection.DeadLetterSink {
        record Call(byte[] key, byte[] value, Headers headers, String vhost, String queueName, String reason) { }
        final List<Call> calls = new java.util.concurrent.CopyOnWriteArrayList<>();

        @Override
        public java.util.concurrent.CompletableFuture<Void> deadLetter(
                byte[] key, byte[] value, Headers headers, String vhost, String queueName, String reason) {
            calls.add(new Call(key, value, headers, vhost, queueName, reason));
            return java.util.concurrent.CompletableFuture.completedFuture(null);
        }
    }

    /** Produces a failed DLX future — exercises the "no DLX / discard" path. */
    private static final class FailingDlx implements WsPoisonMessageProtection.DeadLetterSink {
        private final Throwable failure;
        FailingDlx(Throwable failure) {
            this.failure = failure;
        }
        @Override
        public java.util.concurrent.CompletableFuture<Void> deadLetter(
                byte[] key, byte[] value, Headers headers, String vhost, String queueName, String reason) {
            var cf = new java.util.concurrent.CompletableFuture<Void>();
            cf.completeExceptionally(failure);
            return cf;
        }
    }
}
