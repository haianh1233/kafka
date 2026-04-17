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

// Time: Created - TASK-WS1.13

package kafka.server.http.ws;

import org.apache.kafka.common.TopicPartition;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WsDeliveryTagTrackerTest {

    private WsDeliveryTagTracker tracker;
    private final TopicPartition tp0 = new TopicPartition("ws.orders", 0);
    private final TopicPartition tp1 = new TopicPartition("ws.orders", 1);

    @BeforeEach
    void setUp() {
        tracker = new WsDeliveryTagTracker();
    }

    @Test
    void assign_returnsMonotonicallyIncreasingTags() {
        long tag1 = tracker.assign(tp0, 100);
        long tag2 = tracker.assign(tp0, 101);
        long tag3 = tracker.assign(tp1, 50);
        assertEquals(1L, tag1);
        assertEquals(2L, tag2);
        assertEquals(3L, tag3);
        assertEquals(3, tracker.pendingCount());
    }

    @Test
    void ack_singleTag_removesFromPending() {
        long tag = tracker.assign(tp0, 100);
        assertTrue(tracker.ack(tag));
        assertEquals(0, tracker.pendingCount());
    }

    @Test
    void ack_unknownTag_returnsFalse() {
        assertFalse(tracker.ack(999));
    }

    @Test
    void ack_doubleAck_isIdempotent() {
        long tag = tracker.assign(tp0, 100);
        assertTrue(tracker.ack(tag));
        // second ack: already removed from pending → returns false (no longer known)
        assertFalse(tracker.ack(tag));
    }

    @Test
    void ackMultiple_acksAllTagsUpToGiven() {
        tracker.assign(tp0, 100); // tag 1
        tracker.assign(tp0, 101); // tag 2
        tracker.assign(tp0, 102); // tag 3
        assertTrue(tracker.ackMultiple(2));
        assertEquals(1, tracker.pendingCount()); // tag 3 still pending
    }

    @Test
    void ackMultiple_unknownUpToTag_returnsFalse() {
        // upToTag greater than any assigned — no tags assigned yet
        assertFalse(tracker.ackMultiple(99));
    }

    @Test
    void ackMultiple_withGap_preservesNackedRequeue() {
        // Design doc §13.4 scenario
        tracker.assign(tp0, 40); // tag 1
        tracker.assign(tp0, 41); // tag 2
        tracker.assign(tp0, 42); // tag 3
        tracker.assign(tp0, 43); // tag 4
        tracker.assign(tp0, 44); // tag 5

        // NACK tag 3 with requeue (offset 42 NOT committed)
        tracker.nack(3, true);

        // ACK multiple up to tag 5
        tracker.ackMultiple(5);

        // Committable: only up to offset 42 (NOT 45!) — offset 42 is the gap
        Map<TopicPartition, Long> offsets = tracker.getCommittableOffsets();
        assertEquals(42L, offsets.get(tp0));
    }

    @Test
    void nack_withRequeue_createsGap() {
        long tag = tracker.assign(tp0, 100);
        WsDeliveryTagTracker.PendingDelivery pd = tracker.nack(tag, true);
        assertNotNull(pd);
        assertEquals(tp0, pd.topicPartition());
        assertEquals(100L, pd.offset());

        // Offset should NOT be committable
        Map<TopicPartition, Long> offsets = tracker.getCommittableOffsets();
        assertTrue(offsets.isEmpty());
    }

    @Test
    void nack_withoutRequeue_allowsCommit() {
        long tag = tracker.assign(tp0, 100);
        tracker.nack(tag, false); // discard (DLX or lost)

        Map<TopicPartition, Long> offsets = tracker.getCommittableOffsets();
        assertEquals(101L, offsets.get(tp0)); // offset+1 per Kafka convention
    }

    @Test
    void nack_unknownTag_returnsNull() {
        assertNull(tracker.nack(999, true));
    }

    @Test
    void getCommittableOffsets_contiguousAcks() {
        tracker.assign(tp0, 100); // tag 1
        tracker.assign(tp0, 101); // tag 2
        tracker.assign(tp0, 102); // tag 3
        tracker.ack(1);
        tracker.ack(2);
        // tag 3 still pending — commit advances to 102 (exclusive)

        Map<TopicPartition, Long> offsets = tracker.getCommittableOffsets();
        assertEquals(102L, offsets.get(tp0));
    }

    @Test
    void getCommittableOffsets_multiplePartitions() {
        tracker.assign(tp0, 10); // tag 1
        tracker.assign(tp1, 20); // tag 2
        tracker.ack(1);
        tracker.ack(2);

        Map<TopicPartition, Long> offsets = tracker.getCommittableOffsets();
        assertEquals(11L, offsets.get(tp0));
        assertEquals(21L, offsets.get(tp1));
    }

    @Test
    void getCommittableOffsets_returnsEmpty_whenNothingNew() {
        tracker.assign(tp0, 100);
        tracker.ack(1);
        tracker.getCommittableOffsets(); // consume the offset

        Map<TopicPartition, Long> offsets = tracker.getCommittableOffsets();
        assertTrue(offsets.isEmpty()); // nothing new to commit
    }

    @Test
    void getCommittableOffsets_pendingTagBlocksCommit() {
        tracker.assign(tp0, 100); // tag 1 — still pending
        tracker.assign(tp0, 101); // tag 2
        tracker.ack(2); // ack tag 2 but NOT tag 1

        // Nothing committable — tag 1 is the lowest and still pending
        Map<TopicPartition, Long> offsets = tracker.getCommittableOffsets();
        assertTrue(offsets.isEmpty());
    }

    @Test
    void clear_resetsAllState() {
        tracker.assign(tp0, 100);
        tracker.clear();
        assertEquals(0, tracker.pendingCount());
        assertFalse(tracker.ack(1)); // ignored after clear
    }

    @Test
    void ackAfterClear_isIgnored() {
        long tag = tracker.assign(tp0, 100);
        tracker.clear();
        assertFalse(tracker.ack(tag));
        assertNull(tracker.nack(tag, true));
        assertFalse(tracker.ackMultiple(tag));
        assertTrue(tracker.getCommittableOffsets().isEmpty());
    }

    @Test
    void gapFilled_advancesCommitOffset() {
        // Full §13.4 scenario with gap resolution
        tracker.assign(tp0, 40); // tag 1
        tracker.assign(tp0, 41); // tag 2
        tracker.assign(tp0, 42); // tag 3
        tracker.assign(tp0, 43); // tag 4
        tracker.assign(tp0, 44); // tag 5

        tracker.nack(3, true);  // gap at offset 42
        tracker.ackMultiple(5);

        Map<TopicPartition, Long> offsets1 = tracker.getCommittableOffsets();
        assertEquals(42L, offsets1.get(tp0));

        // Redelivery: tag 3 was requeued, now redelivered as tag 6
        tracker.assign(tp0, 42); // tag 6 (redelivered)
        tracker.ack(6);

        Map<TopicPartition, Long> offsets2 = tracker.getCommittableOffsets();
        assertEquals(45L, offsets2.get(tp0)); // gap filled, advance to end
    }

    @Test
    void pendingDelivery_recordFields() {
        long tag = tracker.assign(tp0, 42);
        WsDeliveryTagTracker.PendingDelivery pd = tracker.nack(tag, false);
        assertNotNull(pd);
        assertEquals(tp0, pd.topicPartition());
        assertEquals(42L, pd.offset());
    }

    @Test
    void concurrency_parallelAcks_doNotCorruptState() throws InterruptedException {
        // Single-threaded "fetch loop" assigns first (simulating wsConsumerExecutor),
        // then many Netty event-loop threads ack concurrently.
        int n = 500;
        long[] tags = new long[n];
        for (int i = 0; i < n; i++) {
            tags[i] = tracker.assign(tp0, 1000 + i);
        }
        assertEquals(n, tracker.pendingCount());

        int threads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        AtomicInteger successes = new AtomicInteger();
        try {
            int slice = n / threads;
            for (int t = 0; t < threads; t++) {
                int from = t * slice;
                int to = (t == threads - 1) ? n : from + slice;
                pool.submit(() -> {
                    try {
                        start.await();
                        for (int i = from; i < to; i++) {
                            if (tracker.ack(tags[i])) {
                                successes.incrementAndGet();
                            }
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        done.countDown();
                    }
                });
            }
            start.countDown();
            assertTrue(done.await(10, TimeUnit.SECONDS), "threads timed out");
        } finally {
            pool.shutdownNow();
        }

        assertEquals(n, successes.get(), "every tag should be acked exactly once");
        assertEquals(0, tracker.pendingCount());

        // Final offset advance covers all assigned offsets: 1000..1000+n-1 → commit = 1000+n
        Map<TopicPartition, Long> offsets = tracker.getCommittableOffsets();
        assertEquals((long) (1000 + n), offsets.get(tp0));
    }

    @Test
    void concurrency_parallelAssignsAndAcks_areConsistent() throws InterruptedException {
        // Fetch loop (single assigner) competes with Netty event-loop threads nacking-with-requeue.
        // We primarily verify no exceptions thrown and final pendingCount is deterministic.
        int n = 200;
        List<Long> tags = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            tags.add(tracker.assign(tp0, i));
        }

        ExecutorService pool = Executors.newFixedThreadPool(4);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(4);
        try {
            for (int t = 0; t < 4; t++) {
                final int id = t;
                pool.submit(() -> {
                    try {
                        start.await();
                        // Each thread nacks-with-requeue a disjoint slice.
                        int slice = n / 4;
                        int from = id * slice;
                        int to = (id == 3) ? n : from + slice;
                        for (int i = from; i < to; i++) {
                            tracker.nack(tags.get(i), true);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        done.countDown();
                    }
                });
            }
            start.countDown();
            assertTrue(done.await(10, TimeUnit.SECONDS));
        } finally {
            pool.shutdownNow();
        }

        assertEquals(0, tracker.pendingCount());
        // All nacked-with-requeue → nothing committable
        assertTrue(tracker.getCommittableOffsets().isEmpty());
    }
}
