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

// Time: Created - TASK-WS3.03

package kafka.server.http.ws;

import org.apache.kafka.common.TopicPartition;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link WsConsumerGroupCoordinator} — competing consumer / consumer-group
 * orchestration for WebSocket subscribers (TASK-WS3.03).
 *
 * <p>This coordinator is a lightweight in-memory stand-in for the live Kafka
 * {@code GroupCoordinator}; the contract it exercises (group name, membership,
 * range-style partition distribution, rebalance callbacks, tag invalidation) is
 * validated here without any live broker integration.
 */
class WsConsumerGroupCoordinatorTest {

    private WsConsumerGroupCoordinator coordinator;

    @BeforeEach
    void setUp() {
        coordinator = new WsConsumerGroupCoordinator();
    }

    // ---------------------------------------------------------------
    // Group naming
    // ---------------------------------------------------------------

    @Test
    void groupName_hasWsPrefix() {
        assertEquals("ws.orders", WsConsumerGroupCoordinator.groupNameFor("orders"));
        assertEquals("ws.vhost1.q1", WsConsumerGroupCoordinator.groupNameFor("vhost1.q1"));
    }

    @Test
    void groupName_rejectsNullOrEmpty() {
        assertThrows(NullPointerException.class, () -> WsConsumerGroupCoordinator.groupNameFor(null));
        assertThrows(IllegalArgumentException.class, () -> WsConsumerGroupCoordinator.groupNameFor(""));
    }

    @Test
    void joinGroup_generatesCorrectGroupName() throws Exception {
        RecordingListener listener = new RecordingListener();
        coordinator.joinGroup("orders", "sub-1", 4, listener).get(1, TimeUnit.SECONDS);
        assertEquals("ws.orders", coordinator.groupIdFor("sub-1"));
    }

    // ---------------------------------------------------------------
    // Partition distribution
    // ---------------------------------------------------------------

    @Test
    void joinGroup_singleMember_getsAllPartitions() throws Exception {
        RecordingListener listener = new RecordingListener();
        Collection<TopicPartition> assigned =
            coordinator.joinGroup("orders", "sub-1", 4, listener).get(1, TimeUnit.SECONDS);

        assertEquals(4, assigned.size());
        assertEquals(Set.of(
            new TopicPartition("ws.orders", 0),
            new TopicPartition("ws.orders", 1),
            new TopicPartition("ws.orders", 2),
            new TopicPartition("ws.orders", 3)), new HashSet<>(assigned));
    }

    @Test
    void twoSubscribers_partitionsDistributed() throws Exception {
        RecordingListener lA = new RecordingListener();
        RecordingListener lB = new RecordingListener();

        Collection<TopicPartition> initA =
            coordinator.joinGroup("orders", "sub-A", 4, lA).get(1, TimeUnit.SECONDS);
        assertEquals(4, initA.size());

        Collection<TopicPartition> initB =
            coordinator.joinGroup("orders", "sub-B", 4, lB).get(1, TimeUnit.SECONDS);

        // After sub-B joins, the assignment reported at join-time must be sub-B's new
        // slice (2 partitions), and the existing member sub-A must have been
        // re-assigned via onPartitionsAssigned / onPartitionsRevoked callbacks.
        assertEquals(2, initB.size());

        Collection<TopicPartition> currentA = coordinator.assignmentOf("sub-A");
        Collection<TopicPartition> currentB = coordinator.assignmentOf("sub-B");

        // Total assigned across both = total partitions, no overlap.
        Set<TopicPartition> union = new HashSet<>(currentA);
        union.addAll(currentB);
        assertEquals(4, union.size(), "no partition should be double-assigned");
        assertEquals(2, currentA.size());
        assertEquals(2, currentB.size());

        // Both members must have received at least one rebalance callback (initial
        // assignment for each, plus a re-assignment for sub-A when sub-B joined).
        assertFalse(lA.assignments.isEmpty(), "sub-A should have received an assignment");
        assertFalse(lB.assignments.isEmpty(), "sub-B should have received an assignment");
    }

    @Test
    void threeSubscribers_unevenPartitions_distributedRangeStyle() throws Exception {
        // 7 partitions / 3 members → 3, 2, 2.
        RecordingListener lA = new RecordingListener();
        RecordingListener lB = new RecordingListener();
        RecordingListener lC = new RecordingListener();

        coordinator.joinGroup("q", "A", 7, lA).get(1, TimeUnit.SECONDS);
        coordinator.joinGroup("q", "B", 7, lB).get(1, TimeUnit.SECONDS);
        coordinator.joinGroup("q", "C", 7, lC).get(1, TimeUnit.SECONDS);

        int sizeA = coordinator.assignmentOf("A").size();
        int sizeB = coordinator.assignmentOf("B").size();
        int sizeC = coordinator.assignmentOf("C").size();

        assertEquals(7, sizeA + sizeB + sizeC);
        // Range-style: the difference between max and min assigned count must be 0 or 1.
        int max = Math.max(sizeA, Math.max(sizeB, sizeC));
        int min = Math.min(sizeA, Math.min(sizeB, sizeC));
        assertTrue(max - min <= 1, "unbalanced assignment: " + sizeA + "/" + sizeB + "/" + sizeC);

        // No double assignment across the three members.
        Set<TopicPartition> union = new HashSet<>();
        union.addAll(coordinator.assignmentOf("A"));
        union.addAll(coordinator.assignmentOf("B"));
        union.addAll(coordinator.assignmentOf("C"));
        assertEquals(7, union.size());
    }

    @Test
    void leaveGroup_triggersRebalance_remainingMemberGetsAllPartitions() throws Exception {
        RecordingListener lA = new RecordingListener();
        RecordingListener lB = new RecordingListener();

        coordinator.joinGroup("orders", "sub-A", 4, lA).get(1, TimeUnit.SECONDS);
        coordinator.joinGroup("orders", "sub-B", 4, lB).get(1, TimeUnit.SECONDS);

        lA.clear();
        lB.clear();

        coordinator.leaveGroup("orders", "sub-B").get(1, TimeUnit.SECONDS);

        // sub-A must now own all 4 partitions
        Collection<TopicPartition> a = coordinator.assignmentOf("sub-A");
        assertEquals(4, a.size());

        // sub-A received a rebalance callback on the remaining slice reassignment
        assertFalse(lA.assignments.isEmpty(), "sub-A should have received rebalance assigned");

        // sub-B is gone — no further assignments should be delivered
        assertNull(coordinator.assignmentOf("sub-B"));
    }

    @Test
    void leaveGroup_unknownSubscription_returnsQuietly() throws Exception {
        // Silent no-op when neither the group nor the subscription exists
        coordinator.leaveGroup("orders", "nope").get(1, TimeUnit.SECONDS);
    }

    @Test
    void leaveGroup_lastMember_removesGroup() throws Exception {
        RecordingListener lA = new RecordingListener();
        coordinator.joinGroup("orders", "sub-A", 4, lA).get(1, TimeUnit.SECONDS);
        coordinator.leaveGroup("orders", "sub-A").get(1, TimeUnit.SECONDS);

        assertEquals(0, coordinator.memberCount("orders"));
        assertNull(coordinator.assignmentOf("sub-A"));
    }

    // ---------------------------------------------------------------
    // Rebalance callbacks
    // ---------------------------------------------------------------

    @Test
    void rebalance_sendsAssignedAndRevokedToExistingMember() throws Exception {
        RecordingListener lA = new RecordingListener();
        RecordingListener lB = new RecordingListener();

        coordinator.joinGroup("orders", "sub-A", 4, lA).get(1, TimeUnit.SECONDS);
        lA.clear();

        coordinator.joinGroup("orders", "sub-B", 4, lB).get(1, TimeUnit.SECONDS);

        // sub-A lost 2 partitions to sub-B — its listener must have observed at least one
        // onPartitionsRevoked callback.
        assertFalse(lA.revocations.isEmpty(), "sub-A must see revocations when sub-B joins");
        // Combined size of revoked partitions should equal 2 (going from 4 → 2 assigned).
        int totalRevoked = lA.revocations.stream().mapToInt(Collection::size).sum();
        assertEquals(2, totalRevoked);
    }

    @Test
    void multipleSubscriptions_sameQueue_sameGroup() throws Exception {
        RecordingListener lA = new RecordingListener();
        RecordingListener lB = new RecordingListener();
        RecordingListener lC = new RecordingListener();

        coordinator.joinGroup("orders", "A", 4, lA).get(1, TimeUnit.SECONDS);
        coordinator.joinGroup("orders", "B", 4, lB).get(1, TimeUnit.SECONDS);
        coordinator.joinGroup("orders", "C", 4, lC).get(1, TimeUnit.SECONDS);

        assertEquals("ws.orders", coordinator.groupIdFor("A"));
        assertEquals("ws.orders", coordinator.groupIdFor("B"));
        assertEquals("ws.orders", coordinator.groupIdFor("C"));
        assertEquals(3, coordinator.memberCount("orders"));
    }

    @Test
    void differentQueues_differentGroups() throws Exception {
        coordinator.joinGroup("orders", "A", 4, new RecordingListener()).get(1, TimeUnit.SECONDS);
        coordinator.joinGroup("events", "B", 4, new RecordingListener()).get(1, TimeUnit.SECONDS);

        assertEquals("ws.orders", coordinator.groupIdFor("A"));
        assertEquals("ws.events", coordinator.groupIdFor("B"));
        assertEquals(1, coordinator.memberCount("orders"));
        assertEquals(1, coordinator.memberCount("events"));
    }

    // ---------------------------------------------------------------
    // Validation
    // ---------------------------------------------------------------

    @Test
    void joinGroup_duplicateSubscription_fails() throws Exception {
        coordinator.joinGroup("orders", "sub-A", 4, new RecordingListener()).get(1, TimeUnit.SECONDS);
        ExecutionException ee = assertThrows(ExecutionException.class, () ->
            coordinator.joinGroup("orders", "sub-A", 4, new RecordingListener()).get(1, TimeUnit.SECONDS));
        assertTrue(ee.getCause() instanceof IllegalStateException);
    }

    @Test
    void joinGroup_invalidArgs_rejected() {
        assertThrows(NullPointerException.class,
            () -> coordinator.joinGroup(null, "s", 4, new RecordingListener()));
        assertThrows(NullPointerException.class,
            () -> coordinator.joinGroup("q", null, 4, new RecordingListener()));
        assertThrows(NullPointerException.class,
            () -> coordinator.joinGroup("q", "s", 4, null));
        assertThrows(IllegalArgumentException.class,
            () -> coordinator.joinGroup("q", "s", 0, new RecordingListener()));
    }

    // ---------------------------------------------------------------
    // Tag invalidation on revoke
    // ---------------------------------------------------------------

    @Test
    void revokedPartitions_invalidateDeliveryTags() throws Exception {
        // When partitions are revoked, the listener must be notified so it can clear its
        // delivery-tag state. This test uses a listener whose onPartitionsRevoked call
        // clears an associated WsDeliveryTagTracker.
        WsDeliveryTagTracker tracker = new WsDeliveryTagTracker();
        RecordingListener lA = new RecordingListener() {
            @Override
            public void onPartitionsRevoked(String subscriptionId, Collection<TopicPartition> revoked) {
                super.onPartitionsRevoked(subscriptionId, revoked);
                tracker.clear();
            }
        };
        coordinator.joinGroup("orders", "sub-A", 4, lA).get(1, TimeUnit.SECONDS);

        // Simulate deliveries
        TopicPartition tp = new TopicPartition("ws.orders", 0);
        long tag1 = tracker.assign(tp, 10L);
        long tag2 = tracker.assign(tp, 11L);
        assertEquals(2, tracker.pendingCount());

        // Second subscriber joins → rebalance → sub-A loses half its partitions.
        coordinator.joinGroup("orders", "sub-B", 4, new RecordingListener()).get(1, TimeUnit.SECONDS);

        // Tracker has been cleared by the listener; prior ACKs are silently ignored.
        assertEquals(0, tracker.pendingCount());
        assertFalse(tracker.ack(tag1));
        assertFalse(tracker.ack(tag2));
    }

    // ---------------------------------------------------------------
    // leaveGroup idempotence
    // ---------------------------------------------------------------

    @Test
    void leaveGroup_twice_isIdempotent() throws Exception {
        coordinator.joinGroup("orders", "sub-A", 4, new RecordingListener()).get(1, TimeUnit.SECONDS);
        coordinator.leaveGroup("orders", "sub-A").get(1, TimeUnit.SECONDS);
        coordinator.leaveGroup("orders", "sub-A").get(1, TimeUnit.SECONDS); // no throw
    }

    @Test
    void joinGroup_null_topicPartitionName_usesWsPrefixedTopic() throws Exception {
        // The assigned TopicPartitions in the callback should carry topic name "ws.{queue}".
        AtomicReference<Collection<TopicPartition>> seen = new AtomicReference<>();
        WsConsumerGroupCoordinator.RebalanceListener listener =
            new WsConsumerGroupCoordinator.RebalanceListener() {
                @Override
                public void onPartitionsAssigned(String subscriptionId, Collection<TopicPartition> assigned) {
                    seen.set(assigned);
                }
                @Override
                public void onPartitionsRevoked(String subscriptionId, Collection<TopicPartition> revoked) { }
            };
        coordinator.joinGroup("vhost1.q1", "sub-A", 2, listener).get(1, TimeUnit.SECONDS);
        assertNotNull(seen.get());
        for (TopicPartition tp : seen.get()) {
            assertEquals("ws.vhost1.q1", tp.topic());
        }
    }

    // ---------------------------------------------------------------
    // Test helpers
    // ---------------------------------------------------------------

    /** A listener that records every callback it receives. Thread-safe enough for tests. */
    private static class RecordingListener implements WsConsumerGroupCoordinator.RebalanceListener {
        final List<Collection<TopicPartition>> assignments =
            Collections.synchronizedList(new ArrayList<>());
        final List<Collection<TopicPartition>> revocations =
            Collections.synchronizedList(new ArrayList<>());

        @Override
        public void onPartitionsAssigned(String subscriptionId, Collection<TopicPartition> assigned) {
            assignments.add(new ArrayList<>(assigned));
        }

        @Override
        public void onPartitionsRevoked(String subscriptionId, Collection<TopicPartition> revoked) {
            revocations.add(new ArrayList<>(revoked));
        }

        void clear() {
            assignments.clear();
            revocations.clear();
        }
    }

    // A sanity timeout helper in case joinGroup returns a future that never completes
    @SuppressWarnings("unused")
    private static void assertCompletes(java.util.concurrent.CompletableFuture<?> f)
            throws InterruptedException, ExecutionException {
        try {
            f.get(1, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            throw new AssertionError("future did not complete in 1s", e);
        }
    }
}
