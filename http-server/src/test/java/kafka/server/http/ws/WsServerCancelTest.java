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
// Time: Created - TASK-WS4.08
package kafka.server.http.ws;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Integration-style tests for server-initiated cancel and rebalance notifications.
 * Exercises the {@link WsServerNotificationWriter} wired to the
 * {@link WsConsumerGroupCoordinator.RebalanceListener} contract plus the
 * delivery-tag invalidation path (TASK-WS4.08).
 *
 * // Time: Created - TASK-WS4.08
 */
class WsServerCancelTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private WsConnectionRegistry registry;
    private WsServerNotificationWriter writer;
    private WsConsumerGroupCoordinator coordinator;
    private ExecutorService executor;

    @BeforeEach
    void setUp() {
        registry = new WsConnectionRegistry();
        writer = new WsServerNotificationWriter(registry);
        coordinator = new WsConsumerGroupCoordinator();
        executor = Executors.newSingleThreadExecutor();
    }

    @AfterEach
    void tearDown() {
        executor.shutdownNow();
    }

    // ---------------------------------------------------------------------
    // Rebalance listener adapter — turns coordinator callbacks into frames.
    // ---------------------------------------------------------------------

    /**
     * Adapter matching design §5.3: translates coordinator callbacks into
     * {@code rebalance} frames. Buffers the revoked set until the next
     * {@code onPartitionsAssigned} call so a single rebalance emits one frame
     * with both the revoked and assigned partition lists.
     *
     * <p>Also invalidates delivery tags on revoke so subsequent acks for
     * redelivered records silently return false.
     */
    private final class RebalanceFrameAdapter implements WsConsumerGroupCoordinator.RebalanceListener {
        private final String connectionId;
        private final WsDeliveryTagTracker tracker;
        private final AtomicReference<Set<Integer>> bufferedRevoked =
            new AtomicReference<>(Collections.emptySet());

        RebalanceFrameAdapter(String connectionId, WsDeliveryTagTracker tracker) {
            this.connectionId = connectionId;
            this.tracker = tracker;
        }

        @Override
        public void onPartitionsRevoked(String subscriptionId, Collection<TopicPartition> revoked) {
            if (tracker != null) {
                tracker.invalidatePartitions(revoked);
            }
            bufferedRevoked.set(toPartitionIds(revoked));
        }

        @Override
        public void onPartitionsAssigned(String subscriptionId, Collection<TopicPartition> assigned) {
            Set<Integer> revoked = bufferedRevoked.getAndSet(Collections.emptySet());
            writer.sendRebalance(connectionId, subscriptionId, toPartitionIds(assigned), revoked);
        }

        private Set<Integer> toPartitionIds(Collection<TopicPartition> ps) {
            Set<Integer> out = new HashSet<>();
            for (TopicPartition p : ps) {
                out.add(p.partition());
            }
            return out;
        }
    }

    // ---------------------------------------------------------------------
    // Tests
    // ---------------------------------------------------------------------

    @Test
    void queueDeleted_sendsSubscriptionCancelled() throws Exception {
        StubConnection conn = StubConnection.register(registry, "conn-1");
        writer.sendSubscriptionCancelled("conn-1", "sub-1",
            WsServerNotificationWriter.REASON_QUEUE_DELETED, "orders");

        assertEquals(1, conn.frames.size());
        JsonNode frame = MAPPER.readTree(conn.frames.get(0));
        assertEquals("subscription-cancelled", frame.get("type").asText());
        assertEquals("QUEUE_DELETED", frame.get("reason").asText());
        assertEquals("orders", frame.get("queue").asText());
    }

    @Test
    void adminCancel_sendsSubscriptionCancelled() throws Exception {
        StubConnection conn = StubConnection.register(registry, "conn-a");
        writer.sendSubscriptionCancelled("conn-a", "sub-a",
            WsServerNotificationWriter.REASON_ADMIN_CANCEL, "events");

        JsonNode frame = MAPPER.readTree(conn.frames.get(0));
        assertEquals("ADMIN_CANCEL", frame.get("reason").asText());
    }

    @Test
    void rebalance_sendsAssignedAndRevoked() throws Exception {
        // Wire two subscribers to the same queue — the second join triggers a rebalance.
        StubConnection connA = StubConnection.register(registry, "conn-a");
        StubConnection connB = StubConnection.register(registry, "conn-b");

        WsDeliveryTagTracker trackerA = new WsDeliveryTagTracker();
        WsDeliveryTagTracker trackerB = new WsDeliveryTagTracker();

        RebalanceFrameAdapter adapterA = new RebalanceFrameAdapter("conn-a", trackerA);
        RebalanceFrameAdapter adapterB = new RebalanceFrameAdapter("conn-b", trackerB);

        coordinator.joinGroup("orders", "sub-a", 4, adapterA).get(1, TimeUnit.SECONDS);
        coordinator.joinGroup("orders", "sub-b", 4, adapterB).get(1, TimeUnit.SECONDS);

        // Both connections should have received at least one rebalance frame.
        assertFalse(connA.frames.isEmpty(), "conn-a should have received a rebalance frame");
        assertFalse(connB.frames.isEmpty(), "conn-b should have received a rebalance frame");

        // conn-a joined first with [0..3]; on B's join, A's assignment shrinks and
        // sends a frame with revokedPartitions containing the partitions given to B.
        JsonNode lastA = MAPPER.readTree(connA.frames.get(connA.frames.size() - 1));
        assertEquals("rebalance", lastA.get("type").asText());
        assertEquals("sub-a", lastA.get("subscriptionId").asText());
        JsonNode assignedA = lastA.get("assignedPartitions");
        JsonNode revokedA = lastA.get("revokedPartitions");
        assertNotNull(assignedA);
        assertNotNull(revokedA);
        assertTrue(revokedA.size() >= 1,
            "A should have revoked at least one partition during rebalance");

        // conn-b is a fresh joiner — revoked is empty, assigned is its new set.
        JsonNode lastB = MAPPER.readTree(connB.frames.get(connB.frames.size() - 1));
        assertEquals("rebalance", lastB.get("type").asText());
        assertEquals(0, lastB.get("revokedPartitions").size());
        assertTrue(lastB.get("assignedPartitions").size() >= 1);
    }

    @Test
    void revokedPartitions_invalidateDeliveryTags() throws Exception {
        StubConnection.register(registry, "conn-a");
        WsDeliveryTagTracker tracker = new WsDeliveryTagTracker();
        // Seed tags bound to every partition before the rebalance happens.
        TopicPartition p0 = new TopicPartition("ws.orders", 0);
        TopicPartition p1 = new TopicPartition("ws.orders", 1);
        TopicPartition p2 = new TopicPartition("ws.orders", 2);
        TopicPartition p3 = new TopicPartition("ws.orders", 3);
        long tag0 = tracker.assign(p0, 100L);
        long tag1 = tracker.assign(p1, 101L);
        long tag2 = tracker.assign(p2, 102L);
        long tag3 = tracker.assign(p3, 103L);

        RebalanceFrameAdapter adapterA = new RebalanceFrameAdapter("conn-a", tracker);
        coordinator.joinGroup("orders", "sub-a", 4, adapterA).get(1, TimeUnit.SECONDS);

        // Second member triggers a revoke for conn-a.
        StubConnection.register(registry, "conn-b");
        WsDeliveryTagTracker trackerB = new WsDeliveryTagTracker();
        RebalanceFrameAdapter adapterB = new RebalanceFrameAdapter("conn-b", trackerB);
        coordinator.joinGroup("orders", "sub-b", 4, adapterB).get(1, TimeUnit.SECONDS);

        // Any tag bound to a partition now owned by B must have been invalidated.
        Collection<TopicPartition> assignmentA = coordinator.assignmentOf("sub-a");
        Set<TopicPartition> keptA = new HashSet<>(assignmentA);
        int keptTagsAcked = 0;
        int revokedTagsFalse = 0;
        for (long tag : List.of(tag0, tag1, tag2, tag3)) {
            WsDeliveryTagTracker.PendingDelivery peek = tracker.peek(tag);
            // After invalidation, peek returns null for dropped tags; for kept tags
            // it still returns the original delivery.
            if (peek == null) {
                // Simulates an ACK for a revoked partition → silently false.
                assertFalse(tracker.ack(tag));
                revokedTagsFalse++;
            } else {
                assertTrue(keptA.contains(peek.topicPartition()),
                    "surviving tag must be bound to a kept partition");
                assertTrue(tracker.ack(tag));
                keptTagsAcked++;
            }
        }
        assertTrue(revokedTagsFalse >= 1, "at least one tag was revoked");
        assertTrue(keptTagsAcked >= 1, "at least one tag was kept");
    }

    @Test
    void cancelledSubscription_noMoreDelivers() throws Exception {
        // After subscription-cancelled is emitted, the subscription manager has
        // unsubscribed and its fetch loop is stopped — simulated here by asserting
        // that unsubscribe() leaves the subscription registry empty so no further
        // delivers can be produced against it.
        WsSubscriptionManager manager = new WsSubscriptionManager(executor);
        Channel channel = mock(Channel.class);
        when(channel.isWritable()).thenReturn(true);
        when(channel.isOpen()).thenReturn(true);
        when(channel.writeAndFlush(any())).thenReturn(mock(ChannelFuture.class));

        manager.subscribe(
            "sub-x", "orders", "ws.orders",
            Set.of(new TopicPartition("ws.orders", 0)),
            java.util.Map.of(new TopicPartition("ws.orders", 0), 0L),
            10, false, channel);
        assertNotNull(manager.getSubscription("sub-x"));

        // Server force-cancels.
        manager.unsubscribe("sub-x");

        // No more deliveries possible — subscription gone.
        assertEquals(null, manager.getSubscription("sub-x"));
    }

    @Test
    void multipleSubscribers_onlyAffectedNotified() throws Exception {
        StubConnection a = StubConnection.register(registry, "conn-a");
        StubConnection b = StubConnection.register(registry, "conn-b");
        StubConnection c = StubConnection.register(registry, "conn-c");

        writer.sendSubscriptionCancelled("conn-b", "sub-b",
            WsServerNotificationWriter.REASON_QUEUE_DELETED, "orders");

        assertTrue(a.frames.isEmpty());
        assertTrue(c.frames.isEmpty());
        assertEquals(1, b.frames.size());
        JsonNode frame = MAPPER.readTree(b.frames.get(0));
        assertEquals("sub-b", frame.get("subscriptionId").asText());
    }

    @Test
    void acksForInvalidatedTags_silentlyIgnored() {
        WsDeliveryTagTracker tracker = new WsDeliveryTagTracker();
        TopicPartition revoked = new TopicPartition("ws.q", 0);
        long tag = tracker.assign(revoked, 50L);
        tracker.invalidatePartitions(Collections.singletonList(revoked));
        // ack() returns false — the caller MUST NOT translate this into an error frame.
        assertFalse(tracker.ack(tag));
    }

    // ---------------------------------------------------------------------
    // StubConnection
    // ---------------------------------------------------------------------

    private static final class StubConnection {
        final List<String> frames = new ArrayList<>();
        final Channel nettyChannel;

        private StubConnection(Channel nettyChannel) {
            this.nettyChannel = nettyChannel;
        }

        static StubConnection register(WsConnectionRegistry registry, String connectionId) {
            ChannelHandlerContext chanCtx = mock(ChannelHandlerContext.class);
            Channel channel = mock(Channel.class);
            when(chanCtx.channel()).thenReturn(channel);
            when(channel.isActive()).thenReturn(true);
            ChannelFuture future = mock(ChannelFuture.class);

            StubConnection stub = new StubConnection(channel);
            when(chanCtx.writeAndFlush(any())).thenAnswer(inv -> {
                Object arg = inv.getArgument(0);
                if (arg instanceof TextWebSocketFrame) {
                    TextWebSocketFrame f = (TextWebSocketFrame) arg;
                    stub.frames.add(f.text());
                    f.release();
                }
                return future;
            });

            KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "u");
            InetSocketAddress remote = new InetSocketAddress("127.0.0.1", 2345);
            WsConnectionContext ctx = new WsConnectionContext(
                connectionId, principal, "/", chanCtx, remote);
            registry.register(connectionId, ctx);
            return stub;
        }
    }
}
