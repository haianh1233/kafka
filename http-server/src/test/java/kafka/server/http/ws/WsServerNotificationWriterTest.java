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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
 * Tests for {@link WsServerNotificationWriter} — server-initiated
 * {@code subscription-cancelled} and {@code rebalance} frames (TASK-WS4.08).
 *
 * // Time: Created - TASK-WS4.08
 */
class WsServerNotificationWriterTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private WsConnectionRegistry registry;
    private WsServerNotificationWriter writer;

    @BeforeEach
    void setUp() {
        registry = new WsConnectionRegistry();
        writer = new WsServerNotificationWriter(registry);
    }

    // ------------------------------------------------------------------
    //  Constructor
    // ------------------------------------------------------------------

    @Test
    void constructor_nullRegistry_throws() {
        assertThrows(NullPointerException.class, () -> new WsServerNotificationWriter(null));
    }

    // ------------------------------------------------------------------
    //  subscription-cancelled frames
    // ------------------------------------------------------------------

    @Test
    void queueDeleted_sendsSubscriptionCancelled() throws Exception {
        StubConnection conn = StubConnection.register(registry, "conn-1");
        writer.sendSubscriptionCancelled("conn-1", "sub-1",
            WsServerNotificationWriter.REASON_QUEUE_DELETED, "orders");

        assertEquals(1, conn.frames.size());
        JsonNode frame = MAPPER.readTree(conn.frames.get(0));
        assertEquals("subscription-cancelled", frame.get("type").asText());
        assertEquals("sub-1", frame.get("subscriptionId").asText());
        assertEquals("QUEUE_DELETED", frame.get("reason").asText());
        assertEquals("orders", frame.get("queue").asText());
    }

    @Test
    void adminCancel_sendsSubscriptionCancelled() throws Exception {
        StubConnection conn = StubConnection.register(registry, "conn-1");
        writer.sendSubscriptionCancelled("conn-1", "sub-7",
            WsServerNotificationWriter.REASON_ADMIN_CANCEL, "events");

        assertEquals(1, conn.frames.size());
        JsonNode frame = MAPPER.readTree(conn.frames.get(0));
        assertEquals("subscription-cancelled", frame.get("type").asText());
        assertEquals("sub-7", frame.get("subscriptionId").asText());
        assertEquals("ADMIN_CANCEL", frame.get("reason").asText());
        assertEquals("events", frame.get("queue").asText());
    }

    @Test
    void exclusiveEviction_sendsSubscriptionCancelled() throws Exception {
        StubConnection conn = StubConnection.register(registry, "conn-e");
        writer.sendSubscriptionCancelled("conn-e", "sub-e",
            WsServerNotificationWriter.REASON_EXCLUSIVE_EVICTION, "vip");

        JsonNode frame = MAPPER.readTree(conn.frames.get(0));
        assertEquals("EXCLUSIVE_EVICTION", frame.get("reason").asText());
        assertEquals("vip", frame.get("queue").asText());
    }

    @Test
    void topicDeleted_sendsSubscriptionCancelled() throws Exception {
        StubConnection conn = StubConnection.register(registry, "conn-t");
        writer.sendSubscriptionCancelled("conn-t", "sub-t",
            WsServerNotificationWriter.REASON_TOPIC_DELETED, "audit");

        JsonNode frame = MAPPER.readTree(conn.frames.get(0));
        assertEquals("TOPIC_DELETED", frame.get("reason").asText());
        assertEquals("audit", frame.get("queue").asText());
    }

    @Test
    void subscriptionCancelled_queueNameOmittedWhenNull() throws Exception {
        StubConnection conn = StubConnection.register(registry, "conn-1");
        writer.sendSubscriptionCancelled("conn-1", "sub-1",
            WsServerNotificationWriter.REASON_ADMIN_CANCEL, null);

        JsonNode frame = MAPPER.readTree(conn.frames.get(0));
        assertNull(frame.get("queue"));
    }

    @Test
    void subscriptionCancelled_nullArgs_throwNPE() {
        StubConnection.register(registry, "conn-1");
        assertThrows(NullPointerException.class,
            () -> writer.sendSubscriptionCancelled(null, "sub", "X", "q"));
        assertThrows(NullPointerException.class,
            () -> writer.sendSubscriptionCancelled("conn-1", null, "X", "q"));
        assertThrows(NullPointerException.class,
            () -> writer.sendSubscriptionCancelled("conn-1", "sub", null, "q"));
    }

    @Test
    void subscriptionCancelled_unknownConnection_silentNoOp() {
        // no register()
        writer.sendSubscriptionCancelled("ghost", "sub-1",
            WsServerNotificationWriter.REASON_QUEUE_DELETED, "q");
        // nothing to assert — the write must simply not throw.
    }

    @Test
    void subscriptionCancelled_inactiveChannel_silentNoOp() {
        StubConnection conn = StubConnection.register(registry, "conn-1");
        conn.markInactive();
        writer.sendSubscriptionCancelled("conn-1", "sub-1",
            WsServerNotificationWriter.REASON_QUEUE_DELETED, "q");
        assertTrue(conn.frames.isEmpty(), "inactive channel must not receive frames");
    }

    @Test
    void multipleSubscribers_onlyAffectedNotified() throws Exception {
        StubConnection a = StubConnection.register(registry, "conn-a");
        StubConnection b = StubConnection.register(registry, "conn-b");
        StubConnection c = StubConnection.register(registry, "conn-c");

        // Only conn-b receives the cancel notification.
        writer.sendSubscriptionCancelled("conn-b", "sub-b",
            WsServerNotificationWriter.REASON_QUEUE_DELETED, "orders");

        assertTrue(a.frames.isEmpty());
        assertEquals(1, b.frames.size());
        assertTrue(c.frames.isEmpty());

        JsonNode frame = MAPPER.readTree(b.frames.get(0));
        assertEquals("sub-b", frame.get("subscriptionId").asText());
    }

    // ------------------------------------------------------------------
    //  rebalance frames
    // ------------------------------------------------------------------

    @Test
    void rebalance_sendsAssignedAndRevoked() throws Exception {
        StubConnection conn = StubConnection.register(registry, "conn-1");
        writer.sendRebalance("conn-1", "sub-1",
            Arrays.asList(0, 1, 3),
            Collections.singletonList(2));

        assertEquals(1, conn.frames.size());
        JsonNode frame = MAPPER.readTree(conn.frames.get(0));
        assertEquals("rebalance", frame.get("type").asText());
        assertEquals("sub-1", frame.get("subscriptionId").asText());
        JsonNode assigned = frame.get("assignedPartitions");
        assertNotNull(assigned);
        assertTrue(assigned.isArray());
        assertEquals(3, assigned.size());
        Set<Integer> assignedSet = new HashSet<>();
        assigned.forEach(n -> assignedSet.add(n.asInt()));
        assertEquals(Set.of(0, 1, 3), assignedSet);

        JsonNode revoked = frame.get("revokedPartitions");
        assertNotNull(revoked);
        assertTrue(revoked.isArray());
        assertEquals(1, revoked.size());
        assertEquals(2, revoked.get(0).asInt());
    }

    @Test
    void rebalance_emptyRevoked_stillWritesArray() throws Exception {
        StubConnection conn = StubConnection.register(registry, "conn-1");
        writer.sendRebalance("conn-1", "sub-1",
            Arrays.asList(0, 1, 2, 3),
            Collections.emptyList());

        JsonNode frame = MAPPER.readTree(conn.frames.get(0));
        JsonNode revoked = frame.get("revokedPartitions");
        assertNotNull(revoked);
        assertTrue(revoked.isArray());
        assertEquals(0, revoked.size());
    }

    @Test
    void rebalance_emptyAssigned_stillWritesArray() throws Exception {
        StubConnection conn = StubConnection.register(registry, "conn-1");
        writer.sendRebalance("conn-1", "sub-1",
            Collections.emptyList(),
            Arrays.asList(0, 1, 2, 3));

        JsonNode frame = MAPPER.readTree(conn.frames.get(0));
        JsonNode assigned = frame.get("assignedPartitions");
        assertNotNull(assigned);
        assertTrue(assigned.isArray());
        assertEquals(0, assigned.size());
    }

    @Test
    void rebalance_unknownConnection_silentNoOp() {
        writer.sendRebalance("ghost", "sub-1",
            Arrays.asList(0, 1), Arrays.asList(2));
        // must not throw
    }

    @Test
    void rebalance_inactiveChannel_silentNoOp() {
        StubConnection conn = StubConnection.register(registry, "conn-1");
        conn.markInactive();
        writer.sendRebalance("conn-1", "sub-1",
            Arrays.asList(0, 1), Arrays.asList(2));
        assertTrue(conn.frames.isEmpty());
    }

    @Test
    void rebalance_nullArgs_throwNPE() {
        StubConnection.register(registry, "conn-1");
        assertThrows(NullPointerException.class,
            () -> writer.sendRebalance(null, "sub", List.of(), List.of()));
        assertThrows(NullPointerException.class,
            () -> writer.sendRebalance("conn-1", null, List.of(), List.of()));
        assertThrows(NullPointerException.class,
            () -> writer.sendRebalance("conn-1", "sub", null, List.of()));
        assertThrows(NullPointerException.class,
            () -> writer.sendRebalance("conn-1", "sub", List.of(), null));
    }

    // ------------------------------------------------------------------
    //  Interaction with delivery-tag invalidation (integration)
    // ------------------------------------------------------------------

    @Test
    void revokedPartitions_invalidateDeliveryTags() {
        // Verify the contract the rebalance frame relies on: tags bound to revoked
        // partitions are dropped by WsDeliveryTagTracker.invalidatePartitions so
        // subsequent ACKs return false and MUST be silently ignored by callers.
        WsDeliveryTagTracker tracker = new WsDeliveryTagTracker();
        TopicPartition tp0 = new TopicPartition("ws.orders", 0);
        TopicPartition tp1 = new TopicPartition("ws.orders", 1);

        long tag0 = tracker.assign(tp0, 100L);
        long tag1 = tracker.assign(tp1, 200L);

        int dropped = tracker.invalidatePartitions(Collections.singletonList(tp0));
        assertEquals(1, dropped, "only the tp0 pending tag should be dropped");

        // ACK for invalidated tag must return false — the caller must treat this
        // as silent success, not an error.
        assertFalse(tracker.ack(tag0));
        // ACK for a still-valid tag (different partition) continues to work.
        assertTrue(tracker.ack(tag1));
    }

    @Test
    void acksForInvalidatedTags_silentlyIgnored() {
        // Confirm the fast-path semantics: ack() after invalidate returns false for the
        // dropped tags and true for unaffected tags — the caller must NOT surface false
        // as an error.
        WsDeliveryTagTracker tracker = new WsDeliveryTagTracker();
        TopicPartition revoked = new TopicPartition("ws.q", 0);
        TopicPartition kept = new TopicPartition("ws.q", 1);

        long[] revokedTags = new long[3];
        for (int i = 0; i < revokedTags.length; i++) {
            revokedTags[i] = tracker.assign(revoked, 10L + i);
        }
        long keptTag = tracker.assign(kept, 99L);

        tracker.invalidatePartitions(Collections.singletonList(revoked));

        for (long tag : revokedTags) {
            assertFalse(tracker.ack(tag), "revoked-partition tag " + tag + " must ack==false");
        }
        assertTrue(tracker.ack(keptTag));
    }

    // ------------------------------------------------------------------
    //  Helpers
    // ------------------------------------------------------------------

    /**
     * Minimal stub connection wired to the registry. Captures every TextWebSocketFrame
     * payload pushed through {@link WsConnectionContext#sendFrame(String)} so tests can
     * assert the JSON shape without standing up an EmbeddedChannel.
     */
    private static final class StubConnection {
        final WsConnectionContext context;
        final List<String> frames = new ArrayList<>();
        final Channel nettyChannel;

        private StubConnection(WsConnectionContext context, Channel nettyChannel) {
            this.context = context;
            this.nettyChannel = nettyChannel;
        }

        static StubConnection register(WsConnectionRegistry registry, String connectionId) {
            ChannelHandlerContext chanCtx = mock(ChannelHandlerContext.class);
            Channel channel = mock(Channel.class);
            when(chanCtx.channel()).thenReturn(channel);
            when(channel.isActive()).thenReturn(true);
            ChannelFuture future = mock(ChannelFuture.class);

            StubConnection stub = new StubConnection(null, channel);
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
            InetSocketAddress remote = new InetSocketAddress("127.0.0.1", 1234);
            WsConnectionContext ctx = new WsConnectionContext(
                connectionId, principal, "/", chanCtx, remote);

            registry.register(connectionId, ctx);
            // Re-seat the context reference now that it's constructed.
            StubConnection seated = new StubConnection(ctx, channel);
            seated.frames.addAll(stub.frames);
            // We want the captured callbacks to populate the *returned* stub's list —
            // so we migrate the answer's target list by retargeting the mock.
            when(chanCtx.writeAndFlush(any())).thenAnswer(inv -> {
                Object arg = inv.getArgument(0);
                if (arg instanceof TextWebSocketFrame) {
                    TextWebSocketFrame f = (TextWebSocketFrame) arg;
                    seated.frames.add(f.text());
                    f.release();
                }
                return future;
            });
            return seated;
        }

        void markInactive() {
            when(nettyChannel.isActive()).thenReturn(false);
        }
    }
}
