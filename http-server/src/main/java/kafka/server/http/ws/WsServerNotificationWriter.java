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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Objects;

/**
 * Writes server-initiated notification frames to WebSocket connections
 * (design doc §5.3 and §5.8, TASK-WS4.08).
 *
 * <p>Two notification frame types are emitted:
 *
 * <ul>
 *   <li>{@code subscription-cancelled} — TERMINAL. After this frame the broker will
 *       send no further {@code deliver} frames for the named subscriptionId. Emitted
 *       when the queue is deleted, an exclusive consumer is evicted, the backing Kafka
 *       topic is deleted, or an admin force-cancels via REST.</li>
 *   <li>{@code rebalance} — informational. Emitted when the consumer group for a queue
 *       has reassigned partitions. Carries the new complete {@code assignedPartitions}
 *       set and the set of {@code revokedPartitions}. Delivery tags for revoked
 *       partitions have already been invalidated in the tracker; ACKs for those tags
 *       silently return {@code false} and MUST NOT be translated into error frames.</li>
 * </ul>
 *
 * <p>The writer looks up the owning connection via the shared
 * {@link WsConnectionRegistry}. When the connection id is unknown (connection closed
 * concurrently) the send is a silent no-op — this is normal during connection teardown
 * races and must not throw.
 *
 * <p>Thread-safety: this class is immutable after construction. All send methods are
 * safe to call from any thread (Netty serialises frame writes per channel).
 *
 * // Time: Created - TASK-WS4.08
 */
public final class WsServerNotificationWriter {

    private static final Logger log = LoggerFactory.getLogger(WsServerNotificationWriter.class);

    /** Cancel reasons — must stay in sync with {@code ConsumerRestHandler} / design §5.8. */
    public static final String REASON_QUEUE_DELETED = "QUEUE_DELETED";
    public static final String REASON_EXCLUSIVE_EVICTION = "EXCLUSIVE_EVICTION";
    public static final String REASON_TOPIC_DELETED = "TOPIC_DELETED";
    public static final String REASON_ADMIN_CANCEL = "ADMIN_CANCEL";

    /** Frame type names. */
    public static final String TYPE_SUBSCRIPTION_CANCELLED = "subscription-cancelled";
    public static final String TYPE_REBALANCE = "rebalance";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final WsConnectionRegistry registry;

    public WsServerNotificationWriter(WsConnectionRegistry registry) {
        this.registry = Objects.requireNonNull(registry, "registry");
    }

    /**
     * Send a {@code subscription-cancelled} frame to the owning connection.
     *
     * <p>Silent no-op when:
     * <ul>
     *   <li>{@code connectionId} is not registered (connection already closed)</li>
     *   <li>the channel is no longer active (concurrent close)</li>
     * </ul>
     *
     * @param connectionId   owning WebSocket connection id (see {@link WsConnectionRegistry})
     * @param subscriptionId the cancelled subscription
     * @param reason         one of {@link #REASON_QUEUE_DELETED}, {@link #REASON_EXCLUSIVE_EVICTION},
     *                       {@link #REASON_TOPIC_DELETED}, {@link #REASON_ADMIN_CANCEL}
     * @param queueName      logical queue name (nullable — omitted from the frame when {@code null})
     */
    public void sendSubscriptionCancelled(String connectionId,
                                          String subscriptionId,
                                          String reason,
                                          String queueName) {
        Objects.requireNonNull(connectionId, "connectionId");
        Objects.requireNonNull(subscriptionId, "subscriptionId");
        Objects.requireNonNull(reason, "reason");

        WsConnectionContext ctx = registry.get(connectionId);
        if (ctx == null) {
            if (log.isDebugEnabled()) {
                log.debug("subscription-cancelled dropped: unknown connection id={} sub={}",
                    connectionId, subscriptionId);
            }
            return;
        }
        if (!ctx.isActive()) {
            if (log.isDebugEnabled()) {
                log.debug("subscription-cancelled dropped: inactive channel id={} sub={}",
                    connectionId, subscriptionId);
            }
            return;
        }

        ObjectNode frame = MAPPER.createObjectNode();
        frame.put("type", TYPE_SUBSCRIPTION_CANCELLED);
        frame.put("subscriptionId", subscriptionId);
        frame.put("reason", reason);
        if (queueName != null) {
            frame.put("queue", queueName);
        }

        writeJsonFrame(ctx, frame, connectionId, subscriptionId);
    }

    /**
     * Send a {@code rebalance} frame to the owning connection.
     *
     * <p>The assigned set is the member's COMPLETE new assignment (not a delta). The
     * revoked set lists partitions the member used to own but no longer will. Delivery
     * tags for revoked partitions must have already been invalidated on the tracker
     * before this frame is written.
     *
     * @param connectionId        owning WebSocket connection id
     * @param subscriptionId      the rebalanced subscription
     * @param assignedPartitions  partition ids now assigned (never {@code null}; may be empty)
     * @param revokedPartitions   partition ids just revoked (never {@code null}; may be empty)
     */
    public void sendRebalance(String connectionId,
                              String subscriptionId,
                              Collection<Integer> assignedPartitions,
                              Collection<Integer> revokedPartitions) {
        Objects.requireNonNull(connectionId, "connectionId");
        Objects.requireNonNull(subscriptionId, "subscriptionId");
        Objects.requireNonNull(assignedPartitions, "assignedPartitions");
        Objects.requireNonNull(revokedPartitions, "revokedPartitions");

        WsConnectionContext ctx = registry.get(connectionId);
        if (ctx == null) {
            if (log.isDebugEnabled()) {
                log.debug("rebalance dropped: unknown connection id={} sub={}",
                    connectionId, subscriptionId);
            }
            return;
        }
        if (!ctx.isActive()) {
            if (log.isDebugEnabled()) {
                log.debug("rebalance dropped: inactive channel id={} sub={}",
                    connectionId, subscriptionId);
            }
            return;
        }

        ObjectNode frame = MAPPER.createObjectNode();
        frame.put("type", TYPE_REBALANCE);
        frame.put("subscriptionId", subscriptionId);
        ArrayNode assigned = frame.putArray("assignedPartitions");
        for (Integer p : assignedPartitions) {
            if (p != null) assigned.add(p.intValue());
        }
        ArrayNode revoked = frame.putArray("revokedPartitions");
        for (Integer p : revokedPartitions) {
            if (p != null) revoked.add(p.intValue());
        }

        writeJsonFrame(ctx, frame, connectionId, subscriptionId);
    }

    private static void writeJsonFrame(WsConnectionContext ctx,
                                       ObjectNode frame,
                                       String connectionId,
                                       String subscriptionId) {
        try {
            ctx.sendFrame(MAPPER.writeValueAsString(frame));
        } catch (JsonProcessingException e) {
            // Should be impossible for a freshly-built ObjectNode.
            log.warn("Failed to serialise notification frame for connection={} sub={}",
                connectionId, subscriptionId, e);
        } catch (RuntimeException e) {
            // The channel may have closed in the narrow window between isActive() and writeAndFlush().
            // Log and move on — callers treat this as best-effort notification.
            if (log.isDebugEnabled()) {
                log.debug("Notification write failed for connection={} sub={}: {}",
                    connectionId, subscriptionId, e.toString());
            }
        }
    }
}
