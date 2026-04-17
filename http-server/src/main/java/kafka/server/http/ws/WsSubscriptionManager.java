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

import org.apache.kafka.common.TopicPartition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

/**
 * Manages the lifecycle of all subscriptions for a single WebSocket connection.
 *
 * <p>Each WebSocket connection owns one {@code WsSubscriptionManager} instance
 * which in turn owns one {@link SubscriptionContext} per active subscription.
 * The manager does NOT own the {@code wsConsumerExecutor}; a single broker-wide
 * executor is injected and shared across all connections. See design doc §21.1.
 *
 * <p>Thread safety: subscription registry is a {@link ConcurrentHashMap}.
 * {@link #subscribe} and {@link #unsubscribe} are safe to invoke from the Netty
 * event loop (typical caller) or from any other thread. {@link #cancelAll} is
 * invoked when the Netty channel closes and must also be safe under concurrent
 * {@link #grantCredits} arriving on in-flight {@code credits} frames.
 *
 * <p>The manager does not enforce a hard upper bound on the number of concurrent
 * subscriptions per connection; that policy lives in {@code WsFrameHandler}
 * (TASK-WS1.04) which will consult {@code wsMaxSubscriptionsPerConnection} from
 * {@link WsConfigs} before calling {@link #subscribe}.
 */
public final class WsSubscriptionManager {

    private static final Logger log = LoggerFactory.getLogger(WsSubscriptionManager.class);

    private final ConcurrentHashMap<String, SubscriptionContext> subscriptions = new ConcurrentHashMap<>();
    private final Executor wsConsumerExecutor;
    private final WsConsumerGroupCoordinator groupCoordinator;

    /**
     * @param wsConsumerExecutor shared executor on which fetch loops are scheduled.
     *                           Typically a {@link java.util.concurrent.ThreadPoolExecutor}
     *                           with {@code num.ws.consumer.threads} threads.
     */
    public WsSubscriptionManager(Executor wsConsumerExecutor) {
        this(wsConsumerExecutor, null);
    }

    /**
     * @param wsConsumerExecutor shared executor on which fetch loops are scheduled.
     * @param groupCoordinator   optional consumer-group coordinator (TASK-WS3.03) used to
     *                           form competing-consumer groups per queue. When {@code null},
     *                           subscriptions are single-member and no rebalance callbacks
     *                           are wired.
     */
    public WsSubscriptionManager(Executor wsConsumerExecutor, WsConsumerGroupCoordinator groupCoordinator) {
        this.wsConsumerExecutor = Objects.requireNonNull(wsConsumerExecutor, "wsConsumerExecutor");
        this.groupCoordinator = groupCoordinator;
    }

    /** Returns the (optional) consumer-group coordinator configured on this manager. */
    public WsConsumerGroupCoordinator groupCoordinator() {
        return groupCoordinator;
    }

    /**
     * Registers a new subscription and schedules its fetch loop on the shared executor.
     *
     * @param subscriptionId  client-chosen subscription identifier (unique within this manager)
     * @param queueName       logical queue name
     * @param topic           resolved Kafka topic name (e.g. {@code ws.<queueName>})
     * @param partitions      assigned partitions (informational; offsets are the authoritative input)
     * @param startOffsets    starting offset per partition
     * @param initialCredits  initial credit count for this subscription (&gt;= 0)
     * @param noAck           if {@code true}, auto-ack mode — delivery tags are not assigned
     * @param channel         the Netty channel for writing {@code deliver} frames
     * @throws IllegalStateException if {@code subscriptionId} is already registered
     * @throws NullPointerException  if any reference argument is null
     */
    public void subscribe(String subscriptionId,
                          String queueName,
                          String topic,
                          Set<TopicPartition> partitions,
                          Map<TopicPartition, Long> startOffsets,
                          int initialCredits,
                          boolean noAck,
                          Channel channel) {
        Objects.requireNonNull(subscriptionId, "subscriptionId");
        Objects.requireNonNull(queueName, "queueName");
        Objects.requireNonNull(topic, "topic");
        Objects.requireNonNull(partitions, "partitions");
        Objects.requireNonNull(startOffsets, "startOffsets");
        Objects.requireNonNull(channel, "channel");

        WsDeliveryTagTracker tagTracker = new WsDeliveryTagTracker();
        WsCreditManager creditManager = new WsCreditManager(initialCredits, channel);
        WsConsumerFetchLoop fetchLoop = new WsConsumerFetchLoop(
            subscriptionId, topic, startOffsets, creditManager, tagTracker, channel, noAck);

        SubscriptionContext ctx = new SubscriptionContext(
            subscriptionId, queueName, topic, tagTracker, creditManager, fetchLoop);

        SubscriptionContext existing = subscriptions.putIfAbsent(subscriptionId, ctx);
        if (existing != null) {
            throw new IllegalStateException("Subscription already exists: " + subscriptionId);
        }

        try {
            wsConsumerExecutor.execute(fetchLoop);
        } catch (RuntimeException e) {
            // Executor rejected (likely shut down) — roll back registry state before rethrowing.
            subscriptions.remove(subscriptionId, ctx);
            fetchLoop.stop();
            throw e;
        }

        if (log.isDebugEnabled()) {
            log.debug("Subscription started: id={} queue={} topic={} partitions={} credits={} noAck={}",
                subscriptionId, queueName, topic, partitions.size(), initialCredits, noAck);
        }
    }

    /**
     * Cancels the subscription with the given id and stops its fetch loop.
     *
     * <p>Returns the committable offsets computed by
     * {@link WsDeliveryTagTracker#getCommittableOffsets()} BEFORE clearing tracker
     * state, so the caller can perform a final offset commit.
     *
     * @param subscriptionId the subscription to cancel
     * @return committable offsets; empty map if the subscription was unknown
     */
    public Map<TopicPartition, Long> unsubscribe(String subscriptionId) {
        SubscriptionContext ctx = subscriptions.remove(subscriptionId);
        if (ctx == null) {
            return Map.of();
        }

        ctx.fetchLoop().stop();
        Map<TopicPartition, Long> offsets = ctx.deliveryTagTracker().getCommittableOffsets();
        ctx.deliveryTagTracker().clear();
        ctx.creditManager().reset();

        if (log.isDebugEnabled()) {
            log.debug("Subscription stopped: id={} finalOffsets={}", subscriptionId, offsets);
        }
        return offsets;
    }

    /**
     * Grants additional credits to an existing subscription.
     *
     * <p>No-op when {@code subscriptionId} is unknown — this can legitimately race
     * with {@link #unsubscribe} when a {@code credits} frame arrives after the
     * channel is closing.
     *
     * @param subscriptionId target subscription
     * @param credits        number of credits to add (must be &gt; 0)
     */
    public void grantCredits(String subscriptionId, int credits) {
        SubscriptionContext ctx = subscriptions.get(subscriptionId);
        if (ctx != null) {
            ctx.creditManager().grant(credits);
        }
    }

    /**
     * @return the context of the given subscription, or {@code null} if absent
     */
    public SubscriptionContext getSubscription(String subscriptionId) {
        return subscriptions.get(subscriptionId);
    }

    /**
     * Cancels ALL subscriptions. Intended for connection close.
     *
     * @return merged committable offsets across all cancelled subscriptions.
     *         If two subscriptions somehow produce offsets for the same
     *         {@link TopicPartition}, the larger offset wins (safer for
     *         at-least-once semantics).
     */
    public Map<TopicPartition, Long> cancelAll() {
        Map<TopicPartition, Long> merged = new HashMap<>();
        for (String subId : subscriptions.keySet()) {
            for (Map.Entry<TopicPartition, Long> e : unsubscribe(subId).entrySet()) {
                merged.merge(e.getKey(), e.getValue(), Math::max);
            }
        }
        return merged;
    }

    /**
     * @return the current number of active subscriptions.
     */
    public int activeCount() {
        return subscriptions.size();
    }

    /**
     * @return a snapshot of the currently-active subscription ids. Safe to iterate
     *         concurrently with {@link #subscribe} / {@link #unsubscribe}; the snapshot
     *         reflects the state at call time and will not throw
     *         {@link java.util.ConcurrentModificationException}.
     */
    public Set<String> activeSubscriptionIds() {
        return Set.copyOf(subscriptions.keySet());
    }

    /**
     * Invalidates delivery-tag state for a subscription after a consumer-group rebalance
     * revokes partitions (TASK-WS3.03). This is the subscription-manager entry point for
     * {@link WsConsumerGroupCoordinator.RebalanceListener#onPartitionsRevoked}.
     *
     * <p>No-op when the subscription is unknown (it may have unsubscribed concurrently).
     * ACKs that arrive after this call for the dropped tags return {@code false} from
     * {@link WsDeliveryTagTracker#ack} and MUST be silently ignored per the at-least-once
     * contract — the revoked partitions will be redelivered to the new owner.
     *
     * @param subscriptionId the affected subscription
     * @param revoked        the partitions to invalidate
     * @return number of pending tags that were dropped; {@code 0} when subscription unknown
     */
    public int invalidateRevokedPartitions(String subscriptionId,
                                           Collection<TopicPartition> revoked) {
        SubscriptionContext ctx = subscriptions.get(subscriptionId);
        if (ctx == null || revoked == null || revoked.isEmpty()) {
            return 0;
        }
        int dropped = ctx.deliveryTagTracker().invalidatePartitions(revoked);
        if (log.isDebugEnabled()) {
            log.debug("Rebalance revoke: sub={} revoked={} dropped={} pending tags",
                subscriptionId, revoked, dropped);
        }
        return dropped;
    }
}
