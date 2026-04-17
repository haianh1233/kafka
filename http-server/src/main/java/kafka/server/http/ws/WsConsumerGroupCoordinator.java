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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Bridges WebSocket subscriptions to Kafka consumer groups (design doc §9.2, §11.4).
 *
 * <p>When multiple WebSocket subscribers target the same logical queue they are joined
 * into a single consumer group named {@code ws.{queueName}}. The coordinator performs
 * <em>range-style</em> partition distribution across members and notifies each member
 * via a {@link RebalanceListener} whenever the membership changes. Delivery tags for
 * revoked partitions must be invalidated by the listener (redelivery expected per
 * at-least-once contract).
 *
 * <h3>Scope</h3>
 *
 * <p>This implementation is deliberately <b>in-memory</b>: it does not submit
 * {@code JoinGroupRequest} / {@code SyncGroupRequest} to the live broker. The broker
 * integration is out of scope for TASK-WS3.03 — the class establishes the API and the
 * state machine (join → assign → revoke → leave) so that higher-level code
 * ({@link WsSubscriptionManager}, {@link WsConsumerFetchLoop}, {@link WsFrameHandler})
 * can be wired against a stable contract.
 *
 * <h3>Thread-safety</h3>
 *
 * <p>Mutating methods ({@link #joinGroup}, {@link #leaveGroup}) are serialised per
 * group via a per-group lock object kept inside {@link Group}. Callbacks are invoked
 * under that lock; listeners must not re-enter the coordinator synchronously or a
 * deadlock can occur. The {@code subscriptionId → groupId} reverse index is a
 * {@link ConcurrentHashMap} and is safe to read without the lock.
 *
 * <h3>Rebalance semantics</h3>
 *
 * <ul>
 *   <li>Existing members receive {@link RebalanceListener#onPartitionsRevoked} for
 *       partitions they will no longer own, then
 *       {@link RebalanceListener#onPartitionsAssigned} for their new complete
 *       assignment (assigned-set semantics, not delta).</li>
 *   <li>New joiners receive {@link RebalanceListener#onPartitionsAssigned} with their
 *       initial partition set. No revoke callback is fired for joiners.</li>
 *   <li>On {@link #leaveGroup}, the leaving member is not notified; remaining members
 *       receive a fresh assignment (including revoke of partitions that were shifted
 *       off them, if any).</li>
 * </ul>
 */
public final class WsConsumerGroupCoordinator {

    private static final Logger log = LoggerFactory.getLogger(WsConsumerGroupCoordinator.class);

    /** Consumer group names are prefixed with {@code ws.} so they cannot collide with
     *  application-provided groups. */
    public static final String GROUP_PREFIX = "ws.";

    /**
     * Rebalance callback contract. Implementations must not block for long — callbacks
     * are invoked under the group's internal lock.
     */
    public interface RebalanceListener {
        /** Called with the member's complete assigned partition set after a rebalance. */
        void onPartitionsAssigned(String subscriptionId, Collection<TopicPartition> assigned);

        /** Called with the partitions the member used to own but no longer will. */
        void onPartitionsRevoked(String subscriptionId, Collection<TopicPartition> revoked);
    }

    /** Internal group state. Guarded by its own {@link #lock} object. */
    private static final class Group {
        final String queueName;
        final String groupId;
        final int numPartitions;
        final Object lock = new Object();
        /** Member order matters for range-style assignment (insertion order). */
        final LinkedHashMap<String, Member> members = new LinkedHashMap<>();

        Group(String queueName, int numPartitions) {
            this.queueName = queueName;
            this.groupId = GROUP_PREFIX + queueName;
            this.numPartitions = numPartitions;
        }
    }

    private static final class Member {
        final String subscriptionId;
        final RebalanceListener listener;
        /** Last complete assignment notified to the listener. */
        Set<TopicPartition> currentAssignment = Collections.emptySet();

        Member(String subscriptionId, RebalanceListener listener) {
            this.subscriptionId = subscriptionId;
            this.listener = listener;
        }
    }

    /** queueName → Group. */
    private final ConcurrentHashMap<String, Group> groups = new ConcurrentHashMap<>();

    /** subscriptionId → queueName (reverse index for quick lookups). */
    private final ConcurrentHashMap<String, String> subscriptionIndex = new ConcurrentHashMap<>();

    // ------------------------------------------------------------------
    //  Public API
    // ------------------------------------------------------------------

    /**
     * Returns the consumer-group id for a given queue. Pure function, exposed for
     * logging/diagnostics.
     */
    public static String groupNameFor(String queueName) {
        Objects.requireNonNull(queueName, "queueName");
        if (queueName.isEmpty()) {
            throw new IllegalArgumentException("queueName must not be empty");
        }
        return GROUP_PREFIX + queueName;
    }

    /**
     * Registers {@code subscriptionId} as a member of the consumer group for
     * {@code queueName} and triggers a rebalance. On completion the returned future
     * carries the partitions this new member has been assigned. Existing members, if
     * any, receive {@link RebalanceListener#onPartitionsRevoked} / {@code onPartitionsAssigned}
     * via their listener.
     *
     * @param queueName      logical queue name (group id will be {@code ws.<queueName>})
     * @param subscriptionId unique subscription identifier (must not already be a member of any group)
     * @param numPartitions  partition count of the backing topic; must be &gt; 0. Re-used across
     *                       joins to the same group — subsequent joins MUST pass the same value or
     *                       an {@link IllegalStateException} is raised.
     * @param listener       rebalance listener for this subscription
     */
    public CompletableFuture<Collection<TopicPartition>> joinGroup(
            String queueName, String subscriptionId, int numPartitions, RebalanceListener listener) {
        Objects.requireNonNull(queueName, "queueName");
        Objects.requireNonNull(subscriptionId, "subscriptionId");
        Objects.requireNonNull(listener, "listener");
        if (numPartitions <= 0) {
            throw new IllegalArgumentException("numPartitions must be > 0 but was " + numPartitions);
        }

        CompletableFuture<Collection<TopicPartition>> result = new CompletableFuture<>();
        try {
            Group group = groups.computeIfAbsent(queueName, q -> new Group(q, numPartitions));

            Collection<TopicPartition> assignedToNew;
            synchronized (group.lock) {
                if (group.numPartitions != numPartitions) {
                    throw new IllegalStateException(
                        "numPartitions mismatch for group " + group.groupId
                            + ": existing=" + group.numPartitions + " new=" + numPartitions);
                }
                if (group.members.containsKey(subscriptionId)) {
                    throw new IllegalStateException(
                        "subscription " + subscriptionId + " already in group " + group.groupId);
                }

                Member member = new Member(subscriptionId, listener);
                group.members.put(subscriptionId, member);
                subscriptionIndex.put(subscriptionId, queueName);

                Map<String, Set<TopicPartition>> newAssignment = rangeAssign(
                    group.queueName, new ArrayList<>(group.members.keySet()), group.numPartitions);

                // Notify every existing member that saw a change; the joiner is handled last
                // so its future resolves with its complete assignment.
                applyAssignment(group, newAssignment);

                assignedToNew = new ArrayList<>(member.currentAssignment);
                log.debug("joinGroup queue={} sub={} assigned={} members={}",
                    queueName, subscriptionId, assignedToNew, group.members.keySet());
            }
            result.complete(assignedToNew);
        } catch (RuntimeException e) {
            result.completeExceptionally(e);
        }
        return result;
    }

    /**
     * Removes {@code subscriptionId} from the group for {@code queueName} and
     * rebalances the remaining members. Silently succeeds when the member or the group
     * is unknown — callers often invoke this from unsubscribe/disconnect paths that
     * can race with each other.
     */
    public CompletableFuture<Void> leaveGroup(String queueName, String subscriptionId) {
        Objects.requireNonNull(queueName, "queueName");
        Objects.requireNonNull(subscriptionId, "subscriptionId");

        CompletableFuture<Void> result = new CompletableFuture<>();
        try {
            Group group = groups.get(queueName);
            if (group == null) {
                // Clean any stray index entry so callers don't see ghost state.
                subscriptionIndex.remove(subscriptionId);
                result.complete(null);
                return result;
            }

            synchronized (group.lock) {
                Member removed = group.members.remove(subscriptionId);
                if (removed == null) {
                    subscriptionIndex.remove(subscriptionId);
                    result.complete(null);
                    return result;
                }

                // Reset the leaver's tracked assignment — no further callback.
                subscriptionIndex.remove(subscriptionId);
                removed.currentAssignment = Collections.emptySet();

                if (group.members.isEmpty()) {
                    // Last member gone: drop the group entirely.
                    groups.remove(queueName, group);
                    log.debug("leaveGroup queue={} sub={} (group removed)", queueName, subscriptionId);
                } else {
                    Map<String, Set<TopicPartition>> newAssignment = rangeAssign(
                        group.queueName, new ArrayList<>(group.members.keySet()), group.numPartitions);
                    applyAssignment(group, newAssignment);
                    log.debug("leaveGroup queue={} sub={} remaining={}",
                        queueName, subscriptionId, group.members.keySet());
                }
            }
            result.complete(null);
        } catch (RuntimeException e) {
            result.completeExceptionally(e);
        }
        return result;
    }

    // ------------------------------------------------------------------
    //  Introspection (mainly for tests / diagnostics)
    // ------------------------------------------------------------------

    /** Returns the consumer-group id a subscription currently belongs to, or {@code null}. */
    public String groupIdFor(String subscriptionId) {
        String queue = subscriptionIndex.get(subscriptionId);
        return queue == null ? null : GROUP_PREFIX + queue;
    }

    /** Returns the current assignment of {@code subscriptionId}, or {@code null} if unknown. */
    public Collection<TopicPartition> assignmentOf(String subscriptionId) {
        String queue = subscriptionIndex.get(subscriptionId);
        if (queue == null) {
            return null;
        }
        Group group = groups.get(queue);
        if (group == null) {
            return null;
        }
        synchronized (group.lock) {
            Member m = group.members.get(subscriptionId);
            return m == null ? null : new ArrayList<>(m.currentAssignment);
        }
    }

    /** Returns the number of members currently in the group for {@code queueName}. */
    public int memberCount(String queueName) {
        Group g = groups.get(queueName);
        if (g == null) {
            return 0;
        }
        synchronized (g.lock) {
            return g.members.size();
        }
    }

    // ------------------------------------------------------------------
    //  Assignment strategy — range-style (contiguous blocks)
    // ------------------------------------------------------------------

    /**
     * Distributes {@code numPartitions} across {@code members} using a range-style
     * strategy: sort members by subscriptionId for determinism, give the first
     * {@code extra} members one additional partition.
     *
     * <p>The returned map is complete: every member id is present even if its
     * assignment is empty (zero partitions, rare when {@code numPartitions < members.size()}).
     */
    static Map<String, Set<TopicPartition>> rangeAssign(
            String queueName, List<String> memberIds, int numPartitions) {
        String topic = GROUP_PREFIX + queueName;
        List<String> sorted = new ArrayList<>(memberIds);
        Collections.sort(sorted);

        Map<String, Set<TopicPartition>> out = new HashMap<>();
        int n = sorted.size();
        if (n == 0) {
            return out;
        }
        int perMember = numPartitions / n;
        int extra = numPartitions % n;

        int partitionCursor = 0;
        for (int i = 0; i < n; i++) {
            String member = sorted.get(i);
            int slice = perMember + (i < extra ? 1 : 0);
            Set<TopicPartition> set = new HashSet<>();
            for (int p = 0; p < slice; p++) {
                set.add(new TopicPartition(topic, partitionCursor++));
            }
            out.put(member, set);
        }
        return out;
    }

    // ------------------------------------------------------------------
    //  Private: apply an assignment and fire callbacks
    // ------------------------------------------------------------------

    /** Must be called with {@code group.lock} held. */
    private void applyAssignment(Group group, Map<String, Set<TopicPartition>> newAssignment) {
        for (Map.Entry<String, Member> entry : group.members.entrySet()) {
            Member member = entry.getValue();
            Set<TopicPartition> target = newAssignment.getOrDefault(entry.getKey(), Collections.emptySet());
            Set<TopicPartition> prior = member.currentAssignment;
            if (prior.equals(target)) {
                continue;
            }
            // Revoked = prior - target
            Set<TopicPartition> revoked = new HashSet<>(prior);
            revoked.removeAll(target);
            // Always fire revoke first (even if empty, only fire when there is real revocation
            // to avoid noise in callbacks) so the listener can flush stale tag state before
            // the new assignment lands.
            if (!revoked.isEmpty()) {
                safeRevoke(member, revoked);
            }
            // Always fire assigned with the FULL new assignment (not the delta) so listeners
            // can replace their partition set in one step.
            safeAssign(member, target);
            member.currentAssignment = target;
        }
    }

    private static void safeAssign(Member m, Set<TopicPartition> assigned) {
        try {
            m.listener.onPartitionsAssigned(m.subscriptionId, assigned);
        } catch (RuntimeException e) {
            log.warn("RebalanceListener.onPartitionsAssigned threw for sub={}", m.subscriptionId, e);
        }
    }

    private static void safeRevoke(Member m, Set<TopicPartition> revoked) {
        try {
            m.listener.onPartitionsRevoked(m.subscriptionId, revoked);
        } catch (RuntimeException e) {
            log.warn("RebalanceListener.onPartitionsRevoked threw for sub={}", m.subscriptionId, e);
        }
    }
}
