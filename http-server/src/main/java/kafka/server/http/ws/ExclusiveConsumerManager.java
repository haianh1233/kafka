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

// Time: Created - TASK-WS3.04

package kafka.server.http.ws;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages exclusive consumer locks on WebSocket queues (TASK-WS3.04, design §11.3).
 *
 * <p>A queue may have at most one <em>exclusive</em> subscriber at any time. When a
 * subscribe frame carries {@code exclusive=true}, the caller attempts to acquire the
 * lock here before creating a {@link SubscriptionContext}. A second subscriber —
 * whether exclusive or not — whose queue is already held is rejected with the
 * {@code EXCLUSIVE_CONSUMER} error code at the frame-handler layer; enforcement at
 * this layer is mechanical (the returned boolean from
 * {@link #tryAcquireExclusive(String, String)}).
 *
 * <h3>Atomicity</h3>
 * Acquire is implemented with {@link ConcurrentHashMap#putIfAbsent(Object, Object)};
 * simultaneous subscribe requests from two connections race for the lock and exactly
 * one wins.
 *
 * <h3>Connection lifecycle</h3>
 * On connection close, {@link #onConnectionClose(String)} returns the list of queues
 * whose exclusive lock this connection held so the caller (connection-close hook in
 * {@link WsConnectionContext}) can schedule cascading cleanup (queue delete, binding
 * removal) per the design doc's rule that the exclusive queue is deleted when the
 * owning connection goes away.
 *
 * <h3>Per-connection index</h3>
 * A reverse map {@code connection → queues} is maintained so
 * {@link #onConnectionClose(String)} is O(k) in the number of locks held by the
 * departing connection rather than O(n) in the global lock registry.
 *
 * // Time: Created - TASK-WS3.04
 */
public final class ExclusiveConsumerManager {

    /** queueName → connectionId of the current exclusive holder. */
    private final ConcurrentHashMap<String, String> exclusiveLocks = new ConcurrentHashMap<>();

    /** connectionId → set of queue names this connection holds exclusively. */
    private final ConcurrentHashMap<String, Set<String>> connectionQueues = new ConcurrentHashMap<>();

    /**
     * Attempt to acquire the exclusive lock on {@code queueName} for {@code connectionId}.
     *
     * <p>Atomic. When two connections call this concurrently, exactly one wins.
     *
     * @return {@code true} if the lock is now held by {@code connectionId} (whether
     *         freshly acquired or already held by the same connection); {@code false}
     *         if another connection holds the lock.
     */
    public boolean tryAcquireExclusive(String queueName, String connectionId) {
        Objects.requireNonNull(queueName, "queueName");
        Objects.requireNonNull(connectionId, "connectionId");

        String existing = exclusiveLocks.putIfAbsent(queueName, connectionId);
        if (existing == null) {
            // We won the race — record the reverse mapping so onConnectionClose is fast.
            connectionQueues
                .computeIfAbsent(connectionId, k -> ConcurrentHashMap.newKeySet())
                .add(queueName);
            return true;
        }
        // Same connection re-acquiring its own lock is idempotent.
        return existing.equals(connectionId);
    }

    /**
     * Release the exclusive lock on {@code queueName} if (and only if) {@code connectionId}
     * currently holds it. A non-owner calling release is a no-op.
     */
    public void releaseExclusive(String queueName, String connectionId) {
        Objects.requireNonNull(queueName, "queueName");
        Objects.requireNonNull(connectionId, "connectionId");

        // remove(K, V) only removes when the mapping matches — guards against a
        // late release from a connection that was evicted in a rebalance.
        if (exclusiveLocks.remove(queueName, connectionId)) {
            Set<String> queues = connectionQueues.get(connectionId);
            if (queues != null) {
                queues.remove(queueName);
                // Clear the per-connection entry if empty so memory doesn't leak across
                // many transient connections.
                connectionQueues.computeIfPresent(connectionId,
                    (k, v) -> v.isEmpty() ? null : v);
            }
        }
    }

    /** @return {@code true} iff any connection currently holds an exclusive lock on {@code queueName}. */
    public boolean isExclusivelyLocked(String queueName) {
        Objects.requireNonNull(queueName, "queueName");
        return exclusiveLocks.containsKey(queueName);
    }

    /** @return {@code true} iff {@code connectionId} currently holds the exclusive lock on {@code queueName}. */
    public boolean isLockedBy(String queueName, String connectionId) {
        Objects.requireNonNull(queueName, "queueName");
        Objects.requireNonNull(connectionId, "connectionId");
        return connectionId.equals(exclusiveLocks.get(queueName));
    }

    /**
     * Release every exclusive lock held by {@code connectionId}. Called from the
     * connection-close hook.
     *
     * @return list of queue names whose lock was released — callers may use this
     *         list to cascade queue deletion for subscription-level exclusive queues
     *         (design §11.3). Never {@code null}; empty when the connection held no locks.
     */
    public List<String> onConnectionClose(String connectionId) {
        Objects.requireNonNull(connectionId, "connectionId");

        Set<String> held = connectionQueues.remove(connectionId);
        if (held == null || held.isEmpty()) {
            return Collections.emptyList();
        }
        // Snapshot before removing from the global registry; ConcurrentHashMap.newKeySet
        // is a live view so we must copy before mutation.
        List<String> released = new ArrayList<>(held);
        for (String queueName : released) {
            // remove(K, V) so a concurrent release by another thread can't accidentally
            // wipe a lock just re-acquired by a new connection with the same id.
            exclusiveLocks.remove(queueName, connectionId);
        }
        return released;
    }
}
