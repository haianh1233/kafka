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

// Time: Created - TASK-WS4.07

package kafka.server.http.ws;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Manages advanced WebSocket queue lifecycle behaviours beyond basic CRUD
 * (design doc §11.3, TASK-WS4.07):
 *
 * <ol>
 *   <li><b>Auto-delete</b> — a queue declared with {@code autoDelete=true} is
 *       deleted as soon as its last subscriber disconnects.</li>
 *   <li><b>Exclusive cleanup</b> — a queue declared with {@code exclusive=true}
 *       is deleted together with its bindings when the declaring connection
 *       closes.</li>
 *   <li><b>x-expires</b> — a queue is deleted after {@code N} milliseconds of
 *       zero consumers. The timer is cancelled as soon as any consumer
 *       subscribes.</li>
 *   <li><b>Server-generated names</b> — when the client supplies an empty
 *       queue name on {@code queue.declare} the broker produces
 *       {@code q.gen-<random-UUID>} via {@link #generateQueueName()}. These
 *       queues respect whatever {@code exclusive}/{@code autoDelete} flags the
 *       client set.</li>
 *   <li><b>Redeclare validation</b> — declaring an existing queue with
 *       different {@code durable}, {@code exclusive} or {@code autoDelete}
 *       flags throws {@link PreconditionFailedException} (mapped to
 *       {@code 409 Precondition Failed} / {@code PRECONDITION_FAILED} WS
 *       error by the caller). Queue arguments (e.g. {@code x-message-ttl})
 *       are intentionally <b>not</b> validated on redeclare so that
 *       idempotent reconnect flows don't trip on config drift.</li>
 * </ol>
 *
 * <h3>Scope</h3>
 * <p>This class does <b>not</b> itself delete queues — the destructive action
 * (removing the routing-metadata entry, deleting the backing Kafka topic,
 * tearing down bindings) lives in a future {@code QueueManager}. Instead the
 * caller injects a {@link Consumer} hook at construction time; every lifecycle
 * rule invokes the hook with the queue name when its condition is satisfied.
 * That decoupling keeps this class pure and testable.
 *
 * <h3>Thread-safety</h3>
 * <p>All mutating methods are safe for concurrent invocation:
 * <ul>
 *   <li>Subscriber counts are {@link AtomicInteger}s so the "unsubscribe
 *       count reaches zero" check is atomic even under concurrent unsubscribes
 *       — this guarantees auto-delete fires <b>exactly once</b>.</li>
 *   <li>Registry operations use {@link ConcurrentHashMap}.</li>
 *   <li>Expiry timers are scheduled on a shared
 *       {@link ScheduledExecutorService} and tracked in a
 *       {@link ConcurrentHashMap} so cancel/restart races don't leak
 *       tasks.</li>
 * </ul>
 *
 * // Time: Created - TASK-WS4.07
 */
public final class WsQueueLifecycleManager {

    private static final Logger log = LoggerFactory.getLogger(WsQueueLifecycleManager.class);

    /** Prefix for server-generated queue names. */
    public static final String GEN_PREFIX = "q.gen-";

    /**
     * Per-queue state maintained by this manager. Holds the subscriber count
     * plus the flags the declarer chose so unsubscribe/close hooks can decide
     * whether to fire a delete.
     */
    private static final class QueueState {
        final AtomicInteger subscriberCount = new AtomicInteger(0);
        final boolean autoDelete;
        final boolean exclusive;
        final String ownerConnectionId;

        QueueState(boolean autoDelete, boolean exclusive, String ownerConnectionId) {
            this.autoDelete = autoDelete;
            this.exclusive = exclusive;
            this.ownerConnectionId = ownerConnectionId;
        }
    }

    private final ConcurrentHashMap<String, QueueState> queues = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ScheduledFuture<?>> expiryTimers = new ConcurrentHashMap<>();
    /** connectionId → set of queue names exclusively owned by it. */
    private final ConcurrentHashMap<String, Set<String>> connectionQueues = new ConcurrentHashMap<>();

    private final ScheduledExecutorService scheduler;
    private final Consumer<String> deleteHook;

    /**
     * @param scheduler  shared scheduler used for {@code x-expires} timers; must
     *                   not be {@code null}. The manager does not own it and will
     *                   not shut it down.
     * @param deleteHook invoked with the queue name whenever a lifecycle rule
     *                   fires (auto-delete, x-expires, exclusive owner close).
     *                   The hook is responsible for the actual queue deletion —
     *                   routing-metadata removal, topic delete, binding teardown.
     */
    public WsQueueLifecycleManager(ScheduledExecutorService scheduler, Consumer<String> deleteHook) {
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
        this.deleteHook = Objects.requireNonNull(deleteHook, "deleteHook");
    }

    // ---------------------------------------------------------------
    // Registration
    // ---------------------------------------------------------------

    /**
     * Register a queue with its lifecycle flags. Idempotent per
     * {@code (queueName, flags)} — a second call with the same name overwrites
     * the stored flags (use {@link #validateRedeclare} first to enforce the
     * "flags must match" rule).
     *
     * @param queueName          queue name, non-null
     * @param autoDelete         delete after last subscriber leaves
     * @param exclusive          owner-scoped — deleted when owner connection closes
     * @param ownerConnectionId  non-null when {@code exclusive=true}; ignored
     *                           otherwise
     */
    public void registerQueue(String queueName, boolean autoDelete, boolean exclusive,
                              String ownerConnectionId) {
        Objects.requireNonNull(queueName, "queueName");
        QueueState state = new QueueState(autoDelete, exclusive, ownerConnectionId);
        queues.put(queueName, state);
        if (exclusive && ownerConnectionId != null) {
            connectionQueues
                .computeIfAbsent(ownerConnectionId, k -> ConcurrentHashMap.newKeySet())
                .add(queueName);
        }
    }

    /**
     * Remove all lifecycle bookkeeping for {@code queueName}. Called by the
     * delete hook after a successful queue deletion so timers and counts
     * don't leak.
     */
    public void unregisterQueue(String queueName) {
        Objects.requireNonNull(queueName, "queueName");
        QueueState state = queues.remove(queueName);
        if (state != null && state.exclusive && state.ownerConnectionId != null) {
            Set<String> owned = connectionQueues.get(state.ownerConnectionId);
            if (owned != null) {
                owned.remove(queueName);
                connectionQueues.computeIfPresent(state.ownerConnectionId,
                    (k, v) -> v.isEmpty() ? null : v);
            }
        }
        cancelExpiryTimer(queueName);
    }

    // ---------------------------------------------------------------
    // Subscribe / unsubscribe
    // ---------------------------------------------------------------

    /**
     * Record that a subscriber attached to {@code queueName}. Cancels any
     * pending {@code x-expires} timer — the queue is no longer idle.
     */
    public void onSubscribe(String queueName) {
        Objects.requireNonNull(queueName, "queueName");
        QueueState state = queues.get(queueName);
        if (state != null) {
            state.subscriberCount.incrementAndGet();
        }
        // Cancel even if we don't know the queue — the caller may have
        // scheduled a timer before calling registerQueue, and an orphaned
        // timer is still worth cancelling.
        cancelExpiryTimer(queueName);
    }

    /**
     * Record that a subscriber detached from {@code queueName}. If this was
     * the last subscriber and the queue was declared {@code autoDelete=true},
     * invoke the delete hook exactly once.
     *
     * <p>The atomic decrement-and-check is crucial: two unsubscribes happening
     * in parallel must not both trigger the hook (that would double-delete).
     * Only the thread that observes the count transition to zero calls the
     * hook.
     */
    public void onUnsubscribe(String queueName) {
        Objects.requireNonNull(queueName, "queueName");
        QueueState state = queues.get(queueName);
        if (state == null) {
            return;
        }
        // Decrement only when strictly positive so a stray unsubscribe without
        // a matching subscribe is a no-op (count stays at 0, no delete fires).
        // Loop with compareAndSet so we know the exact previous value observed
        // by *this* thread — only the thread that CAS'd 1 → 0 owns the
        // transition and fires auto-delete.
        int prev;
        while (true) {
            prev = state.subscriberCount.get();
            if (prev <= 0) {
                return; // no-op — no matching subscribe
            }
            if (state.subscriberCount.compareAndSet(prev, prev - 1)) {
                break;
            }
        }
        if (prev == 1 && state.autoDelete) {
            fireAutoDeleteOnce(queueName, state);
        }
    }

    /**
     * Returns the current subscriber count for {@code queueName}, or {@code 0}
     * if the queue is not registered. Primarily for tests and metrics.
     */
    public int subscriberCount(String queueName) {
        Objects.requireNonNull(queueName, "queueName");
        QueueState state = queues.get(queueName);
        return state == null ? 0 : state.subscriberCount.get();
    }

    // ---------------------------------------------------------------
    // x-expires timers
    // ---------------------------------------------------------------

    /**
     * Schedule an idle-timeout delete for {@code queueName} after
     * {@code expiresMs} milliseconds. A subsequent {@link #onSubscribe} or
     * {@link #cancelExpiryTimer} call cancels this timer; a subsequent
     * {@code startExpiryTimer} call replaces it.
     *
     * @throws IllegalArgumentException if {@code expiresMs < 0}
     */
    public void startExpiryTimer(String queueName, long expiresMs) {
        Objects.requireNonNull(queueName, "queueName");
        if (expiresMs < 0) {
            throw new IllegalArgumentException("expiresMs must be >= 0, got " + expiresMs);
        }
        ScheduledFuture<?> future = scheduler.schedule(() -> {
            // Recheck: any consumer attached between scheduling and firing
            // should keep the queue alive.
            QueueState state = queues.get(queueName);
            if (state != null && state.subscriberCount.get() > 0) {
                return;
            }
            try {
                deleteHook.accept(queueName);
            } catch (RuntimeException e) {
                log.warn("x-expires delete hook threw for queue {}", queueName, e);
            } finally {
                expiryTimers.remove(queueName);
            }
        }, expiresMs, TimeUnit.MILLISECONDS);
        ScheduledFuture<?> old = expiryTimers.put(queueName, future);
        if (old != null) {
            old.cancel(false);
        }
    }

    /**
     * Cancel the pending {@code x-expires} timer for {@code queueName} if one
     * is scheduled. No-op otherwise.
     */
    public void cancelExpiryTimer(String queueName) {
        Objects.requireNonNull(queueName, "queueName");
        ScheduledFuture<?> future = expiryTimers.remove(queueName);
        if (future != null) {
            future.cancel(false);
        }
    }

    // ---------------------------------------------------------------
    // Connection close — exclusive queue cleanup
    // ---------------------------------------------------------------

    /**
     * Invoke when a WebSocket connection closes. Every queue declared
     * {@code exclusive=true} with this connection as its owner is deleted
     * (via {@link #deleteHook}) and returned in the result list for the caller
     * to cascade to bindings / topic teardown.
     *
     * @return unmodifiable list of queue names that were deleted because they
     *         were exclusively owned by {@code connectionId}. Never
     *         {@code null}; empty when no matching queues existed.
     */
    public List<String> onConnectionClose(String connectionId) {
        Objects.requireNonNull(connectionId, "connectionId");
        Set<String> owned = connectionQueues.remove(connectionId);
        if (owned == null || owned.isEmpty()) {
            return Collections.emptyList();
        }
        // Snapshot before iteration — the delete hook may call back into
        // unregisterQueue which mutates connectionQueues.
        List<String> snapshot = new ArrayList<>(owned);
        List<String> deleted = new ArrayList<>(snapshot.size());
        for (String queueName : snapshot) {
            QueueState state = queues.get(queueName);
            if (state == null || !state.exclusive) {
                continue;
            }
            if (!connectionId.equals(state.ownerConnectionId)) {
                continue;
            }
            try {
                deleteHook.accept(queueName);
                deleted.add(queueName);
            } catch (RuntimeException e) {
                log.warn("exclusive delete hook threw for queue {} on close of {}",
                    queueName, connectionId, e);
            } finally {
                cancelExpiryTimer(queueName);
            }
        }
        return Collections.unmodifiableList(deleted);
    }

    // ---------------------------------------------------------------
    // Server-generated names
    // ---------------------------------------------------------------

    /**
     * Generate a fresh server-assigned queue name of the form
     * {@code q.gen-<random-UUID>}. Uses a cryptographically random UUID so
     * the name is unguessable.
     */
    public static String generateQueueName() {
        return GEN_PREFIX + UUID.randomUUID();
    }

    // ---------------------------------------------------------------
    // Redeclare validation
    // ---------------------------------------------------------------

    /**
     * Ensure a redeclare of {@code existing} with the supplied flags is a
     * no-op (i.e. the new flags match the stored ones). Arguments are
     * <b>not</b> validated — a client that reconnects with a slightly
     * different {@code x-message-ttl} still succeeds so the reconnect path
     * stays idempotent.
     *
     * @throws PreconditionFailedException when any of
     *         {@code durable}/{@code exclusive}/{@code autoDelete} differs
     *         from the existing queue
     */
    public void validateRedeclare(QueueMetadata existing,
                                  boolean durable,
                                  boolean exclusive,
                                  boolean autoDelete) {
        Objects.requireNonNull(existing, "existing");
        if (existing.durable() != durable
                || existing.exclusive() != exclusive
                || existing.autoDelete() != autoDelete) {
            throw new PreconditionFailedException(
                "PRECONDITION_FAILED: queue '" + existing.name()
                    + "' flags differ (existing durable=" + existing.durable()
                    + ", exclusive=" + existing.exclusive()
                    + ", autoDelete=" + existing.autoDelete()
                    + "; requested durable=" + durable
                    + ", exclusive=" + exclusive
                    + ", autoDelete=" + autoDelete + ")");
        }
    }

    // ---------------------------------------------------------------
    // Internals
    // ---------------------------------------------------------------

    /**
     * Marker key that signals the auto-delete hook has already fired for this
     * queue. A second unsubscribe that would otherwise transition to 0 again
     * (e.g. if the caller double-decrements) is a no-op.
     */
    private final ConcurrentHashMap<String, Boolean> autoDeleteFired = new ConcurrentHashMap<>();

    private void fireAutoDeleteOnce(String queueName, QueueState state) {
        if (autoDeleteFired.putIfAbsent(queueName, Boolean.TRUE) != null) {
            return;
        }
        try {
            deleteHook.accept(queueName);
        } catch (RuntimeException e) {
            log.warn("auto-delete hook threw for queue {}", queueName, e);
        } finally {
            cancelExpiryTimer(queueName);
        }
    }

    /**
     * Thrown by {@link #validateRedeclare} when a redeclared queue's flags
     * differ from the existing queue. Callers map this to the transport-level
     * {@code PRECONDITION_FAILED} error (AMQP 406 / HTTP 409 / WS error frame).
     */
    public static final class PreconditionFailedException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        public PreconditionFailedException(String message) {
            super(message);
        }
    }
}
