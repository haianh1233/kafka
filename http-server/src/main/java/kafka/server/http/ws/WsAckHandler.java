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

// Time: Created - TASK-WS1.16

package kafka.server.http.ws;

import org.apache.kafka.common.TopicPartition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Translates WebSocket {@code ack} / {@code nack} frames into Kafka offset commits.
 *
 * <p>Responsibilities (design doc §13.4, §12.2–12.3, §5.9):
 * <ul>
 *   <li>Resolve the subscription by id and delegate state transitions to
 *       {@link WsDeliveryTagTracker}.</li>
 *   <li>Collect committable offsets from the tracker and forward them to an injected
 *       {@link OffsetCommitSink} — the eventual integration target is the broker's
 *       {@code GroupCoordinator.commitOffsets} pipeline reached via {@code RequestChannel}.
 *       That wiring is intentionally deferred; see <b>Limitations</b> below.</li>
 *   <li>Optionally batch commits on a {@link ScheduledExecutorService} when
 *       {@code commitIntervalMs > 0}. When the interval is zero, commits are flushed
 *       on each successful ack/nack call.</li>
 *   <li>Emit {@code PRECONDITION_FAILED} error strings to the caller for unknown
 *       subscription or unknown delivery tag (§5.9). The caller (frame handler) is
 *       responsible for turning that string into an error frame on the wire.</li>
 * </ul>
 *
 * <h3>Thread-safety</h3>
 * <p>{@link #handleAck}, {@link #handleNack}, {@link #flush} and the scheduled
 * {@code periodicCommit} are safe to call concurrently. Per-subscription state
 * synchronisation is delegated to {@link WsDeliveryTagTracker} which itself uses
 * a private lock. The scheduler lifecycle is guarded by an {@link AtomicBoolean}
 * so that {@link #start()} and {@link #stop()} are idempotent.
 *
 * <h3>Limitations</h3>
 * <ul>
 *   <li>The actual {@code OffsetCommitRequest} routing through
 *       {@code RequestChannel} / {@code GroupCoordinator} is not wired here — the
 *       {@link OffsetCommitSink} is a placeholder for that integration.</li>
 *   <li>Redelivery trigger for {@code nack(requeue=true)} is not yet fed back into
 *       the fetch loop ({@link WsConsumerFetchLoop} does not yet expose a
 *       {@code redeliver} hook); the tracker correctly holds the offset in
 *       {@code NACKED_REQUEUE} so the commit watermark stays put, and the fetch loop
 *       will naturally re-read from the uncommitted offset on its next fetch.</li>
 *   <li>DLX publish for {@code nack(requeue=false)} is deferred to TASK-WS4.01.
 *       Until then, the message is effectively discarded — the commit advances past
 *       it exactly as the tracker's {@code NACKED_DISCARD} state dictates.</li>
 * </ul>
 */
public final class WsAckHandler {

    private static final Logger log = LoggerFactory.getLogger(WsAckHandler.class);

    /**
     * Sink for committable offsets. Integrators (tests, the eventual broker wiring)
     * implement this to route offsets to the appropriate destination — typically the
     * {@code OffsetCommit} handler of the consumer group coordinator.
     */
    @FunctionalInterface
    public interface OffsetCommitSink {
        /**
         * Called whenever the ack handler produces a non-empty committable offset map.
         *
         * @param subscriptionId the subscription that produced these offsets
         * @param offsets        partition → next-offset-to-commit (Kafka convention)
         */
        void commit(String subscriptionId, Map<TopicPartition, Long> offsets);
    }

    private final WsSubscriptionManager subscriptionManager;
    private final long commitIntervalMs;
    private final OffsetCommitSink sink;
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicBoolean stopped = new AtomicBoolean(false);
    private volatile ScheduledFuture<?> commitTask;

    /**
     * @param subscriptionManager  subscription registry; used to look up tag trackers
     * @param commitIntervalMs     batch flush interval; {@code 0} means flush-on-each-ack
     * @param sink                 offset commit sink (placeholder for {@code RequestChannel}
     *                             wiring)
     */
    public WsAckHandler(WsSubscriptionManager subscriptionManager,
                        long commitIntervalMs,
                        OffsetCommitSink sink) {
        this.subscriptionManager = Objects.requireNonNull(subscriptionManager, "subscriptionManager");
        this.sink = Objects.requireNonNull(sink, "sink");
        if (commitIntervalMs < 0) {
            throw new IllegalArgumentException("commitIntervalMs must be >= 0, got " + commitIntervalMs);
        }
        this.commitIntervalMs = commitIntervalMs;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "ws-ack-commit-timer");
            t.setDaemon(true);
            return t;
        });
    }

    // ---- Public API ----

    /**
     * Handles an {@code ack} frame.
     *
     * @param subscriptionId the subscription that acked
     * @param deliveryTag    the tag being acknowledged
     * @param multiple       if true, ack all tags up to and including {@code deliveryTag}
     * @return {@code null} on success, or an error message suitable for an error frame
     *         (starts with the {@code PRECONDITION_FAILED:} prefix per §5.9).
     */
    public String handleAck(String subscriptionId, long deliveryTag, boolean multiple) {
        SubscriptionContext ctx = subscriptionManager.getSubscription(subscriptionId);
        if (ctx == null) {
            return "PRECONDITION_FAILED: unknown subscription " + subscriptionId;
        }

        WsDeliveryTagTracker tracker = ctx.deliveryTagTracker();
        boolean stateChanged = multiple
            ? tracker.ackMultiple(deliveryTag)
            : tracker.ack(deliveryTag);

        if (!stateChanged) {
            // Distinguish "unknown tag" from "already-acked tag": if the tag has never been
            // within the assigned range, the ackMultiple path returns false for an out-of-range
            // upToTag; the single-ack path returns false both for unknown-and-for-already-acked.
            // For §5.9 compliance we surface PRECONDITION_FAILED only when the tag was never
            // assigned. Already-acked (idempotent double-ack) returns null = success.
            if (!isTagWithinAssignedRange(tracker, deliveryTag)) {
                log.debug("ACK for unknown tag: sub={} tag={} multiple={}",
                    subscriptionId, deliveryTag, multiple);
                return "PRECONDITION_FAILED: unknown delivery tag " + deliveryTag;
            }
            // Known but already-transitioned tag → idempotent success (no commit emitted).
            return null;
        }

        if (commitIntervalMs == 0L) {
            flushSubscription(subscriptionId, ctx);
        }
        return null;
    }

    /**
     * Handles a {@code nack} frame.
     *
     * @param subscriptionId the subscription that nacked
     * @param deliveryTag    the tag being negatively acknowledged
     * @param requeue        if true, message will be redelivered (offset NOT committed);
     *                       if false, DLX path (currently a placeholder) then commit
     * @param multiple       if true, nack all tags up to and including {@code deliveryTag}
     * @return {@code null} on success, or a {@code PRECONDITION_FAILED:}-prefixed error
     */
    public String handleNack(String subscriptionId, long deliveryTag,
                             boolean requeue, boolean multiple) {
        SubscriptionContext ctx = subscriptionManager.getSubscription(subscriptionId);
        if (ctx == null) {
            return "PRECONDITION_FAILED: unknown subscription " + subscriptionId;
        }

        WsDeliveryTagTracker tracker = ctx.deliveryTagTracker();

        // The tracker currently exposes only single-tag nack. For multiple=true we walk
        // from the lowest known tag up to the bound and nack each individually. This is
        // O(n) in the tag-range but matches the §12.2 batch nack semantics.
        if (multiple) {
            if (!isTagWithinAssignedRange(tracker, deliveryTag)) {
                return "PRECONDITION_FAILED: unknown delivery tag " + deliveryTag;
            }
            for (long tag = 1L; tag <= deliveryTag; tag++) {
                WsDeliveryTagTracker.PendingDelivery pd = tracker.nack(tag, requeue);
                if (pd != null) {
                    if (!requeue) {
                        handleDlxOrDiscard(ctx, pd);
                    } else {
                        triggerRedelivery(ctx, pd);
                    }
                }
            }
        } else {
            WsDeliveryTagTracker.PendingDelivery pd = tracker.nack(deliveryTag, requeue);
            if (pd == null) {
                // Distinguish unknown (never assigned) from already-transitioned (idempotent).
                if (!isTagWithinAssignedRange(tracker, deliveryTag)) {
                    return "PRECONDITION_FAILED: unknown delivery tag " + deliveryTag;
                }
                // Known but already acked/nacked → idempotent success.
                return null;
            }
            if (!requeue) {
                handleDlxOrDiscard(ctx, pd);
            } else {
                triggerRedelivery(ctx, pd);
            }
        }

        if (commitIntervalMs == 0L) {
            flushSubscription(subscriptionId, ctx);
        }
        return null;
    }

    /**
     * Flushes committable offsets for every known subscription.
     *
     * <p>Called on connection close, unsubscribe, and on each scheduled tick when
     * batch mode is enabled. Safe to call repeatedly — the tracker prunes committed
     * offsets so a second call with no intervening ack produces no commit.
     */
    public void flush() {
        for (String subId : subscriptionManager.activeSubscriptionIds()) {
            SubscriptionContext ctx = subscriptionManager.getSubscription(subId);
            if (ctx != null) {
                flushSubscription(subId, ctx);
            }
        }
    }

    /**
     * Starts the periodic commit timer. Idempotent — second call is a no-op.
     * No-op when {@code commitIntervalMs == 0} (flush-on-each-ack mode).
     */
    public void start() {
        if (commitIntervalMs == 0L) {
            return;
        }
        if (!started.compareAndSet(false, true)) {
            return;
        }
        commitTask = scheduler.scheduleAtFixedRate(
            this::periodicCommit,
            commitIntervalMs, commitIntervalMs, TimeUnit.MILLISECONDS);
        if (log.isDebugEnabled()) {
            log.debug("WS ACK commit timer started: interval={}ms", commitIntervalMs);
        }
    }

    /**
     * Stops the periodic commit timer and flushes remaining offsets one last time.
     * Idempotent — second call is a no-op.
     */
    public void stop() {
        if (!stopped.compareAndSet(false, true)) {
            return;
        }
        ScheduledFuture<?> task = commitTask;
        if (task != null) {
            task.cancel(false);
        }
        try {
            flush();
        } catch (RuntimeException e) {
            log.warn("Final flush during stop() failed", e);
        }
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(1, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            scheduler.shutdownNow();
        }
        if (log.isDebugEnabled()) {
            log.debug("WS ACK handler stopped");
        }
    }

    // ---- Internals ----

    private void flushSubscription(String subscriptionId, SubscriptionContext ctx) {
        // The tracker may have been cleared by unsubscribe; this simply returns an empty map.
        Map<TopicPartition, Long> offsets = ctx.deliveryTagTracker().getCommittableOffsets();
        if (offsets.isEmpty()) {
            return;
        }
        try {
            sink.commit(subscriptionId, offsets);
        } catch (RuntimeException e) {
            log.warn("Offset commit sink threw for sub={}", subscriptionId, e);
        }
    }

    private void periodicCommit() {
        try {
            flush();
        } catch (Exception e) {
            // Scheduled tasks that throw get suppressed — log so operators can see it.
            log.error("Periodic WS ACK commit failed", e);
        }
    }

    /**
     * Hook for NACK-without-requeue. Currently a TODO placeholder: integration with the
     * DLX publish path is deferred to TASK-WS4.01. The commit watermark is already
     * advanced by the tracker (NACKED_DISCARD is treated as terminal), so the message
     * is effectively dropped until DLX is wired.
     */
    private void handleDlxOrDiscard(SubscriptionContext ctx,
                                     WsDeliveryTagTracker.PendingDelivery pd) {
        log.debug("NACK without requeue: sub={} tp={} offset={} — DLX deferred to WS4.01",
            ctx.subscriptionId(), pd.topicPartition(), pd.offset());
    }

    /**
     * Hook for NACK-with-requeue. The tracker has marked the offset as NACKED_REQUEUE
     * so the commit watermark will not advance. Actively triggering a fresh fetch is a
     * placeholder pending WS1.15 exposing a redeliver entry point — today the fetch
     * loop re-reads the uncommitted offset on its next iteration anyway.
     */
    private void triggerRedelivery(SubscriptionContext ctx,
                                   WsDeliveryTagTracker.PendingDelivery pd) {
        log.debug("NACK with requeue: sub={} tp={} offset={} — redelivery relies on next fetch",
            ctx.subscriptionId(), pd.topicPartition(), pd.offset());
    }

    /**
     * Returns true if {@code tag} is within the range of tags ever assigned by the
     * tracker. Used to distinguish "unknown tag" (never assigned → PRECONDITION_FAILED)
     * from "already-transitioned tag" (idempotent success) per §5.9.
     *
     * <p>The tracker allocates tags monotonically from 1, so a tag is known iff
     * {@code 1 <= tag <= currentTagCounter()}.
     */
    private static boolean isTagWithinAssignedRange(WsDeliveryTagTracker tracker, long tag) {
        return tag >= 1L && tag <= tracker.currentTagCounter();
    }
}
