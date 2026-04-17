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
// Time: Update - TASK-WS3.08 - added metrics recording
// Time: Update - TASK-WS4.01 - added DLX path

package kafka.server.http.ws;

import org.apache.kafka.common.TopicPartition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
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
 *   <li>DLX publish for {@code nack(requeue=false)} is wired through the optional
 *       {@link DeadLetterHook} (TASK-WS4.01). When no hook is injected the historical
 *       behaviour is preserved: the offset advances past the NACKed message and the
 *       record is effectively dropped.</li>
 * </ul>
 *
 * <h3>DLX integration (TASK-WS4.01)</h3>
 * <p>When a {@link DeadLetterHook} is supplied, NACKs with {@code requeue=false}
 * are routed through the hook BEFORE the commit watermark is allowed to advance.
 * The atomicity invariant requires DLX produce to acknowledge before the original
 * offset is committed; we enforce it by deferring the {@code NACKED_DISCARD}
 * transition until the hook future resolves. On hook failure the tag is marked
 * {@code NACKED_REQUEUE} instead so the original message is redelivered (the
 * commit stays put), preserving at-least-once semantics on transient DLX outages.
 *
 * <p>Because the {@link WsDeliveryTagTracker} only stores
 * {@code (TopicPartition, offset)} (memory bound, §21.3), the DLX hook itself is
 * responsible for sourcing the original record bytes — typically by attaching to
 * the {@link WsConsumerFetchLoop} record cache or by re-fetching from Kafka. The
 * fetch-loop / hook integration is the production concern; here we only own the
 * tracker-state transitions and commit ordering.
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

    /**
     * Pluggable dead-letter hook (TASK-WS4.01). Consulted on
     * {@code nack(requeue=false)} BEFORE the tag is transitioned to a terminal
     * state. The returned future MUST complete only once the DLX produce has
     * been acknowledged (or definitively failed). On success the tag is
     * transitioned to {@code NACKED_DISCARD} and the commit watermark is
     * allowed to advance; on failure the tag is transitioned to
     * {@code NACKED_REQUEUE} so the message will be redelivered.
     *
     * <p>While the future is in flight the tag remains in {@code PENDING} —
     * this is what enforces the spec's atomicity invariant: the offset cannot
     * be committed before DLX produce acks.
     */
    @FunctionalInterface
    public interface DeadLetterHook {
        /**
         * Dead-letter the record bound to {@code pd}. The hook is responsible
         * for sourcing the record's bytes and headers (typically by reaching
         * into the fetch loop's recent-record cache) and routing through the
         * DLX configured on {@code queueName}.
         *
         * @param queueName the source queue name (carried on the
         *                  {@link SubscriptionContext})
         * @param pd        the pending delivery (TopicPartition + offset)
         * @return future completing when DLX produce acks; failed future on
         *         transient produce error (caller will requeue)
         */
        CompletableFuture<Void> deadLetter(String queueName, WsDeliveryTagTracker.PendingDelivery pd);
    }

    private final WsSubscriptionManager subscriptionManager;
    private final long commitIntervalMs;
    private final OffsetCommitSink sink;
    private final WsMetrics metrics;
    private final DeadLetterHook deadLetterHook;
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicBoolean stopped = new AtomicBoolean(false);
    private volatile ScheduledFuture<?> commitTask;

    /**
     * Convenience constructor with no metrics and no DLX hook.
     */
    public WsAckHandler(WsSubscriptionManager subscriptionManager,
                        long commitIntervalMs,
                        OffsetCommitSink sink) {
        this(subscriptionManager, commitIntervalMs, sink, null, null);
    }

    /**
     * Convenience constructor with metrics but no DLX hook (preserved for
     * pre-WS4.01 call sites).
     */
    public WsAckHandler(WsSubscriptionManager subscriptionManager,
                        long commitIntervalMs,
                        OffsetCommitSink sink,
                        WsMetrics metrics) {
        this(subscriptionManager, commitIntervalMs, sink, metrics, null);
    }

    /**
     * @param subscriptionManager  subscription registry; used to look up tag trackers
     * @param commitIntervalMs     batch flush interval; {@code 0} means flush-on-each-ack
     * @param sink                 offset commit sink (placeholder for {@code RequestChannel}
     *                             wiring)
     * @param metrics              WS metrics sink (may be {@code null} — useful for tests)
     * @param deadLetterHook       optional DLX hook (TASK-WS4.01); when {@code null} the
     *                             pre-WS4.01 behaviour is preserved (NACK no-requeue
     *                             advances the commit watermark immediately, message lost)
     */
    public WsAckHandler(WsSubscriptionManager subscriptionManager,
                        long commitIntervalMs,
                        OffsetCommitSink sink,
                        WsMetrics metrics,
                        DeadLetterHook deadLetterHook) {
        this.subscriptionManager = Objects.requireNonNull(subscriptionManager, "subscriptionManager");
        this.sink = Objects.requireNonNull(sink, "sink");
        if (commitIntervalMs < 0) {
            throw new IllegalArgumentException("commitIntervalMs must be >= 0, got " + commitIntervalMs);
        }
        this.commitIntervalMs = commitIntervalMs;
        this.metrics = metrics;
        this.deadLetterHook = deadLetterHook;
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

        if (metrics != null) {
            metrics.ackRate.mark();
        }
        if (commitIntervalMs == 0L) {
            flushSubscription(subscriptionId, ctx);
        }
        return null;
    }

    /**
     * Handles a {@code nack} frame.
     *
     * <p>For {@code requeue=true}, the offset is preserved and the message will
     * be redelivered on the next fetch (see {@link #triggerRedelivery}).
     *
     * <p>For {@code requeue=false}: when a {@link DeadLetterHook} is wired
     * (TASK-WS4.01) the tag is held in {@code PENDING} until the hook's future
     * completes — this is the spec's atomicity invariant: DLX produce MUST ack
     * before the original offset is committed. On hook success the tag becomes
     * {@code NACKED_DISCARD} (commit advances); on hook failure
     * {@code NACKED_REQUEUE} (offset stays put, message redelivered). Without a
     * hook the legacy behaviour is preserved (immediate {@code NACKED_DISCARD},
     * message lost).
     *
     * @param subscriptionId the subscription that nacked
     * @param deliveryTag    the tag being negatively acknowledged
     * @param requeue        if true, message will be redelivered (offset NOT committed);
     *                       if false, DLX path (with hook) or discard (without hook)
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

        if (multiple) {
            if (!isTagWithinAssignedRange(tracker, deliveryTag)) {
                return "PRECONDITION_FAILED: unknown delivery tag " + deliveryTag;
            }
            for (long tag = 1L; tag <= deliveryTag; tag++) {
                processSingleNack(ctx, tracker, subscriptionId, tag, requeue);
            }
        } else {
            String err = processSingleNack(ctx, tracker, subscriptionId, deliveryTag, requeue);
            if (err != null) {
                return err;
            }
        }

        if (commitIntervalMs == 0L) {
            flushSubscription(subscriptionId, ctx);
        }
        return null;
    }

    /**
     * Per-tag nack processing. Returns {@code null} on success, or a
     * {@code PRECONDITION_FAILED:}-prefixed error message when the tag was
     * never assigned. For {@code multiple=true} the caller ignores the return
     * value (per-tag failures are logged but do not abort the batch).
     */
    private String processSingleNack(SubscriptionContext ctx, WsDeliveryTagTracker tracker,
                                     String subscriptionId, long tag, boolean requeue) {
        // DLX path: defer the tracker state transition until DLX produce acks.
        if (!requeue && deadLetterHook != null) {
            return processDlxNack(ctx, tracker, subscriptionId, tag);
        }
        // Either requeue=true, or no DLX hook configured (legacy drop path).
        WsDeliveryTagTracker.PendingDelivery pd = tracker.nack(tag, requeue);
        if (pd == null) {
            if (!isTagWithinAssignedRange(tracker, tag)) {
                return "PRECONDITION_FAILED: unknown delivery tag " + tag;
            }
            return null; // already-transitioned tag → idempotent success
        }
        recordNackMetrics(requeue);
        if (requeue) {
            triggerRedelivery(ctx, pd);
        } else {
            handleDlxOrDiscard(ctx, pd);
        }
        return null;
    }

    /**
     * DLX-aware nack: peeks the tag (no state transition), invokes the hook,
     * then commits the tracker transition based on the future's outcome. The
     * tag stays in PENDING while the future is in flight which prevents the
     * commit watermark from advancing past it — that is exactly the spec's
     * atomicity invariant.
     */
    private String processDlxNack(SubscriptionContext ctx, WsDeliveryTagTracker tracker,
                                  String subscriptionId, long tag) {
        WsDeliveryTagTracker.PendingDelivery pd = tracker.peek(tag);
        if (pd == null) {
            if (!isTagWithinAssignedRange(tracker, tag)) {
                return "PRECONDITION_FAILED: unknown delivery tag " + tag;
            }
            return null; // already-transitioned tag → idempotent success
        }
        recordNackMetrics(false);
        CompletableFuture<Void> dlxFuture;
        try {
            dlxFuture = deadLetterHook.deadLetter(ctx.queueName(), pd);
        } catch (RuntimeException e) {
            log.warn("DLX hook threw synchronously for sub={} tag={}: {}",
                subscriptionId, tag, e.toString());
            dlxFuture = CompletableFuture.failedFuture(e);
        }
        // Settle the tracker transition asynchronously so the Netty event loop is
        // never blocked. The completion callback runs on whichever thread the
        // hook chose — typically a Kafka producer ack thread.
        dlxFuture.whenComplete((v, err) -> finishDlxNack(ctx, tracker, subscriptionId, tag, err));
        return null;
    }

    /**
     * Final step of the DLX nack: transition the tracker based on the hook's
     * outcome, then attempt a commit flush so the watermark advances (or stays
     * put) immediately.
     */
    private void finishDlxNack(SubscriptionContext ctx, WsDeliveryTagTracker tracker,
                                String subscriptionId, long tag, Throwable err) {
        boolean dlxSucceeded = (err == null);
        WsDeliveryTagTracker.PendingDelivery pd = tracker.nack(tag, !dlxSucceeded);
        if (pd == null) {
            // Tag was cleared (e.g., subscription cancelled) while DLX was in flight.
            log.debug("DLX completion for sub={} tag={} ignored — tag no longer tracked",
                subscriptionId, tag);
            return;
        }
        if (dlxSucceeded) {
            log.debug("DLX produce succeeded for sub={} tag={} tp={} offset={}",
                subscriptionId, tag, pd.topicPartition(), pd.offset());
        } else {
            log.warn("DLX produce failed for sub={} tag={} tp={} offset={}: {}",
                subscriptionId, tag, pd.topicPartition(), pd.offset(), err.toString());
        }
        // Always attempt the flush — in batched mode this is a no-op, but in
        // flush-on-each-ack mode the commit watermark must advance now.
        if (commitIntervalMs == 0L) {
            flushSubscription(subscriptionId, ctx);
        }
    }

    private void recordNackMetrics(boolean requeue) {
        if (metrics == null) {
            return;
        }
        metrics.nackRate.mark();
        if (!requeue) {
            metrics.dlxRate.mark();
        }
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
     * Hook for NACK-without-requeue when no {@link DeadLetterHook} is wired
     * (legacy / pre-WS4.01 mode). The commit watermark advances immediately
     * (tracker has set {@code NACKED_DISCARD}); the message is dropped. With a
     * DLX hook configured this method is bypassed entirely — see
     * {@link #processDlxNack}.
     */
    private void handleDlxOrDiscard(SubscriptionContext ctx,
                                     WsDeliveryTagTracker.PendingDelivery pd) {
        log.debug("NACK without requeue (no DLX hook): sub={} tp={} offset={} — discarded",
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
