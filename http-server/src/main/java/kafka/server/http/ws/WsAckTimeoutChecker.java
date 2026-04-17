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

// Time: Created - TASK-WS4.06

package kafka.server.http.ws;

import io.netty.channel.Channel;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;

/**
 * Periodic ack-timeout sweeper (design doc §19.6 / TASK-WS4.06).
 *
 * <p>If a consumer holds a delivery without ACKing it for longer than the
 * configured {@code ws.consumer.ack.timeout.ms} (default 300000 / 5 minutes),
 * the broker auto-NACKs the message with {@code requeue=true} and sends a
 * warning {@code ACK_TIMEOUT} error frame to the client. This prevents a slow
 * or crashed consumer from wedging the queue indefinitely.
 *
 * <h3>Sweep ordering (spec §19.6)</h3>
 * <ol>
 *   <li>Snapshot the tracker's pending tags and their assigned-at timestamps.</li>
 *   <li>For each tag older than {@code timeoutMs}:
 *     <ol type="a">
 *       <li>Write the {@code ACK_TIMEOUT} warning frame <b>first</b> — the
 *           client sees the warning before the requeue takes effect.</li>
 *       <li>Route the tag through
 *           {@link WsAckHandler#handleNack} with {@code requeue=true, multiple=false}.
 *           Going through the handler (rather than calling the tracker directly)
 *           ensures the poison-protection gate (WS4.04), metrics (WS3.08), and
 *           any future hooks are consulted the same way a client NACK would.</li>
 *     </ol>
 *   </li>
 * </ol>
 *
 * <h3>Race with client ACK</h3>
 * <p>A client ACK that lands between the snapshot and the handler call will
 * have removed the tag from the tracker already; {@code handleNack} then sees
 * a non-pending tag and returns an idempotent success (no state transition).
 * The error frame may have been delivered — that is the expected warning the
 * spec asks for, and a benign duplicate on the client side.
 *
 * <h3>Scheduling</h3>
 * <p>This class implements {@link Runnable} so it can be scheduled via any
 * {@link java.util.concurrent.ScheduledExecutorService}. Recommended cadence
 * is {@code min(timeoutMs / 10, 30_000)} ms. The caller owns the executor
 * (typically the shared {@code wsConsumerExecutor}) and feeds the
 * {@link ScheduledFuture} back via {@link #setScheduledFuture} so
 * {@link #stop} can cancel it cleanly.
 */
public final class WsAckTimeoutChecker implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(WsAckTimeoutChecker.class);

    /** Warning error code emitted in the pre-requeue frame. */
    public static final String ERROR_CODE = "ACK_TIMEOUT";

    private final WsDeliveryTagTracker tagTracker;
    private final WsAckHandler ackHandler;
    private final String subscriptionId;
    private final Channel channel;
    private final long timeoutMs;
    private final LongSupplier timeSource;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private volatile ScheduledFuture<?> scheduledFuture;

    /**
     * Default time-source variant (wall-clock).
     */
    public WsAckTimeoutChecker(WsDeliveryTagTracker tagTracker,
                               WsAckHandler ackHandler,
                               String subscriptionId,
                               Channel channel,
                               long timeoutMs) {
        this(tagTracker, ackHandler, subscriptionId, channel, timeoutMs, System::currentTimeMillis);
    }

    /**
     * @param tagTracker     per-subscription tracker whose assigned-at timestamps are scanned
     * @param ackHandler     handler through which timed-out tags are auto-NACKed
     *                       (§19.6: same path as client-initiated NACKs)
     * @param subscriptionId the subscription that owns this checker — forwarded to the ack handler
     * @param channel        Netty channel on which the warning error frame is written
     * @param timeoutMs      how long a pending tag may live before auto-NACK; must be {@code > 0}
     * @param timeSource     clock for age comparisons; tests inject a fixed value for
     *                       determinism, production wires {@link System#currentTimeMillis}
     */
    public WsAckTimeoutChecker(WsDeliveryTagTracker tagTracker,
                               WsAckHandler ackHandler,
                               String subscriptionId,
                               Channel channel,
                               long timeoutMs,
                               LongSupplier timeSource) {
        this.tagTracker = Objects.requireNonNull(tagTracker, "tagTracker");
        this.ackHandler = Objects.requireNonNull(ackHandler, "ackHandler");
        this.subscriptionId = Objects.requireNonNull(subscriptionId, "subscriptionId");
        this.channel = Objects.requireNonNull(channel, "channel");
        if (timeoutMs <= 0L) {
            throw new IllegalArgumentException("timeoutMs must be > 0, got " + timeoutMs);
        }
        this.timeoutMs = timeoutMs;
        this.timeSource = Objects.requireNonNull(timeSource, "timeSource");
    }

    /** @return the configured ack timeout in milliseconds (exposed for tests / introspection) */
    public long timeoutMs() {
        return timeoutMs;
    }

    /** @return the subscription id this checker is bound to */
    public String subscriptionId() {
        return subscriptionId;
    }

    /** @return whether the checker is still running (i.e. {@link #stop} has not yet been called) */
    public boolean isRunning() {
        return running.get();
    }

    /**
     * Scans the tracker for expired delivery tags and auto-NACKs each one.
     * Intentionally best-effort: any exception from the handler call is logged
     * and the sweep continues so one poison tag cannot stall the checker.
     */
    @Override
    public void run() {
        if (!running.get()) {
            return;
        }
        final long now = timeSource.getAsLong();
        Map<Long, Long> snapshot = tagTracker.snapshotAssignedAtMillis();
        if (snapshot.isEmpty()) {
            return;
        }
        for (Map.Entry<Long, Long> e : snapshot.entrySet()) {
            if (!running.get()) {
                return; // stop() fired mid-sweep
            }
            long tag = e.getKey();
            long assignedAt = e.getValue();
            if (now - assignedAt < timeoutMs) {
                continue;
            }
            handleExpiredTag(tag, now - assignedAt);
        }
    }

    private void handleExpiredTag(long tag, long ageMs) {
        // 1) Warning frame BEFORE the requeue (spec §19.6 ordering invariant).
        sendTimeoutFrame(tag, ageMs);

        // 2) Auto-NACK through the same path as a client-initiated NACK so the
        //    poison gate, metrics, and any future hooks observe the transition
        //    exactly as they would for a real client NACK. If the tag was acked
        //    concurrently the handler returns a PRECONDITION_FAILED string for
        //    an unknown tag or null for an already-transitioned one; both are
        //    benign from our perspective.
        try {
            String err = ackHandler.handleNack(subscriptionId, tag, /* requeue */ true, /* multiple */ false);
            if (err != null && log.isDebugEnabled()) {
                log.debug("ACK timeout NACK for sub={} tag={} returned: {}", subscriptionId, tag, err);
            }
        } catch (RuntimeException rex) {
            log.warn("ACK timeout NACK threw for sub={} tag={}: {}", subscriptionId, tag, rex.toString());
        }
    }

    private void sendTimeoutFrame(long tag, long ageMs) {
        if (!channel.isOpen()) {
            return; // channel already closed — skip the frame but still let the NACK run
        }
        // Hand-rolled JSON keeps the checker dependency-free; escaping is unnecessary
        // for the three fixed fields. Matches the shape in design doc §19.6.
        String frame = String.format(
            "{\"type\":\"error\",\"errorCode\":\"%s\",\"deliveryTag\":%d,"
                + "\"errorMessage\":\"Delivery tag %d timed out after %dms, requeued\"}",
            ERROR_CODE, tag, tag, timeoutMs);
        try {
            channel.writeAndFlush(new TextWebSocketFrame(frame));
        } catch (RuntimeException e) {
            // Writer failures must not abort the sweep — the NACK still needs to run
            // so the commit watermark isn't wedged on this tag forever.
            log.warn("Failed to write ACK_TIMEOUT frame for sub={} tag={} age={}ms: {}",
                subscriptionId, tag, ageMs, e.toString());
        }
    }

    /**
     * Stops the sweeper. Idempotent. If {@link #setScheduledFuture} was invoked,
     * the scheduled future is cancelled (with {@code mayInterruptIfRunning=false}
     * so an in-flight sweep runs to completion). After {@code stop()} the checker
     * is inert — {@link #run} returns immediately.
     */
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        ScheduledFuture<?> future = scheduledFuture;
        if (future != null) {
            future.cancel(false);
        }
    }

    /**
     * Registers the {@link ScheduledFuture} produced by the caller's
     * {@code scheduleAtFixedRate} call so {@link #stop} can cancel it.
     */
    public void setScheduledFuture(ScheduledFuture<?> future) {
        this.scheduledFuture = future;
    }
}
