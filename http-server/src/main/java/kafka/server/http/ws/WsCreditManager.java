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
package kafka.server.http.ws;

import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

/**
 * Per-subscription credit-based flow control for WebSocket push delivery.
 *
 * <p>Credits provide explicit, client-controlled flow control for WebSocket subscriptions:
 * the fetch loop consumes one credit per {@code deliver} frame and pauses when credits
 * hit zero; the client replenishes with {@code credits} frames. Netty's
 * {@link Channel#isWritable()} acts as an additional backpressure signal — when the
 * write buffer exceeds the high water mark, delivery pauses regardless of credits.
 *
 * <p>Thread safety: {@link #grant(int)} is intended to be called from the Netty event
 * loop (on {@code credits} frames), while {@link #consume()} and
 * {@link #awaitCredits(long)} are called from {@code wsConsumerExecutor} (the fetch
 * loop). The implementation is lock-free and safe under arbitrary concurrency.
 *
 * <p>{@link #awaitCredits(long)} blocks via {@link LockSupport#parkNanos(long)} with
 * a 10ms polling interval — no spin wait, no CPU burn. Callers MUST NOT invoke
 * {@code awaitCredits} from the Netty event loop.
 *
 * // Time: Created - TASK-WS1.14
 */
public final class WsCreditManager {

    private static final Logger log = LoggerFactory.getLogger(WsCreditManager.class);

    /** Poll interval for awaitCredits — 10ms per design doc §21.2. */
    private static final long POLL_INTERVAL_NANOS = 10_000_000L;

    private final AtomicInteger credits;
    private final Channel channel;

    /**
     * @param initialCredits starting credit count (must be &gt;= 0)
     * @param channel        the Netty channel (used for writability backpressure check)
     * @throws IllegalArgumentException if {@code initialCredits < 0}
     * @throws NullPointerException     if {@code channel} is null
     */
    public WsCreditManager(int initialCredits, Channel channel) {
        if (initialCredits < 0) {
            throw new IllegalArgumentException("initialCredits must be >= 0, got " + initialCredits);
        }
        this.credits = new AtomicInteger(initialCredits);
        this.channel = Objects.requireNonNull(channel, "channel");
    }

    /**
     * Grants additional credits. Called when the client sends a {@code credits} frame.
     *
     * @param additional number of credits to add (must be &gt; 0)
     * @throws IllegalArgumentException if {@code additional <= 0}
     */
    public void grant(int additional) {
        if (additional <= 0) {
            throw new IllegalArgumentException("additional must be > 0, got " + additional);
        }
        int newValue = credits.addAndGet(additional);
        if (log.isDebugEnabled()) {
            log.debug("Credits granted: +{} -> {}", additional, newValue);
        }
    }

    /**
     * Consumes one credit. Atomically decrements the counter while flooring at zero.
     *
     * @return {@code true} if a credit was available and consumed, {@code false} if
     *         the counter was already at zero (no decrement performed)
     */
    public boolean consume() {
        int prev = credits.getAndUpdate(c -> c > 0 ? c - 1 : 0);
        return prev > 0;
    }

    /**
     * Blocks until credits are available AND the channel is writable, or the timeout
     * expires. Uses {@link LockSupport#parkNanos(long)} with a 10ms polling interval
     * per design doc §21.2 — no spin-wait.
     *
     * <p>A non-writable channel is treated as zero credits: even if the client has
     * granted credits, we must not pump data into a full Netty write buffer.
     *
     * <p>MUST NOT be called from the Netty event loop thread. The fetch loop runs on
     * {@code wsConsumerExecutor} where blocking is safe.
     *
     * @param timeoutMs maximum time to wait in milliseconds. A value of 0 causes the
     *                  method to return immediately (returning the current credit
     *                  count if writable and positive, 0 otherwise).
     * @return the observed credit count (&gt; 0) when the wait succeeds, or {@code 0}
     *         if the timeout expired or the channel is not writable
     */
    public int awaitCredits(long timeoutMs) {
        long timeoutNanos = timeoutMs * 1_000_000L;
        long deadline = System.nanoTime() + timeoutNanos;
        while (true) {
            int current = credits.get();
            if (current > 0 && channel.isWritable()) {
                return current;
            }
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                return 0;
            }
            LockSupport.parkNanos(Math.min(remaining, POLL_INTERVAL_NANOS));
        }
    }

    /**
     * @return the current credit count (may be stale under concurrent modification)
     */
    public int available() {
        return credits.get();
    }

    /**
     * Resets credits to zero. Called on unsubscribe/close.
     */
    public void reset() {
        credits.set(0);
    }
}
