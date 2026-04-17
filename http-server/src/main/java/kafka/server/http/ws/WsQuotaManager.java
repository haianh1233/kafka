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
// Time: Created - TASK-WS3.06
package kafka.server.http.ws;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Rate-limit and byte-rate quota enforcement for the WebSocket protocol
 * (design doc §19.4).
 *
 * <p>Three kinds of quota are exposed:
 * <ul>
 *   <li><b>Produce byte-rate</b>: checked per publish frame. Delegates to a
 *       functional-interface {@link ByteRateQuotaChecker} so the production
 *       wiring can bridge to Kafka's {@code ClientQuotaManager} without the
 *       http-server module taking a hard dependency on
 *       {@code core:kafka.server.ClientQuotaManager} (which lives in
 *       Scala-compiled code). Tests inject a simple capturing lambda.</li>
 *   <li><b>Fetch byte-rate</b>: checked per fetch iteration. Same shim shape as
 *       produce. When throttled, the fetch loop pauses silently for
 *       {@code throttleTimeMs} (no error frame to the client, per design
 *       §19.4.2).</li>
 *   <li><b>Control message rate</b>: enforced per-connection in
 *       {@link WsFrameHandler}. Uses a simple sliding-window counter (1 second
 *       granularity) configured by
 *       {@link WsConfigs#maxControlMessagesPerSecond()}.</li>
 * </ul>
 *
 * <h3>Thread-safety</h3>
 *
 * <p>Control message rate state is held in a {@link ConcurrentHashMap} keyed
 * by connection id, with per-connection {@code long}s packed into a single
 * {@link AtomicLong} — upper 32 bits hold the window's start-second, lower 32
 * bits hold the count in that window. Atomic {@code compareAndSet} keeps
 * reads and increments lock-free.
 *
 * <p>Byte-rate quotas delegate to the injected shim, so their thread-safety
 * contract is the shim's responsibility; production wiring (Kafka's
 * {@code ClientQuotaManager}) is thread-safe.
 *
 * // Time: Created - TASK-WS3.06
 */
public final class WsQuotaManager {

    /**
     * Value returned by {@link ByteRateQuotaChecker#checkAndRecord} meaning the
     * client is under quota and no throttle is required.
     */
    public static final long NO_THROTTLE = 0L;

    /**
     * Shim over Kafka's quota manager. Returns the throttle time in
     * milliseconds for a given clientId/byteCount pair; {@code 0} indicates
     * "under quota — no throttle required".
     *
     * <p>Production wiring delegates to
     * {@code ClientQuotaManager.maybeRecordAndGetThrottleTimeMs} (via
     * {@code RequestHandlerHelper}); tests use a captured lambda.
     */
    @FunctionalInterface
    public interface ByteRateQuotaChecker {
        /**
         * @param clientId  Kafka client id carried on the underlying request
         *                  (typically the authenticated principal or the
         *                  session id when no client id is supplied)
         * @param byteCount number of bytes attributed to this operation
         * @return {@code >= 0} throttle duration in milliseconds; {@code 0}
         *         when the operation is under quota
         */
        long checkAndRecord(String clientId, long byteCount);
    }

    /** Always-permissive checker — used when a shim is not supplied. */
    private static final ByteRateQuotaChecker NO_OP = (id, n) -> NO_THROTTLE;

    private final ByteRateQuotaChecker produceChecker;
    private final ByteRateQuotaChecker fetchChecker;
    private final int maxControlMessagesPerSecond;

    /**
     * Per-connection control-message window state. Upper 32 bits: window
     * start second (epoch seconds mod 2^32). Lower 32 bits: count observed in
     * that window. Collapsed into a single long so the update is a single
     * {@link AtomicLong#compareAndSet}.
     */
    private final ConcurrentHashMap<String, AtomicLong> controlWindows = new ConcurrentHashMap<>();

    public WsQuotaManager(ByteRateQuotaChecker produceChecker,
                          ByteRateQuotaChecker fetchChecker,
                          int maxControlMessagesPerSecond) {
        if (maxControlMessagesPerSecond <= 0) {
            throw new IllegalArgumentException(
                "maxControlMessagesPerSecond must be > 0, got " + maxControlMessagesPerSecond);
        }
        this.produceChecker = Objects.requireNonNullElse(produceChecker, NO_OP);
        this.fetchChecker = Objects.requireNonNullElse(fetchChecker, NO_OP);
        this.maxControlMessagesPerSecond = maxControlMessagesPerSecond;
    }

    /**
     * Convenience factory wired from {@link WsConfigs}. Both byte-rate shims
     * default to a permissive no-op; production wiring replaces them via
     * {@link #WsQuotaManager(ByteRateQuotaChecker, ByteRateQuotaChecker, int)}.
     */
    public static WsQuotaManager fromConfigs(WsConfigs cfg) {
        Objects.requireNonNull(cfg, "cfg");
        return new WsQuotaManager(NO_OP, NO_OP, cfg.maxControlMessagesPerSecond());
    }

    // ------------------------------------------------------------------
    //  Byte-rate quotas
    // ------------------------------------------------------------------

    /**
     * Records {@code byteCount} bytes of produce traffic against the quota for
     * {@code clientId} and returns the throttle time. {@code 0} means no
     * throttling is required.
     */
    public long checkProduceQuota(String clientId, long byteCount) {
        if (clientId == null || byteCount <= 0) {
            return NO_THROTTLE;
        }
        return Math.max(NO_THROTTLE, produceChecker.checkAndRecord(clientId, byteCount));
    }

    /**
     * Same as {@link #checkProduceQuota} but against the fetch-byte-rate quota.
     */
    public long checkFetchQuota(String clientId, long byteCount) {
        if (clientId == null || byteCount <= 0) {
            return NO_THROTTLE;
        }
        return Math.max(NO_THROTTLE, fetchChecker.checkAndRecord(clientId, byteCount));
    }

    // ------------------------------------------------------------------
    //  Control message rate (per connection)
    // ------------------------------------------------------------------

    /**
     * Records one inbound control message on {@code connectionId} and checks
     * the per-connection sliding window against
     * {@link WsConfigs#maxControlMessagesPerSecond()}.
     *
     * @return {@code true} when the connection is under quota (message
     *         allowed); {@code false} when the window is exhausted and the
     *         caller MUST emit a {@code QUOTA_EXCEEDED} error frame.
     */
    public boolean checkControlMessageRate(String connectionId) {
        return checkControlMessageRate(connectionId, System.currentTimeMillis());
    }

    /** Testable variant that accepts an injected clock. */
    boolean checkControlMessageRate(String connectionId, long nowMs) {
        Objects.requireNonNull(connectionId, "connectionId");
        long nowSec = nowMs / 1000L;
        AtomicLong state = controlWindows.computeIfAbsent(connectionId, k -> new AtomicLong(pack(nowSec, 0)));
        while (true) {
            long snapshot = state.get();
            long windowSec = snapshot >>> 32;
            int count = (int) (snapshot & 0xFFFFFFFFL);
            long nextPacked;
            boolean underLimit;
            if (windowSec != nowSec) {
                // Window rolled — reset count to 1 for this message.
                nextPacked = pack(nowSec, 1);
                underLimit = true;
            } else if (count >= maxControlMessagesPerSecond) {
                // Over limit — do NOT record this message; let caller reject
                // it with QUOTA_EXCEEDED. Return false without mutation so
                // the window count stays stable (avoids "already throttled
                // but still counted").
                return false;
            } else {
                nextPacked = pack(windowSec, count + 1);
                underLimit = true;
            }
            if (state.compareAndSet(snapshot, nextPacked)) {
                return underLimit;
            }
            // Lost the race; retry.
        }
    }

    /**
     * Call when a connection closes so the per-connection state does not leak.
     */
    public void releaseConnection(String connectionId) {
        if (connectionId != null) {
            controlWindows.remove(connectionId);
        }
    }

    public int maxControlMessagesPerSecond() {
        return maxControlMessagesPerSecond;
    }

    private static long pack(long windowSec, int count) {
        return (windowSec << 32) | (count & 0xFFFFFFFFL);
    }
}
