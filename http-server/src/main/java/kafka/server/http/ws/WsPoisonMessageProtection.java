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

// Time: Created - TASK-WS4.04

package kafka.server.http.ws;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;

/**
 * Bounds the redelivery loop introduced by {@code NACK(requeue=true)} so a
 * poison payload cannot wedge the queue forever (design doc §12.6).
 *
 * <p>Each delivery carries a {@code _ws_delivery_count} Kafka header
 * (see {@link WsMessageSerializer#HDR_DELIVERY_COUNT}). First delivery carries
 * count {@code 1}; the count is incremented immediately before each
 * redelivery. When the count reaches the configured threshold the broker
 * auto-routes the record to the DLX with {@code reason="max-retries-exceeded"}
 * <b>instead of</b> requeuing it, and commits the original offset so the
 * message is gone from the source partition.
 *
 * <h3>Threshold resolution</h3>
 * <p>The per-queue argument {@code x-max-retries} wins over the global default
 * from {@link WsConfigs#maxRedeliveryCount()}. A malformed per-queue value is
 * ignored (falls back to the global default) — the invariant is "poison
 * protection must never crash the ack path".
 *
 * <h3>Integration points</h3>
 * <ul>
 *   <li>{@link WsConsumerFetchLoop} — calls
 *       {@link #incrementDeliveryCount} on the record headers before emitting
 *       the {@code deliver} frame. (Wiring lives on the fetch integration
 *       side; this class provides only the pure header helpers.)</li>
 *   <li>{@link WsAckHandler} — calls {@link #exceedsMaxRetries} via the
 *       {@link WsAckHandler.PoisonGate} on each {@code NACK(requeue=true)}.
 *       When the threshold is crossed the gate triggers
 *       {@link #maybeDeadLetter} and allows the commit watermark to
 *       advance.</li>
 * </ul>
 *
 * <h3>Thread-safety</h3>
 * <p>All static helpers are pure functions on {@link Headers}; the instance
 * methods read per-queue metadata and delegate to the injected sinks, both of
 * which must themselves be thread-safe. This class owns no mutable state
 * beyond the injected resolver.
 */
public final class WsPoisonMessageProtection {

    private static final Logger log = LoggerFactory.getLogger(WsPoisonMessageProtection.class);

    /** Per-queue argument name overriding the global {@code ws.max.redelivery.count}. */
    public static final String ARG_MAX_RETRIES = "x-max-retries";

    /** Dead-letter reason carried in {@code x-death} when the threshold is crossed. */
    public static final String REASON_MAX_RETRIES_EXCEEDED = "max-retries-exceeded";

    private final BiFunction<String, String, QueueMetadata> queueResolver;
    private final int defaultMaxRetries;

    /**
     * @param queueResolver     looks up queue metadata by {@code (vhost, name)};
     *                          may return {@code null} when the queue is not
     *                          registered (in which case the global default applies)
     * @param defaultMaxRetries global threshold from {@link WsConfigs#maxRedeliveryCount()};
     *                          must be {@code >= 1}
     */
    public WsPoisonMessageProtection(BiFunction<String, String, QueueMetadata> queueResolver,
                                     int defaultMaxRetries) {
        this.queueResolver = Objects.requireNonNull(queueResolver, "queueResolver");
        if (defaultMaxRetries < 1) {
            throw new IllegalArgumentException(
                "defaultMaxRetries must be >= 1, got " + defaultMaxRetries);
        }
        this.defaultMaxRetries = defaultMaxRetries;
    }

    // ------------------------------------------------------------------
    //  Header helpers (static — pure functions on Kafka Headers)
    // ------------------------------------------------------------------

    /**
     * Reads the {@code _ws_delivery_count} header from a record.
     *
     * @param headers record headers (must not be {@code null})
     * @return the numeric count, or {@code 0} if the header is missing or
     *         malformed. Callers treat {@code 0} as "no prior hops"; the next
     *         call to {@link #incrementDeliveryCount} will stamp {@code 1}.
     */
    public static int readDeliveryCount(Headers headers) {
        Objects.requireNonNull(headers, "headers");
        Header h = lastHeader(headers, WsMessageSerializer.HDR_DELIVERY_COUNT);
        if (h == null || h.value() == null) {
            return 0;
        }
        try {
            return Integer.parseInt(new String(h.value(), StandardCharsets.UTF_8));
        } catch (NumberFormatException e) {
            log.debug("Malformed _ws_delivery_count header (treating as 0): '{}'",
                new String(h.value(), StandardCharsets.UTF_8));
            return 0;
        }
    }

    /**
     * Returns a new header list with {@code _ws_delivery_count} bumped by one.
     * Any existing {@code _ws_delivery_count} header is replaced (never
     * duplicated); all other headers are copied through in original order.
     *
     * <p>First call on a record without a prior count stamps {@code 1} —
     * matching the spec's rule that the first delivery carries count {@code 1},
     * not {@code 0}.
     *
     * @param headers existing record headers (must not be {@code null}; may be empty)
     * @return new unmodifiable header list
     */
    public static List<Header> incrementDeliveryCount(Headers headers) {
        Objects.requireNonNull(headers, "headers");
        int next = readDeliveryCount(headers) + 1;
        List<Header> out = new ArrayList<>();
        for (Header h : headers) {
            if (WsMessageSerializer.HDR_DELIVERY_COUNT.equals(h.key())) {
                continue; // will be replaced below
            }
            out.add(new RecordHeader(h.key(), h.value()));
        }
        out.add(new RecordHeader(WsMessageSerializer.HDR_DELIVERY_COUNT,
            Integer.toString(next).getBytes(StandardCharsets.UTF_8)));
        return Collections.unmodifiableList(out);
    }

    // ------------------------------------------------------------------
    //  Threshold logic
    // ------------------------------------------------------------------

    /**
     * @return {@code true} iff the record's current delivery count has reached
     *         or exceeded the resolved threshold for the queue. {@code count}
     *         is the value AFTER the most recent increment.
     */
    public boolean exceedsMaxRetries(String vhost, String queueName, int count) {
        return count >= resolveMaxRetries(vhost, queueName);
    }

    /**
     * Resolves the threshold for a queue: per-queue {@code x-max-retries} (if
     * present and well-formed) wins over the global default. Malformed values
     * and unknown queues both fall back to the global default.
     *
     * <p>Package-private for unit test access.
     */
    int resolveMaxRetries(String vhost, String queueName) {
        QueueMetadata queue = queueResolver.apply(vhost, queueName);
        if (queue == null) {
            return defaultMaxRetries;
        }
        String raw = queue.arguments().get(ARG_MAX_RETRIES);
        if (raw == null || raw.isEmpty()) {
            return defaultMaxRetries;
        }
        try {
            int parsed = Integer.parseInt(raw);
            if (parsed < 1) {
                log.debug("Queue '{}' x-max-retries={} is non-positive; using default {}",
                    queueName, parsed, defaultMaxRetries);
                return defaultMaxRetries;
            }
            return parsed;
        } catch (NumberFormatException e) {
            log.debug("Queue '{}' x-max-retries='{}' is not a number; using default {}",
                queueName, raw, defaultMaxRetries);
            return defaultMaxRetries;
        }
    }

    // ------------------------------------------------------------------
    //  Dead-letter dispatch
    // ------------------------------------------------------------------

    /**
     * Pluggable DLX sink. Production wiring binds this to
     * {@link WsDeadLetterHandler#deadLetter} via a small adapter so the same
     * DLX plumbing serves both the {@code NACK(requeue=false)} path (reason
     * {@code "rejected"}) and the auto-DLX-on-poison path (reason
     * {@code "max-retries-exceeded"}).
     */
    @FunctionalInterface
    public interface DeadLetterSink {
        CompletableFuture<Void> deadLetter(byte[] key,
                                           byte[] value,
                                           Headers headers,
                                           String vhost,
                                           String queueName,
                                           String reason);
    }

    /**
     * If the record's delivery count has reached the threshold, dispatch it to
     * the DLX with reason {@link #REASON_MAX_RETRIES_EXCEEDED} and return
     * {@code true}. Otherwise return {@code false} so the caller may fall
     * through to its normal requeue path.
     *
     * <p><b>Contract on DLX failure:</b> if the sink returns a failed future
     * (or throws synchronously) the method still returns {@code true}. The
     * poison threshold has been crossed; requeuing the message would continue
     * the infinite bounce that this whole mechanism exists to prevent. We
     * accept the trade-off that a misconfigured DLX may lose the message and
     * emit a WARN-level log so operators can see the drop.
     */
    public boolean maybeDeadLetter(byte[] key,
                                   byte[] value,
                                   Headers headers,
                                   String vhost,
                                   String queueName,
                                   DeadLetterSink sink) {
        Objects.requireNonNull(headers, "headers");
        Objects.requireNonNull(vhost, "vhost");
        Objects.requireNonNull(queueName, "queueName");
        Objects.requireNonNull(sink, "sink");

        int count = readDeliveryCount(headers);
        if (!exceedsMaxRetries(vhost, queueName, count)) {
            return false;
        }

        try {
            CompletableFuture<Void> f = sink.deadLetter(
                key, value, headers, vhost, queueName, REASON_MAX_RETRIES_EXCEEDED);
            if (f != null) {
                f.whenComplete((v, err) -> {
                    if (err != null) {
                        log.warn("Auto-DLX (max-retries-exceeded) failed for queue='{}' count={}; "
                            + "message will be discarded to avoid infinite redelivery: {}",
                            queueName, count, err.toString());
                    }
                });
            }
        } catch (RuntimeException e) {
            log.warn("Auto-DLX sink threw synchronously for queue='{}' count={}; "
                + "message will be discarded to avoid infinite redelivery: {}",
                queueName, count, e.toString());
        }
        return true;
    }

    // ------------------------------------------------------------------
    //  Header utilities
    // ------------------------------------------------------------------

    private static Header lastHeader(Headers headers, String key) {
        Header last = null;
        for (Header h : headers) {
            if (key.equals(h.key())) {
                last = h;
            }
        }
        return last;
    }
}
