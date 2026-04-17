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

// Time: Created - TASK-WS4.02 - per-message TTL check at delivery time

package kafka.server.http.ws;

import org.apache.kafka.common.header.Header;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Pure helpers for WebSocket message TTL (design doc §12.4).
 *
 * <p>Two levels of TTL:
 * <ul>
 *   <li><b>Per-message TTL</b> — carried in the {@code _ws_expiration} Kafka record
 *       header (milliseconds as a UTF-8 decimal string, matching the AMQP
 *       {@code expiration} property). Checked at delivery time in the WebSocket
 *       fetch loop via {@link #isExpired(Iterable, long, long)}.</li>
 *   <li><b>Per-queue TTL</b> — supplied as the {@code x-message-ttl} argument on
 *       {@code queue.declare}. Maps to Kafka {@code retention.ms} on the backing
 *       topic; the storage layer then deletes expired log segments automatically.
 *       Use {@link #retentionMsFromQueueArgs(Map)} when wiring the topic create
 *       request.</li>
 * </ul>
 *
 * <p>Per-message TTL semantics (spec):
 * <ul>
 *   <li>Missing or malformed header → "no TTL", deliver the record.</li>
 *   <li>Negative TTL → treated as missing (defensive).</li>
 *   <li>{@code age > ttlMs} → expired (strict greater-than: age == ttl is still
 *       deliverable). Matches RabbitMQ's {@code x-message-ttl} behaviour.</li>
 *   <li>Clock skew / future-dated records ({@code now < recordTimestamp}) →
 *       never expired.</li>
 *   <li>Expired records still advance the consumer offset watermark so they are
 *       not redelivered on the next fetch.</li>
 * </ul>
 *
 * <p>This class is intentionally side-effect-free so callers own the downstream
 * action (skip, dead-letter, metric) and testability stays trivial.
 *
 * // Time: Created - TASK-WS4.02
 */
public final class WsMessageTtl {

    /** Name of the Kafka record header that carries the per-message TTL value. */
    public static final String HDR_EXPIRATION = WsMessageSerializer.HDR_EXPIRATION;

    /** AMQP-style argument name for per-queue TTL (maps to Kafka {@code retention.ms}). */
    public static final String QUEUE_ARG_X_MESSAGE_TTL = "x-message-ttl";

    /** Sentinel returned by {@link #retentionMsFromQueueArgs(Map)} when no TTL is set. */
    public static final long NO_RETENTION_OVERRIDE = -1L;

    private WsMessageTtl() {
        // utility class
    }

    /**
     * Returns {@code true} if the record carries an {@code _ws_expiration} header
     * whose parsed TTL value is less than the record's age.
     *
     * <p>Always fails open: any parse error, missing header, negative TTL, or
     * negative age returns {@code false} (i.e. the message is still deliverable).
     * Dropping an in-flight record because of malformed metadata would be a
     * worse failure mode than a slightly stale delivery.
     *
     * @param headers         Kafka record headers (non-null, may be empty)
     * @param recordTimestampMs  Kafka record timestamp (milliseconds since epoch)
     * @param nowMs           current wall-clock time (milliseconds since epoch)
     * @return {@code true} if the record is expired and must not be delivered
     */
    public static boolean isExpired(Iterable<Header> headers, long recordTimestampMs, long nowMs) {
        if (headers == null) {
            return false;
        }
        String raw = lastHeaderValue(headers, HDR_EXPIRATION);
        if (raw == null || raw.isEmpty()) {
            return false;
        }
        long ttlMs;
        try {
            ttlMs = Long.parseLong(raw);
        } catch (NumberFormatException e) {
            return false;
        }
        if (ttlMs < 0) {
            return false;
        }
        long ageMs = nowMs - recordTimestampMs;
        if (ageMs < 0) {
            // Clock skew or future-dated record — do not treat as expired.
            return false;
        }
        return ageMs > ttlMs;
    }

    /**
     * Resolves the {@code x-message-ttl} queue argument to a Kafka
     * {@code retention.ms} value, or {@link #NO_RETENTION_OVERRIDE} when the
     * argument is absent or malformed.
     *
     * <p>The resulting value is what a topic-creation request should use for
     * {@code retention.ms} when {@code NO_RETENTION_OVERRIDE} means "do not
     * override — let the broker default apply".
     *
     * @param queueArgs  queue declare arguments (may be {@code null} or empty)
     * @return parsed TTL in milliseconds, or {@link #NO_RETENTION_OVERRIDE}
     */
    public static long retentionMsFromQueueArgs(Map<String, String> queueArgs) {
        if (queueArgs == null) {
            return NO_RETENTION_OVERRIDE;
        }
        String raw = queueArgs.get(QUEUE_ARG_X_MESSAGE_TTL);
        if (raw == null || raw.isEmpty()) {
            return NO_RETENTION_OVERRIDE;
        }
        long ttlMs;
        try {
            ttlMs = Long.parseLong(raw);
        } catch (NumberFormatException e) {
            return NO_RETENTION_OVERRIDE;
        }
        // Kafka retention.ms must be a non-negative duration — reject negatives
        // so the caller falls back to the broker default instead of corrupting
        // the topic config.
        if (ttlMs < 0) {
            return NO_RETENTION_OVERRIDE;
        }
        return ttlMs;
    }

    /**
     * Returns the last value associated with the given header name as a UTF-8
     * string, or {@code null} if no such header is present.
     *
     * <p>Why "last": {@link org.apache.kafka.common.header.Headers} permits
     * duplicate keys and the convention matches
     * {@link org.apache.kafka.common.header.Headers#lastHeader(String)} — later
     * headers override earlier ones.
     */
    private static String lastHeaderValue(Iterable<Header> headers, String name) {
        String last = null;
        for (Header h : headers) {
            if (h != null && name.equals(h.key())) {
                byte[] val = h.value();
                last = (val == null) ? null : new String(val, StandardCharsets.UTF_8);
            }
        }
        return last;
    }
}
