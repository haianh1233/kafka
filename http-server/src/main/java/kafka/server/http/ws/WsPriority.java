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

// Time: Created - TASK-WS4.03 - priority-aware delivery ordering

package kafka.server.http.ws;

import org.apache.kafka.common.header.Header;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Pure helpers for WebSocket priority-aware delivery (design doc §12.5 / §18.7).
 *
 * <p>Priority model (sibling to {@link WsMessageTtl}):
 * <ul>
 *   <li><b>Per-queue max priority</b> — supplied as the {@code x-max-priority}
 *       argument on {@code queue.declare}. A queue with {@code x-max-priority > 0}
 *       enables priority-sorted delivery; otherwise records are delivered in
 *       offset order. Parsed via {@link #maxPriorityFromQueueArgs(Map)}.</li>
 *   <li><b>Per-message priority</b> — carried in the {@code _ws_priority} Kafka
 *       record header (UTF-8 decimal string in {@code [0, 255]}, matching the
 *       AMQP {@code priority} property; written by {@link WsMessageSerializer}).
 *       {@link #extractPriority(Iterable, int)} reads the header value and
 *       clamps it to {@code [0, maxPriority]} per spec.</li>
 * </ul>
 *
 * <p>{@link #sortByPriority(List, Function, int)} applies a <b>stable descending</b>
 * sort by priority within a fetch batch:
 * <pre>
 *   Fetch batch (unsorted):         Sorted for delivery:
 *     offset=10, priority=3           offset=11, priority=9  ← first
 *     offset=11, priority=9           offset=12, priority=5
 *     offset=12, priority=5           offset=10, priority=3  ← last
 * </pre>
 *
 * <p>Ordering is <b>batch-local</b>: a low-priority record already in the current
 * batch will still be delivered before a high-priority record that arrives in a
 * subsequent fetch.
 *
 * <p>Semantics:
 * <ul>
 *   <li>Missing / malformed / empty header → priority 0 (lowest).</li>
 *   <li>Negative header value → clamped to 0.</li>
 *   <li>Header value above {@code maxPriority} → clamped to {@code maxPriority}.</li>
 *   <li>Duplicate {@code _ws_priority} headers → last value wins (matches the
 *       {@link org.apache.kafka.common.header.Headers#lastHeader(String)}
 *       convention).</li>
 *   <li>Same-priority records keep their input order (stable sort).</li>
 *   <li>{@code maxPriority <= 0} → {@link #sortByPriority(List, Function, int)}
 *       is a no-op and returns the same {@link List} instance it received.</li>
 * </ul>
 *
 * <p>This class is intentionally side-effect-free so callers own the downstream
 * action (sort, metric, dispatch) and testability stays trivial.
 */
public final class WsPriority {

    /** Name of the Kafka record header that carries the per-message priority. */
    public static final String HDR_PRIORITY = WsMessageSerializer.HDR_PRIORITY;

    /** AMQP-style argument name for per-queue max priority. */
    public static final String QUEUE_ARG_X_MAX_PRIORITY = "x-max-priority";

    /**
     * Sentinel "no priority ordering" value. A queue without
     * {@code x-max-priority}, or with a malformed / non-positive argument,
     * uses this value and bypasses sort.
     */
    public static final int NO_PRIORITY = 0;

    /**
     * Hard upper bound on {@code x-max-priority} — matches the AMQP
     * {@code priority} octet range ({@code 0-255}).
     */
    public static final int MAX_PRIORITY_CAP = 255;

    private WsPriority() {
        // utility class
    }

    /**
     * Resolves the {@code x-max-priority} queue argument to an effective
     * maximum priority, or {@link #NO_PRIORITY} when the queue has no priority
     * configuration.
     *
     * <p>Fails open: any parse error, missing argument, or non-positive value
     * yields {@link #NO_PRIORITY} so the caller treats the queue as non-priority
     * and skips sorting.
     *
     * <p>Values above {@link #MAX_PRIORITY_CAP} are capped (AMQP octet limit).
     *
     * @param queueArgs  queue declare arguments (may be {@code null} or empty)
     * @return parsed max priority in {@code [0, 255]}, or {@link #NO_PRIORITY}
     */
    public static int maxPriorityFromQueueArgs(Map<String, String> queueArgs) {
        if (queueArgs == null) {
            return NO_PRIORITY;
        }
        String raw = queueArgs.get(QUEUE_ARG_X_MAX_PRIORITY);
        if (raw == null || raw.isEmpty()) {
            return NO_PRIORITY;
        }
        int max;
        try {
            max = Integer.parseInt(raw);
        } catch (NumberFormatException e) {
            return NO_PRIORITY;
        }
        if (max <= 0) {
            return NO_PRIORITY;
        }
        return Math.min(max, MAX_PRIORITY_CAP);
    }

    /**
     * Extracts the clamped priority for a record, honouring {@code maxPriority}.
     *
     * <p>Fails open in both directions:
     * <ul>
     *   <li>{@code maxPriority <= 0} → always returns {@code 0} (non-priority
     *       queue, no sorting intended).</li>
     *   <li>No header / empty / malformed / negative → returns {@code 0}.</li>
     *   <li>Header value above {@code maxPriority} → clamped to
     *       {@code maxPriority}.</li>
     * </ul>
     *
     * <p>When the same header key appears multiple times, the <b>last</b> value
     * wins — this matches
     * {@link org.apache.kafka.common.header.Headers#lastHeader(String)}.
     *
     * @param headers       Kafka record headers (may be {@code null})
     * @param maxPriority   queue's {@code x-max-priority} (clamps upper bound)
     * @return integer priority in {@code [0, max(maxPriority, 0)]}
     */
    public static int extractPriority(Iterable<Header> headers, int maxPriority) {
        if (maxPriority <= 0 || headers == null) {
            return 0;
        }
        String raw = lastHeaderValue(headers, HDR_PRIORITY);
        if (raw == null || raw.isEmpty()) {
            return 0;
        }
        int p;
        try {
            p = Integer.parseInt(raw);
        } catch (NumberFormatException e) {
            return 0;
        }
        if (p < 0) {
            return 0;
        }
        return Math.min(p, maxPriority);
    }

    /**
     * Returns the batch sorted by priority (descending) in a stable manner,
     * or the input list unchanged when priority ordering is disabled.
     *
     * <p>If {@code maxPriority <= 0} the input is returned untouched — same
     * {@link List} instance, offset order preserved. Otherwise the input is
     * sorted in-place and the same reference is returned (so callers that
     * already own a mutable list avoid an extra copy).
     *
     * <p>Callers that cannot tolerate in-place mutation (e.g., because the
     * upstream list is immutable) MUST pass a mutable copy — a natural fit for
     * {@code new ArrayList<>(recordsFromFetch)}.
     *
     * <p>The comparator breaks ties in favour of the original input index so
     * same-priority records keep their relative order (stable sort).
     *
     * @param records        batch of records to sort
     * @param headerExtractor function mapping a record to its Kafka headers
     * @param maxPriority    queue's {@code x-max-priority} (clamps upper bound)
     * @param <T>            record type (anything carrying a header iterable)
     * @return the same list reference, sorted by priority descending, or the
     *         input list unchanged if sorting is not applicable
     */
    public static <T> List<T> sortByPriority(List<T> records,
                                             Function<T, Iterable<Header>> headerExtractor,
                                             int maxPriority) {
        if (records == null || records.size() < 2 || maxPriority <= 0) {
            return records;
        }
        // Pre-compute priorities to avoid re-parsing O(n log n) times during
        // the sort, and so the comparator is a cheap int comparison.
        final int n = records.size();
        int[] priorities = new int[n];
        for (int i = 0; i < n; i++) {
            priorities[i] = extractPriority(headerExtractor.apply(records.get(i)), maxPriority);
        }
        // Index-based stable sort: decorate with the original index so equal
        // priorities keep their input order.
        Integer[] indices = new Integer[n];
        for (int i = 0; i < n; i++) {
            indices[i] = i;
        }
        java.util.Arrays.sort(indices, Comparator
            .<Integer>comparingInt(i -> -priorities[i])  // descending priority
            .thenComparingInt(i -> i));                  // stable by input order
        List<T> reordered = new ArrayList<>(n);
        for (int i : indices) {
            reordered.add(records.get(i));
        }
        // Overwrite the input list in place so the method contract (same
        // reference back) holds even when the sort actually moved elements.
        records.clear();
        records.addAll(reordered);
        return records;
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
