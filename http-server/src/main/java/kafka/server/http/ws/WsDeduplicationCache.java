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
// Time: Created - TASK-WS4.05
package kafka.server.http.ws;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * In-memory message deduplication cache, partitioned by exchange.
 *
 * <p>Each exchange owns an independent bounded LRU/insertion-order map mapping
 * {@code messageId → recordTimestampMs}. A subsequent {@link #isDuplicate} call
 * for an entry whose {@code (now - recordTimestampMs) < ttlMs} is treated as a
 * duplicate; expired entries are evicted lazily on access (and silently
 * re-admitted, returning {@code false}).
 *
 * <h3>Bounding</h3>
 * <ul>
 *     <li>{@code maxSize} bounds each per-exchange map (not the global total).
 *         Evicting per-exchange keeps the LRU bookkeeping cheap and prevents one
 *         busy exchange from starving another's slot budget.</li>
 *     <li>Eviction policy = insertion order (oldest entry dropped first when the
 *         map exceeds {@code maxSize}). LinkedHashMap with {@code accessOrder=false}
 *         is sufficient because dedup checks are typically near-real-time and
 *         expiry is governed by the TTL clock.</li>
 * </ul>
 *
 * <h3>Concurrency</h3>
 * <ul>
 *     <li>The outer map of {@code exchange → cache} is a {@link ConcurrentHashMap}.</li>
 *     <li>Each per-exchange {@link LinkedHashMap} is non-thread-safe and is
 *         guarded by synchronizing on the map instance itself; this keeps the
 *         critical section small (one map probe + at most one eviction).</li>
 * </ul>
 *
 * <h3>Null/empty messageId</h3>
 * <ul>
 *     <li>{@code null} or empty messageId is treated as "publisher opted out of
 *         dedup": always returns {@code false} and is never recorded. Hit/miss
 *         counters are not incremented for these calls.</li>
 * </ul>
 *
 * <p><b>Thread safety:</b> safe for concurrent use across publisher event loops.
 *
 * // Time: Created - TASK-WS4.05
 */
public final class WsDeduplicationCache {

    private final int maxSize;
    private final long ttlMs;

    /** Outer map: exchangeName → bounded LRU dedup map. */
    private final ConcurrentHashMap<String, LinkedHashMap<String, Long>> caches =
        new ConcurrentHashMap<>();

    private final AtomicLong hits = new AtomicLong(0);
    private final AtomicLong misses = new AtomicLong(0);

    /**
     * @param maxSize maximum number of {@code messageId} entries retained per
     *                exchange. Must be {@code >= 0}; {@code 0} effectively
     *                disables caching (no eviction needed because no entry
     *                survives).
     * @param ttlMs   entry lifetime in milliseconds. Entries older than this are
     *                treated as cache misses (and re-admitted under the new
     *                timestamp).
     */
    public WsDeduplicationCache(int maxSize, long ttlMs) {
        if (maxSize < 0) {
            throw new IllegalArgumentException("maxSize must be non-negative: " + maxSize);
        }
        if (ttlMs < 0) {
            throw new IllegalArgumentException("ttlMs must be non-negative: " + ttlMs);
        }
        this.maxSize = maxSize;
        this.ttlMs = ttlMs;
    }

    /**
     * Returns {@code true} if {@code messageId} has been observed for
     * {@code exchangeName} within the configured TTL. Otherwise records the
     * messageId and returns {@code false}.
     *
     * <p>{@code null} or empty {@code messageId} is the publisher's "opt out of
     * dedup" signal: it always returns {@code false}, is never stored, and does
     * not affect hit/miss counters.
     *
     * @param exchangeName the exchange name (acts as a per-publisher namespace)
     * @param messageId    publisher-supplied message identifier
     * @return {@code true} iff this is a duplicate within the TTL window
     */
    public boolean isDuplicate(String exchangeName, String messageId) {
        if (messageId == null || messageId.isEmpty()) {
            return false;
        }
        LinkedHashMap<String, Long> cache = caches.computeIfAbsent(exchangeName, k -> newCache());
        long now = System.currentTimeMillis();

        synchronized (cache) {
            Long existing = cache.get(messageId);
            if (existing != null && (now - existing) < ttlMs) {
                hits.incrementAndGet();
                return true;
            }
            // Miss: record the (re-)insertion. LinkedHashMap with the
            // removeEldestEntry override drops the oldest entry once the map
            // grows past maxSize. When maxSize == 0 the put is followed
            // immediately by eviction of the just-inserted entry, so the cache
            // remains empty.
            cache.put(messageId, now);
            misses.incrementAndGet();
            return false;
        }
    }

    /**
     * Drops every entry recorded for {@code exchangeName}. Typically called
     * from {@code ExchangeManager} when an exchange is deleted so a re-created
     * exchange under the same name starts with a clean dedup namespace.
     */
    public void clearExchange(String exchangeName) {
        caches.remove(exchangeName);
    }

    /**
     * Snapshot of internal counters. The {@code size} field is computed by
     * iterating per-exchange caches under their respective monitors.
     */
    public CacheStats stats() {
        int totalSize = 0;
        for (LinkedHashMap<String, Long> cache : caches.values()) {
            synchronized (cache) {
                totalSize += cache.size();
            }
        }
        return new CacheStats(hits.get(), misses.get(), totalSize);
    }

    /** Snapshot of cache statistics. */
    public record CacheStats(long hits, long misses, int size) { }

    // ---- Internal -----------------------------------------------------------

    private LinkedHashMap<String, Long> newCache() {
        // accessOrder=false → strict insertion order; eviction picks the oldest
        // inserted entry, matching the spec's "evicts oldest" requirement.
        // initialCapacity heuristic: cap at 16 to avoid wasted memory for
        // exchanges that never accumulate entries.
        return new LinkedHashMap<>(Math.min(16, Math.max(1, maxSize)), 0.75f, false) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Long> eldest) {
                return size() > maxSize;
            }
        };
    }
}
