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

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link WsDeduplicationCache}.
 *
 * <p>Covers:
 * <ul>
 *     <li>basic hit/miss semantics on a single exchange</li>
 *     <li>per-exchange isolation (same messageId on different exchange = miss)</li>
 *     <li>TTL-based expiry (entries past their TTL re-admit)</li>
 *     <li>null/empty messageId bypasses the cache</li>
 *     <li>size cap with LRU/insertion-order eviction</li>
 *     <li>{@link WsDeduplicationCache#clearExchange} drops only the named exchange</li>
 *     <li>{@link WsDeduplicationCache.CacheStats} reflects hits, misses, size</li>
 *     <li>concurrent access remains thread-safe (no exceptions, deterministic counts)</li>
 * </ul>
 *
 * // Time: Created - TASK-WS4.05
 */
class WsDeduplicationCacheTest {

    // ---- 1. Hit / miss semantics ------------------------------------------------

    @Test
    void firstPublish_notDuplicate() {
        WsDeduplicationCache cache = new WsDeduplicationCache(100, 60_000);

        assertFalse(cache.isDuplicate("events", "msg-1"),
            "first sighting of a messageId is not a duplicate");
    }

    @Test
    void sameMessageId_sameExchange_isDuplicate() {
        WsDeduplicationCache cache = new WsDeduplicationCache(100, 60_000);

        assertFalse(cache.isDuplicate("events", "msg-1"));
        assertTrue(cache.isDuplicate("events", "msg-1"),
            "second sighting of (events, msg-1) is a duplicate");
    }

    @Test
    void sameMessageId_differentExchange_notDuplicate() {
        WsDeduplicationCache cache = new WsDeduplicationCache(100, 60_000);

        assertFalse(cache.isDuplicate("events", "msg-1"));
        assertFalse(cache.isDuplicate("orders", "msg-1"),
            "different exchange = different namespace, not a duplicate");
        // and now (orders, msg-1) is recorded — second visit should hit
        assertTrue(cache.isDuplicate("orders", "msg-1"));
        // (events, msg-1) is still a hit too
        assertTrue(cache.isDuplicate("events", "msg-1"));
    }

    // ---- 2. TTL ----------------------------------------------------------------

    @Test
    void expiredEntry_notDuplicate() throws InterruptedException {
        // Tight TTL so the test runs quickly. Using >1ms because nanoTime resolution
        // and scheduler jitter can otherwise produce flaky results.
        WsDeduplicationCache cache = new WsDeduplicationCache(100, 50);

        assertFalse(cache.isDuplicate("events", "expiry"));
        Thread.sleep(120); // generous margin past the 50ms TTL
        assertFalse(cache.isDuplicate("events", "expiry"),
            "expired entry should be treated as new");
    }

    // ---- 3. Null / empty messageId --------------------------------------------

    @Test
    void nullMessageId_notDuplicate() {
        WsDeduplicationCache cache = new WsDeduplicationCache(100, 60_000);

        assertFalse(cache.isDuplicate("events", null));
        assertFalse(cache.isDuplicate("events", null),
            "null messageId always bypasses the cache (never recorded)");
        assertEquals(0, cache.stats().size(), "null messageId must not consume cache slots");
    }

    @Test
    void emptyMessageId_notDuplicate() {
        WsDeduplicationCache cache = new WsDeduplicationCache(100, 60_000);

        assertFalse(cache.isDuplicate("events", ""));
        assertFalse(cache.isDuplicate("events", ""),
            "empty messageId always bypasses the cache (never recorded)");
        assertEquals(0, cache.stats().size(), "empty messageId must not consume cache slots");
    }

    // ---- 4. Size cap (LRU eviction) -------------------------------------------

    @Test
    void cacheSizeLimit_evictsOldest() {
        // Cap=3, generous TTL so eviction is purely size-driven.
        WsDeduplicationCache cache = new WsDeduplicationCache(3, 60_000);

        assertFalse(cache.isDuplicate("events", "a"));
        assertFalse(cache.isDuplicate("events", "b"));
        assertFalse(cache.isDuplicate("events", "c"));
        // 4th insertion forces eviction of the oldest entry ("a").
        assertFalse(cache.isDuplicate("events", "d"));

        // Still-present entries remain duplicates (read these BEFORE re-admitting
        // "a" so we don't perturb the LRU/insertion order).
        assertTrue(cache.isDuplicate("events", "b"));
        assertTrue(cache.isDuplicate("events", "c"));
        assertTrue(cache.isDuplicate("events", "d"));
        // The oldest entry "a" should have been evicted by the size cap.
        assertFalse(cache.isDuplicate("events", "a"),
            "oldest entry should have been evicted by size cap");
    }

    @Test
    void cacheSizeLimit_isPerExchange() {
        // Each exchange gets its own size cap.
        WsDeduplicationCache cache = new WsDeduplicationCache(2, 60_000);

        // Fill exchange A to its cap.
        assertFalse(cache.isDuplicate("A", "1"));
        assertFalse(cache.isDuplicate("A", "2"));
        // Exchange B should still admit two entries despite A being full.
        assertFalse(cache.isDuplicate("B", "1"));
        assertFalse(cache.isDuplicate("B", "2"));
        // Both should still be hits (no cross-exchange eviction).
        assertTrue(cache.isDuplicate("A", "1"));
        assertTrue(cache.isDuplicate("A", "2"));
        assertTrue(cache.isDuplicate("B", "1"));
        assertTrue(cache.isDuplicate("B", "2"));
    }

    // ---- 5. clearExchange -----------------------------------------------------

    @Test
    void clearExchange_removesAllEntries() {
        WsDeduplicationCache cache = new WsDeduplicationCache(100, 60_000);

        cache.isDuplicate("events", "x");
        cache.isDuplicate("events", "y");
        cache.isDuplicate("orders", "x");

        cache.clearExchange("events");

        assertFalse(cache.isDuplicate("events", "x"),
            "cleared exchange should re-admit previously seen messageIds");
        assertFalse(cache.isDuplicate("events", "y"));
        // Other exchange untouched.
        assertTrue(cache.isDuplicate("orders", "x"),
            "clearing one exchange must not affect other exchanges");
    }

    @Test
    void clearExchange_unknownExchange_isNoOp() {
        WsDeduplicationCache cache = new WsDeduplicationCache(100, 60_000);
        cache.isDuplicate("events", "x");

        // Should not throw.
        cache.clearExchange("never-existed");

        assertTrue(cache.isDuplicate("events", "x"),
            "clearing an unknown exchange should leave others intact");
    }

    // ---- 6. Stats -------------------------------------------------------------

    @Test
    void stats_tracksHitsAndMisses() {
        WsDeduplicationCache cache = new WsDeduplicationCache(100, 60_000);

        // 3 misses
        cache.isDuplicate("events", "a");
        cache.isDuplicate("events", "b");
        cache.isDuplicate("orders", "a");
        // 2 hits
        cache.isDuplicate("events", "a");
        cache.isDuplicate("orders", "a");

        WsDeduplicationCache.CacheStats stats = cache.stats();
        assertEquals(2, stats.hits(), "two cache hits");
        assertEquals(3, stats.misses(), "three cache misses");
        assertEquals(3, stats.size(), "three distinct (exchange, messageId) entries stored");
    }

    @Test
    void stats_nullAndEmptyMessageId_doNotAffectCounters() {
        WsDeduplicationCache cache = new WsDeduplicationCache(100, 60_000);
        cache.isDuplicate("events", null);
        cache.isDuplicate("events", "");

        WsDeduplicationCache.CacheStats stats = cache.stats();
        assertEquals(0, stats.hits());
        assertEquals(0, stats.misses(),
            "null/empty messageId should not increment hit/miss counters");
        assertEquals(0, stats.size());
    }

    // ---- 7. Concurrency -------------------------------------------------------

    @Test
    void concurrentAccess_threadSafe() throws Exception {
        // Hammer the cache with many threads inserting unique keys + repeating
        // a small set of "hot" keys. Verify nothing crashes and the unique
        // entries land in the cache.
        WsDeduplicationCache cache = new WsDeduplicationCache(100_000, 60_000);

        int threads = 16;
        int perThread = 500;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        AtomicInteger errors = new AtomicInteger();

        try {
            for (int t = 0; t < threads; t++) {
                final int tid = t;
                pool.submit(() -> {
                    try {
                        start.await();
                        for (int i = 0; i < perThread; i++) {
                            // Unique-per-thread key (always a miss)
                            cache.isDuplicate("ex-" + (tid % 4), "uniq-" + tid + "-" + i);
                            // Hot key (could be hit or miss depending on race)
                            cache.isDuplicate("hot", "shared");
                        }
                    } catch (Throwable th) {
                        errors.incrementAndGet();
                    } finally {
                        done.countDown();
                    }
                });
            }
            start.countDown();
            assertTrue(done.await(10, TimeUnit.SECONDS), "concurrent workload timed out");
        } finally {
            pool.shutdownNow();
        }

        assertEquals(0, errors.get(), "no thread should have thrown");

        // Every unique-per-thread key should now be present (a second sighting hits).
        for (int t = 0; t < threads; t++) {
            for (int i = 0; i < perThread; i++) {
                assertTrue(
                    cache.isDuplicate("ex-" + (t % 4), "uniq-" + t + "-" + i),
                    "expected second sighting to be a duplicate");
            }
        }
    }

    // ---- 8. Constructor validation -------------------------------------------

    @Test
    void constructor_zeroMaxSize_isAllowed_butNothingCached() {
        // Edge case: maxSize=0 effectively disables caching (every put gets evicted).
        // First-time check returns false; immediate re-check might also return false.
        // We just assert no exception and stats remain at 0 size.
        WsDeduplicationCache cache = new WsDeduplicationCache(0, 60_000);
        assertFalse(cache.isDuplicate("events", "a"));
        assertEquals(0, cache.stats().size());
    }

    @Test
    void clearExchange_resetsStatsSizeContribution() {
        WsDeduplicationCache cache = new WsDeduplicationCache(100, 60_000);
        cache.isDuplicate("events", "a");
        cache.isDuplicate("events", "b");
        cache.isDuplicate("orders", "x");

        cache.clearExchange("events");
        // size should now reflect only 'orders' contents
        assertEquals(1, cache.stats().size(), "cleared exchange entries removed from total size");
    }

    // Sanity: deterministic insertion ordering across LRU eviction.
    @Test
    void cacheSizeLimit_preservesNewerEntries() {
        WsDeduplicationCache cache = new WsDeduplicationCache(3, 60_000);
        Set<String> ids = new HashSet<>();
        for (int i = 0; i < 5; i++) {
            ids.add("m-" + i);
            cache.isDuplicate("ex", "m-" + i);
        }
        // Cap is 3, last three inserted are m-2, m-3, m-4
        assertTrue(cache.isDuplicate("ex", "m-2"));
        assertTrue(cache.isDuplicate("ex", "m-3"));
        assertTrue(cache.isDuplicate("ex", "m-4"));
        // m-0 and m-1 should have been evicted
        assertFalse(cache.isDuplicate("ex", "m-0"));
        assertFalse(cache.isDuplicate("ex", "m-1"));
    }
}
