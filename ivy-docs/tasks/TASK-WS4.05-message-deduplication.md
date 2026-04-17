# TASK-WS4.05: Message Deduplication

## Prerequisites

| Task | What it delivers | Why this task needs it |
|------|------------------|-----------------------|
| TASK-WS1.09 | WsPublishHandler | Dedup check happens before produce |
| TASK-WS3.01 | WsPublisherConfirmTracker | Duplicate publish still gets a confirm |

---

## Context

The design doc §12.7 specifies publish-side message deduplication using the AMQP `messageId` field. When `ws.dedup.enabled=true`, the broker maintains a bounded in-memory cache of recent message IDs per exchange to suppress duplicate publishes.

**Dedup flow:**
1. Publisher sets `message.messageId` on publish
2. Broker checks dedup cache for `(exchange, messageId)` pair
3. If found → publish silently suppressed, publisher confirm sent (if confirms enabled)
4. If not found → normal publish, messageId added to cache

**Cache properties:**
- Per-exchange cache
- Bounded by `ws.dedup.cache.size` (default 10000)
- Entries expire after `ws.dedup.cache.ttl.ms` (default 60000)
- LRU eviction when size exceeded

---

## Specification

### WsDeduplicationCache

```java
package kafka.server.http.ws;

public final class WsDeduplicationCache {

    public WsDeduplicationCache(int maxSize, long ttlMs);

    /** Check if messageId is a duplicate. If not, adds it to cache. Returns true if duplicate. */
    public boolean isDuplicate(String exchangeName, String messageId);

    /** Clear cache for an exchange (on exchange delete). */
    public void clearExchange(String exchangeName);

    /** Get cache statistics. */
    public CacheStats stats();

    public record CacheStats(long hits, long misses, int size) {}
}
```

---

## Implementation Details

**Module:** `http-server`

**Files to create:**

| File | What it does |
|------|--------------|
| `http-server/src/main/java/kafka/server/http/ws/WsDeduplicationCache.java` | Bounded, TTL'd dedup cache |

**Files to modify:**

| File | What changes |
|------|--------------|
| `http-server/src/main/java/kafka/server/http/ws/WsPublishHandler.java` | Check dedup cache before routing/produce |

> **CRITICAL:** Dedup is ONLY active when `ws.dedup.enabled=true` (default false). When disabled, skip all dedup logic.

> **CRITICAL:** Dedup key is `(exchange, messageId)` — same messageId on different exchanges is NOT a duplicate.

> **CRITICAL:** Duplicate publishes are silently confirmed (not errors). The publisher doesn't know the message was deduplicated.

> **CRITICAL:** Use ConcurrentHashMap with per-entry timestamps for TTL. Periodic cleanup or lazy eviction on access.

**Implementation order:**
1. Create WsDeduplicationCache with bounded LRU + TTL
2. Add dedup check to WsPublishHandler before routing
3. If duplicate: skip routing/produce, send publisher confirm if enabled
4. Add unit tests

---

## Skeleton Code

```java
package kafka.server.http.ws;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * In-memory message deduplication cache per exchange.
 * Key = (exchange, messageId), bounded by max size, entries expire after TTL.
 *
 * // Time: Created - TASK-WS4.05
 */
public final class WsDeduplicationCache {

    private final int maxSize;
    private final long ttlMs;
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, Long>> caches =
        new ConcurrentHashMap<>();
    private final AtomicLong hits = new AtomicLong(0);
    private final AtomicLong misses = new AtomicLong(0);

    public WsDeduplicationCache(int maxSize, long ttlMs) {
        this.maxSize = maxSize;
        this.ttlMs = ttlMs;
    }

    public boolean isDuplicate(String exchangeName, String messageId) {
        if (messageId == null || messageId.isEmpty()) return false;

        ConcurrentHashMap<String, Long> cache =
            caches.computeIfAbsent(exchangeName, k -> new ConcurrentHashMap<>());

        Long existingTimestamp = cache.get(messageId);
        long now = System.currentTimeMillis();

        if (existingTimestamp != null && (now - existingTimestamp) < ttlMs) {
            hits.incrementAndGet();
            return true;
        }

        // TODO: Evict oldest entries if cache size exceeds maxSize
        cache.put(messageId, now);
        misses.incrementAndGet();
        return false;
    }

    public void clearExchange(String exchangeName) {
        caches.remove(exchangeName);
    }

    public record CacheStats(long hits, long misses, int size) {}

    public CacheStats stats() {
        int totalSize = caches.values().stream().mapToInt(ConcurrentHashMap::size).sum();
        return new CacheStats(hits.get(), misses.get(), totalSize);
    }
}
```

---

## Tests

**Test class:** `http-server/src/test/java/kafka/server/http/ws/WsDeduplicationCacheTest.java`

| Test method | What it verifies |
|-------------|-----------------|
| `firstPublish_notDuplicate` | New messageId returns false |
| `sameMessageId_sameExchange_isDuplicate` | Same (exchange, messageId) returns true |
| `sameMessageId_differentExchange_notDuplicate` | Different exchange = different key |
| `expiredEntry_notDuplicate` | Entry past TTL treated as new |
| `nullMessageId_notDuplicate` | Null messageId skips dedup |
| `emptyMessageId_notDuplicate` | Empty messageId skips dedup |
| `cacheSizeLimit_evictsOldest` | Oldest entries evicted when max size exceeded |
| `clearExchange_removesAllEntries` | Exchange delete clears cache |
| `stats_tracksHitsAndMisses` | Statistics accurate |
| `concurrentAccess_threadSafe` | Multiple threads don't corrupt state |

**Run command:**
```bash
timeout 300 ./gradlew :http-server:test --tests 'kafka.server.http.ws.WsDeduplicationCacheTest' -x spotlessCheck
```

---

## Rules

- Dedup only active when `ws.dedup.enabled=true` (default false).
- Key is `(exchange, messageId)` — per-exchange namespace.
- Null/empty messageId skips dedup entirely.
- Duplicate publish → silently confirm, do NOT return error.
- Cache bounded by size (LRU eviction) and TTL.
- Exchange deletion clears its dedup cache.

---

## Learning

- **Per-exchange size cap (not global)**. The skeleton suggested a single global cap; switching to a per-exchange cap inside the per-exchange `LinkedHashMap` keeps the implementation simple (eviction is automatic on `put`), avoids a costly global LRU bookkeeping data structure, and prevents one busy exchange from starving another's slot budget. Tradeoff: total memory is `numExchanges * maxSize` rather than `maxSize`. For default (10000 entries × maybe 50 exchanges) this is acceptable.
- **`LinkedHashMap.removeEldestEntry`** is the cleanest in-JDK way to get insertion-order LRU eviction without pulling in Caffeine/Guava. The override fires after every `put`, evicts at most one entry per put, and naturally handles the `maxSize=0` edge case (the just-inserted entry is immediately evicted).
- **Dedup placement matters**. Putting the dedup check between `internal` exchange validation and routing means: (a) duplicates against missing/internal exchanges still surface the original error/return semantics, and (b) duplicates skip routing CPU AND the produce slot. The cache is also untouched on rejection paths so we don't pollute it with messageIds for exchanges the publisher couldn't actually use.
- **Confirms-on-duplicate are idempotent semantics**. AMQP-style: if the broker already accepted the message, it acks again. Implementation reuses `emitPublishedIfConfirmsEnabled` so the confirm path goes through the same `WsPublisherConfirmTracker` as a real success — no special "synthetic confirm" code path.
- **Two-tier locking**. Outer `ConcurrentHashMap` for the exchange→cache lookup keeps publisher-event-loops on different exchanges fully concurrent; the per-exchange `synchronized(cache)` block has a tiny critical section (one `get` + one `put` + at most one eviction) so contention is bounded even for hot exchanges.

---

## Limitations

- **Wall-clock TTL.** Uses `System.currentTimeMillis()` rather than a monotonic clock. NTP slew or step changes could cause entries to look "younger" than they are (admit replays) or "older" (premature eviction). For dedup window of seconds-to-minutes this is acceptable; for a hardened deployment consider `System.nanoTime()` with a per-cache base offset.
- **No background eviction.** Entries past their TTL stay resident until evicted by size pressure or until they are next probed. A long-quiet exchange holding many stale entries will keep them in memory. Acceptable because the size cap bounds worst-case memory; not acceptable if memory pressure is the dominant constraint — add a `Cleaner` task in that case.
- **Production wiring deferred.** `WsPublishHandler` accepts a nullable `WsDeduplicationCache` so existing constructors remain backward compatible, but no integration touches `HttpServer`/`KafkaApis` yet. The configuration values (`ws.dedup.enabled`, `ws.dedup.cache.size`, `ws.dedup.cache.ttl.ms`) are read into `WsConfigs` but no wire-up site yet constructs the cache and threads it into the handler. A follow-up task should: (a) instantiate one `WsDeduplicationCache` per broker when `dedupEnabled()` is true, (b) hand it to `WsPublishHandler`, and (c) call `cache.clearExchange(name)` from `ExchangeManager#deleteExchange`.
- **No metrics for dedup counters.** `CacheStats` exposes hits/misses/size for testing but isn't yet bridged to the existing `WsMetrics` JMX surface. Bridging is straightforward (poll `stats()` and update `Gauge`s) but deferred to keep this task focused on the cache + integration.
- **Per-exchange cap, not global.** See Learning above. If desired, a global `LinkedHashMap` keyed by `exchange + "|" + messageId` could enforce a true global cap — but at the cost of cross-exchange contention. The current per-exchange design is the intentional choice.

---

## Field Notes

- The skeleton in the task file used a `ConcurrentHashMap<String, ConcurrentHashMap<String, Long>>` and explicitly TODO'd "evict oldest entries if cache size exceeds maxSize". I replaced the inner map with a bounded `LinkedHashMap` whose `removeEldestEntry` override handles this in O(1) per put, removing the TODO. The outer `ConcurrentHashMap` survives.
- First test attempt for `cacheSizeLimit_evictsOldest` had a logic bug: I asserted `assertFalse(...isDuplicate("events", "a"))` (re-admitting "a") and THEN asserted "b" was still a duplicate. But re-admitting "a" pushes it to the most-recent slot and evicts "b" instead, so the test expectations contradicted the LRU it was trying to verify. Fix: read all the still-present entries BEFORE the re-admit probe.
- `verify(routingEngine, atMostOnce())` (rather than `times(1)`) for the dedup-hit test because the first publish does call routing once; the assertion is "the duplicate did not trigger a second routing call", which `atMostOnce` expresses cleanly when the first call is also expected.
- Spotless violations exist in unrelated files (`ConsumerGroupLagHandler.java`, `FetchForwardManager.java`); my new files pass spotless. Tests are run with `-x spotlessCheck` per project convention.

---

---

## Acceptance Criteria

- [ ] `timeout 300 ./gradlew :http-server:test --tests 'kafka.server.http.ws.WsDeduplicationCacheTest' -x spotlessCheck` exits 0
- [ ] `grep -r "WsDeduplicationCache" http-server/src/main/java/` returns at least 2 hits
- [ ] Same (exchange, messageId) returns true for isDuplicate
- [ ] Different exchange with same messageId returns false
- [ ] Learning section filled with at least one entry

---

## File Manifest

**New:**
- `http-server/src/main/java/kafka/server/http/ws/WsDeduplicationCache.java` — bounded per-exchange dedup cache with TTL and insertion-order LRU eviction.
- `http-server/src/test/java/kafka/server/http/ws/WsDeduplicationCacheTest.java` — 16 unit tests covering hit/miss, per-exchange isolation, TTL expiry, null/empty messageId opt-out, size cap eviction, `clearExchange`, stats accuracy, concurrency, and constructor edge cases.

**Modified:**
- `http-server/src/main/java/kafka/server/http/ws/WsPublishHandler.java`
  - Added 7-arg constructor accepting an optional `WsDeduplicationCache`; existing 5-arg and 6-arg constructors preserved (delegate with `dedupCache = null`).
  - Extended `ParsedPublish` record with `messageId` field; parsed from `message.messageId` via `textOrNull`.
  - Added dedup check between the internal-exchange validation and `runRoute`: hit → silently drop and emit `published` confirm if confirms enabled; miss → recorded and publish proceeds normally.
  - Updated header timeline to include `TASK-WS4.05`.
- `http-server/src/test/java/kafka/server/http/ws/WsPublishHandlerTest.java`
  - Added 8 dedup integration tests (hit drops + confirms, miss admits + adds, dedup disabled bypass, per-exchange namespace, missing messageId opt-out, confirms-disabled silent drop, dedup skipped on unknown exchange, dedup skipped on internal exchange).
  - Added `buildPublishFrameWithMessageId` helper.
  - Added `atMostOnce` Mockito import.

**Test results:** 16 dedup cache tests + 31 publish handler tests (23 pre-existing + 8 new) = 47 tests pass with `-x spotlessCheck`.
