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

_To be filled by the executing agent._

---

## Limitations

_To be filled by the executing agent._

---

## Field Notes

_To be filled by the executing agent._

---

## Acceptance Criteria

- [ ] `timeout 300 ./gradlew :http-server:test --tests 'kafka.server.http.ws.WsDeduplicationCacheTest' -x spotlessCheck` exits 0
- [ ] `grep -r "WsDeduplicationCache" http-server/src/main/java/` returns at least 2 hits
- [ ] Same (exchange, messageId) returns true for isDuplicate
- [ ] Different exchange with same messageId returns false
- [ ] Learning section filled with at least one entry

---

## File Manifest

_To be filled by the executing agent._
