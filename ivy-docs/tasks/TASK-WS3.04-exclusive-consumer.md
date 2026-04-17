# TASK-WS3.04: Exclusive Consumer Enforcement

## Prerequisites

| Task | What it delivers | Why this task needs it |
|------|------------------|-----------------------|
| TASK-WS1.11 | WsSubscriptionManager — subscription lifecycle | Tracks active subscriptions per queue |
| TASK-WS1.06 | QueueManager — queue CRUD | Queue metadata stores exclusive flag |
| TASK-WS1.03 | WsConnectionContext | Exclusive lock references owning connection |

---

## Context

The design doc §11.3 specifies exclusive consumer behavior. When `exclusive: true` on a subscribe frame, only one consumer is allowed on that queue. A second subscriber is rejected with `EXCLUSIVE_CONSUMER` error. The exclusive queue is deleted when the owning connection closes.

**Exclusive subscribe rules:**
- First subscriber with `exclusive: true` → succeeds, locks queue to this connection
- Second subscriber (any connection, exclusive or not) → rejected with `EXCLUSIVE_CONSUMER`
- Owning connection closes → queue is deleted (along with backing topic and all bindings)
- Exclusive flag is on the subscription, NOT on the queue declaration (though queues can also be declared exclusive)

**Queue-level exclusive (from declare-queue):**
- `declare-queue` with `exclusive: true` → only the declaring connection can subscribe
- Queue deleted on declaring connection close
- Different from subscription-level exclusive: queue-level exclusive prevents ANY other connection from subscribing, even non-exclusively

---

## Specification

### ExclusiveConsumerManager

```java
package kafka.server.http.ws;

public final class ExclusiveConsumerManager {

    /** Attempt to acquire exclusive lock on queue for a connection. */
    public boolean tryAcquireExclusive(String queueName, String connectionId);

    /** Release exclusive lock (on unsubscribe or disconnect). */
    public void releaseExclusive(String queueName, String connectionId);

    /** Check if queue has an exclusive consumer. */
    public boolean isExclusivelyLocked(String queueName);

    /** Check if a specific connection holds the exclusive lock. */
    public boolean isLockedBy(String queueName, String connectionId);

    /** Handle connection close: release all exclusive locks, delete exclusive queues. */
    public List<String> onConnectionClose(String connectionId);
}
```

---

## Implementation Details

**Module:** `http-server`

**Files to create:**

| File | What it does |
|------|--------------|
| `http-server/src/main/java/kafka/server/http/ws/ExclusiveConsumerManager.java` | Exclusive lock tracking |

**Files to modify:**

| File | What changes |
|------|--------------|
| `http-server/src/main/java/kafka/server/http/ws/WsSubscriptionManager.java` | Check exclusive lock before allowing subscribe |
| `http-server/src/main/java/kafka/server/http/ws/WsConnectionContext.java` | On close, delegate to ExclusiveConsumerManager.onConnectionClose() |
| `http-server/src/main/java/kafka/server/http/routing/QueueManager.java` | Support queue-level exclusive flag |

> **CRITICAL:** Exclusive lock check must be atomic — use `ConcurrentHashMap.putIfAbsent()` to prevent race conditions between two simultaneous subscribe requests.

> **CRITICAL:** Connection close must delete exclusive queues AND release exclusive subscription locks. Both queue-level exclusive and subscription-level exclusive cleanup on disconnect.

**Implementation order:**
1. Create ExclusiveConsumerManager with atomic lock acquire/release
2. Modify WsSubscriptionManager to check exclusive before allowing subscribe
3. Modify WsConnectionContext close handler to clean up exclusive state
4. Add unit tests

---

## Skeleton Code

```java
package kafka.server.http.ws;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages exclusive consumer locks on queues.
 *
 * // Time: Created - TASK-WS3.04
 */
public final class ExclusiveConsumerManager {

    private final ConcurrentHashMap<String, String> exclusiveLocks = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, List<String>> connectionQueues = new ConcurrentHashMap<>();

    public boolean tryAcquireExclusive(String queueName, String connectionId) {
        String existing = exclusiveLocks.putIfAbsent(queueName, connectionId);
        if (existing == null) {
            connectionQueues.computeIfAbsent(connectionId, k -> 
                Collections.synchronizedList(new ArrayList<>())).add(queueName);
            return true;
        }
        return existing.equals(connectionId);
    }

    public void releaseExclusive(String queueName, String connectionId) {
        exclusiveLocks.remove(queueName, connectionId);
        List<String> queues = connectionQueues.get(connectionId);
        if (queues != null) queues.remove(queueName);
    }

    public boolean isExclusivelyLocked(String queueName) {
        return exclusiveLocks.containsKey(queueName);
    }

    public boolean isLockedBy(String queueName, String connectionId) {
        return connectionId.equals(exclusiveLocks.get(queueName));
    }

    public List<String> onConnectionClose(String connectionId) {
        // TODO: Return list of queue names that should be deleted (exclusive queues)
        List<String> queues = connectionQueues.remove(connectionId);
        if (queues != null) {
            for (String q : queues) {
                exclusiveLocks.remove(q, connectionId);
            }
            return queues;
        }
        return Collections.emptyList();
    }
}
```

---

## Tests

**Test class:** `http-server/src/test/java/kafka/server/http/ws/ExclusiveConsumerManagerTest.java`

| Test method | What it verifies |
|-------------|-----------------|
| `acquire_succeeds_whenNoLock` | First exclusive acquire returns true |
| `acquire_fails_whenLockedByOther` | Second connection rejected |
| `acquire_succeeds_whenLockedBySameConnection` | Same connection re-acquire is idempotent |
| `release_removesLock` | After release, another connection can acquire |
| `isExclusivelyLocked_correctState` | Reports lock state accurately |
| `onConnectionClose_releasesAllLocks` | All locks for connection released |
| `onConnectionClose_returnsExclusiveQueues` | Returns list of queues to delete |
| `concurrentAcquire_onlyOneSucceeds` | Atomic putIfAbsent prevents races |

**Run command:**
```bash
timeout 300 ./gradlew :http-server:test --tests 'kafka.server.http.ws.ExclusiveConsumerManagerTest' -x spotlessCheck
```

---

## Rules

- Exclusive lock acquire must be atomic (ConcurrentHashMap.putIfAbsent).
- Second subscriber to exclusively-locked queue → error `EXCLUSIVE_CONSUMER`.
- Connection close → delete exclusive queues, release all exclusive locks.
- Same connection re-acquiring its own exclusive lock is idempotent (returns true).

---

## Learning

- **Reverse index is the only way to keep `onConnectionClose` O(k).** The forward map
  `queue → connectionId` is what makes `tryAcquireExclusive` atomic, but walking it on
  every connection close would be O(n). A second `connectionId → Set<queueName>` map
  built from `ConcurrentHashMap.newKeySet()` lets the close path enumerate only the
  locks this connection held. The two maps are kept consistent by using
  `ConcurrentHashMap.remove(K, V)` on the forward map — it silently no-ops when the
  mapping has already been replaced by a newer holder, so late releases from an
  evicted owner are safe.
- **`putIfAbsent` is the correct primitive for exclusive locks** — not `computeIfAbsent`.
  `computeIfAbsent` locks the bin during the mapping function and would turn concurrent
  acquire calls into a serial point; `putIfAbsent` returns the existing value to the loser
  without blocking. With a stateless "just the connectionId" value, that's all we need.
- **Per-connection set is held as a `ConcurrentHashMap.newKeySet()` not a
  `synchronizedList`.** A set is the right type here (each queue appears at most once per
  connection), and the CHM-backed variant avoids the monitor that `Collections.synchronizedList`
  imposes on every iteration. The reverse entry is removed when empty via
  `computeIfPresent(k, (k,v) -> v.isEmpty() ? null : v)` to avoid a long-lived empty set
  leaking memory across thousands of transient connections.
- **Idempotent same-connection re-acquire is subtle.** The spec says "same connection
  re-acquire returns true". The naive implementation — `return existing.equals(connectionId)`
  after the `putIfAbsent` — is correct, but do NOT also add the queue to the reverse
  index in that branch: the reverse index already contains it, and `Set.add` is idempotent
  anyway but the extra `computeIfAbsent` allocation is wasted. The implementation here
  only touches the reverse index on fresh acquires.
- **Rolling back a failed subscribe is load-bearing.** `subscribeExclusive` acquires
  the exclusive lock BEFORE calling `subscribe`. If `subscribe` rejects (duplicate id,
  executor shutdown, NPE on args), the lock would otherwise stay held for a connection
  with no live subscription — so a try/catch releases it before rethrowing.

---

## Limitations

- **No QueueManager integration (explicitly deferred).** The design distinguishes
  subscription-level exclusive from queue-level exclusive (`declare-queue` with
  `exclusive: true`). Queue-level exclusive requires `QueueManager`, which does not
  exist yet. This task delivers only the subscription-level registry; queue-level
  exclusive will be wired in when `QueueManager` lands.
- **No `EXCLUSIVE_CONSUMER` frame wiring.** The manager throws
  `WsSubscriptionManager.ExclusiveLockDeniedException` on conflict, but nothing in
  `WsFrameHandler` currently routes that exception to an outgoing error frame with
  code `EXCLUSIVE_CONSUMER`. Integration at the frame-handler layer is the next
  task's responsibility (a future WS2 task). The exception carries the queue name
  so the frame handler can fill the error payload verbatim.
- **Exclusive lock cleanup on connection close is wired but not driven.** The hook
  `WsConnectionContext.onClose()` releases locks correctly when invoked, but the
  Netty channel-close handler does not yet call it. That wiring lives in the
  not-yet-merged frame-handler close path (referenced by the design doc but not
  in-scope here). A placeholder regression test verifies the hook mechanics; a
  future task will wire the call site.
- **Cascading queue/binding deletion on owner disconnect is also deferred.**
  `onConnectionClose` returns the list of released queues, but nothing consumes
  that list yet. Once `QueueManager` and `BindingManager` grow
  `deleteQueueCascade` support (design §11.3 last bullet), the list becomes the
  input to that call.

---

## Field Notes

- **QueueManager absent in this branch.** The spec's "Files to modify" entry for
  `QueueManager.java` was not actionable — the file doesn't exist. The worktree
  task brief explicitly allowed skipping that row and tracking exclusivity in a
  stand-alone registry, which is what this commit does.
- **Test count: 90 (19 new ExclusiveConsumerManagerTest + 8 new WsSubscriptionManager
  exclusive tests + 5 new WsConnectionContext wiring tests + 58 pre-existing).** All
  pass, including the concurrency test `concurrentAcquire_onlyOneSucceeds` with 16
  contending threads.
- **Acceptance criteria grep hit count = 13** (1 in ExclusiveConsumerManager.java,
  8 in WsSubscriptionManager.java, 4 in WsConnectionContext.java). Well above the
  ≥3 requirement.
- **No backward-compat breaks.** The pre-existing `WsSubscriptionManager(executor)`
  and `WsSubscriptionManager(executor, groupCoordinator)` constructors delegate to a
  new 3-arg form with `null` exclusiveManager. Same for `WsConnectionContext` — the
  5-arg constructor delegates to a new 6-arg form. All existing callers continue to
  compile and behave identically.
- **`unsubscribeExclusive` is a convenience overload.** Callers that want the
  plain `unsubscribe` semantics can keep calling it; there is no forced migration.

---

---

## Acceptance Criteria

- [x] `timeout 300 ./gradlew :http-server:test --tests 'kafka.server.http.ws.ExclusiveConsumerManagerTest' -x spotlessCheck` exits 0
- [x] `grep -r "ExclusiveConsumerManager" http-server/src/main/java/` returns at least 3 hits (13)
- [x] Lock acquire uses atomic ConcurrentHashMap operation (`putIfAbsent`, `remove(K, V)`)
- [x] Learning section filled with at least one entry

---

## File Manifest

**New files:**

| File | Purpose |
|------|---------|
| `http-server/src/main/java/kafka/server/http/ws/ExclusiveConsumerManager.java` | Atomic exclusive-consumer lock registry with O(k) connection-close path |
| `http-server/src/test/java/kafka/server/http/ws/ExclusiveConsumerManagerTest.java` | 19 unit tests covering acquire / release / close / concurrency / NPE cases |

**Modified files:**

| File | Change |
|------|--------|
| `http-server/src/main/java/kafka/server/http/ws/WsSubscriptionManager.java` | New 3-arg constructor, `subscribeExclusive`, `unsubscribeExclusive`, `exclusiveManager()` accessor, nested `ExclusiveLockDeniedException` |
| `http-server/src/main/java/kafka/server/http/ws/WsConnectionContext.java` | New 6-arg constructor accepting `ExclusiveConsumerManager`, `onClose()` hook, `exclusiveConsumerManager()` accessor |
| `http-server/src/test/java/kafka/server/http/ws/WsSubscriptionManagerTest.java` | 8 new tests for exclusive subscribe/unsubscribe paths |
| `http-server/src/test/java/kafka/server/http/ws/WsConnectionContextTest.java` | 5 new tests for exclusive manager wiring + `onClose()` |
| `ivy-docs/tasks/TASK-WS3.04-exclusive-consumer.md` | Learning / Limitations / Field Notes / File Manifest |

**Skipped (from spec's original "Files to modify" list):**

| File | Why |
|------|-----|
| `http-server/src/main/java/kafka/server/http/routing/QueueManager.java` | Does not exist in this branch; task brief explicitly sanctioned skipping. Queue-level exclusive (vs. subscription-level exclusive delivered here) is deferred until QueueManager lands. |
