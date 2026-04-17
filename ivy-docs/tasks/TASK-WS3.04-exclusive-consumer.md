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

_To be filled by the executing agent._

---

## Limitations

_To be filled by the executing agent._

---

## Field Notes

_To be filled by the executing agent._

---

## Acceptance Criteria

- [ ] `timeout 300 ./gradlew :http-server:test --tests 'kafka.server.http.ws.ExclusiveConsumerManagerTest' -x spotlessCheck` exits 0
- [ ] `grep -r "ExclusiveConsumerManager" http-server/src/main/java/` returns at least 3 hits
- [ ] Lock acquire uses atomic ConcurrentHashMap operation
- [ ] Learning section filled with at least one entry

---

## File Manifest

_To be filled by the executing agent._
