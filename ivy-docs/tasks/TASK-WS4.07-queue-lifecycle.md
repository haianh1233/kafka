# TASK-WS4.07: Queue Lifecycle — Auto-Delete, Exclusive, Expires, Server-Generated Names

## Prerequisites

| Task | What it delivers | Why this task needs it |
|------|------------------|-----------------------|
| TASK-WS1.06 | QueueManager — queue CRUD | Base queue lifecycle to extend |
| TASK-WS1.11 | WsSubscriptionManager | Subscribe/unsubscribe events trigger auto-delete |
| TASK-WS3.04 | ExclusiveConsumerManager | Exclusive queue cleanup on disconnect |

---

## Context

The design doc §11.3 specifies advanced queue lifecycle management. This task implements the non-trivial lifecycle behaviors beyond basic CRUD:

1. **Auto-delete queue:** When `autoDelete: true`, the queue (and its backing Kafka topic) is deleted when the last subscriber disconnects.

2. **Exclusive queue on connection close:** When the declaring connection closes, the exclusive queue is deleted along with all bindings and the backing topic.

3. **x-expires:** Auto-delete after N milliseconds with zero consumers. A background timer checks and deletes idle queues.

4. **Server-generated queue names:** When `queue: ""`, the broker generates `q.gen-<UUID>`. These queues respect explicit `exclusive` and `autoDelete` flags.

5. **Queue redeclare validation:** Declaring an existing queue with different `durable`, `exclusive`, or `autoDelete` flags returns `PRECONDITION_FAILED` (409 via REST, error frame via WS).

---

## Specification

### WsQueueLifecycleManager

```java
package kafka.server.http.ws;

public final class WsQueueLifecycleManager {

    /** Track subscription count per queue. Trigger auto-delete when count reaches 0. */
    public void onSubscribe(String queueName);
    public void onUnsubscribe(String queueName);

    /** Handle connection close for exclusive queues. */
    public List<String> onConnectionClose(String connectionId);

    /** Start expiry timer for a queue with x-expires. */
    public void startExpiryTimer(String queueName, long expiresMs);

    /** Cancel expiry timer (consumer subscribed before expiry). */
    public void cancelExpiryTimer(String queueName);

    /** Generate server queue name. */
    public static String generateQueueName();

    /** Validate redeclare: same durable/exclusive/autoDelete flags. */
    public void validateRedeclare(QueueMetadata existing, boolean durable,
                                   boolean exclusive, boolean autoDelete);
}
```

---

## Implementation Details

**Module:** `http-server`

**Files to create:**

| File | What it does |
|------|--------------|
| `http-server/src/main/java/kafka/server/http/ws/WsQueueLifecycleManager.java` | Queue lifecycle: auto-delete, expires, server names, redeclare validation |

**Files to modify:**

| File | What changes |
|------|--------------|
| `http-server/src/main/java/kafka/server/http/ws/WsSubscriptionManager.java` | Call lifecycle manager on subscribe/unsubscribe |
| `http-server/src/main/java/kafka/server/http/routing/QueueManager.java` | Use lifecycle manager for redeclare validation and server-generated names |
| `http-server/src/main/java/kafka/server/http/ws/WsConnectionContext.java` | Call lifecycle manager on connection close |

> **CRITICAL:** Auto-delete must check subscriber count AFTER the unsubscribe completes. Race condition: two unsubscribes happening simultaneously must not both trigger delete. Use atomic decrement-and-check.

> **CRITICAL:** x-expires timer must be CANCELLED when a new consumer subscribes. The timer only fires when the queue has been idle (zero consumers) for N milliseconds continuously.

> **CRITICAL:** Server-generated names use format `q.gen-<UUID>`. The UUID must be a random UUID (not time-based) to prevent prediction.

> **CRITICAL:** Redeclare validation checks ONLY `durable`, `exclusive`, `autoDelete` flags. Queue arguments (x-message-ttl etc.) are NOT checked on redeclare.

**Implementation order:**
1. Create WsQueueLifecycleManager
2. Implement auto-delete on last-subscriber-disconnect
3. Implement x-expires timer
4. Implement server-generated queue names
5. Implement redeclare validation
6. Wire into subscribe/unsubscribe/close/declare paths
7. Add unit tests

---

## Skeleton Code

```java
package kafka.server.http.ws;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Manages queue lifecycle: auto-delete, x-expires, server-generated names.
 *
 * // Time: Created - TASK-WS4.07
 */
public final class WsQueueLifecycleManager {

    private static final String GEN_PREFIX = "q.gen-";

    private final ConcurrentHashMap<String, AtomicInteger> subscriberCounts =
        new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ScheduledFuture<?>> expiryTimers =
        new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler;

    public WsQueueLifecycleManager(ScheduledExecutorService scheduler) {
        this.scheduler = scheduler;
    }

    public void onSubscribe(String queueName) {
        subscriberCounts.computeIfAbsent(queueName, k -> new AtomicInteger(0))
            .incrementAndGet();
        cancelExpiryTimer(queueName);
    }

    public void onUnsubscribe(String queueName) {
        AtomicInteger count = subscriberCounts.get(queueName);
        if (count != null && count.decrementAndGet() == 0) {
            // TODO: If autoDelete, delete queue
            // TODO: If x-expires, start expiry timer
        }
    }

    public void startExpiryTimer(String queueName, long expiresMs) {
        ScheduledFuture<?> future = scheduler.schedule(
            () -> { /* TODO: Delete queue if still zero consumers */ },
            expiresMs, TimeUnit.MILLISECONDS);
        ScheduledFuture<?> old = expiryTimers.put(queueName, future);
        if (old != null) old.cancel(false);
    }

    public void cancelExpiryTimer(String queueName) {
        ScheduledFuture<?> future = expiryTimers.remove(queueName);
        if (future != null) future.cancel(false);
    }

    public static String generateQueueName() {
        return GEN_PREFIX + UUID.randomUUID();
    }

    public void validateRedeclare(boolean existingDurable, boolean existingExclusive,
                                   boolean existingAutoDelete,
                                   boolean newDurable, boolean newExclusive,
                                   boolean newAutoDelete) {
        if (existingDurable != newDurable || existingExclusive != newExclusive
                || existingAutoDelete != newAutoDelete) {
            throw new IllegalStateException("PRECONDITION_FAILED: queue flags differ");
        }
    }
}
```

---

## Tests

**Test class:** `http-server/src/test/java/kafka/server/http/ws/WsQueueLifecycleManagerTest.java`

| Test method | What it verifies |
|-------------|-----------------|
| `autoDelete_lastUnsubscribe_deletesQueue` | Queue deleted when subscriber count reaches 0 |
| `autoDelete_notLastSubscriber_noDelete` | Queue not deleted when other subscribers remain |
| `xExpires_idleTimeout_deletesQueue` | Queue deleted after x-expires ms with zero consumers |
| `xExpires_cancelled_onNewSubscriber` | Expiry timer cancelled when subscriber joins |
| `serverGeneratedName_format` | Name matches `q.gen-<UUID>` pattern |
| `serverGeneratedName_unique` | Two generated names are different |
| `redeclare_sameFlags_succeeds` | Idempotent redeclare with same flags OK |
| `redeclare_differentFlags_preconditionFailed` | Different flags → PRECONDITION_FAILED |
| `redeclare_argumentsIgnored` | Queue arguments not checked on redeclare |
| `connectionClose_deletesExclusiveQueues` | Exclusive queues cleaned up on disconnect |

**Run command:**
```bash
timeout 300 ./gradlew :http-server:test --tests 'kafka.server.http.ws.WsQueueLifecycleManagerTest' -x spotlessCheck
```

---

## Rules

- Auto-delete fires ONLY when subscriber count reaches 0.
- x-expires timer cancelled when any consumer subscribes.
- Server queue names: `q.gen-<random-UUID>`.
- Redeclare validation: check durable/exclusive/autoDelete only, NOT arguments.
- PRECONDITION_FAILED on flag mismatch.
- Atomic subscriber count decrement-and-check to prevent race conditions.

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

- [ ] `timeout 300 ./gradlew :http-server:test --tests 'kafka.server.http.ws.WsQueueLifecycleManagerTest' -x spotlessCheck` exits 0
- [ ] `grep -r "WsQueueLifecycleManager" http-server/src/main/java/` returns at least 3 hits
- [ ] Server-generated names match `q.gen-*` pattern
- [ ] Redeclare validates durable/exclusive/autoDelete but not arguments
- [ ] Learning section filled with at least one entry

---

## File Manifest

_To be filled by the executing agent._
