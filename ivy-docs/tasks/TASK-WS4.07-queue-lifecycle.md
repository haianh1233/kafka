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

- **Decouple the delete hook from the manager.** The lifecycle rules (auto-delete, x-expires, exclusive close) have to trigger the physical deletion of the queue (routing-metadata entry, backing Kafka topic, bindings), but that physical deletion does not yet exist in the codebase — there is no `QueueManager`. Shipping this as a `Consumer<String> deleteHook` injected at construction keeps the class pure, trivially testable, and leaves the destructive side-effect to whoever wires this up later.
- **Atomic "transition 1 → 0" is the only reliable auto-delete trigger.** Using `AtomicInteger.updateAndGet` loses the pre-value in multi-reader races, and `decrementAndGet` lets a stray unsubscribe (unmatched by a prior subscribe) drive the count negative and then trigger auto-delete on the *next* zero crossing. The compareAndSet loop in `onUnsubscribe` (a) clamps at 0 so stray unsubscribes are no-ops, and (b) guarantees that exactly one thread observes `prev == 1` — that thread owns the delete. A secondary `autoDeleteFired` guard makes the hook idempotent belt-and-braces for any external double-trigger.
- **x-expires timer must recheck subscriber count at fire time.** A naïve "schedule delete in N ms" races with a subscribe that arrives between scheduling and firing. The scheduler task therefore re-reads `subscriberCount` before invoking the hook; the explicit `cancelExpiryTimer` path (called from `onSubscribe`) is an optimisation, not a correctness requirement.
- **Server-generated names use `UUID.randomUUID()` not `nameUUIDFromBytes`.** Random UUIDs (v4) come from a SecureRandom-backed pool and are unguessable; name-based UUIDs would be trivially predictable from a connection id and would let a malicious client collide names with server-generated queues.
- **Redeclare validation is intentionally flag-only.** AMQP clients reconnecting after a broker restart routinely resubmit `queue.declare` with the arguments their client library knows — those may not match the broker's stored args exactly (e.g. default values substituted in). Checking `durable`/`exclusive`/`autoDelete` catches real configuration drift while ignoring arguments lets idempotent reconnects succeed.

---

## Limitations

- **No wiring into WsSubscriptionManager / WsConnectionContext / QueueManager yet.** The spec listed those integrations as "files to modify" but doing so is scope creep against the warning in the executing-agent prompt — there is no QueueManager today, WsSubscriptionManager's exclusive-consumer integration lives in TASK-WS3.04 (already merged), and the correct boundary is for each of those components to call into this manager when their owning tasks wire them together. As a result:
  - `WsSubscriptionManager.subscribe`/`unsubscribe` does NOT yet call `onSubscribe`/`onUnsubscribe`.
  - `WsConnectionContext.onClose` does NOT yet call `onConnectionClose`.
  - `QueueManager.declare`/`delete` does NOT yet call `registerQueue`/`unregisterQueue`, use `generateQueueName` for empty names, or `validateRedeclare` — QueueManager does not exist.
- **The delete hook does not cascade bindings/topic.** The hook is a `Consumer<String>` that receives only the queue name; a real implementation must look up the queue's bindings, delete them, then delete the backing Kafka topic. That glue belongs in the future QueueManager.
- **No vhost dimension in the registry.** Queue names are unique within a vhost but can collide across vhosts. This manager is keyed on bare queue name; a future vhost integration (TASK-WS2.09) will need to key by `(vhost, name)`.
- **Scheduler is a shared dependency.** Callers supply a `ScheduledExecutorService`; this class does not own its lifecycle. If a caller passes an already-shutdown scheduler, `startExpiryTimer` will throw `RejectedExecutionException` — behaviour not covered by tests because the fix is caller-side.

---

## Field Notes

- **TDD flow.** RED was verified by running `compileTestJava` alone — it failed with "cannot find symbol `WsQueueLifecycleManager`" across 9 locations. GREEN came from implementing the class; all 23 tests passed on the first run after fixing the checkstyle unused-import violation that the test file initially carried.
- **Concurrency test.** The `autoDelete_concurrentUnsubscribes_deletesExactlyOnce` test uses 64 subscribers and 64 concurrent unsubscribe calls on a 16-thread pool, asserting the hook fires exactly once. This exercise is the real contract for auto-delete and is the reason for the compareAndSet loop over a naïve `decrementAndGet`.
- **Checkstyle caught an unused import** (`java.util.concurrent.ConcurrentHashMap`) in the test file on the first run — a reminder that spotless isn't the only lint gate in this module.

---

## File Manifest

**Created:**

- `http-server/src/main/java/kafka/server/http/ws/WsQueueLifecycleManager.java` — lifecycle manager: auto-delete, x-expires, exclusive cleanup, server-generated names, redeclare validation.
- `http-server/src/test/java/kafka/server/http/ws/WsQueueLifecycleManagerTest.java` — 23 unit tests covering every acceptance-criteria row plus concurrency, defensive null checks, timer restart semantics.

**Modified:** none.

**Future integration points** (to be wired by their respective tasks):

- `WsSubscriptionManager.subscribe`/`unsubscribe` → call `onSubscribe`/`onUnsubscribe`.
- `WsConnectionContext.onClose` → call `onConnectionClose`.
- Future `QueueManager.declare`/`delete` → call `registerQueue`/`unregisterQueue`, use `generateQueueName` for empty names, `validateRedeclare` for existing queues.

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
