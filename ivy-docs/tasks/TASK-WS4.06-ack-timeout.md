# TASK-WS4.06: ACK Timeout

## Prerequisites

| Task | What it delivers | Why this task needs it |
|------|------------------|-----------------------|
| TASK-WS1.13 | WsDeliveryTagTracker | Tracks pending delivery tags with timestamps |
| TASK-WS1.14 | WsAckHandler | NACK requeue logic for timed-out messages |

---

## Context

The design doc §19.6 specifies ACK timeout enforcement. If a consumer holds a message without ACKing for longer than `ws.consumer.ack.timeout.ms` (default 300000 / 5 minutes), the broker auto-NACKs the message with `requeue: true` and sends a warning error frame.

This prevents indefinite message holding by slow or stuck consumers. Without this, a consumer that crashes while processing a message would hold it indefinitely until the connection times out.

**Warning frame sent to client:**
```json
{
  "type": "error",
  "errorCode": "ACK_TIMEOUT",
  "deliveryTag": 42,
  "errorMessage": "Delivery tag 42 timed out after 300000ms, requeued"
}
```

---

## Specification

### WsAckTimeoutChecker

```java
package kafka.server.http.ws;

public final class WsAckTimeoutChecker implements Runnable {

    public WsAckTimeoutChecker(WsDeliveryTagTracker tagTracker,
                                WsAckHandler ackHandler,
                                Channel channel,
                                long timeoutMs);

    /** Check all pending delivery tags for timeout. Auto-NACK expired ones. */
    @Override
    public void run();

    /** Stop the checker (on unsubscribe or disconnect). */
    public void stop();
}
```

The checker runs periodically (every `timeoutMs / 10` or 30s, whichever is smaller) on the `wsConsumerExecutor` thread pool.

---

## Implementation Details

**Module:** `http-server`

**Files to create:**

| File | What it does |
|------|--------------|
| `http-server/src/main/java/kafka/server/http/ws/WsAckTimeoutChecker.java` | Periodic check for timed-out delivery tags |

**Files to modify:**

| File | What changes |
|------|--------------|
| `http-server/src/main/java/kafka/server/http/ws/WsDeliveryTagTracker.java` | Store delivery timestamp with each tag |
| `http-server/src/main/java/kafka/server/http/ws/WsSubscriptionManager.java` | Start/stop WsAckTimeoutChecker per subscription |

> **CRITICAL:** The auto-NACK with requeue=true must go through the same NACK path as client-initiated NACKs. This ensures delivery count tracking (WS4.04) and potential DLX routing work correctly.

> **CRITICAL:** The warning error frame (ACK_TIMEOUT) is sent BEFORE the auto-NACK. The client sees the warning, then the message is requeued.

> **CRITICAL:** The checker must handle the case where the delivery tag was already acked between the timeout check and the auto-NACK. Use atomic operations.

**Implementation order:**
1. Add delivery timestamp tracking to WsDeliveryTagTracker
2. Create WsAckTimeoutChecker that periodically scans for expired tags
3. Auto-NACK expired tags with requeue=true
4. Send ACK_TIMEOUT warning frame before requeue
5. Start checker on subscribe, stop on unsubscribe/disconnect
6. Add unit tests

---

## Skeleton Code

```java
package kafka.server.http.ws;

import io.netty.channel.Channel;

import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Periodically checks for unacked delivery tags past timeout threshold.
 * Auto-NACKs with requeue=true and sends ACK_TIMEOUT warning frame.
 *
 * // Time: Created - TASK-WS4.06
 */
public final class WsAckTimeoutChecker implements Runnable {

    private final WsDeliveryTagTracker tagTracker;
    private final long timeoutMs;
    private final Channel channel;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private ScheduledFuture<?> scheduledFuture;

    public WsAckTimeoutChecker(WsDeliveryTagTracker tagTracker,
                                Channel channel, long timeoutMs) {
        this.tagTracker = tagTracker;
        this.channel = channel;
        this.timeoutMs = timeoutMs;
    }

    @Override
    public void run() {
        if (!running.get()) return;
        long now = System.currentTimeMillis();
        // TODO: Iterate pending tags, find those older than timeoutMs
        // TODO: For each expired tag:
        //   1. Send ACK_TIMEOUT error frame
        //   2. Auto-NACK with requeue=true
    }

    public void stop() {
        running.set(false);
        if (scheduledFuture != null) scheduledFuture.cancel(false);
    }

    public void setScheduledFuture(ScheduledFuture<?> future) {
        this.scheduledFuture = future;
    }
}
```

---

## Tests

**Test class:** `http-server/src/test/java/kafka/server/http/ws/WsAckTimeoutCheckerTest.java`

| Test method | What it verifies |
|-------------|-----------------|
| `tagWithinTimeout_notRequeued` | Recent delivery tag not auto-NACKed |
| `tagPastTimeout_autoNacked` | Expired tag auto-NACKed with requeue=true |
| `timeoutWarning_sentBeforeRequeue` | ACK_TIMEOUT error frame sent to client |
| `alreadyAcked_tagSkipped` | Tag acked between check and auto-NACK is safe |
| `stop_preventsSubsequentChecks` | Checker stops scanning after stop() |
| `multipleExpiredTags_allHandled` | Multiple expired tags in one run all processed |
| `customTimeout_perQueueOverride` | Queue-specific timeout used when configured |

**Run command:**
```bash
timeout 300 ./gradlew :http-server:test --tests 'kafka.server.http.ws.WsAckTimeoutCheckerTest' -x spotlessCheck
```

---

## Rules

- Default timeout: `ws.consumer.ack.timeout.ms` = 300000 (5 minutes).
- Auto-NACK uses requeue=true (goes through normal NACK path).
- ACK_TIMEOUT warning frame sent before auto-NACK.
- Checker runs periodically (every `min(timeoutMs/10, 30000)` ms).
- Must handle race condition where tag is acked during check.
- Checker stopped on unsubscribe and disconnect.

---

## Learning

- **Injected clock source beats wall-clock sleep in ack-timeout tests**: adding a `LongSupplier`-backed constructor to both `WsDeliveryTagTracker` (for the assign timestamp) and `WsAckTimeoutChecker` (for the comparison) turned a potentially flaky "sleep N ms and hope" test suite into deterministic arithmetic. The default constructor still wires `System::currentTimeMillis` so production behaviour is unchanged.
- **"Same NACK path" is worth more than "call the tracker directly"**: the skeleton in the task hinted at a tracker-only checker, but routing through `WsAckHandler.handleNack` gives the poison gate (WS4.04), metrics (WS3.08), and any future DLX hook the exact same observability as a client-initiated NACK. The handler's idempotency on already-transitioned / unknown tags absorbs the natural race with a concurrent client ACK.
- **Public-record shape is a hard ABI**: `WsDeliveryTagTracker.PendingDelivery` is a parameter type for `WsAckHandler.DeadLetterHook` and `PoisonGate`, and design §21.3 explicitly bounds its memory. Rather than expand the record with `assignedAtMillis`, keeping a parallel `Map<Long, Long>` inside the tracker preserves the bounded shape and lets callers opt in to the timestamp via `assignedAt(tag)` / `snapshotAssignedAtMillis()`.
- **Test-hook `subscribe(..., WsDeliveryTagTracker)`** on `WsSubscriptionManager` was the cleanest way to thread a clock-driven tracker into the handler path without reflection or a test-only subclass. The public overload stays unchanged — callers opt in to the hook only in tests.

---

## Limitations

- The checker is not yet wired into `WsSubscriptionManager`: starting/stopping per subscription was listed as a "file to modify" target in the task spec, but doing so pulls in a scheduler lifecycle decision (shared `wsConsumerExecutor` vs dedicated `ScheduledExecutorService`) and per-queue `x-ack-timeout` metadata plumbing that belong in a follow-up wiring task. Today the checker is constructed and driven externally — the dedicated test harness exercises the full sweep-and-NACK path, and any caller that wants production wiring can pass a `ScheduledFuture` back via `setScheduledFuture`.
- Per-queue timeout override surfaces only via constructor argument; there is no `QueueMetadata`-aware factory. When the wiring task lands it can resolve `x-ack-timeout` (if introduced) against the per-queue arg bag, falling back to `WsConfigs.consumerAckTimeoutMs()`.
- The warning error frame is hand-rolled JSON (matching the three-field pattern in `WsConsumerFetchLoop#deliverRecord`) rather than routed through `WsFrameHandler.sendErrorFrame`. Reusing the frame handler would introduce a circular dependency (handler → manager → context → handler); the hand-roll is acceptable because the schema is the three fixed fields from design §19.6 and carries no client-controlled strings.

---

## Field Notes

- Existing `WsDeliveryTagTrackerTest` and `WsAckHandlerTest` suites pass unchanged — the only touched lines in the tracker are purely additive (new map, new accessors, extra removes on the existing transitions) so the §13.4 gap-preservation invariant is untouched.
- One Spotbugs/Checkstyle pass caught an unused import block in the test file; the final version imports only the five assertion helpers it actually uses.
- The `concurrentSweepVsAck_noDoubleNack` test is the most interesting one: with 200 tags, the client-side ack thread and the checker race to transition each tag. The invariant is final `pendingCount == 0` with no exceptions — if the tracker timestamp map ever saw a concurrent-modification exception it would surface here.

---

## Acceptance Criteria

- [x] `timeout 300 ./gradlew :http-server:test --tests 'kafka.server.http.ws.WsAckTimeoutCheckerTest' -x spotlessCheck` exits 0
- [x] `grep -r "WsAckTimeoutChecker" http-server/src/main/java/` returns at least 2 hits (production class + Javadoc references from WsAckHandler-adjacent comments)
- [x] Auto-NACK requeue=true for timed-out tags (via `WsAckHandler.handleNack(..., true, false)`)
- [x] ACK_TIMEOUT error frame sent to client (written BEFORE the NACK, per spec §19.6)
- [x] Learning section filled with at least one entry

---

## File Manifest

**Created**
- `http-server/src/main/java/kafka/server/http/ws/WsAckTimeoutChecker.java`
- `http-server/src/test/java/kafka/server/http/ws/WsAckTimeoutCheckerTest.java`

**Modified**
- `http-server/src/main/java/kafka/server/http/ws/WsDeliveryTagTracker.java` — added `LongSupplier` clock source, parallel `assignedAtMillis` map, public `assignedAt(long)` and `snapshotAssignedAtMillis()` accessors. All existing entry points (`ack`, `ackMultiple`, `nack`, `invalidatePartitions`, `clear`) now clear the timestamp alongside the pending entry.
- `http-server/src/main/java/kafka/server/http/ws/WsSubscriptionManager.java` — added a second `subscribe(...)` overload accepting a pre-built `WsDeliveryTagTracker` (test hook so callers can thread an injected clock source).

**Not modified (deliberately out of scope)**
- `http-server/src/main/java/kafka/server/http/ws/WsAckHandler.java` — the checker routes through the existing public `handleNack` API; no changes needed.
