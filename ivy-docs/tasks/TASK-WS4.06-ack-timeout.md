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

_To be filled by the executing agent._

---

## Limitations

_To be filled by the executing agent._

---

## Field Notes

_To be filled by the executing agent._

---

## Acceptance Criteria

- [ ] `timeout 300 ./gradlew :http-server:test --tests 'kafka.server.http.ws.WsAckTimeoutCheckerTest' -x spotlessCheck` exits 0
- [ ] `grep -r "WsAckTimeoutChecker" http-server/src/main/java/` returns at least 2 hits
- [ ] Auto-NACK requeue=true for timed-out tags
- [ ] ACK_TIMEOUT error frame sent to client
- [ ] Learning section filled with at least one entry

---

## File Manifest

_To be filled by the executing agent._
