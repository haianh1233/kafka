# TASK-WS3.01: Publisher Confirms

## Prerequisites

| Task | What it delivers | Why this task needs it |
|------|------------------|-----------------------|
| TASK-WS1.09 | WsPublishHandler — publish → routing → ProduceRequest | Confirms are responses to publish operations |
| TASK-WS1.02 | WsFrameHandler — JSON frame dispatcher | Must dispatch `enable-confirms` frame type |
| TASK-WS1.03 | WsConnectionContext — per-connection state | Stores confirms-enabled flag and publishId sequence |

---

## Context

The design doc §5.7 specifies publisher confirms for WebSocket clients. When a client sends an `enable-confirms` frame, the broker begins tracking per-publish sequence numbers and sends `published` or `publish-failed` responses after each produce completes.

**Protocol flow:**

1. Client sends `{"type":"enable-confirms","id":"ec"}` → broker replies `{"type":"confirms-enabled","id":"ec"}`
2. Client includes `publishId` on each `publish` frame (monotonically increasing per connection)
3. On successful produce: `{"type":"published","publishId":1}`
4. On failed produce: `{"type":"publish-failed","publishId":1,"errorCode":"NOT_ENOUGH_REPLICAS","errorMessage":"ISR below minimum"}`

**Edge cases:**
- `publishId` without confirms enabled → silently ignored (no response sent)
- `publishId` out of order → server tracks, but order of confirm responses is the order of completion (not publish order)
- Multi-queue fanout partial failure → entire publish treated as failed, `publish-failed` returned

---

## Specification

### WsPublisherConfirmTracker

```java
package kafka.server.http.ws;

public final class WsPublisherConfirmTracker {

    /** Enable confirms on this connection. */
    public void enable();

    /** Check if confirms are enabled. */
    public boolean isEnabled();

    /** Record a pending publish. Called when publish frame is received. */
    public void recordPending(long publishId);

    /** Mark publish as succeeded. Triggers confirm frame write. */
    public void confirmSuccess(long publishId);

    /** Mark publish as failed. Triggers publish-failed frame write. */
    public void confirmFailure(long publishId, String errorCode, String errorMessage);

    /** Get count of in-flight (pending) confirms. */
    public int pendingCount();
}
```

---

## Implementation Details

**Module:** `http-server`

**Files to create:**

| File | What it does |
|------|--------------|
| `http-server/src/main/java/kafka/server/http/ws/WsPublisherConfirmTracker.java` | Per-connection confirm tracking |

**Files to modify:**

| File | What changes |
|------|--------------|
| `http-server/src/main/java/kafka/server/http/ws/WsFrameHandler.java` | Handle `enable-confirms` frame type |
| `http-server/src/main/java/kafka/server/http/ws/WsPublishHandler.java` | After ProduceResponse, call confirmSuccess/confirmFailure |
| `http-server/src/main/java/kafka/server/http/ws/WsConnectionContext.java` | Add WsPublisherConfirmTracker field |

> **CRITICAL:** Confirm responses are written to the WebSocket channel from the produce completion callback (which runs on `httpAsyncExecutor`). `channel.writeAndFlush()` from a non-Netty thread is safe (Netty schedules on the event loop).

> **CRITICAL:** For multi-queue fanout, wait for ALL produce futures to complete before sending the confirm. `CompletableFuture.allOf(...)` then check all results.

**Implementation order:**
1. Create WsPublisherConfirmTracker with enable/pending/confirm/failure methods
2. Add `enable-confirms` dispatch to WsFrameHandler
3. Modify WsPublishHandler produce completion callback to trigger confirms
4. Add unit tests

---

## Skeleton Code

```java
package kafka.server.http.ws;

import io.netty.channel.Channel;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Tracks publisher confirms for a WebSocket connection.
 *
 * // Time: Created - TASK-WS3.01
 */
public final class WsPublisherConfirmTracker {

    private final Channel channel;
    private final AtomicBoolean enabled = new AtomicBoolean(false);
    private final ConcurrentHashMap<Long, Boolean> pending = new ConcurrentHashMap<>();

    public WsPublisherConfirmTracker(Channel channel) {
        this.channel = Objects.requireNonNull(channel, "channel");
    }

    public void enable() {
        enabled.set(true);
    }

    public boolean isEnabled() {
        return enabled.get();
    }

    public void recordPending(long publishId) {
        if (enabled.get()) {
            pending.put(publishId, Boolean.TRUE);
        }
    }

    public void confirmSuccess(long publishId) {
        if (pending.remove(publishId) != null) {
            // TODO: Write {"type":"published","publishId":N} to channel
        }
    }

    public void confirmFailure(long publishId, String errorCode, String errorMessage) {
        if (pending.remove(publishId) != null) {
            // TODO: Write {"type":"publish-failed","publishId":N,...} to channel
        }
    }

    public int pendingCount() {
        return pending.size();
    }
}
```

---

## Tests

**Test class:** `http-server/src/test/java/kafka/server/http/ws/WsPublisherConfirmTrackerTest.java`

| Test method | What it verifies |
|-------------|-----------------|
| `enable_setsFlag` | isEnabled() returns true after enable() |
| `recordPending_whenDisabled_noOp` | No pending entry when confirms not enabled |
| `confirmSuccess_writesPublishedFrame` | Confirm writes JSON with type=published |
| `confirmFailure_writesPublishFailedFrame` | Failure writes JSON with errorCode/errorMessage |
| `confirmSuccess_unknownPublishId_noOp` | Confirming non-pending ID is silently ignored |
| `pendingCount_tracksInFlight` | Count increments on record, decrements on confirm |
| `publishIdWithoutConfirmsEnabled_silentlyIgnored` | No response when confirms disabled |

**Run command:**
```bash
timeout 300 ./gradlew :http-server:test --tests 'kafka.server.http.ws.WsPublisherConfirmTrackerTest' -x spotlessCheck
```

---

## Rules

- Confirms are per-connection, not per-subscription.
- `published` and `publish-failed` are the only confirm response types.
- publishId without confirms enabled → silently ignored.
- Multi-queue fanout: ALL produces must complete before confirming.
- Confirm frame write uses `channel.writeAndFlush()` — thread-safe from any thread.

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

- [ ] `timeout 300 ./gradlew :http-server:test --tests 'kafka.server.http.ws.WsPublisherConfirmTrackerTest' -x spotlessCheck` exits 0
- [ ] `grep -r "WsPublisherConfirmTracker" http-server/src/main/java/` returns at least 3 hits
- [ ] `grep -r "enable-confirms" http-server/src/main/java/` returns at least 1 hit
- [ ] Learning section filled with at least one entry

---

## File Manifest

_To be filled by the executing agent._
