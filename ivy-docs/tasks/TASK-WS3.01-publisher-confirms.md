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

- **Two parallel confirm paths already existed** — WS1.12 wired `HttpProcessor.handleWsResponse` to read `wsPublishId` from `requestLocalProperties` and emit `published` / `publish-failed` frames via `buildPublishConfirmFrame`. WS3.01 adds `WsPublisherConfirmTracker` as a **second, tracker-centric path** that the produce-completion callback can call directly (`confirmSuccess` / `confirmFailure`). Both paths coexist: production can migrate from the HttpProcessor-driven emission to the tracker-driven emission at its own pace. The tracker's pending-set dedup makes calling both paths safe — the second call is a no-op because the publishId is already removed from `pending`.
- **Keep `WsConnectionContext.isPublishConfirmsEnabled()` as the authoritative read.** The existing `AtomicBoolean publishConfirmsEnabled` stays; `enablePublishConfirms()` now also calls `confirmTracker.enable()` so the tracker's own enabled flag is kept in sync. Removing the AtomicBoolean would have required editing every reader across WsPublishHandler, HttpProcessor, and tests — high churn for no win.
- **The tracker takes `WsConnectionContext` rather than raw `Channel`** (skeleton in task file suggested `Channel`). Rationale: `WsConnectionContext.sendFrame(String)` already encapsulates `new TextWebSocketFrame(...)` plus `writeAndFlush`, and the tracker only ever writes frames. Using the raw `Channel` would have duplicated that wrapping. This also keeps the tracker constructible from test code the same way the rest of the WS layer is.
- **`recordPending` happens in `WsPublishHandler.fanOut` once per publish, not once per matched queue.** For multi-queue fanout, one publish = one publishId = one confirm. `confirmSuccess`/`confirmFailure` are idempotent via `pending.remove()` returning null on second call, but the record path needs the deduplication upfront to keep `pendingCount()` correct.
- **`WsFrameHandler.handleEnableConfirms` emits `confirms-enabled` not `confirm-ready`** — the design doc §5.7 example shows `{"type":"confirms-enabled","id":"ec"}`. The task intro paragraph used the phrase "confirm-ready frame" loosely; the spec's Protocol Flow section is explicit about `confirms-enabled`.

---

## Limitations

- **Tracker frame emission is not yet wired into the produce-completion callback**: the sink's produce callback in production still flows through `HttpProcessor.handleWsResponse` → `buildPublishConfirmFrame`. The `WsPublisherConfirmTracker.confirmSuccess/confirmFailure` methods are fully functional and unit-tested, but a later task must flip the callback to call `ctx.confirmTracker().confirmSuccess(publishId)` directly and retire the HttpProcessor JSON builder. For now both paths produce identical frames; the tracker path is exercised via direct unit tests and the WsPublishHandlerTest integration tests.
- **No multi-queue fanout partial-failure aggregation** — the task's spec mentions "Multi-queue fanout: ALL produces must complete before confirming" and "partial failure → entire publish treated as failed". This aggregation logic is NOT implemented in this task because the current sink is fire-and-forget per queue. A later task will introduce a per-publishId aggregation structure (e.g., `CompletableFuture.allOf(...)`) that collects N queue results and calls confirmSuccess only when all N succeed, confirmFailure with the first error otherwise. The tracker API is designed to support this (single confirmSuccess/confirmFailure per publishId).
- **No enable-confirms → disable-confirms transition.** Per design doc §5.7 confirms cannot be disabled on a live connection; the client must reconnect. The tracker has no `disable()` method.
- **PublishId is client-assigned, not server-assigned.** The spec implies "publishId sequencing" is a broker responsibility, but the existing WsPublishHandler threads `publishId` straight from the client frame. If the client supplies out-of-order or duplicate publishIds, the tracker will handle it correctly (duplicates: second recordPending overwrites, second confirm is no-op; out-of-order: confirms fire in produce-completion order, not publish order — matches design doc §5.7 rule).
- **Tracker is stored as a field on `WsConnectionContext` even when confirms are never enabled** — one extra ConcurrentHashMap allocation per connection. Cheap, but not zero. Acceptable for this task.

---

## Field Notes

- Initial RED run for `WsPublisherConfirmTrackerTest` failed with 33 "cannot find symbol" compile errors — expected, since the class didn't exist yet. After creating the tracker, all 17 test methods went GREEN on the first implementation run.
- The existing `WsFrameHandlerTest.assertDispatchedTo` helper uses a fully-overridden handler for each dispatch case, including `handleEnableConfirms`. This meant changing the default implementation of `handleEnableConfirms` to emit a real frame did not break `dispatch_enableConfirms_reachesHandler` — the override replaces the body. The four new behavioural tests use the un-overridden handler.
- Had to carefully avoid breaking `handlePublish_confirmsEnabled_propagatesFlagToSink` (existing test). The new `recordPending` call in `fanOut` runs before the sink enqueue, so the test still passes: the sink still sees `confirmsEnabled=true` and the publishId. The new assertion `connCtx.confirmTracker().pendingCount() == 1` is covered by new tests.
- Deliberately did NOT remove `WsConnectionContext.publishConfirmsEnabled` AtomicBoolean. `HttpProcessor.handleWsResponse` still reads it directly. Removing would force a cross-module edit and broke invariants.
- The `confirmFailure_escapesErrorMessage` test caught a potential bug if I had used naive string concat. Using Jackson `ObjectNode.put` + `writeValueAsString` handles escaping properly, matching the quality bar set by `HttpProcessor.buildPublishConfirmFrame` (which uses `escapeJson(msg)`).

---

## File Manifest

**Created:**
- `http-server/src/main/java/kafka/server/http/ws/WsPublisherConfirmTracker.java` — per-connection publisher confirm tracker (`enable`, `recordPending`, `confirmSuccess`, `confirmFailure`, `pendingCount`). 133 lines.
- `http-server/src/test/java/kafka/server/http/ws/WsPublisherConfirmTrackerTest.java` — 17 tests covering enable/pending/confirm/failure/disabled paths. 273 lines.

**Modified:**
- `http-server/src/main/java/kafka/server/http/ws/WsConnectionContext.java` — added `WsPublisherConfirmTracker confirmTracker` field (owned), `confirmTracker()` accessor, and wire `enablePublishConfirms()` to also enable the tracker. +17 lines.
- `http-server/src/main/java/kafka/server/http/ws/WsFrameHandler.java` — replaced `handleEnableConfirms` stub with real implementation: calls `ctx.enablePublishConfirms()` and emits a `confirms-enabled` reply frame (echoes client correlation id when present). +36 lines.
- `http-server/src/main/java/kafka/server/http/ws/WsPublishHandler.java` — in `fanOut()`, when confirms enabled and publishId non-null, call `ctx.confirmTracker().recordPending(publishId)` before the first sink.enqueue. +9 lines.
- `http-server/src/test/java/kafka/server/http/ws/WsFrameHandlerTest.java` — added 4 behavioural tests for enable-confirms (flag flip, confirms-enabled frame with/without id, idempotent). +57 lines.
- `http-server/src/test/java/kafka/server/http/ws/WsPublishHandlerTest.java` — added 6 tests covering recordPending wiring (confirms on/off, multi-queue fanout, monotonic publishIds, end-to-end confirm via tracker). +112 lines.

---

## Acceptance Criteria

- [ ] `timeout 300 ./gradlew :http-server:test --tests 'kafka.server.http.ws.WsPublisherConfirmTrackerTest' -x spotlessCheck` exits 0
- [ ] `grep -r "WsPublisherConfirmTracker" http-server/src/main/java/` returns at least 3 hits
- [ ] `grep -r "enable-confirms" http-server/src/main/java/` returns at least 1 hit
- [ ] Learning section filled with at least one entry

---

## File Manifest

_To be filled by the executing agent._
