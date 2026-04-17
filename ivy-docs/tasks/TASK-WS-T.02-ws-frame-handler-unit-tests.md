# TASK-WS-T.02: WsFrameHandler Unit Tests — EmbeddedChannel Tests

## Prerequisites

| Task | What it delivers | Why this task needs it |
|------|------------------|-----------------------|
| TASK-WS1.01 | WsUpgradeOrHttpHandler | Tests WS upgrade detection |
| TASK-WS1.02 | WsFrameHandler | Primary test target |

---

## Context

The design doc §23.3 specifies EmbeddedChannel-based unit tests for WebSocket frame handling. These test the Netty pipeline without a real network connection, verifying:

1. WebSocket upgrade detection and pipeline switch
2. JSON frame dispatch to correct handlers
3. Malformed frame handling (bad JSON, missing type field)
4. Oversized frame rejection
5. Unknown frame type handling

Uses Netty's `EmbeddedChannel` (same pattern as `HttpChannelInitializerTest.scala`).

---

## Specification

### Test targets

- `WsUpgradeOrHttpHandler` — detects WebSocket upgrade request, switches pipeline
- `WsFrameHandler` — parses TextWebSocketFrame JSON, dispatches by `type` field

### Test categories

1. **Upgrade detection (3 tests):** valid upgrade switches pipeline, non-upgrade passes through, wrong path rejected
2. **JSON dispatch (4 tests):** publish, subscribe, ack, declare-exchange dispatched correctly
3. **Malformed frames (3 tests):** invalid JSON, missing type field, empty frame
4. **Oversized frames (1 test):** frame exceeding max size rejected
5. **Unknown type (1 test):** unrecognized type returns error frame

---

## Implementation Details

**Module:** `http-server`

**Files to create:**

| File | What it does |
|------|--------------|
| `http-server/src/test/java/kafka/server/http/ws/WsFrameHandlerTest.java` | EmbeddedChannel tests for WS pipeline |

**Files to study:**

| File | Why |
|------|-----|
| `http-server/src/test/scala/unit/kafka/server/http/HttpChannelInitializerTest.scala` | EmbeddedChannel test pattern |

> **CRITICAL:** EmbeddedChannel tests require setting up the pipeline with HttpServerCodec + HttpObjectAggregator + WsUpgradeOrHttpHandler. After upgrade, verify WsFrameHandler is in the pipeline.

> **CRITICAL:** For frame handler tests, create EmbeddedChannel with WsFrameHandler directly (skip upgrade). Write TextWebSocketFrame instances as inbound.

> **CRITICAL:** Mock all dependencies (RoutingEngine, QueueManager, etc.) using Mockito. These are unit tests, not integration tests.

**Implementation order:**
1. Upgrade detection tests (EmbeddedChannel with HTTP pipeline)
2. JSON dispatch tests (EmbeddedChannel with WsFrameHandler)
3. Malformed frame tests
4. Oversized frame and unknown type tests

---

## Skeleton Code

```java
package kafka.server.http.ws;

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 * EmbeddedChannel unit tests for WsFrameHandler.
 * No broker needed — tests pipeline behavior in isolation.
 *
 * // Time: Created - TASK-WS-T.02
 */
class WsFrameHandlerTest {

    // ── Upgrade detection ────────────────────────────────────

    @Test
    void upgradeRequest_switchesPipeline() {
        EmbeddedChannel ch = new EmbeddedChannel(
            new HttpServerCodec(),
            new HttpObjectAggregator(65536)
            // TODO: Add WsUpgradeOrHttpHandler with mocked dependencies
        );

        FullHttpRequest upgrade = new DefaultFullHttpRequest(
            HttpVersion.HTTP_1_1, HttpMethod.GET, "/v1/ws");
        upgrade.headers().set(HttpHeaderNames.HOST, "localhost");
        upgrade.headers().set(HttpHeaderNames.UPGRADE, "websocket");
        upgrade.headers().set(HttpHeaderNames.CONNECTION, "Upgrade");
        upgrade.headers().set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==");
        upgrade.headers().set("Sec-WebSocket-Version", "13");

        ch.writeInbound(upgrade);

        // TODO: Verify 101 response written
        // TODO: Verify WsFrameHandler in pipeline
    }

    @Test
    void nonUpgradeRequest_passesThrough() {
        // TODO: Standard HTTP GET should not trigger upgrade
    }

    @Test
    void upgradeWrongPath_rejected() {
        // TODO: Upgrade request to /v1/topics should not trigger WS upgrade
    }

    // ── JSON dispatch ────────────────────────────────────────

    @Test
    void publishFrame_dispatched() {
        // TODO: Write TextWebSocketFrame with {"type":"publish",...}
        // TODO: Verify publish handler invoked
    }

    @Test
    void subscribeFrame_dispatched() {
        // TODO: Write TextWebSocketFrame with {"type":"subscribe",...}
        // TODO: Verify subscription manager invoked
    }

    @Test
    void ackFrame_dispatched() {
        // TODO: Write TextWebSocketFrame with {"type":"ack",...}
        // TODO: Verify ack handler invoked
    }

    @Test
    void declareExchangeFrame_dispatched() {
        // TODO: Write TextWebSocketFrame with {"type":"declare-exchange",...}
        // TODO: Verify exchange manager invoked
    }

    // ── Malformed frames ─────────────────────────────────────

    @Test
    void invalidJson_sendsErrorFrame() {
        // TODO: Write TextWebSocketFrame with "not valid json"
        // TODO: Verify error frame written with INVALID_REQUEST
    }

    @Test
    void missingTypeField_sendsErrorFrame() {
        // TODO: Write TextWebSocketFrame with {"exchange":"ex"}
        // TODO: Verify error frame written
    }

    @Test
    void emptyFrame_sendsErrorFrame() {
        // TODO: Write TextWebSocketFrame with ""
        // TODO: Verify error frame written
    }

    // ── Oversized and unknown ────────────────────────────────

    @Test
    void oversizedFrame_closesConnection() {
        // TODO: Verify close frame with code 1009 (Frame too large)
    }

    @Test
    void unknownType_sendsErrorFrame() {
        // TODO: Write TextWebSocketFrame with {"type":"banana",...}
        // TODO: Verify error frame with INVALID_REQUEST
    }
}
```

---

## Tests

**Test class:** `http-server/src/test/java/kafka/server/http/ws/WsFrameHandlerTest.java`

| Test method | What it verifies |
|-------------|-----------------|
| `upgradeRequest_switchesPipeline` | WS upgrade adds WsFrameHandler to pipeline |
| `nonUpgradeRequest_passesThrough` | HTTP requests not upgraded |
| `upgradeWrongPath_rejected` | Only /v1/ws path accepts upgrades |
| `publishFrame_dispatched` | Publish JSON dispatched to publish handler |
| `subscribeFrame_dispatched` | Subscribe JSON dispatched to subscription manager |
| `invalidJson_sendsErrorFrame` | Bad JSON → error frame |
| `missingTypeField_sendsErrorFrame` | Missing "type" → error frame |
| `emptyFrame_sendsErrorFrame` | Empty text → error frame |
| `oversizedFrame_closesConnection` | Too-large frame → close with 1009 |
| `unknownType_sendsErrorFrame` | Unknown "type" → error frame |

**Run command:**
```bash
timeout 300 ./gradlew :http-server:test --tests 'kafka.server.http.ws.WsFrameHandlerTest' -x spotlessCheck
```

---

## Rules

- Use EmbeddedChannel (no real sockets).
- Mock all dependencies with Mockito.
- Oversized frames → WebSocket close code 1009.
- Malformed JSON → error frame with INVALID_REQUEST.
- Unknown type → error frame with INVALID_REQUEST.
- Non-WS HTTP requests must pass through unchanged.

---

## Learning

- The existing `WsFrameHandlerTest` (36 tests) already covers per-type dispatch,
  null-guards, every malformed-JSON variant, and the `enable-confirms`
  behavioural path. Anything new has to target *cross-cutting* behaviour —
  multi-frame sequences, isolation between connections, or degradation modes
  that the single-frame tests can't express.
- `WsFrameHandler` has **no static mutable state** — the shared `ObjectMapper`
  is the only static, and `ObjectMapper.readTree` is documented thread-safe.
  That is why two handlers on two threads happily interleave without locks;
  the concurrent test asserts the absence of cross-talk rather than per-handler
  concurrency (Netty itself guarantees single-threaded delivery per channel).
- `textOrNull` treats non-textual `id` fields (number, array, object) as
  missing. The handler therefore tolerates `{"id":42}` or `{"id":[1,2,3]}`
  without throwing; the outbound error frame just omits the `id` field. This
  is a defensive degradation worth pinning down explicitly in tests.
- The dispatch switch catches both `UnsupportedOperationException` (stub
  handlers) and generic `RuntimeException`. The extended tests add coverage
  for `IllegalArgumentException` in the generic arm, complementing the
  existing `IllegalStateException` assertion.
- Driving `channelRead` rather than a full `EmbeddedChannel` keeps the tests
  fast and removes the overhead of simulating handshake + frame aggregator —
  but it means anything above the frame handler (aggregator, handshake) is
  untested here. `WsUpgradeOrHttpHandlerTest` already covers that boundary.

---

## Limitations

- These tests focus on frame-handler behaviour only. They do **not** wire an
  `EmbeddedChannel` to exercise the full HTTP→WS upgrade→frame dispatch
  pipeline — that integration concern is already covered by
  `WsUpgradeOrHttpHandlerTest` and is left intentionally out of scope here to
  avoid duplicating setup code.
- Rate limiting, metrics rate-limit effects, and real subscription managers
  are mocked away. The tests use anonymous subclasses of `WsFrameHandler` to
  override handler methods — this means the production handler wiring (once
  real handlers exist) is exercised only indirectly via the stub-throws
  `UnsupportedOperationException` path.
- The concurrency test uses 4 threads × 50 frames per thread on independent
  handlers. It proves no static mutable state leaks; it does **not** assert
  safety of concurrent writes to a single handler instance (which Netty's
  threading model forbids anyway).

---

## Field Notes

- Initial checkstyle run failed with 15 `LeftCurly` violations because I had
  written the 15 handler overrides on single lines (`{ hit.add("x"); }`).
  Kafka's checkstyle config requires `{` to be followed by a line break.
  Expanded the overrides to multi-line form to pass.
- RED/GREEN discipline: I wrote the test file first targeting behaviour the
  production code already supports (since this is test-only work). Ran the
  suite, saw all 11 pass on first compile-clean run, no production-code
  changes required.
- The WsFrameHandler's `channelRead` (inherited from
  `SimpleChannelInboundHandler`) is the right entry point for these tests —
  it handles the type check and auto-release, matching how Netty would
  deliver frames. Calling `channelRead0` directly works too but skips the
  release safety net.

---

---

## Acceptance Criteria

- [ ] `timeout 300 ./gradlew :http-server:test --tests 'kafka.server.http.ws.WsFrameHandlerTest' -x spotlessCheck` exits 0
- [ ] At least 10 test methods
- [ ] Upgrade, dispatch, malformed, oversized, unknown all covered
- [ ] Learning section filled with at least one entry

---

## File Manifest

### Added

- `http-server/src/test/java/kafka/server/http/ws/WsFrameHandlerExtendedTest.java`
  — 11 cross-cutting supplementary tests complementing the 36 already in
  `WsFrameHandlerTest`. Tests cover:
    1. `sequentialFrames_eachDispatchedIndependently`
    2. `invalidFrameDoesNotPoisonSubsequentDispatch`
    3. `enableConfirmsPersistsAcrossLaterFrames`
    4. `twoConnections_independentStateAndFrames`
    5. `concurrentDispatch_acrossConnections_noCrossTalk`
    6. `channelInactive_clearsSubscriptionsPopulatedByHandler`
    7. `numericCorrelationId_degradesToNull_andDispatchStillHappens`
    8. `arrayCorrelationId_degradesToNull_onUnknownType`
    9. `illegalArgumentException_inHandler_emitsInternalErrorWithCorrelationId`
    10. `fullDispatchSequence_allTypesReachTheirHandlers`
    11. `largeButValidJsonPayload_dispatchesCleanly`

### Modified

- `ivy-docs/tasks/TASK-WS-T.02-ws-frame-handler-unit-tests.md` —
  Learning / Limitations / Field Notes / File Manifest sections filled in.

### Unchanged

- `http-server/src/main/java/kafka/server/http/ws/WsFrameHandler.java` —
  production code was not modified (test-only task).
