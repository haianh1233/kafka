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

_To be filled by the executing agent._

---

## Limitations

_To be filled by the executing agent._

---

## Field Notes

_To be filled by the executing agent._

---

## Acceptance Criteria

- [ ] `timeout 300 ./gradlew :http-server:test --tests 'kafka.server.http.ws.WsFrameHandlerTest' -x spotlessCheck` exits 0
- [ ] At least 10 test methods
- [ ] Upgrade, dispatch, malformed, oversized, unknown all covered
- [ ] Learning section filled with at least one entry

---

## File Manifest

_To be filled by the executing agent._
