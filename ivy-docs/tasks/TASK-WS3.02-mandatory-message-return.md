# TASK-WS3.02: Mandatory Message Return

## Prerequisites

| Task | What it delivers | Why this task needs it |
|------|------------------|-----------------------|
| TASK-WS1.09 | WsPublishHandler — publish → routing → ProduceRequest | Mandatory flag checked after routing resolution |
| TASK-WS1.08 | RoutingEngine | Routing engine returns matched queues; empty set triggers return logic |
| TASK-WS3.01 | WsPublisherConfirmTracker | Mandatory interacts with confirms: returned messages also get publish-failed |

---

## Context

The design doc §5.7 specifies mandatory message handling. When `mandatory: true` on a publish frame and routing finds no matching queues, the broker sends a `returned` frame back to the publisher with the original message body.

**Returned frame format:**
```json
{
  "type": "returned",
  "exchange": "events",
  "routingKey": "order.xxx",
  "replyCode": 312,
  "replyText": "NO_ROUTE",
  "message": { ... }
}
```

**Edge cases from design doc:**
- Non-existent exchange + `mandatory: true` → message is returned via `returned` frame
- Non-existent exchange + `mandatory: false` → message is silently dropped (NOT an error)
- Internal exchange (`internal: true`) → publish rejected with `ACCESS_REFUSED` error frame
- Alternate exchange is tried BEFORE mandatory return — if alternate exchange routing finds queues, no return
- Multi-queue fanout partial failure → entire publish treated as failed (not a return)

---

## Specification

### Mandatory return logic in WsPublishHandler

```java
// In WsPublishHandler.handlePublish():

Set<String> matchedQueues = routingEngine.route(exchange, routingKey, headers);

// Alternate exchange fallback (already handled by RoutingEngine)
// matchedQueues is empty only if both primary and alternate routing found nothing

if (matchedQueues.isEmpty()) {
    if (mandatory) {
        writeReturnedFrame(ctx, exchange, routingKey, 312, "NO_ROUTE", originalMessage);
    }
    // Non-mandatory: silently drop, no error, no response
    // If confirms enabled: still send "published" (message was accepted, just not routed)
    return;
}
```

---

## Implementation Details

**Module:** `http-server`

**Files to modify:**

| File | What changes |
|------|--------------|
| `http-server/src/main/java/kafka/server/http/ws/WsPublishHandler.java` | Add mandatory flag check after routing, write returned frame |
| `http-server/src/main/java/kafka/server/http/ws/WsFrameHandler.java` | Parse `mandatory` field from publish frame JSON |

> **CRITICAL:** The `returned` frame includes the ORIGINAL message body, not a summary. The entire `message` object from the publish frame must be stored temporarily and included in the returned frame.

> **CRITICAL:** When confirms enabled + mandatory + no route: send BOTH the `returned` frame AND a `published` confirm (the message was accepted by the broker, it just couldn't be routed). This matches AMQP semantics where a mandatory return does not prevent a basic.ack.

> **CRITICAL:** Internal exchange check must happen BEFORE routing. If target exchange has `internal: true`, reject with error frame `ACCESS_REFUSED` — do not route, do not return.

**Implementation order:**
1. Parse `mandatory` boolean from publish frame JSON (default false)
2. Add internal exchange check in WsPublishHandler
3. Add empty-route check with returned frame write
4. Handle interaction with publisher confirms
5. Add unit tests

---

## Skeleton Code

```java
// Addition to WsPublishHandler.handlePublish():

private void handleEmptyRouteResult(ChannelHandlerContext ctx, String exchange,
                                     String routingKey, boolean mandatory,
                                     JsonNode originalMessage, long publishId) {
    if (mandatory) {
        ObjectNode returned = MAPPER.createObjectNode();
        returned.put("type", "returned");
        returned.put("exchange", exchange);
        returned.put("routingKey", routingKey);
        returned.put("replyCode", 312);
        returned.put("replyText", "NO_ROUTE");
        returned.set("message", originalMessage);
        ctx.writeAndFlush(new TextWebSocketFrame(returned.toString()));
    }
    // If confirms enabled, still send published confirm
    if (confirmTracker != null && confirmTracker.isEnabled() && publishId >= 0) {
        confirmTracker.confirmSuccess(publishId);
    }
}

private void checkInternalExchange(ChannelHandlerContext ctx, ExchangeEntry exchange,
                                    String requestId) {
    if (exchange.isInternal()) {
        writeErrorFrame(ctx, requestId, "ACCESS_REFUSED",
            "Cannot publish to internal exchange '" + exchange.name() + "'");
        // Do not proceed with routing
    }
}
```

---

## Tests

**Test class:** `http-server/src/test/java/kafka/server/http/ws/WsMandatoryReturnTest.java`

| Test method | What it verifies |
|-------------|-----------------|
| `mandatory_noRoute_returnsMessage` | Returned frame with replyCode 312 |
| `mandatory_returnedFrame_includesOriginalBody` | Full message body in returned frame |
| `nonMandatory_noRoute_silentDrop` | No returned frame, no error frame |
| `mandatory_withAlternateExchange_noReturn` | Alternate exchange routes succeed → no return |
| `mandatory_nonExistentExchange_returns` | Missing exchange → returned frame |
| `nonMandatory_nonExistentExchange_silentDrop` | Missing exchange → no error, silent drop |
| `internalExchange_rejected` | ACCESS_REFUSED error frame |
| `mandatory_withConfirms_sendsBothReturnAndPublished` | Both returned and published frames sent |

**Run command:**
```bash
timeout 300 ./gradlew :http-server:test --tests 'kafka.server.http.ws.WsMandatoryReturnTest' -x spotlessCheck
```

---

## Rules

- Reply code for NO_ROUTE is always 312.
- Non-mandatory + no route = SILENT DROP. No error frame, no returned frame.
- Internal exchange → ACCESS_REFUSED error (not a return).
- Alternate exchange checked before mandatory return.
- Confirms + mandatory + no route → send both `returned` AND `published`.

---

## Learning

- WS1.11's `WsPublishHandler` already emitted a `returned` frame with
  `replyCode=312` / `replyText=NO_ROUTE` and the original `message` body for
  mandatory + empty-route. Shipping the WS3.02 enhancements was additive on
  top of that, not a rewrite.
- The only "unrouted" branches are (a) exchange missing and (b) routing
  engine returned an empty set. `RoutingEngine.route()` already folds the
  alternate-exchange fallback into its return value, so the handler does not
  need to re-try. I pulled both branches through a single
  `handleUnrouted(...)` helper to keep the confirm + returned matrix
  consistent.
- Publisher-confirm + mandatory-return interplay follows AMQP semantics: a
  `basic.return` does NOT suppress `basic.ack`. The handler records the
  publishId and immediately calls `confirmSuccess` after emitting the
  returned frame — same tracker as real produce callbacks, so the published
  frame shape is identical.
- Internal-exchange rejection is a hard rejection, not a return. It is
  emitted BEFORE routing so the routing engine never sees the publish, and
  it suppresses the published confirm (broker did not accept the message).
- TASK-WS3.02 quietly changes the semantics of "unknown exchange" from
  WS1.11's `NOT_FOUND` error to the spec-mandated silent-drop (non-mandatory)
  / returned-frame (mandatory). The WsPublishHandlerTest that codified the
  old behaviour was replaced with two new tests that match the new spec.

---

## Limitations

- `handleUnrouted` emits confirm and returned frames sequentially on the
  Netty event loop. A partially-failed flush (e.g., the returned frame
  writes succeed but the subsequent published frame write fails because the
  channel closed between them) is not atomic; the client could observe a
  returned frame without the matching published. Acceptable given confirms
  are advisory and the connection is torn down anyway.
- The alternate-exchange test verifies handler behaviour when the routing
  engine returns a non-empty set, but does not exercise an actual
  alternate-exchange chain end-to-end — that is covered by `RoutingEngineTest`.
- We do not distinguish AMQP reply code 313 (`NO_CONSUMERS`) from 312
  (`NO_ROUTE`). The design doc says consumer-aware returns are deferred; a
  queue with no consumers is still "routed" as far as the broker is
  concerned. If/when mandatory-with-consumers semantics are added, the
  handler will need to query consumer presence post-routing.
- `WsPublisherConfirmTracker` has no way to surface "mandatory return
  happened" in a single frame — we emit two separate frames (returned +
  published). Clients that want to correlate them must use the publishId.

---

## Field Notes

- Added a 13-test `WsMandatoryReturnTest` focused on the
  mandatory/internal/confirms matrix. Keeping it separate from the broader
  `WsPublishHandlerTest` made RED → GREEN quicker to read: each new test
  maps to a one-line change in `handlePublish`.
- Broke one existing `WsPublishHandlerTest` case
  (`handlePublish_unknownExchange_emitsErrorFrame`) by design — it encoded
  the pre-WS3.02 `NOT_FOUND` behaviour. Replaced with two new cases covering
  mandatory=true and mandatory=false paths.
- `WsMessageSerializer` is left alone. The returned frame emits the original
  JSON `message` object; only actual routing runs serialization.
- `WsFrameHandler` required no changes: publish parsing (including the
  `mandatory` bool) is owned by `WsPublishHandler.parsePublishFrame`.

---

## Acceptance Criteria

- [ ] `timeout 300 ./gradlew :http-server:test --tests 'kafka.server.http.ws.WsMandatoryReturnTest' -x spotlessCheck` exits 0
- [ ] `grep -r '"returned"' http-server/src/main/java/kafka/server/http/ws/` returns at least 1 hit
- [ ] `grep -r "312" http-server/src/main/java/kafka/server/http/ws/` returns at least 1 hit (replyCode)
- [ ] Learning section filled with at least one entry

---

## File Manifest

### Modified (main)
- `http-server/src/main/java/kafka/server/http/ws/WsPublishHandler.java`
  - Added `ERR_ACCESS_REFUSED` constant.
  - Removed `ERR_NOT_FOUND` (unused after semantics change).
  - Rewrote `handlePublish` to (a) treat missing exchange as unroutable,
    (b) reject internal exchanges, (c) funnel all unrouted cases through
    a new `handleUnrouted` helper.
  - Added `handleUnrouted` + `emitPublishedIfConfirmsEnabled` helpers.
  - Refreshed class Javadoc error-handling table.
  - Added WS3.02 "Time: Update" header.

### Added (tests)
- `http-server/src/test/java/kafka/server/http/ws/WsMandatoryReturnTest.java`
  — 13 tests covering every branch of the mandatory/internal/confirms matrix.

### Modified (tests)
- `http-server/src/test/java/kafka/server/http/ws/WsPublishHandlerTest.java`
  - Replaced `handlePublish_unknownExchange_emitsErrorFrame` with
    `handlePublish_unknownExchange_nonMandatory_silentDrop` and
    `handlePublish_unknownExchange_mandatory_emitsReturnedFrame`.
  - Added WS3.02 "Time: Update" header.
