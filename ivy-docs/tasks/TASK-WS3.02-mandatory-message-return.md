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

_To be filled by the executing agent._

---

## Limitations

_To be filled by the executing agent._

---

## Field Notes

_To be filled by the executing agent._

---

## Acceptance Criteria

- [ ] `timeout 300 ./gradlew :http-server:test --tests 'kafka.server.http.ws.WsMandatoryReturnTest' -x spotlessCheck` exits 0
- [ ] `grep -r '"returned"' http-server/src/main/java/kafka/server/http/ws/` returns at least 1 hit
- [ ] `grep -r "312" http-server/src/main/java/kafka/server/http/ws/` returns at least 1 hit (replyCode)
- [ ] Learning section filled with at least one entry

---

## File Manifest

_To be filled by the executing agent._
