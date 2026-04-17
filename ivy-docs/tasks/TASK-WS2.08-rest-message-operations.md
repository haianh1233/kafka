# TASK-WS2.08: REST Message Operations — Publish, Get, Ack, Nack

## Prerequisites

| Task | What it delivers | Why this task needs it |
|------|------------------|-----------------------|
| TASK-WS2.06 | REST handler pattern + HttpRouter extensions | Same dispatch pattern |
| TASK-WS1.08 | RoutingEngine | Publish via exchange routing requires routing engine |
| TASK-WS1.09 | WsPublishHandler — publish → routing → ProduceRequest | REST publish reuses the same publish pipeline |
| TASK-WS1.14 | WsAckHandler — ACK/NACK → offset commit | REST ack/nack delegates to ack handler |

---

## Context

The design doc §5.8 specifies REST endpoints for message operations: publishing through exchange routing, pulling messages from queues, and acknowledging/rejecting messages — all without requiring a WebSocket connection. This enables HTTP-only clients to participate in the exchange routing model.

Key difference from existing `POST /v1/topics/{t}/records`: that endpoint produces directly to a Kafka topic, bypassing routing. The new `POST /v1/exchanges/{e}/publish` routes through the exchange binding layer, potentially fanning out to multiple queues.

### Endpoints

- `POST /v1/exchanges/{exchange}/publish` — publish via exchange routing (returns routed queues + offsets)
- `POST /v1/queues/{queue}/get` — pull messages from queue (count, ackMode, encoding)
- `POST /v1/queues/{queue}/ack` — acknowledge message (deliveryTag, multiple)
- `POST /v1/queues/{queue}/nack` — negative-acknowledge message (deliveryTag, requeue)

---

## Specification

### MessageRestHandler

```java
package kafka.server.http.rest;

public final class MessageRestHandler {

    public MessageRestHandler(RoutingEngine routingEngine,
                              WsPublishHandler publishHandler,
                              QueueManager queueManager);

    /** POST /v1/exchanges/{exchange}/publish — publish with exchange routing. */
    public FullHttpResponse handlePublish(String exchangeName, String vhost,
                                          FullHttpRequest request);

    /** POST /v1/queues/{queue}/get — pull messages. */
    public FullHttpResponse handleGet(String queueName, String vhost,
                                      FullHttpRequest request);

    /** POST /v1/queues/{queue}/ack — acknowledge message. */
    public FullHttpResponse handleAck(String queueName, String vhost,
                                      FullHttpRequest request);

    /** POST /v1/queues/{queue}/nack — negative-acknowledge message. */
    public FullHttpResponse handleNack(String queueName, String vhost,
                                       FullHttpRequest request);
}
```

### Publish response format

```json
{
  "routed": true,
  "queues": ["order-events", "audit-log"],
  "offsets": [
    { "queue": "order-events", "partition": 0, "offset": 1042 },
    { "queue": "audit-log", "partition": 0, "offset": 5567 }
  ]
}
```

When `mandatory: true` and no queues match:

```json
{
  "routed": false,
  "queues": [],
  "replyCode": 312,
  "replyText": "NO_ROUTE"
}
```

### Get request format

```json
{
  "count": 1,
  "ackMode": "auto",
  "encoding": "auto"
}
```

| Field | Type | Default | Description |
|---|---|---|---|
| `count` | int | 1 | Number of messages to retrieve (max 100) |
| `ackMode` | string | `"auto"` | `manual` / `auto` / `reject-requeue` |
| `encoding` | string | `"auto"` | `auto` / `base64` |

---

## Implementation Details

**Module:** `http-server`

**Files to study:**

| File | Why |
|------|-----|
| `http-server/src/main/java/kafka/server/http/rest/ExchangeRestHandler.java` | Same REST handler pattern |
| `http-server/src/main/java/kafka/server/http/ws/WsPublishHandler.java` | Reuse routing + produce pipeline |

**Files to create:**

| File | What it does |
|------|--------------|
| `http-server/src/main/java/kafka/server/http/rest/MessageRestHandler.java` | REST message operations |

**Files to modify:**

| File | What changes |
|------|--------------|
| `http-server/src/main/java/kafka/server/http/HttpRouter.java` | Add PUBLISH_VIA_EXCHANGE, QUEUE_GET, QUEUE_ACK, QUEUE_NACK HandlerTypes |
| `http-server/src/main/java/kafka/server/http/HttpRequestHandler.java` | Dispatch to MessageRestHandler |

> **CRITICAL:** REST publish must return the list of routed queues and per-queue offsets. This is different from WS publish which returns a simple confirm. The routing result must be captured and serialized.

> **CRITICAL:** REST get with `ackMode: "manual"` requires a per-REST-session delivery tag tracker. Since REST is stateless, use a short-lived tracking session keyed by a server-generated `sessionId` returned in the response. The client passes this sessionId back in ack/nack calls.

> **CRITICAL:** `count` must be capped at 100 to prevent excessive single-request fetches.

**Implementation order:**
1. Add new HandlerType values and URI patterns to HttpRouter
2. Create MessageRestHandler with publish, get, ack, nack
3. Wire dispatch in HttpRequestHandler
4. Add unit tests

---

## Skeleton Code

### MessageRestHandler

```java
package kafka.server.http.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import kafka.server.http.routing.RoutingEngine;

import java.util.Objects;
import java.util.Set;

/**
 * REST handlers for message operations: publish, get, ack, nack.
 *
 * // Time: Created - TASK-WS2.08
 */
public final class MessageRestHandler {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int MAX_GET_COUNT = 100;

    private final RoutingEngine routingEngine;
    // TODO: Add publishHandler, queueManager dependencies

    public MessageRestHandler(RoutingEngine routingEngine) {
        this.routingEngine = Objects.requireNonNull(routingEngine, "routingEngine");
    }

    public FullHttpResponse handlePublish(String exchangeName, String vhost,
                                          FullHttpRequest request) {
        // TODO: Parse body, contentType, headers, deliveryMode, mandatory
        // TODO: Extract X-Routing-Key header
        // TODO: Route through RoutingEngine
        // TODO: If no queues and mandatory → return { routed: false, replyCode: 312 }
        // TODO: Produce to each matched queue, collect offsets
        // TODO: Return { routed: true, queues: [...], offsets: [...] }
        throw new UnsupportedOperationException("Not yet implemented");
    }

    public FullHttpResponse handleGet(String queueName, String vhost,
                                      FullHttpRequest request) {
        // TODO: Parse count (default 1, max 100), ackMode, encoding
        // TODO: Resolve queue → backing topic
        // TODO: FetchRequest for count records
        // TODO: If ackMode=manual, track delivery tags
        // TODO: If ackMode=auto, auto-commit offsets
        // TODO: If ackMode=reject-requeue, do not commit
        // TODO: Return { messages: [...] } or { messages: [] } if empty
        throw new UnsupportedOperationException("Not yet implemented");
    }

    public FullHttpResponse handleAck(String queueName, String vhost,
                                      FullHttpRequest request) {
        // TODO: Parse deliveryTag, multiple
        // TODO: Commit offsets for acked messages
        // TODO: Return 200 OK
        throw new UnsupportedOperationException("Not yet implemented");
    }

    public FullHttpResponse handleNack(String queueName, String vhost,
                                       FullHttpRequest request) {
        // TODO: Parse deliveryTag, requeue
        // TODO: If requeue=false and queue has DLX → dead-letter
        // TODO: If requeue=true → do not commit (redeliver on next get)
        // TODO: Return 200 OK
        throw new UnsupportedOperationException("Not yet implemented");
    }
}
```

### HttpRouter extensions

```java
// New patterns:
private static final Pattern EXCHANGE_PUBLISH_PATTERN =
    Pattern.compile("^/v1/exchanges/([^/?]+)/publish$");
private static final Pattern QUEUE_GET_PATTERN =
    Pattern.compile("^/v1/queues/([^/?]+)/get$");
private static final Pattern QUEUE_ACK_PATTERN =
    Pattern.compile("^/v1/queues/([^/?]+)/ack$");
private static final Pattern QUEUE_NACK_PATTERN =
    Pattern.compile("^/v1/queues/([^/?]+)/nack$");
```

---

## Tests

**Test class:** `http-server/src/test/java/kafka/server/http/rest/MessageRestHandlerTest.java`

| Test method | What it verifies |
|-------------|-----------------|
| `publish_routedToMultipleQueues_returnsOffsetsPerQueue` | Fanout publish returns per-queue offsets |
| `publish_mandatory_noRoute_returnsReplyCode312` | Mandatory unroutable message returns NO_ROUTE |
| `publish_nonMandatory_noRoute_returnsRoutedFalse` | Non-mandatory silently drops |
| `get_returnsMessages` | Pull messages with count=1 |
| `get_emptyQueue_returnsEmptyArray` | Empty queue returns `{ messages: [] }` |
| `get_countExceedsMax_cappedAt100` | Count > 100 silently capped |
| `ack_commitsOffset` | Acknowledge commits consumer offset |
| `nack_requeueTrue_doesNotCommit` | NACK with requeue leaves offset uncommitted |

**Run command:**
```bash
timeout 300 ./gradlew :http-server:test --tests 'kafka.server.http.rest.MessageRestHandlerTest' -x spotlessCheck
```

---

## Rules

- Publish response must include `routed`, `queues`, and `offsets` fields.
- `count` on get must be capped at 100 — values above 100 silently capped.
- REST publish uses `X-Routing-Key` header (not a JSON field).
- `X-Vhost` header defaults to `/` when absent.
- Mandatory + no route = HTTP 200 with `{ "routed": false, "replyCode": 312 }`, NOT an HTTP error status.

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

- [ ] `timeout 300 ./gradlew :http-server:test --tests 'kafka.server.http.rest.MessageRestHandlerTest' -x spotlessCheck` exits 0
- [ ] `grep -r "MessageRestHandler" http-server/src/main/java/` returns at least 2 hits
- [ ] Publish response includes `routed`, `queues`, `offsets` fields
- [ ] Get count capped at 100
- [ ] Learning section filled with at least one entry

---

## File Manifest

_To be filled by the executing agent._
