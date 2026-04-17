# TASK-WS2.07: REST Connection and Consumer Management Endpoints

## Prerequisites

| Task | What it delivers | Why this task needs it |
|------|------------------|-----------------------|
| TASK-WS2.06 | REST handler pattern + HttpRouter extensions | Follows same REST handler pattern and route dispatch |
| TASK-WS1.11 | WsSubscriptionManager — subscription lifecycle | Consumer list and force-cancel delegate to subscription manager |
| TASK-WS1.03 | WsConnectionContext — per-connection state | Connection list reads from connection registry |

---

## Context

The design doc §5.6-5.7 specifies REST endpoints for inspecting and managing WebSocket connections and consumers. These are administration endpoints — they let operators list active connections, inspect subscription details, force-close misbehaving connections, and cancel individual consumers.

Force-close sends a WebSocket close frame with code 1001 and the provided reason text. Force-cancel sends a `subscription-cancelled` frame with reason `ADMIN_CANCEL`. Both operations must clean up resources (requeue unacked messages, remove from connection registry).

### Endpoints

**Connections:**
- `GET /v1/connections` — list all WS connections (connectionId, brokerId, vhost, principal, connectedAt, subscriptionCount, protocol, remoteAddress)
- `GET /v1/connections/{connectionId}` — get connection details (includes subscriptions array, publishConfirmsEnabled, message counters)
- `DELETE /v1/connections/{connectionId}` — force-close connection (sends WS close frame 1001)

**Consumers:**
- `GET /v1/consumers` — list all consumers (optional `?queue=X` filter)
- `DELETE /v1/consumers/{connectionId}/{subscriptionId}` — force-cancel consumer (sends `subscription-cancelled` frame)

---

## Specification

### ConnectionRestHandler

```java
package kafka.server.http.rest;

public final class ConnectionRestHandler {

    public ConnectionRestHandler(WsConnectionRegistry connectionRegistry);

    /** GET /v1/connections — list all connections. */
    public FullHttpResponse handleList();

    /** GET /v1/connections/{connectionId} — connection details with subscriptions. */
    public FullHttpResponse handleGet(String connectionId);

    /** DELETE /v1/connections/{connectionId} — force-close, sends WS close frame 1001. */
    public FullHttpResponse handleForceClose(String connectionId, FullHttpRequest request);
}
```

### ConsumerRestHandler

```java
package kafka.server.http.rest;

public final class ConsumerRestHandler {

    public ConsumerRestHandler(WsConnectionRegistry connectionRegistry,
                               WsSubscriptionManager subscriptionManager);

    /** GET /v1/consumers — list all consumers, optional ?queue=X filter. */
    public FullHttpResponse handleList(String queueFilter);

    /** DELETE /v1/consumers/{connectionId}/{subscriptionId} — force-cancel. */
    public FullHttpResponse handleForceCancel(String connectionId, String subscriptionId);
}
```

---

## Implementation Details

**Module:** `http-server`

**Files to study:**

| File | Why |
|------|-----|
| `http-server/src/main/java/kafka/server/http/rest/ExchangeRestHandler.java` | Same REST handler pattern from WS2.06 |
| `http-server/src/main/java/kafka/server/http/ws/WsConnectionContext.java` | Connection state fields to serialize |

**Files to create:**

| File | What it does |
|------|--------------|
| `http-server/src/main/java/kafka/server/http/rest/ConnectionRestHandler.java` | Connection list/get/force-close |
| `http-server/src/main/java/kafka/server/http/rest/ConsumerRestHandler.java` | Consumer list/force-cancel |

**Files to modify:**

| File | What changes |
|------|--------------|
| `http-server/src/main/java/kafka/server/http/HttpRouter.java` | Add HandlerType + patterns for connection and consumer routes |
| `http-server/src/main/java/kafka/server/http/HttpRequestHandler.java` | Dispatch new HandlerType values |

> **CRITICAL:** Force-close must send WS close frame code 1001 with reason text before closing the TCP connection. Use `channel.writeAndFlush(new CloseWebSocketFrame(1001, reason))` followed by `channel.close()`.

> **CRITICAL:** Force-cancel must send `subscription-cancelled` frame with `reason: "ADMIN_CANCEL"` to the owning connection. Unacked messages must be requeued (offsets not committed).

**Implementation order:**
1. Add new HandlerType values and URI patterns to HttpRouter
2. Create ConnectionRestHandler
3. Create ConsumerRestHandler
4. Wire dispatch in HttpRequestHandler
5. Add unit tests

---

## Skeleton Code

### ConnectionRestHandler

```java
package kafka.server.http.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;

import java.util.Objects;

/**
 * REST handlers for /v1/connections endpoints.
 *
 * // Time: Created - TASK-WS2.07
 */
public final class ConnectionRestHandler {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // TODO: WsConnectionRegistry dependency

    public FullHttpResponse handleList() {
        // TODO: Iterate all connections, build JSON array with:
        //   connectionId, brokerId, vhost, principal, connectedAt,
        //   subscriptionCount, protocol, remoteAddress
        throw new UnsupportedOperationException("Not yet implemented");
    }

    public FullHttpResponse handleGet(String connectionId) {
        // TODO: Look up connection, return details including:
        //   subscriptions array, publishConfirmsEnabled,
        //   messagesPublished, messagesDelivered, messagesAcked
        // TODO: Return 404 if not found
        throw new UnsupportedOperationException("Not yet implemented");
    }

    public FullHttpResponse handleForceClose(String connectionId, FullHttpRequest request) {
        // TODO: Parse { "reason": "Administrative close" } from body
        // TODO: Send CloseWebSocketFrame(1001, reason)
        // TODO: Close channel
        // TODO: Return 204 No Content
        throw new UnsupportedOperationException("Not yet implemented");
    }
}
```

### ConsumerRestHandler

```java
package kafka.server.http.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.handler.codec.http.FullHttpResponse;

import java.util.Objects;

/**
 * REST handlers for /v1/consumers endpoints.
 *
 * // Time: Created - TASK-WS2.07
 */
public final class ConsumerRestHandler {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // TODO: WsConnectionRegistry + WsSubscriptionManager dependencies

    public FullHttpResponse handleList(String queueFilter) {
        // TODO: List all consumers across all connections
        // TODO: If queueFilter non-null, filter by queue name
        // TODO: Return { "consumers": [...] }
        throw new UnsupportedOperationException("Not yet implemented");
    }

    public FullHttpResponse handleForceCancel(String connectionId, String subscriptionId) {
        // TODO: Look up connection, look up subscription
        // TODO: Send subscription-cancelled frame with reason ADMIN_CANCEL
        // TODO: Requeue unacked messages (do not commit offsets)
        // TODO: Return 204 No Content, or 404 if not found
        throw new UnsupportedOperationException("Not yet implemented");
    }
}
```

### HttpRouter extensions

```java
// New URI patterns:
private static final Pattern CONNECTIONS_LIST_PATTERN =
    Pattern.compile("^/v1/connections$");
private static final Pattern CONNECTION_DETAIL_PATTERN =
    Pattern.compile("^/v1/connections/([^/?]+)$");
private static final Pattern CONSUMERS_LIST_PATTERN =
    Pattern.compile("^/v1/consumers$");
private static final Pattern CONSUMER_CANCEL_PATTERN =
    Pattern.compile("^/v1/consumers/([^/?]+)/([^/?]+)$");
```

---

## Tests

**Test classes:**
- `http-server/src/test/java/kafka/server/http/rest/ConnectionRestHandlerTest.java`
- `http-server/src/test/java/kafka/server/http/rest/ConsumerRestHandlerTest.java`

| Test method | What it verifies |
|-------------|-----------------|
| `listConnections_returnsAllActive` | Connection list includes all registered WS connections |
| `getConnection_includesSubscriptions` | Detail response has subscriptions array |
| `getConnection_notFound_returns404` | Non-existent connectionId returns 404 |
| `forceClose_sendsCloseFrame1001` | Close frame with code 1001 sent to channel |
| `listConsumers_filterByQueue` | Queue filter narrows consumer list |
| `forceCancel_sendsSubscriptionCancelled` | subscription-cancelled frame sent with ADMIN_CANCEL |
| `forceCancel_requeuesUnacked` | Unacked messages not committed after cancel |

**Run command:**
```bash
timeout 300 ./gradlew :http-server:test --tests 'kafka.server.http.rest.Connection*Test' --tests 'kafka.server.http.rest.Consumer*Test' -x spotlessCheck
```

---

## Rules

- Force-close uses WS close code 1001 (Going Away), not 1000 (Normal Closure).
- Force-cancel sends `subscription-cancelled` frame, NOT an error frame.
- Both operations must requeue unacked messages (offsets not committed).
- Connection list must include `remoteAddress` for debugging.
- Consumer list supports `?queue=X` query parameter filter.

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

- [ ] `timeout 300 ./gradlew :http-server:test --tests 'kafka.server.http.rest.Connection*Test' --tests 'kafka.server.http.rest.Consumer*Test' -x spotlessCheck` exits 0
- [ ] `grep -r "ConnectionRestHandler" http-server/src/main/java/` returns at least 2 hits
- [ ] `grep -r "ConsumerRestHandler" http-server/src/main/java/` returns at least 2 hits
- [ ] Force-close sends WebSocket close frame code 1001
- [ ] Force-cancel sends subscription-cancelled with reason ADMIN_CANCEL
- [ ] Learning section filled with at least one entry

---

## File Manifest

_To be filled by the executing agent._
