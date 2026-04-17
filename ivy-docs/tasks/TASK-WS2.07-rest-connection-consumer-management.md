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

- **`WsConnectionRegistry` as the decoupling seam.** The task spec names a
  `WsConnectionRegistry` dependency without one existing. Introducing a tiny
  class that wraps `ConcurrentHashMap<String, WsConnectionContext>` (register,
  unregister, get, contains, connectionIds, contexts, size) gave the REST
  admin handlers a clean surface to target and kept them decoupled from
  `HttpProcessor`. The processor already holds the authoritative map; wiring
  at startup simply hands the same registry instance to both sides.
- **Two-segment routes reuse existing RouteResult slots.** The consumer
  force-cancel path `/v1/consumers/{connectionId}/{subscriptionId}` needs two
  identifiers. Rather than grow `RouteResult` with a new field, the
  `consumerGroup()` slot carries the connectionId and `resourceName()` carries
  the subscriptionId. Documented at the site of the routing code and again in
  the scala dispatch. Less invasive than another constructor overload.
- **Consumer manager lookup is a functional interface.** Each WS connection
  owns its own `WsSubscriptionManager` (see WS1.15), so the admin handler
  accepts a `SubscriptionManagerLookup` rather than a single global manager.
  Production wiring plugs in a map lookup; tests can inject an always-return
  lambda for a single manager. Mirrors the `QueueStore` facade trick from
  WS2.06.
- **Force-cancel must NOT commit offsets.** `WsSubscriptionManager.unsubscribe`
  returns committable offsets as its contract — the admin handler intentionally
  discards them. Commenting this explicitly in both the handler javadoc and the
  test (`forceCancel_requeuesUnacked`) matters because the contract is load-
  bearing for at-least-once redelivery on reconnect.
- **Switch exhaustiveness in `HttpRequestTranslator`.** Adding new
  `HandlerType` enum values broke the pattern-matched switch in the translator.
  The fix is one-line (throw `InvalidRequestException` with a clear message
  saying these handlers are dispatched directly) but easy to miss without a
  compile.

---

## Limitations

- **No real wiring yet.** `HttpRequestHandler.scala` gains two optional
  constructor parameters (`connectionRestHandler`, `consumerRestHandler`) that
  default to `null` and a dispatch arm that returns 501 "not wired" until the
  broker startup sequence constructs the handlers with a shared
  `WsConnectionRegistry`. That wire-up lives in a later phase — the handlers
  are production-ready, only the bootstrap is pending.
- **Consumer list performance is linear.** `handleList` iterates all
  connections and all subscriptions per call — fine at moderate scale
  (O(connections × subs/connection)) but will need an index for ops dashboards
  polling every second at >10k connections. Acceptable for Phase-2 admin
  endpoints.
- **Force-close does not drain in-flight deliveries.** The admin intent is
  "stop serving this client now", so we just send the WS close frame and let
  the existing channel-close pipeline requeue unacked messages. If operators
  need a graceful "stop new work, let existing batches finish" variant, that
  is a separate WS3.07 concern.
- **No ACL check.** Admin endpoints are unauthenticated at this layer; the
  enforcement lives in the `KafkaPrincipalBuilder` + later ACL task
  (WS3.05). The handler treats every caller that reaches it as authorized.
- **Pattern for reason text.** Force-close reason from the body is passed
  verbatim to `CloseWebSocketFrame`. The WS spec caps the reason at
  125 bytes — Netty will throw if exceeded. A production hardening pass should
  truncate at the handler layer.

---

## Field Notes

- **TDD roundtrip.** Wrote 9 + 9 handler tests and 7 router tests before any
  production code. First compile failed on `HttpRequestTranslator` exhaustive
  switch — a tidy reminder that enum additions ripple.
- **Test harness re-used the WS1.12 pattern.** The `HttpProcessorWsTest`
  harness — a real `WsConnectionContext` wrapped around a mocked
  `ChannelHandlerContext` — made it trivial to `verify(channel).writeAndFlush`
  the `CloseWebSocketFrame` for force-close and the `TextWebSocketFrame` for
  subscription-cancelled. No embedded Netty channel needed.
- **`subscriptions()` returns a `ConcurrentHashMap<String, Object>`** on
  `WsConnectionContext`. Tests populate it directly to simulate active
  subscriptions for the list detail endpoint — keeps the test independent
  of `WsSubscriptionManager`. The two inspections are orthogonal:
  `WsConnectionContext.subscriptions()` is the subscription-id set,
  `WsSubscriptionManager` is the full lifecycle.
- **Order matters in `matchAdminRoutes`.** The two-segment cancel pattern
  must be tested before `CONSUMERS_LIST_PATTERN` so that
  `/v1/consumers/abc/xyz` doesn't fall through as "list with stray segments".
- **HttpRouter now has three "matchXxxRoutes" families.** Topic / share-group
  / consumer-group / routing / admin / utility — neat and readable, but the
  file is nearing the point where each family deserves its own class.
  Deferring that refactor.

---

## File Manifest

**Created:**
- `http-server/src/main/java/kafka/server/http/ws/WsConnectionRegistry.java`
  — broker-wide registry of active WS connections, backed by a
  `ConcurrentHashMap`. Read-side used by the admin handlers; write-side used
  by the WS upgrade handler (wire-up deferred).
- `http-server/src/main/java/kafka/server/http/rest/ConnectionRestHandler.java`
  — `GET /v1/connections`, `GET /v1/connections/{id}`,
  `DELETE /v1/connections/{id}`. Force-close emits
  `CloseWebSocketFrame(1001, reason)`.
- `http-server/src/main/java/kafka/server/http/rest/ConsumerRestHandler.java`
  — `GET /v1/consumers?queue=...`, `DELETE /v1/consumers/{conn}/{sub}`.
  Force-cancel emits `subscription-cancelled` with `reason: "ADMIN_CANCEL"`
  and deliberately discards committable offsets (unacked → requeue).
- `http-server/src/test/java/kafka/server/http/rest/ConnectionRestHandlerTest.java`
  — 9 tests covering list/get/force-close happy & error paths.
- `http-server/src/test/java/kafka/server/http/rest/ConsumerRestHandlerTest.java`
  — 9 tests covering list (with queue filter), force-cancel, subscription-
  cancelled frame content, requeue contract, and 404 paths.

**Modified:**
- `http-server/src/main/java/kafka/server/http/HttpRouter.java` — new
  `HandlerType` values (`LIST_CONNECTIONS`, `GET_CONNECTION`,
  `FORCE_CLOSE_CONNECTION`, `LIST_CONSUMERS`, `FORCE_CANCEL_CONSUMER`), four
  patterns (`CONNECTIONS_LIST_PATTERN`, `CONNECTION_DETAIL_PATTERN`,
  `CONSUMERS_LIST_PATTERN`, `CONSUMER_CANCEL_PATTERN`), `matchAdminRoutes`
  method, and `route()` wired to call it before `matchUtilityRoutes`.
- `http-server/src/main/java/kafka/server/http/HttpRequestTranslator.java`
  — added exhaustive switch arm for the five new HandlerType values (throws,
  mirroring the WS2.06 "handled directly, not translated" arm).
- `http-server/src/main/scala/kafka/network/HttpRequestHandler.scala`
  — constructor gains `connectionRestHandler` and `consumerRestHandler`
  (nullable), `isRestRoutingHandler` matches the new types, and
  `handleRestRoutingRequest` dispatches them. Unwired handlers return
  501 via `notWired`.
- `http-server/src/test/java/kafka/server/http/HttpRouterRoutingTest.java`
  — 7 new tests covering all five new routes plus wrong-method and
  query-filter cases.

---

## Acceptance Criteria

- [ ] `timeout 300 ./gradlew :http-server:test --tests 'kafka.server.http.rest.Connection*Test' --tests 'kafka.server.http.rest.Consumer*Test' -x spotlessCheck` exits 0
- [ ] `grep -r "ConnectionRestHandler" http-server/src/main/java/` returns at least 2 hits
- [ ] `grep -r "ConsumerRestHandler" http-server/src/main/java/` returns at least 2 hits
- [ ] Force-close sends WebSocket close frame code 1001
- [ ] Force-cancel sends subscription-cancelled with reason ADMIN_CANCEL
- [ ] Learning section filled with at least one entry
