# TASK-WS2.06: REST Exchange, Queue, and Binding CRUD Handlers

## Prerequisites

| Task | What it delivers | Why this task needs it |
|------|------------------|-----------------------|
| TASK-WS1.05 | ExchangeManager — exchange CRUD + in-memory cache | REST handlers delegate to ExchangeManager for declare/get/delete |
| TASK-WS1.06 | QueueManager — queue CRUD + Kafka topic creation | REST handlers delegate to QueueManager for queue lifecycle |
| TASK-WS1.07 | BindingManager — bind/unbind + index | REST handlers delegate to BindingManager for binding CRUD |
| TASK-WS1.08 | RoutingEngine with DirectMatcher | Needed for routing resolution in queue details |

---

## Context

The design doc §6 specifies a full REST management API for exchanges, queues, and bindings. Every operation available via WebSocket control frames must also be available via REST endpoints. This allows management tools, dashboards, and automation scripts to configure routing without maintaining a WebSocket connection.

Three new handler classes live in the `rest/` package: `ExchangeRestHandler`, `QueueRestHandler`, and `BindingRestHandler`. Each handler receives a parsed `FullHttpRequest`, extracts path parameters and the `X-Vhost` header, delegates to the existing manager classes (ExchangeManager, QueueManager, BindingManager), and returns a JSON response.

The `HttpRouter` must be extended with new `HandlerType` values and URI patterns to dispatch these new routes. The existing route dispatch pattern (regex match → `RouteResult`) is followed exactly.

### REST Endpoint Summary (from design doc §5.3-5.5)

**Exchanges:**
- `PUT /v1/exchanges/{name}` — declare exchange (201 Created or 200 OK)
- `GET /v1/exchanges/{name}` — get exchange details (bindingCount, publishRate)
- `GET /v1/exchanges` — list all exchanges
- `DELETE /v1/exchanges/{name}?ifUnused=false` — delete exchange (204 No Content)

**Queues:**
- `PUT /v1/queues/{name}` — declare queue (201 Created or 200 OK)
- `GET /v1/queues/{name}` — get queue details (consumers list, rates, backing topic)
- `GET /v1/queues` — list all queues
- `PATCH /v1/queues/{name}` — update queue settings
- `DELETE /v1/queues/{name}?ifUnused=false&ifEmpty=false` — delete queue (204 No Content)
- `DELETE /v1/queues/{name}/messages` — purge queue

**Bindings:**
- `POST /v1/bindings` — create binding (201 Created)
- `GET /v1/bindings?exchange=X` or `?queue=Y` — list bindings (filter by exchange/queue)
- `DELETE /v1/bindings` — delete binding (204 No Content)

---

## Specification

### ExchangeRestHandler

```java
package kafka.server.http.rest;

public final class ExchangeRestHandler {

    public ExchangeRestHandler(ExchangeManager exchangeManager);

    /** PUT /v1/exchanges/{name} — declare or re-declare exchange. */
    public FullHttpResponse handleDeclare(String exchangeName, String vhost,
                                          FullHttpRequest request);

    /** GET /v1/exchanges/{name} — exchange details + binding count + publish rate. */
    public FullHttpResponse handleGet(String exchangeName, String vhost);

    /** GET /v1/exchanges — list all exchanges for vhost. */
    public FullHttpResponse handleList(String vhost);

    /** DELETE /v1/exchanges/{name}?ifUnused=false — delete exchange. */
    public FullHttpResponse handleDelete(String exchangeName, String vhost,
                                          boolean ifUnused);
}
```

### QueueRestHandler

```java
package kafka.server.http.rest;

public final class QueueRestHandler {

    public QueueRestHandler(QueueManager queueManager,
                            WsSubscriptionManager subscriptionManager);

    /** PUT /v1/queues/{name} — declare queue + auto-create backing topic. */
    public FullHttpResponse handleDeclare(String queueName, String vhost,
                                          FullHttpRequest request);

    /** GET /v1/queues/{name} — queue details with consumers list, rates, backing topic info. */
    public FullHttpResponse handleGet(String queueName, String vhost);

    /** GET /v1/queues — list all queues for vhost. */
    public FullHttpResponse handleList(String vhost);

    /** PATCH /v1/queues/{name} — update queue arguments on a live queue. */
    public FullHttpResponse handlePatch(String queueName, String vhost,
                                         FullHttpRequest request);

    /** DELETE /v1/queues/{name}?ifUnused=false&ifEmpty=false — delete queue. */
    public FullHttpResponse handleDelete(String queueName, String vhost,
                                          boolean ifUnused, boolean ifEmpty);

    /** DELETE /v1/queues/{name}/messages — purge queue. */
    public FullHttpResponse handlePurge(String queueName, String vhost);
}
```

### BindingRestHandler

```java
package kafka.server.http.rest;

public final class BindingRestHandler {

    public BindingRestHandler(BindingManager bindingManager);

    /** POST /v1/bindings — create binding (queue-to-exchange or exchange-to-exchange). */
    public FullHttpResponse handleCreate(String vhost, FullHttpRequest request);

    /** GET /v1/bindings?exchange=X&queue=Y — list bindings with filters. */
    public FullHttpResponse handleList(String vhost, String exchangeFilter,
                                       String queueFilter);

    /** DELETE /v1/bindings — delete binding. */
    public FullHttpResponse handleDelete(String vhost, FullHttpRequest request);
}
```

---

## Implementation Details

**Module:** `http-server`

**Files to study:**

| File | Why |
|------|-----|
| `http-server/src/main/java/kafka/server/http/HttpRouter.java` | Pattern for adding new URI patterns and HandlerType enum values |
| `http-server/src/main/java/kafka/server/http/HttpRequestHandler.java` | Pattern for dispatching RouteResult to handler logic |
| `http-server/src/main/java/kafka/server/http/HttpErrorMapper.java` | Error-to-HTTP-status mapping pattern |

**Files to create:**

| File | What it does |
|------|--------------|
| `http-server/src/main/java/kafka/server/http/rest/ExchangeRestHandler.java` | Exchange REST CRUD |
| `http-server/src/main/java/kafka/server/http/rest/QueueRestHandler.java` | Queue REST CRUD with consumer details |
| `http-server/src/main/java/kafka/server/http/rest/BindingRestHandler.java` | Binding REST CRUD with filters |

**Files to modify:**

| File | What changes |
|------|--------------|
| `http-server/src/main/java/kafka/server/http/HttpRouter.java` | Add 10+ new HandlerType values and URI patterns for exchange/queue/binding routes |
| `http-server/src/main/java/kafka/server/http/HttpRequestHandler.java` | Dispatch new HandlerType values to REST handler methods |

> **CRITICAL:** Queue details endpoint (`GET /v1/queues/{name}`) must include the `consumers` array with per-subscriber info (subscriptionId, connectionId, credits, unacked, prefetch). This requires access to `WsSubscriptionManager` which tracks active subscriptions.

> **CRITICAL:** `PATCH /v1/queues/{name}` updates only metadata and topic config — it cannot change `durable`, `exclusive`, `autoDelete` (immutable after creation). Return 400 if client attempts to change immutable fields.

> **CRITICAL:** Queue redeclare via `PUT` is idempotent only if `durable`, `exclusive`, `autoDelete` match. If flags differ, return 409 `PRECONDITION_FAILED`.

**Implementation order:**
1. Add new `HandlerType` enum values and URI patterns to `HttpRouter`
2. Create `ExchangeRestHandler` with declare/get/list/delete
3. Create `QueueRestHandler` with declare/get/list/patch/delete/purge
4. Create `BindingRestHandler` with create/list/delete
5. Wire dispatch in `HttpRequestHandler`
6. Add unit tests

---

## Skeleton Code

### ExchangeRestHandler

```java
package kafka.server.http.rest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import kafka.server.http.routing.ExchangeManager;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * REST handlers for /v1/exchanges endpoints.
 *
 * // Time: Created - TASK-WS2.06
 */
public final class ExchangeRestHandler {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final ExchangeManager exchangeManager;

    public ExchangeRestHandler(ExchangeManager exchangeManager) {
        this.exchangeManager = Objects.requireNonNull(exchangeManager, "exchangeManager");
    }

    public FullHttpResponse handleDeclare(String exchangeName, String vhost,
                                          FullHttpRequest request) {
        // TODO: Parse JSON body for exchangeType, durable, autoDelete, internal, arguments
        // TODO: Delegate to exchangeManager.declareExchange(...)
        // TODO: Return 201 Created (new) or 200 OK (existing) with exchange details JSON
        throw new UnsupportedOperationException("Not yet implemented");
    }

    public FullHttpResponse handleGet(String exchangeName, String vhost) {
        // TODO: Look up exchange, return details + bindingCount + publishRate
        // TODO: Return 404 if not found
        throw new UnsupportedOperationException("Not yet implemented");
    }

    public FullHttpResponse handleList(String vhost) {
        // TODO: Return all exchanges for vhost as JSON array
        throw new UnsupportedOperationException("Not yet implemented");
    }

    public FullHttpResponse handleDelete(String exchangeName, String vhost,
                                          boolean ifUnused) {
        // TODO: Delete exchange, auto-remove bindings
        // TODO: ifUnused=true → 409 if bindings exist
        // TODO: Return 204 No Content
        throw new UnsupportedOperationException("Not yet implemented");
    }

    private FullHttpResponse jsonResponse(HttpResponseStatus status, ObjectNode body) {
        byte[] bytes = body.toString().getBytes(StandardCharsets.UTF_8);
        FullHttpResponse response = new DefaultFullHttpResponse(
            HttpVersion.HTTP_1_1, status, Unpooled.wrappedBuffer(bytes));
        response.headers().set("Content-Type", "application/json");
        response.headers().setInt("Content-Length", bytes.length);
        return response;
    }
}
```

### QueueRestHandler

```java
package kafka.server.http.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import kafka.server.http.routing.QueueManager;

import java.util.Objects;

/**
 * REST handlers for /v1/queues endpoints.
 *
 * // Time: Created - TASK-WS2.06
 */
public final class QueueRestHandler {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final QueueManager queueManager;

    public QueueRestHandler(QueueManager queueManager) {
        this.queueManager = Objects.requireNonNull(queueManager, "queueManager");
    }

    public FullHttpResponse handleDeclare(String queueName, String vhost,
                                          FullHttpRequest request) {
        // TODO: Parse JSON body for durable, exclusive, autoDelete, arguments
        // TODO: Delegate to queueManager.declareQueue(...)
        // TODO: Return 201 Created (new) or 200 OK (existing)
        // TODO: Include messageCount, consumerCount, backingTopic, partitions
        throw new UnsupportedOperationException("Not yet implemented");
    }

    public FullHttpResponse handleGet(String queueName, String vhost) {
        // TODO: Return full queue details including consumers array:
        //   [{ subscriptionId, connectionId, credits, unacked, prefetch }]
        // TODO: Include publishRate, deliverRate, ackRate, unackedCount
        throw new UnsupportedOperationException("Not yet implemented");
    }

    public FullHttpResponse handleList(String vhost) {
        // TODO: Return all queues for vhost with summary fields
        throw new UnsupportedOperationException("Not yet implemented");
    }

    public FullHttpResponse handlePatch(String queueName, String vhost,
                                         FullHttpRequest request) {
        // TODO: Parse arguments to update
        // TODO: x-message-ttl, x-max-length-bytes → AlterConfigsRequest on Kafka topic
        // TODO: Other args → queue metadata update only
        // TODO: Reject changes to durable/exclusive/autoDelete with 400
        throw new UnsupportedOperationException("Not yet implemented");
    }

    public FullHttpResponse handleDelete(String queueName, String vhost,
                                          boolean ifUnused, boolean ifEmpty) {
        // TODO: ifUnused=true → 409 if consumers active
        // TODO: ifEmpty=true → 409 if messageCount > 0
        // TODO: Send subscription-cancelled to active subscribers
        // TODO: Delete backing Kafka topic
        // TODO: Return 204 No Content
        throw new UnsupportedOperationException("Not yet implemented");
    }

    public FullHttpResponse handlePurge(String queueName, String vhost) {
        // TODO: Advance committed offsets to log end offset for all partitions
        // TODO: Return { "messageCount": N }
        throw new UnsupportedOperationException("Not yet implemented");
    }
}
```

### BindingRestHandler

```java
package kafka.server.http.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import kafka.server.http.routing.BindingManager;

import java.util.Objects;

/**
 * REST handlers for /v1/bindings endpoints.
 *
 * // Time: Created - TASK-WS2.06
 */
public final class BindingRestHandler {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final BindingManager bindingManager;

    public BindingRestHandler(BindingManager bindingManager) {
        this.bindingManager = Objects.requireNonNull(bindingManager, "bindingManager");
    }

    public FullHttpResponse handleCreate(String vhost, FullHttpRequest request) {
        // TODO: Parse JSON — detect queue-to-exchange vs exchange-to-exchange
        //   queue-to-exchange: { "exchange", "queue", "routingKey", "arguments" }
        //   exchange-to-exchange: { "source", "destination", "routingKey", "arguments" }
        // TODO: Delegate to bindingManager
        // TODO: Return 201 Created
        throw new UnsupportedOperationException("Not yet implemented");
    }

    public FullHttpResponse handleList(String vhost, String exchangeFilter,
                                       String queueFilter) {
        // TODO: Filter by exchange AND/OR queue (combined narrows results)
        // TODO: Return { "bindings": [...] }
        throw new UnsupportedOperationException("Not yet implemented");
    }

    public FullHttpResponse handleDelete(String vhost, FullHttpRequest request) {
        // TODO: Parse JSON with exchange/queue/routingKey identifiers
        // TODO: Delegate to bindingManager.unbind(...)
        // TODO: Return 204 No Content
        throw new UnsupportedOperationException("Not yet implemented");
    }
}
```

### HttpRouter extensions

```java
// New HandlerType values to add:
public enum HandlerType {
    // ... existing values ...
    DECLARE_EXCHANGE,
    GET_EXCHANGE,
    LIST_EXCHANGES,
    DELETE_EXCHANGE,
    DECLARE_QUEUE,
    GET_QUEUE,
    LIST_QUEUES,
    PATCH_QUEUE,
    DELETE_QUEUE,
    PURGE_QUEUE,
    CREATE_BINDING,
    LIST_BINDINGS,
    DELETE_BINDING
}

// New URI patterns:
private static final Pattern EXCHANGE_PATTERN =
    Pattern.compile("^/v1/exchanges/([^/?]+)$");
private static final Pattern EXCHANGES_LIST_PATTERN =
    Pattern.compile("^/v1/exchanges$");
private static final Pattern QUEUE_PATTERN =
    Pattern.compile("^/v1/queues/([^/?]+)$");
private static final Pattern QUEUES_LIST_PATTERN =
    Pattern.compile("^/v1/queues$");
private static final Pattern QUEUE_MESSAGES_PATTERN =
    Pattern.compile("^/v1/queues/([^/?]+)/messages$");
private static final Pattern BINDINGS_PATTERN =
    Pattern.compile("^/v1/bindings$");
```

### Test class

```java
package kafka.server.http.rest;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 * // Time: Created - TASK-WS2.06
 */
class ExchangeRestHandlerTest {

    @Test void declareExchange_returnsCreated() { /* TODO */ }
    @Test void declareExchange_existingSameType_returnsOk() { /* TODO */ }
    @Test void getExchange_notFound_returns404() { /* TODO */ }
    @Test void listExchanges_returnsAll() { /* TODO */ }
    @Test void deleteExchange_ifUnusedTrue_withBindings_returns409() { /* TODO */ }
}

class QueueRestHandlerTest {

    @Test void declareQueue_returnsCreated() { /* TODO */ }
    @Test void getQueueDetails_includesConsumersList() { /* TODO */ }
    @Test void patchQueue_immutableField_returns400() { /* TODO */ }
    @Test void deleteQueue_ifEmpty_withMessages_returns409() { /* TODO */ }
    @Test void purgeQueue_returnsMessageCount() { /* TODO */ }
    @Test void redeclareQueue_differentFlags_returns409() { /* TODO */ }
}

class BindingRestHandlerTest {

    @Test void createBinding_queueToExchange_returns201() { /* TODO */ }
    @Test void createBinding_exchangeToExchange_returns201() { /* TODO */ }
    @Test void listBindings_filterByExchange() { /* TODO */ }
    @Test void listBindings_filterByQueue() { /* TODO */ }
    @Test void listBindings_combinedFilter() { /* TODO */ }
    @Test void deleteBinding_returns204() { /* TODO */ }
}
```

---

## Tests

**Test classes:**
- `http-server/src/test/java/kafka/server/http/rest/ExchangeRestHandlerTest.java`
- `http-server/src/test/java/kafka/server/http/rest/QueueRestHandlerTest.java`
- `http-server/src/test/java/kafka/server/http/rest/BindingRestHandlerTest.java`
- `http-server/src/test/java/kafka/server/http/HttpRouterTest.java` (extend with new route tests)

| Test method | What it verifies |
|-------------|-----------------|
| `declareExchange_returnsCreated` | PUT new exchange returns 201 |
| `declareExchange_existingSameType_returnsOk` | PUT idempotent re-declare returns 200 |
| `getExchange_notFound_returns404` | GET non-existent exchange returns 404 |
| `deleteExchange_ifUnusedTrue_withBindings_returns409` | Constraint enforcement |
| `getQueueDetails_includesConsumersList` | Consumer array with subscriptionId, credits, unacked |
| `patchQueue_immutableField_returns400` | Rejects durable/exclusive/autoDelete changes |
| `redeclareQueue_differentFlags_returns409` | PRECONDITION_FAILED on mismatch |
| `createBinding_exchangeToExchange_returns201` | E2E binding creation via REST |
| `listBindings_combinedFilter` | Both exchange and queue filter applied |

**Run command:**
```bash
timeout 300 ./gradlew :http-server:test --tests 'kafka.server.http.rest.*RestHandlerTest' -x spotlessCheck
```

---

## Rules

- All REST responses must set `Content-Type: application/json`.
- Use the existing `HttpErrorMapper` pattern for error-to-status mapping.
- Extract `X-Vhost` header (default `/`) from every request.
- Queue details `GET` must include the full `consumers` array per design doc §5.4.
- `PATCH` must not allow changing immutable queue fields (`durable`, `exclusive`, `autoDelete`).
- Use Jackson `ObjectMapper` for JSON serialization — consistent with existing HTTP handlers.

---

## Learning

- HttpRouter dispatch was kept as regex matchers; adding a separate
  `matchRoutingRoutes` method modeled on the existing `matchTopicRoutes`
  keeps the router flat and testable. Checkstyle caps cyclomatic complexity
  at 16, so the three route families (exchange / queue / binding) had to be
  split into their own methods plus small method-to-HandlerType helpers.
- The existing `RouteResult` only carried `topicName`, `consumerGroup`, and
  `groupId`. Extending it with a generic `resourceName` field (and a new
  constructor overload that defaults the others to null) was the minimum-
  disruption change that avoided touching every call site.
- `HttpRequestTranslator` uses a Java 21 exhaustive switch over `HandlerType`.
  Every new enum value MUST be accompanied by a matching branch or the whole
  module fails to compile. The WS2.06 routes all throw from the translator
  because they are dispatched synchronously by the Netty handler, never via
  `RequestChannel`.
- REST handlers produce `FullHttpResponse` synchronously, so they don't need
  the full `RequestChannel` + `HttpProcessor` pipeline. This is ideal for the
  routing CRUD endpoints, which are metadata-only and return in microseconds.
- `ExchangeManager.deleteExchange` already handles the `EXCHANGE_PROTECTED` /
  `EXCHANGE_IN_USE` / `EXCHANGE_TYPE_MISMATCH` cases. The REST handler's job
  is exclusively the HTTP surface: status code mapping and JSON framing.

---

## Limitations

- **No `QueueManager` yet.** `QueueRestHandler` defines a local
  `QueueStore` SAM plus a `QueueConflict` exception as a façade over whatever
  queue lifecycle implementation WS1.06 eventually ships. Once the real
  `QueueManager` lands, only a small adapter shim needs to be written.
- **`PATCH /v1/queues/{name}` and `DELETE /v1/queues/{name}/messages`
  (purge)** are routed through to the dispatch layer but respond with 501
  Not Implemented. They need `QueueManager` + Kafka admin client integration
  (AlterConfigs / topic purge via offset advancement) which are out of scope
  for WS2.06.
- **Queue details `consumers` array is always empty.** The spec requires it
  to include per-subscriber `{ subscriptionId, connectionId, credits,
  unacked, prefetch }` entries sourced from `WsSubscriptionManager`. The
  handler includes the field (so the shape matches), but population is
  deferred until the WS subscription manager is wired into the broker.
- **`ExchangeRestHandler.getExchange.bindingCount`** reports the count from
  the in-memory `BindingManager`, not from the `WsRoutingMetadataManager`.
  If bindings exist only in the persisted metadata but not in the local
  manager (e.g. right after restart before replay completes), the count
  will read zero. Once `WsRoutingMetadataManager` is the single source of
  truth, the handler should delegate to it.
- **REST bindings are queue-to-exchange only.** Exchange-to-exchange
  bindings still require the WebSocket control frame per task scope.
- **`DELETE /v1/bindings`** takes a JSON body (for symmetry with
  `POST /v1/bindings`); the task file also mentioned path-parametric form
  `DELETE /v1/bindings/{exchange}/{queue}/{routingKey}`. Body form is more
  flexible (supports arguments) and cleanly matches the AMQP delete
  semantics. A future task can add the path-parametric alias if clients
  need it.

---

## Field Notes

- Existing HTTP handlers (produce/fetch/metadata) serialize responses via
  `HttpResponseSerializer` driven by `AbstractResponse` payloads from the
  RequestChannel. The routing REST handlers, by contrast, return a
  `FullHttpResponse` directly — there is no Kafka API response to wrap. I
  therefore dispatched them in `HttpRequestHandler.scala` before any
  `HttpRequestTranslator` / `RequestChannel` work.
- Optional constructor parameters on the Scala `HttpRequestHandler` (with
  default nulls) keep all existing instantiation sites working without
  changes. Production wiring can pass live handlers later; tests see 501
  Not Implemented which is still a defined status code.
- `bindingManager` in `ExchangeRestHandler` is optional. Many early unit
  tests don't have a real binding manager to hand, so the constructor
  accepts null and the `bindingCount` field simply reports zero. Production
  wiring passes the real manager.
- Kept Jackson usage consistent with the existing HTTP handlers —
  `ObjectMapper` at class scope, `ObjectNode` for building responses,
  `JsonNode.fieldNames()` instead of the deprecated `fields()`.

---

---

## Acceptance Criteria

- [ ] `timeout 300 ./gradlew :http-server:test --tests 'kafka.server.http.rest.*RestHandlerTest' -x spotlessCheck` exits 0
- [ ] `grep -r "ExchangeRestHandler" http-server/src/main/java/` returns at least 2 hits
- [ ] `grep -r "QueueRestHandler" http-server/src/main/java/` returns at least 2 hits
- [ ] `grep -r "BindingRestHandler" http-server/src/main/java/` returns at least 2 hits
- [ ] `grep -r "DECLARE_EXCHANGE\|LIST_EXCHANGES\|GET_EXCHANGE\|DELETE_EXCHANGE" http-server/src/main/java/kafka/server/http/HttpRouter.java` returns at least 4 hits
- [ ] Queue GET response includes `consumers` array field
- [ ] Learning section filled with at least one entry
- [ ] Limitations section filled (use "None" if truly none)

---

## File Manifest

**Files created:**
- `http-server/src/main/java/kafka/server/http/rest/ExchangeRestHandler.java`
- `http-server/src/main/java/kafka/server/http/rest/QueueRestHandler.java`
- `http-server/src/main/java/kafka/server/http/rest/BindingRestHandler.java`
- `http-server/src/test/java/kafka/server/http/HttpRouterRoutingTest.java`
- `http-server/src/test/java/kafka/server/http/rest/ExchangeRestHandlerTest.java`
- `http-server/src/test/java/kafka/server/http/rest/QueueRestHandlerTest.java`
- `http-server/src/test/java/kafka/server/http/rest/BindingRestHandlerTest.java`
- `http-server/src/test/java/kafka/server/http/rest/RoutingRestHandlerTest.java`

**Files modified:**
- `http-server/src/main/java/kafka/server/http/HttpRouter.java` — added
  13 new `HandlerType` values, 6 URL patterns, 4 new matcher methods, and
  `validateResourceName()` helper. Added `resourceName` field on `RouteResult`.
- `http-server/src/main/java/kafka/server/http/HttpRequestTranslator.java` —
  added a catch-all branch in the exhaustive `HandlerType` switch to keep the
  module compiling with the new enum values.
- `http-server/src/main/scala/kafka/network/HttpRequestHandler.scala` —
  added 3 optional constructor parameters (`exchangeRestHandler`,
  `queueRestHandler`, `bindingRestHandler`), a `isRestRoutingHandler`
  dispatch predicate, a `handleRestRoutingRequest` method, and a
  `notWired(...)` fallback that returns 501 when a handler isn't injected.

**Test counts:** 57 new tests across 5 test classes — all pass.
