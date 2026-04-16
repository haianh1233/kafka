# TASK-B.05: HttpRequestHandler

## Prerequisites

- **TASK-B.01** (HttpRouter, HttpRequestTranslator) — Provides `HttpRouter` for URI routing and `HttpRequestTranslator` for JSON-to-Kafka-request translation. The handler calls `HttpRouter.route()` to determine the handler type, then `HttpRequestTranslator.translate()` to build the serialized Kafka request.
- **TASK-B.02** (HttpResponseSerializer) — Provides `HttpResponseSerializer` for converting Kafka responses to JSON. Used for health check direct responses and error responses.
- **TASK-B.04** (HttpProcessor) — Provides `HttpProcessor` for response routing from `RequestChannel` back to Netty. The handler calls `httpProcessor.registerChannel()` for each request so that responses can be routed back.
- **TASK-A.01** (HttpAuthenticationContext) — Provides `HttpAuthenticationContext implements AuthenticationContext` for extracting authentication info from HTTP requests (mTLS certs, Bearer tokens, etc.) and passing to `KafkaPrincipalBuilder`.

---

## Context

`HttpRequestHandler` is the Netty `SimpleChannelInboundHandler<FullHttpRequest>` that sits at the end of the Netty pipeline (installed by `HttpChannelInitializer` from TASK-B.03). It is the single entry point for all HTTP requests into the Kafka broker.

For each incoming HTTP request, the handler:
1. Routes the request via `HttpRouter` to determine the handler type
2. For health checks: responds directly without touching `RequestChannel`
3. For all other requests: translates via `HttpRequestTranslator`, builds a `RequestContext`, and enqueues into `RequestChannel`
4. Uses non-blocking `requestChannel.tryEnqueue()` (not blocking `put()`) to prevent Netty worker thread stalls
5. If the queue is full, returns HTTP 503 with `Retry-After: 1` immediately

The handler is per-connection (one instance per Netty `ChannelPipeline`). Since HTTP/1.1 connections are sequential (one outstanding request at a time), the handler tracks at most one pending request via `pendingCtx`.

Key design decisions from the design doc:
- **`connectionId`** uses `ctx.channel().id().asLongText()` (Netty channel ID) — not correlationId (design doc section 14.2)
- **Non-blocking enqueue** via `requestChannel.tryEnqueue()` — returns false when queue is full instead of blocking (design doc section 14.3)
- **`X-Kafka-Request-ID`** — accepts from client `X-Request-ID` header or generates a UUID (design doc section 14.10)
- **Principal** comes from `KafkaPrincipalBuilder.build(HttpAuthenticationContext)` — pluggable auth (design doc section 12.1)

---

## Specification

### `HttpRequestHandler` — `kafka.network.HttpRequestHandler`

```scala
/**
 * Netty SimpleChannelInboundHandler that translates HTTP requests into Kafka
 * protocol requests and places them on the shared RequestChannel.
 *
 * One instance per connection (per Netty ChannelPipeline).
 *
 * Pipeline position: last handler after HttpServerCodec, HttpObjectAggregator,
 * HttpContentCompressor, IdleStateHandler, and IdleStateCloseHandler.
 */
class HttpRequestHandler(
    requestChannel: RequestChannel,
    httpProcessor: HttpProcessor,
    principalBuilder: KafkaPrincipalBuilder,
    config: KafkaConfig,
    endpoint: Endpoint,
    router: HttpRouter,
    translator: HttpRequestTranslator,
    metadataSupplier: Function[String, Integer],
    httpAcceptor: HttpAcceptor,    // for drain check and pending count
    brokerState: () => BrokerState, // for health check
    brokerId: Int,
    clusterId: String
) extends SimpleChannelInboundHandler[FullHttpRequest] {

    override def channelRead0(ctx: ChannelHandlerContext, req: FullHttpRequest): Unit
    override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit
}
```

### `RequestContext` construction

The `RequestContext` is built per-request with:
```java
new RequestContext(
    header,                // RequestHeader(apiKey, latestVersion, clientId, correlationId)
    connectionId,          // ctx.channel().id().asLongText()
    clientAddress,         // ((InetSocketAddress) ctx.channel().remoteAddress()).getAddress()
    principal,             // from KafkaPrincipalBuilder.build(HttpAuthenticationContext)
    listenerName,          // ListenerName.normalised(endpoint.listener())
    securityProtocol,      // endpoint.securityProtocol() (HTTP or HTTPS)
    clientInformation,     // ClientInformation.EMPTY
    fromPrivilegedListener // false (HTTP is never the inter-broker listener)
)
```

### Behavioral contracts

- Health check (`GET /v1/health`): returns `{"status":"RUNNING","brokerId":N,"clusterId":"..."}` with 200 if broker is `RUNNING`, or `{"status":"<state>"}` with 503 otherwise. Never touches `RequestChannel`.
- For all other routes: calls `HttpRouter.route()`, then `HttpRequestTranslator.translate()`, builds `RequestChannel.Request`, and calls `requestChannel.tryEnqueue()`.
- If `tryEnqueue()` returns false (queue full): returns HTTP 503 with JSON body `{"errorCode":"QUEUE_FULL","errorMessage":"Broker request queue saturated, retry later"}` and `Retry-After: 1`.
- If `httpAcceptor.isAccepting` is false (drain mode): returns HTTP 503 with `{"errorCode":"SHUTTING_DOWN","errorMessage":"Broker is shutting down"}`.
- `X-Kafka-Request-ID`: accepts from client `X-Request-ID` header (max 64 chars), or generates `UUID.randomUUID().toString()`. Stored on the request for response echo-back.
- `X-Kafka-Client-ID`: validated via `HttpRouter.validateClientId()`, used in `RequestHeader`.
- `correlationId`: generated from an `AtomicInteger` counter local to the handler instance, incrementing per request.
- `exceptionCaught()`: logs the error and closes the channel. Returns HTTP 500 if the channel is still writable.

---

## Implementation Details

**Module:** `http-server`

**Files to study:**

| File | Why |
|------|-----|
| `clients/src/main/java/org/apache/kafka/common/requests/RequestContext.java` lines 49-67 | `RequestContext` constructor — exact parameter types and order |
| `core/src/main/scala/kafka/network/RequestChannel.scala` lines 65-100 | `RequestChannel.Request` class — constructor parameters |
| `clients/src/main/java/org/apache/kafka/common/security/auth/KafkaPrincipalBuilder.java` | `KafkaPrincipalBuilder.build(AuthenticationContext)` — the auth interface |
| Design doc section 8.5 | The HttpRequestHandler specification and code example |

```java
// From RequestContext.java lines 49-67 — constructor:
public RequestContext(RequestHeader header,
                      String connectionId,
                      InetAddress clientAddress,
                      KafkaPrincipal principal,
                      ListenerName listenerName,
                      SecurityProtocol securityProtocol,
                      ClientInformation clientInformation,
                      boolean fromPrivilegedListener) {
    this(header, connectionId, clientAddress, Optional.empty(),
         principal, listenerName, securityProtocol,
         clientInformation, fromPrivilegedListener, Optional.empty());
}
```

```scala
// From RequestChannel.scala lines 65-72 — Request constructor:
class Request(val processor: Int,
              val context: RequestContext,
              val startTimeNanos: Long,
              val memoryPool: MemoryPool,
              @volatile var buffer: ByteBuffer,
              metrics: RequestChannelMetrics,
              val envelope: Option[RequestChannel.Request] = None) extends BaseRequest
```

**Files to create:**

| File | What it does |
|------|--------------|
| `http-server/src/main/scala/kafka/network/HttpRequestHandler.scala` | Netty handler: route, translate, build RequestContext, enqueue to RequestChannel |

> **CRITICAL:** `channelRead0()` MUST NOT block. All I/O is async. The `requestChannel.tryEnqueue()` call is non-blocking (`offer()` semantics). Never call `requestChannel.sendRequest()` which uses blocking `put()`.

> **CRITICAL:** `connectionId` MUST use `ctx.channel().id().asLongText()` — NOT `correlationId`. The `HttpProcessor` uses `connectionId` to route responses back to the correct Netty channel.

> **CRITICAL:** `fromPrivilegedListener` is always `false` for HTTP. The HTTP listener must never be the inter-broker listener (design doc section 8.1).

> **CRITICAL:** The `RequestChannel.Request` constructor calls `context.parseRequest(buffer)` which deserializes the request from the ByteBuffer. The buffer must contain a valid serialized Kafka request (produced by `HttpRequestTranslator`).

> **CRITICAL:** Register the channel with `HttpProcessor` BEFORE enqueueing the request. If the request is processed faster than the channel registration (race condition), the response would have nowhere to go.

**Implementation order:**
1. Implement `channelRead0()` — route, health check short-circuit, translate, build context, enqueue
2. Implement health check response (direct, no RequestChannel)
3. Implement queue-full response (503 + Retry-After)
4. Implement drain-mode rejection (503)
5. Implement `exceptionCaught()` error handler
6. Implement request ID generation/acceptance

---

## Skeleton Code

### `HttpRequestHandler.scala`

```scala
package kafka.network

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.{Function => JFunction}

import io.netty.buffer.Unpooled
import io.netty.channel.{ChannelFutureListener, ChannelHandlerContext}
import io.netty.handler.codec.http._
import io.netty.handler.codec.http.HttpResponseStatus._
import io.netty.handler.codec.http.HttpVersion.HTTP_1_1
import kafka.server.http.{HttpProcessor, HttpRequestTranslator, HttpResponseSerializer, HttpRouter, HttpServerConfigs}
import kafka.utils.Logging
import org.apache.kafka.common.Endpoint
import org.apache.kafka.common.memory.MemoryPool
import org.apache.kafka.common.network.{ClientInformation, ListenerName}
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.{RequestContext, RequestHeader}
import org.apache.kafka.common.security.auth.{KafkaPrincipal, KafkaPrincipalBuilder, SecurityProtocol}
import org.apache.kafka.server.BrokerState
import org.apache.kafka.server.config.KafkaConfig

import java.nio.charset.StandardCharsets

/**
 * Netty inbound handler that translates HTTP requests into Kafka protocol requests
 * and places them on the shared RequestChannel for processing by KafkaApis.
 *
 * One instance per Netty connection (per ChannelPipeline).
 *
 * Request flow:
 *   1. Route via HttpRouter -> HandlerType
 *   2. Health check -> direct response (no RequestChannel)
 *   3. Translate via HttpRequestTranslator -> (ApiKeys, ByteBuffer)
 *   4. Build RequestContext (principal from KafkaPrincipalBuilder)
 *   5. Register channel with HttpProcessor (for response routing)
 *   6. Non-blocking enqueue via requestChannel.tryEnqueue()
 *   7. Queue full -> HTTP 503 + Retry-After: 1
 *
 * // Time: Created - TASK-B.05
 */
class HttpRequestHandler(
    requestChannel: RequestChannel,
    httpProcessor: HttpProcessor,
    principalBuilder: KafkaPrincipalBuilder,
    config: KafkaConfig,
    endpoint: Endpoint,
    router: HttpRouter,
    translator: HttpRequestTranslator,
    metadataSupplier: JFunction[String, Integer],
    httpAcceptor: HttpAcceptor,
    brokerState: () => BrokerState,
    brokerId: Int,
    clusterId: String
) extends io.netty.channel.SimpleChannelInboundHandler[FullHttpRequest] with Logging {

  // --- Per-connection correlation ID counter ---
  private val correlationIdCounter = new AtomicInteger(0)

  // --- Listener name (cached from endpoint) ---
  private val listenerName: ListenerName = ListenerName.normalised(endpoint.listener())

  // --- Security protocol (HTTP or HTTPS) ---
  private val securityProtocol: SecurityProtocol = endpoint.securityProtocol()

  // --- Max request ID length ---
  private val MAX_REQUEST_ID_LENGTH = 64

  // --- JSON content type ---
  private val JSON_CONTENT_TYPE = "application/json"

  // --- HTTP header names ---
  private val HEADER_CLIENT_ID = "X-Kafka-Client-ID"
  private val HEADER_REQUEST_ID_IN = "X-Request-ID"
  private val HEADER_REQUEST_ID_OUT = "X-Kafka-Request-ID"

  /**
   * Called by Netty for each complete HTTP request (after HttpObjectAggregator).
   *
   * MUST NOT BLOCK — all I/O is async via RequestChannel and HttpProcessor.
   */
  override def channelRead0(ctx: ChannelHandlerContext, req: FullHttpRequest): Unit = {
    // TODO: Step 0 — Check drain mode
    // if (!httpAcceptor.isAccepting) {
    //   sendErrorResponse(ctx, SERVICE_UNAVAILABLE,
    //     """{"errorCode":"SHUTTING_DOWN","errorMessage":"Broker is shutting down"}""",
    //     generateRequestId(req))
    //   return
    // }

    // TODO: Step 1 — Generate or accept request ID
    // val requestId = generateRequestId(req)

    // TODO: Step 2 — Route the request
    // val routeResult = try {
    //   router.route(req.method(), req.uri())
    // } catch {
    //   case e: InvalidRequestException =>
    //     sendErrorResponse(ctx, NOT_FOUND, buildErrorJson(e.getMessage), requestId)
    //     return
    //   case e: InvalidTopicException =>
    //     sendErrorResponse(ctx, BAD_REQUEST, buildErrorJson(e.getMessage), requestId)
    //     return
    // }

    // TODO: Step 3 — Health check short-circuit
    // if (routeResult.handlerType() == HttpRouter.HandlerType.HEALTH) {
    //   sendHealthResponse(ctx, requestId)
    //   return
    // }

    // TODO: Step 4 — Validate client ID
    // val clientId = HttpRouter.validateClientId(req.headers().get(HEADER_CLIENT_ID))

    // TODO: Step 5 — Translate HTTP request to Kafka protocol request
    // val body = extractBody(req)
    // val translationResult = try {
    //   translator.translate(routeResult, body, httpServerConfigs, metadataSupplier)
    // } catch {
    //   case e: InvalidRequestException =>
    //     sendErrorResponse(ctx, BAD_REQUEST, buildErrorJson(e.getMessage), requestId)
    //     return
    // }

    // TODO: Step 6 — Build principal via KafkaPrincipalBuilder
    // val principal = principalBuilder.build(new HttpAuthenticationContext(req, ctx))

    // TODO: Step 7 — Build RequestContext
    // val correlationId = correlationIdCounter.getAndIncrement()
    // val connectionId = ctx.channel().id().asLongText()
    // val clientAddress = ctx.channel().remoteAddress().asInstanceOf[InetSocketAddress].getAddress
    // val header = new RequestHeader(
    //   translationResult.apiKey(),
    //   translationResult.apiVersion(),
    //   clientId,
    //   correlationId)
    // val requestContext = new RequestContext(
    //   header, connectionId, clientAddress, principal,
    //   listenerName, securityProtocol,
    //   ClientInformation.EMPTY, false)

    // TODO: Step 8 — Build RequestChannel.Request
    // val kafkaRequest = new RequestChannel.Request(
    //   processor = httpProcessor.id(),
    //   context = requestContext,
    //   startTimeNanos = System.nanoTime(),
    //   memoryPool = MemoryPool.NONE,
    //   buffer = translationResult.serializedRequest(),
    //   metrics = requestChannel.metrics,
    //   envelope = None)

    // TODO: Step 9 — Register channel with HttpProcessor BEFORE enqueue
    //                 (race condition: if request is processed before registration,
    //                  response has nowhere to go)
    // httpProcessor.registerChannel(connectionId, ctx)
    // httpAcceptor.incrementPending()
    // ctx.channel().closeFuture().addListener { (_: io.netty.channel.ChannelFuture) =>
    //   httpAcceptor.decrementPending()
    // }

    // TODO: Step 10 — Non-blocking enqueue
    // if (!requestChannel.tryEnqueue(kafkaRequest)) {
    //   httpProcessor.channels.remove(connectionId) // cleanup registration
    //   httpAcceptor.decrementPending()
    //   sendErrorResponse(ctx, SERVICE_UNAVAILABLE,
    //     """{"errorCode":"QUEUE_FULL","errorMessage":"Broker request queue saturated, retry later"}""",
    //     requestId, retryAfter = 1)
    //   return
    // }

    throw new UnsupportedOperationException("Not yet implemented")
  }

  /**
   * Handles exceptions thrown during channel operations.
   * Logs the error and closes the channel, optionally sending a 500 response.
   */
  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    // TODO: error(s"Exception in HTTP handler for ${ctx.channel().remoteAddress()}", cause)
    // TODO: if (ctx.channel().isActive) {
    //   sendErrorResponse(ctx, INTERNAL_SERVER_ERROR,
    //     """{"errorCode":"UNKNOWN","errorMessage":"Internal server error"}""", "unknown")
    // }
    // TODO: ctx.close()
    throw new UnsupportedOperationException("Not yet implemented")
  }

  // --- Health check direct response ---

  /**
   * Sends a health check response directly without touching RequestChannel.
   * Returns 200 if broker is RUNNING, 503 otherwise.
   *
   * Response: {"status":"RUNNING","brokerId":N,"clusterId":"..."}
   */
  private def sendHealthResponse(ctx: ChannelHandlerContext, requestId: String): Unit = {
    // TODO: val state = brokerState()
    // TODO: val status = if (state == BrokerState.RUNNING) OK else SERVICE_UNAVAILABLE
    // TODO: val body = s"""{"status":"${state.toString}","brokerId":$brokerId,"clusterId":"$clusterId"}"""
    // TODO: sendJsonResponse(ctx, status, body, requestId)
    throw new UnsupportedOperationException("Not yet implemented")
  }

  // --- Request ID generation ---

  /**
   * Generates or accepts X-Kafka-Request-ID.
   * Accepts client-provided X-Request-ID if present and <= 64 chars.
   * Otherwise generates a UUID.
   */
  private def generateRequestId(req: FullHttpRequest): String = {
    // TODO: val clientRequestId = req.headers().get(HEADER_REQUEST_ID_IN)
    // TODO: if (clientRequestId != null && clientRequestId.length <= MAX_REQUEST_ID_LENGTH)
    //         clientRequestId
    //       else
    //         UUID.randomUUID().toString
    throw new UnsupportedOperationException("Not yet implemented")
  }

  // --- Body extraction ---

  /**
   * Extracts the request body as a byte array from a FullHttpRequest.
   */
  private def extractBody(req: FullHttpRequest): Array[Byte] = {
    // TODO: val content = req.content()
    // TODO: val bytes = new Array[Byte](content.readableBytes())
    // TODO: content.readBytes(bytes)
    // TODO: bytes
    throw new UnsupportedOperationException("Not yet implemented")
  }

  // --- Error response helpers ---

  /**
   * Sends a JSON error response with the given HTTP status.
   */
  private def sendErrorResponse(ctx: ChannelHandlerContext, status: HttpResponseStatus,
                                jsonBody: String, requestId: String,
                                retryAfter: Int = -1): Unit = {
    // TODO: Build DefaultFullHttpResponse(HTTP_1_1, status, Unpooled.copiedBuffer(jsonBody, UTF_8))
    // TODO: Set headers: Content-Type, Content-Length, X-Kafka-Request-ID, Connection: keep-alive
    // TODO: If retryAfter > 0: set Retry-After header
    // TODO: ctx.writeAndFlush(response)
    throw new UnsupportedOperationException("Not yet implemented")
  }

  /**
   * Sends a JSON response with the given HTTP status.
   */
  private def sendJsonResponse(ctx: ChannelHandlerContext, status: HttpResponseStatus,
                               jsonBody: String, requestId: String): Unit = {
    // TODO: same as sendErrorResponse but without Retry-After
    throw new UnsupportedOperationException("Not yet implemented")
  }

  /**
   * Builds a simple JSON error object string.
   */
  private def buildErrorJson(message: String): String = {
    // TODO: Return """{"errorCode":"INVALID_REQUEST","errorMessage":"<message>"}"""
    //       (escape message for JSON safety)
    throw new UnsupportedOperationException("Not yet implemented")
  }
}
```

### Test class — `HttpRequestHandlerTest.scala`

```scala
package kafka.network

import io.netty.buffer.Unpooled
import io.netty.channel._
import io.netty.channel.embedded.EmbeddedChannel
import io.netty.handler.codec.http._
import kafka.server.http.{HttpProcessor, HttpRequestTranslator, HttpRouter, HttpServerConfigs}
import org.apache.kafka.common.Endpoint
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.security.auth.{KafkaPrincipal, KafkaPrincipalBuilder, SecurityProtocol}
import org.apache.kafka.server.BrokerState
import org.apache.kafka.server.config.KafkaConfig
import org.junit.jupiter.api.{BeforeEach, Test}
import org.junit.jupiter.api.Assertions._
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets

/**
 * // Time: Created - TASK-B.05
 */
class HttpRequestHandlerTest {

  private var requestChannel: RequestChannel = _
  private var httpProcessor: HttpProcessor = _
  private var principalBuilder: KafkaPrincipalBuilder = _
  private var config: KafkaConfig = _
  private var endpoint: Endpoint = _
  private var router: HttpRouter = _
  private var translator: HttpRequestTranslator = _
  private var httpAcceptor: HttpAcceptor = _
  private var handler: HttpRequestHandler = _

  @BeforeEach
  def setUp(): Unit = {
    // TODO: Create mocks for all dependencies
    // TODO: Mock principalBuilder to return KafkaPrincipal.ANONYMOUS
    // TODO: Mock httpAcceptor.isAccepting to return true
    // TODO: Create handler instance
  }

  @Test
  def healthCheck_running_returns200(): Unit = {
    // Arrange: broker state = RUNNING
    // Act: send GET /v1/health
    // Assert: response status = 200
    // Assert: body contains "RUNNING"
  }

  @Test
  def healthCheck_notRunning_returns503(): Unit = {
    // Arrange: broker state = STARTING
    // Act: send GET /v1/health
    // Assert: response status = 503
  }

  @Test
  def healthCheck_includesBrokerIdAndClusterId(): Unit = {
    // Arrange: brokerId=3, clusterId="abc123"
    // Act: send GET /v1/health
    // Assert: body contains "brokerId":3 and "clusterId":"abc123"
  }

  @Test
  def normalRequest_registersChannelAndEnqueues(): Unit = {
    // Arrange: valid produce request
    // Act: send POST /v1/topics/orders/records with JSON body
    // Assert: httpProcessor.registerChannel was called
    // Assert: requestChannel.tryEnqueue was called
  }

  @Test
  def queueFull_returns503WithRetryAfter(): Unit = {
    // Arrange: requestChannel.tryEnqueue returns false
    // Act: send valid request
    // Assert: response status = 503
    // Assert: Retry-After: 1 header present
    // Assert: body contains "QUEUE_FULL"
  }

  @Test
  def drainMode_returns503(): Unit = {
    // Arrange: httpAcceptor.isAccepting returns false
    // Act: send any request
    // Assert: response status = 503
    // Assert: body contains "SHUTTING_DOWN"
  }

  @Test
  def invalidRoute_returns404(): Unit = {
    // Arrange: router.route throws InvalidRequestException
    // Act: send GET /v1/nonexistent
    // Assert: response status = 404
  }

  @Test
  def invalidTopic_returns400(): Unit = {
    // Arrange: router.route throws InvalidTopicException
    // Act: send POST /v1/topics/..%00/records
    // Assert: response status = 400
  }

  @Test
  def requestId_echoedFromClient(): Unit = {
    // Arrange: request has X-Request-ID: "client-123"
    // Act: send health check
    // Assert: response has X-Kafka-Request-ID: "client-123"
  }

  @Test
  def requestId_generatedWhenMissing(): Unit = {
    // Arrange: no X-Request-ID header
    // Act: send health check
    // Assert: response has X-Kafka-Request-ID matching UUID pattern
  }

  @Test
  def requestId_generatedWhenTooLong(): Unit = {
    // Arrange: X-Request-ID longer than 64 chars
    // Act: send health check
    // Assert: response X-Kafka-Request-ID is a UUID (not the original)
  }

  @Test
  def exceptionCaught_closesChannel(): Unit = {
    // Arrange: trigger an exception
    // Act: exceptionCaught is called
    // Assert: channel is closed
  }

  @Test
  def channelRegistration_beforeEnqueue(): Unit = {
    // Verify that httpProcessor.registerChannel is called BEFORE requestChannel.tryEnqueue
    // This prevents the race condition where the response arrives before registration
  }
}
```

### Existing pattern reference

```scala
// From design doc section 8.5 — the HttpRequestHandler specification:
class HttpRequestHandler(
  requestChannel:   RequestChannel,
  principalBuilder: KafkaPrincipalBuilder,
  config:           KafkaConfig,
  ...
) extends SimpleChannelInboundHandler[FullHttpRequest] {

  override def channelRead0(ctx: ChannelHandlerContext, req: FullHttpRequest): Unit = {
    if (req.uri() == "/v1/health") { sendHealthResponse(ctx); return }

    val (apiKey, requestBuffer) = HttpRequestTranslator.translate(req)
    val principal = principalBuilder.build(new HttpAuthenticationContext(req))
    val requestContext = new RequestContext(
      new RequestHeader(apiKey, apiKey.latestVersion, clientId, correlationId),
      connectionId      = ctx.channel().id().asLongText(),
      clientAddress     = remoteAddress(ctx),
      principal         = principal,
      listenerName      = ListenerName.normalised("HTTP"),
      securityProtocol  = SecurityProtocol.HTTP,
      clientInformation = ClientInformation.EMPTY,
      fromPrivilegedListener = false
    )

    if (!requestChannel.tryEnqueue(kafkaRequest)) {
      sendQueueFullResponse(ctx)
      return
    }
  }
}
```

```java
// From RequestContext.java lines 49-67 — the constructor used:
public RequestContext(RequestHeader header,
                      String connectionId,
                      InetAddress clientAddress,
                      KafkaPrincipal principal,
                      ListenerName listenerName,
                      SecurityProtocol securityProtocol,
                      ClientInformation clientInformation,
                      boolean fromPrivilegedListener)
```

---

## Tests

**Test class:** `http-server/src/test/scala/kafka/network/HttpRequestHandlerTest.scala`

| Test method | What it verifies |
|-------------|-----------------|
| `healthCheck_running_returns200` | Health check returns 200 when RUNNING |
| `healthCheck_notRunning_returns503` | Health check returns 503 when not RUNNING |
| `healthCheck_includesBrokerIdAndClusterId` | Response body has correct broker info |
| `normalRequest_registersChannelAndEnqueues` | Channel registered + request enqueued |
| `queueFull_returns503WithRetryAfter` | Queue full -> 503 + Retry-After: 1 |
| `drainMode_returns503` | Drain mode -> 503 + SHUTTING_DOWN |
| `invalidRoute_returns404` | Unknown path -> 404 |
| `invalidTopic_returns400` | Bad topic name -> 400 |
| `requestId_echoedFromClient` | Client X-Request-ID echoed back |
| `requestId_generatedWhenMissing` | UUID generated when no client ID |
| `requestId_generatedWhenTooLong` | Oversized client ID replaced with UUID |
| `exceptionCaught_closesChannel` | Exception handler closes channel |
| `channelRegistration_beforeEnqueue` | Registration happens before enqueue (ordering) |

**Run command:**
```bash
./gradlew :http-server:test --tests "kafka.network.HttpRequestHandlerTest"
```

---

## Rules

- `channelRead0()` MUST NOT block. All operations are non-blocking. Use `tryEnqueue()`, never `sendRequest()`.
- `connectionId` MUST be `ctx.channel().id().asLongText()` — NOT correlationId.
- `fromPrivilegedListener` is always `false` for HTTP endpoints.
- Register the channel with `HttpProcessor` BEFORE enqueueing the request to prevent response-before-registration race conditions.
- `X-Request-ID` from clients is accepted only if 64 chars or fewer. Otherwise generate a UUID.
- Health check (`GET /v1/health`) is served directly from the handler — no RequestChannel round-trip.
- The handler is per-connection (one instance per ChannelPipeline), not shared.

---

## Learning

- **EmbeddedChannel uses EmbeddedSocketAddress, not InetSocketAddress.** When testing Netty handlers with `EmbeddedChannel`, `ctx.channel().remoteAddress()` returns `EmbeddedSocketAddress` which cannot be cast to `InetSocketAddress`. The handler must use pattern matching (`match { case inet: InetSocketAddress => ... }`) with a fallback to `InetAddress.getLoopbackAddress` to avoid ClassCastException in tests and edge cases.
- **Java package-private methods are inaccessible from Scala in different packages.** `HttpRouter.validateClientId()` was package-private (default Java access). Since `HttpRequestHandler` is in `kafka.network` (not `kafka.server.http`), it cannot access package-private members. Changed to `public static`.
- **http-server module needed the Scala plugin.** The module was Java-only; adding Scala source files requires `apply plugin: 'scala'` and the `libs.scalaLibrary` dependency in `build.gradle`.
- **HttpProcessor.channels is private.** The skeleton code referenced `httpProcessor.channels.remove()` directly, but this is a private field. Added `unregisterChannel(connectionId)` public method to `HttpProcessor` for cleanup on enqueue failure.
- **RequestChannel.Request constructor eagerly parses the buffer.** `context.parseRequest(buffer)` runs in the constructor body, so the ByteBuffer from the translator must contain a valid serialized Kafka request body (not including the header). The translator's `request.serialize().buffer()` produces exactly the body format that `AbstractRequest.parseRequest()` expects.

---

## Limitations

- **Authentication fields are null placeholders.** The `HttpAuthenticationContext` is constructed with null bearerToken, basicCredentials, and peerCertificates. Actual extraction from HTTP headers and TLS handshake will be wired in TASK-F.05 (security wiring).
- **HttpAcceptor is a stub.** The real `HttpAcceptor` (Netty ServerBootstrap lifecycle) will be implemented in TASK-B.03. The current stub provides the minimal `isAccepting`, `incrementPending`, `decrementPending` interface needed by the handler.

---

## Field Notes

- The `RequestChannelMetrics` class is at `org.apache.kafka.network.metrics.RequestChannelMetrics`, not in the `kafka.network` package. Tests must mock this type for `RequestChannel.Request` construction.
- `BrokerState` is at `org.apache.kafka.metadata.BrokerState`, not `org.apache.kafka.server.BrokerState` as the skeleton imports suggested.
- The skeleton used `kafka.server.KafkaConfig` but this is not needed -- the handler delegates config to `HttpServerConfigs` passed as a constructor parameter.
- 13 test methods all pass: health check (3), normal request (1), queue full (1), drain mode (1), invalid route (1), invalid topic (1), request ID (3), exception handling (1), registration ordering (1).

---

## Acceptance Criteria

- [x] `./gradlew :http-server:test --tests "kafka.network.HttpRequestHandlerTest"` exits 0
- [x] `HttpRequestHandler.scala` exists at `http-server/src/main/scala/kafka/network/HttpRequestHandler.scala`
- [x] Health check returns 200/503 without touching RequestChannel
- [x] Health response includes brokerId and clusterId
- [x] Queue-full returns 503 with `Retry-After: 1` header
- [x] Drain mode returns 503 with SHUTTING_DOWN error
- [x] `X-Kafka-Request-ID` is set on every response
- [x] Channel is registered with HttpProcessor before request is enqueued
- [x] `connectionId` uses `ctx.channel().id().asLongText()`
- [x] `fromPrivilegedListener` is always `false`
- [x] `exceptionCaught()` closes the channel
- [x] Learning section filled with at least one entry
- [x] Limitations section filled (use "None" if truly none)
- [x] File Manifest section updated after commit

---

## File Manifest

### 2026-04-16 -- TASK-B.05: HttpRequestHandler (commit 9bd130d3cc)
Created:
  - http-server/src/main/scala/kafka/network/HttpRequestHandler.scala -- Netty HTTP request handler
  - http-server/src/main/scala/kafka/network/HttpAcceptor.scala -- Stub for HttpAcceptor (full impl in TASK-B.03)
  - http-server/src/test/scala/kafka/network/HttpRequestHandlerTest.scala -- 13 handler tests
Modified:
  - build.gradle -- Added Scala plugin and scalaLibrary dep to http-server module
  - http-server/src/main/java/kafka/server/http/HttpRouter.java -- Made validateClientId() public
  - http-server/src/main/java/kafka/server/http/HttpProcessor.java -- Added unregisterChannel() method
