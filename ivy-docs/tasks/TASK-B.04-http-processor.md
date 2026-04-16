# TASK-B.04: HttpProcessor

## Prerequisites

- **TASK-A.02** (http-server module) — Creates the `http-server` Gradle submodule. This task creates `HttpProcessor.java` inside that module.
- **TASK-A.03** (RequestChannel.tryEnqueue) — Adds `tryEnqueue()` method to `RequestChannel` (non-blocking `offer()` instead of blocking `put()`). `HttpProcessor` is registered with `RequestChannel` so that `sendResponse()` can route responses back to it via `enqueueResponse()`.

---

## Context

When `KafkaApis` finishes processing a request, it calls `requestChannel.sendResponse(request, response)`. For binary protocol clients, the `Processor` (in `SocketServer.scala`) has a `responseQueue` and `inflightResponses` map that routes the response back to the correct NIO channel. HTTP needs an equivalent bridge between `RequestChannel` and Netty.

`HttpProcessor` is this bridge. It mirrors the binary `Processor` response-handling pattern but routes responses to Netty `ChannelHandlerContext` objects instead of NIO channels.

**How the binary Processor works** (from `SocketServer.scala` lines 935-973):
```scala
private def processNewResponses(): Unit = {
    var currentResponse: RequestChannel.Response = null
    while ({currentResponse = dequeueResponse(); currentResponse != null}) {
      val channelId = currentResponse.request.context.connectionId
      currentResponse match {
        case response: SendResponse => sendResponse(response, response.responseSend)
        case _: CloseConnectionResponse => close(channelId)
        case _: StartThrottlingResponse => handleChannelMuteEvent(channelId, THROTTLE_STARTED)
        case _: EndThrottlingResponse => handleChannelMuteEvent(channelId, THROTTLE_ENDED)
      }
    }
}
```

`HttpProcessor` follows this exact pattern but:
- Uses `ConcurrentHashMap<String, ChannelHandlerContext>` instead of NIO channel map
- `StartThrottlingResponse` → immediate HTTP 429 (no channel muting — HTTP is request/response)
- `EndThrottlingResponse` → no-op (HTTP has no mute/unmute concept)
- `SendResponse` → serialize via `HttpResponseSerializer` and write to Netty channel
- `CloseConnectionResponse` → close the Netty channel

The `HttpProcessor` has a unique `id` registered with `RequestChannel.addProcessor()` so that `RequestChannel.sendResponse()` can find it and call `enqueueResponse()`. A dedicated response-drainer thread polls `processResponses()` in a loop.

---

## Specification

### `HttpProcessor` — `kafka.server.http.HttpProcessor`

```java
/**
 * Response routing bridge between RequestChannel and Netty.
 *
 * Registered with RequestChannel.addProcessor(this) so that
 * RequestChannel.sendResponse() can route responses via enqueueResponse().
 *
 * Lifecycle:
 *   - Created by HttpAcceptor during startup
 *   - registerChannel() called by HttpRequestHandler for each new request
 *   - enqueueResponse() called by RequestChannel.sendResponse()
 *   - processResponses() polled by a dedicated response-drainer thread
 *   - close() shuts down the drainer thread and clears state
 */
public class HttpProcessor {

    /**
     * @param id unique processor ID, registered with RequestChannel
     * @param responseSerializer the serializer for Kafka responses to JSON
     */
    public HttpProcessor(int id, HttpResponseSerializer responseSerializer);

    /** Processor ID — used by RequestChannel for response routing. */
    public int id();

    /**
     * Registers a Netty channel for a connection. Called by HttpRequestHandler
     * when a new HTTP request arrives. The channel is automatically removed
     * when the connection closes.
     *
     * @param connectionId Netty channel ID (ctx.channel().id().asLongText())
     * @param ctx          Netty ChannelHandlerContext
     */
    public void registerChannel(String connectionId, ChannelHandlerContext ctx);

    /**
     * Enqueues a response for delivery to the Netty channel.
     * Called by RequestChannel.sendResponse() — same contract as binary Processor.
     * Non-blocking (uses LinkedBlockingDeque.add()).
     *
     * @param response the response to deliver
     */
    public void enqueueResponse(RequestChannel.Response response);

    /**
     * Processes all queued responses. Drains the queue and writes each response
     * to its Netty channel. Called in a loop by the response-drainer thread.
     *
     * Response type handling:
     *   SendResponse            -> serialize to JSON, write to Netty channel
     *   CloseConnectionResponse -> close the Netty channel
     *   StartThrottlingResponse -> HTTP 429 + Retry-After header
     *   EndThrottlingResponse   -> no-op (HTTP has no mute/unmute)
     */
    public void processResponses();

    /**
     * Starts the response-drainer thread.
     */
    public void startDrainer();

    /**
     * Shuts down the response-drainer thread and clears all state.
     */
    public void close();
}
```

### Behavioral contracts

- `registerChannel()` adds an entry to `channels` map. A channel-close listener automatically removes the entry when the client disconnects.
- `enqueueResponse()` is non-blocking — it calls `responseQueue.add()` which never blocks on an unbounded `LinkedBlockingDeque`.
- `processResponses()` drains the queue and processes each response. If the Netty channel for a response is no longer active (client disconnected), the response is silently dropped.
- `SendResponse` writes the serialized JSON to the Netty channel using `ctx.writeAndFlush()`.
- `StartThrottlingResponse` is converted to an immediate HTTP 429 response with `Retry-After` header.
- `EndThrottlingResponse` is a no-op — no logging, no action.
- `CloseConnectionResponse` calls `ctx.close()` on the Netty channel.
- The drainer thread runs `processResponses()` in a polling loop with a short sleep (e.g., 10ms) between empty polls. It checks a shutdown flag to exit.

---

## Implementation Details

**Module:** `http-server`

**Files to study:**

| File | Why |
|------|-----|
| `core/src/main/scala/kafka/network/SocketServer.scala` lines 935-973 | Binary `Processor.processNewResponses()` — the exact pattern to mirror |
| `core/src/main/scala/kafka/network/SocketServer.scala` lines 976-991 | `sendResponse()` — how response is written to a channel |
| `core/src/main/scala/kafka/network/RequestChannel.scala` lines 44-100 | `RequestChannel.Request` class — the request object that carries context |

```scala
// From SocketServer.scala lines 935-973 — the pattern to follow:
private def processNewResponses(): Unit = {
    var currentResponse: RequestChannel.Response = null
    while ({currentResponse = dequeueResponse(); currentResponse != null}) {
      val channelId = currentResponse.request.context.connectionId
      try {
        currentResponse match {
          case response: NoOpResponse =>
            updateRequestMetrics(response)
            handleChannelMuteEvent(channelId, ChannelMuteEvent.RESPONSE_SENT)
            tryUnmuteChannel(channelId)
          case response: SendResponse =>
            sendResponse(response, response.responseSend)
          case response: CloseConnectionResponse =>
            updateRequestMetrics(response)
            close(channelId)
          case _: StartThrottlingResponse =>
            handleChannelMuteEvent(channelId, ChannelMuteEvent.THROTTLE_STARTED)
          case _: EndThrottlingResponse =>
            handleChannelMuteEvent(channelId, ChannelMuteEvent.THROTTLE_ENDED)
            tryUnmuteChannel(channelId)
        }
      } catch {
        case e: Throwable =>
          processChannelException(channelId, s"Exception while processing response for $channelId", e)
      }
    }
}
```

**Files to create:**

| File | What it does |
|------|--------------|
| `http-server/src/main/java/kafka/server/http/HttpProcessor.java` | Response routing bridge between RequestChannel and Netty |

> **CRITICAL:** `enqueueResponse()` MUST be non-blocking. It is called from `KafkaRequestHandler` threads (via `RequestChannel.sendResponse()`). Any blocking here would stall the shared handler pool.

> **CRITICAL:** After writing a response to the Netty channel, the `connectionId` entry must be removed from the `channels` map. HTTP/1.1 is request-response: one request, one response, then the entry is stale. (The channel itself stays open for keep-alive, but the next request will re-register.)

> **CRITICAL:** The drainer thread must handle exceptions per-response. A failure to serialize or write one response must not prevent other responses from being processed.

> **CRITICAL:** `StartThrottlingResponse` for HTTP must be converted to an HTTP 429 response with `Retry-After: ceil(throttleTimeMs / 1000)` header. The binary protocol mutes the channel instead — HTTP has no mute mechanism.

**Implementation order:**
1. Implement the `channels` map with close-listener cleanup
2. Implement `enqueueResponse()` (trivial — just `responseQueue.add()`)
3. Implement `processResponses()` with the response-type switch
4. Implement the drainer thread (daemon thread with polling loop)
5. Implement `startDrainer()` and `close()`

---

## Skeleton Code

### `HttpProcessor.java`

```java
package kafka.server.http;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import kafka.network.RequestChannel;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Response routing bridge between {@link RequestChannel} and Netty.
 *
 * Mirrors the binary protocol's {@code Processor} response handling pattern
 * (SocketServer.scala lines 935-973) but routes to Netty channels instead of
 * NIO channels.
 *
 * <h3>Threading model:</h3>
 * <ul>
 *   <li>{@link #enqueueResponse} is called from KafkaRequestHandler threads (via RequestChannel)</li>
 *   <li>{@link #processResponses} is called from a dedicated response-drainer thread</li>
 *   <li>{@link #registerChannel} is called from Netty worker threads</li>
 * </ul>
 *
 * All three paths are thread-safe via ConcurrentHashMap and LinkedBlockingDeque.
 *
 * <h3>Response type handling:</h3>
 * <ul>
 *   <li>{@code SendResponse} -> serialize to JSON via HttpResponseSerializer, write to Netty channel</li>
 *   <li>{@code CloseConnectionResponse} -> close the Netty channel</li>
 *   <li>{@code StartThrottlingResponse} -> HTTP 429 + Retry-After header</li>
 *   <li>{@code EndThrottlingResponse} -> no-op (HTTP has no mute/unmute)</li>
 * </ul>
 *
 * // Time: Created - TASK-B.04
 */
public class HttpProcessor {

    private static final Logger log = LoggerFactory.getLogger(HttpProcessor.class);

    // --- Processor ID (registered with RequestChannel) ---
    private final int id;

    // --- Response queue (unbounded, non-blocking add) ---
    private final LinkedBlockingDeque<RequestChannel.Response> responseQueue =
        new LinkedBlockingDeque<>();

    // --- Active Netty channels: connectionId -> ChannelHandlerContext ---
    private final ConcurrentHashMap<String, ChannelHandlerContext> channels =
        new ConcurrentHashMap<>();

    // --- Drainer thread state ---
    private final AtomicBoolean running = new AtomicBoolean(false);
    private volatile Thread drainerThread;

    // --- Polling interval for the drainer thread when queue is empty ---
    private static final long POLL_INTERVAL_MS = 10;

    // --- Throttle response JSON template ---
    private static final String THROTTLE_BODY_TEMPLATE =
        "{\"errorCode\":%d,\"errorMessage\":\"%s\",\"throttleTimeMs\":%d}";

    /**
     * Creates a new HttpProcessor.
     *
     * @param id unique processor ID for RequestChannel registration
     */
    public HttpProcessor(int id) {
        this.id = id;
    }

    /**
     * Returns the processor ID used by RequestChannel for response routing.
     */
    public int id() {
        return id;
    }

    /**
     * Registers a Netty channel for a connection. Called by HttpRequestHandler
     * when a new HTTP request is received. A close listener automatically removes
     * the entry when the channel closes (client disconnect).
     *
     * @param connectionId Netty channel long text ID (ctx.channel().id().asLongText())
     * @param ctx          the Netty ChannelHandlerContext for writing responses
     */
    public void registerChannel(String connectionId, ChannelHandlerContext ctx) {
        Objects.requireNonNull(connectionId, "connectionId");
        Objects.requireNonNull(ctx, "ctx");

        // TODO: channels.put(connectionId, ctx)
        // TODO: Add close listener to auto-remove:
        //       ctx.channel().closeFuture().addListener((ChannelFutureListener) future ->
        //           channels.remove(connectionId));
        throw new UnsupportedOperationException("Not yet implemented");
    }

    /**
     * Enqueues a response for delivery to the Netty channel.
     * Non-blocking — called from KafkaRequestHandler threads via RequestChannel.sendResponse().
     *
     * @param response the response to deliver
     */
    public void enqueueResponse(RequestChannel.Response response) {
        Objects.requireNonNull(response, "response");

        // TODO: responseQueue.add(response)  // non-blocking on unbounded deque
        // TODO: Optionally wake up the drainer thread if it's sleeping
        throw new UnsupportedOperationException("Not yet implemented");
    }

    /**
     * Drains and processes all queued responses. For each response:
     * - Looks up the Netty channel by connectionId
     * - Dispatches based on response type (SendResponse, CloseConnection, etc.)
     * - Handles exceptions per-response to prevent one failure from blocking others
     *
     * Called by the response-drainer thread.
     */
    public void processResponses() {
        List<RequestChannel.Response> batch = new ArrayList<>();
        responseQueue.drainTo(batch);

        for (RequestChannel.Response response : batch) {
            String connectionId = response.request().context().connectionId();
            try {
                // TODO: Match response type:
                //
                // if (response instanceof RequestChannel.SendResponse sendResp) {
                //     ChannelHandlerContext ctx = channels.get(connectionId);
                //     if (ctx != null && ctx.channel().isActive()) {
                //         AbstractResponse kafkaResponse = sendResp.response();
                //         ApiKeys apiKey = response.request().header().apiKey();
                //         String requestId = extractRequestId(response.request());
                //         int effectiveMaxWaitMs = extractMaxWaitMs(response.request());
                //
                //         FullHttpResponse httpResponse = HttpResponseSerializer.serialize(
                //             kafkaResponse, requestId, apiKey, effectiveMaxWaitMs);
                //         ctx.writeAndFlush(httpResponse);
                //     } else {
                //         log.debug("Channel closed before response could be sent: {}", connectionId);
                //     }
                //     channels.remove(connectionId);
                // }
                //
                // else if (response instanceof RequestChannel.CloseConnectionResponse) {
                //     ChannelHandlerContext ctx = channels.remove(connectionId);
                //     if (ctx != null) ctx.close();
                // }
                //
                // else if (response instanceof RequestChannel.StartThrottlingResponse) {
                //     // HTTP: convert to immediate 429 response
                //     ChannelHandlerContext ctx = channels.get(connectionId);
                //     if (ctx != null && ctx.channel().isActive()) {
                //         int throttleTimeMs = response.request().apiThrottleTimeMs();
                //         sendThrottleResponse(ctx, throttleTimeMs);
                //     }
                //     channels.remove(connectionId);
                // }
                //
                // else if (response instanceof RequestChannel.EndThrottlingResponse) {
                //     // No-op for HTTP — no mute/unmute mechanism
                // }
                throw new UnsupportedOperationException("Not yet implemented");
            } catch (Exception e) {
                log.error("Error processing response for connection {}", connectionId, e);
                // Continue processing remaining responses
            }
        }
    }

    /**
     * Sends an HTTP 429 Too Many Requests response for throttled requests.
     *
     * @param ctx           Netty channel context
     * @param throttleTimeMs throttle duration in milliseconds
     */
    private void sendThrottleResponse(ChannelHandlerContext ctx, long throttleTimeMs) {
        // TODO: Build JSON body: {"errorCode":89,"errorMessage":"THROTTLING_QUOTA_EXCEEDED","throttleTimeMs":N}
        // TODO: Build FullHttpResponse with status 429
        // TODO: Set Retry-After: Math.max(1, (int) Math.ceil(throttleTimeMs / 1000.0))
        // TODO: Set Content-Type: application/json
        // TODO: ctx.writeAndFlush(response)
        throw new UnsupportedOperationException("Not yet implemented");
    }

    /**
     * Starts the response-drainer thread. This thread polls processResponses()
     * in a loop, sleeping briefly between empty polls.
     */
    public void startDrainer() {
        // TODO: running.set(true)
        // TODO: drainerThread = new Thread(() -> {
        //     while (running.get()) {
        //         try {
        //             processResponses();
        //             if (responseQueue.isEmpty()) {
        //                 Thread.sleep(POLL_INTERVAL_MS);
        //             }
        //         } catch (InterruptedException e) {
        //             Thread.currentThread().interrupt();
        //             break;
        //         } catch (Exception e) {
        //             log.error("Error in HTTP response drainer", e);
        //         }
        //     }
        // }, "http-response-drainer-" + id);
        // drainerThread.setDaemon(true);
        // drainerThread.start();
        throw new UnsupportedOperationException("Not yet implemented");
    }

    /**
     * Shuts down the response-drainer thread and clears all state.
     */
    public void close() {
        // TODO: running.set(false)
        // TODO: if (drainerThread != null) {
        //     drainerThread.interrupt();
        //     try { drainerThread.join(5000); } catch (InterruptedException e) { /* ignored */ }
        // }
        // TODO: channels.clear()
        // TODO: responseQueue.clear()
        throw new UnsupportedOperationException("Not yet implemented");
    }

    /**
     * Returns the number of registered (active) channels.
     * Useful for monitoring and drain logic.
     */
    public int channelCount() {
        return channels.size();
    }

    /**
     * Returns the current response queue size.
     * Useful for monitoring.
     */
    public int responseQueueSize() {
        return responseQueue.size();
    }

    // --- Helper to extract request ID from request context ---
    private static String extractRequestId(RequestChannel.Request request) {
        // TODO: Extract X-Kafka-Request-ID from request context
        //       (stored by HttpRequestHandler when translating the request)
        //       Return a generated UUID if not present
        return "unknown";
    }

    // --- Helper to extract effective maxWaitMs ---
    private static int extractMaxWaitMs(RequestChannel.Request request) {
        // TODO: Extract from request attributes if FETCH request
        return -1;
    }
}
```

### Test class — `HttpProcessorTest.java`

```java
package kafka.server.http;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelId;
import io.netty.handler.codec.http.FullHttpResponse;
import kafka.network.RequestChannel;
import org.apache.kafka.common.requests.RequestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * // Time: Created - TASK-B.04
 */
class HttpProcessorTest {

    private HttpProcessor processor;

    @BeforeEach
    void setUp() {
        processor = new HttpProcessor(100);  // processor ID = 100
    }

    @AfterEach
    void tearDown() {
        processor.close();
    }

    @Test
    void id_returnsConfiguredId() {
        assertEquals(100, processor.id());
    }

    @Test
    void registerChannel_addsToMap() {
        // Arrange
        ChannelHandlerContext ctx = mockActiveCtx("conn-1");
        // Act
        processor.registerChannel("conn-1", ctx);
        // Assert
        assertEquals(1, processor.channelCount());
    }

    @Test
    void registerChannel_autoRemovesOnClose() {
        // TODO: Register channel, trigger close listener, verify channel removed
    }

    @Test
    void enqueueResponse_isNonBlocking() {
        // Arrange: create a mock SendResponse
        // Act: enqueueResponse() — should return immediately
        // Assert: responseQueueSize() == 1
    }

    @Test
    void processResponses_sendResponse_writesToChannel() {
        // Arrange: register channel, enqueue SendResponse
        // Act: processResponses()
        // Assert: ctx.writeAndFlush() was called
        // Assert: channel removed from map after response sent
    }

    @Test
    void processResponses_closeConnectionResponse_closesChannel() {
        // Arrange: register channel, enqueue CloseConnectionResponse
        // Act: processResponses()
        // Assert: ctx.close() was called
    }

    @Test
    void processResponses_startThrottling_sends429() {
        // Arrange: register channel, enqueue StartThrottlingResponse
        // Act: processResponses()
        // Assert: response written with status 429
        // Assert: Retry-After header is set
    }

    @Test
    void processResponses_endThrottling_isNoOp() {
        // Arrange: register channel, enqueue EndThrottlingResponse
        // Act: processResponses()
        // Assert: no writeAndFlush, no close
    }

    @Test
    void processResponses_channelAlreadyClosed_silentlyDropped() {
        // Arrange: register channel, mark channel as inactive, enqueue SendResponse
        // Act: processResponses()
        // Assert: no exception, no writeAndFlush
    }

    @Test
    void processResponses_exceptionInOneResponse_doesNotBlockOthers() {
        // Arrange: register two channels, enqueue bad response for first, good for second
        // Act: processResponses()
        // Assert: second response still delivered
    }

    @Test
    void startDrainer_processesQueuedResponses() throws InterruptedException {
        // Arrange: register channel, enqueue response
        // Act: startDrainer()
        // Assert: within 1 second, response is written to channel
    }

    @Test
    void close_stopsdrainerThread() throws InterruptedException {
        // Arrange: startDrainer()
        // Act: close()
        // Assert: drainer thread is no longer alive
    }

    // --- Mock helpers ---

    private ChannelHandlerContext mockActiveCtx(String connectionId) {
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        Channel channel = mock(Channel.class);
        ChannelId channelId = mock(ChannelId.class);
        ChannelFuture closeFuture = mock(ChannelFuture.class);

        when(ctx.channel()).thenReturn(channel);
        when(channel.isActive()).thenReturn(true);
        when(channel.id()).thenReturn(channelId);
        when(channelId.asLongText()).thenReturn(connectionId);
        when(channel.closeFuture()).thenReturn(closeFuture);
        when(ctx.writeAndFlush(any())).thenReturn(closeFuture);

        return ctx;
    }
}
```

### Existing pattern reference

```scala
// From SocketServer.scala lines 935-973 — the binary Processor response pattern:
private def processNewResponses(): Unit = {
    var currentResponse: RequestChannel.Response = null
    while ({currentResponse = dequeueResponse(); currentResponse != null}) {
      val channelId = currentResponse.request.context.connectionId
      try {
        currentResponse match {
          case response: NoOpResponse =>
            updateRequestMetrics(response)
            handleChannelMuteEvent(channelId, ChannelMuteEvent.RESPONSE_SENT)
            tryUnmuteChannel(channelId)
          case response: SendResponse =>
            sendResponse(response, response.responseSend)
          case response: CloseConnectionResponse =>
            updateRequestMetrics(response)
            close(channelId)
          case _: StartThrottlingResponse =>
            handleChannelMuteEvent(channelId, ChannelMuteEvent.THROTTLE_STARTED)
          case _: EndThrottlingResponse =>
            handleChannelMuteEvent(channelId, ChannelMuteEvent.THROTTLE_ENDED)
            tryUnmuteChannel(channelId)
        }
      } catch {
        case e: Throwable =>
          processChannelException(channelId, s"Exception while processing response for $channelId", e)
      }
    }
}
```

---

## Tests

**Test class:** `http-server/src/test/java/kafka/server/http/HttpProcessorTest.java`

| Test method | What it verifies |
|-------------|-----------------|
| `id_returnsConfiguredId` | Processor ID is correctly stored |
| `registerChannel_addsToMap` | Channel registration works |
| `registerChannel_autoRemovesOnClose` | Close listener cleanup |
| `enqueueResponse_isNonBlocking` | enqueueResponse returns immediately |
| `processResponses_sendResponse_writesToChannel` | SendResponse triggers writeAndFlush |
| `processResponses_closeConnectionResponse_closesChannel` | CloseConnectionResponse triggers ctx.close() |
| `processResponses_startThrottling_sends429` | StartThrottling -> HTTP 429 |
| `processResponses_endThrottling_isNoOp` | EndThrottling is silently ignored |
| `processResponses_channelAlreadyClosed_silentlyDropped` | Disconnected client doesn't cause errors |
| `processResponses_exceptionInOneResponse_doesNotBlockOthers` | Error isolation between responses |
| `startDrainer_processesQueuedResponses` | Drainer thread picks up and delivers responses |
| `close_stopsdrainerThread` | Clean shutdown of drainer thread |

**Run command:**
```bash
./gradlew :http-server:test --tests "kafka.server.http.HttpProcessorTest"
```

---

## Rules

- `enqueueResponse()` MUST be non-blocking. It is called from KafkaRequestHandler threads. Any blocking stalls the shared handler pool.
- After writing a `SendResponse` to the channel, the `connectionId` entry MUST be removed from the `channels` map.
- Exception handling in `processResponses()` MUST be per-response. One bad response must not prevent others from being processed.
- `StartThrottlingResponse` for HTTP converts to an immediate 429 response — do NOT attempt to mute/unmute the Netty channel.
- `EndThrottlingResponse` is a no-op for HTTP — do not log a warning, just ignore.
- The drainer thread MUST be a daemon thread so it doesn't prevent JVM shutdown.
- `close()` MUST be idempotent — calling it multiple times must not throw.

---

## Learning

1. **Scala-Java interop for response types:** The `RequestChannel` response classes (`SendResponse`, `CloseConnectionResponse`, etc.) are Scala case-like classes. From Java, Scala `val` fields become accessor methods (e.g., `response.request()`, `sendResp.responseSend()`). Scala `Option` is accessed via `scala.Option.apply(null)` for `None` and `logOpt.isDefined()` / `logOpt.get()` for pattern matching.

2. **Checkstyle import-control requires package-level configuration:** The default `import-control.xml` has root package `org.apache.kafka` and explicitly disallows `kafka.*`. The `http-server` module uses package `kafka.server.http` (matching core's convention), so it needed its own `import-control-http-server.xml` with root `kafka.server.http`.

3. **SendResponse carries a `Send` (NIO) not `AbstractResponse`:** The binary protocol's `SendResponse` wraps a `Send` object (for NIO channel writing), not the original `AbstractResponse`. For HTTP, we need to use the `responseLog` (Jackson `JsonNode`) as a fallback until `HttpResponseSerializer` is implemented in a future task. This is a design seam where the HTTP path will diverge from the binary path.

4. **`startDrainer()` idempotency via `compareAndSet`:** Using `running.compareAndSet(false, true)` prevents double-starting the drainer thread, which would create a duplicate polling thread. The skeleton code only used `running.set(true)` which is not idempotent.

---

## Limitations

1. **SendResponse serialization is preliminary:** The current implementation uses the `responseLog` JsonNode (if available) as the HTTP response body, or a minimal JSON fallback. Full Kafka-response-to-JSON serialization requires `HttpResponseSerializer` (future task). This means `SendResponse` output is not yet production-ready.

2. **No integration with `RequestChannel.addProcessor()`:** `HttpProcessor` cannot be registered with `RequestChannel.addProcessor()` because that method expects the Scala `Processor` class (from `SocketServer.scala`). Integration requires either a Processor trait/interface extraction or an adapter. This bridging is deferred to a future task.

3. **Drainer thread uses polling with sleep:** The drainer thread polls with a 10ms sleep between empty polls. A more efficient approach would use `responseQueue.take()` (blocking) or `LockSupport.park()`/`unpark()` signaling. The current approach trades slight latency overhead for simplicity.

---

## Field Notes

- The `http-server` module had no checkstyle import-control configuration. Created `checkstyle/import-control-http-server.xml` and configured it in `build.gradle` under `project(':http-server')`.
- All 22 tests pass: covers ID, channel registration, auto-removal on close, null rejection, all 4 response types (Send, CloseConnection, StartThrottling, EndThrottling), inactive channel handling, unregistered channel handling, error isolation, drainer thread lifecycle, close idempotency, and monitoring counters.
- The `HttpResponseStatus.valueOf(429, "Too Many Requests")` approach was used because Netty's `HttpResponseStatus` does not have a pre-defined constant for 429.

---

## Acceptance Criteria

- [x] `./gradlew :http-server:test --tests "kafka.server.http.HttpProcessorTest"` exits 0
- [x] `HttpProcessor.java` exists at `http-server/src/main/java/kafka/server/http/HttpProcessor.java`
- [x] `registerChannel()` adds entry and installs close listener for auto-removal
- [x] `enqueueResponse()` is non-blocking (uses `LinkedBlockingDeque.add()`)
- [x] `processResponses()` handles all four response types: Send, CloseConnection, StartThrottling, EndThrottling
- [x] `StartThrottlingResponse` produces HTTP 429 with `Retry-After` header
- [x] `EndThrottlingResponse` is a no-op
- [x] Exception in one response does not prevent processing of subsequent responses
- [x] Channel is removed from map after `SendResponse` is written
- [x] Drainer thread is a daemon thread
- [x] `close()` is idempotent
- [x] Learning section filled with at least one entry
- [x] Limitations section filled (use "None" if truly none)
- [x] File Manifest section updated after commit

---

## File Manifest

### 2026-04-16 — HttpProcessor response routing bridge (commit 16276d18e5)
Created:
  - http-server/src/main/java/kafka/server/http/HttpProcessor.java — Response routing bridge between RequestChannel and Netty
  - http-server/src/test/java/kafka/server/http/HttpProcessorTest.java — 22 unit tests for HttpProcessor
  - checkstyle/import-control-http-server.xml — Checkstyle import control for kafka.server.http package
Modified:
  - build.gradle — Added checkstyle configuration for http-server module
