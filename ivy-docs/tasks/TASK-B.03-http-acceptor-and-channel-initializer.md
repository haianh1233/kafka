# TASK-B.03: HttpAcceptor + HttpChannelInitializer

## Prerequisites

- **TASK-A.01** (SecurityProtocol.HTTP/HTTPS) — Adds `HTTP(4)` and `HTTPS(5)` to the `SecurityProtocol` enum. `HttpChannelInitializer` checks `securityProtocol == HTTPS` to decide whether to add an `SslHandler` to the pipeline.
- **TASK-A.02** (http-server module, HttpServerConfigs) — Creates the `http-server` Gradle submodule with Netty dependencies. Also provides `HttpServerConfigs` with config constants: `numHttpNetworkThreads` (default 4), `httpRequestMaxBytes` (default 10 MB), `httpConnectionIdleTimeoutMs` (default 60000 ms).

---

## Context

The HTTP protocol layer needs a Netty-based server to accept HTTP connections, decode HTTP frames, and pass them to the Kafka request pipeline. This task implements two classes that form the server-side network layer:

1. **`HttpAcceptor`** — manages the Netty `ServerBootstrap` lifecycle. It is the HTTP equivalent of the existing `DataPlaneAcceptor` (which uses Java NIO `Selector` for the binary protocol). `HttpAcceptor` owns the Netty boss group (1 thread, accepts TCP connections) and worker group (`num.http.network.threads` threads, handles HTTP frame I/O). It provides lifecycle methods: `startup()`, `beginDrain()`, `awaitDrain()`, and `close()`.

2. **`HttpChannelInitializer`** — configures the Netty channel pipeline for each new connection. The pipeline stages are:
   - Optional `SslHandler` (for HTTPS listeners)
   - `HttpServerCodec` — HTTP/1.1 request/response codec
   - `HttpObjectAggregator` — aggregates chunked requests into `FullHttpRequest` (bounded by `http.request.max.bytes`)
   - `HttpContentCompressor` — gzip/deflate response compression
   - `IdleStateHandler` — detects idle connections (§14.11)
   - `IdleStateCloseHandler` — closes connections on idle timeout
   - `HttpRequestHandler` — placeholder, wired in TASK-B.05

The design document (sections 8.2-8.4, 14.5, 14.8, 14.11) specifies the exact pipeline, shutdown drain logic, and idle connection management.

The existing `DataPlaneAcceptor` pattern from `SocketServer.scala` is the reference for lifecycle management. However, `HttpAcceptor` uses Netty instead of Java NIO, so the implementation differs significantly in the actual I/O mechanics.

---

## Specification

### `HttpAcceptor` — `kafka.network.HttpAcceptor`

```scala
/**
 * Netty-based HTTP server acceptor. Manages ServerBootstrap lifecycle.
 * The HTTP equivalent of DataPlaneAcceptor for binary protocol listeners.
 *
 * Lifecycle:
 *   startup()    — binds to the endpoint, starts accepting connections
 *   beginDrain() — stops accepting new requests (in-flight continue)
 *   awaitDrain()  — blocks until pending connections drain or timeout
 *   close()      — shuts down Netty event loop groups and releases resources
 */
class HttpAcceptor(
    socketServer: SocketServer,
    endpoint: Endpoint,
    config: KafkaConfig,
    requestChannel: RequestChannel,
    httpProcessor: HttpProcessor,
    time: Time
) extends Closeable {

    def startup(): Unit
    def beginDrain(): Unit
    def awaitDrain(timeoutMs: Long): Unit
    override def close(): Unit

    // Exposed for SocketServer to track
    val startedFuture: CompletableFuture[Void]
}
```

### `HttpChannelInitializer` — `kafka.network.HttpChannelInitializer`

```scala
/**
 * Configures the Netty pipeline for each new HTTP connection.
 *
 * Pipeline order:
 *   [ssl]          — SslHandler (HTTPS only)
 *   http-codec     — HttpServerCodec
 *   http-aggregator — HttpObjectAggregator(httpRequestMaxBytes)
 *   compressor     — HttpContentCompressor
 *   idle-handler   — IdleStateHandler(0, 0, idleTimeoutMs, MILLISECONDS)
 *   idle-closer    — custom handler that closes on IdleStateEvent
 *   kafka-handler  — HttpRequestHandler (placeholder — wired by TASK-B.05)
 */
class HttpChannelInitializer(
    endpoint: Endpoint,
    config: KafkaConfig,
    requestChannel: RequestChannel,
    httpProcessor: HttpProcessor,
    sslContext: Option[SslContext]
) extends ChannelInitializer[SocketChannel] {

    override def initChannel(ch: SocketChannel): Unit
}
```

### `IdleStateCloseHandler` — inner class or standalone

```scala
/**
 * Closes the channel when an IdleStateEvent is triggered.
 * Simple duplex handler that listens for userEventTriggered.
 */
class IdleStateCloseHandler extends ChannelDuplexHandler {
    override def userEventTriggered(ctx: ChannelHandlerContext, evt: Any): Unit
}
```

### Behavioral contracts

- `HttpAcceptor.startup()` binds the server socket and starts both event loop groups. If bind fails, `startedFuture` completes exceptionally.
- `HttpAcceptor.beginDrain()` sets an `accepting` flag to false. New HTTP requests arriving after this point should be rejected with 503 by the handler (the handler checks this flag).
- `HttpAcceptor.awaitDrain(timeoutMs)` blocks until all pending in-flight connections have completed or `timeoutMs` elapses.
- `HttpAcceptor.close()` shuts down Netty boss and worker groups with graceful timeout, closes the server channel.
- `HttpChannelInitializer.initChannel()` adds pipeline handlers in the exact order specified. For HTTPS, the `SslHandler` is first. The aggregator limit is `config.httpRequestMaxBytes`.
- `IdleStateCloseHandler` closes the channel on any `IdleStateEvent` — this reclaims keep-alive connections that have been idle longer than `http.connection.idle.timeout.ms`.

---

## Implementation Details

**Module:** `http-server`

**Files to study:**

| File | Why |
|------|-----|
| `core/src/main/scala/kafka/network/SocketServer.scala` lines 362-420 | `DataPlaneAcceptor` class — lifecycle pattern (startup, beginShutdown, close) |
| `core/src/main/scala/kafka/network/SocketServer.scala` lines 215-228 | `createDataPlaneAcceptorAndProcessors` — how acceptors are created and registered |
| `core/src/main/scala/kafka/network/SocketServer.scala` lines 239-248 | `stopProcessingRequests` — shutdown sequence with drain |

```scala
// From SocketServer.scala lines 362-377 — DataPlaneAcceptor constructor pattern:
class DataPlaneAcceptor(socketServer: SocketServer,
                        endPoint: Endpoint,
                        config: KafkaConfig,
                        nodeId: Int,
                        connectionQuotas: ConnectionQuotas,
                        time: Time,
                        isPrivilegedListener: Boolean,
                        requestChannel: RequestChannel,
                        metrics: Metrics,
                        credentialProvider: CredentialProvider,
                        logContext: LogContext,
                        memoryPool: MemoryPool,
                        apiVersionManager: ApiVersionManager)
  extends Acceptor(socketServer, endPoint, config, ...) {
    // ... startup(), beginShutdown(), close()
}
```

```scala
// From SocketServer.scala lines 239-248 — shutdown pattern:
def stopProcessingRequests(): Unit = synchronized {
    if (!stopped) {
      stopped = true
      info("Stopping socket server request processors")
      dataPlaneAcceptors.asScala.values.foreach(_.beginShutdown())
      dataPlaneAcceptors.asScala.values.foreach(_.close())
      dataPlaneRequestChannel.clear()
      info("Stopped socket server request processors")
    }
}
```

**Files to create:**

| File | What it does |
|------|--------------|
| `http-server/src/main/scala/kafka/network/HttpAcceptor.scala` | Netty ServerBootstrap lifecycle management |
| `http-server/src/main/scala/kafka/network/HttpChannelInitializer.scala` | Netty pipeline configuration per connection |

> **CRITICAL:** The `HttpObjectAggregator` automatically returns HTTP 413 and closes the connection when a request exceeds `maxContentLength`. This is desired behavior — no handler code needed for oversized requests.

> **CRITICAL:** Boss group MUST use exactly 1 thread. Worker group uses `num.http.network.threads` (default 4). Using more boss threads wastes resources since there is only one server socket.

> **CRITICAL:** `shutdownGracefully()` on Netty event loop groups takes a quiet period and timeout. Use reasonable values (quiet=100ms, timeout=500ms for workers; quiet=100ms, timeout=200ms for boss) to avoid hanging during shutdown.

> **CRITICAL:** `IdleStateHandler` third parameter is `allIdleTimeSeconds` — but we pass milliseconds using the `TimeUnit` parameter overload. Do NOT use the seconds-only constructor.

**Implementation order:**
1. Implement `IdleStateCloseHandler` (simplest — standalone handler)
2. Implement `HttpChannelInitializer` (pipeline assembly)
3. Implement `HttpAcceptor` (lifecycle management with Netty bootstrap)
4. Add `startedFuture` that completes when bind succeeds

---

## Skeleton Code

### `HttpAcceptor.scala`

```scala
package kafka.network

import java.io.Closeable
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.{Channel, ChannelOption, EventLoopGroup}
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.handler.ssl.SslContext
import kafka.server.http.HttpProcessor
import kafka.utils.Logging
import org.apache.kafka.common.Endpoint
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.Time
import org.apache.kafka.server.config.KafkaConfig

/**
 * Netty-based HTTP server acceptor. Manages the ServerBootstrap lifecycle.
 *
 * This is the HTTP equivalent of [[DataPlaneAcceptor]] for binary protocol listeners.
 * Uses Netty NIO event loop groups instead of Java NIO Selector.
 *
 * Thread model:
 *   - Boss group: 1 thread, accepts TCP connections
 *   - Worker group: num.http.network.threads (default 4), handles HTTP I/O
 *
 * Lifecycle:
 *   startup()     -> binds to endpoint, starts accepting connections
 *   beginDrain()  -> stops accepting new requests, in-flight requests continue
 *   awaitDrain()  -> blocks until pending requests drain or timeout
 *   close()       -> shuts down Netty event loop groups
 *
 * // Time: Created - TASK-B.03
 */
class HttpAcceptor(
    socketServer: SocketServer,
    endpoint: Endpoint,
    config: KafkaConfig,
    requestChannel: RequestChannel,
    httpProcessor: HttpProcessor,
    time: Time
) extends Closeable with Logging {

  // --- Netty event loop groups ---
  private val bossGroup: EventLoopGroup = new NioEventLoopGroup(1)

  // num.http.network.threads — default 4
  // TODO: Read from config: config.getInt(HttpServerConfigs.NUM_HTTP_NETWORK_THREADS_CONFIG)
  private val numWorkerThreads: Int = 4
  private val workerGroup: EventLoopGroup = new NioEventLoopGroup(numWorkerThreads)

  // --- Server channel (bound socket) ---
  @volatile private var serverChannel: Channel = _

  // --- Lifecycle state ---
  private val accepting = new AtomicBoolean(true)

  // Tracks in-flight connections that have pending requests
  private val pendingConnectionCount = new AtomicInteger(0)

  // Completes when startup succeeds (or fails)
  val startedFuture: CompletableFuture[Void] = new CompletableFuture[Void]()

  // --- SSL context (for HTTPS) ---
  // TODO: Build SslContext from config if endpoint.securityProtocol == HTTPS
  private val sslContext: Option[SslContext] = {
    // TODO: If endpoint.securityProtocol() == SecurityProtocol.HTTPS:
    //         Build SslContext using SslContextBuilder.forServer(keyCertChainFile, keyFile)
    //         with config-specified keystore/truststore
    //       Else: None
    None
  }

  /**
   * Binds the Netty server to the endpoint and starts accepting connections.
   * Registers the HttpProcessor with RequestChannel.
   */
  def startup(): Unit = {
    // TODO: 1. Register httpProcessor with requestChannel:
    //          requestChannel.addProcessor(httpProcessor)
    //
    // TODO: 2. Build ServerBootstrap:
    //          val bootstrap = new ServerBootstrap()
    //          bootstrap.group(bossGroup, workerGroup)
    //            .channel(classOf[NioServerSocketChannel])
    //            .option(ChannelOption.SO_BACKLOG, Int.box(128))
    //            .childOption(ChannelOption.SO_KEEPALIVE, Boolean.box(true))
    //            .childHandler(new HttpChannelInitializer(
    //              endpoint, config, requestChannel, httpProcessor, sslContext))
    //
    // TODO: 3. Bind to endpoint:
    //          val bindFuture = bootstrap.bind(endpoint.host(), endpoint.port())
    //          bindFuture.addListener { future =>
    //            if (future.isSuccess) {
    //              serverChannel = bindFuture.channel()
    //              startedFuture.complete(null)
    //              info(s"HTTP acceptor started on ${endpoint.host()}:${endpoint.port()}")
    //            } else {
    //              startedFuture.completeExceptionally(future.cause())
    //            }
    //          }
    throw new UnsupportedOperationException("Not yet implemented")
  }

  /**
   * Signals that the acceptor should stop accepting new requests.
   * In-flight requests that are already in the pipeline continue to completion.
   * New requests arriving after this call are rejected with HTTP 503.
   */
  def beginDrain(): Unit = {
    // TODO: accepting.set(false)
    // TODO: The HttpRequestHandler checks this flag and rejects new requests
    throw new UnsupportedOperationException("Not yet implemented")
  }

  /**
   * Blocks until all pending in-flight connections have drained or timeout elapses.
   *
   * @param timeoutMs maximum time to wait for drain
   */
  def awaitDrain(timeoutMs: Long): Unit = {
    // TODO: val deadline = time.milliseconds() + timeoutMs
    // TODO: while (pendingConnectionCount.get() > 0 && time.milliseconds() < deadline)
    //         Thread.sleep(20)
    throw new UnsupportedOperationException("Not yet implemented")
  }

  /**
   * Shuts down the Netty server. Closes the server channel, then gracefully
   * shuts down worker and boss event loop groups.
   */
  override def close(): Unit = {
    // TODO: 1. Close server channel:
    //          if (serverChannel != null) serverChannel.close().sync()
    //
    // TODO: 2. Shutdown worker group gracefully:
    //          workerGroup.shutdownGracefully(100, 500, java.util.concurrent.TimeUnit.MILLISECONDS).sync()
    //
    // TODO: 3. Shutdown boss group gracefully:
    //          bossGroup.shutdownGracefully(100, 200, java.util.concurrent.TimeUnit.MILLISECONDS).sync()
    //
    // TODO: 4. Log shutdown
    throw new UnsupportedOperationException("Not yet implemented")
  }

  /** Exposed for HttpRequestHandler to check if new requests should be accepted. */
  def isAccepting: Boolean = accepting.get()

  /** Increment pending connection count (called by HttpRequestHandler on request start). */
  def incrementPending(): Unit = pendingConnectionCount.incrementAndGet()

  /** Decrement pending connection count (called by HttpRequestHandler on request complete). */
  def decrementPending(): Unit = pendingConnectionCount.decrementAndGet()
}
```

### `HttpChannelInitializer.scala`

```scala
package kafka.network

import java.util.concurrent.TimeUnit

import io.netty.channel.{ChannelDuplexHandler, ChannelHandlerContext, ChannelInitializer}
import io.netty.channel.socket.SocketChannel
import io.netty.handler.codec.http.{HttpContentCompressor, HttpObjectAggregator, HttpServerCodec}
import io.netty.handler.ssl.SslContext
import io.netty.handler.timeout.{IdleStateEvent, IdleStateHandler}
import kafka.server.http.HttpProcessor
import kafka.utils.Logging
import org.apache.kafka.common.Endpoint
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.server.config.KafkaConfig

/**
 * Configures the Netty pipeline for each new HTTP connection.
 *
 * Pipeline order:
 *   [ssl]           — SslHandler (HTTPS only)
 *   http-codec      — HttpServerCodec (HTTP/1.1 request/response encoding/decoding)
 *   http-aggregator — HttpObjectAggregator (aggregate chunked -> FullHttpRequest)
 *   compressor      — HttpContentCompressor (gzip/deflate response compression)
 *   idle-handler    — IdleStateHandler (detect idle connections)
 *   idle-closer     — IdleStateCloseHandler (close on idle timeout)
 *   kafka-handler   — HttpRequestHandler (translate HTTP -> Kafka request)
 *
 * The HttpObjectAggregator is bounded by http.request.max.bytes (default 10 MB).
 * Requests exceeding this limit receive automatic HTTP 413 from Netty.
 *
 * // Time: Created - TASK-B.03
 */
class HttpChannelInitializer(
    endpoint: Endpoint,
    config: KafkaConfig,
    requestChannel: RequestChannel,
    httpProcessor: HttpProcessor,
    sslContext: Option[SslContext]
) extends ChannelInitializer[SocketChannel] with Logging {

  // --- Config values ---
  // TODO: Read from config object:
  // private val httpRequestMaxBytes: Int = config.getInt(HttpServerConfigs.HTTP_REQUEST_MAX_BYTES_CONFIG)
  // private val httpConnectionIdleTimeoutMs: Long = config.getLong(HttpServerConfigs.HTTP_CONNECTION_IDLE_TIMEOUT_MS_CONFIG)
  private val httpRequestMaxBytes: Int = 10_485_760       // 10 MB default
  private val httpConnectionIdleTimeoutMs: Long = 60_000  // 60 s default

  override def initChannel(ch: SocketChannel): Unit = {
    val pipeline = ch.pipeline()

    // TODO: 1. Optional SSL (for HTTPS listeners)
    //       sslContext.foreach { ctx =>
    //         pipeline.addLast("ssl", ctx.newHandler(ch.alloc()))
    //       }

    // TODO: 2. HTTP/1.1 codec — decodes HTTP frames, encodes responses
    //       pipeline.addLast("http-codec", new HttpServerCodec())

    // TODO: 3. Aggregate chunked requests into FullHttpRequest
    //       Netty returns 413 automatically if body exceeds limit
    //       pipeline.addLast("http-aggregator", new HttpObjectAggregator(httpRequestMaxBytes))

    // TODO: 4. Response compression (gzip/deflate based on Accept-Encoding)
    //       pipeline.addLast("compressor", new HttpContentCompressor())

    // TODO: 5. Idle connection detection (§14.11)
    //       Uses allIdle timeout — fires if no read OR write for the configured duration
    //       IMPORTANT: use the TimeUnit overload, not the seconds-only constructor
    //       pipeline.addLast("idle-handler",
    //         new IdleStateHandler(0, 0, httpConnectionIdleTimeoutMs, TimeUnit.MILLISECONDS))

    // TODO: 6. Close channel on idle timeout
    //       pipeline.addLast("idle-closer", new IdleStateCloseHandler())

    // TODO: 7. Kafka HTTP request handler (placeholder — fully wired by TASK-B.05)
    //       pipeline.addLast("kafka-handler",
    //         new HttpRequestHandler(requestChannel, httpProcessor, config, endpoint))

    throw new UnsupportedOperationException("Not yet implemented")
  }
}

/**
 * Closes the channel when Netty's IdleStateHandler fires an IdleStateEvent.
 * This reclaims keep-alive HTTP connections that have been idle longer than
 * http.connection.idle.timeout.ms (default 60 seconds).
 *
 * // Time: Created - TASK-B.03
 */
class IdleStateCloseHandler extends ChannelDuplexHandler with Logging {

  override def userEventTriggered(ctx: ChannelHandlerContext, evt: Any): Unit = {
    evt match {
      case _: IdleStateEvent =>
        // TODO: Log at debug level and close the channel
        // debug(s"Closing idle HTTP connection: ${ctx.channel().remoteAddress()}")
        // ctx.close()
        throw new UnsupportedOperationException("Not yet implemented")
      case _ =>
        super.userEventTriggered(ctx, evt)
    }
  }
}
```

### Test class — `HttpAcceptorTest.scala`

```scala
package kafka.network

import java.io.IOException
import java.net.{HttpURLConnection, URL}
import java.util.concurrent.{CompletableFuture, TimeUnit}

import kafka.server.http.HttpProcessor
import org.apache.kafka.common.Endpoint
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.MockTime
import org.apache.kafka.server.config.KafkaConfig
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.api.Assertions._
import org.mockito.Mockito._

/**
 * // Time: Created - TASK-B.03
 */
class HttpAcceptorTest {

  private var acceptor: HttpAcceptor = _
  private var config: KafkaConfig = _
  private var requestChannel: RequestChannel = _
  private var httpProcessor: HttpProcessor = _
  private val time = new MockTime()

  @BeforeEach
  def setUp(): Unit = {
    // TODO: Create mock config with defaults
    // TODO: Create mock RequestChannel
    // TODO: Create mock HttpProcessor
    // TODO: Create Endpoint on a free port (0 for auto-assign)
  }

  @AfterEach
  def tearDown(): Unit = {
    if (acceptor != null) acceptor.close()
  }

  @Test
  def startup_bindsToPort(): Unit = {
    // Arrange: create acceptor with port 0 (auto-assign)
    // Act: acceptor.startup()
    // Assert: startedFuture completes without exception
    // Assert: can open a TCP connection to the bound port
  }

  @Test
  def startup_completesStartedFuture(): Unit = {
    // Arrange / Act
    // Assert: startedFuture.get(5, SECONDS) does not throw
  }

  @Test
  def close_releasesPort(): Unit = {
    // Arrange: startup, get bound port
    // Act: close
    // Assert: TCP connection to port is refused
  }

  @Test
  def beginDrain_setsAcceptingFalse(): Unit = {
    // Arrange: startup
    // Act: beginDrain
    // Assert: isAccepting == false
  }

  @Test
  def awaitDrain_returnsImmediately_whenNoPending(): Unit = {
    // Arrange: startup, no pending connections
    // Act: awaitDrain(1000)
    // Assert: returns in < 100ms
  }

  @Test
  def pendingCount_incrementAndDecrement(): Unit = {
    // Arrange: startup
    // Act: incrementPending() x3, decrementPending() x1
    // Assert: awaitDrain would wait (pendingCount = 2)
  }
}
```

### Test class — `HttpChannelInitializerTest.scala`

```scala
package kafka.network

import io.netty.channel.embedded.EmbeddedChannel
import io.netty.channel.socket.SocketChannel
import io.netty.handler.codec.http.{HttpObjectAggregator, HttpServerCodec}
import io.netty.handler.timeout.IdleStateHandler
import kafka.server.http.HttpProcessor
import org.apache.kafka.common.Endpoint
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.server.config.KafkaConfig
import org.junit.jupiter.api.{BeforeEach, Test}
import org.junit.jupiter.api.Assertions._

/**
 * // Time: Created - TASK-B.03
 */
class HttpChannelInitializerTest {

  // TODO: set up mock config, requestChannel, httpProcessor, endpoint

  @Test
  def initChannel_addsHttpCodecFirst(): Unit = {
    // Assert pipeline contains "http-codec" handler of type HttpServerCodec
  }

  @Test
  def initChannel_addsAggregatorWithCorrectLimit(): Unit = {
    // Assert pipeline contains "http-aggregator" of type HttpObjectAggregator
    // Assert maxContentLength matches config
  }

  @Test
  def initChannel_addsIdleHandler(): Unit = {
    // Assert pipeline contains "idle-handler" of type IdleStateHandler
  }

  @Test
  def initChannel_addsIdleCloser(): Unit = {
    // Assert pipeline contains "idle-closer" of type IdleStateCloseHandler
  }

  @Test
  def initChannel_httpsEndpoint_addsSslHandler(): Unit = {
    // Assert pipeline contains "ssl" handler when endpoint is HTTPS
  }

  @Test
  def initChannel_httpEndpoint_noSslHandler(): Unit = {
    // Assert pipeline does NOT contain "ssl" handler when endpoint is HTTP
  }
}
```

### Existing pattern reference

```scala
// From SocketServer.scala lines 215-228 — how DataPlaneAcceptor is created:
private def createDataPlaneAcceptorAndProcessors(endpoint: Endpoint): Unit = synchronized {
    if (stopped) {
      throw new RuntimeException("Can't create new data plane acceptor and processors: SocketServer is stopped.")
    }
    val listenerName =  ListenerName.normalised(endpoint.listener)
    val parsedConfigs = config.valuesFromThisConfigWithPrefixOverride(listenerName.configPrefix)
    connectionQuotas.addListener(config, listenerName)
    val isPrivilegedListener = config.interBrokerListenerName == listenerName
    val dataPlaneAcceptor = createDataPlaneAcceptor(endpoint, isPrivilegedListener, dataPlaneRequestChannel)
    config.addReconfigurable(dataPlaneAcceptor)
    dataPlaneAcceptor.configure(parsedConfigs)
    dataPlaneAcceptors.put(endpoint, dataPlaneAcceptor)
    info(s"Created data-plane acceptor and processors for endpoint : ${listenerName}")
}
```

---

## Tests

**Test class:** `http-server/src/test/scala/kafka/network/HttpAcceptorTest.scala`

| Test method | What it verifies |
|-------------|-----------------|
| `startup_bindsToPort` | Server socket binds and accepts TCP connections |
| `startup_completesStartedFuture` | CompletableFuture resolves on successful bind |
| `close_releasesPort` | Port is freed after close |
| `beginDrain_setsAcceptingFalse` | Drain flag toggled |
| `awaitDrain_returnsImmediately_whenNoPending` | No hanging when no pending connections |
| `pendingCount_incrementAndDecrement` | Connection tracking works correctly |

**Test class:** `http-server/src/test/scala/kafka/network/HttpChannelInitializerTest.scala`

| Test method | What it verifies |
|-------------|-----------------|
| `initChannel_addsHttpCodecFirst` | Pipeline has HttpServerCodec |
| `initChannel_addsAggregatorWithCorrectLimit` | Aggregator respects config limit |
| `initChannel_addsIdleHandler` | Pipeline has IdleStateHandler |
| `initChannel_addsIdleCloser` | Pipeline has IdleStateCloseHandler |
| `initChannel_httpsEndpoint_addsSslHandler` | HTTPS adds SslHandler |
| `initChannel_httpEndpoint_noSslHandler` | HTTP omits SslHandler |

**Run command:**
```bash
./gradlew :http-server:test --tests "kafka.network.HttpAcceptorTest" --tests "kafka.network.HttpChannelInitializerTest"
```

---

## Rules

- Boss group MUST use exactly 1 thread. Worker group uses `num.http.network.threads` (default 4).
- `HttpObjectAggregator` limit comes from `http.request.max.bytes` config (default 10 MB). Requests exceeding this get automatic Netty 413.
- `IdleStateHandler` MUST use the `TimeUnit` overload, not the seconds-only constructor, since the config is in milliseconds.
- `shutdownGracefully()` must specify a quiet period and timeout to avoid indefinite shutdown hangs.
- The `HttpRequestHandler` added to the pipeline in this task is a placeholder. TASK-B.05 provides the full implementation.
- `close()` must be idempotent — calling it multiple times must not throw.

---

## Learning

1. **Netty ChannelInitializer type parameter**: Using `ChannelInitializer[Channel]` instead of `ChannelInitializer[SocketChannel]` allows testing with `EmbeddedChannel`, which does not extend `SocketChannel`. This is a pragmatic trade-off that enables testability without sacrificing production functionality.

2. **SelfSignedCertificate JDK compatibility**: Netty's `SelfSignedCertificate` class does not support JDK 26 early access. Using BouncyCastle's `CertificateBuilder` (via Kafka's `TestSslUtils`) provides JDK-independent certificate generation for SSL tests.

3. **Netty IdleStateHandler TimeUnit overload**: The default `IdleStateHandler` constructor takes seconds. When the config value is in milliseconds (as with `http.connection.idle.timeout.ms`), the four-argument overload with explicit `TimeUnit.MILLISECONDS` must be used to avoid silent conversion bugs.

4. **Netty shutdown graceful period**: `shutdownGracefully(quietPeriod, timeout, unit)` requires both parameters. The quiet period (100ms) prevents immediate shutdown if new events arrive, while the timeout (200-500ms) ensures the shutdown doesn't hang indefinitely.

---

## Limitations

1. **SSL context not wired**: The `sslContext` field in `HttpAcceptor` is always `None`. Actual HTTPS support requires building an `SslContext` from the broker's keystore/truststore configuration, which is deferred to a future task.

2. **Concurrent branch modifications**: The `feature/http-protocol` branch has multiple tasks being executed concurrently. Some test files from later tasks (B.05, F.05, share-group tasks) reference APIs that changed between commits, causing compilation failures in the full test suite. Only the targeted `HttpAcceptorTest` and `HttpChannelInitializerTest` are guaranteed to pass.

---

## Field Notes

- The first `project(':http-server')` block in `build.gradle` referenced undefined Netty library aliases (`nettyCodecHttp2`, `nettyBuffer`, `nettyCommon`) causing null dependency errors. Fixed by using `nettyAll` which bundles all Netty modules.
- The `HttpChannelInitializer` was later extended with CORS support (TASK-F.04) adding `corsAllowedOrigins` parameter and `CorsHandler` to the pipeline between `http-codec` and `http-aggregator`.
- `HttpAcceptor` needed a no-arg constructor and `setAccepting` method to support `HttpRequestHandlerTest` from TASK-B.05.
- Pre-existing issues on the branch: `KafkaApisBuilder.java` missing `httpAsyncExecutor` parameter, `DefaultKafkaPrincipalBuilderTest.java` using old `HttpAuthenticationContext` constructor, `KafkaApis.scala` importing from `kafka.server.http` without proper dependency.

---

## Acceptance Criteria

- [x] `./gradlew :http-server:test --tests "kafka.network.HttpAcceptorTest"` exits 0
- [x] `./gradlew :http-server:test --tests "kafka.network.HttpChannelInitializerTest"` exits 0
- [x] `HttpAcceptor.scala` exists at `http-server/src/main/scala/kafka/network/HttpAcceptor.scala`
- [x] `HttpChannelInitializer.scala` exists at `http-server/src/main/scala/kafka/network/HttpChannelInitializer.scala`
- [x] Pipeline order is: [ssl], http-codec, [cors], http-aggregator, compressor, idle-handler, idle-closer
- [x] Boss group uses 1 thread, worker group uses `num.http.network.threads`
- [x] `HttpObjectAggregator` limit matches `http.request.max.bytes` config
- [x] `IdleStateHandler` uses millisecond TimeUnit overload
- [x] `HttpAcceptor.close()` shuts down both event loop groups gracefully
- [x] `startedFuture` completes on successful bind
- [x] Learning section filled with at least one entry
- [x] Limitations section filled (use "None" if truly none)
- [x] File Manifest section updated after commit

---

## File Manifest

### 2026-04-16 -- HttpAcceptor + HttpChannelInitializer implementation
Created:
  - http-server/src/main/scala/kafka/network/HttpAcceptor.scala -- Netty server lifecycle (boss/worker groups, bind, drain, close)
  - http-server/src/main/scala/kafka/network/HttpChannelInitializer.scala -- Netty pipeline config (SSL, codec, aggregator, compressor, idle, CORS)
  - http-server/src/test/scala/kafka/network/HttpAcceptorTest.scala -- Acceptor lifecycle tests (5 tests)
  - http-server/src/test/scala/kafka/network/HttpChannelInitializerTest.scala -- Pipeline configuration tests (13 tests)
  - checkstyle/import-control-http-server.xml -- Import control for http-server module
Modified:
  - build.gradle -- Fixed first http-server project block deps (nettyAll instead of undefined individual modules)
  - clients/src/main/java/org/apache/kafka/common/security/auth/SecurityProtocol.java -- Added HTTP(4), HTTPS(5)
  - gradle/dependencies.gradle -- Added Netty version and library aliases
  - settings.gradle -- Added http-server to included projects
