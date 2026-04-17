/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.network

import java.io.Closeable
import java.util.concurrent.{CompletableFuture, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.{Channel, ChannelOption, EventLoopGroup}
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.handler.ssl.SslContext
import kafka.server.http.HttpProcessor
import kafka.server.http.rest.{BindingRestHandler, ConnectionRestHandler, ConsumerRestHandler, ExchangeRestHandler, MessageRestHandler, QueueRestHandler, VhostRestHandler}
import kafka.server.http.routing.{BindingManager, ExchangeManager, RoutingEngine, VhostManager}
import kafka.server.http.ws.{WsConfigs, WsConnectionRegistry, WsMessageSerializer, WsRoutingMetadataManager}
import kafka.utils.Logging
import org.apache.kafka.common.Endpoint
import org.apache.kafka.common.utils.Time

/**
 * Netty-based HTTP server acceptor. Manages the ServerBootstrap lifecycle.
 *
 * This is the HTTP equivalent of [[kafka.network.SocketServer.DataPlaneAcceptor]]
 * for binary protocol listeners. Uses Netty NIO event loop groups instead of
 * Java NIO Selector.
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
 * // Time: Modified - TASK-B.06 (request pipeline wiring)
 */
class HttpAcceptor(
    val endpoint: Endpoint,
    numWorkerThreads: Int,
    httpRequestMaxBytes: Int,
    httpConnectionIdleTimeoutMs: Long,
    time: Time,
    corsAllowedOrigins: String = "",
    brokerId: Int = -1,
    clusterId: String = ""
) extends Closeable with HttpAcceptorLike with Logging {

  /**
   * No-arg constructor for use in tests where only accepting/pending state tracking is needed
   * (e.g., HttpRequestHandler tests that pass a standalone HttpAcceptor).
   */
  def this() = this(
    new Endpoint("HTTP", org.apache.kafka.common.security.auth.SecurityProtocol.HTTP, "localhost", 0),
    org.apache.kafka.network.HttpServerConfigs.NUM_HTTP_NETWORK_THREADS_DEFAULT,
    org.apache.kafka.network.HttpServerConfigs.HTTP_REQUEST_MAX_BYTES_DEFAULT,
    org.apache.kafka.network.HttpServerConfigs.HTTP_CONNECTION_IDLE_TIMEOUT_MS_DEFAULT,
    Time.SYSTEM
  )

  // --- Netty event loop groups ---
  // Boss group MUST use exactly 1 thread (one server socket)
  private val bossGroup: EventLoopGroup = new NioEventLoopGroup(1)

  // Worker group: num.http.network.threads (default 4)
  private val workerGroup: EventLoopGroup = new NioEventLoopGroup(numWorkerThreads)

  // --- Server channel (bound socket) ---
  @volatile private var serverChannel: Channel = _

  // --- Lifecycle state ---
  private val accepting = new AtomicBoolean(true)
  private val _draining = new AtomicBoolean(false)
  private val closed = new AtomicBoolean(false)

  // Tracks in-flight connections that have pending requests
  private val pendingConnectionCount = new AtomicInteger(0)

  // Completes when startup succeeds (or fails)
  val startedFuture: CompletableFuture[Void] = new CompletableFuture[Void]()

  // --- SSL context (for HTTPS) ---
  // TODO: When endpoint.securityProtocol() == SecurityProtocol.HTTPS, build SslContext
  //       using SslContextBuilder.forServer(keyCertChainFile, keyFile) with config-specified
  //       keystore/truststore. For now, always None.
  private val sslContext: Option[SslContext] = None

  // --- Request pipeline wiring (set via setRequestChannel before startup) ---
  @volatile private var _requestChannel: RequestChannel = _
  @volatile private var _metadataSupplier: java.util.function.Function[String, Integer] = _
  @volatile private var _topicIdSupplier: java.util.function.Function[String, org.apache.kafka.common.Uuid] = _
  @volatile private var _httpProcessor: HttpProcessor = _

  // Processor ID for the HTTP processor. Uses a high base to avoid collision
  // with binary protocol processor IDs (which start from 0).
  private val httpProcessorId = 10000 + brokerId

  // --- TASK-T1: REST handler stack (in-memory, shared across all broker-level requests) ---
  // Constructed lazily in startup() so tests that never call startup() don't pay the cost.
  @volatile private var _exchangeRestHandler: ExchangeRestHandler = _
  @volatile private var _queueRestHandler: QueueRestHandler = _
  @volatile private var _bindingRestHandler: BindingRestHandler = _
  @volatile private var _connectionRestHandler: ConnectionRestHandler = _
  @volatile private var _consumerRestHandler: ConsumerRestHandler = _
  @volatile private var _vhostRestHandler: VhostRestHandler = _
  @volatile private var _messageRestHandler: MessageRestHandler = _
  @volatile private var _wsConnectionRegistry: WsConnectionRegistry = _

  private def buildRestHandlerStack(): Unit = {
    if (_exchangeRestHandler != null) return // already built

    val wsConfigs = WsConfigs.withDefaults()

    // In-memory metadata manager — record writer is a no-op stub. Multi-broker
    // replication of routing metadata is a separate integration task; for
    // single-broker REST CRUD this is fine.
    val metadataManager = new WsRoutingMetadataManager(wsConfigs, (_, _) => ())
    metadataManager.markReplayComplete()

    // Shared single-instance ExchangeManager + BindingManager. Pre-declared
    // exchanges for vhost "/" land here via initializeDefaults.
    val exchangeManager = new ExchangeManager(metadataManager, wsConfigs)
    exchangeManager.initializeDefaults("/")

    val bindingManager = new BindingManager(
      wsConfigs.maxBindingsPerExchange(),
      (ex: String) => exchangeManager.getExchange("/", ex) != null,
      (_: String) => true // no QueueManager yet — accept any queue name as existing
    )

    // VhostManager wraps the same instances; it auto-creates the default vhost.
    val vhostManager = new VhostManager(metadataManager, exchangeManager, bindingManager)

    _wsConnectionRegistry = new WsConnectionRegistry()

    _exchangeRestHandler = new ExchangeRestHandler(exchangeManager, bindingManager)

    // No QueueManager yet — stub QueueStore throws QueueConflict on declare,
    // returns empty on get/list, no-op on delete. Tests that require real
    // queue CRUD remain @Disabled.
    val queueStore = new kafka.server.http.rest.QueueRestHandler.QueueStore {
      override def get(vhost: String, name: String): kafka.server.http.ws.QueueMetadata = null
      override def declare(vhost: String, name: String, durable: Boolean, exclusive: Boolean,
                           autoDelete: Boolean, arguments: java.util.Map[String, String])
          : kafka.server.http.ws.QueueMetadata =
        throw new kafka.server.http.rest.QueueRestHandler.QueueConflict(
          "QueueManager not yet implemented")
      override def list(vhost: String): java.util.Collection[kafka.server.http.ws.QueueMetadata] =
        java.util.Collections.emptyList()
      override def delete(vhost: String, name: String, ifUnused: Boolean, ifEmpty: Boolean): Unit = ()
    }
    _queueRestHandler = new QueueRestHandler(queueStore)
    _bindingRestHandler = new BindingRestHandler(bindingManager)
    _connectionRestHandler = new ConnectionRestHandler(_wsConnectionRegistry, brokerId)
    _consumerRestHandler = new ConsumerRestHandler(_wsConnectionRegistry, (_: String) => null)
    _vhostRestHandler = new VhostRestHandler(vhostManager)

    // TASK-T2: MessageRestHandler with stub sinks. Real produce/fetch/commit
    // to Kafka requires RequestChannel integration, deferred.
    //
    // Per-vhost RoutingEngine on VhostManager reads bindings from the metadata
    // topic — which is write-path only in this no-op stub wiring, so tests
    // that bind via REST wouldn't see the bindings. We build a direct-view
    // RoutingEngine that reads from BindingManager instead, ensuring REST
    // bind + REST publish observe the same state within a single broker.
    val routingEngine = new RoutingEngine(
      (exchangeName: String) => {
        val ex = exchangeManager.getExchange("/", exchangeName)
        if (ex == null) null else ex.`type`()
      },
      (exchangeName: String) => bindingManager.listByExchange(exchangeName),
      (_: String) => java.util.Collections.emptyList()
    )
    val stubPublishSink: MessageRestHandler.PublishSink = (queue, topic, _) =>
      new MessageRestHandler.QueueOffset(queue, 0, 0L)
    val stubGetSink: MessageRestHandler.GetSink = (_, _, _) => java.util.Collections.emptyList()
    val stubAckSink: MessageRestHandler.AckSink = (_, _, _, _) => ()
    _messageRestHandler = new MessageRestHandler(
      routingEngine,
      new WsMessageSerializer(),
      (q: String) => "ws." + q,
      stubPublishSink,
      stubGetSink,
      stubAckSink
    )
  }

  override def setRequestChannel(requestChannel: RequestChannel): Unit = {
    _requestChannel = requestChannel
  }

  override def setMetadataSupplier(supplier: java.util.function.Function[String, Integer]): Unit = {
    _metadataSupplier = supplier
  }

  override def setTopicIdSupplier(supplier: java.util.function.Function[String, org.apache.kafka.common.Uuid]): Unit = {
    _topicIdSupplier = supplier
  }

  // Track whether startup has been called already (make idempotent)
  private val started = new AtomicBoolean(false)

  /**
   * Binds the Netty server to the endpoint and starts accepting connections.
   * Idempotent: calling this multiple times is safe (second call is a no-op).
   */
  def startup(): Unit = {
    if (!started.compareAndSet(false, true)) {
      return // Already started
    }

    // If a RequestChannel was injected, create and register an HttpProcessor
    // for routing responses back to Netty channels.
    if (_requestChannel != null && _httpProcessor == null) {
      _httpProcessor = new HttpProcessor(httpProcessorId, pendingConnectionCount)
      _requestChannel.addProcessor(_httpProcessor)
      _httpProcessor.startDrainer()
      info(s"HTTP processor $httpProcessorId registered with RequestChannel")
    }

    // TASK-T1: construct in-memory REST handler stack for /v1/exchanges|queues|bindings|vhosts|connections|consumers.
    buildRestHandlerStack()

    val bootstrap = new ServerBootstrap()
    bootstrap.group(bossGroup, workerGroup)
      .channel(classOf[NioServerSocketChannel])
      .option(ChannelOption.SO_BACKLOG, Int.box(128))
      .childOption(ChannelOption.SO_KEEPALIVE, Boolean.box(true))
      .childHandler(new HttpChannelInitializer(
        sslContext = sslContext,
        draining = _draining,
        inFlightCount = pendingConnectionCount,
        httpMetrics = new kafka.server.http.HttpMetrics(),
        maxRequestBytes = httpRequestMaxBytes,
        connectionIdleTimeoutMs = httpConnectionIdleTimeoutMs,
        corsAllowedOrigins = corsAllowedOrigins,
        brokerId = brokerId,
        clusterId = clusterId,
        requestChannel = _requestChannel,
        httpProcessor = _httpProcessor,
        metadataSupplier = _metadataSupplier,
        topicIdSupplier = _topicIdSupplier,
        exchangeRestHandler = _exchangeRestHandler,
        queueRestHandler = _queueRestHandler,
        bindingRestHandler = _bindingRestHandler,
        connectionRestHandler = _connectionRestHandler,
        consumerRestHandler = _consumerRestHandler,
        vhostRestHandler = _vhostRestHandler,
        messageRestHandler = _messageRestHandler))

    val host = if (endpoint.host() == null || endpoint.host().isEmpty) "0.0.0.0" else endpoint.host()
    try {
      // Bind synchronously so boundPort() is available immediately after startup()
      val channelFuture = bootstrap.bind(host, endpoint.port()).sync()
      serverChannel = channelFuture.channel()
      startedFuture.complete(null)
      val actualPort = serverChannel.localAddress().asInstanceOf[java.net.InetSocketAddress].getPort
      info(s"HTTP acceptor started on $host:$actualPort")
    } catch {
      case e: Exception =>
        startedFuture.completeExceptionally(e)
        throw e
    }
  }

  /**
   * Returns the actual port the server is bound to. Useful when endpoint port is 0 (random).
   * Must be called after startup() completes.
   */
  def boundPort: Int = {
    if (serverChannel == null) -1
    else serverChannel.localAddress().asInstanceOf[java.net.InetSocketAddress].getPort
  }

  /**
   * Signals that the acceptor should stop accepting new requests.
   * In-flight requests that are already in the pipeline continue to completion.
   * New requests arriving after this call are rejected with HTTP 503.
   */
  def beginDrain(): Unit = {
    accepting.set(false)
    _draining.set(true)
    // Stop accepting new TCP connections by closing the server channel.
    // Existing connections remain open so in-flight responses can be written.
    if (serverChannel != null) {
      try {
        serverChannel.close().sync()
      } catch {
        case e: Exception =>
          warn(s"Error closing server channel during drain: ${e.getMessage}")
      }
    }
    info(s"HTTP acceptor drain started for ${endpoint.host()}:${endpoint.port()}")
  }

  /**
   * Blocks until all pending in-flight connections have drained or timeout elapses.
   *
   * @param timeoutMs maximum time to wait for drain
   */
  def awaitDrain(timeoutMs: Long): Unit = {
    val deadline = time.milliseconds() + timeoutMs
    while (pendingConnectionCount.get() > 0 && time.milliseconds() < deadline) {
      Thread.sleep(20)
    }
    val remaining = pendingConnectionCount.get()
    if (remaining > 0) {
      warn(s"HTTP drain timed out with $remaining requests still in-flight")
    }
  }

  /**
   * Shuts down the Netty server. Closes the server channel, then gracefully
   * shuts down worker and boss event loop groups.
   *
   * This method is idempotent - calling it multiple times does not throw.
   */
  override def close(): Unit = {
    if (closed.compareAndSet(false, true)) {
      try {
        // 0. Shut down HttpProcessor drainer thread
        if (_httpProcessor != null) {
          _httpProcessor.close()
          if (_requestChannel != null) {
            _requestChannel.removeProcessor(httpProcessorId)
          }
        }
      } catch {
        case e: Exception =>
          warn(s"Error closing HttpProcessor: ${e.getMessage}")
      }

      try {
        // 1. Close server channel
        if (serverChannel != null) {
          serverChannel.close().sync()
        }
      } catch {
        case e: Exception =>
          warn(s"Error closing server channel: ${e.getMessage}")
      }

      try {
        // 2. Shutdown worker group gracefully (quiet=100ms, timeout=500ms)
        workerGroup.shutdownGracefully(100, 500, TimeUnit.MILLISECONDS).sync()
      } catch {
        case e: Exception =>
          warn(s"Error shutting down worker group: ${e.getMessage}")
      }

      try {
        // 3. Shutdown boss group gracefully (quiet=100ms, timeout=200ms)
        bossGroup.shutdownGracefully(100, 200, TimeUnit.MILLISECONDS).sync()
      } catch {
        case e: Exception =>
          warn(s"Error shutting down boss group: ${e.getMessage}")
      }

      info(s"HTTP acceptor shut down for ${endpoint.host()}:${endpoint.port()}")
    }
  }

  /** Exposed for HttpRequestHandler to check if new requests should be accepted. */
  def isAccepting: Boolean = accepting.get()

  /** Set the accepting state. Used by tests and the drain logic. */
  def setAccepting(value: Boolean): Unit = accepting.set(value)

  /** Increment pending connection count (called by HttpRequestHandler on request start). */
  def incrementPending(): Unit = pendingConnectionCount.incrementAndGet()

  /** Decrement pending connection count (called by HttpRequestHandler on request complete). */
  def decrementPending(): Unit = pendingConnectionCount.decrementAndGet()

  /** Whether the acceptor is currently draining. */
  def isDraining: Boolean = _draining.get()

  /** Current number of pending (in-flight) requests. */
  def pendingRequestCount: Int = pendingConnectionCount.get()

  /** Direct access to the draining flag for tests. */
  val draining: AtomicBoolean = _draining

  /** Direct access to the in-flight count for tests. */
  val inFlightCount: AtomicInteger = pendingConnectionCount
}
