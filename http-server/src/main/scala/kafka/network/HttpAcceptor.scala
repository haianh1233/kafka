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
 */
class HttpAcceptor(
    endpoint: Endpoint,
    numWorkerThreads: Int,
    httpRequestMaxBytes: Int,
    httpConnectionIdleTimeoutMs: Long,
    time: Time
) extends Closeable with Logging {

  // --- Netty event loop groups ---
  // Boss group MUST use exactly 1 thread (one server socket)
  private val bossGroup: EventLoopGroup = new NioEventLoopGroup(1)

  // Worker group: num.http.network.threads (default 4)
  private val workerGroup: EventLoopGroup = new NioEventLoopGroup(numWorkerThreads)

  // --- Server channel (bound socket) ---
  @volatile private var serverChannel: Channel = _

  // --- Lifecycle state ---
  private val accepting = new AtomicBoolean(true)
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

  /**
   * Binds the Netty server to the endpoint and starts accepting connections.
   */
  def startup(): Unit = {
    val bootstrap = new ServerBootstrap()
    bootstrap.group(bossGroup, workerGroup)
      .channel(classOf[NioServerSocketChannel])
      .option(ChannelOption.SO_BACKLOG, Int.box(128))
      .childOption(ChannelOption.SO_KEEPALIVE, Boolean.box(true))
      .childHandler(new HttpChannelInitializer(
        endpoint, httpRequestMaxBytes, httpConnectionIdleTimeoutMs, sslContext))

    val host = if (endpoint.host() == null || endpoint.host().isEmpty) "0.0.0.0" else endpoint.host()
    val bindFuture = bootstrap.bind(host, endpoint.port())
    bindFuture.addListener { future: io.netty.util.concurrent.Future[_ >: Void] =>
      if (future.isSuccess) {
        serverChannel = bindFuture.channel()
        startedFuture.complete(null)
        info(s"HTTP acceptor started on $host:${endpoint.port()}")
      } else {
        startedFuture.completeExceptionally(future.cause())
      }
    }
  }

  /**
   * Signals that the acceptor should stop accepting new requests.
   * In-flight requests that are already in the pipeline continue to completion.
   * New requests arriving after this call are rejected with HTTP 503.
   */
  def beginDrain(): Unit = {
    accepting.set(false)
    // The HttpRequestHandler (TASK-B.05) checks isAccepting and rejects new requests
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

  /** Increment pending connection count (called by HttpRequestHandler on request start). */
  def incrementPending(): Unit = pendingConnectionCount.incrementAndGet()

  /** Decrement pending connection count (called by HttpRequestHandler on request complete). */
  def decrementPending(): Unit = pendingConnectionCount.decrementAndGet()
}
