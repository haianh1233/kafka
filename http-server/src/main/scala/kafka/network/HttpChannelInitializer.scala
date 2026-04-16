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
<<<<<<< HEAD

package kafka.network

import java.util.concurrent.TimeUnit

import io.netty.channel.{Channel, ChannelDuplexHandler, ChannelHandlerContext, ChannelInitializer}
import io.netty.handler.codec.http.{HttpContentCompressor, HttpObjectAggregator, HttpServerCodec}
import io.netty.handler.codec.http.cors.{CorsConfig, CorsConfigBuilder, CorsHandler}
import io.netty.handler.ssl.SslContext
import io.netty.handler.timeout.{IdleStateEvent, IdleStateHandler}
import kafka.utils.Logging
import org.apache.kafka.common.Endpoint

/**
 * Configures the Netty pipeline for each new HTTP connection.
 *
 * Pipeline order:
 *   [ssl]           - SslHandler (HTTPS only)
 *   http-codec      - HttpServerCodec (HTTP/1.1 request/response encoding/decoding)
 *   [cors]          - CorsHandler (CORS support, when configured)
 *   http-aggregator - HttpObjectAggregator (aggregate chunked -> FullHttpRequest)
 *   compressor      - HttpContentCompressor (gzip/deflate response compression)
 *   idle-handler    - IdleStateHandler (detect idle connections)
 *   idle-closer     - IdleStateCloseHandler (close on idle timeout)
 *
 * The HttpObjectAggregator is bounded by http.request.max.bytes (default 10 MB).
 * Requests exceeding this limit receive automatic HTTP 413 from Netty.
 *
 * Note: kafka-handler (HttpRequestHandler) is NOT added in this task.
 * It will be fully wired in TASK-B.05.
 *
 * // Time: Created - TASK-B.03
 * // Time: Modified - TASK-F.04 (CORS support)
 */
class HttpChannelInitializer(
    endpoint: Endpoint,
    httpRequestMaxBytes: Int,
    httpConnectionIdleTimeoutMs: Long,
    sslContext: Option[SslContext],
    corsAllowedOrigins: String = ""
) extends ChannelInitializer[Channel] with Logging {

  // Build CORS config once at initialization time, reused for every channel
  private val corsConfig: Option[CorsConfig] = HttpChannelInitializer.buildCorsConfig(corsAllowedOrigins)

  override def initChannel(ch: Channel): Unit = {
    val pipeline = ch.pipeline()

    // 1. Optional SSL (for HTTPS listeners)
    sslContext.foreach { ctx =>
      pipeline.addLast("ssl", ctx.newHandler(ch.alloc()))
    }

    // 2. HTTP/1.1 codec - decodes HTTP frames, encodes responses
    pipeline.addLast("http-codec", new HttpServerCodec())

    // 3. CORS handler (if configured) -- must be after codec, before aggregator
    //    Handles preflight OPTIONS requests and sets Access-Control-* headers
    corsConfig.foreach { cc =>
      pipeline.addLast("cors", new CorsHandler(cc))
    }

    // 4. Aggregate chunked requests into FullHttpRequest
    //    Netty returns 413 automatically if body exceeds limit
    pipeline.addLast("http-aggregator", new HttpObjectAggregator(httpRequestMaxBytes))

    // 5. Response compression (gzip/deflate based on Accept-Encoding)
    pipeline.addLast("compressor", new HttpContentCompressor())

    // 6. Idle connection detection
    //    Uses allIdle timeout - fires if no read OR write for the configured duration
    //    IMPORTANT: use the TimeUnit overload, not the seconds-only constructor
    pipeline.addLast("idle-handler",
      new IdleStateHandler(0, 0, httpConnectionIdleTimeoutMs, TimeUnit.MILLISECONDS))

    // 7. Close channel on idle timeout
    pipeline.addLast("idle-closer", new IdleStateCloseHandler())

    // Note: kafka-handler (HttpRequestHandler) will be added by TASK-B.05
  }
}

/**
 * Companion object for HttpChannelInitializer containing CORS configuration utilities.
 *
 * // Time: Created - TASK-F.04
 */
object HttpChannelInitializer {

  /**
   * Parse comma-separated CORS origins from the raw config string.
   *
   * @param raw the raw config value from http.cors.allowed.origins
   * @return array of trimmed, non-empty origin strings; empty array if CORS is disabled
   */
  def parseCorsOrigins(raw: String): Array[String] = {
    if (raw == null || raw.trim.isEmpty) Array.empty
    else raw.split(",").map(_.trim).filter(_.nonEmpty)
  }

  /**
   * Build Netty CorsConfig from the raw CORS origins config string.
   * Returns None if CORS is not configured (empty origins).
   *
   * When origins contains "*", uses CorsConfigBuilder.forAnyOrigin().
   * Otherwise, uses CorsConfigBuilder.forOrigins() with the specific origin list.
   *
   * Configured behavior:
   *   - Allowed methods: GET, POST, OPTIONS
   *   - Allowed request headers: Content-Type, Authorization, X-Kafka-Client-ID, X-Request-ID
   *   - Exposed response headers: X-Kafka-Request-ID, X-Kafka-MaxWait-Applied, Retry-After
   *   - Max age: 3600 seconds (1 hour preflight cache)
   *   - Null origin allowed (for file:// and data: URI browsers)
   *
   * @param corsAllowedOrigins raw comma-separated origins string
   * @return Some(CorsConfig) if CORS is enabled, None otherwise
   */
  def buildCorsConfig(corsAllowedOrigins: String): Option[CorsConfig] = {
    val origins = parseCorsOrigins(corsAllowedOrigins)
    if (origins.isEmpty) return None

    val builder = if (origins.contains("*")) {
      CorsConfigBuilder.forAnyOrigin()
    } else {
      CorsConfigBuilder.forOrigins(origins: _*)
    }

    Some(
      builder
        .allowedRequestMethods(
          io.netty.handler.codec.http.HttpMethod.GET,
          io.netty.handler.codec.http.HttpMethod.POST,
          io.netty.handler.codec.http.HttpMethod.OPTIONS
        )
        .allowedRequestHeaders(
          "Content-Type",
          "Authorization",
          "X-Kafka-Client-ID",
          "X-Request-ID"
        )
        .exposeHeaders(
          "X-Kafka-Request-ID",
          "X-Kafka-MaxWait-Applied",
          "Retry-After"
        )
        .maxAge(3600) // 1 hour preflight cache
        .allowNullOrigin() // some browsers send "null" origin for file:// or data: URIs
        .build()
    )
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
        debug(s"Closing idle HTTP connection: ${ctx.channel().remoteAddress()}")
        ctx.close()
      case _ =>
        super.userEventTriggered(ctx, evt)
    }
=======
package kafka.network

import io.netty.channel.{ChannelInitializer, ChannelPipeline}
import io.netty.channel.socket.SocketChannel
import io.netty.handler.codec.http._
import io.netty.handler.ssl.SslContext
import io.netty.handler.timeout.IdleStateHandler
import kafka.server.http.{HttpMetrics, HttpProtocolNegotiationHandler, HttpRequestHandler, IdleStateCloseHandler}

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

/**
 * Netty channel initializer for the Kafka HTTP server.
 *
 * For TLS-enabled listeners (when sslContext is provided):
 *   - Adds SslHandler for TLS termination
 *   - Adds [[HttpProtocolNegotiationHandler]] for ALPN-based protocol selection
 *   - HTTP/2 (h2) is used when the client negotiates it via ALPN
 *   - HTTP/1.1 is the fallback when the client does not support h2
 *
 * For plaintext listeners (when sslContext is None):
 *   - Always uses HTTP/1.1 (h2c is not supported)
 *   - Standard pipeline: HttpServerCodec -> HttpObjectAggregator -> HttpContentCompressor
 *     -> IdleStateHandler -> IdleStateCloseHandler -> HttpRequestHandler
 *
 * HTTP/2 requires TLS with ALPN negotiation, so h2 is never available on plaintext
 * listeners. This follows the design decision to not support h2c (HTTP/2 upgrade
 * over cleartext) in order to keep complexity manageable.
 */
class HttpChannelInitializer(
  sslContext: Option[SslContext],
  draining: AtomicBoolean,
  inFlightCount: AtomicInteger,
  httpMetrics: HttpMetrics,
  maxRequestBytes: Int,
  connectionIdleTimeoutMs: Long
) extends ChannelInitializer[SocketChannel] {

  override def initChannel(ch: SocketChannel): Unit = {
    val pipeline = ch.pipeline()

    sslContext match {
      case Some(ssl) =>
        // HTTPS: TLS + ALPN protocol negotiation (HTTP/2 or HTTP/1.1)
        pipeline.addLast("ssl", ssl.newHandler(ch.alloc()))
        pipeline.addLast("protocol-negotiation",
          new HttpProtocolNegotiationHandler(
            draining, inFlightCount, httpMetrics,
            maxRequestBytes, connectionIdleTimeoutMs))

      case None =>
        // Plaintext HTTP: always HTTP/1.1 (h2c not supported)
        configureHttp11Pipeline(pipeline)
    }
  }

  private[network] def configureHttp11Pipeline(pipeline: ChannelPipeline): Unit = {
    pipeline.addLast("http-codec", new HttpServerCodec())
    // CORS handler would go here if configured (TASK-F.04)
    pipeline.addLast("http-aggregator",
      new HttpObjectAggregator(maxRequestBytes))
    pipeline.addLast("compressor", new HttpContentCompressor())
    pipeline.addLast("idle-handler", new IdleStateHandler(
      0, 0, connectionIdleTimeoutMs, TimeUnit.MILLISECONDS))
    pipeline.addLast("idle-closer", new IdleStateCloseHandler(httpMetrics))
    pipeline.addLast("kafka-handler",
      new HttpRequestHandler(draining, inFlightCount, httpMetrics, maxRequestBytes))
>>>>>>> 6168ee8c21 (feat(http-server): add HTTP/2 support via Netty ALPN negotiation (TASK-F.07))
  }
}
