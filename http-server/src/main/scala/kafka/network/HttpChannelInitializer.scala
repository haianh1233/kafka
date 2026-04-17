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

import io.netty.channel.{ChannelHandler, ChannelInitializer, ChannelPipeline}
import io.netty.channel.socket.SocketChannel
import io.netty.handler.codec.http._
import io.netty.handler.codec.http.cors.{CorsConfig, CorsConfigBuilder, CorsHandler}
import io.netty.handler.ssl.SslContext
import io.netty.handler.timeout.IdleStateHandler
import kafka.server.http.{HttpMetrics, HttpProcessor, HttpProtocolNegotiationHandler, HttpServerConfigs, IdleStateCloseHandler}
import org.apache.kafka.common.security.auth.{KafkaPrincipalBuilder, SecurityProtocol}
import org.apache.kafka.common.security.authenticator.DefaultKafkaPrincipalBuilder

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
 *
 * // Time: Created - TASK-B.03
 * // Time: Modified - TASK-F.04 (CORS support)
 * // Time: Modified - TASK-F.07 (HTTP/2 ALPN)
 * // Time: Modified - TASK-B.06 (request pipeline wiring)
 */
class HttpChannelInitializer(
  sslContext: Option[SslContext],
  draining: AtomicBoolean,
  inFlightCount: AtomicInteger,
  httpMetrics: HttpMetrics,
  maxRequestBytes: Int,
  connectionIdleTimeoutMs: Long,
  corsAllowedOrigins: String = "",
  principalBuilder: KafkaPrincipalBuilder = new DefaultKafkaPrincipalBuilder(null, null),
  securityProtocol: SecurityProtocol = SecurityProtocol.HTTP,
  brokerId: Int = -1,
  clusterId: String = "",
  requestChannel: RequestChannel = null,
  httpProcessor: HttpProcessor = null,
  metadataSupplier: java.util.function.Function[String, Integer] = null,
  topicIdSupplier: java.util.function.Function[String, org.apache.kafka.common.Uuid] = null,
  httpServerConfigs: HttpServerConfigs = HttpServerConfigs.withDefaults()
) extends ChannelInitializer[SocketChannel] {

  // Build CORS config once at initialization time, reused for every channel
  private val corsConfig: Option[CorsConfig] = HttpChannelInitializer.buildCorsConfig(corsAllowedOrigins)

  override def initChannel(ch: SocketChannel): Unit = {
    val pipeline = ch.pipeline()

    sslContext match {
      case Some(ssl) =>
        // HTTPS: TLS + ALPN protocol negotiation (HTTP/2 or HTTP/1.1)
        pipeline.addLast("ssl", ssl.newHandler(ch.alloc()))
        val handlerFactory: java.util.function.Supplier[ChannelHandler] = () =>
          new HttpRequestHandler(principalBuilder, securityProtocol, draining, inFlightCount,
            brokerId, clusterId, requestChannel, httpProcessor, metadataSupplier, topicIdSupplier, httpServerConfigs)
        pipeline.addLast("protocol-negotiation",
          new HttpProtocolNegotiationHandler(
            draining, inFlightCount, httpMetrics,
            maxRequestBytes, connectionIdleTimeoutMs, handlerFactory))

      case None =>
        // Plaintext HTTP: always HTTP/1.1 (h2c not supported)
        configureHttp11Pipeline(pipeline)
    }
  }

  private[network] def configureHttp11Pipeline(pipeline: ChannelPipeline): Unit = {
    pipeline.addLast("http-codec", new HttpServerCodec())

    // CORS handler (if configured) -- must be after codec, before aggregator
    corsConfig.foreach { cc =>
      pipeline.addLast("cors", new CorsHandler(cc))
    }

    pipeline.addLast("http-aggregator",
      new HttpObjectAggregator(maxRequestBytes))
    pipeline.addLast("compressor", new HttpContentCompressor())
    pipeline.addLast("idle-handler", new IdleStateHandler(
      0, 0, connectionIdleTimeoutMs, TimeUnit.MILLISECONDS))
    pipeline.addLast("idle-closer", new IdleStateCloseHandler(httpMetrics))
    pipeline.addLast("kafka-handler",
      new HttpRequestHandler(principalBuilder, securityProtocol, draining, inFlightCount,
        brokerId, clusterId, requestChannel, httpProcessor, metadataSupplier, topicIdSupplier, httpServerConfigs))
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
