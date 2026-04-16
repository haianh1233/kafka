/*
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
// Time: Created - TASK-F.05
// Time: Modified - TASK-F.06 (drain check + in-flight tracking)
// Time: Modified - TASK-B.06 (request pipeline wiring)
package kafka.network

import io.netty.buffer.Unpooled
import io.netty.channel.{ChannelFutureListener, ChannelHandlerContext, SimpleChannelInboundHandler}
import io.netty.handler.codec.http.{DefaultFullHttpResponse, FullHttpRequest, HttpHeaderNames, HttpResponseStatus, HttpVersion}
import io.netty.handler.ssl.SslHandler
import kafka.server.http.{HttpProcessor, HttpRequestTranslator, HttpRouter, HttpServerConfigs}
import org.apache.kafka.common.memory.MemoryPool
import org.apache.kafka.common.network.{ClientInformation, ListenerName}
import org.apache.kafka.common.requests.{RequestContext, RequestHeader}
import org.apache.kafka.common.security.auth.{HttpAuthenticationContext, KafkaPrincipal, KafkaPrincipalBuilder, SecurityProtocol}
import org.apache.kafka.common.utils.Time

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.security.cert.X509Certificate
import java.util.Base64
import java.util.Optional
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

/**
 * Netty channel handler for HTTP requests that extracts authentication context
 * and delegates to KafkaPrincipalBuilder for principal resolution.
 *
 * Authentication extraction priority: mTLS > Bearer > Basic > Anonymous.
 * Once a method is found, lower-priority methods are not checked.
 *
 * This handler also implements drain-phase request rejection: when the broker
 * is shutting down, new requests receive 503 Service Unavailable with a
 * Retry-After header directing clients to retry on another broker.
 *
 * In-flight request tracking: the handler increments the in-flight counter
 * when a request passes the drain check. The counter is decremented when the
 * response is written (handled by HttpProcessor) or on error paths.
 *
 * When requestChannel and httpProcessor are provided, non-health requests are
 * routed through the full Kafka request pipeline:
 *   HTTP request → HttpRouter → HttpRequestTranslator → RequestChannel → KafkaApis → HttpProcessor → Netty
 */
class HttpRequestHandler(
  principalBuilder: KafkaPrincipalBuilder,
  securityProtocol: SecurityProtocol,
  draining: AtomicBoolean,
  inFlightCount: AtomicInteger,
  brokerId: Int = -1,
  clusterId: String = "",
  requestChannel: RequestChannel = null,
  httpProcessor: HttpProcessor = null,
  metadataSupplier: java.util.function.Function[String, Integer] = null,
  httpServerConfigs: HttpServerConfigs = HttpServerConfigs.withDefaults()
) extends SimpleChannelInboundHandler[FullHttpRequest] {

  /** Convenience constructor for backward compatibility (no drain support). */
  def this(principalBuilder: KafkaPrincipalBuilder, securityProtocol: SecurityProtocol) =
    this(principalBuilder, securityProtocol, new AtomicBoolean(false), new AtomicInteger(0))

  /** Whether the request pipeline is wired (non-null requestChannel + httpProcessor). */
  private val pipelineWired: Boolean = requestChannel != null && httpProcessor != null

  /** Shared stateless router instance. */
  private val router: HttpRouter = new HttpRouter()

  /** Translator instance (has a round-robin counter for batch-sticky partitioning). */
  private val translator: HttpRequestTranslator = new HttpRequestTranslator()

  /** Correlation ID counter — monotonically increasing per handler instance. */
  private val correlationIdCounter: AtomicInteger = new AtomicInteger(0)

  /** JSON body for 503 drain response */
  private val DrainingResponseBody =
    """{"errorCode":-1,"errorMessage":"Broker is shutting down","detail":"SERVICE_UNAVAILABLE"}"""

  private[network] def extractAuthContext(
    ctx: ChannelHandlerContext,
    req: FullHttpRequest
  ): HttpAuthenticationContext = {
    val remoteAddr = ctx.channel().remoteAddress().asInstanceOf[InetSocketAddress]
    val builder = new HttpAuthenticationContext.Builder()
      .clientAddress(remoteAddr.getAddress)
      .securityProtocol(securityProtocol)

    val sslHandler = ctx.pipeline().get(classOf[SslHandler])
    if (sslHandler != null) {
      try {
        val session = sslHandler.engine().getSession
        val peerCerts = session.getPeerCertificates
        if (peerCerts != null && peerCerts.nonEmpty) {
          val x509Certs = peerCerts.collect { case c: X509Certificate => c }
          if (x509Certs.nonEmpty) {
            builder.peerCertificates(x509Certs)
            return builder.build()
          }
        }
      } catch {
        case _: javax.net.ssl.SSLPeerUnverifiedException =>
      }
    }

    val authHeader = req.headers().get(HttpHeaderNames.AUTHORIZATION)
    if (authHeader != null) {
      if (authHeader.regionMatches(true, 0, "Bearer ", 0, 7)) {
        val token = authHeader.substring(7).trim
        if (token.nonEmpty) {
          builder.bearerToken(token)
          return builder.build()
        }
      } else if (authHeader.regionMatches(true, 0, "Basic ", 0, 6)) {
        val encoded = authHeader.substring(6).trim
        try {
          val decoded = new String(Base64.getDecoder.decode(encoded), StandardCharsets.UTF_8)
          val colonIndex = decoded.indexOf(':')
          if (colonIndex > 0) {
            val username = decoded.substring(0, colonIndex)
            val password = decoded.substring(colonIndex + 1)
            builder.basicCredentials(username, password)
            return builder.build()
          }
        } catch {
          case _: IllegalArgumentException =>
        }
      }
    }

    builder.build()
  }

  private[network] def buildPrincipal(ctx: ChannelHandlerContext, req: FullHttpRequest): KafkaPrincipal = {
    val authContext = extractAuthContext(ctx, req)
    principalBuilder.build(authContext)
  }

  override def channelRead0(ctx: ChannelHandlerContext, req: FullHttpRequest): Unit = {
    if (draining.get()) {
      sendDrainingResponse(ctx)
      return
    }

    inFlightCount.incrementAndGet()

    // Track whether we handed the request off to the async pipeline.
    // If true, HttpProcessor owns the inFlightCount decrement.
    var handedOffToAsyncPipeline = false

    try {
      // Health check: respond directly without RequestChannel
      val uri = req.uri()
      if (uri == "/v1/health" || uri.startsWith("/v1/health?")) {
        sendHealthResponse(ctx)
        return
      }

      // If pipeline is not wired, return 501
      if (!pipelineWired) {
        sendErrorResponse(ctx, HttpResponseStatus.NOT_IMPLEMENTED,
          """{"errorCode":-1,"errorMessage":"HTTP endpoint not yet wired to RequestChannel"}""")
        return
      }

      // --- Route the request ---
      val method = io.netty.handler.codec.http.HttpMethod.valueOf(req.method().name())
      val routeResult = try {
        router.route(method, uri)
      } catch {
        case e: org.apache.kafka.common.errors.InvalidRequestException =>
          sendErrorResponse(ctx, HttpResponseStatus.NOT_FOUND,
            s"""{"errorCode":-1,"errorMessage":"${escapeJson(e.getMessage)}"}""")
          return
        case e: org.apache.kafka.common.errors.InvalidTopicException =>
          sendErrorResponse(ctx, HttpResponseStatus.BAD_REQUEST,
            s"""{"errorCode":-1,"errorMessage":"${escapeJson(e.getMessage)}"}""")
          return
      }

      // Health routed through router also handled directly
      if (routeResult.handlerType() == HttpRouter.HandlerType.HEALTH) {
        sendHealthResponse(ctx)
        return
      }

      // OpenAPI spec not yet implemented
      if (routeResult.handlerType() == HttpRouter.HandlerType.OPENAPI_SPEC) {
        sendErrorResponse(ctx, HttpResponseStatus.NOT_IMPLEMENTED,
          """{"errorCode":-1,"errorMessage":"OpenAPI spec endpoint not yet implemented"}""")
        return
      }

      // --- Content-Type validation for POST requests that require JSON body ---
      val handlerType = routeResult.handlerType()
      if (req.method() == io.netty.handler.codec.http.HttpMethod.POST &&
        (handlerType == HttpRouter.HandlerType.PRODUCE ||
         handlerType == HttpRouter.HandlerType.FETCH ||
         handlerType == HttpRouter.HandlerType.COMMIT_OFFSETS)) {
        val contentType = req.headers().get(HttpHeaderNames.CONTENT_TYPE)
        if (contentType == null || !contentType.toLowerCase.startsWith("application/json")) {
          sendErrorResponse(ctx, HttpResponseStatus.UNSUPPORTED_MEDIA_TYPE,
            s"""{"errorCode":-1,"errorMessage":"Content-Type must be application/json, got: ${escapeJson(if (contentType != null) contentType else "null")}"}""")
          return
        }
      }

      // --- Translate the request ---
      val bodyBytes = if (req.content().isReadable) {
        val bytes = new Array[Byte](req.content().readableBytes())
        req.content().readBytes(bytes)
        bytes
      } else {
        null
      }

      val translationResult = try {
        translator.translate(routeResult, bodyBytes, httpServerConfigs, metadataSupplier)
      } catch {
        case e: org.apache.kafka.common.errors.InvalidRequestException =>
          sendErrorResponse(ctx, HttpResponseStatus.BAD_REQUEST,
            s"""{"errorCode":-1,"errorMessage":"${escapeJson(e.getMessage)}"}""")
          return
        case e: Exception =>
          sendErrorResponse(ctx, HttpResponseStatus.INTERNAL_SERVER_ERROR,
            s"""{"errorCode":-1,"errorMessage":"Translation failed: ${escapeJson(e.getMessage)}"}""")
          return
      }

      // --- Build RequestContext and RequestChannel.Request ---
      val principal = buildPrincipal(ctx, req)
      val connectionId = ctx.channel().id().asLongText()
      val clientAddress = ctx.channel().remoteAddress().asInstanceOf[InetSocketAddress]
      val clientId = HttpRouter.validateClientId(req.headers().get("X-Kafka-Client-ID"))
      val correlationId = correlationIdCounter.getAndIncrement()

      val requestHeader = new RequestHeader(
        translationResult.apiKey(),
        translationResult.apiVersion(),
        clientId,
        correlationId
      )

      val requestContext = new RequestContext(
        requestHeader,
        connectionId,
        clientAddress.getAddress,
        Optional.of(Integer.valueOf(clientAddress.getPort)),
        principal,
        ListenerName.normalised("HTTP"),
        securityProtocol,
        ClientInformation.EMPTY,
        false // fromPrivilegedListener
      )

      // Ensure buffer is ready to be read
      val requestBuffer = translationResult.serializedRequest()
      if (requestBuffer.position() != 0) {
        requestBuffer.rewind()
      }

      val channelRequest = new RequestChannel.Request(
        processor = httpProcessor.id(),
        context = requestContext,
        startTimeNanos = Time.SYSTEM.nanoseconds(),
        memoryPool = MemoryPool.NONE,
        buffer = requestBuffer,
        metrics = requestChannel.metrics,
        envelope = None
      )

      // --- Register channel with HttpProcessor for response routing ---
      httpProcessor.registerChannel(connectionId, ctx)

      // --- Enqueue to RequestChannel (non-blocking) ---
      val enqueued = requestChannel.tryEnqueue(channelRequest)
      if (!enqueued) {
        // Queue full — unregister and return 503
        httpProcessor.unregisterChannel(connectionId)
        sendErrorResponse(ctx, HttpResponseStatus.SERVICE_UNAVAILABLE,
          """{"errorCode":-1,"errorMessage":"Request queue full","detail":"SERVICE_UNAVAILABLE"}""")
        return
      }

      // Request is now in the async pipeline. HttpProcessor owns the
      // inFlightCount decrement when the response is written.
      handedOffToAsyncPipeline = true

    } finally {
      if (!handedOffToAsyncPipeline) {
        inFlightCount.decrementAndGet()
      }
    }
  }

  private[network] def sendHealthResponse(ctx: ChannelHandlerContext): Unit = {
    // Health check returns broker state — always RUNNING if we reached here
    val body = s"""{"status":"RUNNING","brokerId":$brokerId,"clusterId":"$clusterId"}""".getBytes(java.nio.charset.StandardCharsets.UTF_8)
    val response = new DefaultFullHttpResponse(
      HttpVersion.HTTP_1_1,
      HttpResponseStatus.OK,
      Unpooled.wrappedBuffer(body))
    response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json")
    response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, body.length)
    ctx.writeAndFlush(response)
  }

  private[network] def sendDrainingResponse(ctx: ChannelHandlerContext): Unit = {
    val body = DrainingResponseBody.getBytes(StandardCharsets.UTF_8)
    val response = new DefaultFullHttpResponse(
      HttpVersion.HTTP_1_1,
      HttpResponseStatus.SERVICE_UNAVAILABLE,
      Unpooled.wrappedBuffer(body))
    response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json")
    response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, body.length)
    response.headers().set("Retry-After", "5")
    response.headers().set(HttpHeaderNames.CONNECTION, "close")
    ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE)
  }

  /**
   * Sends a JSON error response with the given HTTP status and body.
   * Used for synchronous error paths (routing errors, translation errors, etc.)
   */
  private[network] def sendErrorResponse(
    ctx: ChannelHandlerContext,
    status: HttpResponseStatus,
    jsonBody: String
  ): Unit = {
    val body = jsonBody.getBytes(StandardCharsets.UTF_8)
    val response = new DefaultFullHttpResponse(
      HttpVersion.HTTP_1_1,
      status,
      Unpooled.wrappedBuffer(body))
    response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json")
    response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, body.length)
    ctx.writeAndFlush(response)
  }

  /**
   * Minimal JSON string escaping for error messages embedded in JSON strings.
   * Escapes backslash, double-quote, and control characters.
   */
  private def escapeJson(s: String): String = {
    if (s == null) return "null"
    val sb = new StringBuilder(s.length)
    var i = 0
    while (i < s.length) {
      val c = s.charAt(i)
      c match {
        case '"' => sb.append("\\\"")
        case '\\' => sb.append("\\\\")
        case '\n' => sb.append("\\n")
        case '\r' => sb.append("\\r")
        case '\t' => sb.append("\\t")
        case _ if c < 0x20 => sb.append(f"\\u${c.toInt}%04x")
        case _ => sb.append(c)
      }
      i += 1
    }
    sb.toString()
  }
}
