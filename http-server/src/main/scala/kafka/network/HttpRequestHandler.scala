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
// Time: Modified - TASK-WS2.06 (REST exchange/queue/binding CRUD dispatch)
// Time: Modified - TASK-WS2.07 (REST connection/consumer management dispatch)
// Time: Modified - TASK-WS2.08 (REST message operations dispatch)
// Time: Update - TASK-WS2.09 - added vhost scoping (vhost REST dispatch)
package kafka.network

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import io.netty.buffer.Unpooled
import io.netty.channel.{ChannelFutureListener, ChannelHandlerContext, SimpleChannelInboundHandler}
import io.netty.handler.codec.http.{DefaultFullHttpResponse, FullHttpRequest, HttpHeaderNames, HttpResponseStatus, HttpVersion}
import io.netty.handler.ssl.SslHandler
import kafka.server.http.{HttpProcessor, HttpRequestTranslator, HttpRouter, HttpServerConfigs}
import kafka.server.http.rest.{BindingRestHandler, ConnectionRestHandler, ConsumerRestHandler, ExchangeRestHandler, MessageRestHandler, QueueRestHandler, VhostRestHandler}
import org.apache.kafka.common.memory.MemoryPool
import org.apache.kafka.common.network.{ClientInformation, ListenerName}
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.message.ShareGroupHeartbeatRequestData
import org.apache.kafka.common.requests.{RequestContext, RequestHeader, ShareFetchRequest, ShareGroupHeartbeatRequest, ShareRequestMetadata}
import org.apache.kafka.common.security.auth.{HttpAuthenticationContext, KafkaPrincipal, KafkaPrincipalBuilder, SecurityProtocol}
import org.apache.kafka.common.utils.Time

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.security.cert.X509Certificate
import java.util.Base64
import java.util.Optional
import java.util.concurrent.TimeUnit
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
  topicIdSupplier: java.util.function.Function[String, org.apache.kafka.common.Uuid] = null,
  httpServerConfigs: HttpServerConfigs = HttpServerConfigs.withDefaults(),
  exchangeRestHandler: ExchangeRestHandler = null,
  queueRestHandler: QueueRestHandler = null,
  bindingRestHandler: BindingRestHandler = null,
  connectionRestHandler: ConnectionRestHandler = null,
  consumerRestHandler: ConsumerRestHandler = null,
  messageRestHandler: MessageRestHandler = null,
  // WS2.09: vhost admin REST handler
  vhostRestHandler: VhostRestHandler = null
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

  /** Jackson ObjectMapper for JSON parsing in offset request handling. */
  private val objectMapper: ObjectMapper = new ObjectMapper()

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

      // Consumer lag endpoint not yet implemented
      if (routeResult.handlerType() == HttpRouter.HandlerType.CONSUMER_LAG) {
        sendErrorResponse(ctx, HttpResponseStatus.NOT_FOUND,
          """{"errorCode":3,"errorMessage":"UNKNOWN_TOPIC_OR_PARTITION","detail":"Consumer group lag endpoint not yet implemented"}""")
        return
      }

      // --- WS2.06: REST exchange/queue/binding CRUD dispatch ---
      // Handlers produce a fully-formed FullHttpResponse synchronously; no
      // RequestChannel / HttpProcessor involvement is required.
      if (isRestRoutingHandler(routeResult.handlerType())) {
        handleRestRoutingRequest(ctx, req, routeResult)
        return
      }

      // --- Content-Type validation for POST requests that require JSON body ---
      val handlerType = routeResult.handlerType()
      if (req.method() == io.netty.handler.codec.http.HttpMethod.POST &&
        (handlerType == HttpRouter.HandlerType.PRODUCE ||
         handlerType == HttpRouter.HandlerType.FETCH ||
         handlerType == HttpRouter.HandlerType.COMMIT_OFFSETS ||
         handlerType == HttpRouter.HandlerType.SHARE_POLL ||
         handlerType == HttpRouter.HandlerType.SHARE_ACKNOWLEDGE)) {
        val contentType = req.headers().get(HttpHeaderNames.CONTENT_TYPE)
        if (contentType == null || !contentType.toLowerCase.startsWith("application/json")) {
          sendErrorResponse(ctx, HttpResponseStatus.UNSUPPORTED_MEDIA_TYPE,
            s"""{"errorCode":-1,"errorMessage":"Content-Type must be application/json, got: ${escapeJson(if (contentType != null) contentType else "null")}"}""")
          return
        }
      }

      // --- Handle COMMIT_OFFSETS and FETCH_OFFSETS with dedicated translation ---
      if (handlerType == HttpRouter.HandlerType.COMMIT_OFFSETS ||
          handlerType == HttpRouter.HandlerType.FETCH_OFFSETS) {
        handleOffsetRequest(ctx, req, routeResult)
        handedOffToAsyncPipeline = true
        return
      }

      // --- Handle SHARE_POLL and SHARE_ACKNOWLEDGE with dedicated translation ---
      if (handlerType == HttpRouter.HandlerType.SHARE_POLL ||
          handlerType == HttpRouter.HandlerType.SHARE_ACKNOWLEDGE) {
        handleShareRequest(ctx, req, routeResult)
        handedOffToAsyncPipeline = true
        return
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
   * Handles offset commit (POST) and offset fetch (GET) requests.
   * Parses the body, translates to Kafka request, and sends through the standard pipeline.
   *
   * Commit body format (nested by topic):
   * {
   *   "topics": [{"topic":"t","partitions":[{"partition":0,"offset":3}]}]
   * }
   *
   * Also supports the flat format:
   * {
   *   "offsets": [{"topic":"t","partition":0,"offset":3}]
   * }
   */
  private[network] def handleOffsetRequest(
    ctx: ChannelHandlerContext,
    req: FullHttpRequest,
    routeResult: HttpRouter.RouteResult
  ): Unit = {
    val group = routeResult.consumerGroup()
    val isCommit = routeResult.handlerType() == HttpRouter.HandlerType.COMMIT_OFFSETS

    try {
      val (apiKey: ApiKeys, apiVersion: Short, buffer: ByteBuffer) = if (isCommit) {
        val bodyBytes = if (req.content().isReadable) {
          val bytes = new Array[Byte](req.content().readableBytes())
          req.content().readBytes(bytes)
          bytes
        } else {
          null
        }
        if (bodyBytes == null || bodyBytes.isEmpty) {
          sendErrorResponse(ctx, HttpResponseStatus.BAD_REQUEST,
            """{"errorCode":-1,"errorMessage":"Request body is required for offset commit"}""")
          return
        }

        val json = objectMapper.readTree(bodyBytes)
        // Convert nested format to flat format if needed
        val flatJson = convertToFlatOffsetFormat(json)
        val result = HttpRequestTranslator.translateCommitOffsets(group, flatJson)
        // Use version 9 (last version with topic names, before topic IDs in v10)
        val version: Short = Math.min(9, ApiKeys.OFFSET_COMMIT.latestVersion()).toShort
        val request = result.builder().build(version)
        (ApiKeys.OFFSET_COMMIT, version, request.serialize().buffer())
      } else {
        // FETCH_OFFSETS - always fetch all offsets for the group (null = all topics)
        // Topic filtering is done in the response serializer if needed
        val result = HttpRequestTranslator.translateFetchOffsets(group, null)
        val request = result.builder().build()
        val version = request.version()
        (ApiKeys.OFFSET_FETCH, version, request.serialize().buffer())
      }

      // Build request context and enqueue (same pattern as generic requests)
      val principal = buildPrincipal(ctx, req)
      val connectionId = ctx.channel().id().asLongText()
      val clientAddress = ctx.channel().remoteAddress().asInstanceOf[InetSocketAddress]
      val clientId = HttpRouter.validateClientId(req.headers().get("X-Kafka-Client-ID"))
      val correlationId = correlationIdCounter.getAndIncrement()

      val requestHeader = new RequestHeader(apiKey, apiVersion, clientId, correlationId)
      val requestContext = new RequestContext(
        requestHeader, connectionId, clientAddress.getAddress,
        Optional.of(Integer.valueOf(clientAddress.getPort)),
        principal, ListenerName.normalised("HTTP"), securityProtocol,
        ClientInformation.EMPTY, false
      )

      if (buffer.position() != 0) buffer.rewind()

      val channelRequest = new RequestChannel.Request(
        processor = httpProcessor.id(),
        context = requestContext,
        startTimeNanos = Time.SYSTEM.nanoseconds(),
        memoryPool = MemoryPool.NONE,
        buffer = buffer,
        metrics = requestChannel.metrics,
        envelope = None
      )

      httpProcessor.registerChannel(connectionId, ctx)
      val enqueued = requestChannel.tryEnqueue(channelRequest)
      if (!enqueued) {
        httpProcessor.unregisterChannel(connectionId)
        sendErrorResponse(ctx, HttpResponseStatus.SERVICE_UNAVAILABLE,
          """{"errorCode":-1,"errorMessage":"Request queue full","detail":"SERVICE_UNAVAILABLE"}""")
        inFlightCount.decrementAndGet()
      }
    } catch {
      case e: org.apache.kafka.common.errors.InvalidRequestException =>
        sendErrorResponse(ctx, HttpResponseStatus.BAD_REQUEST,
          s"""{"errorCode":-1,"errorMessage":"${escapeJson(e.getMessage)}"}""")
        inFlightCount.decrementAndGet()
      case e: Exception =>
        sendErrorResponse(ctx, HttpResponseStatus.INTERNAL_SERVER_ERROR,
          s"""{"errorCode":-1,"errorMessage":"${escapeJson(e.getMessage)}"}""")
        inFlightCount.decrementAndGet()
    }
  }

  /**
   * Handles share group poll (SHARE_POLL) and acknowledge (SHARE_ACKNOWLEDGE) requests.
   *
   * SHARE_POLL body format:
   * {
   *   "topics": ["topic1", "topic2"],
   *   "maxRecords": 10,
   *   "maxWaitMs": 5000
   * }
   *
   * SHARE_ACKNOWLEDGE body format:
   * {
   *   "acknowledgements": [
   *     {"acquireId": "topic:partition:offset", "type": "ACCEPT|REJECT|RELEASE"}
   *   ]
   * }
   */
  private[network] def handleShareRequest(
    ctx: ChannelHandlerContext,
    req: FullHttpRequest,
    routeResult: HttpRouter.RouteResult
  ): Unit = {
    val groupId = routeResult.groupId()
    val isPoll = routeResult.handlerType() == HttpRouter.HandlerType.SHARE_POLL

    try {
      val bodyBytes = if (req.content().isReadable) {
        val bytes = new Array[Byte](req.content().readableBytes())
        req.content().readBytes(bytes)
        bytes
      } else {
        null
      }
      if (bodyBytes == null || bodyBytes.isEmpty) {
        sendErrorResponse(ctx, HttpResponseStatus.BAD_REQUEST,
          s"""{"errorCode":-1,"errorMessage":"Request body is required for share group ${if (isPoll) "poll" else "acknowledge"} requests"}""")
        inFlightCount.decrementAndGet()
        return
      }

      val json = objectMapper.readTree(bodyBytes)

      // Use a deterministic memberId based on the group ID so the same member
      // is reused across HTTP requests to the same share group.
      val memberId = org.apache.kafka.common.Uuid.fromString(
        java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(
          java.security.MessageDigest.getInstance("MD5").digest(
            s"http-share-$groupId".getBytes(StandardCharsets.UTF_8)
          ).take(16)
        ).take(22)  // Uuid fromString expects 22-char base64
      )

      // For share poll, register the member with the share group coordinator
      // via heartbeat, then send the fetch with epoch progression to acquire records.
      if (isPoll) {
        val topicsNode = json.get("topics")
        val topicNames = new java.util.ArrayList[String]()
        if (topicsNode != null && topicsNode.isArray) {
          val iter = topicsNode.elements()
          while (iter.hasNext) {
            topicNames.add(iter.next().asText())
          }
        }

        // Send heartbeat to register the member with the group coordinator
        // and wait for partition assignment (up to 5 seconds)
        val deadline = System.currentTimeMillis() + 5000
        var assigned = false
        while (!assigned && System.currentTimeMillis() < deadline) {
          val hbResponse = sendInternalHeartbeat(ctx, req, groupId, memberId, topicNames)
          if (hbResponse != null) {
            val hbData = hbResponse.asInstanceOf[org.apache.kafka.common.requests.ShareGroupHeartbeatResponse].data()
            if (hbData.assignment() != null && !hbData.assignment().topicPartitions().isEmpty) {
              assigned = true
            }
          }
          if (!assigned) Thread.sleep(300)
        }

        // Share group protocol requires multi-step session initialization:
        //   epoch=0: opens a new share session (initializes share partitions, returns empty)
        //   epoch=1+: subsequent fetches that actually acquire and return records
        // We send the initial fetch internally, then retry with incrementing epochs
        // until records are acquired or we exhaust retries.
        val topicIdNames = {
          val (_, _, _, names) = buildShareFetchRequest(groupId, json, memberId, 0)
          names
        }

        var fetchResponse: org.apache.kafka.common.requests.ShareFetchResponse = null
        var hasRecords = false
        val maxRetries = 5
        var epoch = 0

        var retry = 0
        while (retry < maxRetries && !hasRecords) {
          fetchResponse = sendInternalShareFetch(ctx, req, groupId, json, memberId, epoch, topicIdNames)
          if (fetchResponse != null) {
            hasRecords = shareFetchResponseHasRecords(fetchResponse)
          }
          if (!hasRecords) {
            epoch += 1
            if (retry < maxRetries - 1) Thread.sleep(200)
          }
          retry += 1
        }

        // Serialize the final response and send directly to the client
        if (fetchResponse != null) {
          val httpResponse = kafka.server.http.HttpResponseSerializer.serialize(
            fetchResponse, null, ApiKeys.SHARE_FETCH, -1, topicIdNames)
          ctx.writeAndFlush(httpResponse)
        } else {
          // No response received at all — return an empty records response
          sendErrorResponse(ctx, HttpResponseStatus.OK,
            """{"records":[]}""")
        }
        inFlightCount.decrementAndGet()
        return
      }

      // --- Acknowledge path ---
      // Acknowledgements are piggybacked on a ShareFetch request because the
      // share session (memberId + epoch) must match. The HTTP protocol is
      // stateless, so we reuse the same deterministic memberId used for poll
      // and let the internal fetch mechanism find the right epoch.
      val ackResult = sendInternalShareAcknowledge(ctx, req, groupId, json, memberId)
      if (ackResult != null) {
        val topicIdNames = new java.util.HashMap[String, String]()
        // Populate topic ID names from the acknowledge response
        val ackRespIter = ackResult.data().responses().iterator()
        while (ackRespIter.hasNext) {
          val topicResp = ackRespIter.next()
          if (topicIdSupplier != null) {
            // Try to resolve topic name from topic ID
            val topicId = topicResp.topicId()
            if (topicId != null) {
              topicIdNames.put(topicId.toString, topicId.toString) // fallback
            }
          }
        }
        val httpResponse = kafka.server.http.HttpResponseSerializer.serialize(
          ackResult, null, ApiKeys.SHARE_FETCH, -1, topicIdNames)
        ctx.writeAndFlush(httpResponse)
      } else {
        sendErrorResponse(ctx, HttpResponseStatus.INTERNAL_SERVER_ERROR,
          """{"errorCode":-1,"errorMessage":"Failed to acknowledge records"}""")
      }
      inFlightCount.decrementAndGet()
    } catch {
      case e: org.apache.kafka.common.errors.InvalidRequestException =>
        sendErrorResponse(ctx, HttpResponseStatus.BAD_REQUEST,
          s"""{"errorCode":-1,"errorMessage":"${escapeJson(e.getMessage)}"}""")
        inFlightCount.decrementAndGet()
      case e: Exception =>
        sendErrorResponse(ctx, HttpResponseStatus.INTERNAL_SERVER_ERROR,
          s"""{"errorCode":-1,"errorMessage":"${escapeJson(e.getMessage)}"}""")
        inFlightCount.decrementAndGet()
    }
  }

  /**
   * Sends a ShareGroupHeartbeat request internally and waits for the response.
   * This is needed to register the member with the share group coordinator before
   * the first share fetch can return records.
   *
   * The heartbeat is sent through the RequestChannel and the response is captured
   * via the HttpProcessor's internal callback mechanism.
   */
  private def sendInternalHeartbeat(
    ctx: ChannelHandlerContext,
    req: FullHttpRequest,
    groupId: String,
    memberId: org.apache.kafka.common.Uuid,
    topicNames: java.util.List[String]
  ): org.apache.kafka.common.requests.AbstractResponse = {
    val hbData = new ShareGroupHeartbeatRequestData()
      .setGroupId(groupId)
      .setMemberId(memberId.toString)
      .setMemberEpoch(0)
      .setSubscribedTopicNames(topicNames)

    val hbVersion: Short = ApiKeys.SHARE_GROUP_HEARTBEAT.latestVersion()
    val hbRequest = new ShareGroupHeartbeatRequest.Builder(hbData).build(hbVersion)
    val hbBuffer = hbRequest.serialize().buffer()

    val principal = buildPrincipal(ctx, req)
    val internalConnectionId = "internal-hb-" + java.util.UUID.randomUUID().toString
    val clientAddress = ctx.channel().remoteAddress().asInstanceOf[InetSocketAddress]
    val clientId = HttpRouter.validateClientId(req.headers().get("X-Kafka-Client-ID"))
    val correlationId = correlationIdCounter.getAndIncrement()

    val hbHeader = new RequestHeader(ApiKeys.SHARE_GROUP_HEARTBEAT, hbVersion, clientId, correlationId)
    val hbContext = new RequestContext(
      hbHeader, internalConnectionId, clientAddress.getAddress,
      Optional.of(Integer.valueOf(clientAddress.getPort)),
      principal, ListenerName.normalised("HTTP"), securityProtocol,
      ClientInformation.EMPTY, false
    )

    if (hbBuffer.position() != 0) hbBuffer.rewind()

    val hbChannelRequest = new RequestChannel.Request(
      processor = httpProcessor.id(),
      context = hbContext,
      startTimeNanos = Time.SYSTEM.nanoseconds(),
      memoryPool = MemoryPool.NONE,
      buffer = hbBuffer,
      metrics = requestChannel.metrics,
      envelope = None
    )

    // Register internal callback to capture the heartbeat response
    val responseFuture = httpProcessor.registerInternalCallback(internalConnectionId)
    inFlightCount.incrementAndGet()

    val enqueued = requestChannel.tryEnqueue(hbChannelRequest)
    if (!enqueued) {
      inFlightCount.decrementAndGet()
      return null // Silently skip heartbeat if queue is full
    }

    // Wait for heartbeat response (up to 5 seconds)
    try {
      val response = responseFuture.get(5, TimeUnit.SECONDS)
      return response
    } catch {
      case _: Exception => return null
    }
  }

  /**
   * Sends a ShareFetch request internally and waits for the response.
   * Used to implement the multi-step share session initialization:
   *   epoch=0 opens the session, epoch=1+ acquires records.
   *
   * Uses the same internal callback mechanism as sendInternalHeartbeat.
   * A stable connectionId (derived from groupId) is used so that the
   * SharePartitionManager can track the share session across fetches.
   */
  private def sendInternalShareFetch(
    ctx: ChannelHandlerContext,
    req: FullHttpRequest,
    groupId: String,
    json: JsonNode,
    memberId: org.apache.kafka.common.Uuid,
    epoch: Int,
    topicIdNames: java.util.Map[String, String]
  ): org.apache.kafka.common.requests.ShareFetchResponse = {
    val (_, apiVersion, buffer, _) = buildShareFetchRequest(groupId, json, memberId, epoch)

    val principal = buildPrincipal(ctx, req)
    // Use a stable connectionId for the share session so the SharePartitionManager
    // can correlate epoch=0 (session open) with epoch=1+ (record fetch).
    val internalConnectionId = "internal-share-fetch-" + groupId + "-" + memberId.toString
    val clientAddress = ctx.channel().remoteAddress().asInstanceOf[InetSocketAddress]
    val clientId = HttpRouter.validateClientId(req.headers().get("X-Kafka-Client-ID"))
    val correlationId = correlationIdCounter.getAndIncrement()

    val header = new RequestHeader(ApiKeys.SHARE_FETCH, apiVersion, clientId, correlationId)
    val context = new RequestContext(
      header, internalConnectionId, clientAddress.getAddress,
      Optional.of(Integer.valueOf(clientAddress.getPort)),
      principal, ListenerName.normalised("HTTP"), securityProtocol,
      ClientInformation.EMPTY, false
    )

    if (buffer.position() != 0) buffer.rewind()

    val channelRequest = new RequestChannel.Request(
      processor = httpProcessor.id(),
      context = context,
      startTimeNanos = Time.SYSTEM.nanoseconds(),
      memoryPool = MemoryPool.NONE,
      buffer = buffer,
      metrics = requestChannel.metrics,
      envelope = None
    )

    // Store topic ID -> name mapping so KafkaApis can resolve topic IDs
    if (topicIdNames != null && !topicIdNames.isEmpty) {
      channelRequest.requestLocalProperties.put("httpTopicIdNames", topicIdNames)
    }

    val responseFuture = httpProcessor.registerInternalCallback(internalConnectionId)
    inFlightCount.incrementAndGet()

    val enqueued = requestChannel.tryEnqueue(channelRequest)
    if (!enqueued) {
      inFlightCount.decrementAndGet()
      return null
    }

    try {
      val response = responseFuture.get(10, TimeUnit.SECONDS)
      if (response != null) {
        response.asInstanceOf[org.apache.kafka.common.requests.ShareFetchResponse]
      } else {
        null
      }
    } catch {
      case _: Exception => null
    }
  }

  /**
   * Checks if a ShareFetchResponse contains any records.
   * Iterates through all topic-partitions and checks for non-empty record batches.
   */
  private def shareFetchResponseHasRecords(
    response: org.apache.kafka.common.requests.ShareFetchResponse
  ): Boolean = {
    val iter = response.data().responses().iterator()
    while (iter.hasNext) {
      val topicResponse = iter.next()
      val partIter = topicResponse.partitions().iterator()
      while (partIter.hasNext) {
        val partData = partIter.next()
        val baseRecords = partData.records()
        if (baseRecords != null) {
          baseRecords match {
            case records: org.apache.kafka.common.record.internal.Records =>
              val batchIter = records.batches().iterator()
              if (batchIter.hasNext) return true
            case _ => // not Records type, skip
          }
        }
      }
    }
    false
  }

  /**
   * Sends an acknowledge request by piggybacking acknowledgements on a ShareFetch
   * request. This is necessary because the HTTP protocol is stateless but the share
   * session requires a matching memberId and epoch.
   *
   * The method tries multiple epochs to find the current session epoch, since we
   * don't track state across HTTP requests.
   */
  private def sendInternalShareAcknowledge(
    ctx: ChannelHandlerContext,
    req: FullHttpRequest,
    groupId: String,
    json: JsonNode,
    memberId: org.apache.kafka.common.Uuid
  ): org.apache.kafka.common.requests.ShareFetchResponse = {
    // Parse acknowledgements from the JSON body
    val acksNode = json.get("acknowledgements")
    if (acksNode == null || !acksNode.isArray || acksNode.size() == 0) {
      return null
    }

    // Build the acknowledgements map: TopicIdPartition -> List[AcknowledgementBatch]
    val ackMap = new java.util.LinkedHashMap[
      org.apache.kafka.common.TopicIdPartition,
      java.util.List[org.apache.kafka.common.message.ShareFetchRequestData.AcknowledgementBatch]
    ]()

    val acksIter = acksNode.elements()
    while (acksIter.hasNext) {
      val ackEntry = acksIter.next()
      val acquireId = ackEntry.get("acquireId").asText()
      val ackTypeStr = ackEntry.get("type").asText()

      // Parse acquireId: "topic:partition:offset"
      val parts = acquireId.split(":")
      if (parts.length < 3) return null

      val topicName = parts.dropRight(2).mkString(":")
      val partition = try { parts(parts.length - 2).toInt } catch { case _: NumberFormatException => return null }
      val offset = try { parts(parts.length - 1).toLong } catch { case _: NumberFormatException => return null }

      val ackType: Byte = ackTypeStr match {
        case "ACCEPT" => 1
        case "RELEASE" => 2
        case "REJECT" => 3
        case _ => return null
      }

      val topicId = if (topicIdSupplier != null) topicIdSupplier.apply(topicName) else null
      if (topicId == null || topicId.equals(org.apache.kafka.common.Uuid.ZERO_UUID)) return null

      val tip = new org.apache.kafka.common.TopicIdPartition(
        topicId, new org.apache.kafka.common.TopicPartition(topicName, partition))

      val batch = new org.apache.kafka.common.message.ShareFetchRequestData.AcknowledgementBatch()
        .setFirstOffset(offset)
        .setLastOffset(offset)
        .setAcknowledgeTypes(java.util.Collections.singletonList(java.lang.Byte.valueOf(ackType)))

      val batches = ackMap.computeIfAbsent(tip, _ =>
        new java.util.ArrayList[org.apache.kafka.common.message.ShareFetchRequestData.AcknowledgementBatch]())
      batches.add(batch)
    }

    // Build a ShareFetch request with piggybacked acknowledgements.
    // Use maxRecords=0 and maxWaitMs=0 since we only want to acknowledge.
    // Try multiple epochs to find the right one.
    val maxEpochRetries = 10
    var epoch = 1 // Start from 1 since 0 = INITIAL_EPOCH (creates new session)
    var result: org.apache.kafka.common.requests.ShareFetchResponse = null

    while (epoch <= maxEpochRetries && result == null) {
      val metadata = new ShareRequestMetadata(memberId, epoch)
      val version: Short = 1.toShort

      val builder = ShareFetchRequest.Builder.forConsumer(
        groupId,
        metadata,
        0, // maxWaitMs - no waiting, just acknowledge
        0, // minBytes
        0, // maxBytes
        0, // maxRecords - no records needed
        0, // batchSize
        0.toByte, // shareAcquireMode
        false, // isRenewAck
        java.util.Collections.emptyList(), // send - no new partitions
        java.util.Collections.emptyList(), // forget
        ackMap // piggybacked acknowledgements
      )

      val request = builder.build(version)
      val buffer = request.serialize().buffer()

      val principal = buildPrincipal(ctx, req)
      val internalConnectionId = "internal-share-fetch-" + groupId + "-" + memberId.toString
      val clientAddress = ctx.channel().remoteAddress().asInstanceOf[InetSocketAddress]
      val clientId = HttpRouter.validateClientId(req.headers().get("X-Kafka-Client-ID"))
      val correlationId = correlationIdCounter.getAndIncrement()

      val header = new RequestHeader(ApiKeys.SHARE_FETCH, version, clientId, correlationId)
      val context = new RequestContext(
        header, internalConnectionId, clientAddress.getAddress,
        Optional.of(Integer.valueOf(clientAddress.getPort)),
        principal, ListenerName.normalised("HTTP"), securityProtocol,
        ClientInformation.EMPTY, false
      )

      if (buffer.position() != 0) buffer.rewind()

      val channelRequest = new RequestChannel.Request(
        processor = httpProcessor.id(),
        context = context,
        startTimeNanos = Time.SYSTEM.nanoseconds(),
        memoryPool = MemoryPool.NONE,
        buffer = buffer,
        metrics = requestChannel.metrics,
        envelope = None
      )

      val responseFuture = httpProcessor.registerInternalCallback(internalConnectionId)
      inFlightCount.incrementAndGet()

      val enqueued = requestChannel.tryEnqueue(channelRequest)
      if (!enqueued) {
        inFlightCount.decrementAndGet()
        epoch += 1
      } else {
        try {
          val response = responseFuture.get(5, TimeUnit.SECONDS)
          if (response != null) {
            val fetchResp = response.asInstanceOf[org.apache.kafka.common.requests.ShareFetchResponse]
            // If the response has no error (or only partition-level errors, not session errors),
            // consider it successful
            if (fetchResp.error() == org.apache.kafka.common.protocol.Errors.NONE) {
              result = fetchResp
            } else if (fetchResp.error() == org.apache.kafka.common.protocol.Errors.INVALID_SHARE_SESSION_EPOCH ||
                       fetchResp.error() == org.apache.kafka.common.protocol.Errors.SHARE_SESSION_NOT_FOUND) {
              // Wrong epoch, try next
              epoch += 1
            } else {
              // Some other error - return it
              result = fetchResp
            }
          } else {
            epoch += 1
          }
        } catch {
          case _: Exception =>
            epoch += 1
        }
      }
    }

    result
  }

  /**
   * Builds a ShareFetchRequest from the JSON body.
   *
   * Topics in the body are resolved to topic IDs via the topicIdSupplier.
   * Each topic's partition count is obtained from metadataSupplier to add
   * all partitions for the topic to the fetch request.
   *
   * Returns (apiKey, version, serializedBuffer, topicIdNamesMap).
   */
  private def buildShareFetchRequest(groupId: String, json: JsonNode, memberId: org.apache.kafka.common.Uuid, epoch: Int): (ApiKeys, Short, java.nio.ByteBuffer, java.util.Map[String, String]) = {
    val topicsNode = json.get("topics")
    if (topicsNode == null || !topicsNode.isArray || topicsNode.size() == 0) {
      throw new org.apache.kafka.common.errors.InvalidRequestException(
        "'topics' array is required and must not be empty in share poll request")
    }

    val maxWaitMs = if (json.has("maxWaitMs")) json.get("maxWaitMs").asInt() else 5000
    val maxRecords = if (json.has("maxRecords")) json.get("maxRecords").asInt() else 100

    // Build the list of TopicIdPartition to fetch and the topic ID -> name mapping
    val topicPartitions = new java.util.ArrayList[org.apache.kafka.common.TopicIdPartition]()
    val topicIdNames = new java.util.HashMap[String, String]()
    val topicsIter = topicsNode.elements()
    while (topicsIter.hasNext) {
      val topicNameNode = topicsIter.next()
      val topicName = topicNameNode.asText()

      // Resolve topic name to topic ID
      val topicId = if (topicIdSupplier != null) {
        topicIdSupplier.apply(topicName)
      } else {
        org.apache.kafka.common.Uuid.ZERO_UUID
      }

      if (topicId == null || topicId.equals(org.apache.kafka.common.Uuid.ZERO_UUID)) {
        throw new org.apache.kafka.common.errors.InvalidRequestException(
          s"Topic '$topicName' not found")
      }

      // Store topic ID -> name mapping for response serialization
      topicIdNames.put(topicId.toString, topicName)

      // Get partition count for the topic
      val partitionCount = if (metadataSupplier != null) {
        metadataSupplier.apply(topicName)
      } else {
        throw new org.apache.kafka.common.errors.InvalidRequestException(
          s"Cannot resolve partitions for topic '$topicName'")
      }

      for (p <- 0 until partitionCount) {
        topicPartitions.add(new org.apache.kafka.common.TopicIdPartition(
          topicId, new org.apache.kafka.common.TopicPartition(topicName, p)))
      }
    }

    val metadata = new ShareRequestMetadata(
      memberId, // memberId from heartbeat registration
      epoch // shareSessionEpoch (0 for init, 1 for actual fetch)
    )

    val version: Short = 1.toShort  // Use version 1 (stable KIP-932) instead of latest
    val builder = ShareFetchRequest.Builder.forConsumer(
      groupId,
      metadata,
      maxWaitMs,
      1, // minBytes
      Integer.MAX_VALUE, // maxBytes
      maxRecords,
      100, // batchSize
      0.toByte, // shareAcquireMode = BATCH_OPTIMIZED
      false, // isRenewAck
      topicPartitions,
      java.util.Collections.emptyList(), // forget
      java.util.Collections.emptyMap() // acknowledgements
    )

    val request = builder.build(version)
    val buffer = request.serialize().buffer()
    (ApiKeys.SHARE_FETCH, version, buffer, topicIdNames.asInstanceOf[java.util.Map[String, String]])
  }

  /**
   * Converts a nested topic/partition offset format to the flat format expected
   * by HttpRequestTranslator.translateCommitOffsets.
   *
   * Nested: {"topics": [{"topic":"t","partitions":[{"partition":0,"offset":3}]}]}
   * Flat:   {"offsets": [{"topic":"t","partition":0,"offset":3}]}
   *
   * If the input already has "offsets" at top level, returns as-is.
   */
  private def convertToFlatOffsetFormat(json: JsonNode): JsonNode = {
    if (json.has("offsets")) return json // Already flat format

    val topicsNode = json.get("topics")
    if (topicsNode == null || !topicsNode.isArray) {
      throw new org.apache.kafka.common.errors.InvalidRequestException(
        "'offsets' or 'topics' array is required in offset commit body")
    }

    val flatRoot = objectMapper.createObjectNode()
    val offsetsArray = flatRoot.putArray("offsets")

    val topicsIter = topicsNode.elements()
    while (topicsIter.hasNext) {
      val topicEntry = topicsIter.next()
      val topicName = topicEntry.get("topic").asText()
      val partitionsNode = topicEntry.get("partitions")
      if (partitionsNode != null && partitionsNode.isArray) {
        val partIter = partitionsNode.elements()
        while (partIter.hasNext) {
          val partEntry = partIter.next()
          val flat = offsetsArray.addObject()
          flat.put("topic", topicName)
          flat.put("partition", partEntry.get("partition").asInt())
          flat.put("offset", partEntry.get("offset").asLong())
          if (partEntry.has("metadata")) {
            flat.put("metadata", partEntry.get("metadata").asText())
          }
        }
      }
    }

    flatRoot
  }

  /**
   * WS2.06 — True for any HandlerType managed by the REST routing handlers
   * (exchange/queue/binding CRUD).
   */
  private def isRestRoutingHandler(t: HttpRouter.HandlerType): Boolean = t match {
    case HttpRouter.HandlerType.DECLARE_EXCHANGE |
         HttpRouter.HandlerType.GET_EXCHANGE |
         HttpRouter.HandlerType.LIST_EXCHANGES |
         HttpRouter.HandlerType.DELETE_EXCHANGE |
         HttpRouter.HandlerType.DECLARE_QUEUE |
         HttpRouter.HandlerType.GET_QUEUE |
         HttpRouter.HandlerType.LIST_QUEUES |
         HttpRouter.HandlerType.PATCH_QUEUE |
         HttpRouter.HandlerType.DELETE_QUEUE |
         HttpRouter.HandlerType.PURGE_QUEUE |
         HttpRouter.HandlerType.CREATE_BINDING |
         HttpRouter.HandlerType.LIST_BINDINGS |
         HttpRouter.HandlerType.DELETE_BINDING |
         HttpRouter.HandlerType.LIST_CONNECTIONS |
         HttpRouter.HandlerType.GET_CONNECTION |
         HttpRouter.HandlerType.FORCE_CLOSE_CONNECTION |
         HttpRouter.HandlerType.LIST_CONSUMERS |
         HttpRouter.HandlerType.FORCE_CANCEL_CONSUMER |
         HttpRouter.HandlerType.PUBLISH_VIA_EXCHANGE |
         HttpRouter.HandlerType.QUEUE_GET |
         HttpRouter.HandlerType.QUEUE_ACK |
         HttpRouter.HandlerType.QUEUE_NACK |
         // WS2.09 vhost admin
         HttpRouter.HandlerType.LIST_VHOSTS |
         HttpRouter.HandlerType.CREATE_VHOST |
         HttpRouter.HandlerType.DELETE_VHOST => true
    case _ => false
  }

  /**
   * WS2.06 — Dispatches an already-routed REST routing request to the
   * appropriate handler instance. If the handler is not wired (typical during
   * early phases), responds with 501 Not Implemented.
   */
  private def handleRestRoutingRequest(
    ctx: ChannelHandlerContext,
    req: FullHttpRequest,
    routeResult: HttpRouter.RouteResult
  ): Unit = {
    val vhostHeader = req.headers().get("X-Vhost")
    val vhost = if (vhostHeader == null || vhostHeader.isEmpty) "/" else vhostHeader
    val qp = routeResult.queryParams()
    val ifUnused = java.lang.Boolean.parseBoolean(qp.getOrDefault("ifUnused", "false"))
    val ifEmpty = java.lang.Boolean.parseBoolean(qp.getOrDefault("ifEmpty", "false"))

    val response: io.netty.handler.codec.http.FullHttpResponse = routeResult.handlerType() match {
      case HttpRouter.HandlerType.DECLARE_EXCHANGE =>
        if (exchangeRestHandler == null) notWired("ExchangeRestHandler")
        else exchangeRestHandler.handleDeclare(routeResult.resourceName(), vhost, req)

      case HttpRouter.HandlerType.GET_EXCHANGE =>
        if (exchangeRestHandler == null) notWired("ExchangeRestHandler")
        else exchangeRestHandler.handleGet(routeResult.resourceName(), vhost)

      case HttpRouter.HandlerType.LIST_EXCHANGES =>
        if (exchangeRestHandler == null) notWired("ExchangeRestHandler")
        else exchangeRestHandler.handleList(vhost)

      case HttpRouter.HandlerType.DELETE_EXCHANGE =>
        if (exchangeRestHandler == null) notWired("ExchangeRestHandler")
        else exchangeRestHandler.handleDelete(routeResult.resourceName(), vhost, ifUnused)

      case HttpRouter.HandlerType.DECLARE_QUEUE =>
        if (queueRestHandler == null) notWired("QueueRestHandler")
        else queueRestHandler.handleDeclare(routeResult.resourceName(), vhost, req)

      case HttpRouter.HandlerType.GET_QUEUE =>
        if (queueRestHandler == null) notWired("QueueRestHandler")
        else queueRestHandler.handleGet(routeResult.resourceName(), vhost)

      case HttpRouter.HandlerType.LIST_QUEUES =>
        if (queueRestHandler == null) notWired("QueueRestHandler")
        else queueRestHandler.handleList(vhost)

      case HttpRouter.HandlerType.DELETE_QUEUE =>
        if (queueRestHandler == null) notWired("QueueRestHandler")
        else queueRestHandler.handleDelete(routeResult.resourceName(), vhost, ifUnused, ifEmpty)

      case HttpRouter.HandlerType.CREATE_BINDING =>
        if (bindingRestHandler == null) notWired("BindingRestHandler")
        else bindingRestHandler.handleCreate(vhost, req)

      case HttpRouter.HandlerType.LIST_BINDINGS =>
        if (bindingRestHandler == null) notWired("BindingRestHandler")
        else bindingRestHandler.handleList(vhost, qp.get("exchange"), qp.get("queue"))

      case HttpRouter.HandlerType.DELETE_BINDING =>
        if (bindingRestHandler == null) notWired("BindingRestHandler")
        else bindingRestHandler.handleDelete(vhost, req)

      case HttpRouter.HandlerType.PATCH_QUEUE | HttpRouter.HandlerType.PURGE_QUEUE =>
        notWired("QueueRestHandler (patch/purge not implemented in WS2.06)")

      // --- WS2.07: connection management ---
      case HttpRouter.HandlerType.LIST_CONNECTIONS =>
        if (connectionRestHandler == null) notWired("ConnectionRestHandler")
        else connectionRestHandler.handleList()

      case HttpRouter.HandlerType.GET_CONNECTION =>
        if (connectionRestHandler == null) notWired("ConnectionRestHandler")
        else connectionRestHandler.handleGet(routeResult.resourceName())

      case HttpRouter.HandlerType.FORCE_CLOSE_CONNECTION =>
        if (connectionRestHandler == null) notWired("ConnectionRestHandler")
        else connectionRestHandler.handleForceClose(routeResult.resourceName(), req)

      // --- WS2.07: consumer management ---
      case HttpRouter.HandlerType.LIST_CONSUMERS =>
        if (consumerRestHandler == null) notWired("ConsumerRestHandler")
        else consumerRestHandler.handleList(qp.get("queue"))

      case HttpRouter.HandlerType.FORCE_CANCEL_CONSUMER =>
        if (consumerRestHandler == null) notWired("ConsumerRestHandler")
        // consumerGroup slot holds connectionId, resourceName holds subscriptionId
        else consumerRestHandler.handleForceCancel(
          routeResult.consumerGroup(), routeResult.resourceName())

      // --- WS2.08: Message operations ---
      case HttpRouter.HandlerType.PUBLISH_VIA_EXCHANGE =>
        if (messageRestHandler == null) notWired("MessageRestHandler")
        else messageRestHandler.handlePublish(routeResult.resourceName(), vhost, req)

      case HttpRouter.HandlerType.QUEUE_GET =>
        if (messageRestHandler == null) notWired("MessageRestHandler")
        else messageRestHandler.handleGet(routeResult.resourceName(), vhost, req)

      case HttpRouter.HandlerType.QUEUE_ACK =>
        if (messageRestHandler == null) notWired("MessageRestHandler")
        else messageRestHandler.handleAck(routeResult.resourceName(), vhost, req)

      case HttpRouter.HandlerType.QUEUE_NACK =>
        if (messageRestHandler == null) notWired("MessageRestHandler")
        else messageRestHandler.handleNack(routeResult.resourceName(), vhost, req)

      // WS2.09: vhost admin — vhost is identified by the path segment, not the
      // X-Vhost header (which scopes all other REST requests).
      case HttpRouter.HandlerType.LIST_VHOSTS =>
        if (vhostRestHandler == null) notWired("VhostRestHandler")
        else vhostRestHandler.handleList()

      case HttpRouter.HandlerType.CREATE_VHOST =>
        if (vhostRestHandler == null) notWired("VhostRestHandler")
        else vhostRestHandler.handleCreate(routeResult.resourceName())

      case HttpRouter.HandlerType.DELETE_VHOST =>
        if (vhostRestHandler == null) notWired("VhostRestHandler")
        else vhostRestHandler.handleDelete(routeResult.resourceName())

      case _ =>
        notWired("Unknown REST routing handler")
    }

    ctx.writeAndFlush(response)
  }

  private def notWired(which: String): io.netty.handler.codec.http.FullHttpResponse = {
    val body = s"""{"errorCode":-1,"errorMessage":"$which not wired in this broker"}"""
      .getBytes(StandardCharsets.UTF_8)
    val response = new DefaultFullHttpResponse(
      HttpVersion.HTTP_1_1, HttpResponseStatus.NOT_IMPLEMENTED,
      Unpooled.wrappedBuffer(body))
    response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json")
    response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, body.length)
    response
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
