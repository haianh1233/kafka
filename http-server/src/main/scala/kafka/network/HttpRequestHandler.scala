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
// Time: Created - TASK-B.05
package kafka.network

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.{Function => JFunction}

import io.netty.buffer.Unpooled
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.http._
import io.netty.handler.codec.http.HttpResponseStatus._
import io.netty.handler.codec.http.HttpVersion.HTTP_1_1
import kafka.server.http.{HttpProcessor, HttpRequestTranslator, HttpRouter, HttpServerConfigs}
import org.apache.kafka.common.Endpoint
import org.apache.kafka.common.errors.{InvalidRequestException, InvalidTopicException}
import org.apache.kafka.common.memory.MemoryPool
import org.apache.kafka.common.network.{ClientInformation, ListenerName}
import org.apache.kafka.common.requests.{RequestContext, RequestHeader}
import org.apache.kafka.common.security.auth.{HttpAuthenticationContext, KafkaPrincipalBuilder, SecurityProtocol}
import org.apache.kafka.metadata.BrokerState
import org.slf4j.LoggerFactory

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
 */
class HttpRequestHandler(
    requestChannel: RequestChannel,
    httpProcessor: HttpProcessor,
    principalBuilder: KafkaPrincipalBuilder,
    endpoint: Endpoint,
    router: HttpRouter,
    translator: HttpRequestTranslator,
    httpServerConfigs: HttpServerConfigs,
    metadataSupplier: JFunction[String, Integer],
    httpAcceptor: HttpAcceptor,
    brokerState: () => BrokerState,
    brokerId: Int,
    clusterId: String
) extends io.netty.channel.SimpleChannelInboundHandler[FullHttpRequest] {

  private val log = LoggerFactory.getLogger(classOf[HttpRequestHandler])

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
   * MUST NOT BLOCK -- all I/O is async via RequestChannel and HttpProcessor.
   */
  override def channelRead0(ctx: ChannelHandlerContext, req: FullHttpRequest): Unit = {
    // Step 0 -- Check drain mode
    if (!httpAcceptor.isAccepting) {
      sendErrorResponse(ctx, SERVICE_UNAVAILABLE,
        """{"errorCode":"SHUTTING_DOWN","errorMessage":"Broker is shutting down"}""",
        generateRequestId(req))
      return
    }

    // Step 1 -- Generate or accept request ID
    val requestId = generateRequestId(req)

    // Step 2 -- Route the request
    val routeResult = try {
      router.route(req.method(), req.uri())
    } catch {
      case e: InvalidRequestException =>
        sendErrorResponse(ctx, NOT_FOUND, buildErrorJson(e.getMessage), requestId)
        return
      case e: InvalidTopicException =>
        sendErrorResponse(ctx, BAD_REQUEST, buildErrorJson(e.getMessage), requestId)
        return
    }

    // Step 3 -- Health check short-circuit
    if (routeResult.handlerType() == HttpRouter.HandlerType.HEALTH) {
      sendHealthResponse(ctx, requestId)
      return
    }

    // Step 4 -- Validate client ID
    val clientId = HttpRouter.validateClientId(req.headers().get(HEADER_CLIENT_ID))

    // Step 5 -- Translate HTTP request to Kafka protocol request
    val body = extractBody(req)
    val translationResult = try {
      translator.translate(routeResult, body, httpServerConfigs, metadataSupplier)
    } catch {
      case e: InvalidRequestException =>
        sendErrorResponse(ctx, BAD_REQUEST, buildErrorJson(e.getMessage), requestId)
        return
    }

    // Step 6 -- Build principal via KafkaPrincipalBuilder
    val clientAddress = ctx.channel().remoteAddress() match {
      case inet: InetSocketAddress => inet.getAddress
      case _ => java.net.InetAddress.getLoopbackAddress
    }
    val principal = principalBuilder.build(
      new HttpAuthenticationContext(
        clientAddress,
        listenerName.value(),
        securityProtocol,
        null, // bearerToken - extracted from headers in future task
        null, // basicCredentials - extracted from headers in future task
        null  // peerCertificates - from TLS handshake in future task
      )
    )

    // Step 7 -- Build RequestContext
    val correlationId = correlationIdCounter.getAndIncrement()
    val connectionId = ctx.channel().id().asLongText()
    val header = new RequestHeader(
      translationResult.apiKey(),
      translationResult.apiVersion(),
      clientId,
      correlationId)
    val requestContext = new RequestContext(
      header, connectionId, clientAddress, principal,
      listenerName, securityProtocol,
      ClientInformation.EMPTY, false)

    // Step 8 -- Build RequestChannel.Request
    val kafkaRequest = new RequestChannel.Request(
      processor = httpProcessor.id(),
      context = requestContext,
      startTimeNanos = System.nanoTime(),
      memoryPool = MemoryPool.NONE,
      buffer = translationResult.serializedRequest(),
      metrics = requestChannel.metrics,
      envelope = None)

    // Step 9 -- Register channel with HttpProcessor BEFORE enqueue
    //           (race condition: if request is processed before registration,
    //            response has nowhere to go)
    httpProcessor.registerChannel(connectionId, ctx)
    httpAcceptor.incrementPending()
    ctx.channel().closeFuture().addListener((_: io.netty.channel.ChannelFuture) => {
      httpAcceptor.decrementPending()
    })

    // Step 10 -- Non-blocking enqueue
    if (!requestChannel.tryEnqueue(kafkaRequest)) {
      httpProcessor.unregisterChannel(connectionId) // cleanup registration
      httpAcceptor.decrementPending()
      sendErrorResponse(ctx, SERVICE_UNAVAILABLE,
        """{"errorCode":"QUEUE_FULL","errorMessage":"Broker request queue saturated, retry later"}""",
        requestId, retryAfter = 1)
      return
    }
  }

  /**
   * Handles exceptions thrown during channel operations.
   * Logs the error and closes the channel, optionally sending a 500 response.
   */
  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    log.error("Exception in HTTP handler for {}", ctx.channel().remoteAddress(), cause)
    if (ctx.channel().isActive) {
      sendErrorResponse(ctx, INTERNAL_SERVER_ERROR,
        """{"errorCode":"UNKNOWN","errorMessage":"Internal server error"}""", "unknown")
    }
    ctx.close()
  }

  // --- Health check direct response ---

  /**
   * Sends a health check response directly without touching RequestChannel.
   * Returns 200 if broker is RUNNING, 503 otherwise.
   *
   * Response: {"status":"RUNNING","brokerId":N,"clusterId":"..."}
   */
  private def sendHealthResponse(ctx: ChannelHandlerContext, requestId: String): Unit = {
    val state = brokerState()
    val status = if (state == BrokerState.RUNNING) OK else SERVICE_UNAVAILABLE
    val body = s"""{"status":"${state.toString}","brokerId":$brokerId,"clusterId":"$clusterId"}"""
    sendJsonResponse(ctx, status, body, requestId)
  }

  // --- Request ID generation ---

  /**
   * Generates or accepts X-Kafka-Request-ID.
   * Accepts client-provided X-Request-ID if present and <= 64 chars.
   * Otherwise generates a UUID.
   */
  private def generateRequestId(req: FullHttpRequest): String = {
    val clientRequestId = req.headers().get(HEADER_REQUEST_ID_IN)
    if (clientRequestId != null && clientRequestId.length <= MAX_REQUEST_ID_LENGTH)
      clientRequestId
    else
      UUID.randomUUID().toString
  }

  // --- Body extraction ---

  /**
   * Extracts the request body as a byte array from a FullHttpRequest.
   */
  private def extractBody(req: FullHttpRequest): Array[Byte] = {
    val content = req.content()
    val bytes = new Array[Byte](content.readableBytes())
    content.readBytes(bytes)
    bytes
  }

  // --- Error response helpers ---

  /**
   * Sends a JSON error response with the given HTTP status.
   */
  private def sendErrorResponse(ctx: ChannelHandlerContext, status: HttpResponseStatus,
                                jsonBody: String, requestId: String,
                                retryAfter: Int = -1): Unit = {
    val bodyBytes = jsonBody.getBytes(StandardCharsets.UTF_8)
    val response = new DefaultFullHttpResponse(HTTP_1_1, status,
      Unpooled.copiedBuffer(bodyBytes))
    response.headers().set(HttpHeaderNames.CONTENT_TYPE, JSON_CONTENT_TYPE)
    response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, bodyBytes.length)
    response.headers().set(HEADER_REQUEST_ID_OUT, requestId)
    response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE)
    if (retryAfter > 0) {
      response.headers().setInt(HttpHeaderNames.RETRY_AFTER, retryAfter)
    }
    ctx.writeAndFlush(response)
  }

  /**
   * Sends a JSON response with the given HTTP status.
   */
  private def sendJsonResponse(ctx: ChannelHandlerContext, status: HttpResponseStatus,
                               jsonBody: String, requestId: String): Unit = {
    val bodyBytes = jsonBody.getBytes(StandardCharsets.UTF_8)
    val response = new DefaultFullHttpResponse(HTTP_1_1, status,
      Unpooled.copiedBuffer(bodyBytes))
    response.headers().set(HttpHeaderNames.CONTENT_TYPE, JSON_CONTENT_TYPE)
    response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, bodyBytes.length)
    response.headers().set(HEADER_REQUEST_ID_OUT, requestId)
    response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE)
    ctx.writeAndFlush(response)
  }

  /**
   * Builds a simple JSON error object string.
   */
  private def buildErrorJson(message: String): String = {
    // Escape message for JSON safety
    val escaped = message
      .replace("\\", "\\\\")
      .replace("\"", "\\\"")
      .replace("\n", "\\n")
      .replace("\r", "\\r")
      .replace("\t", "\\t")
    s"""{"errorCode":"INVALID_REQUEST","errorMessage":"$escaped"}"""
  }
}
