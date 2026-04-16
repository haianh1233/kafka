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

import io.netty.buffer.Unpooled
import io.netty.channel.embedded.EmbeddedChannel
import io.netty.handler.codec.http._
import kafka.server.http.{HttpProcessor, HttpRequestTranslator, HttpRouter, HttpServerConfigs}
import org.apache.kafka.common.Endpoint
import org.apache.kafka.common.message.ProduceRequestData
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.ProduceRequest
import org.apache.kafka.common.security.auth.{KafkaPrincipal, KafkaPrincipalBuilder, SecurityProtocol}
import org.apache.kafka.metadata.BrokerState
import org.apache.kafka.network.metrics.RequestChannelMetrics
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{BeforeEach, Test}
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._

import java.nio.charset.StandardCharsets
import java.util.function.{Function => JFunction}

/**
 * Tests for [[HttpRequestHandler]] -- the Netty inbound handler that routes
 * HTTP requests into the Kafka protocol pipeline.
 */
class HttpRequestHandlerTest {

  private var requestChannel: RequestChannel = _
  private var httpProcessor: HttpProcessor = _
  private var principalBuilder: KafkaPrincipalBuilder = _
  private var endpoint: Endpoint = _
  private var router: HttpRouter = _
  private var translator: HttpRequestTranslator = _
  private var httpAcceptor: HttpAcceptor = _
  private var httpServerConfigs: HttpServerConfigs = _
  private var metadataSupplier: JFunction[String, Integer] = _
  private var brokerStateVar: BrokerState = _

  private val BROKER_ID = 3
  private val CLUSTER_ID = "abc123"

  @BeforeEach
  def setUp(): Unit = {
    requestChannel = mock(classOf[RequestChannel])
    httpProcessor = mock(classOf[HttpProcessor])
    principalBuilder = mock(classOf[KafkaPrincipalBuilder])
    endpoint = new Endpoint("HTTP", SecurityProtocol.HTTP, "localhost", 8082)
    router = new HttpRouter()
    translator = mock(classOf[HttpRequestTranslator])
    httpAcceptor = new HttpAcceptor()
    httpServerConfigs = HttpServerConfigs.withDefaults()
    metadataSupplier = mock(classOf[JFunction[String, Integer]])
    brokerStateVar = BrokerState.RUNNING

    when(principalBuilder.build(any())).thenReturn(KafkaPrincipal.ANONYMOUS)
    when(httpProcessor.id()).thenReturn(0)
  }

  private def createHandler(): HttpRequestHandler = {
    new HttpRequestHandler(
      requestChannel = requestChannel,
      httpProcessor = httpProcessor,
      principalBuilder = principalBuilder,
      endpoint = endpoint,
      router = router,
      translator = translator,
      httpServerConfigs = httpServerConfigs,
      metadataSupplier = metadataSupplier,
      httpAcceptor = httpAcceptor,
      brokerState = () => brokerStateVar,
      brokerId = BROKER_ID,
      clusterId = CLUSTER_ID
    )
  }

  private def buildRequest(method: HttpMethod, uri: String,
                           body: String = null,
                           headers: Map[String, String] = Map.empty): FullHttpRequest = {
    val content = if (body != null)
      Unpooled.copiedBuffer(body, StandardCharsets.UTF_8)
    else
      Unpooled.EMPTY_BUFFER
    val req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, method, uri, content)
    headers.foreach { case (k, v) => req.headers().set(k, v) }
    req
  }

  private def sendAndRead(channel: EmbeddedChannel, req: FullHttpRequest): FullHttpResponse = {
    channel.writeInbound(req)
    channel.readOutbound[FullHttpResponse]()
  }

  // ---- Health check tests ----

  @Test
  def healthCheck_running_returns200(): Unit = {
    brokerStateVar = BrokerState.RUNNING
    val channel = new EmbeddedChannel(createHandler())
    val req = buildRequest(HttpMethod.GET, "/v1/health")
    val resp = sendAndRead(channel, req)

    assertNotNull(resp, "Response should not be null")
    assertEquals(HttpResponseStatus.OK.code(), resp.status().code())
    val bodyStr = resp.content().toString(StandardCharsets.UTF_8)
    assertTrue(bodyStr.contains("RUNNING"), s"Expected RUNNING in body, got: $bodyStr")
    resp.release()
    channel.close()
  }

  @Test
  def healthCheck_notRunning_returns503(): Unit = {
    brokerStateVar = BrokerState.STARTING
    val channel = new EmbeddedChannel(createHandler())
    val req = buildRequest(HttpMethod.GET, "/v1/health")
    val resp = sendAndRead(channel, req)

    assertNotNull(resp, "Response should not be null")
    assertEquals(HttpResponseStatus.SERVICE_UNAVAILABLE.code(), resp.status().code())
    val bodyStr = resp.content().toString(StandardCharsets.UTF_8)
    assertTrue(bodyStr.contains("STARTING"), s"Expected STARTING in body, got: $bodyStr")
    resp.release()
    channel.close()
  }

  @Test
  def healthCheck_includesBrokerIdAndClusterId(): Unit = {
    brokerStateVar = BrokerState.RUNNING
    val channel = new EmbeddedChannel(createHandler())
    val req = buildRequest(HttpMethod.GET, "/v1/health")
    val resp = sendAndRead(channel, req)

    assertNotNull(resp, "Response should not be null")
    val bodyStr = resp.content().toString(StandardCharsets.UTF_8)
    assertTrue(bodyStr.contains(s""""brokerId":$BROKER_ID"""),
      s"Expected brokerId:$BROKER_ID in body, got: $bodyStr")
    assertTrue(bodyStr.contains(s""""clusterId":"$CLUSTER_ID""""),
      s"Expected clusterId:$CLUSTER_ID in body, got: $bodyStr")
    resp.release()
    channel.close()
  }

  // ---- Normal request tests ----

  @Test
  def normalRequest_registersChannelAndEnqueues(): Unit = {
    val translationResult = buildProduceTranslationResult()
    when(translator.translate(any(), any(), any(), any())).thenReturn(translationResult)
    when(requestChannel.tryEnqueue(any())).thenReturn(true)
    when(requestChannel.metrics).thenReturn(mock(classOf[RequestChannelMetrics]))

    val channel = new EmbeddedChannel(createHandler())
    val req = buildRequest(HttpMethod.POST, "/v1/topics/orders/records",
      body = """{"records":[{"value":{"type":"STRING","data":"test"}}]}""")
    channel.writeInbound(req)

    verify(httpProcessor).registerChannel(anyString(), any())
    verify(requestChannel).tryEnqueue(any())
    channel.close()
  }

  @Test
  def queueFull_returns503WithRetryAfter(): Unit = {
    val translationResult = buildProduceTranslationResult()
    when(translator.translate(any(), any(), any(), any())).thenReturn(translationResult)
    when(requestChannel.tryEnqueue(any())).thenReturn(false)
    when(requestChannel.metrics).thenReturn(mock(classOf[RequestChannelMetrics]))

    val channel = new EmbeddedChannel(createHandler())
    val req = buildRequest(HttpMethod.POST, "/v1/topics/orders/records",
      body = """{"records":[{"value":{"type":"STRING","data":"test"}}]}""")
    val resp = sendAndRead(channel, req)

    assertNotNull(resp, "Response should not be null")
    assertEquals(HttpResponseStatus.SERVICE_UNAVAILABLE.code(), resp.status().code())
    assertEquals("1", resp.headers().get(HttpHeaderNames.RETRY_AFTER))
    val bodyStr = resp.content().toString(StandardCharsets.UTF_8)
    assertTrue(bodyStr.contains("QUEUE_FULL"), s"Expected QUEUE_FULL in body, got: $bodyStr")
    resp.release()
    channel.close()
  }

  // ---- Drain mode tests ----

  @Test
  def drainMode_returns503(): Unit = {
    httpAcceptor.setAccepting(false)
    val channel = new EmbeddedChannel(createHandler())
    val req = buildRequest(HttpMethod.GET, "/v1/health")
    val resp = sendAndRead(channel, req)

    assertNotNull(resp, "Response should not be null")
    assertEquals(HttpResponseStatus.SERVICE_UNAVAILABLE.code(), resp.status().code())
    val bodyStr = resp.content().toString(StandardCharsets.UTF_8)
    assertTrue(bodyStr.contains("SHUTTING_DOWN"), s"Expected SHUTTING_DOWN in body, got: $bodyStr")
    resp.release()
    channel.close()
  }

  // ---- Invalid route tests ----

  @Test
  def invalidRoute_returns404(): Unit = {
    val channel = new EmbeddedChannel(createHandler())
    val req = buildRequest(HttpMethod.GET, "/v1/nonexistent")
    val resp = sendAndRead(channel, req)

    assertNotNull(resp, "Response should not be null")
    assertEquals(HttpResponseStatus.NOT_FOUND.code(), resp.status().code())
    resp.release()
    channel.close()
  }

  @Test
  def invalidTopic_returns400(): Unit = {
    val channel = new EmbeddedChannel(createHandler())
    // "." is rejected by Kafka Topic.validate()
    val req = buildRequest(HttpMethod.GET, "/v1/topics/.")
    val resp = sendAndRead(channel, req)

    assertNotNull(resp, "Response should not be null")
    assertEquals(HttpResponseStatus.BAD_REQUEST.code(), resp.status().code())
    resp.release()
    channel.close()
  }

  // ---- Request ID tests ----

  @Test
  def requestId_echoedFromClient(): Unit = {
    brokerStateVar = BrokerState.RUNNING
    val channel = new EmbeddedChannel(createHandler())
    val req = buildRequest(HttpMethod.GET, "/v1/health",
      headers = Map("X-Request-ID" -> "client-123"))
    val resp = sendAndRead(channel, req)

    assertNotNull(resp, "Response should not be null")
    assertEquals("client-123", resp.headers().get("X-Kafka-Request-ID"))
    resp.release()
    channel.close()
  }

  @Test
  def requestId_generatedWhenMissing(): Unit = {
    brokerStateVar = BrokerState.RUNNING
    val channel = new EmbeddedChannel(createHandler())
    val req = buildRequest(HttpMethod.GET, "/v1/health")
    val resp = sendAndRead(channel, req)

    assertNotNull(resp, "Response should not be null")
    val requestId = resp.headers().get("X-Kafka-Request-ID")
    assertNotNull(requestId, "X-Kafka-Request-ID should be present")
    // UUID format: 8-4-4-4-12 hex digits
    assertTrue(requestId.matches("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"),
      s"Expected UUID format, got: $requestId")
    resp.release()
    channel.close()
  }

  @Test
  def requestId_generatedWhenTooLong(): Unit = {
    brokerStateVar = BrokerState.RUNNING
    val channel = new EmbeddedChannel(createHandler())
    val longId = "a" * 65 // 65 chars > 64 max
    val req = buildRequest(HttpMethod.GET, "/v1/health",
      headers = Map("X-Request-ID" -> longId))
    val resp = sendAndRead(channel, req)

    assertNotNull(resp, "Response should not be null")
    val requestId = resp.headers().get("X-Kafka-Request-ID")
    assertNotNull(requestId, "X-Kafka-Request-ID should be present")
    assertNotEquals(longId, requestId,
      "Too-long client ID should not be echoed")
    assertTrue(requestId.matches("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"),
      s"Expected UUID format, got: $requestId")
    resp.release()
    channel.close()
  }

  // ---- Exception handling tests ----

  @Test
  def exceptionCaught_closesChannel(): Unit = {
    val channel = new EmbeddedChannel(createHandler())
    assertTrue(channel.isActive, "Channel should be active before exception")

    channel.pipeline().fireExceptionCaught(new RuntimeException("test error"))

    // The channel should be closed after exception handling
    assertFalse(channel.isActive, "Channel should be closed after exception")
  }

  // ---- Registration ordering test ----

  @Test
  def channelRegistration_beforeEnqueue(): Unit = {
    val translationResult = buildProduceTranslationResult()
    when(translator.translate(any(), any(), any(), any())).thenReturn(translationResult)
    when(requestChannel.tryEnqueue(any())).thenReturn(true)
    when(requestChannel.metrics).thenReturn(mock(classOf[RequestChannelMetrics]))

    val channel = new EmbeddedChannel(createHandler())
    val req = buildRequest(HttpMethod.POST, "/v1/topics/orders/records",
      body = """{"records":[{"value":{"type":"STRING","data":"test"}}]}""")
    channel.writeInbound(req)

    // Verify ordering: registerChannel called before tryEnqueue
    val order = inOrder(httpProcessor, requestChannel)
    order.verify(httpProcessor).registerChannel(anyString(), any())
    order.verify(requestChannel).tryEnqueue(any())
    channel.close()
  }

  // ---- Helper methods ----

  /**
   * Builds a valid ProduceRequest TranslationResult for testing the normal request path.
   */
  private def buildProduceTranslationResult(): HttpRequestTranslator.TranslationResult = {
    val data = new ProduceRequestData()
      .setAcks(-1.toShort)
      .setTimeoutMs(30000)
    val version = ApiKeys.PRODUCE.latestVersion()
    val request = ProduceRequest.builder(data).build(version)
    val buffer = request.serialize().buffer()
    new HttpRequestTranslator.TranslationResult(ApiKeys.PRODUCE, version, buffer)
  }
}
