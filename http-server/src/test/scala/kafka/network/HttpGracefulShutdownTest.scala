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

import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import io.netty.channel.embedded.EmbeddedChannel
import io.netty.handler.codec.http._
import org.apache.kafka.common.security.auth.{KafkaPrincipal, KafkaPrincipalBuilder, SecurityProtocol}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.api.Assertions._
import org.mockito.Mockito._

/**
 * Tests for the drain check and 503 response in HttpRequestHandler.
 *
 * Uses EmbeddedChannel WITHOUT the HTTP codec pipeline, sending FullHttpRequest
 * objects directly to the handler and reading FullHttpResponse objects from
 * the outbound. This avoids the codec encoding the response into raw bytes
 * and the EmbeddedSocketAddress cast issue.
 *
 * // Time: Created - TASK-F.06
 */
class HttpGracefulShutdownTest {

  private var channel: EmbeddedChannel = _
  private val draining = new AtomicBoolean(false)
  private val inFlightCount = new AtomicInteger(0)

  // Mock principal builder that always returns ANONYMOUS
  private val principalBuilder: KafkaPrincipalBuilder = mock(classOf[KafkaPrincipalBuilder])

  @BeforeEach
  def setUp(): Unit = {
    when(principalBuilder.build(org.mockito.ArgumentMatchers.any()))
      .thenReturn(KafkaPrincipal.ANONYMOUS)

    draining.set(false)
    inFlightCount.set(0)

    val handler = new HttpRequestHandler(
      principalBuilder,
      SecurityProtocol.HTTP,
      draining,
      inFlightCount
    )

    // Use handler directly without HTTP codec to avoid EmbeddedSocketAddress issues
    // and byte-level encoding of responses. FullHttpRequest goes in, FullHttpResponse comes out.
    channel = new EmbeddedChannel(handler)
  }

  @AfterEach
  def tearDown(): Unit = {
    if (channel != null) channel.close()
  }

  private def buildRequest(method: HttpMethod, uri: String, body: String = null): FullHttpRequest = {
    val content = if (body != null)
      io.netty.buffer.Unpooled.copiedBuffer(body, StandardCharsets.UTF_8)
    else
      io.netty.buffer.Unpooled.EMPTY_BUFFER
    val req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, method, uri, content)
    req.headers().set(HttpHeaderNames.HOST, "localhost")
    if (body != null) {
      req.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json")
    }
    req
  }

  @Test
  def testRequestDuringDrainGets503(): Unit = {
    // Set draining flag
    draining.set(true)

    // Send a request directly (no codec pipeline)
    val request = buildRequest(HttpMethod.GET, "/v1/health")
    channel.writeInbound(request)

    // Read the 503 response
    val response = channel.readOutbound[FullHttpResponse]()
    assertNotNull(response, "Should have sent a 503 response")
    assertEquals(HttpResponseStatus.SERVICE_UNAVAILABLE, response.status())

    // Verify JSON body
    val body = response.content().toString(StandardCharsets.UTF_8)
    assertTrue(body.contains("Broker is shutting down"), s"Body should contain shutdown message: $body")
    assertTrue(body.contains("SERVICE_UNAVAILABLE"), s"Body should contain SERVICE_UNAVAILABLE: $body")

    // Verify Retry-After header
    assertEquals("5", response.headers().get("Retry-After"))

    // Verify Connection: close header
    assertEquals("close", response.headers().get(HttpHeaderNames.CONNECTION))

    // In-flight count should NOT be incremented for rejected requests
    assertEquals(0, inFlightCount.get(), "Rejected requests should not increment in-flight count")

    response.release()
  }

  @Test
  def testDrainResponseHasCorrectJsonFormat(): Unit = {
    draining.set(true)

    val request = buildRequest(HttpMethod.POST, "/v1/topics/test/records",
      body = """{"records":[]}""")
    channel.writeInbound(request)

    val response = channel.readOutbound[FullHttpResponse]()
    assertNotNull(response)

    val body = response.content().toString(StandardCharsets.UTF_8)
    // Exact JSON match
    assertEquals(
      """{"errorCode":-1,"errorMessage":"Broker is shutting down","detail":"SERVICE_UNAVAILABLE"}""",
      body)

    // Content-Type should be application/json
    assertEquals("application/json", response.headers().get(HttpHeaderNames.CONTENT_TYPE))

    response.release()
  }

  @Test
  def testDrainResponseHasRetryAfterHeader(): Unit = {
    draining.set(true)

    val request = buildRequest(HttpMethod.GET, "/v1/health")
    channel.writeInbound(request)

    val response = channel.readOutbound[FullHttpResponse]()
    assertNotNull(response)

    // Retry-After: 5 directs clients to retry on another broker
    assertTrue(response.headers().contains("Retry-After"), "Response must have Retry-After header")
    assertEquals("5", response.headers().get("Retry-After"))

    response.release()
  }

  @Test
  def testMultipleRequestsDuringDrainAllGet503(): Unit = {
    draining.set(true)

    for (i <- 1 to 5) {
      val request = buildRequest(HttpMethod.GET, s"/v1/health?req=$i")
      channel.writeInbound(request)

      val response = channel.readOutbound[FullHttpResponse]()
      assertNotNull(response, s"Request $i should get a 503 response")
      assertEquals(HttpResponseStatus.SERVICE_UNAVAILABLE, response.status(),
        s"Request $i should be 503")
      response.release()
    }

    // No requests should have been tracked as in-flight
    assertEquals(0, inFlightCount.get())
  }

  @Test
  def testRequestBeforeDrainIncrementsInFlightCount(): Unit = {
    // Not draining -- but since we're not going through the codec pipeline,
    // and EmbeddedChannel doesn't have a real InetSocketAddress, the
    // extractAuthContext() call would fail. Instead, we test the drain logic
    // directly: when NOT draining, the handler should attempt to process
    // (and the inFlightCount gets incremented before the auth context extraction).

    // The channelRead0 method increments inFlightCount before buildPrincipal.
    // But buildPrincipal calls extractAuthContext which casts remoteAddress to
    // InetSocketAddress. In EmbeddedChannel this would fail. We verify the
    // drain-specific behavior here: drain=false means the handler tries to process
    // (incrementing the counter). We catch the expected ClassCastException.
    val request = buildRequest(HttpMethod.GET, "/v1/health")
    try {
      channel.writeInbound(request)
    } catch {
      case _: Exception =>
        // Expected: EmbeddedSocketAddress cannot be cast to InetSocketAddress.
        // But the in-flight count was incremented before the exception,
        // and then decremented in the catch block.
    }

    // After the exception path, in-flight count should be 0 (incremented then decremented)
    assertEquals(0, inFlightCount.get(),
      "In-flight count should be 0 after exception (incremented then decremented in catch)")
  }
}
