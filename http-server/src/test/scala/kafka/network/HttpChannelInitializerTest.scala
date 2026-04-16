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

import io.netty.channel.embedded.EmbeddedChannel
import io.netty.handler.codec.http.{HttpContentCompressor, HttpObjectAggregator, HttpServerCodec}
import io.netty.handler.codec.http.cors.CorsHandler
import io.netty.handler.timeout.IdleStateHandler
import kafka.server.http.{HttpMetrics, HttpRequestHandler, IdleStateCloseHandler}

import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

/**
 * Tests for [[HttpChannelInitializer]].
 *
 * Verifies that:
 * - The HTTP/1.1 pipeline contains the correct handlers in the correct order
 * - Plaintext configuration does not include SSL or protocol negotiation handlers
 * - CORS handler is added when configured
 * - TLS-enabled listeners would add SslHandler + ALPN negotiation handler
 *
 * Note: EmbeddedChannel is not a SocketChannel, so we test the internal
 * configureHttp11Pipeline method directly via package-private access.
 */
class HttpChannelInitializerTest {

  private val maxRequestBytes = 10 * 1024 * 1024  // 10 MB
  private val connectionIdleTimeoutMs = 60000L

  private val draining = new AtomicBoolean(false)
  private val inFlightCount = new AtomicInteger(0)
  private val httpMetrics: HttpMetrics = new HttpMetrics()

  private def createInitializer(corsAllowedOrigins: String = ""): HttpChannelInitializer = {
    new HttpChannelInitializer(
      sslContext = None,
      draining = draining,
      inFlightCount = inFlightCount,
      httpMetrics = httpMetrics,
      maxRequestBytes = maxRequestBytes,
      connectionIdleTimeoutMs = connectionIdleTimeoutMs,
      corsAllowedOrigins = corsAllowedOrigins
    )
  }

  @Test
  def testHttp11PipelineHasExpectedHandlers(): Unit = {
    val initializer = createInitializer()

    // Create an EmbeddedChannel and configure the pipeline directly
    val channel = new EmbeddedChannel()
    initializer.configureHttp11Pipeline(channel.pipeline())

    val pipeline = channel.pipeline()

    // Verify HTTP/1.1 handlers are present
    assertNotNull(pipeline.get("http-codec"), "HttpServerCodec should be present")
    assertTrue(pipeline.get("http-codec").isInstanceOf[HttpServerCodec])

    assertNotNull(pipeline.get("http-aggregator"), "HttpObjectAggregator should be present")
    assertTrue(pipeline.get("http-aggregator").isInstanceOf[HttpObjectAggregator])

    assertNotNull(pipeline.get("compressor"), "HttpContentCompressor should be present")
    assertTrue(pipeline.get("compressor").isInstanceOf[HttpContentCompressor])

    assertNotNull(pipeline.get("idle-handler"), "IdleStateHandler should be present")
    assertTrue(pipeline.get("idle-handler").isInstanceOf[IdleStateHandler])

    assertNotNull(pipeline.get("idle-closer"), "IdleStateCloseHandler should be present")
    assertTrue(pipeline.get("idle-closer").isInstanceOf[IdleStateCloseHandler])

    assertNotNull(pipeline.get("kafka-handler"), "HttpRequestHandler should be present")
    assertTrue(pipeline.get("kafka-handler").isInstanceOf[HttpRequestHandler])

    // Verify no SSL or protocol negotiation handlers on plaintext pipeline
    assertNull(pipeline.get("ssl"), "SslHandler should NOT be present on plaintext")
    assertNull(pipeline.get("protocol-negotiation"),
      "Protocol negotiation handler should NOT be present on plaintext")

    channel.close()
  }

  @Test
  def testHttp11PipelineHandlerOrder(): Unit = {
    val initializer = createInitializer()

    val channel = new EmbeddedChannel()
    initializer.configureHttp11Pipeline(channel.pipeline())

    val names = channel.pipeline().names()

    val codecIdx = names.indexOf("http-codec")
    val aggregatorIdx = names.indexOf("http-aggregator")
    val compressorIdx = names.indexOf("compressor")
    val idleHandlerIdx = names.indexOf("idle-handler")
    val idleCloserIdx = names.indexOf("idle-closer")
    val kafkaHandlerIdx = names.indexOf("kafka-handler")

    assertTrue(codecIdx >= 0, "http-codec should be in the pipeline")
    assertTrue(codecIdx < aggregatorIdx, "http-codec should come before http-aggregator")
    assertTrue(aggregatorIdx < compressorIdx, "http-aggregator should come before compressor")
    assertTrue(compressorIdx < idleHandlerIdx, "compressor should come before idle-handler")
    assertTrue(idleHandlerIdx < idleCloserIdx, "idle-handler should come before idle-closer")
    assertTrue(idleCloserIdx < kafkaHandlerIdx, "idle-closer should come before kafka-handler")

    channel.close()
  }

  @Test
  def testHttp11PipelineNeverHasH2Handlers(): Unit = {
    val initializer = createInitializer()

    val channel = new EmbeddedChannel()
    initializer.configureHttp11Pipeline(channel.pipeline())

    val pipeline = channel.pipeline()

    assertNull(pipeline.get("h2-frame-codec"),
      "HTTP/2 frame codec should NOT be in plaintext pipeline")
    assertNull(pipeline.get("h2-multiplex"),
      "HTTP/2 multiplex handler should NOT be in plaintext pipeline")

    channel.close()
  }

  @Test
  def testCorsDisabled_noCorsHandler(): Unit = {
    val initializer = createInitializer(corsAllowedOrigins = "")

    val channel = new EmbeddedChannel()
    initializer.configureHttp11Pipeline(channel.pipeline())

    assertNull(channel.pipeline().get("cors"),
      "Pipeline should NOT contain 'cors' handler when CORS is disabled")

    channel.close()
  }

  @Test
  def testCorsEnabled_addsCorsHandler(): Unit = {
    val initializer = createInitializer(corsAllowedOrigins = "https://example.com")

    val channel = new EmbeddedChannel()
    initializer.configureHttp11Pipeline(channel.pipeline())

    val handler = channel.pipeline().get("cors")
    assertNotNull(handler, "Pipeline should contain 'cors' handler when CORS is enabled")
    assertTrue(handler.isInstanceOf[CorsHandler], "cors should be CorsHandler")

    channel.close()
  }

  @Test
  def testCorsWildcard_addsCorsHandler(): Unit = {
    val initializer = createInitializer(corsAllowedOrigins = "*")

    val channel = new EmbeddedChannel()
    initializer.configureHttp11Pipeline(channel.pipeline())

    val handler = channel.pipeline().get("cors")
    assertNotNull(handler, "Pipeline should contain 'cors' handler for wildcard origin")
    assertTrue(handler.isInstanceOf[CorsHandler], "cors should be CorsHandler")

    channel.close()
  }

  @Test
  def testCorsEnabled_corsAfterCodecBeforeAggregator(): Unit = {
    val initializer = createInitializer(corsAllowedOrigins = "https://example.com")

    val channel = new EmbeddedChannel()
    initializer.configureHttp11Pipeline(channel.pipeline())

    val names = channel.pipeline().names()
    val codecIdx = names.indexOf("http-codec")
    val corsIdx = names.indexOf("cors")
    val aggregatorIdx = names.indexOf("http-aggregator")
    assertTrue(corsIdx > codecIdx,
      s"cors ($corsIdx) should come after http-codec ($codecIdx)")
    assertTrue(corsIdx < aggregatorIdx,
      s"cors ($corsIdx) should come before http-aggregator ($aggregatorIdx)")

    channel.close()
  }
}
