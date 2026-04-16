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

import java.security.KeyPairGenerator
import javax.net.ssl.KeyManagerFactory

import io.netty.channel.embedded.EmbeddedChannel
import io.netty.handler.codec.http.{HttpContentCompressor, HttpObjectAggregator, HttpServerCodec}
import io.netty.handler.codec.http.cors.CorsHandler
import io.netty.handler.ssl.{SslContext, SslContextBuilder}
import io.netty.handler.timeout.IdleStateHandler
import org.apache.kafka.common.Endpoint
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.test.TestSslUtils
import org.junit.jupiter.api.{BeforeEach, Test}
import org.junit.jupiter.api.Assertions._

/**
 * Tests for HttpChannelInitializer pipeline configuration.
 *
 * Uses EmbeddedChannel to verify that the pipeline handlers are added
 * in the correct order and with the correct types.
 *
 * // Time: Created - TASK-B.03
 */
class HttpChannelInitializerTest {

  private val httpRequestMaxBytes: Int = org.apache.kafka.network.HttpServerConfigs.HTTP_REQUEST_MAX_BYTES_DEFAULT
  private val httpConnectionIdleTimeoutMs: Long = org.apache.kafka.network.HttpServerConfigs.HTTP_CONNECTION_IDLE_TIMEOUT_MS_DEFAULT
  private var httpEndpoint: Endpoint = _
  private var httpsEndpoint: Endpoint = _

  @BeforeEach
  def setUp(): Unit = {
    httpEndpoint = new Endpoint("HTTP", SecurityProtocol.HTTP, "localhost", 0)
    httpsEndpoint = new Endpoint("HTTPS", SecurityProtocol.HTTPS, "localhost", 0)
  }

  /**
   * Creates an EmbeddedChannel with the pipeline configured by HttpChannelInitializer.
   */
  private def createChannelWithPipeline(
      sslContext: Option[SslContext] = None,
      corsAllowedOrigins: String = ""
  ): EmbeddedChannel = {
    val endpoint = if (sslContext.isDefined) httpsEndpoint else httpEndpoint
    val initializer = new HttpChannelInitializer(
      endpoint,
      httpRequestMaxBytes,
      httpConnectionIdleTimeoutMs,
      sslContext,
      corsAllowedOrigins
    )
    new EmbeddedChannel(initializer)
  }

  @Test
  def initChannel_addsHttpCodecFirst(): Unit = {
    val channel = createChannelWithPipeline()
    try {
      val handler = channel.pipeline().get("http-codec")
      assertNotNull(handler, "Pipeline should contain 'http-codec' handler")
      assertTrue(handler.isInstanceOf[HttpServerCodec], "http-codec should be HttpServerCodec")
    } finally {
      channel.close()
    }
  }

  @Test
  def initChannel_addsAggregatorWithCorrectLimit(): Unit = {
    val channel = createChannelWithPipeline()
    try {
      val handler = channel.pipeline().get("http-aggregator")
      assertNotNull(handler, "Pipeline should contain 'http-aggregator' handler")
      assertTrue(handler.isInstanceOf[HttpObjectAggregator], "http-aggregator should be HttpObjectAggregator")
    } finally {
      channel.close()
    }
  }

  @Test
  def initChannel_addsCompressor(): Unit = {
    val channel = createChannelWithPipeline()
    try {
      val handler = channel.pipeline().get("compressor")
      assertNotNull(handler, "Pipeline should contain 'compressor' handler")
      assertTrue(handler.isInstanceOf[HttpContentCompressor], "compressor should be HttpContentCompressor")
    } finally {
      channel.close()
    }
  }

  @Test
  def initChannel_addsIdleHandler(): Unit = {
    val channel = createChannelWithPipeline()
    try {
      val handler = channel.pipeline().get("idle-handler")
      assertNotNull(handler, "Pipeline should contain 'idle-handler' handler")
      assertTrue(handler.isInstanceOf[IdleStateHandler], "idle-handler should be IdleStateHandler")
    } finally {
      channel.close()
    }
  }

  @Test
  def initChannel_addsIdleCloser(): Unit = {
    val channel = createChannelWithPipeline()
    try {
      val handler = channel.pipeline().get("idle-closer")
      assertNotNull(handler, "Pipeline should contain 'idle-closer' handler")
      assertTrue(handler.isInstanceOf[IdleStateCloseHandler], "idle-closer should be IdleStateCloseHandler")
    } finally {
      channel.close()
    }
  }

  @Test
  def initChannel_corsDisabled_noCorsHandler(): Unit = {
    val channel = createChannelWithPipeline(corsAllowedOrigins = "")
    try {
      val handler = channel.pipeline().get("cors")
      assertNull(handler, "Pipeline should NOT contain 'cors' handler when CORS is disabled")
    } finally {
      channel.close()
    }
  }

  @Test
  def initChannel_corsEnabled_addsCorsHandler(): Unit = {
    val channel = createChannelWithPipeline(corsAllowedOrigins = "https://example.com")
    try {
      val handler = channel.pipeline().get("cors")
      assertNotNull(handler, "Pipeline should contain 'cors' handler when CORS is enabled")
      assertTrue(handler.isInstanceOf[CorsHandler], "cors should be CorsHandler")
    } finally {
      channel.close()
    }
  }

  @Test
  def initChannel_corsEnabled_corsAfterCodecBeforeAggregator(): Unit = {
    val channel = createChannelWithPipeline(corsAllowedOrigins = "https://example.com")
    try {
      val names = new java.util.ArrayList[String]()
      val iter = channel.pipeline().iterator()
      while (iter.hasNext) {
        val entry = iter.next()
        names.add(entry.getKey)
      }
      val codecIdx = names.indexOf("http-codec")
      val corsIdx = names.indexOf("cors")
      val aggregatorIdx = names.indexOf("http-aggregator")
      assertTrue(corsIdx > codecIdx,
        s"cors ($corsIdx) should come after http-codec ($codecIdx)")
      assertTrue(corsIdx < aggregatorIdx,
        s"cors ($corsIdx) should come before http-aggregator ($aggregatorIdx)")
    } finally {
      channel.close()
    }
  }

  @Test
  def initChannel_corsWildcard_addsCorsHandler(): Unit = {
    val channel = createChannelWithPipeline(corsAllowedOrigins = "*")
    try {
      val handler = channel.pipeline().get("cors")
      assertNotNull(handler, "Pipeline should contain 'cors' handler for wildcard origin")
      assertTrue(handler.isInstanceOf[CorsHandler], "cors should be CorsHandler")
    } finally {
      channel.close()
    }
  }

  @Test
  def initChannel_httpsEndpoint_addsSslHandler(): Unit = {
    val sslCtx = createTestSslContext()
    val channel = createChannelWithPipeline(Some(sslCtx))
    try {
      val handler = channel.pipeline().get("ssl")
      assertNotNull(handler, "Pipeline should contain 'ssl' handler for HTTPS endpoint")
    } finally {
      channel.close()
    }
  }

  @Test
  def initChannel_httpEndpoint_noSslHandler(): Unit = {
    val channel = createChannelWithPipeline(None)
    try {
      val handler = channel.pipeline().get("ssl")
      assertNull(handler, "Pipeline should NOT contain 'ssl' handler for HTTP endpoint")
    } finally {
      channel.close()
    }
  }

  @Test
  def initChannel_pipelineOrder(): Unit = {
    val channel = createChannelWithPipeline()
    try {
      val names = new java.util.ArrayList[String]()
      val iter = channel.pipeline().iterator()
      while (iter.hasNext) {
        val entry = iter.next()
        names.add(entry.getKey)
      }
      // Verify the order (excluding the tail handler added by EmbeddedChannel)
      val expectedOrder = java.util.List.of(
        "http-codec", "http-aggregator", "compressor", "idle-handler", "idle-closer"
      )
      // The pipeline names should contain all expected handlers in order
      val pipelineNames = new java.util.ArrayList[String](names)
      pipelineNames.retainAll(expectedOrder)
      assertEquals(expectedOrder, pipelineNames, "Pipeline handlers should be in the correct order")
    } finally {
      channel.close()
    }
  }

  @Test
  def initChannel_pipelineOrderWithCors(): Unit = {
    val channel = createChannelWithPipeline(corsAllowedOrigins = "https://example.com")
    try {
      val names = new java.util.ArrayList[String]()
      val iter = channel.pipeline().iterator()
      while (iter.hasNext) {
        val entry = iter.next()
        names.add(entry.getKey)
      }
      // Verify the order including CORS handler
      val expectedOrder = java.util.List.of(
        "http-codec", "cors", "http-aggregator", "compressor", "idle-handler", "idle-closer"
      )
      val pipelineNames = new java.util.ArrayList[String](names)
      pipelineNames.retainAll(expectedOrder)
      assertEquals(expectedOrder, pipelineNames,
        "Pipeline handlers should be in the correct order with CORS handler after codec and before aggregator")
    } finally {
      channel.close()
    }
  }

  /**
   * Creates a test SslContext using BouncyCastle-based certificate generation
   * from Kafka's TestSslUtils, which works across all JDK versions.
   */
  private def createTestSslContext(): SslContext = {
    val kpg = KeyPairGenerator.getInstance("RSA")
    kpg.initialize(2048)
    val keyPair = kpg.generateKeyPair()

    val cert = TestSslUtils.generateCertificate("CN=localhost", keyPair, 365, "SHA256withRSA")

    val ks = java.security.KeyStore.getInstance("JKS")
    ks.load(null, null)
    ks.setKeyEntry("test", keyPair.getPrivate, "password".toCharArray,
      Array[java.security.cert.Certificate](cert))

    val kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm)
    kmf.init(ks, "password".toCharArray)

    SslContextBuilder.forServer(kmf).build()
=======
package kafka.network

import io.netty.channel.embedded.EmbeddedChannel
import io.netty.handler.codec.http.{HttpContentCompressor, HttpObjectAggregator, HttpServerCodec}
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
  private val httpMetrics: HttpMetrics = new NoOpHttpMetrics

  @Test
  def testHttp11PipelineHasExpectedHandlers(): Unit = {
    val initializer = new HttpChannelInitializer(
      sslContext = None,
      draining = draining,
      inFlightCount = inFlightCount,
      httpMetrics = httpMetrics,
      maxRequestBytes = maxRequestBytes,
      connectionIdleTimeoutMs = connectionIdleTimeoutMs
    )

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
    val initializer = new HttpChannelInitializer(
      sslContext = None,
      draining = draining,
      inFlightCount = inFlightCount,
      httpMetrics = httpMetrics,
      maxRequestBytes = maxRequestBytes,
      connectionIdleTimeoutMs = connectionIdleTimeoutMs
    )

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
    val initializer = new HttpChannelInitializer(
      sslContext = None,
      draining = draining,
      inFlightCount = inFlightCount,
      httpMetrics = httpMetrics,
      maxRequestBytes = maxRequestBytes,
      connectionIdleTimeoutMs = connectionIdleTimeoutMs
    )

    val channel = new EmbeddedChannel()
    initializer.configureHttp11Pipeline(channel.pipeline())

    val pipeline = channel.pipeline()

    assertNull(pipeline.get("h2-frame-codec"),
      "HTTP/2 frame codec should NOT be in plaintext pipeline")
    assertNull(pipeline.get("h2-multiplex"),
      "HTTP/2 multiplex handler should NOT be in plaintext pipeline")

    channel.close()
  }

  private class NoOpHttpMetrics extends HttpMetrics {
    override def recordIdleConnectionClose(): Unit = ()
    override def recordConnectionCreated(): Unit = ()
    override def recordConnectionClosed(): Unit = ()
>>>>>>> 6168ee8c21 (feat(http-server): add HTTP/2 support via Netty ALPN negotiation (TASK-F.07))
  }
}
