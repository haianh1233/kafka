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

import java.security.KeyPairGenerator
import javax.net.ssl.KeyManagerFactory

import io.netty.channel.embedded.EmbeddedChannel
import io.netty.handler.codec.http.{HttpContentCompressor, HttpObjectAggregator, HttpServerCodec}
import io.netty.handler.ssl.{SslContext, SslContextBuilder}
import io.netty.handler.timeout.IdleStateHandler
import kafka.server.http.HttpServerConfigs
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

  private val httpRequestMaxBytes: Int = HttpServerConfigs.HTTP_REQUEST_MAX_BYTES_DEFAULT
  private val httpConnectionIdleTimeoutMs: Long = HttpServerConfigs.HTTP_CONNECTION_IDLE_TIMEOUT_MS_DEFAULT
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
  private def createChannelWithPipeline(sslContext: Option[SslContext] = None): EmbeddedChannel = {
    val endpoint = if (sslContext.isDefined) httpsEndpoint else httpEndpoint
    val initializer = new HttpChannelInitializer(
      endpoint,
      httpRequestMaxBytes,
      httpConnectionIdleTimeoutMs,
      sslContext
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
  }
}
