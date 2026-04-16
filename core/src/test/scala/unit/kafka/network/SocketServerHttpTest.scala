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

package kafka.network

import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.message.ApiMessageType.ListenerType
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.security.scram.internals.ScramMechanism
import org.apache.kafka.network.SocketServerConfigs
import org.apache.kafka.security.CredentialProvider
import org.apache.kafka.server.SimpleApiVersionManager
import org.apache.kafka.server.common.{FinalizedFeatures, MetadataVersion}
import org.apache.kafka.server.config.ReplicationConfigs
import org.apache.kafka.server.util.ServerTestUtils
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}

import java.util
import java.util.Properties
import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters._

/**
 * Unit tests for SocketServer HTTP wiring.
 *
 * Tests the HTTP-specific code paths in SocketServer:
 * - HttpAcceptor creation for HTTP/HTTPS endpoints
 * - HTTP drain on stopProcessingRequests()
 * - Inter-broker HTTP listener rejection
 * - Multi-listener (PLAINTEXT + HTTP) configuration
 *
 * Separated from SocketServerTest to avoid conflicts and keep
 * HTTP-specific tests self-contained.
 */
class SocketServerHttpTest {

  private val metrics = new Metrics()
  private val credentialProvider = new CredentialProvider(ScramMechanism.mechanismNames, null)
  private val apiVersionManager = new SimpleApiVersionManager(
    ListenerType.BROKER, true,
    () => new FinalizedFeatures(MetadataVersion.latestTesting(), util.Map.of[String, java.lang.Short], 0))

  private var server: SocketServer = _

  @BeforeEach
  def setUp(): Unit = {
    ServerTestUtils.clearYammerMetrics()
  }

  @AfterEach
  def tearDown(): Unit = {
    if (server != null) {
      server.shutdown()
      server = null
    }
    metrics.close()
    ServerTestUtils.clearYammerMetrics()
  }

  private def createProps(extraProps: Map[String, String] = Map.empty): Properties = {
    val props = TestUtils.createBrokerConfig(0, port = 0)
    props.put(SocketServerConfigs.LISTENERS_CONFIG,
      "PLAINTEXT://localhost:0,HTTP://localhost:0")
    props.put(SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG,
      "PLAINTEXT://localhost:0,HTTP://localhost:0")
    props.put(SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG,
      "PLAINTEXT:PLAINTEXT,HTTP:HTTP,CONTROLLER:PLAINTEXT")
    props.put(ReplicationConfigs.INTER_BROKER_LISTENER_NAME_CONFIG, "PLAINTEXT")
    props.put("num.network.threads", "1")
    extraProps.foreach { case (k, v) => props.put(k, v) }
    props
  }

  private def createSocketServer(props: Properties): SocketServer = {
    val config = KafkaConfig.fromProps(props)
    server = new SocketServer(config, metrics, Time.SYSTEM, credentialProvider, apiVersionManager)
    server
  }

  // ===================================================================
  // Test 1: HTTP listener creates HttpAcceptor (not DataPlaneAcceptor)
  // ===================================================================
  @Test
  def testHttpListenerCreatesHttpAcceptor(): Unit = {
    val props = createProps()
    val ss = createSocketServer(props)
    ss.enableRequestProcessing(Map.empty).get(1, TimeUnit.MINUTES)

    // httpAcceptors should have an entry for the HTTP endpoint
    assertFalse(ss.httpAcceptors.isEmpty,
      "httpAcceptors should contain the HTTP endpoint")
    assertEquals(1, ss.httpAcceptors.size(),
      "Should have exactly one HTTP acceptor")

    // The HTTP endpoint should NOT be in dataPlaneAcceptors
    val httpEndpoints = ss.httpAcceptors.keySet().asScala
    httpEndpoints.foreach { ep =>
      assertNull(ss.dataPlaneAcceptors.get(ep),
        "HTTP endpoint should not be in dataPlaneAcceptors")
    }
  }

  // ===================================================================
  // Test 2: HttpAcceptor startup completes
  // ===================================================================
  @Test
  def testHttpAcceptorStartup(): Unit = {
    val props = createProps()
    val ss = createSocketServer(props)
    ss.enableRequestProcessing(Map.empty).get(1, TimeUnit.MINUTES)

    val httpAcceptorEntry = ss.httpAcceptors.entrySet().asScala.head
    val httpAcceptor = httpAcceptorEntry.getValue

    // The startedFuture should complete after startup
    assertNotNull(httpAcceptor.startedFuture,
      "HttpAcceptor should have a startedFuture")
    assertTrue(httpAcceptor.startedFuture.isDone,
      "HttpAcceptor startedFuture should be complete after enableRequestProcessing")
  }

  // ===================================================================
  // Test 3: stopProcessingRequests drains HTTP acceptors
  // ===================================================================
  @Test
  def testStopProcessingRequestsDrainsHttp(): Unit = {
    val props = createProps()
    val ss = createSocketServer(props)
    ss.enableRequestProcessing(Map.empty).get(1, TimeUnit.MINUTES)

    // Verify HTTP acceptor exists before shutdown
    assertEquals(1, ss.httpAcceptors.size())
    val httpAcceptor = ss.httpAcceptors.values().asScala.head
    assertFalse(httpAcceptor.isDraining,
      "HttpAcceptor should not be draining before stopProcessingRequests")

    // Stop processing requests -- should drain HTTP
    ss.stopProcessingRequests()

    // After stop, the HTTP acceptors should have been drained
    assertTrue(httpAcceptor.isDraining,
      "HttpAcceptor should be draining after stopProcessingRequests")
  }

  // ===================================================================
  // Test 4: Inter-broker listener cannot be HTTP
  // ===================================================================
  @Test
  def testInterBrokerListenerCannotBeHttp(): Unit = {
    val props = TestUtils.createBrokerConfig(0, port = 0)
    props.put(SocketServerConfigs.LISTENERS_CONFIG,
      "HTTP://localhost:0")
    props.put(SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG,
      "HTTP://localhost:0")
    props.put(SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG,
      "HTTP:HTTP,CONTROLLER:PLAINTEXT")
    props.put(ReplicationConfigs.INTER_BROKER_LISTENER_NAME_CONFIG, "HTTP")

    val exception = assertThrows(classOf[Exception], () => {
      val config = KafkaConfig.fromProps(props)
      val ss = new SocketServer(config, metrics, Time.SYSTEM, credentialProvider, apiVersionManager)
      server = ss
      ss.enableRequestProcessing(Map.empty).get(1, TimeUnit.MINUTES)
    })

    // The error message should mention HTTP and inter-broker
    assertTrue(
      exception.getMessage.toLowerCase.contains("http") ||
        exception.getMessage.toLowerCase.contains("inter"),
      s"Exception should mention HTTP/inter-broker restriction, got: ${exception.getMessage}")
  }

  // ===================================================================
  // Test 5: Multiple listeners (PLAINTEXT + HTTP) work independently
  // ===================================================================
  @Test
  def testMultipleListenersPlaintextAndHttp(): Unit = {
    val props = createProps()
    val ss = createSocketServer(props)
    ss.enableRequestProcessing(Map.empty).get(1, TimeUnit.MINUTES)

    // PLAINTEXT should be in dataPlaneAcceptors
    assertEquals(1, ss.dataPlaneAcceptors.size(),
      "Should have exactly one data-plane acceptor (PLAINTEXT)")

    // HTTP should be in httpAcceptors
    assertEquals(1, ss.httpAcceptors.size(),
      "Should have exactly one HTTP acceptor")

    // Verify they are for different endpoints
    val dataPlaneEndpoint = ss.dataPlaneAcceptors.keySet().asScala.head
    val httpEndpoint = ss.httpAcceptors.keySet().asScala.head

    assertNotEquals(dataPlaneEndpoint, httpEndpoint,
      "Data-plane and HTTP endpoints should be different")

    // Verify security protocols
    assertEquals(SecurityProtocol.PLAINTEXT, dataPlaneEndpoint.securityProtocol())
    assertEquals(SecurityProtocol.HTTP, httpEndpoint.securityProtocol())
  }
}
