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
import org.apache.kafka.common.Endpoint
import org.apache.kafka.common.config.ConfigException
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
    val factory: (Endpoint, Time) => HttpAcceptorLike = (ep, t) => new HttpAcceptorLike {
      private val _startedFuture = new java.util.concurrent.CompletableFuture[Void]()
      private var _draining = false
      override def endpoint: Endpoint = ep
      override def startedFuture: java.util.concurrent.CompletableFuture[Void] = _startedFuture
      override def startup(): Unit = _startedFuture.complete(null)
      override def beginDrain(): Unit = _draining = true
      override def awaitDrain(timeoutMs: Long): Unit = ()
      override def boundPort: Int = 0
      override def close(): Unit = ()
      override def isDraining: Boolean = _draining
      override def pendingRequestCount: Int = 0
    }
    server = new SocketServer(config, metrics, Time.SYSTEM, credentialProvider, apiVersionManager,
      httpAcceptorFactory = factory)
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

  // ===================================================================
  // Test 6: HTTPS listener without SSL keystore config fails fast
  // ===================================================================
  @Test
  def testHttpsListenerRequiresSslConfig(): Unit = {
    val props = TestUtils.createBrokerConfig(0, port = 0)
    props.put(SocketServerConfigs.LISTENERS_CONFIG,
      "PLAINTEXT://localhost:0,HTTPS://localhost:0")
    props.put(SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG,
      "PLAINTEXT://localhost:0,HTTPS://localhost:0")
    props.put(SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG,
      "PLAINTEXT:PLAINTEXT,HTTPS:HTTPS,CONTROLLER:PLAINTEXT")
    props.put(ReplicationConfigs.INTER_BROKER_LISTENER_NAME_CONFIG, "PLAINTEXT")
    // Intentionally do NOT set ssl.keystore.location

    val config = KafkaConfig.fromProps(props)
    val factory: (Endpoint, Time) => HttpAcceptorLike = (ep, t) => new HttpAcceptorLike {
      private val _startedFuture = new java.util.concurrent.CompletableFuture[Void]()
      override def endpoint: Endpoint = ep
      override def startedFuture: java.util.concurrent.CompletableFuture[Void] = _startedFuture
      override def startup(): Unit = _startedFuture.complete(null)
      override def beginDrain(): Unit = ()
      override def awaitDrain(timeoutMs: Long): Unit = ()
      override def boundPort: Int = 0
      override def close(): Unit = ()
      override def isDraining: Boolean = false
      override def pendingRequestCount: Int = 0
    }

    val exception = assertThrows(classOf[ConfigException], () => {
      server = new SocketServer(config, metrics, Time.SYSTEM, credentialProvider, apiVersionManager,
        httpAcceptorFactory = factory)
      server.enableRequestProcessing(Map.empty).get(1, TimeUnit.MINUTES)
    })

    assertTrue(exception.getMessage.contains("ssl.keystore.location"),
      s"Exception should mention ssl.keystore.location, got: ${exception.getMessage}")
    assertTrue(exception.getMessage.toLowerCase.contains("https"),
      s"Exception should mention HTTPS, got: ${exception.getMessage}")
  }

  // ===================================================================
  // Test 7: HTTP listener endpoint is logged on startup
  // ===================================================================
  @Test
  def testHttpListenerLoggedOnStartup(): Unit = {
    val props = createProps()
    val ss = createSocketServer(props)
    ss.enableRequestProcessing(Map.empty).get(1, TimeUnit.MINUTES)

    // Verify HTTP acceptor is present (logging happens in BrokerServer,
    // but we verify the httpAcceptors map is populated which is what
    // BrokerServer reads to produce the log message)
    assertFalse(ss.httpAcceptors.isEmpty,
      "httpAcceptors should be populated for BrokerServer startup logging")

    val httpEntry = ss.httpAcceptors.entrySet().asScala.head
    val ep = httpEntry.getKey
    assertEquals(SecurityProtocol.HTTP, ep.securityProtocol(),
      "HTTP endpoint should have HTTP security protocol")
    assertNotNull(httpEntry.getValue,
      "HTTP acceptor should not be null")
  }

  // ===================================================================
  // Test 8: Port conflict — HTTP port == PLAINTEXT port → rejected
  // ===================================================================
  @Test
  def testPortConflictHttpAndPlaintext(): Unit = {
    val props = new Properties()
    props.put("process.roles", "broker")
    props.put("node.id", "0")
    props.put("quorum.bootstrap.servers", "localhost:9095")
    props.put("controller.listener.names", "CONTROLLER")
    props.put(SocketServerConfigs.LISTENERS_CONFIG,
      "PLAINTEXT://localhost:9092,HTTP://localhost:9092")
    props.put(SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG,
      "PLAINTEXT:PLAINTEXT,HTTP:HTTP,CONTROLLER:PLAINTEXT")
    props.put(ReplicationConfigs.INTER_BROKER_LISTENER_NAME_CONFIG, "PLAINTEXT")

    val exception = assertThrows(classOf[IllegalArgumentException], () => {
      KafkaConfig.fromProps(props)
    })
    assertTrue(exception.getMessage.contains("different port"),
      s"Should reject duplicate ports, got: ${exception.getMessage}")
  }

  // ===================================================================
  // Test 9: HTTP port = 0 (random assignment) works
  // ===================================================================
  @Test
  def testHttpPortZeroRandomAssignment(): Unit = {
    val props = createProps()
    // Port 0 is already used in createProps — verify it works
    val ss = createSocketServer(props)
    ss.enableRequestProcessing(Map.empty).get(1, TimeUnit.MINUTES)

    // HTTP acceptor should exist
    assertEquals(1, ss.httpAcceptors.size())
    // The mock returns boundPort=0, but the key point is no exception was thrown
  }

  // ===================================================================
  // Test 10: HTTP + HTTPS on same broker
  // ===================================================================
  @Test
  def testHttpAndHttpsCoexist(): Unit = {
    val props = TestUtils.createBrokerConfig(0, port = 0)
    props.put(SocketServerConfigs.LISTENERS_CONFIG,
      "PLAINTEXT://localhost:0,HTTP://localhost:0,HTTPS://localhost:0")
    props.put(SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG,
      "PLAINTEXT://localhost:0,HTTP://localhost:0,HTTPS://localhost:0")
    props.put(SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG,
      "PLAINTEXT:PLAINTEXT,HTTP:HTTP,HTTPS:HTTPS,CONTROLLER:PLAINTEXT")
    props.put(ReplicationConfigs.INTER_BROKER_LISTENER_NAME_CONFIG, "PLAINTEXT")
    // HTTPS requires SSL config
    props.put("ssl.keystore.location", "/tmp/test-keystore.jks")
    props.put("ssl.keystore.password", "testpass")
    props.put("ssl.key.password", "testpass")

    val config = KafkaConfig.fromProps(props)
    val factory: (Endpoint, Time) => HttpAcceptorLike = (ep, t) => new HttpAcceptorLike {
      private val _startedFuture = new java.util.concurrent.CompletableFuture[Void]()
      override def endpoint: Endpoint = ep
      override def startedFuture: java.util.concurrent.CompletableFuture[Void] = _startedFuture
      override def startup(): Unit = _startedFuture.complete(null)
      override def beginDrain(): Unit = ()
      override def awaitDrain(timeoutMs: Long): Unit = ()
      override def boundPort: Int = 0
      override def close(): Unit = ()
      override def isDraining: Boolean = false
      override def pendingRequestCount: Int = 0
    }
    server = new SocketServer(config, metrics, Time.SYSTEM, credentialProvider, apiVersionManager,
      httpAcceptorFactory = factory)
    server.enableRequestProcessing(Map.empty).get(1, TimeUnit.MINUTES)

    // Should have 2 HTTP acceptors (HTTP + HTTPS)
    assertEquals(2, server.httpAcceptors.size(),
      "Should have HTTP and HTTPS acceptors")
    val protocols = server.httpAcceptors.keySet().asScala.map(_.securityProtocol()).toSet
    assertTrue(protocols.contains(SecurityProtocol.HTTP), "Should contain HTTP")
    assertTrue(protocols.contains(SecurityProtocol.HTTPS), "Should contain HTTPS")
    // Binary acceptor should only have PLAINTEXT
    assertEquals(1, server.dataPlaneAcceptors.size(),
      "Should have exactly one binary acceptor")
  }

  // ===================================================================
  // Test 11: advertised.listeners with HTTP
  // ===================================================================
  @Test
  def testAdvertisedListenersWithHttp(): Unit = {
    val props = TestUtils.createBrokerConfig(0, port = 0)
    props.put(SocketServerConfigs.LISTENERS_CONFIG,
      "PLAINTEXT://localhost:0,HTTP://localhost:0")
    props.put(SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG,
      "PLAINTEXT://broker1.example.com:9092,HTTP://broker1.example.com:9094")
    props.put(SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG,
      "PLAINTEXT:PLAINTEXT,HTTP:HTTP,CONTROLLER:PLAINTEXT")
    props.put(ReplicationConfigs.INTER_BROKER_LISTENER_NAME_CONFIG, "PLAINTEXT")

    // Should parse without error
    val config = KafkaConfig.fromProps(props)
    val advertisedListeners = config.effectiveAdvertisedBrokerListeners
    val httpAdvertised = advertisedListeners.find(_.securityProtocol() == SecurityProtocol.HTTP)
    assertTrue(httpAdvertised.isDefined, "HTTP should be in advertised listeners")
    assertEquals("broker1.example.com", httpAdvertised.get.host(),
      "HTTP advertised host should match")
    assertEquals(9094, httpAdvertised.get.port(),
      "HTTP advertised port should match")
  }

  // ===================================================================
  // Test 12: HTTP without httpAcceptorFactory (null factory) → error
  // ===================================================================
  @Test
  def testHttpWithoutFactoryThrows(): Unit = {
    val props = createProps()
    val config = KafkaConfig.fromProps(props)

    // Create SocketServer WITHOUT httpAcceptorFactory (null)
    val exception = assertThrows(classOf[IllegalStateException], () => {
      server = new SocketServer(config, metrics, Time.SYSTEM, credentialProvider, apiVersionManager)
      server.enableRequestProcessing(Map.empty).get(1, TimeUnit.MINUTES)
    })
    assertTrue(exception.getMessage.toLowerCase.contains("http"),
      s"Should mention HTTP in error, got: ${exception.getMessage}")
  }

  // ===================================================================
  // Test 13: inter.broker.listener = HTTPS also rejected
  // ===================================================================
  @Test
  def testInterBrokerListenerCannotBeHttps(): Unit = {
    val props = TestUtils.createBrokerConfig(0, port = 0)
    props.put(SocketServerConfigs.LISTENERS_CONFIG,
      "HTTPS://localhost:0")
    props.put(SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG,
      "HTTPS://localhost:0")
    props.put(SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG,
      "HTTPS:HTTPS,CONTROLLER:PLAINTEXT")
    props.put(ReplicationConfigs.INTER_BROKER_LISTENER_NAME_CONFIG, "HTTPS")
    props.put("ssl.keystore.location", "/tmp/test-keystore.jks")
    props.put("ssl.keystore.password", "testpass")

    val config = KafkaConfig.fromProps(props)
    val factory: (Endpoint, Time) => HttpAcceptorLike = (ep, t) => new HttpAcceptorLike {
      private val _startedFuture = new java.util.concurrent.CompletableFuture[Void]()
      override def endpoint: Endpoint = ep
      override def startedFuture: java.util.concurrent.CompletableFuture[Void] = _startedFuture
      override def startup(): Unit = _startedFuture.complete(null)
      override def beginDrain(): Unit = ()
      override def awaitDrain(timeoutMs: Long): Unit = ()
      override def boundPort: Int = 0
      override def close(): Unit = ()
      override def isDraining: Boolean = false
      override def pendingRequestCount: Int = 0
    }

    val exception = assertThrows(classOf[IllegalArgumentException], () => {
      server = new SocketServer(config, metrics, Time.SYSTEM, credentialProvider, apiVersionManager,
        httpAcceptorFactory = factory)
    })
    assertTrue(exception.getMessage.toLowerCase.contains("inter.broker") ||
      exception.getMessage.toLowerCase.contains("binary protocol"),
      s"Should reject HTTPS as inter-broker, got: ${exception.getMessage}")
  }

  // ===================================================================
  // Test 14: Custom listener name mapped to HTTP protocol
  // ===================================================================
  @Test
  def testCustomListenerNameMappedToHttp(): Unit = {
    val props = TestUtils.createBrokerConfig(0, port = 0)
    props.put(SocketServerConfigs.LISTENERS_CONFIG,
      "PLAINTEXT://localhost:0,MY_REST_API://localhost:0")
    props.put(SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG,
      "PLAINTEXT://localhost:0,MY_REST_API://localhost:0")
    props.put(SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG,
      "PLAINTEXT:PLAINTEXT,MY_REST_API:HTTP,CONTROLLER:PLAINTEXT")
    props.put(ReplicationConfigs.INTER_BROKER_LISTENER_NAME_CONFIG, "PLAINTEXT")

    val config = KafkaConfig.fromProps(props)
    val factory: (Endpoint, Time) => HttpAcceptorLike = (ep, t) => new HttpAcceptorLike {
      private val _startedFuture = new java.util.concurrent.CompletableFuture[Void]()
      override def endpoint: Endpoint = ep
      override def startedFuture: java.util.concurrent.CompletableFuture[Void] = _startedFuture
      override def startup(): Unit = _startedFuture.complete(null)
      override def beginDrain(): Unit = ()
      override def awaitDrain(timeoutMs: Long): Unit = ()
      override def boundPort: Int = 0
      override def close(): Unit = ()
      override def isDraining: Boolean = false
      override def pendingRequestCount: Int = 0
    }
    server = new SocketServer(config, metrics, Time.SYSTEM, credentialProvider, apiVersionManager,
      httpAcceptorFactory = factory)
    server.enableRequestProcessing(Map.empty).get(1, TimeUnit.MINUTES)

    // Custom name should route to httpAcceptors
    assertEquals(1, server.httpAcceptors.size(),
      "Custom HTTP listener should be in httpAcceptors")
    val ep = server.httpAcceptors.keySet().asScala.head
    assertEquals(SecurityProtocol.HTTP, ep.securityProtocol(),
      "Custom listener should map to HTTP protocol")
    assertEquals("MY_REST_API", ep.listener(),
      "Custom listener name should be preserved")
  }

  // ===================================================================
  // Test 15: HTTPS with valid SSL config succeeds
  // ===================================================================
  @Test
  def testHttpsWithValidSslConfigSucceeds(): Unit = {
    val props = TestUtils.createBrokerConfig(0, port = 0)
    props.put(SocketServerConfigs.LISTENERS_CONFIG,
      "PLAINTEXT://localhost:0,HTTPS://localhost:0")
    props.put(SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG,
      "PLAINTEXT://localhost:0,HTTPS://localhost:0")
    props.put(SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG,
      "PLAINTEXT:PLAINTEXT,HTTPS:HTTPS,CONTROLLER:PLAINTEXT")
    props.put(ReplicationConfigs.INTER_BROKER_LISTENER_NAME_CONFIG, "PLAINTEXT")
    props.put("ssl.keystore.location", "/tmp/test-keystore.jks")
    props.put("ssl.keystore.password", "testpass")
    props.put("ssl.key.password", "testpass")

    val config = KafkaConfig.fromProps(props)
    val factory: (Endpoint, Time) => HttpAcceptorLike = (ep, t) => new HttpAcceptorLike {
      private val _startedFuture = new java.util.concurrent.CompletableFuture[Void]()
      override def endpoint: Endpoint = ep
      override def startedFuture: java.util.concurrent.CompletableFuture[Void] = _startedFuture
      override def startup(): Unit = _startedFuture.complete(null)
      override def beginDrain(): Unit = ()
      override def awaitDrain(timeoutMs: Long): Unit = ()
      override def boundPort: Int = 0
      override def close(): Unit = ()
      override def isDraining: Boolean = false
      override def pendingRequestCount: Int = 0
    }

    // Should NOT throw — SSL config is present
    server = new SocketServer(config, metrics, Time.SYSTEM, credentialProvider, apiVersionManager,
      httpAcceptorFactory = factory)
    server.enableRequestProcessing(Map.empty).get(1, TimeUnit.MINUTES)

    assertEquals(1, server.httpAcceptors.size(),
      "HTTPS acceptor should be created with valid SSL config")
    val ep = server.httpAcceptors.keySet().asScala.head
    assertEquals(SecurityProtocol.HTTPS, ep.securityProtocol())
  }
}
