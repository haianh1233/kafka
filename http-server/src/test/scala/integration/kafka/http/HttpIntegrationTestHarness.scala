/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.http

import java.util.Properties
import kafka.api.IntegrationTestHarness
import kafka.utils.TestUtils
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.network.SocketServerConfigs
import org.apache.kafka.server.config.ReplicationConfigs

import scala.collection.Seq

/**
 * Base class for HTTP integration tests. Starts a Kafka cluster with both
 * PLAINTEXT and HTTP listeners. Subclasses test HTTP produce, consume,
 * health check, and other HTTP endpoints.
 *
 * Usage:
 * {{{
 *   class MyHttpTest extends HttpIntegrationTestHarness {
 *     override def brokerCount: Int = 1
 *
 *     @Test
 *     def testSomething(): Unit = {
 *       val url = httpUrl(0)
 *       val client = new HttpTestClient()
 *       client.start()
 *       try {
 *         // ... send HTTP requests ...
 *       } finally {
 *         client.close()
 *       }
 *     }
 *   }
 * }}}
 *
 * Note: The HTTP listener currently uses a stub HttpAcceptor in core that does
 * not bind to a real network port. Once the Netty-based HTTP server is fully
 * wired (TASK-B.03/B.06 completion), these tests will be able to send real
 * HTTP requests. Until then, the infrastructure compiles and is ready.
 */
abstract class HttpIntegrationTestHarness extends IntegrationTestHarness {

  // Default to 1 broker for local-leader tests.
  // Multi-broker tests (Phase D) override this.
  override def brokerCount: Int = 1

  // The HTTP listener name used in configuration
  val httpListenerName: ListenerName = ListenerName.normalised("HTTP")

  /**
   * Override configureListeners to add HTTP listener alongside PLAINTEXT.
   * Preserves inter-broker listener as PLAINTEXT for binary protocol
   * forwarding (inter.broker.listener.name must remain binary).
   */
  override protected def configureListeners(props: Seq[Properties]): Unit = {
    props.foreach { config =>
      config.remove(ReplicationConfigs.INTER_BROKER_SECURITY_PROTOCOL_CONFIG)
      config.setProperty(
        ReplicationConfigs.INTER_BROKER_LISTENER_NAME_CONFIG,
        interBrokerListenerName.value
      )

      val plaintextPort = TestUtils.RandomPort
      val httpPort = TestUtils.RandomPort

      config.setProperty(SocketServerConfigs.LISTENERS_CONFIG,
        s"${interBrokerListenerName.value}://localhost:$plaintextPort," +
          s"HTTP://localhost:$httpPort")
      config.setProperty(SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG,
        s"${interBrokerListenerName.value}://localhost:$plaintextPort," +
          s"HTTP://localhost:$httpPort")
      config.setProperty(SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG,
        s"${interBrokerListenerName.value}:${SecurityProtocol.PLAINTEXT.name},HTTP:HTTP")

      // HTTP-specific configuration
      config.setProperty("http.consume.max.wait.ms", "5000")
      config.setProperty("num.http.network.threads", "2")
      config.setProperty("num.http.async.threads", "2")
    }
  }

  /**
   * Returns the HTTP base URL for the given broker.
   * Call after setUp() -- brokers must be running.
   *
   * Resolves the HTTP port from the broker's configured listener endpoints.
   * When the HttpAcceptor binds to a real Netty server socket (future work),
   * this method should be updated to read the actual bound port.
   *
   * @param brokerId the broker ID (0-based in tests)
   * @return URL like "http://localhost:54321"
   */
  def httpUrl(brokerId: Int): String = {
    val broker = brokers.find(_.config.brokerId == brokerId)
      .getOrElse(throw new IllegalArgumentException(s"No broker with id $brokerId"))

    // Read the actual bound port from the HTTP acceptor (handles port=0 random assignment)
    val socketServer = broker.socketServer
    val httpAcceptors = socketServer.getClass.getDeclaredField("httpAcceptors")
    httpAcceptors.setAccessible(true)
    val acceptorsMap = httpAcceptors.get(socketServer)
      .asInstanceOf[java.util.concurrent.ConcurrentHashMap[org.apache.kafka.common.Endpoint, kafka.network.HttpAcceptorLike]]

    if (acceptorsMap.isEmpty) {
      // Fallback: read from configured listeners (pre-wiring state)
      val httpEndpoint = broker.config.listeners
        .find(ep => ep.listener().equalsIgnoreCase("HTTP"))
        .getOrElse(throw new IllegalStateException(
          s"No HTTP listener configured on broker $brokerId"))
      s"http://localhost:${httpEndpoint.port()}"
    } else {
      val acceptor = acceptorsMap.values().iterator().next()
      val port = acceptor.boundPort
      s"http://localhost:$port"
    }
  }

  /**
   * Returns the HTTP base URL for broker 0 (convenience for single-broker tests).
   */
  def httpBaseUrl: String = httpUrl(0)
}
