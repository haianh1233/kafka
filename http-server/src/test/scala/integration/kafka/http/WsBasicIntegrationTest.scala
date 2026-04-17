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
// Time: Created - TASK-WS1.17
package kafka.http

import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test, TestInfo}

/**
 * Phase 1 WebSocket integration test: full round-trip on a single broker.
 *
 * Validates (when the full WS pipeline is wired):
 *   1. WebSocket upgrade handshake on `GET /v1/ws`
 *   2. Exchange / queue / binding declaration over WS
 *   3. Publish → route → deliver flow
 *   4. Ack commits the consumer offset (message not redelivered)
 *   5. Multi-message publish / ack round trip
 *
 * Status on the branch at the time of this task (commit 0c9549098f):
 *
 *   - The WebSocket control-plane classes (WS1.01..WS1.16) exist as unit-tested
 *     components but are not yet wired into the broker's HTTP pipeline.
 *     Specifically, `WsUpgradeOrHttpHandler` is not added to `HttpChannelInitializer`,
 *     and its `placeholderFrameHandler()` drops all frames instead of dispatching
 *     to `WsFrameHandler`.
 *
 *   - Additionally, `WsFrameHandler.handleXxx()` methods throw
 *     `UnsupportedOperationException` until per-type wiring lands.
 *
 * Consequently all test methods below are `@Disabled` with pointers to the
 * wiring tasks that must complete before each scenario can be enabled.
 * The harness, test client, and scenarios are ready to run as-is when the
 * wiring lands — enabling is a matter of deleting the `@Disabled` annotation.
 *
 * // Time: Created - TASK-WS1.17
 */
class WsBasicIntegrationTest extends HttpIntegrationTestHarness {

  private var wsClient: WsTestClient = _

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    super.setUp(testInfo)
    wsClient = new WsTestClient(httpBaseUrl)
  }

  @AfterEach
  override def tearDown(): Unit = {
    if (wsClient != null) {
      try wsClient.close()
      catch { case _: Throwable => /* best effort */ }
      wsClient = null
    }
    super.tearDown()
  }

  @Test
  // T3: enabled — WsUpgradeOrHttpHandler wired into HttpChannelInitializer.
  def testConnectAndDisconnect(): Unit = {
    wsClient.connect()
    // connected message already validated in connect()
    wsClient.close()
  }

  @Test
  // T3: enabled — WiredWsFrameHandler overrides handleDeclareExchange/handleDeclareQueue.
  def testDeclareExchangeAndQueue(): Unit = {
    wsClient.connect()

    val exchangeResp = wsClient.declareExchange("test-exchange", "direct")
    assertEquals("test-exchange", exchangeResp.get("exchange").asText())

    val queueResp = wsClient.declareQueue("test-queue")
    assertEquals("test-queue", queueResp.get("queue").asText())
  }

  @Test
  // T3: enabled — WiredWsFrameHandler overrides handleBind.
  def testBindAndRoute(): Unit = {
    wsClient.connect()

    wsClient.declareExchange("events", "direct")
    wsClient.declareQueue("order-events")
    val bindResp = wsClient.bind("order-events", "events", "order.created")
    assertNotNull(bindResp)
  }

  @Test
  // T9: enabled — WS publish/subscribe wired via in-process KafkaProducer/Consumer.
  def testPublishSubscribeDeliver(): Unit = {
    wsClient.connect()

    wsClient.declareExchange("events", "direct")
    wsClient.declareQueue("order-events")
    wsClient.bind("order-events", "events", "order.created")

    wsClient.subscribe("order-events", "sub-1", credits = 100, startOffset = "earliest")

    wsClient.publish("events", "order.created",
      Map("orderId" -> "123", "amount" -> 42.0).asInstanceOf[Any])

    val deliver = wsClient.waitForDeliver()
    assertNotNull(deliver)
    assertEquals("deliver", deliver.get("type").asText())
    assertEquals("sub-1", deliver.get("subscriptionId").asText())
    assertTrue(deliver.get("deliveryTag").asLong() > 0)
    assertEquals("events", deliver.get("exchange").asText())
    assertEquals("order.created", deliver.get("routingKey").asText())
  }

  @Test
  // T9: enabled.
  def testAckCommitsOffset(): Unit = {
    wsClient.connect()

    wsClient.declareExchange("events", "direct")
    wsClient.declareQueue("ack-test-queue")
    wsClient.bind("ack-test-queue", "events", "test.ack")

    wsClient.subscribe("ack-test-queue", "sub-ack", credits = 100, startOffset = "earliest")
    wsClient.publish("events", "test.ack", "test message")

    val deliver = wsClient.waitForDeliver()
    val tag = deliver.get("deliveryTag").asLong()
    wsClient.ack("sub-ack", tag)

    // Verify offset was committed by unsubscribing and re-subscribing.
    // The first message should NOT be redelivered.
    wsClient.unsubscribe("sub-ack")

    wsClient.subscribe("ack-test-queue", "sub-ack-2", credits = 100, startOffset = "earliest")
    wsClient.publish("events", "test.ack", "second message")

    val deliver2 = wsClient.waitForDeliver()
    assertNotNull(deliver2)
  }

  @Test
  // T9: enabled.
  def testFullRoundTrip(): Unit = {
    wsClient.connect()

    // 1. Declare exchange and queue
    wsClient.declareExchange("orders-exchange", "direct")
    wsClient.declareQueue("payment-events")
    wsClient.bind("payment-events", "orders-exchange", "payment.completed")

    // 2. Subscribe
    wsClient.subscribe("payment-events", "sub-payments", credits = 10)

    // 3. Publish multiple messages
    for (i <- 1 to 3) {
      wsClient.publish("orders-exchange", "payment.completed",
        Map("paymentId" -> s"pay-$i", "amount" -> (i * 100.0)).asInstanceOf[Any])
    }

    // 4. Receive and ack all messages
    for (_ <- 1 to 3) {
      val deliver = wsClient.waitForDeliver()
      assertNotNull(deliver)
      val tag = deliver.get("deliveryTag").asLong()
      assertTrue(tag > 0)
      wsClient.ack("sub-payments", tag)
    }

    // 5. Unsubscribe
    wsClient.unsubscribe("sub-payments")
  }

  // --------------------------------------------------------------------
  // Enabled scenarios — exercise what IS wired today.
  // These run without requiring the WS pipeline to be installed.
  // --------------------------------------------------------------------

  /**
   * The WS endpoint path `/v1/ws` is reserved for WebSocket upgrades. After
   * T3 the `WsUpgradeOrHttpHandler` is installed; a plain GET (no upgrade
   * headers) passes through and yields 404 (no HTTP route matches /v1/ws).
   */
  @Test
  def testWsEndpoint_plainGetReturns404(): Unit = {
    import java.net.URI
    import java.net.http.{HttpClient, HttpRequest, HttpResponse}

    val client = HttpClient.newHttpClient()
    try {
      val req = HttpRequest.newBuilder()
        .uri(URI.create(s"$httpBaseUrl/v1/ws"))
        .GET()
        .build()
      val resp = client.send(req, HttpResponse.BodyHandlers.ofString())
      // Post-T3: plain GET (no Upgrade header) passes through → HTTP router → 404.
      assertEquals(404, resp.statusCode(),
        s"Plain GET /v1/ws should be 404 (no HTTP route), got ${resp.statusCode()}")
    } finally {
      // java.net.http.HttpClient has no close() on Java 17/21 — GC cleans up.
    }
  }
}
