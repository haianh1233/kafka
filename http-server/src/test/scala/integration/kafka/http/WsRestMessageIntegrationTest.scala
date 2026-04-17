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
// Time: Created - TASK-WS2.10
package kafka.http

import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api._

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.util.concurrent.TimeUnit

/**
 * Phase 2 integration tests — REST message operations
 * ({@code publish}, {@code get}, {@code ack}, {@code nack}).
 *
 * <p>Wiring status on the branch at task creation (commit 7dc518d2dd):
 * [[kafka.server.http.rest.MessageRestHandler]] is built and unit-tested
 * ({@code .../rest/MessageRestHandlerTest.java}) but is not passed into
 * [[kafka.network.HttpRequestHandler]] (`messageRestHandler = null`), so
 * every route returns 501 today.
 *
 * <p>Enabled scenarios below prove the router paths are correct; once wiring
 * lands the `@Disabled` scenarios become the full end-to-end acceptance.
 *
 * // Time: Created - TASK-WS2.10
 */
@Timeout(value = 120, unit = TimeUnit.SECONDS)
class WsRestMessageIntegrationTest extends HttpIntegrationTestHarness {

  // ------------------------------------------------------------------
  // Enabled — pre-wiring pin for message operations.
  // ------------------------------------------------------------------

  @Test
  @Timeout(30)
  def testMessageRoutesReachable(): Unit = {
    // Post-T2: MessageRestHandler is wired. Publish/get/ack/nack routes
    // resolve to the real handler (not 501 not-wired). Non-existent
    // exchange/queue returns 404 — correct API behavior.
    val client = HttpClient.newHttpClient()
    val base = httpBaseUrl
    // Pre-declare the exchange so publish returns 200, not 404.
    client.send(HttpRequest.newBuilder()
      .uri(URI.create(s"$base/v1/exchanges/orders"))
      .header("Content-Type", "application/json")
      .PUT(HttpRequest.BodyPublishers.ofString("{\"type\":\"direct\"}"))
      .build(), HttpResponse.BodyHandlers.ofString())

    val publishResp = client.send(HttpRequest.newBuilder()
      .uri(URI.create(s"$base/v1/exchanges/orders/publish"))
      .header("Content-Type", "application/json")
      .POST(HttpRequest.BodyPublishers.ofString(
        """{"routingKey":"k","message":{"body":{},"headers":{}}}"""))
      .build(), HttpResponse.BodyHandlers.ofString())
    assertNotEquals(501, publishResp.statusCode(),
      s"publish route should not be 501 after T2 wiring — got ${publishResp.statusCode()}")
  }

  // ------------------------------------------------------------------
  // Disabled — require MessageRestHandler to be wired + exchange state.
  // ------------------------------------------------------------------

  @Test
  @Timeout(30)
  // T2: enabled — MessageRestHandler wired via HttpAcceptor
  def testRestPublish_routesThroughExchange(): Unit = {
    val client = HttpClient.newHttpClient()
    val base = httpBaseUrl

    // Pre-declare exchange + queue + binding via REST (also pending wiring).
    declareExchange(client, base, "events", "direct")
    declareQueue(client, base, "order-events")
    bind(client, base, "events", "order-events", "order.created")

    val publishBody =
      """{"routingKey":"order.created","message":{"body":{"orderId":"1"},"headers":{}}}"""
    val resp = client.send(
      HttpRequest.newBuilder()
        .uri(URI.create(s"$base/v1/exchanges/events/publish"))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(publishBody))
        .build(),
      HttpResponse.BodyHandlers.ofString())

    assertEquals(200, resp.statusCode(),
      s"REST publish should return 200, got ${resp.statusCode()}: ${resp.body()}")
    // Per MessageRestHandler docs: body should contain per-queue offsets array.
    assertTrue(resp.body().contains("order-events"),
      s"Response should list the matched queue, got: ${resp.body()}")
  }

  @Test
  @Timeout(30)
  // T2: enabled
  def testRestPublish_mandatoryUnroutable_surfacesNoRoute(): Unit = {
    val client = HttpClient.newHttpClient()
    val base = httpBaseUrl
    declareExchange(client, base, "events", "direct")

    val resp = client.send(
      HttpRequest.newBuilder()
        .uri(URI.create(s"$base/v1/exchanges/events/publish"))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(
          """{"routingKey":"no.route","mandatory":true,"message":{"body":{},"headers":{}}}"""))
        .build(),
      HttpResponse.BodyHandlers.ofString())

    assertEquals(200, resp.statusCode())
    // Per handler contract: replyCode 312 (NO_ROUTE).
    assertTrue(resp.body().contains("312") || resp.body().contains("NO_ROUTE"),
      s"Response should indicate NO_ROUTE, got: ${resp.body()}")
  }

  @Test
  @Timeout(30)
  // T8: enabled — KafkaConsumer-backed GetSink.
  def testRestGet_pullsMessage(): Unit = {
    val client = HttpClient.newHttpClient()
    val base = httpBaseUrl
    declareExchange(client, base, "events", "direct")
    declareQueue(client, base, "order-events")
    bind(client, base, "events", "order-events", "order.created")

    // Publish then get.
    client.send(
      HttpRequest.newBuilder()
        .uri(URI.create(s"$base/v1/exchanges/events/publish"))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(
          """{"routingKey":"order.created","message":{"body":{"n":1},"headers":{}}}"""))
        .build(),
      HttpResponse.BodyHandlers.ofString())

    val resp = client.send(
      HttpRequest.newBuilder()
        .uri(URI.create(s"$base/v1/queues/order-events/get"))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString("{\"count\":10,\"ackMode\":\"auto\"}"))
        .build(),
      HttpResponse.BodyHandlers.ofString())
    assertEquals(200, resp.statusCode(), s"REST get returned ${resp.statusCode()}: ${resp.body()}")
    assertTrue(resp.body().contains("messages"), s"Response should include messages array, got: ${resp.body()}")
  }

  @Test
  @Timeout(30)
  @Disabled("Requires QueueManager for queue declaration — T8 wired fetch, but declareQueue is still a stub.")
  def testRestGetManualAckFlow(): Unit = {
    val client = HttpClient.newHttpClient()
    val base = httpBaseUrl
    declareExchange(client, base, "events", "direct")
    declareQueue(client, base, "order-events")
    bind(client, base, "events", "order-events", "order.created")

    client.send(
      HttpRequest.newBuilder()
        .uri(URI.create(s"$base/v1/exchanges/events/publish"))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(
          """{"routingKey":"order.created","message":{"body":{"n":1},"headers":{}}}"""))
        .build(),
      HttpResponse.BodyHandlers.ofString())

    // 1. Pull with ackMode=manual — response includes sessionId + deliveryTag.
    val getResp = client.send(
      HttpRequest.newBuilder()
        .uri(URI.create(s"$base/v1/queues/order-events/get"))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString("{\"count\":1,\"ackMode\":\"manual\"}"))
        .build(),
      HttpResponse.BodyHandlers.ofString())
    assertEquals(200, getResp.statusCode())
    // 2. Ack the delivery tag returned by get (delegated to handler unit test for specifics).
    // Enable this full flow once wiring lands — keeping the smoke-assert.
    assertTrue(getResp.body().contains("sessionId") || getResp.body().contains("deliveryTag"),
      s"Manual-ack get should include session info, got: ${getResp.body()}")
  }

  @Test
  @Timeout(30)
  @Disabled("Requires QueueManager + redelivery round-trip through backing topic.")
  def testRestNack_withRequeue_redelivers(): Unit = {
    // Full redelivery behaviour — covered by unit tests today. Enable once
    // wiring lands and the backing topic is reachable end-to-end.
  }

  // ------------------------------------------------------------------
  // Helpers
  // ------------------------------------------------------------------

  private def declareExchange(client: HttpClient, base: String, name: String, kind: String): Unit = {
    client.send(
      HttpRequest.newBuilder()
        .uri(URI.create(s"$base/v1/exchanges/$name"))
        .header("Content-Type", "application/json")
        .PUT(HttpRequest.BodyPublishers.ofString(s"""{"type":"$kind"}"""))
        .build(),
      HttpResponse.BodyHandlers.ofString())
  }

  private def declareQueue(client: HttpClient, base: String, name: String): Unit = {
    client.send(
      HttpRequest.newBuilder()
        .uri(URI.create(s"$base/v1/queues/$name"))
        .header("Content-Type", "application/json")
        .PUT(HttpRequest.BodyPublishers.ofString("{\"durable\":true}"))
        .build(),
      HttpResponse.BodyHandlers.ofString())
  }

  private def bind(client: HttpClient, base: String, exchange: String, queue: String, rk: String): Unit = {
    val body = s"""{"exchange":"$exchange","queue":"$queue","routingKey":"$rk"}"""
    client.send(
      HttpRequest.newBuilder()
        .uri(URI.create(s"$base/v1/bindings"))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(body))
        .build(),
      HttpResponse.BodyHandlers.ofString())
  }
}
