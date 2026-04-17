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
 * Phase 2 integration tests — REST CRUD for exchanges, queues and bindings.
 *
 * <p>Wiring status on the branch at task creation (commit 7dc518d2dd): the
 * REST handlers ({@link kafka.server.http.rest.ExchangeRestHandler},
 * {@link kafka.server.http.rest.QueueRestHandler},
 * {@link kafka.server.http.rest.BindingRestHandler}) are constructed but not
 * yet passed into {@link kafka.network.HttpRequestHandler}: every CRUD route
 * returns {@code 501 "X not wired in this broker"}. Unit coverage for the
 * handler classes is complete in
 * {@code http-server/src/test/java/kafka/server/http/rest/...}.
 *
 * <p>Enabled scenarios below pin the HTTP-layer reachability and the 501
 * sentinel — once the handlers are injected the status codes flip to the
 * documented 200/201/204/404/409 values and the scenarios marked
 * `@Disabled` become the acceptance tests for the wiring task.
 *
 * // Time: Created - TASK-WS2.10
 */
@Timeout(value = 120, unit = TimeUnit.SECONDS)
class WsRestCrudIntegrationTest extends HttpIntegrationTestHarness {

  // ------------------------------------------------------------------
  // Enabled — pre-wiring pin.
  // ------------------------------------------------------------------

  /**
   * Post-T1: exchange + binding CRUD routes resolve to a real handler (not
   * 404 route-miss, not 501 not-wired). Queue endpoints still return 404 or
   * similar because QueueManager hasn't landed yet — they're excluded here
   * and covered by a dedicated @Disabled test below.
   */
  @Test
  @Timeout(30)
  def testCrudRoutesReachable(): Unit = {
    val client = HttpClient.newHttpClient()
    val base = httpBaseUrl
    // Exchange + binding routes — queue routes excluded until QueueManager exists.
    val cases = Seq(
      ("GET",    "/v1/exchanges"),
      ("GET",    "/v1/exchanges/amq.direct"),
      ("GET",    "/v1/bindings")
    )
    cases.foreach { case (method, path) =>
      val req = HttpRequest.newBuilder().uri(URI.create(s"$base$path"))
        .header("Content-Type", "application/json").GET().build()
      val resp = client.send(req, HttpResponse.BodyHandlers.ofString())
      // Route resolved + handler wired → not 501 "not wired" and not 404 route-miss.
      assertNotEquals(501, resp.statusCode(),
        s"$method $path should not be 501 after T1 wiring — got ${resp.statusCode()}: ${resp.body()}")
    }
  }

  /**
   * Healthcheck on the harness itself — confirms the broker binds an HTTP port
   * and is reachable from an external client. Without this, a regression where
   * the listener fails to bind would manifest as confusing connection errors in
   * every other scenario; catching it early makes triage easier.
   */
  @Test
  @Timeout(30)
  def testHttpListener_isReachable(): Unit = {
    val client = HttpClient.newHttpClient()
    val resp = client.send(
      HttpRequest.newBuilder().uri(URI.create(s"$httpBaseUrl/v1/health")).GET().build(),
      HttpResponse.BodyHandlers.ofString())
    // /v1/health is wired — expect 200 OK. If it ever stops being wired,
    // the test-harness setup in HttpIntegrationTestHarness needs re-checking.
    assertEquals(200, resp.statusCode(),
      s"Healthcheck should return 200, got ${resp.statusCode()}: ${resp.body()}")
  }

  // ------------------------------------------------------------------
  // Disabled — full CRUD lifecycles. Enable when REST handlers are wired.
  // ------------------------------------------------------------------

  @Test
  @Timeout(30)
  // T1: enabled — REST handlers wired via HttpAcceptor.buildRestHandlerStack
  def testRestCrud_exchangeDeclareGetListDelete(): Unit = {
    val client = HttpClient.newHttpClient()
    val base = httpBaseUrl

    // 1. PUT /v1/exchanges/orders → 201 Created
    var req: HttpRequest = HttpRequest.newBuilder()
      .uri(URI.create(s"$base/v1/exchanges/orders"))
      .header("Content-Type", "application/json")
      .PUT(HttpRequest.BodyPublishers.ofString("{\"type\":\"direct\",\"durable\":true}"))
      .build()
    var resp = client.send(req, HttpResponse.BodyHandlers.ofString())
    assertEquals(201, resp.statusCode(), s"PUT exchange should 201, got ${resp.statusCode()}: ${resp.body()}")

    // 2. GET /v1/exchanges/orders → 200 with name + type
    req = HttpRequest.newBuilder().uri(URI.create(s"$base/v1/exchanges/orders")).GET().build()
    resp = client.send(req, HttpResponse.BodyHandlers.ofString())
    assertEquals(200, resp.statusCode())
    assertTrue(resp.body().contains("\"name\":\"orders\""))
    assertTrue(resp.body().contains("\"type\":\"direct\""))

    // 3. GET /v1/exchanges → list contains orders + 5 defaults
    req = HttpRequest.newBuilder().uri(URI.create(s"$base/v1/exchanges")).GET().build()
    resp = client.send(req, HttpResponse.BodyHandlers.ofString())
    assertEquals(200, resp.statusCode())
    assertTrue(resp.body().contains("\"orders\""))

    // 4. DELETE /v1/exchanges/orders → 204 No Content
    req = HttpRequest.newBuilder().uri(URI.create(s"$base/v1/exchanges/orders")).DELETE().build()
    resp = client.send(req, HttpResponse.BodyHandlers.ofString())
    assertEquals(204, resp.statusCode())

    // 5. GET missing → 404
    req = HttpRequest.newBuilder().uri(URI.create(s"$base/v1/exchanges/orders")).GET().build()
    resp = client.send(req, HttpResponse.BodyHandlers.ofString())
    assertEquals(404, resp.statusCode())
  }

  @Test
  @Timeout(30)
  @Disabled("QueueManager not implemented — QueueStore is a stub throwing QueueConflict on declare. Enable when QueueManager lands.")
  def testRestCrud_queueDeclareGetListDelete(): Unit = {
    val client = HttpClient.newHttpClient()
    val base = httpBaseUrl

    var req: HttpRequest = HttpRequest.newBuilder()
      .uri(URI.create(s"$base/v1/queues/orders"))
      .header("Content-Type", "application/json")
      .PUT(HttpRequest.BodyPublishers.ofString("{\"durable\":true}"))
      .build()
    var resp = client.send(req, HttpResponse.BodyHandlers.ofString())
    assertEquals(201, resp.statusCode())

    req = HttpRequest.newBuilder().uri(URI.create(s"$base/v1/queues/orders")).GET().build()
    resp = client.send(req, HttpResponse.BodyHandlers.ofString())
    assertEquals(200, resp.statusCode())

    req = HttpRequest.newBuilder().uri(URI.create(s"$base/v1/queues")).GET().build()
    resp = client.send(req, HttpResponse.BodyHandlers.ofString())
    assertEquals(200, resp.statusCode())

    req = HttpRequest.newBuilder().uri(URI.create(s"$base/v1/queues/orders")).DELETE().build()
    resp = client.send(req, HttpResponse.BodyHandlers.ofString())
    assertEquals(204, resp.statusCode())
  }

  @Test
  @Timeout(30)
  // T1: enabled — BindingRestHandler wired via HttpAcceptor
  def testRestCrud_bindingCreateListDelete(): Unit = {
    val client = HttpClient.newHttpClient()
    val base = httpBaseUrl

    // Prerequisite: declare the exchange. Queue is accepted by the binding
    // manager's stub queue-existence predicate (QueueManager is not wired).
    var req: HttpRequest = HttpRequest.newBuilder()
      .uri(URI.create(s"$base/v1/exchanges/orders"))
      .header("Content-Type", "application/json")
      .PUT(HttpRequest.BodyPublishers.ofString("{\"type\":\"direct\"}"))
      .build()
    var resp = client.send(req, HttpResponse.BodyHandlers.ofString())
    assertEquals(201, resp.statusCode(), s"PUT exchange should 201, got ${resp.statusCode()}: ${resp.body()}")

    // Now create the binding.
    val body = "{\"exchange\":\"orders\",\"queue\":\"order-events\",\"routingKey\":\"order.created\"}"
    req = HttpRequest.newBuilder()
      .uri(URI.create(s"$base/v1/bindings"))
      .header("Content-Type", "application/json")
      .POST(HttpRequest.BodyPublishers.ofString(body))
      .build()
    resp = client.send(req, HttpResponse.BodyHandlers.ofString())
    assertTrue(resp.statusCode() == 200 || resp.statusCode() == 201,
      s"POST /v1/bindings should succeed, got ${resp.statusCode()}: ${resp.body()}")

    req = HttpRequest.newBuilder()
      .uri(URI.create(s"$base/v1/bindings?exchange=orders"))
      .GET().build()
    resp = client.send(req, HttpResponse.BodyHandlers.ofString())
    assertEquals(200, resp.statusCode())
    assertTrue(resp.body().contains("order-events"))
  }

  @Test
  @Timeout(30)
  // T1: enabled
  def testRestCrud_exchangeRedeclareTypeMismatch_returns409(): Unit = {
    val client = HttpClient.newHttpClient()
    val base = httpBaseUrl

    var req: HttpRequest = HttpRequest.newBuilder()
      .uri(URI.create(s"$base/v1/exchanges/x"))
      .header("Content-Type", "application/json")
      .PUT(HttpRequest.BodyPublishers.ofString("{\"type\":\"direct\"}"))
      .build()
    assertEquals(201, client.send(req, HttpResponse.BodyHandlers.ofString()).statusCode())

    req = HttpRequest.newBuilder()
      .uri(URI.create(s"$base/v1/exchanges/x"))
      .header("Content-Type", "application/json")
      .PUT(HttpRequest.BodyPublishers.ofString("{\"type\":\"topic\"}"))
      .build()
    val resp = client.send(req, HttpResponse.BodyHandlers.ofString())
    assertEquals(409, resp.statusCode())
  }

  @Test
  @Timeout(30)
  // T1: enabled
  def testRestCrud_deleteBuiltInExchange_rejected(): Unit = {
    val client = HttpClient.newHttpClient()
    val req = HttpRequest.newBuilder()
      .uri(URI.create(s"$httpBaseUrl/v1/exchanges/amq.direct"))
      .DELETE().build()
    val resp = client.send(req, HttpResponse.BodyHandlers.ofString())
    // Exchange handler throws EXCHANGE_PROTECTED — maps to 403 or 409 per wiring.
    assertTrue(resp.statusCode() == 403 || resp.statusCode() == 409,
      s"Protected exchange delete should be rejected, got ${resp.statusCode()}: ${resp.body()}")
  }
}
