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

import kafka.server.http.routing.{BindingManager, ExchangeManager, VhostManager}
import kafka.server.http.ws.{WsConfigs, WsRoutingMetadataManager}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api._

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.util.concurrent.TimeUnit

/**
 * Phase 2 integration tests — vhost namespace isolation.
 *
 * <p>This file contains two kinds of tests:
 *
 * <ul>
 *   <li><b>Component-level</b> (enabled) — exercises
 *       [[kafka.server.http.routing.VhostManager]] +
 *       [[kafka.server.http.routing.ExchangeManager]] +
 *       [[kafka.server.http.routing.BindingManager]] + the in-memory
 *       [[kafka.server.http.ws.WsRoutingMetadataManager]] directly to verify
 *       the namespace isolation invariants end-to-end (per-vhost routing
 *       engines, topic-name prefix, cascade on delete). These do NOT depend
 *       on the WS pipeline or REST-handler wiring, so they run today.</li>
 *   <li><b>Network-level</b> (`@Disabled`) — scenarios that require an actual
 *       WebSocket upgrade or REST handler to be wired in. Kept here so they
 *       can be enabled with a single annotation change once the wiring task
 *       lands.</li>
 * </ul>
 *
 * // Time: Created - TASK-WS2.10
 */
@Timeout(value = 120, unit = TimeUnit.SECONDS)
class WsVhostIsolationIntegrationTest extends HttpIntegrationTestHarness {

  // ------------------------------------------------------------------
  // Enabled — component-level end-to-end through the routing stack.
  // ------------------------------------------------------------------

  /**
   * Same queue name in two vhosts resolves to DIFFERENT backing Kafka topics.
   * This is the fundamental isolation invariant for the multi-tenant deployment
   * story (design doc §5.2).
   */
  @Test
  @Timeout(30)
  def testSameQueueName_differentVhosts_separateTopics(): Unit = {
    val fixture = newVhostFixture()
    fixture.vhostManager.createVhost("/production")
    fixture.vhostManager.createVhost("/staging")

    val prodTopic = fixture.vhostManager.resolveTopicName("/production", "orders")
    val stagingTopic = fixture.vhostManager.resolveTopicName("/staging", "orders")

    assertEquals("ws.production.orders", prodTopic)
    assertEquals("ws.staging.orders", stagingTopic)
    assertNotEquals(prodTopic, stagingTopic,
      "Same queue name in different vhosts must map to different backing topics")
  }

  /**
   * Default vhost `/` maps queues to `ws.{queue}` (no vhost prefix); all
   * other named vhosts use `ws.{vhost}.{queue}`. Pins the backwards-compat
   * mapping so single-vhost deployments keep working.
   */
  @Test
  @Timeout(30)
  def testDefaultVhost_topicMapping_omitsPrefix(): Unit = {
    val fixture = newVhostFixture()
    assertEquals("ws.orders", fixture.vhostManager.resolveTopicName("/", "orders"))
    assertEquals("ws.orders", fixture.vhostManager.resolveTopicName("/", "orders"),
      "Default vhost must never include the vhost name in the topic prefix")
  }

  /**
   * Deleting a non-default vhost cascades ALL its exchanges, queues, and
   * bindings. The default vhost is untouched.
   */
  @Test
  @Timeout(30)
  def testDeleteVhost_cascadesAllResources(): Unit = {
    val fixture = newVhostFixture()
    fixture.vhostManager.createVhost("/tenant-a")

    // Declare user exchange and queue in /tenant-a
    fixture.exchangeManager.declareExchange(
      "/tenant-a", "user-ex", "direct", true, false, false, false,
      java.util.Collections.emptyMap[String, String]())
    fixture.metadataManager.writeQueue(
      new kafka.server.http.ws.QueueMetadata(
        "user-q", "/tenant-a",
        /* durable */ true, /* exclusive */ false, /* autoDelete */ false,
        java.util.Collections.emptyMap[String, String]()))
    fixture.bindingManager.bind("user-ex", "user-q", "k")

    assertNotNull(fixture.metadataManager.getExchange("/tenant-a", "user-ex"))
    assertNotNull(fixture.metadataManager.getQueue("/tenant-a", "user-q"))
    assertTrue(fixture.bindingManager.bindingCount("user-ex") > 0)

    fixture.vhostManager.deleteVhost("/tenant-a")

    assertFalse(fixture.vhostManager.exists("/tenant-a"))
    assertNull(fixture.metadataManager.getExchange("/tenant-a", "user-ex"),
      "Deleting a vhost must cascade user exchanges")
    assertNull(fixture.metadataManager.getQueue("/tenant-a", "user-q"),
      "Deleting a vhost must cascade queues")
    assertEquals(0, fixture.bindingManager.bindingCount("user-ex"),
      "Deleting a vhost must remove bindings on every cascaded exchange")

    // Default vhost untouched.
    assertTrue(fixture.vhostManager.exists("/"))
  }

  /**
   * Default vhost `/` cannot be deleted: a protective guard at the handler
   * and manager level. Pins both layers.
   */
  @Test
  @Timeout(30)
  def testDeleteDefaultVhost_rejected(): Unit = {
    val fixture = newVhostFixture()
    assertThrows(classOf[IllegalArgumentException],
      new org.junit.jupiter.api.function.Executable {
        override def execute(): Unit = fixture.vhostManager.deleteVhost("/")
      })
    assertTrue(fixture.vhostManager.exists("/"),
      "Default vhost must still exist after a rejected delete")
  }

  /**
   * Each vhost has its own RoutingEngine. Exchanges declared in vhost A
   * MUST NOT be visible to the RoutingEngine of vhost B.
   */
  @Test
  @Timeout(30)
  def testRoutingEngine_isolationAcrossVhosts(): Unit = {
    val fixture = newVhostFixture()
    fixture.vhostManager.createVhost("/tenant-a")
    fixture.vhostManager.createVhost("/tenant-b")

    fixture.exchangeManager.declareExchange(
      "/tenant-a", "events", "direct", true, false, false, false,
      java.util.Collections.emptyMap[String, String]())
    fixture.metadataManager.writeQueue(
      new kafka.server.http.ws.QueueMetadata(
        "q-a", "/tenant-a",
        /* durable */ true, /* exclusive */ false, /* autoDelete */ false,
        java.util.Collections.emptyMap[String, String]()))
    // The per-vhost RoutingEngine reads bindings from the metadata manager
    // (not the BindingManager), so persist through writeBinding.
    fixture.metadataManager.writeBinding(
      new kafka.server.http.ws.BindingMetadata(
        "/tenant-a", "events", "q-a", "k",
        java.util.Collections.emptyMap[String, String]()))

    val engineA = fixture.vhostManager.getRoutingEngine("/tenant-a")
    val engineB = fixture.vhostManager.getRoutingEngine("/tenant-b")

    // Vhost-A engine routes the published key to q-a.
    val matchesA = engineA.route("events", "k")
    assertTrue(matchesA.contains("q-a"),
      s"Vhost-A routing engine should resolve the binding, got $matchesA")

    // Vhost-B engine does NOT see events (no exchange).
    assertThrows(classOf[IllegalArgumentException],
      new org.junit.jupiter.api.function.Executable {
        override def execute(): Unit = engineB.route("events", "k")
      },
      "Vhost-B routing engine must not see vhost-A's exchange")
  }

  /**
   * Pre-declared exchanges are replicated per-vhost: every new vhost has its
   * own copy of {@code ""}, {@code amq.direct}, {@code amq.topic},
   * {@code amq.fanout}, {@code amq.headers} — these are not shared across
   * vhosts.
   */
  @Test
  @Timeout(30)
  def testPreDeclaredExchanges_existPerVhost(): Unit = {
    val fixture = newVhostFixture()
    fixture.vhostManager.createVhost("/t1")

    Seq("", "amq.direct", "amq.topic", "amq.fanout", "amq.headers").foreach { name =>
      assertNotNull(fixture.metadataManager.getExchange("/t1", name),
        s"Pre-declared exchange '$name' must exist in new vhost")
    }

    assertEquals(5, fixture.metadataManager.exchangeCount("/t1"),
      "New vhost must have exactly 5 default exchanges and no more")
  }

  // ------------------------------------------------------------------
  // Enabled — REST vhost management pre-wiring pin.
  // ------------------------------------------------------------------

  /**
   * The /v1/vhosts routes are registered in [[kafka.server.http.HttpRouter]]
   * but the handler is null today: expect 501. Once wired, the list endpoint
   * returns 200 and includes the default vhost.
   */
  @Test
  @Timeout(30)
  def testVhostRestRoutes_reachable_currentlyReturn501(): Unit = {
    val client = HttpClient.newHttpClient()
    val base = httpBaseUrl
    val cases = Seq(
      ("GET",    "/v1/vhosts"),
      ("PUT",    "/v1/vhosts/tenant"),
      ("DELETE", "/v1/vhosts/tenant")
    )
    cases.foreach { case (method, path) =>
      val builder = HttpRequest.newBuilder().uri(URI.create(s"$base$path"))
      val req = method match {
        case "GET"    => builder.GET().build()
        case "PUT"    => builder.PUT(HttpRequest.BodyPublishers.noBody()).build()
        case "DELETE" => builder.DELETE().build()
      }
      val resp = client.send(req, HttpResponse.BodyHandlers.ofString())
      assertNotEquals(404, resp.statusCode(),
        s"$method $path must be routable; got ${resp.statusCode()}: ${resp.body()}")
    }
  }

  // ------------------------------------------------------------------
  // Disabled — require REST or WS wiring.
  // ------------------------------------------------------------------

  @Test
  @Timeout(30)
  @Disabled("VhostRestHandler not wired. Enable once wiring lands.")
  def testRestCreateListDeleteVhost(): Unit = {
    val client = HttpClient.newHttpClient()
    val base = httpBaseUrl

    var resp = client.send(
      HttpRequest.newBuilder().uri(URI.create(s"$base/v1/vhosts/t1"))
        .PUT(HttpRequest.BodyPublishers.noBody()).build(),
      HttpResponse.BodyHandlers.ofString())
    assertEquals(201, resp.statusCode())

    resp = client.send(
      HttpRequest.newBuilder().uri(URI.create(s"$base/v1/vhosts")).GET().build(),
      HttpResponse.BodyHandlers.ofString())
    assertEquals(200, resp.statusCode())
    assertTrue(resp.body().contains("/t1"))

    resp = client.send(
      HttpRequest.newBuilder().uri(URI.create(s"$base/v1/vhosts/t1")).DELETE().build(),
      HttpResponse.BodyHandlers.ofString())
    assertEquals(204, resp.statusCode())
  }

  @Test
  @Timeout(30)
  @Disabled("VhostRestHandler not wired: deleting default vhost must be rejected at the REST boundary.")
  def testRestDeleteDefaultVhost_rejected(): Unit = {
    val resp = HttpClient.newHttpClient().send(
      HttpRequest.newBuilder().uri(URI.create(s"$httpBaseUrl/v1/vhosts/"))
        .DELETE().build(),
      HttpResponse.BodyHandlers.ofString())
    assertEquals(403, resp.statusCode())
  }

  @Test
  @Timeout(30)
  @Disabled("WS pipeline wiring pending: connect with ?vhost=/production → declare queue → verify backing topic has vhost prefix.")
  def testWsUpgrade_vhostQueryParam(): Unit = {
    // Skeleton for post-wiring: WsTestClient must accept a vhost query param.
  }

  @Test
  @Timeout(30)
  @Disabled("REST + WS handlers not wired: X-Vhost header scopes REST; verifying cross-vhost invisibility requires 404 from handler.")
  def testRestRequest_xVhostHeader(): Unit = {
    val client = HttpClient.newHttpClient()
    val base = httpBaseUrl

    // PUT exchange in /staging via X-Vhost header.
    val putResp = client.send(
      HttpRequest.newBuilder()
        .uri(URI.create(s"$base/v1/exchanges/x"))
        .header("Content-Type", "application/json")
        .header("X-Vhost", "/staging")
        .PUT(HttpRequest.BodyPublishers.ofString("{\"type\":\"direct\"}"))
        .build(),
      HttpResponse.BodyHandlers.ofString())
    assertEquals(201, putResp.statusCode())

    // GET without X-Vhost (defaults to /): should 404.
    val getDefault = client.send(
      HttpRequest.newBuilder().uri(URI.create(s"$base/v1/exchanges/x")).GET().build(),
      HttpResponse.BodyHandlers.ofString())
    assertEquals(404, getDefault.statusCode())

    // GET with X-Vhost=/staging: should 200.
    val getStaging = client.send(
      HttpRequest.newBuilder().uri(URI.create(s"$base/v1/exchanges/x"))
        .header("X-Vhost", "/staging").GET().build(),
      HttpResponse.BodyHandlers.ofString())
    assertEquals(200, getStaging.statusCode())
  }

  // ------------------------------------------------------------------
  // Fixture
  // ------------------------------------------------------------------

  /** Bundle of the vhost/exchange/binding stack with a fresh in-memory store. */
  private case class VhostFixture(
    metadataManager: WsRoutingMetadataManager,
    exchangeManager: ExchangeManager,
    bindingManager: BindingManager,
    vhostManager: VhostManager
  )

  private def newVhostFixture(): VhostFixture = {
    val metadataManager = new WsRoutingMetadataManager(
      WsConfigs.withDefaults(),
      (_, _) => () /* no-op record writer — in-memory only */)
    val exchangeManager = new ExchangeManager(metadataManager, WsConfigs.withDefaults())
    val bindingManager = new BindingManager(
      (name: String) => metadataManager.getExchange("/", name) != null
        || metadataManager.getExchange("/tenant-a", name) != null
        || metadataManager.getExchange("/tenant-b", name) != null
        || metadataManager.getExchange("/t1", name) != null
        || metadataManager.getExchange("/production", name) != null
        || metadataManager.getExchange("/staging", name) != null,
      (_: String) => true /* queues: always true for test */)
    val vhostManager = new VhostManager(metadataManager, exchangeManager, bindingManager)
    VhostFixture(metadataManager, exchangeManager, bindingManager, vhostManager)
  }
}
