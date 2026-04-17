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
// Time: Created - TASK-WS-T.03
package kafka.http

import kafka.http.HttpTestClient._
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api._

import java.time.{Duration => JDuration}
import java.util.Properties
import java.util.concurrent.TimeUnit

/**
 * Cross-protocol integration tests: verify that messages flow correctly between
 * WebSocket, HTTP REST, and Kafka binary clients on the same backing topic.
 *
 * Per the design doc §23.5, a queue named "orders" maps to the backing Kafka
 * topic "ws.orders". Cross-protocol scenarios produce on one protocol and
 * consume on another against this shared topic.
 *
 * Wiring status on the branch at the time of this task (commit 04b62eae7d):
 *
 *   - HTTP produce/consume on plain Kafka topics is fully wired and tested
 *     (see [[HttpProduceIntegrationTest]] and [[HttpConsumeIntegrationTest]]).
 *
 *   - The WebSocket pipeline is NOT wired into [[kafka.network.HttpChannelInitializer]]:
 *     `WsUpgradeOrHttpHandler` is not added to the pipeline, and the WS REST
 *     handlers (`ExchangeRestHandler`, `MessageRestHandler`, etc.) default to
 *     `null` in `HttpRequestHandler`, causing those routes to return
 *     501 NOT_IMPLEMENTED ("not wired in this broker").
 *
 * The two scenarios that DO work today (HTTP ↔ Kafka binary on a `ws.{queue}`
 * topic) are enabled below; the WS-dependent scenarios are `@Disabled` with
 * pointers to the wiring tasks that must complete before they can be enabled.
 *
 * // Time: Created - TASK-WS-T.03
 */
@Timeout(value = 120, unit = TimeUnit.SECONDS)
class WsCrossProtocolIntegrationTest extends HttpIntegrationTestHarness {

  private var ws: WsTestClient = _
  private var http: HttpTestClient = _

  // Backing topic name for a logical WS queue named "orders".
  private val queueName = "orders"
  private val backingTopic = s"ws.$queueName"
  private val numPartitions = 1
  private val replicationFactor: Short = 1

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    super.setUp(testInfo)
    // Pre-create the backing topic so HTTP produce/consume and Kafka binary
    // producer/consumer can run against it without auto-create races.
    createTopic(backingTopic, numPartitions, replicationFactor)
    http = new HttpTestClient()
    http.start()
    // WS client is constructed lazily because connect() requires the WS
    // pipeline, which is not wired in the current build.
    ws = new WsTestClient(httpBaseUrl)
  }

  @AfterEach
  override def tearDown(): Unit = {
    if (ws != null) {
      try ws.close()
      catch { case _: Throwable => /* best effort */ }
      ws = null
    }
    if (http != null) {
      try http.close()
      catch { case _: Throwable => /* best effort */ }
      http = null
    }
    super.tearDown()
  }

  // ====================================================================
  // ENABLED — exercise the cross-protocol bridge that already works:
  // HTTP REST ↔ Kafka binary on a `ws.{queue}` topic. These scenarios
  // validate that the topic-naming convention used by the WS layer is
  // compatible with both the HTTP endpoint and the binary clients, so that
  // when the WS pipeline lands the bridge already has a verified path.
  // ====================================================================

  /**
   * Kafka binary producer → HTTP fetch on the `ws.{queue}` backing topic.
   *
   * This verifies that records produced by a binary client land on the same
   * topic that the WS layer would consume from, and can be read back via the
   * HTTP fetch endpoint (`POST /v1/topics/{topic}/records:fetch`). This is the
   * "consume side" of the bridge.
   */
  @Test
  @Timeout(30)
  def testKafkaProducer_httpFetch_onWsBackingTopic(): Unit = {
    val producer = createProducer(new StringSerializer, new StringSerializer)
    try {
      val record = new ProducerRecord[String, String](backingTopic, 0, "k", "kafka-binary-payload")
      producer.send(record).get(10, TimeUnit.SECONDS)
      producer.flush()
    } finally {
      producer.close(JDuration.ofSeconds(5))
    }

    val response = http.consume(httpBaseUrl, backingTopic,
      Seq(FetchPartitionSpec(partition = 0, offset = 0)),
      maxWaitMs = 5000)

    assertEquals(200, response.status,
      s"HTTP fetch on backing topic should succeed. Body: ${response.body}")
    val partitions = response.body.get("partitions")
    assertNotNull(partitions, "Response must contain partitions array")
    assertTrue(partitions.size() > 0, "Expected at least one partition entry")
    val records = partitions.get(0).get("records")
    assertNotNull(records, "Records array must be present")
    assertTrue(records.size() > 0,
      s"Expected to fetch the binary-produced record, got: $records")
  }

  /**
   * HTTP POST records → Kafka binary consumer on the `ws.{queue}` backing topic.
   *
   * This verifies that records produced via the HTTP endpoint
   * (`POST /v1/topics/{topic}/records`) land on the same backing topic that the
   * WS layer would publish to / consume from, and can be read back by a
   * standard Kafka binary consumer. This is the "produce side" of the bridge.
   */
  @Test
  @Timeout(30)
  def testHttpProduce_kafkaConsumer_onWsBackingTopic(): Unit = {
    val produceResp = http.produce(httpBaseUrl, backingTopic,
      Seq(ProduceRecord(
        partition = Some(0),
        value = Some(StringValue("http-produced-payload"))
      )),
      acks = "all")

    assertEquals(200, produceResp.status,
      s"HTTP produce to backing topic should succeed. Body: ${produceResp.body}")

    // Read back via Kafka binary consumer.
    val consumer = newBinaryConsumer()
    try {
      consumer.subscribe(java.util.List.of(backingTopic))
      val deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(20)
      var found: ConsumerRecord[String, String] = null
      while (found == null && System.nanoTime() < deadline) {
        val polled = consumer.poll(JDuration.ofMillis(500))
        val it = polled.records(backingTopic).iterator()
        if (it.hasNext) found = it.next()
      }
      assertNotNull(found, s"Kafka binary consumer should read the HTTP-produced record from $backingTopic")
      assertEquals("http-produced-payload", found.value())
    } finally {
      consumer.close(JDuration.ofSeconds(5))
    }
  }

  // ====================================================================
  // DISABLED — require the WS pipeline to be wired in
  // [[kafka.network.HttpChannelInitializer]] (WS upgrade handler) and the
  // WS REST handlers (`ExchangeRestHandler`, `MessageRestHandler`, etc.)
  // to be passed into [[kafka.network.HttpRequestHandler]] instead of the
  // current `null` defaults. Until then these tests cannot exercise the
  // routing path end-to-end.
  // ====================================================================

  @Test
  @Timeout(30)
  @Disabled("Data-plane Kafka wiring pending: WS publish/subscribe/deliver require real Kafka produce + fetch integration (post-T3 control plane works).")
  def testWsPublish_httpFetch(): Unit = {
    ws.connect()
    ws.declareExchange("events", "direct")
    ws.declareQueue(queueName)
    ws.bind(queueName, "events", "order.created")

    ws.publish("events", "order.created",
      Map("orderId" -> "123", "amount" -> 42.0).asInstanceOf[Any])

    // The published message should land on `ws.orders` and be readable via HTTP fetch.
    val response = http.consume(httpBaseUrl, backingTopic,
      Seq(FetchPartitionSpec(partition = 0, offset = 0)),
      maxWaitMs = 5000)

    assertEquals(200, response.status, s"HTTP fetch should succeed. Body: ${response.body}")
    val partitions = response.body.get("partitions")
    assertNotNull(partitions)
    assertTrue(partitions.size() > 0)
    val records = partitions.get(0).get("records")
    assertTrue(records.size() > 0,
      s"Expected the WS-published message to be fetched via HTTP, got: $records")
  }

  @Test
  @Timeout(30)
  @Disabled("Data-plane Kafka wiring pending: WS publish/subscribe/deliver require real Kafka produce + fetch integration (post-T3 control plane works).")
  def testHttpProduce_wsDeliver(): Unit = {
    ws.connect()
    ws.declareQueue(queueName)
    ws.subscribe(queueName, "sub-1", credits = 10, startOffset = "earliest")

    val produceResp = http.produce(httpBaseUrl, backingTopic,
      Seq(ProduceRecord(
        partition = Some(0),
        value = Some(StringValue("http-produced-for-ws"))
      )),
      acks = "all")
    assertEquals(200, produceResp.status, s"HTTP produce should succeed. Body: ${produceResp.body}")

    val deliver = ws.waitForDeliver()
    assertNotNull(deliver, "WS subscriber should receive the HTTP-produced message")
    assertEquals("deliver", deliver.get("type").asText())
    assertEquals("sub-1", deliver.get("subscriptionId").asText())
  }

  @Test
  @Timeout(30)
  @Disabled("Data-plane Kafka wiring pending: WS publish/subscribe/deliver require real Kafka produce + fetch integration (post-T3 control plane works).")
  def testKafkaProducer_wsDeliver(): Unit = {
    ws.connect()
    ws.declareQueue(queueName)
    ws.subscribe(queueName, "sub-1", credits = 10, startOffset = "earliest")

    val producer = createProducer(new StringSerializer, new StringSerializer)
    try {
      val record = new ProducerRecord[String, String](backingTopic, 0, "k", "kafka-binary-for-ws")
      producer.send(record).get(10, TimeUnit.SECONDS)
      producer.flush()
    } finally {
      producer.close(JDuration.ofSeconds(5))
    }

    val deliver = ws.waitForDeliver()
    assertNotNull(deliver, "WS subscriber should receive the Kafka-binary-produced message")
    assertEquals("deliver", deliver.get("type").asText())
    assertEquals("sub-1", deliver.get("subscriptionId").asText())
  }

  @Test
  @Timeout(30)
  @Disabled("Data-plane Kafka wiring pending: WS publish/subscribe/deliver require real Kafka produce + fetch integration (post-T3 control plane works).")
  def testWsPublish_kafkaConsumer(): Unit = {
    ws.connect()
    ws.declareExchange("events", "direct")
    ws.declareQueue(queueName)
    ws.bind(queueName, "events", "order.created")

    ws.publish("events", "order.created",
      Map("orderId" -> "789").asInstanceOf[Any],
      headers = Map("trace-id" -> "abc"))

    val consumer = newBinaryConsumer()
    try {
      consumer.subscribe(java.util.List.of(backingTopic))
      val polled = consumer.poll(JDuration.ofSeconds(5))
      val it = polled.records(backingTopic).iterator()
      assertTrue(it.hasNext, s"Kafka binary consumer should read the WS-published record from $backingTopic")
      val record = it.next()
      // Routing metadata should appear as Kafka record headers (_ws_*) once WS publish wires up.
      val headerKeys = scala.collection.mutable.Set.empty[String]
      record.headers().forEach(h => headerKeys.add(h.key()))
      assertTrue(headerKeys.exists(_.startsWith("_ws_")),
        s"Expected at least one _ws_* header on WS-published record, got: $headerKeys")
    } finally {
      consumer.close(JDuration.ofSeconds(5))
    }
  }

  @Test
  @Timeout(30)
  @Disabled("Data-plane Kafka wiring pending: REST publish path wired but sinks are stubs; real produce/fetch deferred.")
  def testRestPublish_wsDeliver(): Unit = {
    ws.connect()
    ws.declareExchange("events", "direct")
    ws.declareQueue(queueName)
    ws.bind(queueName, "events", "order.created")
    ws.subscribe(queueName, "sub-1", credits = 10, startOffset = "earliest")

    val restBody =
      """{"routingKey":"order.created","message":{"body":{"orderId":"42"},"headers":{}}}"""
    val resp = http.rawPost(s"$httpBaseUrl/v1/exchanges/events/publish", restBody)
    assertEquals(200, resp.getStatus,
      s"REST publish should succeed when wired. Body: ${resp.getContentAsString}")

    val deliver = ws.waitForDeliver()
    assertNotNull(deliver, "WS subscriber should receive the REST-published message")
    assertEquals("deliver", deliver.get("type").asText())
    assertEquals("events", deliver.get("exchange").asText())
    assertEquals("order.created", deliver.get("routingKey").asText())
  }

  // ====================================================================
  // Helpers
  // ====================================================================

  /**
   * Build a Kafka binary consumer directly (bypassing
   * [[kafka.api.IntegrationTestHarness.createConsumer]] which requires
   * `group.protocol` to be set on the harness-level config). We construct
   * the consumer manually so the test class doesn't have to override the
   * harness-wide consumer config and risk affecting other tests.
   */
  private def newBinaryConsumer(): KafkaConsumer[String, String] = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers())
    props.put(ConsumerConfig.GROUP_ID_CONFIG, s"ws-cross-test-${System.nanoTime()}")
    props.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, "classic")
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    new KafkaConsumer[String, String](props, new StringDeserializer, new StringDeserializer)
  }
}
