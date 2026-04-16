# TASK-C.03: Phase 1 Integration Tests

## Prerequisites

| Task | What it delivers | Why this task needs it |
|------|------------------|-----------------------|
| TASK-C.01 | `handleHttpProduceRequest()` in KafkaApis (local leader path) | Cannot test produce without the handler |
| TASK-C.02 | `handleHttpConsumeRequest()` in KafkaApis (local leader path) | Cannot test consume without the handler |
| TASK-B.03 | `HttpAcceptor` (Netty-based HTTP listener) | Cannot send HTTP requests without the listener |
| TASK-B.01 | `HttpRequestTranslator` (JSON -> ProduceRequest/FetchRequest) | Requests must be translated |
| TASK-B.02 | `HttpResponseSerializer` (Kafka response -> JSON) | Responses must be serialized to JSON |
| TASK-B.04 | `HttpProcessor` (response bridge between RequestChannel and Netty) | Responses must flow back to Netty |
| TASK-A.01 | `SecurityProtocol.HTTP` enum value with `isHttp()` helper | Listener must use HTTP security protocol |
| TASK-A.02 | `http-server` Gradle module | Test classes live in this module |

## Context

This task creates the integration test infrastructure and first round of end-to-end tests for HTTP produce and consume on the **local leader path** (Phase 1). The tests verify that an HTTP client can produce records to and consume records from partitions whose leader is the broker under test.

### Testing philosophy

Integration tests start a real Kafka cluster (via `KafkaServerTestHarness`), send actual HTTP requests, and verify actual HTTP responses. This is distinct from the unit tests in TASK-C.01 and TASK-C.02 which use mocks.

### Existing test infrastructure

- **`IntegrationTestHarness`**: at `core/src/test/scala/integration/kafka/api/IntegrationTestHarness.scala` (line 48). Abstract class extending `KafkaServerTestHarness` that provides `brokerCount`, `generateConfigs`, `configureListeners`, and creates `KafkaProducer`/`KafkaConsumer` instances.
- **`KafkaServerTestHarness`**: at `core/src/test/scala/integration/kafka/integration/KafkaServerTestHarness.scala`. Manages broker lifecycle.
- **`TestUtils.createBrokerConfigs`**: generates broker configuration with random ports.
- **Jetty HttpClient**: already a dependency in Kafka Connect (version defined in `gradle/dependencies.gradle`). Used for sending HTTP requests in tests.

### Test scope

These tests use a **single-broker cluster** to test only the local leader path. Multi-broker forwarding tests are covered in Phase D tasks.

## Specification

### Files to create

1. `HttpIntegrationTestHarness.scala` -- base class for all HTTP integration tests
2. `HttpTestClient.scala` -- wrapper around Jetty `HttpClient` for test convenience
3. `HttpProduceIntegrationTest.scala` -- produce path integration tests
4. `HttpConsumeIntegrationTest.scala` -- consume path integration tests
5. `HttpHealthCheckIntegrationTest.scala` -- health check endpoint test

All files go in `http-server/src/test/scala/integration/kafka/http/`.

### HttpIntegrationTestHarness design

Extends `IntegrationTestHarness` and:
- Overrides `configureListeners` to add an HTTP listener alongside the binary listener.
- Provides `httpUrl(brokerId: Int): String` to get the HTTP base URL for a specific broker.
- Starts a single broker by default (`brokerCount = 1`).
- Creates test topics in `@BeforeEach`.

### HttpTestClient design

Thin wrapper around Jetty `HttpClient` providing:
- `produce(baseUrl, topic, records, acks, timeoutMs): HttpProduceResponse`
- `consume(baseUrl, topic, partitions, maxWaitMs, ...): HttpConsumeResponse`
- `health(baseUrl): HttpHealthResponse`
- JSON serialization/deserialization of request/response bodies.
- Automatic `Content-Type: application/json` header.

### Test scenarios

**HttpProduceIntegrationTest:**
1. Single-partition produce with STRING value
2. Single-partition produce with BINARY value (base64)
3. Single-partition produce with JSON value
4. Single-partition produce with NULL value
5. Multi-partition produce (all local)
6. Produce with acks="all" (default)
7. Produce with acks="leader"
8. Produce with acks="none"
9. Produce to non-existing topic returns 404
10. Produce with key (STRING) -- verify key round-trips

**HttpConsumeIntegrationTest:**
1. Fetch with explicit offset returns records
2. Fetch from empty partition returns empty records (not error)
3. maxWaitMs is capped (verify X-Kafka-MaxWait-Applied header)
4. Fetch multiple partitions (all local)
5. Produce then consume round-trip (verify data integrity)
6. Fetch with READ_COMMITTED isolation
7. Fetch from non-existing topic returns error

**HttpHealthCheckIntegrationTest:**
1. Health check returns 200 with status=RUNNING
2. Verify response contains brokerId and clusterId

## Implementation Details

### build.gradle test dependencies

The `http-server/build.gradle` must include test dependencies:

```groovy
testImplementation project(':core').sourceSets.test.output
testImplementation project(':core').sourceSets.integration.output  // if separate
testImplementation libs.junitJupiter
testImplementation libs.mockitoCore
testImplementation "org.eclipse.jetty:jetty-client:${versions.jetty}"
testImplementation "com.fasterxml.jackson.core:jackson-databind:${versions.jackson}"
```

### HTTP listener configuration

The HTTP listener runs on a random port (TestUtils.RandomPort). Configuration in `configureListeners`:

```scala
config.setProperty("listeners",
  s"PLAINTEXT://localhost:${TestUtils.RandomPort},HTTP://localhost:${TestUtils.RandomPort}")
config.setProperty("listener.security.protocol.map",
  "PLAINTEXT:PLAINTEXT,HTTP:HTTP")
config.setProperty("advertised.listeners",
  s"PLAINTEXT://localhost:${TestUtils.RandomPort},HTTP://localhost:${TestUtils.RandomPort}")
```

### Discovering the actual HTTP port after startup

After brokers start, the actual HTTP port is available via:

```scala
def httpUrl(brokerId: Int): String = {
  val broker = brokers.find(_.config.brokerId == brokerId).get
  val httpEndpoint = broker.socketServer.boundEndpoint(ListenerName.normalised("HTTP"))
  s"http://${httpEndpoint.host}:${httpEndpoint.port}"
}
```

## Skeleton Code

### HttpIntegrationTestHarness

```scala
// ============================================================================
// File: http-server/src/test/scala/integration/kafka/http/HttpIntegrationTestHarness.scala
// ============================================================================
package kafka.http

import java.util.Properties
import kafka.api.IntegrationTestHarness
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.network.SocketServerConfigs
import org.apache.kafka.server.config.ReplicationConfigs
import org.junit.jupiter.api.{AfterEach, BeforeEach, TestInfo}

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
 *       // ... send HTTP requests ...
 *     }
 *   }
 * }}}
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
   * @param brokerId the broker ID (0-based in tests)
   * @return URL like "http://localhost:54321"
   */
  def httpUrl(brokerId: Int): String = {
    val broker = brokers.find(_.config.brokerId == brokerId)
      .getOrElse(throw new IllegalArgumentException(s"No broker with id $brokerId"))
    val httpEndpoint = broker.socketServer.boundEndpoint(httpListenerName)
    s"http://${httpEndpoint.host}:${httpEndpoint.port}"
  }

  /**
   * Returns the HTTP base URL for broker 0 (convenience for single-broker tests).
   */
  def httpBaseUrl: String = httpUrl(0)
}
```

### HttpTestClient

```scala
// ============================================================================
// File: http-server/src/test/scala/integration/kafka/http/HttpTestClient.scala
// ============================================================================
package kafka.http

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}
import org.eclipse.jetty.client.{ContentResponse, HttpClient, StringRequestContent}
import org.eclipse.jetty.http.HttpMethod

import java.nio.charset.StandardCharsets
import java.util.Base64
import java.util.concurrent.TimeUnit

/**
 * Test utility for sending HTTP requests to a Kafka broker's HTTP listener.
 * Wraps Jetty HttpClient with convenience methods for Kafka HTTP API endpoints.
 *
 * Usage:
 * {{{
 *   val client = new HttpTestClient()
 *   client.start()
 *   try {
 *     val resp = client.produce("http://localhost:9094", "my-topic",
 *       Seq(ProduceRecord(partition = Some(0), value = StringValue("hello"))))
 *     assertEquals(200, resp.status)
 *   } finally {
 *     client.close()
 *   }
 * }}}
 */
class HttpTestClient extends AutoCloseable {

  private val mapper = new ObjectMapper()
  private val httpClient = new HttpClient()

  def start(): Unit = {
    httpClient.setConnectTimeout(5000)
    httpClient.setIdleTimeout(30000)
    httpClient.start()
  }

  override def close(): Unit = {
    httpClient.stop()
  }

  // ------------------------------------------------------------------
  // Data types for request/response
  // ------------------------------------------------------------------

  sealed trait DataValue
  case class StringValue(data: String) extends DataValue
  case class BinaryValue(data: Array[Byte]) extends DataValue
  case class JsonValue(data: JsonNode) extends DataValue
  case object NullValue extends DataValue

  case class ProduceRecord(
    partition: Option[Int] = None,
    key: Option[DataValue] = None,
    value: Option[DataValue] = None,
    headers: Seq[(String, String)] = Seq.empty
  )

  case class ProduceResponse(
    status: Int,
    headers: Map[String, String],
    body: JsonNode
  )

  case class FetchPartitionSpec(partition: Int, offset: Long)

  case class ConsumeResponse(
    status: Int,
    headers: Map[String, String],
    body: JsonNode
  )

  case class HealthResponse(
    status: Int,
    body: JsonNode
  )

  // ------------------------------------------------------------------
  // Produce
  // ------------------------------------------------------------------

  /**
   * POST /v1/topics/{topic}/records
   */
  def produce(
    baseUrl: String,
    topic: String,
    records: Seq[ProduceRecord],
    acks: String = "all",
    timeoutMs: Int = 5000
  ): ProduceResponse = {
    val url = s"$baseUrl/v1/topics/$topic/records"
    val bodyNode = mapper.createObjectNode()

    val recordsArray = bodyNode.putArray("records")
    records.foreach { record =>
      val recordNode = recordsArray.addObject()
      record.partition.foreach(p => recordNode.put("partition", p))
      record.key.foreach(k => recordNode.set[ObjectNode]("key", serializeDataValue(k)))
      record.value.foreach(v => recordNode.set[ObjectNode]("value", serializeDataValue(v)))
      if (record.headers.nonEmpty) {
        val headersArray = recordNode.putArray("headers")
        record.headers.foreach { case (name, value) =>
          headersArray.addObject().put("name", name).put("value", value)
        }
      }
    }
    bodyNode.put("acks", acks)
    bodyNode.put("timeoutMs", timeoutMs)

    val response = sendPost(url, mapper.writeValueAsString(bodyNode))
    ProduceResponse(
      status = response.getStatus,
      headers = extractHeaders(response),
      body = mapper.readTree(response.getContentAsString)
    )
  }

  // ------------------------------------------------------------------
  // Consume
  // ------------------------------------------------------------------

  /**
   * POST /v1/topics/{topic}/records:fetch
   */
  def consume(
    baseUrl: String,
    topic: String,
    partitions: Seq[FetchPartitionSpec],
    maxWaitMs: Int = 500,
    minBytes: Int = 1,
    maxBytes: Int = 10485760,
    maxBytesPerPartition: Int = 1048576,
    isolationLevel: String = "READ_UNCOMMITTED"
  ): ConsumeResponse = {
    val url = s"$baseUrl/v1/topics/$topic/records:fetch"
    val bodyNode = mapper.createObjectNode()

    val partsArray = bodyNode.putArray("partitions")
    partitions.foreach { spec =>
      partsArray.addObject()
        .put("partition", spec.partition)
        .put("offset", spec.offset)
    }
    bodyNode.put("maxWaitMs", maxWaitMs)
    bodyNode.put("minBytes", minBytes)
    bodyNode.put("maxBytes", maxBytes)
    bodyNode.put("maxBytesPerPartition", maxBytesPerPartition)
    bodyNode.put("isolationLevel", isolationLevel)

    val response = sendPost(url, mapper.writeValueAsString(bodyNode))
    ConsumeResponse(
      status = response.getStatus,
      headers = extractHeaders(response),
      body = mapper.readTree(response.getContentAsString)
    )
  }

  // ------------------------------------------------------------------
  // Health
  // ------------------------------------------------------------------

  /**
   * GET /v1/health
   */
  def health(baseUrl: String): HealthResponse = {
    val url = s"$baseUrl/v1/health"
    val response = httpClient.newRequest(url)
      .method(HttpMethod.GET)
      .timeout(5, TimeUnit.SECONDS)
      .send()
    HealthResponse(
      status = response.getStatus,
      body = mapper.readTree(response.getContentAsString)
    )
  }

  // ------------------------------------------------------------------
  // Helpers
  // ------------------------------------------------------------------

  private def sendPost(url: String, jsonBody: String): ContentResponse = {
    httpClient.newRequest(url)
      .method(HttpMethod.POST)
      .headers(h => h.put("Content-Type", "application/json"))
      .body(new StringRequestContent("application/json", jsonBody, StandardCharsets.UTF_8))
      .timeout(30, TimeUnit.SECONDS)
      .send()
  }

  private def serializeDataValue(value: DataValue): ObjectNode = {
    val node = mapper.createObjectNode()
    value match {
      case StringValue(data) =>
        node.put("type", "STRING")
        node.put("data", data)
      case BinaryValue(data) =>
        node.put("type", "BINARY")
        node.put("data", Base64.getEncoder.encodeToString(data))
      case JsonValue(data) =>
        node.put("type", "JSON")
        node.set[ObjectNode]("data", data)
      case NullValue =>
        node.put("type", "NULL")
    }
    node
  }

  private def extractHeaders(response: ContentResponse): Map[String, String] = {
    import scala.jdk.CollectionConverters._
    response.getHeaders.asScala
      .map(field => field.getName -> field.getValue)
      .toMap
  }
}
```

### HttpProduceIntegrationTest

```scala
// ============================================================================
// File: http-server/src/test/scala/integration/kafka/http/HttpProduceIntegrationTest.scala
// ============================================================================
package kafka.http

import com.fasterxml.jackson.databind.ObjectMapper
import kafka.utils.TestUtils
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test, TestInfo}
import org.junit.jupiter.api.Assertions._

class HttpProduceIntegrationTest extends HttpIntegrationTestHarness {

  private val testTopic = "http-produce-test"
  private val numPartitions = 3
  private var client: HttpTestClient = _
  private val mapper = new ObjectMapper()

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    super.setUp(testInfo)
    TestUtils.createTopic(zkClient = null, testTopic, numPartitions,
      replicationFactor = 1, servers = brokers)
    client = new HttpTestClient()
    client.start()
  }

  @AfterEach
  override def tearDown(): Unit = {
    client.close()
    super.tearDown()
  }

  @Test
  def testProduceSinglePartitionStringValue(): Unit = {
    val response = client.produce(httpBaseUrl, testTopic,
      Seq(client.ProduceRecord(
        partition = Some(0),
        value = Some(client.StringValue("hello world"))
      )))

    assertEquals(200, response.status)
    val offsets = response.body.get("offsets")
    assertNotNull(offsets)
    assertEquals(1, offsets.size())
    assertEquals(0, offsets.get(0).get("partition").asInt())
    assertEquals(0, offsets.get(0).get("errorCode").asInt())
    assertTrue(offsets.get(0).get("offset").asLong() >= 0)
  }

  @Test
  def testProduceSinglePartitionBinaryValue(): Unit = {
    val binaryData = "binary content".getBytes("UTF-8")
    val response = client.produce(httpBaseUrl, testTopic,
      Seq(client.ProduceRecord(
        partition = Some(0),
        value = Some(client.BinaryValue(binaryData))
      )))

    assertEquals(200, response.status)
    assertEquals(0, response.body.get("offsets").get(0).get("errorCode").asInt())
  }

  @Test
  def testProduceSinglePartitionJsonValue(): Unit = {
    val jsonData = mapper.createObjectNode().put("amount", 42.0)
    val response = client.produce(httpBaseUrl, testTopic,
      Seq(client.ProduceRecord(
        partition = Some(0),
        value = Some(client.JsonValue(jsonData))
      )))

    assertEquals(200, response.status)
    assertEquals(0, response.body.get("offsets").get(0).get("errorCode").asInt())
  }

  @Test
  def testProduceSinglePartitionNullValue(): Unit = {
    val response = client.produce(httpBaseUrl, testTopic,
      Seq(client.ProduceRecord(
        partition = Some(0),
        value = Some(client.NullValue)
      )))

    assertEquals(200, response.status)
    assertEquals(0, response.body.get("offsets").get(0).get("errorCode").asInt())
  }

  @Test
  def testProduceMultiPartition(): Unit = {
    val response = client.produce(httpBaseUrl, testTopic,
      Seq(
        client.ProduceRecord(partition = Some(0), value = Some(client.StringValue("p0"))),
        client.ProduceRecord(partition = Some(1), value = Some(client.StringValue("p1"))),
        client.ProduceRecord(partition = Some(2), value = Some(client.StringValue("p2")))
      ))

    assertEquals(200, response.status)
    val offsets = response.body.get("offsets")
    assertEquals(3, offsets.size())
  }

  @Test
  def testProduceWithAcksAll(): Unit = {
    val response = client.produce(httpBaseUrl, testTopic,
      Seq(client.ProduceRecord(partition = Some(0), value = Some(client.StringValue("acks-all")))),
      acks = "all")
    assertEquals(200, response.status)
  }

  @Test
  def testProduceWithAcksLeader(): Unit = {
    val response = client.produce(httpBaseUrl, testTopic,
      Seq(client.ProduceRecord(partition = Some(0), value = Some(client.StringValue("acks-leader")))),
      acks = "leader")
    assertEquals(200, response.status)
  }

  @Test
  def testProduceWithAcksNone(): Unit = {
    val response = client.produce(httpBaseUrl, testTopic,
      Seq(client.ProduceRecord(partition = Some(0), value = Some(client.StringValue("acks-none")))),
      acks = "none")
    // acks=0 may return 200 or 204 depending on implementation
    assertTrue(response.status >= 200 && response.status < 300)
  }

  @Test
  def testProduceToNonExistingTopic(): Unit = {
    val response = client.produce(httpBaseUrl, "does-not-exist",
      Seq(client.ProduceRecord(partition = Some(0), value = Some(client.StringValue("nope")))))
    // Should be 404 or error response
    assertTrue(response.status >= 400)
  }

  @Test
  def testProduceWithKey(): Unit = {
    val response = client.produce(httpBaseUrl, testTopic,
      Seq(client.ProduceRecord(
        partition = Some(0),
        key = Some(client.StringValue("order-123")),
        value = Some(client.StringValue("order data"))
      )))
    assertEquals(200, response.status)
    assertEquals(0, response.body.get("offsets").get(0).get("errorCode").asInt())
  }
}
```

### HttpConsumeIntegrationTest

```scala
// ============================================================================
// File: http-server/src/test/scala/integration/kafka/http/HttpConsumeIntegrationTest.scala
// ============================================================================
package kafka.http

import kafka.utils.TestUtils
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test, TestInfo}
import org.junit.jupiter.api.Assertions._

class HttpConsumeIntegrationTest extends HttpIntegrationTestHarness {

  private val testTopic = "http-consume-test"
  private val numPartitions = 3
  private var client: HttpTestClient = _

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    super.setUp(testInfo)
    TestUtils.createTopic(zkClient = null, testTopic, numPartitions,
      replicationFactor = 1, servers = brokers)
    client = new HttpTestClient()
    client.start()
  }

  @AfterEach
  override def tearDown(): Unit = {
    client.close()
    super.tearDown()
  }

  @Test
  def testFetchWithExplicitOffset(): Unit = {
    // Produce a record first
    client.produce(httpBaseUrl, testTopic,
      Seq(client.ProduceRecord(partition = Some(0), value = Some(client.StringValue("msg-0")))))

    // Consume from offset 0
    val response = client.consume(httpBaseUrl, testTopic,
      Seq(client.FetchPartitionSpec(partition = 0, offset = 0)),
      maxWaitMs = 5000)

    assertEquals(200, response.status)
    val partitions = response.body.get("partitions")
    assertNotNull(partitions)
    assertTrue(partitions.size() > 0)
    val p0 = partitions.get(0)
    assertEquals(0, p0.get("partition").asInt())
    val records = p0.get("records")
    assertTrue(records.size() > 0)
  }

  @Test
  def testFetchEmptyPartitionReturnsEmptyRecords(): Unit = {
    // Fetch from an empty partition with short wait
    val response = client.consume(httpBaseUrl, testTopic,
      Seq(client.FetchPartitionSpec(partition = 0, offset = 0)),
      maxWaitMs = 100)

    assertEquals(200, response.status)
    val partitions = response.body.get("partitions")
    assertNotNull(partitions)
    val p0 = partitions.get(0)
    assertEquals(0, p0.get("errorCode").asInt())  // empty is NOT an error
    val records = p0.get("records")
    assertEquals(0, records.size())
  }

  @Test
  def testMaxWaitMsCapped(): Unit = {
    // Request maxWaitMs=30000 but config caps at 5000
    val response = client.consume(httpBaseUrl, testTopic,
      Seq(client.FetchPartitionSpec(partition = 0, offset = 0)),
      maxWaitMs = 30000)

    assertEquals(200, response.status)
    // Verify X-Kafka-MaxWait-Applied header
    val appliedWait = response.headers.get("X-Kafka-MaxWait-Applied")
    assertNotNull(appliedWait, "X-Kafka-MaxWait-Applied header must be present")
    assertEquals("5000", appliedWait)
  }

  @Test
  def testFetchMultiplePartitions(): Unit = {
    // Produce to multiple partitions
    client.produce(httpBaseUrl, testTopic,
      Seq(
        client.ProduceRecord(partition = Some(0), value = Some(client.StringValue("p0-msg"))),
        client.ProduceRecord(partition = Some(1), value = Some(client.StringValue("p1-msg")))
      ))

    // Fetch from both partitions
    val response = client.consume(httpBaseUrl, testTopic,
      Seq(
        client.FetchPartitionSpec(partition = 0, offset = 0),
        client.FetchPartitionSpec(partition = 1, offset = 0)
      ),
      maxWaitMs = 5000)

    assertEquals(200, response.status)
    val partitions = response.body.get("partitions")
    assertEquals(2, partitions.size())
  }

  @Test
  def testProduceThenConsumeRoundTrip(): Unit = {
    val testValue = "round-trip-value-12345"

    // Produce
    val produceResp = client.produce(httpBaseUrl, testTopic,
      Seq(client.ProduceRecord(
        partition = Some(0),
        key = Some(client.StringValue("my-key")),
        value = Some(client.StringValue(testValue))
      )))
    assertEquals(200, produceResp.status)
    val producedOffset = produceResp.body.get("offsets").get(0).get("offset").asLong()

    // Consume from the produced offset
    val consumeResp = client.consume(httpBaseUrl, testTopic,
      Seq(client.FetchPartitionSpec(partition = 0, offset = producedOffset)),
      maxWaitMs = 5000)

    assertEquals(200, consumeResp.status)
    val records = consumeResp.body.get("partitions").get(0).get("records")
    assertTrue(records.size() > 0)

    val firstRecord = records.get(0)
    assertEquals(producedOffset, firstRecord.get("offset").asLong())
    // Verify value round-tripped correctly
    assertEquals("STRING", firstRecord.get("value").get("type").asText())
    assertEquals(testValue, firstRecord.get("value").get("data").asText())
    // Verify key round-tripped correctly
    assertEquals("STRING", firstRecord.get("key").get("type").asText())
    assertEquals("my-key", firstRecord.get("key").get("data").asText())
  }

  @Test
  def testFetchWithReadCommittedIsolation(): Unit = {
    client.produce(httpBaseUrl, testTopic,
      Seq(client.ProduceRecord(partition = Some(0), value = Some(client.StringValue("committed")))))

    val response = client.consume(httpBaseUrl, testTopic,
      Seq(client.FetchPartitionSpec(partition = 0, offset = 0)),
      maxWaitMs = 5000,
      isolationLevel = "READ_COMMITTED")

    assertEquals(200, response.status)
  }

  @Test
  def testFetchNonExistingTopic(): Unit = {
    val response = client.consume(httpBaseUrl, "does-not-exist",
      Seq(client.FetchPartitionSpec(partition = 0, offset = 0)),
      maxWaitMs = 100)

    assertTrue(response.status >= 400)
  }
}
```

### HttpHealthCheckIntegrationTest

```scala
// ============================================================================
// File: http-server/src/test/scala/integration/kafka/http/HttpHealthCheckIntegrationTest.scala
// ============================================================================
package kafka.http

import org.junit.jupiter.api.{AfterEach, BeforeEach, Test, TestInfo}
import org.junit.jupiter.api.Assertions._

class HttpHealthCheckIntegrationTest extends HttpIntegrationTestHarness {

  private var client: HttpTestClient = _

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    super.setUp(testInfo)
    client = new HttpTestClient()
    client.start()
  }

  @AfterEach
  override def tearDown(): Unit = {
    client.close()
    super.tearDown()
  }

  @Test
  def testHealthCheckReturns200WhenRunning(): Unit = {
    val response = client.health(httpBaseUrl)
    assertEquals(200, response.status)
    assertEquals("RUNNING", response.body.get("status").asText())
  }

  @Test
  def testHealthCheckContainsBrokerId(): Unit = {
    val response = client.health(httpBaseUrl)
    assertTrue(response.body.has("brokerId"))
    assertEquals(0, response.body.get("brokerId").asInt())
  }

  @Test
  def testHealthCheckContainsClusterId(): Unit = {
    val response = client.health(httpBaseUrl)
    assertTrue(response.body.has("clusterId"))
    assertFalse(response.body.get("clusterId").asText().isEmpty)
  }
}
```

## Tests

The skeleton code sections above (HttpProduceIntegrationTest, HttpConsumeIntegrationTest, HttpHealthCheckIntegrationTest) ARE the test deliverables for this task. Each test class is fully specified with method-level test cases covering the scenarios listed in the Specification section. The `HttpIntegrationTestHarness` and `HttpTestClient` are test infrastructure, not test cases themselves.

### Test summary

| Test class | Tests | What it validates |
|---|---|---|
| `HttpProduceIntegrationTest` | 10 tests | Produce: STRING/BINARY/JSON/NULL values, multi-partition, acks modes, error cases, key round-trip |
| `HttpConsumeIntegrationTest` | 7 tests | Consume: fetch with offset, empty poll, maxWaitMs cap, multi-partition, round-trip, READ_COMMITTED, error cases |
| `HttpHealthCheckIntegrationTest` | 3 tests | Health: 200 status, brokerId, clusterId |

## Rules

1. **Test files live in `http-server/src/test/scala/integration/kafka/http/`.** Not in `core`.
2. **Single-broker cluster for Phase 1.** Multi-broker forwarding tests belong to Phase D.
3. **Use Jetty HttpClient, not Apache HttpClient.** Jetty is already a Kafka dependency (via Connect).
4. **Always create test topics explicitly.** Do not rely on auto-create.
5. **Tests must be independent.** Each test creates its own state. No ordering dependencies between tests.
6. **Short timeouts in tests.** Use `maxWaitMs=100` for "empty poll" tests to avoid slow test suites.
7. **Verify HTTP headers, not just body.** Especially `X-Kafka-MaxWait-Applied` and `Content-Type`.
8. **Do NOT test forwarding.** All partitions are local in Phase 1.

## Learning

_To be filled by the executing agent._

## Limitations

_To be filled by the executing agent._

## Field Notes

_To be filled by the executing agent._

## Acceptance Criteria

- [ ] `HttpIntegrationTestHarness` compiles and starts a broker with HTTP listener.
- [ ] `HttpTestClient` can send produce and consume requests over HTTP.
- [ ] `HttpProduceIntegrationTest`: all 10 tests pass.
- [ ] `HttpConsumeIntegrationTest`: all 7 tests pass.
- [ ] `HttpHealthCheckIntegrationTest`: all 3 tests pass.
- [ ] Produce-then-consume round-trip verifies data integrity (key, value, offset).
- [ ] `X-Kafka-MaxWait-Applied` header is present and correct on consume responses.
- [ ] Empty partition fetch returns empty records with `errorCode: 0`.
- [ ] Non-existing topic returns error status code (404 or 4xx).
- [ ] Tests run in under 60 seconds total.
- [ ] No test depends on another test's state.

## File Manifest

| File | Action | Description |
|------|--------|-------------|
| | | |
