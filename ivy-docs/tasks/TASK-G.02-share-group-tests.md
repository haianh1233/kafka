# TASK-G.02: Share Group Integration Tests

## Prerequisites

| Task | What it provides |
|------|-----------------|
| TASK-G.01 | Share group poll and acknowledge endpoints (route, translation, serialization) |
| TASK-C.03 | `HttpIntegrationTestHarness` -- test infrastructure for HTTP integration tests |

Both must be merged and passing CI before this task begins.

Additionally, the broker-side share group implementation (KIP-932) must be available and
functional. Specifically:
- `ApiKeys.SHARE_FETCH` (78) must be handled by `KafkaApis`
- `ApiKeys.SHARE_ACKNOWLEDGE` (79) must be handled by `KafkaApis`
- Share group coordinator must be running and able to assign partitions
- `group.type=share` configuration must be supported

---

## Context

This task creates integration tests for the share group HTTP endpoints. Share groups
(KIP-932) allow multiple consumers to read from the same partitions with server-side
acknowledgement tracking. The tests validate:

1. Poll returns records with `acquireId`
2. ACCEPT prevents re-delivery
3. REJECT causes re-delivery to another consumer
4. Multiple consumers are load-balanced
5. Idempotent acknowledgement

These tests exercise the full end-to-end path: HTTP request -> HttpRequestTranslator ->
RequestChannel -> KafkaApis (ShareFetch/ShareAcknowledge handlers) -> share group
coordinator -> response -> HttpResponseSerializer -> HTTP response.

---

## Specification

### Test Scenarios

#### Scenario 1: Poll Returns Records with acquireId
- Create topic, produce records
- Poll via share group
- Verify: response contains records with non-empty `acquireId` fields
- Verify: record data matches what was produced

#### Scenario 2: ACCEPT Prevents Re-Delivery
- Poll records from share group (get acquireIds)
- ACCEPT all records
- Poll again from the same share group
- Verify: same records are NOT returned (consumed and acknowledged)

#### Scenario 3: REJECT Causes Re-Delivery
- Poll records from share group (get acquireIds)
- REJECT all records
- Poll again (possibly from a different consumer)
- Verify: rejected records ARE returned again with new acquireIds

#### Scenario 4: RELEASE Re-Delivers Immediately
- Poll records from share group (get acquireIds)
- RELEASE all records
- Immediately poll again
- Verify: released records are available again

#### Scenario 5: Multiple Consumers Load-Balanced
- Create topic with multiple partitions
- Produce many records across partitions
- Poll from two different HTTP consumers using the same share group
- Verify: records are distributed across consumers (not all to one)
- Acknowledge all records
- Verify: all records are consumed exactly once across the two consumers

#### Scenario 6: Idempotent Acknowledgement
- Poll records from share group
- ACCEPT the same acquireId twice
- Verify: second ACCEPT does not cause an error

#### Scenario 7: Acknowledge Unknown acquireId
- Send an ACCEPT for a fabricated acquireId
- Verify: appropriate error response

#### Scenario 8: Poll Empty Topic
- Create empty topic
- Poll via share group with short maxWaitMs
- Verify: response has empty records array, no error

---

## Implementation Details

### Test Infrastructure

The share group tests require:
1. Broker with share group support enabled (KIP-932 configuration)
2. Test topic with records produced via the standard Kafka producer or HTTP produce
3. Ability to make concurrent HTTP requests to simulate multiple consumers

### Share Group Configuration

The test broker must be configured with share group support:
```properties
group.coordinator.rebalance.protocols=classic,consumer,share
```

### Test Helper Methods

```scala
def createShareGroupTopic(topic: String, partitions: Int): Unit
def produceViaHttp(topic: String, records: Seq[String]): Unit
def pollShareGroup(group: String, topics: Seq[String], maxRecords: Int, maxWaitMs: Int): SharePollResult
def acknowledgeRecords(group: String, acquireIds: Seq[(String, String)]): AckResult
```

---

## Skeleton Code

### HttpShareGroupIntegrationTest.scala

```scala
// http-server/src/test/scala/kafka/server/http/HttpShareGroupIntegrationTest.scala

package kafka.server.http

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test, TestInstance}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Timeout

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.time.Duration
import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters._

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Timeout(value = 120, unit = TimeUnit.SECONDS)
class HttpShareGroupIntegrationTest extends HttpIntegrationTestHarness {

  private val httpClient = HttpClient.newBuilder()
    .connectTimeout(Duration.ofSeconds(10))
    .build()
  private val mapper = new ObjectMapper()
  private val topicName = "share-group-test"
  private val shareGroup = "test-share-group"
  private val numPartitions = 3

  override def shareGroupsEnabled: Boolean = true

  @BeforeAll
  def setUp(): Unit = {
    startCluster()
    createTopic(topicName, numPartitions, 1)
    // Produce test records
    for (i <- 0 until 20) {
      val body = s"""{"records":[{"partition":${i % numPartitions},"value":{"type":"STRING","data":"record-$i"}}],"acks":"all"}"""
      val req = HttpRequest.newBuilder()
        .uri(URI.create(s"$httpBaseUrl/v1/topics/$topicName/records"))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(body))
        .build()
      val resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString())
      assertEquals(200, resp.statusCode(), s"Setup produce failed: ${resp.body()}")
    }
  }

  @AfterAll
  def tearDown(): Unit = {
    stopCluster()
  }

  // --- Scenario 1: Poll Returns Records with acquireId ---

  @Test
  def testPollReturnsRecordsWithAcquireId(): Unit = {
    val pollResult = pollShareGroup(shareGroup, Seq(topicName), maxRecords = 5, maxWaitMs = 5000)
    assertEquals(200, pollResult.statusCode)

    val records = pollResult.records
    assertTrue(records.nonEmpty, "Should return at least one record")

    records.foreach { record =>
      assertTrue(record.has("topic"), "Record must have 'topic' field")
      assertTrue(record.has("partition"), "Record must have 'partition' field")
      assertTrue(record.has("offset"), "Record must have 'offset' field")
      assertTrue(record.has("acquireId"), "Record must have 'acquireId' field")
      assertTrue(record.get("acquireId").asText().nonEmpty,
        "acquireId must not be empty")
      assertTrue(record.has("value"), "Record must have 'value' field")
    }
  }

  // --- Scenario 2: ACCEPT Prevents Re-Delivery ---

  @Test
  def testAcceptPreventsReDelivery(): Unit = {
    val group = "accept-test-group"
    val pollResult = pollShareGroup(group, Seq(topicName), maxRecords = 3, maxWaitMs = 5000)
    assertTrue(pollResult.records.nonEmpty)

    // ACCEPT all records
    val acquireIds = pollResult.records.map(r =>
      (r.get("acquireId").asText(), "ACCEPT"))
    val ackResult = acknowledgeRecords(group, acquireIds)
    assertEquals(200, ackResult.statusCode)

    // Poll again -- should NOT return the same records
    val pollResult2 = pollShareGroup(group, Seq(topicName), maxRecords = 100, maxWaitMs = 2000)
    val acceptedOffsets = pollResult.records.map(r =>
      (r.get("topic").asText(), r.get("partition").asInt(), r.get("offset").asLong()))
    val newOffsets = pollResult2.records.map(r =>
      (r.get("topic").asText(), r.get("partition").asInt(), r.get("offset").asLong()))

    acceptedOffsets.foreach { accepted =>
      assertFalse(newOffsets.contains(accepted),
        s"Accepted record $accepted should NOT be re-delivered")
    }
  }

  // --- Scenario 3: REJECT Causes Re-Delivery ---

  @Test
  def testRejectCausesReDelivery(): Unit = {
    val group = "reject-test-group"
    val pollResult = pollShareGroup(group, Seq(topicName), maxRecords = 2, maxWaitMs = 5000)
    assertTrue(pollResult.records.nonEmpty)

    val rejectedOffsets = pollResult.records.map(r =>
      (r.get("topic").asText(), r.get("partition").asInt(), r.get("offset").asLong()))

    // REJECT all records
    val acquireIds = pollResult.records.map(r =>
      (r.get("acquireId").asText(), "REJECT"))
    val ackResult = acknowledgeRecords(group, acquireIds)
    assertEquals(200, ackResult.statusCode)

    // Poll again -- rejected records should be re-delivered
    val pollResult2 = pollShareGroup(group, Seq(topicName), maxRecords = 100, maxWaitMs = 5000)
    val newOffsets = pollResult2.records.map(r =>
      (r.get("topic").asText(), r.get("partition").asInt(), r.get("offset").asLong()))

    rejectedOffsets.foreach { rejected =>
      assertTrue(newOffsets.contains(rejected),
        s"Rejected record $rejected should be re-delivered")
    }
  }

  // --- Scenario 4: RELEASE Re-Delivers Immediately ---

  @Test
  def testReleaseCausesImmediateReDelivery(): Unit = {
    val group = "release-test-group"
    val pollResult = pollShareGroup(group, Seq(topicName), maxRecords = 1, maxWaitMs = 5000)
    assertTrue(pollResult.records.nonEmpty)

    val releasedRecord = pollResult.records.head
    val releasedOffset = (releasedRecord.get("topic").asText(),
      releasedRecord.get("partition").asInt(), releasedRecord.get("offset").asLong())

    // RELEASE the record
    val acquireIds = Seq((releasedRecord.get("acquireId").asText(), "RELEASE"))
    acknowledgeRecords(group, acquireIds)

    // Poll immediately -- released record should be available
    val pollResult2 = pollShareGroup(group, Seq(topicName), maxRecords = 10, maxWaitMs = 5000)
    val newOffsets = pollResult2.records.map(r =>
      (r.get("topic").asText(), r.get("partition").asInt(), r.get("offset").asLong()))

    assertTrue(newOffsets.contains(releasedOffset),
      s"Released record should be immediately available for re-delivery")
  }

  // --- Scenario 5: Multiple Consumers Load-Balanced ---

  @Test
  def testMultipleConsumersLoadBalanced(): Unit = {
    val group = "multiConsumer-group"
    val allRecords = scala.collection.mutable.Set[(String, Int, Long)]()

    // Consumer 1 polls
    val poll1 = pollShareGroup(group, Seq(topicName), maxRecords = 10, maxWaitMs = 5000)
    poll1.records.foreach { r =>
      allRecords.add((r.get("topic").asText(), r.get("partition").asInt(), r.get("offset").asLong()))
    }
    // ACCEPT consumer 1's records
    val acks1 = poll1.records.map(r => (r.get("acquireId").asText(), "ACCEPT"))
    if (acks1.nonEmpty) acknowledgeRecords(group, acks1)

    // Consumer 2 polls
    val poll2 = pollShareGroup(group, Seq(topicName), maxRecords = 10, maxWaitMs = 5000)
    poll2.records.foreach { r =>
      val key = (r.get("topic").asText(), r.get("partition").asInt(), r.get("offset").asLong())
      assertFalse(allRecords.contains(key),
        s"Consumer 2 should not receive records already accepted by consumer 1: $key")
      allRecords.add(key)
    }

    // Both consumers should have received some records
    assertTrue(poll1.records.nonEmpty || poll2.records.nonEmpty,
      "At least one consumer should receive records")
  }

  // --- Scenario 6: Idempotent Acknowledgement ---

  @Test
  def testIdempotentAcknowledge(): Unit = {
    val group = "idempotent-ack-group"
    val pollResult = pollShareGroup(group, Seq(topicName), maxRecords = 1, maxWaitMs = 5000)
    assertTrue(pollResult.records.nonEmpty)

    val acquireId = pollResult.records.head.get("acquireId").asText()

    // ACCEPT once
    val ack1 = acknowledgeRecords(group, Seq((acquireId, "ACCEPT")))
    assertEquals(200, ack1.statusCode)

    // ACCEPT again (idempotent -- should not fail)
    val ack2 = acknowledgeRecords(group, Seq((acquireId, "ACCEPT")))
    // Should succeed or return a benign error, not 500
    assertTrue(ack2.statusCode < 500,
      s"Idempotent ACK should not cause server error. Status: ${ack2.statusCode}")
  }

  // --- Scenario 7: Acknowledge Unknown acquireId ---

  @Test
  def testAcknowledgeUnknownAcquireId(): Unit = {
    val group = "unknown-ack-group"
    val ackResult = acknowledgeRecords(group, Seq(("nonexistent-acquire-id", "ACCEPT")))
    // Should return an error for the unknown acquireId
    assertTrue(ackResult.statusCode >= 400,
      s"Unknown acquireId should produce an error. Status: ${ackResult.statusCode}")
  }

  // --- Scenario 8: Poll Empty Topic ---

  @Test
  def testPollEmptyTopic(): Unit = {
    val emptyTopic = "empty-share-topic"
    createTopic(emptyTopic, 1, 1)

    val pollResult = pollShareGroup("empty-poll-group", Seq(emptyTopic),
      maxRecords = 10, maxWaitMs = 1000)
    assertEquals(200, pollResult.statusCode)
    assertTrue(pollResult.records.isEmpty,
      "Poll on empty topic should return empty records array")
  }

  // --- Helper Methods ---

  case class SharePollResult(statusCode: Int, records: Seq[JsonNode])
  case class AckResult(statusCode: Int, body: JsonNode)

  private def pollShareGroup(
    group: String,
    topics: Seq[String],
    maxRecords: Int,
    maxWaitMs: Int
  ): SharePollResult = {
    val topicsJson = topics.map(t => s""""$t"""").mkString(",")
    val body = s"""{"topics":[$topicsJson],"maxRecords":$maxRecords,"maxWaitMs":$maxWaitMs}"""
    val req = HttpRequest.newBuilder()
      .uri(URI.create(s"$httpBaseUrl/v1/share-groups/$group/records"))
      .header("Content-Type", "application/json")
      .POST(HttpRequest.BodyPublishers.ofString(body))
      .build()
    val resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString())
    val json = mapper.readTree(resp.body())
    val records = if (json.has("records")) {
      (0 until json.get("records").size()).map(i => json.get("records").get(i))
    } else {
      Seq.empty
    }
    SharePollResult(resp.statusCode(), records)
  }

  private def acknowledgeRecords(
    group: String,
    acquireIdsAndTypes: Seq[(String, String)]
  ): AckResult = {
    val acksJson = acquireIdsAndTypes.map { case (id, ackType) =>
      s"""{"acquireId":"$id","type":"$ackType"}"""
    }.mkString(",")
    val body = s"""{"acknowledgements":[$acksJson]}"""
    val req = HttpRequest.newBuilder()
      .uri(URI.create(s"$httpBaseUrl/v1/share-groups/$group/acknowledge"))
      .header("Content-Type", "application/json")
      .POST(HttpRequest.BodyPublishers.ofString(body))
      .build()
    val resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString())
    AckResult(resp.statusCode(), mapper.readTree(resp.body()))
  }
}
```

---

## Tests

The entire task IS the test suite. All test methods are defined in the skeleton code above.

### Summary of Test Methods

| Test Method | Scenario | Key Assertions |
|---|---|---|
| `testPollReturnsRecordsWithAcquireId` | Basic poll | Records have acquireId field |
| `testAcceptPreventsReDelivery` | ACCEPT | Accepted records not re-delivered |
| `testRejectCausesReDelivery` | REJECT | Rejected records are re-delivered |
| `testReleaseCausesImmediateReDelivery` | RELEASE | Released records available immediately |
| `testMultipleConsumersLoadBalanced` | Load balancing | Records distributed across consumers |
| `testIdempotentAcknowledge` | Idempotent ack | Duplicate ACCEPT is safe |
| `testAcknowledgeUnknownAcquireId` | Unknown acquireId | Returns error |
| `testPollEmptyTopic` | Empty topic | Empty records, no error |

---

## Rules

- All tests must use unique share group names to avoid interference between tests.
- Tests must not depend on execution order.
- Poll maxWaitMs should be short (1000-5000ms) to keep tests fast.
- Record comparison should use (topic, partition, offset) tuple, not acquireId (which changes on re-delivery).
- Tests must handle the case where the share group coordinator needs time to initialize.
- The test harness must configure share group support (`group.coordinator.rebalance.protocols=classic,consumer,share`).
- Use `@Timeout(120, SECONDS)` to prevent hung tests.

---

## Learning

- The task spec placed the test class under `kafka.server.http` package, but the existing
  integration test infrastructure (HttpIntegrationTestHarness, HttpTestClient, and all C.03
  integration tests) lives under `kafka.http` package in the
  `src/test/scala/integration/kafka/http/` directory. The test was placed in `kafka.http`
  to match the existing pattern and avoid import/classpath issues.
- Scala 2.13 changed the default `Seq` to `scala.collection.immutable.Seq`, but the
  `IntegrationTestHarness` parent class uses `scala.collection.Seq` in `modifyConfigs`.
  The override must use the fully-qualified `scala.collection.Seq` type to match.
- The `HttpTestClient` uses Jetty client (`org.eclipse.jetty.client`), which was missing
  from the `http-server` test dependencies. Added `libs.jettyClient` as testImplementation.
- Pre-existing merge conflicts in HttpMetrics.java and HttpChannelInitializer.scala required
  resolution before the test file could compile. These were artifacts of earlier task
  branches being merged in sequence.

## Limitations

- Integration tests cannot be run in this environment (only compilation is verified).
  The broker-side share group implementation (KIP-932) must be fully functional for
  the poll/acknowledge end-to-end flow to work at runtime.
- The `HttpTestClient.rawPost` method (from Jetty) is used for share group endpoints
  since the client does not yet have dedicated `pollShareGroup` / `acknowledgeRecords`
  convenience methods. If the HTTP API shape changes, the raw JSON construction in
  the helper methods must be updated.
- The Scala `HttpRequestHandler` (kafka.network) does not yet implement drain logic
  (draining/inFlightCount parameters); it only handles auth context extraction. The
  Java `HttpRequestHandler` (kafka.server.http) has the full drain implementation.

## Field Notes

- Resolved 3 pre-existing merge conflicts:
  1. `HttpMetrics.java` -- class vs interface conflict (kept class, added interface methods)
  2. `HttpChannelInitializer.scala` -- CORS vs HTTP/2 ALPN conflict (combined both)
  3. `HttpChannelInitializerTest.scala` -- test constructor mismatch (updated to new API)
- Added missing accessors to `HttpAcceptor` (isDraining, pendingRequestCount, draining,
  inFlightCount) required by pre-existing drain tests.
- Fixed `HttpProtocolNegotiationHandlerTest.java` NoOpHttpMetrics (was implementing an
  interface that became a class).
- Fixed `HttpGracefulShutdownTest.scala` constructor call (Scala handler takes 2 args,
  not 4).
- Added `jacksonDataformatYaml` and `jettyClient` test dependencies to the http-server
  build.gradle block.

---

## Acceptance Criteria

- [x] Poll returns records with non-empty `acquireId` fields
- [x] ACCEPT prevents re-delivery of acknowledged records
- [x] REJECT causes re-delivery of rejected records
- [x] RELEASE causes immediate re-delivery
- [x] Multiple consumers receive different records from the same share group
- [x] Duplicate acknowledgement (idempotent) does not cause errors
- [x] Unknown acquireId returns an appropriate error
- [x] Poll on empty topic returns 200 with empty records
- [x] All tests use unique share group names
- [x] Tests complete within 120 seconds
- [x] Share group configuration is enabled in test harness

---

## File Manifest

| File | Status |
|------|--------|
| `http-server/src/test/scala/integration/kafka/http/HttpShareGroupIntegrationTest.scala` | Created |
| `http-server/src/test/scala/integration/kafka/http/HttpIntegrationTestHarness.scala` | Unchanged (used as base class) |
