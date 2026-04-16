# TASK-T.08: HTTP Offset Commit Integration Test

## Prerequisites

| Task | What it provides |
|------|-----------------|
| TASK-0.04 | Build verified -- base code compiles and existing tests pass |
| TASK-C.03 | Phase 1 integration test infrastructure -- HttpIntegrationTestHarness, HttpTestClient |

---

## Context

The HTTP protocol design includes consumer group offset management endpoints:
- `POST /v1/consumer-groups/{group}/offsets` -- commit offsets for a consumer group
- `GET /v1/consumer-groups/{group}/offsets` -- fetch committed offsets

These are critical for HTTP-based consumers that need to track their position. The commit/fetch offset round-trip is the foundation for reliable HTTP consumption.

Currently there are **zero integration tests** for HTTP offset commit/fetch. This task adds E2E tests that exercise the offset lifecycle.

### Key Code References

- **Test harness**: `HttpIntegrationTestHarness.scala`
- **Test client**: `HttpTestClient.scala` -- will need `rawPost()` and `rawGet()` for offset endpoints
- **Offset commit handler**: `KafkaApis.scala` -- `handleOffsetCommitRequest`, `handleOffsetFetchRequest`
- **HTTP translator**: `HttpRequestTranslator.java` -- `OffsetCommitTranslationResult`, `OffsetFetchTranslationResult`
- **Design doc**: `ivy-docs/http-protocol-design.md` sections on offset management

---

## Specification

### Test 1: `testCommitOffsetsReturns200`
- Create topic, produce records
- POST to `/v1/consumer-groups/{group}/offsets` with offset data
- Verify 200 response

### Test 2: `testFetchCommittedOffsetsMatchesCommit`
- Create topic, produce records
- Commit offsets for partitions 0 and 1
- GET `/v1/consumer-groups/{group}/offsets?topic={topic}`
- Verify the returned offsets match what was committed

### Test 3: `testOffsetCommitThenConsumeFromCommittedPosition`
- Create topic, produce records (e.g., 5 records to partition 0)
- Consume from offset 0, receive records 0-4
- Commit offset 3 (meaning records 0-2 processed)
- Fetch committed offset -- should be 3
- Consume from committed offset (3) -- should get records 3 and 4

### Test 4: `testCommitOffsetsForNonExistentGroupCreatesGroup`
- Commit offsets for a new consumer group that has not been seen before
- Verify 200 response (group is created implicitly)
- Fetch committed offsets for the new group -- should match

### Test 5: `testFetchOffsetsForNonExistentGroupReturnsEmpty`
- Fetch committed offsets for a group that has never committed
- Verify response indicates no committed offsets (empty result or -1 offset)

---

## Implementation Details

### File: `http-server/src/test/scala/integration/kafka/http/HttpOffsetCommitIntegrationTest.scala`

Extends `HttpIntegrationTestHarness`. Uses `HttpTestClient.rawPost()` for commit operations and `HttpTestClient.rawGet()` for fetch operations.

The request/response bodies follow the HTTP API contract:
- Commit: `POST /v1/consumer-groups/{group}/offsets` with JSON body containing topic, partitions, and offsets
- Fetch: `GET /v1/consumer-groups/{group}/offsets?topic={topic}` returns JSON with committed offsets

---

## Skeleton Code

```scala
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
package kafka.http

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import kafka.http.HttpTestClient._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test, TestInfo}
import org.junit.jupiter.api.Assertions._

import scala.jdk.CollectionConverters._

/**
 * Integration tests for HTTP offset commit and fetch endpoints.
 *
 * Tests:
 * - Commit offsets -> 200
 * - Fetch committed offsets -> matches commit
 * - Commit, fetch, resume consumption pattern
 * - Implicit group creation on first commit
 * - Fetch offsets for non-existent group
 *
 * Note: These tests require the HTTP offset commit/fetch endpoints to be
 * fully implemented. They may not pass until the router/translator/handler
 * chain is complete for offset operations. They should compile and serve
 * as executable specifications.
 */
class HttpOffsetCommitIntegrationTest extends HttpIntegrationTestHarness {

  private val testTopic = "offset-commit-test"
  private val numPartitions = 3
  private val testGroup = "test-consumer-group"
  private var client: HttpTestClient = _
  private val mapper = new ObjectMapper()

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    super.setUp(testInfo)
    createTopic(testTopic, numPartitions, replicationFactor = 1)
    client = new HttpTestClient()
    client.start()
  }

  @AfterEach
  override def tearDown(): Unit = {
    if (client != null) client.close()
    super.tearDown()
  }

  /**
   * Helper to build an offset commit request body.
   */
  private def buildCommitBody(
    topic: String,
    offsets: Map[Int, Long],
    metadata: String = ""
  ): String = {
    val body = mapper.createObjectNode()
    val topicsArray = body.putArray("topics")
    val topicNode = topicsArray.addObject()
    topicNode.put("topic", topic)
    val partitionsArray = topicNode.putArray("partitions")
    offsets.foreach { case (partition, offset) =>
      val partNode = partitionsArray.addObject()
      partNode.put("partition", partition)
      partNode.put("offset", offset)
      if (metadata.nonEmpty) {
        partNode.put("metadata", metadata)
      }
    }
    mapper.writeValueAsString(body)
  }

  /**
   * Helper to produce a batch of records and return the next offset per partition.
   */
  private def produceRecords(topic: String, partition: Int, count: Int): Long = {
    var lastOffset = 0L
    for (i <- 0 until count) {
      val response = client.produce(httpBaseUrl, topic,
        Seq(ProduceRecord(
          partition = Some(partition),
          value = Some(StringValue(s"msg-$i")))))
      if (response.status == 200) {
        lastOffset = response.body.get("offsets").get(0).get("offset").asLong() + 1
      }
    }
    lastOffset // Returns the next offset (last produced + 1)
  }

  // ===================================================================
  // Test 1: Commit offsets returns 200
  // ===================================================================
  @Test
  def testCommitOffsetsReturns200(): Unit = {
    // Produce some records first
    produceRecords(testTopic, 0, 5)

    // Commit offset 3 for partition 0
    val commitBody = buildCommitBody(testTopic, Map(0 -> 3L))
    val response = client.rawPost(
      s"$httpBaseUrl/v1/consumer-groups/$testGroup/offsets",
      commitBody)

    assertEquals(200, response.getStatus,
      s"Offset commit should return 200, got ${response.getStatus}")
  }

  // ===================================================================
  // Test 2: Fetch committed offsets matches what was committed
  // ===================================================================
  @Test
  def testFetchCommittedOffsetsMatchesCommit(): Unit = {
    // Produce records
    produceRecords(testTopic, 0, 5)
    produceRecords(testTopic, 1, 3)

    // Commit offsets: partition 0 -> offset 3, partition 1 -> offset 2
    val commitBody = buildCommitBody(testTopic, Map(0 -> 3L, 1 -> 2L))
    val commitResp = client.rawPost(
      s"$httpBaseUrl/v1/consumer-groups/$testGroup/offsets",
      commitBody)
    assertEquals(200, commitResp.getStatus,
      "Commit should succeed")

    // Fetch committed offsets
    val fetchResp = client.rawGet(
      s"$httpBaseUrl/v1/consumer-groups/$testGroup/offsets?topic=$testTopic")

    assertEquals(200, fetchResp.getStatus,
      s"Fetch committed offsets should return 200, got ${fetchResp.getStatus}")

    val body = mapper.readTree(fetchResp.getContentAsString)

    // Parse the response and verify offsets match
    // Response structure may be: { "topics": [{ "topic": "...", "partitions": [{ "partition": 0, "offset": 3 }, ...] }] }
    val topicsNode = body.get("topics")
    if (topicsNode != null && topicsNode.size() > 0) {
      val topicResult = topicsNode.get(0)
      assertEquals(testTopic, topicResult.get("topic").asText())

      val partitions = topicResult.get("partitions")
      val offsetMap = (0 until partitions.size()).map { i =>
        val p = partitions.get(i)
        p.get("partition").asInt() -> p.get("offset").asLong()
      }.toMap

      assertEquals(3L, offsetMap.getOrElse(0, -1L),
        "Partition 0 committed offset should be 3")
      assertEquals(2L, offsetMap.getOrElse(1, -1L),
        "Partition 1 committed offset should be 2")
    }
  }

  // ===================================================================
  // Test 3: Commit, fetch, resume consumption pattern
  // ===================================================================
  @Test
  def testOffsetCommitThenConsumeFromCommittedPosition(): Unit = {
    // Produce 5 records to partition 0
    produceRecords(testTopic, 0, 5)

    // Consume from offset 0 -- should get records 0-4
    val firstFetch = client.consume(httpBaseUrl, testTopic,
      Seq(FetchPartitionSpec(partition = 0, offset = 0)),
      maxWaitMs = 5000)
    assertEquals(200, firstFetch.status)

    // Commit offset 3 (meaning records 0, 1, 2 are processed)
    val commitBody = buildCommitBody(testTopic, Map(0 -> 3L))
    val commitResp = client.rawPost(
      s"$httpBaseUrl/v1/consumer-groups/$testGroup/offsets",
      commitBody)
    assertEquals(200, commitResp.getStatus, "Commit should succeed")

    // Fetch committed offset
    val fetchOffsetResp = client.rawGet(
      s"$httpBaseUrl/v1/consumer-groups/$testGroup/offsets?topic=$testTopic")
    assertEquals(200, fetchOffsetResp.getStatus)

    val offsetBody = mapper.readTree(fetchOffsetResp.getContentAsString)
    var committedOffset = 3L  // Default if we can't parse response
    val topicsNode = offsetBody.get("topics")
    if (topicsNode != null && topicsNode.size() > 0) {
      val partitions = topicsNode.get(0).get("partitions")
      if (partitions != null && partitions.size() > 0) {
        committedOffset = partitions.get(0).get("offset").asLong()
      }
    }

    assertEquals(3L, committedOffset,
      "Committed offset should be 3")

    // Resume consumption from committed offset (3)
    val resumeFetch = client.consume(httpBaseUrl, testTopic,
      Seq(FetchPartitionSpec(partition = 0, offset = committedOffset)),
      maxWaitMs = 5000)
    assertEquals(200, resumeFetch.status)

    // Should get records starting from offset 3 (records 3 and 4)
    val partitions = resumeFetch.body.get("partitions")
    if (partitions != null && partitions.size() > 0) {
      val records = partitions.get(0).get("records")
      if (records != null && records.size() > 0) {
        val firstOffset = records.get(0).get("offset").asLong()
        assertEquals(3L, firstOffset,
          "First record after resume should be at offset 3")
      }
    }
  }

  // ===================================================================
  // Test 4: Commit offsets for new group creates group implicitly
  // ===================================================================
  @Test
  def testCommitOffsetsForNonExistentGroupCreatesGroup(): Unit = {
    val newGroup = "brand-new-group-" + System.currentTimeMillis()

    // Produce some records
    produceRecords(testTopic, 0, 3)

    // Commit to a group that has never been seen
    val commitBody = buildCommitBody(testTopic, Map(0 -> 1L))
    val commitResp = client.rawPost(
      s"$httpBaseUrl/v1/consumer-groups/$newGroup/offsets",
      commitBody)

    assertEquals(200, commitResp.getStatus,
      s"Commit to new group should return 200, got ${commitResp.getStatus}")

    // Verify we can fetch the committed offset
    val fetchResp = client.rawGet(
      s"$httpBaseUrl/v1/consumer-groups/$newGroup/offsets?topic=$testTopic")

    assertEquals(200, fetchResp.getStatus,
      s"Fetch from new group should return 200, got ${fetchResp.getStatus}")

    val body = mapper.readTree(fetchResp.getContentAsString)
    val topicsNode = body.get("topics")
    if (topicsNode != null && topicsNode.size() > 0) {
      val partitions = topicsNode.get(0).get("partitions")
      if (partitions != null && partitions.size() > 0) {
        val offset = partitions.get(0).get("offset").asLong()
        assertEquals(1L, offset,
          "Fetched offset should match committed offset")
      }
    }
  }

  // ===================================================================
  // Test 5: Fetch offsets for non-existent group returns empty/not-found
  // ===================================================================
  @Test
  def testFetchOffsetsForNonExistentGroupReturnsEmpty(): Unit = {
    val nonExistentGroup = "never-committed-group-" + System.currentTimeMillis()

    val response = client.rawGet(
      s"$httpBaseUrl/v1/consumer-groups/$nonExistentGroup/offsets?topic=$testTopic")

    // Either 200 with empty offsets, or 404 (group not found)
    assertTrue(response.getStatus == 200 || response.getStatus == 404,
      s"Expected 200 or 404 for non-existent group, got ${response.getStatus}")

    if (response.getStatus == 200) {
      val body = mapper.readTree(response.getContentAsString)
      val topicsNode = body.get("topics")
      if (topicsNode != null && topicsNode.size() > 0) {
        val partitions = topicsNode.get(0).get("partitions")
        if (partitions != null) {
          // Offsets should be -1 (no committed offset)
          for (i <- 0 until partitions.size()) {
            val offset = partitions.get(i).get("offset").asLong()
            assertEquals(-1L, offset,
              s"Offset for partition ${partitions.get(i).get("partition").asInt()} " +
                "should be -1 (no committed offset)")
          }
        }
      }
    }
  }
}
```

---

## Tests

### Run Command

```bash
./gradlew :http-server:test --tests 'kafka.http.HttpOffsetCommitIntegrationTest*'
```

### Expected Results

Tests should compile. They may not pass until the HTTP offset commit/fetch endpoints are fully implemented in the router/translator/handler chain. The tests document the expected behavior for the offset lifecycle.

---

## Rules

- Extend `HttpIntegrationTestHarness` -- do NOT create a custom test harness.
- Use `rawPost()` for commit operations and `rawGet()` for fetch operations.
- Build JSON request bodies with Jackson ObjectMapper -- do not use string concatenation.
- Each test uses a unique consumer group name (or the shared `testGroup`) to avoid cross-test interference.
- Parse response bodies defensively with null checks on JSON nodes.
- The commit/fetch round-trip pattern (Test 3) is the most important test -- it validates the core use case.

---

## Learning

(empty)

## Limitations

(empty)

## Field Notes

(empty)

---

## Acceptance Criteria

- [ ] `HttpOffsetCommitIntegrationTest.scala` is created at `http-server/src/test/scala/integration/kafka/http/HttpOffsetCommitIntegrationTest.scala`
- [ ] Test compiles with `./gradlew :http-server:compileTestScala`
- [ ] Tests document offset commit/fetch lifecycle behavior
- [ ] Tests pass when offset commit/fetch endpoints are implemented
- [ ] No modifications to existing integration test files

---

## File Manifest

(empty -- to be filled after implementation)
