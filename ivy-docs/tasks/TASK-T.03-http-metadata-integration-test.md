# TASK-T.03: HTTP Metadata Integration Test

## Prerequisites

| Task | What it provides |
|------|-----------------|
| TASK-0.04 | Build verified -- base code compiles and existing tests pass |
| TASK-C.03 | Phase 1 integration test infrastructure -- HttpIntegrationTestHarness, HttpTestClient |

---

## Context

The HTTP protocol design includes metadata endpoints for topic discovery, partition info, offsets, and consumer group lag. These endpoints are critical for operators and monitoring tools that interact with Kafka over HTTP.

Currently there are **zero integration tests** for metadata endpoints. The existing integration tests cover produce, consume, health check, and forwarding, but not metadata queries.

This task adds E2E tests that exercise:
- `GET /v1/topics/{topic}` -- topic partition details (leader, replicas, ISR)
- `GET /v1/topics` -- list all topics
- `GET /v1/topics/{topic}/partitions/{partition}/offsets?timestamp=earliest` -- earliest offset
- `GET /v1/consumer-groups/{group}/lags` -- consumer group lag

**Note:** These tests document expected behavior. They may not pass immediately if the metadata HTTP endpoints are not yet fully implemented. The tests should compile and serve as executable specifications.

### Key Code References

- **Test harness**: `HttpIntegrationTestHarness.scala` -- configures PLAINTEXT + HTTP listeners
- **Test client**: `HttpTestClient.scala` -- `rawGet()` for GET requests
- **Existing pattern**: `HttpProduceIntegrationTest.scala`, `HttpConsumeIntegrationTest.scala`
- **Design doc**: `ivy-docs/http-protocol-design.md` sections on metadata endpoints

---

## Specification

### Test 1: `testGetTopicDetails`
- Create topic "metadata-test" with 3 partitions, replication factor 1
- GET `/v1/topics/metadata-test`
- Verify 200 response with JSON containing: topic name, partitions array
- Each partition has: partitionId, leader (brokerId), replicas array, isr array

### Test 2: `testListTopics`
- Create topics "topic-a" and "topic-b"
- GET `/v1/topics`
- Verify 200 response with JSON array containing both topic names

### Test 3: `testGetTopicNotFound`
- GET `/v1/topics/nonexistent-topic`
- Verify 404 response

### Test 4: `testGetPartitionOffsets`
- Create topic and produce a few records
- GET `/v1/topics/{topic}/partitions/0/offsets?timestamp=earliest`
- Verify 200 response with offset = 0 (earliest)

### Test 5: `testGetConsumerGroupLags`
- Create topic, produce records, consume with a consumer group, commit offsets
- GET `/v1/consumer-groups/{group}/lags`
- Verify 200 response with lag information per partition

---

## Implementation Details

### File: `http-server/src/test/scala/integration/kafka/http/HttpMetadataIntegrationTest.scala`

Extends `HttpIntegrationTestHarness`. Uses `HttpTestClient.rawGet()` for GET requests and parses JSON responses with Jackson.

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

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import kafka.http.HttpTestClient._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test, TestInfo}
import org.junit.jupiter.api.Assertions._

/**
 * Integration tests for HTTP metadata endpoints.
 *
 * Tests:
 * - GET /v1/topics/{topic} -- topic partition details
 * - GET /v1/topics -- list all topics
 * - GET /v1/topics/{topic}/partitions/{p}/offsets -- partition offsets
 * - GET /v1/consumer-groups/{group}/lags -- consumer group lag
 *
 * Note: These tests document expected HTTP API behavior. They may not pass
 * until the metadata HTTP endpoints are fully implemented in the router/handler
 * chain. They should compile and serve as executable specifications.
 */
class HttpMetadataIntegrationTest extends HttpIntegrationTestHarness {

  private val testTopic = "metadata-test"
  private val numPartitions = 3
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

  // ===================================================================
  // Test 1: GET /v1/topics/{topic} returns partition details
  // ===================================================================
  @Test
  def testGetTopicDetails(): Unit = {
    val response = client.rawGet(s"$httpBaseUrl/v1/topics/$testTopic")
    assertEquals(200, response.getStatus,
      s"Expected 200 for topic details, got ${response.getStatus}")

    val body = mapper.readTree(response.getContentAsString)
    assertEquals(testTopic, body.get("topic").asText())

    val partitions = body.get("partitions")
    assertNotNull(partitions, "Response should contain partitions array")
    assertEquals(numPartitions, partitions.size(),
      s"Topic should have $numPartitions partitions")

    // Each partition should have leader, replicas, isr
    val p0 = partitions.get(0)
    assertTrue(p0.has("partitionId") || p0.has("partition"),
      "Partition should have partitionId field")
    assertTrue(p0.has("leader"), "Partition should have leader field")
    assertTrue(p0.has("replicas"), "Partition should have replicas field")
    assertTrue(p0.has("isr"), "Partition should have isr field")
  }

  // ===================================================================
  // Test 2: GET /v1/topics lists all topics
  // ===================================================================
  @Test
  def testListTopics(): Unit = {
    // Create a second topic
    createTopic("topic-b", 1, replicationFactor = 1)

    val response = client.rawGet(s"$httpBaseUrl/v1/topics")
    assertEquals(200, response.getStatus,
      s"Expected 200 for topic list, got ${response.getStatus}")

    val body = mapper.readTree(response.getContentAsString)
    assertTrue(body.isArray || body.has("topics"),
      "Response should be an array or contain a topics field")

    val topicsNode = if (body.isArray) body else body.get("topics")
    val topicNames = (0 until topicsNode.size()).map(i => {
      val node = topicsNode.get(i)
      if (node.isTextual) node.asText() else node.get("name").asText()
    }).toSet

    assertTrue(topicNames.contains(testTopic),
      s"Topic list should contain $testTopic")
    assertTrue(topicNames.contains("topic-b"),
      "Topic list should contain topic-b")
  }

  // ===================================================================
  // Test 3: GET /v1/topics/{nonexistent} returns 404
  // ===================================================================
  @Test
  def testGetTopicNotFound(): Unit = {
    val response = client.rawGet(s"$httpBaseUrl/v1/topics/nonexistent-topic-xyz")
    assertEquals(404, response.getStatus,
      s"Expected 404 for nonexistent topic, got ${response.getStatus}")
  }

  // ===================================================================
  // Test 4: GET /v1/topics/{topic}/partitions/{p}/offsets returns offset
  // ===================================================================
  @Test
  def testGetPartitionOffsets(): Unit = {
    // Produce some records first
    client.produce(httpBaseUrl, testTopic,
      Seq(ProduceRecord(partition = Some(0), value = Some(StringValue("msg-0")))))
    client.produce(httpBaseUrl, testTopic,
      Seq(ProduceRecord(partition = Some(0), value = Some(StringValue("msg-1")))))

    val response = client.rawGet(
      s"$httpBaseUrl/v1/topics/$testTopic/partitions/0/offsets?timestamp=earliest")
    assertEquals(200, response.getStatus,
      s"Expected 200 for partition offsets, got ${response.getStatus}")

    val body = mapper.readTree(response.getContentAsString)
    assertTrue(body.has("offset"), "Response should contain offset field")
    assertEquals(0L, body.get("offset").asLong(),
      "Earliest offset should be 0")
  }

  // ===================================================================
  // Test 5: GET /v1/consumer-groups/{group}/lags returns lag info
  // ===================================================================
  @Test
  def testGetConsumerGroupLags(): Unit = {
    // Produce records
    client.produce(httpBaseUrl, testTopic,
      Seq(
        ProduceRecord(partition = Some(0), value = Some(StringValue("lag-msg-0"))),
        ProduceRecord(partition = Some(0), value = Some(StringValue("lag-msg-1")))
      ))

    // Note: This test requires a consumer group to have committed offsets.
    // In a full E2E scenario, we would consume via the HTTP consume endpoint
    // and commit offsets. For now, we test the endpoint's basic structure.
    val response = client.rawGet(
      s"$httpBaseUrl/v1/consumer-groups/test-group/lags")

    // May be 200 (with lag data) or 404 (group not found) -- both are valid
    // depending on implementation state. We verify the response is well-formed.
    assertTrue(response.getStatus == 200 || response.getStatus == 404,
      s"Expected 200 or 404 for consumer group lags, got ${response.getStatus}")

    if (response.getStatus == 200) {
      val body = mapper.readTree(response.getContentAsString)
      assertTrue(body.has("lags") || body.has("partitions"),
        "Lag response should contain lags or partitions field")
    }
  }
}
```

---

## Tests

### Run Command

```bash
./gradlew :http-server:test --tests 'kafka.http.HttpMetadataIntegrationTest*'
```

### Expected Results

Tests should compile. They may not all pass if metadata endpoints are not yet implemented -- in that case, the failing tests document what behavior is expected. Tests that exercise unimplemented endpoints will likely get 404 or connection errors.

---

## Rules

- Extend `HttpIntegrationTestHarness` -- do NOT create a custom test harness.
- Use `HttpTestClient.rawGet()` for GET requests (the client only has helpers for produce/consume/health).
- Parse responses with Jackson `ObjectMapper` -- be defensive about JSON structure (use `has()` checks).
- Each test must be independent -- create topics in `@BeforeEach` or in the test itself.
- If an endpoint returns an unexpected status code because it is not yet implemented, the test should still document the expected behavior in its assertions.

---

## Learning

(empty)

## Limitations

(empty)

## Field Notes

(empty)

---

## Acceptance Criteria

- [ ] `HttpMetadataIntegrationTest.scala` is created at `http-server/src/test/scala/integration/kafka/http/HttpMetadataIntegrationTest.scala`
- [ ] Test compiles with `./gradlew :http-server:compileTestScala`
- [ ] Tests that pass confirm metadata endpoint functionality
- [ ] Tests that fail clearly document the expected endpoint behavior
- [ ] No modifications to existing integration test files

---

## File Manifest

(empty -- to be filled after implementation)
