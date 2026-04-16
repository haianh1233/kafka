# TASK-T.10: HTTP Multi-Broker Produce/Consume Round-Trip Test

## Prerequisites

| Task | What it provides |
|------|-----------------|
| TASK-0.04 | Build verified -- base code compiles and existing tests pass |
| TASK-C.03 | Phase 1 integration test infrastructure -- HttpIntegrationTestHarness, HttpTestClient |
| TASK-D.03 | Produce forwarding integration (multi-broker produce path) |
| TASK-D.04 | Consume forwarding and quota (multi-broker consume path) |

---

## Context

The existing integration tests (`HttpProduceIntegrationTest`, `HttpConsumeIntegrationTest`) use a single broker where all partitions are local. The forwarding tests (`HttpForwardingIntegrationTest`) test the forwarding path specifically. But there are **no end-to-end round-trip tests** that:

1. Start a multi-broker cluster
2. Create a topic with partitions spread across brokers
3. Produce to ALL partitions via HTTP (some local, some forwarded)
4. Consume from ALL partitions via HTTP (some local, some forwarded)
5. Verify data integrity across the full round-trip

This is the "smoke test" that validates the entire HTTP protocol stack works end-to-end in a realistic multi-broker deployment.

### Key Code References

- **Test harness**: `HttpIntegrationTestHarness.scala` -- `brokerCount` override for multi-broker
- **Test client**: `HttpTestClient.scala`
- **Existing forwarding test**: `HttpForwardingIntegrationTest.scala`
- **Produce forwarding**: `KafkaApis.scala` `handleHttpProduceRequest` -- remote partition logic
- **Consume forwarding**: `KafkaApis.scala` `handleHttpConsumeRequest` -- remote partition logic

---

## Specification

### Test 1: `testProduceToAllPartitionsRoundTrip`
- Start 3-broker cluster
- Create topic with 6 partitions, replication factor 2
- Produce 1 record to each partition (6 records total) via HTTP to broker 0
- Some partitions are local to broker 0, others require forwarding
- Consume from each partition via HTTP to broker 0
- Verify all 6 records are returned with correct data

### Test 2: `testProduceViaEachBrokerRoundTrip`
- Start 3-broker cluster
- Create topic with 3 partitions, replication factor 1
- Produce to each broker's HTTP endpoint (broker 0, 1, 2)
- Consume from broker 0 (verifying forwarding works for consume too)
- Verify all records are present

### Test 3: `testHighVolumeRoundTrip`
- Start 2-broker cluster
- Create topic with 4 partitions
- Produce 100 records spread across partitions
- Consume all records
- Verify count matches (100 records total)
- Verify no duplicates and no missing records

### Test 4: `testKeysPreservedAcrossForwarding`
- Produce records with keys to partitions that require forwarding
- Consume and verify keys are preserved exactly (byte-for-byte)
- This catches any serialization/deserialization bugs in the forwarding path

### Test 5: `testMultiPartitionProduceAtomicity`
- Produce a batch containing records for multiple partitions (mix of local and remote)
- Verify the response contains results for ALL partitions in the batch
- Each partition result should have either a valid offset or an error code

---

## Implementation Details

### File: `http-server/src/test/scala/integration/kafka/http/HttpMultiBrokerRoundTripTest.scala`

Extends `HttpIntegrationTestHarness` with `brokerCount = 3`. Uses `httpUrl(brokerId)` to send requests to specific brokers.

Key difference from single-broker tests: topic partitions are distributed across brokers, so produce/consume requests to a single broker will trigger the forwarding path for non-local partitions.

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
import kafka.http.HttpTestClient._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test, TestInfo, Timeout}
import org.junit.jupiter.api.Assertions._

import java.util.concurrent.TimeUnit
import scala.collection.mutable

/**
 * End-to-end round-trip tests for HTTP produce/consume across a multi-broker cluster.
 *
 * These tests start a 3-broker cluster with partitions spread across brokers,
 * then produce and consume via HTTP to verify the entire stack works including
 * forwarding for non-local partitions.
 *
 * Tests:
 * - Produce to all partitions via single broker (local + forwarded) round-trip
 * - Produce via each broker's HTTP endpoint round-trip
 * - High-volume round-trip (100 records, no duplicates/losses)
 * - Keys preserved across forwarding
 * - Multi-partition produce batch atomicity
 *
 * Note: These tests require produce/consume forwarding to be fully implemented.
 * They may not pass until the forwarding infrastructure (TASK-D.03, D.04) is
 * complete. They should compile and serve as executable specifications.
 */
@Timeout(value = 120, unit = TimeUnit.SECONDS)
class HttpMultiBrokerRoundTripTest extends HttpIntegrationTestHarness {

  override def brokerCount: Int = 3

  private val testTopic = "multi-broker-rt-test"
  private val numPartitions = 6
  private var client: HttpTestClient = _
  private val mapper = new ObjectMapper()

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    super.setUp(testInfo)
    createTopic(testTopic, numPartitions, replicationFactor = 2)
    client = new HttpTestClient()
    client.start()
  }

  @AfterEach
  override def tearDown(): Unit = {
    if (client != null) client.close()
    super.tearDown()
  }

  // ===================================================================
  // Test 1: Produce to all partitions via broker 0, consume all back
  // ===================================================================
  @Test
  def testProduceToAllPartitionsRoundTrip(): Unit = {
    val producedRecords = mutable.Map[Int, String]()

    // Produce 1 record to each of the 6 partitions via broker 0
    for (p <- 0 until numPartitions) {
      val value = s"partition-$p-data"
      val response = client.produce(httpUrl(0), testTopic,
        Seq(ProduceRecord(partition = Some(p), value = Some(StringValue(value)))))

      assertEquals(200, response.status,
        s"Produce to partition $p should succeed, got ${response.status}")
      val errorCode = response.body.get("offsets").get(0).get("errorCode").asInt()
      assertEquals(0, errorCode,
        s"Produce to partition $p should have errorCode 0, got $errorCode")
      producedRecords(p) = value
    }

    // Consume from each partition via broker 0
    for (p <- 0 until numPartitions) {
      val response = client.consume(httpUrl(0), testTopic,
        Seq(FetchPartitionSpec(partition = p, offset = 0)),
        maxWaitMs = 5000)

      assertEquals(200, response.status,
        s"Consume from partition $p should succeed, got ${response.status}")

      val partitions = response.body.get("partitions")
      assertNotNull(partitions, s"Consume response for partition $p should have partitions")
      assertTrue(partitions.size() > 0,
        s"Consume response should have partition data")

      val records = partitions.get(0).get("records")
      assertTrue(records.size() > 0,
        s"Partition $p should have at least 1 record")

      // Verify the data matches what we produced
      val firstRecord = records.get(0)
      val consumedValue = firstRecord.get("value").get("data").asText()
      assertEquals(producedRecords(p), consumedValue,
        s"Round-trip data mismatch for partition $p")
    }
  }

  // ===================================================================
  // Test 2: Produce via each broker's HTTP endpoint
  // ===================================================================
  @Test
  def testProduceViaEachBrokerRoundTrip(): Unit = {
    val topic = "per-broker-rt-test"
    createTopic(topic, 3, replicationFactor = 1)

    val producedByBroker = mutable.Map[Int, String]()

    // Produce via each broker's HTTP endpoint
    for (brokerId <- 0 until brokerCount) {
      val value = s"from-broker-$brokerId"
      val response = client.produce(httpUrl(brokerId), topic,
        Seq(ProduceRecord(partition = Some(brokerId), value = Some(StringValue(value)))))

      assertEquals(200, response.status,
        s"Produce via broker $brokerId should succeed, got ${response.status}")
      producedByBroker(brokerId) = value
    }

    // Consume all partitions via broker 0
    for (p <- 0 until brokerCount) {
      val response = client.consume(httpUrl(0), topic,
        Seq(FetchPartitionSpec(partition = p, offset = 0)),
        maxWaitMs = 5000)

      assertEquals(200, response.status,
        s"Consume partition $p via broker 0 should succeed")

      val records = response.body.get("partitions").get(0).get("records")
      assertTrue(records.size() > 0,
        s"Partition $p should have at least 1 record")

      val consumedValue = records.get(0).get("value").get("data").asText()
      assertEquals(producedByBroker(p), consumedValue,
        s"Data mismatch for partition $p produced via broker $p")
    }
  }

  // ===================================================================
  // Test 3: High-volume round-trip (100 records)
  // ===================================================================
  @Test
  def testHighVolumeRoundTrip(): Unit = {
    val topic = "high-volume-rt-test"
    val partitionCount = 4
    createTopic(topic, partitionCount, replicationFactor = 2)

    val totalRecords = 100
    val expectedPerPartition = mutable.Map[Int, mutable.Set[String]]()
    for (p <- 0 until partitionCount) expectedPerPartition(p) = mutable.Set.empty

    // Produce 100 records, distributed across partitions
    for (i <- 0 until totalRecords) {
      val partition = i % partitionCount
      val value = s"record-$i"
      val response = client.produce(httpUrl(0), topic,
        Seq(ProduceRecord(partition = Some(partition), value = Some(StringValue(value)))))

      assertEquals(200, response.status,
        s"Produce record $i should succeed, got ${response.status}")
      expectedPerPartition(partition).add(value)
    }

    // Consume all records from each partition
    var totalConsumed = 0
    val consumedValues = mutable.Set[String]()

    for (p <- 0 until partitionCount) {
      var offset = 0L
      var done = false

      while (!done) {
        val response = client.consume(httpUrl(0), topic,
          Seq(FetchPartitionSpec(partition = p, offset = offset)),
          maxWaitMs = 2000)

        assertEquals(200, response.status)
        val records = response.body.get("partitions").get(0).get("records")

        if (records.size() == 0) {
          done = true
        } else {
          for (r <- 0 until records.size()) {
            val record = records.get(r)
            val value = record.get("value").get("data").asText()
            consumedValues.add(value)
            totalConsumed += 1
            offset = record.get("offset").asLong() + 1
          }
        }
      }
    }

    assertEquals(totalRecords, totalConsumed,
      s"Should consume exactly $totalRecords records, got $totalConsumed")
    assertEquals(totalRecords, consumedValues.size,
      "No duplicate values should exist")

    // Verify all expected values are present
    for (p <- 0 until partitionCount) {
      expectedPerPartition(p).foreach { expectedValue =>
        assertTrue(consumedValues.contains(expectedValue),
          s"Missing expected value: $expectedValue")
      }
    }
  }

  // ===================================================================
  // Test 4: Keys preserved across forwarding
  // ===================================================================
  @Test
  def testKeysPreservedAcrossForwarding(): Unit = {
    val topic = "key-roundtrip-test"
    createTopic(topic, numPartitions, replicationFactor = 2)

    val testKeys = Seq("order-123", "user-456", "event-789")

    for ((key, i) <- testKeys.zipWithIndex) {
      val partition = i % numPartitions
      val response = client.produce(httpUrl(0), topic,
        Seq(ProduceRecord(
          partition = Some(partition),
          key = Some(StringValue(key)),
          value = Some(StringValue(s"value-for-$key")))))

      assertEquals(200, response.status,
        s"Produce with key '$key' should succeed")
    }

    // Consume and verify keys
    val consumedKeys = mutable.Set[String]()
    for (p <- 0 until numPartitions) {
      val response = client.consume(httpUrl(0), topic,
        Seq(FetchPartitionSpec(partition = p, offset = 0)),
        maxWaitMs = 5000)

      if (response.status == 200) {
        val records = response.body.get("partitions").get(0).get("records")
        for (r <- 0 until records.size()) {
          val record = records.get(r)
          val keyNode = record.get("key")
          if (keyNode != null && !keyNode.isNull && keyNode.has("data")) {
            consumedKeys.add(keyNode.get("data").asText())
          }
        }
      }
    }

    // Verify all keys were preserved
    testKeys.foreach { key =>
      assertTrue(consumedKeys.contains(key),
        s"Key '$key' should be preserved in round-trip, consumed keys: $consumedKeys")
    }
  }

  // ===================================================================
  // Test 5: Multi-partition produce batch has results for all partitions
  // ===================================================================
  @Test
  def testMultiPartitionProduceBatchCompleteness(): Unit = {
    // Produce a batch with records for multiple partitions
    val records = (0 until 3).map { p =>
      ProduceRecord(partition = Some(p), value = Some(StringValue(s"batch-$p")))
    }

    val response = client.produce(httpUrl(0), testTopic, records)

    assertEquals(200, response.status,
      s"Multi-partition batch produce should succeed, got ${response.status}")

    val offsets = response.body.get("offsets")
    assertNotNull(offsets, "Response should have offsets array")
    assertEquals(3, offsets.size(),
      "Response should have results for all 3 partitions in the batch")

    // Verify each partition result has either a valid offset or error code
    for (i <- 0 until offsets.size()) {
      val result = offsets.get(i)
      assertTrue(result.has("partition"), s"Result $i should have partition field")
      assertTrue(result.has("offset") || result.has("errorCode"),
        s"Result $i should have offset or errorCode")
    }
  }
}
```

---

## Tests

### Run Command

```bash
./gradlew :http-server:test --tests 'kafka.http.HttpMultiBrokerRoundTripTest*'
```

### Expected Results

Tests should compile. They require:
1. Multi-broker cluster startup (brokerCount=3)
2. HTTP produce/consume forwarding to be implemented (TASK-D.03, D.04)
3. The HTTP acceptor to bind to a real network port

If the forwarding infrastructure is not complete, tests that produce/consume to non-local partitions will fail. This is expected and the test failures will clearly identify which forwarding paths are broken.

---

## Rules

- Extend `HttpIntegrationTestHarness` with `override def brokerCount: Int = 3`.
- Use `httpUrl(brokerId)` to send requests to specific brokers.
- Use `@Timeout(value = 120, unit = TimeUnit.SECONDS)` to prevent test timeouts in CI (multi-broker startup is slow).
- Track produced vs consumed data using maps/sets to verify completeness.
- Tests must verify BOTH the produce response AND the consume response.
- Do NOT assume partition-to-broker assignment -- partition leaders may be on any broker.
- Each test uses a unique topic name to avoid cross-test interference.

---

## Learning

(empty)

## Limitations

(empty)

## Field Notes

(empty)

---

## Acceptance Criteria

- [ ] `HttpMultiBrokerRoundTripTest.scala` is created at `http-server/src/test/scala/integration/kafka/http/HttpMultiBrokerRoundTripTest.scala`
- [ ] Test compiles with `./gradlew :http-server:compileTestScala`
- [ ] Tests verify end-to-end data integrity across multi-broker HTTP produce/consume
- [ ] High-volume test verifies no duplicates and no missing records
- [ ] Key preservation test catches serialization bugs in forwarding
- [ ] No modifications to existing integration test files

---

## File Manifest

(empty -- to be filled after implementation)
