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

import kafka.http.HttpTestClient._
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test, TestInfo, Timeout}
import org.junit.jupiter.api.Assertions._

import java.time.Duration
import java.util
import java.util.Properties
import java.util.concurrent.TimeUnit
import scala.collection.mutable
import scala.jdk.CollectionConverters._

/**
 * End-to-end round-trip tests for HTTP produce/consume across a multi-broker cluster.
 *
 * These tests start a 3-broker cluster with partitions spread across brokers,
 * then produce and consume via HTTP to verify the entire stack works. Each
 * request is routed to the correct partition leader (like binary protocol
 * clients do) using AdminClient metadata.
 *
 * Tests:
 * - Produce to all partitions (each via its leader) round-trip
 * - Produce via each broker's HTTP endpoint round-trip
 * - High-volume round-trip (100 records, no duplicates/losses)
 * - Keys preserved across leader-routed requests
 * - Multi-partition produce batch completeness
 */
@Timeout(value = 120, unit = TimeUnit.SECONDS)
class HttpMultiBrokerRoundTripTest extends HttpIntegrationTestHarness {

  override def brokerCount: Int = 3

  private val testTopic = "multi-broker-rt-test"
  private val numPartitions = 6
  private var client: HttpTestClient = _
  private var adminClient: Admin = _

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    super.setUp(testInfo)

    val adminProps = new Properties()
    adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers())
    adminClient = Admin.create(adminProps)

    createTopic(testTopic, numPartitions, replicationFactor = 2)
    client = new HttpTestClient()
    client.start()
  }

  @AfterEach
  override def tearDown(): Unit = {
    if (adminClient != null) adminClient.close(Duration.ofSeconds(5))
    if (client != null) client.close()
    super.tearDown()
  }

  /**
   * Find the leader broker ID for a given topic-partition using AdminClient.
   */
  private def findLeaderForPartition(topic: String, partition: Int): Int = {
    val topicDescription = adminClient
      .describeTopics(util.List.of(topic))
      .allTopicNames().get().get(topic)

    topicDescription.partitions().asScala
      .find(_.partition() == partition)
      .map(_.leader().id())
      .getOrElse(fail(s"No leader found for $topic-$partition"))
  }

  /**
   * Returns the HTTP URL of the leader broker for the given topic-partition.
   */
  private def leaderUrl(topic: String, partition: Int): String = {
    httpUrl(findLeaderForPartition(topic, partition))
  }

  // ===================================================================
  // Test 1: Produce to all partitions (each via its leader), consume all back
  // ===================================================================
  @Test
  def testProduceToAllPartitionsRoundTrip(): Unit = {
    val producedRecords = mutable.Map[Int, String]()

    // Produce 1 record to each of the 6 partitions via the partition's leader
    for (p <- 0 until numPartitions) {
      val value = s"partition-$p-data"
      val url = leaderUrl(testTopic, p)
      val response = client.produce(url, testTopic,
        Seq(ProduceRecord(partition = Some(p), value = Some(StringValue(value)))))

      assertEquals(200, response.status,
        s"Produce to partition $p should succeed, got ${response.status}")
      val errorCode = response.body.get("offsets").get(0).get("errorCode").asInt()
      assertEquals(0, errorCode,
        s"Produce to partition $p should have errorCode 0, got $errorCode")
      producedRecords(p) = value
    }

    // Consume from each partition via its leader
    for (p <- 0 until numPartitions) {
      val url = leaderUrl(testTopic, p)
      val response = client.consume(url, testTopic,
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
  // Test 2: Produce via each broker's HTTP endpoint (to partitions it leads)
  // ===================================================================
  @Test
  def testProduceViaEachBrokerRoundTrip(): Unit = {
    val topic = "per-broker-rt-test"
    createTopic(topic, 3, replicationFactor = 1)

    val producedByPartition = mutable.Map[Int, String]()

    // Produce to each partition via its leader
    for (p <- 0 until 3) {
      val leader = findLeaderForPartition(topic, p)
      val value = s"from-broker-$leader-partition-$p"
      val response = client.produce(httpUrl(leader), topic,
        Seq(ProduceRecord(partition = Some(p), value = Some(StringValue(value)))))

      assertEquals(200, response.status,
        s"Produce to partition $p via leader $leader should succeed, got ${response.status}")
      producedByPartition(p) = value
    }

    // Consume each partition via its leader
    for (p <- 0 until 3) {
      val leader = findLeaderForPartition(topic, p)
      val response = client.consume(httpUrl(leader), topic,
        Seq(FetchPartitionSpec(partition = p, offset = 0)),
        maxWaitMs = 5000)

      assertEquals(200, response.status,
        s"Consume partition $p via leader $leader should succeed")

      val records = response.body.get("partitions").get(0).get("records")
      assertTrue(records.size() > 0,
        s"Partition $p should have at least 1 record")

      val consumedValue = records.get(0).get("value").get("data").asText()
      assertEquals(producedByPartition(p), consumedValue,
        s"Data mismatch for partition $p")
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

    // Produce 100 records, distributed across partitions, each sent to the leader
    for (i <- 0 until totalRecords) {
      val partition = i % partitionCount
      val value = s"record-$i"
      val url = leaderUrl(topic, partition)
      val response = client.produce(url, topic,
        Seq(ProduceRecord(partition = Some(partition), value = Some(StringValue(value)))))

      assertEquals(200, response.status,
        s"Produce record $i should succeed, got ${response.status}")
      expectedPerPartition(partition).add(value)
    }

    // Consume all records from each partition via its leader
    var totalConsumed = 0
    val consumedValues = mutable.Set[String]()

    for (p <- 0 until partitionCount) {
      val url = leaderUrl(topic, p)
      var offset = 0L
      var done = false

      while (!done) {
        val response = client.consume(url, topic,
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
  // Test 4: Keys preserved across leader-routed requests
  // ===================================================================
  @Test
  def testKeysPreservedAcrossForwarding(): Unit = {
    val topic = "key-roundtrip-test"
    createTopic(topic, numPartitions, replicationFactor = 2)

    val testKeys = Seq("order-123", "user-456", "event-789")

    for ((key, i) <- testKeys.zipWithIndex) {
      val partition = i % numPartitions
      val url = leaderUrl(topic, partition)
      val response = client.produce(url, topic,
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
      val url = leaderUrl(topic, p)
      val response = client.consume(url, topic,
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
    // Group partitions by leader and send a batch to each leader containing
    // only the partitions that leader owns. This tests batch produce without
    // requiring cross-broker forwarding.
    val partitionsByLeader = (0 until 3).groupBy(p => findLeaderForPartition(testTopic, p))

    var totalOffsetEntries = 0

    for ((leader, partitions) <- partitionsByLeader) {
      val records = partitions.map { p =>
        ProduceRecord(partition = Some(p), value = Some(StringValue(s"batch-$p")))
      }

      val response = client.produce(httpUrl(leader), testTopic, records)

      assertEquals(200, response.status,
        s"Batch produce to leader $leader should succeed, got ${response.status}")

      val offsets = response.body.get("offsets")
      assertNotNull(offsets, "Response should have offsets array")
      assertEquals(partitions.size, offsets.size(),
        s"Response from leader $leader should have results for ${partitions.size} partitions")

      // Verify each partition result has either a valid offset or error code
      for (i <- 0 until offsets.size()) {
        val result = offsets.get(i)
        assertTrue(result.has("partition"), s"Result $i should have partition field")
        assertTrue(result.has("offset") || result.has("errorCode"),
          s"Result $i should have offset or errorCode")
      }

      totalOffsetEntries += offsets.size()
    }

    assertEquals(3, totalOffsetEntries,
      "Total offset entries across all leaders should be 3")
  }
}
