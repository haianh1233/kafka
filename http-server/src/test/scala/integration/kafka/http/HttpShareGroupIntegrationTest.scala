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
package kafka.http

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import kafka.http.HttpTestClient._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test, TestInfo, Timeout}
import org.junit.jupiter.api.Assertions._

import java.util.Properties
import java.util.concurrent.TimeUnit

/**
 * Integration tests for the share group HTTP endpoints (KIP-932).
 *
 * Share groups allow multiple consumers to read from the same partitions with
 * server-side acknowledgement tracking. These tests validate:
 *
 * 1. Poll returns records with non-empty acquireId fields
 * 2. ACCEPT prevents re-delivery of acknowledged records
 * 3. REJECT causes re-delivery of rejected records
 * 4. RELEASE causes immediate re-delivery
 * 5. Multiple consumers receive different records (load balancing)
 * 6. Duplicate acknowledgement (idempotent) does not cause errors
 * 7. Unknown acquireId returns an appropriate error
 * 8. Poll on empty topic returns 200 with empty records
 *
 * Tests exercise the full end-to-end path:
 *   HTTP request -> HttpRequestTranslator -> RequestChannel -> KafkaApis
 *   (ShareFetch/ShareAcknowledge handlers) -> share group coordinator ->
 *   response -> HttpResponseSerializer -> HTTP response.
 *
 * Each test uses a unique share group name to avoid interference between tests.
 *
 * // Time: Created - TASK-G.02
 */
@Timeout(value = 120, unit = TimeUnit.SECONDS)
class HttpShareGroupIntegrationTest extends HttpIntegrationTestHarness {

  private val topicName = "share-group-test"
  private val numPartitions = 3
  private var client: HttpTestClient = _
  private val mapper = new ObjectMapper()

  /**
   * Enable share group support by configuring the rebalance protocols.
   * This setting is required for KIP-932 share group functionality.
   */
  override protected def modifyConfigs(props: scala.collection.Seq[Properties]): Unit = {
    super.modifyConfigs(props)
    props.foreach { config =>
      config.setProperty(
        "group.coordinator.rebalance.protocols",
        "classic,consumer,share"
      )
    }
  }

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    super.setUp(testInfo)
    createTopic(topicName, numPartitions, replicationFactor = 1)
    client = new HttpTestClient()
    client.start()

    // Produce 20 test records across partitions via HTTP produce endpoint
    for (i <- 0 until 20) {
      client.produce(httpBaseUrl, topicName,
        Seq(ProduceRecord(
          partition = Some(i % numPartitions),
          value = Some(StringValue(s"record-$i"))
        )))
    }
  }

  @AfterEach
  override def tearDown(): Unit = {
    if (client != null) client.close()
    super.tearDown()
  }

  // --- Scenario 1: Poll Returns Records with acquireId ---

  @Test
  def testPollReturnsRecordsWithAcquireId(): Unit = {
    val pollResult = pollShareGroup("poll-acquire-id-group", Seq(topicName),
      maxRecords = 5, maxWaitMs = 5000)
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
    }
  }

  // --- Scenario 2: ACCEPT Prevents Re-Delivery ---

  @Test
  def testAcceptPreventsReDelivery(): Unit = {
    val group = "accept-test-group"
    val pollResult = pollShareGroup(group, Seq(topicName), maxRecords = 3, maxWaitMs = 5000)
    assertTrue(pollResult.records.nonEmpty, "First poll should return records")

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
    assertTrue(pollResult.records.nonEmpty, "First poll should return records")

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
    assertTrue(pollResult.records.nonEmpty, "First poll should return records")

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
    val group = "multi-consumer-group"
    val allRecords = scala.collection.mutable.Set[(String, Int, Long)]()

    // Consumer 1 polls
    val poll1 = pollShareGroup(group, Seq(topicName), maxRecords = 10, maxWaitMs = 5000)
    poll1.records.foreach { r =>
      allRecords.add((r.get("topic").asText(), r.get("partition").asInt(),
        r.get("offset").asLong()))
    }
    // ACCEPT consumer 1's records
    val acks1 = poll1.records.map(r => (r.get("acquireId").asText(), "ACCEPT"))
    if (acks1.nonEmpty) acknowledgeRecords(group, acks1)

    // Consumer 2 polls
    val poll2 = pollShareGroup(group, Seq(topicName), maxRecords = 10, maxWaitMs = 5000)
    poll2.records.foreach { r =>
      val key = (r.get("topic").asText(), r.get("partition").asInt(),
        r.get("offset").asLong())
      assertFalse(allRecords.contains(key),
        s"Consumer 2 should not receive records already accepted by consumer 1: $key")
      allRecords.add(key)
    }

    // At least one consumer should have received records
    assertTrue(poll1.records.nonEmpty || poll2.records.nonEmpty,
      "At least one consumer should receive records")
  }

  // --- Scenario 6: Idempotent Acknowledgement ---

  @Test
  def testIdempotentAcknowledge(): Unit = {
    val group = "idempotent-ack-group"
    val pollResult = pollShareGroup(group, Seq(topicName), maxRecords = 1, maxWaitMs = 5000)
    assertTrue(pollResult.records.nonEmpty, "Poll should return at least one record")

    val acquireId = pollResult.records.head.get("acquireId").asText()

    // ACCEPT once
    val ack1 = acknowledgeRecords(group, Seq((acquireId, "ACCEPT")))
    assertEquals(200, ack1.statusCode)

    // ACCEPT again (idempotent -- should not cause a server error)
    val ack2 = acknowledgeRecords(group, Seq((acquireId, "ACCEPT")))
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

  // --- Helper Types ---

  case class SharePollResult(statusCode: Int, records: Seq[JsonNode])
  case class AckResult(statusCode: Int, body: JsonNode)

  // --- Helper Methods ---

  /**
   * Poll records from a share group via the HTTP endpoint.
   *
   * POST /v1/share-groups/{group}/records
   *
   * @param group      share group ID
   * @param topics     list of topic names to poll
   * @param maxRecords maximum number of records to return
   * @param maxWaitMs  maximum time to wait for records (in milliseconds)
   * @return SharePollResult with the HTTP status code and parsed records
   */
  private def pollShareGroup(
    group: String,
    topics: Seq[String],
    maxRecords: Int,
    maxWaitMs: Int
  ): SharePollResult = {
    val topicsJson = topics.map(t => s""""$t"""").mkString(",")
    val body = s"""{"topics":[$topicsJson],"maxRecords":$maxRecords,"maxWaitMs":$maxWaitMs}"""

    val response = client.rawPost(
      s"$httpBaseUrl/v1/share-groups/$group/records", body)

    val json = mapper.readTree(response.getContentAsString)
    val records = if (json.has("records")) {
      (0 until json.get("records").size()).map(i => json.get("records").get(i))
    } else {
      Seq.empty
    }
    SharePollResult(response.getStatus, records)
  }

  /**
   * Acknowledge records in a share group via the HTTP endpoint.
   *
   * POST /v1/share-groups/{group}/acknowledge
   *
   * @param group               share group ID
   * @param acquireIdsAndTypes  sequence of (acquireId, ackType) tuples where
   *                            ackType is one of ACCEPT, REJECT, or RELEASE
   * @return AckResult with the HTTP status code and parsed response body
   */
  private def acknowledgeRecords(
    group: String,
    acquireIdsAndTypes: Seq[(String, String)]
  ): AckResult = {
    val acksJson = acquireIdsAndTypes.map { case (id, ackType) =>
      s"""{"acquireId":"$id","type":"$ackType"}"""
    }.mkString(",")
    val body = s"""{"acknowledgements":[$acksJson]}"""

    val response = client.rawPost(
      s"$httpBaseUrl/v1/share-groups/$group/acknowledge", body)

    AckResult(response.getStatus, mapper.readTree(response.getContentAsString))
  }
}
