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

import kafka.http.HttpTestClient._
import kafka.utils.TestUtils
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test, TestInfo, Timeout}
import org.junit.jupiter.api.Assertions._

import java.time.Duration
import java.util
import java.util.Properties
import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters._

/**
 * Multi-broker integration tests for HTTP request forwarding.
 *
 * Validates the core value proposition of the HTTP protocol: a client can
 * connect to any broker and produce/consume for any partition -- the broker
 * transparently forwards to the correct leader.
 *
 * Uses a 3-broker cluster with replication factor 3 so that every partition
 * has replicas on all brokers and leaders are spread across them.
 *
 * Scenarios tested:
 *   1. Produce to a follower broker -- forwarded to leader
 *   2. Consume from a follower broker -- forwarded to leader
 *   3. Multi-partition produce with mixed local/remote leaders
 *   4. Leader failover -- produce succeeds after leader change
 *   5. Forward timeout -- produce to a dead leader returns 5xx
 *   6. Consume fan-out across multiple brokers
 */
@Timeout(value = 120, unit = TimeUnit.SECONDS)
class HttpForwardingIntegrationTest extends HttpIntegrationTestHarness {

  override def brokerCount: Int = 3

  private val testTopic = "forwarding-test"
  private val numPartitions = 3
  private val replicationFactor: Short = 3
  private var client: HttpTestClient = _
  private var adminClient: Admin = _

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    super.setUp(testInfo)
    client = new HttpTestClient()
    client.start()

    val adminProps = new Properties()
    adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers())
    adminClient = Admin.create(adminProps)

    createTopic(testTopic, numPartitions, replicationFactor)
    waitForAllPartitionLeaders(testTopic, numPartitions)
  }

  @AfterEach
  override def tearDown(): Unit = {
    if (adminClient != null) adminClient.close(Duration.ofSeconds(5))
    if (client != null) client.close()
    // Restart any killed brokers before tearDown to avoid interference
    restartDeadBrokers()
    super.tearDown()
  }

  // -------------------------------------------------------------------
  // Scenario 1: Produce to Follower, Forwarded to Leader
  // -------------------------------------------------------------------

  @Test
  def testProduceToFollowerIsForwardedToLeader(): Unit = {
    val partition = 0
    val leader = findLeaderForPartition(testTopic, partition)
    val follower = findNonLeaderBroker(testTopic, partition)
    val followerUrl = httpUrl(follower)

    val response = client.produce(followerUrl, testTopic,
      Seq(ProduceRecord(
        partition = Some(partition),
        value = Some(StringValue("forwarded-record"))
      )),
      acks = "all")

    assertEquals(200, response.status,
      s"Produce to follower (broker $follower) for partition $partition (leader=$leader) " +
        s"should succeed via forwarding. Body: ${response.body}")

    val offsets = response.body.get("offsets")
    assertNotNull(offsets, "Response must contain offsets array")
    assertTrue(offsets.size() > 0, "At least one offset entry expected")
    assertEquals(partition, offsets.get(0).get("partition").asInt())
    assertTrue(offsets.get(0).get("offset").asLong() >= 0,
      "Offset should be non-negative")
    assertEquals(0, offsets.get(0).get("errorCode").asInt(),
      s"No error expected, but got: ${offsets.get(0)}")
  }

  // -------------------------------------------------------------------
  // Scenario 2: Consume from Follower, Forwarded to Leader
  // -------------------------------------------------------------------

  @Test
  def testConsumeFromFollowerIsForwardedToLeader(): Unit = {
    val partition = 0

    // Produce a record via the leader to ensure it exists
    val leader = findLeaderForPartition(testTopic, partition)
    val leaderUrl = httpUrl(leader)

    val produceResp = client.produce(leaderUrl, testTopic,
      Seq(ProduceRecord(
        partition = Some(partition),
        value = Some(StringValue("consume-via-follower"))
      )),
      acks = "all")
    assertEquals(200, produceResp.status, "Seed produce should succeed")
    val producedOffset = produceResp.body.get("offsets").get(0).get("offset").asLong()

    // Consume from a follower -- the request should be forwarded
    val follower = findNonLeaderBroker(testTopic, partition)
    val followerUrl = httpUrl(follower)

    val consumeResp = client.consume(followerUrl, testTopic,
      Seq(FetchPartitionSpec(partition = partition, offset = producedOffset)),
      maxWaitMs = 5000)

    assertEquals(200, consumeResp.status,
      s"Consume from follower (broker $follower) should succeed via forwarding. " +
        s"Body: ${consumeResp.body}")

    val partitions = consumeResp.body.get("partitions")
    assertNotNull(partitions, "Response must contain partitions array")
    assertTrue(partitions.size() > 0, "At least one partition entry expected")

    val records = partitions.get(0).get("records")
    assertTrue(records.size() > 0,
      "Should fetch the produced record via follower forwarding")
  }

  // -------------------------------------------------------------------
  // Scenario 3: Multi-Partition Produce with Mixed Leaders
  // -------------------------------------------------------------------

  @Test
  def testMultiPartitionProduceWithMixedLeaders(): Unit = {
    // Send records for all 3 partitions to a single broker.
    // Some partitions will be local (this broker is leader),
    // others will be remote (forwarded to the actual leader).
    val targetBroker = 0
    val targetUrl = httpUrl(targetBroker)

    val records = (0 until numPartitions).map { p =>
      ProduceRecord(
        partition = Some(p),
        value = Some(StringValue(s"mixed-partition-$p"))
      )
    }

    val response = client.produce(targetUrl, testTopic, records, acks = "all")

    // Should succeed: 200 (all local+forwarded succeed) or 207 (partial)
    assertTrue(response.status == 200 || response.status == 207,
      s"Expected 200 or 207, got ${response.status}: ${response.body}")

    val offsets = response.body.get("offsets")
    assertNotNull(offsets, "Response must contain offsets array")
    assertEquals(numPartitions, offsets.size(),
      "Should have one offset entry per partition")

    for (i <- 0 until offsets.size()) {
      assertEquals(0, offsets.get(i).get("errorCode").asInt(),
        s"Partition ${offsets.get(i).get("partition")} had error: " +
          s"${offsets.get(i).get("errorMessage")}")
    }
  }

  // -------------------------------------------------------------------
  // Scenario 4: Leader Failover
  // -------------------------------------------------------------------

  @Test
  def testProduceAfterLeaderFailover(): Unit = {
    val failoverTopic = "failover-test"
    createTopic(failoverTopic, 1, replicationFactor)
    waitForAllPartitionLeaders(failoverTopic, 1)

    val partition = 0
    val oldLeader = findLeaderForPartition(failoverTopic, partition)
    val follower = findNonLeaderBroker(failoverTopic, partition)
    val followerUrl = httpUrl(follower)

    // Produce succeeds before failover
    val resp1 = client.produce(followerUrl, failoverTopic,
      Seq(ProduceRecord(
        partition = Some(partition),
        value = Some(StringValue("before-failover"))
      )),
      acks = "all")
    assertEquals(200, resp1.status,
      s"Produce before failover should succeed. Body: ${resp1.body}")

    // Kill the leader broker
    killBroker(oldLeader)

    // Wait for a new leader to be elected
    TestUtils.waitUntilLeaderIsElectedOrChangedWithAdmin(
      adminClient, failoverTopic, partition,
      timeoutMs = 30000L,
      oldLeaderOpt = Some(oldLeader))

    // Produce again via the same follower -- should succeed after metadata refresh
    var resp2 = client.produce(followerUrl, failoverTopic,
      Seq(ProduceRecord(
        partition = Some(partition),
        value = Some(StringValue("after-failover"))
      )),
      acks = "all")

    // Allow one retry if the broker hasn't refreshed metadata yet
    if (resp2.status == 503) {
      Thread.sleep(2000)
      resp2 = client.produce(followerUrl, failoverTopic,
        Seq(ProduceRecord(
          partition = Some(partition),
          value = Some(StringValue("after-failover-retry"))
        )),
        acks = "all")
    }

    assertEquals(200, resp2.status,
      s"Produce after leader failover should succeed. Body: ${resp2.body}")

    // Restart the killed broker for cleanup
    startBroker(oldLeader)
  }

  // -------------------------------------------------------------------
  // Scenario 5: Forward Timeout
  // -------------------------------------------------------------------

  @Test
  def testForwardTimeout(): Unit = {
    val timeoutTopic = "timeout-test"
    createTopic(timeoutTopic, 1, replicationFactor)
    waitForAllPartitionLeaders(timeoutTopic, 1)

    val partition = 0
    val leader = findLeaderForPartition(timeoutTopic, partition)
    val follower = findNonLeaderBroker(timeoutTopic, partition)
    val followerUrl = httpUrl(follower)

    // Kill the leader -- stale metadata on the follower means it will try
    // to forward to a dead broker, which should time out.
    killBroker(leader)

    // Immediately try to produce with a short timeout.
    // The follower should attempt to forward to the dead leader and fail.
    val response = client.produce(followerUrl, timeoutTopic,
      Seq(ProduceRecord(
        partition = Some(partition),
        value = Some(StringValue("timeout-test-record"))
      )),
      acks = "all",
      timeoutMs = 2000)

    // Should get a server error (5xx) -- either 503 (Service Unavailable)
    // or 504 (Gateway Timeout) depending on how forwarding reports the failure
    assertTrue(response.status >= 500,
      s"Expected 5xx error when forwarding to dead broker, got " +
        s"${response.status}: ${response.body}")

    // Restart the broker for cleanup
    startBroker(leader)
    waitForAllPartitionLeaders(timeoutTopic, 1)
  }

  // -------------------------------------------------------------------
  // Scenario 6: Consume Fan-Out Across Multiple Brokers
  // -------------------------------------------------------------------

  @Test
  def testConsumeFanOutAcrossMultipleBrokers(): Unit = {
    // Produce one record per partition via each partition's leader
    for (p <- 0 until numPartitions) {
      val leader = findLeaderForPartition(testTopic, p)
      val leaderUrl = httpUrl(leader)
      val resp = client.produce(leaderUrl, testTopic,
        Seq(ProduceRecord(
          partition = Some(p),
          value = Some(StringValue(s"fanout-partition-$p"))
        )),
        acks = "all")
      assertEquals(200, resp.status,
        s"Seed produce for partition $p should succeed. Body: ${resp.body}")
    }

    // Fetch all partitions from a single broker.
    // Some partitions will be local, others remote -- requiring fan-out.
    val targetBroker = 0
    val targetUrl = httpUrl(targetBroker)

    val fetchSpecs = (0 until numPartitions).map { p =>
      FetchPartitionSpec(partition = p, offset = 0)
    }

    val fetchResp = client.consume(targetUrl, testTopic, fetchSpecs, maxWaitMs = 5000)

    assertEquals(200, fetchResp.status,
      s"Fan-out fetch should succeed. Body: ${fetchResp.body}")

    val partitions = fetchResp.body.get("partitions")
    assertNotNull(partitions, "Response must contain partitions array")
    assertEquals(numPartitions, partitions.size(),
      "Should get results for all partitions, including remote ones")

    // Build a set of returned partition IDs to avoid assuming ordering
    val returnedPartitions = (0 until partitions.size()).map { i =>
      partitions.get(i).get("partition").asInt()
    }.toSet

    for (p <- 0 until numPartitions) {
      assertTrue(returnedPartitions.contains(p),
        s"Partition $p should be present in fan-out response")
    }

    for (i <- 0 until partitions.size()) {
      val records = partitions.get(i).get("records")
      val partId = partitions.get(i).get("partition").asInt()
      assertTrue(records.size() > 0,
        s"Partition $partId should have records from fan-out fetch")
    }
  }

  // -------------------------------------------------------------------
  // Helper Methods
  // -------------------------------------------------------------------

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
   * Find a broker that is NOT the leader for the given topic-partition.
   * Returns a broker ID that differs from the leader.
   */
  private def findNonLeaderBroker(topic: String, partition: Int): Int = {
    val leader = findLeaderForPartition(topic, partition)
    val nonLeader = (0 until brokerCount).find(_ != leader)
    nonLeader.getOrElse(
      fail(s"Could not find a non-leader broker for $topic-$partition (leader=$leader)"))
  }

  /**
   * Wait until all partitions of a topic have elected leaders.
   * Uses AdminClient.describeTopics() to poll until all partitions
   * report a non-negative leader ID.
   */
  private def waitForAllPartitionLeaders(topic: String, numPartitions: Int): Unit = {
    for (p <- 0 until numPartitions) {
      TestUtils.waitUntilLeaderIsElectedOrChangedWithAdmin(
        adminClient, topic, p, timeoutMs = 30000L)
    }
  }
}
