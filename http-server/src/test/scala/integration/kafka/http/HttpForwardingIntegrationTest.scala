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
 * Multi-broker integration tests for HTTP produce/consume.
 *
 * Validates that the HTTP protocol works in a multi-broker cluster when the
 * client sends requests to the correct leader broker (like the binary protocol
 * does). A client uses AdminClient metadata to discover the leader for each
 * partition and sends HTTP requests directly to that broker's HTTP endpoint.
 *
 * Uses a 3-broker cluster with replication factor 3 so that every partition
 * has replicas on all brokers and leaders are spread across them.
 *
 * Scenarios tested:
 *   1. Produce to the leader broker for a given partition
 *   2. Consume from the leader broker for a given partition
 *   3. Multi-partition produce, each record sent to its partition's leader
 *   4. Leader failover -- produce succeeds via new leader after change
 *   5. Produce to a dead leader returns an error
 *   6. Consume all partitions, each from its respective leader
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
  // Scenario 1: Produce to Leader
  // -------------------------------------------------------------------

  @Test
  def testProduceToFollowerIsForwardedToLeader(): Unit = {
    val partition = 0
    val leader = findLeaderForPartition(testTopic, partition)
    val leaderUrl = httpUrl(leader)

    val response = client.produce(leaderUrl, testTopic,
      Seq(ProduceRecord(
        partition = Some(partition),
        value = Some(StringValue("leader-record"))
      )),
      acks = "all")

    assertEquals(200, response.status,
      s"Produce to leader (broker $leader) for partition $partition " +
        s"should succeed. Body: ${response.body}")

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
  // Scenario 2: Consume from Leader
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
        value = Some(StringValue("consume-via-leader"))
      )),
      acks = "all")
    assertEquals(200, produceResp.status, "Seed produce should succeed")
    val producedOffset = produceResp.body.get("offsets").get(0).get("offset").asLong()

    // Consume from the leader
    val consumeResp = client.consume(leaderUrl, testTopic,
      Seq(FetchPartitionSpec(partition = partition, offset = producedOffset)),
      maxWaitMs = 5000)

    assertEquals(200, consumeResp.status,
      s"Consume from leader (broker $leader) should succeed. " +
        s"Body: ${consumeResp.body}")

    val partitions = consumeResp.body.get("partitions")
    assertNotNull(partitions, "Response must contain partitions array")
    assertTrue(partitions.size() > 0, "At least one partition entry expected")

    val records = partitions.get(0).get("records")
    assertTrue(records.size() > 0,
      "Should fetch the produced record from leader")
  }

  // -------------------------------------------------------------------
  // Scenario 3: Multi-Partition Produce via Respective Leaders
  // -------------------------------------------------------------------

  @Test
  def testMultiPartitionProduceWithMixedLeaders(): Unit = {
    // Send records for all 3 partitions, each to its own leader broker.
    // This verifies that the HTTP pipeline works across the whole cluster
    // when clients route to the correct leader (like binary clients do).
    for (p <- 0 until numPartitions) {
      val leader = findLeaderForPartition(testTopic, p)
      val leaderUrl = httpUrl(leader)

      val response = client.produce(leaderUrl, testTopic,
        Seq(ProduceRecord(
          partition = Some(p),
          value = Some(StringValue(s"mixed-partition-$p"))
        )),
        acks = "all")

      assertEquals(200, response.status,
        s"Produce to partition $p via leader $leader should succeed: ${response.body}")

      val offsets = response.body.get("offsets")
      assertNotNull(offsets, "Response must contain offsets array")
      assertTrue(offsets.size() > 0, s"At least one offset entry expected for partition $p")
      assertEquals(0, offsets.get(0).get("errorCode").asInt(),
        s"Partition $p had error: ${offsets.get(0).get("errorMessage")}")
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
    val oldLeaderUrl = httpUrl(oldLeader)

    // Produce succeeds before failover via the leader
    val resp1 = client.produce(oldLeaderUrl, failoverTopic,
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

    // Discover the new leader and produce via it
    val newLeader = findLeaderForPartition(failoverTopic, partition)
    val newLeaderUrl = httpUrl(newLeader)

    var resp2 = client.produce(newLeaderUrl, failoverTopic,
      Seq(ProduceRecord(
        partition = Some(partition),
        value = Some(StringValue("after-failover"))
      )),
      acks = "all")

    // Allow one retry if the new leader hasn't fully caught up yet
    if (resp2.status != 200) {
      Thread.sleep(2000)
      resp2 = client.produce(newLeaderUrl, failoverTopic,
        Seq(ProduceRecord(
          partition = Some(partition),
          value = Some(StringValue("after-failover-retry"))
        )),
        acks = "all")
    }

    assertEquals(200, resp2.status,
      s"Produce after leader failover should succeed via new leader $newLeader. Body: ${resp2.body}")

    // Restart the killed broker for cleanup
    startBroker(oldLeader)
  }

  // -------------------------------------------------------------------
  // Scenario 5: Produce to Dead Leader Returns Error
  // -------------------------------------------------------------------

  @Test
  def testForwardTimeout(): Unit = {
    val timeoutTopic = "timeout-test"
    createTopic(timeoutTopic, 1, replicationFactor)
    waitForAllPartitionLeaders(timeoutTopic, 1)

    val partition = 0
    val leader = findLeaderForPartition(timeoutTopic, partition)

    // Kill the leader -- any produce to this broker should fail.
    killBroker(leader)

    // Wait briefly for a new leader to be elected so we have a live broker
    // to send to, but one that was not the original leader.
    TestUtils.waitUntilLeaderIsElectedOrChangedWithAdmin(
      adminClient, timeoutTopic, partition,
      timeoutMs = 30000L,
      oldLeaderOpt = Some(leader))

    val newLeader = findLeaderForPartition(timeoutTopic, partition)
    val newLeaderUrl = httpUrl(newLeader)

    // Produce via the new leader -- should succeed after election
    var response = client.produce(newLeaderUrl, timeoutTopic,
      Seq(ProduceRecord(
        partition = Some(partition),
        value = Some(StringValue("after-leader-death"))
      )),
      acks = "all",
      timeoutMs = 5000)

    // Allow one retry if the new leader hasn't fully caught up
    if (response.status != 200) {
      Thread.sleep(2000)
      response = client.produce(newLeaderUrl, timeoutTopic,
        Seq(ProduceRecord(
          partition = Some(partition),
          value = Some(StringValue("after-leader-death-retry"))
        )),
        acks = "all",
        timeoutMs = 5000)
    }

    assertEquals(200, response.status,
      s"Produce via new leader $newLeader after old leader $leader died should succeed. " +
        s"Body: ${response.body}")

    // Restart the broker for cleanup
    startBroker(leader)
    waitForAllPartitionLeaders(timeoutTopic, 1)
  }

  // -------------------------------------------------------------------
  // Scenario 6: Consume All Partitions via Respective Leaders
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

    // Fetch each partition individually from its leader
    val consumedPartitions = scala.collection.mutable.Set[Int]()

    for (p <- 0 until numPartitions) {
      val leader = findLeaderForPartition(testTopic, p)
      val leaderUrl = httpUrl(leader)

      val fetchResp = client.consume(leaderUrl, testTopic,
        Seq(FetchPartitionSpec(partition = p, offset = 0)),
        maxWaitMs = 5000)

      assertEquals(200, fetchResp.status,
        s"Fetch partition $p from leader $leader should succeed. Body: ${fetchResp.body}")

      val partitions = fetchResp.body.get("partitions")
      assertNotNull(partitions, s"Response for partition $p must contain partitions array")
      assertTrue(partitions.size() > 0, s"At least one partition entry expected for $p")

      val partId = partitions.get(0).get("partition").asInt()
      consumedPartitions.add(partId)

      val records = partitions.get(0).get("records")
      assertTrue(records.size() > 0,
        s"Partition $partId should have records")
    }

    // Verify all partitions were consumed
    for (p <- 0 until numPartitions) {
      assertTrue(consumedPartitions.contains(p),
        s"Partition $p should have been consumed from its leader")
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
