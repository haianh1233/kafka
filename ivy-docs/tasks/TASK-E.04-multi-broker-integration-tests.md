# TASK-E.04: Multi-Broker Forwarding Integration Tests

## Prerequisites

| Task | What it provides |
|------|-----------------|
| TASK-D.03 | `ProduceForwardManager` / `ProduceForwardThread` -- produce forwarding to remote leaders |
| TASK-D.04 | `FetchForwardManager` / `FetchForwardThread` -- fetch forwarding to remote leaders |
| TASK-C.03 | `HttpIntegrationTestHarness` -- test infrastructure for HTTP integration tests |

All three must be merged and passing CI before this task begins.

---

## Context

The HTTP protocol's core value proposition is that a client can connect to **any broker** and
produce/consume for **any partition** -- the broker transparently forwards to the correct
leader. This task validates that forwarding works correctly across a multi-broker cluster.

All prior integration tests use a single broker (where every partition is local). This task
extends `HttpIntegrationTestHarness` with `brokerCount = 3` and exercises:

1. Produce to a follower broker -- request is forwarded to the leader
2. Consume from a follower broker -- fetch is forwarded to the leader
3. Leader failover -- produce/consume continues after leader change
4. Mixed local + remote partitions in a single request
5. Forward timeout -- remote broker is unresponsive

These tests validate the end-to-end forwarding path including `ProduceForwardManager`,
`FetchForwardManager`, metadata cache lookups, and the `CompletableFuture` merge logic.

---

## Specification

### Test Scenarios

#### Scenario 1: Produce to Follower, Forwarded to Leader
- 3 brokers, topic with 3 partitions (one leader per broker)
- Send HTTP produce to a broker that is NOT the leader for the target partition
- Verify: records appear at the expected offset on the leader
- Verify: HTTP response contains correct offsets

#### Scenario 2: Consume from Follower, Forwarded to Leader
- Produce records directly (via binary protocol or HTTP to leader)
- Send HTTP fetch to a broker that is NOT the leader for the partition
- Verify: response contains the expected records

#### Scenario 3: Multi-Partition Request with Mixed Leaders
- Topic with 3 partitions, leaders spread across 3 brokers
- Single HTTP produce targeting all 3 partitions, sent to broker 0
- Some partitions are local (broker 0 is leader), others are remote
- Verify: all offsets returned, no errors

#### Scenario 4: Leader Failover
- Produce successfully to partition P via broker B (forwarded to leader L)
- Shut down leader L
- Wait for new leader election
- Produce again to partition P via broker B
- Verify: produce succeeds after metadata refresh and re-forward

#### Scenario 5: Forward Timeout
- Configure short `http.internal.forwarding.timeout.ms` (e.g., 500ms)
- Introduce artificial delay on the target broker (or shut it down without triggering leader election)
- Send HTTP produce to a follower
- Verify: HTTP 504 or 503 with `REQUEST_TIMED_OUT` error

#### Scenario 6: Consume Fan-Out Across Multiple Brokers
- Topic with 3 partitions, each on a different broker
- Single HTTP fetch request for all 3 partitions, sent to broker 0
- Verify: response aggregates data from all 3 leaders

---

## Implementation Details

### Extending HttpIntegrationTestHarness

The existing `HttpIntegrationTestHarness` (from TASK-C.03) starts a single broker. Extend it
to support configurable broker count:

```scala
override def brokerCount: Int = 3
```

The harness must:
1. Start 3 broker instances, each with an HTTP listener on a different port
2. Create topics with `replicationFactor = 3` so each partition has replicas on all brokers
3. Provide helper methods to find the leader for a partition and to find a non-leader broker

### Finding Non-Leader Brokers

```scala
def findNonLeaderBroker(topic: String, partition: Int): Int = {
  val leader = findLeader(topic, partition)
  brokerIds.find(_ != leader).get
}

def httpUrlForBroker(brokerId: Int): String = {
  s"http://localhost:${httpPortForBroker(brokerId)}"
}
```

### Leader Failover Testing

Use `killBroker(brokerId)` to stop a broker, then wait for leader election:

```scala
def waitForLeaderChange(topic: String, partition: Int, oldLeader: Int, timeoutMs: Long): Int
```

---

## Skeleton Code

### HttpMultiBrokerIntegrationTest.scala

```scala
// http-server/src/test/scala/kafka/server/http/HttpMultiBrokerIntegrationTest.scala

package kafka.server.http

import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test, TestInstance}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Timeout

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.concurrent.TimeUnit

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Timeout(value = 120, unit = TimeUnit.SECONDS)
class HttpMultiBrokerIntegrationTest extends HttpIntegrationTestHarness {

  override def brokerCount: Int = 3

  private val httpClient = HttpClient.newBuilder()
    .connectTimeout(Duration.ofSeconds(10))
    .build()
  private val mapper = new ObjectMapper()
  private val topicName = "multi-broker-test"
  private val numPartitions = 3
  private val replicationFactor: Short = 3

  @BeforeAll
  def setUp(): Unit = {
    startCluster()
    createTopic(topicName, numPartitions, replicationFactor)
    // Wait for partition assignment to stabilize
    waitForPartitionLeaders(topicName, numPartitions)
  }

  @AfterAll
  def tearDown(): Unit = {
    stopCluster()
  }

  // --- Scenario 1: Produce to Follower ---

  @Test
  def testProduceToFollowerIsForwardedToLeader(): Unit = {
    val partition = 0
    val leader = findLeader(topicName, partition)
    val follower = findNonLeaderBroker(topicName, partition)
    val followerUrl = httpUrlForBroker(follower)

    val body = s"""{"records":[{"partition":$partition,"value":{"type":"STRING","data":"forwarded-record"}}],"acks":"all"}"""
    val request = HttpRequest.newBuilder()
      .uri(URI.create(s"$followerUrl/v1/topics/$topicName/records"))
      .header("Content-Type", "application/json")
      .POST(HttpRequest.BodyPublishers.ofString(body))
      .build()

    val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())

    assertEquals(200, response.statusCode(),
      s"Produce to follower (broker $follower) for partition $partition (leader=$leader) should succeed. Body: ${response.body()}")

    val json = mapper.readTree(response.body())
    val offsets = json.get("offsets")
    assertTrue(offsets.size() > 0)
    assertEquals(partition, offsets.get(0).get("partition").asInt())
    assertTrue(offsets.get(0).get("offset").asLong() >= 0)
    assertEquals(0, offsets.get(0).get("errorCode").asInt())
  }

  // --- Scenario 2: Consume from Follower ---

  @Test
  def testConsumeFromFollowerIsForwardedToLeader(): Unit = {
    val partition = 0
    // First produce a known record
    val leader = findLeader(topicName, partition)
    val leaderUrl = httpUrlForBroker(leader)

    val produceBody = s"""{"records":[{"partition":$partition,"value":{"type":"STRING","data":"consume-test-record"}}],"acks":"all"}"""
    val produceReq = HttpRequest.newBuilder()
      .uri(URI.create(s"$leaderUrl/v1/topics/$topicName/records"))
      .header("Content-Type", "application/json")
      .POST(HttpRequest.BodyPublishers.ofString(produceBody))
      .build()
    val produceResp = httpClient.send(produceReq, HttpResponse.BodyHandlers.ofString())
    assertEquals(200, produceResp.statusCode())
    val producedOffset = mapper.readTree(produceResp.body())
      .get("offsets").get(0).get("offset").asLong()

    // Now consume from a follower
    val follower = findNonLeaderBroker(topicName, partition)
    val followerUrl = httpUrlForBroker(follower)

    val fetchBody = s"""{"partitions":[{"partition":$partition,"offset":$producedOffset}],"maxWaitMs":5000,"minBytes":1}"""
    val fetchReq = HttpRequest.newBuilder()
      .uri(URI.create(s"$followerUrl/v1/topics/$topicName/records:fetch"))
      .header("Content-Type", "application/json")
      .POST(HttpRequest.BodyPublishers.ofString(fetchBody))
      .build()

    val fetchResp = httpClient.send(fetchReq, HttpResponse.BodyHandlers.ofString())
    assertEquals(200, fetchResp.statusCode())

    val fetchJson = mapper.readTree(fetchResp.body())
    val partitions = fetchJson.get("partitions")
    assertTrue(partitions.size() > 0)
    val records = partitions.get(0).get("records")
    assertTrue(records.size() > 0, "Should fetch the produced record via follower forwarding")
  }

  // --- Scenario 3: Mixed Local + Remote Partitions ---

  @Test
  def testMultiPartitionProduceWithMixedLeaders(): Unit = {
    // Send records for all 3 partitions to broker 0
    // Some partitions will be local, others remote
    val broker0Url = httpUrlForBroker(0)

    val records = (0 until numPartitions).map { p =>
      s"""{"partition":$p,"value":{"type":"STRING","data":"partition-$p-record"}}"""
    }.mkString(",")
    val body = s"""{"records":[$records],"acks":"all"}"""

    val request = HttpRequest.newBuilder()
      .uri(URI.create(s"$broker0Url/v1/topics/$topicName/records"))
      .header("Content-Type", "application/json")
      .POST(HttpRequest.BodyPublishers.ofString(body))
      .build()

    val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())

    // Should be 200 (all succeed) or 207 (partial -- but we expect all to succeed)
    assertTrue(response.statusCode() == 200 || response.statusCode() == 207,
      s"Expected 200 or 207, got ${response.statusCode()}: ${response.body()}")

    val json = mapper.readTree(response.body())
    val offsets = json.get("offsets")
    assertEquals(numPartitions, offsets.size())

    for (i <- 0 until offsets.size()) {
      assertEquals(0, offsets.get(i).get("errorCode").asInt(),
        s"Partition ${offsets.get(i).get("partition")} had error: ${offsets.get(i).get("errorMessage")}")
    }
  }

  // --- Scenario 4: Leader Failover ---

  @Test
  def testProduceAfterLeaderFailover(): Unit = {
    val failoverTopic = "failover-test"
    createTopic(failoverTopic, 1, replicationFactor)
    waitForPartitionLeaders(failoverTopic, 1)

    val partition = 0
    val oldLeader = findLeader(failoverTopic, partition)
    val follower = findNonLeaderBroker(failoverTopic, partition)
    val followerUrl = httpUrlForBroker(follower)

    // Produce succeeds before failover
    val body1 = s"""{"records":[{"partition":0,"value":{"type":"STRING","data":"before-failover"}}],"acks":"all"}"""
    val req1 = HttpRequest.newBuilder()
      .uri(URI.create(s"$followerUrl/v1/topics/$failoverTopic/records"))
      .header("Content-Type", "application/json")
      .POST(HttpRequest.BodyPublishers.ofString(body1))
      .build()
    val resp1 = httpClient.send(req1, HttpResponse.BodyHandlers.ofString())
    assertEquals(200, resp1.statusCode())

    // Kill the leader
    killBroker(oldLeader)
    waitForLeaderChange(failoverTopic, partition, oldLeader, 30000)

    // Produce again -- should succeed after metadata refresh
    val body2 = s"""{"records":[{"partition":0,"value":{"type":"STRING","data":"after-failover"}}],"acks":"all"}"""
    val req2 = HttpRequest.newBuilder()
      .uri(URI.create(s"$followerUrl/v1/topics/$failoverTopic/records"))
      .header("Content-Type", "application/json")
      .POST(HttpRequest.BodyPublishers.ofString(body2))
      .build()

    // May need a retry if metadata hasn't refreshed yet
    var resp2 = httpClient.send(req2, HttpResponse.BodyHandlers.ofString())
    if (resp2.statusCode() == 503) {
      Thread.sleep(2000)  // wait for metadata refresh
      resp2 = httpClient.send(req2, HttpResponse.BodyHandlers.ofString())
    }
    assertEquals(200, resp2.statusCode(),
      s"Produce after failover should succeed. Body: ${resp2.body()}")

    // Restart killed broker for cleanup
    restartBroker(oldLeader)
  }

  // --- Scenario 5: Forward Timeout ---

  @Test
  def testForwardTimeout(): Unit = {
    // This test requires a way to simulate a slow/unresponsive broker.
    // Option A: configure very short timeout and shut down the leader without election
    // Option B: use a custom test hook to delay responses

    // For now, test the basic contract: produce to a partition whose leader is down
    // should return 503/504 within the timeout
    val timeoutTopic = "timeout-test"
    createTopic(timeoutTopic, 1, replicationFactor)
    waitForPartitionLeaders(timeoutTopic, 1)

    val partition = 0
    val leader = findLeader(timeoutTopic, partition)
    val follower = findNonLeaderBroker(timeoutTopic, partition)
    val followerUrl = httpUrlForBroker(follower)

    // Kill leader -- new leader should eventually be elected, but there's a window
    killBroker(leader)

    // Immediately try to produce -- forward should fail
    val body = s"""{"records":[{"partition":0,"value":{"type":"STRING","data":"timeout-test"}}],"acks":"all","timeoutMs":2000}"""
    val request = HttpRequest.newBuilder()
      .uri(URI.create(s"$followerUrl/v1/topics/$timeoutTopic/records"))
      .header("Content-Type", "application/json")
      .POST(HttpRequest.BodyPublishers.ofString(body))
      .timeout(Duration.ofSeconds(10))
      .build()

    val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())

    // Should get an error (503 or 504)
    assertTrue(response.statusCode() >= 500,
      s"Expected 5xx error, got ${response.statusCode()}: ${response.body()}")

    // Restart for cleanup
    restartBroker(leader)
    waitForPartitionLeaders(timeoutTopic, 1)
  }

  // --- Scenario 6: Consume Fan-Out ---

  @Test
  def testConsumeFanOutAcrossMultipleBrokers(): Unit = {
    // Produce one record per partition (to different leaders)
    for (p <- 0 until numPartitions) {
      val leader = findLeader(topicName, p)
      val leaderUrl = httpUrlForBroker(leader)
      val body = s"""{"records":[{"partition":$p,"value":{"type":"STRING","data":"fanout-$p"}}],"acks":"all"}"""
      val req = HttpRequest.newBuilder()
        .uri(URI.create(s"$leaderUrl/v1/topics/$topicName/records"))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(body))
        .build()
      httpClient.send(req, HttpResponse.BodyHandlers.ofString())
    }

    // Fetch all partitions from broker 0
    val broker0Url = httpUrlForBroker(0)
    val fetchPartitions = (0 until numPartitions).map { p =>
      s"""{"partition":$p,"offset":0}"""
    }.mkString(",")
    val fetchBody = s"""{"partitions":[$fetchPartitions],"maxWaitMs":5000,"minBytes":1}"""

    val fetchReq = HttpRequest.newBuilder()
      .uri(URI.create(s"$broker0Url/v1/topics/$topicName/records:fetch"))
      .header("Content-Type", "application/json")
      .POST(HttpRequest.BodyPublishers.ofString(fetchBody))
      .build()

    val fetchResp = httpClient.send(fetchReq, HttpResponse.BodyHandlers.ofString())
    assertEquals(200, fetchResp.statusCode())

    val json = mapper.readTree(fetchResp.body())
    val partitions = json.get("partitions")
    assertEquals(numPartitions, partitions.size(),
      "Should get results for all partitions, including remote ones")

    for (i <- 0 until partitions.size()) {
      val records = partitions.get(i).get("records")
      assertTrue(records.size() > 0,
        s"Partition ${partitions.get(i).get("partition")} should have records")
    }
  }

  // --- Helper Methods ---

  /**
   * Find the leader broker ID for a given topic-partition.
   */
  protected def findLeader(topic: String, partition: Int): Int = {
    // Implementation uses AdminClient or MetadataCache
    ???
  }

  /**
   * Find a broker that is NOT the leader for the given topic-partition.
   */
  protected def findNonLeaderBroker(topic: String, partition: Int): Int = {
    val leader = findLeader(topic, partition)
    (0 until brokerCount).find(_ != leader).get
  }

  /**
   * Get the HTTP URL for a specific broker.
   */
  protected def httpUrlForBroker(brokerId: Int): String = {
    s"http://localhost:${httpPortForBroker(brokerId)}"
  }

  /**
   * Wait for all partitions of a topic to have elected leaders.
   */
  protected def waitForPartitionLeaders(topic: String, numPartitions: Int): Unit = {
    // Poll AdminClient.describeTopics() until all partitions have non-negative leader IDs
    ???
  }

  /**
   * Wait for the leader of a partition to change from oldLeader to a new leader.
   */
  protected def waitForLeaderChange(topic: String, partition: Int,
                                     oldLeader: Int, timeoutMs: Long): Int = {
    val deadline = System.currentTimeMillis() + timeoutMs
    while (System.currentTimeMillis() < deadline) {
      val currentLeader = findLeader(topic, partition)
      if (currentLeader >= 0 && currentLeader != oldLeader) return currentLeader
      Thread.sleep(200)
    }
    fail(s"Leader for $topic-$partition did not change from $oldLeader within ${timeoutMs}ms")
    -1 // unreachable
  }

  /**
   * Kill a broker by ID (stop its process).
   */
  protected def killBroker(brokerId: Int): Unit = {
    // Implementation shuts down the broker instance
    ???
  }

  /**
   * Restart a previously killed broker.
   */
  protected def restartBroker(brokerId: Int): Unit = {
    // Implementation restarts the broker instance
    ???
  }
}
```

---

## Tests

The entire task IS the test suite. The skeleton code above contains all test methods.

### Summary of Test Methods

| Test Method | Scenario | Key Assertions |
|---|---|---|
| `testProduceToFollowerIsForwardedToLeader` | Produce to follower | 200 status, correct offsets |
| `testConsumeFromFollowerIsForwardedToLeader` | Consume from follower | 200 status, records present |
| `testMultiPartitionProduceWithMixedLeaders` | Mixed local+remote | All partitions succeed |
| `testProduceAfterLeaderFailover` | Leader failover | Produce succeeds after election |
| `testForwardTimeout` | Forward timeout | 5xx error within timeout |
| `testConsumeFanOutAcrossMultipleBrokers` | Consume fan-out | All partitions have records |

---

## Rules

- All tests must use `brokerCount = 3` and `replicationFactor = 3`.
- Tests must not depend on specific broker-to-partition leader assignments -- use helper methods to discover leaders dynamically.
- Leader failover tests must restart killed brokers in `@AfterAll` or within the test to avoid affecting other tests.
- Forward timeout tests must use short timeouts (2s or less) to keep the test suite fast.
- Tests must not assume the order of partitions in the response body.
- Each test must be independently runnable (no ordering dependency between tests).
- Use `@Timeout(120, SECONDS)` on the class to prevent hung tests from blocking CI.

---

## Learning

_To be filled by the executing agent._

## Limitations

_To be filled by the executing agent._

## Field Notes

_To be filled by the executing agent._

---

## Acceptance Criteria

- [ ] Test suite starts a 3-broker cluster with HTTP listeners
- [ ] Produce to a follower broker is forwarded to the leader and returns correct offsets
- [ ] Consume from a follower broker returns records via forwarding
- [ ] Multi-partition request with mixed local/remote leaders returns all offsets
- [ ] Produce succeeds after leader failover (with at most one retry)
- [ ] Forward timeout returns 5xx within the configured timeout
- [ ] Consume fan-out returns aggregated data from multiple leaders
- [ ] All tests pass in CI with `brokerCount = 3`
- [ ] No test depends on specific partition-leader assignments
- [ ] Tests clean up killed brokers

---

## File Manifest

| File | Status |
|------|--------|
| `http-server/src/test/scala/kafka/server/http/HttpMultiBrokerIntegrationTest.scala` | |
| `http-server/src/test/scala/kafka/server/http/HttpIntegrationTestHarness.scala` | |
