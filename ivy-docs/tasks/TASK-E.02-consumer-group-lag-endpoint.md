# TASK-E.02: Consumer Group Lag Endpoint

## Prerequisites

| Task | What it provides |
|------|-----------------|
| TASK-E.01 | Metadata/offsets endpoint pattern -- establishes the translation and serialization approach for non-produce/consume endpoints |

TASK-E.01 must be merged and passing CI before this task begins.

---

## Context

The HTTP protocol design (section 4.3.3) specifies a consumer group lag endpoint:

```
GET /v1/consumer-groups/{group}/lags
```

This endpoint requires **two concurrent API calls** through RequestChannel:
1. `OFFSET_FETCH` (ApiKey 9) -- returns committed offsets per partition for the group
2. `LIST_OFFSETS` (ApiKey 2) with `timestamp = -1` (LATEST) -- returns log-end offsets

Both are merged to compute `lag = max(0, logEndOffset - committedOffset)`.

Because the two queries are concurrent, they may reflect slightly different points in time.
Lag values are approximate. The serializer clamps lag to `max(0, ...)` to avoid negative
values from sampling skew.

Authorization: `DESCRIBE` on group + `READ` on each topic partition is enforced inside the
existing handlers.

---

## Specification

### Request

```
GET /v1/consumer-groups/checkout-consumer/lags
```

### Response (200 OK)

```json
{
  "group": "checkout-consumer",
  "totalLag": 1543,
  "partitions": [
    {
      "topic": "orders",
      "partition": 0,
      "committedOffset": 8500,
      "logEndOffset": 10043,
      "lag": 1543
    }
  ]
}
```

### Error Cases

| Condition | HTTP Status | Error |
|-----------|-------------|-------|
| Group not found | 404 | `GROUP_ID_NOT_FOUND` |
| Authorization failed | 403 | `GROUP_AUTHORIZATION_FAILED` |
| Internal error | 500 | varies |

---

## Implementation Details

### Two-Phase Request Pattern

This endpoint cannot be served by a single RequestChannel round-trip. It requires:

1. Send `OffsetFetchRequest` for the group through RequestChannel
2. When the OffsetFetch response arrives, extract the list of topic-partitions
3. Send `ListOffsetsRequest` for all those topic-partitions with `timestamp = LATEST`
4. Merge both results and compute lag

**Approach: Sequential Two-Phase via CompletableFuture**

Phase 1 (offset fetch) must complete before Phase 2 (list offsets) because we need to know
which topic-partitions the group has committed offsets for. We cannot use a single
`CompletableFuture.allOf()` because the second request depends on the first response.

Use `CompletableFuture.thenCompose()` to chain the two phases.

### Alternative: Concurrent Both Phases

If the caller knows the topics (via query param `?topic=orders`), both phases can run
concurrently. For the initial implementation, use the sequential approach. The concurrent
optimization can be added later if needed.

### Implementation Location

This endpoint requires custom orchestration logic that spans two RequestChannel round-trips.
The orchestration lives in a new handler class `ConsumerGroupLagHandler` in the http-server
module. The handler:
1. Builds and enqueues the OffsetFetchRequest
2. Receives the OffsetFetchResponse callback
3. Builds and enqueues the ListOffsetsRequest
4. Receives the ListOffsetsResponse callback
5. Merges results and writes the HTTP response

---

## Skeleton Code

### HttpRouter.scala -- new route

```scala
// Add to HttpRouter.route() match cases:

case class ConsumerGroupLagRoute(group: String) extends RouteResult

// In route():
case (HttpMethod.GET, List("v1", "consumer-groups", rawGroup, "lags")) =>
  val group = validateGroupId(rawGroup)
  ConsumerGroupLagRoute(group)

/**
 * Validate group ID from URL path segment.
 * Group IDs have fewer restrictions than topic names but must be non-empty.
 */
def validateGroupId(raw: String): String = {
  val group = URLDecoder.decode(raw, StandardCharsets.UTF_8)
  if (group.isEmpty)
    throw new InvalidRequestException("Group ID must not be empty")
  if (group.length > 255)
    throw new InvalidRequestException("Group ID exceeds maximum length of 255")
  group
}
```

### ConsumerGroupLagHandler.scala

```scala
// http-server/src/main/scala/kafka/server/http/ConsumerGroupLagHandler.scala

package kafka.server.http

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.http._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.message.{ListOffsetsRequestData, OffsetFetchRequestData}
import org.apache.kafka.common.message.ListOffsetsRequestData.{ListOffsetsPartition, ListOffsetsTopic}
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests._
import org.apache.kafka.server.http.HttpProcessor

import java.util.concurrent.CompletableFuture
import java.util.{Collections, ArrayList => JArrayList}
import scala.jdk.CollectionConverters._

/**
 * Handles GET /v1/consumer-groups/{group}/lags.
 *
 * Orchestrates two sequential RequestChannel round-trips:
 * 1. OFFSET_FETCH -> committed offsets for the group
 * 2. LIST_OFFSETS -> log-end offsets for all partitions found in step 1
 *
 * Merges results: lag = max(0, logEndOffset - committedOffset)
 */
class ConsumerGroupLagHandler(
  requestChannel: RequestChannel,
  httpProcessor: HttpProcessor,
  config: KafkaConfig
) {

  private val MAPPER = new ObjectMapper()

  /**
   * Entry point: initiate the lag computation for the given group.
   *
   * @param group consumer group ID
   * @param ctx   Netty channel context for writing the response
   * @param requestContext base RequestContext (principal, listener, etc.)
   */
  def handle(group: String, ctx: ChannelHandlerContext, requestContext: RequestContext): Unit = {
    // Phase 1: Fetch committed offsets
    val offsetFetchFuture = sendOffsetFetchRequest(group, requestContext)

    offsetFetchFuture.thenCompose { offsetFetchResult =>
      // Phase 2: Fetch log-end offsets for all partitions in the group
      val topicPartitions = offsetFetchResult.map { case (tp, _) => tp }.toSeq
      if (topicPartitions.isEmpty) {
        // No committed offsets -- return empty result immediately
        CompletableFuture.completedFuture(
          buildLagResponse(group, offsetFetchResult, Map.empty))
      } else {
        val listOffsetsFuture = sendListOffsetsRequest(topicPartitions, requestContext)
        listOffsetsFuture.thenApply { listOffsetsResult =>
          buildLagResponse(group, offsetFetchResult, listOffsetsResult)
        }
      }
    }.whenComplete { (response, ex) =>
      if (ex != null) {
        writeErrorResponse(ctx, ex)
      } else {
        writeResponse(ctx, response)
      }
    }
  }

  /**
   * Build OffsetFetchRequest and enqueue to RequestChannel.
   * Returns a future that resolves with committed offsets keyed by TopicPartition.
   */
  private def sendOffsetFetchRequest(
    group: String,
    requestContext: RequestContext
  ): CompletableFuture[Map[TopicPartition, Long]] = {
    val future = new CompletableFuture[Map[TopicPartition, Long]]()

    val data = new OffsetFetchRequestData()
    val groupData = new OffsetFetchRequestData.OffsetFetchRequestGroup()
      .setGroupId(group)
      .setTopics(null)  // null = all topics for this group
    data.setGroups(Collections.singletonList(groupData))

    val builder = new OffsetFetchRequest.Builder(data, false)
    // ... enqueue to RequestChannel, complete future in callback ...

    future
  }

  /**
   * Build ListOffsetsRequest for LATEST timestamp across all given partitions.
   * Returns a future that resolves with log-end offsets keyed by TopicPartition.
   */
  private def sendListOffsetsRequest(
    topicPartitions: Seq[TopicPartition],
    requestContext: RequestContext
  ): CompletableFuture[Map[TopicPartition, Long]] = {
    val future = new CompletableFuture[Map[TopicPartition, Long]]()

    // Group partitions by topic
    val byTopic = topicPartitions.groupBy(_.topic())
    val topics = new JArrayList[ListOffsetsTopic]()
    byTopic.foreach { case (topicName, partitions) =>
      val topicData = new ListOffsetsTopic().setName(topicName)
      val partitionList = new JArrayList[ListOffsetsPartition]()
      partitions.foreach { tp =>
        partitionList.add(new ListOffsetsPartition()
          .setPartitionIndex(tp.partition())
          .setTimestamp(ListOffsetsRequest.LATEST_TIMESTAMP)
          .setCurrentLeaderEpoch(-1))
      }
      topicData.setPartitions(partitionList)
      topics.add(topicData)
    }

    val data = new ListOffsetsRequestData().setTopics(topics)
    val builder = ListOffsetsRequest.Builder.forConsumer(
      false, IsolationLevel.READ_UNCOMMITTED, false
    ).setTargetTimes(data)

    // ... enqueue to RequestChannel, complete future in callback ...

    future
  }

  /**
   * Merge committed offsets and log-end offsets into the lag response.
   * lag = max(0, logEndOffset - committedOffset)
   */
  private def buildLagResponse(
    group: String,
    committedOffsets: Map[TopicPartition, Long],
    logEndOffsets: Map[TopicPartition, Long]
  ): FullHttpResponse = {
    val root = MAPPER.createObjectNode()
    root.put("group", group)

    val partitionsArray = root.putArray("partitions")
    var totalLag = 0L

    committedOffsets.toSeq.sortBy(e => (e._1.topic(), e._1.partition())).foreach {
      case (tp, committedOffset) =>
        val logEndOffset = logEndOffsets.getOrElse(tp, -1L)
        val lag = if (logEndOffset >= 0 && committedOffset >= 0)
          Math.max(0L, logEndOffset - committedOffset)
        else
          0L

        totalLag += lag

        val partNode = partitionsArray.addObject()
        partNode.put("topic", tp.topic())
        partNode.put("partition", tp.partition())
        partNode.put("committedOffset", committedOffset)
        partNode.put("logEndOffset", logEndOffset)
        partNode.put("lag", lag)
    }

    root.put("totalLag", totalLag)
    HttpResponseSerializer.buildJsonResponse(HttpResponseStatus.OK, root)
  }

  /**
   * Write an error response for exceptions during lag computation.
   */
  private def writeErrorResponse(ctx: ChannelHandlerContext, ex: Throwable): Unit = {
    val (status, errorCode, detail) = ex.getCause match {
      case e: org.apache.kafka.common.errors.GroupIdNotFoundException =>
        (HttpResponseStatus.NOT_FOUND, 69, "GROUP_ID_NOT_FOUND")
      case e: org.apache.kafka.common.errors.GroupAuthorizationException =>
        (HttpResponseStatus.FORBIDDEN, 30, "GROUP_AUTHORIZATION_FAILED")
      case _ =>
        (HttpResponseStatus.INTERNAL_SERVER_ERROR, -1, "INTERNAL_ERROR")
    }
    val response = HttpResponseSerializer.buildErrorResponse(status, errorCode, detail, ex.getMessage)
    ctx.writeAndFlush(response)
  }

  /**
   * Write a successful response to the Netty channel.
   */
  private def writeResponse(ctx: ChannelHandlerContext, response: FullHttpResponse): Unit = {
    if (ctx.channel().isActive) {
      ctx.writeAndFlush(response)
    }
  }
}
```

### HttpRequestHandler.scala -- dispatch addition

```scala
// In HttpRequestHandler.channelRead0() route match:

case ConsumerGroupLagRoute(group) =>
  consumerGroupLagHandler.handle(group, ctx, buildRequestContext(ctx))
```

---

## Tests

### Unit Tests

```scala
// http-server/src/test/scala/kafka/server/http/ConsumerGroupLagHandlerTest.scala

package kafka.server.http

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions._
import org.apache.kafka.common.TopicPartition

class ConsumerGroupLagHandlerTest {

  @Test
  def testLagCalculationNormal(): Unit = {
    val committed = Map(
      new TopicPartition("orders", 0) -> 8500L,
      new TopicPartition("orders", 1) -> 5000L
    )
    val logEnd = Map(
      new TopicPartition("orders", 0) -> 10043L,
      new TopicPartition("orders", 1) -> 5500L
    )
    // lag(P0) = 10043 - 8500 = 1543
    // lag(P1) = 5500 - 5000 = 500
    // totalLag = 2043
    val handler = new ConsumerGroupLagHandler(null, null, null)
    val response = handler.buildLagResponse("test-group", committed, logEnd)
    // Parse JSON and verify
    val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
    val json = mapper.readTree(
      io.netty.buffer.ByteBufUtil.getBytes(response.content()))
    assertEquals("test-group", json.get("group").asText())
    assertEquals(2043L, json.get("totalLag").asLong())
    assertEquals(2, json.get("partitions").size())
  }

  @Test
  def testLagClampedToZero(): Unit = {
    // Due to concurrent sampling, committed might briefly exceed logEnd
    val committed = Map(new TopicPartition("orders", 0) -> 10050L)
    val logEnd = Map(new TopicPartition("orders", 0) -> 10043L)
    // lag = max(0, 10043 - 10050) = 0

    val handler = new ConsumerGroupLagHandler(null, null, null)
    val response = handler.buildLagResponse("test-group", committed, logEnd)
    val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
    val json = mapper.readTree(
      io.netty.buffer.ByteBufUtil.getBytes(response.content()))
    assertEquals(0L, json.get("totalLag").asLong())
    assertEquals(0L, json.get("partitions").get(0).get("lag").asLong())
  }

  @Test
  def testLagMissingLogEndOffset(): Unit = {
    // logEndOffset missing for a partition (e.g., authorization failed on LIST_OFFSETS)
    val committed = Map(new TopicPartition("orders", 0) -> 8500L)
    val logEnd = Map.empty[TopicPartition, Long]  // no log-end data

    val handler = new ConsumerGroupLagHandler(null, null, null)
    val response = handler.buildLagResponse("test-group", committed, logEnd)
    val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
    val json = mapper.readTree(
      io.netty.buffer.ByteBufUtil.getBytes(response.content()))
    // lag should be 0 when logEnd is unknown
    assertEquals(0L, json.get("partitions").get(0).get("lag").asLong())
    assertEquals(-1L, json.get("partitions").get(0).get("logEndOffset").asLong())
  }

  @Test
  def testEmptyGroupHasZeroLag(): Unit = {
    val committed = Map.empty[TopicPartition, Long]
    val logEnd = Map.empty[TopicPartition, Long]

    val handler = new ConsumerGroupLagHandler(null, null, null)
    val response = handler.buildLagResponse("empty-group", committed, logEnd)
    val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
    val json = mapper.readTree(
      io.netty.buffer.ByteBufUtil.getBytes(response.content()))
    assertEquals(0L, json.get("totalLag").asLong())
    assertEquals(0, json.get("partitions").size())
  }

  @Test
  def testPartitionsSortedByTopicThenPartition(): Unit = {
    val committed = Map(
      new TopicPartition("orders", 1) -> 100L,
      new TopicPartition("events", 0) -> 200L,
      new TopicPartition("orders", 0) -> 300L
    )
    val logEnd = Map(
      new TopicPartition("orders", 1) -> 150L,
      new TopicPartition("events", 0) -> 250L,
      new TopicPartition("orders", 0) -> 350L
    )
    val handler = new ConsumerGroupLagHandler(null, null, null)
    val response = handler.buildLagResponse("test-group", committed, logEnd)
    val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
    val json = mapper.readTree(
      io.netty.buffer.ByteBufUtil.getBytes(response.content()))

    val partitions = json.get("partitions")
    // Should be sorted: events/0, orders/0, orders/1
    assertEquals("events", partitions.get(0).get("topic").asText())
    assertEquals(0, partitions.get(0).get("partition").asInt())
    assertEquals("orders", partitions.get(1).get("topic").asText())
    assertEquals(0, partitions.get(1).get("partition").asInt())
    assertEquals("orders", partitions.get(2).get("topic").asText())
    assertEquals(1, partitions.get(2).get("partition").asInt())
  }
}
```

### Router Tests

```scala
// http-server/src/test/scala/kafka/server/http/HttpRouterLagTest.scala

package kafka.server.http

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions._
import io.netty.handler.codec.http.{DefaultFullHttpRequest, HttpMethod, HttpVersion}

class HttpRouterLagTest {

  @Test
  def testRouteConsumerGroupLag(): Unit = {
    val req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET,
      "/v1/consumer-groups/checkout-consumer/lags")
    val result = HttpRouter.route(req)
    result match {
      case HttpRouter.ConsumerGroupLagRoute(group) =>
        assertEquals("checkout-consumer", group)
      case _ => fail(s"Expected ConsumerGroupLagRoute, got $result")
    }
  }

  @Test
  def testRouteConsumerGroupLagUrlEncoded(): Unit = {
    val req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET,
      "/v1/consumer-groups/my%20group/lags")
    val result = HttpRouter.route(req)
    result match {
      case HttpRouter.ConsumerGroupLagRoute(group) =>
        assertEquals("my group", group)
      case _ => fail(s"Expected ConsumerGroupLagRoute, got $result")
    }
  }

  @Test
  def testRouteConsumerGroupLagEmptyGroup(): Unit = {
    val req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET,
      "/v1/consumer-groups//lags")
    assertThrows(classOf[org.apache.kafka.common.errors.InvalidRequestException], () => {
      HttpRouter.route(req)
    })
  }
}
```

### Integration Test

```scala
// http-server/src/test/scala/kafka/server/http/HttpConsumerGroupLagIntegrationTest.scala

package kafka.server.http

import org.junit.jupiter.api.{AfterAll, BeforeAll, Test, TestInstance}
import org.junit.jupiter.api.Assertions._
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.net.URI
import com.fasterxml.jackson.databind.ObjectMapper

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class HttpConsumerGroupLagIntegrationTest extends HttpIntegrationTestHarness {

  private val httpClient = HttpClient.newHttpClient()
  private val mapper = new ObjectMapper()
  private val topicName = "lag-test-topic"
  private val groupId = "lag-test-group"

  @BeforeAll
  def setUp(): Unit = {
    startCluster()
    createTopic(topicName, 2, 1)
    // Produce some records to create offsets
    produceRecords(topicName, partition = 0, count = 100)
    produceRecords(topicName, partition = 1, count = 50)
    // Commit offsets for the group (simulate consumer)
    commitOffsets(groupId, Map(
      (topicName, 0) -> 80L,
      (topicName, 1) -> 30L
    ))
  }

  @AfterAll
  def tearDown(): Unit = {
    stopCluster()
  }

  @Test
  def testConsumerGroupLag(): Unit = {
    val request = HttpRequest.newBuilder()
      .uri(URI.create(s"$httpBaseUrl/v1/consumer-groups/$groupId/lags"))
      .GET().build()
    val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
    assertEquals(200, response.statusCode())

    val json = mapper.readTree(response.body())
    assertEquals(groupId, json.get("group").asText())
    assertTrue(json.get("totalLag").asLong() > 0)

    val partitions = json.get("partitions")
    assertEquals(2, partitions.size())
    // P0: logEnd=100, committed=80, lag=20
    // P1: logEnd=50, committed=30, lag=20
    // totalLag = 40
    assertEquals(40L, json.get("totalLag").asLong())
  }

  @Test
  def testConsumerGroupLagNonexistentGroup(): Unit = {
    val request = HttpRequest.newBuilder()
      .uri(URI.create(s"$httpBaseUrl/v1/consumer-groups/nonexistent-group-xyz/lags"))
      .GET().build()
    val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
    // Group not found -- either 200 with empty partitions or 404
    // The actual behavior depends on whether KafkaApis returns an empty response
    // or an error for unknown groups in OFFSET_FETCH
    assertTrue(response.statusCode() == 200 || response.statusCode() == 404)
  }

  @Test
  def testLagIsNonNegative(): Unit = {
    // Even with concurrent sampling, lag should never be negative in the response
    val request = HttpRequest.newBuilder()
      .uri(URI.create(s"$httpBaseUrl/v1/consumer-groups/$groupId/lags"))
      .GET().build()
    val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
    assertEquals(200, response.statusCode())

    val json = mapper.readTree(response.body())
    json.get("partitions").forEach { partition =>
      assertTrue(partition.get("lag").asLong() >= 0,
        s"Lag must be non-negative for ${partition.get("topic")}:${partition.get("partition")}")
    }
  }
}
```

---

## Rules

- Lag computation must use `max(0, logEndOffset - committedOffset)` -- never return negative lag.
- The two RequestChannel round-trips must be sequential (Phase 1 feeds Phase 2), not parallel, unless the caller provides topic filter hints.
- Authorization is enforced inside existing `KafkaApis.handleOffsetFetchRequest()` and `KafkaApis.handleListOffsetRequest()`. Do NOT add custom authorization checks.
- The response must include `totalLag` as the sum of all per-partition lag values.
- Partitions in the response must be sorted by topic name, then partition index.
- If logEndOffset is unavailable for a partition (e.g., topic deleted between the two calls), report `logEndOffset: -1` and `lag: 0`.
- The `group` field in the response must match the group ID from the request URL.
- Use `generationId = -1` in the OffsetFetchRequest (simple consumer mode).

---

## Learning

_To be filled by the executing agent._

## Limitations

_To be filled by the executing agent._

## Field Notes

_To be filled by the executing agent._

---

## Acceptance Criteria

- [ ] `GET /v1/consumer-groups/{group}/lags` returns 200 with lag data for all partitions the group has committed offsets for
- [ ] `totalLag` equals the sum of all per-partition lag values
- [ ] Per-partition lag is computed as `max(0, logEndOffset - committedOffset)`
- [ ] Lag is never negative in the response
- [ ] Partitions are sorted by topic name then partition index
- [ ] Nonexistent group returns either empty partitions (200) or 404
- [ ] Group ID is validated (non-empty, max 255 chars)
- [ ] Two RequestChannel round-trips are used (OFFSET_FETCH then LIST_OFFSETS)
- [ ] Authorization is enforced by existing KafkaApis handlers
- [ ] All unit tests pass
- [ ] Integration tests pass against a running broker with produced records and committed offsets

---

## File Manifest

| File | Status |
|------|--------|
| `http-server/src/main/scala/kafka/server/http/HttpRouter.scala` | |
| `http-server/src/main/scala/kafka/server/http/ConsumerGroupLagHandler.scala` | |
| `http-server/src/main/scala/kafka/network/HttpRequestHandler.scala` | |
| `http-server/src/test/scala/kafka/server/http/ConsumerGroupLagHandlerTest.scala` | |
| `http-server/src/test/scala/kafka/server/http/HttpRouterLagTest.scala` | |
| `http-server/src/test/scala/kafka/server/http/HttpConsumerGroupLagIntegrationTest.scala` | |
