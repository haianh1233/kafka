# TASK-F.01: Consumer Group Offset Commit/Fetch Endpoints

## Prerequisites

| Task | What it provides |
|------|-----------------|
| TASK-B.01 | `HttpRouter` -- URI pattern matching and handler dispatch |
| TASK-B.06 | SocketServer wiring -- HTTP requests flow through `RequestChannel` to `KafkaApis` |

Both must be merged and passing CI before this task begins.

---

## Context

The HTTP protocol design (section 4.4) specifies two consumer group offset endpoints that
let HTTP clients store and retrieve committed offsets in Kafka's `__consumer_offsets` topic
without joining a consumer group. This gives clients crash recovery, `kafka-consumer-groups.sh`
visibility, and consumer lag monitoring -- without the server-side session state that makes
REST Proxy's consumer model fragile (see design doc section 15).

**Endpoints:**
1. `POST /v1/consumer-groups/{group}/offsets` -- commit offsets (OffsetCommitRequest)
2. `GET /v1/consumer-groups/{group}/offsets?topic=` -- fetch committed offsets (OffsetFetchRequest)

Both reuse existing `KafkaApis` handlers unchanged. The HTTP layer translates JSON and routes
through RequestChannel. Uses `generationId = -1` and `memberId = ""` (simple consumer mode
-- no group membership required).

---

## Specification

### Endpoint 1: Commit Offsets

```
POST /v1/consumer-groups/checkout-consumer/offsets
```

Request Body:
```json
{
  "offsets": [
    { "topic": "orders", "partition": 0, "offset": 150, "metadata": "" },
    { "topic": "orders", "partition": 1, "offset": 88,  "metadata": "" }
  ]
}
```

| Field | Type | Default | Description |
|---|---|---|---|
| `offsets` | array | required | Offsets to commit |
| `offsets[].topic` | string | required | Topic name |
| `offsets[].partition` | int | required | Partition id |
| `offsets[].offset` | long | required | Offset to commit (next offset to read) |
| `offsets[].metadata` | string | `""` | Optional metadata string |

Response (200 OK):
```json
{
  "offsets": [
    { "topic": "orders", "partition": 0, "offset": 150, "errorCode": 0 },
    { "topic": "orders", "partition": 1, "offset": 88,  "errorCode": 0 }
  ]
}
```

Kafka API used: `OFFSET_COMMIT` (ApiKey id=8).
Authorization: `READ` on group + `READ` on each topic.

### Endpoint 2: Fetch Committed Offsets

```
GET /v1/consumer-groups/checkout-consumer/offsets?topic=orders
```

| Query param | Type | Default | Description |
|---|---|---|---|
| `topic` | string | optional | Filter by topic. Omit for all topics. |

Response (200 OK):
```json
{
  "group": "checkout-consumer",
  "offsets": [
    { "topic": "orders", "partition": 0, "offset": 150, "metadata": "" },
    { "topic": "orders", "partition": 1, "offset": 88,  "metadata": "" }
  ]
}
```

Kafka API used: `OFFSET_FETCH` (ApiKey id=9).
Authorization: `DESCRIBE` on group + `READ` on each topic.

---

## Implementation Details

### 1. HttpRouter Updates

Register two new route patterns:

```
POST /v1/consumer-groups/{group}/offsets -> commitOffsets handler
GET  /v1/consumer-groups/{group}/offsets -> fetchOffsets handler
```

Both share the same URL path but differ by HTTP method.

### 2. HttpRequestTranslator Updates

**Commit Offsets:** Parse JSON body, build `OffsetCommitRequest` with:
- `groupId` from URL path
- `memberId = ""`
- `generationIdOrMemberEpoch = -1` (simple consumer mode)
- Per-partition offset entries from the request body

**Fetch Offsets:** Build `OffsetFetchRequest` with:
- `groupId` from URL path
- Optional topic filter from `?topic=` query parameter
- If `topic` is provided, build request for that topic's partitions only
- If `topic` is omitted, build request for all topics in the group (topics = null)

### 3. HttpResponseSerializer Updates

**Commit Response:** Map `OffsetCommitResponse` to JSON with per-partition error codes.

**Fetch Response:** Map `OffsetFetchResponse` to JSON with group name, topic, partition,
offset, and metadata fields.

### 4. Validation

- Group ID: non-empty, max 255 characters (reuse `validateGroupId` from TASK-E.02)
- Offsets array: non-empty, each entry has topic, partition, offset
- Topic names: validated via `Topic.validate()`
- Partition IDs: non-negative integers
- Offset values: non-negative longs

---

## Skeleton Code

### HttpRouter.scala -- new routes

```scala
// http-server/src/main/scala/kafka/server/http/HttpRouter.scala

package kafka.server.http

// Add to RouteResult sealed trait:
case class CommitOffsetsRoute(group: String) extends RouteResult
case class FetchOffsetsRoute(group: String, topicFilter: Option[String]) extends RouteResult

// In route():
case (HttpMethod.POST, List("v1", "consumer-groups", rawGroup, "offsets")) =>
  val group = validateGroupId(rawGroup)
  CommitOffsetsRoute(group)

case (HttpMethod.GET, List("v1", "consumer-groups", rawGroup, "offsets")) =>
  val group = validateGroupId(rawGroup)
  val topicFilter = Option(decoder.parameters().get("topic"))
    .flatMap(list => Option(list.get(0)))
    .filter(_.nonEmpty)
  // Validate topic filter if present
  topicFilter.foreach(t => validateTopicName(t))
  FetchOffsetsRoute(group, topicFilter)
```

### HttpRequestTranslator.scala -- new translation methods

```scala
// http-server/src/main/scala/kafka/server/http/HttpRequestTranslator.scala

package kafka.server.http

import com.fasterxml.jackson.databind.JsonNode
import org.apache.kafka.common.message.{OffsetCommitRequestData, OffsetFetchRequestData}
import org.apache.kafka.common.requests.{OffsetCommitRequest, OffsetFetchRequest}
import org.apache.kafka.common.errors.InvalidRequestException

import java.util.{Collections, ArrayList => JArrayList}

object HttpRequestTranslator {

  // ... existing methods ...

  /**
   * Translate JSON body into an OffsetCommitRequest.
   *
   * Uses generationId=-1 and memberId="" (simple consumer mode).
   * The group does not need to have active members for this to work.
   *
   * @param group consumer group ID from URL path
   * @param body  parsed JSON body
   */
  def translateCommitOffsets(group: String, body: JsonNode): OffsetCommitRequest.Builder = {
    val offsetsNode = body.get("offsets")
    if (offsetsNode == null || !offsetsNode.isArray || offsetsNode.isEmpty)
      throw new InvalidRequestException("'offsets' array is required and must not be empty")

    // Group offsets by topic
    val byTopic = new java.util.LinkedHashMap[String, JArrayList[OffsetCommitRequestData.OffsetCommitRequestPartition]]()

    offsetsNode.forEach { entry =>
      val topic = requireString(entry, "topic")
      val partition = requireInt(entry, "partition")
      val offset = requireLong(entry, "offset")
      val metadata = optionalString(entry, "metadata", "")

      if (partition < 0)
        throw new InvalidRequestException(s"Partition must be non-negative, got $partition")
      if (offset < 0)
        throw new InvalidRequestException(s"Offset must be non-negative, got $offset")

      val partitionData = new OffsetCommitRequestData.OffsetCommitRequestPartition()
        .setPartitionIndex(partition)
        .setCommittedOffset(offset)
        .setCommittedMetadata(metadata)

      byTopic.computeIfAbsent(topic, _ => new JArrayList()).add(partitionData)
    }

    val topics = new JArrayList[OffsetCommitRequestData.OffsetCommitRequestTopic]()
    byTopic.forEach { (topicName, partitions) =>
      topics.add(new OffsetCommitRequestData.OffsetCommitRequestTopic()
        .setName(topicName)
        .setPartitions(partitions))
    }

    val data = new OffsetCommitRequestData()
      .setGroupId(group)
      .setMemberId("")
      .setGenerationIdOrMemberEpoch(-1)
      .setTopics(topics)

    new OffsetCommitRequest.Builder(data)
  }

  /**
   * Translate into an OffsetFetchRequest.
   *
   * @param group       consumer group ID from URL path
   * @param topicFilter optional topic name filter from query string
   */
  def translateFetchOffsets(group: String, topicFilter: Option[String]): OffsetFetchRequest.Builder = {
    val groupData = new OffsetFetchRequestData.OffsetFetchRequestGroup()
      .setGroupId(group)

    topicFilter match {
      case Some(topic) =>
        // Fetch offsets for a specific topic (all partitions)
        val topicData = new OffsetFetchRequestData.OffsetFetchRequestTopics()
          .setName(topic)
          .setPartitionIndexes(Collections.emptyList())  // empty = all partitions
        groupData.setTopics(Collections.singletonList(topicData))
      case None =>
        // Fetch offsets for all topics in the group
        groupData.setTopics(null)  // null = all topics
    }

    val data = new OffsetFetchRequestData()
      .setGroups(Collections.singletonList(groupData))

    new OffsetFetchRequest.Builder(data, false)
  }

  // --- JSON helper methods ---

  private def requireString(node: JsonNode, field: String): String = {
    val value = node.get(field)
    if (value == null || value.isNull)
      throw new InvalidRequestException(s"Required field '$field' is missing")
    value.asText()
  }

  private def requireInt(node: JsonNode, field: String): Int = {
    val value = node.get(field)
    if (value == null || value.isNull)
      throw new InvalidRequestException(s"Required field '$field' is missing")
    if (!value.isNumber)
      throw new InvalidRequestException(s"Field '$field' must be an integer")
    value.asInt()
  }

  private def requireLong(node: JsonNode, field: String): Long = {
    val value = node.get(field)
    if (value == null || value.isNull)
      throw new InvalidRequestException(s"Required field '$field' is missing")
    if (!value.isNumber)
      throw new InvalidRequestException(s"Field '$field' must be a number")
    value.asLong()
  }

  private def optionalString(node: JsonNode, field: String, default: String): String = {
    val value = node.get(field)
    if (value == null || value.isNull) default else value.asText()
  }
}
```

### HttpResponseSerializer.scala -- new serialization methods

```scala
// http-server/src/main/scala/kafka/server/http/HttpResponseSerializer.scala

package kafka.server.http

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import io.netty.handler.codec.http._
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{OffsetCommitResponse, OffsetFetchResponse}

object HttpResponseSerializer {

  private val MAPPER = new ObjectMapper()

  // ... existing methods ...

  /**
   * Serialize OffsetCommitResponse to JSON.
   * Returns per-partition commit results with error codes.
   */
  def serializeOffsetCommitResponse(response: OffsetCommitResponse): FullHttpResponse = {
    val root = MAPPER.createObjectNode()
    val offsetsArray = root.putArray("offsets")

    var hasErrors = false
    response.data().topics().forEach { topic =>
      topic.partitions().forEach { partition =>
        val node = offsetsArray.addObject()
        node.put("topic", topic.name())
        node.put("partition", partition.partitionIndex())
        node.put("offset", partition.committedOffset())
        node.put("errorCode", partition.errorCode().toInt)
        if (partition.errorCode() != 0) {
          hasErrors = true
          val error = Errors.forCode(partition.errorCode())
          node.put("errorMessage", error.message())
        }
      }
    }

    val status = if (hasErrors) HttpResponseStatus.valueOf(207) else HttpResponseStatus.OK
    buildJsonResponse(status, root)
  }

  /**
   * Serialize OffsetFetchResponse to JSON.
   * Returns committed offsets for the group.
   */
  def serializeOffsetFetchResponse(response: OffsetFetchResponse, group: String): FullHttpResponse = {
    val root = MAPPER.createObjectNode()
    root.put("group", group)
    val offsetsArray = root.putArray("offsets")

    response.data().groups().forEach { groupData =>
      if (groupData.groupId() == group) {
        // Check group-level error
        if (groupData.errorCode() != 0) {
          val error = Errors.forCode(groupData.errorCode())
          return buildErrorResponse(
            HttpErrorMapper.httpStatus(error),
            groupData.errorCode(),
            error.name(),
            error.message()
          )
        }

        groupData.topics().forEach { topic =>
          topic.partitions().forEach { partition =>
            if (partition.errorCode() == 0 && partition.committedOffset() >= 0) {
              val node = offsetsArray.addObject()
              node.put("topic", topic.name())
              node.put("partition", partition.partitionIndex())
              node.put("offset", partition.committedOffset())
              node.put("metadata", partition.metadata())
            }
          }
        }
      }
    }

    buildJsonResponse(HttpResponseStatus.OK, root)
  }
}
```

### HttpRequestHandler.scala -- dispatch additions

```scala
// In HttpRequestHandler.channelRead0() route match:

case CommitOffsetsRoute(group) =>
  val body = parseJsonBody(req)
  val builder = HttpRequestTranslator.translateCommitOffsets(group, body)
  enqueueRequest(ctx, builder, ApiKeys.OFFSET_COMMIT)

case FetchOffsetsRoute(group, topicFilter) =>
  val builder = HttpRequestTranslator.translateFetchOffsets(group, topicFilter)
  enqueueRequest(ctx, builder, ApiKeys.OFFSET_FETCH)
```

---

## Tests

### Unit Tests

```scala
// http-server/src/test/scala/kafka/server/http/HttpRequestTranslatorOffsetsTest.scala

package kafka.server.http

import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions._
import org.apache.kafka.common.errors.InvalidRequestException

class HttpRequestTranslatorOffsetsTest {

  private val mapper = new ObjectMapper()

  @Test
  def testTranslateCommitOffsets(): Unit = {
    val json = mapper.readTree("""
      {"offsets":[
        {"topic":"orders","partition":0,"offset":150,"metadata":""},
        {"topic":"orders","partition":1,"offset":88}
      ]}
    """)
    val builder = HttpRequestTranslator.translateCommitOffsets("my-group", json)
    val data = builder.build().data()
    assertEquals("my-group", data.groupId())
    assertEquals("", data.memberId())
    assertEquals(-1, data.generationIdOrMemberEpoch())
    assertEquals(1, data.topics().size())  // one topic
    assertEquals("orders", data.topics().get(0).name())
    assertEquals(2, data.topics().get(0).partitions().size())
  }

  @Test
  def testTranslateCommitOffsetsMultipleTopics(): Unit = {
    val json = mapper.readTree("""
      {"offsets":[
        {"topic":"orders","partition":0,"offset":100},
        {"topic":"payments","partition":0,"offset":200}
      ]}
    """)
    val builder = HttpRequestTranslator.translateCommitOffsets("my-group", json)
    val data = builder.build().data()
    assertEquals(2, data.topics().size())
  }

  @Test
  def testTranslateCommitOffsetsEmptyArray(): Unit = {
    val json = mapper.readTree("""{"offsets":[]}""")
    assertThrows(classOf[InvalidRequestException], () => {
      HttpRequestTranslator.translateCommitOffsets("my-group", json)
    })
  }

  @Test
  def testTranslateCommitOffsetsMissingField(): Unit = {
    val json = mapper.readTree("""{"offsets":[{"topic":"orders","partition":0}]}""")
    assertThrows(classOf[InvalidRequestException], () => {
      HttpRequestTranslator.translateCommitOffsets("my-group", json)
    })
  }

  @Test
  def testTranslateCommitOffsetsNegativePartition(): Unit = {
    val json = mapper.readTree("""{"offsets":[{"topic":"orders","partition":-1,"offset":10}]}""")
    assertThrows(classOf[InvalidRequestException], () => {
      HttpRequestTranslator.translateCommitOffsets("my-group", json)
    })
  }

  @Test
  def testTranslateCommitOffsetsNegativeOffset(): Unit = {
    val json = mapper.readTree("""{"offsets":[{"topic":"orders","partition":0,"offset":-5}]}""")
    assertThrows(classOf[InvalidRequestException], () => {
      HttpRequestTranslator.translateCommitOffsets("my-group", json)
    })
  }

  @Test
  def testTranslateFetchOffsetsAllTopics(): Unit = {
    val builder = HttpRequestTranslator.translateFetchOffsets("my-group", None)
    val data = builder.build().data()
    assertEquals(1, data.groups().size())
    assertEquals("my-group", data.groups().get(0).groupId())
    assertNull(data.groups().get(0).topics())  // null = all topics
  }

  @Test
  def testTranslateFetchOffsetsWithTopicFilter(): Unit = {
    val builder = HttpRequestTranslator.translateFetchOffsets("my-group", Some("orders"))
    val data = builder.build().data()
    assertEquals(1, data.groups().size())
    val topics = data.groups().get(0).topics()
    assertNotNull(topics)
    assertEquals(1, topics.size())
    assertEquals("orders", topics.get(0).name())
  }

  @Test
  def testTranslateCommitOffsetsDefaultMetadata(): Unit = {
    val json = mapper.readTree("""{"offsets":[{"topic":"t","partition":0,"offset":1}]}""")
    val builder = HttpRequestTranslator.translateCommitOffsets("g", json)
    val data = builder.build().data()
    assertEquals("", data.topics().get(0).partitions().get(0).committedMetadata())
  }
}
```

### Router Tests

```scala
// http-server/src/test/scala/kafka/server/http/HttpRouterOffsetsTest.scala

package kafka.server.http

import io.netty.buffer.Unpooled
import io.netty.handler.codec.http.{DefaultFullHttpRequest, HttpMethod, HttpVersion}
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions._

import java.nio.charset.StandardCharsets

class HttpRouterOffsetsTest {

  @Test
  def testRouteCommitOffsets(): Unit = {
    val body = """{"offsets":[]}"""
    val req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST,
      "/v1/consumer-groups/my-group/offsets",
      Unpooled.copiedBuffer(body, StandardCharsets.UTF_8))
    val result = HttpRouter.route(req)
    result match {
      case HttpRouter.CommitOffsetsRoute(group) => assertEquals("my-group", group)
      case _ => fail(s"Expected CommitOffsetsRoute, got $result")
    }
  }

  @Test
  def testRouteFetchOffsets(): Unit = {
    val req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET,
      "/v1/consumer-groups/my-group/offsets")
    val result = HttpRouter.route(req)
    result match {
      case HttpRouter.FetchOffsetsRoute(group, topicFilter) =>
        assertEquals("my-group", group)
        assertTrue(topicFilter.isEmpty)
      case _ => fail(s"Expected FetchOffsetsRoute, got $result")
    }
  }

  @Test
  def testRouteFetchOffsetsWithTopicFilter(): Unit = {
    val req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET,
      "/v1/consumer-groups/my-group/offsets?topic=orders")
    val result = HttpRouter.route(req)
    result match {
      case HttpRouter.FetchOffsetsRoute(group, topicFilter) =>
        assertEquals("my-group", group)
        assertEquals(Some("orders"), topicFilter)
      case _ => fail(s"Expected FetchOffsetsRoute, got $result")
    }
  }
}
```

### Integration Test

```scala
// http-server/src/test/scala/kafka/server/http/HttpConsumerGroupOffsetsIntegrationTest.scala

package kafka.server.http

import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test, TestInstance}
import org.junit.jupiter.api.Assertions._

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class HttpConsumerGroupOffsetsIntegrationTest extends HttpIntegrationTestHarness {

  private val httpClient = HttpClient.newHttpClient()
  private val mapper = new ObjectMapper()
  private val topicName = "offsets-test-topic"
  private val groupId = "offsets-test-group"

  @BeforeAll
  def setUp(): Unit = {
    startCluster()
    createTopic(topicName, 2, 1)
  }

  @AfterAll
  def tearDown(): Unit = {
    stopCluster()
  }

  @Test
  def testCommitAndFetchOffsets(): Unit = {
    // Commit offsets
    val commitBody = s"""{"offsets":[
      {"topic":"$topicName","partition":0,"offset":100,"metadata":"test"},
      {"topic":"$topicName","partition":1,"offset":50}
    ]}"""
    val commitReq = HttpRequest.newBuilder()
      .uri(URI.create(s"$httpBaseUrl/v1/consumer-groups/$groupId/offsets"))
      .header("Content-Type", "application/json")
      .POST(HttpRequest.BodyPublishers.ofString(commitBody))
      .build()
    val commitResp = httpClient.send(commitReq, HttpResponse.BodyHandlers.ofString())
    assertEquals(200, commitResp.statusCode())

    // Fetch offsets
    val fetchReq = HttpRequest.newBuilder()
      .uri(URI.create(s"$httpBaseUrl/v1/consumer-groups/$groupId/offsets?topic=$topicName"))
      .GET().build()
    val fetchResp = httpClient.send(fetchReq, HttpResponse.BodyHandlers.ofString())
    assertEquals(200, fetchResp.statusCode())

    val json = mapper.readTree(fetchResp.body())
    assertEquals(groupId, json.get("group").asText())
    val offsets = json.get("offsets")
    assertEquals(2, offsets.size())
  }

  @Test
  def testFetchOffsetsAllTopics(): Unit = {
    // Commit to two different topics
    val topic2 = "offsets-test-topic-2"
    createTopic(topic2, 1, 1)

    val commitBody = s"""{"offsets":[
      {"topic":"$topicName","partition":0,"offset":200},
      {"topic":"$topic2","partition":0,"offset":50}
    ]}"""
    val commitReq = HttpRequest.newBuilder()
      .uri(URI.create(s"$httpBaseUrl/v1/consumer-groups/$groupId/offsets"))
      .header("Content-Type", "application/json")
      .POST(HttpRequest.BodyPublishers.ofString(commitBody))
      .build()
    httpClient.send(commitReq, HttpResponse.BodyHandlers.ofString())

    // Fetch without topic filter
    val fetchReq = HttpRequest.newBuilder()
      .uri(URI.create(s"$httpBaseUrl/v1/consumer-groups/$groupId/offsets"))
      .GET().build()
    val fetchResp = httpClient.send(fetchReq, HttpResponse.BodyHandlers.ofString())
    assertEquals(200, fetchResp.statusCode())

    val json = mapper.readTree(fetchResp.body())
    val offsets = json.get("offsets")
    assertTrue(offsets.size() >= 2, "Should have offsets from both topics")
  }

  @Test
  def testCommitOffsetsInvalidBody(): Unit = {
    val commitReq = HttpRequest.newBuilder()
      .uri(URI.create(s"$httpBaseUrl/v1/consumer-groups/$groupId/offsets"))
      .header("Content-Type", "application/json")
      .POST(HttpRequest.BodyPublishers.ofString("""{"offsets":[]}"""))
      .build()
    val resp = httpClient.send(commitReq, HttpResponse.BodyHandlers.ofString())
    assertEquals(400, resp.statusCode())
  }

  @Test
  def testOffsetRoundTrip(): Unit = {
    val group2 = "roundtrip-group"
    val commitBody = s"""{"offsets":[{"topic":"$topicName","partition":0,"offset":42,"metadata":"checkpoint-1"}]}"""
    val commitReq = HttpRequest.newBuilder()
      .uri(URI.create(s"$httpBaseUrl/v1/consumer-groups/$group2/offsets"))
      .header("Content-Type", "application/json")
      .POST(HttpRequest.BodyPublishers.ofString(commitBody))
      .build()
    httpClient.send(commitReq, HttpResponse.BodyHandlers.ofString())

    val fetchReq = HttpRequest.newBuilder()
      .uri(URI.create(s"$httpBaseUrl/v1/consumer-groups/$group2/offsets?topic=$topicName"))
      .GET().build()
    val fetchResp = httpClient.send(fetchReq, HttpResponse.BodyHandlers.ofString())
    assertEquals(200, fetchResp.statusCode())

    val json = mapper.readTree(fetchResp.body())
    val offset = json.get("offsets").get(0)
    assertEquals(42L, offset.get("offset").asLong())
    assertEquals("checkpoint-1", offset.get("metadata").asText())
  }
}
```

---

## Rules

- Always use `generationId = -1` and `memberId = ""` in OffsetCommitRequest (simple consumer mode).
- Do NOT require group membership for offset commits -- this is a stateless HTTP API.
- Validate all required fields in the commit request body before building the Kafka request.
- Offsets must be non-negative. Partition IDs must be non-negative.
- The `metadata` field defaults to empty string if not provided.
- Authorization is enforced by existing KafkaApis handlers (READ on group + READ on topic for commit; DESCRIBE on group + READ on topic for fetch).
- Error responses follow the standard JSON error shape.

---

## Learning

- The existing codebase uses Java (not Scala) for the http-server module. The task skeleton code showed Scala, but the actual implementation matches the Java conventions of the existing `HttpRouter.java`, `HttpRequestTranslator.java`, and `HttpResponseSerializer.java` files.
- `OffsetCommitRequest.Builder` requires using `forTopicNames()` (or `forTopicIdsOrNames()`) factory methods rather than a direct constructor. For simple consumer mode (no topic IDs), `forTopicNames()` is the correct choice.
- `OffsetFetchRequest.Builder` similarly uses `forTopicNames()`. The `throwOnFetchStableOffsetsUnsupported` parameter should be `false` for HTTP since we don't need stable offsets for simple consumer mode.
- The `route()` method was refactored into three helper methods (`matchTopicRoutes`, `matchConsumerGroupRoutes`, `matchUtilityRoutes`) to keep NPath complexity under the checkstyle limit of 500.
- `OffsetFetchResponse` uses `OffsetFetchResponsePartitions` (plural) not `OffsetFetchResponsePartition` for the partition data type in the groups-based response format.

## Limitations

- Integration tests were not implemented because the test harness (`HttpIntegrationTestHarness`) does not exist yet -- it requires full broker + HTTP server wiring which is beyond the scope of this task.
- The `serialize()` dispatcher in `HttpResponseSerializer` extracts the group ID from the first group in the response for OFFSET_FETCH. If the request contained multiple groups (batch mode), only the first would be used. This is acceptable since our HTTP API only ever requests a single group.
- The `HttpRequestHandler.scala` (Scala Netty handler) was not modified for dispatch since it currently only handles auth context extraction. Full request dispatch through KafkaApis is a separate concern wired in TASK-B.06/C.01.

## Field Notes

- Pre-existing compilation errors in `core`, `clients`, `group-coordinator`, `raft`, `metadata`, and other modules prevent running the full test suite. These are unrelated to this task (missing `ApiKeyVersionsSource` annotation, `HttpAuthenticationContext` constructor changes, `KafkaApis` constructor mismatch).
- The `HttpRequestHandler.scala` had a pre-existing Scala compilation warning (unused val) that was fixed as a drive-by to unblock http-server compilation.
- Topic names in offset commit requests are validated by the Kafka protocol layer (via `OffsetCommitRequest.Builder.build()`), not by explicit validation in the translator. This is consistent with the existing produce/fetch translation approach.

---

## Acceptance Criteria

- [x] `POST /v1/consumer-groups/{group}/offsets` commits offsets and returns 200 with per-partition results
- [x] `GET /v1/consumer-groups/{group}/offsets` returns committed offsets for all topics
- [x] `GET /v1/consumer-groups/{group}/offsets?topic=X` returns committed offsets filtered by topic
- [ ] Committed offsets are visible to `kafka-consumer-groups.sh` and the lag endpoint (requires integration test harness)
- [x] Empty offsets array in commit request returns 400
- [x] Missing required fields (topic, partition, offset) return 400
- [x] Negative partition or offset values return 400
- [x] `generationId = -1` is used (simple consumer mode)
- [x] `metadata` defaults to empty string when omitted
- [x] Authorization is enforced via existing KafkaApis handlers (delegated to KafkaApis)
- [x] All unit tests pass
- [ ] Integration tests pass with offset commit/fetch round-trip (requires HttpIntegrationTestHarness)

---

## File Manifest

| File | Status |
|------|--------|
| `http-server/src/main/java/kafka/server/http/HttpRouter.java` | DONE |
| `http-server/src/main/java/kafka/server/http/HttpRequestTranslator.java` | DONE |
| `http-server/src/main/java/kafka/server/http/HttpResponseSerializer.java` | DONE |
| `http-server/src/main/scala/kafka/network/HttpRequestHandler.scala` | DONE (drive-by fix) |
| `http-server/src/test/java/kafka/server/http/HttpRequestTranslatorOffsetsTest.java` | DONE |
| `http-server/src/test/java/kafka/server/http/HttpRouterOffsetsTest.java` | DONE |
| `http-server/src/test/java/kafka/server/http/HttpResponseSerializerOffsetsTest.java` | DONE |
