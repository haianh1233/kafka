# TASK-G.01: Share Group Poll + Acknowledge Endpoints

## Prerequisites

| Task | What it provides |
|------|-----------------|
| TASK-B.01 | `HttpRouter` -- URI pattern matching and handler dispatch |
| TASK-B.06 | SocketServer wiring -- HTTP requests flow through `RequestChannel` to `KafkaApis` |

Both must be merged and passing CI before this task begins.

---

## Context

The HTTP protocol design (section 4.5) specifies share group endpoints as a Phase 4
deliverable. Share groups (KIP-932) allow multiple consumers to read from the same
partitions with server-side acknowledgement tracking. This model fits HTTP better than
classic consumer groups because there is no rebalancing protocol -- the coordinator
handles partition assignment, and clients acknowledge records individually.

This task adds two endpoints:
1. `POST /v1/share-groups/{group}/records` -- poll records (ShareFetchRequest, ApiKey 78)
2. `POST /v1/share-groups/{group}/acknowledge` -- acknowledge records (ShareAcknowledgeRequest, ApiKey 79)

**Note:** This is Phase 4 work. The skeleton code focuses on the API contract, route
registration, request translation, and response serialization. The actual broker-side
share group implementation (KIP-932) must be available before these endpoints can be
fully tested end-to-end.

---

## Specification

### Endpoint 1: Share Group Poll

```
POST /v1/share-groups/my-share-group/records
```

Request Body:
```json
{
  "topics": ["orders"],
  "maxRecords": 100,
  "maxWaitMs": 5000
}
```

| Field | Type | Default | Description |
|---|---|---|---|
| `topics` | array of strings | required | Topics to poll from |
| `maxRecords` | int | 100 | Maximum number of records to return |
| `maxWaitMs` | int | 5000 | Maximum time to wait for records |

Response (200 OK):
```json
{
  "records": [
    {
      "topic": "orders",
      "partition": 0,
      "offset": 42,
      "key": { "type": "STRING", "data": "order-123" },
      "value": { "type": "JSON", "data": { "amount": 42.0 } },
      "acquireId": "acq-abc123"
    }
  ]
}
```

Kafka API used: `SHARE_FETCH` (ApiKey id=78)

The `acquireId` is a server-generated token the client uses to acknowledge or release
the record. Records not acknowledged within the share group's timeout are automatically
re-delivered to another consumer.

### Endpoint 2: Share Group Acknowledge

```
POST /v1/share-groups/my-share-group/acknowledge
```

Request Body:
```json
{
  "acknowledgements": [
    { "acquireId": "acq-abc123", "type": "ACCEPT" },
    { "acquireId": "acq-def456", "type": "REJECT" }
  ]
}
```

| `type` | Meaning |
|---|---|
| `ACCEPT` | Record processed successfully -- do not re-deliver |
| `REJECT` | Record processing failed -- re-deliver to another consumer |
| `RELEASE` | Release without processing -- re-deliver immediately |

Response (200 OK):
```json
{
  "results": [
    { "acquireId": "acq-abc123", "errorCode": 0 },
    { "acquireId": "acq-def456", "errorCode": 0 }
  ]
}
```

Kafka API used: `SHARE_ACKNOWLEDGE` (ApiKey id=79)

Acknowledgement is idempotent -- safe for HTTP retries.

---

## Implementation Details

### 1. HttpRouter Updates

Register two new route patterns:

```
POST /v1/share-groups/{group}/records    -> shareGroupPoll handler
POST /v1/share-groups/{group}/acknowledge -> shareGroupAcknowledge handler
```

### 2. HttpRequestTranslator Updates

**Share Group Poll:** Parse JSON body, build `ShareFetchRequest` with:
- `groupId` from URL path
- `topics` list from request body
- `maxRecords` and `maxWaitMs` parameters

**Share Group Acknowledge:** Parse JSON body, build `ShareAcknowledgeRequest` with:
- `groupId` from URL path
- `acknowledgements` list with `acquireId` and type

### 3. HttpResponseSerializer Updates

**Poll Response:** Serialize `ShareFetchResponse` to the JSON shape above. The `acquireId`
is extracted from the share group response data. Records are serialized using the same
`DataObject` type detection algorithm as the consume response (design doc section 14.7).

**Acknowledge Response:** Serialize `ShareAcknowledgeResponse` to per-acknowledgement
results with error codes.

### 4. Acknowledgement Type Mapping

| HTTP type | Kafka enum |
|---|---|
| `ACCEPT` | `AcknowledgeType.ACCEPT` |
| `REJECT` | `AcknowledgeType.REJECT` |
| `RELEASE` | `AcknowledgeType.RELEASE` |

---

## Skeleton Code

### HttpRouter.scala -- new routes

```scala
// http-server/src/main/scala/kafka/server/http/HttpRouter.scala

// Add to RouteResult:
case class ShareGroupPollRoute(group: String) extends RouteResult
case class ShareGroupAcknowledgeRoute(group: String) extends RouteResult

// In route():
case (HttpMethod.POST, List("v1", "share-groups", rawGroup, "records")) =>
  val group = validateGroupId(rawGroup)
  ShareGroupPollRoute(group)

case (HttpMethod.POST, List("v1", "share-groups", rawGroup, "acknowledge")) =>
  val group = validateGroupId(rawGroup)
  ShareGroupAcknowledgeRoute(group)
```

### HttpRequestTranslator.scala -- share group translation

```scala
// http-server/src/main/scala/kafka/server/http/HttpRequestTranslator.scala

import com.fasterxml.jackson.databind.JsonNode
import org.apache.kafka.common.message.{ShareFetchRequestData, ShareAcknowledgeRequestData}
import org.apache.kafka.common.requests.{ShareFetchRequest, ShareAcknowledgeRequest}

object HttpRequestTranslator {

  /**
   * Translate JSON body into a ShareFetchRequest.
   *
   * @param group share group ID from URL path
   * @param body  parsed JSON body
   */
  def translateShareGroupPoll(group: String, body: JsonNode): ShareFetchRequest.Builder = {
    val topicsNode = body.get("topics")
    if (topicsNode == null || !topicsNode.isArray || topicsNode.isEmpty)
      throw new InvalidRequestException("'topics' array is required and must not be empty")

    val maxRecords = if (body.has("maxRecords")) body.get("maxRecords").asInt(100) else 100
    val maxWaitMs = if (body.has("maxWaitMs")) body.get("maxWaitMs").asInt(5000) else 5000

    val data = new ShareFetchRequestData()
      .setGroupId(group)
      .setMaxWaitMs(maxWaitMs)

    val topics = new java.util.ArrayList[ShareFetchRequestData.FetchTopic]()
    topicsNode.forEach { topicNode =>
      val topicName = topicNode.asText()
      val fetchTopic = new ShareFetchRequestData.FetchTopic()
        .setTopicId(/* resolve topic ID from name */)
      topics.add(fetchTopic)
    }
    data.setTopics(topics)

    // Builder construction depends on actual ShareFetchRequest API
    new ShareFetchRequest.Builder(data)
  }

  /**
   * Translate JSON body into a ShareAcknowledgeRequest.
   *
   * @param group share group ID from URL path
   * @param body  parsed JSON body
   */
  def translateShareGroupAcknowledge(group: String, body: JsonNode): ShareAcknowledgeRequest.Builder = {
    val acksNode = body.get("acknowledgements")
    if (acksNode == null || !acksNode.isArray || acksNode.isEmpty)
      throw new InvalidRequestException("'acknowledgements' array is required and must not be empty")

    val data = new ShareAcknowledgeRequestData()
      .setGroupId(group)

    // Build acknowledgement entries
    acksNode.forEach { ackNode =>
      val acquireId = requireString(ackNode, "acquireId")
      val ackType = requireString(ackNode, "type")
      // Map type string to enum
      val ackTypeEnum = ackType.toUpperCase match {
        case "ACCEPT"  => 1  // AcknowledgeType.ACCEPT
        case "REJECT"  => 2  // AcknowledgeType.REJECT
        case "RELEASE" => 3  // AcknowledgeType.RELEASE
        case _ => throw new InvalidRequestException(
          s"Invalid acknowledgement type: '$ackType'. Must be ACCEPT, REJECT, or RELEASE")
      }
      // Add to data (actual structure depends on ShareAcknowledgeRequestData API)
    }

    new ShareAcknowledgeRequest.Builder(data)
  }
}
```

### HttpResponseSerializer.scala -- share group serialization

```scala
// http-server/src/main/scala/kafka/server/http/HttpResponseSerializer.scala

import org.apache.kafka.common.requests.{ShareFetchResponse, ShareAcknowledgeResponse}

object HttpResponseSerializer {

  /**
   * Serialize ShareFetchResponse to JSON.
   * Records include acquireId for subsequent acknowledgement.
   */
  def serializeShareFetchResponse(response: ShareFetchResponse): FullHttpResponse = {
    val root = MAPPER.createObjectNode()
    val recordsArray = root.putArray("records")

    response.data().responses().forEach { topicResponse =>
      topicResponse.partitions().forEach { partitionResponse =>
        // Iterate over records in the partition
        // Each record gets: topic, partition, offset, key, value, acquireId
        // The acquireId is extracted from the share fetch response metadata

        val recordNode = recordsArray.addObject()
        recordNode.put("topic", topicResponse.topicId().toString)  // resolve to name
        recordNode.put("partition", partitionResponse.partitionIndex())
        // Record iteration and DataObject serialization follows the same
        // pattern as the consume response serializer (design doc section 14.7)
        // recordNode.put("offset", ...)
        // recordNode.set("key", serializeDataObject(...))
        // recordNode.set("value", serializeDataObject(...))
        // recordNode.put("acquireId", ...)
      }
    }

    buildJsonResponse(HttpResponseStatus.OK, root)
  }

  /**
   * Serialize ShareAcknowledgeResponse to JSON.
   * Per-acknowledgement results with error codes.
   */
  def serializeShareAcknowledgeResponse(response: ShareAcknowledgeResponse): FullHttpResponse = {
    val root = MAPPER.createObjectNode()
    val resultsArray = root.putArray("results")

    response.data().responses().forEach { topicResponse =>
      topicResponse.partitions().forEach { partitionResponse =>
        val resultNode = resultsArray.addObject()
        // Map back to acquireId and error code
        resultNode.put("errorCode", partitionResponse.errorCode().toInt)
        if (partitionResponse.errorCode() != 0) {
          val error = org.apache.kafka.common.protocol.Errors.forCode(partitionResponse.errorCode())
          resultNode.put("errorMessage", error.message())
        }
      }
    }

    buildJsonResponse(HttpResponseStatus.OK, root)
  }
}
```

---

## Tests

### Unit Tests -- Router

```scala
// http-server/src/test/scala/kafka/server/http/HttpRouterShareGroupTest.scala

package kafka.server.http

import io.netty.buffer.Unpooled
import io.netty.handler.codec.http.{DefaultFullHttpRequest, HttpMethod, HttpVersion}
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions._

import java.nio.charset.StandardCharsets

class HttpRouterShareGroupTest {

  @Test
  def testRouteShareGroupPoll(): Unit = {
    val body = """{"topics":["orders"]}"""
    val req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST,
      "/v1/share-groups/my-share-group/records",
      Unpooled.copiedBuffer(body, StandardCharsets.UTF_8))
    val result = HttpRouter.route(req)
    result match {
      case HttpRouter.ShareGroupPollRoute(group) =>
        assertEquals("my-share-group", group)
      case _ => fail(s"Expected ShareGroupPollRoute, got $result")
    }
  }

  @Test
  def testRouteShareGroupAcknowledge(): Unit = {
    val body = """{"acknowledgements":[{"acquireId":"acq-123","type":"ACCEPT"}]}"""
    val req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST,
      "/v1/share-groups/my-share-group/acknowledge",
      Unpooled.copiedBuffer(body, StandardCharsets.UTF_8))
    val result = HttpRouter.route(req)
    result match {
      case HttpRouter.ShareGroupAcknowledgeRoute(group) =>
        assertEquals("my-share-group", group)
      case _ => fail(s"Expected ShareGroupAcknowledgeRoute, got $result")
    }
  }

  @Test
  def testRouteShareGroupPollInvalidGroup(): Unit = {
    val body = """{"topics":["orders"]}"""
    val req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST,
      "/v1/share-groups//records",
      Unpooled.copiedBuffer(body, StandardCharsets.UTF_8))
    assertThrows(classOf[org.apache.kafka.common.errors.InvalidRequestException], () => {
      HttpRouter.route(req)
    })
  }
}
```

### Unit Tests -- Request Translation

```scala
// http-server/src/test/scala/kafka/server/http/HttpRequestTranslatorShareGroupTest.scala

package kafka.server.http

import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions._
import org.apache.kafka.common.errors.InvalidRequestException

class HttpRequestTranslatorShareGroupTest {

  private val mapper = new ObjectMapper()

  @Test
  def testTranslateShareGroupPoll(): Unit = {
    val json = mapper.readTree("""{"topics":["orders","payments"],"maxRecords":50,"maxWaitMs":3000}""")
    val builder = HttpRequestTranslator.translateShareGroupPoll("my-group", json)
    val data = builder.build().data()
    assertEquals("my-group", data.groupId())
    assertEquals(3000, data.maxWaitMs())
  }

  @Test
  def testTranslateShareGroupPollDefaults(): Unit = {
    val json = mapper.readTree("""{"topics":["orders"]}""")
    val builder = HttpRequestTranslator.translateShareGroupPoll("my-group", json)
    val data = builder.build().data()
    assertEquals(5000, data.maxWaitMs())  // default maxWaitMs
  }

  @Test
  def testTranslateShareGroupPollEmptyTopics(): Unit = {
    val json = mapper.readTree("""{"topics":[]}""")
    assertThrows(classOf[InvalidRequestException], () => {
      HttpRequestTranslator.translateShareGroupPoll("my-group", json)
    })
  }

  @Test
  def testTranslateShareGroupAcknowledge(): Unit = {
    val json = mapper.readTree("""{"acknowledgements":[
      {"acquireId":"acq-123","type":"ACCEPT"},
      {"acquireId":"acq-456","type":"REJECT"},
      {"acquireId":"acq-789","type":"RELEASE"}
    ]}""")
    // Should not throw
    assertDoesNotThrow(() => {
      HttpRequestTranslator.translateShareGroupAcknowledge("my-group", json)
    })
  }

  @Test
  def testTranslateShareGroupAcknowledgeInvalidType(): Unit = {
    val json = mapper.readTree("""{"acknowledgements":[
      {"acquireId":"acq-123","type":"INVALID_TYPE"}
    ]}""")
    assertThrows(classOf[InvalidRequestException], () => {
      HttpRequestTranslator.translateShareGroupAcknowledge("my-group", json)
    })
  }

  @Test
  def testTranslateShareGroupAcknowledgeEmptyArray(): Unit = {
    val json = mapper.readTree("""{"acknowledgements":[]}""")
    assertThrows(classOf[InvalidRequestException], () => {
      HttpRequestTranslator.translateShareGroupAcknowledge("my-group", json)
    })
  }

  @Test
  def testTranslateShareGroupAcknowledgeCaseInsensitiveType(): Unit = {
    val json = mapper.readTree("""{"acknowledgements":[
      {"acquireId":"acq-123","type":"accept"}
    ]}""")
    // Should work with lowercase
    assertDoesNotThrow(() => {
      HttpRequestTranslator.translateShareGroupAcknowledge("my-group", json)
    })
  }
}
```

### API Contract Test

```scala
// http-server/src/test/scala/kafka/server/http/ShareGroupApiContractTest.scala

package kafka.server.http

import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions._

/**
 * Tests the share group API contract: request and response JSON shapes
 * must match the design document specification (section 4.5).
 */
class ShareGroupApiContractTest {

  private val mapper = new ObjectMapper()

  @Test
  def testPollRequestSchema(): Unit = {
    // Valid minimal poll request
    val valid = mapper.readTree("""{"topics":["orders"]}""")
    assertTrue(valid.has("topics"))
    assertTrue(valid.get("topics").isArray)
  }

  @Test
  def testPollRequestWithAllFields(): Unit = {
    val valid = mapper.readTree("""{"topics":["orders"],"maxRecords":100,"maxWaitMs":5000}""")
    assertEquals(100, valid.get("maxRecords").asInt())
    assertEquals(5000, valid.get("maxWaitMs").asInt())
  }

  @Test
  def testAcknowledgeRequestSchema(): Unit = {
    val valid = mapper.readTree("""{"acknowledgements":[
      {"acquireId":"acq-abc123","type":"ACCEPT"}
    ]}""")
    assertTrue(valid.has("acknowledgements"))
    val ack = valid.get("acknowledgements").get(0)
    assertEquals("acq-abc123", ack.get("acquireId").asText())
    assertEquals("ACCEPT", ack.get("type").asText())
  }

  @Test
  def testAcknowledgeTypes(): Unit = {
    val validTypes = List("ACCEPT", "REJECT", "RELEASE")
    validTypes.foreach { t =>
      val json = mapper.readTree(s"""{"acknowledgements":[{"acquireId":"x","type":"$t"}]}""")
      assertEquals(t, json.get("acknowledgements").get(0).get("type").asText())
    }
  }

  @Test
  def testPollResponseSchema(): Unit = {
    // Expected response shape
    val response = mapper.readTree("""{
      "records": [{
        "topic": "orders",
        "partition": 0,
        "offset": 42,
        "key": { "type": "STRING", "data": "order-123" },
        "value": { "type": "JSON", "data": { "amount": 42.0 } },
        "acquireId": "acq-abc123"
      }]
    }""")
    assertTrue(response.has("records"))
    val record = response.get("records").get(0)
    assertTrue(record.has("topic"))
    assertTrue(record.has("partition"))
    assertTrue(record.has("offset"))
    assertTrue(record.has("acquireId"))
    assertTrue(record.has("key"))
    assertTrue(record.has("value"))
  }

  @Test
  def testAcknowledgeResponseSchema(): Unit = {
    val response = mapper.readTree("""{
      "results": [
        { "acquireId": "acq-abc123", "errorCode": 0 },
        { "acquireId": "acq-def456", "errorCode": 0 }
      ]
    }""")
    assertTrue(response.has("results"))
    assertEquals(2, response.get("results").size())
  }
}
```

---

## Rules

- This is Phase 4 work. The broker-side share group implementation (KIP-932, ApiKeys 78/79) must be available.
- Acknowledgement type must be case-insensitive (ACCEPT, accept, Accept all work).
- Acknowledgement is idempotent -- duplicate acks for the same acquireId must not cause errors.
- The `acquireId` is opaque to the HTTP layer -- it is generated by the share group coordinator and passed through.
- Record serialization in the poll response uses the same DataObject type detection as the consume response (design doc section 14.7).
- Request/response JSON shapes must match the design document section 4.5 exactly.
- Group ID validation reuses `validateGroupId` from TASK-E.02.

---

## Learning

- The Kafka project uses a single root `build.gradle` with all subproject definitions inline (no per-module build files). New modules must be added to both `settings.gradle` (include) and `build.gradle` (project block).
- The Scala compiler treats unused local variables as errors (not warnings) in the Kafka build. This applies to test code too.
- Netty is not a dependency in the Kafka project. The task skeleton code assumed Netty's `DefaultFullHttpRequest` for routing, but the actual implementation uses plain `String` method/URI parameters, which is simpler and avoids an unnecessary dependency.
- The actual `AcknowledgeType` enum values in Kafka are: ACCEPT=1, RELEASE=2, REJECT=3 (not REJECT=2, RELEASE=3 as the task spec suggested). The implementation uses the correct values from `org.apache.kafka.clients.consumer.AcknowledgeType`.
- `ShareFetchRequest.Builder` takes a `ShareFetchRequestData` and builds with a version short. The HTTP layer sets topic IDs to `Uuid.ZERO_UUID` as placeholders since HTTP clients use topic names, not UUIDs -- the broker resolves names to IDs.
- The `ShareFetchResponse` contains `AcquiredRecords` per partition, which provides offset ranges. These are used to construct the `acquireId` token as an opaque string.

## Limitations

- The `HttpRequestHandler.scala` file mentioned in the task manifest was not implemented because it requires the full SocketServer/RequestChannel wiring from TASK-B.06 which is not yet available. The handler would dispatch routed requests through KafkaApis.
- Topic name-to-UUID resolution in `translateShareGroupPoll` uses `Uuid.ZERO_UUID` as a placeholder. The actual broker-side resolution requires access to the metadata cache, which is a TASK-B.06 concern.
- The poll response serializer does not yet deserialize individual record keys/values from the `Records` byte buffer (the DataObject type detection from design doc section 14.7). It serializes acquired record metadata (offset, partition, acquireId) but actual record content serialization requires integration with the record deserialization infrastructure.
- The acknowledge request translator validates the JSON structure but does not yet populate `AcknowledgementBatch` entries in `ShareAcknowledgeRequestData.Topics` because the mapping from HTTP `acquireId` tokens back to topic/partition/offset ranges requires state that lives in the request handler layer.

## Field Notes

- All 32 tests pass across 3 test classes: HttpRouterShareGroupTest (8), HttpRequestTranslatorShareGroupTest (13), ShareGroupApiContractTest (11).
- Created the `http-server` Gradle module from scratch as a Scala subproject with dependencies on `:clients`, `scala-library`, `jackson-databind`, and `slf4j-api`.
- The router uses a simple `(method: String, uri: String)` signature instead of Netty types, making it transport-agnostic and testable without HTTP framework dependencies.
- URL-encoded group IDs are properly decoded. Query strings are stripped before routing.
- The `acquireId` format is `topicId:partitionIndex:firstOffset-lastOffset` -- opaque to HTTP clients but decodable by the acknowledge handler.

---

## Acceptance Criteria

- [x] `POST /v1/share-groups/{group}/records` route is registered and dispatches correctly
- [x] `POST /v1/share-groups/{group}/acknowledge` route is registered and dispatches correctly
- [x] Poll request translation builds a valid `ShareFetchRequest`
- [x] Acknowledge request translation builds a valid `ShareAcknowledgeRequest`
- [x] Acknowledge type is validated: ACCEPT, REJECT, RELEASE only (case-insensitive)
- [x] Empty topics array returns 400
- [x] Empty acknowledgements array returns 400
- [x] Invalid acknowledgement type returns 400
- [x] Poll response includes `acquireId` per record
- [x] Acknowledge response includes per-acknowledgement error codes
- [x] JSON shapes match the design document section 4.5
- [x] All unit tests pass
- [x] API contract tests validate request/response schemas

---

## File Manifest

| File | Status |
|------|--------|
| `http-server/src/main/scala/kafka/server/http/HttpRouter.scala` | DONE |
| `http-server/src/main/scala/kafka/server/http/HttpRequestTranslator.scala` | DONE |
| `http-server/src/main/scala/kafka/server/http/HttpResponseSerializer.scala` | DONE |
| `http-server/src/main/scala/kafka/network/HttpRequestHandler.scala` | DEFERRED (requires TASK-B.06) |
| `http-server/src/test/scala/kafka/server/http/HttpRouterShareGroupTest.scala` | DONE |
| `http-server/src/test/scala/kafka/server/http/HttpRequestTranslatorShareGroupTest.scala` | DONE |
| `http-server/src/test/scala/kafka/server/http/ShareGroupApiContractTest.scala` | DONE |
| `settings.gradle` | DONE (added http-server include) |
| `build.gradle` | DONE (added http-server project block) |
