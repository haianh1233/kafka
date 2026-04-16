# TASK-E.01: Metadata + List Topics + List Offsets Endpoints

## Prerequisites

| Task | What it provides |
|------|-----------------|
| TASK-B.01 | `HttpRouter` — URI pattern matching and handler dispatch |
| TASK-B.02 | `HttpResponseSerializer` — base JSON serialization framework |
| TASK-B.06 | SocketServer wiring — HTTP requests flow through `RequestChannel` to `KafkaApis` |

All three must be merged and passing CI before this task begins.

---

## Context

The HTTP protocol design (design doc section 4.3) specifies three observability endpoints that
reuse existing KafkaApis handlers unchanged. The HTTP layer only translates JSON and routes
through RequestChannel. These endpoints are:

1. **`GET /v1/topics`** -- returns all topics the caller is authorized to see (MetadataRequest with empty topics list)
2. **`GET /v1/topics/{topic}`** -- returns partition/leader/ISR metadata for a single topic
3. **`GET /v1/topics/{topic}/partitions/{partition}/offsets?timestamp=earliest|latest|max|<epoch>`** -- returns offset for a partition at a given timestamp

All three reuse existing `KafkaApis.handleTopicMetadataRequest()` and
`KafkaApis.handleListOffsetRequest()`. Authorization (`DESCRIBE` on topic for metadata,
`READ` on topic for offsets) is enforced inside the existing handlers. No new broker-side
logic is needed -- only HTTP translation and JSON serialization.

---

## Specification

### Endpoint 1: List Topics

```
GET /v1/topics
```

Response (200 OK):
```json
{
  "topics": ["orders", "payments", "inventory"]
}
```

Routed through `KafkaApis.handleTopicMetadataRequest()` with an empty topics list (which
returns all topics). The handler filters results by DESCRIBE authorization on each topic.

**IMPORTANT:** This endpoint must NOT bypass authorization by reading `MetadataCache.getAllTopics()`
directly. Always route through the METADATA request path.

### Endpoint 2: Topic Metadata

```
GET /v1/topics/{topic}
```

Response (200 OK):
```json
{
  "topic": "orders",
  "partitions": [
    {
      "partition": 0,
      "leader": { "brokerId": 1, "host": "broker1.example.com", "port": 9092 },
      "replicas": [1, 2, 3],
      "isr": [1, 2, 3]
    }
  ]
}
```

Kafka API used: `METADATA` (ApiKey id=3).

### Endpoint 3: List Offsets

```
GET /v1/topics/{topic}/partitions/{partition}/offsets?timestamp=earliest
GET /v1/topics/{topic}/partitions/{partition}/offsets?timestamp=latest
GET /v1/topics/{topic}/partitions/{partition}/offsets?timestamp=max
GET /v1/topics/{topic}/partitions/{partition}/offsets?timestamp=1713260400000
```

`timestamp` query parameter mapping:

| Query value | Wire value | Meaning |
|---|---|---|
| `earliest` | `-2` (`EARLIEST_TIMESTAMP`) | First available offset |
| `latest` | `-1` (`LATEST_TIMESTAMP`) | Log-end offset |
| `max` | `-3` (`MAX_TIMESTAMP`) | Offset of record with largest timestamp |
| `<epoch ms>` | positive long | First offset at or after the given epoch ms |

Response (200 OK):
```json
{
  "partition": 0,
  "offset": 10042,
  "timestamp": 1713260400000
}
```

Kafka API used: `LIST_OFFSETS` (ApiKey id=2).

---

## Implementation Details

### 1. HttpRouter Updates

Register three new route patterns in `HttpRouter`:

- `GET /v1/topics` -> `listTopics` handler
- `GET /v1/topics/{topic}` -> `getTopicMetadata` handler
- `GET /v1/topics/{topic}/partitions/{partition}/offsets` -> `listOffsets` handler

Route matching must distinguish between `GET /v1/topics` (list all) and `GET /v1/topics/{topic}` (single topic). The simplest approach: check path segment count after `/v1/topics`.

### 2. HttpRequestTranslator Updates

Add three translation methods:

- `translateListTopics()` -- builds `MetadataRequest` with empty topic list
- `translateGetTopicMetadata(topic)` -- builds `MetadataRequest` for a single topic
- `translateListOffsets(topic, partition, timestamp)` -- builds `ListOffsetsRequest`

For `ListOffsetsRequest`, parse the `timestamp` query parameter:
- `"earliest"` -> `ListOffsetsRequest.EARLIEST_TIMESTAMP` (-2)
- `"latest"` -> `ListOffsetsRequest.LATEST_TIMESTAMP` (-1)
- `"max"` -> `ListOffsetsRequest.MAX_TIMESTAMP` (-3)
- numeric string -> parse as `long`
- missing or invalid -> HTTP 400

### 3. HttpResponseSerializer Updates

Add two serialization methods:

- `serializeMetadataResponse(MetadataResponse, originalUri)` -- projects to the JSON shapes above. Must distinguish between list-all and single-topic based on `originalUri`.
- `serializeListOffsetsResponse(ListOffsetsResponse)` -- projects to the offset JSON shape.

For `MetadataResponse`, iterate `response.data().topics()` and build the JSON. For the
single-topic variant, extract just the one topic entry. For list-all, return an array of
topic names only.

### 4. Validation

Topic name validation is handled by `HttpRouter.validateTopicName()` (from design doc section 14.4).
Partition number must be a non-negative integer. `timestamp` query param is required for the offsets endpoint.

---

## Skeleton Code

### HttpRouter.scala -- new route registrations

```scala
// http-server/src/main/scala/kafka/server/http/HttpRouter.scala

package kafka.server.http

import io.netty.handler.codec.http.{FullHttpRequest, HttpMethod, QueryStringDecoder}
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.internals.Topic

import java.net.URLDecoder
import java.nio.charset.StandardCharsets
import org.apache.kafka.common.errors.InvalidRequestException

object HttpRouter {

  // Existing route types from TASK-B.01...

  sealed trait RouteResult
  case class ProduceRoute(topic: String) extends RouteResult
  case class FetchRoute(topic: String) extends RouteResult
  case object HealthRoute extends RouteResult
  case object ListTopicsRoute extends RouteResult
  case class TopicMetadataRoute(topic: String) extends RouteResult
  case class ListOffsetsRoute(topic: String, partition: Int, timestamp: Long) extends RouteResult
  case class NotFoundRoute(uri: String) extends RouteResult

  // Timestamp query param constants
  val EARLIEST_TIMESTAMP: Long = -2L
  val LATEST_TIMESTAMP: Long = -1L
  val MAX_TIMESTAMP: Long = -3L

  /**
   * Route an incoming HTTP request to the appropriate handler.
   */
  def route(request: FullHttpRequest): RouteResult = {
    val decoder = new QueryStringDecoder(request.uri())
    val path = decoder.path()
    val segments = path.stripPrefix("/").split("/").toList

    (request.method(), segments) match {
      case (HttpMethod.GET, List("v1", "health")) =>
        HealthRoute

      case (HttpMethod.GET, List("v1", "topics")) =>
        ListTopicsRoute

      case (HttpMethod.GET, List("v1", "topics", rawTopic)) =>
        val topic = validateTopicName(rawTopic)
        TopicMetadataRoute(topic)

      case (HttpMethod.GET, List("v1", "topics", rawTopic, "partitions", rawPartition, "offsets")) =>
        val topic = validateTopicName(rawTopic)
        val partition = validatePartitionId(rawPartition)
        val timestamp = parseTimestampParam(decoder)
        ListOffsetsRoute(topic, partition, timestamp)

      // ... existing produce/fetch routes ...

      case _ =>
        NotFoundRoute(request.uri())
    }
  }

  /**
   * Validate topic name from URL path segment.
   * URL-decodes, rejects path traversal, delegates to Kafka's Topic.validate().
   */
  def validateTopicName(rawSegment: String): String = {
    val topic = URLDecoder.decode(rawSegment, StandardCharsets.UTF_8)
    if (topic.contains("\u0000") || topic.contains("/") || topic.contains("\\"))
      throw new InvalidRequestException("Topic name contains illegal characters")
    Topic.validate(topic)
    topic
  }

  /**
   * Validate partition ID from URL path segment.
   */
  def validatePartitionId(raw: String): Int = {
    try {
      val partition = raw.toInt
      if (partition < 0)
        throw new InvalidRequestException(s"Partition must be non-negative, got $partition")
      partition
    } catch {
      case _: NumberFormatException =>
        throw new InvalidRequestException(s"Invalid partition: $raw")
    }
  }

  /**
   * Parse the 'timestamp' query parameter for the list-offsets endpoint.
   */
  def parseTimestampParam(decoder: QueryStringDecoder): Long = {
    val params = decoder.parameters()
    if (!params.containsKey("timestamp") || params.get("timestamp").isEmpty)
      throw new InvalidRequestException("Missing required query parameter: timestamp")

    val value = params.get("timestamp").get(0)
    value.toLowerCase match {
      case "earliest" => EARLIEST_TIMESTAMP
      case "latest"   => LATEST_TIMESTAMP
      case "max"      => MAX_TIMESTAMP
      case numeric =>
        try {
          val ts = numeric.toLong
          if (ts < 0)
            throw new InvalidRequestException(s"Timestamp must be non-negative or a symbolic name, got $ts")
          ts
        } catch {
          case _: NumberFormatException =>
            throw new InvalidRequestException(
              s"Invalid timestamp value: '$value'. Must be 'earliest', 'latest', 'max', or epoch milliseconds")
        }
    }
  }
}
```

### HttpRequestTranslator.scala -- new translation methods

```scala
// http-server/src/main/scala/kafka/server/http/HttpRequestTranslator.scala

package kafka.server.http

import org.apache.kafka.common.message.{MetadataRequestData, ListOffsetsRequestData}
import org.apache.kafka.common.message.ListOffsetsRequestData.{ListOffsetsPartition, ListOffsetsTopic}
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.{MetadataRequest, ListOffsetsRequest}

import java.util.Collections

object HttpRequestTranslator {

  // ... existing translate methods from TASK-B.01 ...

  /**
   * Build a MetadataRequest for listing all topics.
   * Empty topics list = all topics (authorization-filtered by KafkaApis).
   */
  def translateListTopics(): MetadataRequest.Builder = {
    // null topics list means "all topics" in MetadataRequest
    new MetadataRequest.Builder(
      new MetadataRequestData()
        .setTopics(null)  // null = all topics
        .setAllowAutoTopicCreation(false)
    )
  }

  /**
   * Build a MetadataRequest for a single topic.
   */
  def translateGetTopicMetadata(topic: String): MetadataRequest.Builder = {
    val topicData = new MetadataRequestData.MetadataRequestTopic()
      .setName(topic)
    new MetadataRequest.Builder(
      new MetadataRequestData()
        .setTopics(new MetadataRequestData.MetadataRequestTopicCollection(
          Collections.singletonList(topicData).iterator()))
        .setAllowAutoTopicCreation(false)
    )
  }

  /**
   * Build a ListOffsetsRequest for a single partition at a given timestamp.
   *
   * @param topic     topic name
   * @param partition partition id
   * @param timestamp epoch ms, or -1 (latest), -2 (earliest), -3 (max)
   */
  def translateListOffsets(topic: String, partition: Int, timestamp: Long): ListOffsetsRequest.Builder = {
    val partitionData = new ListOffsetsPartition()
      .setPartitionIndex(partition)
      .setTimestamp(timestamp)
      .setCurrentLeaderEpoch(-1)  // HTTP clients do not track leader epochs

    val topicData = new ListOffsetsTopic()
      .setName(topic)
      .setPartitions(Collections.singletonList(partitionData))

    ListOffsetsRequest.Builder.forConsumer(
      false,  // not requireTimestamp
      IsolationLevel.READ_UNCOMMITTED,  // default for metadata queries
      false   // not requireMaxTimestamp
    ).setTargetTimes(
      new ListOffsetsRequestData()
        .setTopics(Collections.singletonList(topicData))
    )
  }
}
```

### HttpResponseSerializer.scala -- new serialization methods

```scala
// http-server/src/main/scala/kafka/server/http/HttpResponseSerializer.scala

package kafka.server.http

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}
import io.netty.buffer.Unpooled
import io.netty.handler.codec.http._
import org.apache.kafka.common.requests.{AbstractResponse, MetadataResponse, ListOffsetsResponse}
import org.apache.kafka.common.message.MetadataResponseData
import org.apache.kafka.common.message.ListOffsetsResponseData

import java.nio.charset.StandardCharsets

object HttpResponseSerializer {

  private val MAPPER = new ObjectMapper()

  // ... existing serialize methods from TASK-B.02 ...

  /**
   * Serialize a MetadataResponse as a topic list (for GET /v1/topics).
   * Returns only topic names, filtered by the handler's authorization.
   */
  def serializeListTopicsResponse(response: MetadataResponse): FullHttpResponse = {
    val root = MAPPER.createObjectNode()
    val topicsArray = root.putArray("topics")
    response.data().topics().forEach { topicMetadata =>
      if (topicMetadata.errorCode() == 0) {
        topicsArray.add(topicMetadata.name())
      }
    }
    buildJsonResponse(HttpResponseStatus.OK, root)
  }

  /**
   * Serialize a MetadataResponse as topic metadata (for GET /v1/topics/{topic}).
   * Returns partition details including leader, replicas, ISR.
   */
  def serializeTopicMetadataResponse(response: MetadataResponse, topic: String): FullHttpResponse = {
    val topicMetadata = response.data().topics().asScala
      .find(_.name() == topic)

    topicMetadata match {
      case None =>
        buildErrorResponse(HttpResponseStatus.NOT_FOUND, 3,
          "UNKNOWN_TOPIC_OR_PARTITION", s"Topic '$topic' not found")

      case Some(meta) if meta.errorCode() != 0 =>
        val error = org.apache.kafka.common.protocol.Errors.forCode(meta.errorCode())
        buildErrorResponse(
          HttpErrorMapper.httpStatus(error),
          meta.errorCode(),
          error.name(),
          error.message()
        )

      case Some(meta) =>
        val root = MAPPER.createObjectNode()
        root.put("topic", meta.name())
        val partitionsArray = root.putArray("partitions")
        meta.partitions().forEach { partition =>
          val partNode = partitionsArray.addObject()
          partNode.put("partition", partition.partitionIndex())

          // Leader info
          val leaderNode = partNode.putObject("leader")
          leaderNode.put("brokerId", partition.leaderId())
          // Host/port from response brokers list
          response.data().brokers().forEach { broker =>
            if (broker.nodeId() == partition.leaderId()) {
              leaderNode.put("host", broker.host())
              leaderNode.put("port", broker.port())
            }
          }

          // Replicas
          val replicasArray = partNode.putArray("replicas")
          partition.replicaNodes().forEach(r => replicasArray.add(r.intValue()))

          // ISR
          val isrArray = partNode.putArray("isr")
          partition.isrNodes().forEach(i => isrArray.add(i.intValue()))
        }
        buildJsonResponse(HttpResponseStatus.OK, root)
    }
  }

  /**
   * Serialize a ListOffsetsResponse (for GET /v1/topics/{t}/partitions/{p}/offsets).
   */
  def serializeListOffsetsResponse(response: ListOffsetsResponse): FullHttpResponse = {
    // Extract the single partition result
    val partitionResult = response.data().topics().asScala.headOption
      .flatMap(_.partitions().asScala.headOption)

    partitionResult match {
      case None =>
        buildErrorResponse(HttpResponseStatus.INTERNAL_SERVER_ERROR, -1,
          "UNKNOWN", "No partition data in response")

      case Some(partition) if partition.errorCode() != 0 =>
        val error = org.apache.kafka.common.protocol.Errors.forCode(partition.errorCode())
        buildErrorResponse(
          HttpErrorMapper.httpStatus(error),
          partition.errorCode(),
          error.name(),
          error.message()
        )

      case Some(partition) =>
        val root = MAPPER.createObjectNode()
        root.put("partition", partition.partitionIndex())
        root.put("offset", partition.offset())
        root.put("timestamp", partition.timestamp())
        buildJsonResponse(HttpResponseStatus.OK, root)
    }
  }

  // -- helper methods --

  private def buildJsonResponse(status: HttpResponseStatus, node: ObjectNode): FullHttpResponse = {
    val json = MAPPER.writeValueAsBytes(node)
    val response = new DefaultFullHttpResponse(
      HttpVersion.HTTP_1_1, status, Unpooled.wrappedBuffer(json))
    response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json")
    response.headers().set(HttpHeaderNames.CONTENT_LENGTH, json.length)
    response
  }

  private def buildErrorResponse(status: HttpResponseStatus, errorCode: Int,
                                  detail: String, message: String): FullHttpResponse = {
    val root = MAPPER.createObjectNode()
    root.put("errorCode", errorCode)
    root.put("errorMessage", message)
    root.put("detail", detail)
    buildJsonResponse(status, root)
  }
}
```

### HttpRequestHandler.scala -- dispatch additions

```scala
// In HttpRequestHandler.channelRead0() -- add cases for new routes:

route match {
  case ListTopicsRoute =>
    val builder = HttpRequestTranslator.translateListTopics()
    enqueueRequest(ctx, builder, ApiKeys.METADATA)

  case TopicMetadataRoute(topic) =>
    val builder = HttpRequestTranslator.translateGetTopicMetadata(topic)
    enqueueRequest(ctx, builder, ApiKeys.METADATA)

  case ListOffsetsRoute(topic, partition, timestamp) =>
    val builder = HttpRequestTranslator.translateListOffsets(topic, partition, timestamp)
    enqueueRequest(ctx, builder, ApiKeys.LIST_OFFSETS)

  // ... existing cases ...
}
```

### HttpProcessor.java -- response dispatch additions

```java
// In HttpProcessor.processResponses() -- add serialization dispatch:

if (kafkaResponse instanceof MetadataResponse metadataResponse) {
    if (originalUri.equals("/v1/topics")) {
        httpResponse = HttpResponseSerializer.serializeListTopicsResponse(metadataResponse);
    } else {
        // Extract topic from URI: /v1/topics/{topic}
        String topic = extractTopicFromUri(originalUri);
        httpResponse = HttpResponseSerializer.serializeTopicMetadataResponse(metadataResponse, topic);
    }
} else if (kafkaResponse instanceof ListOffsetsResponse listOffsetsResponse) {
    httpResponse = HttpResponseSerializer.serializeListOffsetsResponse(listOffsetsResponse);
}
```

---

## Tests

### Unit Tests

```scala
// http-server/src/test/scala/kafka/server/http/HttpRouterMetadataTest.scala

package kafka.server.http

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions._
import io.netty.handler.codec.http.{DefaultFullHttpRequest, HttpMethod, HttpVersion}
import io.netty.buffer.Unpooled

class HttpRouterMetadataTest {

  @Test
  def testRouteListTopics(): Unit = {
    val req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/v1/topics")
    val result = HttpRouter.route(req)
    assertTrue(result.isInstanceOf[HttpRouter.ListTopicsRoute.type])
  }

  @Test
  def testRouteTopicMetadata(): Unit = {
    val req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/v1/topics/orders")
    val result = HttpRouter.route(req)
    result match {
      case HttpRouter.TopicMetadataRoute(topic) => assertEquals("orders", topic)
      case _ => fail(s"Expected TopicMetadataRoute, got $result")
    }
  }

  @Test
  def testRouteListOffsetsEarliest(): Unit = {
    val req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET,
      "/v1/topics/orders/partitions/0/offsets?timestamp=earliest")
    val result = HttpRouter.route(req)
    result match {
      case HttpRouter.ListOffsetsRoute(topic, partition, timestamp) =>
        assertEquals("orders", topic)
        assertEquals(0, partition)
        assertEquals(-2L, timestamp)
      case _ => fail(s"Expected ListOffsetsRoute, got $result")
    }
  }

  @Test
  def testRouteListOffsetsLatest(): Unit = {
    val req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET,
      "/v1/topics/orders/partitions/0/offsets?timestamp=latest")
    val result = HttpRouter.route(req)
    result match {
      case HttpRouter.ListOffsetsRoute(_, _, timestamp) =>
        assertEquals(-1L, timestamp)
      case _ => fail(s"Expected ListOffsetsRoute, got $result")
    }
  }

  @Test
  def testRouteListOffsetsMax(): Unit = {
    val req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET,
      "/v1/topics/orders/partitions/5/offsets?timestamp=max")
    val result = HttpRouter.route(req)
    result match {
      case HttpRouter.ListOffsetsRoute(_, partition, timestamp) =>
        assertEquals(5, partition)
        assertEquals(-3L, timestamp)
      case _ => fail(s"Expected ListOffsetsRoute, got $result")
    }
  }

  @Test
  def testRouteListOffsetsEpoch(): Unit = {
    val req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET,
      "/v1/topics/orders/partitions/0/offsets?timestamp=1713260400000")
    val result = HttpRouter.route(req)
    result match {
      case HttpRouter.ListOffsetsRoute(_, _, timestamp) =>
        assertEquals(1713260400000L, timestamp)
      case _ => fail(s"Expected ListOffsetsRoute, got $result")
    }
  }

  @Test
  def testRouteListOffsetsMissingTimestamp(): Unit = {
    val req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET,
      "/v1/topics/orders/partitions/0/offsets")
    assertThrows(classOf[org.apache.kafka.common.errors.InvalidRequestException], () => {
      HttpRouter.route(req)
    })
  }

  @Test
  def testRouteListOffsetsInvalidTimestamp(): Unit = {
    val req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET,
      "/v1/topics/orders/partitions/0/offsets?timestamp=notanumber")
    assertThrows(classOf[org.apache.kafka.common.errors.InvalidRequestException], () => {
      HttpRouter.route(req)
    })
  }

  @Test
  def testRouteInvalidPartition(): Unit = {
    val req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET,
      "/v1/topics/orders/partitions/-1/offsets?timestamp=earliest")
    assertThrows(classOf[org.apache.kafka.common.errors.InvalidRequestException], () => {
      HttpRouter.route(req)
    })
  }

  @Test
  def testUrlEncodedTopicName(): Unit = {
    val req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET,
      "/v1/topics/my-topic.v2")
    val result = HttpRouter.route(req)
    result match {
      case HttpRouter.TopicMetadataRoute(topic) => assertEquals("my-topic.v2", topic)
      case _ => fail(s"Expected TopicMetadataRoute, got $result")
    }
  }
}
```

### HttpResponseSerializer Tests

```scala
// http-server/src/test/scala/kafka/server/http/HttpResponseSerializerMetadataTest.scala

package kafka.server.http

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions._
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.common.message.MetadataResponseData
import org.apache.kafka.common.requests.MetadataResponse

class HttpResponseSerializerMetadataTest {

  private val mapper = new ObjectMapper()

  @Test
  def testSerializeListTopicsResponse(): Unit = {
    val data = new MetadataResponseData()
    val topic1 = new MetadataResponseData.MetadataResponseTopic()
      .setName("orders").setErrorCode(0)
    val topic2 = new MetadataResponseData.MetadataResponseTopic()
      .setName("payments").setErrorCode(0)
    data.topics().add(topic1)
    data.topics().add(topic2)

    val response = new MetadataResponse(data, ApiVersion.LATEST_PRODUCED)
    val httpResponse = HttpResponseSerializer.serializeListTopicsResponse(response)

    assertEquals(200, httpResponse.status().code())
    val json = mapper.readTree(httpResponse.content().array())
    val topics = json.get("topics")
    assertEquals(2, topics.size())
    assertEquals("orders", topics.get(0).asText())
    assertEquals("payments", topics.get(1).asText())
  }

  @Test
  def testSerializeListTopicsExcludesErrors(): Unit = {
    val data = new MetadataResponseData()
    val topic1 = new MetadataResponseData.MetadataResponseTopic()
      .setName("orders").setErrorCode(0)
    val topic2 = new MetadataResponseData.MetadataResponseTopic()
      .setName("secret").setErrorCode(29)  // TOPIC_AUTHORIZATION_FAILED
    data.topics().add(topic1)
    data.topics().add(topic2)

    val response = new MetadataResponse(data, ApiVersion.LATEST_PRODUCED)
    val httpResponse = HttpResponseSerializer.serializeListTopicsResponse(response)

    val json = mapper.readTree(httpResponse.content().array())
    assertEquals(1, json.get("topics").size())
    assertEquals("orders", json.get("topics").get(0).asText())
  }

  @Test
  def testSerializeTopicMetadataResponse(): Unit = {
    val data = new MetadataResponseData()
    // Add broker
    data.brokers().add(new MetadataResponseData.MetadataResponseBroker()
      .setNodeId(1).setHost("broker1.example.com").setPort(9092))
    // Add topic with partitions
    val topic = new MetadataResponseData.MetadataResponseTopic()
      .setName("orders").setErrorCode(0)
    val partition = new MetadataResponseData.MetadataResponsePartition()
      .setPartitionIndex(0).setLeaderId(1)
      .setReplicaNodes(java.util.Arrays.asList(1, 2, 3))
      .setIsrNodes(java.util.Arrays.asList(1, 2))
    topic.partitions().add(partition)
    data.topics().add(topic)

    val response = new MetadataResponse(data, ApiVersion.LATEST_PRODUCED)
    val httpResponse = HttpResponseSerializer.serializeTopicMetadataResponse(response, "orders")

    assertEquals(200, httpResponse.status().code())
    val json = mapper.readTree(httpResponse.content().array())
    assertEquals("orders", json.get("topic").asText())
    val partitions = json.get("partitions")
    assertEquals(1, partitions.size())
    assertEquals(0, partitions.get(0).get("partition").asInt())
    assertEquals(1, partitions.get(0).get("leader").get("brokerId").asInt())
  }

  @Test
  def testSerializeTopicMetadataNotFound(): Unit = {
    val data = new MetadataResponseData()
    val response = new MetadataResponse(data, ApiVersion.LATEST_PRODUCED)
    val httpResponse = HttpResponseSerializer.serializeTopicMetadataResponse(response, "nonexistent")
    assertEquals(404, httpResponse.status().code())
  }

  @Test
  def testSerializeListOffsetsResponse(): Unit = {
    val data = new org.apache.kafka.common.message.ListOffsetsResponseData()
    val topic = new org.apache.kafka.common.message.ListOffsetsResponseData.ListOffsetsTopicResponse()
      .setName("orders")
    val partition = new org.apache.kafka.common.message.ListOffsetsResponseData.ListOffsetsPartitionResponse()
      .setPartitionIndex(0).setOffset(10042L).setTimestamp(1713260400000L).setErrorCode(0)
    topic.partitions().add(partition)
    data.topics().add(topic)

    val response = new org.apache.kafka.common.requests.ListOffsetsResponse(data)
    val httpResponse = HttpResponseSerializer.serializeListOffsetsResponse(response)

    assertEquals(200, httpResponse.status().code())
    val json = mapper.readTree(httpResponse.content().array())
    assertEquals(0, json.get("partition").asInt())
    assertEquals(10042L, json.get("offset").asLong())
    assertEquals(1713260400000L, json.get("timestamp").asLong())
  }
}
```

### Integration Test

```scala
// http-server/src/test/scala/kafka/server/http/HttpMetadataIntegrationTest.scala

package kafka.server.http

import kafka.server.http.HttpIntegrationTestHarness
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test, TestInstance}
import org.junit.jupiter.api.Assertions._
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.net.URI
import com.fasterxml.jackson.databind.ObjectMapper

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class HttpMetadataIntegrationTest extends HttpIntegrationTestHarness {

  private val httpClient = HttpClient.newHttpClient()
  private val mapper = new ObjectMapper()
  private val topicName = "metadata-test-topic"
  private val numPartitions = 3

  @BeforeAll
  def setUp(): Unit = {
    startCluster()
    createTopic(topicName, numPartitions, 1)
  }

  @AfterAll
  def tearDown(): Unit = {
    stopCluster()
  }

  @Test
  def testListTopics(): Unit = {
    val request = HttpRequest.newBuilder()
      .uri(URI.create(s"$httpBaseUrl/v1/topics"))
      .GET().build()
    val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
    assertEquals(200, response.statusCode())
    val json = mapper.readTree(response.body())
    assertTrue(json.get("topics").isArray)
    val topics = (0 until json.get("topics").size()).map(i => json.get("topics").get(i).asText())
    assertTrue(topics.contains(topicName))
  }

  @Test
  def testGetTopicMetadata(): Unit = {
    val request = HttpRequest.newBuilder()
      .uri(URI.create(s"$httpBaseUrl/v1/topics/$topicName"))
      .GET().build()
    val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
    assertEquals(200, response.statusCode())
    val json = mapper.readTree(response.body())
    assertEquals(topicName, json.get("topic").asText())
    assertEquals(numPartitions, json.get("partitions").size())
  }

  @Test
  def testGetTopicMetadataNotFound(): Unit = {
    val request = HttpRequest.newBuilder()
      .uri(URI.create(s"$httpBaseUrl/v1/topics/nonexistent-topic-xyz"))
      .GET().build()
    val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
    assertEquals(404, response.statusCode())
  }

  @Test
  def testListOffsetsEarliest(): Unit = {
    val request = HttpRequest.newBuilder()
      .uri(URI.create(s"$httpBaseUrl/v1/topics/$topicName/partitions/0/offsets?timestamp=earliest"))
      .GET().build()
    val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
    assertEquals(200, response.statusCode())
    val json = mapper.readTree(response.body())
    assertEquals(0, json.get("partition").asInt())
    assertTrue(json.get("offset").asLong() >= 0)
  }

  @Test
  def testListOffsetsLatest(): Unit = {
    val request = HttpRequest.newBuilder()
      .uri(URI.create(s"$httpBaseUrl/v1/topics/$topicName/partitions/0/offsets?timestamp=latest"))
      .GET().build()
    val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
    assertEquals(200, response.statusCode())
    val json = mapper.readTree(response.body())
    assertTrue(json.has("offset"))
  }

  @Test
  def testListOffsetsInvalidPartition(): Unit = {
    val request = HttpRequest.newBuilder()
      .uri(URI.create(s"$httpBaseUrl/v1/topics/$topicName/partitions/999/offsets?timestamp=earliest"))
      .GET().build()
    val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
    // Partition 999 does not exist -- should return an error
    assertTrue(response.statusCode() >= 400)
  }

  @Test
  def testListOffsetsMissingTimestamp(): Unit = {
    val request = HttpRequest.newBuilder()
      .uri(URI.create(s"$httpBaseUrl/v1/topics/$topicName/partitions/0/offsets"))
      .GET().build()
    val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
    assertEquals(400, response.statusCode())
  }
}
```

---

## Rules

- Do NOT bypass authorization. Always route through the METADATA/LIST_OFFSETS request path via RequestChannel.
- Do NOT read MetadataCache directly for the list-topics endpoint.
- Use `allowAutoTopicCreation = false` in all MetadataRequest builders -- HTTP GET must never create topics.
- All JSON field names use camelCase (matching existing response shapes in design doc section 4).
- Error responses must use the standard error JSON shape: `{ "errorCode": ..., "errorMessage": ..., "detail": ... }`.
- Timestamp parsing must be case-insensitive for symbolic names ("earliest", "EARLIEST", "Earliest" all work).
- Partition ID in URL path must be validated as non-negative integer before building Kafka request.

---

## Learning

_To be filled by the executing agent._

## Limitations

_To be filled by the executing agent._

## Field Notes

_To be filled by the executing agent._

---

## Acceptance Criteria

- [ ] `GET /v1/topics` returns 200 with JSON array of topic names the caller is authorized to see
- [ ] `GET /v1/topics/{topic}` returns 200 with partition/leader/ISR metadata for the topic
- [ ] `GET /v1/topics/{topic}` returns 404 for nonexistent topic
- [ ] `GET /v1/topics/{topic}/partitions/{p}/offsets?timestamp=earliest` returns earliest offset
- [ ] `GET /v1/topics/{topic}/partitions/{p}/offsets?timestamp=latest` returns log-end offset
- [ ] `GET /v1/topics/{topic}/partitions/{p}/offsets?timestamp=max` returns max-timestamp offset
- [ ] `GET /v1/topics/{topic}/partitions/{p}/offsets?timestamp=<epoch>` returns offset for epoch
- [ ] Missing `timestamp` query param returns 400
- [ ] Invalid `timestamp` value (non-numeric, non-symbolic) returns 400
- [ ] Negative partition ID returns 400
- [ ] Non-numeric partition ID returns 400
- [ ] Authorization is enforced via existing KafkaApis handlers (no direct MetadataCache reads)
- [ ] All unit tests pass
- [ ] Integration tests pass against a running single-broker cluster
- [ ] Response JSON shapes match the design doc section 4.3

---

## File Manifest

| File | Status |
|------|--------|
| `http-server/src/main/scala/kafka/server/http/HttpRouter.scala` | |
| `http-server/src/main/scala/kafka/server/http/HttpRequestTranslator.scala` | |
| `http-server/src/main/scala/kafka/server/http/HttpResponseSerializer.scala` | |
| `http-server/src/main/scala/kafka/network/HttpRequestHandler.scala` | |
| `http-server/src/main/java/kafka/server/http/HttpProcessor.java` | |
| `http-server/src/test/scala/kafka/server/http/HttpRouterMetadataTest.scala` | |
| `http-server/src/test/scala/kafka/server/http/HttpResponseSerializerMetadataTest.scala` | |
| `http-server/src/test/scala/kafka/server/http/HttpMetadataIntegrationTest.scala` | |
