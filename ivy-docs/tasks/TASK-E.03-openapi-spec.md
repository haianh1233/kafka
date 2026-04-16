# TASK-E.03: OpenAPI Specification

## Prerequisites

| Task | What it provides |
|------|-----------------|
| TASK-E.01 | Metadata/offsets endpoints -- all Phase 2 endpoints defined |
| TASK-E.02 | Consumer group lag endpoint -- completes the Phase 2 endpoint set |

Both must be merged before this task begins so that the full endpoint surface is known.

---

## Context

The HTTP protocol design (section 14.12) calls for an OpenAPI 3.0 specification file at
`http-server/src/main/resources/openapi.yaml` covering all endpoints in section 4 of the
design document. The spec enables client code generation across languages (the stated
motivation in section 1 is polyglot clients). Optionally, the spec is served at
`GET /v1/openapi.yaml` for client tooling discovery.

The spec must cover all endpoints defined through Phase 2 and include placeholders for
Phase 3 and Phase 4 endpoints.

---

## Specification

### Endpoints to Document

| Endpoint | Method | Phase |
|----------|--------|-------|
| `/v1/health` | GET | 1 |
| `/v1/topics` | GET | 2 |
| `/v1/topics/{topic}` | GET | 2 |
| `/v1/topics/{topic}/records` | POST | 1 |
| `/v1/topics/{topic}/records:fetch` | POST | 1 |
| `/v1/topics/{topic}/partitions/{partition}/offsets` | GET | 2 |
| `/v1/consumer-groups/{group}/lags` | GET | 2 |
| `/v1/consumer-groups/{group}/offsets` | POST | 3 |
| `/v1/consumer-groups/{group}/offsets` | GET | 3 |
| `/v1/share-groups/{group}/records` | POST | 4 |
| `/v1/share-groups/{group}/acknowledge` | POST | 4 |

### Serving the Spec

`GET /v1/openapi.yaml` serves the file directly from the classpath resource. Content-Type
is `application/x-yaml`. This is a simple static file serve -- no RequestChannel round-trip
needed.

---

## Implementation Details

### 1. Create the OpenAPI YAML File

File location: `http-server/src/main/resources/openapi.yaml`

The spec must include:
- `openapi: "3.0.3"` version
- `info` with title, version, description
- `servers` section (parameterized)
- `paths` for all endpoints
- `components/schemas` for reusable types (DataObject, ProduceRecord, ErrorResponse, etc.)
- `components/parameters` for reusable path/query parameters
- Response examples for all endpoints

### 2. Add Route and Handler for Serving the Spec

Add `GET /v1/openapi.yaml` to HttpRouter. The handler reads the classpath resource and
returns it with `Content-Type: application/x-yaml`.

### 3. Schema Definitions

Key reusable schemas:

- `DataObject` -- `{ type: STRING|BINARY|JSON|NULL, data: ... }`
- `ProduceRequest` -- the produce request body
- `ProduceResponse` -- the produce response body
- `FetchRequest` -- the fetch request body
- `FetchResponse` -- the fetch response body
- `ErrorResponse` -- standard error response body
- `TopicMetadata` -- topic metadata response
- `OffsetInfo` -- list offsets response
- `ConsumerGroupLag` -- lag response
- `HealthStatus` -- health check response

---

## Skeleton Code

### openapi.yaml

```yaml
# http-server/src/main/resources/openapi.yaml

openapi: "3.0.3"

info:
  title: Apache Kafka HTTP API
  version: "1.0.0"
  description: |
    Native HTTP protocol for Apache Kafka brokers. Enables produce, consume,
    and metadata operations over HTTP/1.1 without the Kafka binary protocol.

servers:
  - url: "http://{host}:{port}/v1"
    description: Kafka broker HTTP listener
    variables:
      host:
        default: "localhost"
      port:
        default: "9094"

paths:
  /health:
    get:
      operationId: healthCheck
      summary: Broker health check
      description: Returns 200 if the broker is in RUNNING state, 503 otherwise.
      tags: [Health]
      responses:
        "200":
          description: Broker is healthy
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/HealthStatus"
        "503":
          description: Broker is not healthy

  /topics:
    get:
      operationId: listTopics
      summary: List all topics
      description: Returns topic names the caller is authorized to see.
      tags: [Metadata]
      responses:
        "200":
          description: Topic list
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/TopicList"
        "403":
          $ref: "#/components/responses/Forbidden"

  /topics/{topic}:
    get:
      operationId: getTopicMetadata
      summary: Get topic metadata
      description: Returns partition, leader, replica, and ISR metadata for a topic.
      tags: [Metadata]
      parameters:
        - $ref: "#/components/parameters/TopicName"
      responses:
        "200":
          description: Topic metadata
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/TopicMetadata"
        "404":
          $ref: "#/components/responses/NotFound"
        "403":
          $ref: "#/components/responses/Forbidden"

  /topics/{topic}/records:
    post:
      operationId: produceRecords
      summary: Produce records to a topic
      description: |
        Send one or more records to a topic. Records may target different partitions.
        The broker forwards internally to partition leaders.
      tags: [Produce]
      parameters:
        - $ref: "#/components/parameters/TopicName"
      requestBody:
        required: true
        content:
          application/json:
            schema:
              $ref: "#/components/schemas/ProduceRequest"
      responses:
        "200":
          description: All records produced successfully
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/ProduceResponse"
        "207":
          description: Partial success (multi-partition batch)
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/ProduceResponse"
        "400":
          $ref: "#/components/responses/BadRequest"
        "403":
          $ref: "#/components/responses/Forbidden"
        "413":
          $ref: "#/components/responses/PayloadTooLarge"

  /topics/{topic}/records:fetch:
    post:
      operationId: fetchRecords
      summary: Consume records from a topic
      description: |
        Fetch records from specified partitions at given offsets.
        Uses POST because the fetch request carries per-partition offset state.
      tags: [Consume]
      parameters:
        - $ref: "#/components/parameters/TopicName"
      requestBody:
        required: true
        content:
          application/json:
            schema:
              $ref: "#/components/schemas/FetchRequest"
      responses:
        "200":
          description: Fetch results
          headers:
            X-Kafka-MaxWait-Applied:
              description: The effective maxWaitMs after server-side capping
              schema:
                type: integer
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/FetchResponse"
        "400":
          $ref: "#/components/responses/BadRequest"
        "403":
          $ref: "#/components/responses/Forbidden"

  /topics/{topic}/partitions/{partition}/offsets:
    get:
      operationId: listOffsets
      summary: Get offset for a partition
      description: Returns the offset for a partition at a given timestamp.
      tags: [Metadata]
      parameters:
        - $ref: "#/components/parameters/TopicName"
        - $ref: "#/components/parameters/PartitionId"
        - name: timestamp
          in: query
          required: true
          description: |
            Timestamp to look up. Symbolic values: "earliest", "latest", "max".
            Or an epoch millisecond value.
          schema:
            type: string
      responses:
        "200":
          description: Offset info
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/OffsetInfo"
        "400":
          $ref: "#/components/responses/BadRequest"
        "404":
          $ref: "#/components/responses/NotFound"

  /consumer-groups/{group}/lags:
    get:
      operationId: getConsumerGroupLag
      summary: Get consumer group lag
      description: |
        Returns committed offset, log-end offset, and lag per partition.
        Lag is approximate due to concurrent sampling.
      tags: [Consumer Groups]
      parameters:
        - $ref: "#/components/parameters/GroupId"
      responses:
        "200":
          description: Lag information
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/ConsumerGroupLag"
        "403":
          $ref: "#/components/responses/Forbidden"
        "404":
          $ref: "#/components/responses/NotFound"

  /consumer-groups/{group}/offsets:
    post:
      operationId: commitOffsets
      summary: Commit consumer group offsets
      description: |
        Store committed offsets in __consumer_offsets without joining the group.
        Uses generationId=-1 (simple consumer mode).
      tags: [Consumer Groups]
      parameters:
        - $ref: "#/components/parameters/GroupId"
      requestBody:
        required: true
        content:
          application/json:
            schema:
              $ref: "#/components/schemas/OffsetCommitRequest"
      responses:
        "200":
          description: Offsets committed
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/OffsetCommitResponse"
        "403":
          $ref: "#/components/responses/Forbidden"
    get:
      operationId: fetchOffsets
      summary: Fetch committed offsets
      description: Returns committed offsets for the consumer group.
      tags: [Consumer Groups]
      parameters:
        - $ref: "#/components/parameters/GroupId"
        - name: topic
          in: query
          required: false
          description: Filter by topic name. Omit to get all topics.
          schema:
            type: string
      responses:
        "200":
          description: Committed offsets
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/OffsetFetchResponse"
        "403":
          $ref: "#/components/responses/Forbidden"

  /share-groups/{group}/records:
    post:
      operationId: shareGroupPoll
      summary: Poll records from a share group (Phase 4)
      tags: [Share Groups]
      parameters:
        - $ref: "#/components/parameters/GroupId"
      requestBody:
        required: true
        content:
          application/json:
            schema:
              $ref: "#/components/schemas/ShareFetchRequest"
      responses:
        "200":
          description: Fetched records with acquire IDs
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/ShareFetchResponse"

  /share-groups/{group}/acknowledge:
    post:
      operationId: shareGroupAcknowledge
      summary: Acknowledge share group records (Phase 4)
      tags: [Share Groups]
      parameters:
        - $ref: "#/components/parameters/GroupId"
      requestBody:
        required: true
        content:
          application/json:
            schema:
              $ref: "#/components/schemas/ShareAcknowledgeRequest"
      responses:
        "200":
          description: Acknowledgements processed
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/ShareAcknowledgeResponse"

components:
  parameters:
    TopicName:
      name: topic
      in: path
      required: true
      schema:
        type: string
        pattern: "^[a-zA-Z0-9._-]+$"
        maxLength: 249
    PartitionId:
      name: partition
      in: path
      required: true
      schema:
        type: integer
        minimum: 0
    GroupId:
      name: group
      in: path
      required: true
      schema:
        type: string
        maxLength: 255

  responses:
    BadRequest:
      description: Invalid request
      content:
        application/json:
          schema:
            $ref: "#/components/schemas/ErrorResponse"
    Forbidden:
      description: Authorization failed
      content:
        application/json:
          schema:
            $ref: "#/components/schemas/ErrorResponse"
    NotFound:
      description: Resource not found
      content:
        application/json:
          schema:
            $ref: "#/components/schemas/ErrorResponse"
    PayloadTooLarge:
      description: Request body too large
      content:
        application/json:
          schema:
            $ref: "#/components/schemas/ErrorResponse"

  schemas:
    ErrorResponse:
      type: object
      properties:
        errorCode:
          type: integer
        errorMessage:
          type: string
        detail:
          type: string
      required: [errorCode, errorMessage]

    HealthStatus:
      type: object
      properties:
        status:
          type: string
          enum: [RUNNING, NOT_RUNNING, STARTING, RECOVERY, PENDING_CONTROLLED_SHUTDOWN, SHUTTING_DOWN]
        brokerId:
          type: integer
        clusterId:
          type: string
      required: [status, brokerId, clusterId]

    DataObject:
      type: object
      properties:
        type:
          type: string
          enum: [STRING, BINARY, JSON, "NULL"]
        data:
          description: The data payload. String for STRING/BINARY, object for JSON, absent for NULL.
      required: [type]

    TopicList:
      type: object
      properties:
        topics:
          type: array
          items:
            type: string
      required: [topics]

    TopicMetadata:
      type: object
      properties:
        topic:
          type: string
        partitions:
          type: array
          items:
            type: object
            properties:
              partition:
                type: integer
              leader:
                type: object
                properties:
                  brokerId:
                    type: integer
                  host:
                    type: string
                  port:
                    type: integer
              replicas:
                type: array
                items:
                  type: integer
              isr:
                type: array
                items:
                  type: integer
      required: [topic, partitions]

    ProduceRequest:
      type: object
      properties:
        records:
          type: array
          items:
            $ref: "#/components/schemas/ProduceRecord"
          minItems: 1
        acks:
          type: string
          enum: [all, leader, none]
          default: all
        timeoutMs:
          type: integer
          default: 30000
          minimum: 1
      required: [records]

    ProduceRecord:
      type: object
      properties:
        partition:
          type: integer
          minimum: 0
        key:
          $ref: "#/components/schemas/DataObject"
        value:
          $ref: "#/components/schemas/DataObject"
        headers:
          type: array
          items:
            type: object
            properties:
              name:
                type: string
              value:
                type: string
            required: [name, value]

    ProduceResponse:
      type: object
      properties:
        offsets:
          type: array
          items:
            type: object
            properties:
              partition:
                type: integer
              offset:
                type: integer
                format: int64
              errorCode:
                type: integer
              errorMessage:
                type: string
      required: [offsets]

    FetchRequest:
      type: object
      properties:
        partitions:
          type: array
          items:
            type: object
            properties:
              partition:
                type: integer
              offset:
                type: integer
                format: int64
            required: [partition, offset]
          minItems: 1
        maxWaitMs:
          type: integer
          default: 500
        minBytes:
          type: integer
          default: 1
        maxBytes:
          type: integer
          default: 10485760
        maxBytesPerPartition:
          type: integer
          default: 1048576
        isolationLevel:
          type: string
          enum: [READ_COMMITTED, READ_UNCOMMITTED]
          default: READ_UNCOMMITTED
      required: [partitions]

    FetchResponse:
      type: object
      properties:
        partitions:
          type: array
          items:
            type: object
            properties:
              partition:
                type: integer
              highWatermark:
                type: integer
                format: int64
              records:
                type: array
                items:
                  type: object
                  properties:
                    offset:
                      type: integer
                      format: int64
                    timestamp:
                      type: integer
                      format: int64
                    key:
                      $ref: "#/components/schemas/DataObject"
                    value:
                      $ref: "#/components/schemas/DataObject"
                    headers:
                      type: array
                      items:
                        type: object
                        properties:
                          name:
                            type: string
                          value:
                            type: string
              errorCode:
                type: integer
              errorMessage:
                type: string
      required: [partitions]

    OffsetInfo:
      type: object
      properties:
        partition:
          type: integer
        offset:
          type: integer
          format: int64
        timestamp:
          type: integer
          format: int64
      required: [partition, offset, timestamp]

    ConsumerGroupLag:
      type: object
      properties:
        group:
          type: string
        totalLag:
          type: integer
          format: int64
        partitions:
          type: array
          items:
            type: object
            properties:
              topic:
                type: string
              partition:
                type: integer
              committedOffset:
                type: integer
                format: int64
              logEndOffset:
                type: integer
                format: int64
              lag:
                type: integer
                format: int64
      required: [group, totalLag, partitions]

    OffsetCommitRequest:
      type: object
      properties:
        offsets:
          type: array
          items:
            type: object
            properties:
              topic:
                type: string
              partition:
                type: integer
              offset:
                type: integer
                format: int64
              metadata:
                type: string
                default: ""
            required: [topic, partition, offset]
      required: [offsets]

    OffsetCommitResponse:
      type: object
      properties:
        offsets:
          type: array
          items:
            type: object
            properties:
              topic:
                type: string
              partition:
                type: integer
              offset:
                type: integer
                format: int64
              errorCode:
                type: integer

    OffsetFetchResponse:
      type: object
      properties:
        group:
          type: string
        offsets:
          type: array
          items:
            type: object
            properties:
              topic:
                type: string
              partition:
                type: integer
              offset:
                type: integer
                format: int64
              metadata:
                type: string

    ShareFetchRequest:
      type: object
      properties:
        topics:
          type: array
          items:
            type: string
        maxRecords:
          type: integer
          default: 100
        maxWaitMs:
          type: integer
          default: 5000
      required: [topics]

    ShareFetchResponse:
      type: object
      properties:
        records:
          type: array
          items:
            type: object
            properties:
              topic:
                type: string
              partition:
                type: integer
              offset:
                type: integer
                format: int64
              key:
                $ref: "#/components/schemas/DataObject"
              value:
                $ref: "#/components/schemas/DataObject"
              acquireId:
                type: string

    ShareAcknowledgeRequest:
      type: object
      properties:
        acknowledgements:
          type: array
          items:
            type: object
            properties:
              acquireId:
                type: string
              type:
                type: string
                enum: [ACCEPT, REJECT, RELEASE]
            required: [acquireId, type]
      required: [acknowledgements]

    ShareAcknowledgeResponse:
      type: object
      properties:
        results:
          type: array
          items:
            type: object
            properties:
              acquireId:
                type: string
              errorCode:
                type: integer
              errorMessage:
                type: string
```

### HttpRouter.scala -- new route for serving spec

```scala
// Add to HttpRouter:

case object OpenApiRoute extends RouteResult

// In route():
case (HttpMethod.GET, List("v1", "openapi.yaml")) =>
  OpenApiRoute
```

### HttpRequestHandler.scala -- serve OpenAPI spec

```scala
// http-server/src/main/scala/kafka/network/HttpRequestHandler.scala

import java.nio.charset.StandardCharsets

// In channelRead0() route match:
case OpenApiRoute =>
  serveOpenApiSpec(ctx)

/**
 * Serve the OpenAPI specification from the classpath resource.
 * No RequestChannel round-trip needed.
 */
private def serveOpenApiSpec(ctx: ChannelHandlerContext): Unit = {
  val specStream = getClass.getClassLoader.getResourceAsStream("openapi.yaml")
  if (specStream == null) {
    val response = new DefaultFullHttpResponse(
      HttpVersion.HTTP_1_1, HttpResponseStatus.NOT_FOUND,
      Unpooled.copiedBuffer("""{"errorCode":-1,"errorMessage":"OpenAPI spec not found"}""",
        StandardCharsets.UTF_8))
    response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json")
    ctx.writeAndFlush(response)
    return
  }

  try {
    val bytes = specStream.readAllBytes()
    val response = new DefaultFullHttpResponse(
      HttpVersion.HTTP_1_1, HttpResponseStatus.OK,
      Unpooled.wrappedBuffer(bytes))
    response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/x-yaml")
    response.headers().set(HttpHeaderNames.CONTENT_LENGTH, bytes.length)
    response.headers().set(HttpHeaderNames.CACHE_CONTROL, "public, max-age=3600")
    ctx.writeAndFlush(response)
  } finally {
    specStream.close()
  }
}
```

---

## Tests

### YAML Validation Test

```scala
// http-server/src/test/scala/kafka/server/http/OpenApiSpecTest.scala

package kafka.server.http

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions._
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory

import java.io.InputStream

class OpenApiSpecTest {

  private val yamlMapper = new ObjectMapper(new YAMLFactory())

  @Test
  def testSpecExistsOnClasspath(): Unit = {
    val stream: InputStream = getClass.getClassLoader.getResourceAsStream("openapi.yaml")
    assertNotNull(stream, "openapi.yaml must exist on classpath")
    stream.close()
  }

  @Test
  def testSpecIsValidYaml(): Unit = {
    val stream = getClass.getClassLoader.getResourceAsStream("openapi.yaml")
    val tree = yamlMapper.readTree(stream)
    assertNotNull(tree)
    stream.close()
  }

  @Test
  def testSpecVersion(): Unit = {
    val stream = getClass.getClassLoader.getResourceAsStream("openapi.yaml")
    val tree = yamlMapper.readTree(stream)
    assertEquals("3.0.3", tree.get("openapi").asText())
    stream.close()
  }

  @Test
  def testAllEndpointsPresent(): Unit = {
    val stream = getClass.getClassLoader.getResourceAsStream("openapi.yaml")
    val tree = yamlMapper.readTree(stream)
    val paths = tree.get("paths")
    assertNotNull(paths)

    // Phase 1 endpoints
    assertTrue(paths.has("/health"), "Missing /health")
    assertTrue(paths.has("/topics/{topic}/records"), "Missing /topics/{topic}/records")
    assertTrue(paths.has("/topics/{topic}/records:fetch"), "Missing /topics/{topic}/records:fetch")

    // Phase 2 endpoints
    assertTrue(paths.has("/topics"), "Missing /topics")
    assertTrue(paths.has("/topics/{topic}"), "Missing /topics/{topic}")
    assertTrue(paths.has("/topics/{topic}/partitions/{partition}/offsets"),
      "Missing offsets endpoint")
    assertTrue(paths.has("/consumer-groups/{group}/lags"), "Missing lag endpoint")

    // Phase 3 endpoints
    assertTrue(paths.has("/consumer-groups/{group}/offsets"), "Missing offsets endpoint")

    // Phase 4 endpoints
    assertTrue(paths.has("/share-groups/{group}/records"), "Missing share group poll")
    assertTrue(paths.has("/share-groups/{group}/acknowledge"), "Missing share group ack")

    stream.close()
  }

  @Test
  def testAllSchemasPresent(): Unit = {
    val stream = getClass.getClassLoader.getResourceAsStream("openapi.yaml")
    val tree = yamlMapper.readTree(stream)
    val schemas = tree.get("components").get("schemas")

    val requiredSchemas = List(
      "ErrorResponse", "HealthStatus", "DataObject", "TopicList", "TopicMetadata",
      "ProduceRequest", "ProduceRecord", "ProduceResponse",
      "FetchRequest", "FetchResponse", "OffsetInfo",
      "ConsumerGroupLag", "OffsetCommitRequest", "OffsetCommitResponse",
      "OffsetFetchResponse", "ShareFetchRequest", "ShareFetchResponse",
      "ShareAcknowledgeRequest", "ShareAcknowledgeResponse"
    )

    requiredSchemas.foreach { name =>
      assertTrue(schemas.has(name), s"Missing schema: $name")
    }
    stream.close()
  }

  @Test
  def testProduceEndpointHasMethods(): Unit = {
    val stream = getClass.getClassLoader.getResourceAsStream("openapi.yaml")
    val tree = yamlMapper.readTree(stream)
    val producePath = tree.get("paths").get("/topics/{topic}/records")
    assertTrue(producePath.has("post"), "Produce endpoint must have POST method")
    stream.close()
  }

  @Test
  def testErrorResponseSchemaHasRequiredFields(): Unit = {
    val stream = getClass.getClassLoader.getResourceAsStream("openapi.yaml")
    val tree = yamlMapper.readTree(stream)
    val errorSchema = tree.get("components").get("schemas").get("ErrorResponse")
    val required = errorSchema.get("required")
    assertNotNull(required)
    val requiredFields = (0 until required.size()).map(i => required.get(i).asText()).toSet
    assertTrue(requiredFields.contains("errorCode"))
    assertTrue(requiredFields.contains("errorMessage"))
    stream.close()
  }
}
```

### Integration Test -- Serving the Spec

```scala
// http-server/src/test/scala/kafka/server/http/HttpOpenApiIntegrationTest.scala

package kafka.server.http

import org.junit.jupiter.api.{AfterAll, BeforeAll, Test, TestInstance}
import org.junit.jupiter.api.Assertions._
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.net.URI

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class HttpOpenApiIntegrationTest extends HttpIntegrationTestHarness {

  private val httpClient = HttpClient.newHttpClient()

  @BeforeAll
  def setUp(): Unit = {
    startCluster()
  }

  @AfterAll
  def tearDown(): Unit = {
    stopCluster()
  }

  @Test
  def testServeOpenApiSpec(): Unit = {
    val request = HttpRequest.newBuilder()
      .uri(URI.create(s"$httpBaseUrl/v1/openapi.yaml"))
      .GET().build()
    val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
    assertEquals(200, response.statusCode())
    assertTrue(response.headers().firstValue("content-type").orElse("")
      .contains("yaml"))
    assertTrue(response.body().contains("openapi:"))
    assertTrue(response.body().contains("paths:"))
  }

  @Test
  def testOpenApiSpecContentType(): Unit = {
    val request = HttpRequest.newBuilder()
      .uri(URI.create(s"$httpBaseUrl/v1/openapi.yaml"))
      .GET().build()
    val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
    assertEquals("application/x-yaml",
      response.headers().firstValue("content-type").orElse(""))
  }

  @Test
  def testOpenApiSpecIsCacheable(): Unit = {
    val request = HttpRequest.newBuilder()
      .uri(URI.create(s"$httpBaseUrl/v1/openapi.yaml"))
      .GET().build()
    val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
    assertTrue(response.headers().firstValue("cache-control").orElse("")
      .contains("max-age"))
  }
}
```

---

## Rules

- The OpenAPI spec must be valid YAML parseable by any OpenAPI 3.0 compatible tool.
- All endpoint paths must use `/v1` prefix (not in the path definitions themselves since the server URL includes it -- OR include the full path; be consistent).
- Schema field names must use camelCase matching the JSON response shapes from the design doc.
- The `DataObject` schema must document the `type` enum: STRING, BINARY, JSON, NULL.
- Phase 3 and Phase 4 endpoints must be included even if not yet implemented (they document the planned API).
- The spec file must be served with `Content-Type: application/x-yaml`.
- The spec file serving must NOT go through RequestChannel (it is a static file).
- Add `Cache-Control: public, max-age=3600` to the spec response.

---

## Learning

_To be filled by the executing agent._

## Limitations

_To be filled by the executing agent._

## Field Notes

_To be filled by the executing agent._

---

## Acceptance Criteria

- [ ] `openapi.yaml` exists at `http-server/src/main/resources/openapi.yaml`
- [ ] The spec is valid OpenAPI 3.0.3 YAML
- [ ] All Phase 1-4 endpoints are documented in the spec
- [ ] All reusable schemas (DataObject, ErrorResponse, etc.) are defined in `components/schemas`
- [ ] `GET /v1/openapi.yaml` serves the spec with `Content-Type: application/x-yaml`
- [ ] The spec response includes `Cache-Control` header
- [ ] Response JSON shapes in the spec match the design document section 4
- [ ] All unit tests pass (YAML parsing, schema presence, endpoint presence)
- [ ] Integration test confirms the spec is served by a running broker

---

## File Manifest

| File | Status |
|------|--------|
| `http-server/src/main/resources/openapi.yaml` | |
| `http-server/src/main/scala/kafka/server/http/HttpRouter.scala` | |
| `http-server/src/main/scala/kafka/network/HttpRequestHandler.scala` | |
| `http-server/src/test/scala/kafka/server/http/OpenApiSpecTest.scala` | |
| `http-server/src/test/scala/kafka/server/http/HttpOpenApiIntegrationTest.scala` | |
