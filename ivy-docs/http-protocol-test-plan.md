# HTTP Protocol Test Plan

Test strategy for the HTTP protocol layer described in `http-protocol-design.md`.
Leverages existing Kafka test infrastructure wherever possible.

## Table of Contents

1. [Test Pyramid](#1-test-pyramid)
2. [Test Infrastructure](#2-test-infrastructure)
3. [Layer 1: Unit Tests](#3-layer-1-unit-tests)
4. [Layer 2: Single-Broker Integration Tests](#4-layer-2-single-broker-integration-tests)
5. [Layer 3: Multi-Broker / E2E Tests](#5-layer-3-multi-broker--e2e-tests)
6. [Phase 3–4 Tests](#6-phase-34-tests)
7. [Test File Layout](#7-test-file-layout)
8. [Reuse Matrix](#8-reuse-matrix)

---

## 1. Test Pyramid

```
                      ┌───────────────────────┐
                      │   E2E / Multi-Broker   │   5 classes, ~30 tests
                      │  (real HTTP + 3 nodes) │
                  ┌───┴───────────────────────┴───┐
                  │  Integration (single broker)   │   8 classes, ~60 tests
                  │  (Jetty HttpClient + broker)   │
              ┌───┴───────────────────────────────┴───┐
              │  Unit Tests (mocked dependencies)      │  12 classes, ~110 tests
              │  (Mockito + JUnit 5, no real broker)   │
              └───────────────────────────────────────┘
```

Estimated total: **~25 test classes, ~200 test methods**.

---

## 2. Test Infrastructure

### 2.1 What we reuse from Kafka

| Existing class | Location | Reuse for |
|---|---|---|
| `IntegrationTestHarness` | `core/src/test/scala/integration/kafka/api/` | Base for HTTP integration tests — add HTTP listener in `configureListeners()` |
| `KafkaServerTestHarness` | `core/src/test/scala/unit/kafka/integration/` | Single-broker HTTP acceptor tests |
| `BaseQuotaTest` | `core/src/test/scala/integration/kafka/api/` | HTTP quota/throttle tests — override client factory |
| `KafkaApisTest` mock pattern | `core/src/test/scala/unit/kafka/server/` | Mock `RequestChannel` + dependencies for handler unit tests |
| `SocketServerTest` patterns | `core/src/test/scala/unit/kafka/network/` | Adapt for `HttpAcceptorTest` — connection limits, timeouts, metrics |
| `TransactionMarkerChannelManagerTest` | `core/src/test/scala/unit/kafka/coordinator/transaction/` | Mock `NetworkClient` + `InterBrokerSendThread` for forward thread tests |
| `TestUtils` | `core/src/test/scala/unit/kafka/utils/` | `createBrokerConfig`, `createTopic`, `waitUntilTrue`, `tempDir` |
| `JaasTestUtils` | `core/src/test/java/kafka/security/` | SASL/SSL security setup for HTTPS tests |
| Jetty `HttpClient` | `gradle/dependencies.gradle` (v12.0.34) | HTTP client for integration tests (already in Connect's dep tree) |
| `@ClusterTest` extension | `test-common/test-common-runtime/` | Parameterized multi-broker HTTP tests |

### 2.2 New test infrastructure to build

#### `HttpIntegrationTestHarness`

Base class for all HTTP integration tests. Extends `IntegrationTestHarness`, adds HTTP
listener automatically, exposes `httpBootstrapUrl`.

```scala
// http-server/src/test/scala/integration/kafka/http/HttpIntegrationTestHarness.scala

abstract class HttpIntegrationTestHarness extends IntegrationTestHarness {

  // Add HTTP listener alongside default binary listener
  override def configureListeners(props: Seq[Properties]): Unit = {
    super.configureListeners(props)
    props.foreach { config =>
      val existing = config.getProperty(SocketServerConfigs.LISTENERS_CONFIG, "")
      config.setProperty(SocketServerConfigs.LISTENERS_CONFIG,
        existing + ",HTTP://localhost:0")
      val existingMap = config.getProperty(SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG, "")
      config.setProperty(SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG,
        existingMap + ",HTTP:HTTP")
    }
  }

  protected def httpPort(brokerId: Int): Int =
    brokers.find(_.config.brokerId == brokerId)
      .map(_.boundPort(new ListenerName("HTTP")))
      .getOrElse(throw new IllegalStateException(s"Broker $brokerId not found"))

  protected def httpUrl(brokerId: Int): String =
    s"http://localhost:${httpPort(brokerId)}"

  protected def anyHttpUrl: String = httpUrl(aliveBrokers.head.config.brokerId)
}
```

#### `HttpTestClient`

Thin wrapper around Jetty `HttpClient` with Kafka-specific helpers.

```scala
// http-server/src/test/scala/integration/kafka/http/HttpTestClient.scala

class HttpTestClient(baseUrl: String) extends AutoCloseable {

  private val httpClient = new org.eclipse.jetty.client.HttpClient()
  private val mapper = new ObjectMapper()
  httpClient.start()

  def produce(topic: String, body: String): ContentResponse =
    httpClient.POST(s"$baseUrl/v1/topics/$topic/records")
      .headers(h => h.put("Content-Type", "application/json"))
      .body(new StringRequestContent("application/json", body))
      .send()

  def fetch(topic: String, body: String): ContentResponse =
    httpClient.POST(s"$baseUrl/v1/topics/$topic/records:fetch")
      .headers(h => h.put("Content-Type", "application/json"))
      .body(new StringRequestContent("application/json", body))
      .send()

  def getTopicMetadata(topic: String): ContentResponse =
    httpClient.GET(s"$baseUrl/v1/topics/$topic")

  def listTopics(): ContentResponse =
    httpClient.GET(s"$baseUrl/v1/topics")

  def listOffsets(topic: String, partition: Int, timestamp: String): ContentResponse =
    httpClient.GET(s"$baseUrl/v1/topics/$topic/partitions/$partition/offsets?timestamp=$timestamp")

  def consumerGroupLag(group: String): ContentResponse =
    httpClient.GET(s"$baseUrl/v1/consumer-groups/$group/lags")

  def health(): ContentResponse =
    httpClient.GET(s"$baseUrl/v1/health")

  def commitOffsets(group: String, body: String): ContentResponse =
    httpClient.POST(s"$baseUrl/v1/consumer-groups/$group/offsets")
      .headers(h => h.put("Content-Type", "application/json"))
      .body(new StringRequestContent("application/json", body))
      .send()

  def fetchOffsets(group: String, topic: String): ContentResponse =
    httpClient.GET(s"$baseUrl/v1/consumer-groups/$group/offsets?topic=$topic")

  def parseJson(response: ContentResponse): JsonNode =
    mapper.readTree(response.getContentAsString)

  override def close(): Unit = httpClient.stop()
}
```

---

## 3. Layer 1: Unit Tests

No real broker. All Kafka dependencies mocked with Mockito.

---

### 3.1 `HttpRequestTranslatorTest`

**Design ref:** §4.1, §4.2, §5.3, §5.4, §5.5A, §14.4

**Pattern:** Pure function tests — JSON in, `(ApiKey, ByteBuffer)` out.

| # | Test | Assertion |
|---|---|---|
| 1 | Valid produce: single record with STRING value | Returns `ApiKeys.PRODUCE`, buffer deserializes to `ProduceRequest` with correct partition + record bytes |
| 2 | Valid produce: BINARY value (base64) | Decoded bytes match original |
| 3 | Valid produce: JSON value | Serialized as UTF-8 JSON bytes |
| 4 | Valid produce: NULL value | Record value is null |
| 5 | Valid produce: record headers | Headers propagated to `RecordHeaders` |
| 6 | `acks` mapping: `"all"` → -1, `"leader"` → 1, `"none"` → 0 | `ProduceRequestData.acks()` matches |
| 7 | `timeoutMs` mapping | `ProduceRequestData.timeoutMs()` matches |
| 8 | Partition assignment: keyed record → `murmur2(key) % numPartitions` | Same hash as Java `DefaultPartitioner` |
| 9 | Partition assignment: keyless records → same batch-sticky partition | All records in batch get identical partition |
| 10 | Partition assignment: `AtomicInteger` overflow (`0x7fffffff` mask) | No negative partition after `Integer.MAX_VALUE` wraps |
| 11 | Valid fetch: multiple partitions with offsets | Returns `ApiKeys.FETCH`, buffer deserializes to `FetchRequest` with correct offsets |
| 12 | Fetch: `maxWaitMs` clamped to `http.consume.max.wait.ms` | `FetchRequestData.maxWaitMs()` ≤ cap |
| 13 | Fetch: `maxWaitMs` = 0 → accepted (edge) | No exception |
| 14 | **Reject:** empty body | `InvalidRequestException` |
| 15 | **Reject:** `records` array null or empty | `InvalidRequestException` |
| 16 | **Reject:** records count > `http.produce.max.records` | `InvalidRequestException`, maps to 413 |
| 17 | **Reject:** `partition` < 0 | `InvalidRequestException` |
| 18 | **Reject:** `acks` = `"two"` (invalid) | `InvalidRequestException` |
| 19 | **Reject:** `timeoutMs` ≤ 0 | `InvalidRequestException` |
| 20 | **Reject:** BINARY value is not valid base64 | `InvalidRequestException` |
| 21 | **Reject:** `maxWaitMs` < 0 in fetch | `InvalidRequestException` |

---

### 3.2 `HttpResponseSerializerTest`

**Design ref:** §4.1 response, §4.2 response, §11.1, §11.2, §14.7

| # | Test | Assertion |
|---|---|---|
| 1 | `ProduceResponse` → JSON: all partitions success | `200`, `offsets` array with correct partition/offset/errorCode |
| 2 | `ProduceResponse` → JSON: partial failure | `207`, per-partition `errorCode` populated |
| 3 | `FetchResponse` → JSON: records with STRING value | `type: "STRING"`, `data` is the decoded string |
| 4 | `FetchResponse` → JSON: records with binary value (non-UTF8) | `type: "BINARY"`, `data` is base64 |
| 5 | `FetchResponse` → JSON: records with null value | `type: "NULL"` |
| 6 | `FetchResponse` → JSON: records with `_content-type: application/json` header | `type: "JSON"`, `data` is parsed JSON object |
| 7 | `FetchResponse` → JSON: control chars in value → BINARY fallback | `type: "BINARY"` when value contains `0x01` |
| 8 | `FetchResponse` → JSON: record headers serialized | `headers` array with name/value pairs |
| 9 | `FetchResponse` → JSON: `highWatermark` per partition | Present in JSON |
| 10 | `FetchResponse` → JSON: empty partition (no records) | `records: []`, `errorCode: 0` |
| 11 | `MetadataResponse` → topic metadata JSON | Partitions with leader, replicas, ISR |
| 12 | `ListOffsetsResponse` → JSON | `partition`, `offset`, `timestamp` |
| 13 | Error mapping: every Kafka `Errors` value → correct HTTP status | Loop over §11.1 table |
| 14 | Error response: `Retry-After` header on 429/503/504 | Header present with correct value |
| 15 | Error response: JSON body has `errorCode`, `errorMessage`, `detail` | All fields populated |

---

### 3.3 `HttpRouterTest`

**Design ref:** §8.5, §14.4

| # | Test | Assertion |
|---|---|---|
| 1 | `POST /v1/topics/orders/records` → produce handler | Correct handler matched |
| 2 | `POST /v1/topics/orders/records:fetch` → consume handler | Correct handler matched |
| 3 | `GET /v1/topics/orders` → metadata handler | Correct handler matched |
| 4 | `GET /v1/topics/orders/partitions/0/offsets?timestamp=earliest` → offsets handler | Correct handler + query param parsed |
| 5 | `GET /v1/consumer-groups/my-group/lags` → lag handler | Correct handler matched |
| 6 | `GET /v1/health` → health handler | Direct response, no RequestChannel |
| 7 | `GET /v1/topics` → list topics handler | Correct handler matched |
| 8 | `GET /v1/nonexistent` → 404 | Not found response |
| 9 | Topic name with URL-encoded chars: `my%2Dtopic` → `my-topic` | Decoded correctly |
| 10 | **Reject:** topic name with `/`: `orders%2Fsubpath` | 400 `INVALID_REQUEST` |
| 11 | **Reject:** topic name with null byte: `orders%00` | 400 `INVALID_REQUEST` |
| 12 | **Reject:** topic name `.` or `..` | 400 `INVALID_TOPIC_EXCEPTION` |
| 13 | **Reject:** topic name > 249 chars | 400 `INVALID_TOPIC_EXCEPTION` |
| 14 | `X-Kafka-Client-ID: my-app` → clientId = `"my-app"` | Passed through |
| 15 | No `X-Kafka-Client-ID` → clientId = `"http-client"` | Default applied |
| 16 | **Reject:** `X-Kafka-Client-ID` with illegal chars | 400 `INVALID_REQUEST` |

---

### 3.4 `HttpProcessorTest`

**Design ref:** §8.6, §12.3

**Pattern:** Mock `RequestChannel`, create real `HttpProcessor`, verify response routing.

| # | Test | Assertion |
|---|---|---|
| 1 | `registerChannel` + `enqueueResponse(SendResponse)` → writes to correct Netty ctx | `ctx.writeAndFlush()` called with JSON |
| 2 | Response for disconnected channel → silently dropped | No exception, channel removed |
| 3 | `CloseConnectionResponse` → `ctx.close()` called | Channel closed |
| 4 | `StartThrottlingResponse` → HTTP 429 with `Retry-After` | Status 429, header present |
| 5 | `EndThrottlingResponse` → no-op | Nothing written |
| 6 | Channel close listener fires → entry removed from `channels` map | `channels.size()` decremented |
| 7 | Multiple concurrent channels → each gets correct response | `connectionId`-based routing |
| 8 | `processResponses()` drains entire queue in one batch | All responses processed |

---

### 3.5 `ProduceForwardThreadTest`

**Design ref:** §7.2, §14.6

**Pattern:** `TransactionMarkerChannelManagerTest` — mock `NetworkClient`, test
`InterBrokerSendThread` subclass.

| # | Test | Assertion |
|---|---|---|
| 1 | `enqueue()` → `generateRequests()` produces `ProduceRequest` | Request builder has correct partitions, acks, timeout |
| 2 | Successful response → `future.complete()` with partition responses | Future resolved with correct offsets |
| 3 | Disconnected response → `future.completeExceptionally(DisconnectException)` | Future completed exceptionally |
| 4 | Bounded queue: enqueue when full → future completed exceptionally | `offer()` returns false, immediate rejection |
| 5 | `wakeup()` called after enqueue | `networkClient.wakeup()` invoked |
| 6 | Multiple pending entries drained in one `generateRequests()` call | Batch size matches queue depth |

---

### 3.6 `FetchForwardThreadTest`

**Design ref:** §7.6

| # | Test | Assertion |
|---|---|---|
| 1 | `enqueue()` → `generateRequests()` produces `FetchRequest` | Correct partitions, offsets, `maxWaitMs` |
| 2 | Successful response → `future.complete()` with `FetchPartitionData` | Records, highWatermark populated |
| 3 | Disconnected → `future.completeExceptionally` | Exception type matches |
| 4 | Bounded queue full → immediate rejection | Same as produce pattern |

---

### 3.7 `ProduceForwardManagerTest`

**Design ref:** §7.3

| # | Test | Assertion |
|---|---|---|
| 1 | `forward(leaderId=1)` → creates thread for broker 1 | `threads.size() == 1` |
| 2 | Second `forward(leaderId=1)` → reuses existing thread | `threads.size()` still 1 |
| 3 | `forward(leaderId=2)` → creates second thread | `threads.size() == 2` |
| 4 | `cleanupStaleThreads()`: broker 1 removed from MetadataCache → thread shutdown | `threads.size()` decremented |
| 5 | `cleanupStaleThreads()`: broker address changed → thread replaced | New thread created with new `Node` |
| 6 | `close()` → all threads shut down | `initiateShutdown()` + `awaitShutdown()` called on each |

---

### 3.8 `FetchForwardManagerTest`

Same structure as `ProduceForwardManagerTest`, adapted for fetch.

---

### 3.9 `HttpAuthenticationContextTest`

**Design ref:** §12.1

| # | Test | Assertion |
|---|---|---|
| 1 | mTLS: `SslHandler` present → `X509Certificate[]` accessible | `peerCertificates()` returns certs |
| 2 | Bearer token: `Authorization: Bearer abc123` → token extracted | `bearerToken()` = `"abc123"` |
| 3 | Basic auth: `Authorization: Basic dXNlcjpwYXNz` → decoded | `credentials()` = `("user", "pass")` |
| 4 | No auth header → empty | `bearerToken()` = `null` |
| 5 | `securityProtocol()` returns `HTTP` or `HTTPS` | Matches endpoint |
| 6 | `clientAddress()` returns remote IP | Extracted from Netty ctx |

---

### 3.10 `HttpRequestHandlerTest`

**Design ref:** §8.5, §14.2, §14.3

**Pattern:** Mock `HttpProcessor`, `RequestChannel`, Netty `ChannelHandlerContext`.

| # | Test | Assertion |
|---|---|---|
| 1 | Valid produce request → enqueued to `RequestChannel` | `tryEnqueue()` called with correct `Request` |
| 2 | `RequestContext` has `securityProtocol = HTTP` | Verified on captured request |
| 3 | `RequestContext.connectionId` = Netty channel ID | Matches `ctx.channel().id().asLongText()` |
| 4 | `X-Request-ID` header → propagated to request | Stored for response write-back |
| 5 | No `X-Request-ID` → UUID generated | Non-null request ID |
| 6 | Health check → direct response, no `RequestChannel` | `tryEnqueue()` not called |
| 7 | `tryEnqueue()` returns false → HTTP 503 + `Retry-After: 1` | Error response written to ctx |
| 8 | Channel close before response → `pendingCtx` cleaned up | No leak |

---

## 4. Layer 2: Single-Broker Integration Tests

Real broker with HTTP listener. Uses Jetty `HttpClient`.

---

### 4.1 `HttpAcceptorTest` — extends `KafkaServerTestHarness`

**Design ref:** §8.2, §8.3, §8.4, §14.5, §14.8, §14.11

**Pattern:** `SocketServerTest` — creates SocketServer, tests connection lifecycle.

| # | Test | Assertion |
|---|---|---|
| 1 | HTTP listener binds and accepts TCP connections | Socket connects on HTTP port |
| 2 | `GET /v1/health` → 200 when broker running | `{"status":"RUNNING","brokerId":0,...}` |
| 3 | `GET /v1/health` → 503 during shutdown | Status ≠ `RUNNING` |
| 4 | Oversized request body > `http.request.max.bytes` → 413 | `HttpObjectAggregator` rejects |
| 5 | Idle connection closed after `http.connection.idle.timeout.ms` | Connection reset after timeout |
| 6 | Keep-alive: second request on same connection succeeds | `Connection: keep-alive` works |
| 7 | Response includes `X-Kafka-Request-ID` header | Non-null UUID |
| 8 | Client sends `X-Request-ID: abc` → echoed in response | `X-Kafka-Request-ID: abc` |
| 9 | Invalid HTTP method on produce endpoint → 405 | Method not allowed |
| 10 | `Content-Type` not `application/json` → 415 | Unsupported media type |

---

### 4.2 `HttpProduceIntegrationTest` — extends `HttpIntegrationTestHarness`

**Design ref:** §4.1, §5.1, §5.3, §5.4, §5.5

**Pattern:** `BaseProducerSendTest` — 2 brokers, create topics, produce records, verify offsets.

| # | Test | Assertion |
|---|---|---|
| 1 | Single record, explicit partition → 200, offset returned | `offsets[0].offset >= 0`, `errorCode = 0` |
| 2 | Multiple records, same partition → 200, sequential offsets | Offsets are contiguous |
| 3 | Multiple records, different partitions → 200, per-partition offsets | Each partition has its own offset |
| 4 | Keyless records → all assigned same batch-sticky partition | All `offsets[].partition` equal |
| 5 | Keyed records → deterministic partition (murmur2) | Same key always → same partition |
| 6 | `acks: "all"` → waits for ISR | Offset returned after ISR ack |
| 7 | `acks: "leader"` → returns faster | Offset returned |
| 8 | `acks: "none"` → offset is -1 (fire-and-forget) | `offset = -1` |
| 9 | STRING value round-trip: produce then fetch | Value matches |
| 10 | BINARY value round-trip: produce base64, fetch base64 | Decoded bytes match |
| 11 | JSON value round-trip: produce JSON object, fetch JSON | Object matches |
| 12 | NULL value round-trip | `type: "NULL"` on fetch |
| 13 | Record headers round-trip | Headers match |
| 14 | Non-existent topic → 404 `UNKNOWN_TOPIC_OR_PARTITION` | HTTP 404 |
| 15 | Record exceeds `max.message.bytes` → 413 | `MESSAGE_TOO_LARGE` |
| 16 | Records count exceeds `http.produce.max.records` → 413 | Rejected before enqueue |
| 17 | `timeoutMs: 1` with `acks: "all"` and slow ISR → 504 | `REQUEST_TIMED_OUT` |

---

### 4.3 `HttpConsumeIntegrationTest` — extends `HttpIntegrationTestHarness`

**Design ref:** §4.2, §6.1–6.9

**Pattern:** `AbstractConsumerTest` — produce first, then consume via HTTP.

| # | Test | Assertion |
|---|---|---|
| 1 | Fetch with explicit offset → returns records from that offset | First record offset matches request |
| 2 | Fetch multiple partitions → aggregated response | Each partition has its own `records` array |
| 3 | Fetch with no data → empty `records: []` | Not an error, `errorCode: 0` |
| 4 | `maxWaitMs` capped: client sends 30000, cap is 5000 | `X-Kafka-MaxWait-Applied: 5000` header |
| 5 | `maxWaitMs` below cap: client sends 1000 | `X-Kafka-MaxWait-Applied: 1000` |
| 6 | `isolationLevel: "READ_COMMITTED"` skips uncommitted txn records | Uncommitted records not returned |
| 7 | `isolationLevel: "READ_UNCOMMITTED"` returns all records | All records returned |
| 8 | `maxBytes` limits total response size | Response ≤ `maxBytes` |
| 9 | `maxBytesPerPartition` limits per-partition data | Each partition ≤ limit |
| 10 | Fetch from non-existent topic → 404 | `UNKNOWN_TOPIC_OR_PARTITION` |
| 11 | Fetch from non-existent partition → per-partition error | `errorCode` set on that partition |
| 12 | `highWatermark` is populated per partition | Non-negative value |
| 13 | Record `timestamp` is populated | Matches produce-time timestamp |
| 14 | Poll loop pattern: fetch, advance offset, fetch again → next records | Progressive consumption |

---

### 4.4 `HttpMetadataIntegrationTest` — extends `HttpIntegrationTestHarness`

**Design ref:** §4.3.1–4.3.5

| # | Test | Assertion |
|---|---|---|
| 1 | `GET /v1/topics/orders` → partitions with leader, replicas, ISR | All fields populated, leader is a live broker |
| 2 | `GET /v1/topics/nonexistent` → 404 | Topic not found |
| 3 | `GET /v1/topics` → list of created topics | Contains test topics |
| 4 | `GET /v1/topics` with ACLs → only authorized topics | Unauthorized topics excluded |
| 5 | `GET /v1/topics/t/partitions/0/offsets?timestamp=earliest` → first offset | `offset >= 0` |
| 6 | `GET /v1/topics/t/partitions/0/offsets?timestamp=latest` → log-end offset | `offset >= 0`, ≥ earliest |
| 7 | `GET /v1/topics/t/partitions/0/offsets?timestamp=<epoch>` → offset at timestamp | Correct offset |
| 8 | `GET /v1/topics/t/partitions/0/offsets?timestamp=max` → max timestamp offset | Valid offset |
| 9 | `GET /v1/consumer-groups/g/lags` → lag per partition | `lag = logEndOffset - committedOffset`, clamped ≥ 0 |
| 10 | Lag for group with no committed offsets → lag = logEndOffset | `committedOffset = -1` |

---

### 4.5 `HttpErrorHandlingTest` — extends `HttpIntegrationTestHarness`

**Design ref:** §11.1, §11.2

| # | Test | Assertion |
|---|---|---|
| 1 | Every 503 response has `Retry-After` header | Header present |
| 2 | Every 429 response has `Retry-After` header | Header present with throttle-derived value |
| 3 | 504 response has `Retry-After: 1` | Header present |
| 4 | 400/403/404/413 do NOT have `Retry-After` | Header absent |
| 5 | Error response body has `errorCode`, `errorMessage` | JSON structure correct |
| 6 | All responses have `Content-Type: application/json` | Header set |
| 7 | All responses have `X-Kafka-Request-ID` | Header present |
| 8 | Multi-partition partial failure → 207 Multi-Status | Status 207, per-partition errors |
| 9 | All partitions fail with same error → direct error status (not 207) | e.g. 403 if all auth failed |

---

## 5. Layer 3: Multi-Broker / E2E Tests

3+ brokers, tests forwarding, failover, quotas, security.

---

### 5.1 `HttpForwardingIntegrationTest` — extends `HttpIntegrationTestHarness`

**Design ref:** §5.1, §5.2, §5.5C, §7.4, §7.5

**Setup:** 3 brokers, topic with 3 partitions, RF=3.

| # | Test | Assertion |
|---|---|---|
| 1 | Produce to follower broker → forwarded to leader → offset returned | Same result as producing to leader |
| 2 | Consume from follower broker → forwarded to leader → records returned | Same records as fetching from leader |
| 3 | Multi-partition produce: some local, some forwarded → unified response | All partitions have offsets |
| 4 | Multi-partition consume: some local, some forwarded → unified response | All partitions have records |
| 5 | Leader failover during produce → retry once → success | Offset returned after retry |
| 6 | Leader unknown for one partition → 503 for that partition only | Other partitions succeed (207) |
| 7 | Forward timeout → 504 for affected partitions | Other partitions still returned |
| 8 | All 3 brokers produce same results for same request | Any-broker routing is transparent |
| 9 | Produce with `acks: "all"` through forwarding → ISR ack respected | Offset confirmed after ISR |
| 10 | Forward queue full → 503 for affected partitions | `Retry-After` header present |

---

### 5.2 `HttpQuotaIntegrationTest` — extends `BaseQuotaTest` + `HttpIntegrationTestHarness`

**Design ref:** §12.3

**Setup:** 2 brokers, low quotas (produce=8000 bytes/s, consume=2500 bytes/s).

| # | Test | Assertion |
|---|---|---|
| 1 | Produce until throttled → HTTP 429 | `Retry-After` header present, `throttleTimeMs` in body |
| 2 | Consume until throttled → HTTP 429 | Same |
| 3 | `X-Kafka-Client-ID: my-app` → throttled under `my-app` quota | Quota entity matches |
| 4 | No `X-Kafka-Client-ID` → throttled under `http-client` default | Default clientId used |
| 5 | Override quota for `http-client` → new limit applied | Throttle threshold changes |
| 6 | `http.queue.full.rate` metric increments on queue saturation | Metric observable |

---

### 5.3 `HttpSecurityIntegrationTest`

**Design ref:** §12.1, §12.2

**Setup:** HTTPS listener with mTLS, ACL authorizer enabled.

| # | Test | Assertion |
|---|---|---|
| 1 | HTTPS + valid client cert → 200 | Principal extracted from cert CN |
| 2 | HTTPS + no client cert → 401 or ANONYMOUS | Depends on `ssl.client.auth` config |
| 3 | HTTPS + expired cert → connection rejected | TLS handshake fails |
| 4 | Produce without WRITE ACL on topic → 403 | `TOPIC_AUTHORIZATION_FAILED` |
| 5 | Consume without READ ACL on topic → 403 | `TOPIC_AUTHORIZATION_FAILED` |
| 6 | List topics: only DESCRIBE-authorized topics returned | Unauthorized topics excluded |
| 7 | Consumer group lag without DESCRIBE on group → 403 | `GROUP_AUTHORIZATION_FAILED` |
| 8 | `KafkaPrincipal` flows through to `Authorizer` unchanged | Same ACL enforcement as binary |
| 9 | HTTP listener (not HTTPS) → `ANONYMOUS` principal | No auth required |

---

### 5.4 `HttpGracefulShutdownTest` — extends `HttpIntegrationTestHarness`

**Design ref:** §14.8

| # | Test | Assertion |
|---|---|---|
| 1 | In-flight produce completes during drain window | Response received before socket close |
| 2 | New request after `beginDrain()` → 503 | Immediate rejection |
| 3 | Drain timeout expires → connections forcibly closed | No hanging connections |
| 4 | Forwarding threads shut down during close | `ProduceForwardManager.close()` completes |
| 5 | `httpAsyncExecutor` shuts down cleanly | No thread leak |

---

### 5.5 `HttpMetricsIntegrationTest` — extends `HttpIntegrationTestHarness`

**Design ref:** §14.9

| # | Test | Assertion |
|---|---|---|
| 1 | HTTP produce → `RequestsPerSec{protocol=http,request=produce}` increments | Metric > 0 |
| 2 | HTTP consume → `RequestsPerSec{protocol=http,request=consume}` increments | Metric > 0 |
| 3 | Binary produce → no increment on HTTP metric | HTTP metric unchanged |
| 4 | `http.forward.queue.size` gauge reflects pending forwards | Gauge readable |
| 5 | `http.idle.connections.closed.rate` increments on idle timeout | Metric > 0 after idle |
| 6 | `http.queue.full.rate` = 0 under normal load | No spurious rejections |
| 7 | Per-ApiKey metrics (`TotalTimeMs`, `RequestQueueTimeMs`) include HTTP requests | Existing metrics still work |

---

## 6. Phase 3–4 Tests

### 6.1 `HttpOffsetCommitIntegrationTest` (Phase 3)

**Design ref:** §4.4

| # | Test | Assertion |
|---|---|---|
| 1 | Commit offsets → 200 | Per-partition `errorCode: 0` |
| 2 | Fetch committed offsets → matches committed values | Offsets round-trip |
| 3 | Committed offsets visible via `kafka-consumer-groups.sh` | CLI shows group + offsets |
| 4 | Lag endpoint uses committed offsets | `committedOffset` matches |
| 5 | Commit with `generationId = -1` (simple consumer) → works without group membership | No `ILLEGAL_GENERATION` error |
| 6 | Commit to non-existent topic → per-partition error | `UNKNOWN_TOPIC_OR_PARTITION` |
| 7 | Commit without READ ACL on group → 403 | Authorization enforced |
| 8 | Recovery pattern: commit, restart client, fetch offsets, resume consuming | Offsets survive restart |

### 6.2 `HttpShareGroupIntegrationTest` (Phase 4)

**Design ref:** §4.5

| # | Test | Assertion |
|---|---|---|
| 1 | Share group poll → returns records with `acquireId` | Non-null acquire IDs |
| 2 | Acknowledge ACCEPT → records not re-delivered | Next poll returns different records |
| 3 | Acknowledge REJECT → records re-delivered to another consumer | Second consumer sees rejected records |
| 4 | No acknowledgement within timeout → auto re-deliver | Records appear on next poll |
| 5 | Multiple HTTP clients in same share group → load balanced | Both clients receive records |
| 6 | Acknowledge is idempotent | Double-ack same `acquireId` → no error |

---

## 7. Test File Layout

```
http-server/src/test/
├── java/kafka/server/http/
│   ├── ProduceForwardThreadTest.java
│   ├── ProduceForwardManagerTest.java
│   ├── FetchForwardThreadTest.java
│   ├── FetchForwardManagerTest.java
│   └── HttpProcessorTest.java
├── scala/kafka/
│   ├── network/
│   │   ├── HttpAcceptorTest.scala
│   │   ├── HttpRequestHandlerTest.scala
│   │   └── HttpAuthenticationContextTest.scala
│   └── server/http/
│       ├── HttpRequestTranslatorTest.scala
│       ├── HttpResponseSerializerTest.scala
│       └── HttpRouterTest.scala
└── scala/integration/kafka/http/
    ├── HttpIntegrationTestHarness.scala          ← shared base class
    ├── HttpTestClient.scala                      ← Jetty-based HTTP helper
    ├── HttpProduceIntegrationTest.scala
    ├── HttpConsumeIntegrationTest.scala
    ├── HttpMetadataIntegrationTest.scala
    ├── HttpErrorHandlingTest.scala
    ├── HttpForwardingIntegrationTest.scala
    ├── HttpQuotaIntegrationTest.scala
    ├── HttpSecurityIntegrationTest.scala
    ├── HttpGracefulShutdownTest.scala
    ├── HttpMetricsIntegrationTest.scala
    ├── HttpOffsetCommitIntegrationTest.scala      ← phase 3
    └── HttpShareGroupIntegrationTest.scala        ← phase 4
```

---

## 8. Reuse Matrix

Summary of existing Kafka test infrastructure reused vs. new code needed.

| Test area | Existing to reuse | New to build |
|---|---|---|
| **Broker lifecycle** | `IntegrationTestHarness`, `KafkaServerTestHarness`, `QuorumTestHarness` | `HttpIntegrationTestHarness` (thin wrapper adding HTTP listener) |
| **HTTP client** | Jetty `HttpClient` (in Connect dependency tree) | `HttpTestClient` (thin helper with `produce()`, `fetch()`, etc.) |
| **Request mocking** | `KafkaApisTest` Mockito pattern, `MockClient` | Adapt for `HttpRequestHandler` / `HttpProcessor` |
| **Forward thread** | `TransactionMarkerChannelManagerTest` pattern | Copy for `ProduceForwardThread` / `FetchForwardThread` |
| **Quotas** | `BaseQuotaTest`, `QuotaTestClients` | Override to produce/consume via HTTP |
| **Security** | `JaasTestUtils`, `SaslSetup`, ACL helpers in `KafkaServerTestHarness` | `HttpAuthenticationContext` tests |
| **Network layer** | `SocketServerTest` patterns | Adapt for HTTP acceptor connection tests |
| **Topic/broker setup** | `TestUtils.createTopic()`, `TestUtils.createBrokerConfig()` | Nothing — use as-is |
| **Assertions** | `TestUtils.waitUntilTrue()`, JUnit 5 assertions | Nothing — use as-is |
| **Cluster parameterization** | `@ClusterTest` extension, `KafkaClusterTestKit` | Extend for HTTP-aware cluster configs |
| **Metrics verification** | `Metrics` instance access via broker, Yammer metrics | Add HTTP-specific metric name constants |

**Estimated split:** ~70% reuse of existing infrastructure, ~30% new test code (mostly
`HttpTestClient`, `HttpIntegrationTestHarness`, and the test methods themselves).

---

*Test plan version: 1.0 — 2026-04-16*
*Companion to: http-protocol-design.md v0.6*
