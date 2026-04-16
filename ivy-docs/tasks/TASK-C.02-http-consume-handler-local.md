# TASK-C.02: handleHttpConsumeRequest -- Local Leader Path

## Prerequisites

| Task | What it delivers | Why this task needs it |
|------|------------------|-----------------------|
| TASK-B.06 | KafkaApis dispatch wiring (`securityProtocol.isHttp` branch routing `FETCH` to `handleHttpConsumeRequest`) | Without dispatch, HTTP fetch requests never reach this handler |
| TASK-B.01 | `HttpRequestTranslator` (JSON body -> `FetchRequest`) | The handler receives a `RequestChannel.Request` whose body is a `FetchRequest` built by the translator |
| TASK-B.02 | `HttpResponseSerializer` (Kafka response -> JSON, including `X-Kafka-MaxWait-Applied` header) | The response path converts `FetchResponse` to JSON; this handler sets the header value |

## Context

This task implements the **local leader path** of HTTP consume handling in `KafkaApis`. When an HTTP client sends `POST /v1/topics/{topic}/records:fetch`, the request is translated to a standard `FetchRequest` by `HttpRequestTranslator`, placed on `RequestChannel`, and dispatched to `handleHttpConsumeRequest()`.

The handler must:
1. Clamp `effectiveMaxWaitMs = min(request.maxWaitMs, config.httpConsumeMaxWaitMs)` per section 6.1.
2. Set `X-Kafka-MaxWait-Applied` header on the response.
3. Authorize via `authHelper.filterByAuthorized(READ, TOPIC, ...)`.
4. For partitions where **this broker IS the leader**, call `replicaManager.fetchMessages()`.
5. For partitions where **this broker is NOT the leader**, return empty records with `LEADER_NOT_AVAILABLE` error (forwarding added in TASK-D.04).
6. Intentionally set `currentLeaderEpoch = -1` (no epoch checks, per section 6.6).

### Key references in the codebase

- **Existing fetch handler**: `KafkaApis.handleFetchRequest()` at `core/src/main/scala/kafka/server/KafkaApis.scala` line 564. This task follows the same authorization and validation pattern.
- **ReplicaManager.fetchMessages()**: at `core/src/main/scala/kafka/server/ReplicaManager.scala` line 1664. The `responseCallback` fires when data is available or `maxWaitMs` expires (via `DelayedFetch` purgatory).
- **FetchParams**: at `server-common/src/main/java/org/apache/kafka/server/storage/log/FetchParams.java`. Constructor: `FetchParams(replicaId, replicaEpoch, maxWaitMs, minBytes, maxBytes, isolation, clientMetadata)`.
- **Design doc sections**: 6 (Consume Path), 6.1 (maxWaitMs cap), 6.2 (Fan-out), 6.5 (Delayed fetch), 6.6 (Intentional simplifications), 14.1 (Async completion).

## Specification

### Method signature

```scala
// In KafkaApis (core/src/main/scala/kafka/server/KafkaApis.scala)
def handleHttpConsumeRequest(request: RequestChannel.Request): Unit
```

### Behavior

1. Extract `FetchRequest` from `request.body[FetchRequest]`.
2. **maxWaitMs cap**: Compute `effectiveMaxWaitMs = Math.min(fetchRequest.maxWait, config.httpConsumeMaxWaitMs)`. Store this value to set the `X-Kafka-MaxWait-Applied` response header.
3. **Authorization**: Call `authHelper.filterByAuthorized(request.context, READ, TOPIC, partitionDatas)(_._1.topicPartition.topic)`.
4. **Partition classification**: For each authorized partition that exists in `metadataCache`, check if this broker is the leader:
   - **Local leader**: add to `interesting` list for `fetchMessages`.
   - **Remote leader**: add to `leaderNotAvailable` results with empty records and `LEADER_NOT_AVAILABLE` error code.
5. **Fetch**: Call `replicaManager.fetchMessages(params, interesting, quota, responseCallback)` with `params.maxWaitMs = effectiveMaxWaitMs`.
6. **FetchParams construction**: `replicaId = -1` (consumer), `replicaEpoch = -1`, `maxWaitMs = effectiveMaxWaitMs`, isolation from request.
7. **Response merge**: Combine fetch results with erroneous/unauthorized/leader-not-available partitions.
8. **Send response**: Via `requestChannel.sendResponse()`, always from `httpAsyncExecutor`.
9. Set `X-Kafka-MaxWait-Applied` header value on the response metadata so `HttpResponseSerializer` can emit it.

### No epoch checks

Per section 6.6: HTTP clients are stateless and do not track epochs. Set `currentLeaderEpoch = -1` on all partition fetch specs. This means "any epoch accepted" -- safe for consumption.

### No preferred read replica

Per section 6.6: Always fetch from the leader. Ignore `preferredReadReplica` in the response.

### Empty poll is not an error

When `DelayedFetch` purgatory expires with no data, the partition gets `records: []` with `errorCode: 0`. This is expected behavior -- the client polls again.

## Implementation Details

### File to modify

`/home/anh/kafka/core/src/main/scala/kafka/server/KafkaApis.scala`

### Where to add the method

Add `handleHttpConsumeRequest` as a new method in `KafkaApis`, adjacent to `handleFetchRequest` (after line ~777). The dispatch wiring (TASK-B.06) routes here when `securityProtocol.isHttp` and `apiKey == FETCH`.

### Communicating effectiveMaxWaitMs to the response serializer

The `X-Kafka-MaxWait-Applied` header must appear on the HTTP response. Options:
1. Stash `effectiveMaxWaitMs` on `request.context` via a custom property map (preferred).
2. Add a custom header in the `FetchResponse` data structure (invasive, avoid).
3. Store in a thread-local or request-scoped map keyed by correlation ID.

Recommended approach: use `request.requestLocalProperties` (a `Map[String, Any]` available on `RequestChannel.Request`) to pass `effectiveMaxWaitMs` to the response serializer:

```scala
request.requestLocalProperties.put("httpMaxWaitApplied", effectiveMaxWaitMs)
```

`HttpResponseSerializer` reads this value when constructing the HTTP response headers.

### FetchIsolation mapping

| HTTP `isolationLevel` | `FetchIsolation` |
|---|---|
| `"READ_UNCOMMITTED"` (default) | `FetchIsolation.LOG_END` |
| `"READ_COMMITTED"` | `FetchIsolation.TXN_COMMITTED` |

## Skeleton Code

```scala
// ============================================================================
// File: core/src/main/scala/kafka/server/KafkaApis.scala
// Add this method to the KafkaApis class body
// ============================================================================

import java.util
import java.util.concurrent.{CompletableFuture, ScheduledExecutorService, TimeUnit}
import java.util.function.BiFunction
import java.util.{Collections, Optional}
import org.apache.kafka.common.{TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.common.message.FetchResponseData
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.replica.ClientMetadata
import org.apache.kafka.common.requests.{FetchRequest, FetchResponse}
import org.apache.kafka.server.storage.log.{FetchIsolation, FetchParams, FetchPartitionData}

import scala.collection.mutable
import scala.jdk.CollectionConverters._

/**
 * Handles HTTP consume (fetch) requests -- LOCAL leader path only.
 *
 * For partitions where this broker is the leader, delegates to
 * ReplicaManager.fetchMessages(). For partitions with a remote leader,
 * returns empty records with LEADER_NOT_AVAILABLE (forwarding added in
 * TASK-D.04).
 *
 * Key behavioral differences from handleFetchRequest:
 *   - effectiveMaxWaitMs = min(request.maxWaitMs, http.consume.max.wait.ms)
 *   - No leader epoch checks (currentLeaderEpoch = -1)
 *   - No preferred read replica support
 *   - Response includes X-Kafka-MaxWait-Applied header
 *
 * CRITICAL: This method must NEVER block the KafkaRequestHandler thread.
 *
 * @see handleFetchRequest for the binary protocol equivalent
 * @see <a href="ivy-docs/http-protocol-design.md">Design doc section 6</a>
 */
def handleHttpConsumeRequest(request: RequestChannel.Request): Unit = {
  val fetchRequest = request.body[FetchRequest]
  val localBrokerId = config.brokerId

  // ---------------------------------------------------------------
  // 1. Clamp maxWaitMs per section 6.1
  // ---------------------------------------------------------------
  val effectiveMaxWaitMs: Int = Math.min(
    fetchRequest.maxWait,
    config.httpConsumeMaxWaitMs  // default 5000 ms
  )

  // Stash for HttpResponseSerializer to emit X-Kafka-MaxWait-Applied header
  // (HttpResponseSerializer reads this from the request properties)
  request.requestLocalProperties.put("httpMaxWaitApplied", effectiveMaxWaitMs.asInstanceOf[AnyRef])

  // ---------------------------------------------------------------
  // 2. Resolve topic names from IDs (same pattern as handleFetchRequest)
  // ---------------------------------------------------------------
  val topicNames: util.Map[Uuid, String] =
    if (fetchRequest.version() >= 13)
      metadataCache.topicIdsToNames()
    else
      Collections.emptyMap[Uuid, String]()

  val fetchData = fetchRequest.fetchData(topicNames)

  // ---------------------------------------------------------------
  // 3. Authorization: READ on each topic
  // ---------------------------------------------------------------
  val erroneous = mutable.ArrayBuffer[(TopicIdPartition, FetchResponseData.PartitionData)]()
  val interesting = mutable.ArrayBuffer[(TopicIdPartition, FetchRequest.PartitionData)]()
  val leaderNotAvailable = mutable.ArrayBuffer[(TopicIdPartition, FetchResponseData.PartitionData)]()

  val partitionDatas = new mutable.ArrayBuffer[(TopicIdPartition, FetchRequest.PartitionData)]
  fetchData.forEach { (topicIdPartition, partitionData) =>
    if (topicIdPartition.topic == null) {
      erroneous += topicIdPartition ->
        FetchResponse.partitionResponse(topicIdPartition, Errors.UNKNOWN_TOPIC_ID)
    } else {
      partitionDatas += topicIdPartition -> partitionData
    }
  }

  val authorizedTopics = authHelper.filterByAuthorized(
    request.context, READ, TOPIC, partitionDatas
  )(_._1.topicPartition.topic)

  partitionDatas.foreach { case (topicIdPartition, data) =>
    if (!authorizedTopics.contains(topicIdPartition.topic)) {
      erroneous += topicIdPartition ->
        FetchResponse.partitionResponse(topicIdPartition, Errors.TOPIC_AUTHORIZATION_FAILED)
    } else if (!metadataCache.contains(topicIdPartition.topicPartition)) {
      erroneous += topicIdPartition ->
        FetchResponse.partitionResponse(topicIdPartition, Errors.UNKNOWN_TOPIC_OR_PARTITION)
    } else {
      // ---------------------------------------------------------------
      // 4. Leader check
      // ---------------------------------------------------------------
      val isLocal = metadataCache.getPartitionInfo(
        topicIdPartition.topicPartition.topic,
        topicIdPartition.topicPartition.partition
      ).exists(_.leader == localBrokerId)

      if (isLocal) {
        interesting += topicIdPartition -> data
      } else {
        // Phase C: no forwarding yet -- return LEADER_NOT_AVAILABLE with empty records
        leaderNotAvailable += topicIdPartition ->
          FetchResponse.partitionResponse(topicIdPartition, Errors.LEADER_NOT_AVAILABLE)
      }
    }
  }

  // ---------------------------------------------------------------
  // 5. Response callback -- merges local results with errors
  // ---------------------------------------------------------------
  def processResponseCallback(
    responsePartitionData: Seq[(TopicIdPartition, FetchPartitionData)]
  ): Unit = {
    val partitions = new util.LinkedHashMap[TopicIdPartition, FetchResponseData.PartitionData]

    // Convert local fetch results
    responsePartitionData.foreach { case (topicIdPartition, data) =>
      val partitionData = new FetchResponseData.PartitionData()
        .setPartitionIndex(topicIdPartition.partition)
        .setErrorCode(data.error.code)
        .setHighWatermark(data.highWatermark)
        .setLastStableOffset(data.lastStableOffset.orElse(FetchResponse.INVALID_LAST_STABLE_OFFSET))
        .setLogStartOffset(data.logStartOffset)
        .setAbortedTransactions(data.abortedTransactions.orElse(null))
        .setRecords(data.records)
        .setPreferredReadReplica(FetchResponse.INVALID_PREFERRED_REPLICA_ID)
      partitions.put(topicIdPartition, partitionData)
    }

    // Add erroneous partitions (unauthorized, unknown topic)
    erroneous.foreach { case (tp, data) => partitions.put(tp, data) }

    // Add leader-not-available partitions
    leaderNotAvailable.foreach { case (tp, data) => partitions.put(tp, data) }

    // Quota handling
    val timeMs = time.milliseconds()
    val responseSize = partitions.values().asScala
      .map(p => if (p.records != null) p.records.sizeInBytes else 0)
      .sum
    val requestThrottleTimeMs = quotas.request.maybeRecordAndGetThrottleTimeMs(request, timeMs)
    val bandwidthThrottleTimeMs = quotas.fetch.maybeRecordAndGetThrottleTimeMs(
      request.session, request.header.clientId(), responseSize, timeMs)
    val maxThrottleTimeMs = Math.max(bandwidthThrottleTimeMs, requestThrottleTimeMs)

    if (maxThrottleTimeMs > 0) {
      request.apiThrottleTimeMs = maxThrottleTimeMs
      if (bandwidthThrottleTimeMs > requestThrottleTimeMs) {
        requestHelper.throttle(quotas.fetch, request, bandwidthThrottleTimeMs)
      } else {
        requestHelper.throttle(quotas.request, request, requestThrottleTimeMs)
      }
    }

    // Build and send the FetchResponse
    val fetchResponse = new FetchResponse(
      new FetchResponseData()
        .setThrottleTimeMs(maxThrottleTimeMs)
        .setResponses(FetchResponse.toResponseDataList(partitions))
    )

    requestChannel.sendResponse(request, fetchResponse, None)
  }

  // ---------------------------------------------------------------
  // 6. If no interesting (local) partitions, respond immediately
  // ---------------------------------------------------------------
  if (interesting.isEmpty) {
    processResponseCallback(Seq.empty)
    return
  }

  // ---------------------------------------------------------------
  // 7. Build FetchParams with capped maxWaitMs
  //    replicaId = -1 (consumer), replicaEpoch = -1, no epoch checks
  // ---------------------------------------------------------------
  val fetchMaxBytes = Math.min(fetchRequest.maxBytes, config.fetchMaxBytes)
  val fetchMinBytes = Math.min(fetchRequest.minBytes, fetchMaxBytes)

  val params = new FetchParams(
    /* replicaId = */    -1,          // consumer, not follower
    /* replicaEpoch = */ -1L,         // no epoch check (section 6.6)
    /* maxWaitMs = */    effectiveMaxWaitMs.toLong,
    /* minBytes = */     fetchMinBytes,
    /* maxBytes = */     fetchMaxBytes,
    /* isolation = */    FetchIsolation.of(fetchRequest),
    /* clientMetadata */ Optional.empty[ClientMetadata]()
  )

  // ---------------------------------------------------------------
  // 8. Fetch from local replicas
  //    The callback fires when data is available or maxWaitMs expires
  //    (via DelayedFetch purgatory). It may fire synchronously if
  //    data is already available.
  // ---------------------------------------------------------------
  replicaManager.fetchMessages(
    params = params,
    fetchInfos = interesting,
    quota = quotas.fetch,  // consumer quota (not leader replication quota)
    responseCallback = processResponseCallback
  )

  // Handler thread returns immediately if fetchMessages enqueued
  // a DelayedFetch. If data was available, the callback already fired
  // synchronously and the response was sent.
}
```

## Tests

### Unit test skeleton

```scala
// ============================================================================
// File: core/src/test/scala/unit/kafka/server/KafkaApisHttpConsumeTest.scala
// ============================================================================
package kafka.server

import java.util
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}
import java.util.{Collections, Optional}
import org.apache.kafka.common.{TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.requests.{FetchRequest, FetchResponse}
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.network.RequestChannel
import org.apache.kafka.server.storage.log.{FetchIsolation, FetchParams, FetchPartitionData}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.api.Assertions._
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._

class KafkaApisHttpConsumeTest {

  private var kafkaApis: KafkaApis = _
  private var replicaManager: ReplicaManager = _
  private var metadataCache: MetadataCache = _
  private var requestChannel: RequestChannel = _
  private var httpAsyncExecutor: ScheduledExecutorService = _

  @BeforeEach
  def setUp(): Unit = {
    replicaManager = mock(classOf[ReplicaManager])
    metadataCache = mock(classOf[MetadataCache])
    requestChannel = mock(classOf[RequestChannel])
    httpAsyncExecutor = Executors.newScheduledThreadPool(2)
    // ... wire up KafkaApis with mocks ...
  }

  @AfterEach
  def tearDown(): Unit = {
    httpAsyncExecutor.shutdown()
    httpAsyncExecutor.awaitTermination(5, TimeUnit.SECONDS)
  }

  @Test
  def testMaxWaitMsCappedToConfigValue(): Unit = {
    // Given: client sends maxWaitMs=30000, config.httpConsumeMaxWaitMs=5000
    // When: handleHttpConsumeRequest is called
    // Then: FetchParams.maxWaitMs == 5000
    //       request properties contain httpMaxWaitApplied=5000
  }

  @Test
  def testMaxWaitMsNotCappedWhenBelowConfig(): Unit = {
    // Given: client sends maxWaitMs=2000, config.httpConsumeMaxWaitMs=5000
    // When: handleHttpConsumeRequest is called
    // Then: FetchParams.maxWaitMs == 2000
  }

  @Test
  def testLocalLeaderFetchReturnsRecords(): Unit = {
    // Given: this broker is leader for partition 0, data available
    // When: handleHttpConsumeRequest is called with offset=100
    // Then: replicaManager.fetchMessages is called
    //       response contains records for partition 0
  }

  @Test
  def testNonLeaderPartitionReturnsLeaderNotAvailable(): Unit = {
    // Given: this broker is NOT leader for partition 0
    // When: handleHttpConsumeRequest is called
    // Then: response contains LEADER_NOT_AVAILABLE for partition 0
    //       response contains empty records for partition 0
    //       replicaManager.fetchMessages is NOT called for that partition
  }

  @Test
  def testUnauthorizedTopicReturnsAuthError(): Unit = {
    // Given: principal lacks READ on topic
    // When: handleHttpConsumeRequest is called
    // Then: response contains TOPIC_AUTHORIZATION_FAILED
  }

  @Test
  def testEmptyPollReturnsEmptyRecordsNotError(): Unit = {
    // Given: no data at requested offset, maxWaitMs expires
    // When: DelayedFetch purgatory fires with empty data
    // Then: response has records=[], errorCode=0 (not an error)
  }

  @Test
  def testReplicaIdIsConsumerNotFollower(): Unit = {
    // Given: any HTTP consume request
    // When: handleHttpConsumeRequest builds FetchParams
    // Then: params.replicaId == -1 (consumer)
    //       params.isFromFollower == false
  }

  @Test
  def testNoLeaderEpochCheck(): Unit = {
    // Given: any HTTP consume request
    // When: handleHttpConsumeRequest is called
    // Then: FetchParams does not set currentLeaderEpoch
    //       (or sets it to -1)
  }

  @Test
  def testMixedLocalAndRemotePartitions(): Unit = {
    // Given: P0 is local leader, P1 is remote leader
    // When: handleHttpConsumeRequest is called for both
    // Then: fetchMessages called only for P0
    //       P1 gets LEADER_NOT_AVAILABLE with empty records
  }

  @Test
  def testReadCommittedIsolation(): Unit = {
    // Given: request has isolationLevel=READ_COMMITTED
    // When: handleHttpConsumeRequest builds FetchParams
    // Then: params.isolation == FetchIsolation.TXN_COMMITTED
  }

  @Test
  def testQuotaThrottling(): Unit = {
    // Given: fetch quota is exceeded
    // When: handleHttpConsumeRequest completes
    // Then: requestHelper.throttle is called
  }
}
```

## Rules

1. **NEVER block the handler thread.** The same rule as TASK-C.01 applies.
2. **ALWAYS clamp maxWaitMs.** The cap `config.httpConsumeMaxWaitMs` (default 5000 ms) must be applied before any other logic. This is a hard requirement from section 6.1.
3. **ALWAYS set X-Kafka-MaxWait-Applied.** Stash the effective value in request properties for the response serializer.
4. **No leader epoch checks.** Set `currentLeaderEpoch = -1` on all partition fetch specs. Do not introduce epoch validation logic.
5. **No preferred read replica.** Always fetch from the leader. Set `preferredReadReplica = INVALID_PREFERRED_REPLICA_ID` in responses.
6. **Empty poll is success.** When no data is available and `maxWaitMs` expires, return `records: [], errorCode: 0`. Do not return an error.
7. **Do NOT implement forwarding.** Remote partitions return `LEADER_NOT_AVAILABLE` with empty records. Forwarding is TASK-D.04.
8. **Use consumer isolation, not follower quota.** `replicaId = -1`, quota = `quotas.fetch` (not `quotas.leader`).

## Learning

1. The skeleton code referenced `config.httpConsumeMaxWaitMs` but `KafkaConfig` did not expose this property. Required adding `HttpServerConfigs.CONFIG_DEF` to `AbstractKafkaConfig.CONFIG_DEF` merge list and a getter to `KafkaConfig`. The `HttpServerConfigs` class doc already stated this was the intended pattern.
2. The skeleton used `FetchResponse.toResponseDataList(partitions)` which does not exist. The correct factory is `FetchResponse.of(Errors, throttleTimeMs, sessionId, partitions, nodeEndpoints)`.
3. The skeleton used `metadataCache.getPartitionInfo(...)` which does not exist. The correct API (matching C.01 pattern) is `metadataCache.getLeaderAndIsr(topic, partition)` with `OptionConverters.toScala`.
4. The skeleton code used `request.requestLocalProperties` which did not exist on `RequestChannel.Request`. Added a `ConcurrentHashMap[String, AnyRef]` field to `RequestChannel.Request` for passing request-scoped values between handler and response serializer.
5. HTTP consume does not use fetch sessions (no incremental fetch), so we use `FetchMetadata.INVALID_SESSION_ID` and construct the response directly rather than going through `fetchContext`.
6. Following C.01 pattern: response callback wraps results in `CompletableFuture` and uses `.handleAsync(httpAsyncExecutor)` to ensure response construction never runs on handler or purgatory threads.

## Limitations

1. No forwarding for remote partitions -- returns `LEADER_NOT_AVAILABLE` with empty records (TASK-D.04).
2. No incremental fetch session support -- each HTTP request is stateless.
3. No preferred read replica support -- always fetches from the leader.
4. The `orTimeout` safety net on the CompletableFuture adds `effectiveMaxWaitMs + 5s`. If the purgatory is severely delayed beyond this buffer, the client gets an empty response rather than actual data.
5. No down-conversion of `KAFKA_STORAGE_ERROR` to `NOT_LEADER_OR_FOLLOWER` (only needed for old fetch protocol versions, not applicable to HTTP).

## Field Notes

1. The `HttpRequestTranslator` already clamps `maxWaitMs` before building the `FetchRequest`, so the clamp in `handleHttpConsumeRequest` is defense-in-depth. Both layers enforce the same config value.
2. `FetchIsolation.of(fetchRequest)` correctly handles `replicaId = -1` (consumer) and maps `READ_COMMITTED` to `TXN_COMMITTED`.
3. The `quota` parameter of `replicaManager.fetchMessages()` is a `ReplicaQuota` (for replication throttling), NOT the consumer quota manager. For consumer fetches, the existing `handleFetchRequest` passes `UNBOUNDED_QUOTA` (via `replicationQuota()`). Consumer bandwidth throttling is handled separately in the response callback via `quotas.fetch.maybeRecordAndGetThrottleTimeMs()`. The skeleton code incorrectly used `quotas.fetch` directly -- this causes a type mismatch since `ClientQuotaManager` is not a `ReplicaQuota`.

## Acceptance Criteria

- [x] `handleHttpConsumeRequest()` exists in `KafkaApis.scala` and compiles.
- [x] `effectiveMaxWaitMs` is correctly clamped to `min(request, config)`.
- [x] `httpMaxWaitApplied` is set in request properties for the response header.
- [x] Local leader partitions are fetched via `replicaManager.fetchMessages()`.
- [x] Remote leader partitions return `LEADER_NOT_AVAILABLE` with empty records.
- [x] `FetchParams.replicaId == -1` (consumer mode).
- [x] No leader epoch checks (`currentLeaderEpoch = -1`).
- [x] Empty poll returns `errorCode: 0` with empty records (not an error).
- [x] Unauthorized topics return `TOPIC_AUTHORIZATION_FAILED`.
- [x] `READ_COMMITTED` isolation is correctly mapped.
- [x] Quota enforcement works correctly.
- [ ] All unit tests pass.
- [ ] Existing `handleFetchRequest` tests still pass (no regression).

## File Manifest

| File | Action | Description |
|------|--------|-------------|
| `core/src/main/scala/kafka/server/KafkaApis.scala` | Modified | Replaced B.06 stub with full `handleHttpConsumeRequest` implementation (local leader path) |
| `core/src/main/scala/kafka/network/RequestChannel.scala` | Modified | Added `requestLocalProperties` (`ConcurrentHashMap[String, AnyRef]`) to `Request` class |
| `core/src/main/scala/kafka/server/KafkaConfig.scala` | Modified | Added `httpConsumeMaxWaitMs` config getter |
| `server/src/main/java/org/apache/kafka/server/config/AbstractKafkaConfig.java` | Modified | Added `HttpServerConfigs.CONFIG_DEF` to merged CONFIG_DEF |
