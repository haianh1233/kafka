# TASK-C.01: handleHttpProduceRequest -- Local Leader Path

## Prerequisites

| Task | What it delivers | Why this task needs it |
|------|------------------|-----------------------|
| TASK-B.06 | KafkaApis dispatch wiring (`securityProtocol.isHttp` branch in `handle()`) | Without dispatch, HTTP produce requests never reach `handleHttpProduceRequest()` |
| TASK-B.01 | `HttpRequestTranslator` (JSON body -> `ProduceRequest`) | The handler receives a `RequestChannel.Request` whose body is a `ProduceRequest` built by the translator |

## Context

This task implements the **local leader path** of HTTP produce handling in `KafkaApis`. When an HTTP client sends `POST /v1/topics/{topic}/records`, the request is translated to a standard `ProduceRequest` by `HttpRequestTranslator` (TASK-B.01), placed on `RequestChannel`, and dispatched to `handleHttpProduceRequest()` by the `securityProtocol.isHttp` branch (TASK-B.06).

The handler must:
1. Parse the `ProduceRequest` body.
2. Authorize via `authHelper.filterByAuthorized(WRITE, TOPIC, ...)`.
3. Check partition existence in `MetadataCache`.
4. For partitions where **this broker IS the leader**, call `replicaManager.appendRecords()`.
5. For partitions where **this broker is NOT the leader**, return per-partition error `LEADER_NOT_AVAILABLE` (HTTP 503). Forwarding to remote leaders is added later in TASK-D.03.

The handler must be fully async -- it must **never block** the `KafkaRequestHandler` thread. All future composition uses `.handleAsync(httpAsyncExecutor)` per design doc section 14.1.

### Key references in the codebase

- **Existing produce handler**: `KafkaApis.handleProduceRequest()` at `core/src/main/scala/kafka/server/KafkaApis.scala` line 397. This task follows the same authorization and validation pattern but wraps the callback in a `CompletableFuture`.
- **ReplicaManager.appendRecords()**: at `core/src/main/scala/kafka/server/ReplicaManager.scala` line 637. The `responseCallback` may be called synchronously (acks=1 fast path) or asynchronously (via DelayedProduce purgatory).
- **Design doc sections**: 5 (Produce Path), 5.4 (ACK semantics), 5.5A-B (edge cases), 14.1 (async completion pattern).

## Specification

### Method signature

```scala
// In KafkaApis (core/src/main/scala/kafka/server/KafkaApis.scala)
def handleHttpProduceRequest(request: RequestChannel.Request): Unit
```

### Behavior

1. Extract `ProduceRequest` from `request.body[ProduceRequest]`.
2. **Authorization**: Call `authHelper.filterByAuthorized(request.context, WRITE, TOPIC, topicIdToPartitionData)(_._1.topic)` exactly as the existing `handleProduceRequest` does.
3. **Partition existence**: Check `metadataCache.contains(topicIdPartition.topicPartition)` for each authorized partition.
4. **Leader check**: For each valid partition, check if this broker is the leader via `metadataCache.getPartitionLeaderEndpoint(topicIdPartition.topicPartition.topic, topicIdPartition.topicPartition.partition, request.context.listenerName)`. If not the leader, add `LEADER_NOT_AVAILABLE` to the non-local responses map.
5. **Local append**: If `authorizedRequestInfo` is non-empty, call `replicaManager.appendRecords(...)` with a `responseCallback` that completes a `CompletableFuture`.
6. **Merge results**: Combine local results, unauthorized responses, non-existing responses, and leader-not-available responses.
7. **Send response**: Via `requestChannel.sendResponse()` using the merged `ProduceResponse`, always from `httpAsyncExecutor`.
8. The handler thread returns immediately after setting up futures.

### HTTP status mapping (single partition)

| Condition | HTTP Status |
|-----------|-------------|
| All partitions succeed | 200 |
| Mixed success/failure (multi-partition) | 207 |
| All partitions unauthorized | 403 |
| All partitions unknown topic | 404 |
| All partitions leader not available | 503 |

### ACK mapping

| HTTP `acks` value | Kafka `requiredAcks` |
|---|---|
| `"none"` | 0 |
| `"leader"` | 1 |
| `"all"` (default) | -1 |

## Implementation Details

### File to modify

`/home/anh/kafka/core/src/main/scala/kafka/server/KafkaApis.scala`

### Where to add the method

Add `handleHttpProduceRequest` as a new method in `KafkaApis`, adjacent to the existing `handleProduceRequest` (after line ~560). The dispatch wiring in `handle()` (TASK-B.06) calls this method when `request.context.securityProtocol.isHttp` and `apiKey == PRODUCE`.

### httpAsyncExecutor dependency

The `httpAsyncExecutor` is a `ScheduledExecutorService` injected into `KafkaApis` (or accessed via `config`). It is created by `HttpAcceptor.startup()` with `num.http.async.threads` threads (default 4). This executor must already be wired into `KafkaApis` by a prior task (TASK-B.06 or TASK-A.02). If not yet available, declare it as a constructor parameter:

```scala
// Add to KafkaApis constructor parameters (if not already present from B.06):
httpAsyncExecutor: ScheduledExecutorService = null
```

### Leader detection

Use `metadataCache` to determine if this broker is the leader for a given partition:

```scala
val localBrokerId = config.brokerId
metadataCache.getPartitionInfo(topicPartition.topic, topicPartition.partition) match {
  case Some(partitionInfo) if partitionInfo.leader == localBrokerId => true // local
  case _ => false // remote or unknown
}
```

## Skeleton Code

```scala
// ============================================================================
// File: core/src/main/scala/kafka/server/KafkaApis.scala
// Add this method to the KafkaApis class body
// ============================================================================

import java.util.concurrent.{CompletableFuture, ScheduledExecutorService, TimeUnit}
import java.util.function.BiFunction
import org.apache.kafka.common.TopicIdPartition
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.ApiException
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.requests.{ProduceRequest, ProduceResponse}
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.common.resource.Resource.CLUSTER_NAME
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.server.common.RequestLocal
import org.apache.kafka.storage.internals.log.AppendOrigin

import scala.collection.mutable
import scala.jdk.CollectionConverters._

/**
 * Handles HTTP produce requests -- LOCAL leader path only.
 *
 * For partitions where this broker is the leader, delegates to
 * ReplicaManager.appendRecords(). For partitions with a remote leader,
 * returns LEADER_NOT_AVAILABLE (forwarding added in TASK-D.03).
 *
 * CRITICAL: This method must NEVER block the KafkaRequestHandler thread.
 * All CompletableFuture composition uses .handleAsync(httpAsyncExecutor)
 * per design doc section 14.1.
 *
 * @see handleProduceRequest for the binary protocol equivalent
 * @see <a href="ivy-docs/http-protocol-design.md">Design doc section 5</a>
 */
def handleHttpProduceRequest(request: RequestChannel.Request): Unit = {
  val produceRequest = request.body[ProduceRequest]
  val localBrokerId = config.brokerId

  // ---------------------------------------------------------------
  // 1. Parse partition data from the ProduceRequest
  //    (Same pattern as handleProduceRequest lines 415-451)
  // ---------------------------------------------------------------
  val unauthorizedTopicResponses = mutable.Map[TopicIdPartition, PartitionResponse]()
  val nonExistingTopicResponses = mutable.Map[TopicIdPartition, PartitionResponse]()
  val invalidRequestResponses = mutable.Map[TopicIdPartition, PartitionResponse]()
  val leaderNotAvailableResponses = mutable.Map[TopicIdPartition, PartitionResponse]()
  val authorizedLocalRequestInfo = mutable.Map[TopicIdPartition, MemoryRecords]()

  val topicIdToPartitionData =
    new mutable.ArrayBuffer[(TopicIdPartition, ProduceRequest.PartitionProduceData)]

  produceRequest.data.topicData.forEach { topic =>
    topic.partitionData.forEach { partition =>
      val topicName = if (topic.topicId().equals(org.apache.kafka.common.Uuid.ZERO_UUID)) {
        topic.name()
      } else {
        metadataCache.getTopicName(topic.topicId).orElse(topic.name)
      }
      val topicId = if (topic.topicId().equals(org.apache.kafka.common.Uuid.ZERO_UUID)) {
        metadataCache.getTopicId(topic.name())
      } else {
        topic.topicId()
      }

      val topicPartition = new TopicPartition(topicName, partition.index())
      val topicIdPartition = new TopicIdPartition(topicId, topicPartition)

      if (topicName == null || topicName.isEmpty) {
        nonExistingTopicResponses += topicIdPartition ->
          new PartitionResponse(Errors.UNKNOWN_TOPIC_ID)
      } else {
        topicIdToPartitionData += topicIdPartition -> partition
      }
    }
  }

  // ---------------------------------------------------------------
  // 2. Authorization check (same pattern as existing handler)
  // ---------------------------------------------------------------
  val authorizedTopics = authHelper.filterByAuthorized(
    request.context, WRITE, TOPIC, topicIdToPartitionData
  )(_._1.topic)

  // ---------------------------------------------------------------
  // 3. Classify each partition: unauthorized, non-existing,
  //    invalid, local-leader, or remote-leader
  // ---------------------------------------------------------------
  topicIdToPartitionData.foreach { case (topicIdPartition, partition) =>
    val memoryRecords = partition.records.asInstanceOf[MemoryRecords]
    if (!authorizedTopics.contains(topicIdPartition.topic)) {
      unauthorizedTopicResponses += topicIdPartition ->
        new PartitionResponse(Errors.TOPIC_AUTHORIZATION_FAILED)
    } else if (!metadataCache.contains(topicIdPartition.topicPartition)) {
      nonExistingTopicResponses += topicIdPartition ->
        new PartitionResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION)
    } else {
      try {
        ProduceRequest.validateRecords(request.header.apiVersion, memoryRecords)

        // Leader check: is this broker the leader for this partition?
        val isLocal = metadataCache.getPartitionInfo(
          topicIdPartition.topicPartition.topic,
          topicIdPartition.topicPartition.partition
        ).exists(_.leader == localBrokerId)

        if (isLocal) {
          authorizedLocalRequestInfo += (topicIdPartition -> memoryRecords)
        } else {
          // Phase C: no forwarding yet -- return LEADER_NOT_AVAILABLE
          // Phase D (TASK-D.03) will replace this with actual forwarding
          leaderNotAvailableResponses += topicIdPartition ->
            new PartitionResponse(Errors.LEADER_NOT_AVAILABLE)
        }
      } catch {
        case e: ApiException =>
          invalidRequestResponses += topicIdPartition ->
            new PartitionResponse(Errors.forException(e))
      }
    }
  }

  // ---------------------------------------------------------------
  // 4. Merge all non-appendable partition results
  // ---------------------------------------------------------------
  val preComputedErrors: Map[TopicIdPartition, PartitionResponse] =
    (unauthorizedTopicResponses ++ nonExistingTopicResponses ++
      invalidRequestResponses ++ leaderNotAvailableResponses).toMap

  // ---------------------------------------------------------------
  // 5. Build response callback that sends the HTTP response
  // ---------------------------------------------------------------
  def sendMergedResponse(
    appendResults: java.util.Map[TopicIdPartition, PartitionResponse]
  ): Unit = {
    val mergedResponseStatus: java.util.Map[TopicIdPartition, PartitionResponse] =
      new java.util.HashMap[TopicIdPartition, PartitionResponse]()
    mergedResponseStatus.putAll(appendResults)
    preComputedErrors.foreach { case (tp, resp) => mergedResponseStatus.put(tp, resp) }

    // Quota handling (same pattern as existing handler)
    val timeMs = time.milliseconds()
    val requestSize = request.sizeInBytes
    val bandwidthThrottleTimeMs = quotas.produce.maybeRecordAndGetThrottleTimeMs(
      request.session, request.header.clientId(), requestSize, timeMs)
    val requestThrottleTimeMs =
      if (produceRequest.acks == 0) 0
      else quotas.request.maybeRecordAndGetThrottleTimeMs(request, timeMs)
    val maxThrottleTimeMs = Math.max(bandwidthThrottleTimeMs, requestThrottleTimeMs)

    if (maxThrottleTimeMs > 0) {
      request.apiThrottleTimeMs = maxThrottleTimeMs
      if (bandwidthThrottleTimeMs > requestThrottleTimeMs) {
        requestHelper.throttle(quotas.produce, request, bandwidthThrottleTimeMs)
      } else {
        requestHelper.throttle(quotas.request, request, requestThrottleTimeMs)
      }
    }

    if (produceRequest.acks == 0) {
      requestHelper.sendNoOpResponseExemptThrottle(request)
    } else {
      requestChannel.sendResponse(
        request,
        new ProduceResponse(mergedResponseStatus, maxThrottleTimeMs,
          java.util.Collections.emptyList()),
        None
      )
    }
  }

  // ---------------------------------------------------------------
  // 6. If no local partitions to append, send immediate response
  // ---------------------------------------------------------------
  if (authorizedLocalRequestInfo.isEmpty) {
    sendMergedResponse(java.util.Collections.emptyMap())
    return
  }

  // ---------------------------------------------------------------
  // 7. Local append via ReplicaManager (async via CompletableFuture)
  //    CRITICAL: responseCallback may be called synchronously
  //    (acks=1 fast path). Wrap in CompletableFuture so the merge
  //    always runs on httpAsyncExecutor (section 14.1).
  // ---------------------------------------------------------------
  val localFuture = new CompletableFuture[java.util.Map[TopicIdPartition, PartitionResponse]]()

  replicaManager.appendRecords(
    timeout = produceRequest.timeout.toLong,
    requiredAcks = produceRequest.acks,
    internalTopicsAllowed = request.header.clientId == "__admin_client",
    origin = AppendOrigin.CLIENT,
    entriesPerPartition = authorizedLocalRequestInfo,
    responseCallback = { results: java.util.Map[TopicIdPartition, PartitionResponse] =>
      localFuture.complete(results)
    }
  )

  // Clear partition records to allow GC (same as existing handler)
  produceRequest.clearPartitionRecords()

  // ---------------------------------------------------------------
  // 8. When the local append completes, merge and send response
  //    ALWAYS use .handleAsync with httpAsyncExecutor to avoid
  //    running on the handler thread or InterBrokerSendThread.
  // ---------------------------------------------------------------
  localFuture
    .orTimeout(produceRequest.timeout.toLong, TimeUnit.MILLISECONDS)
    .handleAsync(
      new BiFunction[java.util.Map[TopicIdPartition, PartitionResponse], Throwable, Unit] {
        override def apply(
          localResults: java.util.Map[TopicIdPartition, PartitionResponse],
          ex: Throwable
        ): Unit = {
          if (ex != null) {
            // Timeout or unexpected error -- return REQUEST_TIMED_OUT for all local partitions
            val timedOutResults = new java.util.HashMap[TopicIdPartition, PartitionResponse]()
            authorizedLocalRequestInfo.keys.foreach { tp =>
              timedOutResults.put(tp, new PartitionResponse(Errors.REQUEST_TIMED_OUT))
            }
            sendMergedResponse(timedOutResults)
          } else {
            sendMergedResponse(localResults)
          }
        }
      },
      httpAsyncExecutor  // <-- NEVER use plain .handle() here
    )

  // Handler thread returns immediately -- KafkaApis.handle() loops back
}
```

## Tests

### Unit test skeleton

```scala
// ============================================================================
// File: core/src/test/scala/unit/kafka/server/KafkaApisHttpProduceTest.scala
// ============================================================================
package kafka.server

import java.util
import java.util.concurrent.{CompletableFuture, Executors, ScheduledExecutorService, TimeUnit}
import org.apache.kafka.common.{TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.common.memory.MemoryPool
import org.apache.kafka.common.message.ProduceRequestData
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record.{CompressionType, MemoryRecords, SimpleRecord}
import org.apache.kafka.common.requests.{ProduceRequest, ProduceResponse, RequestHeader}
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.network.RequestChannel
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.api.Assertions._
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._

class KafkaApisHttpProduceTest {

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
  def testLocalLeaderProduceSuccess(): Unit = {
    // Given: this broker is leader for partition 0
    // When: handleHttpProduceRequest is called with a single record
    // Then: replicaManager.appendRecords is called
    //       response contains offset for partition 0 with NONE error
  }

  @Test
  def testNonLeaderPartitionReturnsLeaderNotAvailable(): Unit = {
    // Given: this broker is NOT leader for partition 0
    // When: handleHttpProduceRequest is called
    // Then: response contains LEADER_NOT_AVAILABLE for partition 0
    //       replicaManager.appendRecords is NOT called
  }

  @Test
  def testUnauthorizedTopicReturns403(): Unit = {
    // Given: principal lacks WRITE on topic
    // When: handleHttpProduceRequest is called
    // Then: response contains TOPIC_AUTHORIZATION_FAILED
  }

  @Test
  def testNonExistingTopicReturns404(): Unit = {
    // Given: topic does not exist in metadataCache
    // When: handleHttpProduceRequest is called
    // Then: response contains UNKNOWN_TOPIC_OR_PARTITION
  }

  @Test
  def testMixedLocalAndRemotePartitions(): Unit = {
    // Given: P0 is local leader, P1 is remote leader
    // When: handleHttpProduceRequest is called with records for both
    // Then: appendRecords called only for P0
    //       P1 gets LEADER_NOT_AVAILABLE
  }

  @Test
  def testAcksZeroSendsNoOpResponse(): Unit = {
    // Given: acks=0 produce request
    // When: handleHttpProduceRequest is called
    // Then: sendNoOpResponseExemptThrottle is called
  }

  @Test
  def testTimeoutReturnsRequestTimedOut(): Unit = {
    // Given: appendRecords callback never fires within timeout
    // When: orTimeout triggers
    // Then: response contains REQUEST_TIMED_OUT for all local partitions
  }

  @Test
  def testHandlerThreadReturnsImmediately(): Unit = {
    // Given: appendRecords callback is deferred (acks=all, ISR delayed)
    // When: handleHttpProduceRequest returns
    // Then: it returns before the callback fires
    //       (verify with a latch that the test thread continues)
  }

  @Test
  def testQuotaThrottling(): Unit = {
    // Given: quota is exceeded
    // When: handleHttpProduceRequest completes
    // Then: requestHelper.throttle is called with correct throttle time
  }
}
```

## Rules

1. **NEVER block the handler thread.** Do not call `.get()`, `.join()`, or any blocking wait on a `CompletableFuture` inside `handleHttpProduceRequest`. The handler thread must return immediately after setting up the future chain.
2. **ALWAYS use `.handleAsync(httpAsyncExecutor)`** for CompletableFuture callbacks. Never use plain `.handle()` or `.thenApply()` without an executor -- the callback might run on the handler thread if the future completes synchronously.
3. **Do NOT implement forwarding.** Partitions where this broker is not the leader must return `LEADER_NOT_AVAILABLE`. Forwarding is TASK-D.03.
4. **Do NOT introduce new ApiKeys.** HTTP requests use existing `ApiKeys.PRODUCE`.
5. **Follow the existing authorization pattern exactly.** Use `authHelper.filterByAuthorized(WRITE, TOPIC, ...)` -- do not add custom auth logic.
6. **Reuse `PartitionResponse` and `ProduceResponse` types.** The response is serialized to JSON by `HttpResponseSerializer` on the response path, not in this handler.
7. **Clear partition records after enqueue.** Call `produceRequest.clearPartitionRecords()` to allow GC, same as the existing handler.
8. All `import` statements must compile against the Kafka trunk codebase.

## Learning

_To be filled by the executing agent._

## Limitations

_To be filled by the executing agent._

## Field Notes

_To be filled by the executing agent._

## Acceptance Criteria

- [ ] `handleHttpProduceRequest()` exists in `KafkaApis.scala` and compiles.
- [ ] Single-partition produce to a local leader returns offset with `Errors.NONE`.
- [ ] Multi-partition produce where all partitions are local returns correct offsets.
- [ ] Partitions with a remote leader return `Errors.LEADER_NOT_AVAILABLE`.
- [ ] Unauthorized topics return `Errors.TOPIC_AUTHORIZATION_FAILED`.
- [ ] Non-existing topics return `Errors.UNKNOWN_TOPIC_OR_PARTITION`.
- [ ] `acks=0` calls `sendNoOpResponseExemptThrottle`.
- [ ] Handler thread returns immediately -- no blocking on CompletableFuture.
- [ ] All CompletableFuture callbacks run on `httpAsyncExecutor`.
- [ ] Quota enforcement (bandwidth + request quota) works correctly.
- [ ] All unit tests pass.
- [ ] Existing `handleProduceRequest` tests still pass (no regression).

## File Manifest

| File | Action | Description |
|------|--------|-------------|
| | | |
