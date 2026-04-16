# TASK-D.03: Forwarding Integration in Produce Handler

## Prerequisites

| Task | What it delivers | Why this task needs it |
|------|------------------|-----------------------|
| TASK-C.01 | `handleHttpProduceRequest()` in KafkaApis (local leader path) | This task extends that method to add forwarding |
| TASK-D.02 | `ProduceForwardManager` (manages per-broker forwarding threads) | Called to forward remote partition groups |

## Context

This task extends `handleHttpProduceRequest()` (created in TASK-C.01) to support the **full produce path**: partitions where this broker is the leader are handled locally, and partitions where another broker is the leader are forwarded via `ProduceForwardManager`. Results from both paths are merged using `CompletableFuture.allOf().handleAsync(httpAsyncExecutor)`.

### Before this task (TASK-C.01 state)

In TASK-C.01, the handler returns `LEADER_NOT_AVAILABLE` for any partition where this broker is not the leader. This task replaces that stub with actual forwarding.

### After this task

The handler implements the full forwarding decision matrix (design doc section 7.5):

```
is this broker leader for P?
  YES -> ReplicaManager.appendRecords() directly (existing from C.01)
  NO  -> metadataCache knows leader?
    YES -> ProduceForwardManager.forward(leaderId, ...)
    NO  -> return 503 LEADER_NOT_AVAILABLE for that partition
```

### Retry on leader change (section 5.5C)

When a remote broker returns `NOT_LEADER_OR_FOLLOWER`, the handler retries **once** with refreshed metadata:
1. Refresh metadata for the affected partitions.
2. Re-bucket: the partition might now be local, or might have a different remote leader.
3. Re-enqueue to the new leader.
4. If the second attempt also fails, return the error to the client.

The retry budget is bounded by `http.internal.forwarding.timeout.ms` (default 10,000 ms).

### Key design doc references

- Section 5.1: End-to-end happy path with concurrent local + remote.
- Section 5.2: Timeout / partial failure.
- Section 5.5C: Forwarding-specific edge cases and retry.
- Section 7.4: Forwarding internals diagram.
- Section 7.5: Forwarding decision matrix.
- Section 14.1: Async completion pattern (`handleAsync` with `httpAsyncExecutor`).

## Specification

### Modified method

```scala
def handleHttpProduceRequest(request: RequestChannel.Request): Unit
```

in `/home/anh/kafka/core/src/main/scala/kafka/server/KafkaApis.scala`.

### New behavior (changes from TASK-C.01)

1. **Partition bucketing**: After authorization and validation, partition entries into three groups:
   - `localEntries`: `Map[TopicIdPartition, MemoryRecords]` -- this broker is the leader.
   - `remoteEntries`: `Map[Int, Map[TopicIdPartition, MemoryRecords]]` -- keyed by remote leader broker ID.
   - `leaderUnknownEntries`: `Map[TopicIdPartition, PartitionResponse]` -- leader not in MetadataCache.

2. **Local append** (unchanged from C.01): Call `replicaManager.appendRecords()`, wrap callback in `CompletableFuture[localResults]`.

3. **Remote forwarding** (new): For each `(leaderId, entries)` group, call `produceForwardManager.forward(leaderId, entries, requiredAcks, timeoutMs)`. Collect all returned futures.

4. **Merge with `CompletableFuture.allOf`**: Wait for all local + remote futures. Use `.handleAsync(httpAsyncExecutor)` to merge results on the async executor.

5. **Retry on `NOT_LEADER_OR_FOLLOWER`**: In the merge callback, check remote results for `NOT_LEADER_OR_FOLLOWER` errors. For those partitions, retry once:
   - Refresh metadata.
   - Re-bucket the failed partitions.
   - Forward again.
   - Merge the retry results.

6. **Overall timeout**: `CompletableFuture.allOf(...).orTimeout(forwardingTimeoutMs, MILLISECONDS)`.

### HTTP status codes

| Scenario | HTTP Status |
|----------|-------------|
| All partitions succeed | 200 |
| Mixed success/failure | 207 Multi-Status |
| All partitions fail with same error | Mapped error status (e.g., 503, 404) |

### ProduceForwardManager injection

`ProduceForwardManager` must be accessible in `KafkaApis`. It should be passed as a constructor parameter or accessed via a provider:

```scala
// KafkaApis constructor addition:
produceForwardManager: ProduceForwardManager = null
```

When `produceForwardManager` is null (e.g., no HTTP listener configured), the handler falls back to TASK-C.01 behavior (return `LEADER_NOT_AVAILABLE`).

## Implementation Details

### File to modify

`/home/anh/kafka/core/src/main/scala/kafka/server/KafkaApis.scala`

### Changes from TASK-C.01 code

The TASK-C.01 skeleton has a section that classifies partitions as local vs remote and puts remote partitions into `leaderNotAvailableResponses`. This task replaces that logic.

**Before (C.01):**
```scala
if (isLocal) {
  authorizedLocalRequestInfo += (topicIdPartition -> memoryRecords)
} else {
  // Phase C: no forwarding yet
  leaderNotAvailableResponses += topicIdPartition ->
    new PartitionResponse(Errors.LEADER_NOT_AVAILABLE)
}
```

**After (D.03):**
```scala
if (isLocal) {
  authorizedLocalRequestInfo += (topicIdPartition -> memoryRecords)
} else {
  // Determine leader for forwarding
  val leaderOpt = metadataCache.getPartitionInfo(tp.topic, tp.partition)
    .map(_.leader)
    .filter(_ >= 0)
  leaderOpt match {
    case Some(leaderId) =>
      remoteEntriesByLeader.getOrElseUpdate(leaderId, mutable.Map.empty) +=
        (topicIdPartition -> memoryRecords)
    case None =>
      leaderNotAvailableResponses += topicIdPartition ->
        new PartitionResponse(Errors.LEADER_NOT_AVAILABLE)
  }
}
```

## Skeleton Code

```scala
// ============================================================================
// File: core/src/main/scala/kafka/server/KafkaApis.scala
// REPLACEMENT for handleHttpProduceRequest() (originally from TASK-C.01)
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
import org.apache.kafka.storage.internals.log.AppendOrigin
import kafka.server.http.{ProduceForwardManager, ProduceForwardThread}

import scala.collection.mutable
import scala.jdk.CollectionConverters._

/**
 * Handles HTTP produce requests — FULL path with local append + forwarding.
 *
 * Partitions are bucketed by leader:
 *   - LOCAL:   ReplicaManager.appendRecords() (same as C.01)
 *   - REMOTE:  ProduceForwardManager.forward(leaderId, ...)
 *   - UNKNOWN: return LEADER_NOT_AVAILABLE
 *
 * All futures are merged via CompletableFuture.allOf().handleAsync(httpAsyncExecutor).
 *
 * Retry: on NOT_LEADER_OR_FOLLOWER from remote broker, retry ONCE with
 * refreshed metadata (section 5.5C).
 */
def handleHttpProduceRequest(request: RequestChannel.Request): Unit = {
  val produceRequest = request.body[ProduceRequest]
  val localBrokerId = config.brokerId
  val forwardingTimeoutMs = config.httpInternalForwardingTimeoutMs  // default 10000

  // ---------------------------------------------------------------
  // 1. Parse and authorize (same as C.01, unchanged)
  // ---------------------------------------------------------------
  val unauthorizedTopicResponses = mutable.Map[TopicIdPartition, PartitionResponse]()
  val nonExistingTopicResponses = mutable.Map[TopicIdPartition, PartitionResponse]()
  val invalidRequestResponses = mutable.Map[TopicIdPartition, PartitionResponse]()
  val leaderNotAvailableResponses = mutable.Map[TopicIdPartition, PartitionResponse]()
  val authorizedLocalRequestInfo = mutable.Map[TopicIdPartition, MemoryRecords]()
  val remoteEntriesByLeader = mutable.Map[Int, mutable.Map[TopicIdPartition, MemoryRecords]]()

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

  val authorizedTopics = authHelper.filterByAuthorized(
    request.context, WRITE, TOPIC, topicIdToPartitionData
  )(_._1.topic)

  // ---------------------------------------------------------------
  // 2. Classify: unauthorized, non-existing, local, remote, unknown
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

        val partitionInfo = metadataCache.getPartitionInfo(
          topicIdPartition.topicPartition.topic,
          topicIdPartition.topicPartition.partition
        )

        partitionInfo match {
          case Some(info) if info.leader == localBrokerId =>
            // LOCAL: this broker is the leader
            authorizedLocalRequestInfo += (topicIdPartition -> memoryRecords)

          case Some(info) if info.leader >= 0 =>
            // REMOTE: another broker is the leader — forward
            remoteEntriesByLeader.getOrElseUpdate(info.leader, mutable.Map.empty) +=
              (topicIdPartition -> memoryRecords)

          case _ =>
            // UNKNOWN: leader not known
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
  // 3. Pre-computed errors (immutable snapshot)
  // ---------------------------------------------------------------
  val preComputedErrors: Map[TopicIdPartition, PartitionResponse] =
    (unauthorizedTopicResponses ++ nonExistingTopicResponses ++
      invalidRequestResponses ++ leaderNotAvailableResponses).toMap

  // ---------------------------------------------------------------
  // 4. Send merged response helper
  // ---------------------------------------------------------------
  def sendMergedResponse(
    allResults: Map[TopicIdPartition, PartitionResponse]
  ): Unit = {
    val mergedResponseStatus = new java.util.HashMap[TopicIdPartition, PartitionResponse]()
    allResults.foreach { case (tp, resp) => mergedResponseStatus.put(tp, resp) }
    preComputedErrors.foreach { case (tp, resp) => mergedResponseStatus.put(tp, resp) }

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
  // 5. If nothing to append or forward, respond immediately
  // ---------------------------------------------------------------
  if (authorizedLocalRequestInfo.isEmpty && remoteEntriesByLeader.isEmpty) {
    sendMergedResponse(Map.empty)
    return
  }

  // ---------------------------------------------------------------
  // 6. LOCAL append via ReplicaManager (async)
  // ---------------------------------------------------------------
  val localFuture = new CompletableFuture[Map[TopicIdPartition, PartitionResponse]]()

  if (authorizedLocalRequestInfo.nonEmpty) {
    replicaManager.appendRecords(
      timeout = produceRequest.timeout.toLong,
      requiredAcks = produceRequest.acks,
      internalTopicsAllowed = request.header.clientId == "__admin_client",
      origin = AppendOrigin.CLIENT,
      entriesPerPartition = authorizedLocalRequestInfo,
      responseCallback = { results: java.util.Map[TopicIdPartition, PartitionResponse] =>
        localFuture.complete(results.asScala.toMap)
      }
    )
  } else {
    localFuture.complete(Map.empty)
  }

  produceRequest.clearPartitionRecords()

  // ---------------------------------------------------------------
  // 7. REMOTE forwarding via ProduceForwardManager
  // ---------------------------------------------------------------
  val remoteFutures: Seq[(Int, CompletableFuture[Map[TopicIdPartition, ProduceForwardThread.PartitionResponse]])] =
    if (produceForwardManager != null && remoteEntriesByLeader.nonEmpty) {
      remoteEntriesByLeader.toSeq.map { case (leaderId, entries) =>
        leaderId -> produceForwardManager.forward(
          leaderId,
          entries.toMap.asJava,
          produceRequest.acks,
          produceRequest.timeout
        )
      }
    } else {
      // No forwarder available — treat all remote as leader-not-available
      // (This path should not be hit in production if HTTP is enabled)
      remoteEntriesByLeader.foreach { case (_, entries) =>
        entries.keys.foreach { tp =>
          leaderNotAvailableResponses += tp ->
            new PartitionResponse(Errors.LEADER_NOT_AVAILABLE)
        }
      }
      Seq.empty
    }

  // ---------------------------------------------------------------
  // 8. Combine all futures and merge results
  //    ALWAYS use .handleAsync with httpAsyncExecutor (section 14.1)
  // ---------------------------------------------------------------
  val allFutures: Array[CompletableFuture[_]] =
    (Seq(localFuture) ++ remoteFutures.map(_._2)).toArray

  CompletableFuture.allOf(allFutures: _*)
    .orTimeout(forwardingTimeoutMs.toLong, TimeUnit.MILLISECONDS)
    .handleAsync(
      new BiFunction[Void, Throwable, Unit] {
        override def apply(ignored: Void, ex: Throwable): Unit = {
          val allResults = mutable.Map[TopicIdPartition, PartitionResponse]()

          // Collect local results
          try {
            val localResults = localFuture.getNow(Map.empty)
            allResults ++= localResults
          } catch {
            case _: Exception =>
              authorizedLocalRequestInfo.keys.foreach { tp =>
                allResults += tp -> new PartitionResponse(Errors.REQUEST_TIMED_OUT)
              }
          }

          // Collect remote results
          val retriablePartitions = mutable.Map[TopicIdPartition, MemoryRecords]()

          remoteFutures.foreach { case (leaderId, future) =>
            try {
              if (future.isDone && !future.isCompletedExceptionally) {
                val remoteResults = future.getNow(null)
                if (remoteResults != null) {
                  remoteResults.forEach { (tp, resp) =>
                    if (resp.error == Errors.NOT_LEADER_OR_FOLLOWER ||
                        resp.error == Errors.LEADER_NOT_AVAILABLE) {
                      // Candidate for retry (section 5.5C)
                      val originalRecords = remoteEntriesByLeader.get(leaderId)
                        .flatMap(_.get(tp))
                      originalRecords.foreach { records =>
                        retriablePartitions += tp -> records
                      }
                    } else {
                      allResults += tp -> convertPartitionResponse(resp)
                    }
                  }
                }
              } else {
                // Future failed (timeout or disconnect)
                remoteEntriesByLeader.get(leaderId).foreach { entries =>
                  entries.keys.foreach { tp =>
                    allResults += tp -> new PartitionResponse(Errors.REQUEST_TIMED_OUT)
                  }
                }
              }
            } catch {
              case _: Exception =>
                remoteEntriesByLeader.get(leaderId).foreach { entries =>
                  entries.keys.foreach { tp =>
                    allResults += tp -> new PartitionResponse(Errors.REQUEST_TIMED_OUT)
                  }
                }
            }
          }

          // ----------------------------------------------------------
          // 9. Retry once for NOT_LEADER_OR_FOLLOWER partitions
          //    (section 5.5C: at most 1 retry per forwarded group)
          // ----------------------------------------------------------
          if (retriablePartitions.nonEmpty && produceForwardManager != null) {
            retryFailedPartitions(
              retriablePartitions.toMap,
              produceRequest.acks,
              produceRequest.timeout,
              allResults
            )
          }

          sendMergedResponse(allResults.toMap)
        }
      },
      httpAsyncExecutor
    )
  // Handler thread returns immediately
}

/**
 * Retry produce for partitions that returned NOT_LEADER_OR_FOLLOWER.
 * Re-buckets partitions with refreshed metadata and forwards once more.
 * Results (success or failure) are written into allResults.
 *
 * This runs on httpAsyncExecutor, so a short blocking wait is acceptable
 * (bounded by remaining timeout budget).
 */
private def retryFailedPartitions(
  partitions: Map[TopicIdPartition, MemoryRecords],
  requiredAcks: Short,
  timeoutMs: Int,
  allResults: mutable.Map[TopicIdPartition, PartitionResponse]
): Unit = {
  val localBrokerId = config.brokerId

  partitions.foreach { case (tp, records) =>
    // Refresh metadata for this partition
    val partitionInfo = metadataCache.getPartitionInfo(
      tp.topicPartition.topic, tp.topicPartition.partition)

    partitionInfo match {
      case Some(info) if info.leader == localBrokerId =>
        // Leader moved to this broker — append locally (synchronous on retry)
        try {
          val retryFuture = new CompletableFuture[Map[TopicIdPartition, PartitionResponse]]()
          replicaManager.appendRecords(
            timeout = timeoutMs.toLong,
            requiredAcks = requiredAcks,
            internalTopicsAllowed = false,
            origin = AppendOrigin.CLIENT,
            entriesPerPartition = Map(tp -> records),
            responseCallback = { results: java.util.Map[TopicIdPartition, PartitionResponse] =>
              retryFuture.complete(results.asScala.toMap)
            }
          )
          val retryResults = retryFuture.get(timeoutMs.toLong, TimeUnit.MILLISECONDS)
          allResults ++= retryResults
        } catch {
          case _: Exception =>
            allResults += tp -> new PartitionResponse(Errors.REQUEST_TIMED_OUT)
        }

      case Some(info) if info.leader >= 0 =>
        // Forward to new leader
        try {
          val retryResult = produceForwardManager.forward(
            info.leader,
            java.util.Map.of(tp, records),
            requiredAcks,
            timeoutMs
          ).get(timeoutMs.toLong, TimeUnit.MILLISECONDS)

          retryResult.forEach { (retryTp, resp) =>
            allResults += retryTp -> convertPartitionResponse(resp)
          }
        } catch {
          case _: Exception =>
            allResults += tp -> new PartitionResponse(Errors.NOT_LEADER_OR_FOLLOWER)
        }

      case _ =>
        allResults += tp -> new PartitionResponse(Errors.LEADER_NOT_AVAILABLE)
    }
  }
}

/**
 * Convert ProduceForwardThread.PartitionResponse to Kafka PartitionResponse.
 */
private def convertPartitionResponse(
  resp: ProduceForwardThread.PartitionResponse
): PartitionResponse = {
  new PartitionResponse(resp.error, resp.baseOffset, resp.logAppendTimeMs, resp.logStartOffset)
}
```

## Tests

### Unit test skeleton

```scala
// ============================================================================
// File: core/src/test/scala/unit/kafka/server/KafkaApisHttpProduceForwardingTest.scala
// ============================================================================
package kafka.server

import java.util
import java.util.concurrent.{CompletableFuture, Executors, ScheduledExecutorService, TimeUnit}
import org.apache.kafka.common.{TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.{CompressionType, MemoryRecords, SimpleRecord}
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.network.RequestChannel
import kafka.server.http.{ProduceForwardManager, ProduceForwardThread}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.api.Assertions._
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._

class KafkaApisHttpProduceForwardingTest {

  private var kafkaApis: KafkaApis = _
  private var replicaManager: ReplicaManager = _
  private var metadataCache: MetadataCache = _
  private var requestChannel: RequestChannel = _
  private var produceForwardManager: ProduceForwardManager = _
  private var httpAsyncExecutor: ScheduledExecutorService = _

  @BeforeEach
  def setUp(): Unit = {
    replicaManager = mock(classOf[ReplicaManager])
    metadataCache = mock(classOf[MetadataCache])
    requestChannel = mock(classOf[RequestChannel])
    produceForwardManager = mock(classOf[ProduceForwardManager])
    httpAsyncExecutor = Executors.newScheduledThreadPool(2)
    // Wire KafkaApis with mocks including produceForwardManager
  }

  @AfterEach
  def tearDown(): Unit = {
    httpAsyncExecutor.shutdown()
    httpAsyncExecutor.awaitTermination(5, TimeUnit.SECONDS)
  }

  @Test
  def testAllLocalPartitionsNoForwarding(): Unit = {
    // Given: all partitions led by this broker
    // When: handleHttpProduceRequest is called
    // Then: replicaManager.appendRecords is called
    //       produceForwardManager.forward is NOT called
  }

  @Test
  def testAllRemotePartitionsForwarded(): Unit = {
    // Given: no partitions led by this broker
    // When: handleHttpProduceRequest is called
    // Then: produceForwardManager.forward is called for each remote leader
    //       replicaManager.appendRecords is NOT called
  }

  @Test
  def testMixedLocalAndRemotePartitions(): Unit = {
    // Given: P0 local, P1 remote (broker 2), P2 remote (broker 3)
    // When: handleHttpProduceRequest is called
    // Then: appendRecords called for P0
    //       forward(2, {P1}) called
    //       forward(3, {P2}) called
    //       merged response contains all 3 partitions
  }

  @Test
  def testRemoteGroupedByLeader(): Unit = {
    // Given: P0 and P2 led by broker 2, P1 led by broker 3
    // When: handleHttpProduceRequest is called
    // Then: forward(2, {P0, P2}) — single call with both partitions
    //       forward(3, {P1}) — separate call
  }

  @Test
  def testLeaderUnknownReturnsLeaderNotAvailable(): Unit = {
    // Given: P0 has no known leader in MetadataCache
    // When: handleHttpProduceRequest is called
    // Then: LEADER_NOT_AVAILABLE for P0
    //       produceForwardManager.forward NOT called for P0
  }

  @Test
  def testRetryOnNotLeaderOrFollower(): Unit = {
    // Given: forward(2, {P1}) returns NOT_LEADER_OR_FOLLOWER
    //        refreshed metadata shows P1 now led by broker 3
    // When: retry fires
    // Then: forward(3, {P1}) is called
    //       if successful, merged response contains P1 result
  }

  @Test
  def testRetryWhenLeaderMovedToLocal(): Unit = {
    // Given: forward(2, {P1}) returns NOT_LEADER_OR_FOLLOWER
    //        refreshed metadata shows P1 now led by this broker
    // When: retry fires
    // Then: appendRecords called for P1 (local retry)
  }

  @Test
  def testTimeoutReturnsRequestTimedOut(): Unit = {
    // Given: remote broker does not respond within forwardingTimeoutMs
    // When: orTimeout fires
    // Then: affected partitions get REQUEST_TIMED_OUT
    //       local partitions that completed still have their results
  }

  @Test
  def testPartialFailureMergesCorrectly(): Unit = {
    // Given: P0 local succeeds, P1 remote succeeds, P2 remote times out
    // When: handleHttpProduceRequest completes
    // Then: P0 has offset, P1 has offset, P2 has REQUEST_TIMED_OUT
  }

  @Test
  def testNoForwardManagerFallsBackToLeaderNotAvailable(): Unit = {
    // Given: produceForwardManager is null (HTTP not enabled)
    // When: handleHttpProduceRequest is called with remote partitions
    // Then: all remote partitions get LEADER_NOT_AVAILABLE
  }
}
```

## Rules

1. **NEVER block the handler thread.** The handler thread must return immediately. All merging and retry logic runs on `httpAsyncExecutor` inside `.handleAsync()`.
2. **At most 1 retry per forwarded group.** Do not implement exponential backoff or multiple retries. The retry budget is bounded by `http.internal.forwarding.timeout.ms`.
3. **Retry is only for `NOT_LEADER_OR_FOLLOWER` and `LEADER_NOT_AVAILABLE`.** Other errors (e.g., `TOPIC_AUTHORIZATION_FAILED`, `MESSAGE_TOO_LARGE`) are not retried.
4. **Group remote partitions by leader.** All partitions led by the same remote broker go in a single `forward()` call, not one call per partition.
5. **Use `handleAsync(httpAsyncExecutor)` for ALL future callbacks.** Never use plain `.handle()`.
6. **Graceful degradation.** If `ProduceForwardManager` is null, fall back to TASK-C.01 behavior. Do not throw.
7. **Preserve TASK-C.01 local path.** The local append logic is unchanged. Only the remote path is new.

## Learning

_To be filled by the executing agent._

## Limitations

_To be filled by the executing agent._

## Field Notes

_To be filled by the executing agent._

## Acceptance Criteria

- [ ] `handleHttpProduceRequest()` correctly buckets partitions by leader (local, remote, unknown).
- [ ] Local partitions are appended via `replicaManager.appendRecords()` (unchanged from C.01).
- [ ] Remote partitions are forwarded via `produceForwardManager.forward(leaderId, ...)`.
- [ ] Partitions for the same remote leader are grouped in a single `forward()` call.
- [ ] Unknown leaders return `LEADER_NOT_AVAILABLE`.
- [ ] `CompletableFuture.allOf` merges local and remote results.
- [ ] `.handleAsync(httpAsyncExecutor)` is used for all future callbacks.
- [ ] Retry fires once on `NOT_LEADER_OR_FOLLOWER` with refreshed metadata.
- [ ] Retry handles leader-moved-to-local case (local append on retry).
- [ ] `orTimeout(forwardingTimeoutMs)` bounds the total wait.
- [ ] Partial failure: succeeded partitions are still returned.
- [ ] Null `produceForwardManager` falls back gracefully.
- [ ] All unit tests pass.
- [ ] Existing TASK-C.01 tests still pass.

## File Manifest

| File | Action | Description |
|------|--------|-------------|
| | | |
