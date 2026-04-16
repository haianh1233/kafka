# TASK-D.04: Forwarding Integration in Consume Handler + Quota Throttle

## Prerequisites

| Task | What it delivers | Why this task needs it |
|------|------------------|-----------------------|
| TASK-C.02 | `handleHttpConsumeRequest()` in KafkaApis (local leader path) | This task extends that method to add forwarding |
| TASK-D.02 | `FetchForwardManager` (manages per-broker fetch forwarding threads) | Called to forward remote partition fetch requests |

## Context

This task has two parts:

**Part 1: Extend `handleHttpConsumeRequest()`** to fan out fetch requests to remote leader brokers via `FetchForwardManager`, merge results with `CompletableFuture.allOf().handleAsync(httpAsyncExecutor)`, and return a unified response.

**Part 2: HTTP quota throttle adaptation** in `HttpProcessor.processResponses()`. Convert `StartThrottlingResponse` to HTTP 429 with `Retry-After` header, per design doc section 12.3.

### Before this task (TASK-C.02 state)

In TASK-C.02, the handler returns `LEADER_NOT_AVAILABLE` with empty records for any partition where this broker is not the leader.

### After this task

The handler implements full fan-out:

```
is this broker leader for P?
  YES -> ReplicaManager.fetchMessages() directly (existing from C.02)
  NO  -> metadataCache knows leader?
    YES -> FetchForwardManager.forward(leaderId, ...)
    NO  -> partition entry: errorCode=LEADER_NOT_AVAILABLE, records=[]
```

### Key design doc references

- Section 6.2: Fan-out and aggregation.
- Section 6.3: Timeout budget.
- Section 6.8: End-to-end consume with maxWaitMs cap.
- Section 6.9: Cap expires, empty poll.
- Section 7.5: Forwarding decision matrix (consume side).
- Section 7.6: FetchForwardThread design.
- Section 12.3: Quota enforcement -- throttle response adaptation.
- Section 14.1: Async completion pattern.

### effectiveMaxWaitMs budget

The capped `effectiveMaxWaitMs` is passed to both:
1. The local `FetchParams` (for `DelayedFetch` purgatory).
2. The remote `FetchRequest` (via `FetchForwardManager.forward(maxWaitMs=...)`) so the remote broker's purgatory uses the same budget.

This means all fetches (local + remote) complete within the same time window. `CompletableFuture.allOf(...).orTimeout(effectiveMaxWaitMs)` is the hard ceiling.

## Specification

### Part 1: Consume forwarding

#### Modified method

```scala
def handleHttpConsumeRequest(request: RequestChannel.Request): Unit
```

in `/home/anh/kafka/core/src/main/scala/kafka/server/KafkaApis.scala`.

#### New behavior (changes from TASK-C.02)

1. **Partition bucketing**: After authorization, partition fetch specs into:
   - `localFetchSpecs`: `Seq[(TopicIdPartition, PartitionData)]` -- this broker is the leader.
   - `remoteFetchSpecsByLeader`: `Map[Int, Map[TopicIdPartition, FetchRequest.PartitionData]]` -- keyed by remote leader broker ID.
   - `leaderUnknownSpecs`: erroneous entries with `LEADER_NOT_AVAILABLE`.

2. **Local fetch** (mostly unchanged from C.02): Call `replicaManager.fetchMessages()` with `effectiveMaxWaitMs`. Wrap callback in `CompletableFuture`.

3. **Remote forwarding** (new): For each `(leaderId, specs)` group, call `fetchForwardManager.forward(leaderId, specs, effectiveMaxWaitMs, minBytes, maxBytes)`.

4. **Merge with `CompletableFuture.allOf`**: Wait for all futures. Use `.handleAsync(httpAsyncExecutor)`.

5. **Overall timeout**: `CompletableFuture.allOf(...).orTimeout(effectiveMaxWaitMs, MILLISECONDS)`.

6. **Empty partitions on timeout**: Partitions that did not resolve before timeout get `records: [], errorCode: 0` (empty poll, not error).

#### FetchForwardManager injection

```scala
// KafkaApis constructor addition (if not already present):
fetchForwardManager: FetchForwardManager = null
```

### Part 2: HTTP quota throttle adaptation

#### Modified file

`/home/anh/kafka/http-server/src/main/java/kafka/server/http/HttpProcessor.java`

#### Behavior

In `HttpProcessor.processResponses()`, add handling for throttle responses:

| Binary response type | HTTP behavior |
|---|---|
| `StartThrottlingResponse` | Convert to immediate HTTP 429 Too Many Requests with `Retry-After: ceil(throttleTimeMs / 1000)` and JSON body |
| `EndThrottlingResponse` | No-op (HTTP has no mute/unmute concept) |
| `SendResponse` with `throttleTimeMs > 0` | Include `throttleTimeMs` in JSON body + set `Retry-After` header |

The existing binary protocol throttle mechanism (mute channel -> delay -> unmute) does not apply to HTTP because HTTP is stateless request/response. Instead, the broker immediately responds with 429 and the client respects `Retry-After`.

## Implementation Details

### Files to modify

1. `/home/anh/kafka/core/src/main/scala/kafka/server/KafkaApis.scala` -- extend `handleHttpConsumeRequest`
2. `/home/anh/kafka/http-server/src/main/java/kafka/server/http/HttpProcessor.java` -- add throttle handling

### Changes from TASK-C.02 code

**Before (C.02):**
```scala
if (isLocal) {
  interesting += topicIdPartition -> data
} else {
  leaderNotAvailable += topicIdPartition ->
    FetchResponse.partitionResponse(topicIdPartition, Errors.LEADER_NOT_AVAILABLE)
}
```

**After (D.04):**
```scala
if (isLocal) {
  localFetchSpecs += topicIdPartition -> data
} else {
  val leaderOpt = metadataCache.getPartitionInfo(tp.topic, tp.partition)
    .map(_.leader).filter(_ >= 0)
  leaderOpt match {
    case Some(leaderId) =>
      remoteFetchSpecsByLeader.getOrElseUpdate(leaderId, mutable.Map.empty) +=
        (topicIdPartition -> data)
    case None =>
      erroneous += topicIdPartition ->
        FetchResponse.partitionResponse(topicIdPartition, Errors.LEADER_NOT_AVAILABLE)
  }
}
```

## Skeleton Code

### Part 1: Extended handleHttpConsumeRequest

```scala
// ============================================================================
// File: core/src/main/scala/kafka/server/KafkaApis.scala
// REPLACEMENT for handleHttpConsumeRequest() (originally from TASK-C.02)
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
import kafka.server.http.FetchForwardManager

import scala.collection.mutable
import scala.jdk.CollectionConverters._

/**
 * Handles HTTP consume (fetch) requests — FULL path with local fetch + forwarding.
 *
 * Partitions are bucketed by leader:
 *   - LOCAL:   ReplicaManager.fetchMessages() (same as C.02)
 *   - REMOTE:  FetchForwardManager.forward(leaderId, ...)
 *   - UNKNOWN: errorCode=LEADER_NOT_AVAILABLE, records=[]
 *
 * The capped effectiveMaxWaitMs is passed to both local and remote fetches
 * so all branches operate within the same time budget.
 */
def handleHttpConsumeRequest(request: RequestChannel.Request): Unit = {
  val fetchRequest = request.body[FetchRequest]
  val localBrokerId = config.brokerId

  // ---------------------------------------------------------------
  // 1. Clamp maxWaitMs (same as C.02)
  // ---------------------------------------------------------------
  val effectiveMaxWaitMs: Int = Math.min(
    fetchRequest.maxWait,
    config.httpConsumeMaxWaitMs
  )
  request.requestLocalProperties.put("httpMaxWaitApplied", effectiveMaxWaitMs.asInstanceOf[AnyRef])

  // ---------------------------------------------------------------
  // 2. Resolve topic names
  // ---------------------------------------------------------------
  val topicNames: util.Map[Uuid, String] =
    if (fetchRequest.version() >= 13) metadataCache.topicIdsToNames()
    else Collections.emptyMap[Uuid, String]()

  val fetchData = fetchRequest.fetchData(topicNames)

  // ---------------------------------------------------------------
  // 3. Authorization
  // ---------------------------------------------------------------
  val erroneous = mutable.ArrayBuffer[(TopicIdPartition, FetchResponseData.PartitionData)]()
  val localFetchSpecs = mutable.ArrayBuffer[(TopicIdPartition, FetchRequest.PartitionData)]()
  val remoteFetchSpecsByLeader = mutable.Map[Int, mutable.Map[TopicIdPartition, FetchRequest.PartitionData]]()

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
      // 4. Leader check and bucketing
      // ---------------------------------------------------------------
      val partitionInfo = metadataCache.getPartitionInfo(
        topicIdPartition.topicPartition.topic,
        topicIdPartition.topicPartition.partition
      )

      partitionInfo match {
        case Some(info) if info.leader == localBrokerId =>
          localFetchSpecs += topicIdPartition -> data

        case Some(info) if info.leader >= 0 =>
          remoteFetchSpecsByLeader.getOrElseUpdate(info.leader, mutable.Map.empty) +=
            (topicIdPartition -> data)

        case _ =>
          erroneous += topicIdPartition ->
            FetchResponse.partitionResponse(topicIdPartition, Errors.LEADER_NOT_AVAILABLE)
      }
    }
  }

  // ---------------------------------------------------------------
  // 5. Response builder
  // ---------------------------------------------------------------
  def buildAndSendResponse(
    localResults: Seq[(TopicIdPartition, FetchPartitionData)],
    remoteResults: Map[TopicIdPartition, FetchPartitionData]
  ): Unit = {
    val partitions = new util.LinkedHashMap[TopicIdPartition, FetchResponseData.PartitionData]

    // Convert local fetch results
    localResults.foreach { case (topicIdPartition, data) =>
      partitions.put(topicIdPartition, toFetchResponsePartitionData(data))
    }

    // Convert remote fetch results
    remoteResults.foreach { case (topicIdPartition, data) =>
      partitions.put(topicIdPartition, toFetchResponsePartitionData(data))
    }

    // Add erroneous partitions
    erroneous.foreach { case (tp, data) => partitions.put(tp, data) }

    // Quota handling
    val timeMs = time.milliseconds()
    val responseSize = partitions.values().asScala
      .map(p => if (p.records != null) p.records.sizeInBytes else 0).sum
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

    val fetchResponse = new FetchResponse(
      new FetchResponseData()
        .setThrottleTimeMs(maxThrottleTimeMs)
        .setResponses(FetchResponse.toResponseDataList(partitions))
    )
    requestChannel.sendResponse(request, fetchResponse, None)
  }

  // Helper: convert FetchPartitionData to FetchResponseData.PartitionData
  def toFetchResponsePartitionData(data: FetchPartitionData): FetchResponseData.PartitionData = {
    new FetchResponseData.PartitionData()
      .setErrorCode(data.error.code)
      .setHighWatermark(data.highWatermark)
      .setLastStableOffset(data.lastStableOffset.orElse(FetchResponse.INVALID_LAST_STABLE_OFFSET))
      .setLogStartOffset(data.logStartOffset)
      .setAbortedTransactions(data.abortedTransactions.orElse(null))
      .setRecords(data.records)
      .setPreferredReadReplica(FetchResponse.INVALID_PREFERRED_REPLICA_ID)
  }

  // ---------------------------------------------------------------
  // 6. If nothing to fetch, respond immediately
  // ---------------------------------------------------------------
  if (localFetchSpecs.isEmpty && remoteFetchSpecsByLeader.isEmpty) {
    buildAndSendResponse(Seq.empty, Map.empty)
    return
  }

  // ---------------------------------------------------------------
  // 7. Build FetchParams for local fetch
  // ---------------------------------------------------------------
  val fetchMaxBytes = Math.min(fetchRequest.maxBytes, config.fetchMaxBytes)
  val fetchMinBytes = Math.min(fetchRequest.minBytes, fetchMaxBytes)

  val params = new FetchParams(
    -1,           // replicaId = consumer
    -1L,          // replicaEpoch = no epoch check (section 6.6)
    effectiveMaxWaitMs.toLong,
    fetchMinBytes,
    fetchMaxBytes,
    FetchIsolation.of(fetchRequest),
    Optional.empty[ClientMetadata]()
  )

  // ---------------------------------------------------------------
  // 8. LOCAL fetch (async via CompletableFuture)
  // ---------------------------------------------------------------
  val localFuture = new CompletableFuture[Seq[(TopicIdPartition, FetchPartitionData)]]()

  if (localFetchSpecs.nonEmpty) {
    replicaManager.fetchMessages(
      params = params,
      fetchInfos = localFetchSpecs,
      quota = quotas.fetch,
      responseCallback = { results: Seq[(TopicIdPartition, FetchPartitionData)] =>
        localFuture.complete(results)
      }
    )
  } else {
    localFuture.complete(Seq.empty)
  }

  // ---------------------------------------------------------------
  // 9. REMOTE fetch via FetchForwardManager
  // ---------------------------------------------------------------
  val remoteFutures: Seq[CompletableFuture[Map[TopicIdPartition, FetchPartitionData]]] =
    if (fetchForwardManager != null && remoteFetchSpecsByLeader.nonEmpty) {
      remoteFetchSpecsByLeader.toSeq.map { case (leaderId, specs) =>
        fetchForwardManager.forward(
          leaderId,
          specs.toMap.asJava,
          effectiveMaxWaitMs,
          fetchMinBytes,
          fetchMaxBytes
        )
      }
    } else {
      // No forwarder -- add all remote specs as LEADER_NOT_AVAILABLE
      remoteFetchSpecsByLeader.foreach { case (_, specs) =>
        specs.keys.foreach { tp =>
          erroneous += tp ->
            FetchResponse.partitionResponse(tp, Errors.LEADER_NOT_AVAILABLE)
        }
      }
      Seq.empty
    }

  // ---------------------------------------------------------------
  // 10. Combine all futures and merge results
  // ---------------------------------------------------------------
  val allFutures: Array[CompletableFuture[_]] =
    (Seq(localFuture) ++ remoteFutures).toArray

  CompletableFuture.allOf(allFutures: _*)
    .orTimeout(effectiveMaxWaitMs.toLong, TimeUnit.MILLISECONDS)
    .handleAsync(
      new BiFunction[Void, Throwable, Unit] {
        override def apply(ignored: Void, ex: Throwable): Unit = {
          // Collect local results
          val localResults: Seq[(TopicIdPartition, FetchPartitionData)] =
            try { localFuture.getNow(Seq.empty) }
            catch { case _: Exception => Seq.empty }

          // Collect remote results
          val remoteResults = mutable.Map[TopicIdPartition, FetchPartitionData]()
          remoteFutures.foreach { future =>
            try {
              if (future.isDone && !future.isCompletedExceptionally) {
                val results = future.getNow(null)
                if (results != null) {
                  results.forEach { (tp, data) => remoteResults += tp -> data }
                }
              }
              // Partitions from failed futures are simply absent from results
              // They will not appear in the response (or appear as empty)
            } catch {
              case _: Exception => // ignore failed futures
            }
          }

          // Any remote partitions that did not return results get
          // empty records (not an error -- section 6.9)
          remoteFetchSpecsByLeader.foreach { case (_, specs) =>
            specs.keys.foreach { tp =>
              if (!remoteResults.contains(tp)) {
                // Empty poll result -- not an error
                remoteResults += tp -> FetchPartitionData.empty()
              }
            }
          }

          buildAndSendResponse(localResults, remoteResults.toMap)
        }
      },
      httpAsyncExecutor
    )

  // Handler thread returns immediately
}
```

### Part 2: HttpProcessor throttle handling

```java
// ============================================================================
// File: http-server/src/main/java/kafka/server/http/HttpProcessor.java
// ADD to the processResponses() method
// ============================================================================

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.HttpHeaderNames;

import java.nio.charset.StandardCharsets;

// Inside processResponses(), add handling for throttle responses:

/**
 * Process responses from RequestChannel and write HTTP responses to Netty.
 * Extended to handle quota throttle adaptation per section 12.3.
 */
public void processResponses() {
    List<RequestChannel.Response> batch = new ArrayList<>();
    responseQueue.drainTo(batch);

    for (RequestChannel.Response response : batch) {
        String connectionId = response.request().context().connectionId();
        ChannelHandlerContext ctx = channels.get(connectionId);

        if (response instanceof RequestChannel.SendResponse sendResp) {
            if (ctx != null && ctx.channel().isActive()) {
                AbstractResponse kafkaResponse = sendResp.response();
                String originalUri = extractOriginalUri(response.request());
                FullHttpResponse httpResponse = HttpResponseSerializer.serialize(
                        kafkaResponse, originalUri, response.request());

                // If throttleTimeMs > 0, add Retry-After header
                int throttleTimeMs = extractThrottleTimeMs(kafkaResponse);
                if (throttleTimeMs > 0) {
                    int retryAfterSeconds = (int) Math.ceil(throttleTimeMs / 1000.0);
                    httpResponse.headers().set("Retry-After", String.valueOf(retryAfterSeconds));
                }

                ctx.writeAndFlush(httpResponse);
            }
            channels.remove(connectionId);

        } else if (response instanceof RequestChannel.StartThrottlingResponse startThrottle) {
            // ---------------------------------------------------------------
            // Section 12.3: Convert StartThrottlingResponse to HTTP 429
            //
            // The binary protocol mutes the channel during throttling.
            // HTTP cannot mute -- instead, immediately return 429 with
            // Retry-After header.
            // ---------------------------------------------------------------
            if (ctx != null && ctx.channel().isActive()) {
                int throttleTimeMs = startThrottle.throttleTimeMs();
                int retryAfterSeconds = Math.max(1, (int) Math.ceil(throttleTimeMs / 1000.0));

                String jsonBody = String.format(
                    "{\"errorCode\":%d,\"errorMessage\":\"%s\",\"throttleTimeMs\":%d}",
                    89,  // Errors.THROTTLING_QUOTA_EXCEEDED.code()
                    "THROTTLING_QUOTA_EXCEEDED",
                    throttleTimeMs
                );

                FullHttpResponse httpResponse = new DefaultFullHttpResponse(
                    HttpVersion.HTTP_1_1,
                    HttpResponseStatus.TOO_MANY_REQUESTS,  // 429
                    Unpooled.copiedBuffer(jsonBody, StandardCharsets.UTF_8)
                );
                httpResponse.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json");
                httpResponse.headers().set(HttpHeaderNames.CONTENT_LENGTH,
                    httpResponse.content().readableBytes());
                httpResponse.headers().set("Retry-After", String.valueOf(retryAfterSeconds));

                ctx.writeAndFlush(httpResponse);
            }
            channels.remove(connectionId);

        } else if (response instanceof RequestChannel.EndThrottlingResponse) {
            // ---------------------------------------------------------------
            // Section 12.3: EndThrottlingResponse is a no-op for HTTP.
            // Binary protocol unmutes the channel here. HTTP has no
            // mute/unmute concept -- the 429 was already sent.
            // ---------------------------------------------------------------
            // No action needed

        } else if (response instanceof RequestChannel.CloseConnectionResponse) {
            if (ctx != null) ctx.close();
            channels.remove(connectionId);
        }
    }
}

/**
 * Extract throttleTimeMs from a Kafka response.
 * Most response types have a throttleTimeMs field in their data.
 */
private int extractThrottleTimeMs(AbstractResponse response) {
    try {
        // ProduceResponse, FetchResponse, etc. all have throttleTimeMs()
        return response.throttleTimeMs();
    } catch (Exception e) {
        return 0;
    }
}
```

## Tests

### Unit test skeletons

```scala
// ============================================================================
// File: core/src/test/scala/unit/kafka/server/KafkaApisHttpConsumeForwardingTest.scala
// ============================================================================
package kafka.server

import java.util
import java.util.concurrent.{CompletableFuture, Executors, ScheduledExecutorService, TimeUnit}
import org.apache.kafka.common.{TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.common.message.FetchResponseData
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{FetchRequest, FetchResponse}
import org.apache.kafka.network.RequestChannel
import org.apache.kafka.server.storage.log.FetchPartitionData
import kafka.server.http.FetchForwardManager
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.api.Assertions._
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._

class KafkaApisHttpConsumeForwardingTest {

  private var kafkaApis: KafkaApis = _
  private var replicaManager: ReplicaManager = _
  private var metadataCache: MetadataCache = _
  private var requestChannel: RequestChannel = _
  private var fetchForwardManager: FetchForwardManager = _
  private var httpAsyncExecutor: ScheduledExecutorService = _

  @BeforeEach
  def setUp(): Unit = {
    replicaManager = mock(classOf[ReplicaManager])
    metadataCache = mock(classOf[MetadataCache])
    requestChannel = mock(classOf[RequestChannel])
    fetchForwardManager = mock(classOf[FetchForwardManager])
    httpAsyncExecutor = Executors.newScheduledThreadPool(2)
  }

  @AfterEach
  def tearDown(): Unit = {
    httpAsyncExecutor.shutdown()
    httpAsyncExecutor.awaitTermination(5, TimeUnit.SECONDS)
  }

  @Test
  def testAllLocalPartitionsNoForwarding(): Unit = {
    // Given: all partitions led by this broker
    // Then: fetchMessages called, forward NOT called
  }

  @Test
  def testAllRemotePartitionsForwarded(): Unit = {
    // Given: no local partitions
    // Then: forward called for each remote leader, fetchMessages NOT called
  }

  @Test
  def testMixedLocalAndRemotePartitions(): Unit = {
    // Given: P0 local, P1 remote (broker 2)
    // Then: fetchMessages for P0, forward(2, {P1}) for P1
    //       merged response contains both
  }

  @Test
  def testEffectiveMaxWaitMsPassedToRemote(): Unit = {
    // Given: maxWaitMs=30000, config cap=5000
    // Then: forward called with maxWaitMs=5000
  }

  @Test
  def testTimeoutReturnsEmptyRecordsNotError(): Unit = {
    // Given: remote future times out
    // Then: timed-out partitions get empty records with errorCode=0
    //       (section 6.9: empty poll, not error)
  }

  @Test
  def testLeaderUnknownReturnsLeaderNotAvailable(): Unit = {
    // Given: P0 has no known leader
    // Then: LEADER_NOT_AVAILABLE for P0
  }

  @Test
  def testMaxWaitAppliedHeaderSet(): Unit = {
    // Given: any consume request
    // Then: httpMaxWaitApplied set in request properties
  }

  @Test
  def testNoForwardManagerFallsBack(): Unit = {
    // Given: fetchForwardManager is null
    // Then: remote partitions get LEADER_NOT_AVAILABLE
  }
}
```

```java
// ============================================================================
// File: http-server/src/test/java/kafka/server/http/HttpProcessorThrottleTest.java
// ============================================================================
package kafka.server.http;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.kafka.network.RequestChannel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class HttpProcessorThrottleTest {

    private HttpProcessor processor;

    @BeforeEach
    void setUp() {
        // Create HttpProcessor with mock dependencies
    }

    @Test
    void testStartThrottlingReturns429() {
        // Given: a StartThrottlingResponse with throttleTimeMs=500
        // When: processResponses is called
        // Then: HTTP 429 is written with Retry-After: 1
    }

    @Test
    void testStartThrottlingRetryAfterCeiling() {
        // Given: throttleTimeMs=1500
        // Then: Retry-After: 2 (ceil(1500/1000))
    }

    @Test
    void testStartThrottlingMinimumRetryAfter() {
        // Given: throttleTimeMs=100
        // Then: Retry-After: 1 (minimum 1 second)
    }

    @Test
    void testEndThrottlingIsNoOp() {
        // Given: an EndThrottlingResponse
        // When: processResponses is called
        // Then: no HTTP response is written
    }

    @Test
    void testSendResponseWithThrottleTimeAddsRetryAfterHeader() {
        // Given: a SendResponse where response.throttleTimeMs() > 0
        // When: processResponses is called
        // Then: HTTP response includes Retry-After header
    }

    @Test
    void testThrottleResponseJsonBody() {
        // Given: StartThrottlingResponse with throttleTimeMs=500
        // When: processResponses sends 429
        // Then: body contains errorCode=89, errorMessage=THROTTLING_QUOTA_EXCEEDED,
        //       throttleTimeMs=500
    }
}
```

## Rules

1. **NEVER block the handler thread.** Same rule as all HTTP handlers.
2. **Pass `effectiveMaxWaitMs` to remote fetches.** The remote broker's `DelayedFetch` purgatory must use the same time budget.
3. **Empty poll on timeout is NOT an error.** Timed-out partitions return `records: [], errorCode: 0` per section 6.9.
4. **Use `.handleAsync(httpAsyncExecutor)` for all future callbacks.** Never plain `.handle()`.
5. **HTTP 429 for throttling.** `StartThrottlingResponse` must produce a 429 response, not be silently swallowed.
6. **`Retry-After` is in seconds, ceiling.** `ceil(throttleTimeMs / 1000)` with minimum 1.
7. **`EndThrottlingResponse` is a no-op.** Do not write anything to the channel.
8. **Group remote partitions by leader.** Same as TASK-D.03.
9. **`X-Kafka-MaxWait-Applied` header must still be set.** Unchanged from C.02.

## Learning

_To be filled by the executing agent._

## Limitations

_To be filled by the executing agent._

## Field Notes

_To be filled by the executing agent._

## Acceptance Criteria

### Part 1: Consume forwarding

- [ ] `handleHttpConsumeRequest()` correctly buckets partitions by leader.
- [ ] Local partitions are fetched via `replicaManager.fetchMessages()`.
- [ ] Remote partitions are forwarded via `fetchForwardManager.forward(leaderId, ...)`.
- [ ] `effectiveMaxWaitMs` is passed to both local and remote fetches.
- [ ] `CompletableFuture.allOf` merges local and remote results.
- [ ] `.handleAsync(httpAsyncExecutor)` is used for all callbacks.
- [ ] Timed-out remote partitions return empty records (not error).
- [ ] Unknown leaders return `LEADER_NOT_AVAILABLE`.
- [ ] Null `fetchForwardManager` falls back gracefully.
- [ ] `X-Kafka-MaxWait-Applied` header is set.

### Part 2: Quota throttle

- [ ] `StartThrottlingResponse` produces HTTP 429 with `Retry-After` header.
- [ ] `Retry-After` is `ceil(throttleTimeMs / 1000)`, minimum 1.
- [ ] `EndThrottlingResponse` is a no-op.
- [ ] `SendResponse` with `throttleTimeMs > 0` adds `Retry-After` header.
- [ ] 429 response body contains `errorCode: 89`, `errorMessage`, `throttleTimeMs`.
- [ ] All unit tests pass.
- [ ] Existing TASK-C.02 tests still pass.

## File Manifest

| File | Action | Description |
|------|--------|-------------|
| | | |
