# TASK-F.02: Full Error Handling + Forwarding Retry

## Prerequisites

| Task | What it provides |
|------|-----------------|
| TASK-D.03 | `ProduceForwardManager` / `ProduceForwardThread` -- produce forwarding |
| TASK-D.04 | `FetchForwardManager` / `FetchForwardThread` -- fetch forwarding |

Both must be merged and passing CI before this task begins.

---

## Context

The HTTP protocol design (section 5.5) specifies comprehensive error handling tables for
request validation errors, Kafka broker errors, and forwarding-specific edge cases. This
task implements the complete error handling matrix and adds forwarding retry logic for
transient leader-change errors.

Currently, the forwarding path returns errors directly to the HTTP client. This task adds:
1. Full error-to-HTTP-status mapping per the design doc tables
2. Forwarding retry on `NOT_LEADER_OR_FOLLOWER` -- refresh metadata, re-bucket, re-enqueue, max 1 retry bounded by `http.internal.forwarding.timeout.ms`
3. Retry-After headers on retriable errors
4. 207 Multi-Status for mixed success/failure in multi-partition batches

---

## Specification

### Error Tables (from design doc section 5.5)

#### A. Request Validation (before touching Kafka)

| Condition | HTTP Status | errorCode |
|---|---|---|
| Body missing or empty | 400 | `INVALID_REQUEST` |
| `records` array null or empty | 400 | `INVALID_REQUEST` |
| `records` exceeds `http.produce.max.records` | 413 | `BATCH_TOO_LARGE` |
| `partition` < 0 | 422 | `INVALID_PARTITION` |
| `acks` not one of `all`, `leader`, `none` | 422 | `INVALID_REQUEST` |
| `timeoutMs` <= 0 | 422 | `INVALID_REQUEST` |
| Record size > `max.message.bytes` | 413 | `MESSAGE_TOO_LARGE` |
| BINARY value not valid base64 | 422 | `INVALID_DATA` |
| Total body > `http.request.max.bytes` | 413 | `REQUEST_TOO_LARGE` |

#### B. Kafka Errors (local append path)

| Kafka Error | HTTP Status |
|---|---|
| `UNKNOWN_TOPIC_OR_PARTITION` | 404 |
| `TOPIC_AUTHORIZATION_FAILED` | 403 |
| `CLUSTER_AUTHORIZATION_FAILED` | 403 |
| `INVALID_TOPIC_EXCEPTION` | 400 |
| `MESSAGE_TOO_LARGE` | 413 |
| `RECORD_LIST_TOO_LARGE` | 413 |
| `NOT_ENOUGH_REPLICAS` | 503 |
| `NOT_ENOUGH_REPLICAS_AFTER_APPEND` | 503 |
| `KAFKA_STORAGE_ERROR` | 500 |
| `REQUEST_TIMED_OUT` | 504 |
| Any other | 500 |

#### C. Forwarding-Specific

| Condition | Handling |
|---|---|
| Leader unknown in MetadataCache | 503 `LEADER_NOT_AVAILABLE` + `Retry-After: 1` |
| Forward connection failure | 503 for affected partitions |
| Leader changed (NOT_LEADER_OR_FOLLOWER) | Retry once: refresh metadata, re-bucket, re-enqueue |
| Forward timeout | 504 `GATEWAY_TIMEOUT` |
| Remote retriable error | Same as leader-changed: retry once |
| Remote non-retriable error | Map per table B, no retry |

### Retry Budget

At most **1 retry per forwarded group**, bounded by `http.internal.forwarding.timeout.ms`
(default 10000). No retry for local appends.

### Retry-After Header Policy

| HTTP Status | Retry-After (seconds) |
|---|---|
| 429 | `ceil(throttleTimeMs / 1000)` |
| 503 (`LEADER_NOT_AVAILABLE`) | 1 |
| 503 (`NOT_ENOUGH_REPLICAS`) | 5 |
| 504 | 1 |

---

## Implementation Details

### 1. HttpErrorMapper -- Centralized Error Mapping

Create a utility class that maps Kafka `Errors` to HTTP status codes and Retry-After values.
This is used by both `HttpResponseSerializer` and the forwarding retry logic.

### 2. Forwarding Retry Logic

In `KafkaApis.handleHttpProduceRequest()` (or the forwarding manager), add retry logic:

```
Original forward:
  forward(leaderId, entries, acks, timeout) → future

On future completion:
  if error is NOT_LEADER_OR_FOLLOWER or LEADER_NOT_AVAILABLE or REQUEST_TIMED_OUT:
    if retryBudget > 0 and remainingTimeout > 0:
      refresh metadata for affected partitions
      re-bucket entries by new leader
      forward again with remaining timeout
      retryBudget -= 1
    else:
      return error to HTTP client
  else:
    return result (success or non-retriable error)
```

### 3. Multi-Status (207) Logic

When a produce or consume request spans multiple partitions:
- All succeed -> 200
- All fail with same error -> that error's HTTP status
- Mixed success/failure -> 207 Multi-Status

---

## Skeleton Code

### HttpErrorMapper.java

```java
// http-server/src/main/java/kafka/server/http/HttpErrorMapper.java

package kafka.server.http;

import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.kafka.common.protocol.Errors;

import java.util.EnumMap;
import java.util.Map;
import java.util.OptionalInt;

/**
 * Maps Kafka {@link Errors} to HTTP status codes and Retry-After values.
 *
 * This is the single source of truth for all error-to-HTTP mappings
 * in the HTTP protocol layer.
 */
public final class HttpErrorMapper {

    /** HTTP 207 Multi-Status (not in standard Netty enum) */
    public static final HttpResponseStatus MULTI_STATUS = HttpResponseStatus.valueOf(207);

    private static final Map<Errors, HttpResponseStatus> STATUS_MAP = new EnumMap<>(Errors.class);
    private static final Map<Errors, Integer> RETRY_AFTER_MAP = new EnumMap<>(Errors.class);

    static {
        // 2xx
        STATUS_MAP.put(Errors.NONE, HttpResponseStatus.OK);

        // 4xx
        STATUS_MAP.put(Errors.UNKNOWN_TOPIC_OR_PARTITION, HttpResponseStatus.NOT_FOUND);
        STATUS_MAP.put(Errors.TOPIC_AUTHORIZATION_FAILED, HttpResponseStatus.FORBIDDEN);
        STATUS_MAP.put(Errors.CLUSTER_AUTHORIZATION_FAILED, HttpResponseStatus.FORBIDDEN);
        STATUS_MAP.put(Errors.GROUP_AUTHORIZATION_FAILED, HttpResponseStatus.FORBIDDEN);
        STATUS_MAP.put(Errors.INVALID_TOPIC_EXCEPTION, HttpResponseStatus.BAD_REQUEST);
        STATUS_MAP.put(Errors.INVALID_REQUEST, HttpResponseStatus.BAD_REQUEST);
        STATUS_MAP.put(Errors.MESSAGE_TOO_LARGE, HttpResponseStatus.valueOf(413));
        STATUS_MAP.put(Errors.RECORD_LIST_TOO_LARGE, HttpResponseStatus.valueOf(413));
        STATUS_MAP.put(Errors.THROTTLING_QUOTA_EXCEEDED, HttpResponseStatus.TOO_MANY_REQUESTS);

        // 5xx
        STATUS_MAP.put(Errors.LEADER_NOT_AVAILABLE, HttpResponseStatus.SERVICE_UNAVAILABLE);
        STATUS_MAP.put(Errors.NOT_LEADER_OR_FOLLOWER, HttpResponseStatus.SERVICE_UNAVAILABLE);
        STATUS_MAP.put(Errors.NOT_ENOUGH_REPLICAS, HttpResponseStatus.SERVICE_UNAVAILABLE);
        STATUS_MAP.put(Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND, HttpResponseStatus.SERVICE_UNAVAILABLE);
        STATUS_MAP.put(Errors.KAFKA_STORAGE_ERROR, HttpResponseStatus.INTERNAL_SERVER_ERROR);
        STATUS_MAP.put(Errors.REQUEST_TIMED_OUT, HttpResponseStatus.GATEWAY_TIMEOUT);

        // Retry-After values (seconds)
        RETRY_AFTER_MAP.put(Errors.LEADER_NOT_AVAILABLE, 1);
        RETRY_AFTER_MAP.put(Errors.NOT_LEADER_OR_FOLLOWER, 1);
        RETRY_AFTER_MAP.put(Errors.NOT_ENOUGH_REPLICAS, 5);
        RETRY_AFTER_MAP.put(Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND, 5);
        RETRY_AFTER_MAP.put(Errors.REQUEST_TIMED_OUT, 1);
    }

    /**
     * Map a Kafka error to an HTTP status code.
     * Unknown errors default to 500 Internal Server Error.
     */
    public static HttpResponseStatus httpStatus(Errors error) {
        return STATUS_MAP.getOrDefault(error, HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }

    /**
     * Get the Retry-After header value for a given error, if applicable.
     * Returns empty if no Retry-After should be set.
     */
    public static OptionalInt retryAfterSeconds(Errors error) {
        Integer value = RETRY_AFTER_MAP.get(error);
        return value != null ? OptionalInt.of(value) : OptionalInt.empty();
    }

    /**
     * Compute Retry-After for throttle quota exceeded.
     *
     * @param throttleTimeMs the throttle time from the Kafka response
     * @return seconds to include in Retry-After header
     */
    public static int throttleRetryAfter(long throttleTimeMs) {
        return (int) Math.ceil(throttleTimeMs / 1000.0);
    }

    /**
     * Determine if a Kafka error is retriable for forwarding purposes.
     * Used by the forwarding retry logic.
     */
    public static boolean isRetriableForForwarding(Errors error) {
        return error == Errors.NOT_LEADER_OR_FOLLOWER
            || error == Errors.LEADER_NOT_AVAILABLE
            || error == Errors.REQUEST_TIMED_OUT;
    }

    /**
     * Compute the aggregate HTTP status for a multi-partition response.
     *
     * @param errorCodes collection of per-partition error codes
     * @return 200 if all NONE, single error's status if uniform, 207 if mixed
     */
    public static HttpResponseStatus aggregateStatus(Iterable<Short> errorCodes) {
        boolean hasSuccess = false;
        boolean hasError = false;
        Errors singleError = null;
        boolean uniformError = true;

        for (short code : errorCodes) {
            Errors error = Errors.forCode(code);
            if (error == Errors.NONE) {
                hasSuccess = true;
            } else {
                hasError = true;
                if (singleError == null) {
                    singleError = error;
                } else if (singleError != error) {
                    uniformError = false;
                }
            }
        }

        if (!hasError) return HttpResponseStatus.OK;
        if (!hasSuccess && uniformError) return httpStatus(singleError);
        return MULTI_STATUS;
    }

    private HttpErrorMapper() {} // utility class
}
```

### ForwardingRetryHandler.java

```java
// http-server/src/main/java/kafka/server/http/ForwardingRetryHandler.java

package kafka.server.http;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.server.common.MetadataCache;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Handles retry logic for produce/fetch forwarding when the remote broker
 * returns a retriable error (NOT_LEADER_OR_FOLLOWER, LEADER_NOT_AVAILABLE, etc.).
 *
 * Retry budget: at most 1 retry per forwarded group, bounded by
 * http.internal.forwarding.timeout.ms.
 */
public class ForwardingRetryHandler {

    private final ProduceForwardManager forwardManager;
    private final MetadataCache metadataCache;
    private final int maxRetries;          // http.internal.forwarding.retries (default 1)
    private final long forwardTimeoutMs;   // http.internal.forwarding.timeout.ms (default 10000)

    public ForwardingRetryHandler(
            ProduceForwardManager forwardManager,
            MetadataCache metadataCache,
            int maxRetries,
            long forwardTimeoutMs) {
        this.forwardManager = forwardManager;
        this.metadataCache = metadataCache;
        this.maxRetries = maxRetries;
        this.forwardTimeoutMs = forwardTimeoutMs;
    }

    /**
     * Forward produce entries to a remote leader with retry on retriable errors.
     *
     * @param leaderId    target leader broker ID
     * @param entries     partition data to forward
     * @param acks        required acks
     * @param timeoutMs   produce timeout from client request
     * @return future with per-partition results
     */
    public CompletableFuture<Map<TopicIdPartition, PartitionResponse>> forwardWithRetry(
            int leaderId,
            Map<TopicIdPartition, MemoryRecords> entries,
            short acks,
            int timeoutMs) {

        long startMs = System.currentTimeMillis();

        return forwardManager.forward(leaderId, entries, acks, timeoutMs)
            .thenCompose(results -> {
                // Check for retriable errors
                Map<TopicIdPartition, MemoryRecords> retriableEntries = new HashMap<>();
                Map<TopicIdPartition, PartitionResponse> finalResults = new HashMap<>(results);

                for (Map.Entry<TopicIdPartition, PartitionResponse> entry : results.entrySet()) {
                    Errors error = entry.getValue().error;
                    if (HttpErrorMapper.isRetriableForForwarding(error)) {
                        retriableEntries.put(entry.getKey(), entries.get(entry.getKey()));
                    }
                }

                if (retriableEntries.isEmpty()) {
                    return CompletableFuture.completedFuture(finalResults);
                }

                // Check retry budget
                long elapsedMs = System.currentTimeMillis() - startMs;
                long remainingMs = forwardTimeoutMs - elapsedMs;
                if (remainingMs <= 0) {
                    return CompletableFuture.completedFuture(finalResults);
                }

                // Refresh metadata and re-bucket
                // NOTE: metadataCache is eventually consistent; a brief sleep
                // may help but is not required since the cache updates asynchronously.
                Map<Integer, Map<TopicIdPartition, MemoryRecords>> rebucketed = rebucketByLeader(retriableEntries);

                // Forward each rebucketed group
                @SuppressWarnings("unchecked")
                CompletableFuture<Map<TopicIdPartition, PartitionResponse>>[] retryFutures =
                    rebucketed.entrySet().stream()
                        .map(e -> forwardManager.forward(
                            e.getKey(), e.getValue(), acks,
                            Math.min(timeoutMs, (int) remainingMs)))
                        .toArray(CompletableFuture[]::new);

                return CompletableFuture.allOf(retryFutures)
                    .orTimeout(remainingMs, TimeUnit.MILLISECONDS)
                    .thenApply(v -> {
                        for (CompletableFuture<Map<TopicIdPartition, PartitionResponse>> f : retryFutures) {
                            finalResults.putAll(f.join());
                        }
                        return finalResults;
                    });
            });
    }

    /**
     * Re-bucket entries by looking up the current leader in MetadataCache.
     */
    private Map<Integer, Map<TopicIdPartition, MemoryRecords>> rebucketByLeader(
            Map<TopicIdPartition, MemoryRecords> entries) {
        Map<Integer, Map<TopicIdPartition, MemoryRecords>> buckets = new HashMap<>();
        for (Map.Entry<TopicIdPartition, MemoryRecords> entry : entries.entrySet()) {
            TopicIdPartition tp = entry.getKey();
            // Look up current leader from metadata cache
            int newLeader = lookupLeader(tp);
            buckets.computeIfAbsent(newLeader, k -> new HashMap<>())
                   .put(tp, entry.getValue());
        }
        return buckets;
    }

    /**
     * Look up the current leader for a partition from MetadataCache.
     * Returns -1 if leader is unknown.
     */
    private int lookupLeader(TopicIdPartition tp) {
        // Implementation delegates to metadataCache.getPartitionLeaderEndpoint()
        // or metadataCache.getLeaderAndIsr()
        return -1; // stub -- to be implemented
    }
}
```

### HttpResponseSerializer updates -- Retry-After and 207

```scala
// http-server/src/main/scala/kafka/server/http/HttpResponseSerializer.scala

// Add to buildJsonResponse or the produce response serializer:

/**
 * Serialize a produce response with proper HTTP status (200, 207, or error).
 * Adds Retry-After header when applicable.
 */
def serializeProduceResponseWithStatus(
  offsets: Seq[PartitionOffsetResult]
): FullHttpResponse = {
  val errorCodes = offsets.map(_.errorCode.toShort).asJava
  val httpStatus = HttpErrorMapper.aggregateStatus(errorCodes)

  val root = MAPPER.createObjectNode()
  val offsetsArray = root.putArray("offsets")
  offsets.foreach { result =>
    val node = offsetsArray.addObject()
    node.put("partition", result.partition)
    if (result.errorCode == 0) {
      node.put("offset", result.offset)
    } else {
      node.putNull("offset")
    }
    node.put("errorCode", result.errorCode.toInt)
    if (result.errorCode != 0) {
      val error = Errors.forCode(result.errorCode)
      node.put("errorMessage", error.name())
    } else {
      node.putNull("errorMessage")
    }
  }

  val response = buildJsonResponse(httpStatus, root)

  // Add Retry-After for applicable errors
  offsets.foreach { result =>
    if (result.errorCode != 0) {
      val error = Errors.forCode(result.errorCode)
      HttpErrorMapper.retryAfterSeconds(error).ifPresent { seconds =>
        response.headers().set("Retry-After", seconds.toString)
      }
    }
  }

  response
}
```

---

## Tests

### Unit Tests -- HttpErrorMapper

```java
// http-server/src/test/java/kafka/server/http/HttpErrorMapperTest.java

package kafka.server.http;

import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.kafka.common.protocol.Errors;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.OptionalInt;

import static org.junit.jupiter.api.Assertions.*;

class HttpErrorMapperTest {

    @Test
    void testNoneMapsto200() {
        assertEquals(HttpResponseStatus.OK, HttpErrorMapper.httpStatus(Errors.NONE));
    }

    @Test
    void testUnknownTopicMapsTo404() {
        assertEquals(HttpResponseStatus.NOT_FOUND,
            HttpErrorMapper.httpStatus(Errors.UNKNOWN_TOPIC_OR_PARTITION));
    }

    @Test
    void testAuthorizationMapsTo403() {
        assertEquals(HttpResponseStatus.FORBIDDEN,
            HttpErrorMapper.httpStatus(Errors.TOPIC_AUTHORIZATION_FAILED));
        assertEquals(HttpResponseStatus.FORBIDDEN,
            HttpErrorMapper.httpStatus(Errors.CLUSTER_AUTHORIZATION_FAILED));
    }

    @Test
    void testMessageTooLargeMapsTo413() {
        assertEquals(HttpResponseStatus.valueOf(413),
            HttpErrorMapper.httpStatus(Errors.MESSAGE_TOO_LARGE));
        assertEquals(HttpResponseStatus.valueOf(413),
            HttpErrorMapper.httpStatus(Errors.RECORD_LIST_TOO_LARGE));
    }

    @Test
    void testLeaderNotAvailableMapsTo503() {
        assertEquals(HttpResponseStatus.SERVICE_UNAVAILABLE,
            HttpErrorMapper.httpStatus(Errors.LEADER_NOT_AVAILABLE));
    }

    @Test
    void testTimeoutMapsTo504() {
        assertEquals(HttpResponseStatus.GATEWAY_TIMEOUT,
            HttpErrorMapper.httpStatus(Errors.REQUEST_TIMED_OUT));
    }

    @Test
    void testUnknownErrorMapsTo500() {
        assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR,
            HttpErrorMapper.httpStatus(Errors.UNKNOWN_SERVER_ERROR));
    }

    @Test
    void testRetryAfterForLeaderNotAvailable() {
        OptionalInt retryAfter = HttpErrorMapper.retryAfterSeconds(Errors.LEADER_NOT_AVAILABLE);
        assertTrue(retryAfter.isPresent());
        assertEquals(1, retryAfter.getAsInt());
    }

    @Test
    void testRetryAfterForNotEnoughReplicas() {
        OptionalInt retryAfter = HttpErrorMapper.retryAfterSeconds(Errors.NOT_ENOUGH_REPLICAS);
        assertTrue(retryAfter.isPresent());
        assertEquals(5, retryAfter.getAsInt());
    }

    @Test
    void testNoRetryAfterFor404() {
        OptionalInt retryAfter = HttpErrorMapper.retryAfterSeconds(Errors.UNKNOWN_TOPIC_OR_PARTITION);
        assertFalse(retryAfter.isPresent());
    }

    @Test
    void testThrottleRetryAfter() {
        assertEquals(1, HttpErrorMapper.throttleRetryAfter(500));
        assertEquals(1, HttpErrorMapper.throttleRetryAfter(1000));
        assertEquals(2, HttpErrorMapper.throttleRetryAfter(1500));
        assertEquals(3, HttpErrorMapper.throttleRetryAfter(2001));
    }

    @Test
    void testIsRetriableForForwarding() {
        assertTrue(HttpErrorMapper.isRetriableForForwarding(Errors.NOT_LEADER_OR_FOLLOWER));
        assertTrue(HttpErrorMapper.isRetriableForForwarding(Errors.LEADER_NOT_AVAILABLE));
        assertTrue(HttpErrorMapper.isRetriableForForwarding(Errors.REQUEST_TIMED_OUT));
        assertFalse(HttpErrorMapper.isRetriableForForwarding(Errors.UNKNOWN_TOPIC_OR_PARTITION));
        assertFalse(HttpErrorMapper.isRetriableForForwarding(Errors.TOPIC_AUTHORIZATION_FAILED));
        assertFalse(HttpErrorMapper.isRetriableForForwarding(Errors.MESSAGE_TOO_LARGE));
    }

    @Test
    void testAggregateStatusAllSuccess() {
        List<Short> codes = List.of((short) 0, (short) 0, (short) 0);
        assertEquals(HttpResponseStatus.OK, HttpErrorMapper.aggregateStatus(codes));
    }

    @Test
    void testAggregateStatusAllSameError() {
        List<Short> codes = List.of((short) 3, (short) 3);  // UNKNOWN_TOPIC_OR_PARTITION
        assertEquals(HttpResponseStatus.NOT_FOUND, HttpErrorMapper.aggregateStatus(codes));
    }

    @Test
    void testAggregateStatusMixed() {
        List<Short> codes = List.of((short) 0, (short) 3);  // NONE + UNKNOWN_TOPIC
        assertEquals(HttpErrorMapper.MULTI_STATUS, HttpErrorMapper.aggregateStatus(codes));
    }

    @Test
    void testAggregateStatusMixedErrors() {
        List<Short> codes = List.of((short) 3, (short) 29);  // UNKNOWN_TOPIC + AUTH_FAILED
        assertEquals(HttpErrorMapper.MULTI_STATUS, HttpErrorMapper.aggregateStatus(codes));
    }
}
```

### Unit Tests -- ForwardingRetryHandler

```java
// http-server/src/test/java/kafka/server/http/ForwardingRetryHandlerTest.java

package kafka.server.http;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ForwardingRetryHandlerTest {

    private final TopicIdPartition tp0 = new TopicIdPartition(Uuid.randomUuid(), 0, "test");

    @Test
    void testNoRetryOnSuccess() throws Exception {
        ProduceForwardManager mockManager = mock(ProduceForwardManager.class);
        Map<TopicIdPartition, PartitionResponse> successResult = Map.of(
            tp0, new PartitionResponse(Errors.NONE, 100L, -1L, -1L)
        );
        when(mockManager.forward(anyInt(), anyMap(), anyShort(), anyInt()))
            .thenReturn(CompletableFuture.completedFuture(successResult));

        ForwardingRetryHandler handler = new ForwardingRetryHandler(
            mockManager, null, 1, 10000);

        var result = handler.forwardWithRetry(1, new HashMap<>(), (short) -1, 5000).get();
        assertEquals(Errors.NONE, result.get(tp0).error);
        verify(mockManager, times(1)).forward(anyInt(), anyMap(), anyShort(), anyInt());
    }

    @Test
    void testRetryOnNotLeaderOrFollower() throws Exception {
        ProduceForwardManager mockManager = mock(ProduceForwardManager.class);
        Map<TopicIdPartition, PartitionResponse> errorResult = Map.of(
            tp0, new PartitionResponse(Errors.NOT_LEADER_OR_FOLLOWER)
        );
        Map<TopicIdPartition, PartitionResponse> successResult = Map.of(
            tp0, new PartitionResponse(Errors.NONE, 100L, -1L, -1L)
        );
        when(mockManager.forward(anyInt(), anyMap(), anyShort(), anyInt()))
            .thenReturn(CompletableFuture.completedFuture(errorResult))
            .thenReturn(CompletableFuture.completedFuture(successResult));

        ForwardingRetryHandler handler = new ForwardingRetryHandler(
            mockManager, null, 1, 10000);

        var result = handler.forwardWithRetry(1, new HashMap<>(), (short) -1, 5000).get();
        // After retry, should have success
        verify(mockManager, atLeast(2)).forward(anyInt(), anyMap(), anyShort(), anyInt());
    }

    @Test
    void testNoRetryOnNonRetriableError() throws Exception {
        ProduceForwardManager mockManager = mock(ProduceForwardManager.class);
        Map<TopicIdPartition, PartitionResponse> errorResult = Map.of(
            tp0, new PartitionResponse(Errors.TOPIC_AUTHORIZATION_FAILED)
        );
        when(mockManager.forward(anyInt(), anyMap(), anyShort(), anyInt()))
            .thenReturn(CompletableFuture.completedFuture(errorResult));

        ForwardingRetryHandler handler = new ForwardingRetryHandler(
            mockManager, null, 1, 10000);

        var result = handler.forwardWithRetry(1, new HashMap<>(), (short) -1, 5000).get();
        assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED, result.get(tp0).error);
        verify(mockManager, times(1)).forward(anyInt(), anyMap(), anyShort(), anyInt());
    }
}
```

---

## Rules

- Every Kafka `Errors` value must map to an HTTP status code. Unknown errors default to 500.
- Retry is limited to 1 attempt per forwarded group, bounded by `http.internal.forwarding.timeout.ms`.
- Retry must NOT be attempted for local appends (only forwarded requests).
- Retry must NOT be attempted for non-retriable errors (403, 404, 413).
- `Retry-After` header must be set on all 429, 503, and 504 responses.
- Multi-partition responses use 207 when there is a mix of success and failure.
- Single-partition responses use the direct HTTP status for that partition's error.
- The `HttpErrorMapper` class is the single source of truth for error mappings -- all serializers and handlers must use it.

---

## Learning

_To be filled by the executing agent._

## Limitations

_To be filled by the executing agent._

## Field Notes

_To be filled by the executing agent._

---

## Acceptance Criteria

- [ ] `HttpErrorMapper` maps all Kafka errors from design doc section 5.5 to correct HTTP statuses
- [ ] `Retry-After` header is set on 429, 503, 504 responses
- [ ] Forwarding retry occurs on `NOT_LEADER_OR_FOLLOWER` with max 1 retry
- [ ] Forwarding retry is bounded by `http.internal.forwarding.timeout.ms`
- [ ] Non-retriable errors are not retried
- [ ] Multi-partition requests return 207 on mixed success/failure
- [ ] Single-partition requests return the direct error status
- [ ] All unit tests pass (HttpErrorMapper, ForwardingRetryHandler)
- [ ] Integration tests confirm retry after leader change

---

## File Manifest

| File | Status |
|------|--------|
| `http-server/src/main/java/kafka/server/http/HttpErrorMapper.java` | |
| `http-server/src/main/java/kafka/server/http/ForwardingRetryHandler.java` | |
| `http-server/src/main/scala/kafka/server/http/HttpResponseSerializer.scala` | |
| `http-server/src/test/java/kafka/server/http/HttpErrorMapperTest.java` | |
| `http-server/src/test/java/kafka/server/http/ForwardingRetryHandlerTest.java` | |
