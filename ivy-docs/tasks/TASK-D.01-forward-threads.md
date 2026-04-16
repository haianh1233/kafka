# TASK-D.01: ProduceForwardThread + FetchForwardThread

## Prerequisites

| Task | What it delivers | Why this task needs it |
|------|------------------|-----------------------|
| TASK-A.02 | `http-server` Gradle module with dependencies on `core`, `clients`, `server-common` | These classes live in `http-server` and depend on `InterBrokerSendThread` from `server-common` |

## Context

This task implements the two forwarding thread classes that send binary Kafka protocol requests (PRODUCE, FETCH) to remote leader brokers on behalf of the HTTP handler. Each thread instance is dedicated to a single remote broker and manages a bounded queue of pending requests.

### Design pattern: InterBrokerSendThread

`InterBrokerSendThread` (at `server-common/src/main/java/org/apache/kafka/server/util/InterBrokerSendThread.java`) is an existing Kafka abstract class that manages a `NetworkClient` poll loop on a dedicated thread. Subclasses implement `generateRequests()` which is called each poll cycle to drain queued work into `RequestAndCompletionHandler` objects.

The proven precedent is `TransactionMarkerChannelManager` (at `core/src/main/scala/kafka/coordinator/transaction/TransactionMarkerChannelManager.scala`, line 161) which extends `InterBrokerSendThread` to fan out `WriteTxnMarkersRequest` to multiple partition-leader brokers -- exactly the same pattern needed here.

### Key design decisions (from design doc)

- **One thread per remote broker** (section 7.2, 7.3). Created lazily by `ProduceForwardManager` / `FetchForwardManager` (TASK-D.02).
- **Bounded queue** (section 14.6). `LinkedBlockingQueue` with configurable capacity (`http.internal.forwarding.queue.size`, default 10,000). When full, `offer()` returns false and the future completes exceptionally.
- **CompletableFuture-based API.** The `enqueue()` method returns a `CompletableFuture` that resolves when the remote broker's response arrives.
- **Binary protocol forwarding.** The thread sends standard `ProduceRequest` / `FetchRequest` over the inter-broker listener. No HTTP-to-HTTP forwarding.

### InterBrokerSendThread API (from the codebase)

```java
public abstract class InterBrokerSendThread extends ShutdownableThread {
    protected final KafkaClient networkClient;
    protected InterBrokerSendThread(String name, KafkaClient networkClient,
                                     int requestTimeoutMs, Time time);
    public abstract Collection<RequestAndCompletionHandler> generateRequests();
    public void wakeup();  // interrupt the poll sleep
    public void shutdown() throws InterruptedException;
    // doWork() calls pollOnce() which calls generateRequests(), sends, polls
}
```

```java
public final class RequestAndCompletionHandler {
    public final long creationTimeMs;
    public final Node destination;
    public final AbstractRequest.Builder<? extends AbstractRequest> request;
    public final RequestCompletionHandler handler;
}
```

## Specification

### ProduceForwardThread

- **Queue type**: `LinkedBlockingQueue<PendingProduce>` with bounded capacity.
- **`enqueue()`**: Accepts partition data, required acks, timeout. Returns `CompletableFuture<Map<TopicIdPartition, PartitionResponse>>`. Uses `offer()` (non-blocking). If queue is full, completes the future exceptionally.
- **`generateRequests()`**: Drains queue via `drainTo()`, builds a `ProduceRequest.Builder` per `PendingProduce`, returns `Collection<RequestAndCompletionHandler>`.
- **Completion handler**: On response, parses `ProduceResponse` into per-partition results and completes the future. On disconnect, completes exceptionally with `DisconnectException`.

### FetchForwardThread

- **Queue type**: `LinkedBlockingQueue<PendingFetch>` with bounded capacity.
- **`enqueue()`**: Accepts fetch specs, maxWaitMs, minBytes, maxBytes. Returns `CompletableFuture<Map<TopicIdPartition, FetchPartitionData>>`. Uses `offer()`.
- **`generateRequests()`**: Drains queue, builds `FetchRequest.Builder` per `PendingFetch`.
- **Key difference**: `maxWaitMs` is passed through to the remote broker's `FetchRequest` so the remote broker's `DelayedFetch` purgatory operates within the same time budget (section 7.6).

### PendingProduce / PendingFetch records

Inner record classes holding the request data and the `CompletableFuture` to complete.

## Implementation Details

### Files to create

- `http-server/src/main/java/kafka/server/http/ProduceForwardThread.java`
- `http-server/src/main/java/kafka/server/http/FetchForwardThread.java`

### Package

`kafka.server.http` (Java files in the `http-server` module).

### Metrics

Each thread exposes a gauge for queue size:
```java
metricsGroup.newGauge("ForwardQueueSize",
    () -> pendingQueue.size(),
    Map.of("broker.id", String.valueOf(destination.id())));
```

## Skeleton Code

### ProduceForwardThread

```java
// ============================================================================
// File: http-server/src/main/java/kafka/server/http/ProduceForwardThread.java
// ============================================================================
package kafka.server.http;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.RequestCompletionHandler;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.server.util.InterBrokerSendThread;
import org.apache.kafka.server.util.RequestAndCompletionHandler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

/**
 * One instance per remote leader broker. Drains a queue of pending produce
 * entries and forwards them as binary ProduceRequests over the inter-broker
 * listener. Follows the same pattern as TransactionMarkerChannelManager.
 *
 * <p>Thread lifecycle: created by {@link ProduceForwardManager#forward} via
 * {@code computeIfAbsent}, started immediately, and shut down when the
 * broker is no longer alive or during graceful shutdown.</p>
 *
 * @see org.apache.kafka.server.util.InterBrokerSendThread
 * @see kafka.coordinator.transaction.TransactionMarkerChannelManager
 */
public class ProduceForwardThread extends InterBrokerSendThread {

    private final Node destination;
    private final Time time;
    private final BlockingQueue<PendingProduce> pendingQueue;

    /**
     * @param destination      the remote broker node to forward requests to
     * @param networkClient    the network client for this broker (one per thread)
     * @param requestTimeoutMs timeout for individual requests on the wire
     * @param queueCapacity    max pending entries before rejection (config:
     *                         http.internal.forwarding.queue.size, default 10000)
     * @param time             time source
     */
    public ProduceForwardThread(
            Node destination,
            KafkaClient networkClient,
            int requestTimeoutMs,
            int queueCapacity,
            Time time) {
        super("ProduceForwardThread-" + destination.id(), networkClient, requestTimeoutMs, time);
        this.destination = destination;
        this.time = time;
        this.pendingQueue = new LinkedBlockingQueue<>(queueCapacity);
    }

    /**
     * Returns the destination broker node.
     */
    public Node destination() {
        return destination;
    }

    /**
     * Called by KafkaApis handler thread (via ProduceForwardManager).
     * Returns a CompletableFuture that resolves when the remote broker's
     * ProduceResponse arrives.
     *
     * <p>Uses {@code offer()} (non-blocking). If the queue is full, the
     * future completes exceptionally immediately.</p>
     *
     * @param entriesPerPartition records to forward, keyed by TopicIdPartition
     * @param requiredAcks        acks level (-1, 1, or 0)
     * @param timeoutMs           produce timeout from the HTTP request
     * @return future that completes with per-partition responses
     */
    public CompletableFuture<Map<TopicIdPartition, PartitionResponse>> enqueue(
            Map<TopicIdPartition, MemoryRecords> entriesPerPartition,
            short requiredAcks,
            int timeoutMs) {

        CompletableFuture<Map<TopicIdPartition, PartitionResponse>> future =
                new CompletableFuture<>();

        PendingProduce pending = new PendingProduce(
                entriesPerPartition, requiredAcks, timeoutMs, future);

        if (!pendingQueue.offer(pending)) {
            future.completeExceptionally(
                    new org.apache.kafka.common.errors.NotEnoughReplicasException(
                            "Produce forward queue full for broker " + destination.id()
                                    + " (capacity: " + pendingQueue.remainingCapacity() + ")"));
            return future;
        }

        wakeup();  // interrupt the poll sleep so the request is sent immediately
        return future;
    }

    /**
     * Called each poll cycle by InterBrokerSendThread.doWork() -> pollOnce().
     * Drains the pending queue and builds ProduceRequest for each entry.
     */
    @Override
    public Collection<RequestAndCompletionHandler> generateRequests() {
        List<PendingProduce> batch = new ArrayList<>();
        pendingQueue.drainTo(batch);

        return batch.stream().map(pending -> {
            // Build ProduceRequest from the pending entry
            ProduceRequestData data = new ProduceRequestData()
                    .setAcks(pending.requiredAcks)
                    .setTimeoutMs(pending.timeoutMs);

            // Convert entriesPerPartition to ProduceRequestData.TopicProduceDataCollection
            data.setTopicData(toTopicProduceData(pending.entriesPerPartition));

            ProduceRequest.Builder requestBuilder = ProduceRequest.builder(data);

            // Completion handler: parse response and complete future
            RequestCompletionHandler handler = response -> {
                if (response.disconnected()) {
                    pending.future.completeExceptionally(
                            new DisconnectException(
                                    "Lost connection to broker " + destination.id()));
                } else {
                    try {
                        ProduceResponse produceResponse =
                                (ProduceResponse) response.responseBody();
                        pending.future.complete(
                                parsePartitionResponses(produceResponse, pending.entriesPerPartition));
                    } catch (Exception e) {
                        pending.future.completeExceptionally(e);
                    }
                }
            };

            return new RequestAndCompletionHandler(
                    time.milliseconds(), destination, requestBuilder, handler);
        }).collect(Collectors.toList());
    }

    /**
     * Returns the current queue size (for metrics).
     */
    public int queueSize() {
        return pendingQueue.size();
    }

    // ------------------------------------------------------------------
    // Helper: Convert Map<TopicIdPartition, MemoryRecords> to protocol data
    // ------------------------------------------------------------------

    private ProduceRequestData.TopicProduceDataCollection toTopicProduceData(
            Map<TopicIdPartition, MemoryRecords> entries) {

        // Group by topic
        Map<String, List<Map.Entry<TopicIdPartition, MemoryRecords>>> byTopic = new HashMap<>();
        for (Map.Entry<TopicIdPartition, MemoryRecords> entry : entries.entrySet()) {
            byTopic.computeIfAbsent(entry.getKey().topic(), k -> new ArrayList<>()).add(entry);
        }

        ProduceRequestData.TopicProduceDataCollection collection =
                new ProduceRequestData.TopicProduceDataCollection();

        for (Map.Entry<String, List<Map.Entry<TopicIdPartition, MemoryRecords>>> topicEntry
                : byTopic.entrySet()) {

            String topicName = topicEntry.getKey();
            Uuid topicId = topicEntry.getValue().get(0).getKey().topicId();

            ProduceRequestData.TopicProduceData topicData =
                    new ProduceRequestData.TopicProduceData()
                            .setName(topicName)
                            .setTopicId(topicId);

            for (Map.Entry<TopicIdPartition, MemoryRecords> partEntry : topicEntry.getValue()) {
                topicData.partitionData().add(
                        new ProduceRequestData.PartitionProduceData()
                                .setIndex(partEntry.getKey().partition())
                                .setRecords(partEntry.getValue()));
            }

            collection.add(topicData);
        }

        return collection;
    }

    // ------------------------------------------------------------------
    // Helper: Parse ProduceResponse into per-partition results
    // ------------------------------------------------------------------

    private Map<TopicIdPartition, PartitionResponse> parsePartitionResponses(
            ProduceResponse response,
            Map<TopicIdPartition, MemoryRecords> originalEntries) {

        Map<TopicIdPartition, PartitionResponse> result = new HashMap<>();

        response.data().responses().forEach(topicResponse -> {
            String topicName = topicResponse.name();
            Uuid topicId = topicResponse.topicId();

            topicResponse.partitionResponses().forEach(partResp -> {
                TopicPartition tp = new TopicPartition(topicName, partResp.index());
                TopicIdPartition tidp = new TopicIdPartition(topicId, tp);

                PartitionResponse pr = new PartitionResponse(
                        Errors.forCode(partResp.errorCode()),
                        partResp.baseOffset(),
                        partResp.logAppendTimeMs(),
                        partResp.logStartOffset());
                result.put(tidp, pr);
            });
        });

        return result;
    }

    // ------------------------------------------------------------------
    // Inner class: PendingProduce
    // ------------------------------------------------------------------

    /**
     * Holds a pending produce request and the future to complete when
     * the remote broker responds.
     */
    static class PendingProduce {
        final Map<TopicIdPartition, MemoryRecords> entriesPerPartition;
        final short requiredAcks;
        final int timeoutMs;
        final CompletableFuture<Map<TopicIdPartition, PartitionResponse>> future;

        PendingProduce(
                Map<TopicIdPartition, MemoryRecords> entriesPerPartition,
                short requiredAcks,
                int timeoutMs,
                CompletableFuture<Map<TopicIdPartition, PartitionResponse>> future) {
            this.entriesPerPartition = entriesPerPartition;
            this.requiredAcks = requiredAcks;
            this.timeoutMs = timeoutMs;
            this.future = future;
        }
    }

    // Note: PartitionResponse is org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
    // Import alias may be needed depending on Scala/Java interop in the caller.
    // This class uses the canonical Java type.
    // If PartitionResponse is not directly accessible, use:
    //   import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;
    // or define a local data class.

    /**
     * Re-export type alias for callers. Uses the same PartitionResponse
     * type as ReplicaManager.appendRecords() callback.
     */
    public static class PartitionResponse {
        public final Errors error;
        public final long baseOffset;
        public final long logAppendTimeMs;
        public final long logStartOffset;

        public PartitionResponse(Errors error, long baseOffset,
                                  long logAppendTimeMs, long logStartOffset) {
            this.error = error;
            this.baseOffset = baseOffset;
            this.logAppendTimeMs = logAppendTimeMs;
            this.logStartOffset = logStartOffset;
        }

        public PartitionResponse(Errors error) {
            this(error, -1L, -1L, -1L);
        }
    }
}
```

### FetchForwardThread

```java
// ============================================================================
// File: http-server/src/main/java/kafka/server/http/FetchForwardThread.java
// ============================================================================
package kafka.server.http;

import org.apache.kafka.clients.RequestCompletionHandler;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.server.storage.log.FetchPartitionData;
import org.apache.kafka.server.util.InterBrokerSendThread;
import org.apache.kafka.server.util.RequestAndCompletionHandler;
import org.apache.kafka.common.KafkaException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

/**
 * One instance per remote leader broker. Drains a queue of pending fetch
 * entries and forwards them as binary FetchRequests over the inter-broker
 * listener.
 *
 * <p>Key difference from ProduceForwardThread: the {@code maxWaitMs} budget
 * is passed through to the remote broker's FetchRequest so the remote
 * broker's DelayedFetch purgatory operates within the same time budget.</p>
 *
 * @see org.apache.kafka.server.util.InterBrokerSendThread
 * @see ProduceForwardThread
 */
public class FetchForwardThread extends InterBrokerSendThread {

    private final Node destination;
    private final Time time;
    private final BlockingQueue<PendingFetch> pendingQueue;

    /**
     * @param destination      the remote broker node to forward fetch requests to
     * @param networkClient    the network client for this broker (one per thread)
     * @param requestTimeoutMs timeout for individual requests on the wire
     * @param queueCapacity    max pending entries before rejection (config:
     *                         http.internal.forwarding.queue.size, default 10000)
     * @param time             time source
     */
    public FetchForwardThread(
            Node destination,
            KafkaClient networkClient,
            int requestTimeoutMs,
            int queueCapacity,
            Time time) {
        super("FetchForwardThread-" + destination.id(), networkClient, requestTimeoutMs, time);
        this.destination = destination;
        this.time = time;
        this.pendingQueue = new LinkedBlockingQueue<>(queueCapacity);
    }

    /**
     * Returns the destination broker node.
     */
    public Node destination() {
        return destination;
    }

    /**
     * Called by KafkaApis handler thread (via FetchForwardManager).
     * Returns a CompletableFuture that resolves when the remote broker's
     * FetchResponse arrives.
     *
     * @param fetchSpecs  per-partition fetch specifications
     * @param maxWaitMs   effective max wait time (already capped by section 6.1)
     * @param minBytes    minimum bytes before responding
     * @param maxBytes    maximum total bytes across all partitions
     * @return future that completes with per-partition fetch data
     */
    public CompletableFuture<Map<TopicIdPartition, FetchPartitionData>> enqueue(
            Map<TopicIdPartition, FetchRequest.PartitionData> fetchSpecs,
            int maxWaitMs,
            int minBytes,
            int maxBytes) {

        CompletableFuture<Map<TopicIdPartition, FetchPartitionData>> future =
                new CompletableFuture<>();

        PendingFetch pending = new PendingFetch(
                fetchSpecs, maxWaitMs, minBytes, maxBytes, future);

        if (!pendingQueue.offer(pending)) {
            future.completeExceptionally(
                    new KafkaException(
                            "Fetch forward queue full for broker " + destination.id()));
            return future;
        }

        wakeup();  // interrupt the poll sleep
        return future;
    }

    /**
     * Called each poll cycle. Drains the pending queue and builds
     * FetchRequest for each entry.
     */
    @Override
    public Collection<RequestAndCompletionHandler> generateRequests() {
        List<PendingFetch> batch = new ArrayList<>();
        pendingQueue.drainTo(batch);

        return batch.stream().map(pending -> {
            // Build FetchRequest with the same maxWaitMs budget
            // so the remote broker's DelayedFetch purgatory uses the same deadline
            FetchRequest.Builder requestBuilder = FetchRequest.Builder.forConsumer(
                    FetchRequest.ORDINARY_CONSUMER_ID,  // replicaId = -1 (consumer)
                    pending.maxWaitMs,
                    pending.minBytes,
                    toFetchRequestPartitionDataMap(pending.fetchSpecs)
            ).setMaxBytes(pending.maxBytes);

            RequestCompletionHandler handler = response -> {
                if (response.disconnected()) {
                    pending.future.completeExceptionally(
                            new DisconnectException(
                                    "Lost connection to broker " + destination.id()));
                } else {
                    try {
                        FetchResponse fetchResponse =
                                (FetchResponse) response.responseBody();
                        pending.future.complete(
                                parseFetchPartitionData(fetchResponse));
                    } catch (Exception e) {
                        pending.future.completeExceptionally(e);
                    }
                }
            };

            return new RequestAndCompletionHandler(
                    time.milliseconds(), destination, requestBuilder, handler);
        }).collect(Collectors.toList());
    }

    /**
     * Returns the current queue size (for metrics).
     */
    public int queueSize() {
        return pendingQueue.size();
    }

    // ------------------------------------------------------------------
    // Helper: Convert Map<TopicIdPartition, PartitionData> to LinkedHashMap
    // ------------------------------------------------------------------

    private LinkedHashMap<TopicPartition, FetchRequest.PartitionData> toFetchRequestPartitionDataMap(
            Map<TopicIdPartition, FetchRequest.PartitionData> fetchSpecs) {

        LinkedHashMap<TopicPartition, FetchRequest.PartitionData> result = new LinkedHashMap<>();
        for (Map.Entry<TopicIdPartition, FetchRequest.PartitionData> entry : fetchSpecs.entrySet()) {
            result.put(entry.getKey().topicPartition(), entry.getValue());
        }
        return result;
    }

    // ------------------------------------------------------------------
    // Helper: Parse FetchResponse into per-partition FetchPartitionData
    // ------------------------------------------------------------------

    private Map<TopicIdPartition, FetchPartitionData> parseFetchPartitionData(
            FetchResponse response) {

        Map<TopicIdPartition, FetchPartitionData> result = new HashMap<>();

        response.data().responses().forEach(topicResponse -> {
            String topicName = topicResponse.topic();
            Uuid topicId = topicResponse.topicId();

            topicResponse.partitions().forEach(partData -> {
                TopicPartition tp = new TopicPartition(topicName, partData.partitionIndex());
                TopicIdPartition tidp = new TopicIdPartition(topicId, tp);

                Errors error = Errors.forCode(partData.errorCode());
                MemoryRecords records = (MemoryRecords) partData.records();

                FetchPartitionData fpd = new FetchPartitionData(
                        error,
                        partData.highWatermark(),
                        partData.lastStableOffset(),
                        records,
                        Optional.empty(),    // divergingEpoch
                        OptionalLong.empty(), // lastStableOffsetAsOptional
                        Optional.empty(),    // abortedTransactions
                        OptionalInt.empty(), // preferredReadReplica
                        false                // isReassignmentFetch
                );
                result.put(tidp, fpd);
            });
        });

        return result;
    }

    // ------------------------------------------------------------------
    // Inner class: PendingFetch
    // ------------------------------------------------------------------

    /**
     * Holds a pending fetch request and the future to complete when
     * the remote broker responds.
     */
    static class PendingFetch {
        final Map<TopicIdPartition, FetchRequest.PartitionData> fetchSpecs;
        final int maxWaitMs;
        final int minBytes;
        final int maxBytes;
        final CompletableFuture<Map<TopicIdPartition, FetchPartitionData>> future;

        PendingFetch(
                Map<TopicIdPartition, FetchRequest.PartitionData> fetchSpecs,
                int maxWaitMs,
                int minBytes,
                int maxBytes,
                CompletableFuture<Map<TopicIdPartition, FetchPartitionData>> future) {
            this.fetchSpecs = fetchSpecs;
            this.maxWaitMs = maxWaitMs;
            this.minBytes = minBytes;
            this.maxBytes = maxBytes;
            this.future = future;
        }
    }
}
```

## Tests

### Unit test skeletons

```java
// ============================================================================
// File: http-server/src/test/java/kafka/server/http/ProduceForwardThreadTest.java
// ============================================================================
package kafka.server.http;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.server.util.RequestAndCompletionHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ProduceForwardThreadTest {

    private static final Node DESTINATION = new Node(1, "broker1.example.com", 9092);
    private static final int QUEUE_CAPACITY = 100;
    private static final int REQUEST_TIMEOUT_MS = 30000;

    private KafkaClient networkClient;
    private MockTime time;
    private ProduceForwardThread thread;

    @BeforeEach
    void setUp() {
        networkClient = mock(KafkaClient.class);
        when(networkClient.active()).thenReturn(true);
        time = new MockTime();
        thread = new ProduceForwardThread(
                DESTINATION, networkClient, REQUEST_TIMEOUT_MS, QUEUE_CAPACITY, time);
    }

    @AfterEach
    void tearDown() throws Exception {
        thread.initiateShutdown();
    }

    @Test
    void testEnqueueReturnsFuture() {
        Map<TopicIdPartition, MemoryRecords> entries = singlePartitionEntries();
        CompletableFuture<?> future = thread.enqueue(entries, (short) -1, 5000);
        assertNotNull(future);
        assertFalse(future.isDone());
    }

    @Test
    void testGenerateRequestsDrainsQueue() {
        thread.enqueue(singlePartitionEntries(), (short) -1, 5000);
        thread.enqueue(singlePartitionEntries(), (short) 1, 3000);

        Collection<RequestAndCompletionHandler> requests = thread.generateRequests();
        assertEquals(2, requests.size());

        // Queue should be empty after drain
        assertEquals(0, thread.queueSize());
    }

    @Test
    void testGenerateRequestsReturnsEmptyWhenQueueEmpty() {
        Collection<RequestAndCompletionHandler> requests = thread.generateRequests();
        assertTrue(requests.isEmpty());
    }

    @Test
    void testQueueFullRejectsWithException() {
        // Fill the queue
        for (int i = 0; i < QUEUE_CAPACITY; i++) {
            thread.enqueue(singlePartitionEntries(), (short) -1, 5000);
        }

        // Next enqueue should fail
        CompletableFuture<?> future = thread.enqueue(
                singlePartitionEntries(), (short) -1, 5000);
        assertTrue(future.isCompletedExceptionally());
    }

    @Test
    void testDisconnectCompletesExceptionally() {
        thread.enqueue(singlePartitionEntries(), (short) -1, 5000);
        Collection<RequestAndCompletionHandler> requests = thread.generateRequests();

        // Simulate disconnect
        RequestAndCompletionHandler rach = requests.iterator().next();
        ClientResponse disconnectResponse = mock(ClientResponse.class);
        when(disconnectResponse.disconnected()).thenReturn(true);
        rach.handler.onComplete(disconnectResponse);

        // The future from enqueue should have been completed exceptionally
        // (verification depends on capturing the future from enqueue)
    }

    @Test
    void testDestination() {
        assertEquals(DESTINATION, thread.destination());
    }

    private Map<TopicIdPartition, MemoryRecords> singlePartitionEntries() {
        Map<TopicIdPartition, MemoryRecords> entries = new HashMap<>();
        TopicIdPartition tp = new TopicIdPartition(
                Uuid.randomUuid(),
                new TopicPartition("test-topic", 0));
        entries.put(tp, MemoryRecords.withRecords(
                CompressionType.NONE,
                new SimpleRecord("key".getBytes(), "value".getBytes())));
        return entries;
    }
}
```

```java
// ============================================================================
// File: http-server/src/test/java/kafka/server/http/FetchForwardThreadTest.java
// ============================================================================
package kafka.server.http;

import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.server.util.RequestAndCompletionHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class FetchForwardThreadTest {

    private static final Node DESTINATION = new Node(2, "broker2.example.com", 9092);
    private static final int QUEUE_CAPACITY = 100;
    private static final int REQUEST_TIMEOUT_MS = 30000;

    private KafkaClient networkClient;
    private MockTime time;
    private FetchForwardThread thread;

    @BeforeEach
    void setUp() {
        networkClient = mock(KafkaClient.class);
        when(networkClient.active()).thenReturn(true);
        time = new MockTime();
        thread = new FetchForwardThread(
                DESTINATION, networkClient, REQUEST_TIMEOUT_MS, QUEUE_CAPACITY, time);
    }

    @AfterEach
    void tearDown() throws Exception {
        thread.initiateShutdown();
    }

    @Test
    void testEnqueueReturnsFuture() {
        Map<TopicIdPartition, FetchRequest.PartitionData> specs = singlePartitionFetchSpecs();
        CompletableFuture<?> future = thread.enqueue(specs, 5000, 1, 10485760);
        assertNotNull(future);
        assertFalse(future.isDone());
    }

    @Test
    void testGenerateRequestsDrainsQueue() {
        thread.enqueue(singlePartitionFetchSpecs(), 5000, 1, 10485760);
        thread.enqueue(singlePartitionFetchSpecs(), 3000, 1, 5242880);

        Collection<RequestAndCompletionHandler> requests = thread.generateRequests();
        assertEquals(2, requests.size());
        assertEquals(0, thread.queueSize());
    }

    @Test
    void testMaxWaitMsPassedThrough() {
        // Verify the FetchRequest built by generateRequests uses the
        // maxWaitMs from the enqueue call (same budget for remote purgatory)
        thread.enqueue(singlePartitionFetchSpecs(), 3500, 1, 10485760);

        Collection<RequestAndCompletionHandler> requests = thread.generateRequests();
        RequestAndCompletionHandler rach = requests.iterator().next();
        FetchRequest.Builder builder = (FetchRequest.Builder) rach.request;
        FetchRequest built = builder.build();
        assertEquals(3500, built.maxWait());
    }

    @Test
    void testQueueFullRejectsWithException() {
        for (int i = 0; i < QUEUE_CAPACITY; i++) {
            thread.enqueue(singlePartitionFetchSpecs(), 5000, 1, 10485760);
        }

        CompletableFuture<?> future = thread.enqueue(
                singlePartitionFetchSpecs(), 5000, 1, 10485760);
        assertTrue(future.isCompletedExceptionally());
    }

    @Test
    void testDestination() {
        assertEquals(DESTINATION, thread.destination());
    }

    private Map<TopicIdPartition, FetchRequest.PartitionData> singlePartitionFetchSpecs() {
        Map<TopicIdPartition, FetchRequest.PartitionData> specs = new HashMap<>();
        TopicIdPartition tp = new TopicIdPartition(
                Uuid.randomUuid(),
                new TopicPartition("test-topic", 0));
        specs.put(tp, new FetchRequest.PartitionData(
                tp.topicId(), 0L, -1L, 1048576, Optional.empty()));
        return specs;
    }
}
```

## Rules

1. **Use `offer()` not `put()` on the queue.** `put()` blocks -- the handler thread must never block.
2. **Always call `wakeup()` after `offer()`.** This interrupts the poll sleep so the request is sent without waiting for the next poll cycle.
3. **Bounded queue capacity.** Use `LinkedBlockingQueue(capacity)` with the config value. Never use unbounded queues for forward threads.
4. **One thread per remote broker.** Do not share threads between brokers. The `NetworkClient` inside `InterBrokerSendThread` is per-destination.
5. **FetchForwardThread must pass `maxWaitMs` through.** The remote broker's `DelayedFetch` purgatory must use the same time budget as the originating HTTP request's effective cap.
6. **Parse responses defensively.** Catch exceptions in the completion handler and complete the future exceptionally rather than letting them propagate to `InterBrokerSendThread.doWork()`.
7. **Do NOT implement retry logic in the thread.** Retry (section 5.5C) is the manager's or handler's responsibility.

## Learning

- `ClientResponse` uses `wasDisconnected()` not `disconnected()` in the current Kafka codebase. The skeleton code had this wrong.
- `MemoryRecords` has moved to `org.apache.kafka.common.record.internal.MemoryRecords` (not `org.apache.kafka.common.record.MemoryRecords`).
- `FetchRequest.Builder.forConsumer()` takes `(short maxVersion, int maxWait, int minBytes, Map)` -- the first param is the max API version, not a consumer/replica ID constant.
- `Compression.NONE` replaced `CompressionType.NONE` for `MemoryRecords.withRecords()`.
- The `http-server` module (TASK-A.02 prerequisite) did not exist; it was bootstrapped here with settings.gradle inclusion, build.gradle project block, and checkstyle import-control config.
- `ProduceResponse.PartitionResponse` already exists in the clients library; the skeleton's custom inner `PartitionResponse` class was replaced with the canonical type.

## Limitations

- The `http-server` Gradle module was bootstrapped minimally (TASK-A.02 was not completed). Only the dependencies needed for these two classes are wired. Future tasks may need additional deps.
- Metrics (gauge for `ForwardQueueSize`) are not wired -- only `queueSize()` accessor is exposed. Actual metric registration will happen when the metrics infrastructure is set up.
- No integration test coverage for actual network round-trips; tests only exercise queue mechanics and request generation.

## Field Notes

- The `FetchPartitionData` constructor's `lastStableOffset` parameter (position 5) accepts `OptionalLong` but the response provides `partData.lastStableOffset()` as a `long`. Passing `OptionalLong.empty()` for now since the actual value is already captured in position 3 of the constructor. This may need revisiting.
- Checkstyle in this project enforces no star imports in tests (unlike some other Kafka submodules that use them liberally).

## Acceptance Criteria

- [x] `ProduceForwardThread` compiles and extends `InterBrokerSendThread`.
- [x] `FetchForwardThread` compiles and extends `InterBrokerSendThread`.
- [x] `enqueue()` returns a `CompletableFuture` that does not block the caller.
- [x] `generateRequests()` drains the queue and returns correct `RequestAndCompletionHandler` objects.
- [x] Bounded queue: `offer()` returns false when full; future completes exceptionally.
- [x] `wakeup()` is called after every successful `offer()`.
- [x] `FetchForwardThread` passes `maxWaitMs` through to the built `FetchRequest`.
- [x] Disconnect responses complete futures exceptionally with `DisconnectException`.
- [x] All unit tests pass.
- [x] `queueSize()` metric accessor returns correct values.

## File Manifest

| File | Action | Description |
|------|--------|-------------|
| `http-server/src/main/java/kafka/server/http/ProduceForwardThread.java` | Created | Forwarding thread for ProduceRequests to remote leader brokers |
| `http-server/src/main/java/kafka/server/http/FetchForwardThread.java` | Created | Forwarding thread for FetchRequests to remote leader brokers |
| `http-server/src/test/java/kafka/server/http/ProduceForwardThreadTest.java` | Created | Unit tests for ProduceForwardThread (6 tests) |
| `http-server/src/test/java/kafka/server/http/FetchForwardThreadTest.java` | Created | Unit tests for FetchForwardThread (6 tests) |
| `checkstyle/import-control-http-server.xml` | Created | Checkstyle import control for kafka.server.http package |
| `settings.gradle` | Modified | Added `http-server` to project includes |
| `build.gradle` | Modified | Added `project(':http-server')` block with deps and checkstyle config |
