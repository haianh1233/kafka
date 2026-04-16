# TASK-D.02: ProduceForwardManager + FetchForwardManager

## Prerequisites

| Task | What it delivers | Why this task needs it |
|------|------------------|-----------------------|
| TASK-D.01 | `ProduceForwardThread` and `FetchForwardThread` (extends `InterBrokerSendThread`) | Managers create and manage instances of these threads |

## Context

This task implements the manager classes that own the lifecycle of forwarding threads. Each manager maintains a `ConcurrentHashMap<Integer, ForwardThread>` keyed by broker ID, creating threads lazily via `computeIfAbsent` and providing periodic cleanup of threads targeting dead/stale brokers.

### Design pattern (from design doc section 7.3)

The manager is a thread-pool-per-broker abstraction. When `handleHttpProduceRequest()` or `handleHttpConsumeRequest()` determines that a partition's leader is a remote broker, it calls `manager.forward(leaderId, entries, ...)`. The manager:

1. Looks up or creates a `ForwardThread` for that broker ID.
2. Calls `thread.enqueue(...)` which returns a `CompletableFuture`.
3. Returns the future to the caller (KafkaApis handler).

Thread creation requires:
- Resolving the broker node via `MetadataCache.getAliveBrokerNode(brokerId, interBrokerListenerName)`.
- Building a `NetworkClient` with the inter-broker security context (same pattern as `TransactionMarkerChannelManager`).
- Starting the thread.

### Stale thread cleanup (from design doc section 7.3)

A scheduled task calls `cleanupStaleThreads()` every 60 seconds. A thread is stale if:
- The broker is no longer alive in `MetadataCache`.
- The broker's address changed (different `Node` object).

Stale threads are removed from the map and shut down gracefully.

### TransactionMarkerChannelManager precedent

`TransactionMarkerChannelManager` (at `core/src/main/scala/kafka/coordinator/transaction/TransactionMarkerChannelManager.scala`) follows this exact pattern:
- Uses `ChannelBuilders.clientChannelBuilder()` with `config.interBrokerSecurityProtocol`.
- Creates `Selector` and `NetworkClient`.
- Extends `InterBrokerSendThread` (single thread, not per-broker like our design).

Our design uses one thread per broker, so the `NetworkClient` creation is per-broker. The `ChannelBuilder` and security config are shared.

## Specification

### ProduceForwardManager

```java
public class ProduceForwardManager implements Closeable {
    // Lazily-created threads, one per remote broker
    private final ConcurrentHashMap<Integer, ProduceForwardThread> threads;

    // Forward produce entries to a remote leader broker
    public CompletableFuture<Map<TopicIdPartition, PartitionResponse>> forward(
            int leaderId,
            Map<TopicIdPartition, MemoryRecords> entries,
            short requiredAcks,
            int timeoutMs);

    // Remove threads for dead/stale brokers
    public void cleanupStaleThreads();

    // Graceful shutdown of all threads
    public void close();
}
```

### FetchForwardManager

```java
public class FetchForwardManager implements Closeable {
    private final ConcurrentHashMap<Integer, FetchForwardThread> threads;

    public CompletableFuture<Map<TopicIdPartition, FetchPartitionData>> forward(
            int leaderId,
            Map<TopicIdPartition, FetchRequest.PartitionData> fetchSpecs,
            int maxWaitMs,
            int minBytes,
            int maxBytes);

    public void cleanupStaleThreads();

    public void close();
}
```

### Constructor dependencies

Both managers need:
- `MetadataCache` -- to resolve broker nodes.
- `KafkaConfig` -- for inter-broker listener name, security protocol, timeouts, queue size.
- `Time` -- for time source.
- `Metrics` -- for queue size gauges.
- `LogContext` -- for logging.

### NetworkClient construction

Each `ForwardThread` gets its own `NetworkClient`. Construction follows the same pattern as `TransactionMarkerChannelManager.apply()`:

```java
ChannelBuilder channelBuilder = ChannelBuilders.clientChannelBuilder(
    config.interBrokerSecurityProtocol(),
    JaasContext.Type.SERVER,
    config,
    config.interBrokerListenerName(),
    config.saslMechanismInterBrokerProtocol(),
    time,
    logContext
);

Selector selector = new Selector(
    NetworkReceive.UNLIMITED,
    config.connectionsMaxIdleMs(),
    metrics,
    time,
    "http-forward-" + brokerId,
    Collections.emptyMap(),
    false,
    channelBuilder,
    logContext
);

NetworkClient networkClient = new NetworkClient(
    selector,
    new ManualMetadataUpdater(),
    "broker-" + config.brokerId() + "-http-forward-" + brokerId,
    1,    // maxInFlightRequestsPerConnection
    50,   // reconnectBackoffMs
    50,   // reconnectBackoffMaxMs
    Selectable.USE_DEFAULT_BUFFER_SIZE,
    config.socketReceiveBufferBytes(),
    config.requestTimeoutMs(),
    config.connectionSetupTimeoutMs(),
    config.connectionSetupTimeoutMaxMs(),
    time,
    true, // discoverBrokerVersions
    new ApiVersions(),
    logContext
);
```

## Implementation Details

### Files to create

- `http-server/src/main/java/kafka/server/http/ProduceForwardManager.java`
- `http-server/src/main/java/kafka/server/http/FetchForwardManager.java`

### Package

`kafka.server.http`

### Thread safety

- `ConcurrentHashMap.computeIfAbsent` ensures at most one thread per broker.
- `cleanupStaleThreads()` iterates the map and removes entries; `ConcurrentHashMap.forEach` is safe for concurrent modification.
- `close()` initiates shutdown of all threads, then awaits termination.

### Integration point

These managers are instantiated in `HttpAcceptor.startup()` (or `BrokerServer`) and injected into `KafkaApis` for use in `handleHttpProduceRequest()` and `handleHttpConsumeRequest()`.

## Skeleton Code

### ProduceForwardManager

```java
// ============================================================================
// File: http-server/src/main/java/kafka/server/http/ProduceForwardManager.java
// ============================================================================
package kafka.server.http;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ManualMetadataUpdater;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.ChannelBuilders;
import org.apache.kafka.common.network.NetworkReceive;
import org.apache.kafka.common.network.Selectable;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.security.JaasContext;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.metadata.MetadataCache;
import org.apache.kafka.server.config.KafkaConfig;

import java.io.Closeable;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages per-broker {@link ProduceForwardThread} instances for forwarding
 * HTTP produce requests to remote partition leaders over the binary protocol.
 *
 * <p>Thread lifecycle:</p>
 * <ul>
 *   <li>Threads are created lazily on first {@link #forward} call per broker.</li>
 *   <li>{@link #cleanupStaleThreads()} removes threads for dead/relocated brokers.</li>
 *   <li>{@link #close()} shuts down all threads gracefully.</li>
 * </ul>
 *
 * @see ProduceForwardThread
 * @see FetchForwardManager
 */
public class ProduceForwardManager implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(ProduceForwardManager.class);

    private final ConcurrentHashMap<Integer, ProduceForwardThread> threads =
            new ConcurrentHashMap<>();

    private final MetadataCache metadataCache;
    private final KafkaConfig config;
    private final Time time;
    private final Metrics metrics;
    private final LogContext logContext;
    private final int queueCapacity;

    /**
     * @param metadataCache  for resolving broker nodes
     * @param config         broker configuration (inter-broker listener, security, timeouts)
     * @param time           time source
     * @param metrics        metrics registry for queue size gauges
     * @param logContext     logging context
     */
    public ProduceForwardManager(
            MetadataCache metadataCache,
            KafkaConfig config,
            Time time,
            Metrics metrics,
            LogContext logContext) {
        this.metadataCache = metadataCache;
        this.config = config;
        this.time = time;
        this.metrics = metrics;
        this.logContext = logContext;
        this.queueCapacity = config.httpInternalForwardingQueueSize();
    }

    /**
     * Forward produce entries to the leader broker for the given partitions.
     *
     * <p>Creates a {@link ProduceForwardThread} for the target broker if one
     * does not already exist. The thread is started immediately and sends the
     * entries as a binary ProduceRequest.</p>
     *
     * @param leaderId    the leader broker ID
     * @param entries     records to forward, keyed by TopicIdPartition
     * @param requiredAcks acks level (-1, 1, or 0)
     * @param timeoutMs   produce timeout
     * @return future that completes with per-partition responses
     * @throws BrokerNotFoundException if the broker is not alive in MetadataCache
     */
    public CompletableFuture<Map<TopicIdPartition, ProduceForwardThread.PartitionResponse>> forward(
            int leaderId,
            Map<TopicIdPartition, MemoryRecords> entries,
            short requiredAcks,
            int timeoutMs) {

        ProduceForwardThread thread = threads.computeIfAbsent(leaderId, id -> {
            Node node = resolveNode(id);
            NetworkClient client = buildNetworkClient(id);
            ProduceForwardThread t = new ProduceForwardThread(
                    node, client, config.requestTimeoutMs(), queueCapacity, time);
            t.start();
            log.info("Started ProduceForwardThread for broker {} at {}:{}", id, node.host(), node.port());
            return t;
        });

        return thread.enqueue(entries, requiredAcks, timeoutMs);
    }

    /**
     * Periodic cleanup: remove threads for brokers that are no longer alive
     * or whose address has changed. Called from a scheduled task every 60s.
     */
    public void cleanupStaleThreads() {
        threads.forEach((brokerId, thread) -> {
            Optional<Node> current = resolveNodeOptional(brokerId);
            if (current.isEmpty() || !current.get().equals(thread.destination())) {
                log.info("Removing stale ProduceForwardThread for broker {}", brokerId);
                threads.remove(brokerId);
                try {
                    thread.initiateShutdown();
                } catch (Exception e) {
                    log.warn("Error shutting down ProduceForwardThread for broker {}", brokerId, e);
                }
            }
        });
    }

    /**
     * Returns the number of active forward threads (for monitoring).
     */
    public int activeThreadCount() {
        return threads.size();
    }

    @Override
    public void close() {
        log.info("Shutting down ProduceForwardManager with {} active threads", threads.size());
        threads.values().forEach(thread -> {
            try {
                thread.initiateShutdown();
            } catch (Exception e) {
                log.warn("Error initiating shutdown for ProduceForwardThread", e);
            }
        });
        threads.values().forEach(thread -> {
            try {
                thread.awaitShutdown();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Interrupted while awaiting ProduceForwardThread shutdown", e);
            }
        });
        threads.clear();
    }

    // ------------------------------------------------------------------
    // Node resolution
    // ------------------------------------------------------------------

    private Node resolveNode(int brokerId) {
        return resolveNodeOptional(brokerId)
                .orElseThrow(() -> new BrokerNotFoundException(
                        "Broker " + brokerId + " not found in MetadataCache"));
    }

    private Optional<Node> resolveNodeOptional(int brokerId) {
        // MetadataCache.getAliveBrokerNode(brokerId, listenerName) returns Optional<Node>
        return metadataCache.getAliveBrokerNode(
                brokerId, config.interBrokerListenerName());
    }

    // ------------------------------------------------------------------
    // NetworkClient construction
    // (Same pattern as TransactionMarkerChannelManager.apply())
    // ------------------------------------------------------------------

    private NetworkClient buildNetworkClient(int targetBrokerId) {
        var channelBuilder = ChannelBuilders.clientChannelBuilder(
                config.interBrokerSecurityProtocol(),
                JaasContext.Type.SERVER,
                config,
                config.interBrokerListenerName(),
                config.saslMechanismInterBrokerProtocol(),
                time,
                logContext
        );

        var selector = new Selector(
                NetworkReceive.UNLIMITED,
                config.connectionsMaxIdleMs(),
                metrics,
                time,
                "http-produce-forward-" + targetBrokerId,
                Collections.emptyMap(),
                false,
                channelBuilder,
                logContext
        );

        return new NetworkClient(
                selector,
                new ManualMetadataUpdater(),
                String.format("broker-%d-http-produce-forward-%d",
                        config.brokerId(), targetBrokerId),
                1,     // maxInFlightRequestsPerConnection
                50,    // reconnectBackoffMs
                50,    // reconnectBackoffMaxMs
                Selectable.USE_DEFAULT_BUFFER_SIZE,
                config.socketReceiveBufferBytes(),
                config.requestTimeoutMs(),
                config.connectionSetupTimeoutMs(),
                config.connectionSetupTimeoutMaxMs(),
                time,
                true,  // discoverBrokerVersions
                new ApiVersions(),
                logContext
        );
    }

    // ------------------------------------------------------------------
    // Exception types
    // ------------------------------------------------------------------

    /**
     * Thrown when a broker ID cannot be resolved to a Node in MetadataCache.
     */
    public static class BrokerNotFoundException extends RuntimeException {
        public BrokerNotFoundException(String message) {
            super(message);
        }
    }
}
```

### FetchForwardManager

```java
// ============================================================================
// File: http-server/src/main/java/kafka/server/http/FetchForwardManager.java
// ============================================================================
package kafka.server.http;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ManualMetadataUpdater;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.ChannelBuilders;
import org.apache.kafka.common.network.NetworkReceive;
import org.apache.kafka.common.network.Selectable;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.security.JaasContext;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.metadata.MetadataCache;
import org.apache.kafka.server.config.KafkaConfig;
import org.apache.kafka.server.storage.log.FetchPartitionData;

import java.io.Closeable;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages per-broker {@link FetchForwardThread} instances for forwarding
 * HTTP consume (fetch) requests to remote partition leaders over the binary
 * protocol.
 *
 * <p>Mirrors {@link ProduceForwardManager} with the same lifecycle semantics:
 * lazy creation, stale cleanup, graceful shutdown.</p>
 *
 * @see FetchForwardThread
 * @see ProduceForwardManager
 */
public class FetchForwardManager implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(FetchForwardManager.class);

    private final ConcurrentHashMap<Integer, FetchForwardThread> threads =
            new ConcurrentHashMap<>();

    private final MetadataCache metadataCache;
    private final KafkaConfig config;
    private final Time time;
    private final Metrics metrics;
    private final LogContext logContext;
    private final int queueCapacity;

    /**
     * @param metadataCache  for resolving broker nodes
     * @param config         broker configuration
     * @param time           time source
     * @param metrics        metrics registry
     * @param logContext     logging context
     */
    public FetchForwardManager(
            MetadataCache metadataCache,
            KafkaConfig config,
            Time time,
            Metrics metrics,
            LogContext logContext) {
        this.metadataCache = metadataCache;
        this.config = config;
        this.time = time;
        this.metrics = metrics;
        this.logContext = logContext;
        this.queueCapacity = config.httpInternalForwardingQueueSize();
    }

    /**
     * Forward fetch request to the leader broker for the given partitions.
     *
     * @param leaderId   the leader broker ID
     * @param fetchSpecs per-partition fetch specifications
     * @param maxWaitMs  effective max wait time (already capped by section 6.1).
     *                   Passed through to the remote FetchRequest so the remote
     *                   broker's DelayedFetch purgatory uses the same budget.
     * @param minBytes   minimum bytes before responding
     * @param maxBytes   maximum total bytes
     * @return future that completes with per-partition fetch data
     */
    public CompletableFuture<Map<TopicIdPartition, FetchPartitionData>> forward(
            int leaderId,
            Map<TopicIdPartition, FetchRequest.PartitionData> fetchSpecs,
            int maxWaitMs,
            int minBytes,
            int maxBytes) {

        FetchForwardThread thread = threads.computeIfAbsent(leaderId, id -> {
            Node node = resolveNode(id);
            NetworkClient client = buildNetworkClient(id);
            FetchForwardThread t = new FetchForwardThread(
                    node, client, config.requestTimeoutMs(), queueCapacity, time);
            t.start();
            log.info("Started FetchForwardThread for broker {} at {}:{}", id, node.host(), node.port());
            return t;
        });

        return thread.enqueue(fetchSpecs, maxWaitMs, minBytes, maxBytes);
    }

    /**
     * Periodic cleanup: remove threads for brokers that are no longer alive
     * or whose address has changed.
     */
    public void cleanupStaleThreads() {
        threads.forEach((brokerId, thread) -> {
            Optional<Node> current = resolveNodeOptional(brokerId);
            if (current.isEmpty() || !current.get().equals(thread.destination())) {
                log.info("Removing stale FetchForwardThread for broker {}", brokerId);
                threads.remove(brokerId);
                try {
                    thread.initiateShutdown();
                } catch (Exception e) {
                    log.warn("Error shutting down FetchForwardThread for broker {}", brokerId, e);
                }
            }
        });
    }

    /**
     * Returns the number of active forward threads (for monitoring).
     */
    public int activeThreadCount() {
        return threads.size();
    }

    @Override
    public void close() {
        log.info("Shutting down FetchForwardManager with {} active threads", threads.size());
        threads.values().forEach(thread -> {
            try {
                thread.initiateShutdown();
            } catch (Exception e) {
                log.warn("Error initiating shutdown for FetchForwardThread", e);
            }
        });
        threads.values().forEach(thread -> {
            try {
                thread.awaitShutdown();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Interrupted while awaiting FetchForwardThread shutdown", e);
            }
        });
        threads.clear();
    }

    // ------------------------------------------------------------------
    // Node resolution (same as ProduceForwardManager)
    // ------------------------------------------------------------------

    private Node resolveNode(int brokerId) {
        return resolveNodeOptional(brokerId)
                .orElseThrow(() -> new ProduceForwardManager.BrokerNotFoundException(
                        "Broker " + brokerId + " not found in MetadataCache"));
    }

    private Optional<Node> resolveNodeOptional(int brokerId) {
        return metadataCache.getAliveBrokerNode(
                brokerId, config.interBrokerListenerName());
    }

    // ------------------------------------------------------------------
    // NetworkClient construction
    // ------------------------------------------------------------------

    private NetworkClient buildNetworkClient(int targetBrokerId) {
        var channelBuilder = ChannelBuilders.clientChannelBuilder(
                config.interBrokerSecurityProtocol(),
                JaasContext.Type.SERVER,
                config,
                config.interBrokerListenerName(),
                config.saslMechanismInterBrokerProtocol(),
                time,
                logContext
        );

        var selector = new Selector(
                NetworkReceive.UNLIMITED,
                config.connectionsMaxIdleMs(),
                metrics,
                time,
                "http-fetch-forward-" + targetBrokerId,
                Collections.emptyMap(),
                false,
                channelBuilder,
                logContext
        );

        return new NetworkClient(
                selector,
                new ManualMetadataUpdater(),
                String.format("broker-%d-http-fetch-forward-%d",
                        config.brokerId(), targetBrokerId),
                1,     // maxInFlightRequestsPerConnection
                50,    // reconnectBackoffMs
                50,    // reconnectBackoffMaxMs
                Selectable.USE_DEFAULT_BUFFER_SIZE,
                config.socketReceiveBufferBytes(),
                config.requestTimeoutMs(),
                config.connectionSetupTimeoutMs(),
                config.connectionSetupTimeoutMaxMs(),
                time,
                true,
                new ApiVersions(),
                logContext
        );
    }
}
```

## Tests

### Unit test skeletons

```java
// ============================================================================
// File: http-server/src/test/java/kafka/server/http/ProduceForwardManagerTest.java
// ============================================================================
package kafka.server.http;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.metadata.MetadataCache;
import org.apache.kafka.server.config.KafkaConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class ProduceForwardManagerTest {

    private MetadataCache metadataCache;
    private KafkaConfig config;
    private MockTime time;
    private Metrics metrics;
    private ProduceForwardManager manager;

    @BeforeEach
    void setUp() {
        metadataCache = mock(MetadataCache.class);
        config = mock(KafkaConfig.class);
        // Configure mock config values
        when(config.interBrokerListenerName()).thenReturn(/* ListenerName */);
        when(config.requestTimeoutMs()).thenReturn(30000);
        when(config.httpInternalForwardingQueueSize()).thenReturn(10000);
        when(config.brokerId()).thenReturn(0);

        time = new MockTime();
        metrics = new Metrics();
        // Note: actual test may need to mock NetworkClient creation
        // or use a test-specific subclass
    }

    @AfterEach
    void tearDown() {
        if (manager != null) manager.close();
        metrics.close();
    }

    @Test
    void testForwardCreatesThreadForNewBroker() {
        // Given: broker 1 is alive in MetadataCache
        // When: forward(1, ...) is called
        // Then: a ProduceForwardThread is created and started
        //       activeThreadCount() == 1
    }

    @Test
    void testForwardReusesExistingThread() {
        // Given: forward(1, ...) was already called
        // When: forward(1, ...) is called again
        // Then: the same thread is used (no new thread created)
        //       activeThreadCount() == 1
    }

    @Test
    void testForwardToDifferentBrokersCreatesSeparateThreads() {
        // Given: brokers 1 and 2 are alive
        // When: forward(1, ...) and forward(2, ...) are called
        // Then: two separate threads exist
        //       activeThreadCount() == 2
    }

    @Test
    void testForwardToUnknownBrokerThrows() {
        // Given: broker 99 is NOT in MetadataCache
        // When: forward(99, ...) is called
        // Then: BrokerNotFoundException is thrown
    }

    @Test
    void testCleanupRemovesThreadForDeadBroker() {
        // Given: thread exists for broker 1, then broker 1 removed from MetadataCache
        // When: cleanupStaleThreads() is called
        // Then: thread for broker 1 is removed and shut down
        //       activeThreadCount() == 0
    }

    @Test
    void testCleanupRemovesThreadForRelocatedBroker() {
        // Given: thread exists for broker 1 at host:9092
        //        MetadataCache now shows broker 1 at host:9093 (different port)
        // When: cleanupStaleThreads() is called
        // Then: old thread is removed (next forward() creates a new one)
    }

    @Test
    void testCloseShutdownsAllThreads() {
        // Given: threads exist for brokers 1, 2, 3
        // When: close() is called
        // Then: all threads are shut down gracefully
        //       activeThreadCount() == 0
    }
}
```

```java
// ============================================================================
// File: http-server/src/test/java/kafka/server/http/FetchForwardManagerTest.java
// ============================================================================
package kafka.server.http;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.metadata.MetadataCache;
import org.apache.kafka.server.config.KafkaConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class FetchForwardManagerTest {

    private MetadataCache metadataCache;
    private KafkaConfig config;
    private MockTime time;
    private Metrics metrics;
    private FetchForwardManager manager;

    @BeforeEach
    void setUp() {
        metadataCache = mock(MetadataCache.class);
        config = mock(KafkaConfig.class);
        when(config.requestTimeoutMs()).thenReturn(30000);
        when(config.httpInternalForwardingQueueSize()).thenReturn(10000);
        when(config.brokerId()).thenReturn(0);

        time = new MockTime();
        metrics = new Metrics();
    }

    @AfterEach
    void tearDown() {
        if (manager != null) manager.close();
        metrics.close();
    }

    @Test
    void testForwardCreatesThreadForNewBroker() {
        // Same pattern as ProduceForwardManagerTest
    }

    @Test
    void testForwardPassesMaxWaitMsThrough() {
        // Given: forward(1, specs, maxWaitMs=3500, ...) is called
        // Then: the FetchForwardThread.enqueue receives maxWaitMs=3500
    }

    @Test
    void testCleanupRemovesThreadForDeadBroker() {
        // Same pattern as ProduceForwardManagerTest
    }

    @Test
    void testCloseShutdownsAllThreads() {
        // Same pattern as ProduceForwardManagerTest
    }
}
```

## Rules

1. **Use `computeIfAbsent` for thread creation.** This guarantees at most one thread per broker ID even under concurrent access.
2. **Never create a `NetworkClient` on the handler thread.** The `computeIfAbsent` lambda runs on the first caller's thread, which is a `KafkaRequestHandler` thread. `NetworkClient` construction is lightweight (no blocking I/O), so this is acceptable. The thread's `start()` is also non-blocking.
3. **Always use the inter-broker listener.** Forward threads send binary Kafka protocol requests, not HTTP. `config.interBrokerListenerName()` resolves the correct endpoint.
4. **`close()` must be called during graceful shutdown.** This is wired in `HttpAcceptor.close()` (section 14.8).
5. **`cleanupStaleThreads()` runs on a scheduled executor, not on the handler thread.** The caller is `HttpAcceptor`'s cleanup scheduler.
6. **Thread names must include broker IDs.** Format: `ProduceForwardThread-{brokerId}`, `FetchForwardThread-{brokerId}`. This is critical for debugging thread dumps.
7. **Do NOT implement retry logic in the managers.** Retry is the handler's responsibility (TASK-D.03, TASK-D.04).

## Learning

1. **`KafkaConfig` is a Scala class** (`kafka.server.KafkaConfig`), not available as `org.apache.kafka.server.config.KafkaConfig` in Java. The Java base class is `AbstractKafkaConfig` in `server/src/main/java/org/apache/kafka/server/config/AbstractKafkaConfig.java`, which provides `interBrokerListenerName()`, `interBrokerSecurityProtocol()`, `brokerId()`, `requestTimeoutMs()`, `connectionsMaxIdleMs()`, `socketReceiveBufferBytes()`, `connectionSetupTimeoutMs()`, and `connectionSetupTimeoutMaxMs()`.
2. **`saslMechanismInterBrokerProtocol()`** is only on the Scala KafkaConfig. In Java, use `config.getString(BrokerSecurityConfigs.SASL_MECHANISM_INTER_BROKER_PROTOCOL_CONFIG)` instead.
3. **`httpInternalForwardingQueueSize()`** does not exist on any config class. The queue size constant is defined in `org.apache.kafka.network.HttpServerConfigs.HTTP_INTERNAL_FORWARDING_QUEUE_SIZE_DEFAULT` but there is no accessor method. Solution: pass `queueCapacity` as a separate constructor parameter.
4. **`NetworkClient` constructors** all require a `MetadataRecoveryStrategy` parameter (added after the TransactionMarkerChannelManager pattern was documented). Use `MetadataRecoveryStrategy.NONE`.
5. **Testing managers with NetworkClient**: Since `buildNetworkClient()` creates real network objects, extract thread creation into a package-private `createThread()` method that tests can override via `Mockito.spy()` + `doReturn()`.

## Limitations

1. **No integration test with real network**: Tests mock the `createThread()` method, so they validate lifecycle management (creation, reuse, cleanup, shutdown) but not actual network forwarding. Full integration is covered by TASK-C.03 and TASK-E.04.
2. **Checkstyle import-control conflict**: The `import-control-http-server.xml` had a merge conflict between two worktrees. Resolved by merging both sets of allowed imports and adding `org.apache.kafka.metadata`.
3. **Duplicate `project(':http-server')` blocks** in `build.gradle`: The first block (line ~1568) has more dependencies including `:core`, `:server`, netty, jackson. The second block (line ~2290) is more minimal. Gradle merges both additively. Added `:metadata` and `:storage:storage-api` to the first block for explicit dependency clarity.

## Field Notes

- The skeleton code in the task file references `ProduceForwardThread.PartitionResponse` but the actual D.01 implementation uses `ProduceResponse.PartitionResponse` directly. Adapted the manager's `forward()` return type to match.
- `InterBrokerSendThread.initiateShutdown()` returns `boolean` (not void), which matters when setting up mock expectations.
- `ShutdownableThread.awaitShutdown()` throws `InterruptedException`, requiring `close()` to handle it per thread.
- The `FetchForwardManager` reuses `ProduceForwardManager.BrokerNotFoundException` rather than defining its own, keeping the exception type shared.

## Acceptance Criteria

- [x] `ProduceForwardManager` compiles and implements `Closeable`.
- [x] `FetchForwardManager` compiles and implements `Closeable`.
- [x] `forward()` creates a thread on first call per broker ID.
- [x] `forward()` reuses existing threads for repeat calls to same broker.
- [x] `forward()` throws `BrokerNotFoundException` for unknown brokers.
- [x] `cleanupStaleThreads()` removes threads for dead brokers.
- [x] `cleanupStaleThreads()` removes threads for relocated brokers (address changed).
- [x] `close()` shuts down all threads and clears the map.
- [x] `NetworkClient` construction follows `TransactionMarkerChannelManager` pattern.
- [x] Thread names include broker IDs.
- [x] `activeThreadCount()` returns correct count.
- [x] All unit tests pass (16/16: 8 ProduceForwardManagerTest + 8 FetchForwardManagerTest).

## File Manifest

| File | Action | Description |
|------|--------|-------------|
| `http-server/src/main/java/kafka/server/http/ProduceForwardManager.java` | Created | Per-broker thread pool manager for produce forwarding |
| `http-server/src/main/java/kafka/server/http/FetchForwardManager.java` | Created | Per-broker thread pool manager for fetch forwarding |
| `http-server/src/test/java/kafka/server/http/ProduceForwardManagerTest.java` | Created | 8 unit tests for ProduceForwardManager |
| `http-server/src/test/java/kafka/server/http/FetchForwardManagerTest.java` | Created | 8 unit tests for FetchForwardManager |
| `checkstyle/import-control-http-server.xml` | Modified | Resolved merge conflict, added org.apache.kafka.metadata |
| `build.gradle` | Modified | Added :metadata and :storage:storage-api dependencies to http-server |
