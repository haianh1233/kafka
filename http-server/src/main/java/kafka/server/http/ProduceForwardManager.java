/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// Time: Created - TASK-D.02
package kafka.server.http;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ManualMetadataUpdater;
import org.apache.kafka.clients.MetadataRecoveryStrategy;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.ChannelBuilders;
import org.apache.kafka.common.network.NetworkReceive;
import org.apache.kafka.common.network.Selectable;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.security.JaasContext;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.metadata.MetadataCache;
import org.apache.kafka.server.config.AbstractKafkaConfig;
import org.apache.kafka.server.network.ProduceForwarder;

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
public class ProduceForwardManager implements ProduceForwarder, Closeable {

    private static final Logger log = LoggerFactory.getLogger(ProduceForwardManager.class);

    private final ConcurrentHashMap<Integer, ProduceForwardThread> threads =
            new ConcurrentHashMap<>();

    private final MetadataCache metadataCache;
    private final AbstractKafkaConfig config;
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
     * @param queueCapacity  bounded queue capacity per forward thread
     */
    public ProduceForwardManager(
            MetadataCache metadataCache,
            AbstractKafkaConfig config,
            Time time,
            Metrics metrics,
            LogContext logContext,
            int queueCapacity) {
        this.metadataCache = metadataCache;
        this.config = config;
        this.time = time;
        this.metrics = metrics;
        this.logContext = logContext;
        this.queueCapacity = queueCapacity;
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
    public CompletableFuture<Map<TopicIdPartition, ProduceResponse.PartitionResponse>> forward(
            int leaderId,
            Map<TopicIdPartition, MemoryRecords> entries,
            short requiredAcks,
            int timeoutMs) {

        ProduceForwardThread thread = threads.computeIfAbsent(leaderId, id -> createThread(id));

        return thread.enqueue(entries, requiredAcks, timeoutMs);
    }

    /**
     * Creates and starts a new {@link ProduceForwardThread} for the given broker.
     * Package-private for testability.
     */
    ProduceForwardThread createThread(int brokerId) {
        Node node = resolveNode(brokerId);
        NetworkClient client = buildNetworkClient(brokerId);
        ProduceForwardThread t = new ProduceForwardThread(
                node, client, config.requestTimeoutMs(), queueCapacity, time);
        t.start();
        log.info("Started ProduceForwardThread for broker {} at {}:{}", brokerId, node.host(), node.port());
        return t;
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
                config.getString(BrokerSecurityConfigs.SASL_MECHANISM_INTER_BROKER_PROTOCOL_CONFIG),
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
                logContext,
                MetadataRecoveryStrategy.NONE
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
