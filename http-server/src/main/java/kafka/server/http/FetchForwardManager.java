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
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.security.JaasContext;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.metadata.MetadataCache;
import org.apache.kafka.server.config.AbstractKafkaConfig;
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
    private final AbstractKafkaConfig config;
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
     * @param queueCapacity  bounded queue capacity per forward thread
     */
    public FetchForwardManager(
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
     * @throws ProduceForwardManager.BrokerNotFoundException if the broker is not alive in MetadataCache
     */
    public CompletableFuture<Map<TopicIdPartition, FetchPartitionData>> forward(
            int leaderId,
            Map<TopicIdPartition, FetchRequest.PartitionData> fetchSpecs,
            int maxWaitMs,
            int minBytes,
            int maxBytes) {

        FetchForwardThread thread = threads.computeIfAbsent(leaderId, id -> createThread(id));

        return thread.enqueue(fetchSpecs, maxWaitMs, minBytes, maxBytes);
    }

    /**
     * Creates and starts a new {@link FetchForwardThread} for the given broker.
     * Package-private for testability.
     */
    FetchForwardThread createThread(int brokerId) {
        Node node = resolveNode(brokerId);
        NetworkClient client = buildNetworkClient(brokerId);
        FetchForwardThread t = new FetchForwardThread(
                node, client, config.requestTimeoutMs(), queueCapacity, time);
        t.start();
        log.info("Started FetchForwardThread for broker {} at {}:{}", brokerId, node.host(), node.port());
        return t;
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
                config.getString(BrokerSecurityConfigs.SASL_MECHANISM_INTER_BROKER_PROTOCOL_CONFIG),
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
                logContext,
                MetadataRecoveryStrategy.NONE
        );
    }
}
