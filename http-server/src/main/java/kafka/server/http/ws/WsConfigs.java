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
// Time: Created - TASK-WS1.01
package kafka.server.http.ws;

import org.apache.kafka.network.WsServerConfigs;

import java.util.Objects;

/**
 * Runtime configuration holder for the WebSocket and routing layer.
 * <p>
 * Wraps resolved configuration values used by the WebSocket handlers, routing engine,
 * consumer fetch loops, and metadata manager. Defaults come from {@link WsServerConfigs}.
 * <p>
 * Mirrors the pattern used by {@link kafka.server.http.HttpServerConfigs} for the HTTP layer.
 *
 * // Time: Created - TASK-WS1.01
 */
public final class WsConfigs {

    private final boolean wsEnabled;
    private final int maxFrameSize;
    private final int maxSubscriptionsPerConnection;
    private final int defaultCredits;
    private final int maxCredits;
    private final String topicPrefix;
    private final int defaultQueuePartitions;
    private final String metadataTopic;
    private final int metadataReplicationFactor;
    private final int ackCommitIntervalMs;
    private final String consumerStartOffset;
    private final int numConsumerThreads;
    private final int consumerMaxWaitMs;
    private final int consumerMaxBytes;
    private final int publishTimeoutMs;
    private final long connectionMaxIdleMs;
    private final int shutdownDrainMs;
    private final int maxRedeliveryCount;
    private final boolean dedupEnabled;
    private final int dedupCacheSize;
    private final long dedupCacheTtlMs;
    private final int maxConnectionsPerBroker;
    private final int maxExchangesPerVhost;
    private final int maxQueuesPerVhost;
    private final int maxBindingsPerExchange;
    private final int maxControlMessagesPerSecond;
    private final long consumerAckTimeoutMs;
    private final long metadataStartupTimeoutMs;

    public WsConfigs(
            boolean wsEnabled,
            int maxFrameSize,
            int maxSubscriptionsPerConnection,
            int defaultCredits,
            int maxCredits,
            String topicPrefix,
            int defaultQueuePartitions,
            String metadataTopic,
            int metadataReplicationFactor,
            int ackCommitIntervalMs,
            String consumerStartOffset,
            int numConsumerThreads,
            int consumerMaxWaitMs,
            int consumerMaxBytes,
            int publishTimeoutMs,
            long connectionMaxIdleMs,
            int shutdownDrainMs,
            int maxRedeliveryCount,
            boolean dedupEnabled,
            int dedupCacheSize,
            long dedupCacheTtlMs,
            int maxConnectionsPerBroker,
            int maxExchangesPerVhost,
            int maxQueuesPerVhost,
            int maxBindingsPerExchange,
            int maxControlMessagesPerSecond,
            long consumerAckTimeoutMs,
            long metadataStartupTimeoutMs) {
        this.wsEnabled = wsEnabled;
        this.maxFrameSize = maxFrameSize;
        this.maxSubscriptionsPerConnection = maxSubscriptionsPerConnection;
        this.defaultCredits = defaultCredits;
        this.maxCredits = maxCredits;
        this.topicPrefix = Objects.requireNonNull(topicPrefix, "topicPrefix");
        this.defaultQueuePartitions = defaultQueuePartitions;
        this.metadataTopic = Objects.requireNonNull(metadataTopic, "metadataTopic");
        this.metadataReplicationFactor = metadataReplicationFactor;
        this.ackCommitIntervalMs = ackCommitIntervalMs;
        this.consumerStartOffset = Objects.requireNonNull(consumerStartOffset, "consumerStartOffset");
        this.numConsumerThreads = numConsumerThreads;
        this.consumerMaxWaitMs = consumerMaxWaitMs;
        this.consumerMaxBytes = consumerMaxBytes;
        this.publishTimeoutMs = publishTimeoutMs;
        this.connectionMaxIdleMs = connectionMaxIdleMs;
        this.shutdownDrainMs = shutdownDrainMs;
        this.maxRedeliveryCount = maxRedeliveryCount;
        this.dedupEnabled = dedupEnabled;
        this.dedupCacheSize = dedupCacheSize;
        this.dedupCacheTtlMs = dedupCacheTtlMs;
        this.maxConnectionsPerBroker = maxConnectionsPerBroker;
        this.maxExchangesPerVhost = maxExchangesPerVhost;
        this.maxQueuesPerVhost = maxQueuesPerVhost;
        this.maxBindingsPerExchange = maxBindingsPerExchange;
        this.maxControlMessagesPerSecond = maxControlMessagesPerSecond;
        this.consumerAckTimeoutMs = consumerAckTimeoutMs;
        this.metadataStartupTimeoutMs = metadataStartupTimeoutMs;
    }

    /**
     * Creates an instance with default values drawn from {@link WsServerConfigs}.
     */
    public static WsConfigs withDefaults() {
        return new WsConfigs(
            WsServerConfigs.WS_ENABLED_DEFAULT,
            WsServerConfigs.WS_MAX_FRAME_SIZE_DEFAULT,
            WsServerConfigs.WS_MAX_SUBSCRIPTIONS_PER_CONNECTION_DEFAULT,
            WsServerConfigs.WS_DEFAULT_CREDITS_DEFAULT,
            WsServerConfigs.WS_MAX_CREDITS_DEFAULT,
            WsServerConfigs.WS_TOPIC_PREFIX_DEFAULT,
            WsServerConfigs.WS_DEFAULT_QUEUE_PARTITIONS_DEFAULT,
            WsServerConfigs.WS_METADATA_TOPIC_DEFAULT,
            WsServerConfigs.WS_METADATA_REPLICATION_FACTOR_DEFAULT,
            WsServerConfigs.WS_ACK_COMMIT_INTERVAL_MS_DEFAULT,
            WsServerConfigs.WS_CONSUMER_START_OFFSET_DEFAULT,
            WsServerConfigs.NUM_WS_CONSUMER_THREADS_DEFAULT,
            WsServerConfigs.WS_CONSUMER_MAX_WAIT_MS_DEFAULT,
            WsServerConfigs.WS_CONSUMER_MAX_BYTES_DEFAULT,
            WsServerConfigs.WS_PUBLISH_TIMEOUT_MS_DEFAULT,
            WsServerConfigs.WS_CONNECTION_MAX_IDLE_MS_DEFAULT,
            WsServerConfigs.WS_SHUTDOWN_DRAIN_MS_DEFAULT,
            WsServerConfigs.WS_MAX_REDELIVERY_COUNT_DEFAULT,
            WsServerConfigs.WS_DEDUP_ENABLED_DEFAULT,
            WsServerConfigs.WS_DEDUP_CACHE_SIZE_DEFAULT,
            WsServerConfigs.WS_DEDUP_CACHE_TTL_MS_DEFAULT,
            WsServerConfigs.WS_MAX_CONNECTIONS_PER_BROKER_DEFAULT,
            WsServerConfigs.WS_MAX_EXCHANGES_PER_VHOST_DEFAULT,
            WsServerConfigs.WS_MAX_QUEUES_PER_VHOST_DEFAULT,
            WsServerConfigs.WS_MAX_BINDINGS_PER_EXCHANGE_DEFAULT,
            WsServerConfigs.WS_MAX_CONTROL_MESSAGES_PER_SECOND_DEFAULT,
            WsServerConfigs.WS_CONSUMER_ACK_TIMEOUT_MS_DEFAULT,
            WsServerConfigs.WS_METADATA_STARTUP_TIMEOUT_MS_DEFAULT
        );
    }

    // --- Typed accessors ---

    public boolean wsEnabled() {
        return wsEnabled;
    }

    public int maxFrameSize() {
        return maxFrameSize;
    }

    public int maxSubscriptionsPerConnection() {
        return maxSubscriptionsPerConnection;
    }

    public int defaultCredits() {
        return defaultCredits;
    }

    public int maxCredits() {
        return maxCredits;
    }

    public String topicPrefix() {
        return topicPrefix;
    }

    public int defaultQueuePartitions() {
        return defaultQueuePartitions;
    }

    public String metadataTopic() {
        return metadataTopic;
    }

    public int metadataReplicationFactor() {
        return metadataReplicationFactor;
    }

    public int ackCommitIntervalMs() {
        return ackCommitIntervalMs;
    }

    public String consumerStartOffset() {
        return consumerStartOffset;
    }

    public int numConsumerThreads() {
        return numConsumerThreads;
    }

    public int consumerMaxWaitMs() {
        return consumerMaxWaitMs;
    }

    public int consumerMaxBytes() {
        return consumerMaxBytes;
    }

    public int publishTimeoutMs() {
        return publishTimeoutMs;
    }

    public long connectionMaxIdleMs() {
        return connectionMaxIdleMs;
    }

    public int shutdownDrainMs() {
        return shutdownDrainMs;
    }

    public int maxRedeliveryCount() {
        return maxRedeliveryCount;
    }

    public boolean dedupEnabled() {
        return dedupEnabled;
    }

    public int dedupCacheSize() {
        return dedupCacheSize;
    }

    public long dedupCacheTtlMs() {
        return dedupCacheTtlMs;
    }

    public int maxConnectionsPerBroker() {
        return maxConnectionsPerBroker;
    }

    public int maxExchangesPerVhost() {
        return maxExchangesPerVhost;
    }

    public int maxQueuesPerVhost() {
        return maxQueuesPerVhost;
    }

    public int maxBindingsPerExchange() {
        return maxBindingsPerExchange;
    }

    public int maxControlMessagesPerSecond() {
        return maxControlMessagesPerSecond;
    }

    public long consumerAckTimeoutMs() {
        return consumerAckTimeoutMs;
    }

    public long metadataStartupTimeoutMs() {
        return metadataStartupTimeoutMs;
    }
}
