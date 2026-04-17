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
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * // Time: Created - TASK-WS1.01
 */
class WsConfigsTest {

    @Test
    void withDefaults_hasExpectedValues() {
        WsConfigs cfg = WsConfigs.withDefaults();
        assertEquals(WsServerConfigs.WS_ENABLED_DEFAULT, cfg.wsEnabled());
        assertEquals(WsServerConfigs.WS_MAX_FRAME_SIZE_DEFAULT, cfg.maxFrameSize());
        assertEquals(WsServerConfigs.WS_MAX_SUBSCRIPTIONS_PER_CONNECTION_DEFAULT, cfg.maxSubscriptionsPerConnection());
        assertEquals(WsServerConfigs.WS_DEFAULT_CREDITS_DEFAULT, cfg.defaultCredits());
        assertEquals(WsServerConfigs.WS_MAX_CREDITS_DEFAULT, cfg.maxCredits());
        assertEquals(WsServerConfigs.WS_TOPIC_PREFIX_DEFAULT, cfg.topicPrefix());
        assertEquals(WsServerConfigs.WS_DEFAULT_QUEUE_PARTITIONS_DEFAULT, cfg.defaultQueuePartitions());
        assertEquals(WsServerConfigs.WS_METADATA_TOPIC_DEFAULT, cfg.metadataTopic());
        assertEquals(WsServerConfigs.WS_METADATA_REPLICATION_FACTOR_DEFAULT, cfg.metadataReplicationFactor());
        assertEquals(WsServerConfigs.WS_ACK_COMMIT_INTERVAL_MS_DEFAULT, cfg.ackCommitIntervalMs());
        assertEquals(WsServerConfigs.WS_CONSUMER_START_OFFSET_DEFAULT, cfg.consumerStartOffset());
        assertEquals(WsServerConfigs.NUM_WS_CONSUMER_THREADS_DEFAULT, cfg.numConsumerThreads());
        assertEquals(WsServerConfigs.WS_CONSUMER_MAX_WAIT_MS_DEFAULT, cfg.consumerMaxWaitMs());
        assertEquals(WsServerConfigs.WS_CONSUMER_MAX_BYTES_DEFAULT, cfg.consumerMaxBytes());
        assertEquals(WsServerConfigs.WS_PUBLISH_TIMEOUT_MS_DEFAULT, cfg.publishTimeoutMs());
        assertEquals(WsServerConfigs.WS_CONNECTION_MAX_IDLE_MS_DEFAULT, cfg.connectionMaxIdleMs());
        assertEquals(WsServerConfigs.WS_SHUTDOWN_DRAIN_MS_DEFAULT, cfg.shutdownDrainMs());
        assertEquals(WsServerConfigs.WS_MAX_REDELIVERY_COUNT_DEFAULT, cfg.maxRedeliveryCount());
        assertEquals(WsServerConfigs.WS_DEDUP_ENABLED_DEFAULT, cfg.dedupEnabled());
        assertEquals(WsServerConfigs.WS_DEDUP_CACHE_SIZE_DEFAULT, cfg.dedupCacheSize());
        assertEquals(WsServerConfigs.WS_DEDUP_CACHE_TTL_MS_DEFAULT, cfg.dedupCacheTtlMs());
        assertEquals(WsServerConfigs.WS_MAX_CONNECTIONS_PER_BROKER_DEFAULT, cfg.maxConnectionsPerBroker());
        assertEquals(WsServerConfigs.WS_MAX_EXCHANGES_PER_VHOST_DEFAULT, cfg.maxExchangesPerVhost());
        assertEquals(WsServerConfigs.WS_MAX_QUEUES_PER_VHOST_DEFAULT, cfg.maxQueuesPerVhost());
        assertEquals(WsServerConfigs.WS_MAX_BINDINGS_PER_EXCHANGE_DEFAULT, cfg.maxBindingsPerExchange());
        assertEquals(WsServerConfigs.WS_MAX_CONTROL_MESSAGES_PER_SECOND_DEFAULT, cfg.maxControlMessagesPerSecond());
        assertEquals(WsServerConfigs.WS_CONSUMER_ACK_TIMEOUT_MS_DEFAULT, cfg.consumerAckTimeoutMs());
        assertEquals(WsServerConfigs.WS_METADATA_STARTUP_TIMEOUT_MS_DEFAULT, cfg.metadataStartupTimeoutMs());
    }

    @Test
    void constructor_storesAllFields() {
        WsConfigs cfg = new WsConfigs(
            false,      // wsEnabled
            2048,       // maxFrameSize
            42,         // maxSubscriptionsPerConnection
            7,          // defaultCredits
            99,         // maxCredits
            "q.",      // topicPrefix
            4,          // defaultQueuePartitions
            "meta",    // metadataTopic
            2,          // metadataReplicationFactor
            500,        // ackCommitIntervalMs
            "earliest", // consumerStartOffset
            3,          // numConsumerThreads
            250,        // consumerMaxWaitMs
            4096,       // consumerMaxBytes
            15000,      // publishTimeoutMs
            120000L,    // connectionMaxIdleMs
            2500,       // shutdownDrainMs
            5,          // maxRedeliveryCount
            true,       // dedupEnabled
            500,        // dedupCacheSize
            30000L,     // dedupCacheTtlMs
            200,        // maxConnectionsPerBroker
            50,         // maxExchangesPerVhost
            150,        // maxQueuesPerVhost
            250,        // maxBindingsPerExchange
            10,         // maxControlMessagesPerSecond
            60000L,     // consumerAckTimeoutMs
            10000L      // metadataStartupTimeoutMs
        );
        assertEquals(false, cfg.wsEnabled());
        assertEquals(2048, cfg.maxFrameSize());
        assertEquals(42, cfg.maxSubscriptionsPerConnection());
        assertEquals(7, cfg.defaultCredits());
        assertEquals(99, cfg.maxCredits());
        assertEquals("q.", cfg.topicPrefix());
        assertEquals(4, cfg.defaultQueuePartitions());
        assertEquals("meta", cfg.metadataTopic());
        assertEquals(2, cfg.metadataReplicationFactor());
        assertEquals(500, cfg.ackCommitIntervalMs());
        assertEquals("earliest", cfg.consumerStartOffset());
        assertEquals(3, cfg.numConsumerThreads());
        assertEquals(250, cfg.consumerMaxWaitMs());
        assertEquals(4096, cfg.consumerMaxBytes());
        assertEquals(15000, cfg.publishTimeoutMs());
        assertEquals(120000L, cfg.connectionMaxIdleMs());
        assertEquals(2500, cfg.shutdownDrainMs());
        assertEquals(5, cfg.maxRedeliveryCount());
        assertEquals(true, cfg.dedupEnabled());
        assertEquals(500, cfg.dedupCacheSize());
        assertEquals(30000L, cfg.dedupCacheTtlMs());
        assertEquals(200, cfg.maxConnectionsPerBroker());
        assertEquals(50, cfg.maxExchangesPerVhost());
        assertEquals(150, cfg.maxQueuesPerVhost());
        assertEquals(250, cfg.maxBindingsPerExchange());
        assertEquals(10, cfg.maxControlMessagesPerSecond());
        assertEquals(60000L, cfg.consumerAckTimeoutMs());
        assertEquals(10000L, cfg.metadataStartupTimeoutMs());
    }

    @Test
    void nullTopicPrefix_throwsNPE() {
        assertThrows(NullPointerException.class, () -> new WsConfigs(
            true, 1048576, 256, 100, 10000,
            null, 1, "__ws_routing_metadata", 3,
            1000, "latest", 8, 500, 1048576,
            30000, 600000L, 5000, 10, false,
            10000, 60000L, 10000, 1000, 10000,
            10000, 50, 300000L, 30000L));
    }

    @Test
    void nullMetadataTopic_throwsNPE() {
        assertThrows(NullPointerException.class, () -> new WsConfigs(
            true, 1048576, 256, 100, 10000,
            "ws.", 1, null, 3,
            1000, "latest", 8, 500, 1048576,
            30000, 600000L, 5000, 10, false,
            10000, 60000L, 10000, 1000, 10000,
            10000, 50, 300000L, 30000L));
    }

    @Test
    void nullConsumerStartOffset_throwsNPE() {
        assertThrows(NullPointerException.class, () -> new WsConfigs(
            true, 1048576, 256, 100, 10000,
            "ws.", 1, "__ws_routing_metadata", 3,
            1000, null, 8, 500, 1048576,
            30000, 600000L, 5000, 10, false,
            10000, 60000L, 10000, 1000, 10000,
            10000, 50, 300000L, 30000L));
    }

    @Test
    void parseFromPropertiesRoundtrip_matchesDefaults() {
        // Parse defaults through CONFIG_DEF, feed into WsConfigs constructor, verify all getters.
        java.util.Map<String, Object> parsed = WsServerConfigs.CONFIG_DEF.parse(new java.util.HashMap<>());
        WsConfigs cfg = new WsConfigs(
            (Boolean) parsed.get(WsServerConfigs.WS_ENABLED_CONFIG),
            (Integer) parsed.get(WsServerConfigs.WS_MAX_FRAME_SIZE_CONFIG),
            (Integer) parsed.get(WsServerConfigs.WS_MAX_SUBSCRIPTIONS_PER_CONNECTION_CONFIG),
            (Integer) parsed.get(WsServerConfigs.WS_DEFAULT_CREDITS_CONFIG),
            (Integer) parsed.get(WsServerConfigs.WS_MAX_CREDITS_CONFIG),
            (String) parsed.get(WsServerConfigs.WS_TOPIC_PREFIX_CONFIG),
            (Integer) parsed.get(WsServerConfigs.WS_DEFAULT_QUEUE_PARTITIONS_CONFIG),
            (String) parsed.get(WsServerConfigs.WS_METADATA_TOPIC_CONFIG),
            (Integer) parsed.get(WsServerConfigs.WS_METADATA_REPLICATION_FACTOR_CONFIG),
            (Integer) parsed.get(WsServerConfigs.WS_ACK_COMMIT_INTERVAL_MS_CONFIG),
            (String) parsed.get(WsServerConfigs.WS_CONSUMER_START_OFFSET_CONFIG),
            (Integer) parsed.get(WsServerConfigs.NUM_WS_CONSUMER_THREADS_CONFIG),
            (Integer) parsed.get(WsServerConfigs.WS_CONSUMER_MAX_WAIT_MS_CONFIG),
            (Integer) parsed.get(WsServerConfigs.WS_CONSUMER_MAX_BYTES_CONFIG),
            (Integer) parsed.get(WsServerConfigs.WS_PUBLISH_TIMEOUT_MS_CONFIG),
            (Long) parsed.get(WsServerConfigs.WS_CONNECTION_MAX_IDLE_MS_CONFIG),
            (Integer) parsed.get(WsServerConfigs.WS_SHUTDOWN_DRAIN_MS_CONFIG),
            (Integer) parsed.get(WsServerConfigs.WS_MAX_REDELIVERY_COUNT_CONFIG),
            (Boolean) parsed.get(WsServerConfigs.WS_DEDUP_ENABLED_CONFIG),
            (Integer) parsed.get(WsServerConfigs.WS_DEDUP_CACHE_SIZE_CONFIG),
            (Long) parsed.get(WsServerConfigs.WS_DEDUP_CACHE_TTL_MS_CONFIG),
            (Integer) parsed.get(WsServerConfigs.WS_MAX_CONNECTIONS_PER_BROKER_CONFIG),
            (Integer) parsed.get(WsServerConfigs.WS_MAX_EXCHANGES_PER_VHOST_CONFIG),
            (Integer) parsed.get(WsServerConfigs.WS_MAX_QUEUES_PER_VHOST_CONFIG),
            (Integer) parsed.get(WsServerConfigs.WS_MAX_BINDINGS_PER_EXCHANGE_CONFIG),
            (Integer) parsed.get(WsServerConfigs.WS_MAX_CONTROL_MESSAGES_PER_SECOND_CONFIG),
            (Long) parsed.get(WsServerConfigs.WS_CONSUMER_ACK_TIMEOUT_MS_CONFIG),
            (Long) parsed.get(WsServerConfigs.WS_METADATA_STARTUP_TIMEOUT_MS_CONFIG)
        );

        WsConfigs defaults = WsConfigs.withDefaults();
        assertEquals(defaults.wsEnabled(), cfg.wsEnabled());
        assertEquals(defaults.maxFrameSize(), cfg.maxFrameSize());
        assertEquals(defaults.topicPrefix(), cfg.topicPrefix());
        assertEquals(defaults.metadataTopic(), cfg.metadataTopic());
        assertEquals(defaults.consumerStartOffset(), cfg.consumerStartOffset());
        assertEquals(defaults.connectionMaxIdleMs(), cfg.connectionMaxIdleMs());
        assertEquals(defaults.consumerAckTimeoutMs(), cfg.consumerAckTimeoutMs());
        assertEquals(defaults.metadataStartupTimeoutMs(), cfg.metadataStartupTimeoutMs());
        assertEquals(defaults.dedupCacheTtlMs(), cfg.dedupCacheTtlMs());
    }
}
