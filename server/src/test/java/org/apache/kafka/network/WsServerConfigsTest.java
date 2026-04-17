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
package org.apache.kafka.network;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * // Time: Created - TASK-WS1.01
 */
class WsServerConfigsTest {

    @Test
    void testConfigDefNotNull() {
        assertNotNull(WsServerConfigs.CONFIG_DEF);
    }

    @Test
    void testTotalConfigCount() {
        // Exactly 28 WebSocket/routing properties.
        assertEquals(28, WsServerConfigs.CONFIG_DEF.names().size());
    }

    @Test
    void testAllConfigKeysRegistered() {
        ConfigDef configDef = WsServerConfigs.CONFIG_DEF;
        assertTrue(configDef.names().contains(WsServerConfigs.WS_ENABLED_CONFIG));
        assertTrue(configDef.names().contains(WsServerConfigs.WS_MAX_FRAME_SIZE_CONFIG));
        assertTrue(configDef.names().contains(WsServerConfigs.WS_MAX_SUBSCRIPTIONS_PER_CONNECTION_CONFIG));
        assertTrue(configDef.names().contains(WsServerConfigs.WS_DEFAULT_CREDITS_CONFIG));
        assertTrue(configDef.names().contains(WsServerConfigs.WS_MAX_CREDITS_CONFIG));
        assertTrue(configDef.names().contains(WsServerConfigs.WS_TOPIC_PREFIX_CONFIG));
        assertTrue(configDef.names().contains(WsServerConfigs.WS_DEFAULT_QUEUE_PARTITIONS_CONFIG));
        assertTrue(configDef.names().contains(WsServerConfigs.WS_METADATA_TOPIC_CONFIG));
        assertTrue(configDef.names().contains(WsServerConfigs.WS_METADATA_REPLICATION_FACTOR_CONFIG));
        assertTrue(configDef.names().contains(WsServerConfigs.WS_ACK_COMMIT_INTERVAL_MS_CONFIG));
        assertTrue(configDef.names().contains(WsServerConfigs.WS_CONSUMER_START_OFFSET_CONFIG));
        assertTrue(configDef.names().contains(WsServerConfigs.NUM_WS_CONSUMER_THREADS_CONFIG));
        assertTrue(configDef.names().contains(WsServerConfigs.WS_CONSUMER_MAX_WAIT_MS_CONFIG));
        assertTrue(configDef.names().contains(WsServerConfigs.WS_CONSUMER_MAX_BYTES_CONFIG));
        assertTrue(configDef.names().contains(WsServerConfigs.WS_PUBLISH_TIMEOUT_MS_CONFIG));
        assertTrue(configDef.names().contains(WsServerConfigs.WS_CONNECTION_MAX_IDLE_MS_CONFIG));
        assertTrue(configDef.names().contains(WsServerConfigs.WS_SHUTDOWN_DRAIN_MS_CONFIG));
        assertTrue(configDef.names().contains(WsServerConfigs.WS_MAX_REDELIVERY_COUNT_CONFIG));
        assertTrue(configDef.names().contains(WsServerConfigs.WS_DEDUP_ENABLED_CONFIG));
        assertTrue(configDef.names().contains(WsServerConfigs.WS_DEDUP_CACHE_SIZE_CONFIG));
        assertTrue(configDef.names().contains(WsServerConfigs.WS_DEDUP_CACHE_TTL_MS_CONFIG));
        assertTrue(configDef.names().contains(WsServerConfigs.WS_MAX_CONNECTIONS_PER_BROKER_CONFIG));
        assertTrue(configDef.names().contains(WsServerConfigs.WS_MAX_EXCHANGES_PER_VHOST_CONFIG));
        assertTrue(configDef.names().contains(WsServerConfigs.WS_MAX_QUEUES_PER_VHOST_CONFIG));
        assertTrue(configDef.names().contains(WsServerConfigs.WS_MAX_BINDINGS_PER_EXCHANGE_CONFIG));
        assertTrue(configDef.names().contains(WsServerConfigs.WS_MAX_CONTROL_MESSAGES_PER_SECOND_CONFIG));
        assertTrue(configDef.names().contains(WsServerConfigs.WS_CONSUMER_ACK_TIMEOUT_MS_CONFIG));
        assertTrue(configDef.names().contains(WsServerConfigs.WS_METADATA_STARTUP_TIMEOUT_MS_CONFIG));
    }

    @Test
    void testDefaultValues() {
        Map<String, Object> parsed = WsServerConfigs.CONFIG_DEF.parse(new HashMap<>());
        assertEquals(true, parsed.get(WsServerConfigs.WS_ENABLED_CONFIG));
        assertEquals(1048576, parsed.get(WsServerConfigs.WS_MAX_FRAME_SIZE_CONFIG));
        assertEquals(256, parsed.get(WsServerConfigs.WS_MAX_SUBSCRIPTIONS_PER_CONNECTION_CONFIG));
        assertEquals(100, parsed.get(WsServerConfigs.WS_DEFAULT_CREDITS_CONFIG));
        assertEquals(10000, parsed.get(WsServerConfigs.WS_MAX_CREDITS_CONFIG));
        assertEquals("ws.", parsed.get(WsServerConfigs.WS_TOPIC_PREFIX_CONFIG));
        assertEquals(1, parsed.get(WsServerConfigs.WS_DEFAULT_QUEUE_PARTITIONS_CONFIG));
        assertEquals("__ws_routing_metadata", parsed.get(WsServerConfigs.WS_METADATA_TOPIC_CONFIG));
        assertEquals(3, parsed.get(WsServerConfigs.WS_METADATA_REPLICATION_FACTOR_CONFIG));
        assertEquals(1000, parsed.get(WsServerConfigs.WS_ACK_COMMIT_INTERVAL_MS_CONFIG));
        assertEquals("latest", parsed.get(WsServerConfigs.WS_CONSUMER_START_OFFSET_CONFIG));
        assertEquals(8, parsed.get(WsServerConfigs.NUM_WS_CONSUMER_THREADS_CONFIG));
        assertEquals(500, parsed.get(WsServerConfigs.WS_CONSUMER_MAX_WAIT_MS_CONFIG));
        assertEquals(1048576, parsed.get(WsServerConfigs.WS_CONSUMER_MAX_BYTES_CONFIG));
        assertEquals(30000, parsed.get(WsServerConfigs.WS_PUBLISH_TIMEOUT_MS_CONFIG));
        assertEquals(600000L, parsed.get(WsServerConfigs.WS_CONNECTION_MAX_IDLE_MS_CONFIG));
        assertEquals(5000, parsed.get(WsServerConfigs.WS_SHUTDOWN_DRAIN_MS_CONFIG));
        assertEquals(10, parsed.get(WsServerConfigs.WS_MAX_REDELIVERY_COUNT_CONFIG));
        assertEquals(false, parsed.get(WsServerConfigs.WS_DEDUP_ENABLED_CONFIG));
        assertEquals(10000, parsed.get(WsServerConfigs.WS_DEDUP_CACHE_SIZE_CONFIG));
        assertEquals(60000L, parsed.get(WsServerConfigs.WS_DEDUP_CACHE_TTL_MS_CONFIG));
        assertEquals(10000, parsed.get(WsServerConfigs.WS_MAX_CONNECTIONS_PER_BROKER_CONFIG));
        assertEquals(1000, parsed.get(WsServerConfigs.WS_MAX_EXCHANGES_PER_VHOST_CONFIG));
        assertEquals(10000, parsed.get(WsServerConfigs.WS_MAX_QUEUES_PER_VHOST_CONFIG));
        assertEquals(10000, parsed.get(WsServerConfigs.WS_MAX_BINDINGS_PER_EXCHANGE_CONFIG));
        assertEquals(50, parsed.get(WsServerConfigs.WS_MAX_CONTROL_MESSAGES_PER_SECOND_CONFIG));
        assertEquals(300000L, parsed.get(WsServerConfigs.WS_CONSUMER_ACK_TIMEOUT_MS_CONFIG));
        assertEquals(30000L, parsed.get(WsServerConfigs.WS_METADATA_STARTUP_TIMEOUT_MS_CONFIG));
    }

    @Test
    void testCustomValues() {
        Map<String, String> props = new HashMap<>();
        props.put(WsServerConfigs.WS_ENABLED_CONFIG, "false");
        props.put(WsServerConfigs.WS_MAX_FRAME_SIZE_CONFIG, "65536");
        props.put(WsServerConfigs.WS_TOPIC_PREFIX_CONFIG, "queues.");
        props.put(WsServerConfigs.WS_CONSUMER_START_OFFSET_CONFIG, "earliest");
        props.put(WsServerConfigs.WS_DEDUP_ENABLED_CONFIG, "true");
        props.put(WsServerConfigs.WS_CONNECTION_MAX_IDLE_MS_CONFIG, "120000");

        Map<String, Object> parsed = WsServerConfigs.CONFIG_DEF.parse(props);
        assertEquals(false, parsed.get(WsServerConfigs.WS_ENABLED_CONFIG));
        assertEquals(65536, parsed.get(WsServerConfigs.WS_MAX_FRAME_SIZE_CONFIG));
        assertEquals("queues.", parsed.get(WsServerConfigs.WS_TOPIC_PREFIX_CONFIG));
        assertEquals("earliest", parsed.get(WsServerConfigs.WS_CONSUMER_START_OFFSET_CONFIG));
        assertEquals(true, parsed.get(WsServerConfigs.WS_DEDUP_ENABLED_CONFIG));
        assertEquals(120000L, parsed.get(WsServerConfigs.WS_CONNECTION_MAX_IDLE_MS_CONFIG));
    }

    @Test
    void testConsumerStartOffsetRejectsInvalid() {
        Map<String, String> props = new HashMap<>();
        props.put(WsServerConfigs.WS_CONSUMER_START_OFFSET_CONFIG, "middle");
        assertThrows(ConfigException.class, () -> WsServerConfigs.CONFIG_DEF.parse(props));
    }

    @Test
    void testConsumerStartOffsetRejectsArbitraryString() {
        Map<String, String> props = new HashMap<>();
        props.put(WsServerConfigs.WS_CONSUMER_START_OFFSET_CONFIG, "foo");
        assertThrows(ConfigException.class, () -> WsServerConfigs.CONFIG_DEF.parse(props));
    }

    @Test
    void testConsumerStartOffsetAcceptsEarliest() {
        Map<String, String> props = new HashMap<>();
        props.put(WsServerConfigs.WS_CONSUMER_START_OFFSET_CONFIG, "earliest");
        Map<String, Object> parsed = WsServerConfigs.CONFIG_DEF.parse(props);
        assertEquals("earliest", parsed.get(WsServerConfigs.WS_CONSUMER_START_OFFSET_CONFIG));
    }

    @Test
    void testConsumerStartOffsetAcceptsLatest() {
        Map<String, String> props = new HashMap<>();
        props.put(WsServerConfigs.WS_CONSUMER_START_OFFSET_CONFIG, "latest");
        Map<String, Object> parsed = WsServerConfigs.CONFIG_DEF.parse(props);
        assertEquals("latest", parsed.get(WsServerConfigs.WS_CONSUMER_START_OFFSET_CONFIG));
    }

    @Test
    void testMaxFrameSizeRejectsZero() {
        Map<String, String> props = new HashMap<>();
        props.put(WsServerConfigs.WS_MAX_FRAME_SIZE_CONFIG, "0");
        assertThrows(ConfigException.class, () -> WsServerConfigs.CONFIG_DEF.parse(props));
    }

    @Test
    void testMaxFrameSizeRejectsNegative() {
        Map<String, String> props = new HashMap<>();
        props.put(WsServerConfigs.WS_MAX_FRAME_SIZE_CONFIG, "-1");
        assertThrows(ConfigException.class, () -> WsServerConfigs.CONFIG_DEF.parse(props));
    }

    @Test
    void testDefaultCreditsRejectsZero() {
        Map<String, String> props = new HashMap<>();
        props.put(WsServerConfigs.WS_DEFAULT_CREDITS_CONFIG, "0");
        assertThrows(ConfigException.class, () -> WsServerConfigs.CONFIG_DEF.parse(props));
    }

    @Test
    void testMaxCreditsRejectsZero() {
        Map<String, String> props = new HashMap<>();
        props.put(WsServerConfigs.WS_MAX_CREDITS_CONFIG, "0");
        assertThrows(ConfigException.class, () -> WsServerConfigs.CONFIG_DEF.parse(props));
    }

    @Test
    void testConsumerThreadsRejectsZero() {
        Map<String, String> props = new HashMap<>();
        props.put(WsServerConfigs.NUM_WS_CONSUMER_THREADS_CONFIG, "0");
        assertThrows(ConfigException.class, () -> WsServerConfigs.CONFIG_DEF.parse(props));
    }

    @Test
    void testConnectionMaxIdleAllowsZero() {
        Map<String, String> props = new HashMap<>();
        props.put(WsServerConfigs.WS_CONNECTION_MAX_IDLE_MS_CONFIG, "0");
        Map<String, Object> parsed = WsServerConfigs.CONFIG_DEF.parse(props);
        assertEquals(0L, parsed.get(WsServerConfigs.WS_CONNECTION_MAX_IDLE_MS_CONFIG));
    }

    @Test
    void testShutdownDrainAllowsZero() {
        Map<String, String> props = new HashMap<>();
        props.put(WsServerConfigs.WS_SHUTDOWN_DRAIN_MS_CONFIG, "0");
        Map<String, Object> parsed = WsServerConfigs.CONFIG_DEF.parse(props);
        assertEquals(0, parsed.get(WsServerConfigs.WS_SHUTDOWN_DRAIN_MS_CONFIG));
    }

    @Test
    void testMaxRedeliveryCountAllowsZero() {
        Map<String, String> props = new HashMap<>();
        props.put(WsServerConfigs.WS_MAX_REDELIVERY_COUNT_CONFIG, "0");
        Map<String, Object> parsed = WsServerConfigs.CONFIG_DEF.parse(props);
        assertEquals(0, parsed.get(WsServerConfigs.WS_MAX_REDELIVERY_COUNT_CONFIG));
    }

    @Test
    void testConsumerAckTimeoutAllowsZero() {
        Map<String, String> props = new HashMap<>();
        props.put(WsServerConfigs.WS_CONSUMER_ACK_TIMEOUT_MS_CONFIG, "0");
        Map<String, Object> parsed = WsServerConfigs.CONFIG_DEF.parse(props);
        assertEquals(0L, parsed.get(WsServerConfigs.WS_CONSUMER_ACK_TIMEOUT_MS_CONFIG));
    }

    @Test
    void testConsumerMaxWaitAllowsZero() {
        Map<String, String> props = new HashMap<>();
        props.put(WsServerConfigs.WS_CONSUMER_MAX_WAIT_MS_CONFIG, "0");
        Map<String, Object> parsed = WsServerConfigs.CONFIG_DEF.parse(props);
        assertEquals(0, parsed.get(WsServerConfigs.WS_CONSUMER_MAX_WAIT_MS_CONFIG));
    }

    @Test
    void testReplicationFactorRejectsZero() {
        Map<String, String> props = new HashMap<>();
        props.put(WsServerConfigs.WS_METADATA_REPLICATION_FACTOR_CONFIG, "0");
        assertThrows(ConfigException.class, () -> WsServerConfigs.CONFIG_DEF.parse(props));
    }

    @Test
    void testReplicationFactorRejectsNegative() {
        Map<String, String> props = new HashMap<>();
        props.put(WsServerConfigs.WS_METADATA_REPLICATION_FACTOR_CONFIG, "-1");
        assertThrows(ConfigException.class, () -> WsServerConfigs.CONFIG_DEF.parse(props));
    }

    @Test
    void testDedupCacheSizeRejectsZero() {
        Map<String, String> props = new HashMap<>();
        props.put(WsServerConfigs.WS_DEDUP_CACHE_SIZE_CONFIG, "0");
        assertThrows(ConfigException.class, () -> WsServerConfigs.CONFIG_DEF.parse(props));
    }

    @Test
    void testDedupCacheTtlRejectsZero() {
        Map<String, String> props = new HashMap<>();
        props.put(WsServerConfigs.WS_DEDUP_CACHE_TTL_MS_CONFIG, "0");
        assertThrows(ConfigException.class, () -> WsServerConfigs.CONFIG_DEF.parse(props));
    }

    @Test
    void testMaxConnectionsPerBrokerRejectsZero() {
        Map<String, String> props = new HashMap<>();
        props.put(WsServerConfigs.WS_MAX_CONNECTIONS_PER_BROKER_CONFIG, "0");
        assertThrows(ConfigException.class, () -> WsServerConfigs.CONFIG_DEF.parse(props));
    }

    @Test
    void testMetadataStartupTimeoutRejectsZero() {
        Map<String, String> props = new HashMap<>();
        props.put(WsServerConfigs.WS_METADATA_STARTUP_TIMEOUT_MS_CONFIG, "0");
        assertThrows(ConfigException.class, () -> WsServerConfigs.CONFIG_DEF.parse(props));
    }

    @Test
    void testDocsNonEmpty() {
        for (String name : WsServerConfigs.CONFIG_DEF.names()) {
            ConfigDef.ConfigKey key = WsServerConfigs.CONFIG_DEF.configKeys().get(name);
            assertNotNull(key, "ConfigKey must exist: " + name);
            assertNotNull(key.documentation, "Documentation must be set: " + name);
            assertFalse(key.documentation.trim().isEmpty(), "Documentation must be non-empty: " + name);
        }
    }
}
