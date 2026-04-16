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
// Time: Created - TASK-A.02
package org.apache.kafka.network;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HttpServerConfigsTest {

    @Test
    void testConfigDefNotNull() {
        assertNotNull(HttpServerConfigs.CONFIG_DEF);
    }

    @Test
    void testAllConfigKeysRegistered() {
        ConfigDef configDef = HttpServerConfigs.CONFIG_DEF;
        assertTrue(configDef.names().contains(HttpServerConfigs.HTTP_ENABLED_CONFIG));
        assertTrue(configDef.names().contains(HttpServerConfigs.NUM_HTTP_NETWORK_THREADS_CONFIG));
        assertTrue(configDef.names().contains(HttpServerConfigs.HTTP_REQUEST_MAX_BYTES_CONFIG));
        assertTrue(configDef.names().contains(HttpServerConfigs.HTTP_PRODUCE_MAX_RECORDS_CONFIG));
        assertTrue(configDef.names().contains(HttpServerConfigs.HTTP_RESPONSE_TIMEOUT_MS_CONFIG));
        assertTrue(configDef.names().contains(HttpServerConfigs.HTTP_CONSUME_MAX_WAIT_MS_CONFIG));
        assertTrue(configDef.names().contains(HttpServerConfigs.HTTP_CONSUME_MAX_BYTES_CONFIG));
        assertTrue(configDef.names().contains(HttpServerConfigs.HTTP_INTERNAL_FORWARDING_TIMEOUT_MS_CONFIG));
        assertTrue(configDef.names().contains(HttpServerConfigs.HTTP_INTERNAL_FORWARDING_RETRIES_CONFIG));
        assertTrue(configDef.names().contains(HttpServerConfigs.HTTP_INTERNAL_FORWARDING_QUEUE_SIZE_CONFIG));
        assertTrue(configDef.names().contains(HttpServerConfigs.HTTP_CORS_ALLOWED_ORIGINS_CONFIG));
        assertTrue(configDef.names().contains(HttpServerConfigs.HTTP_CONNECTION_IDLE_TIMEOUT_MS_CONFIG));
        assertTrue(configDef.names().contains(HttpServerConfigs.HTTP_SHUTDOWN_DRAIN_MS_CONFIG));
        assertTrue(configDef.names().contains(HttpServerConfigs.NUM_HTTP_ASYNC_THREADS_CONFIG));
    }

    @Test
    void testTotalConfigCount() {
        // Ensure we have exactly 14 config properties
        assertEquals(14, HttpServerConfigs.CONFIG_DEF.names().size());
    }

    @Test
    void testDefaultValues() {
        Map<String, Object> parsedDefaults = HttpServerConfigs.CONFIG_DEF.parse(new HashMap<>());

        assertEquals(false, parsedDefaults.get(HttpServerConfigs.HTTP_ENABLED_CONFIG));
        assertEquals(4, parsedDefaults.get(HttpServerConfigs.NUM_HTTP_NETWORK_THREADS_CONFIG));
        assertEquals(10 * 1024 * 1024, parsedDefaults.get(HttpServerConfigs.HTTP_REQUEST_MAX_BYTES_CONFIG));
        assertEquals(10000, parsedDefaults.get(HttpServerConfigs.HTTP_PRODUCE_MAX_RECORDS_CONFIG));
        assertEquals(30000, parsedDefaults.get(HttpServerConfigs.HTTP_RESPONSE_TIMEOUT_MS_CONFIG));
        assertEquals(5000, parsedDefaults.get(HttpServerConfigs.HTTP_CONSUME_MAX_WAIT_MS_CONFIG));
        assertEquals(1048576, parsedDefaults.get(HttpServerConfigs.HTTP_CONSUME_MAX_BYTES_CONFIG));
        assertEquals(10000, parsedDefaults.get(HttpServerConfigs.HTTP_INTERNAL_FORWARDING_TIMEOUT_MS_CONFIG));
        assertEquals(1, parsedDefaults.get(HttpServerConfigs.HTTP_INTERNAL_FORWARDING_RETRIES_CONFIG));
        assertEquals(10000, parsedDefaults.get(HttpServerConfigs.HTTP_INTERNAL_FORWARDING_QUEUE_SIZE_CONFIG));
        assertEquals("", parsedDefaults.get(HttpServerConfigs.HTTP_CORS_ALLOWED_ORIGINS_CONFIG));
        assertEquals(60000L, parsedDefaults.get(HttpServerConfigs.HTTP_CONNECTION_IDLE_TIMEOUT_MS_CONFIG));
        assertEquals(2000, parsedDefaults.get(HttpServerConfigs.HTTP_SHUTDOWN_DRAIN_MS_CONFIG));
        assertEquals(4, parsedDefaults.get(HttpServerConfigs.NUM_HTTP_ASYNC_THREADS_CONFIG));
    }

    @Test
    void testCustomValues() {
        Map<String, String> props = new HashMap<>();
        props.put(HttpServerConfigs.HTTP_ENABLED_CONFIG, "true");
        props.put(HttpServerConfigs.NUM_HTTP_NETWORK_THREADS_CONFIG, "8");
        props.put(HttpServerConfigs.HTTP_REQUEST_MAX_BYTES_CONFIG, "67108864"); // 64 MB
        props.put(HttpServerConfigs.HTTP_CONSUME_MAX_WAIT_MS_CONFIG, "10000");
        props.put(HttpServerConfigs.HTTP_CORS_ALLOWED_ORIGINS_CONFIG, "https://example.com");
        props.put(HttpServerConfigs.HTTP_CONNECTION_IDLE_TIMEOUT_MS_CONFIG, "120000");

        Map<String, Object> parsed = HttpServerConfigs.CONFIG_DEF.parse(props);

        assertEquals(true, parsed.get(HttpServerConfigs.HTTP_ENABLED_CONFIG));
        assertEquals(8, parsed.get(HttpServerConfigs.NUM_HTTP_NETWORK_THREADS_CONFIG));
        assertEquals(67108864, parsed.get(HttpServerConfigs.HTTP_REQUEST_MAX_BYTES_CONFIG));
        assertEquals(10000, parsed.get(HttpServerConfigs.HTTP_CONSUME_MAX_WAIT_MS_CONFIG));
        assertEquals("https://example.com", parsed.get(HttpServerConfigs.HTTP_CORS_ALLOWED_ORIGINS_CONFIG));
        assertEquals(120000L, parsed.get(HttpServerConfigs.HTTP_CONNECTION_IDLE_TIMEOUT_MS_CONFIG));
    }

    @Test
    void testNetworkThreadsRejectsZero() {
        Map<String, String> props = new HashMap<>();
        props.put(HttpServerConfigs.NUM_HTTP_NETWORK_THREADS_CONFIG, "0");
        assertThrows(ConfigException.class, () -> HttpServerConfigs.CONFIG_DEF.parse(props));
    }

    @Test
    void testNetworkThreadsRejectsNegative() {
        Map<String, String> props = new HashMap<>();
        props.put(HttpServerConfigs.NUM_HTTP_NETWORK_THREADS_CONFIG, "-1");
        assertThrows(ConfigException.class, () -> HttpServerConfigs.CONFIG_DEF.parse(props));
    }

    @Test
    void testRequestMaxBytesRejectsZero() {
        Map<String, String> props = new HashMap<>();
        props.put(HttpServerConfigs.HTTP_REQUEST_MAX_BYTES_CONFIG, "0");
        assertThrows(ConfigException.class, () -> HttpServerConfigs.CONFIG_DEF.parse(props));
    }

    @Test
    void testForwardingRetriesAllowsZero() {
        Map<String, String> props = new HashMap<>();
        props.put(HttpServerConfigs.HTTP_INTERNAL_FORWARDING_RETRIES_CONFIG, "0");
        Map<String, Object> parsed = HttpServerConfigs.CONFIG_DEF.parse(props);
        assertEquals(0, parsed.get(HttpServerConfigs.HTTP_INTERNAL_FORWARDING_RETRIES_CONFIG));
    }

    @Test
    void testConsumeMaxWaitAllowsZero() {
        Map<String, String> props = new HashMap<>();
        props.put(HttpServerConfigs.HTTP_CONSUME_MAX_WAIT_MS_CONFIG, "0");
        Map<String, Object> parsed = HttpServerConfigs.CONFIG_DEF.parse(props);
        assertEquals(0, parsed.get(HttpServerConfigs.HTTP_CONSUME_MAX_WAIT_MS_CONFIG));
    }

    @Test
    void testIdleTimeoutAllowsZero() {
        Map<String, String> props = new HashMap<>();
        props.put(HttpServerConfigs.HTTP_CONNECTION_IDLE_TIMEOUT_MS_CONFIG, "0");
        Map<String, Object> parsed = HttpServerConfigs.CONFIG_DEF.parse(props);
        assertEquals(0L, parsed.get(HttpServerConfigs.HTTP_CONNECTION_IDLE_TIMEOUT_MS_CONFIG));
    }

    @Test
    void testShutdownDrainAllowsZero() {
        Map<String, String> props = new HashMap<>();
        props.put(HttpServerConfigs.HTTP_SHUTDOWN_DRAIN_MS_CONFIG, "0");
        Map<String, Object> parsed = HttpServerConfigs.CONFIG_DEF.parse(props);
        assertEquals(0, parsed.get(HttpServerConfigs.HTTP_SHUTDOWN_DRAIN_MS_CONFIG));
    }

    @Test
    void testAsyncThreadsRejectsZero() {
        Map<String, String> props = new HashMap<>();
        props.put(HttpServerConfigs.NUM_HTTP_ASYNC_THREADS_CONFIG, "0");
        assertThrows(ConfigException.class, () -> HttpServerConfigs.CONFIG_DEF.parse(props));
    }
}
