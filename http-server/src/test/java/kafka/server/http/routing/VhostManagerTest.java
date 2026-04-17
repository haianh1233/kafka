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

// Time: Created - TASK-WS2.09

package kafka.server.http.routing;

import kafka.server.http.ws.ExchangeMetadata;
import kafka.server.http.ws.QueueMetadata;
import kafka.server.http.ws.WsConfigs;
import kafka.server.http.ws.WsRoutingMetadataManager;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link VhostManager}: vhost lifecycle, topic-name resolution
 * with vhost prefix, per-vhost RoutingEngine map, and namespace isolation.
 *
 * // Time: Created - TASK-WS2.09
 */
class VhostManagerTest {

    private WsRoutingMetadataManager metadataManager;
    private ExchangeManager exchangeManager;
    private BindingManager bindingManager;
    private VhostManager vhostManager;
    private List<Map.Entry<String, byte[]>> writtenRecords;

    @BeforeEach
    void setUp() {
        writtenRecords = Collections.synchronizedList(new ArrayList<>());
        metadataManager = new WsRoutingMetadataManager(
            WsConfigs.withDefaults(),
            (key, value) -> writtenRecords.add(new AbstractMap.SimpleEntry<>(key, value)));
        exchangeManager = new ExchangeManager(metadataManager, WsConfigs.withDefaults());
        bindingManager = new BindingManager(
            name -> true,   // any exchange exists — tests don't exercise binding validation
            name -> true);  // any queue exists
        vhostManager = new VhostManager(metadataManager, exchangeManager, bindingManager);
    }

    // ------------------------------------------------------------------
    // Default vhost
    // ------------------------------------------------------------------

    @Test
    void defaultVhost_existsOnStartup() {
        assertTrue(vhostManager.exists("/"),
            "Default vhost '/' must exist on construction");
    }

    @Test
    void defaultVhost_hasFiveDefaultExchanges() {
        // Pre-declared exchanges auto-created for the default vhost.
        assertNotNull(metadataManager.getExchange("/", ""));
        assertNotNull(metadataManager.getExchange("/", "amq.direct"));
        assertNotNull(metadataManager.getExchange("/", "amq.topic"));
        assertNotNull(metadataManager.getExchange("/", "amq.fanout"));
        assertNotNull(metadataManager.getExchange("/", "amq.headers"));
    }

    // ------------------------------------------------------------------
    // createVhost
    // ------------------------------------------------------------------

    @Test
    void createVhost_autoCreatesPreDeclaredExchanges() {
        vhostManager.createVhost("/production");

        assertTrue(vhostManager.exists("/production"));
        // 5 pre-declared exchanges must be present in the new vhost.
        assertNotNull(metadataManager.getExchange("/production", ""));
        assertNotNull(metadataManager.getExchange("/production", "amq.direct"));
        assertNotNull(metadataManager.getExchange("/production", "amq.topic"));
        assertNotNull(metadataManager.getExchange("/production", "amq.fanout"));
        assertNotNull(metadataManager.getExchange("/production", "amq.headers"));
    }

    @Test
    void createVhost_idempotent() {
        vhostManager.createVhost("/staging");
        vhostManager.createVhost("/staging");
        assertTrue(vhostManager.exists("/staging"));
        // Still exactly the 5 default exchanges, not 10.
        assertEquals(5, metadataManager.exchangeCount("/staging"));
    }

    // ------------------------------------------------------------------
    // Topic-name resolution
    // ------------------------------------------------------------------

    @Test
    void resolveTopicName_defaultVhost() {
        assertEquals("ws.orders", vhostManager.resolveTopicName("/", "orders"));
    }

    @Test
    void resolveTopicName_namedVhost() {
        assertEquals("ws.production.orders",
            vhostManager.resolveTopicName("/production", "orders"));
    }

    @Test
    void resolveTopicName_namedVhostNoLeadingSlash() {
        // Tolerant: both "production" and "/production" map the same way.
        assertEquals("ws.production.orders",
            vhostManager.resolveTopicName("production", "orders"));
    }

    // ------------------------------------------------------------------
    // deleteVhost
    // ------------------------------------------------------------------

    @Test
    void deleteDefaultVhost_rejected() {
        assertThrows(IllegalArgumentException.class,
            () -> vhostManager.deleteVhost("/"),
            "Default vhost '/' must not be deletable");
    }

    @Test
    void deleteVhost_cascadesResources() throws Exception {
        vhostManager.createVhost("/tmp");

        // Seed the vhost with one user exchange, one queue, and one binding.
        exchangeManager.declareExchange("/tmp", "orders-ex", "direct",
            /* durable */ true, /* autoDelete */ false, /* passive */ false,
            /* internal */ false, Map.of());
        metadataManager.writeQueue(new QueueMetadata(
            "orders-q", "/tmp", true, false, false, Map.of()));
        bindingManager.bind("orders-ex", "orders-q", "rk");

        // Sanity pre-conditions.
        assertNotNull(metadataManager.getExchange("/tmp", "orders-ex"));
        assertNotNull(metadataManager.getQueue("/tmp", "orders-q"));
        assertFalse(bindingManager.listByExchange("orders-ex").isEmpty());

        vhostManager.deleteVhost("/tmp");

        assertFalse(vhostManager.exists("/tmp"));
        // User exchange gone.
        assertNull(metadataManager.getExchange("/tmp", "orders-ex"));
        // Default exchanges in this vhost also gone.
        assertNull(metadataManager.getExchange("/tmp", ""));
        assertNull(metadataManager.getExchange("/tmp", "amq.direct"));
        // User queue gone.
        assertNull(metadataManager.getQueue("/tmp", "orders-q"));
        // Bindings gone.
        assertTrue(bindingManager.listByExchange("orders-ex").isEmpty());
    }

    @Test
    void deleteVhost_activeConnections_throws() {
        // Register one active connection in the /tmp vhost before creating the
        // vhost via VhostManager (order doesn't matter as the manager
        // delegates the connection check to a supplied predicate).
        vhostManager = new VhostManager(metadataManager, exchangeManager, bindingManager,
            /* activeConnectionFn */ v -> "/tmp".equals(v));
        vhostManager.createVhost("/tmp");

        VhostManager.VhostInUseException ex = assertThrows(
            VhostManager.VhostInUseException.class,
            () -> vhostManager.deleteVhost("/tmp"),
            "Deleting a vhost with active connections must throw");
        assertTrue(ex.getMessage().contains("/tmp"));
    }

    @Test
    void deleteVhost_unknownVhost_noop() {
        // Deleting an unknown vhost is a silent no-op (consistent with deleteExchange semantics).
        vhostManager.deleteVhost("/never-existed");
        // No exception; default vhost still exists.
        assertTrue(vhostManager.exists("/"));
    }

    // ------------------------------------------------------------------
    // listVhosts
    // ------------------------------------------------------------------

    @Test
    void listVhosts_returnsAllVhostsWithCounts() throws Exception {
        vhostManager.createVhost("/prod");
        vhostManager.createVhost("/stage");

        // Add one non-default exchange + one queue to /prod.
        exchangeManager.declareExchange("/prod", "my-ex", "direct",
            true, false, false, false, Map.of());
        metadataManager.writeQueue(new QueueMetadata(
            "my-q", "/prod", true, false, false, Map.of()));

        List<VhostManager.VhostInfo> vhosts = vhostManager.listVhosts();
        assertEquals(3, vhosts.size());

        VhostManager.VhostInfo prod = findByName(vhosts, "/prod");
        assertNotNull(prod);
        // 5 default + 1 user exchange = 6
        assertEquals(6, prod.exchangeCount());
        assertEquals(1, prod.queueCount());

        VhostManager.VhostInfo stage = findByName(vhosts, "/stage");
        assertNotNull(stage);
        assertEquals(5, stage.exchangeCount());
        assertEquals(0, stage.queueCount());
    }

    // ------------------------------------------------------------------
    // RoutingEngine isolation
    // ------------------------------------------------------------------

    @Test
    void getRoutingEngine_unknownVhost_throws() {
        assertThrows(IllegalArgumentException.class,
            () -> vhostManager.getRoutingEngine("/nonexistent"));
    }

    @Test
    void getRoutingEngine_defaultVhost_nonNull() {
        assertNotNull(vhostManager.getRoutingEngine("/"));
    }

    @Test
    void getRoutingEngine_perVhostInstance_isolated() {
        vhostManager.createVhost("/alpha");
        vhostManager.createVhost("/beta");
        RoutingEngine alpha = vhostManager.getRoutingEngine("/alpha");
        RoutingEngine beta = vhostManager.getRoutingEngine("/beta");

        assertNotNull(alpha);
        assertNotNull(beta);
        // Different RoutingEngine instances per vhost.
        assertFalse(alpha == beta, "Each vhost must have its own RoutingEngine");
    }

    // ------------------------------------------------------------------
    // Namespace isolation
    // ------------------------------------------------------------------

    @Test
    void vhostIsolation_separateNamespaces() throws Exception {
        vhostManager.createVhost("/v1");
        vhostManager.createVhost("/v2");

        // Same queue name in two different vhosts → two different topics.
        String t1 = vhostManager.resolveTopicName("/v1", "inbox");
        String t2 = vhostManager.resolveTopicName("/v2", "inbox");

        assertEquals("ws.v1.inbox", t1);
        assertEquals("ws.v2.inbox", t2);
        assertFalse(t1.equals(t2),
            "Same queue name in different vhosts must resolve to different topics");
    }

    @Test
    void vhostIsolation_exchangeInOneNotVisibleInOther() throws Exception {
        vhostManager.createVhost("/alpha");
        vhostManager.createVhost("/beta");

        exchangeManager.declareExchange("/alpha", "my-ex", "direct",
            true, false, false, false, Map.of());

        assertNotNull(exchangeManager.getExchange("/alpha", "my-ex"));
        assertNull(exchangeManager.getExchange("/beta", "my-ex"),
            "Exchange declared in /alpha must not be visible in /beta");
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    private static VhostManager.VhostInfo findByName(List<VhostManager.VhostInfo> list, String name) {
        for (VhostManager.VhostInfo v : list) {
            if (v.name().equals(name)) return v;
        }
        return null;
    }

    // Reference ExchangeMetadata to avoid an unused import when helpers are dropped.
    @SuppressWarnings("unused")
    private static Class<?> keepImport() {
        return ExchangeMetadata.class;
    }
}
