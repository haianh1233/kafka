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

// Time: Created - TASK-WS2.05

package kafka.server.http.routing;

import kafka.server.http.ws.ExchangeMetadata;
import kafka.server.http.ws.WsConfigs;
import kafka.server.http.ws.WsRoutingMetadataManager;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link DefaultExchangeManager}.
 *
 * <p>These tests wire up a real {@link ExchangeManager} + {@link BindingManager}
 * + {@link RoutingEngine} stack so they exercise the same path a publish will
 * take in production: implicit binding → direct matcher → queue name.
 *
 * // Time: Created - TASK-WS2.05
 */
class DefaultExchangeManagerTest {

    private static final String VHOST = "/";

    private WsRoutingMetadataManager metadataManager;
    private ExchangeManager exchangeManager;
    private BindingManager bindingManager;
    private RoutingEngine engine;
    private Set<String> existingQueues;

    @BeforeEach
    void setUp() {
        // Metadata manager with a no-op recordWriter — we don't care about
        // persistence for routing-logic tests.
        metadataManager = new WsRoutingMetadataManager(
            WsConfigs.withDefaults(),
            (key, value) -> { /* no-op */ });
        exchangeManager = new ExchangeManager(metadataManager, WsConfigs.withDefaults());

        // Queue-existence is driven by a mutable set, since there's no QueueManager
        // yet. The test drives it to reflect declare/delete semantics.
        existingQueues = new HashSet<>();

        bindingManager = new BindingManager(
            name -> exchangeManager.getExchange(VHOST, name) != null,
            existingQueues::contains);

        engine = new RoutingEngine(
            // Exchange type lookup: delegate to ExchangeManager for the default vhost.
            name -> {
                ExchangeMetadata ex = exchangeManager.getExchange(VHOST, name);
                return ex == null ? null : ex.type();
            },
            bindingManager::listByExchange,
            bindingManager::listE2EBySource);
    }

    /** Declares a queue: registers it in the existing-queue set and adds the implicit binding. */
    private void declareQueue(String queue) {
        existingQueues.add(queue);
        DefaultExchangeManager.onQueueDeclared(bindingManager, queue);
    }

    /** Deletes a queue: removes the implicit binding and un-registers it. */
    private void deleteQueue(String queue) {
        DefaultExchangeManager.onQueueDeleted(bindingManager, queue);
        existingQueues.remove(queue);
    }

    // ------------------------------------------------------------------
    // initialize()
    // ------------------------------------------------------------------

    @Test
    void initialize_createsDefaultExchange() {
        DefaultExchangeManager.initialize(exchangeManager, VHOST);
        assertNotNull(exchangeManager.getExchange(VHOST, ""));
    }

    @Test
    void initialize_defaultExchangeIsDirectType() {
        DefaultExchangeManager.initialize(exchangeManager, VHOST);
        ExchangeMetadata ex = exchangeManager.getExchange(VHOST, "");
        assertNotNull(ex);
        assertEquals("direct", ex.type());
    }

    @Test
    void initialize_idempotent() {
        DefaultExchangeManager.initialize(exchangeManager, VHOST);
        DefaultExchangeManager.initialize(exchangeManager, VHOST); // no-op on second call
        assertNotNull(exchangeManager.getExchange(VHOST, ""));
    }

    @Test
    void initialize_nullEngine_throwsNPE() {
        assertThrows(NullPointerException.class,
            () -> DefaultExchangeManager.initialize(null, VHOST));
    }

    @Test
    void initialize_nullVhost_throwsNPE() {
        assertThrows(NullPointerException.class,
            () -> DefaultExchangeManager.initialize(exchangeManager, null));
    }

    // ------------------------------------------------------------------
    // onQueueDeclared()
    // ------------------------------------------------------------------

    @Test
    void onQueueDeclared_createsImplicitBinding() {
        DefaultExchangeManager.initialize(exchangeManager, VHOST);
        declareQueue("orders");

        Set<String> matched = engine.route("", "orders");
        assertEquals(Set.of("orders"), matched);
    }

    @Test
    void onQueueDeclared_multipleQueues() {
        DefaultExchangeManager.initialize(exchangeManager, VHOST);
        declareQueue("orders");
        declareQueue("events");

        assertEquals(Set.of("orders"), engine.route("", "orders"));
        assertEquals(Set.of("events"), engine.route("", "events"));
    }

    @Test
    void onQueueDeclared_idempotent() {
        DefaultExchangeManager.initialize(exchangeManager, VHOST);
        declareQueue("orders");
        // Second declare (e.g. client re-declares an existing queue) — should
        // not create a duplicate binding.
        DefaultExchangeManager.onQueueDeclared(bindingManager, "orders");

        Set<String> matched = engine.route("", "orders");
        assertEquals(Set.of("orders"), matched);
        assertEquals(1, bindingManager.bindingCount(""));
    }

    @Test
    void onQueueDeclared_nullManager_throwsNPE() {
        assertThrows(NullPointerException.class,
            () -> DefaultExchangeManager.onQueueDeclared(null, "orders"));
    }

    @Test
    void onQueueDeclared_nullQueue_throwsNPE() {
        assertThrows(NullPointerException.class,
            () -> DefaultExchangeManager.onQueueDeclared(bindingManager, null));
    }

    // ------------------------------------------------------------------
    // onQueueDeleted()
    // ------------------------------------------------------------------

    @Test
    void onQueueDeleted_removesImplicitBinding() {
        DefaultExchangeManager.initialize(exchangeManager, VHOST);
        declareQueue("orders");
        deleteQueue("orders");

        Set<String> matched = engine.route("", "orders");
        assertTrue(matched.isEmpty());
        assertEquals(0, bindingManager.bindingCount(""));
    }

    @Test
    void onQueueDeleted_missingBinding_isNoOp() {
        DefaultExchangeManager.initialize(exchangeManager, VHOST);
        // Never declared "orders" — onQueueDeleted should silently no-op.
        DefaultExchangeManager.onQueueDeleted(bindingManager, "orders");
        assertEquals(0, bindingManager.bindingCount(""));
    }

    @Test
    void onQueueDeleted_onlyRemovesImplicitBinding() {
        // Verify that onQueueDeleted only touches the implicit (default
        // exchange, key=queueName) binding, not bindings on other exchanges.
        DefaultExchangeManager.initialize(exchangeManager, VHOST);
        existingQueues.add("orders");

        // Bind to default exchange (implicit).
        DefaultExchangeManager.onQueueDeclared(bindingManager, "orders");
        // Bind to amq.direct with a different routing key (explicit).
        bindingManager.bind("amq.direct", "orders", "order.created");

        // Now "delete" the queue from the default-exchange perspective.
        DefaultExchangeManager.onQueueDeleted(bindingManager, "orders");

        // The implicit binding is gone.
        assertTrue(engine.route("", "orders").isEmpty());
        // The explicit binding on amq.direct is still there.
        assertEquals(Set.of("orders"), engine.route("amq.direct", "order.created"));
    }

    @Test
    void onQueueDeleted_nullManager_throwsNPE() {
        assertThrows(NullPointerException.class,
            () -> DefaultExchangeManager.onQueueDeleted(null, "orders"));
    }

    @Test
    void onQueueDeleted_nullQueue_throwsNPE() {
        assertThrows(NullPointerException.class,
            () -> DefaultExchangeManager.onQueueDeleted(bindingManager, null));
    }

    // ------------------------------------------------------------------
    // Routing semantics
    // ------------------------------------------------------------------

    @Test
    void routeToQueue_byName() {
        DefaultExchangeManager.initialize(exchangeManager, VHOST);
        declareQueue("payment-events");

        Set<String> matched = engine.route("", "payment-events");
        assertEquals(Set.of("payment-events"), matched);
    }

    @Test
    void routeToQueue_noMatchForUnknownQueue() {
        DefaultExchangeManager.initialize(exchangeManager, VHOST);
        declareQueue("orders");

        Set<String> matched = engine.route("", "nonexistent");
        assertTrue(matched.isEmpty());
    }

    @Test
    void defaultExchange_isDirectType_exactMatchOnly() {
        DefaultExchangeManager.initialize(exchangeManager, VHOST);
        declareQueue("orders");

        // Direct exchange: exact match only. "order" does NOT match "orders"
        // (no prefix/wildcard semantics).
        assertTrue(engine.route("", "order").isEmpty());
        assertTrue(engine.route("", "orders.created").isEmpty());
        assertEquals(Set.of("orders"), engine.route("", "orders"));
    }

    @Test
    void defaultExchange_protectedFromDeletion() {
        DefaultExchangeManager.initialize(exchangeManager, VHOST);

        // The default exchange must not be deletable by clients.
        ExchangeException ex = assertThrows(ExchangeException.class,
            () -> exchangeManager.deleteExchange(VHOST, "", /* ifUnused */ false));
        assertEquals(ExchangeException.ErrorCode.EXCHANGE_PROTECTED, ex.errorCode());
    }

    @Test
    void routeAcrossManyQueues_eachIndependent() {
        DefaultExchangeManager.initialize(exchangeManager, VHOST);
        List<String> queues = List.of("a", "b", "c", "d", "e");
        for (String q : queues) {
            declareQueue(q);
        }
        for (String q : queues) {
            assertEquals(Set.of(q), engine.route("", q),
                "each queue should receive only its own name-routed message");
        }
        // Cross-check: a routing key not matching any queue returns empty.
        assertTrue(engine.route("", "not-a-queue").isEmpty());
    }
}
