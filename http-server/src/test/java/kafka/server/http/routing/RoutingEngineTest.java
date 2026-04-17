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

// Time: Created - TASK-WS1.08

package kafka.server.http.routing;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link RoutingEngine}.
 *
 * // Time: Created - TASK-WS1.08
 */
class RoutingEngineTest {

    private Map<String, String> exchangeTypes;
    private Map<String, List<Binding>> bindings;
    private Map<String, List<E2EBinding>> e2eBindings;
    private RoutingEngine engine;

    @BeforeEach
    void setUp() {
        exchangeTypes = new HashMap<>();
        bindings = new HashMap<>();
        e2eBindings = new HashMap<>();

        engine = new RoutingEngine(
            exchangeTypes::get,
            ex -> bindings.getOrDefault(ex, List.of()),
            ex -> e2eBindings.getOrDefault(ex, List.of())
        );
    }

    // --- Direct exchange routing ---

    @Test
    void route_directExchange_exactMatch() {
        exchangeTypes.put("events", "direct");
        bindings.put("events", List.of(
            new Binding("events", "order-events", "order.created", Map.of()),
            new Binding("events", "audit-log", "order.created", Map.of())
        ));

        Set<String> result = engine.route("events", "order.created");
        assertEquals(Set.of("order-events", "audit-log"), result);
    }

    @Test
    void route_directExchange_singleMatchAmongMany() {
        exchangeTypes.put("events", "direct");
        bindings.put("events", List.of(
            new Binding("events", "a", "order.created", Map.of()),
            new Binding("events", "b", "order.updated", Map.of()),
            new Binding("events", "c", "order.deleted", Map.of())
        ));

        Set<String> result = engine.route("events", "order.updated");
        assertEquals(Set.of("b"), result);
    }

    @Test
    void route_directExchange_noMatch_returnsEmpty() {
        exchangeTypes.put("events", "direct");
        bindings.put("events", List.of(
            new Binding("events", "order-events", "order.created", Map.of())
        ));

        Set<String> result = engine.route("events", "order.updated");
        assertTrue(result.isEmpty());
    }

    @Test
    void route_directExchange_noBindings_returnsEmpty() {
        exchangeTypes.put("events", "direct");
        Set<String> result = engine.route("events", "any.key");
        assertTrue(result.isEmpty());
    }

    @Test
    void route_directExchange_ignoresHeaders() {
        exchangeTypes.put("events", "direct");
        bindings.put("events", List.of(
            new Binding("events", "q", "k", Map.of())
        ));
        // Headers should be ignored for the direct type.
        Set<String> result = engine.route("events", "k", Map.of("ignored", "value"));
        assertEquals(Set.of("q"), result);
    }

    // --- Exchange not found ---

    @Test
    void route_exchangeNotFound_throws() {
        assertThrows(IllegalArgumentException.class,
            () -> engine.route("nonexistent", "key"));
    }

    // --- Unsupported exchange types (Phase 1 stubs) ---

    @Test
    void route_topicExchange_throwsUnsupported() {
        exchangeTypes.put("events", "topic");
        bindings.put("events", List.of());
        assertThrows(UnsupportedOperationException.class,
            () -> engine.route("events", "key"));
    }

    @Test
    void route_fanoutExchange_throwsUnsupported() {
        exchangeTypes.put("broadcast", "fanout");
        bindings.put("broadcast", List.of());
        assertThrows(UnsupportedOperationException.class,
            () -> engine.route("broadcast", "key"));
    }

    @Test
    void route_headersExchange_throwsUnsupported() {
        exchangeTypes.put("hdrs", "headers");
        bindings.put("hdrs", List.of());
        assertThrows(UnsupportedOperationException.class,
            () -> engine.route("hdrs", "key", Map.of("k", "v")));
    }

    // --- E2E routing ---

    @Test
    void route_directE2E_recursesIntoDestination() {
        exchangeTypes.put("source", "direct");
        exchangeTypes.put("dest", "direct");
        bindings.put("source", List.of());
        bindings.put("dest", List.of(
            new Binding("dest", "final-queue", "order.created", Map.of())
        ));
        e2eBindings.put("source", List.of(
            new E2EBinding("source", "dest", "order.created", Map.of())
        ));

        Set<String> result = engine.route("source", "order.created");
        assertEquals(Set.of("final-queue"), result);
    }

    @Test
    void route_directE2E_noMatchOnE2EKey_doesNotRecurse() {
        exchangeTypes.put("source", "direct");
        exchangeTypes.put("dest", "direct");
        bindings.put("source", List.of());
        bindings.put("dest", List.of(
            new Binding("dest", "final-queue", "order.created", Map.of())
        ));
        e2eBindings.put("source", List.of(
            new E2EBinding("source", "dest", "different.key", Map.of())
        ));

        Set<String> result = engine.route("source", "order.created");
        assertTrue(result.isEmpty());
    }

    @Test
    void route_e2eCycleDetection_doesNotLoop() {
        exchangeTypes.put("A", "direct");
        exchangeTypes.put("B", "direct");
        bindings.put("A", List.of(new Binding("A", "queue-a", "key", Map.of())));
        bindings.put("B", List.of(new Binding("B", "queue-b", "key", Map.of())));
        // A -> B -> A (cycle).
        e2eBindings.put("A", List.of(new E2EBinding("A", "B", "key", Map.of())));
        e2eBindings.put("B", List.of(new E2EBinding("B", "A", "key", Map.of())));

        Set<String> result = engine.route("A", "key");
        assertEquals(Set.of("queue-a", "queue-b"), result);
    }

    @Test
    void route_combinedDirectAndE2E_mergesResults() {
        exchangeTypes.put("events", "direct");
        exchangeTypes.put("audit", "direct");
        bindings.put("events", List.of(
            new Binding("events", "order-events", "order.created", Map.of())
        ));
        bindings.put("audit", List.of(
            new Binding("audit", "audit-log", "order.created", Map.of())
        ));
        e2eBindings.put("events", List.of(
            new E2EBinding("events", "audit", "order.created", Map.of())
        ));

        Set<String> result = engine.route("events", "order.created");
        assertEquals(Set.of("order-events", "audit-log"), result);
    }

    @Test
    void route_e2eDanglingDestination_silentlyIgnored() {
        // Dangling e2e destination (exchange doesn't exist) must not blow up
        // because the root call is valid; only unknown root exchanges throw.
        exchangeTypes.put("source", "direct");
        bindings.put("source", List.of(
            new Binding("source", "q", "k", Map.of())
        ));
        e2eBindings.put("source", List.of(
            new E2EBinding("source", "ghost", "k", Map.of())
        ));

        Set<String> result = engine.route("source", "k");
        assertEquals(Set.of("q"), result);
    }

    // --- Null handling ---

    @Test
    void route_nullExchange_throwsNPE() {
        assertThrows(NullPointerException.class, () -> engine.route(null, "key"));
    }

    @Test
    void route_nullRoutingKey_throwsNPE() {
        assertThrows(NullPointerException.class, () -> engine.route("events", null));
    }

    @Test
    void route_nullHeaders_allowedForDirect() {
        exchangeTypes.put("events", "direct");
        bindings.put("events", List.of(
            new Binding("events", "q", "k", Map.of())
        ));
        Set<String> result = engine.route("events", "k", null);
        assertEquals(Set.of("q"), result);
    }

    // --- Constructor validation ---

    @Test
    void constructor_nullExchangeTypeFn_throws() {
        assertThrows(NullPointerException.class,
            () -> new RoutingEngine(null, bindings::get, e2eBindings::get));
    }

    @Test
    void constructor_nullBindingsFn_throws() {
        assertThrows(NullPointerException.class,
            () -> new RoutingEngine(exchangeTypes::get, null, e2eBindings::get));
    }

    @Test
    void constructor_nullE2eBindingsFn_throws() {
        assertThrows(NullPointerException.class,
            () -> new RoutingEngine(exchangeTypes::get, bindings::get, null));
    }
}
