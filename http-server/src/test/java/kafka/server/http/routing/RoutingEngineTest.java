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

// Time: Update - TASK-WS2.04 - added e2e + alternate exchange + matcher wiring tests
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
 * // Time: Update - TASK-WS2.04
 */
class RoutingEngineTest {

    private Map<String, String> exchangeTypes;
    private Map<String, List<Binding>> bindings;
    private Map<String, List<E2EBinding>> e2eBindings;
    private Map<String, String> alternateExchanges;
    private RoutingEngine engine;

    @BeforeEach
    void setUp() {
        exchangeTypes = new HashMap<>();
        bindings = new HashMap<>();
        e2eBindings = new HashMap<>();
        alternateExchanges = new HashMap<>();

        engine = new RoutingEngine(
            exchangeTypes::get,
            ex -> bindings.getOrDefault(ex, List.of()),
            ex -> e2eBindings.getOrDefault(ex, List.of()),
            alternateExchanges::get
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

    // --- Topic / fanout / headers matcher wiring (WS2.04) ---

    @Test
    void route_topicExchange_wildcardMatch() {
        exchangeTypes.put("events", "topic");
        bindings.put("events", List.of(
            new Binding("events", "q1", "order.*", Map.of()),
            new Binding("events", "q2", "#", Map.of()),
            new Binding("events", "q3", "payment.#", Map.of())
        ));

        assertEquals(Set.of("q1", "q2"), engine.route("events", "order.created"));
        assertEquals(Set.of("q2", "q3"), engine.route("events", "payment.received"));
        assertEquals(Set.of("q2"), engine.route("events", "ticket.opened"));
    }

    @Test
    void route_fanoutExchange_allBoundQueues() {
        exchangeTypes.put("broadcast", "fanout");
        bindings.put("broadcast", List.of(
            new Binding("broadcast", "q1", "ignored-key", Map.of()),
            new Binding("broadcast", "q2", "also-ignored", Map.of()),
            new Binding("broadcast", "q3", "", Map.of())
        ));

        // Fanout ignores the routing key entirely.
        assertEquals(Set.of("q1", "q2", "q3"), engine.route("broadcast", "whatever"));
        assertEquals(Set.of("q1", "q2", "q3"), engine.route("broadcast", ""));
    }

    @Test
    void route_headersExchange_allMode() {
        exchangeTypes.put("hdrs", "headers");
        bindings.put("hdrs", List.of(
            new Binding("hdrs", "q1", "", Map.of(
                "x-match", "all", "priority", "high", "region", "us"))
        ));

        assertEquals(Set.of("q1"),
            engine.route("hdrs", "", Map.of("priority", "high", "region", "us")));
        // Missing one of the required headers → no match.
        assertTrue(engine.route("hdrs", "", Map.of("priority", "high")).isEmpty());
    }

    @Test
    void route_headersExchange_anyMode() {
        exchangeTypes.put("hdrs", "headers");
        bindings.put("hdrs", List.of(
            new Binding("hdrs", "q1", "", Map.of(
                "x-match", "any", "priority", "high", "region", "eu"))
        ));

        // One header matches → match.
        assertEquals(Set.of("q1"),
            engine.route("hdrs", "", Map.of("priority", "high", "region", "us")));
        // No header matches → no match.
        assertTrue(engine.route("hdrs", "", Map.of("priority", "low")).isEmpty());
    }

    // --- E2E routing (pre-existing direct-source cases) ---

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

    // --- E2E routing — new type coverage (WS2.04) ---

    @Test
    void route_e2e_directToFanout() {
        // Source is direct, destination is fanout: all queues on dest match.
        exchangeTypes.put("source", "direct");
        exchangeTypes.put("dest", "fanout");
        bindings.put("dest", List.of(
            new Binding("dest", "q1", "", Map.of()),
            new Binding("dest", "q2", "", Map.of())
        ));
        e2eBindings.put("source", List.of(
            new E2EBinding("source", "dest", "order.created", Map.of())
        ));

        assertEquals(Set.of("q1", "q2"), engine.route("source", "order.created"));
    }

    @Test
    void route_e2e_fanoutSourceAlwaysMatches() {
        // Fanout source: every e2e binding matches regardless of routing key.
        exchangeTypes.put("source", "fanout");
        exchangeTypes.put("dest", "fanout");
        bindings.put("dest", List.of(
            new Binding("dest", "q1", "", Map.of())
        ));
        e2eBindings.put("source", List.of(
            new E2EBinding("source", "dest", "does-not-matter", Map.of())
        ));

        assertEquals(Set.of("q1"), engine.route("source", "literally-anything"));
    }

    @Test
    void route_e2e_topicSourceUsesWildcardOnBindingKey() {
        // Source is a topic exchange; the e2e binding key is matched as a
        // topic pattern against the message routing key.
        exchangeTypes.put("source", "topic");
        exchangeTypes.put("dest", "direct");
        bindings.put("dest", List.of(
            new Binding("dest", "qd", "order.created", Map.of())
        ));
        e2eBindings.put("source", List.of(
            new E2EBinding("source", "dest", "order.*", Map.of())
        ));

        assertEquals(Set.of("qd"), engine.route("source", "order.created"));
        // Pattern 'order.*' does not match 'payment.made' → empty.
        assertTrue(engine.route("source", "payment.made").isEmpty());
    }

    @Test
    void route_e2e_headersSourceUsesBindingArguments() {
        // Source is a headers exchange; the e2e binding's arguments are
        // matched against message headers.
        exchangeTypes.put("source", "headers");
        exchangeTypes.put("dest", "fanout");
        bindings.put("dest", List.of(
            new Binding("dest", "qd", "", Map.of())
        ));
        e2eBindings.put("source", List.of(
            new E2EBinding("source", "dest", "",
                Map.of("x-match", "all", "priority", "high"))
        ));

        assertEquals(Set.of("qd"),
            engine.route("source", "", Map.of("priority", "high")));
        assertTrue(
            engine.route("source", "", Map.of("priority", "low")).isEmpty());
    }

    @Test
    void route_e2e_chainOfThree() {
        // A -> B -> C, queues only on C. All three are fanouts so e2e
        // bindings always match.
        exchangeTypes.put("A", "fanout");
        exchangeTypes.put("B", "fanout");
        exchangeTypes.put("C", "fanout");
        bindings.put("C", List.of(new Binding("C", "final", "", Map.of())));
        e2eBindings.put("A", List.of(new E2EBinding("A", "B", "", Map.of())));
        e2eBindings.put("B", List.of(new E2EBinding("B", "C", "", Map.of())));

        assertEquals(Set.of("final"), engine.route("A", "any.key"));
    }

    // --- Alternate exchange fallback (WS2.04) ---

    @Test
    void route_alternateExchange_usedWhenNoMatch() {
        exchangeTypes.put("primary", "direct");
        exchangeTypes.put("fallback", "fanout");
        bindings.put("primary", List.of(
            new Binding("primary", "q1", "order.created", Map.of())
        ));
        bindings.put("fallback", List.of(
            new Binding("fallback", "catch-all", "", Map.of())
        ));
        alternateExchanges.put("primary", "fallback");

        // No match on primary → falls through to fallback.
        assertEquals(Set.of("catch-all"), engine.route("primary", "unknown.key"));
    }

    @Test
    void route_alternateExchange_notUsedWhenPrimaryMatches() {
        exchangeTypes.put("primary", "direct");
        exchangeTypes.put("fallback", "fanout");
        bindings.put("primary", List.of(
            new Binding("primary", "q1", "order.created", Map.of())
        ));
        bindings.put("fallback", List.of(
            new Binding("fallback", "catch-all", "", Map.of())
        ));
        alternateExchanges.put("primary", "fallback");

        // Primary matches → alternate MUST NOT be invoked.
        assertEquals(Set.of("q1"), engine.route("primary", "order.created"));
    }

    @Test
    void route_alternateExchange_chainedFallback() {
        // primary -> alt1 -> alt2; only alt2 has matching bindings.
        exchangeTypes.put("primary", "direct");
        exchangeTypes.put("alt1", "direct");
        exchangeTypes.put("alt2", "fanout");
        bindings.put("alt2", List.of(new Binding("alt2", "final-catch", "", Map.of())));
        alternateExchanges.put("primary", "alt1");
        alternateExchanges.put("alt1", "alt2");

        assertEquals(Set.of("final-catch"), engine.route("primary", "no-match"));
    }

    @Test
    void route_alternateExchange_notUsedWhenE2EMatches() {
        // Primary has no direct bindings, but an e2e binding fires.
        // Because the e2e recursion produces a match, the alternate exchange
        // MUST NOT be consulted.
        exchangeTypes.put("primary", "direct");
        exchangeTypes.put("dest", "direct");
        exchangeTypes.put("fallback", "fanout");
        bindings.put("dest", List.of(
            new Binding("dest", "e2e-queue", "key", Map.of())
        ));
        bindings.put("fallback", List.of(
            new Binding("fallback", "fallback-queue", "", Map.of())
        ));
        e2eBindings.put("primary", List.of(
            new E2EBinding("primary", "dest", "key", Map.of())
        ));
        alternateExchanges.put("primary", "fallback");

        assertEquals(Set.of("e2e-queue"), engine.route("primary", "key"));
    }

    @Test
    void route_alternateExchange_cycleGuard() {
        // primary -> alt -> primary. The visited-set must prevent infinite
        // recursion, yielding an empty result (neither side has bindings).
        exchangeTypes.put("primary", "direct");
        exchangeTypes.put("alt", "direct");
        alternateExchanges.put("primary", "alt");
        alternateExchanges.put("alt", "primary");

        assertTrue(engine.route("primary", "anything").isEmpty());
    }

    @Test
    void route_alternateExchange_missingAltSilentlyIgnored() {
        // Alternate exchange points at an undeclared exchange: routing should
        // complete cleanly (the recursive call sees null type and returns).
        exchangeTypes.put("primary", "direct");
        alternateExchanges.put("primary", "ghost");

        assertTrue(engine.route("primary", "any.key").isEmpty());
    }

    @Test
    void route_alternateExchange_emptyStringTreatedAsNone() {
        // Defensive: alternate = "" should NOT trigger fallback (treat like absent).
        exchangeTypes.put("primary", "direct");
        alternateExchanges.put("primary", "");

        assertTrue(engine.route("primary", "any.key").isEmpty());
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

    @Test
    void constructor_nullAlternateExchangeFn_throws() {
        assertThrows(NullPointerException.class,
            () -> new RoutingEngine(
                exchangeTypes::get, bindings::get, e2eBindings::get, null));
    }

    @Test
    void constructor_legacyThreeArgConstructor_stillWorks() {
        // Verify the legacy 3-arg constructor (pre-WS2.04) still compiles and
        // behaves identically for simple direct routing (no alternate exchange).
        RoutingEngine legacy = new RoutingEngine(
            exchangeTypes::get,
            ex -> bindings.getOrDefault(ex, List.of()),
            ex -> e2eBindings.getOrDefault(ex, List.of())
        );
        exchangeTypes.put("events", "direct");
        bindings.put("events", List.of(new Binding("events", "q", "k", Map.of())));
        assertEquals(Set.of("q"), legacy.route("events", "k"));
    }
}
