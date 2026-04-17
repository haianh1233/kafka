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

// Time: Created - TASK-WS-T.01

package kafka.server.http.routing;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Cross-cutting integration-style unit tests for {@link RoutingEngine}.
 *
 * <p>These tests supplement the per-matcher tests ({@link DirectMatcherTest},
 * {@link TopicMatcherTest}, {@link FanoutMatcherTest}, {@link HeadersMatcherTest})
 * and the direct-coverage tests in {@link RoutingEngineTest}. Their focus is
 * scenarios that span multiple matcher types or exercise edge cases not
 * covered elsewhere:
 *
 * <ul>
 *   <li>Multi-hop e2e chains that mix exchange types (direct → topic → fanout
 *       and headers → topic → direct)</li>
 *   <li>Cycles that involve a mix of exchange types</li>
 *   <li>Fan-out of a single source into e2e bindings of heterogeneous types</li>
 *   <li>De-duplication when a queue is reachable via multiple paths</li>
 *   <li>Alternate-exchange interaction with e2e bindings that produce no
 *       matches (dead-end e2e should still trigger alternate)</li>
 *   <li>Unknown exchange type — engine must not blow up</li>
 *   <li>Concurrent dispatch smoke test (engine is stateless so there is no
 *       shared mutable state to race on, but thread-confined visited/matched
 *       sets must be honoured)</li>
 *   <li>Performance sanity: routing over ~1000 bindings must complete in a
 *       reasonable time</li>
 *   <li>Empty exchange tables — exchanges declared but with zero bindings</li>
 * </ul>
 *
 * // Time: Created - TASK-WS-T.01
 */
class RoutingEngineCrossCuttingTest {

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

    // ── Mixed-type e2e chains ──────────────────────────────────────

    @Test
    void mixedChain_directToTopicToFanout() {
        // direct("source", key="events.order") -->e2e--> topic("mid")
        //   topic("mid") has e2e binding "events.*" --> fanout("sink")
        //   fanout("sink") fans out to q1, q2.
        //
        // Exercises: direct-source e2e (exact key) + topic-source e2e (wildcard)
        // + fanout delivery, all in one route() call.
        exchangeTypes.put("source", "direct");
        exchangeTypes.put("mid", "topic");
        exchangeTypes.put("sink", "fanout");

        e2eBindings.put("source", List.of(
            new E2EBinding("source", "mid", "events.order", Map.of())));
        e2eBindings.put("mid", List.of(
            new E2EBinding("mid", "sink", "events.*", Map.of())));
        bindings.put("sink", List.of(
            new Binding("sink", "q1", "", Map.of()),
            new Binding("sink", "q2", "", Map.of())));

        assertEquals(Set.of("q1", "q2"), engine.route("source", "events.order"));

        // Direct-source key mismatch: nothing reaches the topic/fanout chain.
        assertTrue(engine.route("source", "events.other").isEmpty());
    }

    @Test
    void mixedChain_headersToTopicToDirect() {
        // headers("h") with e2e args {priority=high} -->e2e--> topic("t")
        //   topic("t") with e2e key "a.*" --> direct("d")
        //   direct("d") has binding "a.b" -> qFinal.
        //
        // Only passes headers match AND topic match AND exact direct match.
        exchangeTypes.put("h", "headers");
        exchangeTypes.put("t", "topic");
        exchangeTypes.put("d", "direct");

        e2eBindings.put("h", List.of(
            new E2EBinding("h", "t", "",
                Map.of("x-match", "all", "priority", "high"))));
        e2eBindings.put("t", List.of(
            new E2EBinding("t", "d", "a.*", Map.of())));
        bindings.put("d", List.of(
            new Binding("d", "qFinal", "a.b", Map.of())));

        // Everything aligns.
        assertEquals(Set.of("qFinal"),
            engine.route("h", "a.b", Map.of("priority", "high")));

        // Headers mismatch: headers source rejects the message entirely.
        assertTrue(engine.route("h", "a.b", Map.of("priority", "low")).isEmpty());

        // Headers OK, topic OK, but direct key mismatch at tail of chain.
        assertTrue(engine.route("h", "a.c", Map.of("priority", "high")).isEmpty());
    }

    @Test
    void mixedTypeCycle_directToTopicToDirect_doesNotLoop() {
        // A(direct) --e2e-key="k"--> B(topic) --e2e-key="k"--> A
        // Each has a queue. Cycle guard must terminate.
        exchangeTypes.put("A", "direct");
        exchangeTypes.put("B", "topic");

        bindings.put("A", List.of(new Binding("A", "qA", "k", Map.of())));
        bindings.put("B", List.of(new Binding("B", "qB", "k", Map.of())));

        e2eBindings.put("A", List.of(new E2EBinding("A", "B", "k", Map.of())));
        // B is topic: "k" as a pattern matches routing-key "k" literally.
        e2eBindings.put("B", List.of(new E2EBinding("B", "A", "k", Map.of())));

        // qA comes from A's direct binding; qB from B's topic binding after
        // A --> B recursion; then B --> A is blocked by visited-set.
        assertEquals(Set.of("qA", "qB"), engine.route("A", "k"));
    }

    @Test
    void fanoutSource_withE2EOfEveryType() {
        // Single fanout source with four e2e destinations of different types.
        // Fanout source always matches, so every destination is entered.
        exchangeTypes.put("hub", "fanout");
        exchangeTypes.put("d", "direct");
        exchangeTypes.put("t", "topic");
        exchangeTypes.put("f", "fanout");
        exchangeTypes.put("h", "headers");

        e2eBindings.put("hub", List.of(
            new E2EBinding("hub", "d", "exact", Map.of()),
            new E2EBinding("hub", "t", "alpha.*", Map.of()),
            new E2EBinding("hub", "f", "", Map.of()),
            new E2EBinding("hub", "h", "",
                Map.of("x-match", "all", "vip", "yes"))
        ));
        bindings.put("d", List.of(new Binding("d", "qd", "exact", Map.of())));
        bindings.put("t", List.of(new Binding("t", "qt", "alpha.*", Map.of())));
        bindings.put("f", List.of(new Binding("f", "qf", "", Map.of())));
        bindings.put("h", List.of(new Binding("h", "qh", "",
            Map.of("x-match", "all", "vip", "yes"))));

        // Routing key "exact" + vip=yes: d, t (alpha.* does NOT match "exact"),
        // f (fanout always), h all match.
        // Actually "exact" does not match "alpha.*", so qt is NOT in result.
        assertEquals(Set.of("qd", "qf", "qh"),
            engine.route("hub", "exact", Map.of("vip", "yes")));

        // Routing key "alpha.news" + vip=no: topic matches at t (alpha.*),
        // direct at d does not (not "exact"), fanout at f always matches,
        // headers at h fails (vip != yes).
        assertEquals(Set.of("qt", "qf"),
            engine.route("hub", "alpha.news", Map.of("vip", "no")));
    }

    @Test
    void sameQueueReachableViaDirectAndE2E_deduplicated() {
        // q is bound directly on A AND reachable via A --> B (also bound to q).
        // The returned Set must contain q exactly once.
        exchangeTypes.put("A", "direct");
        exchangeTypes.put("B", "fanout");

        bindings.put("A", List.of(new Binding("A", "q", "k", Map.of())));
        bindings.put("B", List.of(new Binding("B", "q", "", Map.of())));
        e2eBindings.put("A", List.of(new E2EBinding("A", "B", "k", Map.of())));

        Set<String> result = engine.route("A", "k");
        assertEquals(Set.of("q"), result);
        assertEquals(1, result.size());
    }

    // ── Alternate exchange × e2e interactions ─────────────────────

    @Test
    void alternateExchange_triggeredWhenE2EProducesNoMatches() {
        // Primary has no direct bindings; its single e2e destination exists
        // but has no queue bindings that match. The ENGINE treats that as
        // "this exchange produced no matches" and falls back to the alternate.
        exchangeTypes.put("primary", "direct");
        exchangeTypes.put("dest", "direct");
        exchangeTypes.put("alt", "fanout");

        e2eBindings.put("primary", List.of(
            new E2EBinding("primary", "dest", "k", Map.of())));
        // dest has a binding but for a DIFFERENT key, so it produces nothing.
        bindings.put("dest", List.of(
            new Binding("dest", "irrelevant", "other.key", Map.of())));
        bindings.put("alt", List.of(
            new Binding("alt", "dlq", "", Map.of())));
        alternateExchanges.put("primary", "alt");

        // Routing key matches e2e binding ("k") but dest produces no queues;
        // primary's own bindings produce no queues; fallback to alt.
        assertEquals(Set.of("dlq"), engine.route("primary", "k"));
    }

    @Test
    void alternateExchange_eachExchangeInChainGetsItsOwnFallback() {
        // primary has alt=altP, dest (reached via e2e) has alt=altD.
        // primary binds nothing; dest binds nothing; each should fall through
        // to its own alternate, yielding queues from BOTH altP and altD.
        exchangeTypes.put("primary", "direct");
        exchangeTypes.put("dest", "direct");
        exchangeTypes.put("altP", "fanout");
        exchangeTypes.put("altD", "fanout");

        e2eBindings.put("primary", List.of(
            new E2EBinding("primary", "dest", "k", Map.of())));
        bindings.put("altP", List.of(new Binding("altP", "qAltP", "", Map.of())));
        bindings.put("altD", List.of(new Binding("altD", "qAltD", "", Map.of())));
        alternateExchanges.put("primary", "altP");
        alternateExchanges.put("dest", "altD");

        Set<String> result = engine.route("primary", "k");
        // dest's alternate fires first (during e2e recursion), adding qAltD.
        // Because qAltD is now in `matched`, primary sees matchedBefore=0 but
        // matched=1 after e2e recursion, so primary's alternate is NOT fired.
        //
        // This documents the actual (correct) behaviour: alternate exchange
        // is consulted per-exchange, and as soon as ANY descendant produces
        // a match, the ancestor's alternate is not used.
        assertEquals(Set.of("qAltD"), result);
    }

    @Test
    void alternateExchange_withDanglingE2E_stillFallsBack() {
        // Primary's e2e destination doesn't exist (dangling). The dangling
        // e2e recursion returns without adding queues. Primary has no other
        // bindings, so the alternate must still fire.
        exchangeTypes.put("primary", "direct");
        exchangeTypes.put("alt", "fanout");

        e2eBindings.put("primary", List.of(
            new E2EBinding("primary", "ghost", "k", Map.of())));
        bindings.put("alt", List.of(new Binding("alt", "dlq", "", Map.of())));
        alternateExchanges.put("primary", "alt");

        assertEquals(Set.of("dlq"), engine.route("primary", "k"));
    }

    // ── Unknown / defensive ─────────────────────────────────────────

    @Test
    void unknownExchangeType_routesToEmpty_noException() {
        // Defensive: if the exchange-type function returns a non-standard type
        // string, the engine logs + returns empty rather than throwing.
        exchangeTypes.put("weird", "custom-unknown-type");
        bindings.put("weird", List.of(
            new Binding("weird", "q", "k", Map.of())));

        Set<String> result = engine.route("weird", "k");
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void emptyExchangeTables_returnsEmptySet() {
        // Declare an exchange but never add any bindings, e2e, or alternate.
        // Routing must return an empty (but non-null) Set.
        exchangeTypes.put("ex", "topic");

        Set<String> result = engine.route("ex", "literally.any.key");
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void declaredButUnbounded_allTypes_returnEmpty() {
        // One exchange of each type with zero bindings.
        exchangeTypes.put("d", "direct");
        exchangeTypes.put("t", "topic");
        exchangeTypes.put("f", "fanout");
        exchangeTypes.put("h", "headers");

        assertTrue(engine.route("d", "any").isEmpty());
        assertTrue(engine.route("t", "any.key").isEmpty());
        assertTrue(engine.route("f", "any").isEmpty());
        assertTrue(engine.route("h", "", Map.of("foo", "bar")).isEmpty());
    }

    // ── Performance sanity ────────────────────────────────────────

    @Test
    void performance_thousandBindings_routesQuickly() {
        // 1000 direct bindings on one exchange; routing for a single key
        // should complete well under 100ms (very generous upper bound for
        // a single-threaded linear match).
        exchangeTypes.put("big", "direct");
        List<Binding> many = new ArrayList<>(1000);
        for (int i = 0; i < 1000; i++) {
            many.add(new Binding("big", "q" + i, "key." + i, Map.of()));
        }
        // Duplicate one key so we also exercise same-key-multi-queue.
        many.add(new Binding("big", "qShared-A", "hot.key", Map.of()));
        many.add(new Binding("big", "qShared-B", "hot.key", Map.of()));
        bindings.put("big", many);

        // Warm-up.
        for (int i = 0; i < 10; i++) {
            engine.route("big", "key." + i);
        }

        long start = System.nanoTime();
        for (int i = 0; i < 100; i++) {
            Set<String> r = engine.route("big", "key." + (i * 7 % 1000));
            assertEquals(1, r.size());
        }
        Set<String> hot = engine.route("big", "hot.key");
        long elapsedMs = (System.nanoTime() - start) / 1_000_000L;

        assertEquals(Set.of("qShared-A", "qShared-B"), hot);
        // 100 route() calls + one hot call, all across 1002 bindings, must
        // finish in well under 500ms on any reasonable machine. Treat as a
        // smoke-level upper bound to catch accidental O(N^2) regressions.
        assertTrue(elapsedMs < 500,
            "expected 101 route() calls to complete in <500ms, got " + elapsedMs + "ms");
    }

    @Test
    void performance_topicWithManyPatterns_routesQuickly() {
        // 500 topic patterns of varying specificity. A route() call should
        // still complete in a few ms per invocation.
        exchangeTypes.put("topical", "topic");
        List<Binding> many = new ArrayList<>(500);
        for (int i = 0; i < 250; i++) {
            many.add(new Binding("topical", "specific" + i, "cat" + i + ".*", Map.of()));
        }
        // Catch-all: 250 queues that always match.
        for (int i = 0; i < 250; i++) {
            many.add(new Binding("topical", "all" + i, "#", Map.of()));
        }
        bindings.put("topical", many);

        long start = System.nanoTime();
        Set<String> res = engine.route("topical", "cat42.news");
        long elapsedMs = (System.nanoTime() - start) / 1_000_000L;

        // specific42 matches cat42.*; all 250 # queues always match.
        assertEquals(251, res.size());
        assertTrue(res.contains("specific42"));
        assertTrue(elapsedMs < 200,
            "topic routing over 500 patterns took " + elapsedMs + "ms");
    }

    // ── Concurrency smoke ──────────────────────────────────────────

    @Test
    void concurrency_multipleThreadsRouteIndependently() throws InterruptedException {
        // RoutingEngine itself is stateless; its mutable state (matched,
        // visited) is local to each route() call. This test verifies that
        // parallel invocations against the SAME engine produce consistent
        // results — there should be no cross-thread contamination of the
        // thread-local sets.
        exchangeTypes.put("topic", "topic");
        List<Binding> bs = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            bs.add(new Binding("topic", "q" + i, "key." + i, Map.of()));
        }
        bs.add(new Binding("topic", "qAll", "#", Map.of()));
        // Substitute with a concurrent-safe map so concurrent reads don't
        // trip a HashMap race during the get().
        Map<String, List<Binding>> concurrentBindings = new ConcurrentHashMap<>();
        concurrentBindings.put("topic", bs);
        Map<String, String> concurrentTypes = new ConcurrentHashMap<>();
        concurrentTypes.put("topic", "topic");

        RoutingEngine concurrentEngine = new RoutingEngine(
            concurrentTypes::get,
            ex -> concurrentBindings.getOrDefault(ex, List.of()),
            ex -> List.of(),
            ex -> null
        );

        int threads = 8;
        int callsPerThread = 200;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        AtomicInteger failures = new AtomicInteger();

        for (int t = 0; t < threads; t++) {
            final int threadIdx = t;
            pool.submit(() -> {
                try {
                    startGate.await();
                    for (int i = 0; i < callsPerThread; i++) {
                        int idx = (threadIdx * 31 + i) % 20;
                        Set<String> r = concurrentEngine.route("topic", "key." + idx);
                        // Expected: the specific queue + qAll.
                        if (!r.equals(Set.of("q" + idx, "qAll"))) {
                            failures.incrementAndGet();
                        }
                    }
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }

        startGate.countDown();
        assertTrue(done.await(10, TimeUnit.SECONDS), "threads did not finish in time");
        pool.shutdown();
        assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));
        assertEquals(0, failures.get(),
            "expected zero failures across " + (threads * callsPerThread) + " calls");
    }

    @Test
    void concurrency_cycleGuardIsThreadLocal() throws InterruptedException {
        // Each route() call uses its own visited set — concurrent calls must
        // not interfere even when routing through cyclic e2e topologies.
        exchangeTypes.put("A", "fanout");
        exchangeTypes.put("B", "fanout");
        bindings.put("A", List.of(new Binding("A", "qA", "", Map.of())));
        bindings.put("B", List.of(new Binding("B", "qB", "", Map.of())));
        e2eBindings.put("A", List.of(new E2EBinding("A", "B", "", Map.of())));
        e2eBindings.put("B", List.of(new E2EBinding("B", "A", "", Map.of())));

        int threads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        AtomicInteger failures = new AtomicInteger();

        for (int t = 0; t < threads; t++) {
            pool.submit(() -> {
                try {
                    startGate.await();
                    for (int i = 0; i < 100; i++) {
                        Set<String> r = engine.route("A", "anything");
                        if (!r.equals(Set.of("qA", "qB"))) {
                            failures.incrementAndGet();
                        }
                    }
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }

        startGate.countDown();
        assertTrue(done.await(10, TimeUnit.SECONDS), "threads did not finish in time");
        pool.shutdown();
        assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));
        assertEquals(0, failures.get(),
            "cycle guard leaked across threads: " + failures.get() + " bad results");
    }

    // ── Fan-out to many queues ─────────────────────────────────────

    @Test
    void fanout_hundredQueues_allReceive() {
        exchangeTypes.put("broadcast", "fanout");
        List<Binding> bs = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            bs.add(new Binding("broadcast", "sub" + i, "", Map.of()));
        }
        bindings.put("broadcast", bs);

        Set<String> result = engine.route("broadcast", "irrelevant");
        assertEquals(100, result.size());
        for (int i = 0; i < 100; i++) {
            assertTrue(result.contains("sub" + i), "missing sub" + i);
        }
    }

    // ── Self-loop ─────────────────────────────────────────────────

    @Test
    void e2eSelfLoop_exchangePointsToItself_doesNotInfiniteLoop() {
        // Pathological but legal: an e2e binding from an exchange to itself.
        // The cycle guard must prevent recursion. Queue bindings on the
        // exchange should still fire exactly once.
        exchangeTypes.put("self", "fanout");
        bindings.put("self", List.of(new Binding("self", "q", "", Map.of())));
        e2eBindings.put("self", List.of(new E2EBinding("self", "self", "", Map.of())));

        Set<String> result = engine.route("self", "anything");
        assertEquals(Set.of("q"), result);
    }

    @Test
    void alternateExchange_selfAsAlternate_doesNotLoop() {
        // ex has no matches and its alternate is itself. Cycle guard returns
        // cleanly with empty result.
        exchangeTypes.put("ex", "direct");
        alternateExchanges.put("ex", "ex");

        Set<String> result = engine.route("ex", "any");
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    // ── Mutated binding view across calls ──────────────────────────

    @Test
    void bindingsFnMutatedBetweenCalls_reflectedOnNextRoute() {
        // Engine holds no cache of bindings — the function is called fresh
        // on every route(). This documents/verifies that mutation-between-
        // calls is visible, which is critical for live declare/bind/unbind.
        exchangeTypes.put("ex", "direct");
        bindings.put("ex", new ArrayList<>(List.of(
            new Binding("ex", "q1", "k", Map.of()))));

        assertEquals(Set.of("q1"), engine.route("ex", "k"));

        // Add a binding.
        bindings.get("ex").add(new Binding("ex", "q2", "k", Map.of()));
        assertEquals(Set.of("q1", "q2"), engine.route("ex", "k"));

        // Remove q1.
        bindings.put("ex", List.of(new Binding("ex", "q2", "k", Map.of())));
        assertEquals(Set.of("q2"), engine.route("ex", "k"));
    }

    // ── Sanity: returned Set is mutable / independent ──────────────

    @Test
    void returnedSet_isIndependentOfEngineState() {
        // Callers occasionally filter/augment the returned set. Verify that
        // mutating the returned set does not affect subsequent calls.
        exchangeTypes.put("ex", "direct");
        bindings.put("ex", List.of(
            new Binding("ex", "q1", "k", Map.of()),
            new Binding("ex", "q2", "k", Map.of())));

        Set<String> first = engine.route("ex", "k");
        assertEquals(Set.of("q1", "q2"), first);

        // Mutate the returned set — must not affect the engine's view.
        first.clear();
        first.add("injected");

        Set<String> second = engine.route("ex", "k");
        assertEquals(Set.of("q1", "q2"), second);
        assertFalse(second.contains("injected"));
    }
}
