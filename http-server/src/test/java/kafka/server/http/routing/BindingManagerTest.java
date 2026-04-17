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

// Time: Created - TASK-WS1.07

package kafka.server.http.routing;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link BindingManager}.
 *
 * // Time: Created - TASK-WS1.07
 */
class BindingManagerTest {

    private BindingManager manager;
    private Set<String> existingExchanges;
    private Set<String> existingQueues;

    @BeforeEach
    void setUp() {
        existingExchanges = new HashSet<>(Set.of(
            "events", "orders", "broadcast", "source", "dest", "dest2", "fanout"));
        existingQueues = new HashSet<>(Set.of(
            "order-events", "payment-events", "audit-log", "all-events"));
        manager = new BindingManager(
            existingExchanges::contains,
            existingQueues::contains
        );
    }

    // --- bind() ---

    @Test
    void bind_validExchangeAndQueue_succeeds() {
        manager.bind("events", "order-events", "order.created");
        List<Binding> bindings = manager.listByExchange("events");
        assertEquals(1, bindings.size());
        assertEquals("order-events", bindings.get(0).queue());
        assertEquals("order.created", bindings.get(0).routingKey());
    }

    @Test
    void bind_multipleBindings_allStored() {
        manager.bind("events", "order-events", "order.created");
        manager.bind("events", "payment-events", "payment.completed");
        assertEquals(2, manager.listByExchange("events").size());
    }

    @Test
    void bind_duplicateBinding_idempotent() {
        manager.bind("events", "order-events", "order.created");
        manager.bind("events", "order-events", "order.created");
        assertEquals(1, manager.listByExchange("events").size());
    }

    @Test
    void bind_sameQueueDifferentRoutingKey_createsBoth() {
        manager.bind("events", "order-events", "order.created");
        manager.bind("events", "order-events", "order.updated");
        assertEquals(2, manager.listByExchange("events").size());
    }

    @Test
    void bind_exchangeNotFound_throws() {
        assertThrows(IllegalArgumentException.class,
            () -> manager.bind("nonexistent", "order-events", "key"));
    }

    @Test
    void bind_queueNotFound_throws() {
        assertThrows(IllegalArgumentException.class,
            () -> manager.bind("events", "nonexistent", "key"));
    }

    @Test
    void bind_exceedsMaxBindings_throws() {
        BindingManager limited = new BindingManager(
            2, existingExchanges::contains, existingQueues::contains);
        limited.bind("events", "order-events", "key1");
        limited.bind("events", "payment-events", "key2");
        assertThrows(IllegalStateException.class,
            () -> limited.bind("events", "audit-log", "key3"));
    }

    @Test
    void bind_maxBindings_perExchangeIndependent() {
        BindingManager limited = new BindingManager(
            2, existingExchanges::contains, existingQueues::contains);
        limited.bind("events", "order-events", "k1");
        limited.bind("events", "payment-events", "k2");
        // `orders` exchange is independent — should still allow 2 bindings there.
        assertDoesNotThrow(() -> limited.bind("orders", "order-events", "k1"));
        assertDoesNotThrow(() -> limited.bind("orders", "payment-events", "k2"));
    }

    @Test
    void bind_withArguments_storedCorrectly() {
        Map<String, String> args = Map.of("x-match", "all", "priority", "high");
        manager.bind("events", "order-events", "", args);
        List<Binding> bindings = manager.listByExchange("events");
        assertEquals(1, bindings.size());
        assertEquals("all", bindings.get(0).arguments().get("x-match"));
        assertEquals("high", bindings.get(0).arguments().get("priority"));
    }

    @Test
    void bind_sameQueueKeyDifferentArgs_createsBoth() {
        manager.bind("events", "order-events", "",
            Map.of("x-match", "all", "priority", "high"));
        manager.bind("events", "order-events", "",
            Map.of("x-match", "any", "region", "us"));
        assertEquals(2, manager.listByExchange("events").size());
    }

    @Test
    void bind_nullArguments_defaultsToEmptyMap() {
        manager.bind("events", "order-events", "key", null);
        assertEquals(Map.of(), manager.listByExchange("events").get(0).arguments());
    }

    @Test
    void bind_nullExchange_throwsNPE() {
        assertThrows(NullPointerException.class,
            () -> manager.bind(null, "order-events", "key"));
    }

    @Test
    void bind_nullQueue_throwsNPE() {
        assertThrows(NullPointerException.class,
            () -> manager.bind("events", null, "key"));
    }

    @Test
    void bind_nullRoutingKey_throwsNPE() {
        assertThrows(NullPointerException.class,
            () -> manager.bind("events", "order-events", null));
    }

    // --- unbind() ---

    @Test
    void unbind_existingBinding_removes() {
        manager.bind("events", "order-events", "order.created");
        manager.unbind("events", "order-events", "order.created");
        assertTrue(manager.listByExchange("events").isEmpty());
    }

    @Test
    void unbind_nonExistentBinding_silentSuccess() {
        assertDoesNotThrow(() -> manager.unbind("events", "order-events", "nonexistent"));
    }

    @Test
    void unbind_nonExistentExchange_silentSuccess() {
        assertDoesNotThrow(() -> manager.unbind("nonexistent", "order-events", "key"));
    }

    @Test
    void unbind_onlyMatching_leavesOthers() {
        manager.bind("events", "order-events", "order.created");
        manager.bind("events", "payment-events", "payment.completed");
        manager.unbind("events", "order-events", "order.created");
        List<Binding> remaining = manager.listByExchange("events");
        assertEquals(1, remaining.size());
        assertEquals("payment-events", remaining.get(0).queue());
    }

    // --- listByExchange / listByQueue ---

    @Test
    void listByExchange_noBindings_returnsEmpty() {
        assertTrue(manager.listByExchange("events").isEmpty());
    }

    @Test
    void listByExchange_returnsSnapshot_notLiveView() {
        manager.bind("events", "order-events", "k1");
        List<Binding> snapshot = manager.listByExchange("events");
        manager.bind("events", "payment-events", "k2");
        // snapshot taken before the second bind must not change
        assertEquals(1, snapshot.size());
    }

    @Test
    void listByQueue_acrossMultipleExchanges() {
        manager.bind("events", "order-events", "order.created");
        manager.bind("orders", "order-events", "payment.done");
        List<Binding> bindings = manager.listByQueue("order-events");
        assertEquals(2, bindings.size());
    }

    @Test
    void listByQueue_noBindings_returnsEmpty() {
        assertTrue(manager.listByQueue("order-events").isEmpty());
    }

    @Test
    void listByQueue_filtersOutOtherQueues() {
        manager.bind("events", "order-events", "k1");
        manager.bind("events", "payment-events", "k2");
        manager.bind("orders", "order-events", "k3");
        List<Binding> bindings = manager.listByQueue("order-events");
        assertEquals(2, bindings.size());
        assertTrue(bindings.stream().allMatch(b -> b.queue().equals("order-events")));
    }

    // --- e2e bindings ---

    @Test
    void bindE2E_validExchanges_succeeds() {
        manager.bindExchangeToExchange("source", "dest", "order.*", Map.of());
        List<E2EBinding> e2eBindings = manager.listE2EBySource("source");
        assertEquals(1, e2eBindings.size());
        assertEquals("dest", e2eBindings.get(0).destination());
    }

    @Test
    void bindE2E_duplicate_idempotent() {
        manager.bindExchangeToExchange("source", "dest", "key", Map.of());
        manager.bindExchangeToExchange("source", "dest", "key", Map.of());
        assertEquals(1, manager.listE2EBySource("source").size());
    }

    @Test
    void bindE2E_sourceNotFound_throws() {
        assertThrows(IllegalArgumentException.class,
            () -> manager.bindExchangeToExchange("nonexistent", "dest", "key", Map.of()));
    }

    @Test
    void bindE2E_destinationNotFound_throws() {
        assertThrows(IllegalArgumentException.class,
            () -> manager.bindExchangeToExchange("source", "nonexistent", "key", Map.of()));
    }

    @Test
    void bindE2E_differentDestinations_bothStored() {
        manager.bindExchangeToExchange("source", "dest", "key", Map.of());
        manager.bindExchangeToExchange("source", "dest2", "key", Map.of());
        assertEquals(2, manager.listE2EBySource("source").size());
    }

    @Test
    void unbindE2E_removes() {
        manager.bindExchangeToExchange("source", "dest", "key", Map.of());
        manager.unbindExchangeFromExchange("source", "dest", "key");
        assertTrue(manager.listE2EBySource("source").isEmpty());
    }

    @Test
    void unbindE2E_nonExistent_silentSuccess() {
        assertDoesNotThrow(
            () -> manager.unbindExchangeFromExchange("source", "dest", "key"));
    }

    @Test
    void listE2EBySource_noBindings_returnsEmpty() {
        assertTrue(manager.listE2EBySource("source").isEmpty());
    }

    // --- cleanup ---

    @Test
    void removeAllForExchange_clearsAllBindings() {
        manager.bind("events", "order-events", "key1");
        manager.bind("events", "payment-events", "key2");
        manager.bindExchangeToExchange("events", "dest", "key3", Map.of());
        manager.removeAllForExchange("events");
        assertTrue(manager.listByExchange("events").isEmpty());
        assertTrue(manager.listE2EBySource("events").isEmpty());
    }

    @Test
    void removeAllForExchange_cleansUpE2EDestinations() {
        manager.bindExchangeToExchange("source", "events", "key", Map.of());
        manager.bindExchangeToExchange("source", "dest", "other", Map.of());
        manager.removeAllForExchange("events");
        // e2e bindings where events was DESTINATION should be gone
        List<E2EBinding> remaining = manager.listE2EBySource("source");
        assertFalse(remaining.stream().anyMatch(b -> b.destination().equals("events")));
        // unrelated bindings are preserved
        assertTrue(remaining.stream().anyMatch(b -> b.destination().equals("dest")));
    }

    @Test
    void removeAllForExchange_nonExistent_silentSuccess() {
        assertDoesNotThrow(() -> manager.removeAllForExchange("never-bound"));
    }

    @Test
    void removeAllForQueue_removesFromAllExchanges() {
        manager.bind("events", "order-events", "key1");
        manager.bind("orders", "order-events", "key2");
        manager.bind("events", "payment-events", "key3");
        manager.removeAllForQueue("order-events");
        assertTrue(manager.listByQueue("order-events").isEmpty());
        // payment-events binding on `events` is preserved
        assertEquals(1, manager.listByExchange("events").size());
        assertEquals("payment-events", manager.listByExchange("events").get(0).queue());
    }

    @Test
    void removeAllForQueue_nonExistent_silentSuccess() {
        assertDoesNotThrow(() -> manager.removeAllForQueue("never-bound"));
    }

    // --- totalBindingCount ---

    @Test
    void totalBindingCount_sumsAllExchanges() {
        manager.bind("events", "order-events", "key1");
        manager.bind("events", "payment-events", "key2");
        manager.bind("orders", "audit-log", "key3");
        assertEquals(3, manager.totalBindingCount());
    }

    @Test
    void totalBindingCount_empty_returnsZero() {
        assertEquals(0, manager.totalBindingCount());
    }

    @Test
    void totalBindingCount_doesNotCountE2E() {
        manager.bind("events", "order-events", "key1");
        manager.bindExchangeToExchange("source", "dest", "key", Map.of());
        // totalBindingCount reflects queue-bindings only per contract
        assertEquals(1, manager.totalBindingCount());
    }

    // --- concurrency ---

    @Test
    void concurrentBindUnbind_doesNotCorruptIndex() throws Exception {
        int threads = 8;
        int opsPerThread = 500;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            CountDownLatch ready = new CountDownLatch(threads);
            CountDownLatch go = new CountDownLatch(1);
            AtomicInteger failures = new AtomicInteger();

            for (int t = 0; t < threads; t++) {
                final int id = t;
                pool.submit(() -> {
                    try {
                        ready.countDown();
                        go.await();
                        String queue = (id % 2 == 0) ? "order-events" : "payment-events";
                        for (int i = 0; i < opsPerThread; i++) {
                            String key = "k-" + id + "-" + (i % 50);
                            try {
                                manager.bind("events", queue, key);
                                manager.unbind("events", queue, key);
                            } catch (Exception e) {
                                failures.incrementAndGet();
                            }
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
            }

            ready.await();
            go.countDown();
            pool.shutdown();
            assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));
            assertEquals(0, failures.get());

            // Index should be consistent — iterate listByExchange without ConcurrentModification.
            List<Binding> finalBindings = manager.listByExchange("events");
            // Count must match sum-over-queues via listByQueue.
            int sum = manager.listByQueue("order-events").size()
                + manager.listByQueue("payment-events").size();
            assertEquals(finalBindings.size(), sum);
        } finally {
            pool.shutdownNow();
        }
    }

    // --- Binding record ---

    @Test
    void bindingRecord_nullExchange_throwsNPE() {
        assertThrows(NullPointerException.class,
            () -> new Binding(null, "q", "k", Map.of()));
    }

    @Test
    void bindingRecord_nullQueue_throwsNPE() {
        assertThrows(NullPointerException.class,
            () -> new Binding("ex", null, "k", Map.of()));
    }

    @Test
    void bindingRecord_nullRoutingKey_throwsNPE() {
        assertThrows(NullPointerException.class,
            () -> new Binding("ex", "q", null, Map.of()));
    }

    @Test
    void bindingRecord_nullArguments_defaultsToEmptyMap() {
        Binding b = new Binding("ex", "q", "k", null);
        assertEquals(Map.of(), b.arguments());
    }

    @Test
    void bindingRecord_argumentsAreImmutable() {
        HashMap<String, String> args = new HashMap<>();
        args.put("key", "value");
        Binding b = new Binding("ex", "q", "k", args);
        // mutating the original must not affect the record
        args.put("added", "after");
        assertFalse(b.arguments().containsKey("added"));
        // returned view must be read-only
        assertThrows(UnsupportedOperationException.class,
            () -> b.arguments().put("new", "val"));
    }

    @Test
    void e2eBindingRecord_nullSource_throwsNPE() {
        assertThrows(NullPointerException.class,
            () -> new E2EBinding(null, "d", "k", Map.of()));
    }

    @Test
    void e2eBindingRecord_nullDestination_throwsNPE() {
        assertThrows(NullPointerException.class,
            () -> new E2EBinding("s", null, "k", Map.of()));
    }

    @Test
    void e2eBindingRecord_nullArguments_defaultsToEmptyMap() {
        E2EBinding b = new E2EBinding("s", "d", "k", null);
        assertEquals(Map.of(), b.arguments());
    }
}
