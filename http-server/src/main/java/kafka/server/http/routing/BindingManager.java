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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Predicate;

/**
 * Manages binding lifecycle: bind, unbind, list, and cleanup.
 *
 * <p>In-memory index backed by {@link ConcurrentHashMap} of
 * {@link CopyOnWriteArrayList}, keyed by exchange name for fast lookup during
 * message routing. Exchange-to-exchange (e2e) bindings are tracked separately
 * for phase-2 exchange fan-out.
 *
 * <p><b>Validation.</b> Exchange existence is checked via
 * {@code exchangeExistsFn}; queue existence via {@code queueExistsFn}. These
 * are supplied as {@link Predicate}s so the manager does not need a hard
 * dependency on ExchangeManager / QueueManager (the latter is not yet built).
 *
 * <p><b>Duplicate detection.</b> A binding is considered duplicate when
 * (exchange, queue, routingKey, arguments) all match; duplicate {@code bind()}
 * is a silent no-op (AMQP semantics). Different {@code arguments} maps produce
 * distinct bindings, so headers-exchange bindings with different match sets
 * coexist.
 *
 * <p><b>Thread-safety.</b> The exchange→list map uses {@link ConcurrentHashMap}
 * for lock-free reads; the per-exchange bucket is a
 * {@link CopyOnWriteArrayList} so writes never corrupt concurrent iterators.
 * The check-then-add sequence inside {@code bind()} is guarded by a
 * per-exchange monitor so two concurrent identical binds cannot both append.
 *
 * // Time: Created - TASK-WS1.07
 */
public final class BindingManager {

    private static final Logger log = LoggerFactory.getLogger(BindingManager.class);

    /** Default limit for {@code ws.max.bindings.per.exchange} (design doc §17). */
    public static final int DEFAULT_MAX_BINDINGS_PER_EXCHANGE = 10_000;

    private final int maxBindingsPerExchange;

    /** exchange name → list of queue bindings. */
    private final ConcurrentHashMap<String, CopyOnWriteArrayList<Binding>> bindingsByExchange =
        new ConcurrentHashMap<>();

    /** source exchange name → list of e2e bindings. */
    private final ConcurrentHashMap<String, CopyOnWriteArrayList<E2EBinding>> e2eBindingsBySource =
        new ConcurrentHashMap<>();

    /** Per-exchange locks guarding check-then-add regions. */
    private final ConcurrentHashMap<String, Object> exchangeLocks = new ConcurrentHashMap<>();

    private final Predicate<String> exchangeExistsFn;
    private final Predicate<String> queueExistsFn;

    /**
     * @param maxBindingsPerExchange max bindings per exchange
     *                               ({@code ws.max.bindings.per.exchange})
     * @param exchangeExistsFn       checks if an exchange exists (typically a method
     *                               reference to {@code ExchangeManager::exchangeExists})
     * @param queueExistsFn          checks if a queue exists (typically a method reference
     *                               to {@code QueueManager::queueExists}; see Limitations)
     */
    public BindingManager(int maxBindingsPerExchange,
                          Predicate<String> exchangeExistsFn,
                          Predicate<String> queueExistsFn) {
        if (maxBindingsPerExchange <= 0) {
            throw new IllegalArgumentException(
                "maxBindingsPerExchange must be positive, got " + maxBindingsPerExchange);
        }
        this.maxBindingsPerExchange = maxBindingsPerExchange;
        this.exchangeExistsFn = Objects.requireNonNull(exchangeExistsFn, "exchangeExistsFn");
        this.queueExistsFn = Objects.requireNonNull(queueExistsFn, "queueExistsFn");
    }

    public BindingManager(Predicate<String> exchangeExistsFn, Predicate<String> queueExistsFn) {
        this(DEFAULT_MAX_BINDINGS_PER_EXCHANGE, exchangeExistsFn, queueExistsFn);
    }

    // ------------------------------------------------------------------
    // bind / unbind
    // ------------------------------------------------------------------

    /**
     * Binds a queue to an exchange with a routing key and arguments.
     * Idempotent: duplicate (exchange, queue, routingKey, arguments) silently succeeds.
     *
     * @throws NullPointerException     if any of exchange/queue/routingKey is {@code null}
     * @throws IllegalArgumentException if exchange or queue does not exist
     * @throws IllegalStateException    if max bindings per exchange exceeded
     */
    public void bind(String exchange, String queue, String routingKey, Map<String, String> arguments) {
        Objects.requireNonNull(exchange, "exchange");
        Objects.requireNonNull(queue, "queue");
        Objects.requireNonNull(routingKey, "routingKey");

        if (!exchangeExistsFn.test(exchange)) {
            throw new IllegalArgumentException("Exchange does not exist: " + exchange);
        }
        if (!queueExistsFn.test(queue)) {
            throw new IllegalArgumentException("Queue does not exist: " + queue);
        }

        Binding candidate = new Binding(exchange, queue, routingKey, arguments);

        synchronized (lockFor(exchange)) {
            CopyOnWriteArrayList<Binding> list =
                bindingsByExchange.computeIfAbsent(exchange, k -> new CopyOnWriteArrayList<>());

            // Duplicate detection on all four fields — argument-aware.
            for (Binding existing : list) {
                if (existing.equals(candidate)) {
                    log.debug("bind: duplicate ignored exchange={} queue={} routingKey={}",
                        exchange, queue, routingKey);
                    return;
                }
            }

            if (list.size() >= maxBindingsPerExchange) {
                throw new IllegalStateException(
                    "Exchange '" + exchange + "' already has " + list.size()
                        + " bindings; limit is " + maxBindingsPerExchange);
            }

            list.add(candidate);
            log.debug("bind: exchange={} queue={} routingKey={} args={}",
                exchange, queue, routingKey, candidate.arguments());
        }
    }

    /** Convenience overload with no arguments. */
    public void bind(String exchange, String queue, String routingKey) {
        bind(exchange, queue, routingKey, Map.of());
    }

    /**
     * Creates an exchange-to-exchange binding.
     * Idempotent: duplicate (source, destination, routingKey, arguments) silently succeeds.
     *
     * @throws IllegalArgumentException if either exchange does not exist
     */
    public void bindExchangeToExchange(String source, String destination,
                                       String routingKey, Map<String, String> arguments) {
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(destination, "destination");
        Objects.requireNonNull(routingKey, "routingKey");

        if (!exchangeExistsFn.test(source)) {
            throw new IllegalArgumentException("Source exchange does not exist: " + source);
        }
        if (!exchangeExistsFn.test(destination)) {
            throw new IllegalArgumentException(
                "Destination exchange does not exist: " + destination);
        }

        E2EBinding candidate = new E2EBinding(source, destination, routingKey, arguments);

        synchronized (lockFor("e2e:" + source)) {
            CopyOnWriteArrayList<E2EBinding> list =
                e2eBindingsBySource.computeIfAbsent(source, k -> new CopyOnWriteArrayList<>());

            for (E2EBinding existing : list) {
                if (existing.equals(candidate)) {
                    log.debug("bindE2E: duplicate ignored source={} dest={} routingKey={}",
                        source, destination, routingKey);
                    return;
                }
            }

            list.add(candidate);
            log.debug("bindE2E: source={} dest={} routingKey={} args={}",
                source, destination, routingKey, candidate.arguments());
        }
    }

    /**
     * Removes a queue-to-exchange binding. Silent no-op if no match (AMQP spec).
     * Matches on (queue, routingKey); argument-sensitive duplicates are all removed.
     */
    public void unbind(String exchange, String queue, String routingKey) {
        Objects.requireNonNull(exchange, "exchange");
        Objects.requireNonNull(queue, "queue");
        Objects.requireNonNull(routingKey, "routingKey");

        CopyOnWriteArrayList<Binding> list = bindingsByExchange.get(exchange);
        if (list == null) {
            return;
        }
        boolean removed = list.removeIf(
            b -> b.queue().equals(queue) && b.routingKey().equals(routingKey));
        if (removed) {
            log.debug("unbind: exchange={} queue={} routingKey={}", exchange, queue, routingKey);
        }
    }

    /** Removes an exchange-to-exchange binding. Silent no-op if missing. */
    public void unbindExchangeFromExchange(String source, String destination, String routingKey) {
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(destination, "destination");
        Objects.requireNonNull(routingKey, "routingKey");

        CopyOnWriteArrayList<E2EBinding> list = e2eBindingsBySource.get(source);
        if (list == null) {
            return;
        }
        boolean removed = list.removeIf(
            b -> b.destination().equals(destination) && b.routingKey().equals(routingKey));
        if (removed) {
            log.debug("unbindE2E: source={} dest={} routingKey={}",
                source, destination, routingKey);
        }
    }

    // ------------------------------------------------------------------
    // List
    // ------------------------------------------------------------------

    /** Returns an unmodifiable snapshot of all bindings for an exchange. */
    public List<Binding> listByExchange(String exchange) {
        Objects.requireNonNull(exchange, "exchange");
        CopyOnWriteArrayList<Binding> list = bindingsByExchange.get(exchange);
        if (list == null || list.isEmpty()) {
            return Collections.emptyList();
        }
        // Snapshot: a plain copy of the COW list's current state.
        return List.copyOf(list);
    }

    /**
     * Returns all bindings that target a specific queue across all exchanges.
     * Uses a scan over the exchange map; O(total bindings) — acceptable at the
     * typical scale ({@code <= ws.max.bindings.per.exchange * #exchanges}).
     */
    public List<Binding> listByQueue(String queue) {
        Objects.requireNonNull(queue, "queue");
        List<Binding> result = new ArrayList<>();
        for (CopyOnWriteArrayList<Binding> list : bindingsByExchange.values()) {
            for (Binding b : list) {
                if (b.queue().equals(queue)) {
                    result.add(b);
                }
            }
        }
        return Collections.unmodifiableList(result);
    }

    /** Returns an unmodifiable snapshot of all e2e bindings for a source exchange. */
    public List<E2EBinding> listE2EBySource(String source) {
        Objects.requireNonNull(source, "source");
        CopyOnWriteArrayList<E2EBinding> list = e2eBindingsBySource.get(source);
        if (list == null || list.isEmpty()) {
            return Collections.emptyList();
        }
        return List.copyOf(list);
    }

    // ------------------------------------------------------------------
    // Cleanup (cascade hooks)
    // ------------------------------------------------------------------

    /**
     * Removes all bindings involving an exchange — typically called when the
     * exchange is deleted. Clears:
     * <ul>
     *   <li>All queue bindings on {@code exchange}.</li>
     *   <li>All e2e bindings where {@code exchange} is the source.</li>
     *   <li>Any e2e bindings in other sources that target {@code exchange} as
     *       their destination.</li>
     * </ul>
     */
    public void removeAllForExchange(String exchange) {
        Objects.requireNonNull(exchange, "exchange");
        bindingsByExchange.remove(exchange);
        e2eBindingsBySource.remove(exchange);
        for (CopyOnWriteArrayList<E2EBinding> list : e2eBindingsBySource.values()) {
            list.removeIf(b -> b.destination().equals(exchange));
        }
        log.debug("removeAllForExchange: {}", exchange);
    }

    /**
     * Removes all bindings targeting a queue across all exchanges — typically
     * called when the queue is deleted.
     */
    public void removeAllForQueue(String queue) {
        Objects.requireNonNull(queue, "queue");
        for (CopyOnWriteArrayList<Binding> list : bindingsByExchange.values()) {
            list.removeIf(b -> b.queue().equals(queue));
        }
        log.debug("removeAllForQueue: {}", queue);
    }

    // ------------------------------------------------------------------
    // Metrics / quotas
    // ------------------------------------------------------------------

    /** Returns total queue-binding count across all exchanges (e2e excluded). */
    public int totalBindingCount() {
        int sum = 0;
        for (CopyOnWriteArrayList<Binding> list : bindingsByExchange.values()) {
            sum += list.size();
        }
        return sum;
    }

    /** Returns binding count for a single exchange (queue bindings only). */
    public int bindingCount(String exchange) {
        Objects.requireNonNull(exchange, "exchange");
        CopyOnWriteArrayList<Binding> list = bindingsByExchange.get(exchange);
        return list == null ? 0 : list.size();
    }

    /** Configured upper bound from {@code ws.max.bindings.per.exchange}. */
    public int maxBindingsPerExchange() {
        return maxBindingsPerExchange;
    }

    // ------------------------------------------------------------------
    // Internal
    // ------------------------------------------------------------------

    private Object lockFor(String key) {
        return exchangeLocks.computeIfAbsent(key, k -> new Object());
    }
}
