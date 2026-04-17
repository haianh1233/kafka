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
// Time: Created - TASK-WS3.06
package kafka.server.http.ws;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tracks resource counts for the WebSocket protocol and enforces hard upper
 * bounds configured by the broker operator (design doc §19.5):
 *
 * <ul>
 *   <li>{@code ws.max.connections.per.broker} — total live WebSocket
 *       connections on this broker.</li>
 *   <li>{@code ws.max.exchanges.per.vhost} — exchanges created per vhost.</li>
 *   <li>{@code ws.max.queues.per.vhost} — queues created per vhost.</li>
 *   <li>{@code ws.max.bindings.per.exchange} — bindings targeting a single
 *       exchange.</li>
 * </ul>
 *
 * <h3>Role vs. existing managers</h3>
 *
 * <p>{@link kafka.server.http.routing.ExchangeManager} and
 * {@link kafka.server.http.routing.BindingManager} already enforce per-vhost
 * exchange and per-exchange binding quotas by consulting
 * {@link WsRoutingMetadataManager} / their own internal state. This manager is
 * the <b>authoritative counter</b> for resources that do <i>not</i> have a
 * persisted metadata record — specifically {@code connectionCount} — and the
 * <b>single point of truth</b> that callers can query before attempting an
 * expensive operation (preflight check) without loading the routing cache.
 *
 * <p>For exchange/queue/binding counts the check methods are intentionally
 * lightweight and operate on local counters. Callers that need a strictly
 * consistent check must still rely on the routing metadata manager (which
 * serialises the check-then-write via its per-vhost lock). This manager is
 * intended for the "fast-path preflight" + "rejected with
 * {@code RESOURCE_LIMIT_EXCEEDED}" outcome described in the WS3.06 task spec.
 *
 * <h3>Thread-safety</h3>
 *
 * <p>All state is held in {@link ConcurrentHashMap}s of {@link AtomicInteger}s
 * and a single {@link AtomicInteger} for connection count, so every method is
 * safe to call concurrently from Netty worker threads, the consumer executor,
 * and REST request threads.
 *
 * // Time: Created - TASK-WS3.06
 */
public final class WsResourceLimitManager {

    private final int maxExchangesPerVhost;
    private final int maxQueuesPerVhost;
    private final int maxBindingsPerExchange;
    private final int maxConnectionsPerBroker;

    private final AtomicInteger connectionCount = new AtomicInteger(0);
    private final ConcurrentHashMap<String, AtomicInteger> exchangeCounts = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicInteger> queueCounts = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicInteger> bindingCounts = new ConcurrentHashMap<>();

    public WsResourceLimitManager(int maxExchangesPerVhost,
                                   int maxQueuesPerVhost,
                                   int maxBindingsPerExchange,
                                   int maxConnectionsPerBroker) {
        if (maxExchangesPerVhost <= 0) {
            throw new IllegalArgumentException("maxExchangesPerVhost must be > 0");
        }
        if (maxQueuesPerVhost <= 0) {
            throw new IllegalArgumentException("maxQueuesPerVhost must be > 0");
        }
        if (maxBindingsPerExchange <= 0) {
            throw new IllegalArgumentException("maxBindingsPerExchange must be > 0");
        }
        if (maxConnectionsPerBroker <= 0) {
            throw new IllegalArgumentException("maxConnectionsPerBroker must be > 0");
        }
        this.maxExchangesPerVhost = maxExchangesPerVhost;
        this.maxQueuesPerVhost = maxQueuesPerVhost;
        this.maxBindingsPerExchange = maxBindingsPerExchange;
        this.maxConnectionsPerBroker = maxConnectionsPerBroker;
    }

    /**
     * Convenience factory that wires the manager to the configured limits on
     * {@link WsConfigs}. Use this in production; the explicit-int constructor
     * is convenient for tests that need per-case tuning.
     */
    public static WsResourceLimitManager fromConfigs(WsConfigs cfg) {
        Objects.requireNonNull(cfg, "cfg");
        return new WsResourceLimitManager(
            cfg.maxExchangesPerVhost(),
            cfg.maxQueuesPerVhost(),
            cfg.maxBindingsPerExchange(),
            cfg.maxConnectionsPerBroker());
    }

    // ------------------------------------------------------------------
    //  Connection limit (used by WsUpgradeOrHttpHandler)
    // ------------------------------------------------------------------

    /**
     * @return {@code true} when the broker has capacity for one more
     *         connection. Caller must still call {@link #recordConnectionOpen}
     *         when the connection is actually accepted — a true return is not
     *         a reservation.
     */
    public boolean canAcceptConnection() {
        return connectionCount.get() < maxConnectionsPerBroker;
    }

    /**
     * Atomically increments the connection counter iff capacity is available.
     * This is the safe "reserve a slot" path — returns {@code true} on
     * success, {@code false} without any side effect when the broker is full.
     */
    public boolean tryReserveConnection() {
        while (true) {
            int current = connectionCount.get();
            if (current >= maxConnectionsPerBroker) {
                return false;
            }
            if (connectionCount.compareAndSet(current, current + 1)) {
                return true;
            }
        }
    }

    public void recordConnectionOpen() {
        connectionCount.incrementAndGet();
    }

    public void recordConnectionClose() {
        // Decrement but never drop below zero — defensive against double-close.
        connectionCount.updateAndGet(v -> Math.max(0, v - 1));
    }

    public int connectionCount() {
        return connectionCount.get();
    }

    public int maxConnectionsPerBroker() {
        return maxConnectionsPerBroker;
    }

    // ------------------------------------------------------------------
    //  Exchange limit (per-vhost)
    // ------------------------------------------------------------------

    public boolean canCreateExchange(String vhost) {
        Objects.requireNonNull(vhost, "vhost");
        return counter(exchangeCounts, vhost).get() < maxExchangesPerVhost;
    }

    public void recordExchangeCreated(String vhost) {
        Objects.requireNonNull(vhost, "vhost");
        counter(exchangeCounts, vhost).incrementAndGet();
    }

    public void recordExchangeDeleted(String vhost) {
        Objects.requireNonNull(vhost, "vhost");
        counter(exchangeCounts, vhost).updateAndGet(v -> Math.max(0, v - 1));
    }

    public int exchangeCount(String vhost) {
        Objects.requireNonNull(vhost, "vhost");
        AtomicInteger c = exchangeCounts.get(vhost);
        return c == null ? 0 : c.get();
    }

    public int maxExchangesPerVhost() {
        return maxExchangesPerVhost;
    }

    // ------------------------------------------------------------------
    //  Queue limit (per-vhost)
    // ------------------------------------------------------------------

    public boolean canCreateQueue(String vhost) {
        Objects.requireNonNull(vhost, "vhost");
        return counter(queueCounts, vhost).get() < maxQueuesPerVhost;
    }

    public void recordQueueCreated(String vhost) {
        Objects.requireNonNull(vhost, "vhost");
        counter(queueCounts, vhost).incrementAndGet();
    }

    public void recordQueueDeleted(String vhost) {
        Objects.requireNonNull(vhost, "vhost");
        counter(queueCounts, vhost).updateAndGet(v -> Math.max(0, v - 1));
    }

    public int queueCount(String vhost) {
        Objects.requireNonNull(vhost, "vhost");
        AtomicInteger c = queueCounts.get(vhost);
        return c == null ? 0 : c.get();
    }

    public int maxQueuesPerVhost() {
        return maxQueuesPerVhost;
    }

    // ------------------------------------------------------------------
    //  Binding limit (per-exchange)
    // ------------------------------------------------------------------

    public boolean canCreateBinding(String exchangeName) {
        Objects.requireNonNull(exchangeName, "exchangeName");
        return counter(bindingCounts, exchangeName).get() < maxBindingsPerExchange;
    }

    public void recordBindingCreated(String exchangeName) {
        Objects.requireNonNull(exchangeName, "exchangeName");
        counter(bindingCounts, exchangeName).incrementAndGet();
    }

    public void recordBindingDeleted(String exchangeName) {
        Objects.requireNonNull(exchangeName, "exchangeName");
        counter(bindingCounts, exchangeName).updateAndGet(v -> Math.max(0, v - 1));
    }

    public int bindingCount(String exchangeName) {
        Objects.requireNonNull(exchangeName, "exchangeName");
        AtomicInteger c = bindingCounts.get(exchangeName);
        return c == null ? 0 : c.get();
    }

    public int maxBindingsPerExchange() {
        return maxBindingsPerExchange;
    }

    // ------------------------------------------------------------------
    //  Internal
    // ------------------------------------------------------------------

    private static AtomicInteger counter(ConcurrentHashMap<String, AtomicInteger> map, String key) {
        return map.computeIfAbsent(key, k -> new AtomicInteger(0));
    }
}
