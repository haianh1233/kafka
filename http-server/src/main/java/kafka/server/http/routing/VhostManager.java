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

import kafka.server.http.ws.BindingMetadata;
import kafka.server.http.ws.ExchangeMetadata;
import kafka.server.http.ws.QueueMetadata;
import kafka.server.http.ws.WsRoutingMetadataManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;

/**
 * Manages virtual hosts — namespace isolation for exchanges, queues and bindings.
 *
 * <p>Each vhost has:
 * <ul>
 *   <li>Its own set of 5 pre-declared exchanges ({@code ""}, {@code amq.direct},
 *       {@code amq.topic}, {@code amq.fanout}, {@code amq.headers}).</li>
 *   <li>Its own {@link RoutingEngine} instance that resolves
 *       {@code (exchange, routingKey, headers)} to the queues of that vhost
 *       only.</li>
 *   <li>A topic-name prefix: {@code ws.} for the default vhost {@code /},
 *       {@code ws.{vhost_without_leading_slash}.} for named vhosts.</li>
 * </ul>
 *
 * <p>The default vhost {@code /} is auto-created on construction and cannot be
 * deleted. Pre-declared exchanges for every created vhost are injected via
 * {@link ExchangeManager#initializeDefaults(String)} — they exist in memory
 * but are not persisted to the {@code __ws_routing_metadata} topic.
 *
 * <p>Thread-safety: vhost create/delete are serialised per-vhost via a
 * {@link ConcurrentHashMap} of per-vhost monitors (same pattern as
 * {@link ExchangeManager}). The routing-engine map, exchange list, and
 * binding manager are already concurrent, so reads are lock-free.
 *
 * // Time: Created - TASK-WS2.09
 */
public final class VhostManager {

    private static final Logger log = LoggerFactory.getLogger(VhostManager.class);

    /** The always-present default vhost. */
    public static final String DEFAULT_VHOST = "/";

    private final WsRoutingMetadataManager metadataManager;
    private final ExchangeManager exchangeManager;
    private final BindingManager bindingManager;
    /**
     * Predicate returning {@code true} when the given vhost has one or more
     * currently-active WebSocket connections. Default (test path) returns
     * {@code false} — wire a real implementation in production from the
     * {@code WsConnectionRegistry}.
     */
    private final Predicate<String> activeConnectionFn;

    /** vhost name -> RoutingEngine. */
    private final ConcurrentHashMap<String, RoutingEngine> routingEngines = new ConcurrentHashMap<>();

    /** Per-vhost monitors guarding create/delete critical sections. */
    private final ConcurrentHashMap<String, Object> vhostLocks = new ConcurrentHashMap<>();

    public VhostManager(WsRoutingMetadataManager metadataManager,
                        ExchangeManager exchangeManager,
                        BindingManager bindingManager) {
        this(metadataManager, exchangeManager, bindingManager, v -> false);
    }

    /**
     * @param activeConnectionFn predicate returning {@code true} when the given
     *                           vhost has active connections; a {@code true}
     *                           result causes {@link #deleteVhost(String)} to
     *                           throw {@link VhostInUseException}.
     */
    public VhostManager(WsRoutingMetadataManager metadataManager,
                        ExchangeManager exchangeManager,
                        BindingManager bindingManager,
                        Predicate<String> activeConnectionFn) {
        this.metadataManager = Objects.requireNonNull(metadataManager, "metadataManager");
        this.exchangeManager = Objects.requireNonNull(exchangeManager, "exchangeManager");
        this.bindingManager = Objects.requireNonNull(bindingManager, "bindingManager");
        this.activeConnectionFn = Objects.requireNonNull(activeConnectionFn, "activeConnectionFn");

        // Auto-create the default vhost.
        createVhost(DEFAULT_VHOST);
    }

    // ------------------------------------------------------------------
    // Lifecycle
    // ------------------------------------------------------------------

    /**
     * Creates a vhost if it does not already exist. Idempotent: repeated calls
     * for the same name are no-ops after the first one populates the
     * pre-declared exchanges.
     */
    public void createVhost(String vhost) {
        Objects.requireNonNull(vhost, "vhost");
        synchronized (lockFor(vhost)) {
            if (routingEngines.containsKey(vhost)) {
                return; // already exists
            }
            exchangeManager.initializeDefaults(vhost);
            routingEngines.put(vhost, newRoutingEngine(vhost));
            log.info("Created vhost '{}' with {} pre-declared exchanges",
                vhost, ExchangeManager.DEFAULT_EXCHANGE_NAMES.size());
        }
    }

    /**
     * Deletes a vhost and all its exchanges, queues, bindings and routing state.
     *
     * <p>Rules:
     * <ul>
     *   <li>Deleting the {@link #DEFAULT_VHOST default vhost} throws
     *       {@link IllegalArgumentException}.</li>
     *   <li>Deleting a vhost that has active connections throws
     *       {@link VhostInUseException}.</li>
     *   <li>Deleting an unknown vhost is a silent no-op (matches the
     *       exchange/queue delete idempotency contract).</li>
     * </ul>
     *
     * <p>This cascades:
     * <ul>
     *   <li>Every user and default exchange in the vhost is removed from the
     *       metadata manager (and its tombstone persisted to the metadata
     *       topic).</li>
     *   <li>Every queue in the vhost is removed.</li>
     *   <li>Every binding on every removed exchange is dropped from the
     *       {@link BindingManager}.</li>
     *   <li>The per-vhost {@link RoutingEngine} is evicted from the local map.</li>
     * </ul>
     *
     * <p><b>Backing Kafka topics are NOT deleted by this method.</b> See
     * Limitations for why — topic deletion requires an admin client wire-up
     * that is outside this task's scope.
     */
    public void deleteVhost(String vhost) {
        Objects.requireNonNull(vhost, "vhost");
        if (DEFAULT_VHOST.equals(vhost)) {
            throw new IllegalArgumentException("Default vhost '/' cannot be deleted");
        }

        synchronized (lockFor(vhost)) {
            if (!routingEngines.containsKey(vhost)) {
                return; // unknown vhost — silent no-op
            }

            // Active-connection gate. Checked inside the lock so a concurrent
            // handshake racing with delete is observed consistently.
            if (activeConnectionFn.test(vhost)) {
                throw new VhostInUseException(vhost);
            }

            // 1. Collect snapshots before mutating so iteration is stable.
            List<ExchangeMetadata> exchanges = new ArrayList<>(metadataManager.listExchanges(vhost));
            List<QueueMetadata> queues = new ArrayList<>(metadataManager.listQueues(vhost));

            // 2. Drop every exchange — includes the 5 pre-declared defaults.
            //    Built-in exchanges are protected against user-initiated deletes
            //    via ExchangeManager.deleteExchange, so we go directly through
            //    the metadata manager here. Bindings in each exchange are
            //    cascaded via BindingManager.removeAllForExchange.
            for (ExchangeMetadata ex : exchanges) {
                bindingManager.removeAllForExchange(ex.name());
                metadataManager.deleteExchange(vhost, ex.name());
            }

            // 3. Drop every queue in the vhost — metadata tombstones only.
            for (QueueMetadata q : queues) {
                bindingManager.removeAllForQueue(q.name());
                metadataManager.deleteQueue(vhost, q.name());
            }

            // 4. Evict the routing engine.
            routingEngines.remove(vhost);

            log.info("Deleted vhost '{}' ({} exchanges, {} queues cascaded)",
                vhost, exchanges.size(), queues.size());
        }
    }

    // ------------------------------------------------------------------
    // Lookup
    // ------------------------------------------------------------------

    /**
     * @return the per-vhost routing engine
     * @throws IllegalArgumentException if the vhost does not exist
     */
    public RoutingEngine getRoutingEngine(String vhost) {
        Objects.requireNonNull(vhost, "vhost");
        RoutingEngine engine = routingEngines.get(vhost);
        if (engine == null) {
            throw new IllegalArgumentException("Vhost not found: " + vhost);
        }
        return engine;
    }

    public boolean exists(String vhost) {
        if (vhost == null) return false;
        return routingEngines.containsKey(vhost);
    }

    /**
     * Resolves a vhost-scoped queue name to its backing Kafka topic.
     *
     * <p>Mapping:
     * <ul>
     *   <li>{@code "/"} + {@code "orders"} → {@code "ws.orders"}</li>
     *   <li>{@code "/production"} + {@code "orders"} → {@code "ws.production.orders"}</li>
     *   <li>{@code "production"} (no leading slash) is tolerated and maps the same way</li>
     * </ul>
     *
     * @throws NullPointerException if vhost or queueName is {@code null}
     */
    public String resolveTopicName(String vhost, String queueName) {
        Objects.requireNonNull(vhost, "vhost");
        Objects.requireNonNull(queueName, "queueName");
        if (DEFAULT_VHOST.equals(vhost)) {
            return "ws." + queueName;
        }
        String normalised = vhost.startsWith("/") ? vhost.substring(1) : vhost;
        return "ws." + normalised + "." + queueName;
    }

    /**
     * @return an unmodifiable list of {@link VhostInfo} records, one per
     *         registered vhost. Counts are snapshots taken at call time.
     */
    public List<VhostInfo> listVhosts() {
        List<VhostInfo> out = new ArrayList<>();
        for (String name : routingEngines.keySet()) {
            int exchanges = metadataManager.exchangeCount(name);
            int queues = metadataManager.queueCount(name);
            out.add(new VhostInfo(name, exchanges, queues));
        }
        return Collections.unmodifiableList(out);
    }

    /** @return the snapshot of currently-known vhost names. */
    public Collection<String> vhostNames() {
        return Collections.unmodifiableCollection(new ArrayList<>(routingEngines.keySet()));
    }

    // ------------------------------------------------------------------
    // Internal
    // ------------------------------------------------------------------

    /**
     * Builds a {@link RoutingEngine} that sees only the vhost's own metadata.
     * The closures capture {@code vhost} so there is no cross-vhost leakage.
     */
    private RoutingEngine newRoutingEngine(String vhost) {
        return new RoutingEngine(
            /* exchangeTypeFn */ exchangeName -> {
                ExchangeMetadata ex = metadataManager.getExchange(vhost, exchangeName);
                return ex == null ? null : ex.type();
            },
            /* bindingsFn */ exchangeName -> {
                List<Binding> bindings = new ArrayList<>();
                for (BindingMetadata bm : metadataManager.getBindings(vhost, exchangeName)) {
                    bindings.add(new Binding(bm.exchange(), bm.queue(), bm.routingKey(), bm.arguments()));
                }
                return bindings;
            },
            /* e2eBindingsFn */ exchangeName -> Collections.emptyList(),
            /* alternateExchangeFn */ exchangeName -> {
                ExchangeMetadata ex = metadataManager.getExchange(vhost, exchangeName);
                if (ex == null) return null;
                Map<String, String> args = ex.arguments();
                return args == null ? null : args.get("alternate-exchange");
            });
    }

    private Object lockFor(String vhost) {
        return vhostLocks.computeIfAbsent(vhost, v -> new Object());
    }

    // ------------------------------------------------------------------
    // Types
    // ------------------------------------------------------------------

    /**
     * Summary of one vhost: its name and current exchange/queue counts. Used
     * by {@code GET /v1/vhosts} to render the admin list.
     */
    public record VhostInfo(String name, int exchangeCount, int queueCount) {
        public VhostInfo {
            Objects.requireNonNull(name, "name");
        }
    }

    /**
     * Thrown by {@link #deleteVhost(String)} when the target vhost still has
     * active WebSocket connections. The REST handler maps this to HTTP 409
     * Conflict.
     */
    public static final class VhostInUseException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        public VhostInUseException(String vhost) {
            super("Vhost '" + vhost + "' has active connections and cannot be deleted");
        }
    }
}
