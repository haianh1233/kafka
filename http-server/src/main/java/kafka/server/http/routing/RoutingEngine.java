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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/**
 * Main routing entry point: resolves {@code (exchange, routingKey, headers)}
 * to a {@code Set<String>} of matched queue names.
 *
 * <p>Delegates to per-type matcher using the strategy pattern. Phase 1 supports
 * only the {@code direct} exchange type via {@link DirectMatcher}. The
 * {@code topic}, {@code fanout}, and {@code headers} types are stubbed and
 * throw {@link UnsupportedOperationException} — they are implemented by later
 * tasks (WS2.01–WS2.03).
 *
 * <p>Exchange-to-exchange (e2e) routing recurses into destination exchanges
 * whose e2e binding matches per the source exchange type's rules. For the
 * direct type, the e2e binding matches when its routing key equals the
 * message routing key. A {@link HashSet} of visited exchanges prevents
 * infinite loops when e2e bindings are cyclic.
 *
 * <p>Thread-safe: all data access is through caller-supplied functional
 * interfaces; the engine itself holds no mutable state.
 *
 * // Time: Created - TASK-WS1.08
 */
public final class RoutingEngine {

    private static final Logger log = LoggerFactory.getLogger(RoutingEngine.class);

    private final DirectMatcher directMatcher = new DirectMatcher();

    private final Function<String, String> exchangeTypeFn;
    private final Function<String, List<Binding>> bindingsFn;
    private final Function<String, List<E2EBinding>> e2eBindingsFn;

    /**
     * @param exchangeTypeFn returns exchange type string (e.g. {@code "direct"})
     *                       or {@code null} if the exchange does not exist
     * @param bindingsFn     returns queue bindings for an exchange (never {@code null})
     * @param e2eBindingsFn  returns exchange-to-exchange bindings for an exchange
     *                       (never {@code null})
     */
    public RoutingEngine(Function<String, String> exchangeTypeFn,
                         Function<String, List<Binding>> bindingsFn,
                         Function<String, List<E2EBinding>> e2eBindingsFn) {
        this.exchangeTypeFn = Objects.requireNonNull(exchangeTypeFn, "exchangeTypeFn");
        this.bindingsFn = Objects.requireNonNull(bindingsFn, "bindingsFn");
        this.e2eBindingsFn = Objects.requireNonNull(e2eBindingsFn, "e2eBindingsFn");
    }

    /**
     * Routes a message to matching queues.
     *
     * @param exchange   exchange name; must not be {@code null}
     * @param routingKey message routing key; must not be {@code null}
     * @param headers    message headers (may be {@code null}; ignored for direct type)
     * @return set of matched queue names (empty if no matches, never {@code null})
     * @throws IllegalArgumentException      if {@code exchange} does not exist
     * @throws UnsupportedOperationException if the exchange type is not
     *                                       supported in Phase 1
     */
    public Set<String> route(String exchange, String routingKey, Map<String, String> headers) {
        Objects.requireNonNull(exchange, "exchange");
        Objects.requireNonNull(routingKey, "routingKey");

        Set<String> matched = new HashSet<>();
        Set<String> visited = new HashSet<>();
        routeRecursive(exchange, routingKey, headers, matched, visited, true);
        return matched;
    }

    /**
     * Convenience overload with no headers.
     */
    public Set<String> route(String exchange, String routingKey) {
        return route(exchange, routingKey, null);
    }

    /**
     * Recursive routing with cycle detection.
     *
     * @param rootCall {@code true} when this call is the initial dispatch
     *                 (unknown exchange is an error); {@code false} when
     *                 reached via e2e recursion (a dangling destination is
     *                 silently ignored after a warning)
     */
    private void routeRecursive(String exchange, String routingKey,
                                Map<String, String> headers,
                                Set<String> matched, Set<String> visited,
                                boolean rootCall) {
        // Cycle guard: visited.add returns false if already present.
        if (!visited.add(exchange)) {
            return;
        }

        String type = resolveType(exchange, rootCall);
        if (type == null) {
            return;
        }

        if (!matchQueueBindings(exchange, type, routingKey, matched)) {
            return;
        }

        recurseE2E(exchange, type, routingKey, headers, matched, visited);
    }

    /**
     * Looks up the exchange type. Returns {@code null} when the exchange does
     * not exist and the recursion should stop without error. Throws when the
     * caller is the root dispatch.
     */
    private String resolveType(String exchange, boolean rootCall) {
        String type = exchangeTypeFn.apply(exchange);
        if (type != null) {
            return type;
        }
        if (rootCall) {
            throw new IllegalArgumentException("Exchange not found: " + exchange);
        }
        log.warn("E2E destination exchange '{}' does not exist; skipping", exchange);
        return null;
    }

    /**
     * Dispatches queue-binding matching to the type-specific matcher.
     *
     * @return {@code true} if routing should continue into e2e recursion,
     *         {@code false} if the exchange type is unknown (no e2e recursion)
     * @throws UnsupportedOperationException for Phase 1 stub types
     */
    private boolean matchQueueBindings(String exchange, String type, String routingKey,
                                       Set<String> matched) {
        List<Binding> bindings = bindingsFn.apply(exchange);
        if (bindings == null) {
            bindings = List.of();
        }
        switch (type) {
            case "direct":
                matched.addAll(directMatcher.match(bindings, routingKey));
                return true;
            case "topic":
                throw new UnsupportedOperationException(
                    "Topic exchange not supported in Phase 1 (see WS2.01): " + exchange);
            case "fanout":
                throw new UnsupportedOperationException(
                    "Fanout exchange not supported in Phase 1 (see WS2.02): " + exchange);
            case "headers":
                throw new UnsupportedOperationException(
                    "Headers exchange not supported in Phase 1 (see WS2.03): " + exchange);
            default:
                log.warn("Unknown exchange type '{}' for exchange '{}'; skipping", type, exchange);
                return false;
        }
    }

    /**
     * Traverses e2e bindings for the given source exchange and recurses into
     * each destination whose binding matches per the source type's rules.
     */
    private void recurseE2E(String exchange, String type, String routingKey,
                            Map<String, String> headers,
                            Set<String> matched, Set<String> visited) {
        List<E2EBinding> bindings = e2eBindingsFn.apply(exchange);
        if (bindings == null || bindings.isEmpty()) {
            return;
        }
        for (E2EBinding e2e : bindings) {
            if (e2eMatches(type, e2e, routingKey)) {
                routeRecursive(e2e.destination(), routingKey, headers, matched, visited, false);
            }
        }
    }

    /**
     * E2E binding match per source exchange type. Phase 1 only implements
     * direct; fanout/topic/headers e2e semantics are added in WS2.01–WS2.03.
     */
    private static boolean e2eMatches(String type, E2EBinding e2e, String routingKey) {
        if ("direct".equals(type)) {
            return e2e.routingKey().equals(routingKey);
        }
        return false;
    }
}
