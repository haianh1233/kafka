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

// Time: Update - TASK-WS2.04 - added e2e recursion + alternate exchange fallback
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
 * <p>Delegates to per-type matcher using the strategy pattern. All four AMQP
 * exchange types are supported:
 * <ul>
 *   <li>{@code direct}  — exact routing-key equality ({@link DirectMatcher})</li>
 *   <li>{@code topic}   — dot-separated {@code *} / {@code #} wildcards
 *       ({@link TopicMatcher})</li>
 *   <li>{@code fanout}  — all bound queues ({@link FanoutMatcher})</li>
 *   <li>{@code headers} — match arguments against message headers
 *       ({@link HeadersMatcher})</li>
 * </ul>
 *
 * <p>Exchange-to-exchange (e2e) routing recurses into destination exchanges
 * whose e2e binding matches per the source exchange type's rules. A
 * {@link HashSet} of visited exchanges prevents infinite loops when e2e
 * bindings are cyclic.
 *
 * <p><b>Alternate exchange fallback</b> (WS2.04): when an exchange produces
 * <i>no</i> matches from either its queue bindings or e2e bindings, and an
 * alternate exchange is configured (looked up via the caller-supplied
 * {@code alternateExchangeFn}), routing recurses into that alternate
 * exchange. The alternate exchange is NOT used when any binding matched.
 * The same visited set guards against alternate-exchange cycles.
 *
 * <p>Thread-safe: all data access is through caller-supplied functional
 * interfaces; the engine itself holds no mutable state.
 *
 * // Time: Created - TASK-WS1.08
 * // Time: Update - TASK-WS2.04
 */
public final class RoutingEngine {

    private static final Logger log = LoggerFactory.getLogger(RoutingEngine.class);

    private final DirectMatcher directMatcher = new DirectMatcher();

    private final Function<String, String> exchangeTypeFn;
    private final Function<String, List<Binding>> bindingsFn;
    private final Function<String, List<E2EBinding>> e2eBindingsFn;
    private final Function<String, String> alternateExchangeFn;

    /**
     * Legacy constructor (pre-WS2.04). No alternate exchange support — every
     * exchange's alternate exchange resolves to {@code null}.
     *
     * <p>Prefer {@link #RoutingEngine(Function, Function, Function, Function)}
     * in new code.
     *
     * @param exchangeTypeFn returns exchange type string (e.g. {@code "direct"})
     *                       or {@code null} if the exchange does not exist
     * @param bindingsFn     returns queue bindings for an exchange (never {@code null})
     * @param e2eBindingsFn  returns exchange-to-exchange bindings for an exchange
     *                       (never {@code null})
     */
    public RoutingEngine(Function<String, String> exchangeTypeFn,
                         Function<String, List<Binding>> bindingsFn,
                         Function<String, List<E2EBinding>> e2eBindingsFn) {
        this(exchangeTypeFn, bindingsFn, e2eBindingsFn, ex -> null);
    }

    /**
     * Full constructor with alternate exchange lookup (WS2.04).
     *
     * @param exchangeTypeFn      returns exchange type or {@code null} if the
     *                            exchange does not exist
     * @param bindingsFn          returns queue bindings for an exchange
     *                            (never {@code null})
     * @param e2eBindingsFn       returns e2e bindings for an exchange
     *                            (never {@code null})
     * @param alternateExchangeFn returns the alternate exchange name for a
     *                            given exchange, or {@code null} if none;
     *                            invoked once per routed exchange during
     *                            fallback
     */
    public RoutingEngine(Function<String, String> exchangeTypeFn,
                         Function<String, List<Binding>> bindingsFn,
                         Function<String, List<E2EBinding>> e2eBindingsFn,
                         Function<String, String> alternateExchangeFn) {
        this.exchangeTypeFn = Objects.requireNonNull(exchangeTypeFn, "exchangeTypeFn");
        this.bindingsFn = Objects.requireNonNull(bindingsFn, "bindingsFn");
        this.e2eBindingsFn = Objects.requireNonNull(e2eBindingsFn, "e2eBindingsFn");
        this.alternateExchangeFn = Objects.requireNonNull(alternateExchangeFn, "alternateExchangeFn");
    }

    /**
     * Routes a message to matching queues.
     *
     * @param exchange   exchange name; must not be {@code null}
     * @param routingKey message routing key; must not be {@code null}
     * @param headers    message headers (may be {@code null}; ignored for
     *                   direct/topic/fanout types)
     * @return set of matched queue names (empty if no matches, never {@code null})
     * @throws IllegalArgumentException if {@code exchange} does not exist
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
     * Recursive routing with cycle detection and alternate-exchange fallback.
     *
     * <p>Algorithm:
     * <ol>
     *   <li>Cycle guard: if the exchange has already been visited, return.</li>
     *   <li>Resolve exchange type. Unknown + root call → throw; unknown +
     *       recursive call → silently skip.</li>
     *   <li>Match queue bindings for this exchange.</li>
     *   <li>Recurse into e2e destinations whose binding key matches.</li>
     *   <li>If this exchange produced <i>no</i> new matches (queue + e2e
     *       combined) and an alternate exchange is configured, recurse into
     *       the alternate exchange.</li>
     * </ol>
     *
     * @param rootCall {@code true} when this call is the initial dispatch
     *                 (unknown exchange is an error); {@code false} when
     *                 reached via e2e or alternate-exchange recursion (a
     *                 dangling destination is silently ignored after a warning)
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

        // Track how many queues were matched *before* this exchange ran so
        // that, after queue matching + e2e recursion, we can tell whether
        // this exchange contributed anything. If it did not, the alternate
        // exchange (if any) gets a shot.
        int matchedBefore = matched.size();

        if (!matchQueueBindings(exchange, type, routingKey, headers, matched)) {
            return;
        }

        recurseE2E(exchange, type, routingKey, headers, matched, visited);

        // Alternate exchange fallback: only if this exchange produced no
        // matches from its own bindings or e2e recursion.
        if (matched.size() == matchedBefore) {
            String alternate = alternateExchangeFn.apply(exchange);
            if (alternate != null && !alternate.isEmpty()) {
                log.debug("Exchange '{}' produced no matches; falling back to alternate '{}'",
                    exchange, alternate);
                routeRecursive(alternate, routingKey, headers, matched, visited, false);
            }
        }
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
        log.warn("Recursive destination exchange '{}' does not exist; skipping", exchange);
        return null;
    }

    /**
     * Dispatches queue-binding matching to the type-specific matcher.
     *
     * @return {@code true} if routing should continue into e2e recursion,
     *         {@code false} if the exchange type is unknown (no e2e recursion)
     */
    private boolean matchQueueBindings(String exchange, String type, String routingKey,
                                       Map<String, String> headers, Set<String> matched) {
        List<Binding> bindings = bindingsFn.apply(exchange);
        if (bindings == null) {
            bindings = List.of();
        }
        switch (type) {
            case "direct":
                matched.addAll(directMatcher.match(bindings, routingKey));
                return true;
            case "topic":
                for (Binding b : bindings) {
                    if (TopicMatcher.matches(b.routingKey(), routingKey)) {
                        matched.add(b.queue());
                    }
                }
                return true;
            case "fanout":
                for (Binding b : bindings) {
                    matched.add(b.queue());
                }
                return true;
            case "headers":
                for (Binding b : bindings) {
                    if (HeadersMatcher.matches(b.arguments(), headers)) {
                        matched.add(b.queue());
                    }
                }
                return true;
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
            if (e2eMatches(type, e2e, routingKey, headers)) {
                routeRecursive(e2e.destination(), routingKey, headers, matched, visited, false);
            }
        }
    }

    /**
     * E2E binding match per source exchange type. The binding key is matched
     * against the message routing key using the SOURCE exchange's type rules
     * (per ivy-ref {@code Amqp091RoutingEngine}):
     * <ul>
     *   <li>direct  — exact equality</li>
     *   <li>topic   — wildcard match via {@link TopicMatcher}</li>
     *   <li>fanout  — always matches</li>
     *   <li>headers — binding arguments matched against message headers via
     *       {@link HeadersMatcher}</li>
     * </ul>
     */
    private static boolean e2eMatches(String type, E2EBinding e2e, String routingKey,
                                      Map<String, String> headers) {
        switch (type) {
            case "direct":
                return e2e.routingKey().equals(routingKey);
            case "topic":
                return TopicMatcher.matches(e2e.routingKey(), routingKey);
            case "fanout":
                return true;
            case "headers":
                return HeadersMatcher.matches(e2e.arguments(), headers);
            default:
                return false;
        }
    }
}
