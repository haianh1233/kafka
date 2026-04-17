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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Direct exchange matcher: exact string equality between the message routing
 * key and each binding's routing key.
 *
 * <p>Builds a {@code HashMap<String, List<String>>} index from the supplied
 * bindings on every call for O(1) lookup from routing key to queue names. The
 * index is deliberately rebuilt per-call in Phase 1 for simplicity; later
 * phases may cache it with invalidation on binding mutations (see design doc
 * §6.1).
 *
 * <p>Thread-safe: stateless; safe for concurrent invocation.
 *
 * // Time: Created - TASK-WS1.08
 */
public final class DirectMatcher {

    /**
     * Matches bindings against a routing key using exact string equality.
     *
     * @param bindings   bindings for the exchange (from {@code BindingManager.listByExchange});
     *                   must not be {@code null}
     * @param routingKey the message routing key; must not be {@code null}
     * @return set of queue names whose binding routing key equals the message
     *         routing key (empty if none, never {@code null})
     */
    public Set<String> match(List<Binding> bindings, String routingKey) {
        Objects.requireNonNull(bindings, "bindings");
        Objects.requireNonNull(routingKey, "routingKey");

        if (bindings.isEmpty()) {
            return Set.of();
        }

        // Build routingKey -> queues index (Phase 1: per-call; later phases may cache).
        Map<String, List<String>> index = new HashMap<>();
        for (Binding b : bindings) {
            index.computeIfAbsent(b.routingKey(), k -> new ArrayList<>()).add(b.queue());
        }

        List<String> queues = index.get(routingKey);
        if (queues == null || queues.isEmpty()) {
            return Set.of();
        }
        return new HashSet<>(queues);
    }
}
