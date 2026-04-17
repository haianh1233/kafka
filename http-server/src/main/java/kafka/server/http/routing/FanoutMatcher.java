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

// Time: Created - TASK-WS2.02

package kafka.server.http.routing;

import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Fanout exchange matcher — returns all bound queues, routing key ignored.
 *
 * <p>Fanout is the simplest AMQP exchange type: every queue bound to the
 * exchange receives every message, regardless of the message routing key or
 * headers. This matches the ivy-ref {@code Amqp091RoutingEngine} pattern:
 *
 * <pre>
 * case "fanout" -&gt; {
 *     for (Binding b : routing.bindings) {
 *         matched.add(b.queue());
 *     }
 * }
 * </pre>
 *
 * <p>The caller is responsible for pulling {@code queue()} off each
 * {@link Binding} before invoking {@link #match(Collection, String)} so this
 * class can be reused wherever a flat queue-name list is already available
 * (e.g. a cached fanout binding index).
 *
 * <p>Stateless utility. O(K) where K = number of bound queues. Thread-safe.
 *
 * // Time: Created - TASK-WS2.02
 */
public final class FanoutMatcher {

    private FanoutMatcher() {
        // utility class — no instances
    }

    /**
     * Returns every bound queue name as a deduplicated {@link Set}. Routing
     * key is accepted for interface consistency with other matchers and is
     * ignored per AMQP fanout semantics.
     *
     * @param boundQueues queue names bound to the fanout exchange; must not be
     *                    {@code null} (may be empty; duplicates are collapsed)
     * @param routingKey  ignored (may be {@code null})
     * @return set of bound queue names (empty if {@code boundQueues} is empty,
     *         never {@code null})
     */
    public static Set<String> match(Collection<String> boundQueues, String routingKey) {
        Objects.requireNonNull(boundQueues, "boundQueues");
        if (boundQueues.isEmpty()) {
            return Set.of();
        }
        return new HashSet<>(boundQueues);
    }
}
