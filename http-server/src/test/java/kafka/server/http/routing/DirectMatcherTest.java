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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link DirectMatcher}.
 *
 * // Time: Created - TASK-WS1.08
 */
class DirectMatcherTest {

    private DirectMatcher matcher;

    @BeforeEach
    void setUp() {
        matcher = new DirectMatcher();
    }

    @Test
    void match_exactKey_returnsQueue() {
        var bindings = List.of(
            new Binding("ex", "order-events", "order.created", Map.of()),
            new Binding("ex", "payment-events", "payment.completed", Map.of())
        );
        Set<String> result = matcher.match(bindings, "order.created");
        assertEquals(Set.of("order-events"), result);
    }

    @Test
    void match_noMatch_returnsEmpty() {
        var bindings = List.of(
            new Binding("ex", "order-events", "order.created", Map.of())
        );
        Set<String> result = matcher.match(bindings, "order.updated");
        assertTrue(result.isEmpty());
    }

    @Test
    void match_multipleQueuesSameKey_returnsAll() {
        var bindings = List.of(
            new Binding("ex", "queue-a", "order.created", Map.of()),
            new Binding("ex", "queue-b", "order.created", Map.of())
        );
        Set<String> result = matcher.match(bindings, "order.created");
        assertEquals(Set.of("queue-a", "queue-b"), result);
    }

    @Test
    void match_multipleBindings_onlyMatchingKeyReturned() {
        var bindings = List.of(
            new Binding("ex", "order-events", "order.created", Map.of()),
            new Binding("ex", "payment-events", "payment.completed", Map.of()),
            new Binding("ex", "shipping-events", "order.shipped", Map.of())
        );
        Set<String> result = matcher.match(bindings, "payment.completed");
        assertEquals(Set.of("payment-events"), result);
    }

    @Test
    void match_emptyBindings_returnsEmpty() {
        Set<String> result = matcher.match(List.of(), "any.key");
        assertTrue(result.isEmpty());
    }

    @Test
    void match_emptyRoutingKey_matchesEmptyBinding() {
        var bindings = List.of(
            new Binding("ex", "default-queue", "", Map.of())
        );
        Set<String> result = matcher.match(bindings, "");
        assertEquals(Set.of("default-queue"), result);
    }

    @Test
    void match_caseSensitive_noMatch() {
        var bindings = List.of(
            new Binding("ex", "queue", "Order.Created", Map.of())
        );
        Set<String> result = matcher.match(bindings, "order.created");
        assertTrue(result.isEmpty());
    }

    @Test
    void match_nullBindings_throws() {
        assertThrows(NullPointerException.class, () -> matcher.match(null, "key"));
    }

    @Test
    void match_nullRoutingKey_throws() {
        assertThrows(NullPointerException.class,
            () -> matcher.match(List.of(new Binding("ex", "q", "k", Map.of())), null));
    }

    @Test
    void match_duplicateBindings_deduplicatedInSet() {
        var bindings = List.of(
            new Binding("ex", "q", "k", Map.of()),
            new Binding("ex", "q", "k", Map.of())
        );
        Set<String> result = matcher.match(bindings, "k");
        assertEquals(Set.of("q"), result);
    }
}
