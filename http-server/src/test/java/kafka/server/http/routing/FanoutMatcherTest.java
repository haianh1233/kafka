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

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link FanoutMatcher}.
 *
 * <p>Fanout semantics: all bound queues match regardless of routing key or
 * headers. The matcher takes a pre-extracted {@code Collection<String>} of
 * queue names so the caller is responsible for pulling {@code queue()} off
 * each {@link Binding} (mirrors the ivy-ref {@code Amqp091RoutingEngine}
 * fanout loop).
 *
 * // Time: Created - TASK-WS2.02
 */
class FanoutMatcherTest {

    @Test
    void match_emptyBindings_returnsEmpty() {
        Set<String> result = FanoutMatcher.match(List.of(), "anything");
        assertTrue(result.isEmpty(), "empty bindings must yield empty set");
    }

    @Test
    void match_singleBinding_returnsThatQueue() {
        Set<String> result = FanoutMatcher.match(List.of("only-queue"), "ignored");
        assertEquals(Set.of("only-queue"), result);
    }

    @Test
    void match_multipleBindings_returnsAllQueues() {
        Set<String> result = FanoutMatcher.match(
            List.of("service-a", "service-b", "service-c"), "any.routing.key");
        assertEquals(Set.of("service-a", "service-b", "service-c"), result);
    }

    @Test
    void match_duplicateBindings_deduplicates() {
        // Simulates multiple bindings for the same queue with different routing
        // keys — the caller extracts queue() from each Binding, so duplicates
        // appear; HashSet must collapse them to a single entry.
        Set<String> result = FanoutMatcher.match(List.of("q1", "q1", "q2"), "key");
        assertEquals(Set.of("q1", "q2"), result);
    }

    @Test
    void match_routingKeyIgnored_sameResultAcrossKeys() {
        Set<String> result1 = FanoutMatcher.match(List.of("q1", "q2"), "alpha");
        Set<String> result2 = FanoutMatcher.match(List.of("q1", "q2"), "beta");
        Set<String> result3 = FanoutMatcher.match(List.of("q1", "q2"), "");
        assertEquals(result1, result2);
        assertEquals(result2, result3);
    }

    @Test
    void match_nullRoutingKey_works() {
        // Fanout ignores routing key entirely, so null must not throw.
        Set<String> result = FanoutMatcher.match(List.of("q1"), null);
        assertEquals(Set.of("q1"), result);
    }

    @Test
    void match_extractedFromBindings_headersIgnored() {
        // Headers live on Binding.arguments() — demonstrate that the caller
        // pattern (extract queue() only) produces the expected fanout result
        // regardless of what headers/arguments each binding carries.
        Binding b1 = new Binding("ex", "q1", "rk-1", Map.of("x-match", "all"));
        Binding b2 = new Binding("ex", "q2", "rk-2", Map.of("foo", "bar"));
        Binding b3 = new Binding("ex", "q1", "rk-3", Map.of());
        List<String> queues = List.of(b1.queue(), b2.queue(), b3.queue());

        Set<String> result = FanoutMatcher.match(queues, "ignored");
        assertEquals(Set.of("q1", "q2"), result);
    }
}
