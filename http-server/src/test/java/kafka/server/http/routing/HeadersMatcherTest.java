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

// Time: Created - TASK-WS2.03

package kafka.server.http.routing;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link HeadersMatcher}.
 *
 * <p>Covers x-match=all, x-match=any, default mode, empty criteria, null inputs,
 * and design doc §7.4 examples.
 */
class HeadersMatcherTest {

    // --- x-match: all ---

    @Test
    void matchAll_allCriteriaMatch() {
        assertTrue(HeadersMatcher.matches(
            Map.of("x-match", "all", "priority", "high", "region", "us-east"),
            Map.of("priority", "high", "region", "us-east")));
    }

    @Test
    void matchAll_notAllCriteriaMatch() {
        assertFalse(HeadersMatcher.matches(
            Map.of("x-match", "all", "priority", "high", "region", "us-east"),
            Map.of("priority", "high", "region", "eu-west")));
    }

    @Test
    void matchAll_extraHeadersIgnored() {
        assertTrue(HeadersMatcher.matches(
            Map.of("x-match", "all", "priority", "high"),
            Map.of("priority", "high", "region", "us-east", "source", "web")));
    }

    @Test
    void matchAll_missingHeader() {
        assertFalse(HeadersMatcher.matches(
            Map.of("x-match", "all", "priority", "high", "region", "us-east"),
            Map.of("priority", "high")));
    }

    // --- x-match: any ---

    @Test
    void matchAny_oneCriterionMatches() {
        assertTrue(HeadersMatcher.matches(
            Map.of("x-match", "any", "priority", "high", "region", "eu-west"),
            Map.of("priority", "low", "region", "eu-west")));
    }

    @Test
    void matchAny_noCriteriaMatch() {
        assertFalse(HeadersMatcher.matches(
            Map.of("x-match", "any", "priority", "high", "region", "eu-west"),
            Map.of("priority", "low", "region", "us-east")));
    }

    @Test
    void matchAny_allCriteriaMatch() {
        assertTrue(HeadersMatcher.matches(
            Map.of("x-match", "any", "priority", "high", "region", "eu-west"),
            Map.of("priority", "high", "region", "eu-west")));
    }

    // --- Default x-match ---

    @Test
    void defaultXMatch_isAll_positive() {
        // Missing x-match key → defaults to "all"
        assertTrue(HeadersMatcher.matches(
            Map.of("priority", "high"),
            Map.of("priority", "high")));
    }

    @Test
    void defaultXMatch_isAll_negative() {
        assertFalse(HeadersMatcher.matches(
            Map.of("priority", "high", "region", "us"),
            Map.of("priority", "high")));
    }

    // --- Empty / null edge cases ---

    @Test
    void emptyCriteria_matchesEverything() {
        assertTrue(HeadersMatcher.matches(
            Map.of("x-match", "all"),
            Map.of("anything", "value")));
    }

    @Test
    void emptyCriteriaAny_matchesEverything() {
        assertTrue(HeadersMatcher.matches(
            Map.of("x-match", "any"),
            Map.of("anything", "value")));
    }

    @Test
    void nullBindingArgs_matchesEverything() {
        assertTrue(HeadersMatcher.matches(null, Map.of("key", "value")));
    }

    @Test
    void emptyBindingArgs_matchesEverything() {
        assertTrue(HeadersMatcher.matches(Map.of(), Map.of("key", "value")));
    }

    @Test
    void nullMsgHeaders_noMatch() {
        assertFalse(HeadersMatcher.matches(
            Map.of("x-match", "all", "priority", "high"),
            null));
    }

    @Test
    void nullMsgHeadersWithEmptyCriteria_matches() {
        // No criteria → matches everything, even when headers are null
        assertTrue(HeadersMatcher.matches(
            Map.of("x-match", "all"),
            null));
    }

    @Test
    void emptyMsgHeaders_noMatch() {
        assertFalse(HeadersMatcher.matches(
            Map.of("x-match", "all", "priority", "high"),
            Map.of()));
    }

    // --- Value mismatch edge cases ---

    @Test
    void matchAll_valueMismatch() {
        // Key present in headers but value differs → no match
        assertFalse(HeadersMatcher.matches(
            Map.of("x-match", "all", "priority", "high"),
            Map.of("priority", "low")));
    }

    @Test
    void matchAny_valueMismatchOnAll() {
        assertFalse(HeadersMatcher.matches(
            Map.of("x-match", "any", "priority", "high", "region", "us"),
            Map.of("priority", "low", "region", "eu")));
    }

    // --- Design doc §7.4 examples ---

    @Test
    void designDocExample_priorityUs() {
        // Binding: x-match=all, priority=high, region=us-east
        // Message: priority=high, region=us-east → matches
        assertTrue(HeadersMatcher.matches(
            Map.of("x-match", "all", "priority", "high", "region", "us-east"),
            Map.of("priority", "high", "region", "us-east")));
    }

    @Test
    void designDocExample_anyHigh() {
        // Binding: x-match=any, priority=high, region=eu-west
        // Message: priority=high, region=us-east → matches (priority matches)
        assertTrue(HeadersMatcher.matches(
            Map.of("x-match", "any", "priority", "high", "region", "eu-west"),
            Map.of("priority", "high", "region", "us-east")));
    }

    @Test
    void designDocExample_lowPriority_anyMatch() {
        // Binding: x-match=any, priority=high, region=eu-west
        // Message: priority=low, region=eu-west → matches (region matches)
        assertTrue(HeadersMatcher.matches(
            Map.of("x-match", "any", "priority", "high", "region", "eu-west"),
            Map.of("priority", "low", "region", "eu-west")));
    }

    // --- Routing key is ignored (headers exchange semantics) ---

    @Test
    void routingKeyIgnored_matchesByHeadersOnly() {
        // The matches() API accepts only headers, so routing key is inherently ignored.
        // This test is a sanity check that header-only inputs are the only criterion.
        Map<String, String> args = Map.of("x-match", "all", "priority", "high");
        assertTrue(HeadersMatcher.matches(args, Map.of("priority", "high")));
        assertTrue(HeadersMatcher.matches(args, Map.of("priority", "high", "routing-key", "foo.bar")));
    }

    // --- Multiple bindings evaluated independently ---

    @Test
    void multipleBindings_evaluatedIndependently() {
        // Simulate a caller evaluating several bindings against the same message headers.
        Map<String, String> msgHeaders = Map.of("priority", "high", "region", "us-east");

        Map<String, String> bindingA = Map.of("x-match", "all", "priority", "high", "region", "us-east");
        Map<String, String> bindingB = Map.of("x-match", "any", "priority", "low", "region", "us-east");
        Map<String, String> bindingC = Map.of("x-match", "all", "priority", "low");
        Map<String, String> bindingD = Map.of("x-match", "any", "priority", "low", "region", "eu");

        assertTrue(HeadersMatcher.matches(bindingA, msgHeaders));  // all match
        assertTrue(HeadersMatcher.matches(bindingB, msgHeaders));  // region matches (any)
        assertFalse(HeadersMatcher.matches(bindingC, msgHeaders)); // priority mismatch
        assertFalse(HeadersMatcher.matches(bindingD, msgHeaders)); // neither matches (any)
    }

    // --- Mutable map inputs (defensive) ---

    @Test
    void mutableMapInputs_work() {
        Map<String, String> args = new HashMap<>();
        args.put("x-match", "all");
        args.put("priority", "high");

        Map<String, String> headers = new HashMap<>();
        headers.put("priority", "high");

        assertTrue(HeadersMatcher.matches(args, headers));
    }
}
