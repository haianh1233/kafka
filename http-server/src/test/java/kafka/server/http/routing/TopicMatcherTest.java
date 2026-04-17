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

// Time: Created - TASK-WS2.01

package kafka.server.http.routing;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link TopicMatcher}.
 *
 * <p>Covers exact matching, the {@code *} (one word) and {@code #} (zero or
 * more words) wildcards including mid-pattern {@code #}, empty-string edge
 * cases, case sensitivity, and the manual {@code splitByDot} helper.
 *
 * // Time: Created - TASK-WS2.01
 */
class TopicMatcherTest {

    // --- Exact match (no wildcards) ---

    @Test
    void exactMatch_identical() {
        assertTrue(TopicMatcher.matches("order.created", "order.created"));
    }

    @Test
    void exactMatch_different() {
        assertFalse(TopicMatcher.matches("order.created", "order.updated"));
    }

    @Test
    void exactMatch_differentLength_patternShorter() {
        assertFalse(TopicMatcher.matches("order", "order.created"));
    }

    @Test
    void exactMatch_differentLength_patternLonger() {
        assertFalse(TopicMatcher.matches("order.created", "order"));
    }

    @Test
    void exactMatch_singleWord() {
        assertTrue(TopicMatcher.matches("foo", "foo"));
        assertFalse(TopicMatcher.matches("foo", "bar"));
    }

    // --- Star wildcard (*) ---

    @Test
    void star_matchesOneWord() {
        assertTrue(TopicMatcher.matches("order.*", "order.created"));
        assertTrue(TopicMatcher.matches("order.*", "order.updated"));
    }

    @Test
    void star_doesNotMatchMultipleWords() {
        assertFalse(TopicMatcher.matches("order.*", "order.payment.created"));
    }

    @Test
    void star_doesNotMatchZeroWords() {
        assertFalse(TopicMatcher.matches("order.*", "order"));
    }

    @Test
    void star_atBeginning() {
        assertTrue(TopicMatcher.matches("*.created", "order.created"));
        assertTrue(TopicMatcher.matches("*.created", "payment.created"));
        assertFalse(TopicMatcher.matches("*.created", "order.payment.created"));
    }

    @Test
    void star_inMiddle() {
        assertTrue(TopicMatcher.matches("order.*.completed", "order.payment.completed"));
        assertFalse(TopicMatcher.matches("order.*.completed", "order.payment.step.completed"));
        assertFalse(TopicMatcher.matches("order.*.completed", "order.completed"));
    }

    @Test
    void star_alone_matchesOneWord() {
        assertTrue(TopicMatcher.matches("*", "foo"));
        assertFalse(TopicMatcher.matches("*", "foo.bar"));
    }

    @Test
    void star_alone_matchesEmptyStringAsOneEmptyWord() {
        // splitByDot("") returns one empty word, which * matches.
        assertTrue(TopicMatcher.matches("*", ""));
    }

    @Test
    void star_multipleStars_matchesExactlyThatManyWords() {
        assertTrue(TopicMatcher.matches("*.*.*", "a.b.c"));
        assertFalse(TopicMatcher.matches("*.*.*", "a.b"));
        assertFalse(TopicMatcher.matches("*.*.*", "a.b.c.d"));
    }

    // --- Hash wildcard (#) ---

    @Test
    void hash_alone_matchesEverything() {
        assertTrue(TopicMatcher.matches("#", ""));
        assertTrue(TopicMatcher.matches("#", "foo"));
        assertTrue(TopicMatcher.matches("#", "foo.bar"));
        assertTrue(TopicMatcher.matches("#", "foo.bar.baz"));
    }

    @Test
    void hash_atEnd_matchesZeroOrMoreWords() {
        assertTrue(TopicMatcher.matches("order.#", "order"));
        assertTrue(TopicMatcher.matches("order.#", "order.created"));
        assertTrue(TopicMatcher.matches("order.#", "order.payment.created"));
        assertFalse(TopicMatcher.matches("order.#", "payment.created"));
    }

    @Test
    void hash_atBeginning_matchesPrefix() {
        assertTrue(TopicMatcher.matches("#.foo", "foo"));
        assertTrue(TopicMatcher.matches("#.foo", "bar.foo"));
        assertTrue(TopicMatcher.matches("#.foo", "a.b.c.foo"));
        assertFalse(TopicMatcher.matches("#.foo", "foo.bar"));
    }

    @Test
    void hash_inMiddle_zeroWords() {
        assertTrue(TopicMatcher.matches("foo.#.bar", "foo.bar"));
    }

    @Test
    void hash_inMiddle_oneWord() {
        assertTrue(TopicMatcher.matches("foo.#.bar", "foo.x.bar"));
    }

    @Test
    void hash_inMiddle_manyWords() {
        assertTrue(TopicMatcher.matches("foo.#.bar", "foo.x.y.z.bar"));
    }

    @Test
    void hash_inMiddle_noTrailingAnchor_fails() {
        assertFalse(TopicMatcher.matches("foo.#.bar", "foo.x.baz"));
        assertFalse(TopicMatcher.matches("foo.#.bar", "foo"));
    }

    // --- Combined wildcards ---

    @Test
    void combined_starAndHash_trailing() {
        assertTrue(TopicMatcher.matches("*.*.#", "a.b"));
        assertTrue(TopicMatcher.matches("*.*.#", "a.b.c"));
        assertTrue(TopicMatcher.matches("*.*.#", "a.b.c.d.e"));
        assertFalse(TopicMatcher.matches("*.*.#", "a"));
    }

    @Test
    void combined_hashAndStar_leading() {
        assertTrue(TopicMatcher.matches("#.*", "a"));
        assertTrue(TopicMatcher.matches("#.*", "a.b"));
        assertTrue(TopicMatcher.matches("#.*", "a.b.c"));
    }

    // --- Design doc examples (§7.2) ---

    @ParameterizedTest
    @CsvSource({
        "order.#, order.created, true",
        "order.created, order.created, true",
        "*.created, order.created, true",
        "#, order.created, true",
        "*.created, order.payment.created, false",
        "#, payment.completed, true",
        "order.#, order.payment.created, true"
    })
    void designDocExamples(String pattern, String routingKey, boolean expected) {
        assertEquals(expected, TopicMatcher.matches(pattern, routingKey),
            () -> String.format("Pattern '%s' vs key '%s'", pattern, routingKey));
    }

    // --- Empty strings ---

    @Test
    void emptyPattern_matchesOnlyEmpty() {
        assertTrue(TopicMatcher.matches("", ""));
        assertFalse(TopicMatcher.matches("", "foo"));
    }

    @Test
    void emptyRoutingKey_matchedByHash() {
        assertTrue(TopicMatcher.matches("#", ""));
    }

    @Test
    void emptyRoutingKey_matchedByStar() {
        // splitByDot("") yields one empty word which * matches.
        assertTrue(TopicMatcher.matches("*", ""));
    }

    @Test
    void emptyRoutingKey_notMatchedByLiteral() {
        assertFalse(TopicMatcher.matches("foo", ""));
    }

    // --- Case sensitivity ---

    @Test
    void caseSensitive_literalWords() {
        assertFalse(TopicMatcher.matches("Order.Created", "order.created"));
        assertFalse(TopicMatcher.matches("order.created", "Order.Created"));
        assertTrue(TopicMatcher.matches("Order.Created", "Order.Created"));
    }

    @Test
    void caseSensitive_withWildcards() {
        assertFalse(TopicMatcher.matches("Order.*", "order.created"));
        assertTrue(TopicMatcher.matches("Order.*", "Order.Created"));
    }

    // --- splitByDot ---

    @Test
    void splitByDot_simple() {
        assertArrayEquals(new String[]{"a", "b", "c"}, TopicMatcher.splitByDot("a.b.c"));
    }

    @Test
    void splitByDot_singleWord() {
        assertArrayEquals(new String[]{"abc"}, TopicMatcher.splitByDot("abc"));
    }

    @Test
    void splitByDot_empty() {
        assertArrayEquals(new String[]{""}, TopicMatcher.splitByDot(""));
    }

    @Test
    void splitByDot_trailingDot() {
        assertArrayEquals(new String[]{"a", ""}, TopicMatcher.splitByDot("a."));
    }

    @Test
    void splitByDot_leadingDot() {
        assertArrayEquals(new String[]{"", "a"}, TopicMatcher.splitByDot(".a"));
    }

    @Test
    void splitByDot_consecutiveDots() {
        assertArrayEquals(new String[]{"a", "", "b"}, TopicMatcher.splitByDot("a..b"));
    }

    @Test
    void splitByDot_onlyDot() {
        assertArrayEquals(new String[]{"", ""}, TopicMatcher.splitByDot("."));
    }
}
