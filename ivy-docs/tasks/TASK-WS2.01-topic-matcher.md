# TASK-WS2.01: TopicMatcher — Wildcard Pattern Matching for Topic Exchanges

## Prerequisites

| Task | What it delivers | Why this task needs it |
|------|------------------|-----------------------|
| None | — | Standalone matching algorithm with no upstream dependencies |

---

## Context

The topic exchange type uses **dot-separated word matching** with two wildcards: `*` (matches exactly one word) and `#` (matches zero or more words). This is the most complex of the four exchange types and the most performance-sensitive — it runs on every publish to a topic-type exchange.

From **design doc §7.2**:

> **Algorithm:** Dot-separated word matching with `*` (one word) and `#` (zero or more words).
>
> ```
> Exchange "events" (type=topic):
>   Binding: queue="all-orders",    routingKey="order.#"
>   Binding: queue="order-created", routingKey="order.created"
>   Binding: queue="any-created",   routingKey="*.created"
>   Binding: queue="everything",    routingKey="#"
>
> Publish: routingKey="order.created"
>   → matches ["all-orders", "order-created", "any-created", "everything"]
> ```

The ivy-ref `Amqp091RoutingEngine.topicMatches()` (lines 315-353) provides the established implementation pattern. It uses manual dot-split (no regex in hot path) and a word-by-word walk.

**Edge cases from design doc §7.2:**
- `"*"` alone matches exactly one word: `"foo"` matches, `"foo.bar"` does not, `""` matches (empty string is one empty word)
- `"#"` alone matches everything including empty routing key `""`
- Empty pattern `""` matches only empty routing key `""` (exact match, no wildcards)
- `"#.foo"` matches `"foo"` and `"bar.foo"` and `"a.b.c.foo"`
- `"foo.#.bar"` matches `"foo.bar"` and `"foo.x.bar"` and `"foo.x.y.z.bar"`

**Optimization:** For exchanges with many bindings, a pre-compiled trie structure indexed by the first word reduces per-publish scan. The trie is rebuilt on binding changes (rare).

---

## Specification

```java
package kafka.server.http.routing;

/**
 * AMQP topic pattern matcher — dot-separated words with * and # wildcards.
 *
 * Stateless utility class. All methods are static and thread-safe.
 */
public final class TopicMatcher {

    /**
     * Tests whether a binding pattern matches a routing key.
     *
     * @param pattern    the binding pattern (may contain * and # wildcards, dot-separated)
     * @param routingKey the published routing key (no wildcards, dot-separated)
     * @return true if the routing key matches the pattern
     */
    public static boolean matches(String pattern, String routingKey);

    /**
     * Splits a string by dots without regex. Manual dot-split for hot path performance.
     *
     * @param s the string to split
     * @return array of dot-separated words
     */
    static String[] splitByDot(String s);
}
```

---

## Implementation Details

**Module:** `http-server`

**Files to study:**

| File | Why |
|------|-----|
| `references/ivy-ref/ivy-server/src/main/java/com/ivy/server/handler/amqp091/Amqp091RoutingEngine.java` lines 315-380 | topicMatches + splitByDot — the exact algorithm to adapt |
| `ivy-docs/http-protocol-extend-design.md` §7.2 | Edge cases and optimization notes |

```java
// From Amqp091RoutingEngine.java lines 315-380 — topicMatches + splitByDot:
static boolean topicMatches(String pattern, String routingKey) {
    // Fast path: no wildcards — exact string comparison
    if (pattern.indexOf('*') < 0 && pattern.indexOf('#') < 0) {
        return pattern.equals(routingKey);
    }

    String[] patternWords = splitByDot(pattern);
    String[] routingWords = splitByDot(routingKey);

    int minLen = Math.min(patternWords.length, routingWords.length);

    for (int i = 0; i < minLen; i++) {
        String pw = patternWords[i];
        if ("#".equals(pw)) {
            return true;
        }
        if ("*".equals(pw)) {
            continue;
        }
        if (!pw.equals(routingWords[i])) {
            return false;
        }
    }

    if (patternWords.length == routingWords.length) {
        return true;
    }

    // Pattern has one extra word and it's # — matches zero remaining routing words
    if (patternWords.length == routingWords.length + 1
            && "#".equals(patternWords[patternWords.length - 1])) {
        return true;
    }

    return false;
}

private static String[] splitByDot(String s) {
    int count = 1;
    for (int i = 0; i < s.length(); i++) {
        if (s.charAt(i) == '.') count++;
    }
    String[] parts = new String[count];
    int partIndex = 0;
    int start = 0;
    for (int i = 0; i < s.length(); i++) {
        if (s.charAt(i) == '.') {
            parts[partIndex++] = s.substring(start, i);
            start = i + 1;
        }
    }
    parts[partIndex] = s.substring(start);
    return parts;
}
```

**Files to create:**

| File | What it does |
|------|--------------|
| `http-server/src/main/java/kafka/server/http/routing/TopicMatcher.java` | Wildcard pattern matching for topic exchanges |
| `http-server/src/test/java/kafka/server/http/routing/TopicMatcherTest.java` | Comprehensive edge case tests |

**Files to modify:**

None.

> **CRITICAL:** The ivy-ref algorithm handles `#` only at the END of a pattern or as the last remaining pattern word. The design doc specifies that `#` can also appear in the MIDDLE (`foo.#.bar`). The design doc §7.2 includes a recursive algorithm for this case. The ivy-ref implementation does NOT handle mid-pattern `#` correctly. You must use the design doc algorithm, not the ivy-ref shortcut.

> **CRITICAL:** Manual dot-split (no `String.split()` or regex) per ivy-ref pattern and design doc §8.5. The `splitByDot` method from ivy-ref is the correct approach.

> **EDGE CASE:** Empty strings. `splitByDot("")` returns `[""]` (one empty word). `"*"` matches `""` because `""` is one empty word.

**Implementation order:**
1. Implement `splitByDot()` — manual, no regex
2. Implement `matches()` with the recursive algorithm from design doc §7.2 (handles mid-pattern `#`)
3. Fast path: if no wildcards, use exact string comparison
4. Write comprehensive edge case tests

---

## Skeleton Code

### Production class

```java
package kafka.server.http.routing;

/**
 * AMQP topic pattern matcher — dot-separated words with * and # wildcards.
 *
 * // Time: Created - TASK-WS2.01
 */
public final class TopicMatcher {

    private TopicMatcher() {} // utility class

    /**
     * Tests whether a binding pattern matches a routing key.
     */
    public static boolean matches(String pattern, String routingKey) {
        // Fast path: no wildcards — exact string comparison
        if (pattern.indexOf('*') < 0 && pattern.indexOf('#') < 0) {
            return pattern.equals(routingKey);
        }

        String[] patternWords = splitByDot(pattern);
        String[] routingWords = splitByDot(routingKey);

        return matchWords(patternWords, 0, routingWords, 0);
    }

    /**
     * Recursive word-by-word matching. Handles # in any position.
     */
    private static boolean matchWords(String[] pattern, int pi, String[] routing, int ri) {
        while (pi < pattern.length) {
            String pw = pattern[pi];

            if ("#".equals(pw)) {
                // # at end of pattern matches everything remaining
                if (pi == pattern.length - 1) return true;

                // # in middle: try matching rest of pattern against remaining routing words
                // Try consuming 0, 1, 2, ... routing words with #
                for (int skip = ri; skip <= routing.length; skip++) {
                    if (matchWords(pattern, pi + 1, routing, skip)) return true;
                }
                return false;
            }

            // No more routing words but pattern has more non-# words
            if (ri >= routing.length) return false;

            if ("*".equals(pw)) {
                // * matches exactly one word — just advance both
                pi++;
                ri++;
                continue;
            }

            // Literal word — must match exactly
            if (!pw.equals(routing[ri])) return false;
            pi++;
            ri++;
        }

        // Pattern exhausted — routing must also be exhausted
        return ri == routing.length;
    }

    /**
     * Splits a string by dots without regex. Manual split for hot path performance.
     */
    static String[] splitByDot(String s) {
        int count = 1;
        for (int i = 0; i < s.length(); i++) {
            if (s.charAt(i) == '.') count++;
        }

        String[] parts = new String[count];
        int partIndex = 0;
        int start = 0;

        for (int i = 0; i < s.length(); i++) {
            if (s.charAt(i) == '.') {
                parts[partIndex++] = s.substring(start, i);
                start = i + 1;
            }
        }
        parts[partIndex] = s.substring(start);

        return parts;
    }
}
```

### Test class

```java
package kafka.server.http.routing;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.*;

/**
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
    void exactMatch_differentLength() {
        assertFalse(TopicMatcher.matches("order", "order.created"));
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
    void star_atBeginning() {
        assertTrue(TopicMatcher.matches("*.created", "order.created"));
        assertTrue(TopicMatcher.matches("*.created", "payment.created"));
    }

    @Test
    void star_inMiddle() {
        assertTrue(TopicMatcher.matches("order.*.completed", "order.payment.completed"));
        assertFalse(TopicMatcher.matches("order.*.completed", "order.payment.step.completed"));
    }

    @Test
    void star_alone_matchesOneWord() {
        assertTrue(TopicMatcher.matches("*", "foo"));
        assertTrue(TopicMatcher.matches("*", ""));  // empty string is one empty word
        assertFalse(TopicMatcher.matches("*", "foo.bar"));
    }

    // --- Hash wildcard (#) ---

    @Test
    void hash_atEnd_matchesZeroOrMore() {
        assertTrue(TopicMatcher.matches("order.#", "order"));
        assertTrue(TopicMatcher.matches("order.#", "order.created"));
        assertTrue(TopicMatcher.matches("order.#", "order.payment.created"));
    }

    @Test
    void hash_alone_matchesEverything() {
        assertTrue(TopicMatcher.matches("#", ""));
        assertTrue(TopicMatcher.matches("#", "foo"));
        assertTrue(TopicMatcher.matches("#", "foo.bar"));
        assertTrue(TopicMatcher.matches("#", "foo.bar.baz"));
    }

    @Test
    void hash_atBeginning_matchesPrefix() {
        assertTrue(TopicMatcher.matches("#.foo", "foo"));
        assertTrue(TopicMatcher.matches("#.foo", "bar.foo"));
        assertTrue(TopicMatcher.matches("#.foo", "a.b.c.foo"));
        assertFalse(TopicMatcher.matches("#.foo", "foo.bar"));
    }

    @Test
    void hash_inMiddle() {
        assertTrue(TopicMatcher.matches("foo.#.bar", "foo.bar"));
        assertTrue(TopicMatcher.matches("foo.#.bar", "foo.x.bar"));
        assertTrue(TopicMatcher.matches("foo.#.bar", "foo.x.y.z.bar"));
        assertFalse(TopicMatcher.matches("foo.#.bar", "foo.x.baz"));
    }

    // --- Combined wildcards ---

    @Test
    void combined_starAndHash() {
        assertTrue(TopicMatcher.matches("*.*.#", "a.b"));
        assertTrue(TopicMatcher.matches("*.*.#", "a.b.c"));
        assertFalse(TopicMatcher.matches("*.*.#", "a"));
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
    void emptyRoutingKey_matchedByHashAndStar() {
        assertTrue(TopicMatcher.matches("#", ""));
        assertTrue(TopicMatcher.matches("*", "")); // one empty word
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
}
```

### Existing pattern reference

```java
// From Amqp091RoutingEngine.java lines 315-353 — topicMatches:
// NOTE: This implementation does NOT handle mid-pattern # correctly.
// The design doc §7.2 algorithm (recursive) must be used instead.
static boolean topicMatches(String pattern, String routingKey) {
    if (pattern.indexOf('*') < 0 && pattern.indexOf('#') < 0) {
        return pattern.equals(routingKey);
    }
    String[] patternWords = splitByDot(pattern);
    String[] routingWords = splitByDot(routingKey);
    int minLen = Math.min(patternWords.length, routingWords.length);
    for (int i = 0; i < minLen; i++) {
        String pw = patternWords[i];
        if ("#".equals(pw)) return true;
        if ("*".equals(pw)) continue;
        if (!pw.equals(routingWords[i])) return false;
    }
    if (patternWords.length == routingWords.length) return true;
    if (patternWords.length == routingWords.length + 1
            && "#".equals(patternWords[patternWords.length - 1])) return true;
    return false;
}
```

---

## Tests

**Test class:** `http-server/src/test/java/kafka/server/http/routing/TopicMatcherTest.java`

| Test method | What it verifies |
|-------------|-----------------|
| `exactMatch_identical` | Exact match works |
| `exactMatch_different` | Non-match returns false |
| `star_matchesOneWord` | `*` matches exactly one word |
| `star_doesNotMatchMultipleWords` | `*` does NOT match multiple words |
| `star_atBeginning` | `*.created` pattern |
| `star_inMiddle` | `order.*.completed` pattern |
| `star_alone_matchesOneWord` | `*` alone |
| `hash_atEnd_matchesZeroOrMore` | `order.#` pattern |
| `hash_alone_matchesEverything` | `#` alone |
| `hash_atBeginning_matchesPrefix` | `#.foo` pattern (mid-# case) |
| `hash_inMiddle` | `foo.#.bar` pattern (mid-# case) |
| `combined_starAndHash` | Mixed wildcards |
| `designDocExamples` | All examples from §7.2 |
| `emptyPattern_matchesOnlyEmpty` | Empty string edge case |
| `splitByDot_*` | Manual split correctness |

**Run command:**
```bash
cd /home/anh/kafka && ./gradlew :http-server:test --tests 'kafka.server.http.routing.TopicMatcherTest'
```

---

## Rules

- No regex in hot path (§8.5) — manual dot-split only
- Must handle mid-pattern `#` (design doc §7.2 requires recursive algorithm)
- Stateless utility — all methods static, no mutable state
- Fast path for patterns without wildcards (exact string comparison)

---

## Learning

- The ivy-ref `topicMatches` shortcut (early `return true` on encountering any `#`) is **wrong** for mid-pattern `#`: `foo.#.bar` would incorrectly match `foo.x.baz` because it returns on the `#` without verifying the `bar` anchor. The recursive algorithm from design doc §7.2 is mandatory — not an optimization.
- `splitByDot("")` intentionally returns `[""]` (length 1) rather than `[]` (length 0). This is what lets `"*"` match the empty routing key as "one empty word" (per AMQP spec and design doc §7.2 edge case). The implementation achieves this by initializing `count = 1` and always writing a final trailing segment after the last dot (or the entire string if there are no dots).
- For mid-pattern `#` the recursive attempt range is `ri` through `routing.length` **inclusive** (not exclusive). The upper-bound `routing.length` is what lets `#` consume zero remaining words; forgetting this off-by-one breaks `foo.#.bar` matching `foo.bar`.
- The fast path (`indexOf('*') < 0 && indexOf('#') < 0`) short-circuits the majority of real-world bindings (most topic bindings are literal multi-segment keys like `order.created`) and avoids allocating two `String[]` arrays per non-wildcard binding on the publish hot path.

---

## Limitations

- No trie-based pre-compilation (design doc §7.2 mentions this as a future optimization). Current implementation is O(P * R) per match where P = pattern word count and R = routing word count (worst-case O(P * R^2) with mid-pattern `#` due to the skip loop). For Phase 1 with ~tens of bindings per exchange this is fine; revisit if profiling shows topic dispatch as a hotspot.
- `RoutingEngine` dispatch table is **not** wired to use this matcher in this task. `RoutingEngine.java` still throws `UnsupportedOperationException` for `TOPIC` exchanges. A later wiring task must inject `TopicMatcher.matches` into the TOPIC dispatch branch (scope explicitly excluded by the task spec).
- Input is assumed non-null; passing `null` for either argument throws `NullPointerException` from `indexOf`/`equals`. Matches the existing `DirectMatcher` posture but callers must validate upstream.

---

## Field Notes

- Task-spec signature `matches(String pattern, String routingKey)` is a pure pattern-matching utility — narrower than the `match(vhost, exchange, routingKey, headers, bindings)` orchestration signature the higher-level matchers (like `DirectMatcher.match(bindings, routingKey)`) expose. Followed the task file as authoritative; the binding-iteration loop will live in whatever wires `TopicMatcher` into `RoutingEngine`.
- Used `@ParameterizedTest` with `@CsvSource` only for the design-doc example table (test-runner count 42 vs. method count 32 reflects the 7 CSV row expansions plus a handful of assertion-per-row tests). Avoided splitting each CSV row into a separate method per test-design hygiene.
- Verified no-regex rule with `grep -E "Pattern|regex|\.split\("` on the production file — the only hits are inside comments/Javadoc ("Pattern exhausted", "without regex"). No runtime `String.split()` or `java.util.regex.Pattern` reference.

---

## Acceptance Criteria

- [ ] `cd /home/anh/kafka && ./gradlew :http-server:test --tests 'kafka.server.http.routing.TopicMatcherTest'` exits 0
- [ ] `grep -r "TopicMatcher" http-server/src/main/java/` returns at least 1 hit
- [ ] Mid-pattern `#` tests pass (`#.foo`, `foo.#.bar`)
- [ ] No regex usage in production code: `grep -r "Pattern\|regex\|split(" http-server/src/main/java/kafka/server/http/routing/TopicMatcher.java` returns 0
- [ ] Learning section filled with at least one entry
- [ ] Limitations section filled (use "None" if truly none)
- [ ] File Manifest section updated after commit

---

## File Manifest

> Filled by the executing agent after each commit.
> Run: `git diff --name-status HEAD~1 HEAD -- '*.java' '*.xml' '*.json' '*.yaml' '*.yml'`

<!-- ### YYYY-MM-DD — <short description> (commit <hash>)
Created:
  - path/to/NewFile.java — <what it does>
Modified:
  - path/to/Existing.java — <what changed>
-->
