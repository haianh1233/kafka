# TASK-WS2.03: HeadersMatcher — Headers Exchange Routing

## Prerequisites

| Task | What it delivers | Why this task needs it |
|------|------------------|-----------------------|
| None | — | Standalone matcher with no upstream dependencies |

---

## Context

The headers exchange type routes messages based on header key-value matching against binding criteria. Each binding specifies an `x-match` mode (`all` or `any`) and a set of header criteria. The routing key is ignored.

From **design doc §7.4**:

> `x-match: all` → all criteria must match; `x-match: any` → at least one
> Empty criteria matches everything

```
Exchange "amq.headers" (type=headers):
  Binding: queue="priority-us",
           arguments={"x-match":"all", "priority":"high", "region":"us-east"}
  Binding: queue="any-high",
           arguments={"x-match":"any", "priority":"high", "region":"eu-west"}

Publish: headers={"priority":"high", "region":"us-east"}
  → "priority-us" matches (all criteria met)
  → "any-high" matches (priority=high matches, x-match=any requires only 1)
  → matches ["priority-us", "any-high"]
```

The ivy-ref `Amqp091RoutingEngine.headersMatch()` (lines 234-257) provides the exact algorithm to adapt.

---

## Specification

```java
package kafka.server.http.routing;

import java.util.Map;

/**
 * Headers exchange matcher — matches message headers against binding criteria.
 *
 * Stateless utility class. All methods are static and thread-safe.
 */
public final class HeadersMatcher {

    /**
     * Tests whether message headers match binding criteria.
     *
     * @param bindingArgs  binding arguments including x-match and header criteria
     * @param msgHeaders   message headers to check
     * @return true if the message matches the binding
     */
    public static boolean matches(Map<String, String> bindingArgs, Map<String, String> msgHeaders);
}
```

---

## Implementation Details

**Module:** `http-server`

**Files to study:**

| File | Why |
|------|-----|
| `references/ivy-ref/ivy-server/src/main/java/com/ivy/server/handler/amqp091/Amqp091RoutingEngine.java` lines 234-257 | headersMatch — exact algorithm |
| `ivy-docs/http-protocol-extend-design.md` §7.4 | Headers exchange specification |

```java
// From Amqp091RoutingEngine.java lines 234-257 — headersMatch:
static boolean headersMatch(Map<String, String> bindingArgs, Map<String, String> msgHeaders) {
    String xMatch = bindingArgs.getOrDefault("x-match", "all");
    boolean matchAll = !"any".equals(xMatch);

    int criteriaCount = 0;
    int matchCount = 0;

    for (Map.Entry<String, String> entry : bindingArgs.entrySet()) {
        if ("x-match".equals(entry.getKey())) {
            continue; // skip the x-match meta-key
        }
        criteriaCount++;
        String msgValue = msgHeaders.get(entry.getKey());
        if (msgValue != null && msgValue.equals(entry.getValue())) {
            matchCount++;
        }
    }

    if (criteriaCount == 0) {
        return true; // no criteria = match all
    }

    return matchAll ? matchCount == criteriaCount : matchCount > 0;
}
```

**Files to create:**

| File | What it does |
|------|--------------|
| `http-server/src/main/java/kafka/server/http/routing/HeadersMatcher.java` | Headers exchange matching |
| `http-server/src/test/java/kafka/server/http/routing/HeadersMatcherTest.java` | Unit tests |

**Files to modify:**

None.

> **EDGE CASE:** Empty criteria (no keys other than `x-match`) matches everything.
> **EDGE CASE:** Null message headers should be treated as empty map (no matches for any criteria).
> **EDGE CASE:** Missing `x-match` defaults to `"all"`.

**Implementation order:**
1. Create `HeadersMatcher` with `matches()` method following ivy-ref pattern
2. Write comprehensive tests covering all/any modes, empty criteria, null headers

---

## Skeleton Code

### Production class

```java
package kafka.server.http.routing;

import java.util.Map;

/**
 * Headers exchange matcher — matches message headers against binding criteria.
 *
 * // Time: Created - TASK-WS2.03
 */
public final class HeadersMatcher {

    private HeadersMatcher() {} // utility class

    /**
     * Tests whether message headers match binding criteria.
     *
     * <p>The binding arguments contain an {@code x-match} key that determines the
     * matching mode:
     * <ul>
     *   <li>{@code "all"} (default): all criteria must match</li>
     *   <li>{@code "any"}: at least one criterion must match</li>
     * </ul>
     *
     * <p>Empty criteria (no keys other than x-match) matches everything.
     */
    public static boolean matches(Map<String, String> bindingArgs, Map<String, String> msgHeaders) {
        if (bindingArgs == null || bindingArgs.isEmpty()) {
            return true;
        }

        String xMatch = bindingArgs.getOrDefault("x-match", "all");
        boolean matchAll = !"any".equals(xMatch);

        if (msgHeaders == null) {
            msgHeaders = Map.of();
        }

        int criteriaCount = 0;
        int matchCount = 0;

        for (Map.Entry<String, String> entry : bindingArgs.entrySet()) {
            if ("x-match".equals(entry.getKey())) {
                continue;
            }
            criteriaCount++;
            String msgValue = msgHeaders.get(entry.getKey());
            if (msgValue != null && msgValue.equals(entry.getValue())) {
                matchCount++;
            }
        }

        if (criteriaCount == 0) {
            return true;
        }

        return matchAll ? matchCount == criteriaCount : matchCount > 0;
    }
}
```

### Test class

```java
package kafka.server.http.routing;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * // Time: Created - TASK-WS2.03
 */
class HeadersMatcherTest {

    // --- x-match: all (default) ---

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
    void defaultXMatch_isAll() {
        // No x-match key → defaults to "all"
        assertTrue(HeadersMatcher.matches(
            Map.of("priority", "high"),
            Map.of("priority", "high")));

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
    void emptyMsgHeaders_noMatch() {
        assertFalse(HeadersMatcher.matches(
            Map.of("x-match", "all", "priority", "high"),
            Map.of()));
    }

    // --- Design doc examples (§7.4) ---

    @Test
    void designDocExample_priorityUs() {
        assertTrue(HeadersMatcher.matches(
            Map.of("x-match", "all", "priority", "high", "region", "us-east"),
            Map.of("priority", "high", "region", "us-east")));
    }

    @Test
    void designDocExample_anyHigh() {
        assertTrue(HeadersMatcher.matches(
            Map.of("x-match", "any", "priority", "high", "region", "eu-west"),
            Map.of("priority", "high", "region", "us-east")));
    }

    @Test
    void designDocExample_lowPriority_anyMatch() {
        assertTrue(HeadersMatcher.matches(
            Map.of("x-match", "any", "priority", "high", "region", "eu-west"),
            Map.of("priority", "low", "region", "eu-west")));
    }
}
```

---

## Tests

**Test class:** `http-server/src/test/java/kafka/server/http/routing/HeadersMatcherTest.java`

| Test method | What it verifies |
|-------------|-----------------|
| `matchAll_allCriteriaMatch` | All-mode match |
| `matchAll_notAllCriteriaMatch` | All-mode partial → no match |
| `matchAll_extraHeadersIgnored` | Extra headers don't affect match |
| `matchAll_missingHeader` | Missing required header → no match |
| `matchAny_oneCriterionMatches` | Any-mode single match |
| `matchAny_noCriteriaMatch` | Any-mode no match |
| `matchAny_allCriteriaMatch` | Any-mode all match |
| `defaultXMatch_isAll` | Missing x-match defaults to all |
| `emptyCriteria_matchesEverything` | Empty criteria → match all |
| `nullBindingArgs_matchesEverything` | Null args → match all |
| `nullMsgHeaders_noMatch` | Null headers with criteria → no match |
| `designDocExample_*` | Design doc §7.4 examples |

**Run command:**
```bash
cd /home/anh/kafka && ./gradlew :http-server:test --tests 'kafka.server.http.routing.HeadersMatcherTest'
```

---

## Rules

- `x-match` defaults to `"all"` when not specified
- Empty criteria (no keys besides x-match) matches everything
- Null-safe for both binding args and message headers
- Stateless utility — all methods static

---

## Learning

- The ivy-ref algorithm ports cleanly to this codebase; the key insight is the `matchCount == criteriaCount` equality check for `x-match=all`, which lets a single pass over entries cover both modes without a second loop.
- `Map.of()` literals in tests are concise but note they are immutable; the `mutableMapInputs_work` test guards against accidental reliance on immutable impls.
- Checkstyle's `HeaderCheck` is enforced on every new `.java` in `http-server/` (both `main/` and `test/`), so the Apache license header must precede the `// Time:` marker and package statement. The task file skeleton omitted the license header.
- The `HeadersMatcher.matches(args, headers)` signature intentionally takes no routing key — the headers-exchange contract forbids routing-key use — so "routing key ignored" is structural, not a runtime check. The `routingKeyIgnored_matchesByHeadersOnly` test documents this as a design note.

---

## Limitations

- **No value-type coercion**: matching is pure string equality via `String.equals()`. AMQP 0-9-1 allows typed header values (int, bool, array); callers must serialize to string before calling this matcher. This mirrors the ivy-ref design and aligns with the `Map<String, String>` binding-arguments type already used by `Binding`.
- **No glob / regex / numeric comparison**: RabbitMQ extensions like `x-match=all-with-x` or numeric operators are out of scope; this matcher only implements AMQP 0-9-1 headers-exchange semantics.
- **RoutingEngine dispatch-table wiring deferred**: `RoutingEngine.java` still throws `UnsupportedOperationException` for HEADERS type; wiring `HeadersMatcher` in is a separate task (per the context note in TASK-WS2.03).

---

## Field Notes

- TDD cycle worked cleanly: 28 compile errors (RED) → 24 test PASSED (GREEN) after adding the production class.
- Total wall-clock: ~2 min RED compile, ~16 s GREEN after checkstyle fix.
- The skeleton in the task file used `// Time:` before `package` — that failed checkstyle `HeaderCheck` with `"Line does not match expected header line of '/*'."`. Fix: prepend the Apache ASF license header block. Future WS2.x task skeletons should include the license header to avoid this churn.
- 24 tests implemented (spec listed ~13 methods). Extras cover: `emptyCriteriaAny_matchesEverything`, `nullMsgHeadersWithEmptyCriteria_matches`, `matchAll_valueMismatch`, `matchAny_valueMismatchOnAll`, `routingKeyIgnored_matchesByHeadersOnly`, `multipleBindings_evaluatedIndependently`, `mutableMapInputs_work`, plus split `defaultXMatch` positive/negative.

---

## Acceptance Criteria

- [ ] `cd /home/anh/kafka && ./gradlew :http-server:test --tests 'kafka.server.http.routing.HeadersMatcherTest'` exits 0
- [ ] `grep -r "HeadersMatcher" http-server/src/main/java/` returns at least 1 hit
- [ ] All x-match modes tested (all, any, default)
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
