# TASK-WS2.02: FanoutMatcher — Fanout Exchange Routing

## Prerequisites

| Task | What it delivers | Why this task needs it |
|------|------------------|-----------------------|
| None | — | Standalone matcher with no upstream dependencies |

---

## Context

The fanout exchange is the simplest exchange type: all bound queues receive every message, regardless of the routing key. This is used for broadcast scenarios where every subscriber needs to receive every published message.

From **design doc §7.3**:

> **Algorithm:** All bound queues receive the message. Routing key is ignored.
>
> ```
> Exchange "broadcast" (type=fanout):
>   Binding: queue="service-a"
>   Binding: queue="service-b"
>   Binding: queue="service-c"
>
> Publish: routingKey="anything" → matches ["service-a", "service-b", "service-c"]
> ```
>
> **Implementation:** Simple `List<String>` of queue names. O(K) where K = bound queues.

The ivy-ref `Amqp091RoutingEngine` (lines 207-211) shows the pattern:

```java
case "fanout" -> {
    for (Binding b : routing.bindings) {
        matched.add(b.queue());
    }
}
```

This is intentionally trivial — the value is in having a consistent `Matcher` interface across all exchange types so that `RoutingEngine` can dispatch uniformly.

---

## Specification

```java
package kafka.server.http.routing;

import java.util.Collection;
import java.util.Set;

/**
 * Fanout exchange matcher — returns all bound queues, routing key ignored.
 *
 * Stateless utility. O(K) where K = number of bound queues.
 */
public final class FanoutMatcher {

    /**
     * Returns all queue names from the given bindings. Routing key is ignored.
     *
     * @param boundQueues the set of queue names bound to the fanout exchange
     * @param routingKey  ignored (included for interface consistency)
     * @return all bound queue names
     */
    public static Set<String> match(Collection<String> boundQueues, String routingKey);
}
```

---

## Implementation Details

**Module:** `http-server`

**Files to study:**

| File | Why |
|------|-----|
| `references/ivy-ref/ivy-server/src/main/java/com/ivy/server/handler/amqp091/Amqp091RoutingEngine.java` lines 207-211 | Fanout routing pattern |
| `ivy-docs/http-protocol-extend-design.md` §7.3 | Fanout specification |

```java
// From Amqp091RoutingEngine.java lines 207-211 — fanout routing:
case "fanout" -> {
    for (Binding b : routing.bindings) {
        matched.add(b.queue());
    }
}
```

**Files to create:**

| File | What it does |
|------|--------------|
| `http-server/src/main/java/kafka/server/http/routing/FanoutMatcher.java` | Fanout exchange routing |
| `http-server/src/test/java/kafka/server/http/routing/FanoutMatcherTest.java` | Unit tests |

**Files to modify:**

None.

**Implementation order:**
1. Create `FanoutMatcher` with `match()` method
2. Write tests covering empty bindings, single binding, multiple bindings

---

## Skeleton Code

### Production class

```java
package kafka.server.http.routing;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Fanout exchange matcher — returns all bound queues, routing key ignored.
 *
 * // Time: Created - TASK-WS2.02
 */
public final class FanoutMatcher {

    private FanoutMatcher() {} // utility class

    /**
     * Returns all queue names from the given bindings. Routing key is ignored.
     */
    public static Set<String> match(Collection<String> boundQueues, String routingKey) {
        return new HashSet<>(boundQueues);
    }
}
```

### Test class

```java
package kafka.server.http.routing;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * // Time: Created - TASK-WS2.02
 */
class FanoutMatcherTest {

    @Test
    void match_returnsAllBoundQueues() {
        Set<String> result = FanoutMatcher.match(
            List.of("service-a", "service-b", "service-c"), "any.routing.key");
        assertEquals(Set.of("service-a", "service-b", "service-c"), result);
    }

    @Test
    void match_ignoresRoutingKey() {
        Set<String> result1 = FanoutMatcher.match(List.of("q1"), "key1");
        Set<String> result2 = FanoutMatcher.match(List.of("q1"), "key2");
        assertEquals(result1, result2);
    }

    @Test
    void match_emptyBindings_returnsEmpty() {
        Set<String> result = FanoutMatcher.match(List.of(), "anything");
        assertTrue(result.isEmpty());
    }

    @Test
    void match_singleBinding() {
        Set<String> result = FanoutMatcher.match(List.of("only-queue"), "ignored");
        assertEquals(Set.of("only-queue"), result);
    }

    @Test
    void match_nullRoutingKey_works() {
        Set<String> result = FanoutMatcher.match(List.of("q1"), null);
        assertEquals(Set.of("q1"), result);
    }

    @Test
    void match_duplicateBindings_deduplicates() {
        Set<String> result = FanoutMatcher.match(
            List.of("q1", "q1", "q2"), "key");
        assertEquals(Set.of("q1", "q2"), result);
    }
}
```

---

## Tests

**Test class:** `http-server/src/test/java/kafka/server/http/routing/FanoutMatcherTest.java`

| Test method | What it verifies |
|-------------|-----------------|
| `match_returnsAllBoundQueues` | All queues returned |
| `match_ignoresRoutingKey` | Routing key irrelevant |
| `match_emptyBindings_returnsEmpty` | No bindings → empty set |
| `match_singleBinding` | Single queue case |
| `match_nullRoutingKey_works` | Null routing key doesn't throw |
| `match_duplicateBindings_deduplicates` | HashSet deduplicates |

**Run command:**
```bash
cd /home/anh/kafka && ./gradlew :http-server:test --tests 'kafka.server.http.routing.FanoutMatcherTest'
```

---

## Rules

- O(K) where K = bound queues
- Routing key is ignored per AMQP fanout semantics
- Stateless utility — all methods static

---

## Learning

- API shape deliberately diverges from `DirectMatcher.match(List<Binding>, String)`: fanout takes `Collection<String>` because routing-key/headers/arguments are irrelevant, so the caller can reuse a cached flat queue-name list (anticipating the Phase 2 fanout binding index) without paying to re-box Binding records on every publish.
- `Set.of()` is used for the empty-input fast path to avoid allocating a throwaway `HashSet`. Matches the Phase 1 `DirectMatcher` empty-bindings branch, so the `RoutingEngine` dispatch table sees consistent immutable-vs-mutable return conventions across matchers.
- Added `Objects.requireNonNull(boundQueues, ...)` even though the task skeleton omitted it — every sibling matcher (DirectMatcher) null-checks its binding collection, and we want a fast, attributable NPE rather than one buried inside `HashSet` copy.
- Duplicate-queue dedup is a free property of `HashSet`, not an explicit filter — this is why `match()` can accept the raw `b.queue()` stream from the ivy-ref loop without an intermediate "unique queues" pass.

---

## Limitations

- Not yet wired into `RoutingEngine.matchQueueBindings` — the `"fanout"` branch still throws `UnsupportedOperationException`. Wiring is deferred per the WS2.02 task-card pre-flight note ("THIS task creates the matcher — wiring into the dispatch table is deferred"). The wiring task will also need to decide whether to extract `queue()` at the call site or cache a fanout-specific binding index.
- Fanout exchange-to-exchange (e2e) routing is likewise deferred: `RoutingEngine.e2eMatches` only handles `"direct"`. Fanout e2e semantics (all e2e bindings match) belong to the wiring task, not this one.
- `boundQueues` is expected non-null; a null argument produces an NPE rather than an empty set. Chose fail-fast over silent-empty so a broken caller surfaces immediately in tests.

---

## Field Notes

- TDD loop: RED (compile error, 9 `cannot find symbol` on `FanoutMatcher`) → GREEN (7/7 PASS on first implementation). No refactor phase needed — the implementation is a two-line delegate to `HashSet` copy.
- Task-file skeleton showed 6 tests; shipped 7 by adding `match_extractedFromBindings_headersIgnored` which exercises the intended call pattern (pull `queue()` off real `Binding` records with non-trivial `arguments`). This locks the contract that arguments/headers are irrelevant to fanout, which would otherwise only be implicit.
- Pre-flight `git log --oneline -1` returned `f95a1f995d` (a drifted worktree); `git fetch origin && git reset --hard origin/feature/http-protocol` brought the worktree to the expected `0c9549098f` HEAD before any work began.

---

## Acceptance Criteria

- [ ] `cd /home/anh/kafka && ./gradlew :http-server:test --tests 'kafka.server.http.routing.FanoutMatcherTest'` exits 0
- [ ] `grep -r "FanoutMatcher" http-server/src/main/java/` returns at least 1 hit
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

### 2026-04-17 — FanoutMatcher + tests (commit 33788de005)
Created:
  - http-server/src/main/java/kafka/server/http/routing/FanoutMatcher.java — stateless utility that returns all bound queue names as a deduplicated Set, ignoring routing key; O(K) HashSet copy with Set.of() fast-path for empty input.
  - http-server/src/test/java/kafka/server/http/routing/FanoutMatcherTest.java — 7 JUnit 5 tests covering empty bindings, single/multiple queues, duplicate dedup, routing-key invariance across keys, null routing key, and the extract-from-Binding caller pattern (headers/arguments ignored).
