# TASK-WS-T.01: RoutingEngine Unit Tests — All 4 Exchange Types + Edge Cases

## Prerequisites

| Task | What it delivers | Why this task needs it |
|------|------------------|-----------------------|
| TASK-WS1.08 | RoutingEngine with DirectMatcher | Primary test target |
| TASK-WS2.01 | TopicMatcher | Test wildcard matching |
| TASK-WS2.02 | FanoutMatcher | Test fanout delivery |
| TASK-WS2.03 | HeadersMatcher | Test header criteria matching |
| TASK-WS2.04 | Exchange-to-exchange bindings with cycle detection | Test e2e routing |

---

## Context

The design doc §23.3 specifies comprehensive unit tests for the RoutingEngine. These are pure logic tests — no broker, no I/O, fast execution. They cover all 4 exchange types (direct, topic/wildcard, fanout, headers), exchange-to-exchange bindings with cycle detection, alternate exchange fallback, default exchange, and unbind.

The test patterns follow the example in the design doc §23.3:

```java
class RoutingEngineTest {
    @Test void directExchange_exactMatch() { ... }
    @Test void topicExchange_starMatchesSingleWord() { ... }
    @Test void fanoutExchange_allQueuesReceive() { ... }
    @Test void headersExchange_matchAll() { ... }
    @Test void e2eBinding_recursiveRoute() { ... }
    @Test void e2eBinding_cycleDoesNotInfiniteLoop() { ... }
    @Test void defaultExchange_routesByQueueName() { ... }
    @Test void unbind_removesRouting() { ... }
}
```

**Minimum 15 test methods** covering all combinations and edge cases.

---

## Specification

### Test target: RoutingEngine

Test the `route(exchangeName, routingKey, headers)` method which returns `Set<String>` of matched queue names.

### Test categories

1. **Direct exchange (4 tests):** exact match, no match, multiple queues same key, empty routing key
2. **Topic exchange (5 tests):** star matches one word, hash matches zero+, hash alone matches everything, complex patterns, edge cases (empty routing key, consecutive wildcards)
3. **Fanout exchange (2 tests):** all queues receive, routing key ignored
4. **Headers exchange (3 tests):** match-all, match-any, empty criteria
5. **Exchange-to-exchange (2 tests):** recursive routing, cycle detection
6. **Default exchange (1 test):** routes by queue name
7. **Alternate exchange (1 test):** fallback when primary finds no match
8. **Unbind (1 test):** removes routing after unbind

---

## Implementation Details

**Module:** `http-server`

**Files to create:**

| File | What it does |
|------|--------------|
| `http-server/src/test/java/kafka/server/http/routing/RoutingEngineTest.java` | 15+ unit tests |

**Files to study:**

| File | Why |
|------|-----|
| `http-server/src/test/java/kafka/server/http/HttpRouterTest.java` | Existing unit test pattern in this module |

> **CRITICAL:** No broker needed. Instantiate RoutingEngine directly in @BeforeEach, call declareExchange/declareQueue/bind/route.

> **CRITICAL:** Topic exchange edge cases from design doc §6.2:
> - `"*"` alone matches exactly one word
> - `"#"` alone matches everything including empty routing key
> - Empty pattern "" matches only empty routing key ""
> - `"#.foo"` matches `"foo"` and `"bar.foo"` and `"a.b.c.foo"`
> - `"foo.#.bar"` matches `"foo.bar"` and `"foo.x.bar"` and `"foo.x.y.z.bar"`

**Implementation order:**
1. Direct exchange tests
2. Topic exchange tests (including wildcard edge cases)
3. Fanout exchange tests
4. Headers exchange tests
5. Exchange-to-exchange tests
6. Default exchange, alternate exchange, unbind tests

---

## Skeleton Code

```java
package kafka.server.http.routing;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.util.Map;
import java.util.Set;

/**
 * Unit tests for RoutingEngine — all 4 exchange types + edge cases.
 * No broker needed.
 *
 * // Time: Created - TASK-WS-T.01
 */
class RoutingEngineTest {

    private RoutingEngine engine;

    @BeforeEach
    void setUp() {
        engine = new RoutingEngine();
    }

    // ── Direct exchange ──────────────────────────────────────

    @Test void directExchange_exactMatch() {
        engine.declareExchange("orders", "direct");
        engine.declareQueue("q1");
        engine.bind("orders", "q1", "order.created");
        assertEquals(Set.of("q1"), engine.route("orders", "order.created", Map.of()));
    }

    @Test void directExchange_noMatch() {
        engine.declareExchange("orders", "direct");
        engine.declareQueue("q1");
        engine.bind("orders", "q1", "order.created");
        assertEquals(Set.of(), engine.route("orders", "order.updated", Map.of()));
    }

    @Test void directExchange_multipleQueues_sameKey() {
        engine.declareExchange("orders", "direct");
        engine.declareQueue("q1");
        engine.declareQueue("q2");
        engine.bind("orders", "q1", "order.created");
        engine.bind("orders", "q2", "order.created");
        assertEquals(Set.of("q1", "q2"), engine.route("orders", "order.created", Map.of()));
    }

    @Test void directExchange_emptyRoutingKey() {
        engine.declareExchange("ex", "direct");
        engine.declareQueue("q1");
        engine.bind("ex", "q1", "");
        assertEquals(Set.of("q1"), engine.route("ex", "", Map.of()));
        assertEquals(Set.of(), engine.route("ex", "something", Map.of()));
    }

    // ── Topic exchange (wildcard) ────────────────────────────

    @Test void topicExchange_starMatchesSingleWord() {
        engine.declareExchange("events", "topic");
        engine.declareQueue("q1");
        engine.bind("events", "q1", "order.*");
        assertEquals(Set.of("q1"), engine.route("events", "order.created", Map.of()));
        assertEquals(Set.of(), engine.route("events", "order.a.b", Map.of()));
    }

    @Test void topicExchange_hashMatchesZeroOrMore() {
        engine.declareExchange("events", "topic");
        engine.declareQueue("q1");
        engine.bind("events", "q1", "order.#");
        assertEquals(Set.of("q1"), engine.route("events", "order", Map.of()));
        assertEquals(Set.of("q1"), engine.route("events", "order.created", Map.of()));
        assertEquals(Set.of("q1"), engine.route("events", "order.a.b.c", Map.of()));
        assertEquals(Set.of(), engine.route("events", "payment.done", Map.of()));
    }

    @Test void topicExchange_hashAlone_matchesEverything() {
        engine.declareExchange("all", "topic");
        engine.declareQueue("q1");
        engine.bind("all", "q1", "#");
        assertEquals(Set.of("q1"), engine.route("all", "anything.at.all", Map.of()));
        assertEquals(Set.of("q1"), engine.route("all", "", Map.of()));
    }

    @Test void topicExchange_hashDotFoo_matchesTrailing() {
        engine.declareExchange("ex", "topic");
        engine.declareQueue("q1");
        engine.bind("ex", "q1", "#.foo");
        assertEquals(Set.of("q1"), engine.route("ex", "foo", Map.of()));
        assertEquals(Set.of("q1"), engine.route("ex", "bar.foo", Map.of()));
        assertEquals(Set.of("q1"), engine.route("ex", "a.b.c.foo", Map.of()));
        assertEquals(Set.of(), engine.route("ex", "foo.bar", Map.of()));
    }

    @Test void topicExchange_fooHashBar_matchesMiddle() {
        engine.declareExchange("ex", "topic");
        engine.declareQueue("q1");
        engine.bind("ex", "q1", "foo.#.bar");
        assertEquals(Set.of("q1"), engine.route("ex", "foo.bar", Map.of()));
        assertEquals(Set.of("q1"), engine.route("ex", "foo.x.bar", Map.of()));
        assertEquals(Set.of("q1"), engine.route("ex", "foo.x.y.z.bar", Map.of()));
        assertEquals(Set.of(), engine.route("ex", "foo.bar.baz", Map.of()));
    }

    // ── Fanout exchange ──────────────────────────────────────

    @Test void fanoutExchange_allQueuesReceive() {
        engine.declareExchange("broadcast", "fanout");
        engine.declareQueue("q1");
        engine.declareQueue("q2");
        engine.declareQueue("q3");
        engine.bind("broadcast", "q1", "");
        engine.bind("broadcast", "q2", "");
        engine.bind("broadcast", "q3", "");
        assertEquals(Set.of("q1", "q2", "q3"),
            engine.route("broadcast", "ignored", Map.of()));
    }

    @Test void fanoutExchange_noBindings_emptyResult() {
        engine.declareExchange("empty", "fanout");
        assertEquals(Set.of(), engine.route("empty", "key", Map.of()));
    }

    // ── Headers exchange ─────────────────────────────────────

    @Test void headersExchange_matchAll() {
        engine.declareExchange("hdrs", "headers");
        engine.declareQueue("q1");
        engine.bind("hdrs", "q1", "",
            Map.of("x-match", "all", "region", "us", "tier", "premium"));
        assertEquals(Set.of("q1"),
            engine.route("hdrs", "", Map.of("region", "us", "tier", "premium")));
        assertEquals(Set.of(),
            engine.route("hdrs", "", Map.of("region", "us", "tier", "free")));
    }

    @Test void headersExchange_matchAny() {
        engine.declareExchange("hdrs", "headers");
        engine.declareQueue("q1");
        engine.bind("hdrs", "q1", "",
            Map.of("x-match", "any", "region", "us", "tier", "premium"));
        assertEquals(Set.of("q1"),
            engine.route("hdrs", "", Map.of("region", "eu", "tier", "premium")));
        assertEquals(Set.of(),
            engine.route("hdrs", "", Map.of("region", "eu", "tier", "free")));
    }

    @Test void headersExchange_emptyCriteria_matchesEverything() {
        engine.declareExchange("hdrs", "headers");
        engine.declareQueue("q1");
        engine.bind("hdrs", "q1", "", Map.of("x-match", "all"));
        assertEquals(Set.of("q1"),
            engine.route("hdrs", "", Map.of("any", "header")));
    }

    // ── Exchange-to-exchange routing ─────────────────────────

    @Test void e2eBinding_recursiveRoute() {
        engine.declareExchange("source", "topic");
        engine.declareExchange("sink", "direct");
        engine.declareQueue("q1");
        engine.bindExchangeToExchange("source", "sink", "order.*");
        engine.bind("sink", "q1", "order.created");
        assertEquals(Set.of("q1"),
            engine.route("source", "order.created", Map.of()));
    }

    @Test void e2eBinding_cycleDoesNotInfiniteLoop() {
        engine.declareExchange("a", "fanout");
        engine.declareExchange("b", "fanout");
        engine.declareQueue("q1");
        engine.bindExchangeToExchange("a", "b", "");
        engine.bindExchangeToExchange("b", "a", "");
        engine.bind("b", "q1", "");
        assertEquals(Set.of("q1"), engine.route("a", "", Map.of()));
    }

    // ── Default exchange ─────────────────────────────────────

    @Test void defaultExchange_routesByQueueName() {
        engine.declareQueue("my-queue");
        assertEquals(Set.of("my-queue"), engine.route("", "my-queue", Map.of()));
    }

    // ── Alternate exchange ───────────────────────────────────

    @Test void alternateExchange_fallbackOnNoMatch() {
        // TODO: Declare exchange with alternate-exchange argument
        // TODO: Publish unroutable → falls back to alternate
    }

    // ── Unbind ───────────────────────────────────────────────

    @Test void unbind_removesRouting() {
        engine.declareExchange("ex", "direct");
        engine.declareQueue("q1");
        engine.bind("ex", "q1", "key");
        assertEquals(Set.of("q1"), engine.route("ex", "key", Map.of()));
        engine.unbind("ex", "q1", "key");
        assertEquals(Set.of(), engine.route("ex", "key", Map.of()));
    }
}
```

---

## Tests

**Test class:** `http-server/src/test/java/kafka/server/http/routing/RoutingEngineTest.java`

| Test method | What it verifies |
|-------------|-----------------|
| 4 direct exchange tests | Exact match, no match, multi-queue, empty key |
| 5 topic exchange tests | *, #, # alone, #.suffix, prefix.#.suffix |
| 2 fanout tests | All queues, no bindings |
| 3 headers tests | match-all, match-any, empty criteria |
| 2 e2e tests | Recursive routing, cycle guard |
| 1 default exchange test | Route by queue name |
| 1 alternate exchange test | Fallback routing |
| 1 unbind test | Route removed after unbind |

**Run command:**
```bash
timeout 300 ./gradlew :http-server:test --tests 'kafka.server.http.routing.RoutingEngineTest' -x spotlessCheck
```

---

## Rules

- No broker needed — pure unit tests.
- Minimum 15 test methods.
- All 4 exchange types covered.
- Topic exchange wildcard edge cases from design doc §6.2 tested.
- Cycle guard tested (exchange A → B → A must not hang).

---

## Learning

_To be filled by the executing agent._

---

## Limitations

_To be filled by the executing agent._

---

## Field Notes

_To be filled by the executing agent._

---

## Acceptance Criteria

- [ ] `timeout 300 ./gradlew :http-server:test --tests 'kafka.server.http.routing.RoutingEngineTest' -x spotlessCheck` exits 0
- [ ] At least 15 test methods in RoutingEngineTest
- [ ] All 4 exchange types tested (direct, topic, fanout, headers)
- [ ] Cycle guard tested
- [ ] Learning section filled with at least one entry

---

## File Manifest

_To be filled by the executing agent._
