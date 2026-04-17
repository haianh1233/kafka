# TASK-WS2.04: Exchange-to-Exchange Bindings and Alternate Exchange Fallback

## Prerequisites

| Task | What it delivers | Why this task needs it |
|------|------------------|-----------------------|
| TASK-WS2.01 | `TopicMatcher` | E2E bindings use topic matching for routing decision |
| TASK-WS2.02 | `FanoutMatcher` | E2E bindings use fanout matching for routing decision |
| TASK-WS2.03 | `HeadersMatcher` | E2E bindings use headers matching for routing decision |

---

## Context

Exchange-to-exchange (E2E) bindings allow messages entering a source exchange to be routed through a destination exchange as well, creating multi-level routing topologies. Combined with alternate exchange fallback, this provides a complete routing graph.

From **design doc §7.5 (Exchange-to-Exchange Routing)**:

> Messages entering a source exchange that match an e2e binding are also routed through the destination exchange. Recursive routing with a **visited set** prevents infinite cycles.

```java
Set<String> routeRecursive(String exchange, String routingKey,
                           Map<String, String> headers,
                           Set<String> matchedQueues,
                           Set<String> visited) {
    if (!visited.add(exchange)) return matchedQueues;  // cycle guard

    ExchangeEntry entry = exchanges.get(exchange);
    // 1. Match queue bindings
    // 2. Match e2e bindings → recurse
    return matchedQueues;
}
```

From **design doc §7.7 (Alternate Exchange)**:

> When an exchange has an `alternate-exchange` argument and a publish matches **no bindings**, the message is re-routed to the alternate exchange instead of being discarded.

```java
Set<String> matchedQueues = matchBindings(exchange, routingKey, headers);

if (matchedQueues.isEmpty() && exchange.alternateExchange != null) {
    return routeRecursive(exchange.alternateExchange, routingKey, headers,
                          matchedQueues, visited);
}
```

The ivy-ref `Amqp091RoutingEngine` (lines 111-186) already implements both E2E bindings and alternate exchange with cycle-safe recursion. This task extends the existing `RoutingEngine` (created in earlier WS tasks) with these capabilities.

---

## Specification

```java
package kafka.server.http.routing;

import java.util.Map;
import java.util.Set;

/**
 * Extensions to RoutingEngine for exchange-to-exchange bindings and alternate exchange.
 */
public interface RoutingEngineExtensions {

    /**
     * Binds a destination exchange to a source exchange.
     * When source receives a message matching the binding, it also routes through destination.
     *
     * @param source      source exchange name
     * @param destination destination exchange name
     * @param routingKey  routing key for the binding
     */
    void bindExchangeToExchange(String source, String destination, String routingKey);

    /**
     * Removes an exchange-to-exchange binding.
     */
    void unbindExchangeFromExchange(String source, String destination, String routingKey);

    /**
     * Routes a message through an exchange, including e2e bindings and alternate exchange.
     * Uses recursive routing with visited set for cycle prevention.
     *
     * @param exchange    starting exchange name
     * @param routingKey  message routing key
     * @param msgHeaders  message headers (for headers exchange routing); may be null
     * @return set of matched queue names
     */
    Set<String> route(String exchange, String routingKey, Map<String, String> msgHeaders);
}
```

---

## Implementation Details

**Module:** `http-server`

**Files to study:**

| File | Why |
|------|-----|
| `references/ivy-ref/ivy-server/src/main/java/com/ivy/server/handler/amqp091/Amqp091RoutingEngine.java` lines 111-186 | E2E binding + recursive routing + cycle guard |
| `references/ivy-ref/ivy-server/src/main/java/com/ivy/server/handler/amqp091/Amqp091RoutingEngine.java` lines 47-59 | Alternate exchange declaration |
| `ivy-docs/http-protocol-extend-design.md` §7.5 | E2E routing specification |
| `ivy-docs/http-protocol-extend-design.md` §7.7 | Alternate exchange specification |

```java
// From Amqp091RoutingEngine.java lines 160-186 — recursive routing with e2e + cycle guard:
private void routeRecursive(String exchange, String routingKey, Map<String, String> msgHeaders,
                             Set<String> matched, Set<String> visited) {
    if (!visited.add(exchange)) {
        return; // Cycle guard
    }

    ExchangeRouting routing = exchanges.get(exchange);
    if (routing == null) {
        return;
    }

    // Route to directly bound queues
    routeToQueues(routing, routingKey, msgHeaders, matched);

    // Exchange-to-exchange routing: recurse into destination exchanges
    for (E2EBinding e2e : routing.e2eBindings) {
        boolean e2eMatch = switch (routing.type) {
            case "fanout" -> true;
            case "direct" -> e2e.routingKey().equals(routingKey);
            case "topic" -> topicMatches(e2e.routingKey(), routingKey);
            default -> false;
        };
        if (e2eMatch) {
            routeRecursive(e2e.destination(), routingKey, msgHeaders, matched, visited);
        }
    }
}
```

```java
// From Amqp091RoutingEngine.java lines 47-59 — alternate exchange support:
public void declareExchange(String exchange, String type, String alternateExchange) {
    exchanges.put(exchange, new ExchangeRouting(type, alternateExchange));
}

public String getAlternateExchange(String exchange) {
    ExchangeRouting routing = exchanges.get(exchange);
    return routing != null ? routing.alternateExchange : null;
}
```

**Files to create:**

| File | What it does |
|------|--------------|
| `http-server/src/main/java/kafka/server/http/routing/RoutingEngine.java` | Full routing engine with E2E and alternate exchange support |
| `http-server/src/test/java/kafka/server/http/routing/RoutingEngineTest.java` | Unit tests for E2E and alternate exchange |

**Files to modify:**

None (this task creates the complete `RoutingEngine`; earlier WS tasks may have created partial versions that this replaces/extends).

> **CRITICAL:** The visited set prevents infinite cycles in E2E routing. Without it, two exchanges bound to each other would cause a `StackOverflowError`. The visited set is passed by reference through the recursion and is per-routing-call (not shared across publishes).

> **CRITICAL:** Alternate exchange is tried AFTER all direct bindings and E2E bindings are checked. The alternate exchange is only used when `matchedQueues.isEmpty()` — if any binding matched, alternate exchange is NOT used.

> **GOTCHA:** E2E match uses the SOURCE exchange's type for matching the E2E binding key. A fanout source matches all E2E bindings; a direct source requires exact key match.

**Implementation order:**
1. Create `RoutingEngine` with exchange/binding storage (ConcurrentHashMap + CopyOnWriteArrayList)
2. Implement `declareExchange` with optional alternate exchange
3. Implement `bind` / `unbind` for queue bindings
4. Implement `bindExchangeToExchange` / `unbindExchangeFromExchange`
5. Implement `route()` → `routeRecursive()` with visited set
6. Add alternate exchange fallback in `routeRecursive()`
7. Write tests covering cycles, alternate exchange, mixed exchange types

---

## Skeleton Code

### Production class

```java
package kafka.server.http.routing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * WebSocket routing engine — routes messages from exchanges to bound queues.
 * Supports direct, topic, fanout, headers exchange types, exchange-to-exchange
 * bindings, and alternate exchange fallback.
 *
 * Thread-safe — uses ConcurrentHashMap for exchange storage and
 * CopyOnWriteArrayList for bindings.
 *
 * // Time: Created - TASK-WS2.04
 */
public final class RoutingEngine {

    private static final Logger log = LoggerFactory.getLogger(RoutingEngine.class);

    private final ConcurrentHashMap<String, ExchangeEntry> exchanges =
        new ConcurrentHashMap<>(16, 0.75f, 4);

    // --- Exchange lifecycle ---

    public void declareExchange(String exchange, String type) {
        declareExchange(exchange, type, null);
    }

    public void declareExchange(String exchange, String type, String alternateExchange) {
        exchanges.put(exchange, new ExchangeEntry(type, alternateExchange));
        log.debug("Exchange '{}' declared: type={} alt={}", exchange, type, alternateExchange);
    }

    public boolean exchangeExists(String exchange) {
        return exchanges.containsKey(exchange);
    }

    public String getAlternateExchange(String exchange) {
        ExchangeEntry entry = exchanges.get(exchange);
        return entry != null ? entry.alternateExchange : null;
    }

    public void removeExchange(String exchange) {
        exchanges.remove(exchange);
    }

    // --- Queue bindings ---

    public void bind(String exchange, String queue, String routingKey) {
        bind(exchange, queue, routingKey, Map.of());
    }

    public void bind(String exchange, String queue, String routingKey, Map<String, String> args) {
        ExchangeEntry entry = exchanges.get(exchange);
        if (entry == null) {
            log.warn("Bind to undeclared exchange '{}'", exchange);
            return;
        }
        var finalArgs = args != null ? args : Map.<String, String>of();
        entry.bindings.removeIf(b -> b.queue().equals(queue) && b.routingKey().equals(routingKey));
        entry.bindings.add(new Binding(queue, routingKey, finalArgs));
    }

    public void unbind(String exchange, String queue, String routingKey) {
        ExchangeEntry entry = exchanges.get(exchange);
        if (entry == null) return;
        entry.bindings.removeIf(b -> b.queue().equals(queue) && b.routingKey().equals(routingKey));
    }

    public void removeQueue(String queue) {
        for (ExchangeEntry entry : exchanges.values()) {
            entry.bindings.removeIf(b -> b.queue().equals(queue));
        }
    }

    // --- Exchange-to-exchange bindings ---

    public void bindExchangeToExchange(String source, String destination, String routingKey) {
        ExchangeEntry entry = exchanges.get(source);
        if (entry == null) {
            log.warn("E2E bind to undeclared source '{}'", source);
            return;
        }
        entry.e2eBindings.add(new E2EBinding(destination, routingKey));
        log.debug("E2E binding: {} → {} key={}", source, destination, routingKey);
    }

    public void unbindExchangeFromExchange(String source, String destination, String routingKey) {
        ExchangeEntry entry = exchanges.get(source);
        if (entry == null) return;
        entry.e2eBindings.removeIf(
            b -> b.destination().equals(destination) && b.routingKey().equals(routingKey));
    }

    // --- Routing ---

    public Set<String> route(String exchange, String routingKey) {
        return route(exchange, routingKey, null);
    }

    public Set<String> route(String exchange, String routingKey, Map<String, String> msgHeaders) {
        Set<String> matched = new HashSet<>();
        routeRecursive(exchange, routingKey, msgHeaders, matched, new HashSet<>());
        return matched;
    }

    private void routeRecursive(String exchange, String routingKey,
                                 Map<String, String> msgHeaders,
                                 Set<String> matched, Set<String> visited) {
        if (!visited.add(exchange)) return; // cycle guard

        ExchangeEntry entry = exchanges.get(exchange);
        if (entry == null) return;

        int matchedBefore = matched.size();

        // 1. Route to directly bound queues
        routeToQueues(entry, routingKey, msgHeaders, matched);

        // 2. Exchange-to-exchange routing
        for (E2EBinding e2e : entry.e2eBindings) {
            boolean e2eMatch = switch (entry.type) {
                case "fanout" -> true;
                case "direct" -> e2e.routingKey().equals(routingKey);
                case "topic" -> TopicMatcher.matches(e2e.routingKey(), routingKey);
                case "headers" -> msgHeaders != null; // headers e2e always matches if headers present
                default -> false;
            };
            if (e2eMatch) {
                routeRecursive(e2e.destination(), routingKey, msgHeaders, matched, visited);
            }
        }

        // 3. Alternate exchange fallback: only if no queues matched from this exchange
        if (matched.size() == matchedBefore && entry.alternateExchange != null) {
            routeRecursive(entry.alternateExchange, routingKey, msgHeaders, matched, visited);
        }
    }

    private void routeToQueues(ExchangeEntry entry, String routingKey,
                               Map<String, String> msgHeaders, Set<String> matched) {
        switch (entry.type) {
            case "direct" -> {
                for (Binding b : entry.bindings) {
                    if (b.routingKey().equals(routingKey)) matched.add(b.queue());
                }
            }
            case "topic" -> {
                for (Binding b : entry.bindings) {
                    if (TopicMatcher.matches(b.routingKey(), routingKey)) matched.add(b.queue());
                }
            }
            case "fanout" -> {
                for (Binding b : entry.bindings) {
                    matched.add(b.queue());
                }
            }
            case "headers" -> {
                if (msgHeaders == null) return;
                for (Binding b : entry.bindings) {
                    if (HeadersMatcher.matches(b.args(), msgHeaders)) matched.add(b.queue());
                }
            }
            default -> log.debug("Unsupported exchange type '{}'", entry.type);
        }
    }

    // --- Internal types ---

    private static final class ExchangeEntry {
        final String type;
        final String alternateExchange;
        final List<Binding> bindings = new CopyOnWriteArrayList<>();
        final List<E2EBinding> e2eBindings = new CopyOnWriteArrayList<>();

        ExchangeEntry(String type, String alternateExchange) {
            this.type = type;
            this.alternateExchange = alternateExchange;
        }
    }

    private record Binding(String queue, String routingKey, Map<String, String> args) {}
    private record E2EBinding(String destination, String routingKey) {}
}
```

### Test class

```java
package kafka.server.http.routing;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * // Time: Created - TASK-WS2.04
 */
class RoutingEngineTest {

    private RoutingEngine engine;

    @BeforeEach
    void setUp() {
        engine = new RoutingEngine();
    }

    // --- E2E bindings ---

    @Test
    void e2e_directToFanout() {
        engine.declareExchange("source", "direct");
        engine.declareExchange("dest", "fanout");
        engine.bind("dest", "q1", "");
        engine.bind("dest", "q2", "");
        engine.bindExchangeToExchange("source", "dest", "order.created");

        Set<String> matched = engine.route("source", "order.created");
        assertEquals(Set.of("q1", "q2"), matched);
    }

    @Test
    void e2e_noMatchOnSource() {
        engine.declareExchange("source", "direct");
        engine.declareExchange("dest", "fanout");
        engine.bind("dest", "q1", "");
        engine.bindExchangeToExchange("source", "dest", "order.created");

        Set<String> matched = engine.route("source", "order.updated");
        assertTrue(matched.isEmpty());
    }

    @Test
    void e2e_cycleGuard() {
        engine.declareExchange("ex1", "fanout");
        engine.declareExchange("ex2", "fanout");
        engine.bind("ex1", "q1", "");
        engine.bind("ex2", "q2", "");
        engine.bindExchangeToExchange("ex1", "ex2", "");
        engine.bindExchangeToExchange("ex2", "ex1", ""); // cycle!

        Set<String> matched = engine.route("ex1", "anything");
        assertEquals(Set.of("q1", "q2"), matched); // no infinite loop
    }

    @Test
    void e2e_topicMatchOnSource() {
        engine.declareExchange("source", "topic");
        engine.declareExchange("dest", "direct");
        engine.bind("dest", "q1", "order.created");
        engine.bindExchangeToExchange("source", "dest", "order.*");

        Set<String> matched = engine.route("source", "order.created");
        assertEquals(Set.of("q1"), matched);
    }

    @Test
    void e2e_chainOfThree() {
        engine.declareExchange("ex1", "fanout");
        engine.declareExchange("ex2", "fanout");
        engine.declareExchange("ex3", "fanout");
        engine.bind("ex3", "q1", "");
        engine.bindExchangeToExchange("ex1", "ex2", "");
        engine.bindExchangeToExchange("ex2", "ex3", "");

        Set<String> matched = engine.route("ex1", "key");
        assertEquals(Set.of("q1"), matched);
    }

    // --- Alternate exchange ---

    @Test
    void alternateExchange_usedWhenNoMatch() {
        engine.declareExchange("primary", "direct", "fallback");
        engine.declareExchange("fallback", "fanout");
        engine.bind("primary", "q1", "order.created");
        engine.bind("fallback", "catch-all", "");

        // No match on primary → falls back
        Set<String> matched = engine.route("primary", "unknown.key");
        assertEquals(Set.of("catch-all"), matched);
    }

    @Test
    void alternateExchange_notUsedWhenMatch() {
        engine.declareExchange("primary", "direct", "fallback");
        engine.declareExchange("fallback", "fanout");
        engine.bind("primary", "q1", "order.created");
        engine.bind("fallback", "catch-all", "");

        // Match on primary → alternate NOT used
        Set<String> matched = engine.route("primary", "order.created");
        assertEquals(Set.of("q1"), matched);
    }

    @Test
    void alternateExchange_chainedFallback() {
        engine.declareExchange("ex1", "direct", "ex2");
        engine.declareExchange("ex2", "direct", "ex3");
        engine.declareExchange("ex3", "fanout");
        engine.bind("ex3", "final-catch", "");

        Set<String> matched = engine.route("ex1", "no-match");
        assertEquals(Set.of("final-catch"), matched);
    }

    // --- Basic routing (sanity) ---

    @Test
    void directExchange_exactMatch() {
        engine.declareExchange("direct-ex", "direct");
        engine.bind("direct-ex", "q1", "key1");
        engine.bind("direct-ex", "q2", "key2");

        assertEquals(Set.of("q1"), engine.route("direct-ex", "key1"));
        assertEquals(Set.of("q2"), engine.route("direct-ex", "key2"));
        assertTrue(engine.route("direct-ex", "key3").isEmpty());
    }

    @Test
    void fanoutExchange_allQueues() {
        engine.declareExchange("fanout-ex", "fanout");
        engine.bind("fanout-ex", "q1", "");
        engine.bind("fanout-ex", "q2", "");

        assertEquals(Set.of("q1", "q2"), engine.route("fanout-ex", "anything"));
    }

    @Test
    void topicExchange_wildcardMatch() {
        engine.declareExchange("topic-ex", "topic");
        engine.bind("topic-ex", "q1", "order.*");
        engine.bind("topic-ex", "q2", "#");

        assertEquals(Set.of("q1", "q2"), engine.route("topic-ex", "order.created"));
        assertEquals(Set.of("q2"), engine.route("topic-ex", "payment.completed"));
    }

    @Test
    void headersExchange_allMode() {
        engine.declareExchange("headers-ex", "headers");
        engine.bind("headers-ex", "q1", "",
            Map.of("x-match", "all", "priority", "high", "region", "us"));

        assertEquals(Set.of("q1"),
            engine.route("headers-ex", "", Map.of("priority", "high", "region", "us")));
        assertTrue(
            engine.route("headers-ex", "", Map.of("priority", "high")).isEmpty());
    }

    @Test
    void undeclaredExchange_returnsEmpty() {
        assertTrue(engine.route("nonexistent", "key").isEmpty());
    }

    @Test
    void unbindExchangeFromExchange_removesBinding() {
        engine.declareExchange("src", "fanout");
        engine.declareExchange("dst", "fanout");
        engine.bind("dst", "q1", "");
        engine.bindExchangeToExchange("src", "dst", "");

        assertEquals(Set.of("q1"), engine.route("src", "key"));

        engine.unbindExchangeFromExchange("src", "dst", "");
        assertTrue(engine.route("src", "key").isEmpty());
    }
}
```

---

## Tests

**Test class:** `http-server/src/test/java/kafka/server/http/routing/RoutingEngineTest.java`

| Test method | What it verifies |
|-------------|-----------------|
| `e2e_directToFanout` | E2E from direct to fanout works |
| `e2e_noMatchOnSource` | E2E binding key must match on source |
| `e2e_cycleGuard` | Cycle between two exchanges does not loop |
| `e2e_topicMatchOnSource` | Topic wildcard on E2E binding key |
| `e2e_chainOfThree` | Three-level E2E chain |
| `alternateExchange_usedWhenNoMatch` | Alternate used on no direct match |
| `alternateExchange_notUsedWhenMatch` | Alternate NOT used when matched |
| `alternateExchange_chainedFallback` | Chained alternate exchanges |
| `directExchange_exactMatch` | Basic direct routing sanity |
| `fanoutExchange_allQueues` | Basic fanout routing sanity |
| `topicExchange_wildcardMatch` | Basic topic routing sanity |
| `headersExchange_allMode` | Basic headers routing sanity |
| `undeclaredExchange_returnsEmpty` | Unknown exchange → empty |
| `unbindExchangeFromExchange_removesBinding` | E2E unbind works |

**Run command:**
```bash
cd /home/anh/kafka && ./gradlew :http-server:test --tests 'kafka.server.http.routing.RoutingEngineTest'
```

---

## Rules

- Visited set per route() call for cycle prevention (design doc §7.5)
- Alternate exchange only used when matchedQueues is empty (design doc §7.7)
- ConcurrentHashMap for exchanges, CopyOnWriteArrayList for bindings (§21.4)
- E2E match uses SOURCE exchange type for binding key matching

---

## Learning

- **Existing `RoutingEngine` uses functional-interface storage.** Rather than
  owning `ConcurrentHashMap<String, ExchangeEntry>` like the task skeleton
  proposes, the Phase-1 engine accepts `Function<String, ...>` lookups so that
  storage stays in `ExchangeManager` / `BindingManager`. WS2.04 preserves that
  design and simply adds a fourth functional parameter
  (`Function<String, String> alternateExchangeFn`). This keeps the engine
  stateless and avoids reintroducing duplicate routing state.
- **Legacy 3-arg constructor is retained** and delegates to the 4-arg form
  with `ex -> null`. All pre-existing WsPublishHandler mocks and
  RoutingEngineTest call sites continue to compile unchanged.
- **Alternate-exchange fallback fires after BOTH queue-binding matching AND
  e2e recursion.** The guard is `matched.size() == matchedBefore`: it fires
  only when *this exchange* contributed nothing (neither direct bindings nor
  e2e bindings produced a queue). This matches ivy-ref semantics — e2e
  recursion counts as "matching" so you don't get double-delivery via the
  alternate chain when e2e already routed somewhere.
- **Cycle guard covers BOTH e2e and alternate-exchange recursion** because
  they share the same `visited` set passed through `routeRecursive`. A
  mutual-alternate loop (A.alt=B, B.alt=A) terminates cleanly with an empty
  result; a mutual-e2e loop terminates with the union of both exchanges'
  bindings.
- **Matcher wiring scope:** the task explicitly wires Topic/Fanout/Headers
  matchers into `matchQueueBindings` in the Skeleton Code section, so the
  three `UnsupportedOperationException` Phase-1 stubs are now replaced with
  real matching. The previous tests that asserted
  `UnsupportedOperationException` were replaced with positive matcher-wiring
  tests (`route_topicExchange_wildcardMatch`, `route_fanoutExchange_allBoundQueues`,
  `route_headersExchange_allMode`, `route_headersExchange_anyMode`).
- **E2E on a fanout source must always match** per ivy-ref — the fanout case
  in `e2eMatches` returns `true` unconditionally. Same applies to fanout
  queue-binding matching, which ignores the message routing key entirely.
- **E2E on a headers source uses `HeadersMatcher.matches(binding.arguments(),
  headers)`**, so an `E2EBinding` can carry the same `x-match`/header
  arguments a normal headers binding would.

---

## Limitations

- The alternate-exchange lookup is a `Function<String, String>`; callers
  building on top of `ExchangeManager` / `ExchangeMetadata` will need to
  project `arguments().get("alternate-exchange")` into that function. This
  task does not add an `alternateExchange` field to `ExchangeMetadata`
  (tracked implicitly via the `arguments` map, per AMQP 0-9-1 convention).
- The engine itself is stateless; per-call allocation of `HashSet<String>`
  for `matched` and `visited` is unchanged from Phase 1. A future perf task
  (§8.5 in the design doc) may pool these sets.
- No integration with WS2.05+ (REST endpoints for declaring alternate
  exchange). Integration comes later when the publish handler starts passing
  an `alternateExchangeFn` derived from `ExchangeManager`.

---

## Field Notes

- Mockito-based `WsPublishHandlerTest` continued to pass with no changes: it
  mocks `RoutingEngine` by class, so the new constructor arg is invisible
  to the mock.
- The skeleton in the spec uses a monolithic engine with internal storage,
  but reconciling that with the existing `BindingManager`-driven storage in
  the codebase would have forced a duplicate source of truth. The functional-
  interface approach was kept and the skeleton treated as algorithmic guidance.
- Empty-string alternate exchange is treated identically to `null` (explicitly
  tested via `route_alternateExchange_emptyStringTreatedAsNone`). This is a
  defensive guard in case the metadata serializer round-trips a missing
  argument as `""`.

---

## Acceptance Criteria

- [ ] `cd /home/anh/kafka && ./gradlew :http-server:test --tests 'kafka.server.http.routing.RoutingEngineTest'` exits 0
- [ ] `grep -r "RoutingEngine" http-server/src/main/java/` returns at least 1 hit
- [ ] Cycle guard test passes (no StackOverflowError)
- [ ] Alternate exchange fallback test passes
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
