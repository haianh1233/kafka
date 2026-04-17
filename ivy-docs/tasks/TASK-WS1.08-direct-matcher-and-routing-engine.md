# TASK-WS1.08: DirectMatcher + RoutingEngine — Exchange Routing Resolution

## Prerequisites

- **TASK-WS1.05** (ExchangeManager) — Provides exchange type lookup (`getExchangeType()`). RoutingEngine needs to know each exchange's type to delegate to the correct matcher.
- **TASK-WS1.07** (BindingManager) — Provides `listByExchange()` and `listE2EBySource()` for binding resolution. RoutingEngine reads bindings during routing.

---

## Context

The RoutingEngine is the central routing component that resolves `(exchange, routingKey, headers)` to a `Set<String>` of matched queue names. It is the main entry point called by WsPublishHandler on every publish operation.

The design doc (§7) specifies four exchange types: direct, topic, fanout, and headers. Phase 1 only implements **direct** exchange matching (exact string equality on routing key). Other types throw `UnsupportedOperationException` — they are implemented in later phases (WS2.xx).

The RoutingEngine uses the **strategy pattern**: each exchange type has a dedicated matcher class. For Phase 1, only `DirectMatcher` is implemented. The engine looks up the exchange type, delegates to the appropriate matcher, and collects matched queues. It also handles exchange-to-exchange (e2e) bindings by recursing into destination exchanges.

The `DirectMatcher` uses `HashMap<String, List<String>>` for O(1) lookup from routing key to queue names. This index is rebuilt from the binding list on each route call (Phase 1 simplicity). Later phases may cache the index and invalidate on binding changes.

The ivy-ref `Amqp091RoutingEngine` at `references/ivy-ref/ivy-server/src/main/java/com/ivy/server/handler/amqp091/Amqp091RoutingEngine.java` provides the reference pattern for routing dispatch, e2e recursion, and cycle detection.

---

## Specification

### `DirectMatcher` — `kafka.server.http.routing.DirectMatcher`

```java
/**
 * Direct exchange matcher: exact string equality on routing key.
 * Uses HashMap<String, List<String>> for O(1) lookup.
 */
public final class DirectMatcher {

    /**
     * Matches bindings against a routing key using exact string equality.
     *
     * @param bindings   all bindings for the exchange
     * @param routingKey the message routing key
     * @return set of queue names that match
     */
    public Set<String> match(List<Binding> bindings, String routingKey);
}
```

### `RoutingEngine` — `kafka.server.http.routing.RoutingEngine`

```java
/**
 * Main routing entry point: resolves (exchange, routingKey, headers) → matched queues.
 *
 * Phase 1: only direct exchange type supported.
 */
public final class RoutingEngine {

    /**
     * Routes a message to matching queues.
     *
     * @param exchange   exchange name
     * @param routingKey message routing key
     * @param headers    message headers (for headers exchange, may be null)
     * @return set of matched queue names (empty if no matches)
     * @throws IllegalArgumentException if exchange does not exist
     * @throws UnsupportedOperationException if exchange type is not supported in Phase 1
     */
    public Set<String> route(String exchange, String routingKey, Map<String, String> headers);

    /**
     * Convenience overload with no headers.
     */
    public Set<String> route(String exchange, String routingKey);
}
```

### Behavioral contracts

- `route()` returns an empty set if no bindings match — never null.
- For direct exchange: exact string equality between message `routingKey` and binding `routingKey`.
- Exchange-to-exchange recursion: after matching queue bindings, check e2e bindings. If a matching e2e binding is found, recurse into the destination exchange.
- Cycle detection: track visited exchanges in a `Set<String>`. Skip any exchange already visited.
- Phase 1 only supports `direct` type. `topic`, `fanout`, `headers` → `UnsupportedOperationException`.
- Unknown exchange → `IllegalArgumentException`.

---

## Implementation Details

**Module:** `http-server`

**Files to study:**

| File | Why |
|------|-----|
| `references/ivy-ref/ivy-server/src/main/java/com/ivy/server/handler/amqp091/Amqp091RoutingEngine.java` lines 142-225 | Route dispatch, e2e recursion with cycle guard |
| `ivy-docs/http-protocol-extend-design.md` §6.1 | Direct exchange algorithm: HashMap<String, List<String>> |
| `ivy-docs/http-protocol-extend-design.md` §7 | Routing engine overview |

```java
// From Amqp091RoutingEngine.java lines 154-186 — route dispatch with e2e recursion:
public Set<String> route(String exchange, String routingKey, Map<String, String> msgHeaders) {
    Set<String> matched = new HashSet<>();
    routeRecursive(exchange, routingKey, msgHeaders, matched, new HashSet<>());
    return matched;
}

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
// From Amqp091RoutingEngine.java lines 193-199 — direct match in routeToQueues:
case "direct" -> {
    for (Binding b : routing.bindings) {
        if (b.routingKey().equals(routingKey)) {
            matched.add(b.queue());
        }
    }
}
```

**Files to create:**

| File | What it does |
|------|--------------|
| `http-server/src/main/java/kafka/server/http/routing/DirectMatcher.java` | O(1) exact routing key match using HashMap index |
| `http-server/src/main/java/kafka/server/http/routing/RoutingEngine.java` | Main routing entry point with exchange type dispatch and e2e recursion |
| `http-server/src/test/java/kafka/server/http/routing/DirectMatcherTest.java` | DirectMatcher unit tests |
| `http-server/src/test/java/kafka/server/http/routing/RoutingEngineTest.java` | RoutingEngine unit tests |

> **CRITICAL:** The e2e binding match for direct exchanges uses exact string equality between the e2e binding's `routingKey` and the message's `routingKey` — same algorithm as queue binding match. This is different from fanout (always match) and topic (wildcard match).

> **CRITICAL:** The cycle guard in e2e recursion must use `Set<String>` with `add()` check, not `contains()` + `add()` separately. This is critical to prevent infinite loops in circular e2e binding configurations.

**Implementation order:**
1. Create `DirectMatcher.java` with HashMap-based O(1) lookup
2. Create `RoutingEngine.java` with exchange type dispatch
3. Implement e2e recursion with cycle detection
4. Write `DirectMatcherTest.java`
5. Write `RoutingEngineTest.java`

---

## Skeleton Code

### `DirectMatcher.java`

```java
package kafka.server.http.routing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Direct exchange matcher: exact string equality between message routing key
 * and binding routing key.
 *
 * Uses HashMap for O(1) lookup from routing key to queue names.
 *
 * // Time: Created - TASK-WS1.08
 */
public final class DirectMatcher {

    /**
     * Matches bindings against a routing key using exact string equality.
     *
     * @param bindings   all bindings for the exchange (from BindingManager.listByExchange)
     * @param routingKey the message routing key
     * @return set of queue names whose binding routing key equals the message routing key
     */
    public Set<String> match(List<Binding> bindings, String routingKey) {
        // TODO: 1. Build HashMap<String, List<String>>: routingKey → list of queue names
        //          (This is rebuilt per-call for Phase 1 simplicity; cache later)
        // TODO: 2. Look up routingKey in the map
        // TODO: 3. Return matched queue names as a Set, or empty set if no match
        throw new UnsupportedOperationException("Not yet implemented");
    }
}
```

### `RoutingEngine.java`

```java
package kafka.server.http.routing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/**
 * Main routing entry point: resolves (exchange, routingKey, headers) → Set of matched queues.
 *
 * Phase 1: only direct exchange type supported. Topic, fanout, headers throw
 * UnsupportedOperationException.
 *
 * Handles exchange-to-exchange (e2e) routing with cycle detection.
 *
 * // Time: Created - TASK-WS1.08
 */
public final class RoutingEngine {

    private static final Logger log = LoggerFactory.getLogger(RoutingEngine.class);

    private final DirectMatcher directMatcher = new DirectMatcher();

    // --- Dependencies (functional interfaces for loose coupling) ---
    private final Function<String, String> exchangeTypeFn;  // exchange name → type string
    private final Function<String, List<Binding>> bindingsFn;  // exchange name → bindings
    private final Function<String, List<E2EBinding>> e2eBindingsFn;  // exchange name → e2e bindings

    /**
     * @param exchangeTypeFn  returns exchange type ("direct", "topic", etc.) or null if not found
     * @param bindingsFn      returns bindings for an exchange (from BindingManager)
     * @param e2eBindingsFn   returns e2e bindings for an exchange (from BindingManager)
     */
    public RoutingEngine(Function<String, String> exchangeTypeFn,
                         Function<String, List<Binding>> bindingsFn,
                         Function<String, List<E2EBinding>> e2eBindingsFn) {
        this.exchangeTypeFn = Objects.requireNonNull(exchangeTypeFn, "exchangeTypeFn");
        this.bindingsFn = Objects.requireNonNull(bindingsFn, "bindingsFn");
        this.e2eBindingsFn = Objects.requireNonNull(e2eBindingsFn, "e2eBindingsFn");
    }

    /**
     * Routes a message to matching queues.
     *
     * @param exchange   exchange name
     * @param routingKey message routing key
     * @param headers    message headers (for headers exchange, may be null)
     * @return set of matched queue names (empty if no matches, never null)
     * @throws IllegalArgumentException      if exchange does not exist
     * @throws UnsupportedOperationException if exchange type is not supported
     */
    public Set<String> route(String exchange, String routingKey, Map<String, String> headers) {
        Objects.requireNonNull(exchange, "exchange");
        Objects.requireNonNull(routingKey, "routingKey");

        Set<String> matched = new HashSet<>();
        Set<String> visited = new HashSet<>();

        // TODO: Call routeRecursive(exchange, routingKey, headers, matched, visited)
        // TODO: Return matched
        throw new UnsupportedOperationException("Not yet implemented");
    }

    /**
     * Convenience overload with no headers.
     */
    public Set<String> route(String exchange, String routingKey) {
        return route(exchange, routingKey, null);
    }

    /**
     * Recursive routing with cycle detection.
     */
    private void routeRecursive(String exchange, String routingKey,
                                 Map<String, String> headers,
                                 Set<String> matched, Set<String> visited) {
        // TODO: 1. Cycle guard: if (!visited.add(exchange)) return
        // TODO: 2. Look up exchange type via exchangeTypeFn
        //          - null → throw IllegalArgumentException("Exchange not found: " + exchange)
        // TODO: 3. Get bindings via bindingsFn
        // TODO: 4. Dispatch to matcher based on type:
        //          - "direct" → directMatcher.match(bindings, routingKey) → add all to matched
        //          - "topic" → throw UnsupportedOperationException("Topic exchange not supported in Phase 1")
        //          - "fanout" → throw UnsupportedOperationException("Fanout exchange not supported in Phase 1")
        //          - "headers" → throw UnsupportedOperationException("Headers exchange not supported in Phase 1")
        //          - default → log warning, return
        // TODO: 5. Handle e2e bindings:
        //          - Get e2e bindings via e2eBindingsFn
        //          - For direct type: if e2e.routingKey().equals(routingKey), recurse into e2e.destination()
        throw new UnsupportedOperationException("Not yet implemented");
    }
}
```

### Test class — `DirectMatcherTest.java`

```java
package kafka.server.http.routing;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
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
}
```

### Test class — `RoutingEngineTest.java`

```java
package kafka.server.http.routing;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.*;

/**
 * // Time: Created - TASK-WS1.08
 */
class RoutingEngineTest {

    private Map<String, String> exchangeTypes;
    private Map<String, List<Binding>> bindings;
    private Map<String, List<E2EBinding>> e2eBindings;
    private RoutingEngine engine;

    @BeforeEach
    void setUp() {
        exchangeTypes = new HashMap<>();
        bindings = new HashMap<>();
        e2eBindings = new HashMap<>();

        engine = new RoutingEngine(
            exchangeTypes::get,
            ex -> bindings.getOrDefault(ex, List.of()),
            ex -> e2eBindings.getOrDefault(ex, List.of())
        );
    }

    // --- Direct exchange routing ---

    @Test
    void route_directExchange_exactMatch() {
        exchangeTypes.put("events", "direct");
        bindings.put("events", List.of(
            new Binding("events", "order-events", "order.created", Map.of()),
            new Binding("events", "audit-log", "order.created", Map.of())
        ));

        Set<String> result = engine.route("events", "order.created");
        assertEquals(Set.of("order-events", "audit-log"), result);
    }

    @Test
    void route_directExchange_noMatch_returnsEmpty() {
        exchangeTypes.put("events", "direct");
        bindings.put("events", List.of(
            new Binding("events", "order-events", "order.created", Map.of())
        ));

        Set<String> result = engine.route("events", "order.updated");
        assertTrue(result.isEmpty());
    }

    @Test
    void route_directExchange_noBindings_returnsEmpty() {
        exchangeTypes.put("events", "direct");
        Set<String> result = engine.route("events", "any.key");
        assertTrue(result.isEmpty());
    }

    // --- Exchange not found ---

    @Test
    void route_exchangeNotFound_throws() {
        assertThrows(IllegalArgumentException.class,
            () -> engine.route("nonexistent", "key"));
    }

    // --- Unsupported exchange types ---

    @Test
    void route_topicExchange_throwsUnsupported() {
        exchangeTypes.put("events", "topic");
        bindings.put("events", List.of());
        assertThrows(UnsupportedOperationException.class,
            () -> engine.route("events", "key"));
    }

    @Test
    void route_fanoutExchange_throwsUnsupported() {
        exchangeTypes.put("broadcast", "fanout");
        bindings.put("broadcast", List.of());
        assertThrows(UnsupportedOperationException.class,
            () -> engine.route("broadcast", "key"));
    }

    @Test
    void route_headersExchange_throwsUnsupported() {
        exchangeTypes.put("hdrs", "headers");
        bindings.put("hdrs", List.of());
        assertThrows(UnsupportedOperationException.class,
            () -> engine.route("hdrs", "key", Map.of("k", "v")));
    }

    // --- E2E routing ---

    @Test
    void route_directE2E_recursesIntoDestination() {
        exchangeTypes.put("source", "direct");
        exchangeTypes.put("dest", "direct");
        bindings.put("source", List.of());
        bindings.put("dest", List.of(
            new Binding("dest", "final-queue", "order.created", Map.of())
        ));
        e2eBindings.put("source", List.of(
            new E2EBinding("source", "dest", "order.created", Map.of())
        ));

        Set<String> result = engine.route("source", "order.created");
        assertEquals(Set.of("final-queue"), result);
    }

    @Test
    void route_directE2E_noMatchOnE2EKey_doesNotRecurse() {
        exchangeTypes.put("source", "direct");
        exchangeTypes.put("dest", "direct");
        bindings.put("source", List.of());
        bindings.put("dest", List.of(
            new Binding("dest", "final-queue", "order.created", Map.of())
        ));
        e2eBindings.put("source", List.of(
            new E2EBinding("source", "dest", "different.key", Map.of())
        ));

        Set<String> result = engine.route("source", "order.created");
        assertTrue(result.isEmpty());
    }

    @Test
    void route_e2eCycleDetection_doesNotLoop() {
        exchangeTypes.put("A", "direct");
        exchangeTypes.put("B", "direct");
        bindings.put("A", List.of(new Binding("A", "queue-a", "key", Map.of())));
        bindings.put("B", List.of(new Binding("B", "queue-b", "key", Map.of())));
        // A → B → A (cycle)
        e2eBindings.put("A", List.of(new E2EBinding("A", "B", "key", Map.of())));
        e2eBindings.put("B", List.of(new E2EBinding("B", "A", "key", Map.of())));

        Set<String> result = engine.route("A", "key");
        assertEquals(Set.of("queue-a", "queue-b"), result);
    }

    @Test
    void route_combinedDirectAndE2E_mergesResults() {
        exchangeTypes.put("events", "direct");
        exchangeTypes.put("audit", "direct");
        bindings.put("events", List.of(
            new Binding("events", "order-events", "order.created", Map.of())
        ));
        bindings.put("audit", List.of(
            new Binding("audit", "audit-log", "order.created", Map.of())
        ));
        e2eBindings.put("events", List.of(
            new E2EBinding("events", "audit", "order.created", Map.of())
        ));

        Set<String> result = engine.route("events", "order.created");
        assertEquals(Set.of("order-events", "audit-log"), result);
    }

    // --- Null handling ---

    @Test
    void route_nullExchange_throwsNPE() {
        assertThrows(NullPointerException.class, () -> engine.route(null, "key"));
    }

    @Test
    void route_nullRoutingKey_throwsNPE() {
        assertThrows(NullPointerException.class, () -> engine.route("events", null));
    }
}
```

### Existing pattern reference

```java
// From Amqp091RoutingEngine.java lines 160-186 — routing with e2e recursion:
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

---

## Tests

**Test class:** `http-server/src/test/java/kafka/server/http/routing/DirectMatcherTest.java`

| Test method | What it verifies |
|-------------|-----------------|
| `match_exactKey_returnsQueue` | Exact routing key match returns the bound queue |
| `match_noMatch_returnsEmpty` | Non-matching routing key returns empty set |
| `match_multipleQueuesSameKey_returnsAll` | Multiple queues with same key all matched |
| `match_emptyBindings_returnsEmpty` | No bindings → empty result |
| `match_emptyRoutingKey_matchesEmptyBinding` | Empty string routing key matches empty binding key |
| `match_caseSensitive_noMatch` | Case matters in routing key comparison |

**Test class:** `http-server/src/test/java/kafka/server/http/routing/RoutingEngineTest.java`

| Test method | What it verifies |
|-------------|-----------------|
| `route_directExchange_exactMatch` | Direct routing happy path |
| `route_directExchange_noMatch_returnsEmpty` | No binding match → empty set |
| `route_directExchange_noBindings_returnsEmpty` | Exchange with no bindings → empty set |
| `route_exchangeNotFound_throws` | Unknown exchange → IllegalArgumentException |
| `route_topicExchange_throwsUnsupported` | Topic type → UnsupportedOperationException |
| `route_fanoutExchange_throwsUnsupported` | Fanout type → UnsupportedOperationException |
| `route_headersExchange_throwsUnsupported` | Headers type → UnsupportedOperationException |
| `route_directE2E_recursesIntoDestination` | E2E routing traverses to destination exchange |
| `route_directE2E_noMatchOnE2EKey_doesNotRecurse` | E2E key mismatch prevents recursion |
| `route_e2eCycleDetection_doesNotLoop` | Circular e2e bindings don't cause infinite loop |
| `route_combinedDirectAndE2E_mergesResults` | Queue and e2e results merged into one set |
| `route_nullExchange_throwsNPE` | Null exchange rejected |
| `route_nullRoutingKey_throwsNPE` | Null routing key rejected |

**Run command:**
```bash
./gradlew :http-server:test --tests "kafka.server.http.routing.DirectMatcherTest" --tests "kafka.server.http.routing.RoutingEngineTest"
```

---

## Rules

- Thread-safe: RoutingEngine is called from Netty handler threads. All data access is through functional interfaces backed by concurrent data structures.
- Phase 1 only supports `direct` exchange type. Other types throw `UnsupportedOperationException`.
- E2E recursion must use cycle detection (`Set<String>` with `add()` as guard).
- Route result is never null — always returns a `Set<String>` (may be empty).
- DirectMatcher uses `HashMap<String, List<String>>` for O(1) lookup per design doc §6.1.

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

- [ ] `./gradlew :http-server:test --tests "kafka.server.http.routing.DirectMatcherTest"` exits 0
- [ ] `./gradlew :http-server:test --tests "kafka.server.http.routing.RoutingEngineTest"` exits 0
- [ ] `DirectMatcher.java` exists at `http-server/src/main/java/kafka/server/http/routing/DirectMatcher.java`
- [ ] `RoutingEngine.java` exists at `http-server/src/main/java/kafka/server/http/routing/RoutingEngine.java`
- [ ] Direct exchange routing matches exact routing key (test passes)
- [ ] E2E routing recurses with cycle detection (test passes)
- [ ] Topic/fanout/headers types throw UnsupportedOperationException (test passes)
- [ ] Unknown exchange throws IllegalArgumentException (test passes)
- [ ] Learning section filled with at least one entry
- [ ] Limitations section filled (use "None" if truly none)
- [ ] File Manifest section updated after commit

---

## File Manifest

> Filled by the executing agent after each commit.

<!-- ### YYYY-MM-DD — <short description> (commit <hash>)
Created:
  - path/to/NewFile.java — <what it does>
Modified:
  - path/to/Existing.java — <what changed>
-->
