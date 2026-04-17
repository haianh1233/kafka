# TASK-WS1.07: BindingManager — Binding Lifecycle and Index

## Prerequisites

- **TASK-WS1.04** (WsRoutingMetadataManager) — Provides persistence layer for routing metadata. BindingManager delegates to WsRoutingMetadataManager to persist binding records to `__ws_routing_metadata` topic.
- **TASK-WS1.05** (ExchangeManager) — Provides `exchangeExists()` and `getExchangeType()` lookups. BindingManager validates that the target exchange exists before creating bindings.
- **TASK-WS1.06** (QueueManager) — Provides `queueExists()` lookup. BindingManager validates that the target queue exists before creating bindings.

---

## Context

The BindingManager connects exchanges to queues (and exchanges to exchanges) through binding records. In AMQP-style routing, a message published to an exchange is delivered to all queues whose bindings match the message's routing key and/or headers. Bindings are the edges in this routing graph.

This task implements the `BindingManager` class in the `routing/` package. It manages the full binding lifecycle: create (bind), remove (unbind), list, and cleanup. The in-memory index uses `ConcurrentHashMap<String, CopyOnWriteArrayList<Binding>>` keyed by exchange name, enabling fast lookup during message routing. A separate index handles exchange-to-exchange (e2e) bindings.

The design doc (§14.2) specifies that bindings are stored in the `__ws_routing_metadata` topic with key format `binding:{exchange}:{queue}:{routingKey}:{argsHash}`. The BindingManager maintains the in-memory index and delegates persistence to WsRoutingMetadataManager. Duplicate bind operations are idempotent — binding the same (exchange, queue, routingKey, arguments) tuple silently succeeds without creating a duplicate entry.

Resource limits are enforced: `ws.max.bindings.per.exchange` (default 10,000) prevents any single exchange from accumulating unbounded bindings. When an exchange or queue is deleted, all associated bindings are cleaned up automatically.

---

## Specification

### `Binding` — `kafka.server.http.routing.Binding`

```java
/**
 * Immutable binding record: connects a queue to an exchange with a routing key and arguments.
 */
public record Binding(
    String exchange,
    String queue,
    String routingKey,
    Map<String, String> arguments
) {
    public Binding {
        Objects.requireNonNull(exchange, "exchange");
        Objects.requireNonNull(queue, "queue");
        Objects.requireNonNull(routingKey, "routingKey");
        arguments = arguments == null ? Map.of() : Map.copyOf(arguments);
    }
}
```

### `E2EBinding` — `kafka.server.http.routing.E2EBinding`

```java
/**
 * Immutable exchange-to-exchange binding record.
 */
public record E2EBinding(
    String source,
    String destination,
    String routingKey,
    Map<String, String> arguments
) {
    public E2EBinding {
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(destination, "destination");
        Objects.requireNonNull(routingKey, "routingKey");
        arguments = arguments == null ? Map.of() : Map.copyOf(arguments);
    }
}
```

### `BindingManager` — `kafka.server.http.routing.BindingManager`

```java
/**
 * Manages binding lifecycle: bind, unbind, list, and cleanup.
 *
 * Thread-safe via ConcurrentHashMap + CopyOnWriteArrayList.
 */
public final class BindingManager {

    /**
     * Binds a queue to an exchange.
     * Idempotent: duplicate bindings silently succeed.
     *
     * @throws IllegalArgumentException if exchange does not exist
     * @throws IllegalArgumentException if queue does not exist
     * @throws IllegalStateException if max bindings per exchange exceeded
     */
    public void bind(String exchange, String queue, String routingKey, Map<String, String> arguments);

    /**
     * Creates an exchange-to-exchange binding.
     * Idempotent: duplicate e2e bindings silently succeed.
     */
    public void bindExchangeToExchange(String source, String destination, String routingKey, Map<String, String> arguments);

    /**
     * Removes a queue-to-exchange binding.
     * Silent no-op if binding does not exist.
     */
    public void unbind(String exchange, String queue, String routingKey);

    /**
     * Removes an exchange-to-exchange binding.
     */
    public void unbindExchangeFromExchange(String source, String destination, String routingKey);

    /** Returns all bindings for an exchange. */
    public List<Binding> listByExchange(String exchange);

    /** Returns all bindings that target a specific queue (across all exchanges). */
    public List<Binding> listByQueue(String queue);

    /** Returns all e2e bindings for a source exchange. */
    public List<E2EBinding> listE2EBySource(String source);

    /** Removes all bindings for an exchange (called when exchange is deleted). */
    public void removeAllForExchange(String exchange);

    /** Removes all bindings targeting a queue from all exchanges (called when queue is deleted). */
    public void removeAllForQueue(String queue);

    /** Returns total binding count across all exchanges. */
    public int totalBindingCount();
}
```

### Behavioral contracts

- `bind()` validates exchange exists via ExchangeManager and queue exists via QueueManager before adding.
- `bind()` checks `ws.max.bindings.per.exchange` (default 10,000) and throws `IllegalStateException` if exceeded.
- Duplicate detection: a binding is considered duplicate if (exchange, queue, routingKey, arguments) all match. Duplicate `bind()` is a silent no-op.
- `unbind()` removes exactly one matching binding. If no match, silent success (per AMQP spec).
- `removeAllForExchange()` clears all queue-to-exchange bindings AND all e2e bindings where this exchange is the source.
- `removeAllForQueue()` iterates all exchanges and removes bindings targeting the specified queue.
- All operations persist changes via WsRoutingMetadataManager (or a persistence callback in Phase 1 stub form).

---

## Implementation Details

**Module:** `http-server`

**Files to study:**

| File | Why |
|------|-----|
| `references/ivy-ref/ivy-server/src/main/java/com/ivy/server/handler/amqp091/Amqp091RoutingEngine.java` | Pattern for bind/unbind/removeQueue with CopyOnWriteArrayList |
| `ivy-docs/http-protocol-extend-design.md` §14.2 | In-memory cache structure and key formats |
| `ivy-docs/http-protocol-extend-design.md` §17 | `ws.max.bindings.per.exchange` = 10,000 |

```java
// From Amqp091RoutingEngine.java lines 86-101 — bind pattern:
public void bind(String exchange, String queue, String routingKey, Map<String, String> args) {
    ExchangeRouting routing = exchanges.get(exchange);
    if (routing == null) {
        log.warn("Bind to undeclared exchange '{}'", exchange);
        return;
    }
    var finalArgs = args != null ? args : Map.<String, String>of();
    routing.bindings.removeIf(b -> b.queue().equals(queue) && b.routingKey().equals(routingKey));
    routing.bindings.add(new Binding(queue, routingKey, finalArgs));
}
```

```java
// From Amqp091RoutingEngine.java lines 299-303 — removeQueue pattern:
public void removeQueue(String queue) {
    for (ExchangeRouting routing : exchanges.values()) {
        routing.bindings.removeIf(b -> b.queue().equals(queue));
    }
}
```

**Files to create:**

| File | What it does |
|------|--------------|
| `http-server/src/main/java/kafka/server/http/routing/Binding.java` | Immutable binding record |
| `http-server/src/main/java/kafka/server/http/routing/E2EBinding.java` | Immutable exchange-to-exchange binding record |
| `http-server/src/main/java/kafka/server/http/routing/BindingManager.java` | Binding lifecycle management with in-memory index |
| `http-server/src/test/java/kafka/server/http/routing/BindingManagerTest.java` | Unit tests |

> **CRITICAL:** The dedup check in `bind()` must compare ALL four fields (exchange, queue, routingKey, arguments). The ivy-ref only compares (queue, routingKey) which loses argument information for headers exchange bindings. For our implementation, include arguments in the equality check.

> **CRITICAL:** `CopyOnWriteArrayList.removeIf()` is atomic — it takes a snapshot internally. But `bind()` (check-then-add) is NOT atomic. For Phase 1 with single-threaded metadata replay, this is acceptable. Document this as a limitation.

**Implementation order:**
1. Create `Binding.java` record
2. Create `E2EBinding.java` record
3. Create `BindingManager.java` with in-memory indexes
4. Implement `bind()` with validation and dedup
5. Implement `unbind()` and list methods
6. Implement cleanup methods (`removeAllForExchange`, `removeAllForQueue`)
7. Write tests

---

## Skeleton Code

### `Binding.java`

```java
package kafka.server.http.routing;

import java.util.Map;
import java.util.Objects;

/**
 * Immutable binding record connecting a queue to an exchange.
 *
 * // Time: Created - TASK-WS1.07
 */
public record Binding(
    String exchange,
    String queue,
    String routingKey,
    Map<String, String> arguments
) {
    public Binding {
        Objects.requireNonNull(exchange, "exchange");
        Objects.requireNonNull(queue, "queue");
        Objects.requireNonNull(routingKey, "routingKey");
        arguments = arguments == null ? Map.of() : Map.copyOf(arguments);
    }
}
```

### `E2EBinding.java`

```java
package kafka.server.http.routing;

import java.util.Map;
import java.util.Objects;

/**
 * Immutable exchange-to-exchange binding record.
 *
 * // Time: Created - TASK-WS1.07
 */
public record E2EBinding(
    String source,
    String destination,
    String routingKey,
    Map<String, String> arguments
) {
    public E2EBinding {
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(destination, "destination");
        Objects.requireNonNull(routingKey, "routingKey");
        arguments = arguments == null ? Map.of() : Map.copyOf(arguments);
    }
}
```

### `BindingManager.java`

```java
package kafka.server.http.routing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Predicate;

/**
 * Manages binding lifecycle: bind, unbind, list, and cleanup.
 *
 * In-memory index backed by ConcurrentHashMap + CopyOnWriteArrayList.
 * Exchange-name keyed for fast lookup during message routing.
 *
 * // Time: Created - TASK-WS1.07
 */
public final class BindingManager {

    private static final Logger log = LoggerFactory.getLogger(BindingManager.class);

    // --- Configuration ---
    private static final int DEFAULT_MAX_BINDINGS_PER_EXCHANGE = 10_000;

    private final int maxBindingsPerExchange;

    // --- In-memory indexes ---
    // exchange name → list of queue bindings
    private final ConcurrentHashMap<String, CopyOnWriteArrayList<Binding>> bindingsByExchange =
        new ConcurrentHashMap<>();

    // source exchange name → list of e2e bindings
    private final ConcurrentHashMap<String, CopyOnWriteArrayList<E2EBinding>> e2eBindingsBySource =
        new ConcurrentHashMap<>();

    // --- Dependencies (Phase 1: functional interfaces for loose coupling) ---
    private final Predicate<String> exchangeExistsFn;
    private final Predicate<String> queueExistsFn;

    /**
     * @param maxBindingsPerExchange max bindings per exchange (ws.max.bindings.per.exchange)
     * @param exchangeExistsFn       checks if an exchange exists (from ExchangeManager)
     * @param queueExistsFn          checks if a queue exists (from QueueManager)
     */
    public BindingManager(int maxBindingsPerExchange,
                          Predicate<String> exchangeExistsFn,
                          Predicate<String> queueExistsFn) {
        this.maxBindingsPerExchange = maxBindingsPerExchange;
        this.exchangeExistsFn = Objects.requireNonNull(exchangeExistsFn, "exchangeExistsFn");
        this.queueExistsFn = Objects.requireNonNull(queueExistsFn, "queueExistsFn");
    }

    public BindingManager(Predicate<String> exchangeExistsFn, Predicate<String> queueExistsFn) {
        this(DEFAULT_MAX_BINDINGS_PER_EXCHANGE, exchangeExistsFn, queueExistsFn);
    }

    /**
     * Binds a queue to an exchange with a routing key and arguments.
     * Idempotent: duplicate bindings (same exchange, queue, routingKey, arguments) silently succeed.
     *
     * @param exchange   exchange name
     * @param queue      queue name
     * @param routingKey routing key
     * @param arguments  binding arguments (for headers exchange)
     * @throws IllegalArgumentException if exchange does not exist
     * @throws IllegalArgumentException if queue does not exist
     * @throws IllegalStateException    if max bindings per exchange exceeded
     */
    public void bind(String exchange, String queue, String routingKey, Map<String, String> arguments) {
        Objects.requireNonNull(exchange, "exchange");
        Objects.requireNonNull(queue, "queue");
        Objects.requireNonNull(routingKey, "routingKey");

        // TODO: 1. Validate exchange exists via exchangeExistsFn
        // TODO: 2. Validate queue exists via queueExistsFn
        // TODO: 3. Get or create CopyOnWriteArrayList for this exchange
        // TODO: 4. Check for duplicate: same (exchange, queue, routingKey, arguments) → return silently
        // TODO: 5. Check max bindings limit
        // TODO: 6. Add new Binding to the list
        // TODO: 7. Log the binding creation
        throw new UnsupportedOperationException("Not yet implemented");
    }

    /**
     * Convenience overload with no arguments.
     */
    public void bind(String exchange, String queue, String routingKey) {
        bind(exchange, queue, routingKey, Map.of());
    }

    /**
     * Creates an exchange-to-exchange binding.
     * Idempotent: duplicate e2e bindings silently succeed.
     *
     * @param source      source exchange name
     * @param destination destination exchange name
     * @param routingKey  routing key for the e2e binding
     * @param arguments   binding arguments
     * @throws IllegalArgumentException if source exchange does not exist
     * @throws IllegalArgumentException if destination exchange does not exist
     */
    public void bindExchangeToExchange(String source, String destination,
                                       String routingKey, Map<String, String> arguments) {
        // TODO: 1. Validate both exchanges exist
        // TODO: 2. Get or create CopyOnWriteArrayList for source
        // TODO: 3. Check for duplicate
        // TODO: 4. Add new E2EBinding
        throw new UnsupportedOperationException("Not yet implemented");
    }

    /**
     * Removes a queue-to-exchange binding.
     * Silent no-op if binding does not exist (per AMQP spec).
     */
    public void unbind(String exchange, String queue, String routingKey) {
        // TODO: 1. Get bindings list for exchange
        // TODO: 2. removeIf matching (queue, routingKey)
        // TODO: 3. Log the unbind
        throw new UnsupportedOperationException("Not yet implemented");
    }

    /**
     * Removes an exchange-to-exchange binding.
     */
    public void unbindExchangeFromExchange(String source, String destination, String routingKey) {
        // TODO: removeIf from e2eBindingsBySource
        throw new UnsupportedOperationException("Not yet implemented");
    }

    /**
     * Returns all bindings for an exchange (unmodifiable snapshot).
     */
    public List<Binding> listByExchange(String exchange) {
        // TODO: return snapshot from bindingsByExchange, or empty list
        throw new UnsupportedOperationException("Not yet implemented");
    }

    /**
     * Returns all bindings that target a specific queue (across all exchanges).
     */
    public List<Binding> listByQueue(String queue) {
        // TODO: iterate all exchanges, collect bindings where queue matches
        throw new UnsupportedOperationException("Not yet implemented");
    }

    /**
     * Returns all e2e bindings for a source exchange.
     */
    public List<E2EBinding> listE2EBySource(String source) {
        // TODO: return snapshot from e2eBindingsBySource, or empty list
        throw new UnsupportedOperationException("Not yet implemented");
    }

    /**
     * Removes all bindings for an exchange (called when exchange is deleted).
     * Removes both queue bindings and e2e bindings where this exchange is the source.
     */
    public void removeAllForExchange(String exchange) {
        // TODO: 1. Remove from bindingsByExchange
        // TODO: 2. Remove from e2eBindingsBySource
        // TODO: 3. Remove e2e bindings in OTHER exchanges that target this exchange as destination
        throw new UnsupportedOperationException("Not yet implemented");
    }

    /**
     * Removes all bindings targeting a queue from all exchanges.
     */
    public void removeAllForQueue(String queue) {
        // TODO: iterate all entries in bindingsByExchange, removeIf b.queue().equals(queue)
        throw new UnsupportedOperationException("Not yet implemented");
    }

    /**
     * Returns total binding count across all exchanges (queue bindings only).
     */
    public int totalBindingCount() {
        // TODO: sum all list sizes in bindingsByExchange
        throw new UnsupportedOperationException("Not yet implemented");
    }
}
```

### Test class — `BindingManagerTest.java`

```java
package kafka.server.http.routing;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * // Time: Created - TASK-WS1.07
 */
class BindingManagerTest {

    private BindingManager manager;
    private Set<String> existingExchanges;
    private Set<String> existingQueues;

    @BeforeEach
    void setUp() {
        existingExchanges = new java.util.HashSet<>(Set.of("events", "orders", "broadcast", "source", "dest"));
        existingQueues = new java.util.HashSet<>(Set.of("order-events", "payment-events", "audit-log", "all-events"));
        manager = new BindingManager(
            existingExchanges::contains,
            existingQueues::contains
        );
    }

    // --- bind() ---

    @Test
    void bind_validExchangeAndQueue_succeeds() {
        manager.bind("events", "order-events", "order.created");
        var bindings = manager.listByExchange("events");
        assertEquals(1, bindings.size());
        assertEquals("order-events", bindings.get(0).queue());
        assertEquals("order.created", bindings.get(0).routingKey());
    }

    @Test
    void bind_multipleBindings_allStored() {
        manager.bind("events", "order-events", "order.created");
        manager.bind("events", "payment-events", "payment.completed");
        assertEquals(2, manager.listByExchange("events").size());
    }

    @Test
    void bind_duplicateBinding_idempotent() {
        manager.bind("events", "order-events", "order.created");
        manager.bind("events", "order-events", "order.created");
        assertEquals(1, manager.listByExchange("events").size());
    }

    @Test
    void bind_sameQueueDifferentRoutingKey_createsBoth() {
        manager.bind("events", "order-events", "order.created");
        manager.bind("events", "order-events", "order.updated");
        assertEquals(2, manager.listByExchange("events").size());
    }

    @Test
    void bind_exchangeNotFound_throws() {
        assertThrows(IllegalArgumentException.class,
            () -> manager.bind("nonexistent", "order-events", "key"));
    }

    @Test
    void bind_queueNotFound_throws() {
        assertThrows(IllegalArgumentException.class,
            () -> manager.bind("events", "nonexistent", "key"));
    }

    @Test
    void bind_exceedsMaxBindings_throws() {
        var limited = new BindingManager(2, existingExchanges::contains, existingQueues::contains);
        limited.bind("events", "order-events", "key1");
        limited.bind("events", "payment-events", "key2");
        assertThrows(IllegalStateException.class,
            () -> limited.bind("events", "audit-log", "key3"));
    }

    @Test
    void bind_withArguments_storedCorrectly() {
        var args = Map.of("x-match", "all", "priority", "high");
        manager.bind("events", "order-events", "", args);
        var bindings = manager.listByExchange("events");
        assertEquals(1, bindings.size());
        assertEquals("all", bindings.get(0).arguments().get("x-match"));
    }

    @Test
    void bind_sameQueueKeyDifferentArgs_createsBoth() {
        manager.bind("events", "order-events", "", Map.of("x-match", "all", "priority", "high"));
        manager.bind("events", "order-events", "", Map.of("x-match", "any", "region", "us"));
        assertEquals(2, manager.listByExchange("events").size());
    }

    @Test
    void bind_nullArguments_defaultsToEmptyMap() {
        manager.bind("events", "order-events", "key", null);
        assertEquals(Map.of(), manager.listByExchange("events").get(0).arguments());
    }

    // --- unbind() ---

    @Test
    void unbind_existingBinding_removes() {
        manager.bind("events", "order-events", "order.created");
        manager.unbind("events", "order-events", "order.created");
        assertTrue(manager.listByExchange("events").isEmpty());
    }

    @Test
    void unbind_nonExistentBinding_silentSuccess() {
        assertDoesNotThrow(() -> manager.unbind("events", "order-events", "nonexistent"));
    }

    @Test
    void unbind_nonExistentExchange_silentSuccess() {
        assertDoesNotThrow(() -> manager.unbind("nonexistent", "order-events", "key"));
    }

    // --- listByQueue() ---

    @Test
    void listByQueue_acrossMultipleExchanges() {
        manager.bind("events", "order-events", "order.created");
        manager.bind("orders", "order-events", "payment.done");
        var bindings = manager.listByQueue("order-events");
        assertEquals(2, bindings.size());
    }

    @Test
    void listByQueue_noBindings_returnsEmpty() {
        assertTrue(manager.listByQueue("order-events").isEmpty());
    }

    // --- e2e bindings ---

    @Test
    void bindE2E_validExchanges_succeeds() {
        manager.bindExchangeToExchange("source", "dest", "order.*", Map.of());
        var e2eBindings = manager.listE2EBySource("source");
        assertEquals(1, e2eBindings.size());
        assertEquals("dest", e2eBindings.get(0).destination());
    }

    @Test
    void bindE2E_duplicate_idempotent() {
        manager.bindExchangeToExchange("source", "dest", "key", Map.of());
        manager.bindExchangeToExchange("source", "dest", "key", Map.of());
        assertEquals(1, manager.listE2EBySource("source").size());
    }

    @Test
    void unbindE2E_removes() {
        manager.bindExchangeToExchange("source", "dest", "key", Map.of());
        manager.unbindExchangeFromExchange("source", "dest", "key");
        assertTrue(manager.listE2EBySource("source").isEmpty());
    }

    // --- cleanup ---

    @Test
    void removeAllForExchange_clearsAllBindings() {
        manager.bind("events", "order-events", "key1");
        manager.bind("events", "payment-events", "key2");
        manager.bindExchangeToExchange("events", "dest", "key3", Map.of());
        manager.removeAllForExchange("events");
        assertTrue(manager.listByExchange("events").isEmpty());
        assertTrue(manager.listE2EBySource("events").isEmpty());
    }

    @Test
    void removeAllForExchange_cleansUpE2EDestinations() {
        manager.bindExchangeToExchange("source", "events", "key", Map.of());
        manager.removeAllForExchange("events");
        // e2e bindings where events is the DESTINATION should also be removed
        var e2e = manager.listE2EBySource("source");
        assertTrue(e2e.stream().noneMatch(b -> b.destination().equals("events")));
    }

    @Test
    void removeAllForQueue_removesFromAllExchanges() {
        manager.bind("events", "order-events", "key1");
        manager.bind("orders", "order-events", "key2");
        manager.bind("events", "payment-events", "key3");
        manager.removeAllForQueue("order-events");
        assertTrue(manager.listByQueue("order-events").isEmpty());
        assertEquals(1, manager.listByExchange("events").size());
    }

    // --- totalBindingCount ---

    @Test
    void totalBindingCount_sumsAllExchanges() {
        manager.bind("events", "order-events", "key1");
        manager.bind("events", "payment-events", "key2");
        manager.bind("orders", "audit-log", "key3");
        assertEquals(3, manager.totalBindingCount());
    }

    @Test
    void totalBindingCount_empty_returnsZero() {
        assertEquals(0, manager.totalBindingCount());
    }

    // --- Binding record ---

    @Test
    void bindingRecord_nullExchange_throwsNPE() {
        assertThrows(NullPointerException.class, () -> new Binding(null, "q", "k", Map.of()));
    }

    @Test
    void bindingRecord_nullQueue_throwsNPE() {
        assertThrows(NullPointerException.class, () -> new Binding("ex", null, "k", Map.of()));
    }

    @Test
    void bindingRecord_nullArguments_defaultsToEmptyMap() {
        var b = new Binding("ex", "q", "k", null);
        assertEquals(Map.of(), b.arguments());
    }

    @Test
    void bindingRecord_argumentsAreImmutable() {
        var args = new java.util.HashMap<String, String>();
        args.put("key", "value");
        var b = new Binding("ex", "q", "k", args);
        assertThrows(UnsupportedOperationException.class, () -> b.arguments().put("new", "val"));
    }
}
```

### Existing pattern reference

```java
// From Amqp091RoutingEngine.java lines 86-101 — bind with dedup:
public void bind(String exchange, String queue, String routingKey, Map<String, String> args) {
    ExchangeRouting routing = exchanges.get(exchange);
    if (routing == null) {
        log.warn("Bind to undeclared exchange '{}'", exchange);
        return;
    }
    var finalArgs = args != null ? args : Map.<String, String>of();
    routing.bindings.removeIf(b -> b.queue().equals(queue) && b.routingKey().equals(routingKey));
    routing.bindings.add(new Binding(queue, routingKey, finalArgs));
}

// From Amqp091RoutingEngine.java lines 299-303 — removeQueue:
public void removeQueue(String queue) {
    for (ExchangeRouting routing : exchanges.values()) {
        routing.bindings.removeIf(b -> b.queue().equals(queue));
    }
}
```

---

## Tests

**Test class:** `http-server/src/test/java/kafka/server/http/routing/BindingManagerTest.java`

| Test method | What it verifies |
|-------------|-----------------|
| `bind_validExchangeAndQueue_succeeds` | Happy path binding creation |
| `bind_multipleBindings_allStored` | Multiple bindings on same exchange |
| `bind_duplicateBinding_idempotent` | Duplicate bind is silent no-op |
| `bind_sameQueueDifferentRoutingKey_createsBoth` | Different routing keys create separate bindings |
| `bind_exchangeNotFound_throws` | Exchange validation |
| `bind_queueNotFound_throws` | Queue validation |
| `bind_exceedsMaxBindings_throws` | Resource limit enforcement |
| `bind_withArguments_storedCorrectly` | Arguments preserved in binding record |
| `bind_sameQueueKeyDifferentArgs_createsBoth` | Different args create distinct bindings |
| `bind_nullArguments_defaultsToEmptyMap` | Null args normalized to empty map |
| `unbind_existingBinding_removes` | Unbind removes the binding |
| `unbind_nonExistentBinding_silentSuccess` | Missing binding is silent no-op |
| `unbind_nonExistentExchange_silentSuccess` | Missing exchange is silent no-op |
| `listByQueue_acrossMultipleExchanges` | Cross-exchange queue lookup |
| `listByQueue_noBindings_returnsEmpty` | Empty result for unbound queue |
| `bindE2E_validExchanges_succeeds` | E2E binding creation |
| `bindE2E_duplicate_idempotent` | E2E duplicate is silent no-op |
| `unbindE2E_removes` | E2E unbind works |
| `removeAllForExchange_clearsAllBindings` | Exchange deletion cleanup |
| `removeAllForExchange_cleansUpE2EDestinations` | E2E destination cleanup |
| `removeAllForQueue_removesFromAllExchanges` | Queue deletion cleanup across exchanges |
| `totalBindingCount_sumsAllExchanges` | Count aggregation |
| `totalBindingCount_empty_returnsZero` | Zero count for empty manager |
| `bindingRecord_nullExchange_throwsNPE` | Record validates null exchange |
| `bindingRecord_nullQueue_throwsNPE` | Record validates null queue |
| `bindingRecord_nullArguments_defaultsToEmptyMap` | Null args normalization in record |
| `bindingRecord_argumentsAreImmutable` | Defensive copy in record |

**Run command:**
```bash
./gradlew :http-server:test --tests "kafka.server.http.routing.BindingManagerTest"
```

---

## Rules

- Thread-safe via ConcurrentHashMap + CopyOnWriteArrayList (design doc §14.2, §18.4).
- Duplicate bindings silently succeed — idempotent operations (AMQP spec).
- Resource limit: `ws.max.bindings.per.exchange` (default 10,000) enforced on bind.
- When exchange deleted, remove ALL its bindings (queue and e2e). When queue deleted, remove from ALL exchanges.
- Binding record arguments must be immutable (use `Map.copyOf()` in compact constructor).

---

## Learning

- **Per-exchange monitor + COW list gives correctness without global locks.** The bind path is a check-then-add (scan for dup → check quota → append). A `CopyOnWriteArrayList` alone is not enough — two concurrent identical `bind()` calls could both pass the scan and both append. A `ConcurrentHashMap<String, Object>` of per-exchange monitors, entered as `synchronized(lockFor(exchange))`, serializes only the critical section for a given exchange while leaving reads and cross-exchange writes fully concurrent. The concurrency stress test (`concurrentBindUnbind_doesNotCorruptIndex`) catches this — drop the synchronized block and the listByExchange count diverges from the sum across queues under 8-thread contention.
- **Dedup must compare `arguments`, not just `(queue, routingKey)`.** The ivy-ref `Amqp091RoutingEngine.bind()` comment in the task spec explicitly calls this out. Headers-exchange bindings with the same routing key but different `x-match` rules are semantically distinct; using `Binding.equals()` (record-default, compares all four fields) as the dedup predicate preserves them correctly. The test `bind_sameQueueKeyDifferentArgs_createsBoth` is the regression guard.
- **`unbind()` matches on `(queue, routingKey)` only, not arguments.** This is deliberate and matches AMQP 0-9-1 semantics — the broker cannot expect clients to re-send the full argument map on unbind. It implies that `bind()` with different args followed by a single `unbind()` deletes ALL matching bindings for that (queue, routingKey). Documented in the Javadoc.
- **`CopyOnWriteArrayList.removeIf` is atomic wrt iterators** — we do not need the synchronized block for unbind or for cleanup (removeAllForExchange / removeAllForQueue), only for the check-then-add in bind. This matters: cascade cleanup can run concurrently with reads on other exchanges without contention.
- **`List.copyOf()` returns a truly immutable snapshot.** This is the right API for `listByExchange` — the caller gets a frozen view that cannot observe concurrent mutations even if the underlying COW list is modified later, which is what `listByExchange_returnsSnapshot_notLiveView` asserts.
- **Functional-predicate injection for validation is cheap and testable.** The manager takes `Predicate<String> exchangeExistsFn` / `queueExistsFn` instead of concrete manager references. Tests pass `Set::contains`; production wiring will pass `exchangeManager::exchangeExists` / `queueManager::queueExists` method references. No test doubles required.

---

## Limitations

- **Not integrated with `WsRoutingMetadataManager.writeBinding` / `deleteBinding`** — this task follows the authoritative task spec (§Specification), which treats persistence as "a persistence callback in Phase 1 stub form". Bindings are in-memory only; they do not survive broker restart and do not replay across nodes. Integration with the metadata topic is a follow-up task (likely as part of WS1.11 / publish-handler wiring, where bindings first need to be durable for routing to be correct on restart).
- **Not vhost-aware.** The task spec uses a flat exchange-name keyspace (no `(vhost, exchange)` tuple), whereas ExchangeManager and WsRoutingMetadataManager are vhost-aware. For Phase 1 single-vhost ("/") this is fine; multi-vhost support (planned for WS2.09) will require adding a vhost dimension to both the `bindingsByExchange` key and the constructor predicates. The callsite will need to pass a vhost-scoped predicate (e.g. `name -> exchangeManager.getExchange(vhost, name) != null`).
- **`queueExistsFn` is a placeholder — QueueManager does not yet exist.** The task spec's `queueExistsFn` is declared as a functional interface specifically so BindingManager can be built and tested before QueueManager (which is a separate, deferred task) lands. Once QueueManager exists, the production wiring will pass `queueManager::queueExists` as the predicate; no changes to BindingManager are required.
- **Default exchange `""` is not special-cased.** AMQP 0-9-1 auto-binds every queue to the default exchange with the queue name as the routing key. BindingManager does not handle this — the caller must explicitly `bind("", queueName, queueName, Map.of())` when a queue is declared. Wiring this into QueueManager is deferred.
- **`bindingCount(exchange)` reflects only in-memory state.** If the metadata topic integration lands later and does an async replay, callers relying on this for quota decisions during replay may observe stale counts momentarily. Not an issue while Phase 1 is in-memory only.

---

## Field Notes

- Package placement: the task file specifies `kafka.server.http.routing` (peer of `ExchangeManager`), so this task uses that package rather than `kafka.server.http.ws` despite the invocation preamble's alternative guidance. ExchangeManager already lives there; BindingManager fits alongside it.
- Chose `ConcurrentHashMap<String, Object>` for per-exchange locks rather than one big `synchronized(this)` or a `ReadWriteLock`. The former would serialize all exchanges against each other; the latter is heavier and `bind()` needs write-mode for the check-then-add so it would degenerate to an exclusive lock anyway. Per-exchange monitors keep the hot path (reads + cross-exchange writes) fully parallel.
- `totalBindingCount()` iterates the exchange map once; with ConcurrentHashMap it's weakly consistent (may miss concurrent inserts) but does not throw `ConcurrentModificationException`. Good enough for metrics; do not use for correctness decisions.
- `removeAllForExchange()` does three things atomically-enough: (1) remove the exchange's own queue-bindings bucket, (2) remove its e2e-source bucket, (3) scan all *other* e2e buckets and remove bindings whose `destination` equals this exchange. Step 3 is O(total e2e bindings) but is only called on exchange deletion so the cost is acceptable.
- The concurrency stress test runs 8 threads × 500 ops = 4000 bind/unbind pairs against the same exchange. With the synchronized block present the final list size always equals the sum across queues; without it the invariant breaks.

---

## File Manifest

### 2026-04-17 — WS1.07 BindingManager (commit 8ca2e9f43b)
Created:
  - http-server/src/main/java/kafka/server/http/routing/Binding.java — immutable queue-to-exchange binding record (all four fields in equality)
  - http-server/src/main/java/kafka/server/http/routing/E2EBinding.java — immutable exchange-to-exchange binding record
  - http-server/src/main/java/kafka/server/http/routing/BindingManager.java — bind/unbind/list/cleanup with dual in-memory index, per-exchange monitors, per-exchange quota
  - http-server/src/test/java/kafka/server/http/routing/BindingManagerTest.java — 48 test methods (bind, unbind, listByExchange, listByQueue, e2e bindings, cleanup, quota, concurrency, record invariants)

---

## Acceptance Criteria

- [ ] `./gradlew :http-server:test --tests "kafka.server.http.routing.BindingManagerTest"` exits 0
- [ ] `Binding.java` exists at `http-server/src/main/java/kafka/server/http/routing/Binding.java`
- [ ] `E2EBinding.java` exists at `http-server/src/main/java/kafka/server/http/routing/E2EBinding.java`
- [ ] `BindingManager.java` exists at `http-server/src/main/java/kafka/server/http/routing/BindingManager.java`
- [ ] Duplicate bindings are idempotent (test passes)
- [ ] Max bindings per exchange enforced (test passes)
- [ ] Exchange deletion cleans up all bindings (test passes)
- [ ] Queue deletion cleans up bindings across all exchanges (test passes)
- [ ] E2E bindings created and cleaned up properly (test passes)
- [ ] Binding record arguments are immutable (`Map.copyOf()`)
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
