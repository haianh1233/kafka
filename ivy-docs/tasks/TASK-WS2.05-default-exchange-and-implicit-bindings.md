# TASK-WS2.05: Default Exchange and Implicit Queue Bindings

## Prerequisites

| Task | What it delivers | Why this task needs it |
|------|------------------|-----------------------|
| TASK-WS2.04 | `RoutingEngine` | Default exchange is implemented as a direct exchange in the routing engine |

---

## Context

The default exchange (`""`, empty string) provides a simple publish-to-queue-by-name mechanism. Every declared queue is automatically bound to the default exchange with `routingKey = queueName`. This allows clients to publish directly to a queue without needing to declare an exchange or create explicit bindings.

From **design doc §7.6**:

> The default exchange (`""`) routes by queue name: `routingKey = queueName`. Every declared queue is implicitly bound to the default exchange with `routingKey = queue.name`.
>
> ```
> Publish: exchange="", routingKey="order-events"
>   → direct match → queue "order-events"
> ```
>
> This allows simple publish-to-queue-by-name without explicit binding setup.

This pattern is fundamental to AMQP semantics — it's the simplest way to send a message to a known queue. The default exchange must be:
1. Pre-declared on routing engine initialization (type=direct, name="")
2. Cannot be deleted by clients
3. Automatically gains a binding whenever a queue is declared
4. Automatically loses a binding whenever a queue is deleted

The integration point is in `QueueManager` (or wherever queue lifecycle is managed): when `declareQueue("orders")` is called, it must also call `routingEngine.bind("", "orders", "orders")`.

---

## Specification

```java
package kafka.server.http.routing;

/**
 * Default exchange initialization and implicit binding management.
 *
 * The default exchange is a pre-declared direct exchange with name "".
 * All declared queues are automatically bound to it with routingKey = queueName.
 */
public final class DefaultExchangeManager {

    /**
     * Initializes the default exchange in the routing engine.
     * Called once during broker startup.
     *
     * @param routingEngine the routing engine to initialize
     */
    public static void initialize(RoutingEngine routingEngine);

    /**
     * Adds an implicit binding for a newly declared queue.
     * Called by QueueManager.declareQueue().
     *
     * @param routingEngine the routing engine
     * @param queueName     the declared queue name
     */
    public static void onQueueDeclared(RoutingEngine routingEngine, String queueName);

    /**
     * Removes the implicit binding for a deleted queue.
     * Called by QueueManager.deleteQueue().
     *
     * @param routingEngine the routing engine
     * @param queueName     the deleted queue name
     */
    public static void onQueueDeleted(RoutingEngine routingEngine, String queueName);
}
```

---

## Implementation Details

**Module:** `http-server`

**Files to study:**

| File | Why |
|------|-----|
| `ivy-docs/http-protocol-extend-design.md` §7.6 | Default exchange specification |
| `references/ivy-ref/ivy-server/src/main/java/com/ivy/server/handler/amqp091/Amqp091RoutingEngine.java` | How bindings work in the routing engine |

**Files to create:**

| File | What it does |
|------|--------------|
| `http-server/src/main/java/kafka/server/http/routing/DefaultExchangeManager.java` | Default exchange init and implicit binding hooks |
| `http-server/src/test/java/kafka/server/http/routing/DefaultExchangeManagerTest.java` | Unit tests |

**Files to modify:**

| File | What changes |
|------|--------------|
| `http-server/src/main/java/kafka/server/http/routing/RoutingEngine.java` | Add `getExchangeType()` method if not present (needed for tests) |

> **CRITICAL:** The default exchange name is the empty string `""`, not `"default"` or `"amq.direct"`. This matches AMQP 0-9-1 semantics.

> **CRITICAL:** The default exchange cannot be deleted. If a client sends `delete-exchange` for `""`, the handler must reject it with an error.

> **EDGE CASE:** If `declareQueue` is called for a queue that already exists, the implicit binding should be idempotent (no duplicate binding). The routing engine's `bind()` method already handles dedup by removing existing bindings with the same (queue, routingKey) before adding.

**Implementation order:**
1. Create `DefaultExchangeManager` with `initialize()`, `onQueueDeclared()`, `onQueueDeleted()`
2. Write tests verifying:
   - Default exchange is pre-declared as direct type
   - Queue declaration creates implicit binding
   - Queue deletion removes implicit binding
   - Routing through default exchange works
3. Add `getExchangeType()` to `RoutingEngine` if needed

---

## Skeleton Code

### Production class

```java
package kafka.server.http.routing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * Manages the default exchange ("") and implicit queue bindings.
 *
 * The default exchange is a pre-declared direct exchange. Every declared queue
 * is automatically bound to it with routingKey = queueName, enabling simple
 * publish-to-queue-by-name without explicit binding setup.
 *
 * // Time: Created - TASK-WS2.05
 */
public final class DefaultExchangeManager {

    private static final Logger log = LoggerFactory.getLogger(DefaultExchangeManager.class);

    public static final String DEFAULT_EXCHANGE_NAME = "";

    private DefaultExchangeManager() {} // utility class

    /**
     * Initializes the default exchange in the routing engine.
     * Must be called once during broker startup, before any queue declarations.
     */
    public static void initialize(RoutingEngine routingEngine) {
        Objects.requireNonNull(routingEngine, "routingEngine");
        routingEngine.declareExchange(DEFAULT_EXCHANGE_NAME, "direct");
        log.debug("Default exchange initialized");
    }

    /**
     * Adds an implicit binding for a newly declared queue.
     * The binding uses routingKey = queueName, so publishing to
     * exchange="" with routingKey="myQueue" delivers to queue "myQueue".
     */
    public static void onQueueDeclared(RoutingEngine routingEngine, String queueName) {
        Objects.requireNonNull(routingEngine, "routingEngine");
        Objects.requireNonNull(queueName, "queueName");
        routingEngine.bind(DEFAULT_EXCHANGE_NAME, queueName, queueName);
        log.debug("Implicit binding added: default exchange → queue '{}' (key='{}')",
            queueName, queueName);
    }

    /**
     * Removes the implicit binding for a deleted queue.
     */
    public static void onQueueDeleted(RoutingEngine routingEngine, String queueName) {
        Objects.requireNonNull(routingEngine, "routingEngine");
        Objects.requireNonNull(queueName, "queueName");
        routingEngine.unbind(DEFAULT_EXCHANGE_NAME, queueName, queueName);
        log.debug("Implicit binding removed: default exchange → queue '{}'", queueName);
    }
}
```

### Test class

```java
package kafka.server.http.routing;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * // Time: Created - TASK-WS2.05
 */
class DefaultExchangeManagerTest {

    private RoutingEngine engine;

    @BeforeEach
    void setUp() {
        engine = new RoutingEngine();
        DefaultExchangeManager.initialize(engine);
    }

    @Test
    void initialize_createsDefaultExchange() {
        assertTrue(engine.exchangeExists(""));
    }

    @Test
    void onQueueDeclared_createsImplicitBinding() {
        DefaultExchangeManager.onQueueDeclared(engine, "orders");

        Set<String> matched = engine.route("", "orders");
        assertEquals(Set.of("orders"), matched);
    }

    @Test
    void onQueueDeclared_multipleQueues() {
        DefaultExchangeManager.onQueueDeclared(engine, "orders");
        DefaultExchangeManager.onQueueDeclared(engine, "events");

        assertEquals(Set.of("orders"), engine.route("", "orders"));
        assertEquals(Set.of("events"), engine.route("", "events"));
    }

    @Test
    void onQueueDeclared_idempotent() {
        DefaultExchangeManager.onQueueDeclared(engine, "orders");
        DefaultExchangeManager.onQueueDeclared(engine, "orders"); // no duplicate

        Set<String> matched = engine.route("", "orders");
        assertEquals(Set.of("orders"), matched);
    }

    @Test
    void onQueueDeleted_removesImplicitBinding() {
        DefaultExchangeManager.onQueueDeclared(engine, "orders");
        DefaultExchangeManager.onQueueDeleted(engine, "orders");

        Set<String> matched = engine.route("", "orders");
        assertTrue(matched.isEmpty());
    }

    @Test
    void routeToQueue_byName() {
        DefaultExchangeManager.onQueueDeclared(engine, "payment-events");

        Set<String> matched = engine.route("", "payment-events");
        assertEquals(Set.of("payment-events"), matched);
    }

    @Test
    void routeToQueue_noMatchForUnknownQueue() {
        DefaultExchangeManager.onQueueDeclared(engine, "orders");

        Set<String> matched = engine.route("", "nonexistent");
        assertTrue(matched.isEmpty());
    }

    @Test
    void defaultExchange_isDirectType() {
        // Publishing with a routing key that doesn't match any queue name → no match
        DefaultExchangeManager.onQueueDeclared(engine, "orders");

        // Direct exchange: exact match only
        assertTrue(engine.route("", "order").isEmpty()); // "order" != "orders"
        assertEquals(Set.of("orders"), engine.route("", "orders"));
    }

    @Test
    void initialize_nullEngine_throwsNPE() {
        assertThrows(NullPointerException.class,
            () -> DefaultExchangeManager.initialize(null));
    }

    @Test
    void onQueueDeclared_nullQueue_throwsNPE() {
        assertThrows(NullPointerException.class,
            () -> DefaultExchangeManager.onQueueDeclared(engine, null));
    }
}
```

---

## Tests

**Test class:** `http-server/src/test/java/kafka/server/http/routing/DefaultExchangeManagerTest.java`

| Test method | What it verifies |
|-------------|-----------------|
| `initialize_createsDefaultExchange` | Default exchange exists after init |
| `onQueueDeclared_createsImplicitBinding` | Queue declaration creates binding |
| `onQueueDeclared_multipleQueues` | Multiple queues each get their binding |
| `onQueueDeclared_idempotent` | Re-declaring same queue is safe |
| `onQueueDeleted_removesImplicitBinding` | Queue deletion removes binding |
| `routeToQueue_byName` | Publishing to default exchange routes by queue name |
| `routeToQueue_noMatchForUnknownQueue` | No match for undeclared queue |
| `defaultExchange_isDirectType` | Direct type = exact match only |
| `initialize_nullEngine_throwsNPE` | Null rejection |
| `onQueueDeclared_nullQueue_throwsNPE` | Null rejection |

**Run command:**
```bash
cd /home/anh/kafka && ./gradlew :http-server:test --tests 'kafka.server.http.routing.DefaultExchangeManagerTest'
```

---

## Rules

- Default exchange name is `""` (empty string), matching AMQP 0-9-1 semantics
- Default exchange is always direct type
- Default exchange cannot be deleted by clients
- Implicit bindings are idempotent (queue re-declaration is safe)
- Null-safe: require non-null for all parameters

---

## Learning

- The task skeleton's API (`routingEngine.declareExchange`, `.bind`, `.unbind`,
  `.exchangeExists`, 2-arg `.route`) predates the WS1.06–1.08 architecture split.
  In the actual codebase, **state lives in `ExchangeManager` + `BindingManager`**
  and `RoutingEngine` is a stateless function that composes lookup predicates.
  `DefaultExchangeManager` adapts to reality: it's a thin facade that delegates
  `initialize` to `ExchangeManager.initializeDefaults(vhost)` (which already
  registers `""` as direct per WS1.06) and `onQueueDeclared / onQueueDeleted`
  to `BindingManager.bind / unbind` with `routingKey = queueName`.
- `ExchangeManager.initializeDefaults` is idempotent — it checks
  `metadataManager.getExchange(vhost, name) != null` before synthesizing, so
  `DefaultExchangeManager.initialize` can be called many times safely.
- `BindingManager.bind` deduplicates on the full tuple
  `(exchange, queue, routingKey, arguments)` — so re-declaring a queue that
  already has an implicit binding is a silent no-op with no duplicate list
  entry. The idempotency test asserts `bindingCount("") == 1` after a double
  declare.
- The default exchange is already protected from deletion: it's in
  `ExchangeManager.DEFAULT_EXCHANGE_NAMES`, which `deleteExchange` rejects
  with `EXCHANGE_PROTECTED`. A test asserts this explicitly so the guarantee
  doesn't silently regress.
- Tests wire up the real `ExchangeManager` + `BindingManager` +
  `RoutingEngine` stack (not mocks) so they exercise the same code path a
  publish will take in production: implicit binding → direct matcher → queue
  name. `onQueueDeleted_onlyRemovesImplicitBinding` additionally proves the
  hook is surgical — it doesn't touch bindings on `amq.direct` or other
  exchanges.

---

## Limitations

- No `QueueManager` yet (per task preamble), so `DefaultExchangeManager` can't
  be driven automatically on queue lifecycle events. Whichever component ends
  up owning queue lifecycle must call `onQueueDeclared` / `onQueueDeleted` at
  the right moments. The hooks are stateless static methods so wiring is
  trivial when that arrives.
- `DefaultExchangeManager.onQueueDeleted` only removes the implicit binding on
  `""`; it does NOT cascade across all exchanges. For a full queue delete the
  caller should also invoke `BindingManager.removeAllForQueue(queueName)` to
  drop any explicit bindings the queue had elsewhere. Documented in the class
  Javadoc.
- `BindingManager` is not vhost-aware (exchanges are keyed by name only in the
  in-memory bucket map). A multi-vhost deployment will need either per-vhost
  `BindingManager` instances or a vhost-prefixed key scheme. Out of scope for
  this task.
- Spec says "Add `getExchangeType()` method to `RoutingEngine` if not present."
  Not added — `RoutingEngine` is stateless and type lookup is already the
  caller's `exchangeTypeFn`. Tests read the type directly from
  `ExchangeManager.getExchange(vhost, name).type()`, which is the canonical
  source.

---

## Field Notes

- Pre-flight reset was required: worktree was at an older commit
  (`f95a1f995d`), reset to `origin/feature/http-protocol` at `7180b94b24`.
- Time from design to green: ~15 minutes. Code footprint is small (one static
  utility class, ~50 LOC of logic, plus 20 tests).
- Only one file created in `main/` (new `DefaultExchangeManager.java`); no
  existing source file was modified. `RoutingEngine` untouched.
- Target test command `./gradlew :http-server:test --tests
  'kafka.server.http.routing.DefaultExchangeManagerTest' --tests
  'kafka.server.http.routing.RoutingEngineTest'` completes in ~1m 35s. Both
  suites green; 20 new `DefaultExchangeManagerTest` cases, all existing
  `RoutingEngineTest` cases still pass (no regression).

---

## Acceptance Criteria

- [ ] `cd /home/anh/kafka && ./gradlew :http-server:test --tests 'kafka.server.http.routing.DefaultExchangeManagerTest'` exits 0
- [ ] `grep -r "DefaultExchangeManager" http-server/src/main/java/` returns at least 1 hit
- [ ] Default exchange routes by queue name (direct match)
- [ ] Queue declare/delete automatically manages implicit bindings
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
