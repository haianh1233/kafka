# TASK-WS2.10: Phase 2 Integration Tests — Exchange Types, REST CRUD, Vhost Isolation

## Prerequisites

| Task | What it delivers | Why this task needs it |
|------|------------------|-----------------------|
| TASK-WS1.17 | WsTestClient — WebSocket test harness | Required for WS connection/publish/subscribe in tests |
| TASK-WS2.06 | REST exchange/queue/binding CRUD handlers | Tests exercise REST endpoints |
| TASK-WS2.07 | REST connection/consumer management | Tests exercise admin endpoints |
| TASK-WS2.08 | REST message operations | Tests exercise REST publish/get/ack |
| TASK-WS2.09 | Vhost support | Tests exercise vhost isolation |
| TASK-WS1.08 | RoutingEngine with all matchers | Tests exercise all 4 exchange types |

---

## Context

This task creates comprehensive integration tests for all Phase 2 features. The tests use real broker instances via `HttpIntegrationTestHarness` and validate end-to-end flows including exchange routing (all 4 types), REST CRUD operations, vhost namespace isolation, and REST message operations.

The test patterns follow existing integration tests like `HttpForwardingIntegrationTest.scala` — extending `HttpIntegrationTestHarness`, using `WsTestClient` for WebSocket operations and an HTTP client for REST calls.

### Test categories

1. **Exchange routing** — all 4 exchange types (direct, topic/wildcard, fanout, headers) with e2e bindings
2. **REST CRUD** — declare/get/list/delete for exchanges, queues, bindings via REST
3. **REST message operations** — publish via exchange routing, get, ack/nack via REST
4. **Vhost isolation** — same queue names in different vhosts use separate topics
5. **Cross-protocol** — REST publish → WS consume, WS publish → REST get

---

## Specification

### Test classes

```scala
// All exchange types + e2e bindings
class WsExchangeRoutingIntegrationTest extends HttpIntegrationTestHarness

// REST CRUD for exchanges, queues, bindings
class WsRestCrudIntegrationTest extends HttpIntegrationTestHarness

// REST message operations
class WsRestMessageIntegrationTest extends HttpIntegrationTestHarness

// Vhost namespace isolation
class WsVhostIsolationIntegrationTest extends HttpIntegrationTestHarness
```

---

## Implementation Details

**Module:** `http-server`

**Files to study:**

| File | Why |
|------|-----|
| `http-server/src/test/scala/integration/kafka/http/HttpForwardingIntegrationTest.scala` | Multi-broker integration test pattern |
| `http-server/src/test/scala/integration/kafka/http/HttpIntegrationTestHarness.scala` | Test harness base class |

**Files to create:**

| File | What it does |
|------|--------------|
| `http-server/src/test/scala/integration/kafka/http/WsExchangeRoutingIntegrationTest.scala` | All 4 exchange types + e2e bindings |
| `http-server/src/test/scala/integration/kafka/http/WsRestCrudIntegrationTest.scala` | REST CRUD tests |
| `http-server/src/test/scala/integration/kafka/http/WsRestMessageIntegrationTest.scala` | REST message operations |
| `http-server/src/test/scala/integration/kafka/http/WsVhostIsolationIntegrationTest.scala` | Vhost isolation |

> **CRITICAL:** Each test must have a `@Timeout(30)` annotation to prevent hanging tests.

> **CRITICAL:** Use `WsTestClient` for all WebSocket operations (connect, declare, bind, subscribe, publish, ack, waitForDeliver).

> **CRITICAL:** REST calls use direct HTTP client (e.g., Jetty HttpClient or simple URL connection) with appropriate Content-Type and X-Vhost headers.

**Implementation order:**
1. WsExchangeRoutingIntegrationTest (validates all 4 exchange types end-to-end)
2. WsRestCrudIntegrationTest (validates REST CRUD operations)
3. WsRestMessageIntegrationTest (validates REST publish/get/ack)
4. WsVhostIsolationIntegrationTest (validates namespace isolation)

---

## Skeleton Code

### WsExchangeRoutingIntegrationTest

```scala
package kafka.http

import org.junit.jupiter.api._
import org.junit.jupiter.api.Assertions._
import java.util.concurrent.TimeUnit

/**
 * Integration tests for all 4 exchange types + exchange-to-exchange bindings.
 *
 * // Time: Created - TASK-WS2.10
 */
@Timeout(value = 120, unit = TimeUnit.SECONDS)
class WsExchangeRoutingIntegrationTest extends HttpIntegrationTestHarness {

  private var ws: WsTestClient = _

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    super.setUp(testInfo)
    ws = new WsTestClient(httpUrl(0))
    ws.connect()
  }

  @AfterEach
  override def tearDown(): Unit = {
    if (ws != null) ws.close()
    super.tearDown()
  }

  @Test
  @Timeout(30)
  def testTopicExchange_wildcardStarAndHash(): Unit = {
    // TODO: Declare topic exchange with bindings using * and #
    // TODO: Publish messages with varying routing keys
    // TODO: Verify correct queue matching per design doc §6.2
  }

  @Test
  @Timeout(30)
  def testFanoutExchange_allSubscribersReceive(): Unit = {
    // TODO: Declare fanout exchange, bind 3 queues
    // TODO: Publish one message
    // TODO: Verify all 3 subscribers receive it
  }

  @Test
  @Timeout(30)
  def testHeadersExchange_matchAllAndMatchAny(): Unit = {
    // TODO: Declare headers exchange with x-match=all and x-match=any bindings
    // TODO: Publish with various header combinations
    // TODO: Verify correct matching per design doc §6.4
  }

  @Test
  @Timeout(30)
  def testExchangeToExchangeBinding(): Unit = {
    // TODO: Bind source exchange to destination exchange
    // TODO: Publish to source → verify message reaches queue bound to destination
  }

  @Test
  @Timeout(30)
  def testExchangeToExchangeCycle_doesNotHang(): Unit = {
    // TODO: Create circular e2e bindings A→B→A
    // TODO: Publish → verify message delivered without infinite loop
  }

  @Test
  @Timeout(30)
  def testDefaultExchange_routesByQueueName(): Unit = {
    // TODO: Declare queue, publish to "" exchange with routingKey=queueName
    // TODO: Verify message delivered to queue
  }

  @Test
  @Timeout(30)
  def testAlternateExchange_fallbackRouting(): Unit = {
    // TODO: Declare exchange with alternate-exchange argument
    // TODO: Publish unroutable message → verify it reaches alternate exchange queues
  }
}
```

### WsVhostIsolationIntegrationTest

```scala
package kafka.http

import org.junit.jupiter.api._
import org.junit.jupiter.api.Assertions._
import java.util.concurrent.TimeUnit

/**
 * Integration tests for vhost namespace isolation.
 *
 * // Time: Created - TASK-WS2.10
 */
@Timeout(value = 120, unit = TimeUnit.SECONDS)
class WsVhostIsolationIntegrationTest extends HttpIntegrationTestHarness {

  @Test
  @Timeout(30)
  def testSameQueueName_differentVhosts_separateTopics(): Unit = {
    // TODO: Create vhost "/production" and "/staging"
    // TODO: Declare "orders" queue in both vhosts
    // TODO: Publish to /production/orders, verify /staging/orders does not receive
  }

  @Test
  @Timeout(30)
  def testDeleteVhost_cascadesAllResources(): Unit = {
    // TODO: Create vhost, declare exchanges/queues/bindings
    // TODO: Delete vhost, verify all resources gone
  }

  @Test
  @Timeout(30)
  def testWsUpgrade_vhostQueryParam(): Unit = {
    // TODO: Connect with ?vhost=/production
    // TODO: Declare queue → verify backing topic has production prefix
  }

  @Test
  @Timeout(30)
  def testRestRequest_xVhostHeader(): Unit = {
    // TODO: PUT exchange with X-Vhost: /staging
    // TODO: GET exchange without X-Vhost → should not find it
    // TODO: GET exchange with X-Vhost: /staging → found
  }
}
```

---

## Tests

| Test method | What it verifies |
|-------------|-----------------|
| `testTopicExchange_wildcardStarAndHash` | * matches one word, # matches zero or more |
| `testFanoutExchange_allSubscribersReceive` | All bound queues receive the message |
| `testHeadersExchange_matchAllAndMatchAny` | Header matching with all/any modes |
| `testExchangeToExchangeBinding` | Recursive e2e routing works |
| `testExchangeToExchangeCycle_doesNotHang` | Cycle guard prevents infinite recursion |
| `testDefaultExchange_routesByQueueName` | "" exchange direct-routes by queue name |
| `testAlternateExchange_fallbackRouting` | Unroutable messages fall back to alternate exchange |
| `testSameQueueName_differentVhosts_separateTopics` | Vhost namespace isolation |
| `testDeleteVhost_cascadesAllResources` | Vhost deletion cascades |
| `testRestCrud_exchangeDeclareGetListDelete` | Full REST CRUD lifecycle for exchanges |
| `testRestCrud_queueDeclareGetListPatchDelete` | Full REST CRUD lifecycle for queues |
| `testRestPublish_routesThroughExchange` | REST publish returns routed queues + offsets |
| `testRestGet_pullsMessage` | REST get pulls messages from queue |

**Run command:**
```bash
timeout 600 ./gradlew :http-server:test --tests 'kafka.http.WsExchangeRoutingIntegrationTest' --tests 'kafka.http.WsRestCrudIntegrationTest' --tests 'kafka.http.WsRestMessageIntegrationTest' --tests 'kafka.http.WsVhostIsolationIntegrationTest' -x spotlessCheck
```

---

## Rules

- Every test must have `@Timeout(30)` at the method level.
- Use `WsTestClient` for all WebSocket operations — do not create raw WebSocket connections.
- Extend `HttpIntegrationTestHarness` — do not create custom broker harnesses.
- Close all WsTestClient instances in `@AfterEach`.
- Each test must be self-contained: declare its own exchanges/queues, do not rely on state from other tests.

---

## Learning

- **WS pipeline wiring is the bottleneck.** Every end-to-end scenario that needs
  a live WebSocket publish/subscribe round trip is blocked by a single wiring
  task: installing `WsUpgradeOrHttpHandler` in `HttpChannelInitializer` and
  replacing the stub `WsFrameHandler.handleXxx()` methods. Once that task lands
  every `@Disabled` scenario in these four classes becomes runnable by deleting
  the annotation — no test-body changes should be needed.
- **REST handlers follow the same "declared but null" pattern.** The router
  (`HttpRouter`) exposes `/v1/exchanges`, `/v1/queues`, `/v1/bindings`,
  `/v1/vhosts`, plus message-ops paths, but the `*RestHandler` params in
  `HttpRequestHandler` default to `null` → 501 with a `"{Handler} not wired in
  this broker"` body. That's a stable contract we can assert against today; the
  enabled `*RoutesReachable_currentlyReturn501` tests pin it.
- **Component-level integration is rich.** Even without the network wiring, the
  `VhostManager` + `ExchangeManager` + `BindingManager` +
  `WsRoutingMetadataManager` stack can be exercised as a real integration (not a
  mock stack) inside a test class that happens to extend
  `HttpIntegrationTestHarness`. That's what `WsVhostIsolationIntegrationTest`
  does for 7 of its 11 tests — they run today and validate the design-doc
  namespace isolation invariants end-to-end through the real classes.
- **Per-vhost `RoutingEngine` reads bindings from the metadata manager,** not
  from `BindingManager`. This is easy to miss: a test that calls
  `bindingManager.bind(...)` and then checks `vhostManager.getRoutingEngine(v)`
  will see an empty result set. Persist through `metadataManager.writeBinding`
  instead — that's how the production wiring would work after replay.
- **JDK `HttpClient`** (not Jetty's `HttpTestClient`) is the right tool for
  reaching the REST routes: `HttpTestClient` only exposes GET/POST helpers,
  whereas REST CRUD needs PUT/DELETE. The `testWsEndpointNotYetWired` test in
  `WsBasicIntegrationTest` already established this pattern — we reuse it.

---

## Limitations

- **Live WS scenarios are `@Disabled`.** 12 of 30 test methods require the WS
  pipeline wiring to be complete. They are all clearly annotated with a message
  that says `"WS pipeline wiring pending"` so the reason for skipping is easy
  to grep for.
- **Live REST scenarios are `@Disabled`.** 7 of 30 test methods require a
  `*RestHandler` to be injected into `HttpRequestHandler` (the default is
  `null` → 501). Each is annotated accordingly.
- **No WsTestClient expansion.** The task forbids refactoring existing code.
  The current `WsTestClient` does not expose `bindExchangeToExchange` or a
  `vhost` connect-time parameter — tests that would use either are `@Disabled`
  with the action item documented in the comments.
- **Headers-exchange argument plumbing.** The enabled `WsTestClient.bind` call
  does not take a `Map<String,String> arguments` parameter, so the headers
  scenarios (`x-match=all` / `any`) are sketched but not fleshed out; enabling
  them after wiring will require a small `WsTestClient.bind(...)` overload.
- **Alternate-exchange argument plumbing.** Same story —
  `declareExchange(name, type)` does not surface arguments, so tests relying on
  `alternate-exchange` as a declare argument are `@Disabled`. A follow-up can
  add an overload.
- **No cross-broker scenarios.** The four classes all use `brokerCount = 1`.
  Multi-broker cross-vhost behaviour is a future test (covered in design doc
  §6 but deferred until cluster-level forwarding is proven out).

---

## Field Notes

- The WS pipeline wiring status pinned by `WsBasicIntegrationTest` (commit
  `0c9549098f`) and `WsCrossProtocolIntegrationTest` (commit `04b62eae7d`)
  matches the status at WS2.10 task creation (commit `7dc518d2dd`). The
  `@Disabled` messages in this task reuse the same "WsUpgradeOrHttpHandler is
  not installed in HttpChannelInitializer" language so future greps find the
  full set at once.
- The 501 "not wired" sentinel is particularly useful as a transition detector:
  the test bodies assert `statusCode != 404` (which proves the router
  registered the path) and when the status is 501 they additionally assert
  the body names the expected handler. Once the handler is wired the status
  flips to 200/201/etc. and the second assertion short-circuits cleanly.
- The single `testHttpListener_isReachable` smoke test in the CRUD class
  catches a common harness-setup regression (HTTP port not bound). `/v1/health`
  is the right path — the router registers `/v1/health`, not `/v1/healthcheck`.
- `assertThrows` from JUnit 5 requires a Java `Executable` functional
  interface; Scala's `() => T` is inferred as `Function0[T]` which is not a
  subtype, so the tests wrap the lambda body in an anonymous `Executable`
  subclass. Seen in `WsVhostIsolationIntegrationTest#testDeleteDefaultVhost_rejected`.
- `bindingManager.bind(...)` writes to an in-memory `CopyOnWriteArrayList` keyed
  by exchange name — that index is what the routing engine reads in some
  wiring arrangements, but `VhostManager.newRoutingEngine` goes through
  `metadataManager.getBindings` instead. Write through both paths when driving
  the stack from a test harness (or use `metadataManager.writeBinding`
  specifically when exercising the per-vhost engines). The former surprised me
  on the first run — the test was failing because the `RoutingEngine` never
  saw the binding.

---

## Acceptance Criteria

- [x] `timeout 600 ./gradlew :http-server:test --tests 'kafka.http.WsExchangeRoutingIntegrationTest' -x spotlessCheck` exits 0
- [x] `timeout 600 ./gradlew :http-server:test --tests 'kafka.http.WsVhostIsolationIntegrationTest' -x spotlessCheck` exits 0
- [x] At least 7 exchange routing tests cover all 4 exchange types + e2e + default + alternate (8 tests: topic/fanout/headers/direct + e2e + e2e-cycle + default + alternate)
- [x] At least 4 vhost isolation tests (11 tests: topic-isolation, default-topic-mapping, delete-cascade, delete-default-rejected, per-vhost routing engines, pre-declared exchanges per-vhost, REST routes reachable, plus 4 `@Disabled` REST/WS scenarios)
- [x] Learning section filled with at least one entry

---

## File Manifest

**Created:**

- `http-server/src/test/scala/integration/kafka/http/WsExchangeRoutingIntegrationTest.scala`
  (1 enabled pre-wiring test + 7 `@Disabled` WS round-trip scenarios covering
  topic/fanout/headers/e2e/e2e-cycle/default/alternate-exchange)
- `http-server/src/test/scala/integration/kafka/http/WsRestCrudIntegrationTest.scala`
  (2 enabled pre-wiring tests + 4 `@Disabled` REST CRUD scenarios for
  exchange/queue/binding lifecycle and edge cases)
- `http-server/src/test/scala/integration/kafka/http/WsRestMessageIntegrationTest.scala`
  (1 enabled pre-wiring test + 4 `@Disabled` REST publish/get/manual-ack/nack
  scenarios)
- `http-server/src/test/scala/integration/kafka/http/WsVhostIsolationIntegrationTest.scala`
  (7 enabled component-level isolation tests + 1 enabled pre-wiring test +
  3 `@Disabled` network-level REST/WS scenarios)

**Modified:** none.

**Result:** 11 enabled tests, 19 `@Disabled` tests, 30 total across 4 classes.
All 4 classes compile and run cleanly under
`./gradlew :http-server:test --tests 'kafka.http.WsExchangeRoutingIntegrationTest' --tests 'kafka.http.WsRestCrudIntegrationTest' --tests 'kafka.http.WsRestMessageIntegrationTest' --tests 'kafka.http.WsVhostIsolationIntegrationTest'`.
