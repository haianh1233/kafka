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

_To be filled by the executing agent._

---

## Limitations

_To be filled by the executing agent._

---

## Field Notes

_To be filled by the executing agent._

---

## Acceptance Criteria

- [ ] `timeout 600 ./gradlew :http-server:test --tests 'kafka.http.WsExchangeRoutingIntegrationTest' -x spotlessCheck` exits 0
- [ ] `timeout 600 ./gradlew :http-server:test --tests 'kafka.http.WsVhostIsolationIntegrationTest' -x spotlessCheck` exits 0
- [ ] At least 7 exchange routing tests cover all 4 exchange types + e2e + default + alternate
- [ ] At least 4 vhost isolation tests
- [ ] Learning section filled with at least one entry

---

## File Manifest

_To be filled by the executing agent._
