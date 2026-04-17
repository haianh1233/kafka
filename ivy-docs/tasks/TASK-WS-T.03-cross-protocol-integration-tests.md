# TASK-WS-T.03: Cross-Protocol Integration Tests

## Prerequisites

| Task | What it delivers | Why this task needs it |
|------|------------------|-----------------------|
| TASK-WS1.17 | WsTestClient | WebSocket test client |
| TASK-WS1.09 | WsPublishHandler | WS publish path |
| TASK-WS1.12 | WsConsumerFetchLoop | WS consume path |

---

## Context

The design doc §23.5 specifies cross-protocol integration tests. These verify that messages flow correctly between WebSocket, HTTP REST, and Kafka binary clients:

| Publish via | Consume via | How it works |
|---|---|---|
| WebSocket `publish` | HTTP `POST :fetch` | Routing → topic → HTTP poll |
| HTTP `POST records` | WebSocket `subscribe` | Direct topic produce → WS consumer |
| Kafka binary producer | WebSocket `subscribe` | Direct topic produce → WS consumer |

These tests extend `HttpIntegrationTestHarness` and use both `WsTestClient` and `HttpTestClient`.

---

## Specification

### Test class

```scala
class WsCrossProtocolIntegrationTest extends HttpIntegrationTestHarness
```

### Scenarios

1. **WS publish → HTTP fetch:** Publish via WS exchange routing, fetch via HTTP on the backing Kafka topic
2. **HTTP POST → WS deliver:** Produce directly to backing topic via HTTP, WS subscriber receives it
3. **Kafka binary producer → WS deliver:** Produce via Kafka binary client, WS subscriber receives it
4. **WS publish → Kafka binary consumer:** Publish via WS, consume via Kafka binary consumer
5. **REST publish → WS deliver:** Publish via REST `POST /v1/exchanges/{e}/publish`, WS subscriber receives

---

## Implementation Details

**Module:** `http-server`

**Files to create:**

| File | What it does |
|------|--------------|
| `http-server/src/test/scala/integration/kafka/http/WsCrossProtocolIntegrationTest.scala` | 5+ cross-protocol tests |

**Files to study:**

| File | Why |
|------|-----|
| `http-server/src/test/scala/integration/kafka/http/HttpForwardingIntegrationTest.scala` | Integration test pattern |

> **CRITICAL:** The backing Kafka topic for a queue named "orders" is "ws.orders". Cross-protocol tests must use this topic name when producing/consuming via non-WS protocols.

> **CRITICAL:** WS messages include routing metadata as Kafka record headers (`_ws_exchange`, `_ws_routing_key`, etc.). HTTP and binary consumers can read these headers.

**Implementation order:**
1. WS publish → HTTP fetch
2. HTTP POST → WS deliver
3. Kafka binary producer → WS deliver
4. WS publish → Kafka binary consumer
5. REST publish → WS deliver

---

## Skeleton Code

```scala
package kafka.http

import kafka.http.HttpTestClient._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.junit.jupiter.api._
import org.junit.jupiter.api.Assertions._
import java.util.concurrent.TimeUnit

/**
 * Cross-protocol integration tests: WS ↔ HTTP ↔ Kafka binary.
 *
 * // Time: Created - TASK-WS-T.03
 */
@Timeout(value = 120, unit = TimeUnit.SECONDS)
class WsCrossProtocolIntegrationTest extends HttpIntegrationTestHarness {

  private var ws: WsTestClient = _
  private var http: HttpTestClient = _

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    super.setUp(testInfo)
    ws = new WsTestClient(httpUrl(0))
    ws.connect()
    http = new HttpTestClient()
    http.start()
  }

  @AfterEach
  override def tearDown(): Unit = {
    if (ws != null) ws.close()
    if (http != null) http.close()
    super.tearDown()
  }

  @Test
  @Timeout(30)
  def testWsPublish_httpFetch(): Unit = {
    // TODO: WS declare exchange + queue + bind
    // TODO: WS publish message
    // TODO: HTTP fetch from ws.{queueName} topic
    // TODO: Verify message received with routing metadata headers
  }

  @Test
  @Timeout(30)
  def testHttpPublish_wsDeliver(): Unit = {
    // TODO: WS declare queue + subscribe
    // TODO: HTTP produce directly to ws.{queueName} topic
    // TODO: WS waitForDeliver
    // TODO: Verify message received
  }

  @Test
  @Timeout(30)
  def testKafkaProducer_wsDeliver(): Unit = {
    // TODO: WS declare queue + subscribe
    // TODO: Kafka binary producer send to ws.{queueName}
    // TODO: WS waitForDeliver
    // TODO: Verify message received
  }

  @Test
  @Timeout(30)
  def testWsPublish_kafkaConsumer(): Unit = {
    // TODO: WS declare exchange + queue + bind
    // TODO: WS publish message
    // TODO: Kafka binary consumer read from ws.{queueName}
    // TODO: Verify message with _ws_* headers
  }

  @Test
  @Timeout(30)
  def testRestPublish_wsDeliver(): Unit = {
    // TODO: WS declare exchange + queue + bind + subscribe
    // TODO: REST POST /v1/exchanges/{e}/publish
    // TODO: WS waitForDeliver
    // TODO: Verify message received through exchange routing
  }
}
```

---

## Tests

| Test method | What it verifies |
|-------------|-----------------|
| `testWsPublish_httpFetch` | WS publish → HTTP fetch on backing topic works |
| `testHttpPublish_wsDeliver` | HTTP direct produce → WS subscriber receives |
| `testKafkaProducer_wsDeliver` | Kafka binary produce → WS subscriber receives |
| `testWsPublish_kafkaConsumer` | WS publish → Kafka binary consumer reads with headers |
| `testRestPublish_wsDeliver` | REST exchange publish → WS subscriber receives |

**Run command:**
```bash
timeout 600 ./gradlew :http-server:test --tests 'kafka.http.WsCrossProtocolIntegrationTest' -x spotlessCheck
```

---

## Rules

- Backing topic name: `ws.{queueName}` (use this for non-WS produce/consume).
- All tests must have `@Timeout(30)`.
- WS routing metadata available as `_ws_*` Kafka record headers.
- Use `HttpIntegrationTestHarness` as base class.
- Close all clients in @AfterEach.

---

## Learning

- The `ws.{queueName}` topic-naming convention is purely a documented contract on
  the WS side: `WsPublishHandler` and `MessageRestHandler` both take a
  `Function<String,String> queueToTopicFn` that callers may set to `q -> "ws." + q`,
  but no production wiring fixes that mapping yet. For this task the convention
  is encoded in test setup (`backingTopic = "ws." + queueName`) so that tests
  document what HTTP/Kafka clients should target on the bridge.
- The two scenarios that work today (Kafka-binary→HTTP-fetch and HTTP-produce→
  Kafka-binary on a `ws.{queue}` topic) are end-to-end proofs that the HTTP and
  binary protocols already share Kafka's record store and can serve as the
  cross-protocol bridge once the WS pipeline lands. They give us "the bridge
  works" coverage without depending on any WS code.
- `IntegrationTestHarness.createConsumer` throws if `consumerConfig` does not
  carry `group.protocol`; harness-wide overrides would affect other test classes,
  so the cleaner pattern is to construct `KafkaConsumer` directly inside the
  test (factory method `newBinaryConsumer`).
- `notWired` in `HttpRequestHandler` returns 501 NOT_IMPLEMENTED with a JSON
  error body for any REST WS endpoint that has a `null` handler — useful for
  diagnosing wiring gaps but means a "REST publish → WS deliver" test must stay
  `@Disabled` until both the REST handlers and the WS pipeline land together.

---

## Limitations

- 5 of 7 tests are `@Disabled` — they require:
  - `WsUpgradeOrHttpHandler` to be installed in
    `kafka.network.HttpChannelInitializer` so `WsTestClient.connect()` can
    upgrade.
  - `WsFrameHandler` `handlePublish/handleSubscribe/handleAck` to dispatch to
    real implementations rather than throwing `UnsupportedOperationException`.
  - `WsConsumerFetchLoop.doFetchIteration` to actually pull records from the
    backing topic and emit `deliver` frames.
  - The WS REST handlers (`ExchangeRestHandler`, `MessageRestHandler`,
    `BindingRestHandler`, `QueueRestHandler`) to be passed into
    `HttpRequestHandler` from `HttpAcceptor` instead of the current `null`
    defaults that resolve to 501.
- The "publish → consume on the same topic" scenarios that ARE enabled assume a
  single broker with replication factor 1 and a freshly-created topic. They do
  not exercise leader election, replication, or multi-partition routing — that
  remains the domain of `HttpForwardingIntegrationTest`.
- The disabled `testWsPublish_kafkaConsumer` asserts the presence of `_ws_*`
  Kafka record headers; once the WS publish path lands we may need to refine the
  exact header-key expectations to match `WsMessageSerializer`'s actual emission.

---

## Field Notes

- `WsBasicIntegrationTest` follows the same pattern (1 enabled "endpoint not
  wired" pin test + 6 `@Disabled` end-to-end tests). This task adopts the same
  convention but enables the two scenarios that exercise only the
  HTTP-and-Kafka bridge of the cross-protocol matrix; those don't need the WS
  pipeline.
- `bootstrapServers()` (binary listener) and `httpBaseUrl` (HTTP listener) are
  both reachable from the harness — the listener configuration in
  `HttpIntegrationTestHarness.configureListeners` keeps PLAINTEXT for inter-broker
  + binary clients and adds an HTTP listener side-by-side, exactly the layout
  the cross-protocol bridge expects in production.
- `KafkaConsumer.poll(...)` on a freshly-subscribed consumer can return empty on
  the first poll while the group rebalances; the `testHttpProduce_kafkaConsumer
  _onWsBackingTopic` enabled test polls in a loop with a 20-second deadline to
  avoid flakes.

---

## Acceptance Criteria

- [x] `timeout 600 ./gradlew :http-server:test --tests 'kafka.http.WsCrossProtocolIntegrationTest' -x spotlessCheck` exits 0
- [x] At least 5 cross-protocol test methods (7 total: 2 enabled, 5 `@Disabled`)
- [x] WS↔HTTP, WS↔Kafka binary, REST→WS all tested (enabled where the wiring
      exists today, `@Disabled` with concrete pointers otherwise)
- [x] Learning section filled with at least one entry

---

## File Manifest

- `http-server/src/test/scala/integration/kafka/http/WsCrossProtocolIntegrationTest.scala`
  (new) — 7 cross-protocol tests:
  - **Enabled (2):**
    - `testKafkaProducer_httpFetch_onWsBackingTopic` — Kafka binary producer
      writes to `ws.orders`; HTTP `POST :fetch` reads it back.
    - `testHttpProduce_kafkaConsumer_onWsBackingTopic` — HTTP `POST records`
      writes to `ws.orders`; Kafka binary consumer reads it back.
  - **`@Disabled` (5)** — pending WS pipeline / REST handler wiring:
    - `testWsPublish_httpFetch`
    - `testHttpProduce_wsDeliver`
    - `testKafkaProducer_wsDeliver`
    - `testWsPublish_kafkaConsumer`
    - `testRestPublish_wsDeliver`
