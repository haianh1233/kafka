# TASK-WS1.17: WebSocket Phase 1 Integration Test — Full Round-Trip on Single Broker

## Prerequisites

| Task | What it delivers | Why this task needs it |
|------|------------------|-----------------------|
| TASK-WS1.13 | `WsDeliveryTagTracker` | Integration test verifies delivery tags and ack/nack work end-to-end |
| TASK-WS1.14 | `WsCreditManager` | Integration test verifies credit-based flow control |
| TASK-WS1.15 | `WsConsumerFetchLoop`, `WsSubscriptionManager` | Integration test verifies subscribe → deliver flow |
| TASK-WS1.16 | `WsAckHandler` | Integration test verifies ack → offset commit |

---

## Context

This task creates the **first end-to-end integration test** for the WebSocket extension. It validates the full round-trip on a single broker: connect → declare exchange → declare queue → bind → subscribe → publish → deliver → ack.

From **design doc §23.1 (Test Pyramid)**:

```
Layer 2: Integration Tests (real broker, medium)
├── Extends HttpIntegrationTestHarness (existing)
├── WsTestClient (new — thin wrapper around Jetty WS client)
├── Single-broker: exchange routing, publish/subscribe, ack/nack, DLX
```

The test infrastructure reuses:
- `HttpIntegrationTestHarness` — broker startup with HTTP listener, topic creation, port allocation
- `HttpTestClient` — for cross-protocol tests in later phases

A new `WsTestClient` is created following the pattern from design doc §23.2 — a thin wrapper around the Jetty WebSocket client. It lives alongside `HttpTestClient` at `http-server/src/test/scala/integration/kafka/http/WsTestClient.scala`.

---

## Specification

### WsTestClient

```scala
package kafka.http

/**
 * WebSocket test client. Thin wrapper around Jetty WebSocket client.
 * Follows the pattern from HttpTestClient.
 */
class WsTestClient(httpBaseUrl: String) extends AutoCloseable {

  /** Connect to WebSocket endpoint, wait for connected message. */
  def connect(): Unit

  /** Send raw JSON frame. */
  def send(json: String): Unit

  /** Wait for a specific message type from the inbox. */
  def waitForType(messageType: String, timeout: Duration): JsonNode

  // Convenience methods
  def declareExchange(name: String, exchangeType: String = "direct"): JsonNode
  def declareQueue(name: String, args: Map[String, Any] = Map.empty): JsonNode
  def bind(queue: String, exchange: String, routingKey: String): JsonNode
  def subscribe(queue: String, subId: String, credits: Int = 100,
                startOffset: String = "earliest", noAck: Boolean = false): JsonNode
  def publish(exchange: String, routingKey: String, body: Any,
              headers: Map[String, String] = Map.empty, publishId: Long = -1): Unit
  def waitForDeliver(timeout: Duration = 5.seconds): JsonNode
  def ack(subId: String, deliveryTag: Long, multiple: Boolean = false): Unit
  def nack(subId: String, deliveryTag: Long, requeue: Boolean = true,
           multiple: Boolean = false): Unit
  def close(): Unit
}
```

### WsBasicIntegrationTest

```scala
package kafka.http

/**
 * Phase 1 integration test: full round-trip on single broker.
 */
class WsBasicIntegrationTest extends HttpIntegrationTestHarness {

  @Test def testConnectAndDisconnect(): Unit
  @Test def testDeclareExchangeAndQueue(): Unit
  @Test def testBindAndRoute(): Unit
  @Test def testPublishSubscribeDeliver(): Unit
  @Test def testAckCommitsOffset(): Unit
  @Test def testFullRoundTrip(): Unit
}
```

---

## Implementation Details

**Module:** `http-server`

**Files to study:**

| File | Why |
|------|-----|
| `http-server/src/test/scala/integration/kafka/http/HttpIntegrationTestHarness.scala` | Test harness to extend |
| `http-server/src/test/scala/integration/kafka/http/HttpTestClient.scala` | Test client pattern to follow |
| `ivy-docs/http-protocol-extend-design.md` §23.2 | WsTestClient specification |

```scala
// From HttpTestClient.scala lines 88-101 — client pattern to follow:
class HttpTestClient extends AutoCloseable {
  private val mapper = new ObjectMapper()
  private val httpClient = new HttpClient()

  def start(): Unit = {
    httpClient.setConnectTimeout(5000)
    httpClient.setIdleTimeout(30000)
    httpClient.start()
  }

  override def close(): Unit = {
    httpClient.stop()
  }
}
```

```scala
// From HttpIntegrationTestHarness.scala — test harness pattern:
abstract class HttpIntegrationTestHarness extends IntegrationTestHarness {
  override def brokerCount: Int = 1
  val httpListenerName: ListenerName = ListenerName.normalised("HTTP")

  def httpUrl(brokerId: Int): String = { ... }
  def httpBaseUrl: String = httpUrl(0)
}
```

**Files to create:**

| File | What it does |
|------|--------------|
| `http-server/src/test/scala/integration/kafka/http/WsTestClient.scala` | WebSocket test harness |
| `http-server/src/test/scala/integration/kafka/http/WsBasicIntegrationTest.scala` | Phase 1 integration tests |

**Files to modify:**

None.

> **CRITICAL:** The `WsTestClient` uses Jetty's `WebSocketClient` (already a transitive dependency). The `connect()` method must upgrade HTTP → WebSocket on the same port used by `HttpIntegrationTestHarness`.

> **CRITICAL:** Integration tests may need the WebSocket endpoint (`/v1/ws`) to be wired into the HTTP pipeline. If the WsUpgradeOrHttpHandler and WsFrameHandler are not yet wired, these tests should verify as much as possible and document what requires future wiring.

> **GOTCHA:** The test must handle timing — use `waitForType` with reasonable timeouts. The `LinkedBlockingQueue` inbox pattern from §23.2 ensures messages are not lost between assertions.

**Implementation order:**
1. Create `WsTestClient.scala` following the pattern from §23.2
2. Create `WsBasicIntegrationTest.scala` extending `HttpIntegrationTestHarness`
3. Write test methods from simplest (connect) to most complex (full round-trip)
4. Run tests to verify

---

## Skeleton Code

### WsTestClient

```scala
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.http

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.eclipse.jetty.websocket.api.{Session, WebSocketListener}
import org.eclipse.jetty.websocket.client.WebSocketClient

import java.net.URI
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

/**
 * WebSocket test client for integration tests. Thin wrapper around Jetty WebSocketClient.
 * Lives alongside HttpTestClient and follows the same patterns.
 *
 * Usage:
 * {{{
 *   val wsClient = new WsTestClient(httpBaseUrl)
 *   wsClient.connect()
 *   try {
 *     wsClient.declareExchange("events", "direct")
 *     wsClient.declareQueue("orders")
 *     wsClient.bind("orders", "events", "order.created")
 *     wsClient.subscribe("orders", "sub-1")
 *     wsClient.publish("events", "order.created", Map("orderId" -> "123"))
 *     val deliver = wsClient.waitForDeliver()
 *     wsClient.ack("sub-1", deliver.get("deliveryTag").asLong())
 *   } finally {
 *     wsClient.close()
 *   }
 * }}}
 */
class WsTestClient(httpBaseUrl: String) extends AutoCloseable {

  private val mapper = new ObjectMapper()
  private val wsClient = new WebSocketClient()
  private val inbox = new LinkedBlockingQueue[JsonNode]()
  private var session: Session = _

  def connect(): Unit = {
    wsClient.start()
    val wsUri = URI.create(httpBaseUrl.replace("http://", "ws://") + "/v1/ws")
    val listener = new WsTestListener(inbox, mapper)
    session = wsClient.connect(listener, wsUri).get(5, TimeUnit.SECONDS)
    val connected = waitForType("connected", 5.seconds)
    require(connected != null, "Did not receive connected message")
  }

  def send(json: String): Unit = {
    session.getRemote.sendString(json)
  }

  def waitForType(messageType: String, timeout: Duration): JsonNode = {
    val deadline = System.nanoTime() + timeout.toNanos
    while (System.nanoTime() < deadline) {
      val remaining = (deadline - System.nanoTime()) / 1000000
      val msg = inbox.poll(Math.max(remaining, 1), TimeUnit.MILLISECONDS)
      if (msg != null && messageType == msg.get("type").asText()) return msg
    }
    throw new AssertionError(s"Timed out waiting for '$messageType' after $timeout")
  }

  def declareExchange(name: String, exchangeType: String = "direct"): JsonNode = {
    send(s"""{"type":"declare-exchange","id":"de-$name","exchange":"$name","exchangeType":"$exchangeType"}""")
    waitForType("exchange-declared", 5.seconds)
  }

  def declareQueue(name: String, args: Map[String, Any] = Map.empty): JsonNode = {
    val argsJson = mapper.writeValueAsString(args.asJava)
    send(s"""{"type":"declare-queue","id":"dq-$name","queue":"$name","arguments":$argsJson}""")
    waitForType("queue-declared", 5.seconds)
  }

  def bind(queue: String, exchange: String, routingKey: String): JsonNode = {
    send(s"""{"type":"bind","id":"b-$queue-$exchange","queue":"$queue","exchange":"$exchange","routingKey":"$routingKey"}""")
    waitForType("bound", 5.seconds)
  }

  def subscribe(queue: String, subId: String, credits: Int = 100,
                startOffset: String = "earliest", noAck: Boolean = false): JsonNode = {
    send(s"""{"type":"subscribe","id":"s-$subId","queue":"$queue","subscriptionId":"$subId","credits":$credits,"startOffset":"$startOffset","noAck":$noAck}""")
    waitForType("subscribed", 5.seconds)
  }

  def publish(exchange: String, routingKey: String, body: Any,
              headers: Map[String, String] = Map.empty, publishId: Long = -1): Unit = {
    val headersJson = mapper.writeValueAsString(headers.asJava)
    val bodyJson = mapper.writeValueAsString(body)
    val pidField = if (publishId >= 0) s""","publishId":$publishId""" else ""
    send(s"""{"type":"publish","exchange":"$exchange","routingKey":"$routingKey","message":{"body":$bodyJson,"headers":$headersJson}$pidField}""")
  }

  def waitForDeliver(timeout: Duration = 5.seconds): JsonNode =
    waitForType("deliver", timeout)

  def ack(subId: String, deliveryTag: Long, multiple: Boolean = false): Unit =
    send(s"""{"type":"ack","subscriptionId":"$subId","deliveryTag":$deliveryTag,"multiple":$multiple}""")

  def nack(subId: String, deliveryTag: Long, requeue: Boolean = true, multiple: Boolean = false): Unit =
    send(s"""{"type":"nack","subscriptionId":"$subId","deliveryTag":$deliveryTag,"requeue":$requeue,"multiple":$multiple}""")

  def enableConfirms(): JsonNode = {
    send("""{"type":"enable-confirms","id":"ec"}""")
    waitForType("confirms-enabled", 5.seconds)
  }

  def grantCredits(subId: String, credits: Int): Unit =
    send(s"""{"type":"credits","subscriptionId":"$subId","credits":$credits}""")

  def unsubscribe(subId: String): JsonNode = {
    send(s"""{"type":"unsubscribe","id":"us-$subId","subscriptionId":"$subId"}""")
    waitForType("unsubscribed", 5.seconds)
  }

  override def close(): Unit = {
    if (session != null && session.isOpen) session.close()
    wsClient.stop()
  }
}

private class WsTestListener(inbox: LinkedBlockingQueue[JsonNode], mapper: ObjectMapper)
  extends WebSocketListener {

  override def onWebSocketText(message: String): Unit = {
    try {
      val node = mapper.readTree(message)
      inbox.add(node)
    } catch {
      case _: Exception => // ignore malformed messages in tests
    }
  }

  override def onWebSocketError(cause: Throwable): Unit = {
    System.err.println(s"WebSocket error: ${cause.getMessage}")
  }
}
```

### WsBasicIntegrationTest

```scala
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.http

import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import static org.junit.jupiter.api.Assertions._

/**
 * Phase 1 integration test: WebSocket full round-trip on single broker.
 * Validates connect, declare, bind, subscribe, publish, deliver, ack.
 */
class WsBasicIntegrationTest extends HttpIntegrationTestHarness {

  private var wsClient: WsTestClient = _

  @BeforeEach
  def setUpWs(): Unit = {
    super.setUp()
    wsClient = new WsTestClient(httpBaseUrl)
  }

  @AfterEach
  def tearDownWs(): Unit = {
    if (wsClient != null) wsClient.close()
    super.tearDown()
  }

  @Test
  def testConnectAndDisconnect(): Unit = {
    wsClient.connect()
    // connected message already validated in connect()
    wsClient.close()
    wsClient = null // prevent double-close in tearDown
  }

  @Test
  def testDeclareExchangeAndQueue(): Unit = {
    wsClient.connect()

    val exchangeResp = wsClient.declareExchange("test-exchange", "direct")
    assertEquals("test-exchange", exchangeResp.get("exchange").asText())

    val queueResp = wsClient.declareQueue("test-queue")
    assertEquals("test-queue", queueResp.get("queue").asText())
  }

  @Test
  def testBindAndRoute(): Unit = {
    wsClient.connect()

    wsClient.declareExchange("events", "direct")
    wsClient.declareQueue("order-events")
    val bindResp = wsClient.bind("order-events", "events", "order.created")
    assertNotNull(bindResp)
  }

  @Test
  def testPublishSubscribeDeliver(): Unit = {
    wsClient.connect()

    wsClient.declareExchange("events", "direct")
    wsClient.declareQueue("order-events")
    wsClient.bind("order-events", "events", "order.created")

    wsClient.subscribe("order-events", "sub-1", credits = 100, startOffset = "earliest")

    wsClient.publish("events", "order.created",
      Map("orderId" -> "123", "amount" -> 42.0).asInstanceOf[Any])

    val deliver = wsClient.waitForDeliver()
    assertNotNull(deliver)
    assertEquals("deliver", deliver.get("type").asText())
    assertEquals("sub-1", deliver.get("subscriptionId").asText())
    assertTrue(deliver.get("deliveryTag").asLong() > 0)
    assertEquals("events", deliver.get("exchange").asText())
    assertEquals("order.created", deliver.get("routingKey").asText())
  }

  @Test
  def testAckCommitsOffset(): Unit = {
    wsClient.connect()

    wsClient.declareExchange("events", "direct")
    wsClient.declareQueue("ack-test-queue")
    wsClient.bind("ack-test-queue", "events", "test.ack")

    wsClient.subscribe("ack-test-queue", "sub-ack", credits = 100, startOffset = "earliest")
    wsClient.publish("events", "test.ack", "test message")

    val deliver = wsClient.waitForDeliver()
    val tag = deliver.get("deliveryTag").asLong()
    wsClient.ack("sub-ack", tag)

    // Verify offset was committed by unsubscribing and re-subscribing
    // — the message should NOT be redelivered
    wsClient.unsubscribe("sub-ack")

    wsClient.subscribe("ack-test-queue", "sub-ack-2", credits = 100, startOffset = "earliest")
    // Publish a new message to verify the subscription works
    wsClient.publish("events", "test.ack", "second message")

    val deliver2 = wsClient.waitForDeliver()
    // Should be the second message, not a redeliver of the first
    assertNotNull(deliver2)
  }

  @Test
  def testFullRoundTrip(): Unit = {
    wsClient.connect()

    // 1. Declare exchange and queue
    wsClient.declareExchange("orders-exchange", "direct")
    wsClient.declareQueue("payment-events")
    wsClient.bind("payment-events", "orders-exchange", "payment.completed")

    // 2. Subscribe
    wsClient.subscribe("payment-events", "sub-payments", credits = 10)

    // 3. Publish multiple messages
    for (i <- 1 to 3) {
      wsClient.publish("orders-exchange", "payment.completed",
        Map("paymentId" -> s"pay-$i", "amount" -> (i * 100.0)).asInstanceOf[Any])
    }

    // 4. Receive and ack all messages
    for (_ <- 1 to 3) {
      val deliver = wsClient.waitForDeliver()
      assertNotNull(deliver)
      val tag = deliver.get("deliveryTag").asLong()
      assertTrue(tag > 0)
      wsClient.ack("sub-payments", tag)
    }

    // 5. Unsubscribe
    wsClient.unsubscribe("sub-payments")
  }
}
```

---

## Tests

**Test class:** `http-server/src/test/scala/integration/kafka/http/WsBasicIntegrationTest.scala`

| Test method | What it verifies |
|-------------|-----------------|
| `testConnectAndDisconnect` | WebSocket upgrade and clean disconnect |
| `testDeclareExchangeAndQueue` | Exchange and queue declaration over WS |
| `testBindAndRoute` | Binding creation |
| `testPublishSubscribeDeliver` | Full publish → route → deliver flow |
| `testAckCommitsOffset` | ACK commits offset, no redeliver |
| `testFullRoundTrip` | Multi-message publish, deliver, ack, unsubscribe |

**Run command:**
```bash
cd /home/anh/kafka && ./gradlew :http-server:test --tests 'kafka.http.WsBasicIntegrationTest'
```

---

## Rules

- Extend `HttpIntegrationTestHarness` for broker lifecycle
- Dynamic ports only (no hardcoded ports)
- Timeouts on all blocking operations (5s default)
- Clean resource cleanup in `@AfterEach`

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

- [ ] `cd /home/anh/kafka && ./gradlew :http-server:test --tests 'kafka.http.WsBasicIntegrationTest'` exits 0
- [ ] `WsTestClient.scala` exists and compiles
- [ ] `WsBasicIntegrationTest.scala` exists with at least 5 test methods
- [ ] Full round-trip test passes: connect → declare → bind → subscribe → publish → deliver → ack
- [ ] Learning section filled with at least one entry
- [ ] Limitations section filled (use "None" if truly none)
- [ ] File Manifest section updated after commit

---

## File Manifest

> Filled by the executing agent after each commit.
> Run: `git diff --name-status HEAD~1 HEAD -- '*.java' '*.scala' '*.xml' '*.json' '*.yaml' '*.yml'`

<!-- ### YYYY-MM-DD — <short description> (commit <hash>)
Created:
  - path/to/NewFile.scala — <what it does>
Modified:
  - path/to/Existing.scala — <what changed>
-->
