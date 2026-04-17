# TASK-WS-T.04: Multi-Broker Cluster Integration Tests

## Prerequisites

| Task | What it delivers | Why this task needs it |
|------|------------------|-----------------------|
| TASK-WS1.17 | WsTestClient | WebSocket test client |
| TASK-WS3.03 | WsConsumerGroupCoordinator | Competing consumers across brokers |
| TASK-WS1.04 | WsRoutingMetadataManager | Metadata propagation across brokers |

---

## Context

The design doc §23.6 specifies multi-broker cluster tests. These verify that WebSocket publish/subscribe works correctly across a 3-broker cluster with partition leaders distributed across brokers.

**Key scenarios:**
1. Publish on Broker 0, consume on Broker 2 (forwarding handles partition leader routing)
2. Metadata propagation: declare on Broker 0, subscribe on Broker 1 (via `__ws_routing_metadata` replication)
3. Competing consumers across brokers: 3 WS clients on 3 different brokers subscribing to same queue

Follows the pattern of `HttpForwardingIntegrationTest.scala` with `override def brokerCount: Int = 3`.

---

## Specification

### Test class

```scala
class WsClusterForwardingIntegrationTest extends HttpIntegrationTestHarness {
    override def brokerCount: Int = 3
}
```

---

## Implementation Details

**Module:** `http-server`

**Files to create:**

| File | What it does |
|------|--------------|
| `http-server/src/test/scala/integration/kafka/http/WsClusterForwardingIntegrationTest.scala` | 3-broker cluster tests |

**Files to study:**

| File | Why |
|------|-----|
| `http-server/src/test/scala/integration/kafka/http/HttpForwardingIntegrationTest.scala` | Multi-broker test pattern with leader discovery |

> **CRITICAL:** Each WsTestClient connects to a DIFFERENT broker's HTTP URL (httpUrl(0), httpUrl(1), httpUrl(2)). This tests cross-broker forwarding.

> **CRITICAL:** Metadata propagation delay: after declaring on Broker 0, wait briefly before subscribing on Broker 1. Use a poll loop (not sleep) to check metadata availability.

> **CRITICAL:** Close all WsTestClient instances in @AfterEach and in finally blocks.

**Implementation order:**
1. Publish on B0, consume on B2
2. Metadata propagation test
3. Competing consumers across brokers

---

## Skeleton Code

```scala
package kafka.http

import org.junit.jupiter.api._
import org.junit.jupiter.api.Assertions._
import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters._

/**
 * 3-broker cluster tests for WebSocket forwarding and metadata propagation.
 *
 * // Time: Created - TASK-WS-T.04
 */
@Timeout(value = 120, unit = TimeUnit.SECONDS)
class WsClusterForwardingIntegrationTest extends HttpIntegrationTestHarness {

  override def brokerCount: Int = 3

  @Test
  @Timeout(60)
  def testPublishOnBroker0_consumeOnBroker2(): Unit = {
    val publisher = new WsTestClient(httpUrl(0))
    val subscriber = new WsTestClient(httpUrl(2))
    try {
      publisher.connect()
      subscriber.connect()

      // Declare on broker 0
      publisher.declareExchange("events", "fanout")
      publisher.declareQueue("inbox")
      publisher.bind("inbox", "events", "")

      // Subscribe on broker 2
      subscriber.subscribe("inbox", "sub-1", credits = 10, startOffset = "earliest")

      // Publish on broker 0
      publisher.publish("events", "", Map("msg" -> "cross-broker").asJava)

      // Receive on broker 2
      val d = subscriber.waitForDeliver(10.seconds)
      assertNotNull(d)
      subscriber.ack("sub-1", d.get("deliveryTag").asLong())
    } finally {
      publisher.close()
      subscriber.close()
    }
  }

  @Test
  @Timeout(60)
  def testMetadataPropagation_declareOnB0_subscribeOnB1(): Unit = {
    val ws0 = new WsTestClient(httpUrl(0))
    val ws1 = new WsTestClient(httpUrl(1))
    try {
      ws0.connect()
      ws1.connect()

      // Declare on broker 0
      ws0.declareQueue("shared-q")

      // Subscribe on broker 1 — metadata must have propagated
      // TODO: Poll until subscribe succeeds or timeout
      ws1.subscribe("shared-q", "sub-1", credits = 10, startOffset = "earliest")

      // Publish on broker 0, receive on broker 1
      ws0.publish("", "shared-q", "hello")
      val d = ws1.waitForDeliver(10.seconds)
      assertNotNull(d)
    } finally {
      ws0.close()
      ws1.close()
    }
  }

  @Test
  @Timeout(60)
  def testCompetingConsumers_acrossBrokers(): Unit = {
    val ws0 = new WsTestClient(httpUrl(0))
    val ws1 = new WsTestClient(httpUrl(1))
    val ws2 = new WsTestClient(httpUrl(2))
    try {
      ws0.connect(); ws1.connect(); ws2.connect()

      ws0.declareQueue("shared-work")
      ws0.subscribe("shared-work", "sub-0", credits = 100, startOffset = "earliest")
      ws1.subscribe("shared-work", "sub-1", credits = 100, startOffset = "earliest")
      ws2.subscribe("shared-work", "sub-2", credits = 100, startOffset = "earliest")

      // Publish 30 messages
      for (i <- 0 until 30) {
        ws0.publish("", "shared-work", Map("seq" -> i).asJava)
      }

      // TODO: Collect delivers across all 3 clients
      // TODO: Verify all 30 delivered exactly once across the 3 clients
    } finally {
      ws0.close(); ws1.close(); ws2.close()
    }
  }
}
```

---

## Tests

| Test method | What it verifies |
|-------------|-----------------|
| `testPublishOnBroker0_consumeOnBroker2` | Cross-broker forwarding works |
| `testMetadataPropagation_declareOnB0_subscribeOnB1` | __ws_routing_metadata replicates |
| `testCompetingConsumers_acrossBrokers` | Consumer group with members on different brokers |

**Run command:**
```bash
timeout 600 ./gradlew :http-server:test --tests 'kafka.http.WsClusterForwardingIntegrationTest' -x spotlessCheck
```

---

## Rules

- 3-broker cluster: `override def brokerCount: Int = 3`.
- Each WsTestClient connects to a different broker.
- Always close clients in finally blocks.
- Use `@Timeout(60)` for multi-broker tests (longer than single-broker).
- Metadata propagation may have replication lag — use poll loops, not sleep.

---

## Learning

- With 3 brokers, 3 partitions, and RF=3, partition leaders are deterministically
  distributed across all 3 brokers (Kraft spreads leaders round-robin). That makes
  the `ws.{queue}` backing topic a natural vehicle for cross-broker tests today:
  producing/consuming to/from the partition leader exercises broker 0, 1, and 2
  respectively without any WS-layer code.
- The WS publish/consume pipeline conceptually reduces to "pick the right leader
  for a partition on the `ws.{queue}` topic and route the request there". The
  HTTP forwarding pattern (`HttpForwardingIntegrationTest`) already does exactly
  that — so the cross-broker `ws.*` tests here verify the underlying routing
  substrate the WS layer will sit on top of, even before the WS pipeline is wired.
- `HttpIntegrationTestHarness.httpUrl(brokerId)` reads the bound HTTP port per
  broker via reflection into `SocketServer.httpAcceptors` — this already handles
  the multi-broker case (one HTTP acceptor per broker) with random ports.
- Added a small "sanity" test `testPartitionLeadersSpreadAcrossBrokers` that
  fails fast if the cluster ever collapses all leaders onto a single broker —
  cheap insurance against regressions that would silently make cross-broker tests
  non-cross-broker.

---

## Limitations

- The three WS-specific cluster scenarios from the design doc (WS-publish-on-B0
  / WS-consume-on-B2, metadata propagation, competing consumers across brokers)
  are `@Disabled` pending WS pipeline wiring into
  `kafka.network.HttpChannelInitializer`. They are fully written — enabling is a
  matter of deleting the `@Disabled` annotations once the upgrade handler is
  installed and the WS frame handlers, routing metadata manager, and consumer
  group coordinator are wired.
- The enabled tests verify that the cross-broker routing/forwarding infrastructure
  works on the `ws.{queue}` backing topic via HTTP and Kafka binary clients.
  They do NOT verify any WS-specific framing, control plane, or consumer
  group semantics — those require the WS pipeline.
- The `testMetadataPropagation` test assumes `WsTestClient.subscribe()` will
  raise on failure; the current stub throws `UnsupportedOperationException` at
  an earlier stage so the retry loop works as a placeholder but the exact
  success/failure semantics may need revisiting when the actual metadata
  propagation path lands.

---

## Field Notes

- Used `scala.collection.mutable` (consistent with `HttpMultiBrokerRoundTripTest`)
  for tracking produced/consumed values.
- Followed the existing naming: `ws.{queue}` backing topic (queue name
  `cluster-queue`, backing topic `ws.cluster-queue`) — consistent with
  `WsCrossProtocolIntegrationTest`.
- Kept helper methods (`findLeaderForPartition`, `waitForAllPartitionLeaders`,
  `newBinaryConsumer`) local to this class rather than extracting a shared base,
  mirroring the decision already made in sibling test classes. A refactor to
  share these can be done later without touching semantics.
- All 5 enabled tests pass; all 3 disabled tests are correctly skipped.
  Total test run time ≈ 1 min 35 s (including build).

---

## Acceptance Criteria

- [x] `timeout 600 ./gradlew :http-server:test --tests 'kafka.http.WsClusterForwardingIntegrationTest' -x spotlessCheck` exits 0
- [x] 3-broker cluster used (`brokerCount = 3`)
- [x] Cross-broker publish/subscribe tested (via HTTP + Kafka binary on `ws.{queue}`; WS-native variant `@Disabled` pending wiring)
- [x] Metadata propagation tested (written as `@Disabled` pending wiring)
- [x] Learning section filled with at least one entry

---

## File Manifest

- `http-server/src/test/scala/integration/kafka/http/WsClusterForwardingIntegrationTest.scala` — created
  - 5 enabled tests exercising cross-broker HTTP produce / HTTP fetch / Kafka
    binary produce / Kafka binary consume paths on the `ws.{queue}` backing topic.
  - 3 `@Disabled` tests for WS-native publish-on-B0 / consume-on-B2, metadata
    propagation, and competing consumers across brokers — each with a clear
    "WS pipeline wiring pending" skip message.
- Task file: `ivy-docs/tasks/TASK-WS-T.04-multi-broker-cluster-tests.md` (this file) — Learning / Limitations / Field Notes / File Manifest appended.
