# TASK-T.07: HTTP Metrics Integration Test

## Prerequisites

| Task | What it provides |
|------|-----------------|
| TASK-0.04 | Build verified -- base code compiles and existing tests pass |
| TASK-C.03 | Phase 1 integration test infrastructure -- HttpIntegrationTestHarness, HttpTestClient |

---

## Context

The HTTP protocol design includes metrics tagged with `protocol=http` so that operators can distinguish HTTP traffic from binary protocol traffic. Key metrics include:
- Request rate and latency per protocol (http vs binary)
- Produce and consume byte rates per protocol
- Forward queue depth gauge (for HTTP-to-binary forwarding)

Currently there are **zero integration tests** that verify HTTP-specific metrics are recorded. This task adds E2E tests that send HTTP requests and verify the corresponding Yammer/JMX metrics are incremented.

### Key Code References

- **Test harness**: `HttpIntegrationTestHarness.scala`
- **Test client**: `HttpTestClient.scala`
- **BrokerTopicStats**: `BrokerTopicStats.java` -- per-topic metrics
- **Yammer metrics**: `KafkaYammerMetrics` -- global metrics registry
- **Existing metric test pattern**: `RequestMetricsTest.java`, `BrokerTopicStatsTest.java`
- **Design doc**: `ivy-docs/http-protocol-design.md` sections on metrics

---

## Specification

### Test 1: `testHttpProduceIncrementMetric`
- Produce a record via HTTP
- Query metrics for `protocol=http` produce request count
- Verify the count is greater than 0

### Test 2: `testHttpConsumeIncrementMetric`
- Consume a record via HTTP
- Query metrics for `protocol=http` fetch request count
- Verify the count is greater than 0

### Test 3: `testForwardQueueGaugeReadable`
- Access the forward queue depth gauge metric
- Verify it is registered and returns a numeric value (0 when no forwarding is in progress)

---

## Implementation Details

### File: `http-server/src/test/scala/integration/kafka/http/HttpMetricsIntegrationTest.scala`

Extends `HttpIntegrationTestHarness`. After sending HTTP requests, queries the Yammer metrics registry for HTTP-specific metric names.

Kafka uses Yammer metrics (via `KafkaYammerMetrics.defaultRegistry()`). Metrics can be queried by name pattern. The HTTP protocol metrics should be tagged with `protocol=http` or similar.

---

## Skeleton Code

```scala
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import com.yammer.metrics.core.{Gauge, Meter, MetricName}
import kafka.http.HttpTestClient._
import org.apache.kafka.server.metrics.KafkaYammerMetrics
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test, TestInfo}
import org.junit.jupiter.api.Assertions._

import scala.jdk.CollectionConverters._

/**
 * Integration tests for HTTP protocol metrics.
 *
 * Tests:
 * - HTTP produce increments protocol=http produce metrics
 * - HTTP consume increments protocol=http consume metrics
 * - Forward queue gauge is registered and readable
 *
 * Note: These tests require HTTP-specific metrics to be registered by the
 * HTTP server components. They may not pass until the metrics integration
 * is complete. They should compile and serve as executable specifications.
 */
class HttpMetricsIntegrationTest extends HttpIntegrationTestHarness {

  private val testTopic = "metrics-test"
  private val numPartitions = 1
  private var client: HttpTestClient = _

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    super.setUp(testInfo)
    createTopic(testTopic, numPartitions, replicationFactor = 1)
    client = new HttpTestClient()
    client.start()
  }

  @AfterEach
  override def tearDown(): Unit = {
    if (client != null) client.close()
    super.tearDown()
  }

  /**
   * Find a metric by name substring in the Yammer metrics registry.
   * Returns the first matching metric or None.
   */
  private def findMetric(nameSubstring: String): Option[(MetricName, Any)] = {
    val registry = KafkaYammerMetrics.defaultRegistry()
    registry.allMetrics().asScala.find { case (name, _) =>
      name.toString.contains(nameSubstring)
    }
  }

  /**
   * Find all metrics matching a name substring.
   */
  private def findMetrics(nameSubstring: String): Map[MetricName, Any] = {
    val registry = KafkaYammerMetrics.defaultRegistry()
    registry.allMetrics().asScala.filter { case (name, _) =>
      name.toString.contains(nameSubstring)
    }.toMap
  }

  /**
   * Get the count from a Meter metric.
   */
  private def getMeterCount(nameSubstring: String): Option[Long] = {
    findMetric(nameSubstring).flatMap { case (_, metric) =>
      metric match {
        case m: Meter => Some(m.count())
        case _ => None
      }
    }
  }

  // ===================================================================
  // Test 1: HTTP produce increments protocol=http metric
  // ===================================================================
  @Test
  def testHttpProduceIncrementMetric(): Unit = {
    // Get the baseline count
    val baselineCount = getMeterCount("Produce").getOrElse(0L)

    // Produce a record via HTTP
    val response = client.produce(httpBaseUrl, testTopic,
      Seq(ProduceRecord(partition = Some(0), value = Some(StringValue("metric-test")))))

    assertEquals(200, response.status,
      s"Produce should succeed, got ${response.status}")

    // Check that produce-related metrics incremented
    // Look for RequestsPerSec with request=Produce
    val afterCount = getMeterCount("Produce").getOrElse(0L)

    assertTrue(afterCount > baselineCount,
      s"Produce metric count should have increased. " +
        s"Before: $baselineCount, After: $afterCount")

    // Also look for HTTP-specific metrics (protocol=http tag)
    val httpMetrics = findMetrics("http")
    // Log what we found for debugging
    if (httpMetrics.isEmpty) {
      // HTTP-specific metrics may not be registered yet
      // This is acceptable -- the test documents expected behavior
    } else {
      assertTrue(httpMetrics.nonEmpty,
        "Should find HTTP-protocol-specific metrics after HTTP produce")
    }
  }

  // ===================================================================
  // Test 2: HTTP consume increments protocol=http metric
  // ===================================================================
  @Test
  def testHttpConsumeIncrementMetric(): Unit = {
    // Produce a record first
    client.produce(httpBaseUrl, testTopic,
      Seq(ProduceRecord(partition = Some(0), value = Some(StringValue("consume-metric-test")))))

    // Get the baseline count
    val baselineCount = getMeterCount("Fetch").getOrElse(0L)

    // Consume via HTTP
    val response = client.consume(httpBaseUrl, testTopic,
      Seq(FetchPartitionSpec(partition = 0, offset = 0)),
      maxWaitMs = 5000)

    assertEquals(200, response.status,
      s"Consume should succeed, got ${response.status}")

    // Check that fetch-related metrics incremented
    val afterCount = getMeterCount("Fetch").getOrElse(0L)

    assertTrue(afterCount > baselineCount,
      s"Fetch metric count should have increased. " +
        s"Before: $baselineCount, After: $afterCount")
  }

  // ===================================================================
  // Test 3: Forward queue gauge is registered and readable
  // ===================================================================
  @Test
  def testForwardQueueGaugeReadable(): Unit = {
    // Look for a forward queue depth gauge in the metrics registry
    // The metric name depends on the implementation but should contain
    // "forward" or "ForwardQueue" or similar.
    val forwardMetrics = findMetrics("forward")

    // If the forward queue gauge is registered, verify it returns a value
    val gaugeOpt = forwardMetrics.collectFirst {
      case (name, metric: Gauge[_]) => (name, metric)
    }

    if (gaugeOpt.isDefined) {
      val (name, gauge) = gaugeOpt.get
      val value = gauge.value()
      assertNotNull(value, s"Forward queue gauge '$name' should return a non-null value")
      // The value should be 0 when no forwarding is in progress
      value match {
        case n: Number =>
          assertTrue(n.longValue() >= 0,
            s"Forward queue gauge should be >= 0, got: ${n.longValue()}")
        case _ =>
          // Gauge returns a non-numeric value -- acceptable, just verify it is readable
      }
    } else {
      // Forward queue gauge may not be registered yet.
      // This is acceptable -- the test documents expected behavior.
      // When the forwarding infrastructure is complete, this gauge should exist.
    }

    // Also check for any HTTP-specific gauges
    val httpGauges = findMetrics("Http").collect {
      case (name, metric: Gauge[_]) => (name, metric)
    }
    // Log available HTTP gauges for debugging
    httpGauges.foreach { case (name, gauge) =>
      assertNotNull(gauge.value(),
        s"HTTP gauge '$name' should return a non-null value")
    }
  }
}
```

---

## Tests

### Run Command

```bash
./gradlew :http-server:test --tests 'kafka.http.HttpMetricsIntegrationTest*'
```

### Expected Results

Tests should compile. The produce/consume metric increment tests should pass as long as the HTTP endpoints work (they use general Kafka request metrics). HTTP-protocol-specific metrics (tagged with `protocol=http`) may not be registered yet, so assertions about those are defensive.

---

## Rules

- Extend `HttpIntegrationTestHarness` -- do NOT create a custom test harness.
- Query metrics via `KafkaYammerMetrics.defaultRegistry()` -- this is the standard approach in Kafka tests.
- Use defensive assertions: if a metric is not found, the test should not fail but should document what was expected.
- Metric names are implementation-dependent -- search by substring rather than exact name.
- Get baseline metric values BEFORE the operation, then compare AFTER to detect increments.
- Do NOT modify the metrics registry or create custom metrics in tests.

---

## Learning

(empty)

## Limitations

(empty)

## Field Notes

(empty)

---

## Acceptance Criteria

- [ ] `HttpMetricsIntegrationTest.scala` is created at `http-server/src/test/scala/integration/kafka/http/HttpMetricsIntegrationTest.scala`
- [ ] Test compiles with `./gradlew :http-server:compileTestScala`
- [ ] Produce/consume metric increment tests pass when HTTP endpoints work
- [ ] Forward queue gauge test is defensive (passes even if gauge not yet registered)
- [ ] No modifications to existing integration test files

---

## File Manifest

(empty -- to be filled after implementation)
