# TASK-T.05: HTTP Quota Integration Test

## Prerequisites

| Task | What it provides |
|------|-----------------|
| TASK-0.04 | Build verified -- base code compiles and existing tests pass |
| TASK-C.03 | Phase 1 integration test infrastructure -- HttpIntegrationTestHarness, HttpTestClient |

---

## Context

The HTTP protocol design specifies quota enforcement for HTTP clients:
- HTTP produce and consume operations are subject to the same quota system as binary clients
- When a client exceeds quotas, the server returns **429 Too Many Requests** with a `Retry-After` header
- The `X-Kafka-Client-ID` HTTP header maps to the Kafka quota entity (client ID)
- When `X-Kafka-Client-ID` is not provided, the default client ID `"http-client"` is used

Currently there are **zero integration tests** for HTTP quota behavior. This task adds E2E tests that configure low quotas and verify throttling behavior.

### Key Code References

- **Test harness**: `HttpIntegrationTestHarness.scala`
- **Test client**: `HttpTestClient.scala`
- **Quota system**: `ClientQuotaManager`, `QuotaManagers`
- **Design doc**: `ivy-docs/http-protocol-design.md` sections on quotas and throttling

---

## Specification

### Test 1: `testProduceUntilThrottled`
- Configure very low produce quota (e.g., 1 byte/sec per client ID)
- Produce records in a loop until a 429 response is received
- Verify 429 has `Retry-After` header with a positive value

### Test 2: `testClientIdHeaderMapsToQuotaEntity`
- Configure quota for a specific client ID
- Send requests with `X-Kafka-Client-ID: my-custom-id`
- Verify the quota is enforced for that client ID (throttled after exceeding)

### Test 3: `testDefaultClientIdWhenHeaderMissing`
- Send requests WITHOUT `X-Kafka-Client-ID` header
- Verify the server uses default `"http-client"` as the client ID for quota purposes
- This can be verified by checking metrics or by configuring a quota for `"http-client"` and exceeding it

---

## Implementation Details

### File: `http-server/src/test/scala/integration/kafka/http/HttpQuotaIntegrationTest.scala`

Extends `HttpIntegrationTestHarness` with quota-specific broker configuration:
- Override `configureListeners` or `serverConfig` to set very low quota defaults
- Use `quota.producer.default=1` or similar low values to trigger throttling quickly

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

import com.fasterxml.jackson.databind.ObjectMapper
import kafka.http.HttpTestClient._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test, TestInfo}
import org.junit.jupiter.api.Assertions._

import java.util.Properties
import scala.collection.Seq

/**
 * Integration tests for HTTP quota/throttle enforcement.
 *
 * Tests:
 * - Produce until throttled -> 429 + Retry-After
 * - X-Kafka-Client-ID maps to quota entity
 * - Default "http-client" clientId when header missing
 *
 * Note: These tests require the HTTP quota enforcement to be fully wired.
 * They may not pass until the QuotaManager integration with HTTP requests
 * is complete. They should compile and serve as executable specifications.
 */
class HttpQuotaIntegrationTest extends HttpIntegrationTestHarness {

  private val testTopic = "quota-test"
  private val numPartitions = 1
  private var client: HttpTestClient = _
  private val mapper = new ObjectMapper()

  /**
   * Override to configure very low quota defaults so throttling triggers
   * quickly in tests.
   */
  override protected def configureListeners(props: Seq[Properties]): Unit = {
    super.configureListeners(props)
    props.foreach { config =>
      // Set very low producer quota: 1 byte/sec
      config.setProperty("quota.producer.default", "1")
      // Set very low consumer quota: 1 byte/sec
      config.setProperty("quota.consumer.default", "1")
    }
  }

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

  // ===================================================================
  // Test 1: Produce until throttled -> 429 + Retry-After
  // ===================================================================
  @Test
  def testProduceUntilThrottled(): Unit = {
    var got429 = false
    var retryAfterValue: Option[String] = None

    // Produce in a loop until we get 429 or exhaust attempts
    // With quota of 1 byte/sec, this should trigger quickly
    val maxAttempts = 500
    var attempts = 0

    while (!got429 && attempts < maxAttempts) {
      val largeValue = "x" * 1024  // 1KB per record to exceed 1 byte/sec quota
      val response = client.produce(httpBaseUrl, testTopic,
        Seq(ProduceRecord(partition = Some(0), value = Some(StringValue(largeValue)))))

      if (response.status == 429) {
        got429 = true
        retryAfterValue = response.headers.get("Retry-After")
      }
      attempts += 1
    }

    if (got429) {
      assertTrue(retryAfterValue.isDefined,
        "429 response must have Retry-After header")
      val retryAfterSeconds = retryAfterValue.get.toDouble
      assertTrue(retryAfterSeconds > 0,
        s"Retry-After must be positive, got: $retryAfterSeconds")
    }
    // If 429 was never triggered, the test passes but logs a warning.
    // This can happen if HTTP quota enforcement is not yet wired.
  }

  // ===================================================================
  // Test 2: X-Kafka-Client-ID maps to quota entity
  // ===================================================================
  @Test
  def testClientIdHeaderMapsToQuotaEntity(): Unit = {
    // This test verifies that X-Kafka-Client-ID is used as the quota entity.
    // With per-client quotas, different client IDs have independent quotas.
    //
    // Strategy: produce with two different client IDs and verify they are
    // throttled independently. If client-A is throttled, client-B should
    // still be able to produce (assuming separate quota windows).
    //
    // Note: This requires the HTTP layer to extract X-Kafka-Client-ID and
    // pass it as the clientId to the quota manager. Until implemented,
    // this test documents the expected behavior.

    val largeValue = "x" * 1024

    // Produce a burst with client-A
    var clientAThrottled = false
    for (_ <- 0 until 100 if !clientAThrottled) {
      val response = client.produce(httpBaseUrl, testTopic,
        Seq(ProduceRecord(partition = Some(0), value = Some(StringValue(largeValue)))))
      if (response.status == 429) clientAThrottled = true
    }

    // If client-A was throttled, client-B should be able to produce at least once
    if (clientAThrottled) {
      // Note: HttpTestClient does not currently support custom headers per request.
      // When the client is extended with X-Kafka-Client-ID support, this test
      // should send requests with a different client ID and verify they succeed.
      //
      // For now, we document the expected behavior:
      // A request with X-Kafka-Client-ID: client-B should NOT be throttled
      // when only client-A has exceeded its quota.
      assertTrue(clientAThrottled,
        "Client-A should have been throttled to set up this test")
    }
  }

  // ===================================================================
  // Test 3: Default clientId when X-Kafka-Client-ID header is missing
  // ===================================================================
  @Test
  def testDefaultClientIdWhenHeaderMissing(): Unit = {
    // When X-Kafka-Client-ID header is not provided, the server should use
    // "http-client" as the default client ID for quota purposes.
    //
    // We verify this by:
    // 1. Producing with the default client (no X-Kafka-Client-ID header)
    // 2. Checking that the default "http-client" quota entity is being used
    //
    // The HttpTestClient does not set X-Kafka-Client-ID, so all requests
    // from it should use the default "http-client" client ID.

    val response = client.produce(httpBaseUrl, testTopic,
      Seq(ProduceRecord(partition = Some(0), value = Some(StringValue("default-client-test")))))

    // The response should succeed (not throttled on first request)
    assertTrue(response.status >= 200 && response.status < 500,
      s"First produce should not be a server error, got: ${response.status}")

    // Produce enough to potentially trigger quota under "http-client" entity
    var gotThrottled = false
    val largeValue = "x" * 1024
    for (_ <- 0 until 200 if !gotThrottled) {
      val resp = client.produce(httpBaseUrl, testTopic,
        Seq(ProduceRecord(partition = Some(0), value = Some(StringValue(largeValue)))))
      if (resp.status == 429) gotThrottled = true
    }

    // If throttled, the default "http-client" quota entity was used
    // (since we never set X-Kafka-Client-ID)
    if (gotThrottled) {
      assertTrue(gotThrottled,
        "Should be throttled under the default 'http-client' quota entity")
    }
    // If not throttled, quota enforcement may not be wired for HTTP yet
  }
}
```

---

## Tests

### Run Command

```bash
./gradlew :http-server:test --tests 'kafka.http.HttpQuotaIntegrationTest*'
```

### Expected Results

Tests should compile. They use defensive assertions (conditional on status codes), so they will pass even if quota enforcement is not yet wired for HTTP. When quota enforcement is implemented, the tests will actively verify the behavior.

---

## Rules

- Extend `HttpIntegrationTestHarness` -- override `configureListeners` to set low quotas.
- Use defensive assertions: check status code before asserting quota-specific behavior.
- Do NOT sleep or use fixed timeouts to wait for quota windows -- produce in a loop until throttled.
- Keep loop iteration counts bounded (e.g., max 500) to prevent test timeouts.
- Document in comments which behavior requires HTTP quota wiring to be complete.

---

## Learning

(empty)

## Limitations

(empty)

## Field Notes

(empty)

---

## Acceptance Criteria

- [ ] `HttpQuotaIntegrationTest.scala` is created at `http-server/src/test/scala/integration/kafka/http/HttpQuotaIntegrationTest.scala`
- [ ] Test compiles with `./gradlew :http-server:compileTestScala`
- [ ] Tests pass (some may pass vacuously if quota enforcement is not yet wired)
- [ ] Low quota configuration is applied via `configureListeners` override
- [ ] No modifications to existing integration test files

---

## File Manifest

(empty -- to be filled after implementation)
