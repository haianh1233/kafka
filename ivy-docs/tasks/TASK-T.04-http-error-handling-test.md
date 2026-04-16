# TASK-T.04: HTTP Error Handling Integration Test

## Prerequisites

| Task | What it provides |
|------|-----------------|
| TASK-0.04 | Build verified -- base code compiles and existing tests pass |
| TASK-C.03 | Phase 1 integration test infrastructure -- HttpIntegrationTestHarness, HttpTestClient |

---

## Context

The HTTP protocol design specifies strict error response conventions:
- **Retry-After header**: present on 429 (rate limited) and 503 (service unavailable), absent on 400/403/404
- **Error body structure**: every error response has `errorCode` (int) and `errorMessage` (string)
- **Content-Type**: every response has `Content-Type: application/json`
- **X-Kafka-Request-ID**: every response has a unique request ID for tracing
- **207 Multi-Status**: partial failure across partitions returns 207 with per-partition results

Currently there are **zero integration tests** for these error response conventions. This task adds E2E tests that verify the HTTP error contract.

### Key Code References

- **Test harness**: `HttpIntegrationTestHarness.scala`
- **Test client**: `HttpTestClient.scala` -- `rawGet()`, `rawPost()`, response headers
- **Response serializer**: `HttpResponseSerializer.java` -- sets headers and status codes
- **Design doc**: `ivy-docs/http-protocol-design.md` sections on error handling

---

## Specification

### Test 1: `testRetryAfterHeaderOn503`
- Trigger a 503 response (e.g., produce to a topic when broker is overloaded or shutting down)
- Verify `Retry-After` header is present and is a positive integer (seconds)

### Test 2: `testRetryAfterHeaderOn429`
- Trigger a 429 response (produce until rate limited)
- Verify `Retry-After` header is present

### Test 3: `testNoRetryAfterOn400`
- Send a malformed request body (invalid JSON)
- Verify 400 response does NOT have `Retry-After` header

### Test 4: `testNoRetryAfterOn403`
- Produce to a topic without authorization (if ACLs are configured)
- Verify 403 response does NOT have `Retry-After` header

### Test 5: `testNoRetryAfterOn404`
- GET a nonexistent topic
- Verify 404 response does NOT have `Retry-After` header

### Test 6: `testErrorBodyStructure`
- Trigger any error response (e.g., 404)
- Verify body contains `errorCode` (int) and `errorMessage` (string)

### Test 7: `testContentTypeIsJson`
- Send a successful produce request
- Verify `Content-Type` header is `application/json`
- Send a request that triggers an error
- Verify `Content-Type` header is still `application/json`

### Test 8: `testRequestIdHeader`
- Send any request
- Verify `X-Kafka-Request-ID` header is present and non-empty
- Send two requests
- Verify the request IDs are different

### Test 9: `testMultiPartitionPartialFailure207`
- Produce to multiple partitions where some succeed and some fail (e.g., partition exists + nonexistent partition)
- Verify 207 Multi-Status response
- Verify per-partition results with individual error codes

---

## Implementation Details

### File: `http-server/src/test/scala/integration/kafka/http/HttpErrorHandlingTest.scala`

Extends `HttpIntegrationTestHarness`. Uses `HttpTestClient` methods and checks response headers and body structure.

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

import scala.jdk.CollectionConverters._

/**
 * Integration tests for HTTP error handling conventions.
 *
 * Verifies:
 * - Retry-After header presence/absence by status code
 * - Error body structure (errorCode + errorMessage)
 * - Content-Type: application/json on all responses
 * - X-Kafka-Request-ID on all responses
 * - 207 Multi-Status for partial failures
 *
 * Note: Some tests may not pass until the HTTP error handling is fully
 * implemented. They document the expected error contract.
 */
class HttpErrorHandlingTest extends HttpIntegrationTestHarness {

  private val testTopic = "error-handling-test"
  private val numPartitions = 3
  private var client: HttpTestClient = _
  private val mapper = new ObjectMapper()

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
  // Test 1: 503 has Retry-After header
  // ===================================================================
  @Test
  def testRetryAfterHeaderOn503(): Unit = {
    // Triggering a true 503 requires broker shutdown or overload.
    // For now, we verify the contract: if we ever get a 503, it must
    // have Retry-After. This test is a placeholder for when we can
    // reliably trigger 503.
    //
    // Alternative: stop the broker and try to produce
    // (the HTTP layer should return 503 during drain)
    val response = client.produce(httpBaseUrl, testTopic,
      Seq(ProduceRecord(partition = Some(0), value = Some(StringValue("test")))))

    if (response.status == 503) {
      val retryAfter = response.headers.get("Retry-After")
      assertTrue(retryAfter.isDefined,
        "503 response must have Retry-After header")
      assertTrue(retryAfter.get.toInt > 0,
        "Retry-After must be a positive integer")
    }
    // If not 503, this test passes vacuously (no 503 triggered)
  }

  // ===================================================================
  // Test 2: 429 has Retry-After header
  // ===================================================================
  @Test
  def testRetryAfterHeaderOn429(): Unit = {
    // Triggering 429 requires exceeding quota limits.
    // This test documents the expected behavior.
    // Full quota testing is in TASK-T.05.
    var got429 = false
    for (_ <- 0 until 1000 if !got429) {
      val response = client.produce(httpBaseUrl, testTopic,
        Seq(ProduceRecord(partition = Some(0), value = Some(StringValue("flood")))))
      if (response.status == 429) {
        got429 = true
        val retryAfter = response.headers.get("Retry-After")
        assertTrue(retryAfter.isDefined,
          "429 response must have Retry-After header")
      }
    }
    // If 429 was never triggered (quotas not configured), test passes vacuously
  }

  // ===================================================================
  // Test 3: 400 does NOT have Retry-After
  // ===================================================================
  @Test
  def testNoRetryAfterOn400(): Unit = {
    // Send malformed JSON
    val response = client.rawPost(
      s"$httpBaseUrl/v1/topics/$testTopic/records",
      "{invalid json!!!")

    if (response.getStatus == 400) {
      val headers = response.getHeaders.asScala
        .map(f => f.getName -> f.getValue).toMap
      assertFalse(headers.contains("Retry-After"),
        "400 response should NOT have Retry-After header")
    }
  }

  // ===================================================================
  // Test 4: 403 does NOT have Retry-After
  // ===================================================================
  @Test
  def testNoRetryAfterOn403(): Unit = {
    // Produce to a topic that would require ACLs.
    // Without ACL configuration, this may not trigger 403.
    // This test documents the expected behavior.
    val response = client.produce(httpBaseUrl, testTopic,
      Seq(ProduceRecord(partition = Some(0), value = Some(StringValue("test")))))

    if (response.status == 403) {
      assertFalse(response.headers.contains("Retry-After"),
        "403 response should NOT have Retry-After header")
    }
  }

  // ===================================================================
  // Test 5: 404 does NOT have Retry-After
  // ===================================================================
  @Test
  def testNoRetryAfterOn404(): Unit = {
    val response = client.rawGet(s"$httpBaseUrl/v1/topics/nonexistent-topic-xyz")

    if (response.getStatus == 404) {
      val headers = response.getHeaders.asScala
        .map(f => f.getName -> f.getValue).toMap
      assertFalse(headers.contains("Retry-After"),
        "404 response should NOT have Retry-After header")
    }
  }

  // ===================================================================
  // Test 6: Error body has errorCode + errorMessage
  // ===================================================================
  @Test
  def testErrorBodyStructure(): Unit = {
    // Trigger a 404 by requesting a nonexistent topic
    val response = client.rawGet(s"$httpBaseUrl/v1/topics/nonexistent-topic-xyz")

    if (response.getStatus >= 400) {
      val content = response.getContentAsString
      if (content != null && content.nonEmpty) {
        val body = mapper.readTree(content)
        assertTrue(body.has("errorCode"),
          "Error response should have errorCode field")
        assertTrue(body.has("errorMessage") || body.has("message"),
          "Error response should have errorMessage or message field")

        val errorCode = body.get("errorCode").asInt()
        assertTrue(errorCode != 0,
          "Error errorCode should be non-zero")
      }
    }
  }

  // ===================================================================
  // Test 7: Content-Type is always application/json
  // ===================================================================
  @Test
  def testContentTypeIsJson(): Unit = {
    // Successful response
    val successResp = client.produce(httpBaseUrl, testTopic,
      Seq(ProduceRecord(partition = Some(0), value = Some(StringValue("ct-test")))))
    val successContentType = successResp.headers.get("Content-Type")
    assertTrue(successContentType.exists(_.contains("application/json")),
      s"Success response Content-Type should be application/json, got: $successContentType")

    // Error response
    val errorResp = client.rawGet(s"$httpBaseUrl/v1/topics/nonexistent-xyz")
    val errorHeaders = errorResp.getHeaders.asScala
      .map(f => f.getName -> f.getValue).toMap
    val errorContentType = errorHeaders.get("Content-Type")
    if (errorResp.getStatus >= 400) {
      assertTrue(errorContentType.exists(_.contains("application/json")),
        s"Error response Content-Type should be application/json, got: $errorContentType")
    }
  }

  // ===================================================================
  // Test 8: X-Kafka-Request-ID is present and unique
  // ===================================================================
  @Test
  def testRequestIdHeader(): Unit = {
    val resp1 = client.produce(httpBaseUrl, testTopic,
      Seq(ProduceRecord(partition = Some(0), value = Some(StringValue("req-id-1")))))
    val resp2 = client.produce(httpBaseUrl, testTopic,
      Seq(ProduceRecord(partition = Some(0), value = Some(StringValue("req-id-2")))))

    val reqId1 = resp1.headers.get("X-Kafka-Request-ID")
    val reqId2 = resp2.headers.get("X-Kafka-Request-ID")

    assertTrue(reqId1.isDefined && reqId1.get.nonEmpty,
      "Response 1 should have X-Kafka-Request-ID header")
    assertTrue(reqId2.isDefined && reqId2.get.nonEmpty,
      "Response 2 should have X-Kafka-Request-ID header")
    assertNotEquals(reqId1.get, reqId2.get,
      "Two requests should have different request IDs")
  }

  // ===================================================================
  // Test 9: Multi-partition partial failure -> 207 Multi-Status
  // ===================================================================
  @Test
  def testMultiPartitionPartialFailure207(): Unit = {
    // Produce to a mix of valid and invalid partitions
    // Partition 0 exists (valid), partition 999 does not (invalid)
    val bodyJson = mapper.createObjectNode()
    val recordsArray = bodyJson.putArray("records")

    val validRecord = recordsArray.addObject()
    validRecord.put("partition", 0)
    val validValue = validRecord.putObject("value")
    validValue.put("type", "STRING")
    validValue.put("data", "good-record")

    val invalidRecord = recordsArray.addObject()
    invalidRecord.put("partition", 999)  // nonexistent partition
    val invalidValue = invalidRecord.putObject("value")
    invalidValue.put("type", "STRING")
    invalidValue.put("data", "bad-record")

    bodyJson.put("acks", "all")
    bodyJson.put("timeoutMs", 5000)

    val response = client.rawPost(
      s"$httpBaseUrl/v1/topics/$testTopic/records",
      mapper.writeValueAsString(bodyJson))

    // If partial failure is implemented, expect 207
    // Otherwise, the server may return 200 (all succeed) or 400 (all fail)
    val status = response.getStatus
    if (status == 207) {
      val body = mapper.readTree(response.getContentAsString)
      val offsets = body.get("offsets")
      assertNotNull(offsets, "207 response should have offsets array")
      assertTrue(offsets.size() >= 2,
        "Should have results for both partitions")

      // Verify at least one success and one error
      var hasSuccess = false
      var hasError = false
      for (i <- 0 until offsets.size()) {
        val partResult = offsets.get(i)
        if (partResult.get("errorCode").asInt() == 0) hasSuccess = true
        else hasError = true
      }
      assertTrue(hasSuccess, "207 should have at least one successful partition")
      assertTrue(hasError, "207 should have at least one failed partition")
    }
    // If not 207, the test passes vacuously (partial failure not yet implemented)
  }
}
```

---

## Tests

### Run Command

```bash
./gradlew :http-server:test --tests 'kafka.http.HttpErrorHandlingTest*'
```

### Expected Results

Tests should compile. Some tests use conditional assertions (checking behavior only when the expected status code is returned), so they will pass even if the error handling is not yet fully implemented. This is intentional -- they serve as executable documentation.

---

## Rules

- Extend `HttpIntegrationTestHarness` -- do NOT create a custom test harness.
- Use conditional assertions: check the status code first, then assert headers/body only if the expected status was returned. This prevents false failures when endpoints are not yet implemented.
- Tests that cannot reliably trigger a specific status code (e.g., 503, 429) should document this limitation in comments.
- Use `rawGet()` and `rawPost()` for low-level requests where the `HttpTestClient` helpers are not sufficient.
- Parse headers from `ContentResponse.getHeaders` for raw requests, or from `ProduceResponse.headers` for typed responses.

---

## Learning

(empty)

## Limitations

(empty)

## Field Notes

(empty)

---

## Acceptance Criteria

- [ ] `HttpErrorHandlingTest.scala` is created at `http-server/src/test/scala/integration/kafka/http/HttpErrorHandlingTest.scala`
- [ ] Test compiles with `./gradlew :http-server:compileTestScala`
- [ ] Tests pass (some may pass vacuously if endpoints are not yet implemented)
- [ ] Error contract is documented through assertions: Retry-After, error body, Content-Type, X-Kafka-Request-ID, 207
- [ ] No modifications to existing integration test files

---

## File Manifest

(empty -- to be filled after implementation)
