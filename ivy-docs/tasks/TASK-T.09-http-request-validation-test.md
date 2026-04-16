# TASK-T.09: HTTP Request Validation Integration Test

## Prerequisites

| Task | What it provides |
|------|-----------------|
| TASK-0.04 | Build verified -- base code compiles and existing tests pass |
| TASK-C.03 | Phase 1 integration test infrastructure -- HttpIntegrationTestHarness, HttpTestClient |

---

## Context

The HTTP protocol must validate incoming requests and return clear error responses for malformed input. This is a critical boundary -- HTTP clients can send arbitrary payloads, unlike binary protocol clients that use structured RPC frames.

Currently there are **zero integration tests** for HTTP request validation. This task adds E2E tests that verify the server correctly rejects bad requests with appropriate status codes and error messages.

Validation areas:
- Malformed JSON body (parse error)
- Missing required fields (e.g., produce without records)
- Invalid field values (e.g., negative partition, unknown acks value)
- Oversized request body (exceeds `message.max.bytes`)
- Wrong Content-Type header
- Empty request body
- Invalid URL paths (no matching route)

### Key Code References

- **Test harness**: `HttpIntegrationTestHarness.scala`
- **Test client**: `HttpTestClient.scala` -- `rawPost()`, `rawGet()`
- **Router**: `HttpRouter.java` -- URL path parsing and validation
- **Translator**: `HttpRequestTranslator.java` -- JSON-to-Kafka request translation
- **Design doc**: `ivy-docs/http-protocol-design.md` sections on request validation

---

## Specification

### Test 1: `testMalformedJsonReturns400`
- POST to `/v1/topics/{topic}/records` with `{not valid json}`
- Verify 400 Bad Request response
- Verify error body has `errorCode` and `errorMessage`

### Test 2: `testMissingRecordsFieldReturns400`
- POST to `/v1/topics/{topic}/records` with `{"acks": "all"}` (no records array)
- Verify 400 Bad Request response

### Test 3: `testInvalidAcksValueReturns400`
- POST to `/v1/topics/{topic}/records` with `{"records": [...], "acks": "invalid"}`
- Verify 400 Bad Request response

### Test 4: `testOversizedRequestBodyReturns413`
- POST to `/v1/topics/{topic}/records` with a body larger than `message.max.bytes`
- Verify 413 Payload Too Large response (or 400 with appropriate error)

### Test 5: `testWrongContentTypeReturns415`
- POST to `/v1/topics/{topic}/records` with `Content-Type: text/plain`
- Verify 415 Unsupported Media Type (or 400)

### Test 6: `testEmptyRequestBodyReturns400`
- POST to `/v1/topics/{topic}/records` with empty body
- Verify 400 Bad Request response

### Test 7: `testInvalidUrlPathReturns404`
- GET `/v1/nonexistent/path`
- Verify 404 Not Found response

### Test 8: `testNegativePartitionReturns400`
- POST produce with `"partition": -1`
- Verify 400 Bad Request response

---

## Implementation Details

### File: `http-server/src/test/scala/integration/kafka/http/HttpRequestValidationTest.scala`

Extends `HttpIntegrationTestHarness`. Uses `HttpTestClient.rawPost()` and `rawGet()` with hand-crafted payloads to test validation boundaries.

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
import org.eclipse.jetty.client.{HttpClient, StringRequestContent}
import org.eclipse.jetty.http.HttpMethod
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test, TestInfo}
import org.junit.jupiter.api.Assertions._

import java.nio.charset.StandardCharsets
import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters._

/**
 * Integration tests for HTTP request validation.
 *
 * Tests:
 * - Malformed JSON -> 400
 * - Missing required fields -> 400
 * - Invalid field values -> 400
 * - Oversized payload -> 413 or 400
 * - Wrong Content-Type -> 415 or 400
 * - Empty body -> 400
 * - Invalid URL path -> 404
 * - Negative partition -> 400
 *
 * Note: These tests document expected validation behavior. They may not pass
 * until the HTTP router and translator validation is complete. They should
 * compile and serve as executable specifications.
 */
class HttpRequestValidationTest extends HttpIntegrationTestHarness {

  private val testTopic = "validation-test"
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
  // Test 1: Malformed JSON -> 400
  // ===================================================================
  @Test
  def testMalformedJsonReturns400(): Unit = {
    val response = client.rawPost(
      s"$httpBaseUrl/v1/topics/$testTopic/records",
      "{not valid json at all!!!")

    assertEquals(400, response.getStatus,
      s"Malformed JSON should return 400, got ${response.getStatus}")

    // Verify error body structure
    val content = response.getContentAsString
    if (content != null && content.nonEmpty) {
      try {
        val body = mapper.readTree(content)
        assertTrue(body.has("errorCode") || body.has("error"),
          "Error response should have errorCode or error field")
      } catch {
        case _: Exception => // If error body is not JSON, that is a separate concern
      }
    }
  }

  // ===================================================================
  // Test 2: Missing records field -> 400
  // ===================================================================
  @Test
  def testMissingRecordsFieldReturns400(): Unit = {
    val body = """{"acks": "all", "timeoutMs": 5000}"""
    val response = client.rawPost(
      s"$httpBaseUrl/v1/topics/$testTopic/records", body)

    assertTrue(response.getStatus == 400 || response.getStatus == 422,
      s"Missing records field should return 400 or 422, got ${response.getStatus}")
  }

  // ===================================================================
  // Test 3: Invalid acks value -> 400
  // ===================================================================
  @Test
  def testInvalidAcksValueReturns400(): Unit = {
    val body = mapper.createObjectNode()
    val records = body.putArray("records")
    val record = records.addObject()
    record.put("partition", 0)
    val value = record.putObject("value")
    value.put("type", "STRING")
    value.put("data", "test")
    body.put("acks", "invalid-value")  // Not "all", "leader", or "none"
    body.put("timeoutMs", 5000)

    val response = client.rawPost(
      s"$httpBaseUrl/v1/topics/$testTopic/records",
      mapper.writeValueAsString(body))

    assertTrue(response.getStatus >= 400 && response.getStatus < 500,
      s"Invalid acks value should return 4xx, got ${response.getStatus}")
  }

  // ===================================================================
  // Test 4: Oversized request body -> 413 or 400
  // ===================================================================
  @Test
  def testOversizedRequestBodyReturns413(): Unit = {
    // Create a very large payload (larger than default message.max.bytes = 1MB)
    val largeValue = "x" * (2 * 1024 * 1024)  // 2MB
    val body = mapper.createObjectNode()
    val records = body.putArray("records")
    val record = records.addObject()
    record.put("partition", 0)
    val value = record.putObject("value")
    value.put("type", "STRING")
    value.put("data", largeValue)
    body.put("acks", "all")
    body.put("timeoutMs", 5000)

    val response = client.rawPost(
      s"$httpBaseUrl/v1/topics/$testTopic/records",
      mapper.writeValueAsString(body))

    // Either 413 (payload too large) or 400 (bad request)
    assertTrue(response.getStatus == 413 || response.getStatus == 400,
      s"Oversized payload should return 413 or 400, got ${response.getStatus}")
  }

  // ===================================================================
  // Test 5: Wrong Content-Type -> 415 or 400
  // ===================================================================
  @Test
  def testWrongContentTypeReturns415(): Unit = {
    // Send with text/plain instead of application/json
    // Need to use a raw Jetty client to control Content-Type
    val jettyClient = new HttpClient()
    jettyClient.setConnectTimeout(5000)
    jettyClient.start()
    try {
      val validJson = """{"records": [{"partition": 0, "value": {"type": "STRING", "data": "test"}}], "acks": "all", "timeoutMs": 5000}"""
      val response = jettyClient.newRequest(s"$httpBaseUrl/v1/topics/$testTopic/records")
        .method(HttpMethod.POST)
        .headers(h => h.put("Content-Type", "text/plain"))
        .body(new StringRequestContent("text/plain", validJson, StandardCharsets.UTF_8))
        .timeout(5, TimeUnit.SECONDS)
        .send()

      // Either 415 (unsupported media type) or 400 (bad request)
      assertTrue(response.getStatus == 415 || response.getStatus == 400,
        s"Wrong Content-Type should return 415 or 400, got ${response.getStatus}")
    } finally {
      jettyClient.stop()
    }
  }

  // ===================================================================
  // Test 6: Empty body -> 400
  // ===================================================================
  @Test
  def testEmptyRequestBodyReturns400(): Unit = {
    val response = client.rawPost(
      s"$httpBaseUrl/v1/topics/$testTopic/records", "")

    assertTrue(response.getStatus >= 400 && response.getStatus < 500,
      s"Empty body should return 4xx, got ${response.getStatus}")
  }

  // ===================================================================
  // Test 7: Invalid URL path -> 404
  // ===================================================================
  @Test
  def testInvalidUrlPathReturns404(): Unit = {
    val response = client.rawGet(s"$httpBaseUrl/v1/nonexistent/path/here")

    assertEquals(404, response.getStatus,
      s"Invalid URL path should return 404, got ${response.getStatus}")
  }

  // ===================================================================
  // Test 8: Negative partition -> 400
  // ===================================================================
  @Test
  def testNegativePartitionReturns400(): Unit = {
    val response = client.produce(httpBaseUrl, testTopic,
      Seq(ProduceRecord(partition = Some(-1), value = Some(StringValue("bad-partition")))))

    assertTrue(response.status >= 400 && response.status < 500,
      s"Negative partition should return 4xx, got ${response.status}")
  }
}
```

---

## Tests

### Run Command

```bash
./gradlew :http-server:test --tests 'kafka.http.HttpRequestValidationTest*'
```

### Expected Results

Tests should compile. They may not all pass until the HTTP router and translator implement full validation. Each test documents the expected validation behavior.

---

## Rules

- Extend `HttpIntegrationTestHarness` -- do NOT create a custom test harness.
- Use `rawPost()` for sending hand-crafted payloads that bypass normal client validation.
- Accept multiple valid error status codes where the specification is ambiguous (e.g., 400 vs 422 for missing fields).
- Each test should be self-contained and test exactly one validation rule.
- Error bodies should be parseable JSON where possible.

---

## Learning

(empty)

## Limitations

(empty)

## Field Notes

(empty)

---

## Acceptance Criteria

- [ ] `HttpRequestValidationTest.scala` is created at `http-server/src/test/scala/integration/kafka/http/HttpRequestValidationTest.scala`
- [ ] Test compiles with `./gradlew :http-server:compileTestScala`
- [ ] Tests document request validation behavior for the HTTP protocol
- [ ] Each test targets a single validation rule
- [ ] No modifications to existing integration test files

---

## File Manifest

(empty -- to be filled after implementation)
