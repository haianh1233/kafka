# TASK-T.06: HTTP Security Integration Test

## Prerequisites

| Task | What it provides |
|------|-----------------|
| TASK-0.04 | Build verified -- base code compiles and existing tests pass |
| TASK-C.03 | Phase 1 integration test infrastructure -- HttpIntegrationTestHarness, HttpTestClient |

---

## Context

The HTTP protocol design specifies security behavior:
- HTTP listener uses `ANONYMOUS` principal (no SASL authentication over HTTP)
- ACL enforcement applies to HTTP requests the same as binary requests
- Produce without WRITE ACL returns 403
- Consume without READ ACL returns 403
- List topics filters by DESCRIBE ACL
- HTTPS listener supports TLS (mTLS for client certificate authentication)

Currently there are **zero integration tests** for HTTP security. This task adds E2E tests that configure ACLs and verify access control enforcement over the HTTP protocol.

### Key Code References

- **Test harness**: `HttpIntegrationTestHarness.scala`
- **Test client**: `HttpTestClient.scala`
- **Auth in KafkaApis**: `KafkaApis.scala` -- `authHelper.filterByAuthorized()` in HTTP handlers
- **ACL test pattern**: existing Kafka authorizer integration tests
- **Design doc**: `ivy-docs/http-protocol-design.md` sections on security

---

## Specification

### Test 1: `testProduceWithoutWriteAclReturns403`
- Configure authorizer with ACLs enabled
- Do NOT grant WRITE ACL on the test topic for the ANONYMOUS principal
- Produce a record via HTTP
- Verify 403 Forbidden response

### Test 2: `testConsumeWithoutReadAclReturns403`
- Configure authorizer with ACLs enabled
- Do NOT grant READ ACL on the test topic for the ANONYMOUS principal
- Consume via HTTP
- Verify 403 Forbidden response

### Test 3: `testHttpListenerUsesAnonymousPrincipal`
- Configure authorizer
- Grant WRITE ACL for `User:ANONYMOUS` on the test topic
- Produce a record via HTTP
- Verify 200 success (proving the ANONYMOUS principal was used)

### Test 4: `testListTopicsFiltersByDescribeAcl`
- Create two topics: "visible-topic" and "hidden-topic"
- Grant DESCRIBE ACL on "visible-topic" for ANONYMOUS
- Do NOT grant DESCRIBE ACL on "hidden-topic"
- List topics via HTTP (`GET /v1/topics`)
- Verify "visible-topic" is in the response, "hidden-topic" is not

---

## Implementation Details

### File: `http-server/src/test/scala/integration/kafka/http/HttpSecurityIntegrationTest.scala`

Extends `HttpIntegrationTestHarness`. Overrides broker configuration to enable an authorizer (e.g., `AclAuthorizer`). Sets up ACLs using `AdminClient` or direct authorizer API.

**Important**: The standard `HttpIntegrationTestHarness` does not configure an authorizer. This test must override broker properties to enable `authorizer.class.name`.

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
import org.apache.kafka.common.acl._
import org.apache.kafka.common.resource.{PatternType, ResourcePattern, ResourceType}
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test, TestInfo}
import org.junit.jupiter.api.Assertions._

import java.util.Properties
import scala.collection.Seq
import scala.jdk.CollectionConverters._

/**
 * Integration tests for HTTP security and ACL enforcement.
 *
 * Tests:
 * - Produce without WRITE ACL -> 403
 * - Consume without READ ACL -> 403
 * - HTTP listener uses ANONYMOUS principal
 * - List topics filtered by DESCRIBE ACL
 *
 * Note: These tests require the authorizer to be enabled and ACL enforcement
 * to work with HTTP requests. They may not pass until the security integration
 * is complete. They should compile and serve as executable specifications.
 */
class HttpSecurityIntegrationTest extends HttpIntegrationTestHarness {

  private val testTopic = "security-test"
  private val numPartitions = 1
  private var client: HttpTestClient = _
  private val mapper = new ObjectMapper()

  // The ANONYMOUS principal used by HTTP listeners
  private val anonymousPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "ANONYMOUS")

  /**
   * Override to configure authorizer for ACL enforcement.
   */
  override protected def configureListeners(props: Seq[Properties]): Unit = {
    super.configureListeners(props)
    props.foreach { config =>
      // Enable the standard AclAuthorizer
      config.setProperty("authorizer.class.name",
        "org.apache.kafka.metadata.authorizer.StandardAuthorizer")
      // Allow all actions for super users (the inter-broker listener)
      config.setProperty("super.users", "User:admin")
      // Allow broker operations for the inter-broker principal
      config.setProperty("allow.everyone.if.no.acl.found", "false")
    }
  }

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    super.setUp(testInfo)
    // Create topic (may need super-user privileges)
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
   * Helper to add an ACL entry via the broker's authorizer.
   */
  private def addAcl(
    principal: KafkaPrincipal,
    operation: AclOperation,
    resourceType: ResourceType,
    resourceName: String,
    permissionType: AclPermissionType = AclPermissionType.ALLOW
  ): Unit = {
    // Use AdminClient to create ACLs
    val resource = new ResourcePattern(resourceType, resourceName, PatternType.LITERAL)
    val entry = new AccessControlEntry(
      principal.toString, "*", operation, permissionType)
    val binding = new AclBinding(resource, entry)

    // Get the admin client from the test harness
    val adminClient = createAdminClient()
    try {
      adminClient.createAcls(java.util.List.of(binding)).all().get()
    } finally {
      adminClient.close()
    }
  }

  // ===================================================================
  // Test 1: Produce without WRITE ACL -> 403
  // ===================================================================
  @Test
  def testProduceWithoutWriteAclReturns403(): Unit = {
    // Do NOT add WRITE ACL for ANONYMOUS on testTopic
    // With allow.everyone.if.no.acl.found=false, this should be denied

    val response = client.produce(httpBaseUrl, testTopic,
      Seq(ProduceRecord(partition = Some(0), value = Some(StringValue("denied")))))

    assertEquals(403, response.status,
      s"Produce without WRITE ACL should return 403, got ${response.status}")
  }

  // ===================================================================
  // Test 2: Consume without READ ACL -> 403
  // ===================================================================
  @Test
  def testConsumeWithoutReadAclReturns403(): Unit = {
    // Do NOT add READ ACL for ANONYMOUS on testTopic
    val response = client.consume(httpBaseUrl, testTopic,
      Seq(FetchPartitionSpec(partition = 0, offset = 0)),
      maxWaitMs = 100)

    assertEquals(403, response.status,
      s"Consume without READ ACL should return 403, got ${response.status}")
  }

  // ===================================================================
  // Test 3: HTTP listener uses ANONYMOUS principal
  // ===================================================================
  @Test
  def testHttpListenerUsesAnonymousPrincipal(): Unit = {
    // Grant WRITE ACL specifically for User:ANONYMOUS
    addAcl(anonymousPrincipal, AclOperation.WRITE, ResourceType.TOPIC, testTopic)
    // Also need DESCRIBE for topic metadata
    addAcl(anonymousPrincipal, AclOperation.DESCRIBE, ResourceType.TOPIC, testTopic)

    val response = client.produce(httpBaseUrl, testTopic,
      Seq(ProduceRecord(partition = Some(0), value = Some(StringValue("anonymous-test")))))

    assertEquals(200, response.status,
      s"Produce with ANONYMOUS WRITE ACL should return 200, got ${response.status}. " +
        "This proves the HTTP listener uses the ANONYMOUS principal.")
  }

  // ===================================================================
  // Test 4: List topics filtered by DESCRIBE ACL
  // ===================================================================
  @Test
  def testListTopicsFiltersByDescribeAcl(): Unit = {
    // Create a second topic
    createTopic("visible-topic", 1, replicationFactor = 1)
    createTopic("hidden-topic", 1, replicationFactor = 1)

    // Grant DESCRIBE only on "visible-topic" for ANONYMOUS
    addAcl(anonymousPrincipal, AclOperation.DESCRIBE, ResourceType.TOPIC, "visible-topic")
    // Do NOT grant DESCRIBE on "hidden-topic"

    val response = client.rawGet(s"$httpBaseUrl/v1/topics")

    if (response.getStatus == 200) {
      val body = mapper.readTree(response.getContentAsString)
      val topicsNode = if (body.isArray) body else body.get("topics")

      if (topicsNode != null) {
        val topicNames = (0 until topicsNode.size()).map(i => {
          val node = topicsNode.get(i)
          if (node.isTextual) node.asText() else node.get("name").asText()
        }).toSet

        assertTrue(topicNames.contains("visible-topic"),
          "Topic list should contain visible-topic (has DESCRIBE ACL)")
        assertFalse(topicNames.contains("hidden-topic"),
          "Topic list should NOT contain hidden-topic (no DESCRIBE ACL)")
      }
    }
    // If not 200, the metadata endpoint may not be implemented yet
  }
}
```

---

## Tests

### Run Command

```bash
./gradlew :http-server:test --tests 'kafka.http.HttpSecurityIntegrationTest*'
```

### Expected Results

Tests should compile. They may not pass until:
1. The authorizer integration with HTTP requests is complete
2. The `StandardAuthorizer` is available in the test classpath
3. The HTTP listener correctly sets up the ANONYMOUS principal

The tests document the expected security contract for HTTP endpoints.

---

## Rules

- Extend `HttpIntegrationTestHarness` -- override `configureListeners` to enable the authorizer.
- Use `allow.everyone.if.no.acl.found=false` to ensure ACLs are enforced by default.
- Add ACLs explicitly for the ANONYMOUS principal where access should be allowed.
- The ANONYMOUS principal is the default for HTTP listeners (no SASL/authentication).
- If the `StandardAuthorizer` is not available, use `AclAuthorizer` or whichever authorizer is configured in the test environment.
- Each test must be independent -- ACL setup should be done within each test method.

---

## Learning

(empty)

## Limitations

(empty)

## Field Notes

(empty)

---

## Acceptance Criteria

- [ ] `HttpSecurityIntegrationTest.scala` is created at `http-server/src/test/scala/integration/kafka/http/HttpSecurityIntegrationTest.scala`
- [ ] Test compiles with `./gradlew :http-server:compileTestScala`
- [ ] Tests document ACL enforcement behavior for HTTP endpoints
- [ ] ANONYMOUS principal usage is verified
- [ ] No modifications to existing integration test files

---

## File Manifest

(empty -- to be filled after implementation)
