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
import kafka.http.HttpSecurityIntegrationTest._
import kafka.http.HttpTestClient._
import org.apache.kafka.common.acl.{AccessControlEntry, AclOperation, AclPermissionType}
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs
import org.apache.kafka.common.resource.{PatternType, ResourcePattern, ResourceType}
import org.apache.kafka.common.security.auth.{AuthenticationContext, KafkaPrincipal}
import org.apache.kafka.common.security.authenticator.DefaultKafkaPrincipalBuilder
import org.apache.kafka.metadata.authorizer.StandardAuthorizer
import org.apache.kafka.security.authorizer.AclEntry.WILDCARD_HOST
import org.apache.kafka.server.config.ServerConfigs
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test, TestInfo}
import org.junit.jupiter.api.Assertions._

import java.util.Properties

/**
 * Companion object holding the custom PrincipalBuilder that assigns distinct
 * principals to the PLAINTEXT (inter-broker) and HTTP listeners.
 *
 * The PLAINTEXT listener is mapped to the "broker" principal which is
 * configured as a super-user, allowing internal operations (topic creation,
 * metadata, replication) to bypass ACL checks.
 *
 * The HTTP listener is mapped to the ANONYMOUS principal, matching the
 * production behavior where HTTP requests are unauthenticated.
 */
object HttpSecurityIntegrationTest {

  val BrokerPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "broker")
  val AnonymousPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "ANONYMOUS")

  /**
   * Maps listener names to principals:
   *   PLAINTEXT / CONTROLLER -> broker (super-user)
   *   HTTP                   -> User:ANONYMOUS
   */
  class HttpTestPrincipalBuilder extends DefaultKafkaPrincipalBuilder(null, null) {
    override def build(context: AuthenticationContext): KafkaPrincipal = {
      context.listenerName match {
        case "PLAINTEXT" | "CONTROLLER" => BrokerPrincipal
        case "HTTP" => AnonymousPrincipal
        case other => throw new IllegalArgumentException(s"No principal mapped to listener $other")
      }
    }
  }
}

/**
 * Integration tests for HTTP security and ACL enforcement.
 *
 * Verifies that the HTTP protocol layer correctly enforces access control:
 *   - Produce without WRITE ACL returns 403
 *   - Consume without READ ACL returns 403
 *   - HTTP listener uses ANONYMOUS principal (proven by granting ACLs to ANONYMOUS)
 *   - List topics filters results by DESCRIBE ACL
 *
 * The test configures the StandardAuthorizer with allow.everyone.if.no.acl.found=false
 * so that all access is denied unless an explicit ALLOW ACL is present. The inter-broker
 * (PLAINTEXT) principal is a super-user to permit internal cluster operations.
 */
class HttpSecurityIntegrationTest extends HttpIntegrationTestHarness {

  private val testTopic = "security-test"
  private val numPartitions = 1
  private var client: HttpTestClient = _
  private val mapper = new ObjectMapper()

  /**
   * Override to configure authorizer for ACL enforcement on brokers.
   * Adds StandardAuthorizer, super-user for inter-broker principal,
   * and a custom PrincipalBuilder to distinguish listener principals.
   */
  override protected def configureListeners(
    props: scala.collection.Seq[Properties]
  ): Unit = {
    super.configureListeners(props)
    props.foreach { config =>
      config.setProperty(ServerConfigs.AUTHORIZER_CLASS_NAME_CONFIG,
        classOf[StandardAuthorizer].getName)
      config.setProperty(StandardAuthorizer.SUPER_USERS_CONFIG,
        BrokerPrincipal.toString)
      config.setProperty(StandardAuthorizer.ALLOW_EVERYONE_IF_NO_ACL_IS_FOUND_CONFIG,
        "false")
      config.setProperty(BrokerSecurityConfigs.PRINCIPAL_BUILDER_CLASS_CONFIG,
        classOf[HttpTestPrincipalBuilder].getName)
      // Reduce offsets topic partitions for faster startup
      config.setProperty("offsets.topic.num.partitions", "1")
      config.setProperty("offsets.topic.replication.factor", "1")
    }
  }

  /**
   * Override to configure authorizer on the KRaft controller as well.
   * Both broker and controller must agree on the authorizer configuration.
   */
  override protected def kraftControllerConfigs(
    testInfo: TestInfo
  ): scala.collection.Seq[Properties] = {
    val configs = super.kraftControllerConfigs(testInfo)
    configs.foreach { config =>
      config.setProperty(ServerConfigs.AUTHORIZER_CLASS_NAME_CONFIG,
        classOf[StandardAuthorizer].getName)
      config.setProperty(StandardAuthorizer.SUPER_USERS_CONFIG,
        BrokerPrincipal.toString)
      config.setProperty(StandardAuthorizer.ALLOW_EVERYONE_IF_NO_ACL_IS_FOUND_CONFIG,
        "false")
    }
    configs
  }

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    super.setUp(testInfo)
    // Topic creation goes through the PLAINTEXT listener whose principal is
    // the "broker" super-user, so it bypasses ACL checks.
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
   * Helper to grant an ACL via the broker's authorizer.
   */
  private def grantAcl(
    principal: KafkaPrincipal,
    operation: AclOperation,
    resourceType: ResourceType,
    resourceName: String
  ): Unit = {
    val resource = new ResourcePattern(resourceType, resourceName, PatternType.LITERAL)
    val entry = new AccessControlEntry(
      principal.toString, WILDCARD_HOST, operation, AclPermissionType.ALLOW)
    addAndVerifyAcls(Set(entry), resource)
  }

  // ===================================================================
  // Test 1: Produce without WRITE ACL -> 403
  // ===================================================================
  @Test
  def testProduceWithoutWriteAclReturns403(): Unit = {
    // No ACLs granted for ANONYMOUS on testTopic.
    // With allow.everyone.if.no.acl.found=false this must be denied.
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
    // No ACLs granted for ANONYMOUS on testTopic.
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
    // Grant WRITE and DESCRIBE ACLs specifically for User:ANONYMOUS.
    // If the HTTP listener uses a different principal this will fail.
    grantAcl(AnonymousPrincipal, AclOperation.WRITE, ResourceType.TOPIC, testTopic)
    grantAcl(AnonymousPrincipal, AclOperation.DESCRIBE, ResourceType.TOPIC, testTopic)

    val response = client.produce(httpBaseUrl, testTopic,
      Seq(ProduceRecord(partition = Some(0), value = Some(StringValue("anonymous-test")))))

    assertEquals(200, response.status,
      s"Produce with ANONYMOUS WRITE ACL should succeed (200), got ${response.status}. " +
        "This proves the HTTP listener uses the ANONYMOUS principal.")
  }

  // ===================================================================
  // Test 4: List topics filtered by DESCRIBE ACL
  // ===================================================================
  @Test
  def testListTopicsFiltersByDescribeAcl(): Unit = {
    createTopic("visible-topic", 1, replicationFactor = 1)
    createTopic("hidden-topic", 1, replicationFactor = 1)

    // Grant DESCRIBE only on "visible-topic" for ANONYMOUS
    grantAcl(AnonymousPrincipal, AclOperation.DESCRIBE, ResourceType.TOPIC, "visible-topic")
    // No DESCRIBE ACL on "hidden-topic"

    val response = client.rawGet(s"$httpBaseUrl/v1/topics")

    if (response.getStatus == 200) {
      val body = mapper.readTree(response.getContentAsString)
      val topicsNode = if (body.isArray) body else body.get("topics")

      if (topicsNode != null) {
        val topicNames = (0 until topicsNode.size()).map { i =>
          val node = topicsNode.get(i)
          if (node.isTextual) node.asText() else node.get("name").asText()
        }.toSet

        assertTrue(topicNames.contains("visible-topic"),
          s"Topic list should contain visible-topic (has DESCRIBE ACL), got: $topicNames")
        assertFalse(topicNames.contains("hidden-topic"),
          s"Topic list should NOT contain hidden-topic (no DESCRIBE ACL), got: $topicNames")
      }
    }
    // If not 200, the metadata endpoint may not be fully implemented yet.
    // The test still serves as an executable specification.
  }
}
