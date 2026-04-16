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

import org.junit.jupiter.api.{AfterEach, BeforeEach, Test, TestInfo}
import org.junit.jupiter.api.Assertions._

/**
 * Integration tests for the HTTP health check endpoint (GET /v1/health).
 *
 * Verifies that the health check returns correct status, brokerId,
 * and clusterId when the broker is running.
 */
class HttpHealthCheckIntegrationTest extends HttpIntegrationTestHarness {

  private var client: HttpTestClient = _

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    super.setUp(testInfo)
    client = new HttpTestClient()
    client.start()
  }

  @AfterEach
  override def tearDown(): Unit = {
    if (client != null) client.close()
    super.tearDown()
  }

  @Test
  def testHealthCheckReturns200WhenRunning(): Unit = {
    val response = client.health(httpBaseUrl)
    assertEquals(200, response.status)
    assertEquals("RUNNING", response.body.get("status").asText())
  }

  @Test
  def testHealthCheckContainsBrokerId(): Unit = {
    val response = client.health(httpBaseUrl)
    assertTrue(response.body.has("brokerId"))
    assertEquals(0, response.body.get("brokerId").asInt())
  }

  @Test
  def testHealthCheckContainsClusterId(): Unit = {
    val response = client.health(httpBaseUrl)
    assertTrue(response.body.has("clusterId"))
    assertFalse(response.body.get("clusterId").asText().isEmpty)
  }
}
