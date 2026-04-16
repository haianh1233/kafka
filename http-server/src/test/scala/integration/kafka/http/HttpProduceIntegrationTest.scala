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

import com.fasterxml.jackson.databind.ObjectMapper
import kafka.http.HttpTestClient._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test, TestInfo}
import org.junit.jupiter.api.Assertions._

/**
 * Integration tests for the HTTP produce endpoint (POST /v1/topics/{topic}/records).
 *
 * Tests the local leader path (single-broker, all partitions are local).
 * Multi-broker produce forwarding tests are in Phase D.
 */
class HttpProduceIntegrationTest extends HttpIntegrationTestHarness {

  private val testTopic = "http-produce-test"
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

  @Test
  def testProduceSinglePartitionStringValue(): Unit = {
    val response = client.produce(httpBaseUrl, testTopic,
      Seq(ProduceRecord(
        partition = Some(0),
        value = Some(StringValue("hello world"))
      )))

    assertEquals(200, response.status)
    val offsets = response.body.get("offsets")
    assertNotNull(offsets)
    assertEquals(1, offsets.size())
    assertEquals(0, offsets.get(0).get("partition").asInt())
    assertEquals(0, offsets.get(0).get("errorCode").asInt())
    assertTrue(offsets.get(0).get("offset").asLong() >= 0)
  }

  @Test
  def testProduceSinglePartitionBinaryValue(): Unit = {
    val binaryData = "binary content".getBytes("UTF-8")
    val response = client.produce(httpBaseUrl, testTopic,
      Seq(ProduceRecord(
        partition = Some(0),
        value = Some(BinaryValue(binaryData))
      )))

    assertEquals(200, response.status)
    assertEquals(0, response.body.get("offsets").get(0).get("errorCode").asInt())
  }

  @Test
  def testProduceSinglePartitionJsonValue(): Unit = {
    val jsonData = mapper.createObjectNode().put("amount", 42.0)
    val response = client.produce(httpBaseUrl, testTopic,
      Seq(ProduceRecord(
        partition = Some(0),
        value = Some(JsonValue(jsonData))
      )))

    assertEquals(200, response.status)
    assertEquals(0, response.body.get("offsets").get(0).get("errorCode").asInt())
  }

  @Test
  def testProduceSinglePartitionNullValue(): Unit = {
    val response = client.produce(httpBaseUrl, testTopic,
      Seq(ProduceRecord(
        partition = Some(0),
        value = Some(NullValue)
      )))

    assertEquals(200, response.status)
    assertEquals(0, response.body.get("offsets").get(0).get("errorCode").asInt())
  }

  @Test
  def testProduceMultiPartition(): Unit = {
    val response = client.produce(httpBaseUrl, testTopic,
      Seq(
        ProduceRecord(partition = Some(0), value = Some(StringValue("p0"))),
        ProduceRecord(partition = Some(1), value = Some(StringValue("p1"))),
        ProduceRecord(partition = Some(2), value = Some(StringValue("p2")))
      ))

    assertEquals(200, response.status)
    val offsets = response.body.get("offsets")
    assertEquals(3, offsets.size())
  }

  @Test
  def testProduceWithAcksAll(): Unit = {
    val response = client.produce(httpBaseUrl, testTopic,
      Seq(ProduceRecord(partition = Some(0), value = Some(StringValue("acks-all")))),
      acks = "all")
    assertEquals(200, response.status)
  }

  @Test
  def testProduceWithAcksLeader(): Unit = {
    val response = client.produce(httpBaseUrl, testTopic,
      Seq(ProduceRecord(partition = Some(0), value = Some(StringValue("acks-leader")))),
      acks = "leader")
    assertEquals(200, response.status)
  }

  @Test
  def testProduceWithAcksNone(): Unit = {
    val response = client.produce(httpBaseUrl, testTopic,
      Seq(ProduceRecord(partition = Some(0), value = Some(StringValue("acks-none")))),
      acks = "none")
    // acks=0 may return 200 or 204 depending on implementation
    assertTrue(response.status >= 200 && response.status < 300)
  }

  @Test
  def testProduceToNonExistingTopic(): Unit = {
    val response = client.produce(httpBaseUrl, "does-not-exist",
      Seq(ProduceRecord(partition = Some(0), value = Some(StringValue("nope")))))
    // Should be 404 or error response
    assertTrue(response.status >= 400)
  }

  @Test
  def testProduceWithKey(): Unit = {
    val response = client.produce(httpBaseUrl, testTopic,
      Seq(ProduceRecord(
        partition = Some(0),
        key = Some(StringValue("order-123")),
        value = Some(StringValue("order data"))
      )))
    assertEquals(200, response.status)
    assertEquals(0, response.body.get("offsets").get(0).get("errorCode").asInt())
  }
}
