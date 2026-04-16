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

import kafka.http.HttpTestClient._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test, TestInfo}
import org.junit.jupiter.api.Assertions._

/**
 * Integration tests for the HTTP consume endpoint (POST /v1/topics/{topic}/records:fetch).
 *
 * Tests the local leader path (single-broker, all partitions are local).
 * Multi-broker consume forwarding tests are in Phase D.
 */
class HttpConsumeIntegrationTest extends HttpIntegrationTestHarness {

  private val testTopic = "http-consume-test"
  private val numPartitions = 3
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

  @Test
  def testFetchWithExplicitOffset(): Unit = {
    // Produce a record first
    client.produce(httpBaseUrl, testTopic,
      Seq(ProduceRecord(partition = Some(0), value = Some(StringValue("msg-0")))))

    // Consume from offset 0
    val response = client.consume(httpBaseUrl, testTopic,
      Seq(FetchPartitionSpec(partition = 0, offset = 0)),
      maxWaitMs = 5000)

    assertEquals(200, response.status)
    val partitions = response.body.get("partitions")
    assertNotNull(partitions)
    assertTrue(partitions.size() > 0)
    val p0 = partitions.get(0)
    assertEquals(0, p0.get("partition").asInt())
    val records = p0.get("records")
    assertTrue(records.size() > 0)
  }

  @Test
  def testFetchEmptyPartitionReturnsEmptyRecords(): Unit = {
    // Fetch from an empty partition with short wait
    val response = client.consume(httpBaseUrl, testTopic,
      Seq(FetchPartitionSpec(partition = 0, offset = 0)),
      maxWaitMs = 100)

    assertEquals(200, response.status)
    val partitions = response.body.get("partitions")
    assertNotNull(partitions)
    val p0 = partitions.get(0)
    assertEquals(0, p0.get("errorCode").asInt()) // empty is NOT an error
    val records = p0.get("records")
    assertEquals(0, records.size())
  }

  @Test
  def testMaxWaitMsCapped(): Unit = {
    // Request maxWaitMs=30000 but config caps at 5000
    val response = client.consume(httpBaseUrl, testTopic,
      Seq(FetchPartitionSpec(partition = 0, offset = 0)),
      maxWaitMs = 30000)

    assertEquals(200, response.status)
    // Verify X-Kafka-MaxWait-Applied header
    val appliedWait = response.headers.get("X-Kafka-MaxWait-Applied")
    assertNotNull(appliedWait, "X-Kafka-MaxWait-Applied header must be present")
    assertEquals("5000", appliedWait.orNull)
  }

  @Test
  def testFetchMultiplePartitions(): Unit = {
    // Produce to multiple partitions
    client.produce(httpBaseUrl, testTopic,
      Seq(
        ProduceRecord(partition = Some(0), value = Some(StringValue("p0-msg"))),
        ProduceRecord(partition = Some(1), value = Some(StringValue("p1-msg")))
      ))

    // Fetch from both partitions
    val response = client.consume(httpBaseUrl, testTopic,
      Seq(
        FetchPartitionSpec(partition = 0, offset = 0),
        FetchPartitionSpec(partition = 1, offset = 0)
      ),
      maxWaitMs = 5000)

    assertEquals(200, response.status)
    val partitions = response.body.get("partitions")
    assertEquals(2, partitions.size())
  }

  @Test
  def testProduceThenConsumeRoundTrip(): Unit = {
    val testValue = "round-trip-value-12345"

    // Produce
    val produceResp = client.produce(httpBaseUrl, testTopic,
      Seq(ProduceRecord(
        partition = Some(0),
        key = Some(StringValue("my-key")),
        value = Some(StringValue(testValue))
      )))
    assertEquals(200, produceResp.status)
    val producedOffset = produceResp.body.get("offsets").get(0).get("offset").asLong()

    // Consume from the produced offset
    val consumeResp = client.consume(httpBaseUrl, testTopic,
      Seq(FetchPartitionSpec(partition = 0, offset = producedOffset)),
      maxWaitMs = 5000)

    assertEquals(200, consumeResp.status)
    val records = consumeResp.body.get("partitions").get(0).get("records")
    assertTrue(records.size() > 0)

    val firstRecord = records.get(0)
    assertEquals(producedOffset, firstRecord.get("offset").asLong())
    // Verify value round-tripped correctly
    assertEquals("STRING", firstRecord.get("value").get("type").asText())
    assertEquals(testValue, firstRecord.get("value").get("data").asText())
    // Verify key round-tripped correctly
    assertEquals("STRING", firstRecord.get("key").get("type").asText())
    assertEquals("my-key", firstRecord.get("key").get("data").asText())
  }

  @Test
  def testFetchWithReadCommittedIsolation(): Unit = {
    client.produce(httpBaseUrl, testTopic,
      Seq(ProduceRecord(partition = Some(0), value = Some(StringValue("committed")))))

    val response = client.consume(httpBaseUrl, testTopic,
      Seq(FetchPartitionSpec(partition = 0, offset = 0)),
      maxWaitMs = 5000,
      isolationLevel = "READ_COMMITTED")

    assertEquals(200, response.status)
  }

  @Test
  def testFetchNonExistingTopic(): Unit = {
    val response = client.consume(httpBaseUrl, "does-not-exist",
      Seq(FetchPartitionSpec(partition = 0, offset = 0)),
      maxWaitMs = 100)

    assertTrue(response.status >= 400)
  }
}
