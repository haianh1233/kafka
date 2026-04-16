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
package kafka.server.http

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.message.{ShareAcknowledgeResponseData, ShareFetchResponseData}
import org.apache.kafka.common.requests.{ShareAcknowledgeResponse, ShareFetchResponse}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

/**
 * Tests the share group API contract: request and response JSON shapes
 * must match the design document specification (section 4.5).
 */
class ShareGroupApiContractTest {

  private val mapper = new ObjectMapper()

  @Test
  def testPollRequestSchema(): Unit = {
    // Valid minimal poll request
    val valid = mapper.readTree("""{"topics":["orders"]}""")
    assertTrue(valid.has("topics"))
    assertTrue(valid.get("topics").isArray)
  }

  @Test
  def testPollRequestWithAllFields(): Unit = {
    val valid = mapper.readTree("""{"topics":["orders"],"maxRecords":100,"maxWaitMs":5000}""")
    assertEquals(100, valid.get("maxRecords").asInt())
    assertEquals(5000, valid.get("maxWaitMs").asInt())
  }

  @Test
  def testAcknowledgeRequestSchema(): Unit = {
    val valid = mapper.readTree("""{"acknowledgements":[
      {"acquireId":"acq-abc123","type":"ACCEPT"}
    ]}""")
    assertTrue(valid.has("acknowledgements"))
    val ack = valid.get("acknowledgements").get(0)
    assertEquals("acq-abc123", ack.get("acquireId").asText())
    assertEquals("ACCEPT", ack.get("type").asText())
  }

  @Test
  def testAcknowledgeTypes(): Unit = {
    val validTypes = List("ACCEPT", "REJECT", "RELEASE")
    validTypes.foreach { t =>
      val json = mapper.readTree(s"""{"acknowledgements":[{"acquireId":"x","type":"$t"}]}""")
      assertEquals(t, json.get("acknowledgements").get(0).get("type").asText())
    }
  }

  @Test
  def testPollResponseSchema(): Unit = {
    // Expected response shape
    val response = mapper.readTree("""{
      "records": [{
        "topic": "orders",
        "partition": 0,
        "offset": 42,
        "key": { "type": "STRING", "data": "order-123" },
        "value": { "type": "JSON", "data": { "amount": 42.0 } },
        "acquireId": "acq-abc123"
      }]
    }""")
    assertTrue(response.has("records"))
    val record = response.get("records").get(0)
    assertTrue(record.has("topic"))
    assertTrue(record.has("partition"))
    assertTrue(record.has("offset"))
    assertTrue(record.has("acquireId"))
    assertTrue(record.has("key"))
    assertTrue(record.has("value"))
  }

  @Test
  def testAcknowledgeResponseSchema(): Unit = {
    val response = mapper.readTree("""{
      "results": [
        { "acquireId": "acq-abc123", "errorCode": 0 },
        { "acquireId": "acq-def456", "errorCode": 0 }
      ]
    }""")
    assertTrue(response.has("results"))
    assertEquals(2, response.get("results").size())
  }

  @Test
  def testSerializeShareFetchResponseEmptyRecords(): Unit = {
    // Build a ShareFetchResponse with no records
    val response = ShareFetchResponse.of(
      org.apache.kafka.common.protocol.Errors.NONE,
      0,
      new java.util.LinkedHashMap(),
      java.util.List.of(),
      0
    )
    val topicNames = new java.util.HashMap[Uuid, String]()
    val json = HttpResponseSerializer.serializeShareFetchResponse(response, topicNames)
    val parsed = mapper.readTree(json)
    assertTrue(parsed.has("records"))
    assertEquals(0, parsed.get("records").size())
  }

  @Test
  def testSerializeShareAcknowledgeResponseEmpty(): Unit = {
    val responseData = new ShareAcknowledgeResponseData()
      .setErrorCode(0.toShort)
      .setThrottleTimeMs(0)
    val response = new ShareAcknowledgeResponse(responseData)
    val json = HttpResponseSerializer.serializeShareAcknowledgeResponse(response)
    val parsed = mapper.readTree(json)
    assertTrue(parsed.has("results"))
    assertEquals(0, parsed.get("results").size())
  }

  @Test
  def testSerializeShareAcknowledgeResponseWithPartitionError(): Unit = {
    val partitionData = new ShareAcknowledgeResponseData.PartitionData()
      .setPartitionIndex(0)
      .setErrorCode(org.apache.kafka.common.protocol.Errors.INVALID_RECORD_STATE.code())

    val topicResponse = new ShareAcknowledgeResponseData.ShareAcknowledgeTopicResponse()
      .setTopicId(Uuid.randomUuid())
    topicResponse.partitions().add(partitionData)

    val responseData = new ShareAcknowledgeResponseData()
      .setErrorCode(0.toShort)
      .setThrottleTimeMs(0)
    responseData.responses().add(topicResponse)

    val response = new ShareAcknowledgeResponse(responseData)
    val json = HttpResponseSerializer.serializeShareAcknowledgeResponse(response)
    val parsed = mapper.readTree(json)

    assertTrue(parsed.has("results"))
    assertEquals(1, parsed.get("results").size())
    val result = parsed.get("results").get(0)
    assertTrue(result.get("errorCode").asInt() != 0)
    assertTrue(result.has("errorMessage"))
  }

  @Test
  def testSerializeShareFetchResponseWithAcquiredRecords(): Unit = {
    val topicId = Uuid.randomUuid()
    val topicNames = new java.util.HashMap[Uuid, String]()
    topicNames.put(topicId, "orders")

    // Build partition data with acquired records
    val acquiredRecord = new ShareFetchResponseData.AcquiredRecords()
      .setFirstOffset(42L)
      .setLastOffset(42L)
      .setDeliveryCount(1.toShort)

    val partitionData = new ShareFetchResponseData.PartitionData()
      .setPartitionIndex(0)
      .setErrorCode(0.toShort)
    partitionData.acquiredRecords().add(acquiredRecord)
    partitionData.setRecords(org.apache.kafka.common.record.internal.MemoryRecords.EMPTY)

    val topicIdPartition = new org.apache.kafka.common.TopicIdPartition(
      topicId,
      new org.apache.kafka.common.TopicPartition("orders", 0)
    )
    val responseData = new java.util.LinkedHashMap[org.apache.kafka.common.TopicIdPartition, ShareFetchResponseData.PartitionData]()
    responseData.put(topicIdPartition, partitionData)

    val response = ShareFetchResponse.of(
      org.apache.kafka.common.protocol.Errors.NONE,
      0,
      responseData,
      java.util.List.of(),
      0
    )

    val json = HttpResponseSerializer.serializeShareFetchResponse(response, topicNames)
    val parsed = mapper.readTree(json)

    assertTrue(parsed.has("records"))
    assertEquals(1, parsed.get("records").size())
    val record = parsed.get("records").get(0)
    assertEquals("orders", record.get("topic").asText())
    assertEquals(0, record.get("partition").asInt())
    assertEquals(42, record.get("offset").asLong())
    assertTrue(record.has("acquireId"))
    // acquireId should contain the offset range
    assertTrue(record.get("acquireId").asText().contains("42"))
  }

  @Test
  def testSerializeErrorResponse(): Unit = {
    val json = HttpResponseSerializer.serializeError(400, "Bad Request")
    val parsed = mapper.readTree(json)
    assertTrue(parsed.get("error").asBoolean())
    assertEquals(400, parsed.get("status").asInt())
    assertEquals("Bad Request", parsed.get("message").asText())
  }
}
