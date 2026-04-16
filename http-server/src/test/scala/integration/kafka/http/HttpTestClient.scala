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

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.ObjectNode
import kafka.http.HttpTestClient._
import org.eclipse.jetty.client.{ContentResponse, HttpClient, StringRequestContent}
import org.eclipse.jetty.http.HttpMethod

import java.nio.charset.StandardCharsets
import java.util.Base64
import java.util.concurrent.TimeUnit

/**
 * Companion object holding data types for HTTP request/response test payloads.
 * Placed in the companion to avoid path-dependent type issues when used from
 * test classes.
 */
object HttpTestClient {

  sealed trait DataValue
  case class StringValue(data: String) extends DataValue
  case class BinaryValue(data: Array[Byte]) extends DataValue
  case class JsonValue(data: JsonNode) extends DataValue
  case object NullValue extends DataValue

  case class ProduceRecord(
    partition: Option[Int] = None,
    key: Option[DataValue] = None,
    value: Option[DataValue] = None,
    headers: Seq[(String, String)] = Seq.empty
  )

  case class ProduceResponse(
    status: Int,
    headers: Map[String, String],
    body: JsonNode
  )

  case class FetchPartitionSpec(partition: Int, offset: Long)

  case class ConsumeResponse(
    status: Int,
    headers: Map[String, String],
    body: JsonNode
  )

  case class HealthResponse(
    status: Int,
    body: JsonNode
  )
}

/**
 * Test utility for sending HTTP requests to a Kafka broker's HTTP listener.
 * Wraps Jetty HttpClient with convenience methods for Kafka HTTP API endpoints.
 *
 * Usage:
 * {{{
 *   import kafka.http.HttpTestClient._
 *
 *   val client = new HttpTestClient()
 *   client.start()
 *   try {
 *     val resp = client.produce("http://localhost:9094", "my-topic",
 *       Seq(ProduceRecord(partition = Some(0), value = Some(StringValue("hello")))))
 *     assertEquals(200, resp.status)
 *   } finally {
 *     client.close()
 *   }
 * }}}
 */
class HttpTestClient extends AutoCloseable {

  private val mapper = new ObjectMapper()
  private val httpClient = new HttpClient()

  def start(): Unit = {
    httpClient.setConnectTimeout(5000)
    httpClient.setIdleTimeout(30000)
    httpClient.start()
  }

  override def close(): Unit = {
    httpClient.stop()
  }

  // ------------------------------------------------------------------
  // Produce
  // ------------------------------------------------------------------

  /**
   * POST /v1/topics/{topic}/records
   */
  def produce(
    baseUrl: String,
    topic: String,
    records: Seq[ProduceRecord],
    acks: String = "all",
    timeoutMs: Int = 5000
  ): ProduceResponse = {
    val url = s"$baseUrl/v1/topics/$topic/records"
    val bodyNode = mapper.createObjectNode()

    val recordsArray = bodyNode.putArray("records")
    records.foreach { record =>
      val recordNode = recordsArray.addObject()
      record.partition.foreach(p => recordNode.put("partition", p))
      record.key.foreach(k => recordNode.set[ObjectNode]("key", serializeDataValue(k)))
      record.value.foreach(v => recordNode.set[ObjectNode]("value", serializeDataValue(v)))
      if (record.headers.nonEmpty) {
        val headersArray = recordNode.putArray("headers")
        record.headers.foreach { case (name, value) =>
          headersArray.addObject().put("name", name).put("value", value)
        }
      }
    }
    bodyNode.put("acks", acks)
    bodyNode.put("timeoutMs", timeoutMs)

    val response = sendPost(url, mapper.writeValueAsString(bodyNode))
    ProduceResponse(
      status = response.getStatus,
      headers = extractHeaders(response),
      body = parseBodySafe(response)
    )
  }

  // ------------------------------------------------------------------
  // Consume
  // ------------------------------------------------------------------

  /**
   * POST /v1/topics/{topic}/records:fetch
   */
  def consume(
    baseUrl: String,
    topic: String,
    partitions: Seq[FetchPartitionSpec],
    maxWaitMs: Int = 500,
    minBytes: Int = 1,
    maxBytes: Int = 10485760,
    maxBytesPerPartition: Int = 1048576,
    isolationLevel: String = "READ_UNCOMMITTED"
  ): ConsumeResponse = {
    val url = s"$baseUrl/v1/topics/$topic/records:fetch"
    val bodyNode = mapper.createObjectNode()

    val partsArray = bodyNode.putArray("partitions")
    partitions.foreach { spec =>
      partsArray.addObject()
        .put("partition", spec.partition)
        .put("offset", spec.offset)
    }
    bodyNode.put("maxWaitMs", maxWaitMs)
    bodyNode.put("minBytes", minBytes)
    bodyNode.put("maxBytes", maxBytes)
    bodyNode.put("maxBytesPerPartition", maxBytesPerPartition)
    bodyNode.put("isolationLevel", isolationLevel)

    val response = sendPost(url, mapper.writeValueAsString(bodyNode))
    ConsumeResponse(
      status = response.getStatus,
      headers = extractHeaders(response),
      body = parseBodySafe(response)
    )
  }

  // ------------------------------------------------------------------
  // Health
  // ------------------------------------------------------------------

  /**
   * GET /v1/health
   */
  def health(baseUrl: String): HealthResponse = {
    val url = s"$baseUrl/v1/health"
    val response = httpClient.newRequest(url)
      .method(HttpMethod.GET)
      .timeout(5, TimeUnit.SECONDS)
      .send()
    HealthResponse(
      status = response.getStatus,
      body = parseBodySafe(response)
    )
  }

  // ------------------------------------------------------------------
  // Raw request methods (for custom/edge-case tests)
  // ------------------------------------------------------------------

  /**
   * Send a raw GET request to the given URL.
   */
  def rawGet(url: String): ContentResponse = {
    httpClient.newRequest(url)
      .method(HttpMethod.GET)
      .timeout(5, TimeUnit.SECONDS)
      .send()
  }

  /**
   * Send a raw POST request with JSON body.
   */
  def rawPost(url: String, jsonBody: String): ContentResponse = {
    sendPost(url, jsonBody)
  }

  // ------------------------------------------------------------------
  // Helpers
  // ------------------------------------------------------------------

  private def sendPost(url: String, jsonBody: String): ContentResponse = {
    httpClient.newRequest(url)
      .method(HttpMethod.POST)
      .headers(h => h.put("Content-Type", "application/json"))
      .body(new StringRequestContent("application/json", jsonBody, StandardCharsets.UTF_8))
      .timeout(30, TimeUnit.SECONDS)
      .send()
  }

  private def serializeDataValue(value: DataValue): ObjectNode = {
    val node = mapper.createObjectNode()
    value match {
      case StringValue(data) =>
        node.put("type", "STRING")
        node.put("data", data)
      case BinaryValue(data) =>
        node.put("type", "BINARY")
        node.put("data", Base64.getEncoder.encodeToString(data))
      case JsonValue(data) =>
        node.put("type", "JSON")
        node.set[ObjectNode]("data", data)
      case NullValue =>
        node.put("type", "NULL")
    }
    node
  }

  private def extractHeaders(response: ContentResponse): Map[String, String] = {
    import scala.jdk.CollectionConverters._
    response.getHeaders.asScala
      .map(field => field.getName -> field.getValue)
      .toMap
  }

  /**
   * Parse the response body as JSON. Returns an empty ObjectNode if the
   * body is empty or not valid JSON (e.g. connection refused error page).
   */
  private def parseBodySafe(response: ContentResponse): JsonNode = {
    val content = response.getContentAsString
    if (content == null || content.trim.isEmpty) {
      mapper.createObjectNode()
    } else {
      try {
        mapper.readTree(content)
      } catch {
        case _: Exception => mapper.createObjectNode().put("raw", content)
      }
    }
  }
}
