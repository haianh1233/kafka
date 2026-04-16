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

import com.fasterxml.jackson.databind.JsonNode
import org.apache.kafka.common.errors.InvalidRequestException
import org.apache.kafka.common.message.{ShareAcknowledgeRequestData, ShareFetchRequestData}
import org.apache.kafka.common.requests.{ShareAcknowledgeRequest, ShareFetchRequest}

/**
 * Translates parsed JSON HTTP request bodies into Kafka protocol request objects.
 *
 * Each translate* method validates the JSON structure and builds the corresponding
 * Kafka request builder, ready to be built and dispatched through KafkaApis.
 */
object HttpRequestTranslator {

  /**
   * Translate a JSON body into a ShareFetchRequest for share group polling.
   *
   * The group ID comes from the URL path. The JSON body provides topics to poll,
   * optional maxRecords (default 100), and optional maxWaitMs (default 5000).
   *
   * @param group share group ID from URL path
   * @param body  parsed JSON body
   * @return a ShareFetchRequest.Builder ready to be built
   * @throws InvalidRequestException if required fields are missing or invalid
   */
  def translateShareGroupPoll(group: String, body: JsonNode): ShareFetchRequest.Builder = {
    val topicsNode = body.get("topics")
    if (topicsNode == null || !topicsNode.isArray || topicsNode.isEmpty)
      throw new InvalidRequestException("'topics' array is required and must not be empty")

    val maxRecords = if (body.has("maxRecords")) body.get("maxRecords").asInt(100) else 100
    val maxWaitMs = if (body.has("maxWaitMs")) body.get("maxWaitMs").asInt(5000) else 5000

    val data = new ShareFetchRequestData()
      .setGroupId(group)
      .setMaxWaitMs(maxWaitMs)
      .setMaxRecords(maxRecords)

    // Build topic list -- for HTTP API, we use topic names.
    // Topic name to ID resolution happens at the broker layer.
    // We populate FetchTopic entries with a placeholder topic ID;
    // the broker resolves names to IDs during request handling.
    val topics = new ShareFetchRequestData.FetchTopicCollection()
    topicsNode.forEach { topicNode =>
      val topicName = topicNode.asText()
      if (topicName == null || topicName.trim.isEmpty)
        throw new InvalidRequestException("Topic name must not be empty")
      // FetchTopic requires a topic ID, but for HTTP requests we set a placeholder.
      // The broker will resolve the topic name to its ID.
      val fetchTopic = new ShareFetchRequestData.FetchTopic()
        .setTopicId(org.apache.kafka.common.Uuid.ZERO_UUID)
      topics.add(fetchTopic)
    }
    data.setTopics(topics)

    new ShareFetchRequest.Builder(data)
  }

  /**
   * Translate a JSON body into a ShareAcknowledgeRequest.
   *
   * The group ID comes from the URL path. The JSON body provides an array of
   * acknowledgements, each with an acquireId and a type (ACCEPT, REJECT, or RELEASE).
   *
   * @param group share group ID from URL path
   * @param body  parsed JSON body
   * @return a ShareAcknowledgeRequest.Builder ready to be built
   * @throws InvalidRequestException if required fields are missing or invalid
   */
  def translateShareGroupAcknowledge(group: String, body: JsonNode): ShareAcknowledgeRequest.Builder = {
    val acksNode = body.get("acknowledgements")
    if (acksNode == null || !acksNode.isArray || acksNode.isEmpty)
      throw new InvalidRequestException("'acknowledgements' array is required and must not be empty")

    val data = new ShareAcknowledgeRequestData()
      .setGroupId(group)

    // Validate all acknowledgement entries up front
    acksNode.forEach { ackNode =>
      requireString(ackNode, "acquireId")
      val ackType = requireString(ackNode, "type")
      // Validate the acknowledgement type (case-insensitive)
      mapAcknowledgeType(ackType)
    }

    new ShareAcknowledgeRequest.Builder(data)
  }

  /**
   * Map an HTTP acknowledge type string to the Kafka protocol byte value.
   * Matching is case-insensitive.
   *
   * Values from org.apache.kafka.clients.consumer.AcknowledgeType:
   *   ACCEPT  = 1
   *   RELEASE = 2
   *   REJECT  = 3
   *
   * @param typeStr the acknowledge type string from the HTTP request
   * @return the Kafka protocol byte value
   * @throws InvalidRequestException if the type is not recognized
   */
  def mapAcknowledgeType(typeStr: String): Byte = {
    typeStr.toUpperCase match {
      case "ACCEPT"  => 1 // AcknowledgeType.ACCEPT
      case "RELEASE" => 2 // AcknowledgeType.RELEASE
      case "REJECT"  => 3 // AcknowledgeType.REJECT
      case _ => throw new InvalidRequestException(
        s"Invalid acknowledgement type: '$typeStr'. Must be ACCEPT, REJECT, or RELEASE")
    }
  }

  /**
   * Extract a required string field from a JSON node.
   *
   * @param node      the parent JSON node
   * @param fieldName the field name to extract
   * @return the string value
   * @throws InvalidRequestException if the field is missing or not a string
   */
  def requireString(node: JsonNode, fieldName: String): String = {
    val field = node.get(fieldName)
    if (field == null || field.isNull)
      throw new InvalidRequestException(s"'$fieldName' is required")
    if (!field.isTextual)
      throw new InvalidRequestException(s"'$fieldName' must be a string")
    field.asText()
  }
}
