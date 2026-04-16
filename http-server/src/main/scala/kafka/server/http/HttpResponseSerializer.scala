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
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{ShareAcknowledgeResponse, ShareFetchResponse}

/**
 * Serializes Kafka protocol responses into JSON for HTTP clients.
 *
 * Response JSON shapes conform to the HTTP protocol design document (section 4.5).
 * The serializer is stateless -- all methods are pure functions on the response data.
 */
object HttpResponseSerializer {

  private val MAPPER = new ObjectMapper()

  /**
   * Serialize a ShareFetchResponse to the JSON poll response shape.
   *
   * Each record includes topic, partition, offset, and acquireId fields.
   * The acquireId is extracted from the AcquiredRecords metadata in the response.
   *
   * Output format:
   * {{{
   * {
   *   "records": [
   *     {
   *       "topic": "orders",
   *       "partition": 0,
   *       "offset": 42,
   *       "acquireId": "topic:partition:firstOffset-lastOffset"
   *     }
   *   ]
   * }
   * }}}
   *
   * @param response the ShareFetchResponse from the broker
   * @param topicNames mapping from topic UUID to topic name for resolution
   * @return the JSON string representing the response
   */
  def serializeShareFetchResponse(
    response: ShareFetchResponse,
    topicNames: java.util.Map[org.apache.kafka.common.Uuid, String]
  ): String = {
    val root = MAPPER.createObjectNode()
    val recordsArray = root.putArray("records")

    // Check for top-level error
    if (response.data().errorCode() != 0) {
      val error = Errors.forCode(response.data().errorCode())
      root.put("errorCode", response.data().errorCode().toInt)
      root.put("errorMessage", error.message())
      return MAPPER.writeValueAsString(root)
    }

    response.data().responses().forEach { topicResponse =>
      val topicName = topicNames.get(topicResponse.topicId())
      val resolvedName = if (topicName != null) topicName else topicResponse.topicId().toString

      topicResponse.partitions().forEach { partitionData =>
        val partitionIndex = partitionData.partitionIndex()

        // Each AcquiredRecords entry represents a batch of acquired records.
        // We create one JSON record entry per acquired batch, using the
        // offset range as the acquireId token.
        partitionData.acquiredRecords().forEach { acquired =>
          val recordNode = recordsArray.addObject()
          recordNode.put("topic", resolvedName)
          recordNode.put("partition", partitionIndex)
          recordNode.put("offset", acquired.firstOffset())
          // The acquireId is an opaque token for the HTTP client.
          // We encode it as topicId:partition:firstOffset-lastOffset so
          // the acknowledge endpoint can map it back.
          val acquireId = s"${topicResponse.topicId()}:$partitionIndex:${acquired.firstOffset()}-${acquired.lastOffset()}"
          recordNode.put("acquireId", acquireId)
        }

        // If there are partition-level errors, report them
        if (partitionData.errorCode() != 0) {
          val errorNode = recordsArray.addObject()
          errorNode.put("topic", resolvedName)
          errorNode.put("partition", partitionIndex)
          errorNode.put("errorCode", partitionData.errorCode().toInt)
          val error = Errors.forCode(partitionData.errorCode())
          errorNode.put("errorMessage", error.message())
        }
      }
    }

    MAPPER.writeValueAsString(root)
  }

  /**
   * Serialize a ShareAcknowledgeResponse to the JSON acknowledge response shape.
   *
   * Output format:
   * {{{
   * {
   *   "results": [
   *     { "errorCode": 0 },
   *     { "errorCode": 27, "errorMessage": "..." }
   *   ]
   * }
   * }}}
   *
   * @param response the ShareAcknowledgeResponse from the broker
   * @return the JSON string representing the response
   */
  def serializeShareAcknowledgeResponse(response: ShareAcknowledgeResponse): String = {
    val root = MAPPER.createObjectNode()
    val resultsArray = root.putArray("results")

    // Check for top-level error
    if (response.data().errorCode() != 0) {
      val error = Errors.forCode(response.data().errorCode())
      root.put("errorCode", response.data().errorCode().toInt)
      root.put("errorMessage", error.message())
      return MAPPER.writeValueAsString(root)
    }

    response.data().responses().forEach { topicResponse =>
      topicResponse.partitions().forEach { partitionData =>
        val resultNode = resultsArray.addObject()
        resultNode.put("errorCode", partitionData.errorCode().toInt)
        if (partitionData.errorCode() != 0) {
          val error = Errors.forCode(partitionData.errorCode())
          resultNode.put("errorMessage", error.message())
        }
      }
    }

    MAPPER.writeValueAsString(root)
  }

  /**
   * Build a JSON error response.
   *
   * @param statusCode HTTP status code
   * @param message    error message
   * @return the JSON string representing the error
   */
  def serializeError(statusCode: Int, message: String): String = {
    val root = MAPPER.createObjectNode()
    root.put("error", true)
    root.put("status", statusCode)
    root.put("message", message)
    MAPPER.writeValueAsString(root)
  }
}
