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
package kafka.server.http;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.ListOffsetsResponseData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.message.OffsetFetchResponseData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.message.ShareAcknowledgeResponseData;
import org.apache.kafka.common.message.ShareFetchResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.internal.BaseRecords;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.record.internal.Records;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.ListOffsetsResponse;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.OffsetCommitResponse;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.ShareAcknowledgeResponse;
import org.apache.kafka.common.requests.ShareFetchResponse;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Serializes Kafka {@link AbstractResponse} objects into Netty {@link FullHttpResponse}
 * with JSON body.
 *
 * Responsibilities:
 * - Kafka error code -> HTTP status mapping (section 11.1)
 * - Value type detection for consumed records (section 14.7)
 * - Partial failure -> 207 Multi-Status (section 11.2)
 * - Response headers: X-Kafka-Request-ID, X-Kafka-MaxWait-Applied, Retry-After
 *
 * // Time: Created - TASK-B.02
 */
public final class HttpResponseSerializer {

    // --- Jackson ObjectMapper ---
    private static final ObjectMapper MAPPER = new ObjectMapper();

    // --- HTTP header names ---
    static final String HEADER_REQUEST_ID = "X-Kafka-Request-ID";
    static final String HEADER_MAX_WAIT_APPLIED = "X-Kafka-MaxWait-Applied";
    static final String HEADER_RETRY_AFTER = "Retry-After";

    // --- Content-type record header name (underscore prefix to avoid HTTP header collision) ---
    static final String RECORD_HEADER_CONTENT_TYPE = "_content-type";
    static final String CONTENT_TYPE_JSON = "application/json";

    // --- DataObject type constants ---
    static final String TYPE_NULL = "NULL";
    static final String TYPE_JSON = "JSON";
    static final String TYPE_STRING = "STRING";
    static final String TYPE_BINARY = "BINARY";

    // --- HTTP 207 Multi-Status -- delegates to HttpErrorMapper ---
    static final HttpResponseStatus MULTI_STATUS = HttpErrorMapper.MULTI_STATUS;

    private HttpResponseSerializer() {} // utility class

    /**
     * Serializes a Kafka AbstractResponse into a Netty FullHttpResponse,
     * with an optional topic ID -> topic name mapping for share group responses.
     *
     * @param kafkaResponse     the Kafka protocol response
     * @param requestId         X-Kafka-Request-ID to echo back in response header
     * @param apiKey            the ApiKey of the original request
     * @param effectiveMaxWaitMs applied maxWaitMs cap for FETCH responses (-1 if not applicable)
     * @param topicIdNames      mapping from topic ID string to topic name (for share group responses)
     * @return FullHttpResponse ready to write to Netty channel
     */
    public static FullHttpResponse serialize(
            AbstractResponse kafkaResponse,
            String requestId,
            ApiKeys apiKey,
            int effectiveMaxWaitMs,
            Map<String, String> topicIdNames) {
        // Store the mapping in a thread-local so the share serializers can access it
        TOPIC_ID_NAMES.set(topicIdNames);
        try {
            return serialize(kafkaResponse, requestId, apiKey, effectiveMaxWaitMs);
        } finally {
            TOPIC_ID_NAMES.remove();
        }
    }

    // Thread-local for passing topic ID -> name mapping to share group serializers
    private static final ThreadLocal<Map<String, String>> TOPIC_ID_NAMES = new ThreadLocal<>();

    /**
     * Serializes a Kafka AbstractResponse into a Netty FullHttpResponse.
     *
     * @param kafkaResponse     the Kafka protocol response (ProduceResponse, FetchResponse, etc.)
     * @param requestId         X-Kafka-Request-ID to echo back in response header
     * @param apiKey            the ApiKey of the original request (determines JSON shape)
     * @param effectiveMaxWaitMs applied maxWaitMs cap for FETCH responses (-1 if not applicable)
     * @return FullHttpResponse ready to write to Netty channel
     */
    public static FullHttpResponse serialize(
            AbstractResponse kafkaResponse,
            String requestId,
            ApiKeys apiKey,
            int effectiveMaxWaitMs) {
        Objects.requireNonNull(kafkaResponse, "kafkaResponse");
        Objects.requireNonNull(apiKey, "apiKey");

        List<Errors> partitionErrors = new ArrayList<>();

        // Metadata has a special early-return path for topic-level errors
        if (apiKey == ApiKeys.METADATA) {
            MetadataResponse metadataResponse = (MetadataResponse) kafkaResponse;
            Errors metadataTopicError = collectMetadataTopicError(metadataResponse);
            if (metadataTopicError != Errors.NONE) {
                String errorBody = buildErrorBody(metadataTopicError, null);
                return buildResponse(mapErrorToHttpStatus(metadataTopicError), errorBody,
                        requestId, -1);
            }
        }

        ObjectNode body = serializeResponseBody(kafkaResponse, apiKey, partitionErrors);

        String jsonBody;
        try {
            jsonBody = MAPPER.writeValueAsString(body);
        } catch (JsonProcessingException e) {
            // Should not happen for ObjectNode, but handle gracefully
            jsonBody = buildErrorBody(Errors.UNKNOWN_SERVER_ERROR, "Failed to serialize response: " + e.getMessage());
            return buildResponse(HttpResponseStatus.INTERNAL_SERVER_ERROR, jsonBody,
                    requestId, effectiveMaxWaitMs);
        }

        HttpResponseStatus status;
        if (!partitionErrors.isEmpty()) {
            status = determineOverallStatus(partitionErrors);
        } else {
            status = HttpResponseStatus.OK;
        }

        FullHttpResponse response = buildResponse(status, jsonBody, requestId,
                apiKey == ApiKeys.FETCH ? effectiveMaxWaitMs : -1);

        // Add Retry-After header if applicable
        if (!partitionErrors.isEmpty()) {
            // Find a representative retriable error for Retry-After header
            for (Errors err : partitionErrors) {
                int retryAfter = retryAfterSeconds(err);
                if (retryAfter > 0) {
                    response.headers().set(HEADER_RETRY_AFTER, String.valueOf(retryAfter));
                    break;
                }
            }
        }

        return response;
    }

    // --- Response body dispatch ---

    /**
     * Dispatches to the appropriate serializer based on apiKey and collects errors.
     */
    private static ObjectNode serializeResponseBody(
            AbstractResponse kafkaResponse, ApiKeys apiKey, List<Errors> partitionErrors) {
        switch (apiKey) {
            case PRODUCE:
                collectProduceErrors((ProduceResponse) kafkaResponse, partitionErrors);
                return serializeProduceResponse((ProduceResponse) kafkaResponse);
            case FETCH:
                collectFetchErrors((FetchResponse) kafkaResponse, partitionErrors);
                return serializeFetchResponse((FetchResponse) kafkaResponse);
            case METADATA:
                return serializeMetadataResponse((MetadataResponse) kafkaResponse);
            case LIST_OFFSETS:
                collectListOffsetsErrors((ListOffsetsResponse) kafkaResponse, partitionErrors);
                return serializeListOffsetsResponse((ListOffsetsResponse) kafkaResponse);
            case OFFSET_COMMIT:
                collectOffsetCommitErrors((OffsetCommitResponse) kafkaResponse, partitionErrors);
                return serializeOffsetCommitResponse((OffsetCommitResponse) kafkaResponse);
            case OFFSET_FETCH:
                OffsetFetchResponse ofr = (OffsetFetchResponse) kafkaResponse;
                String groupId = ofr.data().groups().isEmpty() ? "" :
                    ofr.data().groups().get(0).groupId();
                return serializeOffsetFetchResponse(ofr, groupId);
            case SHARE_FETCH:
                collectShareFetchErrors((ShareFetchResponse) kafkaResponse, partitionErrors);
                return serializeShareFetchResponse((ShareFetchResponse) kafkaResponse);
            case SHARE_ACKNOWLEDGE:
                collectShareAcknowledgeErrors((ShareAcknowledgeResponse) kafkaResponse, partitionErrors);
                return serializeShareAcknowledgeResponse((ShareAcknowledgeResponse) kafkaResponse);
            default:
                return serializeGenericResponse(kafkaResponse);
        }
    }

    // --- Produce response errors ---

    private static void collectProduceErrors(ProduceResponse response, List<Errors> errors) {
        for (ProduceResponseData.TopicProduceResponse topicResponse : response.data().responses()) {
            for (ProduceResponseData.PartitionProduceResponse partitionResponse : topicResponse.partitionResponses()) {
                errors.add(Errors.forCode(partitionResponse.errorCode()));
            }
        }
    }

    // --- Fetch response errors ---

    private static void collectFetchErrors(FetchResponse response, List<Errors> errors) {
        for (FetchResponseData.FetchableTopicResponse topicResponse : response.data().responses()) {
            for (FetchResponseData.PartitionData partitionData : topicResponse.partitions()) {
                errors.add(Errors.forCode(partitionData.errorCode()));
            }
        }
    }

    // --- OffsetCommit response errors ---

    private static void collectOffsetCommitErrors(OffsetCommitResponse response, List<Errors> errors) {
        for (OffsetCommitResponseData.OffsetCommitResponseTopic topicResult : response.data().topics()) {
            for (OffsetCommitResponseData.OffsetCommitResponsePartition partitionResult : topicResult.partitions()) {
                errors.add(Errors.forCode(partitionResult.errorCode()));
            }
        }
    }

    // --- Metadata response errors ---

    /**
     * Checks for a topic-level error in a single-topic MetadataResponse.
     * Returns the error if found, or Errors.NONE if no error.
     * For multi-topic responses, returns NONE (errors are per-topic in the list).
     */
    private static Errors collectMetadataTopicError(MetadataResponse response) {
        MetadataResponseData data = response.data();
        if (data.topics().size() == 1) {
            MetadataResponseData.MetadataResponseTopic topic = data.topics().iterator().next();
            if (topic.errorCode() != 0) {
                return Errors.forCode(topic.errorCode());
            }
        }
        return Errors.NONE;
    }

    // --- ListOffsets response errors ---

    private static void collectListOffsetsErrors(ListOffsetsResponse response, List<Errors> errors) {
        for (ListOffsetsResponseData.ListOffsetsTopicResponse topicResponse : response.data().topics()) {
            for (ListOffsetsResponseData.ListOffsetsPartitionResponse partitionResponse : topicResponse.partitions()) {
                errors.add(Errors.forCode(partitionResponse.errorCode()));
            }
        }
    }

    /**
     * Serializes a ProduceResponse to JSON.
     *
     * Output shape:
     * {
     *   "offsets": [
     *     { "partition": 0, "offset": 10042, "errorCode": 0, "errorMessage": null }
     *   ]
     * }
     */
    static ObjectNode serializeProduceResponse(ProduceResponse response) {
        ObjectNode root = MAPPER.createObjectNode();
        ArrayNode offsets = root.putArray("offsets");

        for (ProduceResponseData.TopicProduceResponse topicResponse : response.data().responses()) {
            for (ProduceResponseData.PartitionProduceResponse partitionResponse : topicResponse.partitionResponses()) {
                ObjectNode entry = offsets.addObject();
                entry.put("partition", partitionResponse.index());
                entry.put("offset", partitionResponse.baseOffset());
                entry.put("errorCode", partitionResponse.errorCode());
                Errors error = Errors.forCode(partitionResponse.errorCode());
                if (error == Errors.NONE) {
                    entry.putNull("errorMessage");
                } else {
                    entry.put("errorMessage", error.name());
                }
            }
        }

        return root;
    }

    /**
     * Serializes a FetchResponse to JSON with value type detection.
     *
     * Output shape:
     * {
     *   "partitions": [
     *     {
     *       "partition": 0,
     *       "highWatermark": 10050,
     *       "records": [...],
     *       "errorCode": 0, "errorMessage": null
     *     }
     *   ]
     * }
     */
    static ObjectNode serializeFetchResponse(FetchResponse response) {
        ObjectNode root = MAPPER.createObjectNode();
        ArrayNode partitionsArray = root.putArray("partitions");

        for (FetchResponseData.FetchableTopicResponse topicResponse : response.data().responses()) {
            for (FetchResponseData.PartitionData partitionData : topicResponse.partitions()) {
                ObjectNode partitionNode = partitionsArray.addObject();
                partitionNode.put("partition", partitionData.partitionIndex());
                partitionNode.put("highWatermark", partitionData.highWatermark());

                ArrayNode recordsArray = partitionNode.putArray("records");
                BaseRecords baseRecords = partitionData.records();
                // Handle both MemoryRecords and FileRecords (and any other Records impl)
                // via the common Records interface which provides batches().
                if (baseRecords instanceof Records) {
                    Records records = (Records) baseRecords;
                    for (RecordBatch batch : records.batches()) {
                        for (Record record : batch) {
                            ObjectNode recordNode = recordsArray.addObject();
                            recordNode.put("offset", record.offset());
                            recordNode.put("timestamp", record.timestamp());

                            // Serialize key
                            byte[] keyBytes = bufferToBytes(record.key());
                            recordNode.set("key", detectAndSerializeValue(keyBytes, record.headers()));

                            // Serialize value
                            byte[] valueBytes = bufferToBytes(record.value());
                            recordNode.set("value", detectAndSerializeValue(valueBytes, record.headers()));

                            // Serialize record headers
                            ArrayNode headersArray = recordNode.putArray("headers");
                            for (Header header : record.headers()) {
                                ObjectNode headerNode = headersArray.addObject();
                                headerNode.put("name", header.key());
                                if (header.value() != null) {
                                    headerNode.put("value", new String(header.value(), StandardCharsets.UTF_8));
                                } else {
                                    headerNode.putNull("value");
                                }
                            }
                        }
                    }
                }

                partitionNode.put("errorCode", partitionData.errorCode());
                Errors error = Errors.forCode(partitionData.errorCode());
                if (error == Errors.NONE) {
                    partitionNode.putNull("errorMessage");
                } else {
                    partitionNode.put("errorMessage", error.name());
                }
            }
        }

        return root;
    }

    /**
     * Converts a ByteBuffer to byte array, or null if the buffer is null.
     */
    private static byte[] bufferToBytes(ByteBuffer buffer) {
        if (buffer == null) {
            return null;
        }
        byte[] bytes = new byte[buffer.remaining()];
        buffer.duplicate().get(bytes);
        return bytes;
    }

    /**
     * Serializes a MetadataResponse to JSON.
     *
     * Output shape for single topic:
     * {
     *   "topic": "orders",
     *   "partitions": [...]
     * }
     *
     * Output shape for all topics:
     * {
     *   "topics": ["orders", "payments", "inventory"]
     * }
     */
    static ObjectNode serializeMetadataResponse(MetadataResponse response) {
        ObjectNode root = MAPPER.createObjectNode();
        MetadataResponseData data = response.data();

        if (data.topics().size() == 1) {
            // Single topic — detailed partition info
            MetadataResponseData.MetadataResponseTopic topic = data.topics().iterator().next();
            root.put("topic", topic.name());

            ArrayNode partitionsArray = root.putArray("partitions");
            for (MetadataResponseData.MetadataResponsePartition partition : topic.partitions()) {
                ObjectNode partitionNode = partitionsArray.addObject();
                partitionNode.put("partition", partition.partitionIndex());

                // Find leader broker info
                ObjectNode leaderNode = partitionNode.putObject("leader");
                int leaderId = partition.leaderId();
                leaderNode.put("brokerId", leaderId);
                // Look up broker host/port from brokers collection
                for (MetadataResponseData.MetadataResponseBroker broker : data.brokers()) {
                    if (broker.nodeId() == leaderId) {
                        leaderNode.put("host", broker.host());
                        leaderNode.put("port", broker.port());
                        break;
                    }
                }

                ArrayNode replicasArray = partitionNode.putArray("replicas");
                for (int replicaId : partition.replicaNodes()) {
                    replicasArray.add(replicaId);
                }

                ArrayNode isrArray = partitionNode.putArray("isr");
                for (int isrId : partition.isrNodes()) {
                    isrArray.add(isrId);
                }
            }
        } else {
            // Multiple topics — just list the names
            ArrayNode topicsArray = root.putArray("topics");
            for (MetadataResponseData.MetadataResponseTopic topic : data.topics()) {
                topicsArray.add(topic.name());
            }
        }

        return root;
    }

    /**
     * Serializes a ListOffsetsResponse to JSON.
     *
     * Output shape:
     * { "partition": 0, "offset": 10042, "timestamp": 1713260400000 }
     */
    static ObjectNode serializeListOffsetsResponse(ListOffsetsResponse response) {
        ObjectNode root = MAPPER.createObjectNode();

        for (ListOffsetsResponseData.ListOffsetsTopicResponse topicResponse : response.data().topics()) {
            for (ListOffsetsResponseData.ListOffsetsPartitionResponse partitionResponse : topicResponse.partitions()) {
                root.put("partition", partitionResponse.partitionIndex());
                root.put("offset", partitionResponse.offset());
                root.put("timestamp", partitionResponse.timestamp());
                // Return first partition data (single-partition expected for HTTP use case)
                return root;
            }
        }

        return root;
    }

    /**
     * Serializes an OffsetCommitResponse to JSON.
     * Returns per-partition commit results with error codes.
     *
     * Output shape:
     * {
     *   "offsets": [
     *     { "topic": "orders", "partition": 0, "errorCode": 0 },
     *     { "topic": "orders", "partition": 1, "errorCode": 0 }
     *   ]
     * }
     */
    static ObjectNode serializeOffsetCommitResponse(OffsetCommitResponse response) {
        ObjectNode root = MAPPER.createObjectNode();
        ArrayNode offsetsArray = root.putArray("offsets");

        for (OffsetCommitResponseData.OffsetCommitResponseTopic topic : response.data().topics()) {
            for (OffsetCommitResponseData.OffsetCommitResponsePartition partition : topic.partitions()) {
                ObjectNode node = offsetsArray.addObject();
                node.put("topic", topic.name());
                node.put("partition", partition.partitionIndex());
                node.put("errorCode", partition.errorCode());
                if (partition.errorCode() != 0) {
                    Errors error = Errors.forCode(partition.errorCode());
                    node.put("errorMessage", error.message());
                }
            }
        }

        return root;
    }

    /**
     * Serializes an OffsetFetchResponse to JSON.
     * Returns committed offsets for the group, grouped by topic.
     *
     * If a group-level error is present, returns an error object instead.
     *
     * Output shape (success):
     * {
     *   "group": "checkout-consumer",
     *   "topics": [
     *     {
     *       "topic": "orders",
     *       "partitions": [
     *         { "partition": 0, "offset": 150, "metadata": "" }
     *       ]
     *     }
     *   ]
     * }
     *
     * Output shape (group-level error):
     * {
     *   "group": "checkout-consumer",
     *   "errorCode": 30,
     *   "errorMessage": "GROUP_AUTHORIZATION_FAILED"
     * }
     */
    static ObjectNode serializeOffsetFetchResponse(OffsetFetchResponse response, String group) {
        ObjectNode root = MAPPER.createObjectNode();
        root.put("group", group);

        for (OffsetFetchResponseData.OffsetFetchResponseGroup groupData : response.data().groups()) {
            if (groupData.groupId().equals(group)) {
                // Check group-level error
                if (groupData.errorCode() != 0) {
                    Errors error = Errors.forCode(groupData.errorCode());
                    root.put("errorCode", groupData.errorCode());
                    root.put("errorMessage", error.name());
                    return root;
                }

                ArrayNode topicsArray = root.putArray("topics");
                for (OffsetFetchResponseData.OffsetFetchResponseTopics topic : groupData.topics()) {
                    ObjectNode topicNode = topicsArray.addObject();
                    topicNode.put("topic", topic.name());
                    ArrayNode partitionsArray = topicNode.putArray("partitions");
                    for (OffsetFetchResponseData.OffsetFetchResponsePartitions partition : topic.partitions()) {
                        ObjectNode partNode = partitionsArray.addObject();
                        partNode.put("partition", partition.partitionIndex());
                        partNode.put("offset", partition.committedOffset());
                        if (partition.metadata() != null) {
                            partNode.put("metadata", partition.metadata());
                        }
                    }
                }
                return root;
            }
        }

        // Group not found in response -- return empty topics
        root.putArray("topics");
        return root;
    }

    // --- ShareFetch response errors ---

    private static void collectShareFetchErrors(ShareFetchResponse response, List<Errors> errors) {
        // Top-level error
        if (response.error() != Errors.NONE) {
            errors.add(response.error());
        }
        for (ShareFetchResponseData.ShareFetchableTopicResponse topicResponse : response.data().responses()) {
            for (ShareFetchResponseData.PartitionData partitionData : topicResponse.partitions()) {
                errors.add(Errors.forCode(partitionData.errorCode()));
            }
        }
    }

    // --- ShareAcknowledge response errors ---

    private static void collectShareAcknowledgeErrors(ShareAcknowledgeResponse response, List<Errors> errors) {
        // Top-level error
        if (response.error() != Errors.NONE) {
            errors.add(response.error());
        }
        for (ShareAcknowledgeResponseData.ShareAcknowledgeTopicResponse topicResponse : response.data().responses()) {
            for (ShareAcknowledgeResponseData.PartitionData partitionData : topicResponse.partitions()) {
                errors.add(Errors.forCode(partitionData.errorCode()));
            }
        }
    }

    /**
     * Serializes a ShareFetchResponse to JSON.
     *
     * Output shape:
     * {
     *   "records": [
     *     { "topic": "t", "partition": 0, "offset": 42, "key": null, "value": {...}, "acquireId": "t:0:42" }
     *   ]
     * }
     *
     * The acquireId is synthesized from topic:partition:offset since the HTTP
     * client is stateless and doesn't maintain share sessions.
     */
    static ObjectNode serializeShareFetchResponse(ShareFetchResponse response) {
        ObjectNode root = MAPPER.createObjectNode();

        // Include top-level error if present
        if (response.error() != Errors.NONE) {
            root.put("errorCode", response.error().code());
            root.put("errorMessage", response.error().name());
        }

        ArrayNode recordsArray = root.putArray("records");

        for (ShareFetchResponseData.ShareFetchableTopicResponse topicResponse : response.data().responses()) {
            String topicName = resolveTopicName(topicResponse);
            for (ShareFetchResponseData.PartitionData partitionData : topicResponse.partitions()) {
                if (partitionData.errorCode() != 0) {
                    continue;
                }

                BaseRecords baseRecords = partitionData.records();
                if (baseRecords instanceof Records) {
                    Records records = (Records) baseRecords;
                    for (RecordBatch batch : records.batches()) {
                        for (Record record : batch) {
                            ObjectNode recordNode = recordsArray.addObject();
                            recordNode.put("topic", topicName);
                            recordNode.put("partition", partitionData.partitionIndex());
                            recordNode.put("offset", record.offset());

                            // Serialize key
                            byte[] keyBytes = bufferToBytes(record.key());
                            if (keyBytes != null) {
                                recordNode.put("key", new String(keyBytes, StandardCharsets.UTF_8));
                            } else {
                                recordNode.putNull("key");
                            }

                            // Serialize value
                            byte[] valueBytes = bufferToBytes(record.value());
                            if (valueBytes != null) {
                                recordNode.set("value", detectAndSerializeValue(valueBytes, record.headers()));
                            } else {
                                recordNode.putNull("value");
                            }

                            // Synthesize acquireId from topic:partition:offset
                            String acquireId = topicName + ":" + partitionData.partitionIndex() + ":" + record.offset();
                            recordNode.put("acquireId", acquireId);
                        }
                    }
                }
            }
        }

        return root;
    }

    /**
     * Resolves the topic name from a ShareFetchableTopicResponse.
     * The response carries topic IDs, not names. Uses the thread-local mapping
     * set by the serialize() overload that accepts topicIdNames.
     *
     * Falls back to the topic ID string if the name cannot be resolved.
     */
    private static String resolveTopicName(ShareFetchResponseData.ShareFetchableTopicResponse topicResponse) {
        Map<String, String> names = TOPIC_ID_NAMES.get();
        if (names != null) {
            String name = names.get(topicResponse.topicId().toString());
            if (name != null) {
                return name;
            }
        }
        return topicResponse.topicId().toString();
    }

    /**
     * Serializes a ShareAcknowledgeResponse to JSON.
     *
     * Output shape:
     * {
     *   "partitions": [
     *     { "topicId": "uuid", "partition": 0, "errorCode": 0 }
     *   ]
     * }
     *
     * Or on top-level error:
     * {
     *   "errorCode": 42,
     *   "errorMessage": "ERROR_NAME"
     * }
     */
    static ObjectNode serializeShareAcknowledgeResponse(ShareAcknowledgeResponse response) {
        ObjectNode root = MAPPER.createObjectNode();

        // Check top-level error
        if (response.error() != Errors.NONE) {
            root.put("errorCode", response.error().code());
            root.put("errorMessage", response.error().name());
            return root;
        }

        ArrayNode partitionsArray = root.putArray("partitions");
        for (ShareAcknowledgeResponseData.ShareAcknowledgeTopicResponse topicResponse : response.data().responses()) {
            for (ShareAcknowledgeResponseData.PartitionData partitionData : topicResponse.partitions()) {
                ObjectNode partNode = partitionsArray.addObject();
                partNode.put("topicId", topicResponse.topicId().toString());
                partNode.put("partition", partitionData.partitionIndex());
                partNode.put("errorCode", partitionData.errorCode());
                if (partitionData.errorCode() != 0) {
                    Errors error = Errors.forCode(partitionData.errorCode());
                    partNode.put("errorMessage", error.name());
                }
            }
        }

        return root;
    }

    /**
     * Serializes an unrecognized response type to a generic JSON envelope.
     */
    private static ObjectNode serializeGenericResponse(AbstractResponse response) {
        ObjectNode root = MAPPER.createObjectNode();
        root.put("apiKey", response.apiKey().name);
        root.put("throttleTimeMs", response.throttleTimeMs());
        return root;
    }

    // --- Error mapping ---

    /**
     * Maps a Kafka Errors enum value to an HTTP status code.
     * Delegates to {@link HttpErrorMapper#httpStatus(Errors)} as the single source of truth.
     *
     * @param error the Kafka error
     * @return corresponding HttpResponseStatus
     */
    static HttpResponseStatus mapErrorToHttpStatus(Errors error) {
        return HttpErrorMapper.httpStatus(error);
    }

    /**
     * Returns the Retry-After header value in seconds for retriable errors.
     * Returns -1 for non-retriable errors (no Retry-After header needed).
     * Delegates to {@link HttpErrorMapper#retryAfterSeconds(Errors)} as the single source of truth.
     */
    static int retryAfterSeconds(Errors error) {
        return HttpErrorMapper.retryAfterSeconds(error).orElse(-1);
    }

    /**
     * Overload for THROTTLING_QUOTA_EXCEEDED that takes the throttle time.
     * Delegates to {@link HttpErrorMapper#throttleRetryAfter(long)}.
     */
    static int retryAfterSeconds(Errors error, int throttleTimeMs) {
        if (error == Errors.THROTTLING_QUOTA_EXCEEDED) {
            return Math.max(1, HttpErrorMapper.throttleRetryAfter(throttleTimeMs));
        }
        return retryAfterSeconds(error);
    }

    // --- Value type detection ---

    /**
     * Detects the value type of a record's byte[] payload and serializes as DataObject.
     *
     * Algorithm (section 14.7):
     * 1. null -> {"type":"NULL"}
     * 2. _content-type: application/json header -> {"type":"JSON","data":<parsed>}
     * 3. Valid UTF-8 without control chars -> {"type":"STRING","data":"..."}
     * 4. Fallback -> {"type":"BINARY","data":"<base64>"}
     *
     * @param valueBytes raw record value bytes (may be null)
     * @param headers    record headers (checked for _content-type)
     * @return ObjectNode representing the DataObject JSON
     */
    static ObjectNode detectAndSerializeValue(byte[] valueBytes, Header[] headers) {
        ObjectNode node = MAPPER.createObjectNode();

        // Step 1 — null check
        if (valueBytes == null) {
            node.put("type", TYPE_NULL);
            return node;
        }

        // Step 2 — check headers for _content-type: application/json
        String contentType = getRecordHeader(headers, RECORD_HEADER_CONTENT_TYPE);
        if (CONTENT_TYPE_JSON.equals(contentType)) {
            try {
                JsonNode parsed = MAPPER.readTree(valueBytes);
                node.put("type", TYPE_JSON);
                node.set("data", parsed);
                return node;
            } catch (Exception e) {
                // Fall through to step 3
            }
        }

        // Step 3 — try UTF-8 decode, check for control chars
        try {
            CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT);
            String decoded = decoder.decode(ByteBuffer.wrap(valueBytes)).toString();

            if (!containsControlChars(decoded)) {
                node.put("type", TYPE_STRING);
                node.put("data", decoded);
                return node;
            }
            // Has control chars — fall through to step 4
        } catch (CharacterCodingException e) {
            // Invalid UTF-8 — fall through to step 4
        }

        // Step 4 — base64 encode as BINARY
        node.put("type", TYPE_BINARY);
        node.put("data", Base64.getEncoder().encodeToString(valueBytes));
        return node;
    }

    /**
     * Checks if a string contains C0/C1 control characters that make it unsuitable
     * for STRING type. Allows HT (0x09), LF (0x0A), CR (0x0D).
     *
     * @param s decoded UTF-8 string
     * @return true if the string contains disallowed control characters
     */
    static boolean containsControlChars(String s) {
        for (int i = 0; i < s.length(); i++) {
            if (isDisallowedControlChar(s.charAt(i))) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns true if the character is a disallowed control character.
     * Disallowed: 0x00-0x08, 0x0B, 0x0C, 0x0E-0x1F, 0x7F.
     * Allowed control chars: HT (0x09), LF (0x0A), CR (0x0D).
     */
    private static boolean isDisallowedControlChar(char c) {
        if (c <= 0x08) return true;                  // 0x00-0x08
        if (c == 0x0B || c == 0x0C) return true;     // VT, FF
        if (c >= 0x0E && c <= 0x1F) return true;     // 0x0E-0x1F
        return c == 0x7F;                             // DEL
    }

    /**
     * Checks record headers for a specific header name and returns its value.
     */
    static String getRecordHeader(Header[] headers, String name) {
        if (headers == null) {
            return null;
        }
        for (Header header : headers) {
            if (name.equals(header.key())) {
                byte[] value = header.value();
                if (value != null) {
                    return new String(value, StandardCharsets.UTF_8);
                }
                return null;
            }
        }
        return null;
    }

    // --- Partial failure ---

    /**
     * Determines the overall HTTP status for a response with potentially mixed errors.
     *
     * - All NONE -> 200
     * - All same non-NONE error -> that error's HTTP status
     * - Mixed (some success, some failure, or different errors) -> 207 Multi-Status
     *
     * @param errors collection of per-partition Errors values
     * @return the appropriate HTTP status
     */
    static HttpResponseStatus determineOverallStatus(Collection<Errors> errors) {
        if (errors.isEmpty()) {
            return HttpResponseStatus.OK;
        }

        Errors first = null;
        for (Errors error : errors) {
            if (first == null) {
                first = error;
            } else if (error != first) {
                return MULTI_STATUS;
            }
        }

        return mapErrorToHttpStatus(first);
    }

    // --- Error body builder ---

    /**
     * Builds a standalone JSON error response body.
     *
     * { "errorCode": 3, "errorMessage": "UNKNOWN_TOPIC_OR_PARTITION", "detail": "..." }
     */
    static String buildErrorBody(Errors error, String detail) {
        ObjectNode node = MAPPER.createObjectNode();
        node.put("errorCode", error.code());
        node.put("errorMessage", error.name());
        if (detail != null) {
            node.put("detail", detail);
        } else {
            node.putNull("detail");
        }
        try {
            return MAPPER.writeValueAsString(node);
        } catch (JsonProcessingException e) {
            // Should never happen with ObjectNode
            return "{\"errorCode\":" + error.code() + ",\"errorMessage\":\"" + error.name() + "\"}";
        }
    }

    // --- Response builder helper ---

    /**
     * Builds a FullHttpResponse from a JSON body and status, with standard headers.
     */
    static FullHttpResponse buildResponse(HttpResponseStatus status, String jsonBody,
                                          String requestId, int effectiveMaxWaitMs) {
        byte[] bodyBytes = jsonBody.getBytes(StandardCharsets.UTF_8);
        FullHttpResponse response = new DefaultFullHttpResponse(
                HttpVersion.HTTP_1_1,
                status,
                Unpooled.copiedBuffer(bodyBytes));

        response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json");
        response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, bodyBytes.length);

        if (requestId != null) {
            response.headers().set(HEADER_REQUEST_ID, requestId);
        }

        if (effectiveMaxWaitMs >= 0) {
            response.headers().setInt(HEADER_MAX_WAIT_APPLIED, effectiveMaxWaitMs);
        }

        response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);

        return response;
    }
}
