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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.message.ListOffsetsRequestData;
import org.apache.kafka.common.message.ListOffsetsResponseData;
import org.apache.kafka.common.message.OffsetFetchRequestData;
import org.apache.kafka.common.message.OffsetFetchResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.requests.ListOffsetsResponse;
import org.apache.kafka.common.requests.OffsetFetchRequest;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URLDecoder;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Handles GET /v1/consumer-groups/{group}/lags.
 *
 * Orchestrates two sequential RequestChannel round-trips:
 * <ol>
 *   <li>OFFSET_FETCH -> committed offsets for the group</li>
 *   <li>LIST_OFFSETS -> log-end offsets for all partitions found in step 1</li>
 * </ol>
 *
 * Merges results: lag = max(0, logEndOffset - committedOffset).
 *
 * The two queries are inherently sequential because phase 2 (LIST_OFFSETS) needs to know
 * which topic-partitions the group has committed offsets for, which is only known after
 * phase 1 (OFFSET_FETCH) completes. Uses {@link CompletableFuture#thenCompose} to chain
 * the two phases.
 *
 * Authorization is enforced inside existing KafkaApis handlers -- DESCRIBE on group for
 * OFFSET_FETCH, READ on topic-partitions for LIST_OFFSETS. This handler does NOT add
 * custom authorization checks.
 */
public class ConsumerGroupLagHandler {

    private static final Logger log = LoggerFactory.getLogger(ConsumerGroupLagHandler.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * Creates a new ConsumerGroupLagHandler.
     */
    public ConsumerGroupLagHandler() {
    }

    /**
     * Validates a consumer group ID from a URL path segment.
     * Group IDs have fewer restrictions than topic names but must be non-empty
     * and at most 255 characters after URL-decoding.
     *
     * @param rawSegment URL-encoded group ID from the URI path
     * @return validated, decoded group ID
     * @throws org.apache.kafka.common.errors.InvalidRequestException if validation fails
     */
    public static String validateGroupId(String rawSegment) {
        String decoded = URLDecoder.decode(rawSegment, StandardCharsets.UTF_8);
        if (decoded.isEmpty()) {
            throw new org.apache.kafka.common.errors.InvalidRequestException(
                "Group ID must not be empty");
        }
        if (decoded.length() > 255) {
            throw new org.apache.kafka.common.errors.InvalidRequestException(
                "Group ID exceeds maximum length of 255");
        }
        return decoded;
    }

    /**
     * Builds an OffsetFetchRequest for the given consumer group to fetch all committed offsets.
     * Uses null topics to request offsets for all topics the group has committed to.
     *
     * @param group consumer group ID
     * @return serialized OffsetFetchRequest as ByteBuffer
     */
    public ByteBuffer buildOffsetFetchRequest(String group) {
        OffsetFetchRequestData data = new OffsetFetchRequestData();
        OffsetFetchRequestData.OffsetFetchRequestGroup groupData =
            new OffsetFetchRequestData.OffsetFetchRequestGroup()
                .setGroupId(group)
                .setTopics(null); // null = all topics for this group
        data.setGroups(Collections.singletonList(groupData));

        short version = org.apache.kafka.common.protocol.ApiKeys.OFFSET_FETCH.latestVersion();
        OffsetFetchRequest request = OffsetFetchRequest.Builder.forTopicIdsOrNames(data, false).build(version);
        return request.serialize().buffer();
    }

    /**
     * Extracts committed offsets from an OffsetFetchResponse.
     * Returns a map from TopicPartition to committed offset.
     * Partitions with error codes are skipped.
     *
     * @param response the OffsetFetch response data
     * @return map of TopicPartition to committed offset
     */
    public Map<TopicPartition, Long> extractCommittedOffsets(OffsetFetchResponse response) {
        Map<TopicPartition, Long> offsets = new LinkedHashMap<>();

        for (OffsetFetchResponseData.OffsetFetchResponseGroup group : response.data().groups()) {
            Errors groupError = Errors.forCode(group.errorCode());
            if (groupError != Errors.NONE) {
                if (groupError == Errors.GROUP_AUTHORIZATION_FAILED) {
                    throw new GroupAuthorizationException("Not authorized to describe group");
                }
                if (groupError == Errors.GROUP_ID_NOT_FOUND) {
                    throw new GroupIdNotFoundException("Group not found");
                }
                log.warn("OffsetFetch returned group-level error: {}", groupError);
                continue;
            }

            for (OffsetFetchResponseData.OffsetFetchResponseTopics topic : group.topics()) {
                for (OffsetFetchResponseData.OffsetFetchResponsePartitions partition : topic.partitions()) {
                    Errors partError = Errors.forCode(partition.errorCode());
                    if (partError != Errors.NONE) {
                        log.debug("Skipping partition {}-{} due to error: {}",
                            topic.name(), partition.partitionIndex(), partError);
                        continue;
                    }
                    offsets.put(
                        new TopicPartition(topic.name(), partition.partitionIndex()),
                        partition.committedOffset()
                    );
                }
            }
        }
        return offsets;
    }

    /**
     * Builds a ListOffsetsRequest for LATEST timestamp across all given partitions.
     * Used to fetch log-end offsets for lag computation.
     *
     * @param topicPartitions the partitions to query
     * @return serialized ListOffsetsRequest as ByteBuffer
     */
    public ByteBuffer buildListOffsetsRequest(List<TopicPartition> topicPartitions) {
        // Group partitions by topic
        Map<String, List<TopicPartition>> byTopic = new LinkedHashMap<>();
        for (TopicPartition tp : topicPartitions) {
            byTopic.computeIfAbsent(tp.topic(), k -> new ArrayList<>()).add(tp);
        }

        List<ListOffsetsRequestData.ListOffsetsTopic> topics = new ArrayList<>();
        for (Map.Entry<String, List<TopicPartition>> entry : byTopic.entrySet()) {
            ListOffsetsRequestData.ListOffsetsTopic topicData =
                new ListOffsetsRequestData.ListOffsetsTopic().setName(entry.getKey());
            List<ListOffsetsRequestData.ListOffsetsPartition> partitionList = new ArrayList<>();
            for (TopicPartition tp : entry.getValue()) {
                partitionList.add(new ListOffsetsRequestData.ListOffsetsPartition()
                    .setPartitionIndex(tp.partition())
                    .setTimestamp(ListOffsetsRequest.LATEST_TIMESTAMP)
                    .setCurrentLeaderEpoch(-1));
            }
            topicData.setPartitions(partitionList);
            topics.add(topicData);
        }

        short version = org.apache.kafka.common.protocol.ApiKeys.LIST_OFFSETS.latestVersion();
        ListOffsetsRequest.Builder builder = ListOffsetsRequest.Builder.forConsumer(
            false, IsolationLevel.READ_UNCOMMITTED);
        builder.setTargetTimes(topics);
        ListOffsetsRequest request = builder.build(version);
        return request.serialize().buffer();
    }

    /**
     * Extracts log-end offsets from a ListOffsetsResponse.
     * Returns a map from TopicPartition to log-end offset.
     * Partitions with error codes are skipped.
     *
     * @param response the ListOffsets response
     * @return map of TopicPartition to log-end offset
     */
    public Map<TopicPartition, Long> extractLogEndOffsets(ListOffsetsResponse response) {
        Map<TopicPartition, Long> offsets = new LinkedHashMap<>();

        for (ListOffsetsResponseData.ListOffsetsTopicResponse topicResponse : response.data().topics()) {
            for (ListOffsetsResponseData.ListOffsetsPartitionResponse partition : topicResponse.partitions()) {
                Errors error = Errors.forCode(partition.errorCode());
                if (error != Errors.NONE) {
                    log.debug("Skipping partition {}-{} due to ListOffsets error: {}",
                        topicResponse.name(), partition.partitionIndex(), error);
                    continue;
                }
                offsets.put(
                    new TopicPartition(topicResponse.name(), partition.partitionIndex()),
                    partition.offset()
                );
            }
        }
        return offsets;
    }

    /**
     * Merge committed offsets and log-end offsets into the lag response.
     * <p>
     * Per section 4.3.3: lag = max(0, logEndOffset - committedOffset).
     * This clamping prevents negative lag values that can arise from sampling skew
     * between the two concurrent queries.
     * <p>
     * Partitions are sorted by topic name then partition index for deterministic output.
     * If logEndOffset is unavailable for a partition, reports logEndOffset as -1 and lag as 0.
     *
     * @param group            consumer group ID
     * @param committedOffsets committed offsets keyed by TopicPartition
     * @param logEndOffsets    log-end offsets keyed by TopicPartition
     * @return FullHttpResponse with JSON body containing lag data
     */
    FullHttpResponse buildLagResponse(
            String group,
            Map<TopicPartition, Long> committedOffsets,
            Map<TopicPartition, Long> logEndOffsets) {

        ObjectNode root = MAPPER.createObjectNode();
        root.put("group", group);

        ArrayNode partitionsArray = root.putArray("partitions");
        long totalLag = 0L;

        // Sort by topic name, then partition index for deterministic output
        List<Map.Entry<TopicPartition, Long>> sortedEntries = new ArrayList<>(committedOffsets.entrySet());
        sortedEntries.sort(Comparator
            .<Map.Entry<TopicPartition, Long>, String>comparing(e -> e.getKey().topic())
            .thenComparingInt(e -> e.getKey().partition()));

        for (Map.Entry<TopicPartition, Long> entry : sortedEntries) {
            TopicPartition tp = entry.getKey();
            long committedOffset = entry.getValue();
            long logEndOffset = logEndOffsets.getOrDefault(tp, -1L);

            long lag;
            if (logEndOffset >= 0 && committedOffset >= 0) {
                lag = Math.max(0L, logEndOffset - committedOffset);
            } else {
                lag = 0L;
            }

            totalLag += lag;

            ObjectNode partNode = partitionsArray.addObject();
            partNode.put("topic", tp.topic());
            partNode.put("partition", tp.partition());
            partNode.put("committedOffset", committedOffset);
            partNode.put("logEndOffset", logEndOffset);
            partNode.put("lag", lag);
        }

        root.put("totalLag", totalLag);

        return buildJsonResponse(HttpResponseStatus.OK, root);
    }

    /**
     * Builds an error JSON response for exceptions during lag computation.
     *
     * @param ex the exception that occurred
     * @return FullHttpResponse with appropriate HTTP status and error JSON
     */
    public FullHttpResponse buildErrorResponse(Throwable ex) {
        Throwable cause = (ex.getCause() != null) ? ex.getCause() : ex;

        HttpResponseStatus status;
        int errorCode;
        String errorMessage;

        if (cause instanceof GroupIdNotFoundException) {
            status = HttpResponseStatus.NOT_FOUND;
            errorCode = Errors.GROUP_ID_NOT_FOUND.code();
            errorMessage = "GROUP_ID_NOT_FOUND";
        } else if (cause instanceof GroupAuthorizationException) {
            status = HttpResponseStatus.FORBIDDEN;
            errorCode = Errors.GROUP_AUTHORIZATION_FAILED.code();
            errorMessage = "GROUP_AUTHORIZATION_FAILED";
        } else {
            status = HttpResponseStatus.INTERNAL_SERVER_ERROR;
            errorCode = Errors.UNKNOWN_SERVER_ERROR.code();
            errorMessage = "INTERNAL_ERROR";
        }

        ObjectNode body = MAPPER.createObjectNode();
        body.put("errorCode", errorCode);
        body.put("errorMessage", errorMessage);
        if (cause.getMessage() != null) {
            body.put("detail", cause.getMessage());
        } else {
            body.putNull("detail");
        }

        return buildJsonResponse(status, body);
    }

    /**
     * Helper to build a FullHttpResponse from a Jackson ObjectNode.
     */
    private static FullHttpResponse buildJsonResponse(HttpResponseStatus status, ObjectNode body) {
        byte[] bodyBytes;
        try {
            bodyBytes = MAPPER.writeValueAsBytes(body);
        } catch (Exception e) {
            bodyBytes = "{\"errorCode\":-1,\"errorMessage\":\"SERIALIZATION_ERROR\"}".getBytes(StandardCharsets.UTF_8);
            status = HttpResponseStatus.INTERNAL_SERVER_ERROR;
        }

        FullHttpResponse response = new DefaultFullHttpResponse(
            HttpVersion.HTTP_1_1,
            status,
            Unpooled.copiedBuffer(bodyBytes));

        response.headers().set(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON);
        response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, bodyBytes.length);
        response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);

        return response;
    }
}
