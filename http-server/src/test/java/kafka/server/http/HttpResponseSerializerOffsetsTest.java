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
// Time: Created - TASK-F.01
package kafka.server.http;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.message.OffsetFetchResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.OffsetCommitResponse;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HttpResponseSerializerOffsetsTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // --- OffsetCommitResponse ---

    @Test
    void serializeOffsetCommitResponse_success_returnsPerPartitionResults() throws Exception {
        OffsetCommitResponseData data = new OffsetCommitResponseData();
        OffsetCommitResponseData.OffsetCommitResponseTopic topic =
            new OffsetCommitResponseData.OffsetCommitResponseTopic().setName("orders");
        topic.partitions().add(new OffsetCommitResponseData.OffsetCommitResponsePartition()
            .setPartitionIndex(0).setErrorCode(Errors.NONE.code()));
        topic.partitions().add(new OffsetCommitResponseData.OffsetCommitResponsePartition()
            .setPartitionIndex(1).setErrorCode(Errors.NONE.code()));
        data.topics().add(topic);

        OffsetCommitResponse response = new OffsetCommitResponse(data);
        ObjectNode result = HttpResponseSerializer.serializeOffsetCommitResponse(response);

        JsonNode offsets = result.get("offsets");
        assertEquals(2, offsets.size());
        assertEquals("orders", offsets.get(0).get("topic").asText());
        assertEquals(0, offsets.get(0).get("partition").asInt());
        assertEquals(0, offsets.get(0).get("errorCode").asInt());
        assertFalse(offsets.get(0).has("errorMessage"));
    }

    @Test
    void serializeOffsetCommitResponse_withErrors_includesErrorMessage() throws Exception {
        OffsetCommitResponseData data = new OffsetCommitResponseData();
        OffsetCommitResponseData.OffsetCommitResponseTopic topic =
            new OffsetCommitResponseData.OffsetCommitResponseTopic().setName("orders");
        topic.partitions().add(new OffsetCommitResponseData.OffsetCommitResponsePartition()
            .setPartitionIndex(0).setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code()));
        data.topics().add(topic);

        OffsetCommitResponse response = new OffsetCommitResponse(data);
        ObjectNode result = HttpResponseSerializer.serializeOffsetCommitResponse(response);

        JsonNode offsets = result.get("offsets");
        assertEquals(1, offsets.size());
        assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION.code(), offsets.get(0).get("errorCode").asInt());
        assertTrue(offsets.get(0).has("errorMessage"));
    }

    // --- OffsetFetchResponse ---

    @Test
    void serializeOffsetFetchResponse_success_returnsGroupAndOffsets() throws Exception {
        OffsetFetchResponseData data = new OffsetFetchResponseData();
        OffsetFetchResponseData.OffsetFetchResponseGroup group =
            new OffsetFetchResponseData.OffsetFetchResponseGroup()
                .setGroupId("my-group")
                .setErrorCode(Errors.NONE.code());

        OffsetFetchResponseData.OffsetFetchResponseTopics topicData =
            new OffsetFetchResponseData.OffsetFetchResponseTopics().setName("orders");
        topicData.partitions().add(new OffsetFetchResponseData.OffsetFetchResponsePartitions()
            .setPartitionIndex(0)
            .setCommittedOffset(150)
            .setMetadata("test")
            .setErrorCode(Errors.NONE.code()));
        topicData.partitions().add(new OffsetFetchResponseData.OffsetFetchResponsePartitions()
            .setPartitionIndex(1)
            .setCommittedOffset(88)
            .setMetadata("")
            .setErrorCode(Errors.NONE.code()));
        group.topics().add(topicData);
        data.groups().add(group);

        OffsetFetchResponse response = new OffsetFetchResponse(data, ApiKeys.OFFSET_FETCH.latestVersion());
        ObjectNode result = HttpResponseSerializer.serializeOffsetFetchResponse(response, "my-group");

        assertEquals("my-group", result.get("group").asText());
        JsonNode offsets = result.get("offsets");
        assertEquals(2, offsets.size());
        assertEquals("orders", offsets.get(0).get("topic").asText());
        assertEquals(0, offsets.get(0).get("partition").asInt());
        assertEquals(150, offsets.get(0).get("offset").asLong());
        assertEquals("test", offsets.get(0).get("metadata").asText());
    }

    @Test
    void serializeOffsetFetchResponse_negativeOffset_filteredOut() throws Exception {
        OffsetFetchResponseData data = new OffsetFetchResponseData();
        OffsetFetchResponseData.OffsetFetchResponseGroup group =
            new OffsetFetchResponseData.OffsetFetchResponseGroup()
                .setGroupId("my-group")
                .setErrorCode(Errors.NONE.code());

        OffsetFetchResponseData.OffsetFetchResponseTopics topicData =
            new OffsetFetchResponseData.OffsetFetchResponseTopics().setName("orders");
        topicData.partitions().add(new OffsetFetchResponseData.OffsetFetchResponsePartitions()
            .setPartitionIndex(0)
            .setCommittedOffset(-1)  // No offset committed
            .setMetadata("")
            .setErrorCode(Errors.NONE.code()));
        group.topics().add(topicData);
        data.groups().add(group);

        OffsetFetchResponse response = new OffsetFetchResponse(data, ApiKeys.OFFSET_FETCH.latestVersion());
        ObjectNode result = HttpResponseSerializer.serializeOffsetFetchResponse(response, "my-group");

        JsonNode offsets = result.get("offsets");
        assertEquals(0, offsets.size());  // Negative offset = no committed offset, filtered out
    }

    @Test
    void serializeOffsetFetchResponse_groupLevelError_returnsError() throws Exception {
        OffsetFetchResponseData data = new OffsetFetchResponseData();
        OffsetFetchResponseData.OffsetFetchResponseGroup group =
            new OffsetFetchResponseData.OffsetFetchResponseGroup()
                .setGroupId("my-group")
                .setErrorCode(Errors.GROUP_AUTHORIZATION_FAILED.code());
        data.groups().add(group);

        OffsetFetchResponse response = new OffsetFetchResponse(data, ApiKeys.OFFSET_FETCH.latestVersion());
        ObjectNode result = HttpResponseSerializer.serializeOffsetFetchResponse(response, "my-group");

        // Group-level error should be indicated
        assertTrue(result.has("errorCode"));
        assertEquals(Errors.GROUP_AUTHORIZATION_FAILED.code(), result.get("errorCode").asInt());
    }
}
