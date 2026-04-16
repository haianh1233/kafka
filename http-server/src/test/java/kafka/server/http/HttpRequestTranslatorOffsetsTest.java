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
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetFetchRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.OffsetFetchRequest;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class HttpRequestTranslatorOffsetsTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void translateCommitOffsets_validRequest_buildsCorrectData() throws Exception {
        JsonNode json = MAPPER.readTree("""
                {"offsets":[
                    {"topic":"orders","partition":0,"offset":150,"metadata":""},
                    {"topic":"orders","partition":1,"offset":88}
                ]}
                """);

        HttpRequestTranslator.OffsetCommitTranslationResult result =
            HttpRequestTranslator.translateCommitOffsets("my-group", json);

        assertEquals(ApiKeys.OFFSET_COMMIT, result.apiKey());

        OffsetCommitRequest request = result.builder().build(
            (short) (ApiKeys.OFFSET_COMMIT.latestVersion() < 10 ? ApiKeys.OFFSET_COMMIT.latestVersion() : 9));
        OffsetCommitRequestData data = request.data();
        assertEquals("my-group", data.groupId());
        assertEquals("", data.memberId());
        assertEquals(-1, data.generationIdOrMemberEpoch());
        assertEquals(1, data.topics().size());
        assertEquals("orders", data.topics().get(0).name());
        assertEquals(2, data.topics().get(0).partitions().size());
    }

    @Test
    void translateCommitOffsets_multipleTopics_groupsByTopic() throws Exception {
        JsonNode json = MAPPER.readTree("""
                {"offsets":[
                    {"topic":"orders","partition":0,"offset":100},
                    {"topic":"payments","partition":0,"offset":200}
                ]}
                """);

        HttpRequestTranslator.OffsetCommitTranslationResult result =
            HttpRequestTranslator.translateCommitOffsets("my-group", json);

        OffsetCommitRequest request = result.builder().build(
            (short) (ApiKeys.OFFSET_COMMIT.latestVersion() < 10 ? ApiKeys.OFFSET_COMMIT.latestVersion() : 9));
        assertEquals(2, request.data().topics().size());
    }

    @Test
    void translateCommitOffsets_emptyArray_throws() throws Exception {
        JsonNode json = MAPPER.readTree("{\"offsets\":[]}");
        assertThrows(InvalidRequestException.class, () ->
            HttpRequestTranslator.translateCommitOffsets("my-group", json));
    }

    @Test
    void translateCommitOffsets_missingOffsetsField_throws() throws Exception {
        JsonNode json = MAPPER.readTree("{}");
        assertThrows(InvalidRequestException.class, () ->
            HttpRequestTranslator.translateCommitOffsets("my-group", json));
    }

    @Test
    void translateCommitOffsets_missingRequiredField_throws() throws Exception {
        // Missing 'offset' field
        JsonNode json = MAPPER.readTree("{\"offsets\":[{\"topic\":\"orders\",\"partition\":0}]}");
        assertThrows(InvalidRequestException.class, () ->
            HttpRequestTranslator.translateCommitOffsets("my-group", json));
    }

    @Test
    void translateCommitOffsets_negativePartition_throws() throws Exception {
        JsonNode json = MAPPER.readTree("{\"offsets\":[{\"topic\":\"orders\",\"partition\":-1,\"offset\":10}]}");
        assertThrows(InvalidRequestException.class, () ->
            HttpRequestTranslator.translateCommitOffsets("my-group", json));
    }

    @Test
    void translateCommitOffsets_negativeOffset_throws() throws Exception {
        JsonNode json = MAPPER.readTree("{\"offsets\":[{\"topic\":\"orders\",\"partition\":0,\"offset\":-5}]}");
        assertThrows(InvalidRequestException.class, () ->
            HttpRequestTranslator.translateCommitOffsets("my-group", json));
    }

    @Test
    void translateCommitOffsets_defaultMetadata_isEmpty() throws Exception {
        JsonNode json = MAPPER.readTree("{\"offsets\":[{\"topic\":\"t\",\"partition\":0,\"offset\":1}]}");

        HttpRequestTranslator.OffsetCommitTranslationResult result =
            HttpRequestTranslator.translateCommitOffsets("g", json);

        OffsetCommitRequest request = result.builder().build(
            (short) (ApiKeys.OFFSET_COMMIT.latestVersion() < 10 ? ApiKeys.OFFSET_COMMIT.latestVersion() : 9));
        assertEquals("", request.data().topics().get(0).partitions().get(0).committedMetadata());
    }

    @Test
    void translateFetchOffsets_allTopics_nullTopics() {
        HttpRequestTranslator.OffsetFetchTranslationResult result =
            HttpRequestTranslator.translateFetchOffsets("my-group", null);

        assertEquals(ApiKeys.OFFSET_FETCH, result.apiKey());

        // Build at a version that supports batching (v8+) to use groups format
        short version = (short) Math.min(ApiKeys.OFFSET_FETCH.latestVersion(),
            (short) (OffsetFetchRequest.TOPIC_ID_MIN_VERSION - 1));
        if (version < OffsetFetchRequest.BATCH_MIN_VERSION) {
            version = OffsetFetchRequest.BATCH_MIN_VERSION;
        }
        OffsetFetchRequest request = result.builder().build(version);
        OffsetFetchRequestData data = request.data();
        assertEquals(1, data.groups().size());
        assertEquals("my-group", data.groups().get(0).groupId());
        assertNull(data.groups().get(0).topics());
    }

    @Test
    void translateFetchOffsets_withTopicFilter_setsTopic() {
        HttpRequestTranslator.OffsetFetchTranslationResult result =
            HttpRequestTranslator.translateFetchOffsets("my-group", "orders");

        short version = (short) Math.min(ApiKeys.OFFSET_FETCH.latestVersion(),
            (short) (OffsetFetchRequest.TOPIC_ID_MIN_VERSION - 1));
        if (version < OffsetFetchRequest.BATCH_MIN_VERSION) {
            version = OffsetFetchRequest.BATCH_MIN_VERSION;
        }
        OffsetFetchRequest request = result.builder().build(version);
        OffsetFetchRequestData data = request.data();
        assertEquals(1, data.groups().size());
        var topics = data.groups().get(0).topics();
        assertNotNull(topics);
        assertEquals(1, topics.size());
        assertEquals("orders", topics.get(0).name());
    }
}
