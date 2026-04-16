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
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.protocol.Errors;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * // Time: Created - TASK-B.02
 */
class HttpResponseSerializerTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // --- Error to HTTP status mapping ---

    @Test
    void mapError_none_returns200() {
        assertEquals(HttpResponseStatus.OK,
            HttpResponseSerializer.mapErrorToHttpStatus(Errors.NONE));
    }

    @Test
    void mapError_unknownTopicOrPartition_returns404() {
        assertEquals(HttpResponseStatus.NOT_FOUND,
            HttpResponseSerializer.mapErrorToHttpStatus(Errors.UNKNOWN_TOPIC_OR_PARTITION));
    }

    @Test
    void mapError_leaderNotAvailable_returns503() {
        assertEquals(HttpResponseStatus.SERVICE_UNAVAILABLE,
            HttpResponseSerializer.mapErrorToHttpStatus(Errors.LEADER_NOT_AVAILABLE));
    }

    @Test
    void mapError_messageTooLarge_returns413() {
        assertEquals(HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE,
            HttpResponseSerializer.mapErrorToHttpStatus(Errors.MESSAGE_TOO_LARGE));
    }

    @Test
    void mapError_topicAuthorizationFailed_returns403() {
        assertEquals(HttpResponseStatus.FORBIDDEN,
            HttpResponseSerializer.mapErrorToHttpStatus(Errors.TOPIC_AUTHORIZATION_FAILED));
    }

    @Test
    void mapError_requestTimedOut_returns504() {
        assertEquals(HttpResponseStatus.GATEWAY_TIMEOUT,
            HttpResponseSerializer.mapErrorToHttpStatus(Errors.REQUEST_TIMED_OUT));
    }

    @Test
    void mapError_throttlingQuotaExceeded_returns429() {
        assertEquals(HttpResponseStatus.TOO_MANY_REQUESTS,
            HttpResponseSerializer.mapErrorToHttpStatus(Errors.THROTTLING_QUOTA_EXCEEDED));
    }

    @Test
    void mapError_unknownError_returns500() {
        assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR,
            HttpResponseSerializer.mapErrorToHttpStatus(Errors.UNKNOWN_SERVER_ERROR));
    }

    // --- Retry-After ---

    @Test
    void retryAfter_leaderNotAvailable_returns1() {
        assertEquals(1, HttpResponseSerializer.retryAfterSeconds(Errors.LEADER_NOT_AVAILABLE));
    }

    @Test
    void retryAfter_notEnoughReplicas_returns5() {
        assertEquals(5, HttpResponseSerializer.retryAfterSeconds(Errors.NOT_ENOUGH_REPLICAS));
    }

    @Test
    void retryAfter_requestTimedOut_returns1() {
        assertEquals(1, HttpResponseSerializer.retryAfterSeconds(Errors.REQUEST_TIMED_OUT));
    }

    @Test
    void retryAfter_noRetry_returnsMinusOne() {
        assertEquals(-1, HttpResponseSerializer.retryAfterSeconds(Errors.INVALID_REQUEST));
    }

    @Test
    void retryAfter_throttling_ceilsToSeconds() {
        // 500ms -> ceil(0.5) = 1 second
        assertEquals(1, HttpResponseSerializer.retryAfterSeconds(
            Errors.THROTTLING_QUOTA_EXCEEDED, 500));
        // 1500ms -> ceil(1.5) = 2 seconds
        assertEquals(2, HttpResponseSerializer.retryAfterSeconds(
            Errors.THROTTLING_QUOTA_EXCEEDED, 1500));
        // 0ms -> max(1, 0) = 1 second minimum
        assertEquals(1, HttpResponseSerializer.retryAfterSeconds(
            Errors.THROTTLING_QUOTA_EXCEEDED, 0));
    }

    // --- Value type detection ---

    @Test
    void detectValue_null_returnsNullType() {
        ObjectNode result = HttpResponseSerializer.detectAndSerializeValue(null, new Header[0]);
        assertEquals("NULL", result.get("type").asText());
        assertFalse(result.has("data"));
    }

    @Test
    void detectValue_jsonContentType_returnsJsonType() {
        byte[] json = "{\"amount\":42.0}".getBytes(StandardCharsets.UTF_8);
        Header[] headers = {new RecordHeader("_content-type", "application/json".getBytes())};
        ObjectNode result = HttpResponseSerializer.detectAndSerializeValue(json, headers);
        assertEquals("JSON", result.get("type").asText());
        assertEquals(42.0, result.get("data").get("amount").asDouble());
    }

    @Test
    void detectValue_jsonContentTypeInvalidJson_fallsToString() {
        byte[] notJson = "not json".getBytes(StandardCharsets.UTF_8);
        Header[] headers = {new RecordHeader("_content-type", "application/json".getBytes())};
        ObjectNode result = HttpResponseSerializer.detectAndSerializeValue(notJson, headers);
        // Falls through to STRING since it's valid UTF-8
        assertEquals("STRING", result.get("type").asText());
        assertEquals("not json", result.get("data").asText());
    }

    @Test
    void detectValue_validUtf8_returnsStringType() {
        byte[] text = "Hello, world!".getBytes(StandardCharsets.UTF_8);
        ObjectNode result = HttpResponseSerializer.detectAndSerializeValue(text, new Header[0]);
        assertEquals("STRING", result.get("type").asText());
        assertEquals("Hello, world!", result.get("data").asText());
    }

    @Test
    void detectValue_utf8WithTab_returnsStringType() {
        byte[] text = "col1\tcol2".getBytes(StandardCharsets.UTF_8);
        ObjectNode result = HttpResponseSerializer.detectAndSerializeValue(text, new Header[0]);
        assertEquals("STRING", result.get("type").asText());
    }

    @Test
    void detectValue_utf8WithControlChars_returnsBinaryType() {
        byte[] data = new byte[]{0x01, 0x02, 0x03};  // control chars
        ObjectNode result = HttpResponseSerializer.detectAndSerializeValue(data, new Header[0]);
        assertEquals("BINARY", result.get("type").asText());
        assertEquals(Base64.getEncoder().encodeToString(data), result.get("data").asText());
    }

    @Test
    void detectValue_invalidUtf8_returnsBinaryType() {
        byte[] data = new byte[]{(byte) 0xFF, (byte) 0xFE};  // invalid UTF-8
        ObjectNode result = HttpResponseSerializer.detectAndSerializeValue(data, new Header[0]);
        assertEquals("BINARY", result.get("type").asText());
    }

    // --- Control char detection ---

    @Test
    void containsControlChars_normalText_returnsFalse() {
        assertFalse(HttpResponseSerializer.containsControlChars("Hello, world!"));
    }

    @Test
    void containsControlChars_withTab_returnsFalse() {
        assertFalse(HttpResponseSerializer.containsControlChars("col1\tcol2"));
    }

    @Test
    void containsControlChars_withNewline_returnsFalse() {
        assertFalse(HttpResponseSerializer.containsControlChars("line1\nline2"));
    }

    @Test
    void containsControlChars_withNullByte_returnsTrue() {
        assertTrue(HttpResponseSerializer.containsControlChars("hello\0world"));
    }

    @Test
    void containsControlChars_withBell_returnsTrue() {
        assertTrue(HttpResponseSerializer.containsControlChars("hello\u0007world"));
    }

    // --- Partial failure / 207 ---

    @Test
    void determineOverallStatus_allNone_returns200() {
        assertEquals(HttpResponseStatus.OK,
            HttpResponseSerializer.determineOverallStatus(List.of(Errors.NONE, Errors.NONE)));
    }

    @Test
    void determineOverallStatus_allSameError_returnsThatError() {
        assertEquals(HttpResponseStatus.NOT_FOUND,
            HttpResponseSerializer.determineOverallStatus(
                List.of(Errors.UNKNOWN_TOPIC_OR_PARTITION, Errors.UNKNOWN_TOPIC_OR_PARTITION)));
    }

    @Test
    void determineOverallStatus_mixed_returns207() {
        assertEquals(HttpResponseSerializer.MULTI_STATUS,
            HttpResponseSerializer.determineOverallStatus(
                List.of(Errors.NONE, Errors.UNKNOWN_TOPIC_OR_PARTITION)));
    }

    @Test
    void determineOverallStatus_singleSuccess_returns200() {
        assertEquals(HttpResponseStatus.OK,
            HttpResponseSerializer.determineOverallStatus(List.of(Errors.NONE)));
    }

    // --- Error body ---

    @Test
    void buildErrorBody_includesAllFields() throws Exception {
        String body = HttpResponseSerializer.buildErrorBody(
            Errors.UNKNOWN_TOPIC_OR_PARTITION, "Topic 'orders' not found");
        var node = MAPPER.readTree(body);
        assertEquals(3, node.get("errorCode").asInt());
        assertEquals("UNKNOWN_TOPIC_OR_PARTITION", node.get("errorMessage").asText());
        assertEquals("Topic 'orders' not found", node.get("detail").asText());
    }
}
