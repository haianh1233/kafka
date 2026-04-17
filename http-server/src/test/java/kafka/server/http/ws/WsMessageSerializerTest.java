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
// Time: Created - TASK-WS1.09
package kafka.server.http.ws;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import kafka.server.http.HttpRequestTranslator;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests for WsMessageSerializer — the pure transformation class that converts a
 * WebSocket publish JSON frame into Kafka record components (key/value/headers).
 *
 * // Time: Created - TASK-WS1.09
 */
class WsMessageSerializerTest {

    private static final ObjectMapper MAPPER = HttpRequestTranslator.MAPPER;
    private WsMessageSerializer serializer;

    @BeforeEach
    void setUp() {
        serializer = new WsMessageSerializer();
    }

    // --- Key serialization ---

    @Test
    void serialize_routingKey_becomesRecordKey() {
        ObjectNode msg = MAPPER.createObjectNode();
        msg.putObject("body").put("orderId", "123");

        var result = serializer.serialize("events", "order.created", msg, "/");
        assertArrayEquals("order.created".getBytes(StandardCharsets.UTF_8), result.key());
    }

    @Test
    void serialize_emptyRoutingKey_keyIsNull() {
        ObjectNode msg = MAPPER.createObjectNode();
        msg.put("body", "hello");

        var result = serializer.serialize("events", "", msg, "/");
        assertNull(result.key());
    }

    // --- Body serialization ---

    @Test
    void serialize_jsonBody_serializedAsJson() throws Exception {
        ObjectNode msg = MAPPER.createObjectNode();
        ObjectNode body = msg.putObject("body");
        body.put("orderId", "123");
        msg.put("contentType", "application/json");

        var result = serializer.serialize("events", "key", msg, "/");
        JsonNode parsed = MAPPER.readTree(result.value());
        assertEquals("123", parsed.get("orderId").asText());
    }

    @Test
    void serialize_stringBody_serializedAsUtf8() {
        ObjectNode msg = MAPPER.createObjectNode();
        msg.put("body", "hello world");

        var result = serializer.serialize("events", "key", msg, "/");
        assertArrayEquals("hello world".getBytes(StandardCharsets.UTF_8), result.value());
    }

    @Test
    void serialize_binaryBody_base64Decoded() {
        ObjectNode msg = MAPPER.createObjectNode();
        msg.put("body", Base64.getEncoder().encodeToString("binary data".getBytes(StandardCharsets.UTF_8)));
        msg.put("contentType", "application/octet-stream");

        var result = serializer.serialize("events", "key", msg, "/");
        assertArrayEquals("binary data".getBytes(StandardCharsets.UTF_8), result.value());
    }

    @Test
    void serialize_binaryBody_invalidBase64_throws() {
        ObjectNode msg = MAPPER.createObjectNode();
        msg.put("body", "!!!not-base64!!!");
        msg.put("contentType", "application/octet-stream");

        assertThrows(IllegalArgumentException.class,
            () -> serializer.serialize("events", "key", msg, "/"));
    }

    @Test
    void serialize_jsonObjectBody_noContentType_serializedAsJson() throws Exception {
        ObjectNode msg = MAPPER.createObjectNode();
        msg.putObject("body").put("key", "value");

        var result = serializer.serialize("events", "key", msg, "/");
        JsonNode parsed = MAPPER.readTree(result.value());
        assertEquals("value", parsed.get("key").asText());
    }

    @Test
    void serialize_jsonArrayBody_noContentType_serializedAsJson() throws Exception {
        ObjectNode msg = MAPPER.createObjectNode();
        msg.putArray("body").add("a").add("b");

        var result = serializer.serialize("events", "key", msg, "/");
        JsonNode parsed = MAPPER.readTree(result.value());
        assertTrue(parsed.isArray());
        assertEquals("a", parsed.get(0).asText());
    }

    @Test
    void serialize_utf8Body_preservesUtf8Bytes() {
        ObjectNode msg = MAPPER.createObjectNode();
        msg.put("body", "héllo wörld \u00e9");

        var result = serializer.serialize("events", "key", msg, "/");
        assertArrayEquals("héllo wörld \u00e9".getBytes(StandardCharsets.UTF_8), result.value());
    }

    @Test
    void serialize_emptyStringBody_emptyValueBytes() {
        ObjectNode msg = MAPPER.createObjectNode();
        msg.put("body", "");

        var result = serializer.serialize("events", "key", msg, "/");
        assertNotNull(result.value());
        assertEquals(0, result.value().length);
    }

    // --- Header mapping ---

    @Test
    void serialize_alwaysIncludesRoutingHeaders() {
        ObjectNode msg = MAPPER.createObjectNode();
        msg.put("body", "test");

        var result = serializer.serialize("events", "order.created", msg, "/production");
        assertHeaderValue(result, WsMessageSerializer.HDR_EXCHANGE, "events");
        assertHeaderValue(result, WsMessageSerializer.HDR_ROUTING_KEY, "order.created");
        assertHeaderValue(result, WsMessageSerializer.HDR_VHOST, "/production");
    }

    @Test
    void serialize_emptyRoutingKey_stillAddsRoutingKeyHeader() {
        ObjectNode msg = MAPPER.createObjectNode();
        msg.put("body", "test");

        var result = serializer.serialize("events", "", msg, "/");
        // The header records the routing-key value even when empty (for round-trip fidelity).
        assertHeaderValue(result, WsMessageSerializer.HDR_ROUTING_KEY, "");
    }

    @Test
    void serialize_contentType_mappedToHeader_noWsPrefix() {
        ObjectNode msg = MAPPER.createObjectNode();
        msg.put("body", "test");
        msg.put("contentType", "application/json");

        var result = serializer.serialize("events", "key", msg, "/");
        assertHeaderValue(result, WsMessageSerializer.HDR_CONTENT_TYPE, "application/json");
        assertEquals("_content-type", WsMessageSerializer.HDR_CONTENT_TYPE,
            "_content-type header MUST NOT have the _ws_ prefix");
    }

    @Test
    void serialize_deliveryMode_mappedToHeader() {
        ObjectNode msg = MAPPER.createObjectNode();
        msg.put("body", "test");
        msg.put("deliveryMode", 2);

        var result = serializer.serialize("events", "key", msg, "/");
        assertHeaderValue(result, WsMessageSerializer.HDR_DELIVERY_MODE, "2");
    }

    @Test
    void serialize_correlationId_mappedToHeader() {
        ObjectNode msg = MAPPER.createObjectNode();
        msg.put("body", "test");
        msg.put("correlationId", "req-123");

        var result = serializer.serialize("events", "key", msg, "/");
        assertHeaderValue(result, WsMessageSerializer.HDR_CORRELATION_ID, "req-123");
    }

    @Test
    void serialize_replyTo_mappedToHeader() {
        ObjectNode msg = MAPPER.createObjectNode();
        msg.put("body", "test");
        msg.put("replyTo", "replies");

        var result = serializer.serialize("events", "key", msg, "/");
        assertHeaderValue(result, WsMessageSerializer.HDR_REPLY_TO, "replies");
    }

    @Test
    void serialize_messageId_mappedToHeader() {
        ObjectNode msg = MAPPER.createObjectNode();
        msg.put("body", "test");
        msg.put("messageId", "msg-abc");

        var result = serializer.serialize("events", "key", msg, "/");
        assertHeaderValue(result, WsMessageSerializer.HDR_MESSAGE_ID, "msg-abc");
    }

    @Test
    void serialize_allAmqpProperties_allMapped() {
        ObjectNode msg = MAPPER.createObjectNode();
        msg.put("body", "test");
        msg.put("contentType", "text/plain");
        msg.put("contentEncoding", "utf-8");
        msg.put("deliveryMode", 2);
        msg.put("priority", 5);
        msg.put("correlationId", "corr-1");
        msg.put("replyTo", "reply-q");
        msg.put("expiration", "60000");
        msg.put("messageId", "msg-1");
        msg.put("type", "OrderCreated");
        msg.put("userId", "svc-user");
        msg.put("appId", "checkout");
        msg.putObject("headers").put("source", "web");

        var result = serializer.serialize("events", "key", msg, "/");

        assertHeaderValue(result, WsMessageSerializer.HDR_CONTENT_TYPE, "text/plain");
        assertHeaderValue(result, WsMessageSerializer.HDR_CONTENT_ENCODING, "utf-8");
        assertHeaderValue(result, WsMessageSerializer.HDR_DELIVERY_MODE, "2");
        assertHeaderValue(result, WsMessageSerializer.HDR_PRIORITY, "5");
        assertHeaderValue(result, WsMessageSerializer.HDR_CORRELATION_ID, "corr-1");
        assertHeaderValue(result, WsMessageSerializer.HDR_REPLY_TO, "reply-q");
        assertHeaderValue(result, WsMessageSerializer.HDR_EXPIRATION, "60000");
        assertHeaderValue(result, WsMessageSerializer.HDR_MESSAGE_ID, "msg-1");
        assertHeaderValue(result, WsMessageSerializer.HDR_TYPE, "OrderCreated");
        assertHeaderValue(result, WsMessageSerializer.HDR_USER_ID, "svc-user");
        assertHeaderValue(result, WsMessageSerializer.HDR_APP_ID, "checkout");
        assertHeaderPresent(result, WsMessageSerializer.HDR_HEADERS);
    }

    @Test
    void serialize_missingOptionalProperties_omitsHeaders() {
        ObjectNode msg = MAPPER.createObjectNode();
        msg.put("body", "test");

        var result = serializer.serialize("events", "key", msg, "/");

        // Only routing headers should be present
        assertHeaderPresent(result, WsMessageSerializer.HDR_EXCHANGE);
        assertHeaderPresent(result, WsMessageSerializer.HDR_ROUTING_KEY);
        assertHeaderPresent(result, WsMessageSerializer.HDR_VHOST);
        assertHeaderAbsent(result, WsMessageSerializer.HDR_CORRELATION_ID);
        assertHeaderAbsent(result, WsMessageSerializer.HDR_REPLY_TO);
        assertHeaderAbsent(result, WsMessageSerializer.HDR_CONTENT_TYPE);
        assertHeaderAbsent(result, WsMessageSerializer.HDR_HEADERS);
        assertHeaderAbsent(result, WsMessageSerializer.HDR_DELIVERY_MODE);
        assertHeaderAbsent(result, WsMessageSerializer.HDR_PRIORITY);
        assertHeaderAbsent(result, WsMessageSerializer.HDR_MESSAGE_ID);
        assertHeaderAbsent(result, WsMessageSerializer.HDR_TYPE);
        assertHeaderAbsent(result, WsMessageSerializer.HDR_USER_ID);
        assertHeaderAbsent(result, WsMessageSerializer.HDR_APP_ID);
        assertHeaderAbsent(result, WsMessageSerializer.HDR_EXPIRATION);
        assertHeaderAbsent(result, WsMessageSerializer.HDR_CONTENT_ENCODING);
    }

    @Test
    void serialize_nullJsonNodeProperty_omitsHeader() {
        ObjectNode msg = MAPPER.createObjectNode();
        msg.put("body", "test");
        msg.putNull("correlationId");

        var result = serializer.serialize("events", "key", msg, "/");
        assertHeaderAbsent(result, WsMessageSerializer.HDR_CORRELATION_ID);
    }

    @Test
    void serialize_applicationHeaders_serializedAsJson() throws Exception {
        ObjectNode msg = MAPPER.createObjectNode();
        msg.put("body", "test");
        ObjectNode headers = msg.putObject("headers");
        headers.put("source", "web");
        headers.put("priority", "high");

        var result = serializer.serialize("events", "key", msg, "/");
        String headersJson = getHeaderValue(result, WsMessageSerializer.HDR_HEADERS);
        assertNotNull(headersJson);
        JsonNode parsed = MAPPER.readTree(headersJson);
        assertEquals("web", parsed.get("source").asText());
        assertEquals("high", parsed.get("priority").asText());
    }

    // --- Null handling ---

    @Test
    void serialize_nullMessage_throwsNPE() {
        assertThrows(NullPointerException.class,
            () -> serializer.serialize("ex", "key", null, "/"));
    }

    @Test
    void serialize_nullExchange_throwsNPE() {
        ObjectNode msg = MAPPER.createObjectNode();
        msg.put("body", "test");
        assertThrows(NullPointerException.class,
            () -> serializer.serialize(null, "key", msg, "/"));
    }

    @Test
    void serialize_nullRoutingKey_throwsNPE() {
        ObjectNode msg = MAPPER.createObjectNode();
        msg.put("body", "test");
        assertThrows(NullPointerException.class,
            () -> serializer.serialize("ex", null, msg, "/"));
    }

    @Test
    void serialize_nullVhost_throwsNPE() {
        ObjectNode msg = MAPPER.createObjectNode();
        msg.put("body", "test");
        assertThrows(NullPointerException.class,
            () -> serializer.serialize("ex", "key", msg, null));
    }

    // --- Helper methods ---

    private void assertHeaderValue(WsMessageSerializer.SerializedMessage result,
                                   String headerName, String expectedValue) {
        for (Header h : result.headers()) {
            if (h.key().equals(headerName)) {
                assertEquals(expectedValue, new String(h.value(), StandardCharsets.UTF_8));
                return;
            }
        }
        fail("Header not found: " + headerName);
    }

    private void assertHeaderPresent(WsMessageSerializer.SerializedMessage result, String headerName) {
        assertTrue(result.headers().stream().anyMatch(h -> h.key().equals(headerName)),
            "Header should be present: " + headerName);
    }

    private void assertHeaderAbsent(WsMessageSerializer.SerializedMessage result, String headerName) {
        assertFalse(result.headers().stream().anyMatch(h -> h.key().equals(headerName)),
            "Header should be absent: " + headerName);
    }

    private String getHeaderValue(WsMessageSerializer.SerializedMessage result, String headerName) {
        for (Header h : result.headers()) {
            if (h.key().equals(headerName)) {
                return new String(h.value(), StandardCharsets.UTF_8);
            }
        }
        return null;
    }
}
