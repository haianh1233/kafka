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
// Time: Created - TASK-WS1.10
package kafka.server.http.ws;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import kafka.server.http.HttpRequestTranslator;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link WsMessageDeserializer}.
 *
 * Covers:
 * - Routing metadata reconstruction from _ws_* headers
 * - Cross-protocol fallback when _ws_* headers absent
 * - Body deserialization for JSON / octet-stream / string / null
 * - AMQP property reconstruction from headers
 * - Application header JSON deserialization
 * - Kafka metadata (partition / offset / timestamp) passthrough
 * - Round-trip with {@link WsMessageSerializer}
 *
 * // Time: Created - TASK-WS1.10
 */
class WsMessageDeserializerTest {

    private static final ObjectMapper MAPPER = HttpRequestTranslator.MAPPER;
    private WsMessageDeserializer deserializer;

    @BeforeEach
    void setUp() {
        deserializer = new WsMessageDeserializer();
    }

    // --- Routing metadata reconstruction ---

    @Test
    void deserialize_wsExchangeHeader_usedAsExchange() {
        List<Header> headers = new ArrayList<>();
        headers.add(header(WsMessageSerializer.HDR_EXCHANGE, "events"));
        headers.add(header(WsMessageSerializer.HDR_ROUTING_KEY, "order.created"));

        var result = deserializer.deserialize(
            "order.created".getBytes(StandardCharsets.UTF_8),
            "{}".getBytes(StandardCharsets.UTF_8),
            headers, 0, 42, 1000L);
        assertEquals("events", result.exchange());
        assertEquals("order.created", result.routingKey());
    }

    @Test
    void deserialize_noWsHeaders_synthesizesDefaults() {
        List<Header> headers = new ArrayList<>();

        var result = deserializer.deserialize(
            "my-key".getBytes(StandardCharsets.UTF_8),
            "hello".getBytes(StandardCharsets.UTF_8),
            headers, 0, 42, 1000L);
        assertEquals("", result.exchange());
        assertEquals("my-key", result.routingKey());
    }

    @Test
    void deserialize_noKeyNoHeaders_emptyRoutingKey() {
        List<Header> headers = new ArrayList<>();

        var result = deserializer.deserialize(
            null, "hello".getBytes(StandardCharsets.UTF_8), headers, 0, 42, 1000L);
        assertEquals("", result.routingKey());
    }

    @Test
    void deserialize_nullHeadersIterable_throwsNpe() {
        assertThrows(NullPointerException.class,
            () -> deserializer.deserialize(null, null, null, 0, 0, 0L));
    }

    @Test
    void deserialize_emptyHeadersIterable_ok() {
        var result = deserializer.deserialize(
            null, "hello".getBytes(StandardCharsets.UTF_8),
            Collections.emptyList(), 0, 0, 0L);
        assertEquals("", result.exchange());
        assertEquals("", result.routingKey());
        assertEquals("hello", result.message().get("body").asText());
    }

    // --- Body deserialization ---

    @Test
    void deserialize_jsonContentType_parsesAsJson() {
        List<Header> headers = new ArrayList<>();
        headers.add(header(WsMessageSerializer.HDR_CONTENT_TYPE, "application/json"));

        var result = deserializer.deserialize(
            null, "{\"orderId\":\"123\"}".getBytes(StandardCharsets.UTF_8),
            headers, 0, 42, 1000L);
        assertEquals("123", result.message().get("body").get("orderId").asText());
    }

    @Test
    void deserialize_octetStreamContentType_base64Encodes() {
        List<Header> headers = new ArrayList<>();
        headers.add(header(WsMessageSerializer.HDR_CONTENT_TYPE, "application/octet-stream"));
        byte[] binary = new byte[]{0x01, 0x02, 0x03};

        var result = deserializer.deserialize(null, binary, headers, 0, 42, 1000L);
        String bodyStr = result.message().get("body").asText();
        assertArrayEquals(binary, Base64.getDecoder().decode(bodyStr));
    }

    @Test
    void deserialize_noContentType_treatsAsString() {
        List<Header> headers = new ArrayList<>();

        var result = deserializer.deserialize(
            null, "plain text".getBytes(StandardCharsets.UTF_8),
            headers, 0, 42, 1000L);
        assertEquals("plain text", result.message().get("body").asText());
    }

    @Test
    void deserialize_nullValue_bodyIsNull() {
        List<Header> headers = new ArrayList<>();

        var result = deserializer.deserialize(null, null, headers, 0, 42, 1000L);
        assertTrue(result.message().get("body").isNull());
    }

    @Test
    void deserialize_invalidJson_fallsBackToString() {
        List<Header> headers = new ArrayList<>();
        headers.add(header(WsMessageSerializer.HDR_CONTENT_TYPE, "application/json"));

        var result = deserializer.deserialize(
            null, "not json {".getBytes(StandardCharsets.UTF_8),
            headers, 0, 42, 1000L);
        assertEquals("not json {", result.message().get("body").asText());
    }

    @Test
    void deserialize_utf8StringBody_roundtrip() {
        List<Header> headers = new ArrayList<>();
        String input = "cafe \u00e9\u00e8\u00e0 \u4e2d\u6587";  // UTF-8 multi-byte

        var result = deserializer.deserialize(
            null, input.getBytes(StandardCharsets.UTF_8), headers, 0, 0, 0L);
        assertEquals(input, result.message().get("body").asText());
    }

    // --- AMQP property reconstruction ---

    @Test
    void deserialize_allAmqpProperties_reconstructed() {
        List<Header> headers = new ArrayList<>();
        headers.add(header(WsMessageSerializer.HDR_EXCHANGE, "events"));
        headers.add(header(WsMessageSerializer.HDR_ROUTING_KEY, "key"));
        headers.add(header(WsMessageSerializer.HDR_CONTENT_TYPE, "text/plain"));
        headers.add(header(WsMessageSerializer.HDR_CONTENT_ENCODING, "utf-8"));
        headers.add(header(WsMessageSerializer.HDR_DELIVERY_MODE, "2"));
        headers.add(header(WsMessageSerializer.HDR_PRIORITY, "5"));
        headers.add(header(WsMessageSerializer.HDR_CORRELATION_ID, "corr-1"));
        headers.add(header(WsMessageSerializer.HDR_REPLY_TO, "reply-q"));
        headers.add(header(WsMessageSerializer.HDR_EXPIRATION, "60000"));
        headers.add(header(WsMessageSerializer.HDR_MESSAGE_ID, "msg-1"));
        headers.add(header(WsMessageSerializer.HDR_TYPE, "OrderCreated"));
        headers.add(header(WsMessageSerializer.HDR_USER_ID, "svc-user"));
        headers.add(header(WsMessageSerializer.HDR_APP_ID, "checkout"));

        var result = deserializer.deserialize(
            null, "test".getBytes(StandardCharsets.UTF_8), headers, 3, 100, 5000L);

        JsonNode msg = result.message();
        assertEquals("text/plain", msg.get("contentType").asText());
        assertEquals("utf-8", msg.get("contentEncoding").asText());
        assertEquals(2, msg.get("deliveryMode").asInt());
        assertEquals(5, msg.get("priority").asInt());
        assertEquals("corr-1", msg.get("correlationId").asText());
        assertEquals("reply-q", msg.get("replyTo").asText());
        assertEquals("60000", msg.get("expiration").asText());
        assertEquals("msg-1", msg.get("messageId").asText());
        assertEquals("OrderCreated", msg.get("type").asText());
        assertEquals("svc-user", msg.get("userId").asText());
        assertEquals("checkout", msg.get("appId").asText());
    }

    @Test
    void deserialize_missingOptionalAmqpProperties_omitted() {
        List<Header> headers = new ArrayList<>();
        headers.add(header(WsMessageSerializer.HDR_EXCHANGE, "events"));

        var result = deserializer.deserialize(
            null, "test".getBytes(StandardCharsets.UTF_8), headers, 0, 0, 0L);

        JsonNode msg = result.message();
        assertFalse(msg.has("contentType"));
        assertFalse(msg.has("correlationId"));
        assertFalse(msg.has("replyTo"));
        assertFalse(msg.has("messageId"));
        assertFalse(msg.has("priority"));
    }

    @Test
    void deserialize_invalidNumericProperty_fallsBackToString() {
        List<Header> headers = new ArrayList<>();
        // deliveryMode should be int but is garbage -- deserializer must not throw
        headers.add(header(WsMessageSerializer.HDR_DELIVERY_MODE, "not-a-number"));

        var result = deserializer.deserialize(
            null, "test".getBytes(StandardCharsets.UTF_8), headers, 0, 0, 0L);

        // Either the field is stored as a string, or it is omitted -- either is acceptable,
        // but we must not throw.
        JsonNode deliveryMode = result.message().get("deliveryMode");
        if (deliveryMode != null) {
            assertEquals("not-a-number", deliveryMode.asText());
        }
    }

    @Test
    void deserialize_applicationHeaders_reconstructed() {
        List<Header> headers = new ArrayList<>();
        headers.add(header(WsMessageSerializer.HDR_HEADERS,
            "{\"source\":\"web\",\"priority\":\"high\"}"));

        var result = deserializer.deserialize(
            null, "test".getBytes(StandardCharsets.UTF_8), headers, 0, 0, 0L);

        JsonNode appHeaders = result.message().get("headers");
        assertNotNull(appHeaders);
        assertEquals("web", appHeaders.get("source").asText());
        assertEquals("high", appHeaders.get("priority").asText());
    }

    @Test
    void deserialize_invalidApplicationHeaders_emptyOrAbsent() {
        List<Header> headers = new ArrayList<>();
        headers.add(header(WsMessageSerializer.HDR_HEADERS, "not valid json {"));

        var result = deserializer.deserialize(
            null, "test".getBytes(StandardCharsets.UTF_8), headers, 0, 0, 0L);

        JsonNode appHeaders = result.message().get("headers");
        assertTrue(appHeaders == null || appHeaders.isEmpty());
    }

    // --- Non-ws headers ignored ---

    @Test
    void deserialize_nonWsHeaderIgnored_notPromotedToMessage() {
        List<Header> headers = new ArrayList<>();
        headers.add(header("unrelated-header", "some value"));
        headers.add(header("x-custom", "whatever"));

        var result = deserializer.deserialize(
            null, "test".getBytes(StandardCharsets.UTF_8), headers, 0, 0, 0L);

        JsonNode msg = result.message();
        // Only the body should be in the message -- no unrelated properties promoted
        assertFalse(msg.has("unrelated-header"));
        assertFalse(msg.has("x-custom"));
    }

    // --- Kafka metadata passthrough ---

    @Test
    void deserialize_kafkaMetadata_preserved() {
        List<Header> headers = new ArrayList<>();

        var result = deserializer.deserialize(
            null, "test".getBytes(StandardCharsets.UTF_8), headers, 5, 999, 1713260400000L);
        assertEquals(5, result.partition());
        assertEquals(999, result.offset());
        assertEquals(1713260400000L, result.kafkaTimestamp());
    }

    // --- Round-trip with WsMessageSerializer (WS1.09) ---

    @Test
    void roundTrip_serializeThenDeserialize_lossless() {
        var serializer = new WsMessageSerializer();

        ObjectNode msgNode = MAPPER.createObjectNode();
        msgNode.putObject("body").put("orderId", "123");
        msgNode.put("contentType", "application/json");
        msgNode.put("correlationId", "corr-abc");
        msgNode.put("messageId", "msg-xyz");
        msgNode.put("deliveryMode", 2);
        msgNode.putObject("headers").put("source", "web");

        var serialized = serializer.serialize("events", "order.created", msgNode, "/prod");

        var deserialized = deserializer.deserialize(
            serialized.key(), serialized.value(), serialized.headers(), 0, 100, 5000L);

        assertEquals("events", deserialized.exchange());
        assertEquals("order.created", deserialized.routingKey());
        assertEquals("corr-abc", deserialized.message().get("correlationId").asText());
        assertEquals("msg-xyz", deserialized.message().get("messageId").asText());
        assertEquals(2, deserialized.message().get("deliveryMode").asInt());
        assertEquals("application/json", deserialized.message().get("contentType").asText());
        assertEquals("123", deserialized.message().get("body").get("orderId").asText());
        assertEquals("web", deserialized.message().get("headers").get("source").asText());
    }

    @Test
    void roundTrip_stringBody_lossless() {
        var serializer = new WsMessageSerializer();

        ObjectNode msgNode = MAPPER.createObjectNode();
        msgNode.put("body", "plain text message");

        var serialized = serializer.serialize("", "key-1", msgNode, "/");

        var deserialized = deserializer.deserialize(
            serialized.key(), serialized.value(), serialized.headers(), 0, 0, 0L);

        assertEquals("", deserialized.exchange());
        assertEquals("key-1", deserialized.routingKey());
        assertEquals("plain text message", deserialized.message().get("body").asText());
    }

    @Test
    void roundTrip_binaryBody_lossless() {
        var serializer = new WsMessageSerializer();

        byte[] original = new byte[]{0x01, 0x02, 0x03, (byte) 0xFF};
        ObjectNode msgNode = MAPPER.createObjectNode();
        msgNode.put("body", Base64.getEncoder().encodeToString(original));
        msgNode.put("contentType", "application/octet-stream");

        var serialized = serializer.serialize("ex", "k", msgNode, "/");

        var deserialized = deserializer.deserialize(
            serialized.key(), serialized.value(), serialized.headers(), 0, 0, 0L);

        String decodedBody = deserialized.message().get("body").asText();
        assertArrayEquals(original, Base64.getDecoder().decode(decodedBody));
    }

    // --- Helper ---

    private static Header header(String name, String value) {
        return new RecordHeader(name, value.getBytes(StandardCharsets.UTF_8));
    }
}
