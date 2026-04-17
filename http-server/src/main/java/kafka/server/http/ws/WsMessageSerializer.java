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
// Time: Update - TASK-WS4.04 - added poison protection
package kafka.server.http.ws;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.server.http.HttpRequestTranslator;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Objects;

/**
 * Converts a WebSocket publish frame into Kafka record components (key, value, headers).
 *
 * <p>Maps all 13 AMQP content properties to Kafka record headers with the {@code _ws_} prefix.
 * The routing key becomes the record key (used for murmur2 partition assignment).
 * The message body becomes the record value — JSON, UTF-8 string, or Base64-decoded binary
 * bytes depending on {@code contentType}.
 *
 * <p>Thread-safe: stateless, uses the shared {@link HttpRequestTranslator#MAPPER}.
 *
 * <p>Header mapping (see design doc §13.3):
 * <ul>
 *     <li>{@code exchange} → {@link #HDR_EXCHANGE}
 *     <li>{@code routingKey} → {@link #HDR_ROUTING_KEY}
 *     <li>{@code vhost} → {@link #HDR_VHOST}
 *     <li>{@code message.contentType} → {@link #HDR_CONTENT_TYPE} (note: NO {@code _ws_} prefix)
 *     <li>{@code message.headers} → {@link #HDR_HEADERS} (serialized as JSON string)
 *     <li>{@code message.deliveryMode} → {@link #HDR_DELIVERY_MODE}
 *     <li>{@code message.correlationId} → {@link #HDR_CORRELATION_ID}
 *     <li>{@code message.replyTo} → {@link #HDR_REPLY_TO}
 *     <li>{@code message.messageId} → {@link #HDR_MESSAGE_ID}
 *     <li>{@code message.contentEncoding} → {@link #HDR_CONTENT_ENCODING}
 *     <li>{@code message.priority} → {@link #HDR_PRIORITY}
 *     <li>{@code message.type} → {@link #HDR_TYPE}
 *     <li>{@code message.userId} → {@link #HDR_USER_ID}
 *     <li>{@code message.appId} → {@link #HDR_APP_ID}
 *     <li>{@code message.expiration} → {@link #HDR_EXPIRATION}
 * </ul>
 *
 * <p>Only non-null properties are added as headers; null or missing properties are omitted.
 */
public final class WsMessageSerializer {

    // --- Header name constants ---
    public static final String HDR_EXCHANGE = "_ws_exchange";
    public static final String HDR_ROUTING_KEY = "_ws_routing_key";
    /** Note: intentionally NOT prefixed with {@code _ws_} — matches the HTTP layer's value-type header for cross-protocol compatibility. */
    public static final String HDR_CONTENT_TYPE = "_content-type";
    public static final String HDR_HEADERS = "_ws_headers";
    public static final String HDR_DELIVERY_MODE = "_ws_delivery_mode";
    public static final String HDR_CORRELATION_ID = "_ws_correlation_id";
    public static final String HDR_REPLY_TO = "_ws_reply_to";
    public static final String HDR_MESSAGE_ID = "_ws_message_id";
    public static final String HDR_CONTENT_ENCODING = "_ws_content_encoding";
    public static final String HDR_PRIORITY = "_ws_priority";
    public static final String HDR_TYPE = "_ws_type";
    public static final String HDR_USER_ID = "_ws_user_id";
    public static final String HDR_APP_ID = "_ws_app_id";
    public static final String HDR_EXPIRATION = "_ws_expiration";
    public static final String HDR_VHOST = "_ws_vhost";
    /**
     * Kafka-header name that carries the per-message redelivery count used by
     * {@link WsPoisonMessageProtection}. Incremented before each redelivery; first
     * delivery carries count {@code 1}. Not written by the publish path — only the
     * redelivery / dead-letter path produces this header. See design doc §12.6.
     */
    public static final String HDR_DELIVERY_COUNT = "_ws_delivery_count";

    // --- Content types ---
    private static final String CT_JSON = "application/json";
    private static final String CT_OCTET_STREAM = "application/octet-stream";

    // --- Message property field names ---
    private static final String FIELD_BODY = "body";
    private static final String FIELD_CONTENT_TYPE = "contentType";
    private static final String FIELD_HEADERS = "headers";
    private static final String FIELD_DELIVERY_MODE = "deliveryMode";
    private static final String FIELD_CORRELATION_ID = "correlationId";
    private static final String FIELD_REPLY_TO = "replyTo";
    private static final String FIELD_MESSAGE_ID = "messageId";
    private static final String FIELD_CONTENT_ENCODING = "contentEncoding";
    private static final String FIELD_PRIORITY = "priority";
    private static final String FIELD_TYPE = "type";
    private static final String FIELD_USER_ID = "userId";
    private static final String FIELD_APP_ID = "appId";
    private static final String FIELD_EXPIRATION = "expiration";

    private static final ObjectMapper MAPPER = HttpRequestTranslator.MAPPER;

    private static final byte[] EMPTY_BYTES = new byte[0];

    /**
     * Result of serializing a publish frame.
     *
     * @param key      record key bytes (null for empty routing key → round-robin partitioning)
     * @param value    record value bytes (payload serialized per contentType)
     * @param headers  immutable list of Kafka headers (routing metadata + {@code _ws_*} properties)
     */
    public record SerializedMessage(byte[] key, byte[] value, List<Header> headers) { }

    /**
     * Serializes a publish frame's message content into Kafka record components.
     *
     * @param exchange    target exchange name (non-null)
     * @param routingKey  message routing key (non-null; empty string maps to null key)
     * @param message     the "message" JSON object from the publish frame (non-null)
     * @param vhost       virtual host (non-null)
     * @return SerializedMessage with key, value, and headers
     * @throws NullPointerException  if any argument is null
     * @throws IllegalArgumentException if the body is not base64 when contentType is application/octet-stream,
     *         or if the JSON body cannot be serialized
     */
    public SerializedMessage serialize(String exchange, String routingKey,
                                       JsonNode message, String vhost) {
        Objects.requireNonNull(exchange, "exchange");
        Objects.requireNonNull(routingKey, "routingKey");
        Objects.requireNonNull(message, "message");
        Objects.requireNonNull(vhost, "vhost");

        // 1. Compute key bytes: empty routingKey → null (triggers round-robin partition assignment).
        byte[] keyBytes = routingKey.isEmpty()
            ? null
            : routingKey.getBytes(StandardCharsets.UTF_8);

        // 2. Extract contentType (may be null / missing).
        String contentType = textOrNull(message, FIELD_CONTENT_TYPE);

        // 3. Serialize body bytes based on contentType and body node shape.
        JsonNode bodyNode = message.get(FIELD_BODY);
        byte[] valueBytes = serializeBody(bodyNode, contentType);

        // 4. Build headers list.
        List<Header> headers = new ArrayList<>(16);
        addHeader(headers, HDR_EXCHANGE, exchange);
        addHeader(headers, HDR_ROUTING_KEY, routingKey);
        addHeader(headers, HDR_VHOST, vhost);

        // contentType uses special _content-type header name (no _ws_ prefix).
        if (contentType != null) {
            addHeader(headers, HDR_CONTENT_TYPE, contentType);
        }

        // message.headers → serialized as JSON string under _ws_headers.
        JsonNode appHeaders = message.get(FIELD_HEADERS);
        if (appHeaders != null && !appHeaders.isNull()) {
            try {
                addHeader(headers, HDR_HEADERS, MAPPER.writeValueAsString(appHeaders));
            } catch (JsonProcessingException e) {
                throw new IllegalArgumentException("Failed to serialize message.headers to JSON", e);
            }
        }

        // All remaining AMQP content properties (only when present + non-null).
        addHeaderIfPresent(headers, HDR_DELIVERY_MODE, message, FIELD_DELIVERY_MODE);
        addHeaderIfPresent(headers, HDR_CORRELATION_ID, message, FIELD_CORRELATION_ID);
        addHeaderIfPresent(headers, HDR_REPLY_TO, message, FIELD_REPLY_TO);
        addHeaderIfPresent(headers, HDR_MESSAGE_ID, message, FIELD_MESSAGE_ID);
        addHeaderIfPresent(headers, HDR_CONTENT_ENCODING, message, FIELD_CONTENT_ENCODING);
        addHeaderIfPresent(headers, HDR_PRIORITY, message, FIELD_PRIORITY);
        addHeaderIfPresent(headers, HDR_TYPE, message, FIELD_TYPE);
        addHeaderIfPresent(headers, HDR_USER_ID, message, FIELD_USER_ID);
        addHeaderIfPresent(headers, HDR_APP_ID, message, FIELD_APP_ID);
        addHeaderIfPresent(headers, HDR_EXPIRATION, message, FIELD_EXPIRATION);

        return new SerializedMessage(keyBytes, valueBytes, List.copyOf(headers));
    }

    // ---- internal helpers ----

    /**
     * Serializes the message body to bytes based on content type.
     *
     * <ul>
     *     <li>{@code null} / missing body → empty byte array.</li>
     *     <li>{@code application/json} OR body is JSON object/array → {@code MAPPER.writeValueAsBytes(body)}.</li>
     *     <li>{@code application/octet-stream} → Base64-decode the body string.</li>
     *     <li>Otherwise (string) → {@code body.asText().getBytes(UTF_8)}.</li>
     * </ul>
     */
    private byte[] serializeBody(JsonNode body, String contentType) {
        if (body == null || body.isNull()) {
            return EMPTY_BYTES;
        }

        boolean isStructured = body.isObject() || body.isArray();
        if (CT_JSON.equalsIgnoreCase(contentType) || isStructured) {
            try {
                return MAPPER.writeValueAsBytes(body);
            } catch (JsonProcessingException e) {
                throw new IllegalArgumentException("Failed to serialize message body to JSON", e);
            }
        }

        if (CT_OCTET_STREAM.equalsIgnoreCase(contentType)) {
            // Body must be a base64-encoded string for binary payloads.
            String encoded = body.asText();
            try {
                return Base64.getDecoder().decode(encoded);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(
                    "Invalid base64 body for application/octet-stream: " + e.getMessage(), e);
            }
        }

        // Default: treat as UTF-8 text.
        return body.asText().getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Adds a header for {@code field} in {@code messageNode} if the field exists and is non-null.
     * The value is rendered via {@link JsonNode#asText()} and encoded as UTF-8 bytes.
     */
    private void addHeaderIfPresent(List<Header> headers, String name, JsonNode messageNode, String field) {
        JsonNode node = messageNode.get(field);
        if (node == null || node.isNull()) {
            return;
        }
        addHeader(headers, name, node.asText());
    }

    /**
     * Adds a string header (UTF-8 encoded) to the list.
     */
    private void addHeader(List<Header> headers, String name, String value) {
        headers.add(new RecordHeader(name, value.getBytes(StandardCharsets.UTF_8)));
    }

    /**
     * Returns the text value for {@code field} or null if absent or JSON null.
     */
    private static String textOrNull(JsonNode node, String field) {
        JsonNode child = node.get(field);
        return (child == null || child.isNull()) ? null : child.asText();
    }
}
