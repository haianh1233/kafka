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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import kafka.server.http.HttpRequestTranslator;
import org.apache.kafka.common.header.Header;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Converts a Kafka record (key / value / headers / partition / offset / timestamp) into
 * the components of a WebSocket {@code deliver} frame.
 *
 * <p>Reconstructs routing metadata and all 13 AMQP content properties from {@code _ws_*}
 * Kafka record headers (see header mapping in {@link WsMessageSerializer}). For records
 * that were produced by non-WebSocket protocols (no {@code _ws_*} headers), synthesizes
 * reasonable defaults from available Kafka metadata:
 * <ul>
 *     <li>{@code exchange} → {@code ""} (empty string)
 *     <li>{@code routingKey} → record key as UTF-8 string, or {@code ""} if key is null
 *     <li>{@code message.body} → value decoded per {@code _content-type} header
 *     <li>All other AMQP properties → omitted from the message object
 * </ul>
 *
 * <p>This is the inverse of {@link WsMessageSerializer}; together they enable lossless
 * round-trip of messages across WebSocket / HTTP / Kafka binary protocols.
 *
 * <p>Body deserialization:
 * <ul>
 *     <li>{@code application/json} → parsed as JSON (falls back to UTF-8 string on parse error)
 *     <li>{@code application/octet-stream} → Base64-encoded string
 *     <li>null value → JSON {@code null}
 *     <li>otherwise → UTF-8 string
 * </ul>
 *
 * <p>Thread-safe: stateless, uses the shared {@link HttpRequestTranslator#MAPPER}.
 */
public final class WsMessageDeserializer {

    // --- Header name constants (must match WsMessageSerializer) ---
    static final String HDR_EXCHANGE = WsMessageSerializer.HDR_EXCHANGE;
    static final String HDR_ROUTING_KEY = WsMessageSerializer.HDR_ROUTING_KEY;
    static final String HDR_CONTENT_TYPE = WsMessageSerializer.HDR_CONTENT_TYPE;
    static final String HDR_HEADERS = WsMessageSerializer.HDR_HEADERS;
    static final String HDR_DELIVERY_MODE = WsMessageSerializer.HDR_DELIVERY_MODE;
    static final String HDR_CORRELATION_ID = WsMessageSerializer.HDR_CORRELATION_ID;
    static final String HDR_REPLY_TO = WsMessageSerializer.HDR_REPLY_TO;
    static final String HDR_MESSAGE_ID = WsMessageSerializer.HDR_MESSAGE_ID;
    static final String HDR_CONTENT_ENCODING = WsMessageSerializer.HDR_CONTENT_ENCODING;
    static final String HDR_PRIORITY = WsMessageSerializer.HDR_PRIORITY;
    static final String HDR_TYPE = WsMessageSerializer.HDR_TYPE;
    static final String HDR_USER_ID = WsMessageSerializer.HDR_USER_ID;
    static final String HDR_APP_ID = WsMessageSerializer.HDR_APP_ID;
    static final String HDR_EXPIRATION = WsMessageSerializer.HDR_EXPIRATION;

    // --- Content types ---
    private static final String CT_JSON = "application/json";
    private static final String CT_OCTET_STREAM = "application/octet-stream";

    // --- JSON field names on the reconstructed message object ---
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

    /**
     * Reconstructed deliver-frame components.
     *
     * @param exchange        exchange name from {@code _ws_exchange}, or {@code ""} if absent
     * @param routingKey      routing key from {@code _ws_routing_key}, fallback to record key as
     *                        UTF-8 string, or {@code ""} if both absent
     * @param message         reconstructed message object with body and AMQP properties
     * @param partition       Kafka partition
     * @param offset          Kafka record offset
     * @param kafkaTimestamp  Kafka record timestamp (milliseconds)
     */
    public record DeliverFrame(
        String exchange,
        String routingKey,
        JsonNode message,
        int partition,
        long offset,
        long kafkaTimestamp
    ) { }

    /**
     * Deserializes a Kafka record into deliver-frame components.
     *
     * <p>This method never throws on malformed payload / header data — a delivery
     * failure would block the consumer. Invalid JSON bodies fall back to string
     * representation; invalid {@code _ws_headers} JSON yields an empty headers map.
     *
     * @param key        record key bytes (may be null)
     * @param value      record value bytes (may be null)
     * @param headers    Kafka record headers (non-null; may be empty)
     * @param partition  partition number
     * @param offset     record offset
     * @param timestamp  Kafka record timestamp
     * @return DeliverFrame with all reconstructed fields
     * @throws NullPointerException if {@code headers} is null
     */
    public DeliverFrame deserialize(byte[] key, byte[] value,
                                    Iterable<Header> headers,
                                    int partition, long offset, long timestamp) {
        Objects.requireNonNull(headers, "headers");

        // 1. Extract _ws_* header values into a lookup map (single pass).
        Map<String, String> hdrMap = buildHeaderMap(headers);

        // 2. Resolve exchange (from _ws_exchange header or "").
        String exchange = hdrMap.getOrDefault(HDR_EXCHANGE, "");

        // 3. Resolve routingKey: _ws_routing_key header, then record key, then "".
        String routingKey = hdrMap.get(HDR_ROUTING_KEY);
        if (routingKey == null) {
            routingKey = (key != null) ? new String(key, StandardCharsets.UTF_8) : "";
        }

        // 4. Resolve contentType (may be null if absent).
        String contentType = hdrMap.get(HDR_CONTENT_TYPE);

        // 5. Build the message object: body + all present AMQP properties.
        ObjectNode message = MAPPER.createObjectNode();
        message.set(FIELD_BODY, deserializeBody(value, contentType));

        // contentType field mirrors the _content-type header when present.
        addIfPresent(message, FIELD_CONTENT_TYPE, contentType);
        addIfPresent(message, FIELD_CONTENT_ENCODING, hdrMap.get(HDR_CONTENT_ENCODING));
        addIntIfPresent(message, FIELD_DELIVERY_MODE, hdrMap.get(HDR_DELIVERY_MODE));
        addIntIfPresent(message, FIELD_PRIORITY, hdrMap.get(HDR_PRIORITY));
        addIfPresent(message, FIELD_CORRELATION_ID, hdrMap.get(HDR_CORRELATION_ID));
        addIfPresent(message, FIELD_REPLY_TO, hdrMap.get(HDR_REPLY_TO));
        addIfPresent(message, FIELD_EXPIRATION, hdrMap.get(HDR_EXPIRATION));
        addIfPresent(message, FIELD_MESSAGE_ID, hdrMap.get(HDR_MESSAGE_ID));
        addIfPresent(message, FIELD_TYPE, hdrMap.get(HDR_TYPE));
        addIfPresent(message, FIELD_USER_ID, hdrMap.get(HDR_USER_ID));
        addIfPresent(message, FIELD_APP_ID, hdrMap.get(HDR_APP_ID));

        // Reconstruct application headers from JSON blob in _ws_headers.
        String headersJson = hdrMap.get(HDR_HEADERS);
        if (headersJson != null) {
            JsonNode parsedHeaders = tryParseJsonObject(headersJson);
            if (parsedHeaders != null) {
                message.set(FIELD_HEADERS, parsedHeaders);
            }
        }

        return new DeliverFrame(exchange, routingKey, message, partition, offset, timestamp);
    }

    // ---- internal helpers ----

    /**
     * Walks the headers iterable once, collecting every known {@code _ws_*} (and
     * {@code _content-type}) value into a map keyed by header name. Non-ws headers
     * and duplicates (last-write-wins) are ignored for the message construction.
     */
    private Map<String, String> buildHeaderMap(Iterable<Header> headers) {
        Map<String, String> map = new HashMap<>(16);
        for (Header h : headers) {
            String name = h.key();
            if (!isWsHeader(name)) {
                continue;
            }
            byte[] val = h.value();
            map.put(name, val == null ? null : new String(val, StandardCharsets.UTF_8));
        }
        return map;
    }

    /**
     * Returns true if the header name is one the deserializer consumes
     * (either the {@code _ws_} prefix or the special {@code _content-type}).
     */
    private static boolean isWsHeader(String name) {
        return name != null && (name.startsWith("_ws_") || HDR_CONTENT_TYPE.equals(name));
    }

    /**
     * Deserializes the record value bytes into a JSON body node.
     *
     * <ul>
     *     <li>null value → {@link NullNode#getInstance()}
     *     <li>{@code application/json} → {@code MAPPER.readTree(value)}; falls back to
     *         UTF-8 string on parse failure (delivery must never fail).
     *     <li>{@code application/octet-stream} → Base64-encoded string
     *     <li>null / other content type → UTF-8 string
     * </ul>
     */
    private JsonNode deserializeBody(byte[] value, String contentType) {
        if (value == null) {
            return NullNode.getInstance();
        }

        if (CT_JSON.equalsIgnoreCase(contentType)) {
            try {
                return MAPPER.readTree(value);
            } catch (Exception e) {
                // Corrupt / non-JSON payload: degrade to UTF-8 string.
                return MAPPER.getNodeFactory()
                    .textNode(new String(value, StandardCharsets.UTF_8));
            }
        }

        if (CT_OCTET_STREAM.equalsIgnoreCase(contentType)) {
            return MAPPER.getNodeFactory()
                .textNode(Base64.getEncoder().encodeToString(value));
        }

        // Default: treat as UTF-8 text.
        return MAPPER.getNodeFactory()
            .textNode(new String(value, StandardCharsets.UTF_8));
    }

    /**
     * Parses a JSON string to an {@link ObjectNode}. Returns {@code null} if the value
     * is not a JSON object (the deliver frame only populates {@code headers} when the
     * reconstructed value is an object).
     */
    private JsonNode tryParseJsonObject(String json) {
        try {
            JsonNode node = MAPPER.readTree(json);
            return node.isObject() ? node : null;
        } catch (JsonProcessingException e) {
            return null;
        }
    }

    /**
     * Adds a string field to the message node when the value is non-null.
     */
    private void addIfPresent(ObjectNode message, String jsonField, String headerValue) {
        if (headerValue != null) {
            message.put(jsonField, headerValue);
        }
    }

    /**
     * Adds an integer field when possible, otherwise falls back to storing the raw
     * string value (so malformed numeric headers do not silently disappear).
     */
    private void addIntIfPresent(ObjectNode message, String jsonField, String headerValue) {
        if (headerValue == null) {
            return;
        }
        try {
            message.put(jsonField, Integer.parseInt(headerValue));
        } catch (NumberFormatException e) {
            message.put(jsonField, headerValue);
        }
    }
}
