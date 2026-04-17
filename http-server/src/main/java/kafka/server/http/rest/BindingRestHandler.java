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
// Time: Created - TASK-WS2.06
package kafka.server.http.rest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import kafka.server.http.routing.Binding;
import kafka.server.http.routing.BindingManager;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * REST handlers for /v1/bindings endpoints.
 *
 * <p>Only queue-to-exchange bindings are surfaced in the REST API for phase 2;
 * exchange-to-exchange bindings still require the WebSocket control frame.
 *
 * // Time: Created - TASK-WS2.06
 */
public final class BindingRestHandler {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final BindingManager bindingManager;

    public BindingRestHandler(BindingManager bindingManager) {
        this.bindingManager = Objects.requireNonNull(bindingManager, "bindingManager");
    }

    /**
     * POST /v1/bindings — create binding.
     * Body: {"exchange": "ex", "queue": "q", "routingKey": "rk", "arguments": {...}}.
     */
    public FullHttpResponse handleCreate(String vhost, FullHttpRequest request) {
        Objects.requireNonNull(vhost, "vhost");
        Objects.requireNonNull(request, "request");

        JsonNode body;
        try {
            body = readBody(request);
        } catch (Exception e) {
            return errorResponse(HttpResponseStatus.BAD_REQUEST, "Malformed JSON body: " + e.getMessage());
        }

        String exchange = stringField(body, "exchange", null);
        String queue = stringField(body, "queue", null);
        String routingKey = stringField(body, "routingKey", "");
        if (exchange == null || queue == null) {
            return errorResponse(HttpResponseStatus.BAD_REQUEST,
                "Binding requires 'exchange' and 'queue' fields");
        }
        Map<String, String> args = readArguments(body);

        try {
            bindingManager.bind(exchange, queue, routingKey, args);
        } catch (IllegalArgumentException e) {
            return errorResponse(HttpResponseStatus.NOT_FOUND, e.getMessage());
        } catch (IllegalStateException e) {
            return errorResponse(HttpResponseStatus.TOO_MANY_REQUESTS, e.getMessage());
        }

        ObjectNode node = bindingToJson(new Binding(exchange, queue, routingKey, args));
        return jsonResponse(HttpResponseStatus.CREATED, node);
    }

    /** GET /v1/bindings?exchange=X&queue=Y — optional filters. */
    public FullHttpResponse handleList(String vhost, String exchangeFilter, String queueFilter) {
        Objects.requireNonNull(vhost, "vhost");
        List<Binding> matches;
        if (exchangeFilter != null && !exchangeFilter.isEmpty()) {
            matches = new ArrayList<>(bindingManager.listByExchange(exchangeFilter));
            if (queueFilter != null && !queueFilter.isEmpty()) {
                matches.removeIf(b -> !b.queue().equals(queueFilter));
            }
        } else if (queueFilter != null && !queueFilter.isEmpty()) {
            matches = new ArrayList<>(bindingManager.listByQueue(queueFilter));
        } else {
            // No filter supplied — return empty list to avoid full scans by default.
            matches = Collections.emptyList();
        }

        ObjectNode root = MAPPER.createObjectNode();
        ArrayNode arr = root.putArray("bindings");
        for (Binding b : matches) {
            arr.add(bindingToJson(b));
        }
        return jsonResponse(HttpResponseStatus.OK, root);
    }

    /**
     * DELETE /v1/bindings — body identifies the binding to remove:
     * {"exchange": "ex", "queue": "q", "routingKey": "rk"}.
     */
    public FullHttpResponse handleDelete(String vhost, FullHttpRequest request) {
        Objects.requireNonNull(vhost, "vhost");
        Objects.requireNonNull(request, "request");

        JsonNode body;
        try {
            body = readBody(request);
        } catch (Exception e) {
            return errorResponse(HttpResponseStatus.BAD_REQUEST, "Malformed JSON body: " + e.getMessage());
        }

        String exchange = stringField(body, "exchange", null);
        String queue = stringField(body, "queue", null);
        String routingKey = stringField(body, "routingKey", "");
        if (exchange == null || queue == null) {
            return errorResponse(HttpResponseStatus.BAD_REQUEST,
                "Binding delete requires 'exchange' and 'queue' fields");
        }

        bindingManager.unbind(exchange, queue, routingKey);
        return emptyResponse(HttpResponseStatus.NO_CONTENT);
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    private ObjectNode bindingToJson(Binding b) {
        ObjectNode node = MAPPER.createObjectNode();
        node.put("exchange", b.exchange());
        node.put("queue", b.queue());
        node.put("routingKey", b.routingKey());
        ObjectNode argsNode = node.putObject("arguments");
        if (b.arguments() != null) {
            for (Map.Entry<String, String> e : b.arguments().entrySet()) {
                argsNode.put(e.getKey(), e.getValue());
            }
        }
        return node;
    }

    private JsonNode readBody(FullHttpRequest request) throws Exception {
        if (request.content() == null || !request.content().isReadable()) {
            return MAPPER.createObjectNode();
        }
        byte[] bytes = new byte[request.content().readableBytes()];
        request.content().getBytes(request.content().readerIndex(), bytes);
        if (bytes.length == 0) {
            return MAPPER.createObjectNode();
        }
        return MAPPER.readTree(bytes);
    }

    private static String stringField(JsonNode node, String field, String dflt) {
        if (node == null || !node.hasNonNull(field)) return dflt;
        return node.get(field).asText();
    }

    private static Map<String, String> readArguments(JsonNode body) {
        Map<String, String> args = new HashMap<>();
        if (body == null) return args;
        JsonNode argsNode = body.get("arguments");
        if (argsNode == null || !argsNode.isObject()) return args;
        Iterator<String> names = argsNode.fieldNames();
        while (names.hasNext()) {
            String name = names.next();
            args.put(name, argsNode.get(name).asText());
        }
        return args;
    }

    private FullHttpResponse errorResponse(HttpResponseStatus status, String message) {
        ObjectNode node = MAPPER.createObjectNode();
        node.put("errorCode", -1);
        node.put("errorMessage", message);
        return jsonResponse(status, node);
    }

    private FullHttpResponse jsonResponse(HttpResponseStatus status, JsonNode body) {
        byte[] bytes = body.toString().getBytes(StandardCharsets.UTF_8);
        FullHttpResponse response = new DefaultFullHttpResponse(
            HttpVersion.HTTP_1_1, status, Unpooled.wrappedBuffer(bytes));
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json");
        response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, bytes.length);
        return response;
    }

    private FullHttpResponse emptyResponse(HttpResponseStatus status) {
        FullHttpResponse response = new DefaultFullHttpResponse(
            HttpVersion.HTTP_1_1, status, Unpooled.EMPTY_BUFFER);
        response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, 0);
        return response;
    }
}
