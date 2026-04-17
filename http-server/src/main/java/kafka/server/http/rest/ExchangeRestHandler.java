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
import kafka.server.http.routing.BindingManager;
import kafka.server.http.routing.ExchangeException;
import kafka.server.http.routing.ExchangeManager;
import kafka.server.http.ws.ExchangeMetadata;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

/**
 * REST handlers for /v1/exchanges endpoints. Delegates persistence and domain
 * validation to {@link ExchangeManager} and uses {@link BindingManager} only to
 * read the binding count for {@code GET} responses.
 *
 * <p>All responses are {@code application/json}. Errors are surfaced as JSON
 * bodies with an {@code errorCode} and {@code errorMessage} field, matching the
 * pattern used by the existing HTTP handlers.
 *
 * // Time: Created - TASK-WS2.06
 */
public final class ExchangeRestHandler {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final ExchangeManager exchangeManager;
    private final BindingManager bindingManager;

    public ExchangeRestHandler(ExchangeManager exchangeManager, BindingManager bindingManager) {
        this.exchangeManager = Objects.requireNonNull(exchangeManager, "exchangeManager");
        // bindingManager is optional — if null, bindingCount will be reported as 0.
        this.bindingManager = bindingManager;
    }

    /**
     * PUT /v1/exchanges/{name} — declare or re-declare an exchange.
     * Body: {"type": "direct"|"topic"|"fanout"|"headers", "durable": bool,
     *        "autoDelete": bool, "internal": bool, "arguments": {...}}.
     */
    public FullHttpResponse handleDeclare(String exchangeName, String vhost, FullHttpRequest request) {
        Objects.requireNonNull(exchangeName, "exchangeName");
        Objects.requireNonNull(vhost, "vhost");
        Objects.requireNonNull(request, "request");

        JsonNode body;
        try {
            body = readBody(request);
        } catch (Exception e) {
            return errorResponse(HttpResponseStatus.BAD_REQUEST, "Malformed JSON body: " + e.getMessage());
        }

        String type = stringField(body, "type", "direct");
        boolean durable = boolField(body, "durable", true);
        boolean autoDelete = boolField(body, "autoDelete", false);
        boolean internal = boolField(body, "internal", false);
        boolean passive = boolField(body, "passive", false);
        Map<String, String> args = readArguments(body);

        boolean existedBefore = exchangeManager.getExchange(vhost, exchangeName) != null;

        try {
            ExchangeMetadata created = exchangeManager.declareExchange(
                vhost, exchangeName, type, durable, autoDelete, passive, internal, args);
            HttpResponseStatus status = existedBefore ? HttpResponseStatus.OK : HttpResponseStatus.CREATED;
            return jsonResponse(status, toExchangeJson(created));
        } catch (ExchangeException e) {
            return errorResponseForException(e);
        }
    }

    /** GET /v1/exchanges/{name}. */
    public FullHttpResponse handleGet(String exchangeName, String vhost) {
        Objects.requireNonNull(exchangeName, "exchangeName");
        Objects.requireNonNull(vhost, "vhost");

        ExchangeMetadata exchange = exchangeManager.getExchange(vhost, exchangeName);
        if (exchange == null) {
            return errorResponse(HttpResponseStatus.NOT_FOUND,
                "Exchange '" + exchangeName + "' does not exist in vhost '" + vhost + "'");
        }
        return jsonResponse(HttpResponseStatus.OK, toExchangeJson(exchange));
    }

    /** GET /v1/exchanges. */
    public FullHttpResponse handleList(String vhost) {
        Objects.requireNonNull(vhost, "vhost");
        Collection<ExchangeMetadata> all = exchangeManager.listExchanges(vhost);
        ObjectNode root = MAPPER.createObjectNode();
        ArrayNode arr = root.putArray("exchanges");
        for (ExchangeMetadata ex : all) {
            arr.add(toExchangeJson(ex));
        }
        return jsonResponse(HttpResponseStatus.OK, root);
    }

    /** DELETE /v1/exchanges/{name}?ifUnused=bool. */
    public FullHttpResponse handleDelete(String exchangeName, String vhost, boolean ifUnused) {
        Objects.requireNonNull(exchangeName, "exchangeName");
        Objects.requireNonNull(vhost, "vhost");

        try {
            exchangeManager.deleteExchange(vhost, exchangeName, ifUnused);
            // Cascade: remove any in-memory bindings tracked by the BindingManager.
            if (bindingManager != null) {
                bindingManager.removeAllForExchange(exchangeName);
            }
            return emptyResponse(HttpResponseStatus.NO_CONTENT);
        } catch (ExchangeException e) {
            return errorResponseForException(e);
        }
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    private ObjectNode toExchangeJson(ExchangeMetadata ex) {
        ObjectNode node = MAPPER.createObjectNode();
        node.put("name", ex.name());
        node.put("vhost", ex.vhost());
        node.put("type", ex.type());
        node.put("durable", ex.durable());
        node.put("autoDelete", ex.autoDelete());
        node.put("internal", ex.internal());
        ObjectNode argsNode = node.putObject("arguments");
        for (Map.Entry<String, String> e : ex.arguments().entrySet()) {
            argsNode.put(e.getKey(), e.getValue());
        }
        int bindingCount = bindingManager == null ? 0 : bindingManager.bindingCount(ex.name());
        node.put("bindingCount", bindingCount);
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

    private static boolean boolField(JsonNode node, String field, boolean dflt) {
        if (node == null || !node.hasNonNull(field)) return dflt;
        return node.get(field).asBoolean(dflt);
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

    private FullHttpResponse errorResponseForException(ExchangeException e) {
        HttpResponseStatus status;
        switch (e.errorCode()) {
            case EXCHANGE_NOT_FOUND:
                status = HttpResponseStatus.NOT_FOUND;
                break;
            case EXCHANGE_TYPE_MISMATCH:
            case EXCHANGE_IN_USE:
                status = HttpResponseStatus.CONFLICT;
                break;
            case EXCHANGE_PROTECTED:
                status = HttpResponseStatus.FORBIDDEN;
                break;
            case EXCHANGE_LIMIT_EXCEEDED:
                status = HttpResponseStatus.TOO_MANY_REQUESTS;
                break;
            default:
                status = HttpResponseStatus.BAD_REQUEST;
        }
        return errorResponse(status, e.getMessage());
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
