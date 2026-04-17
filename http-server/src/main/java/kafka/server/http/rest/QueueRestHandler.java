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
import kafka.server.http.ws.QueueMetadata;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

/**
 * REST handlers for /v1/queues endpoints.
 *
 * <p><b>Placeholder implementation.</b> TASK-WS2.06 ships before the WS1.06
 * {@code QueueManager} exists. To keep this handler self-contained and
 * test-driven, it collaborates with a tiny functional interface
 * ({@link QueueStore}) that callers plug in at wiring time. Once the real
 * {@code QueueManager} lands, its methods can be adapted via a method-reference
 * façade and this class does not need to change.
 *
 * <p>The {@link QueueStore} only exposes the minimum surface that the REST
 * endpoints require (declare, get, list, delete). Details like Kafka-topic
 * creation, quota enforcement, and auto-delete are the adapter's responsibility.
 *
 * // Time: Created - TASK-WS2.06
 */
public final class QueueRestHandler {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** Minimal façade over queue state — to be satisfied by {@code QueueManager}. */
    public interface QueueStore {
        /** Return existing or null. */
        QueueMetadata get(String vhost, String name);

        /** Declare (idempotent when flags match); throw QueueConflict when flags differ. */
        QueueMetadata declare(String vhost, String name, boolean durable, boolean exclusive,
                              boolean autoDelete, Map<String, String> arguments) throws QueueConflict;

        /** List all queues for vhost. */
        Collection<QueueMetadata> list(String vhost);

        /**
         * Delete. If {@code ifUnused=true} and consumers are active, or
         * {@code ifEmpty=true} and there are unconsumed messages, throw.
         */
        void delete(String vhost, String name, boolean ifUnused, boolean ifEmpty) throws QueueConflict;
    }

    /** Operational conflict during declare/delete (409). */
    public static final class QueueConflict extends Exception {
        private static final long serialVersionUID = 1L;

        public QueueConflict(String message) {
            super(message);
        }
    }

    private final QueueStore store;

    public QueueRestHandler(QueueStore store) {
        this.store = Objects.requireNonNull(store, "store");
    }

    /** PUT /v1/queues/{name}. */
    public FullHttpResponse handleDeclare(String queueName, String vhost, FullHttpRequest request) {
        Objects.requireNonNull(queueName, "queueName");
        Objects.requireNonNull(vhost, "vhost");

        JsonNode body;
        try {
            body = readBody(request);
        } catch (Exception e) {
            return errorResponse(HttpResponseStatus.BAD_REQUEST, "Malformed JSON body: " + e.getMessage());
        }

        boolean durable = boolField(body, "durable", true);
        boolean exclusive = boolField(body, "exclusive", false);
        boolean autoDelete = boolField(body, "autoDelete", false);
        Map<String, String> args = readArguments(body);

        boolean existedBefore = store.get(vhost, queueName) != null;
        try {
            QueueMetadata q = store.declare(vhost, queueName, durable, exclusive, autoDelete, args);
            HttpResponseStatus status = existedBefore ? HttpResponseStatus.OK : HttpResponseStatus.CREATED;
            return jsonResponse(status, toQueueJson(q));
        } catch (QueueConflict e) {
            return errorResponse(HttpResponseStatus.CONFLICT, e.getMessage());
        }
    }

    /** GET /v1/queues/{name}. */
    public FullHttpResponse handleGet(String queueName, String vhost) {
        Objects.requireNonNull(queueName, "queueName");
        Objects.requireNonNull(vhost, "vhost");
        QueueMetadata q = store.get(vhost, queueName);
        if (q == null) {
            return errorResponse(HttpResponseStatus.NOT_FOUND,
                "Queue '" + queueName + "' does not exist in vhost '" + vhost + "'");
        }
        ObjectNode body = toQueueJson(q);
        // Placeholder: consumers array always empty until WsSubscriptionManager wired in.
        body.putArray("consumers");
        return jsonResponse(HttpResponseStatus.OK, body);
    }

    /** GET /v1/queues. */
    public FullHttpResponse handleList(String vhost) {
        Objects.requireNonNull(vhost, "vhost");
        ObjectNode root = MAPPER.createObjectNode();
        ArrayNode arr = root.putArray("queues");
        for (QueueMetadata q : store.list(vhost)) {
            arr.add(toQueueJson(q));
        }
        return jsonResponse(HttpResponseStatus.OK, root);
    }

    /** DELETE /v1/queues/{name}?ifUnused=bool&ifEmpty=bool. */
    public FullHttpResponse handleDelete(String queueName, String vhost, boolean ifUnused, boolean ifEmpty) {
        Objects.requireNonNull(queueName, "queueName");
        Objects.requireNonNull(vhost, "vhost");
        try {
            store.delete(vhost, queueName, ifUnused, ifEmpty);
            return emptyResponse(HttpResponseStatus.NO_CONTENT);
        } catch (QueueConflict e) {
            return errorResponse(HttpResponseStatus.CONFLICT, e.getMessage());
        }
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    private ObjectNode toQueueJson(QueueMetadata q) {
        ObjectNode node = MAPPER.createObjectNode();
        node.put("name", q.name());
        node.put("vhost", q.vhost());
        node.put("durable", q.durable());
        node.put("exclusive", q.exclusive());
        node.put("autoDelete", q.autoDelete());
        ObjectNode argsNode = node.putObject("arguments");
        Map<String, String> args = q.arguments() == null ? Collections.emptyMap() : q.arguments();
        for (Map.Entry<String, String> e : args.entrySet()) {
            argsNode.put(e.getKey(), e.getValue());
        }
        return node;
    }

    private JsonNode readBody(FullHttpRequest request) throws Exception {
        if (request == null || request.content() == null || !request.content().isReadable()) {
            return MAPPER.createObjectNode();
        }
        byte[] bytes = new byte[request.content().readableBytes()];
        request.content().getBytes(request.content().readerIndex(), bytes);
        if (bytes.length == 0) {
            return MAPPER.createObjectNode();
        }
        return MAPPER.readTree(bytes);
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
