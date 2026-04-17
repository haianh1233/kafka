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
// Time: Created - TASK-WS2.07
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
import kafka.server.http.ws.WsConnectionContext;
import kafka.server.http.ws.WsConnectionRegistry;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * REST handlers for /v1/connections endpoints — list, get, force-close.
 *
 * <p>Works purely against the {@link WsConnectionRegistry} snapshot: list and
 * get read the registry; force-close looks up the {@link WsConnectionContext}
 * and invokes {@link WsConnectionContext#close(int, String)} with WebSocket
 * close code {@code 1001 (Going Away)} and the admin-supplied reason. The
 * registry entry is removed on success so the admin tool does not need to
 * poll for eventual cleanup.
 *
 * // Time: Created - TASK-WS2.07
 */
public final class ConnectionRestHandler {

    /** WS close code 1001: server endpoint going away / administrative close. */
    private static final int WS_CLOSE_CODE_GOING_AWAY = 1001;

    private static final String DEFAULT_CLOSE_REASON = "Administrative close";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final WsConnectionRegistry registry;
    private final int brokerId;

    public ConnectionRestHandler(WsConnectionRegistry registry, int brokerId) {
        this.registry = Objects.requireNonNull(registry, "registry");
        this.brokerId = brokerId;
    }

    /** GET /v1/connections — list all active WebSocket connections. */
    public FullHttpResponse handleList() {
        ObjectNode root = MAPPER.createObjectNode();
        ArrayNode arr = root.putArray("connections");
        for (WsConnectionContext ctx : registry.contexts()) {
            arr.add(toSummaryJson(ctx));
        }
        return jsonResponse(HttpResponseStatus.OK, root);
    }

    /** GET /v1/connections/{connectionId} — detailed view of one connection. */
    public FullHttpResponse handleGet(String connectionId) {
        Objects.requireNonNull(connectionId, "connectionId");
        WsConnectionContext ctx = registry.get(connectionId);
        if (ctx == null) {
            return errorResponse(HttpResponseStatus.NOT_FOUND,
                "Connection '" + connectionId + "' does not exist");
        }
        return jsonResponse(HttpResponseStatus.OK, toDetailJson(connectionId, ctx));
    }

    /**
     * DELETE /v1/connections/{connectionId} — force-close.
     *
     * <p>Body: {@code {"reason":"Administrative close"}} — the reason text is
     * included in the WebSocket close frame. If the body is empty or malformed,
     * a default reason is used.
     *
     * <p>Sends {@code CloseWebSocketFrame(1001, reason)} to the channel and
     * removes the connection from the registry. Subscription cleanup / offset
     * commits are handled by the WS close pipeline (outside this handler).
     */
    public FullHttpResponse handleForceClose(String connectionId, FullHttpRequest request) {
        Objects.requireNonNull(connectionId, "connectionId");
        Objects.requireNonNull(request, "request");

        WsConnectionContext ctx = registry.get(connectionId);
        if (ctx == null) {
            return errorResponse(HttpResponseStatus.NOT_FOUND,
                "Connection '" + connectionId + "' does not exist");
        }

        String reason = DEFAULT_CLOSE_REASON;
        try {
            JsonNode body = readBody(request);
            if (body != null && body.hasNonNull("reason")) {
                String text = body.get("reason").asText();
                if (!text.isEmpty()) reason = text;
            }
        } catch (Exception ignored) {
            // Malformed body — fall back to the default reason. Force-close is
            // an administrative operation and should be tolerant of body noise.
        }

        ctx.close(WS_CLOSE_CODE_GOING_AWAY, reason);
        registry.unregister(connectionId);
        return emptyResponse(HttpResponseStatus.NO_CONTENT);
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    private ObjectNode toSummaryJson(WsConnectionContext ctx) {
        ObjectNode node = MAPPER.createObjectNode();
        node.put("connectionId", ctx.sessionId());
        node.put("brokerId", brokerId);
        node.put("vhost", ctx.vhost());
        node.put("principal", ctx.principal().toString());
        node.put("connectedAt", ctx.connectTime().toString());
        node.put("subscriptionCount", ctx.subscriptions().size());
        node.put("protocol", "ws");
        node.put("remoteAddress", formatRemote(ctx.remoteAddress()));
        return node;
    }

    private ObjectNode toDetailJson(String connectionId, WsConnectionContext ctx) {
        ObjectNode node = toSummaryJson(ctx);
        // Prefer the registry key as the canonical connectionId when it differs
        // from the sessionId (HttpProcessor uses the Netty channel id).
        node.put("connectionId", connectionId);
        node.put("publishConfirmsEnabled", ctx.isPublishConfirmsEnabled());
        ArrayNode subs = node.putArray("subscriptions");
        for (String subId : ctx.subscriptions().keySet()) {
            subs.add(subId);
        }
        return node;
    }

    private static String formatRemote(InetSocketAddress addr) {
        if (addr == null) return "";
        if (addr.getAddress() == null) {
            return addr.getHostString() + ":" + addr.getPort();
        }
        return addr.getAddress().getHostAddress() + ":" + addr.getPort();
    }

    private JsonNode readBody(FullHttpRequest request) throws Exception {
        if (request.content() == null || !request.content().isReadable()) {
            return null;
        }
        byte[] bytes = new byte[request.content().readableBytes()];
        request.content().getBytes(request.content().readerIndex(), bytes);
        if (bytes.length == 0) {
            return null;
        }
        return MAPPER.readTree(bytes);
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
