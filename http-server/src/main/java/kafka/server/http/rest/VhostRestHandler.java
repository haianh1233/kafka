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

// Time: Created - TASK-WS2.09

package kafka.server.http.rest;

import kafka.server.http.routing.VhostManager;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

/**
 * REST handlers for {@code /v1/vhosts} (design doc §5.2).
 *
 * <ul>
 *   <li>{@code GET /v1/vhosts} — list virtual hosts with exchange/queue counts.</li>
 *   <li>{@code PUT /v1/vhosts/{vhost}} — create a vhost; pre-declared exchanges
 *       are auto-created.</li>
 *   <li>{@code DELETE /v1/vhosts/{vhost}} — delete the vhost and all its
 *       resources. Returns {@code 409 Conflict} when active WebSocket
 *       connections exist.</li>
 * </ul>
 *
 * <p>Delegates lifecycle to {@link VhostManager}. All responses are
 * {@code application/json}; errors carry {@code errorCode} and
 * {@code errorMessage} fields to mirror the other REST handlers.
 *
 * // Time: Created - TASK-WS2.09
 */
public final class VhostRestHandler {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final VhostManager vhostManager;

    public VhostRestHandler(VhostManager vhostManager) {
        this.vhostManager = Objects.requireNonNull(vhostManager, "vhostManager");
    }

    // ------------------------------------------------------------------
    // Handlers
    // ------------------------------------------------------------------

    /** {@code GET /v1/vhosts} — returns a JSON array of vhost summaries. */
    public FullHttpResponse handleList() {
        List<VhostManager.VhostInfo> vhosts = vhostManager.listVhosts();
        ObjectNode root = MAPPER.createObjectNode();
        ArrayNode arr = root.putArray("vhosts");
        for (VhostManager.VhostInfo v : vhosts) {
            ObjectNode node = MAPPER.createObjectNode();
            node.put("name", v.name());
            node.put("exchangeCount", v.exchangeCount());
            node.put("queueCount", v.queueCount());
            arr.add(node);
        }
        return jsonResponse(HttpResponseStatus.OK, root);
    }

    /**
     * {@code PUT /v1/vhosts/{vhost}} — create a vhost. Idempotent: returns
     * {@code 201 Created} on first create, {@code 200 OK} on repeated creates.
     */
    public FullHttpResponse handleCreate(String vhostName) {
        Objects.requireNonNull(vhostName, "vhostName");
        if (vhostName.isEmpty()) {
            return errorResponse(HttpResponseStatus.BAD_REQUEST, "Vhost name must not be empty");
        }
        String normalised = vhostName.startsWith("/") ? vhostName : "/" + vhostName;
        boolean existedBefore = vhostManager.exists(normalised);
        vhostManager.createVhost(normalised);
        return jsonResponse(
            existedBefore ? HttpResponseStatus.OK : HttpResponseStatus.CREATED,
            toVhostJson(normalised));
    }

    /**
     * {@code DELETE /v1/vhosts/{vhost}} — delete the vhost and cascade its
     * resources.
     *
     * <ul>
     *   <li>{@code 204 No Content} on success.</li>
     *   <li>{@code 403 Forbidden} when the caller attempts to delete the
     *       default vhost {@code /}.</li>
     *   <li>{@code 404 Not Found} when the vhost does not exist.</li>
     *   <li>{@code 409 Conflict} when active connections are present.</li>
     * </ul>
     */
    public FullHttpResponse handleDelete(String vhostName) {
        Objects.requireNonNull(vhostName, "vhostName");
        if (vhostName.isEmpty()) {
            return errorResponse(HttpResponseStatus.BAD_REQUEST, "Vhost name must not be empty");
        }
        String normalised = vhostName.startsWith("/") ? vhostName : "/" + vhostName;

        if (VhostManager.DEFAULT_VHOST.equals(normalised)) {
            return errorResponse(HttpResponseStatus.FORBIDDEN,
                "Default vhost '/' cannot be deleted");
        }
        if (!vhostManager.exists(normalised)) {
            return errorResponse(HttpResponseStatus.NOT_FOUND,
                "Vhost '" + normalised + "' does not exist");
        }
        try {
            vhostManager.deleteVhost(normalised);
            return emptyResponse(HttpResponseStatus.NO_CONTENT);
        } catch (VhostManager.VhostInUseException e) {
            return errorResponse(HttpResponseStatus.CONFLICT, e.getMessage());
        } catch (IllegalArgumentException e) {
            // Default-vhost protection is also enforced inside VhostManager.
            return errorResponse(HttpResponseStatus.FORBIDDEN, e.getMessage());
        }
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    private ObjectNode toVhostJson(String vhost) {
        ObjectNode node = MAPPER.createObjectNode();
        node.put("name", vhost);
        // Look up the current counts so the response reflects post-create state.
        for (VhostManager.VhostInfo v : vhostManager.listVhosts()) {
            if (v.name().equals(vhost)) {
                node.put("exchangeCount", v.exchangeCount());
                node.put("queueCount", v.queueCount());
                return node;
            }
        }
        node.put("exchangeCount", 0);
        node.put("queueCount", 0);
        return node;
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
