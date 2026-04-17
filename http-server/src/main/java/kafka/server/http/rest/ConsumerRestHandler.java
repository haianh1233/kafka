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
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import kafka.server.http.ws.SubscriptionContext;
import kafka.server.http.ws.WsConnectionContext;
import kafka.server.http.ws.WsConnectionRegistry;
import kafka.server.http.ws.WsSubscriptionManager;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * REST handlers for /v1/consumers endpoints — list, force-cancel.
 *
 * <p>Each WebSocket connection owns its own {@link WsSubscriptionManager}; this
 * handler fans out across all active connections via a user-supplied
 * {@link SubscriptionManagerLookup} to answer list requests, and addresses a
 * specific manager by connectionId for force-cancel.
 *
 * <p>Force-cancel sends a {@code subscription-cancelled} frame with
 * {@code reason: "ADMIN_CANCEL"} before invoking
 * {@link WsSubscriptionManager#unsubscribe(String)}. The returned committable
 * offsets are intentionally <b>discarded</b> — administrative cancellation
 * must not commit offsets, so that unacked messages are re-delivered when the
 * consumer reconnects.
 *
 * // Time: Created - TASK-WS2.07
 */
public final class ConsumerRestHandler {

    /**
     * Resolves the per-connection {@link WsSubscriptionManager} for the given
     * connection id. Returns {@code null} when unknown. Broker wiring typically
     * plugs this with a lookup into a {@code Map<connectionId,
     * WsSubscriptionManager>} maintained by the WS frame handler.
     */
    @FunctionalInterface
    public interface SubscriptionManagerLookup {
        WsSubscriptionManager forConnection(String connectionId);
    }

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final WsConnectionRegistry registry;
    private final SubscriptionManagerLookup managers;

    public ConsumerRestHandler(WsConnectionRegistry registry,
                               SubscriptionManagerLookup managers) {
        this.registry = Objects.requireNonNull(registry, "registry");
        this.managers = Objects.requireNonNull(managers, "managers");
    }

    /**
     * GET /v1/consumers — list all active subscriptions.
     *
     * @param queueFilter optional exact-match queue name; {@code null} returns all.
     */
    public FullHttpResponse handleList(String queueFilter) {
        ObjectNode root = MAPPER.createObjectNode();
        ArrayNode arr = root.putArray("consumers");

        for (String connId : registry.connectionIds()) {
            WsSubscriptionManager mgr = managers.forConnection(connId);
            if (mgr == null) continue;
            WsConnectionContext connCtx = registry.get(connId);

            for (String subId : mgr.activeSubscriptionIds()) {
                SubscriptionContext sub = mgr.getSubscription(subId);
                if (sub == null) continue; // race with unsubscribe — skip
                if (queueFilter != null && !queueFilter.equals(sub.queueName())) {
                    continue;
                }
                arr.add(toConsumerJson(connId, connCtx, sub));
            }
        }
        return jsonResponse(HttpResponseStatus.OK, root);
    }

    /**
     * DELETE /v1/consumers/{connectionId}/{subscriptionId} — force-cancel.
     *
     * <p>Looks up the connection and subscription, sends a
     * {@code subscription-cancelled} frame with {@code reason: "ADMIN_CANCEL"}
     * to the owning connection, then unsubscribes. The offset map returned by
     * {@link WsSubscriptionManager#unsubscribe(String)} is <b>not</b> committed:
     * unacked messages must be re-delivered upon reconnection.
     *
     * @return 204 on success, 404 if connection or subscription is unknown.
     */
    public FullHttpResponse handleForceCancel(String connectionId, String subscriptionId) {
        Objects.requireNonNull(connectionId, "connectionId");
        Objects.requireNonNull(subscriptionId, "subscriptionId");

        WsConnectionContext connCtx = registry.get(connectionId);
        if (connCtx == null) {
            return errorResponse(HttpResponseStatus.NOT_FOUND,
                "Connection '" + connectionId + "' does not exist");
        }
        WsSubscriptionManager mgr = managers.forConnection(connectionId);
        if (mgr == null || mgr.getSubscription(subscriptionId) == null) {
            return errorResponse(HttpResponseStatus.NOT_FOUND,
                "Subscription '" + subscriptionId
                    + "' does not exist on connection '" + connectionId + "'");
        }

        // 1) Notify the client: subscription-cancelled, reason=ADMIN_CANCEL.
        connCtx.sendFrame(cancelledFrameJson(subscriptionId));

        // 2) Stop the fetch loop and drop tracker state. The returned committable
        //    offsets are intentionally discarded — administrative cancel must
        //    not commit offsets so that unacked messages requeue on reconnect.
        mgr.unsubscribe(subscriptionId);

        return emptyResponse(HttpResponseStatus.NO_CONTENT);
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    private static String cancelledFrameJson(String subscriptionId) {
        ObjectNode node = MAPPER.createObjectNode();
        node.put("type", "subscription-cancelled");
        node.put("subscriptionId", subscriptionId);
        node.put("reason", "ADMIN_CANCEL");
        return node.toString();
    }

    private ObjectNode toConsumerJson(String connId,
                                      WsConnectionContext connCtx,
                                      SubscriptionContext sub) {
        ObjectNode node = MAPPER.createObjectNode();
        node.put("connectionId", connId);
        node.put("subscriptionId", sub.subscriptionId());
        node.put("queue", sub.queueName());
        node.put("topic", sub.topic());
        node.put("creditsAvailable", sub.creditManager().available());
        node.put("pendingAcks", sub.deliveryTagTracker().pendingCount());
        if (connCtx != null) {
            node.put("principal", connCtx.principal().toString());
            node.put("vhost", connCtx.vhost());
        }
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
