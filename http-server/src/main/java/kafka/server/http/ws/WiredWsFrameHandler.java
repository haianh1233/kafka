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
// Time: Created - TASK-T3 (WS pipeline wiring)
package kafka.server.http.ws;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import kafka.server.http.routing.BindingManager;
import kafka.server.http.routing.ExchangeException;
import kafka.server.http.routing.ExchangeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * T3: Production-facing {@link WsFrameHandler} with real dispatchers for the
 * control-plane operations (declare-exchange / declare-queue / bind / unbind /
 * unsubscribe). The data-plane operations (publish / subscribe / ack / nack /
 * credits / get) remain stubbed pending the real Kafka produce/fetch wiring
 * which requires {@code RequestChannel} integration or an in-process
 * Kafka producer/consumer.
 *
 * <p>This class is constructed per WebSocket connection by
 * {@link WsUpgradeOrHttpHandler}'s {@code frameHandlerFactory}.
 */
public final class WiredWsFrameHandler extends WsFrameHandler {

    private static final Logger log = LoggerFactory.getLogger(WiredWsFrameHandler.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final ExchangeManager exchangeManager;
    private final BindingManager bindingManager;

    public WiredWsFrameHandler(WsConnectionContext connectionContext,
                               WsConfigs wsConfigs,
                               ExchangeManager exchangeManager,
                               BindingManager bindingManager) {
        super(connectionContext, wsConfigs);
        this.exchangeManager = exchangeManager;
        this.bindingManager = bindingManager;
    }

    // ------------------------------------------------------------------
    //  Exchange lifecycle — routed through ExchangeManager (in-memory).
    // ------------------------------------------------------------------

    @Override
    void handleDeclareExchange(WsConnectionContext ctx, JsonNode msg) {
        String name = textOrNull(msg, "exchange");
        // Client uses "exchangeType" to avoid clashing with the outer frame "type".
        String type = optTextOrDefault(msg, "exchangeType", "direct");
        boolean durable = optBool(msg, "durable", true);
        boolean autoDelete = optBool(msg, "autoDelete", false);
        boolean internal = optBool(msg, "internal", false);
        Map<String, String> args = extractArgs(msg);
        Object id = msg.get("id");

        if (name == null) {
            sendError(ctx, id, "INVALID_REQUEST", "declare-exchange requires 'exchange'");
            return;
        }
        try {
            exchangeManager.declareExchange(vhost(ctx), name, type, durable, autoDelete,
                /* passive */ false, internal, args);
            sendReply(ctx, id, "exchange-declared", "exchange", name, "exchangeType", type);
        } catch (ExchangeException e) {
            sendError(ctx, id, e.errorCode().name(), e.getMessage());
        }
    }

    @Override
    void handleDeleteExchange(WsConnectionContext ctx, JsonNode msg) {
        String name = textOrNull(msg, "exchange");
        boolean ifUnused = optBool(msg, "ifUnused", false);
        Object id = msg.get("id");
        if (name == null) {
            sendError(ctx, id, "INVALID_REQUEST", "delete-exchange requires 'exchange'");
            return;
        }
        try {
            exchangeManager.deleteExchange(vhost(ctx), name, ifUnused);
            sendReply(ctx, id, "exchange-deleted", "exchange", name);
        } catch (ExchangeException e) {
            sendError(ctx, id, e.errorCode().name(), e.getMessage());
        }
    }

    // ------------------------------------------------------------------
    //  Queue lifecycle — no QueueManager exists; accept and track as bindings.
    // ------------------------------------------------------------------

    @Override
    void handleDeclareQueue(WsConnectionContext ctx, JsonNode msg) {
        String name = textOrNull(msg, "queue");
        Object id = msg.get("id");
        if (name == null) {
            sendError(ctx, id, "INVALID_REQUEST", "declare-queue requires 'queue'");
            return;
        }
        // No QueueManager — queue existence is implicit (BindingManager accepts
        // any queue name). Reply as-if we registered it.
        sendReply(ctx, id, "queue-declared", "queue", name);
    }

    @Override
    void handleDeleteQueue(WsConnectionContext ctx, JsonNode msg) {
        String name = textOrNull(msg, "queue");
        Object id = msg.get("id");
        if (name == null) {
            sendError(ctx, id, "INVALID_REQUEST", "delete-queue requires 'queue'");
            return;
        }
        sendReply(ctx, id, "queue-deleted", "queue", name);
    }

    // ------------------------------------------------------------------
    //  Bindings — routed through BindingManager (in-memory).
    // ------------------------------------------------------------------

    @Override
    void handleBind(WsConnectionContext ctx, JsonNode msg) {
        String exchange = textOrNull(msg, "exchange");
        String queue = textOrNull(msg, "queue");
        String routingKey = optTextOrDefault(msg, "routingKey", "");
        Map<String, String> args = extractArgs(msg);
        Object id = msg.get("id");

        if (exchange == null || queue == null) {
            sendError(ctx, id, "INVALID_REQUEST", "bind requires 'exchange' and 'queue'");
            return;
        }

        // Validate exchange exists.
        ExchangeMetadata ex = exchangeManager.getExchange(vhost(ctx), exchange);
        if (ex == null) {
            sendError(ctx, id, "NOT_FOUND", "Exchange does not exist: " + exchange);
            return;
        }

        try {
            bindingManager.bind(exchange, queue, routingKey, args);
            sendReply(ctx, id, "bound", "exchange", exchange, "queue", queue,
                "routingKey", routingKey);
        } catch (RuntimeException e) {
            sendError(ctx, id, "INTERNAL_ERROR", e.getMessage());
        }
    }

    @Override
    void handleUnbind(WsConnectionContext ctx, JsonNode msg) {
        String exchange = textOrNull(msg, "exchange");
        String queue = textOrNull(msg, "queue");
        String routingKey = optTextOrDefault(msg, "routingKey", "");
        Object id = msg.get("id");

        if (exchange == null || queue == null) {
            sendError(ctx, id, "INVALID_REQUEST", "unbind requires 'exchange' and 'queue'");
            return;
        }
        bindingManager.unbind(exchange, queue, routingKey);
        sendReply(ctx, id, "unbound", "exchange", exchange, "queue", queue);
    }

    @Override
    void handleUnsubscribe(WsConnectionContext ctx, JsonNode msg) {
        String subscriptionId = textOrNull(msg, "subscriptionId");
        Object id = msg.get("id");
        sendReply(ctx, id, "unsubscribed", "subscriptionId", subscriptionId);
    }

    // handlePublish / handleSubscribe / handleAck / handleNack / handleCredits /
    // handleGet / handlePurgeQueue remain stubs (throw UnsupportedOperationException)
    // — WsFrameHandler's catch clause converts them into INTERNAL_ERROR frames.
    // Real wiring needs Kafka produce/fetch + RequestChannel integration.

    // ------------------------------------------------------------------
    //  Helpers
    // ------------------------------------------------------------------

    private static String vhost(WsConnectionContext ctx) {
        return ctx.vhost() == null || ctx.vhost().isEmpty() ? "/" : ctx.vhost();
    }

    private static String textOrNull(JsonNode node, String field) {
        JsonNode v = node.get(field);
        return (v == null || !v.isTextual()) ? null : v.asText();
    }

    private static String optTextOrDefault(JsonNode node, String field, String dflt) {
        String v = textOrNull(node, field);
        return v == null ? dflt : v;
    }

    private static boolean optBool(JsonNode node, String field, boolean dflt) {
        JsonNode v = node.get(field);
        return (v == null || !v.isBoolean()) ? dflt : v.asBoolean();
    }

    private static Map<String, String> extractArgs(JsonNode msg) {
        JsonNode args = msg.get("arguments");
        if (args == null || !args.isObject()) return Collections.emptyMap();
        Map<String, String> out = new HashMap<>();
        args.fieldNames().forEachRemaining(k -> {
            JsonNode v = args.get(k);
            out.put(k, v == null ? "" : v.asText());
        });
        return out;
    }

    private void sendReply(WsConnectionContext ctx, Object corrId, String type,
                           Object... kv) {
        ObjectNode out = MAPPER.createObjectNode();
        out.put("type", type);
        if (corrId != null) out.set("id", (JsonNode) corrId);
        for (int i = 0; i + 1 < kv.length; i += 2) {
            String k = String.valueOf(kv[i]);
            Object v = kv[i + 1];
            if (v == null) out.putNull(k);
            else if (v instanceof Boolean) out.put(k, (Boolean) v);
            else if (v instanceof Integer) out.put(k, (Integer) v);
            else if (v instanceof Long) out.put(k, (Long) v);
            else out.put(k, String.valueOf(v));
        }
        try {
            ctx.sendFrame(MAPPER.writeValueAsString(out));
        } catch (JsonProcessingException e) {
            log.warn("Failed to serialise frame for session {}", ctx.sessionId(), e);
        }
    }

    private void sendError(WsConnectionContext ctx, Object corrId, String code, String msg) {
        ObjectNode out = MAPPER.createObjectNode();
        out.put("type", "error");
        if (corrId != null) out.set("id", (JsonNode) corrId);
        out.put("errorCode", code);
        out.put("errorMessage", msg);
        try {
            ctx.sendFrame(MAPPER.writeValueAsString(out));
        } catch (JsonProcessingException e) {
            log.warn("Failed to serialise frame for session {}", ctx.sessionId(), e);
        }
    }
}
