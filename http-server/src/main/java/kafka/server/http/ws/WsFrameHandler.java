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
// Time: Created - TASK-WS1.04
// Time: Update - TASK-WS3.05 - added ACL checks (WsAuthorizationHelper)
package kafka.server.http.ws;

import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.resource.ResourceType;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;

/**
 * Parses incoming WebSocket JSON text frames and dispatches them by the
 * {@code type} field to the appropriate handler method.
 *
 * <p>Installed at the tail of the WebSocket pipeline (after the frame
 * aggregator) by {@link WsUpgradeOrHttpHandler} on successful upgrade.
 *
 * <h3>Dispatch table</h3>
 *
 * <p>The 15 client&#x2192;broker message types from design doc §5.2 are each
 * routed to a dedicated {@code handleXxx} method.  In this task the handler
 * methods are <b>stubs</b> that throw {@link UnsupportedOperationException};
 * real implementations arrive in later tasks when the routing engine,
 * publish handler, and subscription manager are available.
 *
 * <h3>Error frames</h3>
 *
 * <p>On malformed JSON, missing {@code type}, or an unknown {@code type}, a
 * JSON error frame is sent back via
 * {@link WsConnectionContext#sendFrame(String)}.  The error frame echoes the
 * request {@code id} (if present) so the client can correlate the error with
 * the originating request, per design doc §18.
 *
 * <pre>
 * {
 *   "type": "error",
 *   "id": "req-1",
 *   "errorCode": "PROTOCOL_ERROR",
 *   "errorMessage": "Invalid JSON"
 * }
 * </pre>
 *
 * <h3>Rate limiting</h3>
 *
 * <p>Per-connection rate limiting of control messages is <b>out of scope</b>
 * for this task and will be added in a later task.
 *
 * // Time: Created - TASK-WS1.04
 * // Time: Update - TASK-WS3.01 - added enable-confirms handling
 * // Time: Update - TASK-WS3.08 - added metrics recording
 */
public class WsFrameHandler extends SimpleChannelInboundHandler<TextWebSocketFrame> {

    private static final Logger log = LoggerFactory.getLogger(WsFrameHandler.class);

    /**
     * Shared thread-safe mapper.  Creating an ObjectMapper per-frame would be a
     * performance disaster — instance creation is heavyweight and the parsed
     * {@link JsonNode}s are produced via {@link ObjectMapper#readTree(String)},
     * which allocates a fresh tree per call.
     */
    private static final ObjectMapper MAPPER = new ObjectMapper();

    // --- Protocol constants --------------------------------------------------

    static final String FIELD_TYPE = "type";
    static final String FIELD_ID = "id";
    static final String FIELD_ERROR = "error";

    static final String ERR_PROTOCOL = "PROTOCOL_ERROR";
    static final String ERR_UNKNOWN_TYPE = "UNKNOWN_MESSAGE_TYPE";
    static final String ERR_INTERNAL = "INTERNAL_ERROR";
    /** TASK-WS3.05: emitted on any ACL denial. */
    static final String ERR_ACCESS_REFUSED = "ACCESS_REFUSED";
    /** TASK-WS3.05: conventional CLUSTER resource name used by existing Kafka authorizers. */
    static final String CLUSTER_RESOURCE_NAME = "kafka-cluster";
    /** TASK-WS3.05: queue → topic prefix (mirrors the placeholder mapping used by {@link WsPublishHandler}). */
    static final String WS_TOPIC_PREFIX = "ws.";

    // --- Message types -------------------------------------------------------

    static final String TYPE_DECLARE_EXCHANGE = "declare-exchange";
    static final String TYPE_DELETE_EXCHANGE  = "delete-exchange";
    static final String TYPE_DECLARE_QUEUE    = "declare-queue";
    static final String TYPE_DELETE_QUEUE     = "delete-queue";
    static final String TYPE_BIND             = "bind";
    static final String TYPE_UNBIND           = "unbind";
    static final String TYPE_PUBLISH          = "publish";
    static final String TYPE_SUBSCRIBE        = "subscribe";
    static final String TYPE_UNSUBSCRIBE      = "unsubscribe";
    static final String TYPE_GET              = "get";
    static final String TYPE_PURGE_QUEUE      = "purge-queue";
    static final String TYPE_ACK              = "ack";
    static final String TYPE_NACK             = "nack";
    static final String TYPE_CREDITS          = "credits";
    static final String TYPE_ENABLE_CONFIRMS  = "enable-confirms";

    private final WsConnectionContext connectionContext;
    private final WsConfigs wsConfigs;
    private final WsMetrics metrics;
    /**
     * TASK-WS3.05: optional ACL authorization helper. {@code null} disables ACL
     * checks for this handler — appropriate for unit tests that focus on
     * dispatching / protocol parsing rather than authorization.
     */
    private final WsAuthorizationHelper authorizationHelper;

    public WsFrameHandler(WsConnectionContext connectionContext, WsConfigs wsConfigs) {
        this(connectionContext, wsConfigs, null, null);
    }

    public WsFrameHandler(WsConnectionContext connectionContext, WsConfigs wsConfigs, WsMetrics metrics) {
        this(connectionContext, wsConfigs, metrics, null);
    }

    public WsFrameHandler(WsConnectionContext connectionContext, WsConfigs wsConfigs, WsMetrics metrics,
                          WsAuthorizationHelper authorizationHelper) {
        this.connectionContext = Objects.requireNonNull(connectionContext, "connectionContext");
        this.wsConfigs = Objects.requireNonNull(wsConfigs, "wsConfigs");
        this.metrics = metrics;
        this.authorizationHelper = authorizationHelper;
    }

    // ------------------------------------------------------------------
    //  Netty lifecycle
    // ------------------------------------------------------------------

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, TextWebSocketFrame frame) {
        if (metrics != null) {
            // Count every inbound text frame as a control message — this includes
            // publish and subscribe frames, matching the §11.5 definition of the
            // ws.control.message.rate metric.
            metrics.controlMessageRate.mark();
        }
        JsonNode msg = parseFrame(frame.text());
        if (msg == null) {
            return; // parseFrame already emitted the error frame
        }
        routeAndDispatch(msg);
    }

    /**
     * Parses a raw frame payload into a validated top-level JSON object.  Emits the
     * appropriate error frame and returns {@code null} on any parse / shape failure.
     */
    private JsonNode parseFrame(String text) {
        if (isBlank(text)) {
            sendErrorFrame(null, ERR_PROTOCOL, "Empty frame");
            return null;
        }
        JsonNode msg;
        try {
            msg = MAPPER.readTree(text);
        } catch (JsonProcessingException e) {
            log.debug("Malformed JSON frame on session {}: {}", connectionContext.sessionId(),
                    e.getOriginalMessage());
            sendErrorFrame(null, ERR_PROTOCOL, "Invalid JSON: " + e.getOriginalMessage());
            return null;
        }
        // readTree on a bare JSON literal (e.g. the text "null") returns a non-null
        // node whose type is NULL; guard against that and any non-object top level.
        if (msg == null || !msg.isObject()) {
            sendErrorFrame(null, ERR_PROTOCOL, "Frame must be a JSON object");
            return null;
        }
        return msg;
    }

    private static boolean isBlank(String text) {
        return text == null || text.trim().isEmpty();
    }

    /**
     * Resolves the {@code type} field and routes the parsed message.  Wraps the
     * dispatch in a best-effort try/catch so an exploding handler cannot drop the
     * client without feedback.
     */
    private void routeAndDispatch(JsonNode msg) {
        String correlationId = textOrNull(msg, FIELD_ID);
        String type = extractType(msg);
        if (type == null) {
            sendErrorFrame(correlationId, ERR_PROTOCOL, "Missing or invalid 'type' field");
            return;
        }
        try {
            dispatch(type, msg);
        } catch (UnsupportedOperationException e) {
            log.debug("Unimplemented handler for type='{}' on session {}: {}",
                    type, connectionContext.sessionId(), e.getMessage());
            sendErrorFrame(correlationId, ERR_INTERNAL,
                    "Handler not yet implemented for type: " + type);
        } catch (RuntimeException e) {
            log.warn("Handler for type='{}' threw on session {}",
                    type, connectionContext.sessionId(), e);
            sendErrorFrame(correlationId, ERR_INTERNAL,
                    "Internal error processing '" + type + "'");
        }
    }

    private static String extractType(JsonNode msg) {
        JsonNode typeNode = msg.get(FIELD_TYPE);
        if (typeNode == null || !typeNode.isTextual()) {
            return null;
        }
        String t = typeNode.asText();
        return t.isEmpty() ? null : t;
    }

    private void dispatch(String type, JsonNode msg) {
        switch (type) {
            case TYPE_DECLARE_EXCHANGE:
                handleDeclareExchange(connectionContext, msg);
                break;
            case TYPE_DELETE_EXCHANGE:
                handleDeleteExchange(connectionContext, msg);
                break;
            case TYPE_DECLARE_QUEUE:
                handleDeclareQueue(connectionContext, msg);
                break;
            case TYPE_DELETE_QUEUE:
                handleDeleteQueue(connectionContext, msg);
                break;
            case TYPE_BIND:
                handleBind(connectionContext, msg);
                break;
            case TYPE_UNBIND:
                handleUnbind(connectionContext, msg);
                break;
            case TYPE_PUBLISH:
                handlePublish(connectionContext, msg);
                break;
            case TYPE_SUBSCRIBE:
                handleSubscribe(connectionContext, msg);
                break;
            case TYPE_UNSUBSCRIBE:
                handleUnsubscribe(connectionContext, msg);
                break;
            case TYPE_GET:
                handleGet(connectionContext, msg);
                break;
            case TYPE_PURGE_QUEUE:
                handlePurgeQueue(connectionContext, msg);
                break;
            case TYPE_ACK:
                handleAck(connectionContext, msg);
                break;
            case TYPE_NACK:
                handleNack(connectionContext, msg);
                break;
            case TYPE_CREDITS:
                handleCredits(connectionContext, msg);
                break;
            case TYPE_ENABLE_CONFIRMS:
                handleEnableConfirms(connectionContext, msg);
                break;
            default:
                sendErrorFrame(textOrNull(msg, FIELD_ID), ERR_UNKNOWN_TYPE,
                        "Unknown message type: " + type);
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        // Subscription cleanup is routed through WsSubscriptionManager in a later
        // task.  For now we simply clear the map so stale references don't outlive
        // the connection and we log the event for operator visibility.
        int active = connectionContext.subscriptions().size();
        connectionContext.subscriptions().clear();
        if (log.isInfoEnabled()) {
            log.info("WebSocket session closed: sessionId={} vhost={} activeSubscriptions={}",
                    connectionContext.sessionId(), connectionContext.vhost(), active);
        }
        ctx.fireChannelInactive();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.warn("Unhandled error on WebSocket session {}: {}",
                connectionContext.sessionId(), cause.toString(), cause);
        // Close the channel rather than try to recover — the Netty pipeline's
        // invariants may have been violated and further frames could corrupt state.
        ctx.close();
    }

    // ------------------------------------------------------------------
    //  Dispatch methods — stubs in this task
    // ------------------------------------------------------------------

    void handleDeclareExchange(WsConnectionContext ctx, JsonNode msg) {
        throw new UnsupportedOperationException("Not yet implemented: declare-exchange");
    }

    void handleDeleteExchange(WsConnectionContext ctx, JsonNode msg) {
        throw new UnsupportedOperationException("Not yet implemented: delete-exchange");
    }

    void handleDeclareQueue(WsConnectionContext ctx, JsonNode msg) {
        throw new UnsupportedOperationException("Not yet implemented: declare-queue");
    }

    void handleDeleteQueue(WsConnectionContext ctx, JsonNode msg) {
        throw new UnsupportedOperationException("Not yet implemented: delete-queue");
    }

    void handleBind(WsConnectionContext ctx, JsonNode msg) {
        throw new UnsupportedOperationException("Not yet implemented: bind");
    }

    void handleUnbind(WsConnectionContext ctx, JsonNode msg) {
        throw new UnsupportedOperationException("Not yet implemented: unbind");
    }

    void handlePublish(WsConnectionContext ctx, JsonNode msg) {
        throw new UnsupportedOperationException("Not yet implemented: publish");
    }

    void handleSubscribe(WsConnectionContext ctx, JsonNode msg) {
        throw new UnsupportedOperationException("Not yet implemented: subscribe");
    }

    void handleUnsubscribe(WsConnectionContext ctx, JsonNode msg) {
        throw new UnsupportedOperationException("Not yet implemented: unsubscribe");
    }

    void handleGet(WsConnectionContext ctx, JsonNode msg) {
        throw new UnsupportedOperationException("Not yet implemented: get");
    }

    void handlePurgeQueue(WsConnectionContext ctx, JsonNode msg) {
        throw new UnsupportedOperationException("Not yet implemented: purge-queue");
    }

    void handleAck(WsConnectionContext ctx, JsonNode msg) {
        throw new UnsupportedOperationException("Not yet implemented: ack");
    }

    void handleNack(WsConnectionContext ctx, JsonNode msg) {
        throw new UnsupportedOperationException("Not yet implemented: nack");
    }

    void handleCredits(WsConnectionContext ctx, JsonNode msg) {
        throw new UnsupportedOperationException("Not yet implemented: credits");
    }

    /**
     * Handle an {@code enable-confirms} frame: flip the connection-level
     * publisher-confirms flag (idempotent) and emit a {@code confirms-enabled}
     * reply, echoing the client's correlation id when present.
     *
     * <p>After this frame, every subsequent {@code publish} with a
     * {@code publishId} will trigger a {@code published} or
     * {@code publish-failed} reply once the underlying produce completes
     * (produced by {@link kafka.server.http.HttpProcessor#handleWsResponse}).
     *
     * <p>Per design doc §5.7, confirms cannot be disabled on a live
     * connection — the client must close and reopen.
     */
    void handleEnableConfirms(WsConnectionContext ctx, JsonNode msg) {
        ctx.enablePublishConfirms();
        sendConfirmsEnabledFrame(textOrNull(msg, FIELD_ID));
    }

    /**
     * Build and emit the {@code confirms-enabled} reply. Correlation id is
     * omitted from the frame when {@code correlationId} is {@code null}.
     */
    private void sendConfirmsEnabledFrame(String correlationId) {
        ObjectNode frame = MAPPER.createObjectNode();
        frame.put(FIELD_TYPE, "confirms-enabled");
        if (correlationId != null) {
            frame.put(FIELD_ID, correlationId);
        }
        try {
            connectionContext.sendFrame(MAPPER.writeValueAsString(frame));
        } catch (JsonProcessingException e) {
            // Should be impossible for a freshly-built ObjectNode.
            log.warn("Failed to serialise confirms-enabled frame for session {}",
                    connectionContext.sessionId(), e);
        }
    }

    // ------------------------------------------------------------------
    //  TASK-WS3.05 — ACL helpers for subclasses / later-task handler impls
    // ------------------------------------------------------------------

    /**
     * Checks that the authenticated principal is authorized for the given
     * resource / operation. Emits an {@code ACCESS_REFUSED} error frame (with
     * the supplied correlation id) and returns {@code false} on denial.
     *
     * <p>When no {@link WsAuthorizationHelper} is configured (e.g. unit tests
     * that don't exercise ACLs) this is a permissive no-op — always returns
     * {@code true} without touching the wire. Production wiring always supplies
     * a helper.
     *
     * @return {@code true} if authorized (or helper not configured);
     *         {@code false} after emitting the error frame.
     */
    boolean requireAuthorized(String correlationId, String failingOperation,
                              ResourceType resourceType, String resourceName,
                              AclOperation operation) {
        if (authorizationHelper == null) {
            return true;
        }
        if (authorizationHelper.authorize(
                connectionContext.principal(), resourceType, resourceName, operation)) {
            return true;
        }
        sendErrorFrame(correlationId, ERR_ACCESS_REFUSED,
            "Not authorized: " + operation + " on " + resourceType + ":" + resourceName,
            failingOperation, null);
        return false;
    }

    /**
     * Convenience: ACL check for a {@code declare-exchange} / {@code delete-exchange}
     * operation ({@code CLUSTER:ALTER}).
     */
    boolean requireExchangeAdmin(String correlationId, String failingOperation) {
        return requireAuthorized(correlationId, failingOperation,
            ResourceType.CLUSTER, CLUSTER_RESOURCE_NAME, AclOperation.ALTER);
    }

    /**
     * Convenience: ACL check for a queue-admin operation against the backing
     * topic {@code ws.<queue>}.
     *
     * @param operation {@link AclOperation#CREATE} for {@code declare-queue},
     *                  {@link AclOperation#DELETE} for {@code delete-queue},
     *                  {@link AclOperation#ALTER} for {@code bind}/{@code unbind}
     */
    boolean requireQueueAccess(String correlationId, String failingOperation,
                               String queueName, AclOperation operation) {
        String topic = WS_TOPIC_PREFIX + queueName;
        return requireAuthorized(correlationId, failingOperation,
            ResourceType.TOPIC, topic, operation);
    }

    /** @return the authorization helper configured on this handler, or {@code null}. */
    WsAuthorizationHelper authorizationHelper() {
        return authorizationHelper;
    }

    // ------------------------------------------------------------------
    //  Error frame helpers
    // ------------------------------------------------------------------

    /**
     * Build and send a JSON error frame per design doc §18.  The {@code id} field
     * is omitted from the frame when {@code correlationId} is {@code null}.
     */
    void sendErrorFrame(String correlationId, String errorCode, String errorMessage) {
        sendErrorFrame(correlationId, errorCode, errorMessage, null, null);
    }

    /**
     * Extended variant that attaches a {@code detail} object carrying
     * {@code failingOperation} and a single free-form {@code detailMessage} — both
     * are optional.
     */
    void sendErrorFrame(String correlationId, String errorCode, String errorMessage,
                        String failingOperation, String detailMessage) {
        ObjectNode err = MAPPER.createObjectNode();
        err.put(FIELD_TYPE, FIELD_ERROR);
        if (correlationId != null) {
            err.put(FIELD_ID, correlationId);
        }
        err.put("errorCode", Objects.requireNonNullElse(errorCode, ERR_INTERNAL));
        err.put("errorMessage", Objects.requireNonNullElse(errorMessage, ""));
        if (failingOperation != null || detailMessage != null) {
            ObjectNode detail = err.putObject("detail");
            if (failingOperation != null) {
                detail.put("failingOperation", failingOperation);
            }
            if (detailMessage != null) {
                detail.put("detail", detailMessage);
            }
        }
        try {
            connectionContext.sendFrame(MAPPER.writeValueAsString(err));
        } catch (JsonProcessingException e) {
            // Extremely unlikely — we're serialising an ObjectNode we just built.  If
            // it happens, log and move on; swallowing is better than crashing the
            // pipeline because we couldn't render an error frame.
            log.warn("Failed to serialise error frame for session {}",
                    connectionContext.sessionId(), e);
        }
        if (metrics != null) {
            metrics.errorRate.mark();
        }
    }

    // ------------------------------------------------------------------
    //  JSON helpers
    // ------------------------------------------------------------------

    /**
     * Returns the text value of the named field or {@code null} when the field is
     * missing, not textual, or blank.
     */
    private static String textOrNull(JsonNode node, String field) {
        JsonNode child = node.get(field);
        if (child == null || !child.isTextual()) {
            return null;
        }
        String v = child.asText();
        return v.isEmpty() ? null : v;
    }

    // ------------------------------------------------------------------
    //  Package-private accessors (for tests)
    // ------------------------------------------------------------------

    WsConnectionContext connectionContext() {
        return connectionContext;
    }

    WsConfigs wsConfigs() {
        return wsConfigs;
    }
}
