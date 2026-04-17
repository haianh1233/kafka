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
// Time: Created - TASK-WS1.03
package kafka.server.http.ws;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrameAggregator;
import io.netty.handler.codec.http.websocketx.WebSocketServerHandshaker;
import io.netty.handler.codec.http.websocketx.WebSocketServerHandshakerFactory;
import org.apache.kafka.common.security.auth.HttpAuthenticationContext;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.KafkaPrincipalBuilder;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Detects WebSocket upgrade requests on {@code GET /v1/ws} and switches the
 * Netty pipeline from HTTP mode to WebSocket mode.
 *
 * <p>Sits as the terminal handler for the first request on a connection
 * (after the standard HTTP decoder + aggregator).  Each {@link FullHttpRequest}
 * is inspected:
 * <ul>
 *   <li>If the request has {@code Upgrade: websocket} + {@code Connection: Upgrade}
 *       and targets {@code /v1/ws}, the WebSocket handshake is performed and the
 *       pipeline is rewired.</li>
 *   <li>Otherwise, the request is retained and forwarded to the next handler
 *       via {@link ChannelHandlerContext#fireChannelRead(Object)}.</li>
 * </ul>
 *
 * <p>On successful upgrade the pipeline has:
 * <ol>
 *   <li>{@code http-aggregator} removed (WebSocket has its own aggregator).</li>
 *   <li>{@code compressor} removed (WebSocket does not use chunked HTTP compression).</li>
 *   <li>{@code ws-or-http} — this handler — removed.</li>
 *   <li>{@code ws-frame-aggregator} added (caps frame size at {@link WsConfigs#maxFrameSize()}).</li>
 *   <li>{@code ws-handler} added — currently a no-op placeholder; replaced by
 *       {@code WsFrameHandler} in TASK-WS1.04.</li>
 * </ol>
 *
 * <p>After the handshake completes, a {@code connected} JSON message is written
 * as a {@link TextWebSocketFrame}.
 *
 * // Time: Created - TASK-WS1.03
 */
public class WsUpgradeOrHttpHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

    private static final Logger log = LoggerFactory.getLogger(WsUpgradeOrHttpHandler.class);

    /** The single well-known WebSocket endpoint path. */
    static final String WS_PATH = "/v1/ws";

    /** Pipeline handler names — kept constant so downstream tasks can refer to them. */
    private static final String HANDLER_NAME = "ws-or-http";
    private static final String HTTP_AGGREGATOR_NAME = "http-aggregator";
    private static final String COMPRESSOR_NAME = "compressor";
    private static final String WS_FRAME_AGGREGATOR_NAME = "ws-frame-aggregator";
    private static final String WS_HANDLER_NAME = "ws-handler";

    /** Server capabilities advertised in the {@code connected} message. */
    private static final String SERVER_CAPABILITIES =
            "\"exchange.direct\",\"exchange.topic\",\"exchange.fanout\","
          + "\"exchange.headers\",\"publisher-confirms\",\"credits\"";

    private final WsConfigs wsConfigs;
    private final int brokerId;
    private final String clusterId;
    private final KafkaPrincipalBuilder principalBuilder;
    private final SecurityProtocol securityProtocol;

    public WsUpgradeOrHttpHandler(
            WsConfigs wsConfigs,
            int brokerId,
            String clusterId,
            KafkaPrincipalBuilder principalBuilder,
            SecurityProtocol securityProtocol) {
        this.wsConfigs = Objects.requireNonNull(wsConfigs, "wsConfigs");
        this.brokerId = brokerId;
        this.clusterId = Objects.requireNonNull(clusterId, "clusterId");
        this.principalBuilder = Objects.requireNonNull(principalBuilder, "principalBuilder");
        this.securityProtocol = Objects.requireNonNull(securityProtocol, "securityProtocol");
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest req) {
        if (!isWebSocketUpgrade(req)) {
            // Not a WebSocket upgrade — forward to the next handler.  SimpleChannelInboundHandler
            // will release the original reference when this method returns, so we retain
            // before firing down the pipeline.
            ctx.fireChannelRead(req.retain());
            return;
        }

        if (!wsConfigs.wsEnabled()) {
            log.debug("WebSocket upgrade rejected — ws.enabled=false");
            sendHttpErrorAndClose(ctx, HttpResponseStatus.NOT_FOUND);
            return;
        }

        try {
            upgradeToWebSocket(ctx, req);
        } catch (RuntimeException e) {
            log.warn("WebSocket upgrade failed", e);
            sendHttpErrorAndClose(ctx, HttpResponseStatus.BAD_REQUEST);
        }
    }

    /**
     * Returns {@code true} iff the request is a valid WebSocket upgrade targeting
     * {@link #WS_PATH}.
     *
     * <p>Checks:
     * <ul>
     *   <li>HTTP method is {@code GET}.</li>
     *   <li>Path component of the URI equals {@code /v1/ws} (query string ignored).</li>
     *   <li>{@code Upgrade} header contains {@code websocket} (case-insensitive).</li>
     *   <li>{@code Connection} header contains {@code Upgrade} (case-insensitive,
     *       allowing comma-separated values such as {@code keep-alive, Upgrade}).</li>
     * </ul>
     */
    static boolean isWebSocketUpgrade(FullHttpRequest req) {
        if (req == null || !HttpMethod.GET.equals(req.method())) {
            return false;
        }
        QueryStringDecoder decoder = new QueryStringDecoder(req.uri());
        if (!WS_PATH.equals(decoder.path())) {
            return false;
        }
        String upgrade = req.headers().get(HttpHeaderNames.UPGRADE);
        if (upgrade == null || !upgrade.toLowerCase(Locale.ROOT)
                .contains(HttpHeaderValues.WEBSOCKET.toString())) {
            return false;
        }
        // Connection header may be comma-separated (e.g. "keep-alive, Upgrade").
        // HttpUtil.isKeepAlive is NOT what we want — we need to test for "Upgrade".
        List<String> connectionValues = req.headers().getAll(HttpHeaderNames.CONNECTION);
        for (String value : connectionValues) {
            for (String part : value.split(",")) {
                if (part.trim().equalsIgnoreCase(HttpHeaderValues.UPGRADE.toString())) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Returns the {@code vhost} query parameter, or {@code "/"} if absent or blank.
     */
    static String extractVhost(FullHttpRequest req) {
        QueryStringDecoder decoder = new QueryStringDecoder(req.uri());
        Map<String, List<String>> params = decoder.parameters();
        List<String> values = params.get("vhost");
        if (values == null || values.isEmpty()) {
            return "/";
        }
        String first = values.get(0);
        if (first == null || first.isEmpty()) {
            return "/";
        }
        return first;
    }

    // ------------------------------------------------------------------
    //  Internal
    // ------------------------------------------------------------------

    private void upgradeToWebSocket(ChannelHandlerContext ctx, FullHttpRequest req) {
        // 1. Build the principal from the upgrade request — same path as HTTP.
        KafkaPrincipal principal = buildPrincipal(ctx);
        String vhost = extractVhost(req);

        // 2. Handshake.  wsUrl must reflect ws:// vs wss:// based on TLS presence.
        String wsUrl = webSocketLocation(ctx, req);
        WebSocketServerHandshakerFactory wsFactory = new WebSocketServerHandshakerFactory(
                wsUrl, /* subprotocols */ null, /* allowExtensions */ true,
                wsConfigs.maxFrameSize());
        WebSocketServerHandshaker handshaker = wsFactory.newHandshaker(req);
        if (handshaker == null) {
            // Unsupported WebSocket version — Netty helpfully writes the error.
            WebSocketServerHandshakerFactory.sendUnsupportedVersionResponse(ctx.channel());
            return;
        }

        ChannelFuture handshakeFuture = handshaker.handshake(ctx.channel(), req);

        // 3. All pipeline mutation and the `connected` frame happen strictly AFTER
        //    the handshake response (HTTP 101) is written.  See design doc §15.1.
        handshakeFuture.addListener((ChannelFutureListener) future -> {
            if (!future.isSuccess()) {
                log.warn("WebSocket handshake failed", future.cause());
                ctx.close();
                return;
            }

            ChannelPipeline pipeline = ctx.pipeline();

            // Remove HTTP-only handlers.  Null-guard each one: the pipeline shape can
            // vary (e.g. compressor may be absent in tests or behind a proxy).
            removeIfPresent(pipeline, HTTP_AGGREGATOR_NAME);
            removeIfPresent(pipeline, COMPRESSOR_NAME);

            // Install WebSocket handlers BEFORE removing ourselves so they end up
            // in the same relative position in the pipeline.
            pipeline.addAfter(HANDLER_NAME, WS_HANDLER_NAME, placeholderFrameHandler());
            pipeline.addAfter(HANDLER_NAME, WS_FRAME_AGGREGATOR_NAME,
                    new WebSocketFrameAggregator(wsConfigs.maxFrameSize()));

            // 4. Build the per-connection context.  Stored via AttributeKey so later
            //    handlers in the pipeline can retrieve it.  (TASK-WS1.04 will wire this
            //    into WsFrameHandler.)
            InetSocketAddress remote = remoteAddress(ctx.channel().remoteAddress());
            String sessionId = generateSessionId();
            WsConnectionContext connCtx = new WsConnectionContext(
                    sessionId, principal, vhost, ctx, remote);
            ctx.channel().attr(WsAttributes.CONNECTION_CONTEXT).set(connCtx);

            // 5. Emit `connected` frame.
            String connectedJson = buildConnectedJson(sessionId);
            ctx.writeAndFlush(new TextWebSocketFrame(connectedJson));

            // Finally, remove ourselves — no more HTTP requests on this channel.
            removeIfPresent(pipeline, HANDLER_NAME);

            log.info("WebSocket session established: sessionId={} principal={} vhost={} remote={}",
                    sessionId, principal, vhost, remote);
        });
    }

    private KafkaPrincipal buildPrincipal(ChannelHandlerContext ctx) {
        InetSocketAddress remote = remoteAddress(ctx.channel().remoteAddress());
        HttpAuthenticationContext.Builder builder = new HttpAuthenticationContext.Builder()
                .clientAddress(remote.getAddress())
                .securityProtocol(securityProtocol);
        return principalBuilder.build(builder.build());
    }

    private static InetSocketAddress remoteAddress(SocketAddress sa) {
        return sa instanceof InetSocketAddress ? (InetSocketAddress) sa
                : new InetSocketAddress("0.0.0.0", 0);
    }

    private String webSocketLocation(ChannelHandlerContext ctx, FullHttpRequest req) {
        String host = req.headers().get(HttpHeaderNames.HOST);
        if (host == null || host.isEmpty()) {
            host = "localhost";
        }
        // TLS presence is inferred from the pipeline — an `ssl` handler means wss.
        boolean tls = ctx.pipeline().get("ssl") != null;
        return (tls ? "wss://" : "ws://") + host + req.uri();
    }

    private String buildConnectedJson(String sessionId) {
        // Hand-rolled JSON to avoid depending on Jackson in a hot path.  All fields
        // are plain ASCII or numeric — no escaping required for brokerId/clusterId
        // since clusterId comes from KRaft and is [0-9A-Za-z_-]+.
        StringBuilder sb = new StringBuilder(192);
        sb.append("{\"type\":\"connected\",")
          .append("\"brokerId\":").append(brokerId).append(',')
          .append("\"clusterId\":\"").append(clusterId).append("\",")
          .append("\"sessionId\":\"").append(sessionId).append("\",")
          .append("\"serverCapabilities\":[").append(SERVER_CAPABILITIES).append("]}");
        return sb.toString();
    }

    private String generateSessionId() {
        return "ws-" + brokerId + "-" + UUID.randomUUID().toString().substring(0, 8);
    }

    private static void removeIfPresent(ChannelPipeline pipeline, String name) {
        if (pipeline.get(name) != null) {
            pipeline.remove(name);
        }
    }

    private static void sendHttpErrorAndClose(ChannelHandlerContext ctx, HttpResponseStatus status) {
        FullHttpResponse resp = new DefaultFullHttpResponse(
                HttpVersion.HTTP_1_1, status, Unpooled.EMPTY_BUFFER);
        HttpUtil.setContentLength(resp, 0);
        ctx.writeAndFlush(resp).addListener(ChannelFutureListener.CLOSE);
    }

    /**
     * Placeholder frame handler installed at {@code ws-handler}.  TASK-WS1.04
     * ({@code WsFrameHandler}) will replace this with the real handler.
     */
    private static ChannelHandler placeholderFrameHandler() {
        return new ChannelInboundHandlerAdapter() {
            @Override
            public void channelRead(ChannelHandlerContext ctx, Object msg) {
                // Drop frames on the floor until WS1.04 is wired in.
                io.netty.util.ReferenceCountUtil.release(msg);
            }
        };
    }
}
