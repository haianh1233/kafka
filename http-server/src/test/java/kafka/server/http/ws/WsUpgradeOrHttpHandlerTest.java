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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.websocketx.WebSocketFrameAggregator;
import io.netty.util.ReferenceCountUtil;
import org.apache.kafka.common.security.auth.HttpAuthenticationContext;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.KafkaPrincipalBuilder;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.security.authenticator.DefaultKafkaPrincipalBuilder;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link WsUpgradeOrHttpHandler} using Netty's {@link EmbeddedChannel}.
 *
 * <p>These tests cover:
 * <ul>
 *   <li>Static helpers {@code isWebSocketUpgrade} and {@code extractVhost}.</li>
 *   <li>Pass-through behavior for non-upgrade requests (plain HTTP GET, wrong path,
 *       wrong method, disabled feature).</li>
 *   <li>Successful WebSocket upgrade: pipeline rewiring, {@code connected} frame,
 *       session-id formatting, and frame-size propagation.</li>
 * </ul>
 *
 * // Time: Created - TASK-WS1.03
 */
class WsUpgradeOrHttpHandlerTest {

    private static final String CLUSTER_ID = "test-cluster-abc";
    private static final int BROKER_ID = 7;

    // ---------------------------------------------------------------------
    //  Static helper tests — isWebSocketUpgrade
    // ---------------------------------------------------------------------

    @Test
    void isWebSocketUpgrade_validUpgrade_returnsTrue() {
        FullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/v1/ws");
        req.headers().set(HttpHeaderNames.UPGRADE, HttpHeaderValues.WEBSOCKET);
        req.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.UPGRADE);
        try {
            assertTrue(WsUpgradeOrHttpHandler.isWebSocketUpgrade(req));
        } finally {
            req.release();
        }
    }

    @Test
    void isWebSocketUpgrade_noUpgradeHeader_returnsFalse() {
        FullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/v1/ws");
        try {
            assertFalse(WsUpgradeOrHttpHandler.isWebSocketUpgrade(req));
        } finally {
            req.release();
        }
    }

    @Test
    void isWebSocketUpgrade_missingConnectionHeader_returnsFalse() {
        FullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/v1/ws");
        req.headers().set(HttpHeaderNames.UPGRADE, HttpHeaderValues.WEBSOCKET);
        try {
            assertFalse(WsUpgradeOrHttpHandler.isWebSocketUpgrade(req));
        } finally {
            req.release();
        }
    }

    @Test
    void isWebSocketUpgrade_wrongPath_returnsFalse() {
        FullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/v1/topics");
        req.headers().set(HttpHeaderNames.UPGRADE, HttpHeaderValues.WEBSOCKET);
        req.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.UPGRADE);
        try {
            assertFalse(WsUpgradeOrHttpHandler.isWebSocketUpgrade(req));
        } finally {
            req.release();
        }
    }

    @Test
    void isWebSocketUpgrade_wrongMethod_returnsFalse() {
        FullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/v1/ws");
        req.headers().set(HttpHeaderNames.UPGRADE, HttpHeaderValues.WEBSOCKET);
        req.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.UPGRADE);
        try {
            assertFalse(WsUpgradeOrHttpHandler.isWebSocketUpgrade(req));
        } finally {
            req.release();
        }
    }

    @Test
    void isWebSocketUpgrade_caseInsensitiveHeaders_returnsTrue() {
        FullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/v1/ws");
        req.headers().set(HttpHeaderNames.UPGRADE, "WebSocket");
        req.headers().set(HttpHeaderNames.CONNECTION, "keep-alive, Upgrade");
        try {
            assertTrue(WsUpgradeOrHttpHandler.isWebSocketUpgrade(req));
        } finally {
            req.release();
        }
    }

    @Test
    void isWebSocketUpgrade_withQueryParams_returnsTrue() {
        FullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/v1/ws?vhost=/dev");
        req.headers().set(HttpHeaderNames.UPGRADE, HttpHeaderValues.WEBSOCKET);
        req.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.UPGRADE);
        try {
            assertTrue(WsUpgradeOrHttpHandler.isWebSocketUpgrade(req));
        } finally {
            req.release();
        }
    }

    // ---------------------------------------------------------------------
    //  Static helper tests — extractVhost
    // ---------------------------------------------------------------------

    @Test
    void extractVhost_present_returnsValue() {
        FullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/v1/ws?vhost=/production");
        try {
            assertEquals("/production", WsUpgradeOrHttpHandler.extractVhost(req));
        } finally {
            req.release();
        }
    }

    @Test
    void extractVhost_absent_returnsDefault() {
        FullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/v1/ws");
        try {
            assertEquals("/", WsUpgradeOrHttpHandler.extractVhost(req));
        } finally {
            req.release();
        }
    }

    @Test
    void extractVhost_empty_returnsDefault() {
        FullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/v1/ws?vhost=");
        try {
            assertEquals("/", WsUpgradeOrHttpHandler.extractVhost(req));
        } finally {
            req.release();
        }
    }

    @Test
    void extractVhost_multipleParams_returnsFirst() {
        FullHttpRequest req = new DefaultFullHttpRequest(
                HttpVersion.HTTP_1_1, HttpMethod.GET, "/v1/ws?vhost=/staging&foo=bar");
        try {
            assertEquals("/staging", WsUpgradeOrHttpHandler.extractVhost(req));
        } finally {
            req.release();
        }
    }

    // ---------------------------------------------------------------------
    //  channelRead0 — pass-through (non-upgrade) path
    // ---------------------------------------------------------------------

    @Test
    void channelRead0_plainHttpGet_forwardedToNextHandler() {
        CapturingHandler capture = new CapturingHandler();
        EmbeddedChannel ch = buildChannel(WsConfigs.withDefaults(), capture);

        FullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/v1/topics");
        ch.writeInbound(req);

        assertEquals(1, capture.captured.size(), "request should be forwarded exactly once");
        FullHttpRequest forwarded = (FullHttpRequest) capture.captured.get(0);
        assertEquals("/v1/topics", forwarded.uri());
        // No outbound response should have been written by the handler.
        assertNull(ch.readOutbound());
        ReferenceCountUtil.release(forwarded);
        ch.finishAndReleaseAll();
    }

    @Test
    void channelRead0_wsPathWithoutUpgrade_forwardedToNextHandler() {
        CapturingHandler capture = new CapturingHandler();
        EmbeddedChannel ch = buildChannel(WsConfigs.withDefaults(), capture);

        FullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/v1/ws");
        ch.writeInbound(req);

        assertEquals(1, capture.captured.size(), "request without Upgrade header should be forwarded");
        ReferenceCountUtil.release(capture.captured.get(0));
        ch.finishAndReleaseAll();
    }

    @Test
    void channelRead0_postOnWsPath_forwardedToNextHandler() {
        // POST with Upgrade headers must still be rejected as upgrade and forwarded
        // to the next handler (which would likely return 405 for real traffic).
        CapturingHandler capture = new CapturingHandler();
        EmbeddedChannel ch = buildChannel(WsConfigs.withDefaults(), capture);

        FullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/v1/ws");
        req.headers().set(HttpHeaderNames.UPGRADE, HttpHeaderValues.WEBSOCKET);
        req.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.UPGRADE);
        ch.writeInbound(req);

        assertEquals(1, capture.captured.size(), "POST on /v1/ws must not be upgraded");
        ReferenceCountUtil.release(capture.captured.get(0));
        ch.finishAndReleaseAll();
    }

    @Test
    void channelRead0_wsDisabled_returns404() {
        WsConfigs disabled = new WsConfigs(
                false,
                WsConfigs.withDefaults().maxFrameSize(),
                WsConfigs.withDefaults().maxSubscriptionsPerConnection(),
                WsConfigs.withDefaults().defaultCredits(),
                WsConfigs.withDefaults().maxCredits(),
                WsConfigs.withDefaults().topicPrefix(),
                WsConfigs.withDefaults().defaultQueuePartitions(),
                WsConfigs.withDefaults().metadataTopic(),
                WsConfigs.withDefaults().metadataReplicationFactor(),
                WsConfigs.withDefaults().ackCommitIntervalMs(),
                WsConfigs.withDefaults().consumerStartOffset(),
                WsConfigs.withDefaults().numConsumerThreads(),
                WsConfigs.withDefaults().consumerMaxWaitMs(),
                WsConfigs.withDefaults().consumerMaxBytes(),
                WsConfigs.withDefaults().publishTimeoutMs(),
                WsConfigs.withDefaults().connectionMaxIdleMs(),
                WsConfigs.withDefaults().shutdownDrainMs(),
                WsConfigs.withDefaults().maxRedeliveryCount(),
                WsConfigs.withDefaults().dedupEnabled(),
                WsConfigs.withDefaults().dedupCacheSize(),
                WsConfigs.withDefaults().dedupCacheTtlMs(),
                WsConfigs.withDefaults().maxConnectionsPerBroker(),
                WsConfigs.withDefaults().maxExchangesPerVhost(),
                WsConfigs.withDefaults().maxQueuesPerVhost(),
                WsConfigs.withDefaults().maxBindingsPerExchange(),
                WsConfigs.withDefaults().maxControlMessagesPerSecond(),
                WsConfigs.withDefaults().consumerAckTimeoutMs(),
                WsConfigs.withDefaults().metadataStartupTimeoutMs()
        );
        CapturingHandler capture = new CapturingHandler();
        EmbeddedChannel ch = buildChannel(disabled, capture);

        FullHttpRequest req = validUpgradeRequest();
        ch.writeInbound(req);

        assertEquals(0, capture.captured.size(), "upgrade should not be forwarded when disabled");
        // The outbound response is encoded by HttpServerCodec into one or more
        // ByteBufs. Collect the bytes and assert the status line.
        StringBuilder sb = new StringBuilder();
        Object out;
        while ((out = ch.readOutbound()) != null) {
            if (out instanceof ByteBuf buf) {
                sb.append(buf.toString(java.nio.charset.StandardCharsets.UTF_8));
                buf.release();
            }
        }
        assertTrue(sb.length() > 0, "expected an HTTP response body on the wire");
        assertTrue(sb.toString().startsWith("HTTP/1.1 404"),
                "expected a 404 status line, got: " + sb);
        ch.finishAndReleaseAll();
    }

    // ---------------------------------------------------------------------
    //  channelRead0 — successful upgrade path
    // ---------------------------------------------------------------------

    @Test
    void channelRead0_validUpgrade_rewiresPipelineAndSendsConnected() {
        CapturingHandler capture = new CapturingHandler();
        WsConfigs cfg = WsConfigs.withDefaults();
        EmbeddedChannel ch = buildChannel(cfg, capture);

        FullHttpRequest req = validUpgradeRequest();
        ch.writeInbound(req);

        // The request must have been consumed, not forwarded.
        assertEquals(0, capture.captured.size(), "upgrade request should not be forwarded downstream");

        ChannelPipeline pipeline = ch.pipeline();
        // Handshake writes the HTTP 101 response (consumed by handshake) then our
        // connected frame. Consume any outbound data to prevent leaks at channel
        // teardown.
        Object out;
        while ((out = ch.readOutbound()) != null) {
            ReferenceCountUtil.release(out);
        }

        // The upgrade/HTTP handlers should have been removed from the pipeline.
        assertNull(pipeline.get("http-aggregator"), "http-aggregator should be removed");
        assertNull(pipeline.get("compressor"), "compressor should be removed");
        assertNull(pipeline.get("ws-or-http"), "ws-or-http should have removed itself");
        // And the WebSocket handlers should have been added.
        assertNotNull(pipeline.get("ws-frame-aggregator"), "ws-frame-aggregator should be added");
        assertNotNull(pipeline.get("ws-handler"), "ws-handler should be added");

        ch.finishAndReleaseAll();
    }

    @Test
    void channelRead0_validUpgrade_maxFrameSizePropagates() throws Exception {
        // Build a WsConfigs with a distinctive max frame size and verify it lands on
        // the WebSocketFrameAggregator installed by the upgrade handler.
        int customMax = 123_456;
        WsConfigs cfg = withMaxFrameSize(customMax);
        CapturingHandler capture = new CapturingHandler();
        EmbeddedChannel ch = buildChannel(cfg, capture);

        FullHttpRequest req = validUpgradeRequest();
        ch.writeInbound(req);

        // Drain outbound to avoid leaks.
        Object out;
        while ((out = ch.readOutbound()) != null) {
            ReferenceCountUtil.release(out);
        }

        WebSocketFrameAggregator agg =
                (WebSocketFrameAggregator) ch.pipeline().get("ws-frame-aggregator");
        assertNotNull(agg, "frame aggregator should be installed");
        assertEquals(customMax, agg.maxContentLength(),
                "max frame size from WsConfigs must propagate");

        ch.finishAndReleaseAll();
    }

    // ---------------------------------------------------------------------
    //  Helpers
    // ---------------------------------------------------------------------

    private static EmbeddedChannel buildChannel(WsConfigs cfg, CapturingHandler capture) {
        // DefaultKafkaPrincipalBuilder yields ANONYMOUS for HTTP context without auth,
        // which is sufficient for these tests — we only assert upgrade mechanics.
        KafkaPrincipalBuilder pb = new DefaultKafkaPrincipalBuilder(null, null);
        WsUpgradeOrHttpHandler handler = new WsUpgradeOrHttpHandler(
                cfg, BROKER_ID, CLUSTER_ID, pb, SecurityProtocol.HTTP);
        // Pipeline shape mirrors the production HTTP/1.1 pipeline so the handler
        // can find and remove "http-aggregator"/"compressor". Each handler is given
        // a unique name so it can be removed by name from the pipeline. An
        // HttpServerCodec at the head is required for Netty's WebSocket handshaker
        // to write the 101 response.
        EmbeddedChannel ch = new EmbeddedChannel();
        ChannelPipeline p = ch.pipeline();
        p.addLast("http-codec", new io.netty.handler.codec.http.HttpServerCodec());
        p.addLast("http-aggregator", new HttpObjectAggregator(65536));
        p.addLast("compressor", new io.netty.handler.codec.http.HttpContentCompressor());
        p.addLast("ws-or-http", handler);
        p.addLast("downstream", capture);
        return ch;
    }

    private static FullHttpRequest validUpgradeRequest() {
        FullHttpRequest req = new DefaultFullHttpRequest(
                HttpVersion.HTTP_1_1, HttpMethod.GET, "/v1/ws", Unpooled.EMPTY_BUFFER);
        req.headers().set(HttpHeaderNames.HOST, "localhost");
        req.headers().set(HttpHeaderNames.UPGRADE, HttpHeaderValues.WEBSOCKET);
        req.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.UPGRADE);
        req.headers().set(HttpHeaderNames.SEC_WEBSOCKET_KEY, "dGhlIHNhbXBsZSBub25jZQ==");
        req.headers().set(HttpHeaderNames.SEC_WEBSOCKET_VERSION, "13");
        req.headers().set(HttpHeaderNames.ORIGIN, "http://localhost");
        return req;
    }

    private static WsConfigs withMaxFrameSize(int max) {
        WsConfigs d = WsConfigs.withDefaults();
        return new WsConfigs(
                d.wsEnabled(),
                max,
                d.maxSubscriptionsPerConnection(),
                d.defaultCredits(),
                d.maxCredits(),
                d.topicPrefix(),
                d.defaultQueuePartitions(),
                d.metadataTopic(),
                d.metadataReplicationFactor(),
                d.ackCommitIntervalMs(),
                d.consumerStartOffset(),
                d.numConsumerThreads(),
                d.consumerMaxWaitMs(),
                d.consumerMaxBytes(),
                d.publishTimeoutMs(),
                d.connectionMaxIdleMs(),
                d.shutdownDrainMs(),
                d.maxRedeliveryCount(),
                d.dedupEnabled(),
                d.dedupCacheSize(),
                d.dedupCacheTtlMs(),
                d.maxConnectionsPerBroker(),
                d.maxExchangesPerVhost(),
                d.maxQueuesPerVhost(),
                d.maxBindingsPerExchange(),
                d.maxControlMessagesPerSecond(),
                d.consumerAckTimeoutMs(),
                d.metadataStartupTimeoutMs());
    }

    /** Captures all inbound messages so assertions can verify forwarding. */
    private static final class CapturingHandler extends ChannelInboundHandlerAdapter {
        final List<Object> captured = new ArrayList<>();

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            captured.add(msg);
            // Do NOT propagate further; EmbeddedChannel tail would otherwise auto-release.
        }
    }

    // Suppress unused-warning: referenced only to force static init of the type we import.
    @SuppressWarnings("unused")
    private static final Class<?> HTTP_AUTH_CTX = HttpAuthenticationContext.class;

    // Suppress unused-warning: referenced only to keep the import for future tests.
    @SuppressWarnings("unused")
    private static final Class<?> KAFKA_PRINCIPAL = KafkaPrincipal.class;
}
