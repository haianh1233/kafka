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
// Time: Created - TASK-WS3.07
package kafka.server.http.ws;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
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
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.util.ReferenceCountUtil;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.KafkaPrincipalBuilder;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.security.authenticator.DefaultKafkaPrincipalBuilder;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * End-to-end tests for the graceful shutdown drain mechanism introduced in
 * TASK-WS3.07. Exercises the interaction between
 * {@link WsUpgradeOrHttpHandler#beginDrain()} (reject new upgrades) and
 * {@link WsConnectionRegistry#drain(long)} (send close frame 1001, wait for
 * all connections to clear).
 *
 * // Time: Created - TASK-WS3.07
 */
class WsDrainTest {

    private static final String CLUSTER_ID = "test-cluster-abc";
    private static final int BROKER_ID = 7;

    // ------------------------------------------------------------------
    //  Upgrade-rejection during drain
    // ------------------------------------------------------------------

    @Test
    void newUpgradeDuringDrain_returns503() {
        WsConfigs cfg = WsConfigs.withDefaults();
        CapturingHandler capture = new CapturingHandler();
        KafkaPrincipalBuilder pb = new DefaultKafkaPrincipalBuilder(null, null);
        WsUpgradeOrHttpHandler handler = new WsUpgradeOrHttpHandler(
                cfg, BROKER_ID, CLUSTER_ID, pb, SecurityProtocol.HTTP);

        EmbeddedChannel ch = buildChannel(handler, capture);

        // Flip the drain flag before the upgrade request arrives.
        handler.beginDrain();
        assertTrue(handler.isDraining());

        ch.writeInbound(validUpgradeRequest());

        // The upgrade must NOT be forwarded and must NOT have completed.
        assertEquals(0, capture.captured.size(),
                "upgrade must not propagate once drain has started");

        String out = drainOutbound(ch);
        assertTrue(out.startsWith("HTTP/1.1 503"),
                "drain should respond with 503 Service Unavailable, got: " + out);

        ch.finishAndReleaseAll();
    }

    @Test
    void upgradeBeforeDrain_stillSucceeds() {
        WsConfigs cfg = WsConfigs.withDefaults();
        CapturingHandler capture = new CapturingHandler();
        KafkaPrincipalBuilder pb = new DefaultKafkaPrincipalBuilder(null, null);
        WsUpgradeOrHttpHandler handler = new WsUpgradeOrHttpHandler(
                cfg, BROKER_ID, CLUSTER_ID, pb, SecurityProtocol.HTTP);

        EmbeddedChannel ch = buildChannel(handler, capture);

        assertFalse(handler.isDraining(),
                "handler must not be draining before beginDrain is called");

        ch.writeInbound(validUpgradeRequest());
        // The handshake response should look like an HTTP 101 on the wire.
        String out = drainOutbound(ch);
        assertTrue(out.startsWith("HTTP/1.1 101"),
                "expected a 101 Switching Protocols, got: " + out);

        ch.finishAndReleaseAll();
    }

    @Test
    void beginDrain_isIdempotent() {
        WsConfigs cfg = WsConfigs.withDefaults();
        KafkaPrincipalBuilder pb = new DefaultKafkaPrincipalBuilder(null, null);
        WsUpgradeOrHttpHandler handler = new WsUpgradeOrHttpHandler(
                cfg, BROKER_ID, CLUSTER_ID, pb, SecurityProtocol.HTTP);

        handler.beginDrain();
        handler.beginDrain();
        handler.beginDrain();
        assertTrue(handler.isDraining());
    }

    // ------------------------------------------------------------------
    //  Registry drain semantics
    // ------------------------------------------------------------------

    @Test
    void registryDrain_closesAllWithCode1001_andClears() {
        WsConnectionRegistry registry = new WsConnectionRegistry();

        List<ChannelHandlerContext> chs = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            String id = "conn-" + i;
            ChannelHandlerContext ch = mock(ChannelHandlerContext.class);
            Channel ntty = mock(Channel.class);
            when(ch.channel()).thenReturn(ntty);
            when(ntty.isActive()).thenReturn(true);
            ChannelFuture fut = mock(ChannelFuture.class);
            // Simulate prompt close: the moment a CloseWebSocketFrame is written,
            // unregister this connection from the registry.
            when(ch.writeAndFlush(any())).thenAnswer(invocation -> {
                Object arg = invocation.getArgument(0);
                if (arg instanceof CloseWebSocketFrame) {
                    registry.unregister(id);
                }
                return fut;
            });
            WsConnectionContext ctx = new WsConnectionContext(
                    "sess-" + i,
                    new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "u"),
                    "/",
                    ch,
                    new InetSocketAddress("127.0.0.1", 10000 + i));
            registry.register(id, ctx);
            chs.add(ch);
        }

        assertEquals(4, registry.size());
        assertTrue(registry.drain(2000), "drain should succeed when all connections clear");
        assertEquals(0, registry.size(), "registry should be empty after successful drain");

        // Every connection received a close frame with code 1001.
        for (ChannelHandlerContext ch : chs) {
            ArgumentCaptor<CloseWebSocketFrame> captor = ArgumentCaptor.forClass(CloseWebSocketFrame.class);
            verify(ch, atLeastOnce()).writeAndFlush(captor.capture());
            CloseWebSocketFrame frame = captor.getValue();
            assertEquals(1001, frame.statusCode(),
                    "all close frames must use code 1001 (Going Away)");
            frame.release();
        }
    }

    @Test
    void registryDrain_timesOutWhenConnectionsDoNotClear() {
        WsConnectionRegistry registry = new WsConnectionRegistry();

        // Connection never unregisters itself — drain must bail after timeoutMs.
        ChannelHandlerContext ch = mock(ChannelHandlerContext.class);
        Channel ntty = mock(Channel.class);
        when(ch.channel()).thenReturn(ntty);
        when(ntty.isActive()).thenReturn(true);
        ChannelFuture fut = mock(ChannelFuture.class);
        when(ch.writeAndFlush(any())).thenReturn(fut);

        WsConnectionContext ctx = new WsConnectionContext(
                "sess-stuck",
                new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "u"),
                "/",
                ch,
                new InetSocketAddress("127.0.0.1", 20000));
        registry.register("stuck", ctx);

        long start = System.currentTimeMillis();
        assertFalse(registry.drain(250),
                "drain must return false when connections remain past timeout");
        long elapsed = System.currentTimeMillis() - start;
        assertTrue(elapsed >= 250, "drain must wait at least the timeout, elapsed=" + elapsed);
        assertTrue(elapsed < 2000, "drain must not wait far beyond timeout, elapsed=" + elapsed);
        assertEquals(1, registry.size(), "stuck connection still registered");
    }

    // ------------------------------------------------------------------
    //  Helpers
    // ------------------------------------------------------------------

    private static EmbeddedChannel buildChannel(WsUpgradeOrHttpHandler handler, CapturingHandler capture) {
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

    private static String drainOutbound(EmbeddedChannel ch) {
        StringBuilder sb = new StringBuilder();
        Object out;
        while ((out = ch.readOutbound()) != null) {
            if (out instanceof ByteBuf buf) {
                sb.append(buf.toString(StandardCharsets.UTF_8));
                buf.release();
            } else {
                ReferenceCountUtil.release(out);
            }
        }
        return sb.toString();
    }

    /** Captures inbound messages to assert upgrade-forwarding behavior. */
    private static final class CapturingHandler extends ChannelInboundHandlerAdapter {
        final List<Object> captured = new ArrayList<>();

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            captured.add(msg);
        }
    }
}
