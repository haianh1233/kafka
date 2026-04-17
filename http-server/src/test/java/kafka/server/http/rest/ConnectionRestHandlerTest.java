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
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import kafka.server.http.ws.WsConnectionContext;
import kafka.server.http.ws.WsConnectionRegistry;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * // Time: Created - TASK-WS2.07
 */
class ConnectionRestHandlerTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private WsConnectionRegistry registry;
    private ConnectionRestHandler handler;

    @BeforeEach
    void setUp() {
        registry = new WsConnectionRegistry();
        handler = new ConnectionRestHandler(registry, /*brokerId*/ 7);
    }

    @Test
    void listConnections_empty_returnsEmptyArray() throws Exception {
        FullHttpResponse resp = handler.handleList();
        assertEquals(HttpResponseStatus.OK, resp.status());
        JsonNode body = parseBody(resp);
        assertTrue(body.has("connections"));
        assertTrue(body.get("connections").isArray());
        assertEquals(0, body.get("connections").size());
    }

    @Test
    void listConnections_returnsAllActive() throws Exception {
        registry.register("c1", newContext("c1", "alice", "/"));
        registry.register("c2", newContext("c2", "bob", "/v2"));

        FullHttpResponse resp = handler.handleList();
        assertEquals(HttpResponseStatus.OK, resp.status());
        JsonNode body = parseBody(resp);
        assertEquals(2, body.get("connections").size());

        // Each element must have the documented fields.
        JsonNode first = body.get("connections").get(0);
        assertTrue(first.has("connectionId"));
        assertTrue(first.has("brokerId"));
        assertEquals(7, first.get("brokerId").asInt());
        assertTrue(first.has("vhost"));
        assertTrue(first.has("principal"));
        assertTrue(first.has("connectedAt"));
        assertTrue(first.has("subscriptionCount"));
        assertTrue(first.has("protocol"));
        assertEquals("ws", first.get("protocol").asText());
        assertTrue(first.has("remoteAddress"));
    }

    @Test
    void getConnection_existing_returnsDetails() throws Exception {
        WsConnectionContext ctx = newContext("c1", "alice", "/");
        registry.register("c1", ctx);

        FullHttpResponse resp = handler.handleGet("c1");
        assertEquals(HttpResponseStatus.OK, resp.status());
        JsonNode body = parseBody(resp);

        assertEquals("c1", body.get("connectionId").asText());
        assertEquals("User:alice", body.get("principal").asText());
        assertEquals("/", body.get("vhost").asText());
        assertEquals("ws", body.get("protocol").asText());
        // detail fields
        assertTrue(body.has("publishConfirmsEnabled"));
        assertTrue(body.has("subscriptions"));
        assertTrue(body.get("subscriptions").isArray());
        assertEquals(0, body.get("subscriptions").size());
    }

    @Test
    void getConnection_includesSubscriptions() throws Exception {
        WsConnectionContext ctx = newContext("c1", "alice", "/");
        // Populate the connection's subscription map with two entries.
        ctx.subscriptions().put("sub-1", new Object());
        ctx.subscriptions().put("sub-2", new Object());
        registry.register("c1", ctx);

        FullHttpResponse resp = handler.handleGet("c1");
        JsonNode body = parseBody(resp);
        assertEquals(2, body.get("subscriptions").size());
        // The list contains the subscription IDs
        String arrText = body.get("subscriptions").toString();
        assertTrue(arrText.contains("sub-1"), arrText);
        assertTrue(arrText.contains("sub-2"), arrText);
        assertEquals(2, body.get("subscriptionCount").asInt());
    }

    @Test
    void getConnection_notFound_returns404() throws Exception {
        FullHttpResponse resp = handler.handleGet("nope");
        assertEquals(HttpResponseStatus.NOT_FOUND, resp.status());
        JsonNode body = parseBody(resp);
        assertTrue(body.get("errorMessage").asText().contains("nope"));
    }

    @Test
    void forceClose_sendsCloseFrame1001() throws Exception {
        Harness h = newHarness("c1", "alice", "/");
        registry.register("c1", h.wsCtx);

        FullHttpResponse resp = handler.handleForceClose(
            "c1", requestBody("{\"reason\":\"Administrative close\"}"));
        assertEquals(HttpResponseStatus.NO_CONTENT, resp.status());

        // Verify a CloseWebSocketFrame with code 1001 was written
        ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
        verify(h.channel).writeAndFlush(captor.capture());
        Object written = captor.getValue();
        assertTrue(written instanceof CloseWebSocketFrame,
            "expected CloseWebSocketFrame but got " + written.getClass());
        CloseWebSocketFrame frame = (CloseWebSocketFrame) written;
        assertEquals(1001, frame.statusCode());
        assertTrue(frame.reasonText().contains("Administrative close"),
            "reasonText=" + frame.reasonText());
    }

    @Test
    void forceClose_defaultReason_usedWhenBodyAbsent() throws Exception {
        Harness h = newHarness("c1", "alice", "/");
        registry.register("c1", h.wsCtx);

        FullHttpResponse resp = handler.handleForceClose("c1", requestBody(""));
        assertEquals(HttpResponseStatus.NO_CONTENT, resp.status());

        ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
        verify(h.channel).writeAndFlush(captor.capture());
        assertTrue(captor.getValue() instanceof CloseWebSocketFrame);
    }

    @Test
    void forceClose_notFound_returns404() throws Exception {
        FullHttpResponse resp = handler.handleForceClose(
            "nope", requestBody("{\"reason\":\"x\"}"));
        assertEquals(HttpResponseStatus.NOT_FOUND, resp.status());
    }

    @Test
    void forceClose_removesFromRegistry() {
        Harness h = newHarness("c1", "alice", "/");
        registry.register("c1", h.wsCtx);

        handler.handleForceClose("c1", requestBody("{\"reason\":\"bye\"}"));

        // After force-close, the registry must no longer contain the connection —
        // the admin tool should not have to poll for eventual cleanup.
        assertNotNull(registry);
        org.junit.jupiter.api.Assertions.assertFalse(registry.contains("c1"));
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    private static FullHttpRequest requestBody(String json) {
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        return new DefaultFullHttpRequest(
            HttpVersion.HTTP_1_1, HttpMethod.DELETE, "/v1/connections/x",
            Unpooled.wrappedBuffer(bytes));
    }

    private static JsonNode parseBody(FullHttpResponse resp) throws Exception {
        byte[] bytes = new byte[resp.content().readableBytes()];
        resp.content().getBytes(resp.content().readerIndex(), bytes);
        return MAPPER.readTree(bytes);
    }

    /** Builds a real WsConnectionContext backed by a mocked ChannelHandlerContext. */
    private WsConnectionContext newContext(String sessionId, String user, String vhost) {
        return newHarness(sessionId, user, vhost).wsCtx;
    }

    private Harness newHarness(String sessionId, String user, String vhost) {
        ChannelHandlerContext channel = mock(ChannelHandlerContext.class);
        Channel ch = mock(Channel.class);
        ChannelFuture writeFuture = mock(ChannelFuture.class);
        when(channel.channel()).thenReturn(ch);
        when(ch.isActive()).thenReturn(true);
        when(channel.writeAndFlush(any())).thenReturn(writeFuture);
        when(writeFuture.addListener(any())).thenReturn(writeFuture);

        WsConnectionContext wsCtx = new WsConnectionContext(
            sessionId,
            new KafkaPrincipal(KafkaPrincipal.USER_TYPE, user),
            vhost,
            channel,
            new InetSocketAddress("127.0.0.1", 4321));
        return new Harness(wsCtx, channel);
    }

    private static final class Harness {
        final WsConnectionContext wsCtx;
        final ChannelHandlerContext channel;

        Harness(WsConnectionContext wsCtx, ChannelHandlerContext channel) {
            this.wsCtx = wsCtx;
            this.channel = channel;
        }
    }
}
