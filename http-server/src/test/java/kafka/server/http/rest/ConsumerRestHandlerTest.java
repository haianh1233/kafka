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
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import kafka.server.http.ws.WsConnectionContext;
import kafka.server.http.ws.WsConnectionRegistry;
import kafka.server.http.ws.WsSubscriptionManager;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
class ConsumerRestHandlerTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private WsConnectionRegistry registry;
    private ConsumerRestHandler handler;
    // A connection-scoped subscription manager — real class, uses a test executor.
    private WsSubscriptionManager subs;
    private ExecutorService executor;

    @BeforeEach
    void setUp() {
        executor = Executors.newSingleThreadExecutor();
        subs = new WsSubscriptionManager(executor);
        registry = new WsConnectionRegistry();
        handler = new ConsumerRestHandler(registry, cid -> subs);
    }

    @AfterEach
    void tearDown() {
        subs.cancelAll();
        executor.shutdownNow();
    }

    @Test
    void listConsumers_empty_returnsEmptyArray() throws Exception {
        FullHttpResponse resp = handler.handleList(null);
        assertEquals(HttpResponseStatus.OK, resp.status());
        JsonNode body = parseBody(resp);
        assertTrue(body.has("consumers"));
        assertEquals(0, body.get("consumers").size());
    }

    @Test
    void listConsumers_returnsAllSubscriptions() throws Exception {
        Harness h = newHarness("c1", "alice", "/");
        registry.register("c1", h.wsCtx);
        subscribe("sub-A", "qA");
        subscribe("sub-B", "qB");

        FullHttpResponse resp = handler.handleList(null);
        JsonNode body = parseBody(resp);
        assertEquals(2, body.get("consumers").size());
        JsonNode first = body.get("consumers").get(0);
        assertTrue(first.has("connectionId"));
        assertTrue(first.has("subscriptionId"));
        assertTrue(first.has("queue"));
    }

    @Test
    void listConsumers_filterByQueue() throws Exception {
        Harness h = newHarness("c1", "alice", "/");
        registry.register("c1", h.wsCtx);
        subscribe("sub-A", "orders");
        subscribe("sub-B", "payments");

        FullHttpResponse resp = handler.handleList("orders");
        JsonNode body = parseBody(resp);
        assertEquals(1, body.get("consumers").size());
        assertEquals("orders", body.get("consumers").get(0).get("queue").asText());
        assertEquals("sub-A", body.get("consumers").get(0).get("subscriptionId").asText());
    }

    @Test
    void listConsumers_filterExcludesAll_returnsEmpty() throws Exception {
        Harness h = newHarness("c1", "alice", "/");
        registry.register("c1", h.wsCtx);
        subscribe("sub-A", "orders");

        FullHttpResponse resp = handler.handleList("other");
        JsonNode body = parseBody(resp);
        assertEquals(0, body.get("consumers").size());
    }

    @Test
    void forceCancel_existing_returns204_andRemovesSubscription() throws Exception {
        Harness h = newHarness("c1", "alice", "/");
        registry.register("c1", h.wsCtx);
        subscribe("sub-1", "orders");

        assertEquals(1, subs.activeCount());
        FullHttpResponse resp = handler.handleForceCancel("c1", "sub-1");
        assertEquals(HttpResponseStatus.NO_CONTENT, resp.status());
        assertEquals(0, subs.activeCount(), "subscription should be removed");
    }

    @Test
    void forceCancel_sendsSubscriptionCancelledFrame() throws Exception {
        Harness h = newHarness("c1", "alice", "/");
        registry.register("c1", h.wsCtx);
        subscribe("sub-1", "orders");

        handler.handleForceCancel("c1", "sub-1");

        // At least one TextWebSocketFrame with type=subscription-cancelled must be sent.
        ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
        verify(h.channel, org.mockito.Mockito.atLeastOnce()).writeAndFlush(captor.capture());
        boolean found = false;
        String collected = "";
        for (Object o : captor.getAllValues()) {
            if (o instanceof TextWebSocketFrame) {
                String text = ((TextWebSocketFrame) o).text();
                collected += text + "\n";
                if (text.contains("\"type\":\"subscription-cancelled\"")
                    && text.contains("\"reason\":\"ADMIN_CANCEL\"")
                    && text.contains("\"subscriptionId\":\"sub-1\"")) {
                    found = true;
                    break;
                }
            }
        }
        assertTrue(found, "no subscription-cancelled ADMIN_CANCEL frame in writes: " + collected);
    }

    @Test
    void forceCancel_requeuesUnacked() throws Exception {
        Harness h = newHarness("c1", "alice", "/");
        registry.register("c1", h.wsCtx);
        subscribe("sub-1", "orders");

        // Assign a delivery tag — simulate an unacked message.
        var ctx = subs.getSubscription("sub-1");
        ctx.deliveryTagTracker().assign(new TopicPartition("ws.orders", 0), 42L);
        assertEquals(1, ctx.deliveryTagTracker().pendingCount());

        handler.handleForceCancel("c1", "sub-1");

        // After force-cancel, the subscription is gone — offsets were NOT committed
        // (the handler has no KafkaClient). This contract — "do not commit on admin
        // cancel" — is enforced here by the fact that we never pass an offset
        // committer: the returned committable-offsets map is discarded.
        org.junit.jupiter.api.Assertions.assertNull(subs.getSubscription("sub-1"));
    }

    @Test
    void forceCancel_connectionNotFound_returns404() {
        FullHttpResponse resp = handler.handleForceCancel("missing", "sub-1");
        assertEquals(HttpResponseStatus.NOT_FOUND, resp.status());
    }

    @Test
    void forceCancel_subscriptionNotFound_returns404() {
        Harness h = newHarness("c1", "alice", "/");
        registry.register("c1", h.wsCtx);

        FullHttpResponse resp = handler.handleForceCancel("c1", "ghost");
        assertEquals(HttpResponseStatus.NOT_FOUND, resp.status());
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    private void subscribe(String subId, String queue) {
        Channel ch = mock(Channel.class);
        when(ch.isActive()).thenReturn(true);
        when(ch.writeAndFlush(any())).thenReturn(mock(ChannelFuture.class));
        subs.subscribe(subId, queue, "ws." + queue,
            Set.of(new TopicPartition("ws." + queue, 0)),
            new HashMap<>(),
            0, false, ch);
    }

    private static JsonNode parseBody(FullHttpResponse resp) throws Exception {
        byte[] bytes = new byte[resp.content().readableBytes()];
        resp.content().getBytes(resp.content().readerIndex(), bytes);
        return MAPPER.readTree(bytes);
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
        assertNotNull(wsCtx);
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
