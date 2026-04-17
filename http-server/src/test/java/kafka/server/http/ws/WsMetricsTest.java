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
// Time: Created - TASK-WS3.08
package kafka.server.http.ws;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.yammer.metrics.core.MetricName;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import kafka.server.http.HttpRequestTranslator;
import kafka.server.http.routing.ExchangeManager;
import kafka.server.http.routing.RoutingEngine;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.server.metrics.KafkaYammerMetrics;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link WsMetrics}. Follows the pattern of
 * {@link kafka.server.http.HttpMetricsTest}.
 */
class WsMetricsTest {

    private WsMetrics metrics;

    @BeforeEach
    void setUp() {
        metrics = new WsMetrics();
    }

    @AfterEach
    void tearDown() {
        if (metrics != null) {
            metrics.close();
        }
    }

    // ------------------------------------------------------------------
    //  Meters — all must be registered with protocol=ws tag
    // ------------------------------------------------------------------

    @Test
    void publishRate_registered_andMarkable() {
        assertNotNull(metrics.publishRate);
        long before = metrics.publishRate.count();
        metrics.publishRate.mark();
        assertTrue(metrics.publishRate.count() > before);
    }

    @Test
    void deliverRate_registered_andMarkable() {
        assertNotNull(metrics.deliverRate);
        metrics.deliverRate.mark();
        metrics.deliverRate.mark();
        assertEquals(2, metrics.deliverRate.count());
    }

    @Test
    void ackRate_registered_andMarkable() {
        assertNotNull(metrics.ackRate);
        metrics.ackRate.mark();
        assertTrue(metrics.ackRate.count() > 0);
    }

    @Test
    void nackRate_registered_andMarkable() {
        assertNotNull(metrics.nackRate);
        metrics.nackRate.mark();
        assertTrue(metrics.nackRate.count() > 0);
    }

    @Test
    void errorRate_registered_andMarkable() {
        assertNotNull(metrics.errorRate);
        metrics.errorRate.mark();
        assertTrue(metrics.errorRate.count() > 0);
    }

    @Test
    void creditExhaustedRate_registered_andMarkable() {
        assertNotNull(metrics.creditExhaustedRate);
        metrics.creditExhaustedRate.mark();
        assertTrue(metrics.creditExhaustedRate.count() > 0);
    }

    @Test
    void redeliveryRate_registered_andMarkable() {
        assertNotNull(metrics.redeliveryRate);
        metrics.redeliveryRate.mark();
        assertTrue(metrics.redeliveryRate.count() > 0);
    }

    @Test
    void dlxRate_registered_andMarkable() {
        assertNotNull(metrics.dlxRate);
        metrics.dlxRate.mark();
        assertTrue(metrics.dlxRate.count() > 0);
    }

    @Test
    void mandatoryReturnRate_registered_andMarkable() {
        assertNotNull(metrics.mandatoryReturnRate);
        metrics.mandatoryReturnRate.mark();
        assertTrue(metrics.mandatoryReturnRate.count() > 0);
    }

    @Test
    void confirmRate_registered_andMarkable() {
        assertNotNull(metrics.confirmRate);
        metrics.confirmRate.mark();
        assertTrue(metrics.confirmRate.count() > 0);
    }

    @Test
    void controlMessageRate_registered_andMarkable() {
        assertNotNull(metrics.controlMessageRate);
        metrics.controlMessageRate.mark();
        assertTrue(metrics.controlMessageRate.count() > 0);
    }

    @Test
    void atLeastElevenMeters_registered() {
        // Design doc §11.5 mandates 11+ meters with protocol=ws.
        long wsMeterCount = KafkaYammerMetrics.defaultRegistry().allMetrics().entrySet().stream()
            .filter(e -> e.getKey().getMBeanName().contains("protocol=ws"))
            .filter(e -> e.getValue() instanceof com.yammer.metrics.core.Meter)
            .count();
        assertTrue(wsMeterCount >= 11,
            "Expected at least 11 meters with protocol=ws; found " + wsMeterCount);
    }

    @Test
    void allMeters_haveProtocolWsTag() {
        // Every metric of type Meter under this package must carry protocol=ws.
        KafkaYammerMetrics.defaultRegistry().allMetrics().forEach((name, metric) -> {
            if (metric instanceof com.yammer.metrics.core.Meter
                && name.getGroup().equals("kafka.server.http.ws")
                && name.getType().equals("WsMetrics")) {
                assertTrue(name.getMBeanName().contains("protocol=ws"),
                    "meter " + name.getMBeanName() + " missing protocol=ws tag");
            }
        });
    }

    // ------------------------------------------------------------------
    //  Gauge registration
    // ------------------------------------------------------------------

    @Test
    void connectionCountGauge_registersAndReads() {
        AtomicInteger connCount = new AtomicInteger(3);
        metrics.registerConnectionCountGauge(connCount::get);
        boolean found = findGauge("ConnectionCount", "protocol=ws");
        assertTrue(found, "ConnectionCount gauge should be registered");
    }

    @Test
    void subscriptionCountGauge_registersAndReads() {
        AtomicInteger subCount = new AtomicInteger(5);
        metrics.registerSubscriptionCountGauge(subCount::get);
        boolean found = findGauge("SubscriptionCount", "protocol=ws");
        assertTrue(found, "SubscriptionCount gauge should be registered");
    }

    @Test
    void queueDepthGauge_registeredOnQueueCreate() {
        AtomicLong depth = new AtomicLong(42L);
        metrics.registerQueueDepthGauge("orders", depth::get);

        boolean found = KafkaYammerMetrics.defaultRegistry().allMetrics().entrySet().stream()
            .anyMatch(e -> e.getKey().getName().equals("QueueDepth")
                && e.getKey().getMBeanName().contains("queue=orders")
                && e.getKey().getMBeanName().contains("protocol=ws"));
        assertTrue(found, "QueueDepth gauge for queue 'orders' should be registered");
    }

    @Test
    void queueConsumersGauge_registeredOnQueueCreate() {
        AtomicInteger cons = new AtomicInteger(2);
        metrics.registerQueueConsumersGauge("orders", cons::get);

        boolean found = KafkaYammerMetrics.defaultRegistry().allMetrics().entrySet().stream()
            .anyMatch(e -> e.getKey().getName().equals("QueueConsumers")
                && e.getKey().getMBeanName().contains("queue=orders")
                && e.getKey().getMBeanName().contains("protocol=ws"));
        assertTrue(found, "QueueConsumers gauge for queue 'orders' should be registered");
    }

    @Test
    void queueGauges_removedOnQueueDelete() {
        metrics.registerQueueDepthGauge("q1", () -> 10L);
        metrics.registerQueueConsumersGauge("q1", () -> 1);

        metrics.removeQueueGauges("q1");

        boolean found = KafkaYammerMetrics.defaultRegistry().allMetrics().entrySet().stream()
            .anyMatch(e -> (e.getKey().getName().equals("QueueDepth")
                || e.getKey().getName().equals("QueueConsumers"))
                && e.getKey().getMBeanName().contains("queue=q1"));
        assertFalse(found, "Queue gauges for 'q1' should be removed after removeQueueGauges");
    }

    @Test
    void multipleQueueGauges_registeredIndependently() {
        metrics.registerQueueDepthGauge("q1", () -> 1L);
        metrics.registerQueueDepthGauge("q2", () -> 2L);
        metrics.registerQueueDepthGauge("q3", () -> 3L);

        long count = KafkaYammerMetrics.defaultRegistry().allMetrics().entrySet().stream()
            .filter(e -> e.getKey().getName().equals("QueueDepth"))
            .count();
        assertEquals(3, count, "three QueueDepth gauges should be registered");
    }

    // ------------------------------------------------------------------
    //  close() cleanup
    // ------------------------------------------------------------------

    @Test
    void close_removesAllMetrics() {
        metrics.registerConnectionCountGauge(() -> 1);
        metrics.registerSubscriptionCountGauge(() -> 1);
        metrics.registerQueueDepthGauge("qA", () -> 0L);
        metrics.registerQueueConsumersGauge("qA", () -> 0);

        metrics.close();
        metrics = null; // prevent double-close in tearDown

        long remaining = KafkaYammerMetrics.defaultRegistry().allMetrics().entrySet().stream()
            .filter(e -> e.getKey().getMBeanName().contains("protocol=ws"))
            .count();
        assertEquals(0, remaining,
            "All WS metrics should be deregistered after close()");
    }

    // ------------------------------------------------------------------
    //  Integration: handlers drive the metrics
    // ------------------------------------------------------------------

    @Test
    void publishHandler_successfulPublish_incrementsPublishRate() {
        ExchangeManager em = mock(ExchangeManager.class);
        RoutingEngine re = mock(RoutingEngine.class);
        when(em.getExchange("/", "ex"))
            .thenReturn(new ExchangeMetadata("ex", "/", "direct", true, false, false, java.util.Map.of()));
        when(re.route(eq("ex"), eq("rk"), any())).thenReturn(Set.of("q"));

        WsPublishHandler.ProduceRequestSink sink = (topic, s, pid, c, ctx) -> { };
        WsPublishHandler handler = new WsPublishHandler(em, re, new WsMessageSerializer(),
            q -> "ws." + q, sink, metrics);

        WsConnectionContext connCtx = newConnectionContext();

        long before = metrics.publishRate.count();
        handler.handlePublish(buildPublishFrame("ex", "rk", "hello", 1L, false), connCtx);
        assertEquals(before + 1, metrics.publishRate.count(),
            "publishRate must tick once per successful enqueue");
    }

    @Test
    void publishHandler_mandatoryNoRoute_incrementsMandatoryReturnRate() {
        ExchangeManager em = mock(ExchangeManager.class);
        RoutingEngine re = mock(RoutingEngine.class);
        when(em.getExchange("/", "ex"))
            .thenReturn(new ExchangeMetadata("ex", "/", "direct", true, false, false, java.util.Map.of()));
        when(re.route(eq("ex"), eq("nowhere"), any())).thenReturn(Set.of());

        WsPublishHandler.ProduceRequestSink sink = (topic, s, pid, c, ctx) -> { };
        WsPublishHandler handler = new WsPublishHandler(em, re, new WsMessageSerializer(),
            q -> "ws." + q, sink, metrics);

        long before = metrics.mandatoryReturnRate.count();
        handler.handlePublish(buildPublishFrame("ex", "nowhere", "x", 1L, true), newConnectionContext());
        assertEquals(before + 1, metrics.mandatoryReturnRate.count(),
            "mandatoryReturnRate must tick on NO_ROUTE mandatory returns");
    }

    @Test
    void publishHandler_unknownExchange_incrementsErrorRate() {
        ExchangeManager em = mock(ExchangeManager.class);
        RoutingEngine re = mock(RoutingEngine.class);
        when(em.getExchange("/", "ghost")).thenReturn(null);

        WsPublishHandler.ProduceRequestSink sink = (topic, s, pid, c, ctx) -> { };
        WsPublishHandler handler = new WsPublishHandler(em, re, new WsMessageSerializer(),
            q -> "ws." + q, sink, metrics);

        long before = metrics.errorRate.count();
        handler.handlePublish(buildPublishFrame("ghost", "rk", "x", 1L, false), newConnectionContext());
        assertEquals(before + 1, metrics.errorRate.count(),
            "errorRate must tick when publish emits an error frame");
    }

    @Test
    void consumerFetchLoop_deliverRecord_incrementsDeliverRate() {
        Channel ch = mock(Channel.class);
        ChannelFuture future = mock(ChannelFuture.class);
        when(ch.isWritable()).thenReturn(true);
        when(ch.isOpen()).thenReturn(true);
        when(ch.writeAndFlush(any())).thenReturn(future);
        WsCreditManager cm = new WsCreditManager(10, ch);
        WsDeliveryTagTracker tt = new WsDeliveryTagTracker();
        TopicPartition tp = new TopicPartition("ws.orders", 0);

        WsConsumerFetchLoop loop = new WsConsumerFetchLoop(
            "sub-1", "ws.orders", java.util.Map.of(tp, 0L), cm, tt, ch, false, metrics);

        long before = metrics.deliverRate.count();
        loop.deliverRecord(tp, 5L, "ex", "rk", "{}", false);
        assertEquals(before + 1, metrics.deliverRate.count(),
            "deliverRate must tick on deliverRecord");

        long redBefore = metrics.redeliveryRate.count();
        loop.deliverRecord(tp, 6L, "ex", "rk", "{}", true);
        assertEquals(redBefore + 1, metrics.redeliveryRate.count(),
            "redeliveryRate must tick when redelivered=true");
    }

    @Test
    void ackHandler_handleAck_incrementsAckRate() throws Exception {
        Channel ch = mock(Channel.class);
        ChannelFuture future = mock(ChannelFuture.class);
        when(ch.isWritable()).thenReturn(true);
        when(ch.isOpen()).thenReturn(true);
        when(ch.writeAndFlush(any())).thenReturn(future);
        java.util.concurrent.ExecutorService exec = java.util.concurrent.Executors.newSingleThreadExecutor();
        try {
            WsSubscriptionManager sm = new WsSubscriptionManager(exec);
            TopicPartition tp = new TopicPartition("ws.orders", 0);
            sm.subscribe("sub-1", "orders", "ws.orders", Set.of(tp), java.util.Map.of(tp, 0L), 0, false, ch);
            SubscriptionContext sctx = sm.getSubscription("sub-1");
            long tag = sctx.deliveryTagTracker().assign(tp, 10L);

            WsAckHandler ackH = new WsAckHandler(sm, 0L, (id, offs) -> { }, metrics);
            long before = metrics.ackRate.count();
            ackH.handleAck("sub-1", tag, false);
            assertEquals(before + 1, metrics.ackRate.count(), "ackRate must tick on successful ack");

            long nackBefore = metrics.nackRate.count();
            long dlxBefore = metrics.dlxRate.count();
            long tag2 = sctx.deliveryTagTracker().assign(tp, 11L);
            ackH.handleNack("sub-1", tag2, false, false); // requeue=false → DLX path
            assertEquals(nackBefore + 1, metrics.nackRate.count(), "nackRate must tick on nack");
            assertEquals(dlxBefore + 1, metrics.dlxRate.count(),
                "dlxRate must tick when nack has requeue=false");

            ackH.stop();
            sm.cancelAll();
        } finally {
            exec.shutdownNow();
            exec.awaitTermination(2, java.util.concurrent.TimeUnit.SECONDS);
        }
    }

    @Test
    void frameHandler_inboundFrame_incrementsControlMessageRate() throws Exception {
        ChannelHandlerContext ctxMock = mock(ChannelHandlerContext.class);
        Channel ch = mock(Channel.class);
        when(ctxMock.channel()).thenReturn(ch);
        when(ch.isActive()).thenReturn(true);
        ChannelFuture future = mock(ChannelFuture.class);
        when(ctxMock.writeAndFlush(any())).thenReturn(future);

        KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "alice");
        WsConnectionContext connCtx = new WsConnectionContext(
            "ws-m-test", principal, "/", ctxMock, new InetSocketAddress("127.0.0.1", 12345));

        WsFrameHandler handler = new WsFrameHandler(connCtx, WsConfigs.withDefaults(), metrics);

        long before = metrics.controlMessageRate.count();
        // Send a minimal valid enable-confirms frame — dispatch succeeds without stubs.
        TextWebSocketFrame frame = new TextWebSocketFrame("{\"type\":\"enable-confirms\"}");
        handler.channelRead0(ctxMock, frame);
        assertEquals(before + 1, metrics.controlMessageRate.count(),
            "controlMessageRate must tick on every inbound frame");

        long errBefore = metrics.errorRate.count();
        // Malformed JSON triggers an error frame — that should bump errorRate.
        TextWebSocketFrame bad = new TextWebSocketFrame("not json");
        handler.channelRead0(ctxMock, bad);
        assertTrue(metrics.errorRate.count() > errBefore,
            "errorRate must tick when frame handler emits an error frame");
    }

    // ------------------------------------------------------------------
    //  Helpers
    // ------------------------------------------------------------------

    private boolean findGauge(String gaugeName, String tagSubstring) {
        for (MetricName n : KafkaYammerMetrics.defaultRegistry().allMetrics().keySet()) {
            if (n.getName().equals(gaugeName) && n.getMBeanName().contains(tagSubstring)) {
                return true;
            }
        }
        return false;
    }

    private WsConnectionContext newConnectionContext() {
        ChannelHandlerContext ctxMock = mock(ChannelHandlerContext.class);
        Channel ch = mock(Channel.class);
        when(ctxMock.channel()).thenReturn(ch);
        when(ch.isActive()).thenReturn(true);
        ChannelFuture future = mock(ChannelFuture.class);
        when(ctxMock.writeAndFlush(any())).thenReturn(future);
        KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "alice");
        return new WsConnectionContext(
            "ws-m-test", principal, "/", ctxMock, new InetSocketAddress("127.0.0.1", 12345));
    }

    private static ObjectNode buildPublishFrame(String exchange, String rk, String body,
                                                long publishId, boolean mandatory) {
        ObjectMapper m = HttpRequestTranslator.MAPPER;
        ObjectNode frame = m.createObjectNode();
        frame.put("type", "publish");
        frame.put("exchange", exchange);
        frame.put("routingKey", rk);
        frame.put("publishId", publishId);
        frame.put("mandatory", mandatory);
        ObjectNode msg = frame.putObject("message");
        msg.put("body", body.getBytes(StandardCharsets.UTF_8));
        return frame;
    }
}
