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
// Time: Created - TASK-WS3.06
package kafka.server.http.ws;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import kafka.server.http.HttpRequestTranslator;
import kafka.server.http.routing.ExchangeManager;
import kafka.server.http.routing.RoutingEngine;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * End-to-end quota enforcement tests (TASK-WS3.06).
 *
 * <p>Verifies that:
 * <ul>
 *   <li>Produce byte-rate quota triggers a {@code QUOTA_EXCEEDED} error frame
 *       on {@link WsPublishHandler} and suppresses the downstream sink.</li>
 *   <li>Control message rate limit in {@link WsFrameHandler} emits the error
 *       frame at the configured threshold.</li>
 *   <li>Fetch byte-rate quota on {@link WsConsumerFetchLoop} applies a silent
 *       throttle with no error frame (design §19.4).</li>
 *   <li>Resource limit / connection counting behaves correctly across the
 *       managers.</li>
 * </ul>
 *
 * // Time: Created - TASK-WS3.06
 */
class WsQuotaEnforcementTest {

    private static final ObjectMapper MAPPER = HttpRequestTranslator.MAPPER;

    private ExchangeManager exchangeManager;
    private RoutingEngine routingEngine;
    private WsMessageSerializer messageSerializer;
    private ChannelHandlerContext mockChannelCtx;
    private WsConnectionContext connCtx;
    private final List<String> writtenFrames = new ArrayList<>();
    private int capturedSinkCount;

    @BeforeEach
    void setUp() {
        exchangeManager = mock(ExchangeManager.class);
        routingEngine = mock(RoutingEngine.class);
        messageSerializer = new WsMessageSerializer();
        capturedSinkCount = 0;

        mockChannelCtx = mock(ChannelHandlerContext.class);
        Channel mockChannel = mock(Channel.class);
        when(mockChannelCtx.channel()).thenReturn(mockChannel);
        when(mockChannel.isActive()).thenReturn(true);

        ChannelFuture future = mock(ChannelFuture.class);
        when(mockChannelCtx.writeAndFlush(any())).thenAnswer(inv -> {
            Object arg = inv.getArgument(0);
            if (arg instanceof TextWebSocketFrame) {
                TextWebSocketFrame f = (TextWebSocketFrame) arg;
                writtenFrames.add(f.text());
                f.release();
            }
            return future;
        });

        KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "alice");
        InetSocketAddress remote = new InetSocketAddress("127.0.0.1", 12345);
        connCtx = new WsConnectionContext(
            "ws-1-quota-test", principal, "/", mockChannelCtx, remote);
    }

    // ------------------------------------------------------------------
    //  Produce byte-rate quota (WsPublishHandler)
    // ------------------------------------------------------------------

    @Test
    void publishHandler_overQuota_emitsQuotaExceededFrame_andSkipsSink() throws Exception {
        AtomicLong captureBytes = new AtomicLong();
        WsQuotaManager.ByteRateQuotaChecker produce = (id, bytes) -> {
            captureBytes.set(bytes);
            return 500L; // always throttle
        };
        WsQuotaManager quotas = new WsQuotaManager(produce, null, 50);

        WsPublishHandler handler = new WsPublishHandler(
            exchangeManager, routingEngine, messageSerializer,
            queue -> "ws." + queue,
            (topic, serialized, pubId, confirms, ctx) -> capturedSinkCount++,
            null, null, null, quotas);

        when(exchangeManager.getExchange("/", "events"))
            .thenReturn(new ExchangeMetadata("events", "/", "direct", true, false, false, java.util.Map.of()));
        when(routingEngine.route(eq("events"), eq("k"), any()))
            .thenReturn(Set.of("q1"));

        handler.handlePublish(buildPublishFrame("events", "k", "payload", 77L, false), connCtx);

        assertEquals(0, capturedSinkCount, "throttled publish must not reach the sink");
        assertEquals(1, writtenFrames.size(), "one error frame expected");
        JsonNode frame = MAPPER.readTree(writtenFrames.get(0));
        assertEquals("error", frame.get("type").asText());
        assertEquals("QUOTA_EXCEEDED", frame.get("errorCode").asText());
        assertEquals(500L, frame.get("retryAfterMs").asLong(),
            "retryAfterMs echoes broker-supplied throttle");
        assertEquals(77L, frame.get("publishId").asLong(),
            "publishId echoed so the client can correlate");
        assertTrue(captureBytes.get() > 0, "quota checker should see a positive byte count");
    }

    @Test
    void publishHandler_underQuota_reachesSink_noErrorFrame() {
        WsQuotaManager.ByteRateQuotaChecker permissive = (id, bytes) -> 0L;
        WsQuotaManager quotas = new WsQuotaManager(permissive, null, 50);
        WsPublishHandler handler = new WsPublishHandler(
            exchangeManager, routingEngine, messageSerializer,
            queue -> "ws." + queue,
            (topic, serialized, pubId, confirms, ctx) -> capturedSinkCount++,
            null, null, null, quotas);

        when(exchangeManager.getExchange("/", "events"))
            .thenReturn(new ExchangeMetadata("events", "/", "direct", true, false, false, java.util.Map.of()));
        when(routingEngine.route(eq("events"), eq("k"), any()))
            .thenReturn(Set.of("q1"));

        handler.handlePublish(buildPublishFrame("events", "k", "payload", 1L, false), connCtx);

        assertEquals(1, capturedSinkCount, "under-quota publish enqueues normally");
        assertTrue(writtenFrames.isEmpty(), "no error frame expected, got: " + writtenFrames);
    }

    @Test
    void publishHandler_noQuotaManager_behavesAsPre306() {
        // 5-arg legacy constructor: no quota manager wired. Must not touch the
        // quota code path at all — the publish simply flows through.
        WsPublishHandler handler = new WsPublishHandler(
            exchangeManager, routingEngine, messageSerializer,
            queue -> "ws." + queue,
            (topic, serialized, pubId, confirms, ctx) -> capturedSinkCount++);

        when(exchangeManager.getExchange("/", "events"))
            .thenReturn(new ExchangeMetadata("events", "/", "direct", true, false, false, java.util.Map.of()));
        when(routingEngine.route(eq("events"), eq("k"), any()))
            .thenReturn(Set.of("q"));

        handler.handlePublish(buildPublishFrame("events", "k", "p", 1L, false), connCtx);

        assertEquals(1, capturedSinkCount);
        assertTrue(writtenFrames.isEmpty());
    }

    // ------------------------------------------------------------------
    //  Control message rate limit (WsFrameHandler)
    // ------------------------------------------------------------------

    @Test
    void frameHandler_underControlRate_dispatchesNormally() throws Exception {
        // 2 messages allowed per second — send 2 and watch them dispatch.
        WsQuotaManager quotas = new WsQuotaManager(null, null, 2);
        WsFrameHandler handler = new WsFrameHandler(
            connCtx, WsConfigs.withDefaults(), null, null, quotas);

        deliver(handler, "{\"type\":\"publish\",\"id\":\"r1\"}");
        deliver(handler, "{\"type\":\"publish\",\"id\":\"r2\"}");

        // Both messages trigger the publish stub (UnsupportedOperationException
        // → INTERNAL_ERROR), but no QUOTA_EXCEEDED.
        assertEquals(2, writtenFrames.size());
        for (String f : writtenFrames) {
            JsonNode frame = MAPPER.readTree(f);
            assertFalse("QUOTA_EXCEEDED".equals(frame.get("errorCode").asText()),
                "under limit must not trigger QUOTA_EXCEEDED: " + f);
        }
    }

    @Test
    void frameHandler_overControlRate_emitsQuotaExceeded() throws Exception {
        WsQuotaManager quotas = new WsQuotaManager(null, null, 1);
        WsFrameHandler handler = new WsFrameHandler(
            connCtx, WsConfigs.withDefaults(), null, null, quotas);

        deliver(handler, "{\"type\":\"publish\",\"id\":\"r1\"}");
        deliver(handler, "{\"type\":\"publish\",\"id\":\"r2\"}");

        assertEquals(2, writtenFrames.size());
        // First frame: INTERNAL_ERROR from the stub handler.
        JsonNode first = MAPPER.readTree(writtenFrames.get(0));
        assertEquals("INTERNAL_ERROR", first.get("errorCode").asText(),
            "1st message under limit triggers normal stub dispatch");
        // Second frame: QUOTA_EXCEEDED — rejected before dispatch.
        JsonNode second = MAPPER.readTree(writtenFrames.get(1));
        assertEquals("QUOTA_EXCEEDED", second.get("errorCode").asText(),
            "2nd message in the same second blocked by quota");
        assertTrue(second.has("retryAfterMs"), "quota frame carries retryAfterMs");
    }

    @Test
    void frameHandler_releasesQuotaStateOnChannelInactive() {
        WsQuotaManager quotas = new WsQuotaManager(null, null, 1);
        WsFrameHandler handler = new WsFrameHandler(
            connCtx, WsConfigs.withDefaults(), null, null, quotas);

        deliver(handler, "{\"type\":\"publish\"}");
        deliver(handler, "{\"type\":\"publish\"}");
        assertEquals(2, writtenFrames.size(),
            "first under limit, second rejected QUOTA_EXCEEDED");

        // Simulate channel close — handler should release the rate-limit state.
        handler.channelInactive(mockChannelCtx);

        // Fresh check on the same connection id — a new message is allowed.
        assertTrue(quotas.checkControlMessageRate(connCtx.sessionId()),
            "after channel close, quota state is released");
    }

    // ------------------------------------------------------------------
    //  Fetch byte-rate quota (WsConsumerFetchLoop)
    // ------------------------------------------------------------------

    @Test
    void fetchLoop_overQuota_appliesSilentThrottle() throws Exception {
        WsQuotaManager.ByteRateQuotaChecker fetch = (id, bytes) -> 30L;
        WsQuotaManager quotas = new WsQuotaManager(null, fetch, 50);

        // Minimal fetch loop — tests only the applyFetchThrottle path.
        WsConsumerFetchLoop loop = newLoopWithQuotas(quotas, "alice");
        long actual = loop.applyFetchThrottle(1024);

        assertEquals(30L, actual,
            "broker-suggested 30ms throttle is applied");
        // Critically: NO error frames written on the mock channel.
        assertTrue(writtenFrames.isEmpty(),
            "fetch throttle is SILENT — no error frame");
    }

    @Test
    void fetchLoop_underQuota_noThrottle() throws Exception {
        WsQuotaManager.ByteRateQuotaChecker fetch = (id, bytes) -> 0L;
        WsQuotaManager quotas = new WsQuotaManager(null, fetch, 50);
        WsConsumerFetchLoop loop = newLoopWithQuotas(quotas, "alice");

        long actual = loop.applyFetchThrottle(1024);
        assertEquals(0L, actual, "under-quota returns 0");
    }

    @Test
    void fetchLoop_noQuotaManager_noThrottle() throws Exception {
        WsConsumerFetchLoop loop = newLoopWithQuotas(null, null);
        long actual = loop.applyFetchThrottle(1024);
        assertEquals(0L, actual, "no quota manager → no throttle");
    }

    @Test
    void fetchLoop_quotaManagerButNullClientId_noThrottle() throws Exception {
        WsQuotaManager.ByteRateQuotaChecker fetch = (id, bytes) -> 30L;
        WsQuotaManager quotas = new WsQuotaManager(null, fetch, 50);
        // Null clientId disables the quota lookup.
        WsConsumerFetchLoop loop = newLoopWithQuotas(quotas, null);
        long actual = loop.applyFetchThrottle(1024);
        assertEquals(0L, actual, "null clientId disables the check");
    }

    private WsConsumerFetchLoop newLoopWithQuotas(WsQuotaManager quotas, String clientId) {
        Channel ch = mock(Channel.class);
        when(ch.isOpen()).thenReturn(true);
        WsCreditManager creditMgr = new WsCreditManager(10, ch);
        WsDeliveryTagTracker tracker = new WsDeliveryTagTracker();
        return new WsConsumerFetchLoop(
            "sub-1", "ws.test", java.util.Map.of(),
            creditMgr, tracker, ch, false, null, 0, quotas, clientId);
    }

    // ------------------------------------------------------------------
    //  Resource limit manager (cross-check with WsUpgradeOrHttpHandler close code)
    // ------------------------------------------------------------------

    @Test
    void closeCode4429_matchesSpec() {
        assertEquals(4429, WsUpgradeOrHttpHandler.CLOSE_CODE_CONNECTION_LIMIT,
            "WS close code 4429 mirrors HTTP 429 per design §19.5");
    }

    @Test
    void resourceLimitManager_tryReserveConnection_raceFreeUnderLimit() {
        WsResourceLimitManager m = new WsResourceLimitManager(10, 10, 10, 2);
        assertTrue(m.tryReserveConnection());
        assertTrue(m.tryReserveConnection());
        assertFalse(m.tryReserveConnection(),
            "third reservation must fail — cap is 2");
        // Release a slot and retry.
        m.recordConnectionClose();
        assertTrue(m.tryReserveConnection());
    }

    // ------------------------------------------------------------------
    //  Helpers
    // ------------------------------------------------------------------

    private void deliver(WsFrameHandler handler, String text) {
        TextWebSocketFrame frame = new TextWebSocketFrame(text);
        try {
            handler.channelRead(mockChannelCtx, frame);
        } catch (Exception e) {
            throw new AssertionError("channelRead threw", e);
        }
    }

    private ObjectNode buildPublishFrame(String exchange, String routingKey,
                                         String body, long publishId, boolean mandatory) {
        ObjectNode frame = MAPPER.createObjectNode();
        frame.put("type", "publish");
        frame.put("exchange", exchange);
        frame.put("routingKey", routingKey);
        frame.put("mandatory", mandatory);
        frame.put("publishId", publishId);
        ObjectNode message = frame.putObject("message");
        message.put("body", body);
        return frame;
    }

}
