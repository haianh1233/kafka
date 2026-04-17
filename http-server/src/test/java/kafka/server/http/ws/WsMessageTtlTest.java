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

// Time: Created - TASK-WS4.02 - per-message TTL check at delivery

package kafka.server.http.ws;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for per-message TTL check at WebSocket delivery time.
 *
 * <p>Covers:
 * <ul>
 *   <li>{@link WsMessageTtl#isExpired} pure TTL predicate semantics.</li>
 *   <li>Round-trip of the {@code _ws_expiration} header produced by
 *       {@link WsMessageSerializer} and consumed by {@link WsMessageDeserializer}.</li>
 *   <li>{@link WsConsumerFetchLoop#maybeDeliverRecord} delivery / skip behaviour:
 *       expired messages are dropped, non-expired are delivered normally, and the
 *       consumer offset watermark advances in both cases so expired records are
 *       not redelivered.</li>
 * </ul>
 *
 * <p>Per-queue TTL ({@code x-message-ttl} → Kafka {@code retention.ms}) is
 * storage-level and validated in {@link #perQueueTtl_mapsToRetentionMs} via the
 * {@link WsMessageTtl#retentionMsFromQueueArgs} helper.
 */
class WsMessageTtlTest {

    private Channel channel;
    private WsCreditManager creditManager;
    private WsDeliveryTagTracker tagTracker;
    private final TopicPartition tp0 = new TopicPartition("ws.orders", 0);

    @BeforeEach
    void setUp() {
        channel = mock(Channel.class);
        ChannelFuture future = mock(ChannelFuture.class);
        when(channel.isWritable()).thenReturn(true);
        when(channel.isOpen()).thenReturn(true);
        when(channel.writeAndFlush(any())).thenReturn(future);
        creditManager = new WsCreditManager(10, channel);
        tagTracker = new WsDeliveryTagTracker();
    }

    // ---- WsMessageTtl.isExpired semantics ----

    @Test
    void isExpired_noHeader_returnsFalse() {
        assertFalse(WsMessageTtl.isExpired(Collections.emptyList(), 1_000L, 10_000_000L));
    }

    @Test
    void isExpired_headerPresent_notExpired() {
        List<Header> headers = List.of(expirationHeader("60000")); // 60s TTL
        // record produced 10s ago: 10s < 60s → not expired
        long ts = 1_000L;
        long now = 1_000L + 10_000L;
        assertFalse(WsMessageTtl.isExpired(headers, ts, now));
    }

    @Test
    void isExpired_headerPresent_expired() {
        List<Header> headers = List.of(expirationHeader("5000")); // 5s TTL
        // record produced 10s ago: 10s > 5s → expired
        long ts = 1_000L;
        long now = 1_000L + 10_000L;
        assertTrue(WsMessageTtl.isExpired(headers, ts, now));
    }

    @Test
    void isExpired_headerPresent_exactlyAtTtl_notExpired() {
        // Edge case: age == ttlMs should be "not expired" (strict > semantics per spec).
        List<Header> headers = List.of(expirationHeader("5000"));
        long ts = 1_000L;
        long now = 1_000L + 5_000L;
        assertFalse(WsMessageTtl.isExpired(headers, ts, now));
    }

    @Test
    void isExpired_malformedHeader_returnsFalse() {
        // Non-numeric expiration → fail-open: deliver rather than silently drop.
        List<Header> headers = List.of(expirationHeader("not-a-number"));
        assertFalse(WsMessageTtl.isExpired(headers, 1L, 10_000_000L));
    }

    @Test
    void isExpired_emptyHeader_returnsFalse() {
        List<Header> headers = List.of(expirationHeader(""));
        assertFalse(WsMessageTtl.isExpired(headers, 1L, 10_000_000L));
    }

    @Test
    void isExpired_negativeTtl_returnsFalse() {
        // Negative TTL is nonsensical → treat as "no TTL".
        List<Header> headers = List.of(expirationHeader("-1"));
        assertFalse(WsMessageTtl.isExpired(headers, 1L, 10_000_000L));
    }

    @Test
    void isExpired_zeroTtl_anyAgeIsExpired() {
        // TTL=0 means "deliver only if fetched at exactly the produce time".
        List<Header> headers = List.of(expirationHeader("0"));
        // age = 1ms > 0 → expired
        assertTrue(WsMessageTtl.isExpired(headers, 1_000L, 1_001L));
    }

    @Test
    void isExpired_clockSkew_negativeAge_notExpired() {
        // now < recordTimestamp (clock skew or future-dated record).
        // Age is negative, never expired regardless of TTL.
        List<Header> headers = List.of(expirationHeader("1000"));
        assertFalse(WsMessageTtl.isExpired(headers, 10_000L, 1_000L));
    }

    // ---- Header round-trip: serializer writes, deserializer reads ----

    @Test
    void expirationHeader_serializedCorrectly() {
        WsMessageSerializer serializer = new WsMessageSerializer();
        com.fasterxml.jackson.databind.node.ObjectNode msg =
            kafka.server.http.HttpRequestTranslator.MAPPER.createObjectNode();
        msg.put("body", "hello");
        msg.put("expiration", "60000");

        WsMessageSerializer.SerializedMessage result = serializer.serialize(
            "ex", "rk", msg, "/");

        boolean found = false;
        for (Header h : result.headers()) {
            if (WsMessageSerializer.HDR_EXPIRATION.equals(h.key())) {
                assertEquals("60000", new String(h.value(), StandardCharsets.UTF_8));
                found = true;
            }
        }
        assertTrue(found, "_ws_expiration header missing from serialized headers");

        // Round-trip through the deserializer
        WsMessageDeserializer deserializer = new WsMessageDeserializer();
        WsMessageDeserializer.DeliverFrame frame = deserializer.deserialize(
            result.key(), result.value(), result.headers(), 0, 42L, 1_000L);
        assertEquals("60000", frame.message().get("expiration").asText());
    }

    // ---- maybeDeliverRecord: drop vs. deliver ----

    @Test
    void perMessageTtl_notExpired_delivered() {
        WsConsumerFetchLoop loop = new WsConsumerFetchLoop(
            "sub-1", "ws.orders", Map.of(tp0, 0L), creditManager, tagTracker, channel, false);

        List<Header> headers = List.of(expirationHeader("60000"));
        long recordTs = 1_000L;
        long now = recordTs + 10_000L; // 10s < 60s

        boolean delivered = loop.maybeDeliverRecord(
            tp0, 5L, "ex", "rk", "{}", false, headers, recordTs, now);

        assertTrue(delivered);
        assertEquals(1, tagTracker.pendingCount());
        assertEquals(9, creditManager.available());
        assertEquals(6L, loop.currentOffsets().get(tp0));
        verify(channel).writeAndFlush(any(TextWebSocketFrame.class));
    }

    @Test
    void perMessageTtl_expired_skipped() {
        WsConsumerFetchLoop loop = new WsConsumerFetchLoop(
            "sub-1", "ws.orders", Map.of(tp0, 0L), creditManager, tagTracker, channel, false);

        List<Header> headers = List.of(expirationHeader("1000"));
        long recordTs = 1_000L;
        long now = recordTs + 10_000L; // 10s > 1s → expired

        boolean delivered = loop.maybeDeliverRecord(
            tp0, 5L, "ex", "rk", "{}", false, headers, recordTs, now);

        assertFalse(delivered);
        // No tag assigned, no credit consumed, no frame written
        assertEquals(0, tagTracker.pendingCount());
        assertEquals(10, creditManager.available());
        verify(channel, never()).writeAndFlush(any());
    }

    @Test
    void perMessageTtl_expired_offsetAdvanced() {
        // CRITICAL: expired messages MUST advance the offset watermark so they
        // are never re-fetched / redelivered.
        WsConsumerFetchLoop loop = new WsConsumerFetchLoop(
            "sub-1", "ws.orders", Map.of(tp0, 0L), creditManager, tagTracker, channel, false);

        List<Header> headers = List.of(expirationHeader("1"));
        long recordTs = 1_000L;
        long now = recordTs + 10_000L;

        loop.maybeDeliverRecord(tp0, 5L, "ex", "rk", "{}", false, headers, recordTs, now);
        assertEquals(6L, loop.currentOffsets().get(tp0),
            "offset must advance past expired record");
    }

    @Test
    void perMessageTtl_expired_noDlx_discarded() {
        // WS4.01 DLX not yet implemented — absence of DLX must not block the
        // consumer. Expired + no DLX → drop, advance offset.
        WsConsumerFetchLoop loop = new WsConsumerFetchLoop(
            "sub-1", "ws.orders", Map.of(tp0, 0L), creditManager, tagTracker, channel, false);

        List<Header> headers = List.of(expirationHeader("1"));
        boolean delivered = loop.maybeDeliverRecord(
            tp0, 7L, "ex", "rk", "{}", false, headers, 1_000L, 1_000_000L);

        assertFalse(delivered);
        assertEquals(8L, loop.currentOffsets().get(tp0));
        verify(channel, never()).writeAndFlush(any());
    }

    @Test
    void noExpiration_noTtlCheck() {
        // Messages without the _ws_expiration header are always delivered
        // regardless of age.
        WsConsumerFetchLoop loop = new WsConsumerFetchLoop(
            "sub-1", "ws.orders", Map.of(tp0, 0L), creditManager, tagTracker, channel, false);

        boolean delivered = loop.maybeDeliverRecord(
            tp0, 0L, "ex", "rk", "{}", false,
            Collections.emptyList(), 1_000L, 1_000_000_000L);

        assertTrue(delivered);
        assertEquals(1L, loop.currentOffsets().get(tp0));
        verify(channel).writeAndFlush(any(TextWebSocketFrame.class));
    }

    @Test
    void multipleRecords_mixedExpiration_offsetsAdvanceForAll() {
        WsConsumerFetchLoop loop = new WsConsumerFetchLoop(
            "sub-1", "ws.orders", Map.of(tp0, 0L), creditManager, tagTracker, channel, false);

        List<Header> expired = List.of(expirationHeader("1"));
        List<Header> live = List.of(expirationHeader("60000"));
        long now = 1_000L + 10_000L;

        // offset 0: live → delivered
        assertTrue(loop.maybeDeliverRecord(
            tp0, 0L, "ex", "rk", "{}", false, live, 1_000L, now));
        // offset 1: expired → skipped
        assertFalse(loop.maybeDeliverRecord(
            tp0, 1L, "ex", "rk", "{}", false, expired, 1_000L, now));
        // offset 2: live → delivered
        assertTrue(loop.maybeDeliverRecord(
            tp0, 2L, "ex", "rk", "{}", false, live, 1_000L, now));

        // Watermark after all three: 3 (advanced past every record, expired or not)
        assertEquals(3L, loop.currentOffsets().get(tp0));
        // Only two delivered → two pending tags, two credits consumed
        assertEquals(2, tagTracker.pendingCount());
        assertEquals(8, creditManager.available());
    }

    // ---- Per-queue TTL: x-message-ttl → retention.ms ----

    @Test
    void perQueueTtl_mapsToRetentionMs() {
        // Per design doc §12.4, x-message-ttl on queue declare maps to
        // retention.ms on the backing Kafka topic. WsMessageTtl exposes a
        // pure helper that config code uses at topic-create time.
        Map<String, String> args = Map.of("x-message-ttl", "60000");
        assertEquals(60000L, WsMessageTtl.retentionMsFromQueueArgs(args));
    }

    @Test
    void perQueueTtl_absentArgument_returnsMinusOne() {
        // -1 is the caller's sentinel for "do not set retention.ms" (use broker
        // default); anything else means the queue declares an explicit retention.
        assertEquals(-1L, WsMessageTtl.retentionMsFromQueueArgs(Map.of()));
        assertEquals(-1L, WsMessageTtl.retentionMsFromQueueArgs(null));
    }

    @Test
    void perQueueTtl_malformedArgument_returnsMinusOne() {
        // Invalid or negative values are rejected (caller falls back to default
        // retention rather than corrupting the topic config).
        assertEquals(-1L, WsMessageTtl.retentionMsFromQueueArgs(
            Map.of("x-message-ttl", "abc")));
        assertEquals(-1L, WsMessageTtl.retentionMsFromQueueArgs(
            Map.of("x-message-ttl", "-1")));
    }

    // ---- Helpers ----

    private static Header expirationHeader(String value) {
        return new RecordHeader(
            WsMessageSerializer.HDR_EXPIRATION,
            value.getBytes(StandardCharsets.UTF_8));
    }
}
