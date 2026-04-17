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

// Time: Created - TASK-WS4.03 - priority-aware delivery ordering

package kafka.server.http.ws;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for TASK-WS4.03 priority-aware delivery ordering
 * (design doc §12.5 / §18.7).
 *
 * <p>Covers:
 * <ul>
 *   <li>{@link WsPriority#extractPriority} header parsing, default-0, clamping.</li>
 *   <li>{@link WsPriority#maxPriorityFromQueueArgs} argument parsing.</li>
 *   <li>{@link WsPriority#sortByPriority} stable descending sort.</li>
 *   <li>{@link WsConsumerFetchLoop} with a configured {@code queueMaxPriority}:
 *       in-batch reordering preserves within-priority offset order, no-priority
 *       queues leave the batch untouched, and the publish→fetch round-trip
 *       carries the {@code _ws_priority} header intact.</li>
 * </ul>
 */
class WsPriorityDeliveryTest {

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
        creditManager = new WsCreditManager(100, channel);
        tagTracker = new WsDeliveryTagTracker();
    }

    // ---- WsPriority.extractPriority ----

    @Test
    void extractPriority_missingHeader_returnsZero() {
        assertEquals(0, WsPriority.extractPriority(Collections.emptyList(), 10));
    }

    @Test
    void extractPriority_nullIterable_returnsZero() {
        assertEquals(0, WsPriority.extractPriority(null, 10));
    }

    @Test
    void extractPriority_presentHeader_parsedAsInt() {
        List<Header> headers = List.of(priorityHeader("7"));
        assertEquals(7, WsPriority.extractPriority(headers, 10));
    }

    @Test
    void extractPriority_headerExceedsMax_cappedAtMax() {
        // Spec: values above x-max-priority are capped.
        List<Header> headers = List.of(priorityHeader("42"));
        assertEquals(10, WsPriority.extractPriority(headers, 10));
    }

    @Test
    void extractPriority_headerNegative_clampedToZero() {
        // Negative values are nonsensical — clamp to the lowest priority.
        List<Header> headers = List.of(priorityHeader("-5"));
        assertEquals(0, WsPriority.extractPriority(headers, 10));
    }

    @Test
    void extractPriority_malformedHeader_defaultsToZero() {
        // Malformed → default (lowest) priority, never throws.
        List<Header> headers = List.of(priorityHeader("not-a-number"));
        assertEquals(0, WsPriority.extractPriority(headers, 10));
    }

    @Test
    void extractPriority_emptyHeader_defaultsToZero() {
        List<Header> headers = List.of(priorityHeader(""));
        assertEquals(0, WsPriority.extractPriority(headers, 10));
    }

    @Test
    void extractPriority_duplicateHeaders_lastWins() {
        // Kafka headers allow duplicate keys; the last value is authoritative
        // (matches Headers#lastHeader convention).
        List<Header> headers = List.of(priorityHeader("3"), priorityHeader("9"));
        assertEquals(9, WsPriority.extractPriority(headers, 10));
    }

    @Test
    void extractPriority_maxPriorityZero_alwaysZero() {
        // Non-priority queue (maxPriority == 0) always reports 0 — no sorting
        // semantic even if a header happens to be present on a record.
        List<Header> headers = List.of(priorityHeader("7"));
        assertEquals(0, WsPriority.extractPriority(headers, 0));
    }

    // ---- WsPriority.maxPriorityFromQueueArgs ----

    @Test
    void maxPriorityFromQueueArgs_present_parsed() {
        assertEquals(10, WsPriority.maxPriorityFromQueueArgs(
            Map.of("x-max-priority", "10")));
    }

    @Test
    void maxPriorityFromQueueArgs_absent_returnsZero() {
        assertEquals(0, WsPriority.maxPriorityFromQueueArgs(Map.of()));
        assertEquals(0, WsPriority.maxPriorityFromQueueArgs(null));
    }

    @Test
    void maxPriorityFromQueueArgs_malformed_returnsZero() {
        assertEquals(0, WsPriority.maxPriorityFromQueueArgs(
            Map.of("x-max-priority", "abc")));
    }

    @Test
    void maxPriorityFromQueueArgs_negative_returnsZero() {
        assertEquals(0, WsPriority.maxPriorityFromQueueArgs(
            Map.of("x-max-priority", "-3")));
    }

    @Test
    void maxPriorityFromQueueArgs_above255_cappedAt255() {
        // AMQP priorities are 0-255; cap to protect downstream consumers.
        assertEquals(255, WsPriority.maxPriorityFromQueueArgs(
            Map.of("x-max-priority", "500")));
    }

    // ---- WsPriority.sortByPriority ----

    @Test
    void sortByPriority_maxPriorityZero_returnsInputUnchanged() {
        // Non-priority queue: skip sorting entirely, preserve offset order.
        List<PriRec> in = List.of(
            new PriRec(10L, "3"),
            new PriRec(11L, "9"),
            new PriRec(12L, "5"));
        List<PriRec> out = WsPriority.sortByPriority(in, PriRec::headers, 0);
        assertSame(in, out, "sort must be a no-op when maxPriority<=0");
    }

    @Test
    void sortByPriority_descendingByPriority() {
        List<PriRec> in = new ArrayList<>(List.of(
            new PriRec(10L, "3"),
            new PriRec(11L, "9"),
            new PriRec(12L, "5")));
        List<PriRec> out = WsPriority.sortByPriority(in, PriRec::headers, 10);

        // Original list returned (sort in place), contents re-ordered.
        assertEquals(3, out.size());
        assertEquals(11L, out.get(0).offset()); // priority 9
        assertEquals(12L, out.get(1).offset()); // priority 5
        assertEquals(10L, out.get(2).offset()); // priority 3
    }

    @Test
    void sortByPriority_stableWithinPriority() {
        // Records with equal priority keep their original (offset) order.
        List<PriRec> in = new ArrayList<>(List.of(
            new PriRec(10L, "5"),
            new PriRec(11L, "5"),
            new PriRec(12L, "5"),
            new PriRec(13L, "9")));
        List<PriRec> out = WsPriority.sortByPriority(in, PriRec::headers, 10);

        assertEquals(13L, out.get(0).offset()); // priority 9 first
        assertEquals(10L, out.get(1).offset()); // then original order within p=5
        assertEquals(11L, out.get(2).offset());
        assertEquals(12L, out.get(3).offset());
    }

    @Test
    void sortByPriority_missingHeader_treatedAsZero() {
        List<PriRec> in = new ArrayList<>(List.of(
            new PriRec(10L, null),   // no header → 0
            new PriRec(11L, "9"),
            new PriRec(12L, null),   // no header → 0
            new PriRec(13L, "5")));
        List<PriRec> out = WsPriority.sortByPriority(in, PriRec::headers, 10);

        assertEquals(11L, out.get(0).offset()); // 9
        assertEquals(13L, out.get(1).offset()); // 5
        // Within priority=0 the original order is preserved.
        assertEquals(10L, out.get(2).offset());
        assertEquals(12L, out.get(3).offset());
    }

    @Test
    void sortByPriority_valueExceedsMax_cappedThenSorted() {
        // priority=42 capped to 10, priority=9 stays 9 → capped record wins.
        List<PriRec> in = new ArrayList<>(List.of(
            new PriRec(10L, "9"),
            new PriRec(11L, "42")));
        List<PriRec> out = WsPriority.sortByPriority(in, PriRec::headers, 10);
        assertEquals(11L, out.get(0).offset()); // capped to 10 > 9
        assertEquals(10L, out.get(1).offset());
    }

    @Test
    void sortByPriority_singleRecord_unchanged() {
        List<PriRec> in = new ArrayList<>(List.of(new PriRec(10L, "5")));
        List<PriRec> out = WsPriority.sortByPriority(in, PriRec::headers, 10);
        assertEquals(1, out.size());
        assertEquals(10L, out.get(0).offset());
    }

    @Test
    void sortByPriority_emptyList_unchanged() {
        List<PriRec> out = WsPriority.sortByPriority(
            new ArrayList<>(), PriRec::headers, 10);
        assertTrue(out.isEmpty());
    }

    // ---- WsConsumerFetchLoop integration ----

    @Test
    void loop_defaultMaxPriority_isZero() {
        WsConsumerFetchLoop loop = new WsConsumerFetchLoop(
            "sub-1", "ws.orders", Map.of(tp0, 0L), creditManager, tagTracker, channel, false);
        assertEquals(0, loop.queueMaxPriority(),
            "default constructor must set queueMaxPriority=0 (non-priority queue)");
    }

    @Test
    void loop_constructorWithMaxPriority_storesValue() {
        WsConsumerFetchLoop loop = WsConsumerFetchLoop.withMaxPriority(
            "sub-1", "ws.orders", Map.of(tp0, 0L),
            creditManager, tagTracker, channel, false, /*maxPriority*/ 10);
        assertEquals(10, loop.queueMaxPriority());
    }

    @Test
    void priorityQueue_highestDeliveredFirst() {
        WsConsumerFetchLoop loop = WsConsumerFetchLoop.withMaxPriority(
            "sub-1", "ws.orders", Map.of(tp0, 0L),
            creditManager, tagTracker, channel, false, 10);

        List<PriRec> batch = new ArrayList<>(List.of(
            new PriRec(10L, "3"),
            new PriRec(11L, "9"),
            new PriRec(12L, "5")));
        List<PriRec> ordered = loop.maybeSortByPriority(batch, PriRec::headers);

        // Deliver in sorted order and capture frames.
        for (PriRec r : ordered) {
            loop.deliverRecord(tp0, r.offset(), "ex", "rk", "{}", false);
        }

        ArgumentCaptor<TextWebSocketFrame> captor = ArgumentCaptor.forClass(TextWebSocketFrame.class);
        verify(channel, times(3)).writeAndFlush(captor.capture());
        List<TextWebSocketFrame> frames = captor.getAllValues();
        // Priority 9 at offset=11 is delivered first, priority 3 at offset=10 last.
        assertTrue(frames.get(0).text().contains("\"offset\":11"), frames.get(0).text());
        assertTrue(frames.get(1).text().contains("\"offset\":12"), frames.get(1).text());
        assertTrue(frames.get(2).text().contains("\"offset\":10"), frames.get(2).text());
    }

    @Test
    void priorityQueue_samePriority_offsetOrder() {
        WsConsumerFetchLoop loop = WsConsumerFetchLoop.withMaxPriority(
            "sub-1", "ws.orders", Map.of(tp0, 0L),
            creditManager, tagTracker, channel, false, 10);

        List<PriRec> batch = new ArrayList<>(List.of(
            new PriRec(20L, "5"),
            new PriRec(21L, "5"),
            new PriRec(22L, "5")));
        List<PriRec> ordered = loop.maybeSortByPriority(batch, PriRec::headers);

        assertEquals(20L, ordered.get(0).offset());
        assertEquals(21L, ordered.get(1).offset());
        assertEquals(22L, ordered.get(2).offset());
    }

    @Test
    void noPriorityQueue_offsetOrder() {
        // maxPriority=0 → no sorting, list returned as-is (same reference).
        WsConsumerFetchLoop loop = new WsConsumerFetchLoop(
            "sub-1", "ws.orders", Map.of(tp0, 0L),
            creditManager, tagTracker, channel, false);

        List<PriRec> batch = List.of(
            new PriRec(10L, "3"),
            new PriRec(11L, "9"),
            new PriRec(12L, "5"));
        List<PriRec> ordered = loop.maybeSortByPriority(batch, PriRec::headers);

        assertSame(batch, ordered);
        assertEquals(10L, ordered.get(0).offset());
        assertEquals(11L, ordered.get(1).offset());
        assertEquals(12L, ordered.get(2).offset());
    }

    @Test
    void missingPriorityHeader_defaultsToZero() {
        WsConsumerFetchLoop loop = WsConsumerFetchLoop.withMaxPriority(
            "sub-1", "ws.orders", Map.of(tp0, 0L),
            creditManager, tagTracker, channel, false, 10);

        List<PriRec> batch = new ArrayList<>(List.of(
            new PriRec(10L, null),  // no priority header
            new PriRec(11L, "1")));
        List<PriRec> ordered = loop.maybeSortByPriority(batch, PriRec::headers);

        // "1" > "0" so the record with priority=1 is delivered first.
        assertEquals(11L, ordered.get(0).offset());
        assertEquals(10L, ordered.get(1).offset());
    }

    @Test
    void priorityExceedsMax_capped() {
        // Spec: values above x-max-priority clamp to the max.
        WsConsumerFetchLoop loop = WsConsumerFetchLoop.withMaxPriority(
            "sub-1", "ws.orders", Map.of(tp0, 0L),
            creditManager, tagTracker, channel, false, 10);

        List<PriRec> batch = new ArrayList<>(List.of(
            new PriRec(10L, "50"),  // capped to 10
            new PriRec(11L, "10")));
        List<PriRec> ordered = loop.maybeSortByPriority(batch, PriRec::headers);

        // Capped to same priority — stable sort keeps input order.
        assertEquals(10L, ordered.get(0).offset());
        assertEquals(11L, ordered.get(1).offset());
    }

    @Test
    void batchLocalOrdering() {
        // Priority sort is per-fetch-batch: a low-priority record already in
        // batch #1 delivers BEFORE a high-priority record that arrives in
        // batch #2 (spec §12.5).
        WsConsumerFetchLoop loop = WsConsumerFetchLoop.withMaxPriority(
            "sub-1", "ws.orders", Map.of(tp0, 0L),
            creditManager, tagTracker, channel, false, 10);

        // Batch 1: single low-priority record.
        List<PriRec> batch1 = new ArrayList<>(List.of(new PriRec(10L, "1")));
        List<PriRec> ordered1 = loop.maybeSortByPriority(batch1, PriRec::headers);
        for (PriRec r : ordered1) {
            loop.deliverRecord(tp0, r.offset(), "ex", "rk", "{}", false);
        }

        // Batch 2: single high-priority record — delivered AFTER batch 1.
        List<PriRec> batch2 = new ArrayList<>(List.of(new PriRec(11L, "9")));
        List<PriRec> ordered2 = loop.maybeSortByPriority(batch2, PriRec::headers);
        for (PriRec r : ordered2) {
            loop.deliverRecord(tp0, r.offset(), "ex", "rk", "{}", false);
        }

        ArgumentCaptor<TextWebSocketFrame> captor = ArgumentCaptor.forClass(TextWebSocketFrame.class);
        verify(channel, times(2)).writeAndFlush(captor.capture());
        // offset=10 (low priority, batch 1) was delivered FIRST.
        assertTrue(captor.getAllValues().get(0).text().contains("\"offset\":10"));
        assertTrue(captor.getAllValues().get(1).text().contains("\"offset\":11"));
    }

    @Test
    void priorityHeader_roundTrip() {
        // Publish → serializer writes _ws_priority; fetch batch → deserializer
        // reads it back out; sort logic sees identical numeric priority.
        WsMessageSerializer serializer = new WsMessageSerializer();
        com.fasterxml.jackson.databind.node.ObjectNode msg =
            kafka.server.http.HttpRequestTranslator.MAPPER.createObjectNode();
        msg.put("body", "hi");
        msg.put("priority", 7);

        WsMessageSerializer.SerializedMessage serialized = serializer.serialize(
            "ex", "rk", msg, "/");

        // _ws_priority header present with value "7".
        boolean found = false;
        for (Header h : serialized.headers()) {
            if (WsMessageSerializer.HDR_PRIORITY.equals(h.key())) {
                assertEquals("7", new String(h.value(), StandardCharsets.UTF_8));
                found = true;
            }
        }
        assertTrue(found, "_ws_priority header missing from serialized headers");

        // Extraction sees 7 as the numeric priority.
        assertEquals(7, WsPriority.extractPriority(serialized.headers(), 10));

        // And the deserializer writes it back into message.priority so the
        // deliver frame carries it for the client.
        WsMessageDeserializer deserializer = new WsMessageDeserializer();
        WsMessageDeserializer.DeliverFrame frame = deserializer.deserialize(
            serialized.key(), serialized.value(), serialized.headers(),
            /*partition*/ 0, /*offset*/ 42L, /*ts*/ 1_000L);
        assertEquals(7, frame.message().get("priority").asInt());
    }

    // ---- Helpers ----

    private static Header priorityHeader(String value) {
        return new RecordHeader(
            WsMessageSerializer.HDR_PRIORITY,
            value.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Minimal pseudo-record carrying just enough state for priority-sort tests.
     */
    private record PriRec(long offset, String priorityValue) {
        List<Header> headers() {
            if (priorityValue == null) {
                return Collections.emptyList();
            }
            return List.of(new RecordHeader(
                WsMessageSerializer.HDR_PRIORITY,
                priorityValue.getBytes(StandardCharsets.UTF_8)));
        }
    }
}
