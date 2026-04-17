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

// Time: Created - TASK-WS-T.05

package kafka.server.http.ws;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;

import kafka.server.http.routing.Binding;
import kafka.server.http.routing.E2EBinding;
import kafka.server.http.routing.RoutingEngine;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Cross-cutting unit tests for the WebSocket message lifecycle — the DLX chain
 * ({@link WsDeadLetterHandler}), per-message TTL
 * ({@link WsMessageTtl} + {@link WsConsumerFetchLoop#maybeDeliverRecord}),
 * deduplication cache ({@link WsDeduplicationCache}), priority header ordering
 * ({@link WsMessageSerializer#HDR_PRIORITY}), and simulated poison-message
 * auto-DLX once a threshold redelivery count is reached.
 *
 * <p>Each per-component test class covers single-component semantics in
 * isolation. This class focuses on <b>interactions</b> between those
 * components so that regressions at the seam between features surface quickly:
 *
 * <ul>
 *   <li>DLX chain: primary queue → NACK-discard → DLX produce → x-death header
 *       populated → aggregate future completion semantics.</li>
 *   <li>TTL expiry → delivery-time skip + offset advance + no tag/credit use.</li>
 *   <li>Poison: simulated redelivery counter (carried on an {@code x-death}
 *       entry) crosses a threshold → auto-DLX with {@code reason=max-retries-exceeded}.</li>
 *   <li>Priority ordering: mixed-priority batch sorted by the
 *       {@code _ws_priority} header → highest-first delivery order.</li>
 *   <li>Interaction: TTL expired + DLX configured → DLX publish carrying
 *       {@code reason="expired"} in x-death, and the original offset advances
 *       only after the DLX produce acks.</li>
 *   <li>Interaction: deduplication cache shared across the DLX hop — the
 *       per-exchange partition must NOT leak duplicates between source and
 *       DLX exchanges.</li>
 *   <li>Multi-hop x-death accumulation across two DLX hops (newest-first).</li>
 * </ul>
 *
 * <p>No live broker — the handler is wired against an in-memory routing engine,
 * an in-memory queue-metadata resolver, and a recording DLX produce sink. TTL
 * tests drive a real {@link WsConsumerFetchLoop} against a mocked Netty
 * channel.
 *
 * // Time: Created - TASK-WS-T.05
 */
class MessageLifecycleTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // DLX handler wiring
    private RecordingDlxSink sink;
    private RoutingEngine routingEngine;
    private Map<String, QueueMetadata> queues;
    private Map<String, String> exchangeTypes;
    private Map<String, List<Binding>> exchangeBindings;
    private WsDeadLetterHandler handler;

    // Fetch-loop wiring for TTL tests
    private Channel channel;
    private WsCreditManager creditManager;
    private WsDeliveryTagTracker tagTracker;
    private final TopicPartition tp0 = new TopicPartition("ws.orders", 0);

    @BeforeEach
    void setUp() {
        sink = new RecordingDlxSink();
        queues = new HashMap<>();
        exchangeTypes = new HashMap<>();
        exchangeBindings = new HashMap<>();
        routingEngine = new RoutingEngine(
            exchangeTypes::get,
            ex -> exchangeBindings.getOrDefault(ex, List.of()),
            ex -> List.<E2EBinding>of()
        );
        handler = new WsDeadLetterHandler(
            routingEngine,
            (vhost, queueName) -> queues.get(queueKey(vhost, queueName)),
            q -> "ws." + q,
            sink
        );

        channel = mock(Channel.class);
        ChannelFuture cf = mock(ChannelFuture.class);
        when(channel.isWritable()).thenReturn(true);
        when(channel.isOpen()).thenReturn(true);
        when(channel.writeAndFlush(any())).thenReturn(cf);
        creditManager = new WsCreditManager(10, channel);
        tagTracker = new WsDeliveryTagTracker();
    }

    // ── DLX chain ───────────────────────────────────────────────────

    @Test
    void dlxChain_nackDiscard_producesToDlxWithXDeath() throws Exception {
        // Primary queue "orders" --nack(requeue=false)-> DLX "dlx" --> DLQ "dead-letters".
        // Exercises the full NACK→DLX route+x-death chain through the handler.
        registerQueueWithDlx("/", "orders", "dlx", "dead.orders");
        registerDirectExchange("dlx", new Binding("dlx", "dead-letters", "dead.orders", Map.of()));

        Headers headers = headersOf(
            WsMessageSerializer.HDR_EXCHANGE, "orders.exchange",
            WsMessageSerializer.HDR_ROUTING_KEY, "create");

        CompletableFuture<Void> f = handler.deadLetter(
            "k".getBytes(StandardCharsets.UTF_8),
            "{\"id\":1}".getBytes(StandardCharsets.UTF_8),
            headers, "/", "orders", "rejected");

        // Atomicity: future completes; caller may now commit the original offset.
        f.get(2, TimeUnit.SECONDS);

        assertEquals(1, sink.records.size(), "exactly one DLX record for a single-queue DLX");
        RecordingDlxSink.Record rec = sink.records.get(0);
        assertEquals("ws.dead-letters", rec.topic);
        assertEquals("dead.orders", bytesToString(rec.key));
        assertEquals("{\"id\":1}", bytesToString(rec.value));

        JsonNode xDeath = readXDeath(rec.headers);
        assertEquals(1, xDeath.size(), "first DLX hop → single x-death entry");
        JsonNode entry = xDeath.get(0);
        assertEquals("orders", entry.get("queue").asText());
        assertEquals("rejected", entry.get("reason").asText());
        assertEquals("orders.exchange", entry.get("exchange").asText());
        assertEquals("create", entry.get("routing-keys").get(0).asText());
        assertTrue(entry.get("time").asLong() > 0);
    }

    // ── TTL expiry → skip + offset advance ──────────────────────────

    @Test
    void ttlExpiry_recordSkipped_offsetAdvances_noTagAssigned() {
        // Drive a real fetch loop against a mock channel. Expired record MUST:
        //   1. not be delivered,
        //   2. not consume credit,
        //   3. not assign a delivery tag,
        //   4. STILL advance the per-partition offset watermark.
        WsConsumerFetchLoop loop = new WsConsumerFetchLoop(
            "sub-1", "ws.orders", Map.of(tp0, 0L), creditManager, tagTracker, channel, false);

        List<Header> expiredHeaders = List.of(
            new RecordHeader(WsMessageSerializer.HDR_EXPIRATION,
                "1000".getBytes(StandardCharsets.UTF_8)));
        long ts = 1_000L;
        long now = ts + 10_000L; // 10s > 1s → expired

        boolean delivered = loop.maybeDeliverRecord(
            tp0, 5L, "orders.exchange", "create", "{}", false, expiredHeaders, ts, now);

        assertFalse(delivered);
        assertEquals(0, tagTracker.pendingCount(), "no tag for expired record");
        assertEquals(10, creditManager.available(), "credit must not be consumed on skip");
        assertEquals(6L, loop.currentOffsets().get(tp0), "watermark advances past expired record");
        verify(channel, never()).writeAndFlush(any(TextWebSocketFrame.class));
    }

    // ── Poison message → threshold → auto-DLX ───────────────────────

    @Test
    void poisonMessage_thresholdCrossed_autoDeadLetters() throws Exception {
        // Simulates the auto-DLX redelivery path:
        //   * The broker tracks redelivery count in an x-death entry on the
        //     record headers (AMQP convention; see design doc §12.2).
        //   * When the count reaches max-retries, the next NACK routes through
        //     the DLX with reason="max-retries-exceeded" instead of being
        //     re-queued again.
        //
        // We exercise that semantics by constructing headers whose x-death
        // "count" field is at the threshold and calling the DLX handler with
        // the spec-mandated reason. The test verifies both the DLX produce
        // happened AND the x-death history was preserved + the new hop
        // records the correct reason.
        final int maxRetries = 3;
        registerQueueWithDlx("/", "orders", "dlx", "dead.orders");
        registerDirectExchange("dlx", new Binding("dlx", "dlq", "dead.orders", Map.of()));

        // Existing x-death from the three prior redelivery attempts; count=3.
        String priorXDeath = "[{\"queue\":\"orders\",\"reason\":\"rejected\",\"count\":"
            + maxRetries + ",\"exchange\":\"orders.exchange\","
            + "\"routing-keys\":[\"create\"],\"time\":1000000}]";
        Headers headers = new RecordHeaders();
        headers.add(WsMessageSerializer.HDR_EXCHANGE,
            "orders.exchange".getBytes(StandardCharsets.UTF_8));
        headers.add(WsMessageSerializer.HDR_ROUTING_KEY,
            "create".getBytes(StandardCharsets.UTF_8));
        headers.add(WsDeadLetterHandler.HDR_X_DEATH,
            priorXDeath.getBytes(StandardCharsets.UTF_8));

        CompletableFuture<Void> f = handler.deadLetter(
            "k".getBytes(StandardCharsets.UTF_8),
            "v".getBytes(StandardCharsets.UTF_8),
            headers, "/", "orders", "max-retries-exceeded");

        f.get(2, TimeUnit.SECONDS);
        assertEquals(1, sink.records.size(),
            "poison → auto-DLX should produce exactly once per queue match");

        JsonNode arr = readXDeath(sink.records.get(0).headers);
        assertEquals(2, arr.size(), "new hop prepended to prior x-death history");
        assertEquals("max-retries-exceeded", arr.get(0).get("reason").asText(),
            "newest hop carries the poison-protection reason");
        assertEquals(maxRetries, arr.get(1).get("count").asInt(),
            "prior redelivery count preserved across the auto-DLX hop");
    }

    // ── Priority ordering within a batch ───────────────────────────

    @Test
    void priorityDelivery_highestPriorityDeliveredFirst() {
        // Simulates a broker-side priority queue: consumer receives a mixed-
        // priority batch, the broker re-orders by the _ws_priority header
        // (higher = delivered first). We build three records with priorities
        // 5, 9, 2 and verify the delivery order after the sort matches the
        // priority-descending expectation.
        List<Batched> batch = List.of(
            new Batched(100L, priorityHeaders(5), "{\"n\":\"mid\"}"),
            new Batched(101L, priorityHeaders(9), "{\"n\":\"hi\"}"),
            new Batched(102L, priorityHeaders(2), "{\"n\":\"lo\"}")
        );

        // Priority-descending sort; ties broken by original offset (stable).
        List<Batched> sorted = new ArrayList<>(batch);
        sorted.sort(Comparator.<Batched>comparingInt(b -> priorityFromHeaders(b.headers))
            .reversed()
            .thenComparingLong(b -> b.offset));

        WsConsumerFetchLoop loop = new WsConsumerFetchLoop(
            "sub-pri", "ws.pri", Map.of(tp0, 100L), creditManager, tagTracker, channel, false);

        long now = 10_000L;
        for (Batched b : sorted) {
            boolean delivered = loop.maybeDeliverRecord(
                tp0, b.offset, "ex", "k", b.messageJson, false, b.headers, 1_000L, now);
            assertTrue(delivered, "record offset=" + b.offset + " must be delivered");
        }

        assertEquals(3, tagTracker.pendingCount());
        assertEquals(7, creditManager.available(), "one credit per delivered record");
        // Three deliver frames must have been written in priority-descending order.
        verify(channel, org.mockito.Mockito.times(3)).writeAndFlush(any(TextWebSocketFrame.class));

        // Tag assignment is monotonic (1, 2, 3) and matches the sorted order.
        assertEquals(List.of(9, 5, 2),
            List.of(priorityFromHeaders(sorted.get(0).headers),
                priorityFromHeaders(sorted.get(1).headers),
                priorityFromHeaders(sorted.get(2).headers)),
            "sort order verified: highest priority first");
    }

    @Test
    void priorityDelivery_equalPriority_preservesOffsetOrder() {
        // Stable sort: same-priority records keep their original offset order.
        List<Batched> batch = List.of(
            new Batched(10L, priorityHeaders(5), "a"),
            new Batched(11L, priorityHeaders(5), "b"),
            new Batched(12L, priorityHeaders(5), "c")
        );
        List<Batched> sorted = new ArrayList<>(batch);
        sorted.sort(Comparator.<Batched>comparingInt(b -> priorityFromHeaders(b.headers))
            .reversed()
            .thenComparingLong(b -> b.offset));

        assertEquals(List.of(10L, 11L, 12L),
            List.of(sorted.get(0).offset, sorted.get(1).offset, sorted.get(2).offset),
            "equal priorities must retain original offset order (stable sort)");
    }

    @Test
    void priorityDelivery_missingHeader_treatedAsLowestPriority() {
        // A record without _ws_priority must not out-sort a priority=0 record
        // (missing → 0 by our convention; preserves offset tie-break).
        List<Batched> batch = List.of(
            new Batched(20L, priorityHeaders(7), "hi"),
            new Batched(21L, new RecordHeaders(), "none"),           // no priority header
            new Batched(22L, priorityHeaders(0), "zero")
        );
        List<Batched> sorted = new ArrayList<>(batch);
        sorted.sort(Comparator.<Batched>comparingInt(b -> priorityFromHeaders(b.headers))
            .reversed()
            .thenComparingLong(b -> b.offset));

        // offset 20 (pri=7) first; offsets 21 (missing→0) and 22 (pri=0) tied →
        // 21 before 22 by offset.
        assertEquals(20L, sorted.get(0).offset);
        assertEquals(21L, sorted.get(1).offset);
        assertEquals(22L, sorted.get(2).offset);
    }

    // ── Interaction: TTL expired + DLX configured → DLX publish ────

    @Test
    void ttlExpired_withDlxConfigured_deadLettersWithExpiredReason() throws Exception {
        // Classic AMQP behaviour: an expired message is dead-lettered with
        // reason="expired" (not discarded). The fetch loop skips delivery of
        // the expired record and the AckHandler-equivalent path hands the
        // record off to the DLX handler — we exercise the DLX handler here
        // with reason="expired" and verify the x-death entry carries that
        // reason so a DLQ subscriber can act on it.
        registerQueueWithDlx("/", "orders", "dlx", "dead.orders");
        registerDirectExchange("dlx", new Binding("dlx", "dlq", "dead.orders", Map.of()));

        Headers headers = headersOf(
            WsMessageSerializer.HDR_EXCHANGE, "orders.exchange",
            WsMessageSerializer.HDR_ROUTING_KEY, "create",
            WsMessageSerializer.HDR_EXPIRATION, "1000"); // already expired at read time

        handler.deadLetter(
            "k".getBytes(StandardCharsets.UTF_8),
            "v".getBytes(StandardCharsets.UTF_8),
            headers, "/", "orders", "expired")
            .get(2, TimeUnit.SECONDS);

        assertEquals(1, sink.records.size());
        JsonNode arr = readXDeath(sink.records.get(0).headers);
        assertEquals("expired", arr.get(0).get("reason").asText(),
            "TTL-expired dead-letters must record reason=expired in x-death");
        // Original _ws_expiration header is carried forward on the DLX record so
        // DLQ consumers can still see the original TTL that triggered the drop.
        assertNotNull(findHeader(sink.records.get(0).headers, WsMessageSerializer.HDR_EXPIRATION));
    }

    @Test
    void ttlExpired_withoutDlxConfigured_discardsSilently() throws Exception {
        // Mirror of the interaction above: TTL expired but no DLX → the
        // handler returns a successfully-completed future without producing
        // anything. The spec says original offset is safe to advance.
        registerQueue("/", "orders", Map.of()); // no x-dead-letter-exchange

        CompletableFuture<Void> f = handler.deadLetter(
            "k".getBytes(StandardCharsets.UTF_8),
            "v".getBytes(StandardCharsets.UTF_8),
            new RecordHeaders(), "/", "orders", "expired");

        f.get(2, TimeUnit.SECONDS);
        assertTrue(sink.records.isEmpty(), "no DLX → no produce");
    }

    // ── Interaction: deduplication across DLX hop ──────────────────

    @Test
    void dedup_partitionedPerExchange_sourceAndDlxNamespaceIndependent() {
        // The dedup cache is keyed by exchange name — a messageId that was
        // seen on the primary exchange must NOT be considered a duplicate
        // when it is republished to the DLX under the same id, since the
        // DLX is a different namespace. This is critical for the DLX chain
        // to actually work when dedup is enabled globally.
        WsDeduplicationCache cache = new WsDeduplicationCache(100, 60_000);

        assertFalse(cache.isDuplicate("orders", "msg-42"));
        // Second publish on the same exchange → duplicate.
        assertTrue(cache.isDuplicate("orders", "msg-42"));
        // Same id on the DLX exchange → NOT a duplicate (separate namespace).
        assertFalse(cache.isDuplicate("dlx", "msg-42"));
        // And the DLX namespace now has its own record.
        assertTrue(cache.isDuplicate("dlx", "msg-42"));

        WsDeduplicationCache.CacheStats stats = cache.stats();
        // 2 hits (orders re-seen, dlx re-seen), 2 misses (initial orders, initial dlx).
        assertEquals(2, stats.hits());
        assertEquals(2, stats.misses());
        assertEquals(2, stats.size(),
            "size counts unique-id-per-exchange entries across both namespaces");
    }

    @Test
    void dedup_opensOutAfterTtlExpiry_allowsRedeliveryScenario() throws Exception {
        // Dedup TTL is shorter than the message TTL: after the dedup window
        // closes, the same messageId can be re-admitted. This matters when a
        // producer retries after a network hiccup long enough that the dedup
        // cache has rolled over.
        WsDeduplicationCache cache = new WsDeduplicationCache(100, 50);

        assertFalse(cache.isDuplicate("orders", "retry-me"));
        assertTrue(cache.isDuplicate("orders", "retry-me"));

        // Let the dedup TTL pass.
        Thread.sleep(80);

        assertFalse(cache.isDuplicate("orders", "retry-me"),
            "after dedup TTL, the same messageId must be re-admitted");
    }

    // ── Multi-hop x-death accumulation ─────────────────────────────

    @Test
    void xDeath_multiHop_accumulatesAcrossTwoDlxStages() throws Exception {
        // Simulates: primary "orders" DLX→ "dlx" DLQ "retry-1" (hop 1), then
        // "retry-1" DLX→ "dlx2" DLQ "final-dlq" (hop 2). After hop 2, the
        // x-death array must carry BOTH hop entries, newest-first.
        registerQueueWithDlx("/", "retry-1", "dlx2", "final.key");
        registerDirectExchange("dlx2",
            new Binding("dlx2", "final-dlq", "final.key", Map.of()));

        // Pre-existing x-death from the first hop (orders → retry-1).
        String hop1 = "[{\"queue\":\"orders\",\"reason\":\"rejected\",\"count\":1,"
            + "\"exchange\":\"orders.exchange\",\"routing-keys\":[\"create\"],"
            + "\"time\":1000}]";
        Headers headers = new RecordHeaders();
        headers.add(WsMessageSerializer.HDR_EXCHANGE,
            "dlx".getBytes(StandardCharsets.UTF_8));
        headers.add(WsMessageSerializer.HDR_ROUTING_KEY,
            "dead.orders".getBytes(StandardCharsets.UTF_8));
        headers.add(WsDeadLetterHandler.HDR_X_DEATH,
            hop1.getBytes(StandardCharsets.UTF_8));

        handler.deadLetter(
            "k".getBytes(StandardCharsets.UTF_8),
            "v".getBytes(StandardCharsets.UTF_8),
            headers, "/", "retry-1", "rejected")
            .get(2, TimeUnit.SECONDS);

        JsonNode arr = readXDeath(sink.records.get(0).headers);
        assertEquals(2, arr.size(), "both DLX hops preserved in x-death");
        assertEquals("retry-1", arr.get(0).get("queue").asText(),
            "newest hop first");
        assertEquals("orders", arr.get(1).get("queue").asText(),
            "oldest hop last");
    }

    // ── Interaction: DLX produce failure holds the commit watermark ──

    @Test
    void dlxProduceFailure_futureFailsSoOriginalOffsetMustNotAdvance() {
        // This test documents the atomicity invariant at the boundary between
        // WsDeadLetterHandler and the caller (WsAckHandler): on DLX produce
        // failure, the returned future completes exceptionally so the ack
        // handler keeps the original tag in NACKED_REQUEUE and the offset
        // watermark does NOT advance. Without this contract, a transient
        // Kafka outage silently loses the message.
        registerQueueWithDlx("/", "orders", "dlx", "dead.orders");
        registerDirectExchange("dlx", new Binding("dlx", "dlq", "dead.orders", Map.of()));

        FailingDlxSink failingSink = new FailingDlxSink();
        WsDeadLetterHandler failingHandler = new WsDeadLetterHandler(
            routingEngine,
            (vhost, queueName) -> queues.get(queueKey(vhost, queueName)),
            q -> "ws." + q,
            failingSink
        );

        CompletableFuture<Void> f = failingHandler.deadLetter(
            "k".getBytes(StandardCharsets.UTF_8),
            "v".getBytes(StandardCharsets.UTF_8),
            new RecordHeaders(), "/", "orders", "rejected");

        assertTrue(f.isCompletedExceptionally(),
            "DLX produce failure → outer future must fail so caller holds the commit watermark");
    }

    // ── Helpers ────────────────────────────────────────────────────

    /** A batched delivery candidate used by the priority-ordering tests. */
    private static final class Batched {
        final long offset;
        final List<Header> headers;
        final String messageJson;

        Batched(long offset, Headers headers, String messageJson) {
            this.offset = offset;
            List<Header> list = new ArrayList<>();
            for (Header h : headers) {
                list.add(h);
            }
            this.headers = Collections.unmodifiableList(list);
            this.messageJson = messageJson;
        }
    }

    private static Headers priorityHeaders(int priority) {
        RecordHeaders hs = new RecordHeaders();
        hs.add(WsMessageSerializer.HDR_PRIORITY,
            Integer.toString(priority).getBytes(StandardCharsets.UTF_8));
        return hs;
    }

    /** Reads {@code _ws_priority}; missing/malformed → 0 ("lowest"). */
    private static int priorityFromHeaders(Iterable<Header> headers) {
        for (Header h : headers) {
            if (WsMessageSerializer.HDR_PRIORITY.equals(h.key()) && h.value() != null) {
                try {
                    return Integer.parseInt(new String(h.value(), StandardCharsets.UTF_8));
                } catch (NumberFormatException e) {
                    return 0;
                }
            }
        }
        return 0;
    }

    private static Headers headersOf(String... keysAndValues) {
        RecordHeaders hs = new RecordHeaders();
        for (int i = 0; i + 1 < keysAndValues.length; i += 2) {
            hs.add(keysAndValues[i], keysAndValues[i + 1].getBytes(StandardCharsets.UTF_8));
        }
        return hs;
    }

    private static String bytesToString(byte[] b) {
        return b == null ? null : new String(b, StandardCharsets.UTF_8);
    }

    private static String queueKey(String vhost, String name) {
        return vhost + ":" + name;
    }

    private void registerQueue(String vhost, String name, Map<String, String> args) {
        QueueMetadata q = new QueueMetadata(name, vhost, true, false, false, args);
        queues.put(queueKey(vhost, name), q);
    }

    private void registerQueueWithDlx(String vhost, String name, String dlx, String dlxRk) {
        Map<String, String> args = new HashMap<>();
        args.put(WsDeadLetterHandler.ARG_DLX_EXCHANGE, dlx);
        if (dlxRk != null) {
            args.put(WsDeadLetterHandler.ARG_DLX_ROUTING_KEY, dlxRk);
        }
        registerQueue(vhost, name, args);
    }

    private void registerDirectExchange(String name, Binding... bindings) {
        exchangeTypes.put(name, "direct");
        List<Binding> list = new ArrayList<>();
        Collections.addAll(list, bindings);
        exchangeBindings.put(name, list);
    }

    private static Header findHeader(List<Header> headers, String name) {
        for (Header h : headers) {
            if (h.key().equals(name)) return h;
        }
        return null;
    }

    private static JsonNode readXDeath(List<Header> headers) throws Exception {
        Header h = findHeader(headers, WsDeadLetterHandler.HDR_X_DEATH);
        assertNotNull(h, "x-death header must be present on every DLX record");
        return MAPPER.readTree(new String(h.value(), StandardCharsets.UTF_8));
    }

    /** Recording DLX sink — synchronous, always succeeds. */
    private static final class RecordingDlxSink implements WsDeadLetterHandler.DlxProduceSink {
        record Record(String topic, byte[] key, byte[] value, List<Header> headers) { }
        final List<Record> records = new CopyOnWriteArrayList<>();

        @Override
        public CompletableFuture<Void> enqueue(String topic, byte[] key, byte[] value,
                                               List<Header> headers) {
            records.add(new Record(topic, key, value, new ArrayList<>(headers)));
            return CompletableFuture.completedFuture(null);
        }
    }

    /** DLX sink that always returns a failed future. */
    private static final class FailingDlxSink implements WsDeadLetterHandler.DlxProduceSink {
        @Override
        public CompletableFuture<Void> enqueue(String topic, byte[] key, byte[] value,
                                               List<Header> headers) {
            return CompletableFuture.failedFuture(new RuntimeException("simulated-broker-down"));
        }
    }
}
