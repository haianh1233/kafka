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

// Time: Created - TASK-WS4.01

package kafka.server.http.ws;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import kafka.server.http.routing.Binding;
import kafka.server.http.routing.E2EBinding;
import kafka.server.http.routing.RoutingEngine;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link WsDeadLetterHandler} — DLX routing, x-death header
 * accumulation, and the atomicity invariant (DLX produce completes before the
 * caller is allowed to commit the original offset).
 *
 * <p>The handler is wired against an in-memory routing engine, an in-memory
 * queue metadata resolver, and a recording produce sink. Tests assert on the
 * produced records (topic, headers including the rebuilt {@code x-death} array)
 * rather than going through a real Kafka pipeline.
 */
class WsDeadLetterHandlerTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private RecordingDlxSink sink;
    private RoutingEngine routingEngine;
    private Map<String, QueueMetadata> queues;
    private Map<String, String> exchangeTypes;
    private Map<String, List<Binding>> exchangeBindings;
    private WsDeadLetterHandler handler;

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
            queue -> "ws." + queue,
            sink
        );
    }

    // ---- Construction ----

    @Test
    void constructor_rejectsNullArgs() {
        assertThrows(NullPointerException.class,
            () -> new WsDeadLetterHandler(null, (v, q) -> null, q -> q, sink));
        assertThrows(NullPointerException.class,
            () -> new WsDeadLetterHandler(routingEngine, null, q -> q, sink));
        assertThrows(NullPointerException.class,
            () -> new WsDeadLetterHandler(routingEngine, (v, q) -> null, null, sink));
        assertThrows(NullPointerException.class,
            () -> new WsDeadLetterHandler(routingEngine, (v, q) -> null, q -> q, null));
    }

    // ---- Core behaviour ----

    @Test
    void nackNoRequeue_withDlx_producesToDlxTopic() throws Exception {
        // queue "orders" → DLX "dlx" with routing-key override "dead.orders"
        registerQueueWithDlx("/", "orders", "dlx", "dead.orders");
        registerDirectExchange("dlx", new Binding("dlx", "dlq", "dead.orders", Map.of()));

        Headers headers = new RecordHeaders();
        headers.add(WsMessageSerializer.HDR_EXCHANGE, "orders.exchange".getBytes(StandardCharsets.UTF_8));
        headers.add(WsMessageSerializer.HDR_ROUTING_KEY, "create".getBytes(StandardCharsets.UTF_8));

        CompletableFuture<Void> f = handler.deadLetter(
            "key1".getBytes(StandardCharsets.UTF_8),
            "{\"id\":1}".getBytes(StandardCharsets.UTF_8),
            headers, "/", "orders", "rejected");

        f.get(2, TimeUnit.SECONDS);
        assertEquals(1, sink.records.size(), "expected exactly one DLX record");
        RecordingDlxSink.Record rec = sink.records.get(0);
        assertEquals("ws.dlq", rec.topic);
        assertEquals("dead.orders", new String(rec.key, StandardCharsets.UTF_8));
        assertEquals("{\"id\":1}", new String(rec.value, StandardCharsets.UTF_8));
    }

    @Test
    void nackNoRequeue_noDlx_discards() throws Exception {
        registerQueue("/", "orders", Map.of()); // no x-dead-letter-exchange

        CompletableFuture<Void> f = handler.deadLetter(
            "k".getBytes(StandardCharsets.UTF_8),
            "v".getBytes(StandardCharsets.UTF_8),
            new RecordHeaders(), "/", "orders", "rejected");

        // Future completes successfully (caller may then commit), but no DLX produce.
        f.get(2, TimeUnit.SECONDS);
        assertTrue(sink.records.isEmpty(), "no DLX configured → no produce");
    }

    @Test
    void nackNoRequeue_unknownQueue_discards() throws Exception {
        // Queue not registered → resolver returns null → behave like "no DLX".
        CompletableFuture<Void> f = handler.deadLetter(
            "k".getBytes(StandardCharsets.UTF_8),
            "v".getBytes(StandardCharsets.UTF_8),
            new RecordHeaders(), "/", "orders", "rejected");

        f.get(2, TimeUnit.SECONDS);
        assertTrue(sink.records.isEmpty());
    }

    // ---- x-death header ----

    @Test
    void xDeathHeader_addedToDeadLetteredMessage() throws Exception {
        registerQueueWithDlx("/", "orders", "dlx", "dead.orders");
        registerDirectExchange("dlx", new Binding("dlx", "dlq", "dead.orders", Map.of()));

        Headers headers = new RecordHeaders();
        headers.add(WsMessageSerializer.HDR_EXCHANGE, "orders.exchange".getBytes(StandardCharsets.UTF_8));
        headers.add(WsMessageSerializer.HDR_ROUTING_KEY, "create".getBytes(StandardCharsets.UTF_8));

        handler.deadLetter("k".getBytes(StandardCharsets.UTF_8),
            "v".getBytes(StandardCharsets.UTF_8), headers, "/", "orders", "rejected")
            .get(2, TimeUnit.SECONDS);

        RecordingDlxSink.Record rec = sink.records.get(0);
        Header xDeath = findHeader(rec.headers, WsDeadLetterHandler.HDR_X_DEATH);
        assertNotNull(xDeath, "x-death header must be present");
        JsonNode arr = MAPPER.readTree(new String(xDeath.value(), StandardCharsets.UTF_8));
        assertTrue(arr.isArray());
        assertEquals(1, arr.size());
        JsonNode entry = arr.get(0);
        assertEquals("orders", entry.get("queue").asText());
        assertEquals("rejected", entry.get("reason").asText());
        assertEquals(1, entry.get("count").asInt());
        assertEquals("orders.exchange", entry.get("exchange").asText());
        assertTrue(entry.get("routing-keys").isArray());
        assertEquals("create", entry.get("routing-keys").get(0).asText());
        assertTrue(entry.get("time").asLong() > 0);
    }

    @Test
    void xDeathHeader_accumulatesAcrossDlxHops() throws Exception {
        registerQueueWithDlx("/", "orders", "dlx", "dead.orders");
        registerDirectExchange("dlx", new Binding("dlx", "dlq", "dead.orders", Map.of()));

        // Pre-existing x-death array carrying one earlier hop.
        String existing = "[{\"queue\":\"retry-1\",\"reason\":\"rejected\",\"count\":1,"
            + "\"exchange\":\"retry\",\"routing-keys\":[\"r\"],\"time\":1234567890}]";
        Headers headers = new RecordHeaders();
        headers.add(WsMessageSerializer.HDR_EXCHANGE, "orders.exchange".getBytes(StandardCharsets.UTF_8));
        headers.add(WsMessageSerializer.HDR_ROUTING_KEY, "create".getBytes(StandardCharsets.UTF_8));
        headers.add(WsDeadLetterHandler.HDR_X_DEATH, existing.getBytes(StandardCharsets.UTF_8));

        handler.deadLetter("k".getBytes(StandardCharsets.UTF_8),
            "v".getBytes(StandardCharsets.UTF_8), headers, "/", "orders", "rejected")
            .get(2, TimeUnit.SECONDS);

        Header xDeath = findHeader(sink.records.get(0).headers, WsDeadLetterHandler.HDR_X_DEATH);
        assertNotNull(xDeath);
        JsonNode arr = MAPPER.readTree(new String(xDeath.value(), StandardCharsets.UTF_8));
        assertEquals(2, arr.size(), "second hop must append to existing x-death");
        // Newest hop is first per spec ("oldest-first" wording in task is ambiguous; the
        // common convention used by RabbitMQ is newest-first. We follow RabbitMQ.).
        assertEquals("orders", arr.get(0).get("queue").asText());
        assertEquals("retry-1", arr.get(1).get("queue").asText());
    }

    @Test
    void xDeathHeader_handlesMalformedExisting_byStartingFresh() throws Exception {
        registerQueueWithDlx("/", "orders", "dlx", "dead.orders");
        registerDirectExchange("dlx", new Binding("dlx", "dlq", "dead.orders", Map.of()));

        Headers headers = new RecordHeaders();
        headers.add(WsDeadLetterHandler.HDR_X_DEATH, "not json".getBytes(StandardCharsets.UTF_8));

        handler.deadLetter("k".getBytes(StandardCharsets.UTF_8),
            "v".getBytes(StandardCharsets.UTF_8), headers, "/", "orders", "rejected")
            .get(2, TimeUnit.SECONDS);

        Header xDeath = findHeader(sink.records.get(0).headers, WsDeadLetterHandler.HDR_X_DEATH);
        JsonNode arr = MAPPER.readTree(new String(xDeath.value(), StandardCharsets.UTF_8));
        assertEquals(1, arr.size(), "malformed prior x-death replaced by a fresh single entry");
        assertEquals("orders", arr.get(0).get("queue").asText());
    }

    // ---- Routing-key override and full routing ----

    @Test
    void dlxRoutingKey_overridden() throws Exception {
        registerQueueWithDlx("/", "orders", "dlx", "dead.orders");
        registerDirectExchange("dlx", new Binding("dlx", "dlq", "dead.orders", Map.of()));

        Headers headers = new RecordHeaders();
        headers.add(WsMessageSerializer.HDR_ROUTING_KEY, "original.key".getBytes(StandardCharsets.UTF_8));

        handler.deadLetter("k".getBytes(StandardCharsets.UTF_8),
            "v".getBytes(StandardCharsets.UTF_8), headers, "/", "orders", "rejected")
            .get(2, TimeUnit.SECONDS);

        // Record key (used as Kafka partition key) must be the overridden routing key.
        assertEquals("dead.orders", new String(sink.records.get(0).key, StandardCharsets.UTF_8));
    }

    @Test
    void dlxRoutingKey_defaultsToOriginal_whenNotOverridden() throws Exception {
        // x-dead-letter-exchange but no x-dead-letter-routing-key — fall back to original key.
        Map<String, String> args = new HashMap<>();
        args.put(WsDeadLetterHandler.ARG_DLX_EXCHANGE, "dlx");
        registerQueue("/", "orders", args);
        registerDirectExchange("dlx", new Binding("dlx", "dlq", "original.key", Map.of()));

        Headers headers = new RecordHeaders();
        headers.add(WsMessageSerializer.HDR_ROUTING_KEY, "original.key".getBytes(StandardCharsets.UTF_8));

        handler.deadLetter("k".getBytes(StandardCharsets.UTF_8),
            "v".getBytes(StandardCharsets.UTF_8), headers, "/", "orders", "rejected")
            .get(2, TimeUnit.SECONDS);

        assertEquals("original.key", new String(sink.records.get(0).key, StandardCharsets.UTF_8));
    }

    @Test
    void dlxRouting_fullExchangeResolution() throws Exception {
        // DLX is a fanout — message must reach BOTH bound DLQs.
        registerQueueWithDlx("/", "orders", "dlx", "ignored.for.fanout");
        exchangeTypes.put("dlx", "fanout");
        exchangeBindings.put("dlx", List.of(
            new Binding("dlx", "dlq-a", "", Map.of()),
            new Binding("dlx", "dlq-b", "", Map.of())
        ));

        handler.deadLetter("k".getBytes(StandardCharsets.UTF_8),
            "v".getBytes(StandardCharsets.UTF_8), new RecordHeaders(), "/", "orders", "rejected")
            .get(2, TimeUnit.SECONDS);

        assertEquals(2, sink.records.size(), "fanout DLX must produce to every bound DLQ");
        List<String> topics = new ArrayList<>();
        for (RecordingDlxSink.Record r : sink.records) {
            topics.add(r.topic);
        }
        Collections.sort(topics);
        assertEquals(List.of("ws.dlq-a", "ws.dlq-b"), topics);
    }

    @Test
    void dlxRouting_noBindings_completesWithoutProducing() throws Exception {
        registerQueueWithDlx("/", "orders", "dlx", "dead.orders");
        registerDirectExchange("dlx" /* no bindings */);

        // Future still completes successfully so caller may commit; the spec defines
        // an unrouted DLX as "best effort done".
        handler.deadLetter("k".getBytes(StandardCharsets.UTF_8),
            "v".getBytes(StandardCharsets.UTF_8), new RecordHeaders(), "/", "orders", "rejected")
            .get(2, TimeUnit.SECONDS);

        assertTrue(sink.records.isEmpty());
    }

    @Test
    void dlxRouting_unknownExchange_completesExceptionally() {
        // queue.x-dead-letter-exchange points to an exchange that does not exist.
        registerQueueWithDlx("/", "orders", "missing-dlx", "k");
        // exchangeTypes does NOT contain "missing-dlx"

        CompletableFuture<Void> f = handler.deadLetter(
            "k".getBytes(StandardCharsets.UTF_8),
            "v".getBytes(StandardCharsets.UTF_8),
            new RecordHeaders(), "/", "orders", "rejected");

        ExecutionException ex = assertThrows(ExecutionException.class,
            () -> f.get(2, TimeUnit.SECONDS));
        assertTrue(ex.getCause() instanceof IllegalArgumentException);
        assertTrue(sink.records.isEmpty());
    }

    // ---- Atomicity invariant ----

    @Test
    void dlxProduce_completesBeforeReturnedFutureCompletes() throws Exception {
        registerQueueWithDlx("/", "orders", "dlx", "dead.orders");
        registerDirectExchange("dlx", new Binding("dlx", "dlq", "dead.orders", Map.of()));

        // Use an asynchronous sink whose ack we control.
        AsyncControlledSink async = new AsyncControlledSink();
        WsDeadLetterHandler asyncHandler = new WsDeadLetterHandler(
            routingEngine,
            (vhost, queueName) -> queues.get(queueKey(vhost, queueName)),
            queue -> "ws." + queue,
            async);

        CompletableFuture<Void> f = asyncHandler.deadLetter(
            "k".getBytes(StandardCharsets.UTF_8),
            "v".getBytes(StandardCharsets.UTF_8),
            new RecordHeaders(), "/", "orders", "rejected");

        // Sink received the produce request, but ack hasn't fired → outer future MUST
        // still be incomplete. This is the atomicity invariant in observable form.
        assertEquals(1, async.pending.size());
        assertFalse(f.isDone(), "outer future must not complete before DLX produce ack");

        async.completePending(null);
        f.get(2, TimeUnit.SECONDS); // now resolves
    }

    @Test
    void dlxProduce_failure_completesExceptionally() {
        registerQueueWithDlx("/", "orders", "dlx", "dead.orders");
        registerDirectExchange("dlx", new Binding("dlx", "dlq", "dead.orders", Map.of()));

        AsyncControlledSink async = new AsyncControlledSink();
        WsDeadLetterHandler asyncHandler = new WsDeadLetterHandler(
            routingEngine,
            (vhost, queueName) -> queues.get(queueKey(vhost, queueName)),
            queue -> "ws." + queue,
            async);

        CompletableFuture<Void> f = asyncHandler.deadLetter(
            "k".getBytes(StandardCharsets.UTF_8),
            "v".getBytes(StandardCharsets.UTF_8),
            new RecordHeaders(), "/", "orders", "rejected");

        async.completePending(new RuntimeException("kafka-down"));
        ExecutionException ex = assertThrows(ExecutionException.class,
            () -> f.get(2, TimeUnit.SECONDS));
        assertTrue(ex.getCause().getMessage().contains("kafka-down"));
    }

    @Test
    void dlxProduce_partialFailure_completesExceptionally_aggregatesAcrossQueues() {
        // Two DLQs; only one fails. Per atomicity, outer future must fail.
        registerQueueWithDlx("/", "orders", "dlx", "k");
        exchangeTypes.put("dlx", "fanout");
        exchangeBindings.put("dlx", List.of(
            new Binding("dlx", "dlq-a", "", Map.of()),
            new Binding("dlx", "dlq-b", "", Map.of())
        ));

        AsyncControlledSink async = new AsyncControlledSink();
        WsDeadLetterHandler asyncHandler = new WsDeadLetterHandler(
            routingEngine,
            (vhost, queueName) -> queues.get(queueKey(vhost, queueName)),
            queue -> "ws." + queue,
            async);

        CompletableFuture<Void> f = asyncHandler.deadLetter(
            "k".getBytes(StandardCharsets.UTF_8),
            "v".getBytes(StandardCharsets.UTF_8),
            new RecordHeaders(), "/", "orders", "rejected");

        assertEquals(2, async.pending.size());
        async.completeNth(0, null);
        async.completeNth(1, new RuntimeException("dlq-b-down"));

        ExecutionException ex = assertThrows(ExecutionException.class,
            () -> f.get(2, TimeUnit.SECONDS));
        assertNotNull(ex.getCause());
    }

    // ---- Reasons ----

    @Test
    void reason_rejected_recordedInXDeath() throws Exception {
        registerQueueWithDlx("/", "orders", "dlx", "k");
        registerDirectExchange("dlx", new Binding("dlx", "dlq", "k", Map.of()));

        handler.deadLetter("k".getBytes(StandardCharsets.UTF_8),
            "v".getBytes(StandardCharsets.UTF_8), new RecordHeaders(), "/", "orders", "rejected")
            .get(2, TimeUnit.SECONDS);

        assertEquals("rejected", readXDeathReason(sink.records.get(0).headers));
    }

    @Test
    void reason_expired_recordedInXDeath() throws Exception {
        registerQueueWithDlx("/", "orders", "dlx", "k");
        registerDirectExchange("dlx", new Binding("dlx", "dlq", "k", Map.of()));

        handler.deadLetter("k".getBytes(StandardCharsets.UTF_8),
            "v".getBytes(StandardCharsets.UTF_8), new RecordHeaders(), "/", "orders", "expired")
            .get(2, TimeUnit.SECONDS);

        assertEquals("expired", readXDeathReason(sink.records.get(0).headers));
    }

    // ---- Helpers ----

    private static String queueKey(String vhost, String queueName) {
        return vhost + ":" + queueName;
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

    private static String readXDeathReason(List<Header> headers) throws Exception {
        Header h = findHeader(headers, WsDeadLetterHandler.HDR_X_DEATH);
        assertNotNull(h);
        JsonNode arr = MAPPER.readTree(new String(h.value(), StandardCharsets.UTF_8));
        return arr.get(0).get("reason").asText();
    }

    /** Captures DLX produce calls in-memory for assertions. */
    private static final class RecordingDlxSink implements WsDeadLetterHandler.DlxProduceSink {
        record Record(String topic, byte[] key, byte[] value, List<Header> headers) { }
        final List<Record> records = new CopyOnWriteArrayList<>();

        @Override
        public CompletableFuture<Void> enqueue(String topic, byte[] key, byte[] value, List<Header> headers) {
            records.add(new Record(topic, key, value, new ArrayList<>(headers)));
            return CompletableFuture.completedFuture(null);
        }
    }

    /** Sink that lets the test control when individual produce futures complete. */
    private static final class AsyncControlledSink implements WsDeadLetterHandler.DlxProduceSink {
        final List<CompletableFuture<Void>> pending = new CopyOnWriteArrayList<>();

        @Override
        public CompletableFuture<Void> enqueue(String topic, byte[] key, byte[] value, List<Header> headers) {
            CompletableFuture<Void> cf = new CompletableFuture<>();
            pending.add(cf);
            return cf;
        }

        void completePending(Throwable err) {
            for (CompletableFuture<Void> cf : pending) {
                if (err == null) {
                    cf.complete(null);
                } else {
                    cf.completeExceptionally(err);
                }
            }
        }

        void completeNth(int idx, Throwable err) {
            CompletableFuture<Void> cf = pending.get(idx);
            if (err == null) {
                cf.complete(null);
            } else {
                cf.completeExceptionally(err);
            }
        }
    }
}
