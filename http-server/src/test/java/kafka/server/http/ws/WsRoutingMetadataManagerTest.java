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

// Time: Created - TASK-WS1.05

package kafka.server.http.ws;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * // Time: Created - TASK-WS1.05
 */
class WsRoutingMetadataManagerTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private WsRoutingMetadataManager manager;
    private List<Map.Entry<String, byte[]>> writtenRecords;

    @BeforeEach
    void setUp() {
        // Use a synchronized list so concurrent write tests don't lose records due to
        // ArrayList's non-thread-safety (the manager itself can be called concurrently).
        writtenRecords = Collections.synchronizedList(new ArrayList<>());
        manager = new WsRoutingMetadataManager(
            WsConfigs.withDefaults(),
            (key, value) -> writtenRecords.add(new AbstractMap.SimpleEntry<>(key, value)));
    }

    // --- Write / cache tests ---

    @Test
    void writeExchange_addsToCache() {
        ExchangeMetadata ex = new ExchangeMetadata("events", "/", "topic", true, false, false, Collections.emptyMap());
        manager.writeExchange(ex);
        assertNotNull(manager.getExchange("/", "events"));
        assertEquals("topic", manager.getExchange("/", "events").type());
    }

    @Test
    void writeExchange_writesToRecordWriter() {
        ExchangeMetadata ex = new ExchangeMetadata("test", "/", "direct", true, false, false, Collections.emptyMap());
        manager.writeExchange(ex);
        assertEquals(1, writtenRecords.size());
        assertTrue(writtenRecords.get(0).getKey().startsWith("exchange:"));
        assertNotNull(writtenRecords.get(0).getValue());
    }

    @Test
    void deleteExchange_removesFromCache() {
        ExchangeMetadata ex = new ExchangeMetadata("events", "/", "topic", true, false, false, Collections.emptyMap());
        manager.writeExchange(ex);
        manager.deleteExchange("/", "events");
        assertNull(manager.getExchange("/", "events"));
    }

    @Test
    void deleteExchange_writesTombstone() {
        ExchangeMetadata ex = new ExchangeMetadata("events", "/", "topic", true, false, false, Collections.emptyMap());
        manager.writeExchange(ex);
        writtenRecords.clear();
        manager.deleteExchange("/", "events");
        assertEquals(1, writtenRecords.size());
        assertEquals("exchange:/:events", writtenRecords.get(0).getKey());
        assertNull(writtenRecords.get(0).getValue());
    }

    @Test
    void writeQueue_addsToCache() {
        QueueMetadata q = new QueueMetadata("orders", "/", true, false, false, Collections.emptyMap());
        manager.writeQueue(q);
        assertNotNull(manager.getQueue("/", "orders"));
        assertTrue(manager.getQueue("/", "orders").durable());
    }

    @Test
    void deleteQueue_removesFromCache() {
        QueueMetadata q = new QueueMetadata("orders", "/", true, false, false, Collections.emptyMap());
        manager.writeQueue(q);
        manager.deleteQueue("/", "orders");
        assertNull(manager.getQueue("/", "orders"));
    }

    @Test
    void deleteQueue_writesTombstone() {
        QueueMetadata q = new QueueMetadata("orders", "/", true, false, false, Collections.emptyMap());
        manager.writeQueue(q);
        writtenRecords.clear();
        manager.deleteQueue("/", "orders");
        assertEquals(1, writtenRecords.size());
        assertEquals("queue:/:orders", writtenRecords.get(0).getKey());
        assertNull(writtenRecords.get(0).getValue());
    }

    @Test
    void writeBinding_addsToBindingsList() {
        BindingMetadata b = new BindingMetadata("/", "events", "q1", "order.#", Collections.emptyMap());
        manager.writeBinding(b);
        assertEquals(1, manager.getBindings("/", "events").size());
        assertEquals("order.#", manager.getBindings("/", "events").get(0).routingKey());
    }

    @Test
    void writeBinding_multipleBindingsSameExchange() {
        manager.writeBinding(new BindingMetadata("/", "events", "q1", "order.#", Collections.emptyMap()));
        manager.writeBinding(new BindingMetadata("/", "events", "q2", "payment.#", Collections.emptyMap()));
        assertEquals(2, manager.getBindings("/", "events").size());
    }

    @Test
    void deleteBinding_removesFromBindingsList() {
        manager.writeBinding(new BindingMetadata("/", "events", "q1", "order.#", Collections.emptyMap()));
        manager.writeBinding(new BindingMetadata("/", "events", "q2", "payment.#", Collections.emptyMap()));
        manager.deleteBinding("/", "events", "q1", "order.#", Collections.emptyMap());
        List<BindingMetadata> remaining = manager.getBindings("/", "events");
        assertEquals(1, remaining.size());
        assertEquals("q2", remaining.get(0).queue());
    }

    @Test
    void deleteBinding_writesTombstone() {
        manager.writeBinding(new BindingMetadata("/", "events", "q1", "order.#", Collections.emptyMap()));
        writtenRecords.clear();
        manager.deleteBinding("/", "events", "q1", "order.#", Collections.emptyMap());
        assertEquals(1, writtenRecords.size());
        assertTrue(writtenRecords.get(0).getKey().startsWith("binding:/:events:q1:order.#:"));
        assertNull(writtenRecords.get(0).getValue());
    }

    @Test
    void getBindings_emptyWhenNoBindings() {
        assertTrue(manager.getBindings("/", "unknown").isEmpty());
    }

    // --- Counting / listing ---

    @Test
    void listExchanges_filtersByVhost() {
        manager.writeExchange(new ExchangeMetadata("e1", "/", "direct", true, false, false, Collections.emptyMap()));
        manager.writeExchange(new ExchangeMetadata("e2", "/", "topic", true, false, false, Collections.emptyMap()));
        manager.writeExchange(new ExchangeMetadata("e3", "/tenant-a", "fanout", true, false, false, Collections.emptyMap()));
        Collection<ExchangeMetadata> root = manager.listExchanges("/");
        assertEquals(2, root.size());
        Collection<ExchangeMetadata> tenantA = manager.listExchanges("/tenant-a");
        assertEquals(1, tenantA.size());
    }

    @Test
    void listQueues_filtersByVhost() {
        manager.writeQueue(new QueueMetadata("q1", "/", true, false, false, Collections.emptyMap()));
        manager.writeQueue(new QueueMetadata("q2", "/tenant-a", true, false, false, Collections.emptyMap()));
        assertEquals(1, manager.listQueues("/").size());
        assertEquals(1, manager.listQueues("/tenant-a").size());
        assertEquals(0, manager.listQueues("/unknown").size());
    }

    @Test
    void exchangeCount_countsOnlyInVhost() {
        manager.writeExchange(new ExchangeMetadata("e1", "/", "direct", true, false, false, Collections.emptyMap()));
        manager.writeExchange(new ExchangeMetadata("e2", "/", "topic", true, false, false, Collections.emptyMap()));
        manager.writeExchange(new ExchangeMetadata("e3", "/tenant-a", "fanout", true, false, false, Collections.emptyMap()));
        assertEquals(2, manager.exchangeCount("/"));
        assertEquals(1, manager.exchangeCount("/tenant-a"));
        assertEquals(0, manager.exchangeCount("/unknown"));
    }

    @Test
    void queueCount_countsOnlyInVhost() {
        manager.writeQueue(new QueueMetadata("q1", "/", true, false, false, Collections.emptyMap()));
        manager.writeQueue(new QueueMetadata("q2", "/", true, false, false, Collections.emptyMap()));
        manager.writeQueue(new QueueMetadata("q3", "/tenant-a", true, false, false, Collections.emptyMap()));
        assertEquals(2, manager.queueCount("/"));
        assertEquals(1, manager.queueCount("/tenant-a"));
    }

    @Test
    void bindingCount_perExchange() {
        manager.writeBinding(new BindingMetadata("/", "events", "q1", "a", Collections.emptyMap()));
        manager.writeBinding(new BindingMetadata("/", "events", "q2", "b", Collections.emptyMap()));
        manager.writeBinding(new BindingMetadata("/", "orders", "q3", "c", Collections.emptyMap()));
        assertEquals(2, manager.bindingCount("/", "events"));
        assertEquals(1, manager.bindingCount("/", "orders"));
        assertEquals(0, manager.bindingCount("/", "unknown"));
    }

    // --- Key format ---

    @Test
    void exchangeKey_format() {
        assertEquals("exchange:/:events", WsRoutingMetadataManager.exchangeKey("/", "events"));
    }

    @Test
    void queueKey_format() {
        assertEquals("queue:/:orders", WsRoutingMetadataManager.queueKey("/", "orders"));
    }

    @Test
    void bindingKey_includesArgsHash() {
        String key = WsRoutingMetadataManager.bindingKey("/", "events", "q1", "order.#", Collections.emptyMap());
        assertTrue(key.startsWith("binding:/:events:q1:order.#:"));
    }

    @Test
    void argsHash_deterministic() {
        Map<String, String> args1 = new HashMap<>();
        args1.put("b", "2");
        args1.put("a", "1");
        Map<String, String> args2 = new HashMap<>();
        args2.put("a", "1");
        args2.put("b", "2");
        assertEquals(WsRoutingMetadataManager.argsHash(args1), WsRoutingMetadataManager.argsHash(args2));
    }

    @Test
    void argsHash_emptyReturnsZero() {
        assertEquals(0, WsRoutingMetadataManager.argsHash(Collections.emptyMap()));
        assertEquals(0, WsRoutingMetadataManager.argsHash(null));
    }

    @Test
    void argsHash_differentArgsDifferentHash() {
        Map<String, String> args1 = new HashMap<>();
        args1.put("a", "1");
        Map<String, String> args2 = new HashMap<>();
        args2.put("a", "2");
        // Not strictly required to differ, but with our hashing scheme they should.
        assertFalse(WsRoutingMetadataManager.argsHash(args1) == WsRoutingMetadataManager.argsHash(args2));
    }

    // --- Replay ---

    @Test
    void applyRecord_exchangeWrite_populatesCache() throws Exception {
        ExchangeMetadata ex = new ExchangeMetadata("events", "/", "topic", true, false, false, Collections.emptyMap());
        byte[] json = MAPPER.writeValueAsBytes(Map.of(
            "name", ex.name(),
            "vhost", ex.vhost(),
            "type", ex.type(),
            "durable", ex.durable(),
            "autoDelete", ex.autoDelete(),
            "internal", ex.internal(),
            "arguments", ex.arguments()
        ));
        manager.applyRecord("exchange:/:events", json);
        ExchangeMetadata got = manager.getExchange("/", "events");
        assertNotNull(got);
        assertEquals("topic", got.type());
        assertTrue(got.durable());
    }

    @Test
    void applyRecord_queueWrite_populatesCache() throws Exception {
        QueueMetadata q = new QueueMetadata("orders", "/", true, false, false, Collections.emptyMap());
        byte[] json = MAPPER.writeValueAsBytes(Map.of(
            "name", q.name(),
            "vhost", q.vhost(),
            "durable", q.durable(),
            "exclusive", q.exclusive(),
            "autoDelete", q.autoDelete(),
            "arguments", q.arguments()
        ));
        manager.applyRecord("queue:/:orders", json);
        assertNotNull(manager.getQueue("/", "orders"));
    }

    @Test
    void applyRecord_bindingWrite_populatesCache() throws Exception {
        BindingMetadata b = new BindingMetadata("/", "events", "q1", "order.#", Collections.emptyMap());
        byte[] json = MAPPER.writeValueAsBytes(Map.of(
            "vhost", b.vhost(),
            "exchange", b.exchange(),
            "queue", b.queue(),
            "routingKey", b.routingKey(),
            "arguments", b.arguments()
        ));
        String key = WsRoutingMetadataManager.bindingKey("/", "events", "q1", "order.#", Collections.emptyMap());
        manager.applyRecord(key, json);
        assertEquals(1, manager.getBindings("/", "events").size());
    }

    @Test
    void applyRecord_exchangeTombstone_removesFromCache() {
        manager.writeExchange(new ExchangeMetadata("events", "/", "topic", true, false, false, Collections.emptyMap()));
        manager.applyRecord("exchange:/:events", null);
        assertNull(manager.getExchange("/", "events"));
    }

    @Test
    void applyRecord_queueTombstone_removesFromCache() {
        manager.writeQueue(new QueueMetadata("orders", "/", true, false, false, Collections.emptyMap()));
        manager.applyRecord("queue:/:orders", null);
        assertNull(manager.getQueue("/", "orders"));
    }

    @Test
    void applyRecord_bindingTombstone_removesFromList() {
        manager.writeBinding(new BindingMetadata("/", "events", "q1", "order.#", Collections.emptyMap()));
        manager.writeBinding(new BindingMetadata("/", "events", "q2", "payment.#", Collections.emptyMap()));
        String key = WsRoutingMetadataManager.bindingKey("/", "events", "q1", "order.#", Collections.emptyMap());
        manager.applyRecord(key, null);
        List<BindingMetadata> remaining = manager.getBindings("/", "events");
        assertEquals(1, remaining.size());
        assertEquals("q2", remaining.get(0).queue());
    }

    @Test
    void applyRecord_unknownKeyPrefix_ignored() {
        // Should not throw
        manager.applyRecord("unknown:foo:bar", new byte[]{1, 2, 3});
        assertTrue(manager.listExchanges("/").isEmpty());
    }

    @Test
    void replayComplete_defaultFalse() {
        assertFalse(manager.isReplayComplete());
    }

    @Test
    void markReplayComplete_setsTrue() {
        manager.markReplayComplete();
        assertTrue(manager.isReplayComplete());
    }

    // --- Concurrency ---

    @Test
    void concurrentWrites_doNotCorruptCache() throws Exception {
        int threads = 8;
        int writesPerThread = 100;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(threads);
        try {
            for (int t = 0; t < threads; t++) {
                final int threadId = t;
                pool.submit(() -> {
                    try {
                        for (int i = 0; i < writesPerThread; i++) {
                            String name = "ex-" + threadId + "-" + i;
                            manager.writeExchange(new ExchangeMetadata(name, "/", "direct",
                                true, false, false, Collections.emptyMap()));
                        }
                    } finally {
                        latch.countDown();
                    }
                });
            }
            assertTrue(latch.await(30, TimeUnit.SECONDS));
            assertEquals(threads * writesPerThread, manager.exchangeCount("/"));
            assertEquals(threads * writesPerThread, writtenRecords.size());
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void concurrentBindingWrites_allRecorded() throws Exception {
        int threads = 8;
        int writesPerThread = 50;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(threads);
        try {
            for (int t = 0; t < threads; t++) {
                final int threadId = t;
                pool.submit(() -> {
                    try {
                        for (int i = 0; i < writesPerThread; i++) {
                            String queue = "q-" + threadId + "-" + i;
                            manager.writeBinding(new BindingMetadata("/", "events", queue,
                                "rk", Collections.emptyMap()));
                        }
                    } finally {
                        latch.countDown();
                    }
                });
            }
            assertTrue(latch.await(30, TimeUnit.SECONDS));
            assertEquals(threads * writesPerThread, manager.bindingCount("/", "events"));
        } finally {
            pool.shutdownNow();
        }
    }

    // --- Roundtrip: write → record bytes → applyRecord rebuilds cache ---

    @Test
    void writeThenReplayViaAppliedRecords_rebuildsCache() {
        manager.writeExchange(new ExchangeMetadata("events", "/", "topic", true, false, false, Collections.emptyMap()));
        manager.writeQueue(new QueueMetadata("orders", "/", true, false, false, Collections.emptyMap()));
        manager.writeBinding(new BindingMetadata("/", "events", "orders", "order.#", Collections.emptyMap()));

        // Snapshot records
        List<Map.Entry<String, byte[]>> records = new ArrayList<>(writtenRecords);

        // Simulate broker restart: fresh manager, replay produced records.
        WsRoutingMetadataManager replayed = new WsRoutingMetadataManager(
            WsConfigs.withDefaults(), (k, v) -> { });
        for (Map.Entry<String, byte[]> r : records) {
            replayed.applyRecord(r.getKey(), r.getValue());
        }

        assertNotNull(replayed.getExchange("/", "events"));
        assertEquals("topic", replayed.getExchange("/", "events").type());
        assertNotNull(replayed.getQueue("/", "orders"));
        assertEquals(1, replayed.getBindings("/", "events").size());
    }

    @Test
    void writeThenDelete_replayReflectsDelete() {
        manager.writeExchange(new ExchangeMetadata("events", "/", "topic", true, false, false, Collections.emptyMap()));
        manager.deleteExchange("/", "events");

        // Snapshot records — both write and tombstone should be present.
        List<Map.Entry<String, byte[]>> records = new ArrayList<>(writtenRecords);
        assertEquals(2, records.size());

        WsRoutingMetadataManager replayed = new WsRoutingMetadataManager(
            WsConfigs.withDefaults(), (k, v) -> { });
        for (Map.Entry<String, byte[]> r : records) {
            replayed.applyRecord(r.getKey(), r.getValue());
        }
        assertNull(replayed.getExchange("/", "events"));
    }

    // --- Serialization shape ---

    @Test
    void writeExchange_serializedValueIsJson() throws Exception {
        ExchangeMetadata ex = new ExchangeMetadata("events", "/", "topic", true, false, false,
            Map.of("x-max-length", "1000"));
        manager.writeExchange(ex);
        byte[] value = writtenRecords.get(0).getValue();
        assertNotNull(value);
        String json = new String(value, StandardCharsets.UTF_8);
        assertTrue(json.contains("\"events\""));
        assertTrue(json.contains("\"topic\""));
        assertTrue(json.contains("x-max-length"));
    }

    @Test
    void exchangeMetadata_argumentsUnmodifiable() {
        Map<String, String> args = new HashMap<>();
        args.put("k", "v");
        ExchangeMetadata ex = new ExchangeMetadata("e", "/", "direct", true, false, false, args);
        try {
            ex.arguments().put("k2", "v2");
            // Unmodifiable: expect exception
            throw new AssertionError("expected UnsupportedOperationException");
        } catch (UnsupportedOperationException expected) {
            // pass
        }
    }

    @Test
    void exchangeMetadata_nullArgumentsBecomesEmpty() {
        ExchangeMetadata ex = new ExchangeMetadata("e", "/", "direct", true, false, false, null);
        assertNotNull(ex.arguments());
        assertTrue(ex.arguments().isEmpty());
    }

    @Test
    void writeExchange_recordValueSurvivesRoundtrip() throws Exception {
        ExchangeMetadata ex = new ExchangeMetadata("events", "/", "topic", true, true, false, Map.of("a", "1"));
        manager.writeExchange(ex);
        byte[] value = writtenRecords.get(0).getValue();

        // Apply the bytes back on a fresh manager.
        WsRoutingMetadataManager replayed = new WsRoutingMetadataManager(
            WsConfigs.withDefaults(), (k, v) -> { });
        replayed.applyRecord(writtenRecords.get(0).getKey(), value);

        ExchangeMetadata got = replayed.getExchange("/", "events");
        assertNotNull(got);
        assertEquals("events", got.name());
        assertEquals("topic", got.type());
        assertTrue(got.durable());
        assertTrue(got.autoDelete());
        assertFalse(got.internal());
        assertEquals("1", got.arguments().get("a"));
        // round-trip value must not be empty
        assertFalse(value.length == 0);
        // exact bytes don't need to match - semantic equality suffices
        assertArrayEquals(value, writtenRecords.get(0).getValue());
    }
}
