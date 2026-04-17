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

// Time: Created - TASK-WS1.06

package kafka.server.http.routing;

import kafka.server.http.ws.BindingMetadata;
import kafka.server.http.ws.ExchangeMetadata;
import kafka.server.http.ws.WsConfigs;
import kafka.server.http.ws.WsRoutingMetadataManager;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * // Time: Created - TASK-WS1.06
 */
class ExchangeManagerTest {

    private ExchangeManager exchangeManager;
    private WsRoutingMetadataManager metadataManager;
    private List<Map.Entry<String, byte[]>> writtenRecords;

    @BeforeEach
    void setUp() {
        writtenRecords = Collections.synchronizedList(new ArrayList<>());
        metadataManager = new WsRoutingMetadataManager(
            WsConfigs.withDefaults(),
            (key, value) -> writtenRecords.add(new AbstractMap.SimpleEntry<>(key, value)));
        exchangeManager = new ExchangeManager(metadataManager, WsConfigs.withDefaults());
    }

    // ------------------------------------------------------------------
    // initializeDefaults
    // ------------------------------------------------------------------

    @Test
    void initializeDefaults_creates5Exchanges() {
        exchangeManager.initializeDefaults("/");

        assertNotNull(exchangeManager.getExchange("/", ""));
        assertNotNull(exchangeManager.getExchange("/", "amq.direct"));
        assertNotNull(exchangeManager.getExchange("/", "amq.topic"));
        assertNotNull(exchangeManager.getExchange("/", "amq.fanout"));
        assertNotNull(exchangeManager.getExchange("/", "amq.headers"));
    }

    @Test
    void initializeDefaults_correctTypes() {
        exchangeManager.initializeDefaults("/");

        assertEquals("direct", exchangeManager.getExchange("/", "").type());
        assertEquals("direct", exchangeManager.getExchange("/", "amq.direct").type());
        assertEquals("topic", exchangeManager.getExchange("/", "amq.topic").type());
        assertEquals("fanout", exchangeManager.getExchange("/", "amq.fanout").type());
        assertEquals("headers", exchangeManager.getExchange("/", "amq.headers").type());
    }

    @Test
    void initializeDefaults_durableTrue_autoDeleteFalse_internalFalse() {
        exchangeManager.initializeDefaults("/");

        for (String name : new String[]{"", "amq.direct", "amq.topic", "amq.fanout", "amq.headers"}) {
            ExchangeMetadata ex = exchangeManager.getExchange("/", name);
            assertTrue(ex.durable(), "default exchange '" + name + "' must be durable");
            assertFalse(ex.autoDelete(), "default exchange '" + name + "' must not be autoDelete");
            assertFalse(ex.internal(), "default exchange '" + name + "' must not be internal");
            assertTrue(ex.arguments().isEmpty(), "default exchange '" + name + "' must have empty arguments");
        }
    }

    @Test
    void initializeDefaults_doesNotPersistToMetadataTopic() {
        exchangeManager.initializeDefaults("/");
        // Defaults are synthesized, not written to the __ws_routing_metadata topic.
        assertTrue(writtenRecords.isEmpty(),
            "initializeDefaults must not write to the metadata topic (it is synthetic)");
    }

    @Test
    void initializeDefaults_isIdempotent() {
        exchangeManager.initializeDefaults("/");
        exchangeManager.initializeDefaults("/");

        assertEquals(5, metadataManager.exchangeCount("/"));
    }

    // ------------------------------------------------------------------
    // declareExchange
    // ------------------------------------------------------------------

    @Test
    void declareExchange_newExchange_succeeds() throws ExchangeException {
        exchangeManager.initializeDefaults("/");

        ExchangeMetadata result = exchangeManager.declareExchange(
            "/", "events", "topic", true, false, false, false, Collections.emptyMap());

        assertNotNull(result);
        assertEquals("topic", result.type());
        assertEquals("events", result.name());
        assertEquals("/", result.vhost());
        assertNotNull(exchangeManager.getExchange("/", "events"));
    }

    @Test
    void declareExchange_newExchange_persistsToMetadataTopic() throws ExchangeException {
        exchangeManager.initializeDefaults("/");
        writtenRecords.clear();

        exchangeManager.declareExchange(
            "/", "events", "topic", true, false, false, false, Collections.emptyMap());

        assertEquals(1, writtenRecords.size());
        assertTrue(writtenRecords.get(0).getKey().startsWith("exchange:"));
        assertNotNull(writtenRecords.get(0).getValue());
    }

    @Test
    void declareExchange_sameType_idempotent() throws ExchangeException {
        exchangeManager.initializeDefaults("/");

        ExchangeMetadata first = exchangeManager.declareExchange(
            "/", "events", "topic", true, false, false, false, Collections.emptyMap());
        writtenRecords.clear();
        ExchangeMetadata second = exchangeManager.declareExchange(
            "/", "events", "topic", true, false, false, false, Collections.emptyMap());

        assertNotNull(first);
        assertNotNull(second);
        // Idempotent re-declare should NOT cause another metadata write.
        assertTrue(writtenRecords.isEmpty(),
            "idempotent re-declare must not re-write to the metadata topic");
    }

    @Test
    void declareExchange_differentType_throwsMismatch() throws ExchangeException {
        exchangeManager.initializeDefaults("/");
        exchangeManager.declareExchange(
            "/", "events", "topic", true, false, false, false, Collections.emptyMap());

        ExchangeException ex = assertThrows(ExchangeException.class, () ->
            exchangeManager.declareExchange(
                "/", "events", "direct", true, false, false, false, Collections.emptyMap()));

        assertEquals(ExchangeException.ErrorCode.EXCHANGE_TYPE_MISMATCH, ex.errorCode());
    }

    @Test
    void declareExchange_passive_existingExchange_succeeds() throws ExchangeException {
        exchangeManager.initializeDefaults("/");

        // Passive declare on a default exchange should return its metadata.
        ExchangeMetadata result = exchangeManager.declareExchange(
            "/", "amq.direct", "direct", true, false, true, false, Collections.emptyMap());

        assertNotNull(result);
        assertEquals("direct", result.type());
    }

    @Test
    void declareExchange_passive_typeIgnoredWhenExisting() throws ExchangeException {
        exchangeManager.initializeDefaults("/");

        // Passive declare should return the stored metadata even when the
        // caller-supplied type differs — passive = lookup only.
        ExchangeMetadata result = exchangeManager.declareExchange(
            "/", "amq.topic", "direct", true, false, true, false, Collections.emptyMap());

        assertNotNull(result);
        assertEquals("topic", result.type(), "passive declare returns stored type, not requested type");
    }

    @Test
    void declareExchange_passive_nonExistent_throwsNotFound() {
        exchangeManager.initializeDefaults("/");

        ExchangeException ex = assertThrows(ExchangeException.class, () ->
            exchangeManager.declareExchange(
                "/", "nonexistent", "direct", true, false, true, false, Collections.emptyMap()));

        assertEquals(ExchangeException.ErrorCode.EXCHANGE_NOT_FOUND, ex.errorCode());
    }

    @Test
    void declareExchange_passive_doesNotWriteToMetadata() {
        exchangeManager.initializeDefaults("/");
        writtenRecords.clear();

        assertThrows(ExchangeException.class, () ->
            exchangeManager.declareExchange(
                "/", "absent", "direct", true, false, true, false, Collections.emptyMap()));

        assertTrue(writtenRecords.isEmpty(), "passive declare must never write");
    }

    @Test
    void declareExchange_respectsAllFields() throws ExchangeException {
        exchangeManager.initializeDefaults("/");
        Map<String, String> args = new HashMap<>();
        args.put("alternate-exchange", "amq.fanout");

        ExchangeMetadata result = exchangeManager.declareExchange(
            "/", "audit", "headers", false, true, false, true, args);

        assertFalse(result.durable());
        assertTrue(result.autoDelete());
        assertTrue(result.internal());
        assertEquals("amq.fanout", result.arguments().get("alternate-exchange"));
    }

    // ------------------------------------------------------------------
    // deleteExchange
    // ------------------------------------------------------------------

    @Test
    void deleteExchange_defaultExchange_throwsProtected() {
        exchangeManager.initializeDefaults("/");

        for (String name : new String[]{"", "amq.direct", "amq.topic", "amq.fanout", "amq.headers"}) {
            ExchangeException ex = assertThrows(ExchangeException.class, () ->
                exchangeManager.deleteExchange("/", name, false));
            assertEquals(ExchangeException.ErrorCode.EXCHANGE_PROTECTED, ex.errorCode(),
                "delete of default '" + name + "' must throw EXCHANGE_PROTECTED");
        }
    }

    @Test
    void deleteExchange_nonExistent_noOp() {
        exchangeManager.initializeDefaults("/");

        assertDoesNotThrow(() -> exchangeManager.deleteExchange("/", "nonexistent", false));
        assertDoesNotThrow(() -> exchangeManager.deleteExchange("/", "nonexistent", true));
    }

    @Test
    void deleteExchange_existingExchange_removesFromCache() throws ExchangeException {
        exchangeManager.initializeDefaults("/");
        exchangeManager.declareExchange(
            "/", "events", "topic", true, false, false, false, Collections.emptyMap());
        assertNotNull(exchangeManager.getExchange("/", "events"));

        exchangeManager.deleteExchange("/", "events", false);

        assertNull(exchangeManager.getExchange("/", "events"));
    }

    @Test
    void deleteExchange_existingExchange_emitsTombstone() throws ExchangeException {
        exchangeManager.initializeDefaults("/");
        exchangeManager.declareExchange(
            "/", "events", "topic", true, false, false, false, Collections.emptyMap());
        writtenRecords.clear();

        exchangeManager.deleteExchange("/", "events", false);

        // Expect a single tombstone (null value) record written.
        assertEquals(1, writtenRecords.size());
        assertTrue(writtenRecords.get(0).getKey().startsWith("exchange:"));
        assertNull(writtenRecords.get(0).getValue(), "tombstone must have null value");
    }

    @Test
    void deleteExchange_ifUnusedWithBindings_throwsInUse() throws ExchangeException {
        exchangeManager.initializeDefaults("/");
        exchangeManager.declareExchange(
            "/", "events", "topic", true, false, false, false, Collections.emptyMap());
        // Inject a binding directly into the metadata manager to simulate "in use".
        metadataManager.writeBinding(new BindingMetadata(
            "/", "events", "q1", "user.*", Collections.emptyMap()));

        ExchangeException ex = assertThrows(ExchangeException.class, () ->
            exchangeManager.deleteExchange("/", "events", true));
        assertEquals(ExchangeException.ErrorCode.EXCHANGE_IN_USE, ex.errorCode());
        // Exchange must still exist after a rejected delete.
        assertNotNull(exchangeManager.getExchange("/", "events"));
    }

    @Test
    void deleteExchange_ifUnusedNoBindings_succeeds() throws ExchangeException {
        exchangeManager.initializeDefaults("/");
        exchangeManager.declareExchange(
            "/", "events", "topic", true, false, false, false, Collections.emptyMap());

        exchangeManager.deleteExchange("/", "events", true);

        assertNull(exchangeManager.getExchange("/", "events"));
    }

    @Test
    void deleteExchange_ifUnusedFalseWithBindings_cascades() throws ExchangeException {
        exchangeManager.initializeDefaults("/");
        exchangeManager.declareExchange(
            "/", "events", "topic", true, false, false, false, Collections.emptyMap());
        metadataManager.writeBinding(new BindingMetadata(
            "/", "events", "q1", "user.*", Collections.emptyMap()));

        // ifUnused=false allows deletion even with bindings; they are cascaded.
        exchangeManager.deleteExchange("/", "events", false);

        assertNull(exchangeManager.getExchange("/", "events"));
        assertEquals(0, metadataManager.bindingCount("/", "events"));
    }

    // ------------------------------------------------------------------
    // Listing / helpers
    // ------------------------------------------------------------------

    @Test
    void listExchanges_returnsAllIncludingDefaults() throws ExchangeException {
        exchangeManager.initializeDefaults("/");
        exchangeManager.declareExchange(
            "/", "events", "topic", true, false, false, false, Collections.emptyMap());
        exchangeManager.declareExchange(
            "/", "orders", "direct", true, false, false, false, Collections.emptyMap());

        Collection<ExchangeMetadata> all = exchangeManager.listExchanges("/");

        assertEquals(7, all.size(), "5 defaults + 2 user exchanges");
    }

    @Test
    void listExchanges_isolatedByVhost() throws ExchangeException {
        exchangeManager.initializeDefaults("/");
        exchangeManager.initializeDefaults("/other");
        exchangeManager.declareExchange(
            "/", "events", "topic", true, false, false, false, Collections.emptyMap());

        assertEquals(6, exchangeManager.listExchanges("/").size());
        assertEquals(5, exchangeManager.listExchanges("/other").size());
    }

    @Test
    void isDefaultExchange_trueForDefaults() {
        assertTrue(exchangeManager.isDefaultExchange(""));
        assertTrue(exchangeManager.isDefaultExchange("amq.direct"));
        assertTrue(exchangeManager.isDefaultExchange("amq.topic"));
        assertTrue(exchangeManager.isDefaultExchange("amq.fanout"));
        assertTrue(exchangeManager.isDefaultExchange("amq.headers"));
    }

    @Test
    void isDefaultExchange_falseForUserExchanges() {
        assertFalse(exchangeManager.isDefaultExchange("events"));
        assertFalse(exchangeManager.isDefaultExchange("my-exchange"));
        assertFalse(exchangeManager.isDefaultExchange("amq.custom")); // not in the 5 reserved names
        assertFalse(exchangeManager.isDefaultExchange("AMQ.direct")); // case sensitive
    }

    // ------------------------------------------------------------------
    // Quota
    // ------------------------------------------------------------------

    @Test
    void declareExchange_exceedsLimit_throwsLimitExceeded() throws ExchangeException {
        // Build a config with maxExchangesPerVhost = 6 so 5 defaults + 1 user exchange fits
        // but the 7th fails.
        WsConfigs lowLimit = lowLimitConfigs(6);
        WsRoutingMetadataManager mgr = new WsRoutingMetadataManager(lowLimit, (k, v) -> { });
        ExchangeManager limited = new ExchangeManager(mgr, lowLimit);
        limited.initializeDefaults("/");

        limited.declareExchange(
            "/", "one", "direct", true, false, false, false, Collections.emptyMap());

        ExchangeException ex = assertThrows(ExchangeException.class, () ->
            limited.declareExchange(
                "/", "two", "direct", true, false, false, false, Collections.emptyMap()));
        assertEquals(ExchangeException.ErrorCode.EXCHANGE_LIMIT_EXCEEDED, ex.errorCode());
    }

    @Test
    void declareExchange_atLimit_idempotentReDeclareAllowed() throws ExchangeException {
        WsConfigs lowLimit = lowLimitConfigs(6);
        WsRoutingMetadataManager mgr = new WsRoutingMetadataManager(lowLimit, (k, v) -> { });
        ExchangeManager limited = new ExchangeManager(mgr, lowLimit);
        limited.initializeDefaults("/");
        limited.declareExchange(
            "/", "one", "direct", true, false, false, false, Collections.emptyMap());

        // Same-name re-declare at the limit must NOT be rejected: it's a no-op.
        assertDoesNotThrow(() -> limited.declareExchange(
            "/", "one", "direct", true, false, false, false, Collections.emptyMap()));
    }

    // ------------------------------------------------------------------
    // Concurrency
    // ------------------------------------------------------------------

    @Test
    void declareExchange_concurrent_noDuplicatesNoCorruption() throws Exception {
        exchangeManager.initializeDefaults("/");

        int threads = 8;
        int perThread = 50;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger failures = new AtomicInteger();
        List<java.util.concurrent.Future<?>> futures = new ArrayList<>();

        for (int t = 0; t < threads; t++) {
            futures.add(pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < perThread; i++) {
                        // Mix of unique-per-thread + shared names so threads race on both.
                        String shared = "shared-" + (i % 10);
                        exchangeManager.declareExchange(
                            "/", shared, "topic", true, false, false, false, Collections.emptyMap());
                    }
                } catch (Exception e) {
                    failures.incrementAndGet();
                }
            }));
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
        for (java.util.concurrent.Future<?> f : futures) {
            f.get(1, TimeUnit.SECONDS);
        }
        assertEquals(0, failures.get(), "concurrent idempotent declares must not fail");
        // 5 defaults + 10 shared = 15
        assertEquals(15, metadataManager.exchangeCount("/"));
        // Every "shared-N" should be a topic exchange (not mangled).
        for (int i = 0; i < 10; i++) {
            assertEquals("topic", exchangeManager.getExchange("/", "shared-" + i).type());
        }
    }

    @Test
    void declareAndDelete_concurrent_cacheStaysConsistent() throws Exception {
        exchangeManager.initializeDefaults("/");
        int iters = 200;
        ExecutorService pool = Executors.newFixedThreadPool(4);
        CountDownLatch start = new CountDownLatch(1);

        Runnable declarer = () -> {
            try {
                start.await();
                for (int i = 0; i < iters; i++) {
                    exchangeManager.declareExchange(
                        "/", "churn", "direct", true, false, false, false, Collections.emptyMap());
                }
            } catch (Exception e) {
                // either outcome is fine; we just want the cache to stay valid
            }
        };
        Runnable deleter = () -> {
            try {
                start.await();
                for (int i = 0; i < iters; i++) {
                    exchangeManager.deleteExchange("/", "churn", false);
                }
            } catch (Exception e) {
                // same as above
            }
        };

        pool.submit(declarer);
        pool.submit(declarer);
        pool.submit(deleter);
        pool.submit(deleter);
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(15, TimeUnit.SECONDS));

        // Final state is either present or absent; both are legal and the cache must be queryable.
        ExchangeMetadata finalState = exchangeManager.getExchange("/", "churn");
        if (finalState != null) {
            assertEquals("direct", finalState.type());
        }
        // The 5 defaults must still be intact.
        assertEquals("direct", exchangeManager.getExchange("/", "amq.direct").type());
        assertEquals("topic", exchangeManager.getExchange("/", "amq.topic").type());
    }

    // ------------------------------------------------------------------
    // helpers
    // ------------------------------------------------------------------

    private WsConfigs lowLimitConfigs(int maxExchangesPerVhost) {
        WsConfigs defaults = WsConfigs.withDefaults();
        return new WsConfigs(
            defaults.wsEnabled(),
            defaults.maxFrameSize(),
            defaults.maxSubscriptionsPerConnection(),
            defaults.defaultCredits(),
            defaults.maxCredits(),
            defaults.topicPrefix(),
            defaults.defaultQueuePartitions(),
            defaults.metadataTopic(),
            defaults.metadataReplicationFactor(),
            defaults.ackCommitIntervalMs(),
            defaults.consumerStartOffset(),
            defaults.numConsumerThreads(),
            defaults.consumerMaxWaitMs(),
            defaults.consumerMaxBytes(),
            defaults.publishTimeoutMs(),
            defaults.connectionMaxIdleMs(),
            defaults.shutdownDrainMs(),
            defaults.maxRedeliveryCount(),
            defaults.dedupEnabled(),
            defaults.dedupCacheSize(),
            defaults.dedupCacheTtlMs(),
            defaults.maxConnectionsPerBroker(),
            maxExchangesPerVhost,
            defaults.maxQueuesPerVhost(),
            defaults.maxBindingsPerExchange(),
            defaults.maxControlMessagesPerSecond(),
            defaults.consumerAckTimeoutMs(),
            defaults.metadataStartupTimeoutMs()
        );
    }
}
