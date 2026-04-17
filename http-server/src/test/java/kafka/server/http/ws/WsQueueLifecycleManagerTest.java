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

// Time: Created - TASK-WS4.07

package kafka.server.http.ws;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link WsQueueLifecycleManager} — TASK-WS4.07.
 *
 * <p>Covers auto-delete on last unsubscribe, x-expires idle timeout, server-
 * generated queue-name format and uniqueness, redeclare flag validation,
 * exclusive-queue cleanup on connection close, and atomicity under concurrent
 * unsubscribe.
 */
class WsQueueLifecycleManagerTest {

    private ScheduledExecutorService scheduler;
    private ConcurrentLinkedQueue<String> deleted;
    private WsQueueLifecycleManager mgr;

    @BeforeEach
    void setUp() {
        scheduler = Executors.newSingleThreadScheduledExecutor();
        deleted = new ConcurrentLinkedQueue<>();
        mgr = new WsQueueLifecycleManager(scheduler, deleted::add);
    }

    @AfterEach
    void tearDown() {
        scheduler.shutdownNow();
    }

    // ---- Auto-delete on last unsubscribe ----

    @Test
    void autoDelete_lastUnsubscribe_deletesQueue() {
        mgr.registerQueue("q1", /* autoDelete */ true, /* exclusive */ false, /* ownerConn */ null);
        mgr.onSubscribe("q1");
        mgr.onUnsubscribe("q1");
        assertTrue(deleted.contains("q1"),
            "auto-delete queue should be deleted when last subscriber leaves");
    }

    @Test
    void autoDelete_notLastSubscriber_noDelete() {
        mgr.registerQueue("q1", true, false, null);
        mgr.onSubscribe("q1");
        mgr.onSubscribe("q1");
        mgr.onUnsubscribe("q1");
        assertFalse(deleted.contains("q1"),
            "auto-delete must not fire while other subscribers remain");
        assertEquals(1, mgr.subscriberCount("q1"));
    }

    @Test
    void autoDelete_disabled_noDelete() {
        mgr.registerQueue("q1", /* autoDelete */ false, false, null);
        mgr.onSubscribe("q1");
        mgr.onUnsubscribe("q1");
        assertFalse(deleted.contains("q1"),
            "auto-delete must not fire when the queue has autoDelete=false");
    }

    @Test
    void autoDelete_zeroSubscribersNeverSubscribed_noDelete() {
        // Unsubscribing without a prior subscribe must not trigger delete — count stays at 0
        // but never transitioned from 1 → 0.
        mgr.registerQueue("q1", true, false, null);
        mgr.onUnsubscribe("q1");
        assertFalse(deleted.contains("q1"));
    }

    @Test
    @Timeout(5)
    void autoDelete_concurrentUnsubscribes_deletesExactlyOnce() throws Exception {
        mgr.registerQueue("q1", true, false, null);
        int n = 64;
        for (int i = 0; i < n; i++) {
            mgr.onSubscribe("q1");
        }
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(16);
        AtomicInteger completed = new AtomicInteger();
        try {
            for (int i = 0; i < n; i++) {
                pool.submit(() -> {
                    try {
                        start.await();
                        mgr.onUnsubscribe("q1");
                        completed.incrementAndGet();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
            }
            start.countDown();
            pool.shutdown();
            assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));
        } finally {
            pool.shutdownNow();
        }
        assertEquals(n, completed.get());
        long fires = deleted.stream().filter("q1"::equals).count();
        assertEquals(1, fires,
            "auto-delete hook must fire exactly once across concurrent unsubscribes");
    }

    // ---- x-expires idle timeout ----

    @Test
    @Timeout(5)
    void xExpires_idleTimeout_deletesQueue() throws Exception {
        mgr.registerQueue("q1", false, false, null);
        mgr.startExpiryTimer("q1", 50);
        // Wait enough for the scheduler to fire.
        Thread.sleep(250);
        assertTrue(deleted.contains("q1"),
            "queue with x-expires should be deleted after idle timeout");
    }

    @Test
    @Timeout(5)
    void xExpires_cancelled_onNewSubscriber() throws Exception {
        mgr.registerQueue("q1", false, false, null);
        mgr.startExpiryTimer("q1", 500);
        // Consumer attaches before the expiry fires.
        mgr.onSubscribe("q1");
        Thread.sleep(700);
        assertFalse(deleted.contains("q1"),
            "expiry timer must be cancelled when a consumer subscribes before it fires");
    }

    @Test
    @Timeout(5)
    void xExpires_cancelled_explicitly() throws Exception {
        mgr.registerQueue("q1", false, false, null);
        mgr.startExpiryTimer("q1", 200);
        mgr.cancelExpiryTimer("q1");
        Thread.sleep(400);
        assertFalse(deleted.contains("q1"));
    }

    @Test
    @Timeout(5)
    void xExpires_restarted_overwritesOldTimer() throws Exception {
        mgr.registerQueue("q1", false, false, null);
        mgr.startExpiryTimer("q1", 100);
        // Restart with a longer timer — old one should be cancelled, only new one fires.
        mgr.startExpiryTimer("q1", 500);
        Thread.sleep(250);
        assertFalse(deleted.contains("q1"),
            "restarted timer must cancel the previous one");
        Thread.sleep(400);
        assertTrue(deleted.contains("q1"),
            "restarted timer must fire after its own delay");
        assertEquals(1, deleted.stream().filter("q1"::equals).count(),
            "exactly one delete hook should fire");
    }

    // ---- Server-generated queue names ----

    @Test
    void serverGeneratedName_format() {
        String name = WsQueueLifecycleManager.generateQueueName();
        assertNotNull(name);
        assertTrue(name.startsWith("q.gen-"),
            "server-generated name must start with 'q.gen-': " + name);
        String suffix = name.substring("q.gen-".length());
        Pattern uuidPattern = Pattern.compile(
            "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$");
        assertTrue(uuidPattern.matcher(suffix).matches(),
            "suffix must be a UUID: " + suffix);
    }

    @Test
    void serverGeneratedName_unique() {
        Set<String> names = new HashSet<>();
        for (int i = 0; i < 1000; i++) {
            names.add(WsQueueLifecycleManager.generateQueueName());
        }
        assertEquals(1000, names.size(), "generated names must be unique across invocations");
    }

    @Test
    void serverGeneratedName_twoDifferent() {
        String a = WsQueueLifecycleManager.generateQueueName();
        String b = WsQueueLifecycleManager.generateQueueName();
        assertNotEquals(a, b);
    }

    // ---- Redeclare validation ----

    @Test
    void redeclare_sameFlags_succeeds() {
        QueueMetadata existing = qm("q", true, false, true, Collections.emptyMap());
        // No exception — idempotent.
        mgr.validateRedeclare(existing, true, false, true);
    }

    @Test
    void redeclare_differentDurable_preconditionFailed() {
        QueueMetadata existing = qm("q", true, false, true, Collections.emptyMap());
        assertThrows(WsQueueLifecycleManager.PreconditionFailedException.class,
            () -> mgr.validateRedeclare(existing, false, false, true));
    }

    @Test
    void redeclare_differentExclusive_preconditionFailed() {
        QueueMetadata existing = qm("q", true, false, true, Collections.emptyMap());
        assertThrows(WsQueueLifecycleManager.PreconditionFailedException.class,
            () -> mgr.validateRedeclare(existing, true, true, true));
    }

    @Test
    void redeclare_differentAutoDelete_preconditionFailed() {
        QueueMetadata existing = qm("q", true, false, true, Collections.emptyMap());
        assertThrows(WsQueueLifecycleManager.PreconditionFailedException.class,
            () -> mgr.validateRedeclare(existing, true, false, false));
    }

    @Test
    void redeclare_argumentsIgnored() {
        Map<String, String> args1 = new HashMap<>();
        args1.put("x-message-ttl", "60000");
        QueueMetadata existing = qm("q", true, false, true, args1);

        // Even if future call has different arguments in the metadata, redeclare validates
        // only the three flags. This test documents the contract: args are not checked.
        mgr.validateRedeclare(existing, true, false, true);
    }

    // ---- Connection close → exclusive queue cleanup ----

    @Test
    void connectionClose_deletesExclusiveQueues() {
        mgr.registerQueue("qx", /* autoDelete */ false, /* exclusive */ true, "conn-A");
        mgr.registerQueue("qy", false, true, "conn-A");
        mgr.registerQueue("qShared", false, false, null);

        List<String> deletedNames = mgr.onConnectionClose("conn-A");
        Set<String> deletedSet = new HashSet<>(deletedNames);
        assertTrue(deletedSet.contains("qx"));
        assertTrue(deletedSet.contains("qy"));
        assertFalse(deletedSet.contains("qShared"));
        // Delete hook is invoked for each exclusive queue.
        assertTrue(deleted.contains("qx"));
        assertTrue(deleted.contains("qy"));
    }

    @Test
    void connectionClose_noExclusiveQueues_noDeletes() {
        mgr.registerQueue("q1", false, false, null);
        List<String> deletedNames = mgr.onConnectionClose("conn-X");
        assertTrue(deletedNames.isEmpty());
        assertFalse(deleted.contains("q1"));
    }

    @Test
    void connectionClose_onlyDeletesQueuesOwnedByClosingConnection() {
        mgr.registerQueue("qA", false, true, "conn-A");
        mgr.registerQueue("qB", false, true, "conn-B");

        List<String> deletedA = mgr.onConnectionClose("conn-A");
        assertEquals(List.of("qA"), deletedA);
        assertTrue(deleted.contains("qA"));
        assertFalse(deleted.contains("qB"));
    }

    // ---- Null / defensive ----

    @Test
    void onSubscribe_null_throwsNpe() {
        assertThrows(NullPointerException.class, () -> mgr.onSubscribe(null));
    }

    @Test
    void onUnsubscribe_null_throwsNpe() {
        assertThrows(NullPointerException.class, () -> mgr.onUnsubscribe(null));
    }

    @Test
    void startExpiryTimer_negative_throws() {
        assertThrows(IllegalArgumentException.class, () -> mgr.startExpiryTimer("q", -1));
    }

    // ---- helpers ----

    private static QueueMetadata qm(String name, boolean durable, boolean exclusive,
                                    boolean autoDelete, Map<String, String> args) {
        return new QueueMetadata(name, "/", durable, exclusive, autoDelete, args);
    }
}
