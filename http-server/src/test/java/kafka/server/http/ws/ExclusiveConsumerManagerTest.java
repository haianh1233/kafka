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

// Time: Created - TASK-WS3.04

package kafka.server.http.ws;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link ExclusiveConsumerManager} — exclusive consumer lock enforcement.
 */
class ExclusiveConsumerManagerTest {

    private ExclusiveConsumerManager mgr;

    @BeforeEach
    void setUp() {
        mgr = new ExclusiveConsumerManager();
    }

    // ---- Acquire ----

    @Test
    void acquire_succeeds_whenNoLock() {
        assertTrue(mgr.tryAcquireExclusive("orders", "conn-A"));
        assertTrue(mgr.isExclusivelyLocked("orders"));
        assertTrue(mgr.isLockedBy("orders", "conn-A"));
    }

    @Test
    void acquire_fails_whenLockedByOther() {
        assertTrue(mgr.tryAcquireExclusive("orders", "conn-A"));
        assertFalse(mgr.tryAcquireExclusive("orders", "conn-B"));
        assertTrue(mgr.isLockedBy("orders", "conn-A"));
        assertFalse(mgr.isLockedBy("orders", "conn-B"));
    }

    @Test
    void acquire_succeeds_whenLockedBySameConnection() {
        assertTrue(mgr.tryAcquireExclusive("orders", "conn-A"));
        assertTrue(mgr.tryAcquireExclusive("orders", "conn-A"), "same-connection re-acquire should be idempotent");
        assertTrue(mgr.isLockedBy("orders", "conn-A"));
    }

    @Test
    void acquire_nullArgs_throwsNPE() {
        assertThrows(NullPointerException.class, () -> mgr.tryAcquireExclusive(null, "conn-A"));
        assertThrows(NullPointerException.class, () -> mgr.tryAcquireExclusive("orders", null));
    }

    // ---- Release ----

    @Test
    void release_removesLock() {
        assertTrue(mgr.tryAcquireExclusive("orders", "conn-A"));
        mgr.releaseExclusive("orders", "conn-A");
        assertFalse(mgr.isExclusivelyLocked("orders"));
        // Different connection can now acquire.
        assertTrue(mgr.tryAcquireExclusive("orders", "conn-B"));
    }

    @Test
    void release_byNonOwner_isNoOp() {
        assertTrue(mgr.tryAcquireExclusive("orders", "conn-A"));
        // conn-B attempts to release — must not affect conn-A's lock.
        mgr.releaseExclusive("orders", "conn-B");
        assertTrue(mgr.isExclusivelyLocked("orders"));
        assertTrue(mgr.isLockedBy("orders", "conn-A"));
    }

    @Test
    void release_unknownQueue_isNoOp() {
        mgr.releaseExclusive("never-locked", "conn-A"); // no throw
        assertFalse(mgr.isExclusivelyLocked("never-locked"));
    }

    @Test
    void release_nullArgs_throwsNPE() {
        assertThrows(NullPointerException.class, () -> mgr.releaseExclusive(null, "conn-A"));
        assertThrows(NullPointerException.class, () -> mgr.releaseExclusive("orders", null));
    }

    // ---- State inspection ----

    @Test
    void isExclusivelyLocked_correctState() {
        assertFalse(mgr.isExclusivelyLocked("orders"));
        mgr.tryAcquireExclusive("orders", "conn-A");
        assertTrue(mgr.isExclusivelyLocked("orders"));
        mgr.releaseExclusive("orders", "conn-A");
        assertFalse(mgr.isExclusivelyLocked("orders"));
    }

    @Test
    void isLockedBy_distinguishesConnections() {
        mgr.tryAcquireExclusive("orders", "conn-A");
        assertTrue(mgr.isLockedBy("orders", "conn-A"));
        assertFalse(mgr.isLockedBy("orders", "conn-B"));
        assertFalse(mgr.isLockedBy("events", "conn-A"));
    }

    // ---- Connection close ----

    @Test
    void onConnectionClose_releasesAllLocks() {
        mgr.tryAcquireExclusive("orders", "conn-A");
        mgr.tryAcquireExclusive("events", "conn-A");
        mgr.tryAcquireExclusive("logs", "conn-B");

        List<String> released = mgr.onConnectionClose("conn-A");
        assertEquals(Set.of("orders", "events"), Set.copyOf(released));

        // conn-A's queues are now free
        assertFalse(mgr.isExclusivelyLocked("orders"));
        assertFalse(mgr.isExclusivelyLocked("events"));
        // conn-B's queue still locked
        assertTrue(mgr.isLockedBy("logs", "conn-B"));
    }

    @Test
    void onConnectionClose_returnsExclusiveQueues() {
        mgr.tryAcquireExclusive("q1", "conn-A");
        mgr.tryAcquireExclusive("q2", "conn-A");

        List<String> released = mgr.onConnectionClose("conn-A");
        assertEquals(2, released.size());
        assertTrue(released.contains("q1"));
        assertTrue(released.contains("q2"));
    }

    @Test
    void onConnectionClose_noLocks_returnsEmptyList() {
        List<String> released = mgr.onConnectionClose("conn-unknown");
        assertNotNull(released);
        assertTrue(released.isEmpty());
    }

    @Test
    void onConnectionClose_nullConnection_throwsNPE() {
        assertThrows(NullPointerException.class, () -> mgr.onConnectionClose(null));
    }

    @Test
    void onConnectionClose_afterRelease_doesNotReReport() {
        mgr.tryAcquireExclusive("q1", "conn-A");
        mgr.releaseExclusive("q1", "conn-A");
        List<String> released = mgr.onConnectionClose("conn-A");
        assertTrue(released.isEmpty(), "explicit release already dropped q1");
    }

    @Test
    void onConnectionClose_twice_isIdempotent() {
        mgr.tryAcquireExclusive("q1", "conn-A");
        mgr.onConnectionClose("conn-A");
        List<String> second = mgr.onConnectionClose("conn-A");
        assertTrue(second.isEmpty());
    }

    // ---- Concurrency ----

    @Test
    @Timeout(10)
    void concurrentAcquire_onlyOneSucceeds() throws InterruptedException {
        int threads = 16;
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        AtomicInteger wins = new AtomicInteger();
        ConcurrentHashMap<String, Boolean> winners = new ConcurrentHashMap<>();
        ExecutorService pool = Executors.newFixedThreadPool(threads);

        try {
            for (int i = 0; i < threads; i++) {
                final String connId = "conn-" + i;
                pool.submit(() -> {
                    try {
                        start.await();
                        if (mgr.tryAcquireExclusive("race-queue", connId)) {
                            wins.incrementAndGet();
                            winners.put(connId, Boolean.TRUE);
                        }
                    } catch (InterruptedException ignored) {
                        Thread.currentThread().interrupt();
                    } finally {
                        done.countDown();
                    }
                });
            }
            start.countDown();
            assertTrue(done.await(5, TimeUnit.SECONDS));

            assertEquals(1, wins.get(), "only one thread must win the exclusive lock");
            String winningConn = winners.keySet().iterator().next();
            assertTrue(mgr.isLockedBy("race-queue", winningConn));
        } finally {
            pool.shutdownNow();
            pool.awaitTermination(2, TimeUnit.SECONDS);
        }
    }

    @Test
    @Timeout(10)
    void concurrentAcquireRelease_maintainsConsistency() throws InterruptedException {
        // Independent queues per thread — there should be no blocking between different queues.
        int threads = 8;
        int perThread = 200;
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        AtomicInteger failures = new AtomicInteger();
        ExecutorService pool = Executors.newFixedThreadPool(threads);

        try {
            for (int t = 0; t < threads; t++) {
                final String conn = "conn-" + t;
                pool.submit(() -> {
                    try {
                        start.await();
                        for (int i = 0; i < perThread; i++) {
                            String q = conn + "-q-" + i;
                            if (!mgr.tryAcquireExclusive(q, conn)) failures.incrementAndGet();
                            mgr.releaseExclusive(q, conn);
                        }
                    } catch (InterruptedException ignored) {
                        Thread.currentThread().interrupt();
                    } finally {
                        done.countDown();
                    }
                });
            }
            start.countDown();
            assertTrue(done.await(5, TimeUnit.SECONDS));
            assertEquals(0, failures.get());
        } finally {
            pool.shutdownNow();
            pool.awaitTermination(2, TimeUnit.SECONDS);
        }
    }

    @Test
    void sameConnection_multipleQueues_trackedIndependently() {
        assertTrue(mgr.tryAcquireExclusive("q1", "conn-A"));
        assertTrue(mgr.tryAcquireExclusive("q2", "conn-A"));
        assertTrue(mgr.isLockedBy("q1", "conn-A"));
        assertTrue(mgr.isLockedBy("q2", "conn-A"));

        mgr.releaseExclusive("q1", "conn-A");
        assertFalse(mgr.isExclusivelyLocked("q1"));
        assertTrue(mgr.isLockedBy("q2", "conn-A"));
    }
}
