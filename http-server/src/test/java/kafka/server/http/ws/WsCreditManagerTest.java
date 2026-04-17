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
package kafka.server.http.ws;

import io.netty.channel.Channel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link WsCreditManager} — per-subscription credit-based flow control.
 *
 * // Time: Created - TASK-WS1.14
 */
class WsCreditManagerTest {

    private Channel channel;
    private WsCreditManager manager;

    @BeforeEach
    void setUp() {
        channel = mock(Channel.class);
        when(channel.isWritable()).thenReturn(true);
        manager = new WsCreditManager(10, channel);
    }

    // ---- Basic state ----

    @Test
    void initialCredits_availableImmediately() {
        assertEquals(10, manager.available());
    }

    @Test
    void initialCredits_zero_allowed() {
        WsCreditManager m = new WsCreditManager(0, channel);
        assertEquals(0, m.available());
    }

    // ---- consume() ----

    @Test
    void consume_decrementsCredits() {
        assertTrue(manager.consume());
        assertEquals(9, manager.available());
    }

    @Test
    void consume_atZero_returnsFalse() {
        WsCreditManager empty = new WsCreditManager(0, channel);
        assertFalse(empty.consume());
        assertEquals(0, empty.available());
    }

    @Test
    void consume_neverGoesBelowZero() {
        WsCreditManager one = new WsCreditManager(1, channel);
        assertTrue(one.consume());
        assertFalse(one.consume());
        assertFalse(one.consume());
        assertEquals(0, one.available());
    }

    @Test
    void consume_drainsAllCreditsAccurately() {
        for (int i = 0; i < 10; i++) {
            assertTrue(manager.consume(), "consume " + i + " should succeed");
        }
        assertFalse(manager.consume());
        assertEquals(0, manager.available());
    }

    // ---- grant() ----

    @Test
    void grant_addsCredits() {
        manager.grant(5);
        assertEquals(15, manager.available());
    }

    @Test
    void grant_multipleTimes_accumulates() {
        manager.grant(3);
        manager.grant(7);
        assertEquals(20, manager.available());
    }

    @Test
    void grant_zeroOrNegative_throwsIAE() {
        assertThrows(IllegalArgumentException.class, () -> manager.grant(0));
        assertThrows(IllegalArgumentException.class, () -> manager.grant(-1));
        assertThrows(IllegalArgumentException.class, () -> manager.grant(-100));
    }

    // ---- awaitCredits() ----

    @Test
    void awaitCredits_returnsImmediately_whenAvailable() {
        int credits = manager.awaitCredits(1000);
        assertEquals(10, credits);
    }

    @Test
    void awaitCredits_zeroTimeout_whenCreditsAvailable_returnsImmediately() {
        int credits = manager.awaitCredits(0);
        assertEquals(10, credits);
    }

    @Test
    void awaitCredits_zeroTimeout_whenNoCredits_returnsZero() {
        WsCreditManager empty = new WsCreditManager(0, channel);
        long start = System.nanoTime();
        int credits = empty.awaitCredits(0);
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;
        assertEquals(0, credits);
        assertTrue(elapsedMs < 50, "zero-timeout should return nearly instantly, got " + elapsedMs + "ms");
    }

    @Test
    void awaitCredits_returnsZero_onTimeout() {
        WsCreditManager empty = new WsCreditManager(0, channel);
        long start = System.nanoTime();
        int credits = empty.awaitCredits(50);
        long elapsed = (System.nanoTime() - start) / 1_000_000;
        assertEquals(0, credits);
        assertTrue(elapsed >= 40, "Should have waited ~50ms, got " + elapsed);
    }

    @Test
    void awaitCredits_returnsZero_whenChannelNotWritable() {
        when(channel.isWritable()).thenReturn(false);
        long start = System.nanoTime();
        int credits = manager.awaitCredits(50);
        long elapsed = (System.nanoTime() - start) / 1_000_000;
        assertEquals(0, credits);
        assertTrue(elapsed >= 40, "should have waited full timeout when not writable");
    }

    @Test
    void awaitCredits_unblocksOnGrant() throws InterruptedException {
        WsCreditManager empty = new WsCreditManager(0, channel);
        AtomicInteger result = new AtomicInteger(-1);
        CountDownLatch latch = new CountDownLatch(1);

        Thread waiter = new Thread(() -> {
            result.set(empty.awaitCredits(5000));
            latch.countDown();
        });
        waiter.start();

        // Let the waiter start polling
        Thread.sleep(50);
        empty.grant(5);

        assertTrue(latch.await(2, TimeUnit.SECONDS),
            "Waiter should have been unblocked by grant within 2s");
        assertTrue(result.get() > 0, "expected > 0 credits after grant, got " + result.get());
        assertEquals(5, result.get());
    }

    @Test
    void awaitCredits_unblocksWhenChannelBecomesWritable() throws InterruptedException {
        when(channel.isWritable()).thenReturn(false);
        AtomicInteger result = new AtomicInteger(-1);
        CountDownLatch latch = new CountDownLatch(1);

        Thread waiter = new Thread(() -> {
            result.set(manager.awaitCredits(5000));
            latch.countDown();
        });
        waiter.start();

        Thread.sleep(50);
        when(channel.isWritable()).thenReturn(true);

        assertTrue(latch.await(2, TimeUnit.SECONDS),
            "Waiter should have been unblocked within 2s once writable returns true");
        assertEquals(10, result.get());
    }

    // ---- reset() ----

    @Test
    void reset_setsCreditsToZero() {
        manager.reset();
        assertEquals(0, manager.available());
    }

    @Test
    void reset_afterGrants_stillZero() {
        manager.grant(100);
        manager.reset();
        assertEquals(0, manager.available());
    }

    @Test
    void consume_afterReset_returnsFalse() {
        manager.reset();
        assertFalse(manager.consume());
    }

    // ---- Constructor validation ----

    @Test
    void constructor_nullChannel_throwsNPE() {
        assertThrows(NullPointerException.class, () -> new WsCreditManager(10, null));
    }

    @Test
    void constructor_negativeCredits_throwsIAE() {
        assertThrows(IllegalArgumentException.class, () -> new WsCreditManager(-1, channel));
    }

    // ---- Concurrency ----

    @Test
    void concurrentGrantAndConsume_preservesTotalInvariant() throws InterruptedException {
        // Start with 0 credits. Grants add up to a known total. Consumers drain.
        // After all work completes: granted == consumed + remaining.
        WsCreditManager m = new WsCreditManager(0, channel);

        final int numGrantThreads = 4;
        final int numConsumeThreads = 4;
        final int grantsPerThread = 1_000;
        final int consumesPerThread = 1_000;
        final int grantAmount = 1;

        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(numGrantThreads + numConsumeThreads);

        AtomicInteger totalGranted = new AtomicInteger(0);
        AtomicInteger totalConsumed = new AtomicInteger(0);

        // Grant threads
        for (int t = 0; t < numGrantThreads; t++) {
            new Thread(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < grantsPerThread; i++) {
                        m.grant(grantAmount);
                        totalGranted.addAndGet(grantAmount);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            }, "grant-" + t).start();
        }

        // Consume threads
        for (int t = 0; t < numConsumeThreads; t++) {
            new Thread(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < consumesPerThread; i++) {
                        if (m.consume()) {
                            totalConsumed.incrementAndGet();
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            }, "consume-" + t).start();
        }

        startLatch.countDown();
        assertTrue(doneLatch.await(10, TimeUnit.SECONDS), "threads did not finish in time");

        int granted = totalGranted.get();
        int consumed = totalConsumed.get();
        int remaining = m.available();

        // Invariant: everything granted must be accounted for by what was consumed + what's still available
        assertEquals(granted, consumed + remaining,
            "granted=" + granted + " consumed=" + consumed + " remaining=" + remaining);
        // Floor at 0 — available cannot be negative
        assertTrue(remaining >= 0, "remaining credits must not be negative: " + remaining);
        // Consumed cannot exceed granted
        assertTrue(consumed <= granted, "consumed (" + consumed + ") cannot exceed granted (" + granted + ")");
    }
}
