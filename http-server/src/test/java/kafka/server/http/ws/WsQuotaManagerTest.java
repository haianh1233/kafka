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

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link WsQuotaManager}.
 *
 * // Time: Created - TASK-WS3.06
 */
class WsQuotaManagerTest {

    @Test
    void constructor_rejectsNonPositiveControlRate() {
        assertThrows(IllegalArgumentException.class,
            () -> new WsQuotaManager(null, null, 0));
        assertThrows(IllegalArgumentException.class,
            () -> new WsQuotaManager(null, null, -5));
    }

    // ------------------------------------------------------------------
    //  Produce byte-rate
    // ------------------------------------------------------------------

    @Test
    void checkProduceQuota_delegatesToShim() {
        AtomicLong captured = new AtomicLong();
        WsQuotaManager.ByteRateQuotaChecker checker = (id, bytes) -> {
            captured.set(bytes);
            return 250L;
        };
        WsQuotaManager q = new WsQuotaManager(checker, null, 50);
        assertEquals(250L, q.checkProduceQuota("alice", 1024));
        assertEquals(1024L, captured.get(), "shim should see the byte count");
    }

    @Test
    void checkProduceQuota_underLimit_returnsZero() {
        WsQuotaManager q = new WsQuotaManager(
            (id, bytes) -> 0L, null, 50);
        assertEquals(0L, q.checkProduceQuota("alice", 1024));
    }

    @Test
    void checkProduceQuota_nullClientId_noThrottle() {
        WsQuotaManager q = new WsQuotaManager(
            (id, bytes) -> 9999L, null, 50);
        assertEquals(0L, q.checkProduceQuota(null, 1024),
            "null clientId bypasses quota check");
    }

    @Test
    void checkProduceQuota_zeroOrNegativeBytes_noThrottle() {
        WsQuotaManager q = new WsQuotaManager(
            (id, bytes) -> 9999L, null, 50);
        assertEquals(0L, q.checkProduceQuota("alice", 0));
        assertEquals(0L, q.checkProduceQuota("alice", -5));
    }

    @Test
    void checkProduceQuota_defaultShim_noThrottle() {
        WsQuotaManager q = new WsQuotaManager(null, null, 50);
        assertEquals(0L, q.checkProduceQuota("alice", 1024),
            "no shim → permissive default");
    }

    // ------------------------------------------------------------------
    //  Fetch byte-rate
    // ------------------------------------------------------------------

    @Test
    void checkFetchQuota_delegatesToShim() {
        AtomicLong captured = new AtomicLong();
        WsQuotaManager.ByteRateQuotaChecker checker = (id, bytes) -> {
            captured.set(bytes);
            return 750L;
        };
        WsQuotaManager q = new WsQuotaManager(null, checker, 50);
        assertEquals(750L, q.checkFetchQuota("alice", 4096));
        assertEquals(4096L, captured.get());
    }

    @Test
    void checkFetchQuota_defaultShim_noThrottle() {
        WsQuotaManager q = new WsQuotaManager(null, null, 50);
        assertEquals(0L, q.checkFetchQuota("alice", 4096));
    }

    // ------------------------------------------------------------------
    //  Control-message rate
    // ------------------------------------------------------------------

    @Test
    void checkControlMessageRate_underLimit_allows() {
        WsQuotaManager q = new WsQuotaManager(null, null, 3);
        long t0 = 1_700_000_000_000L;
        assertTrue(q.checkControlMessageRate("c1", t0));
        assertTrue(q.checkControlMessageRate("c1", t0));
        assertTrue(q.checkControlMessageRate("c1", t0));
    }

    @Test
    void checkControlMessageRate_exceedsLimit_rejects() {
        WsQuotaManager q = new WsQuotaManager(null, null, 2);
        long t0 = 1_700_000_000_000L;
        assertTrue(q.checkControlMessageRate("c1", t0));
        assertTrue(q.checkControlMessageRate("c1", t0));
        assertFalse(q.checkControlMessageRate("c1", t0),
            "third message in the same second is rejected");
    }

    @Test
    void checkControlMessageRate_windowRolls_afterOneSecond() {
        WsQuotaManager q = new WsQuotaManager(null, null, 1);
        long t0 = 1_700_000_000_000L;
        assertTrue(q.checkControlMessageRate("c1", t0));
        assertFalse(q.checkControlMessageRate("c1", t0),
            "second message in the same second is over limit");
        // Advance past the second boundary — window resets.
        long t1 = t0 + 1_000L;
        assertTrue(q.checkControlMessageRate("c1", t1),
            "new second gets a fresh budget");
    }

    @Test
    void checkControlMessageRate_perConnection() {
        WsQuotaManager q = new WsQuotaManager(null, null, 1);
        long t0 = 1_700_000_000_000L;
        assertTrue(q.checkControlMessageRate("c1", t0));
        // A different connection has its own window.
        assertTrue(q.checkControlMessageRate("c2", t0));
        assertFalse(q.checkControlMessageRate("c1", t0),
            "c1 hits its own limit independently of c2");
    }

    @Test
    void releaseConnection_clearsWindow() {
        WsQuotaManager q = new WsQuotaManager(null, null, 1);
        long t0 = 1_700_000_000_000L;
        assertTrue(q.checkControlMessageRate("c1", t0));
        assertFalse(q.checkControlMessageRate("c1", t0));
        q.releaseConnection("c1");
        // A fresh window for the same id is now available even before the
        // second rolls — because state was released.
        assertTrue(q.checkControlMessageRate("c1", t0));
    }

    @Test
    void fromConfigs_usesConfiguredControlRate() {
        WsQuotaManager q = WsQuotaManager.fromConfigs(WsConfigs.withDefaults());
        assertEquals(WsConfigs.withDefaults().maxControlMessagesPerSecond(),
            q.maxControlMessagesPerSecond());
    }
}
