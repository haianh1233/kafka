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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link WsResourceLimitManager}.
 *
 * // Time: Created - TASK-WS3.06
 */
class WsResourceLimitManagerTest {

    @Test
    void constructor_rejectsNonPositiveLimits() {
        assertThrows(IllegalArgumentException.class,
            () -> new WsResourceLimitManager(0, 10, 10, 10));
        assertThrows(IllegalArgumentException.class,
            () -> new WsResourceLimitManager(10, 0, 10, 10));
        assertThrows(IllegalArgumentException.class,
            () -> new WsResourceLimitManager(10, 10, 0, 10));
        assertThrows(IllegalArgumentException.class,
            () -> new WsResourceLimitManager(10, 10, 10, 0));
    }

    // ------------------------------------------------------------------
    //  Connection limit
    // ------------------------------------------------------------------

    @Test
    void connectionLimit_rejectsExcess() {
        WsResourceLimitManager m = new WsResourceLimitManager(10, 10, 10, 2);
        assertTrue(m.canAcceptConnection());
        m.recordConnectionOpen();
        assertTrue(m.canAcceptConnection());
        m.recordConnectionOpen();
        assertFalse(m.canAcceptConnection(), "third connection should be rejected");
    }

    @Test
    void tryReserveConnection_atomicallyIncrementsUnderLimit() {
        WsResourceLimitManager m = new WsResourceLimitManager(10, 10, 10, 3);
        assertTrue(m.tryReserveConnection());
        assertTrue(m.tryReserveConnection());
        assertTrue(m.tryReserveConnection());
        assertFalse(m.tryReserveConnection(), "4th reservation past the cap should fail");
        assertEquals(3, m.connectionCount());
    }

    @Test
    void recordConnectionClose_decrementsCount() {
        WsResourceLimitManager m = new WsResourceLimitManager(10, 10, 10, 5);
        m.recordConnectionOpen();
        m.recordConnectionOpen();
        assertEquals(2, m.connectionCount());
        m.recordConnectionClose();
        assertEquals(1, m.connectionCount());
    }

    @Test
    void recordConnectionClose_neverGoesNegative() {
        WsResourceLimitManager m = new WsResourceLimitManager(10, 10, 10, 5);
        m.recordConnectionClose();
        m.recordConnectionClose();
        assertEquals(0, m.connectionCount(),
            "double-close must not push count negative");
    }

    // ------------------------------------------------------------------
    //  Exchange limit (per-vhost)
    // ------------------------------------------------------------------

    @Test
    void exchangeLimit_perVhost() {
        WsResourceLimitManager m = new WsResourceLimitManager(2, 10, 10, 10);
        assertTrue(m.canCreateExchange("/"));
        m.recordExchangeCreated("/");
        assertTrue(m.canCreateExchange("/"));
        m.recordExchangeCreated("/");
        assertFalse(m.canCreateExchange("/"), "at cap, vhost / should reject");
        // Different vhost has its own count — still under cap.
        assertTrue(m.canCreateExchange("/other"));
    }

    @Test
    void exchangeLimit_countersDecrement_onDelete() {
        WsResourceLimitManager m = new WsResourceLimitManager(1, 10, 10, 10);
        m.recordExchangeCreated("/");
        assertFalse(m.canCreateExchange("/"));
        m.recordExchangeDeleted("/");
        assertTrue(m.canCreateExchange("/"), "after delete, capacity restored");
    }

    // ------------------------------------------------------------------
    //  Queue limit (per-vhost)
    // ------------------------------------------------------------------

    @Test
    void queueLimit_perVhost() {
        WsResourceLimitManager m = new WsResourceLimitManager(10, 2, 10, 10);
        assertTrue(m.canCreateQueue("/"));
        m.recordQueueCreated("/");
        m.recordQueueCreated("/");
        assertFalse(m.canCreateQueue("/"));
        assertTrue(m.canCreateQueue("/prod"), "other vhost should be unaffected");
    }

    @Test
    void queueLimit_countersDecrement_onDelete() {
        WsResourceLimitManager m = new WsResourceLimitManager(10, 1, 10, 10);
        m.recordQueueCreated("/");
        assertFalse(m.canCreateQueue("/"));
        m.recordQueueDeleted("/");
        assertTrue(m.canCreateQueue("/"));
    }

    // ------------------------------------------------------------------
    //  Binding limit (per-exchange)
    // ------------------------------------------------------------------

    @Test
    void bindingLimit_perExchange() {
        WsResourceLimitManager m = new WsResourceLimitManager(10, 10, 2, 10);
        assertTrue(m.canCreateBinding("orders"));
        m.recordBindingCreated("orders");
        m.recordBindingCreated("orders");
        assertFalse(m.canCreateBinding("orders"));
        assertTrue(m.canCreateBinding("events"), "other exchange should be unaffected");
    }

    @Test
    void bindingLimit_countersDecrement_onDelete() {
        WsResourceLimitManager m = new WsResourceLimitManager(10, 10, 1, 10);
        m.recordBindingCreated("orders");
        assertFalse(m.canCreateBinding("orders"));
        m.recordBindingDeleted("orders");
        assertTrue(m.canCreateBinding("orders"));
    }

    // ------------------------------------------------------------------
    //  Factory
    // ------------------------------------------------------------------

    @Test
    void fromConfigs_wiresAllLimits() {
        WsResourceLimitManager m = WsResourceLimitManager.fromConfigs(WsConfigs.withDefaults());
        WsConfigs cfg = WsConfigs.withDefaults();
        assertEquals(cfg.maxExchangesPerVhost(), m.maxExchangesPerVhost());
        assertEquals(cfg.maxQueuesPerVhost(), m.maxQueuesPerVhost());
        assertEquals(cfg.maxBindingsPerExchange(), m.maxBindingsPerExchange());
        assertEquals(cfg.maxConnectionsPerBroker(), m.maxConnectionsPerBroker());
    }

    @Test
    void withinLimits_allowed() {
        WsResourceLimitManager m = new WsResourceLimitManager(5, 5, 5, 5);
        for (int i = 0; i < 4; i++) {
            assertTrue(m.canAcceptConnection());
            assertTrue(m.canCreateExchange("/"));
            assertTrue(m.canCreateQueue("/"));
            assertTrue(m.canCreateBinding("x"));
            m.recordConnectionOpen();
            m.recordExchangeCreated("/");
            m.recordQueueCreated("/");
            m.recordBindingCreated("x");
        }
        // All now at 4, cap 5 — still one slot each.
        assertTrue(m.canAcceptConnection());
        assertTrue(m.canCreateExchange("/"));
        assertTrue(m.canCreateQueue("/"));
        assertTrue(m.canCreateBinding("x"));
    }
}
