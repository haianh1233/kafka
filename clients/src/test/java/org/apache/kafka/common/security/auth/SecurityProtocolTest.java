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
// Time: Created - TASK-A.01
package org.apache.kafka.common.security.auth;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SecurityProtocolTest {

    @Test
    void testHttpEnumValues() {
        assertEquals(4, SecurityProtocol.HTTP.id);
        assertEquals("HTTP", SecurityProtocol.HTTP.name);
        assertEquals(5, SecurityProtocol.HTTPS.id);
        assertEquals("HTTPS", SecurityProtocol.HTTPS.name);
    }

    @Test
    void testForId() {
        assertEquals(SecurityProtocol.HTTP, SecurityProtocol.forId((short) 4));
        assertEquals(SecurityProtocol.HTTPS, SecurityProtocol.forId((short) 5));
        // Existing values still work
        assertEquals(SecurityProtocol.PLAINTEXT, SecurityProtocol.forId((short) 0));
        assertEquals(SecurityProtocol.SSL, SecurityProtocol.forId((short) 1));
        assertEquals(SecurityProtocol.SASL_PLAINTEXT, SecurityProtocol.forId((short) 2));
        assertEquals(SecurityProtocol.SASL_SSL, SecurityProtocol.forId((short) 3));
    }

    @Test
    void testForName() {
        assertEquals(SecurityProtocol.HTTP, SecurityProtocol.forName("HTTP"));
        assertEquals(SecurityProtocol.HTTP, SecurityProtocol.forName("http"));
        assertEquals(SecurityProtocol.HTTPS, SecurityProtocol.forName("HTTPS"));
        assertEquals(SecurityProtocol.HTTPS, SecurityProtocol.forName("https"));
    }

    @Test
    void testNames() {
        assertTrue(SecurityProtocol.names().contains("HTTP"));
        assertTrue(SecurityProtocol.names().contains("HTTPS"));
        assertEquals(6, SecurityProtocol.names().size());
    }

    @Test
    void testIsHttp() {
        assertTrue(SecurityProtocol.HTTP.isHttp());
        assertTrue(SecurityProtocol.HTTPS.isHttp());
        assertFalse(SecurityProtocol.PLAINTEXT.isHttp());
        assertFalse(SecurityProtocol.SSL.isHttp());
        assertFalse(SecurityProtocol.SASL_PLAINTEXT.isHttp());
        assertFalse(SecurityProtocol.SASL_SSL.isHttp());
    }

    @Test
    void testAllProtocolsHaveUniqueIds() {
        SecurityProtocol[] protocols = SecurityProtocol.values();
        for (int i = 0; i < protocols.length; i++) {
            for (int j = i + 1; j < protocols.length; j++) {
                assertFalse(protocols[i].id == protocols[j].id,
                    "Duplicate id " + protocols[i].id + " for " +
                    protocols[i] + " and " + protocols[j]);
            }
        }
    }

    @Test
    void testAllProtocolsRoundTripById() {
        for (SecurityProtocol protocol : SecurityProtocol.values()) {
            assertNotNull(SecurityProtocol.forId(protocol.id),
                "forId(" + protocol.id + ") returned null for " + protocol);
            assertEquals(protocol, SecurityProtocol.forId(protocol.id));
        }
    }
}
