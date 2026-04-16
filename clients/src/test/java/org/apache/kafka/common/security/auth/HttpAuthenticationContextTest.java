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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.cert.X509Certificate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.mockito.Mockito.mock;

class HttpAuthenticationContextTest {

    @Test
    void testBasicConstructionHttp() throws UnknownHostException {
        InetAddress addr = InetAddress.getByName("127.0.0.1");
        HttpAuthenticationContext ctx = new HttpAuthenticationContext(
            addr, "HTTP", SecurityProtocol.HTTP, null, null, null);

        assertEquals(SecurityProtocol.HTTP, ctx.securityProtocol());
        assertEquals(addr, ctx.clientAddress());
        assertEquals("HTTP", ctx.listenerName());
        assertNull(ctx.bearerToken());
        assertNull(ctx.basicCredentials());
        assertNull(ctx.peerCertificates());
    }

    @Test
    void testBasicConstructionHttps() throws UnknownHostException {
        InetAddress addr = InetAddress.getByName("192.168.1.1");
        X509Certificate[] certs = new X509Certificate[]{mock(X509Certificate.class)};
        HttpAuthenticationContext ctx = new HttpAuthenticationContext(
            addr, "HTTPS", SecurityProtocol.HTTPS,
            "my-bearer-token", null, certs);

        assertEquals(SecurityProtocol.HTTPS, ctx.securityProtocol());
        assertEquals(addr, ctx.clientAddress());
        assertEquals("HTTPS", ctx.listenerName());
        assertEquals("my-bearer-token", ctx.bearerToken());
        assertNull(ctx.basicCredentials());
        assertNotNull(ctx.peerCertificates());
        assertArrayEquals(certs, ctx.peerCertificates());
    }

    @Test
    void testBasicCredentials() throws UnknownHostException {
        InetAddress addr = InetAddress.getByName("10.0.0.1");
        HttpAuthenticationContext ctx = new HttpAuthenticationContext(
            addr, "HTTP", SecurityProtocol.HTTP,
            null, "user:password", null);

        assertEquals("user:password", ctx.basicCredentials());
        assertNull(ctx.bearerToken());
    }

    @Test
    void testRejectsNonHttpSecurityProtocol() throws UnknownHostException {
        InetAddress addr = InetAddress.getByName("127.0.0.1");
        assertThrows(IllegalArgumentException.class, () ->
            new HttpAuthenticationContext(
                addr, "PLAINTEXT", SecurityProtocol.PLAINTEXT,
                null, null, null));
        assertThrows(IllegalArgumentException.class, () ->
            new HttpAuthenticationContext(
                addr, "SSL", SecurityProtocol.SSL,
                null, null, null));
        assertThrows(IllegalArgumentException.class, () ->
            new HttpAuthenticationContext(
                addr, "SASL_PLAINTEXT", SecurityProtocol.SASL_PLAINTEXT,
                null, null, null));
        assertThrows(IllegalArgumentException.class, () ->
            new HttpAuthenticationContext(
                addr, "SASL_SSL", SecurityProtocol.SASL_SSL,
                null, null, null));
    }

    @Test
    void testRejectsNullClientAddress() {
        assertThrows(NullPointerException.class, () ->
            new HttpAuthenticationContext(
                null, "HTTP", SecurityProtocol.HTTP,
                null, null, null));
    }

    @Test
    void testRejectsNullListenerName() throws UnknownHostException {
        InetAddress addr = InetAddress.getByName("127.0.0.1");
        assertThrows(NullPointerException.class, () ->
            new HttpAuthenticationContext(
                addr, null, SecurityProtocol.HTTP,
                null, null, null));
    }
}
