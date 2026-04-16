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
// Time: Created - TASK-F.05
package org.apache.kafka.common.security.auth;

import org.apache.kafka.test.TestSslUtils;

import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.security.KeyPair;
import java.security.cert.X509Certificate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HttpAuthenticationContextTest {

    @Test
    void testAnonymousContext() throws Exception {
        HttpAuthenticationContext ctx = new HttpAuthenticationContext.Builder()
            .clientAddress(InetAddress.getLoopbackAddress())
            .securityProtocol(SecurityProtocol.PLAINTEXT)
            .build();

        assertFalse(ctx.hasCredentials());
        assertTrue(ctx.peerCertificates().isEmpty());
        assertTrue(ctx.bearerToken().isEmpty());
        assertTrue(ctx.basicUsername().isEmpty());
        assertTrue(ctx.basicPassword().isEmpty());
        assertEquals(SecurityProtocol.PLAINTEXT, ctx.securityProtocol());
        assertEquals(InetAddress.getLoopbackAddress(), ctx.clientAddress());
    }

    @Test
    void testBearerTokenContext() throws Exception {
        HttpAuthenticationContext ctx = new HttpAuthenticationContext.Builder()
            .clientAddress(InetAddress.getLoopbackAddress())
            .securityProtocol(SecurityProtocol.SSL)
            .bearerToken("eyJhbGciOiJSUzI1NiJ9.test.sig")
            .build();

        assertTrue(ctx.hasCredentials());
        assertTrue(ctx.bearerToken().isPresent());
        assertEquals("eyJhbGciOiJSUzI1NiJ9.test.sig", ctx.bearerToken().get());
        assertTrue(ctx.peerCertificates().isEmpty());
        assertTrue(ctx.basicUsername().isEmpty());
    }

    @Test
    void testBasicAuthContext() throws Exception {
        HttpAuthenticationContext ctx = new HttpAuthenticationContext.Builder()
            .clientAddress(InetAddress.getLoopbackAddress())
            .securityProtocol(SecurityProtocol.SSL)
            .basicCredentials("admin", "secret")
            .build();

        assertTrue(ctx.hasCredentials());
        assertTrue(ctx.basicUsername().isPresent());
        assertEquals("admin", ctx.basicUsername().get());
        assertTrue(ctx.basicPassword().isPresent());
        assertEquals("secret", ctx.basicPassword().get());
        assertTrue(ctx.peerCertificates().isEmpty());
        assertTrue(ctx.bearerToken().isEmpty());
    }

    @Test
    void testMtlsContext() throws Exception {
        X509Certificate[] certs = createTestCertificates();

        HttpAuthenticationContext ctx = new HttpAuthenticationContext.Builder()
            .clientAddress(InetAddress.getLoopbackAddress())
            .securityProtocol(SecurityProtocol.SSL)
            .peerCertificates(certs)
            .build();

        assertTrue(ctx.hasCredentials());
        assertTrue(ctx.peerCertificates().isPresent());
        assertEquals(1, ctx.peerCertificates().get().length);
        assertTrue(ctx.bearerToken().isEmpty());
        assertTrue(ctx.basicUsername().isEmpty());
    }

    @Test
    void testBuilderRequiresClientAddress() {
        assertThrows(IllegalArgumentException.class, () ->
            new HttpAuthenticationContext.Builder()
                .securityProtocol(SecurityProtocol.PLAINTEXT)
                .build()
        );
    }

    @Test
    void testDefaultSecurityProtocol() throws Exception {
        HttpAuthenticationContext ctx = new HttpAuthenticationContext.Builder()
            .clientAddress(InetAddress.getLoopbackAddress())
            .build();

        assertEquals(SecurityProtocol.PLAINTEXT, ctx.securityProtocol());
    }

    @Test
    void testDefaultListenerName() throws Exception {
        HttpAuthenticationContext ctx = new HttpAuthenticationContext.Builder()
            .clientAddress(InetAddress.getLoopbackAddress())
            .build();

        assertEquals("HTTP", ctx.listenerName());
    }

    @Test
    void testCustomListenerName() throws Exception {
        HttpAuthenticationContext ctx = new HttpAuthenticationContext.Builder()
            .clientAddress(InetAddress.getLoopbackAddress())
            .listenerName("HTTPS_EXTERNAL")
            .build();

        assertEquals("HTTPS_EXTERNAL", ctx.listenerName());
    }

    @Test
    void testHasCredentialsWithOnlyPeerCertificates() throws Exception {
        X509Certificate[] certs = createTestCertificates();

        HttpAuthenticationContext ctx = new HttpAuthenticationContext.Builder()
            .clientAddress(InetAddress.getLoopbackAddress())
            .peerCertificates(certs)
            .build();

        assertTrue(ctx.hasCredentials());
    }

    @Test
    void testHasCredentialsWithOnlyBearerToken() throws Exception {
        HttpAuthenticationContext ctx = new HttpAuthenticationContext.Builder()
            .clientAddress(InetAddress.getLoopbackAddress())
            .bearerToken("some-token")
            .build();

        assertTrue(ctx.hasCredentials());
    }

    @Test
    void testHasCredentialsWithOnlyBasicAuth() throws Exception {
        HttpAuthenticationContext ctx = new HttpAuthenticationContext.Builder()
            .clientAddress(InetAddress.getLoopbackAddress())
            .basicCredentials("user", "pass")
            .build();

        assertTrue(ctx.hasCredentials());
    }

    private X509Certificate[] createTestCertificates() throws Exception {
        KeyPair keyPair = TestSslUtils.generateKeyPair("RSA");
        X509Certificate cert = TestSslUtils.generateCertificate("CN=testclient", keyPair, 365, "SHA256withRSA");
        return new X509Certificate[]{cert};
    }
}
