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

import org.apache.kafka.common.security.authenticator.DefaultKafkaPrincipalBuilder;
import org.apache.kafka.common.security.ssl.SslPrincipalMapper;
import org.apache.kafka.test.TestSslUtils;

import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.security.KeyPair;
import java.security.cert.X509Certificate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class DefaultKafkaPrincipalBuilderHttpTest {

    @Test
    void testAnonymousHttpContextReturnsAnonymous() throws Exception {
        DefaultKafkaPrincipalBuilder builder = new DefaultKafkaPrincipalBuilder(null, null);
        HttpAuthenticationContext ctx = new HttpAuthenticationContext.Builder()
            .clientAddress(InetAddress.getLoopbackAddress())
            .securityProtocol(SecurityProtocol.PLAINTEXT)
            .build();

        KafkaPrincipal principal = builder.build(ctx);
        assertEquals(KafkaPrincipal.ANONYMOUS, principal);
    }

    @Test
    void testBearerTokenReturnsAnonymousFromDefaultBuilder() throws Exception {
        DefaultKafkaPrincipalBuilder builder = new DefaultKafkaPrincipalBuilder(null, null);
        HttpAuthenticationContext ctx = new HttpAuthenticationContext.Builder()
            .clientAddress(InetAddress.getLoopbackAddress())
            .securityProtocol(SecurityProtocol.SSL)
            .bearerToken("some-jwt-token")
            .build();

        // Default builder does NOT handle Bearer tokens -- returns ANONYMOUS
        KafkaPrincipal principal = builder.build(ctx);
        assertEquals(KafkaPrincipal.ANONYMOUS, principal);
    }

    @Test
    void testBasicAuthReturnsAnonymousFromDefaultBuilder() throws Exception {
        DefaultKafkaPrincipalBuilder builder = new DefaultKafkaPrincipalBuilder(null, null);
        HttpAuthenticationContext ctx = new HttpAuthenticationContext.Builder()
            .clientAddress(InetAddress.getLoopbackAddress())
            .securityProtocol(SecurityProtocol.SSL)
            .basicCredentials("admin", "secret")
            .build();

        // Default builder does NOT handle Basic auth -- returns ANONYMOUS
        KafkaPrincipal principal = builder.build(ctx);
        assertEquals(KafkaPrincipal.ANONYMOUS, principal);
    }

    @Test
    void testMtlsCertificateExtractsPrincipal() throws Exception {
        KeyPair keyPair = TestSslUtils.generateKeyPair("RSA");
        X509Certificate cert = TestSslUtils.generateCertificate(
            "CN=testclient, OU=Engineering, O=TestOrg", keyPair, 365, "SHA256withRSA");
        X509Certificate[] certs = new X509Certificate[]{cert};

        // SslPrincipalMapper with DEFAULT rule passes through the X500 name
        SslPrincipalMapper mapper = SslPrincipalMapper.fromRules("DEFAULT");
        DefaultKafkaPrincipalBuilder builder = new DefaultKafkaPrincipalBuilder(null, mapper);
        HttpAuthenticationContext ctx = new HttpAuthenticationContext.Builder()
            .clientAddress(InetAddress.getLoopbackAddress())
            .securityProtocol(SecurityProtocol.SSL)
            .peerCertificates(certs)
            .build();

        KafkaPrincipal principal = builder.build(ctx);
        // With DEFAULT rule and X500Principal, the full distinguished name is used
        assertNotEquals(KafkaPrincipal.ANONYMOUS, principal);
        assertEquals(KafkaPrincipal.USER_TYPE, principal.getPrincipalType());
    }

    @Test
    void testMtlsWithSslPrincipalMapper() throws Exception {
        KeyPair keyPair = TestSslUtils.generateKeyPair("RSA");
        X509Certificate cert = TestSslUtils.generateCertificate(
            "CN=Duke, OU=ServiceUsers, O=Org, C=US", keyPair, 365, "SHA256withRSA");
        X509Certificate[] certs = new X509Certificate[]{cert};

        // Use a regex that matches the CN regardless of DN order (RFC 2253 may reverse it).
        // CN may appear at the end of the string, so we match until comma or end-of-string.
        String rules = String.join(", ",
            "RULE:^.*CN=([^,]*).*$/$1/L",
            "DEFAULT"
        );
        SslPrincipalMapper mapper = SslPrincipalMapper.fromRules(rules);
        DefaultKafkaPrincipalBuilder builder = new DefaultKafkaPrincipalBuilder(null, mapper);

        HttpAuthenticationContext ctx = new HttpAuthenticationContext.Builder()
            .clientAddress(InetAddress.getLoopbackAddress())
            .securityProtocol(SecurityProtocol.SSL)
            .peerCertificates(certs)
            .build();

        KafkaPrincipal principal = builder.build(ctx);
        assertEquals(KafkaPrincipal.USER_TYPE, principal.getPrincipalType());
        assertEquals("duke", principal.getName());
    }

    @Test
    void testMtlsWithEmptyCertArrayReturnsAnonymous() throws Exception {
        DefaultKafkaPrincipalBuilder builder = new DefaultKafkaPrincipalBuilder(null, null);
        HttpAuthenticationContext ctx = new HttpAuthenticationContext.Builder()
            .clientAddress(InetAddress.getLoopbackAddress())
            .securityProtocol(SecurityProtocol.SSL)
            .peerCertificates(new X509Certificate[0])
            .build();

        KafkaPrincipal principal = builder.build(ctx);
        assertEquals(KafkaPrincipal.ANONYMOUS, principal);
    }
}
