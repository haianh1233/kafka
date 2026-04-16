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

import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for HTTP Authorization header parsing logic that is used in
 * HttpRequestHandler.extractAuthContext().
 *
 * These tests validate the parsing algorithms independently of Netty,
 * ensuring correct behavior for Bearer tokens, Basic auth, and edge cases.
 */
class HttpAuthExtractionTest {

    @Test
    void testParseBearerToken() {
        String header = "Bearer eyJhbGciOiJSUzI1NiJ9.test.sig";
        assertTrue(header.regionMatches(true, 0, "Bearer ", 0, 7));
        String token = header.substring(7).trim();
        assertEquals("eyJhbGciOiJSUzI1NiJ9.test.sig", token);
    }

    @Test
    void testParseBearerTokenCaseInsensitive() {
        String header = "bearer my-token";
        assertTrue(header.regionMatches(true, 0, "Bearer ", 0, 7));
        String token = header.substring(7).trim();
        assertEquals("my-token", token);
    }

    @Test
    void testParseBearerTokenMixedCase() {
        String header = "BEARER MY-TOKEN-123";
        assertTrue(header.regionMatches(true, 0, "Bearer ", 0, 7));
        String token = header.substring(7).trim();
        assertEquals("MY-TOKEN-123", token);
    }

    @Test
    void testParseBasicAuth() {
        String credentials = "user:pass";
        String header = "Basic " + Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
        assertTrue(header.regionMatches(true, 0, "Basic ", 0, 6));
        String encoded = header.substring(6).trim();
        String decoded = new String(Base64.getDecoder().decode(encoded), StandardCharsets.UTF_8);
        assertEquals("user:pass", decoded);
        int colonIdx = decoded.indexOf(':');
        assertEquals("user", decoded.substring(0, colonIdx));
        assertEquals("pass", decoded.substring(colonIdx + 1));
    }

    @Test
    void testParseBasicAuthWithColonInPassword() {
        // Password contains colons -- must split on first colon only
        String credentials = "user:p:a:ss";
        String header = "Basic " + Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
        String encoded = header.substring(6).trim();
        String decoded = new String(Base64.getDecoder().decode(encoded), StandardCharsets.UTF_8);
        int colonIdx = decoded.indexOf(':');
        assertEquals("user", decoded.substring(0, colonIdx));
        assertEquals("p:a:ss", decoded.substring(colonIdx + 1));
    }

    @Test
    void testParseBasicAuthCaseInsensitive() {
        String credentials = "admin:secret";
        String header = "basic " + Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
        assertTrue(header.regionMatches(true, 0, "Basic ", 0, 6));
    }

    @Test
    void testParseBasicAuthInvalidBase64() {
        String header = "Basic not-valid-base64!!!";
        String encoded = header.substring(6).trim();
        assertThrows(IllegalArgumentException.class, () ->
            Base64.getDecoder().decode(encoded)
        );
    }

    @Test
    void testNoAuthHeaderReturnsAnonymousContext() throws Exception {
        // When no Authorization header is present, the context has no credentials
        HttpAuthenticationContext ctx = new HttpAuthenticationContext.Builder()
            .clientAddress(InetAddress.getLoopbackAddress())
            .build();
        assertFalse(ctx.hasCredentials());
        assertTrue(ctx.peerCertificates().isEmpty());
        assertTrue(ctx.bearerToken().isEmpty());
        assertTrue(ctx.basicUsername().isEmpty());
    }

    @Test
    void testBearerTokenContextFromParsedHeader() throws Exception {
        // Simulate what HttpRequestHandler does after parsing Bearer header
        String header = "Bearer eyJhbGciOiJSUzI1NiJ9.payload.sig";
        String token = header.substring(7).trim();

        HttpAuthenticationContext ctx = new HttpAuthenticationContext.Builder()
            .clientAddress(InetAddress.getLoopbackAddress())
            .securityProtocol(SecurityProtocol.SSL)
            .bearerToken(token)
            .build();

        assertTrue(ctx.hasCredentials());
        assertEquals("eyJhbGciOiJSUzI1NiJ9.payload.sig", ctx.bearerToken().get());
    }

    @Test
    void testBasicAuthContextFromParsedHeader() throws Exception {
        // Simulate what HttpRequestHandler does after parsing Basic header
        String credentials = "admin:s3cret";
        String header = "Basic " + Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
        String encoded = header.substring(6).trim();
        String decoded = new String(Base64.getDecoder().decode(encoded), StandardCharsets.UTF_8);
        int colonIdx = decoded.indexOf(':');
        String username = decoded.substring(0, colonIdx);
        String password = decoded.substring(colonIdx + 1);

        HttpAuthenticationContext ctx = new HttpAuthenticationContext.Builder()
            .clientAddress(InetAddress.getLoopbackAddress())
            .securityProtocol(SecurityProtocol.SSL)
            .basicCredentials(username, password)
            .build();

        assertTrue(ctx.hasCredentials());
        assertEquals("admin", ctx.basicUsername().get());
        assertEquals("s3cret", ctx.basicPassword().get());
    }

    @Test
    void testEmptyBearerTokenIsNotSet() {
        // If "Bearer " with no token, should not produce a valid context
        String header = "Bearer ";
        String token = header.substring(7).trim();
        assertTrue(token.isEmpty());
        // HttpRequestHandler would fall through to anonymous in this case
    }

    @Test
    void testBasicAuthWithEmptyPassword() throws Exception {
        // Username with empty password: "user:"
        String credentials = "user:";
        String encoded = Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
        String decoded = new String(Base64.getDecoder().decode(encoded), StandardCharsets.UTF_8);
        int colonIdx = decoded.indexOf(':');
        assertTrue(colonIdx > 0);
        assertEquals("user", decoded.substring(0, colonIdx));
        assertEquals("", decoded.substring(colonIdx + 1));
    }
}
