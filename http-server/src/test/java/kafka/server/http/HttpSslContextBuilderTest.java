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
package kafka.server.http;

import io.netty.handler.ssl.SslContext;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.util.Base64;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link HttpSslContextBuilder}.
 *
 * Verifies that the builder creates an SslContext with ALPN support
 * advertising both h2 and http/1.1 protocols.
 */
class HttpSslContextBuilderTest {

    @TempDir
    Path tempDir;

    @Test
    void testBuildSslContextWithAlpn() throws Exception {
        TestCertificateHolder certHolder = generateTestCertificateFiles();

        SslContext sslCtx = HttpSslContextBuilder.build(
            certHolder.certFile, certHolder.keyFile, null, false);

        assertNotNull(sslCtx, "SslContext should not be null");
        assertTrue(sslCtx.isServer(), "SslContext should be a server context");

        // Verify ALPN protocols are configured
        List<String> protocols = sslCtx.applicationProtocolNegotiator().protocols();
        assertNotNull(protocols, "ALPN protocols list should not be null");
        assertTrue(protocols.contains("h2"),
            "ALPN should include h2, but got: " + protocols);
        assertTrue(protocols.contains("http/1.1"),
            "ALPN should include http/1.1, but got: " + protocols);
    }

    @Test
    void testBuildSslContextWithClientAuth() throws Exception {
        TestCertificateHolder certHolder = generateTestCertificateFiles();

        // Should not throw with clientAuth=true and trust cert
        SslContext sslCtx = HttpSslContextBuilder.build(
            certHolder.certFile, certHolder.keyFile, certHolder.certFile, true);
        assertNotNull(sslCtx, "SslContext should not be null with client auth enabled");
    }

    @Test
    void testAlpnProtocolOrderH2First() throws Exception {
        TestCertificateHolder certHolder = generateTestCertificateFiles();

        SslContext sslCtx = HttpSslContextBuilder.build(
            certHolder.certFile, certHolder.keyFile, null, false);

        List<String> protocols = sslCtx.applicationProtocolNegotiator().protocols();
        // h2 should be listed before http/1.1 (server preference order)
        int h2Index = protocols.indexOf("h2");
        int http11Index = protocols.indexOf("http/1.1");
        assertTrue(h2Index >= 0, "h2 should be in ALPN protocols");
        assertTrue(http11Index >= 0, "http/1.1 should be in ALPN protocols");
        assertTrue(h2Index < http11Index,
            "h2 should be preferred (listed before http/1.1) in ALPN order");
    }

    /**
     * Generate self-signed certificate and private key PEM files for testing.
     * Uses Java's keytool-equivalent APIs to avoid dependency on sun.security.x509.
     */
    private TestCertificateHolder generateTestCertificateFiles() throws Exception {
        // Create a temporary keystore with keytool, then extract cert and key as PEM
        File keystoreFile = tempDir.resolve("test.p12").toFile();
        String keystorePass = "testpass";

        ProcessBuilder pb = new ProcessBuilder(
            "keytool", "-genkeypair",
            "-alias", "test",
            "-keyalg", "RSA",
            "-keysize", "2048",
            "-validity", "365",
            "-dname", "CN=localhost,OU=Test,O=Kafka,L=Test,ST=Test,C=US",
            "-keystore", keystoreFile.getAbsolutePath(),
            "-storetype", "PKCS12",
            "-storepass", keystorePass,
            "-keypass", keystorePass
        );
        pb.inheritIO();
        Process process = pb.start();
        int exitCode = process.waitFor();
        if (exitCode != 0) {
            throw new RuntimeException("keytool failed with exit code " + exitCode);
        }

        // Load the keystore
        KeyStore keyStore = KeyStore.getInstance("PKCS12");
        try (var fis = new java.io.FileInputStream(keystoreFile)) {
            keyStore.load(fis, keystorePass.toCharArray());
        }

        // Export certificate to PEM
        Certificate cert = keyStore.getCertificate("test");
        File certFile = tempDir.resolve("cert.pem").toFile();
        try (FileWriter writer = new FileWriter(certFile)) {
            writer.write("-----BEGIN CERTIFICATE-----\n");
            writer.write(Base64.getMimeEncoder(64, "\n".getBytes()).encodeToString(cert.getEncoded()));
            writer.write("\n-----END CERTIFICATE-----\n");
        }

        // Export private key to PEM
        java.security.Key key = keyStore.getKey("test", keystorePass.toCharArray());
        File keyFile = tempDir.resolve("key.pem").toFile();
        try (FileWriter writer = new FileWriter(keyFile)) {
            writer.write("-----BEGIN PRIVATE KEY-----\n");
            writer.write(Base64.getMimeEncoder(64, "\n".getBytes()).encodeToString(key.getEncoded()));
            writer.write("\n-----END PRIVATE KEY-----\n");
        }

        return new TestCertificateHolder(certFile, keyFile);
    }

    private static class TestCertificateHolder {
        final File certFile;
        final File keyFile;

        TestCertificateHolder(File certFile, File keyFile) {
            this.certFile = certFile;
            this.keyFile = keyFile;
        }
    }
}
