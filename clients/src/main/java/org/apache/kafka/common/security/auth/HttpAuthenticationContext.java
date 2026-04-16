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

import java.net.InetAddress;
import java.security.cert.X509Certificate;
import java.util.Objects;

/**
 * Authentication context for HTTP and HTTPS listeners.
 * <p>
 * Carries HTTP-specific authentication data extracted from the incoming request:
 * bearer tokens, basic auth credentials, and mTLS peer certificates. A configured
 * {@link org.apache.kafka.common.security.auth.KafkaPrincipalBuilder} uses this
 * context to derive the {@link KafkaPrincipal} for authorization.
 *
 * @see PlaintextAuthenticationContext
 * @see SslAuthenticationContext
 * @see SaslAuthenticationContext
 */
public class HttpAuthenticationContext implements AuthenticationContext {

    private final InetAddress clientAddress;
    private final String listenerName;
    private final SecurityProtocol securityProtocol;
    private final String bearerToken;
    private final String basicCredentials;
    private final X509Certificate[] peerCertificates;

    /**
     * Construct a new HttpAuthenticationContext.
     *
     * @param clientAddress    IP address of the HTTP client (must not be null)
     * @param listenerName     name of the Kafka listener (must not be null)
     * @param securityProtocol must be {@link SecurityProtocol#HTTP} or {@link SecurityProtocol#HTTPS}
     * @param bearerToken      value from {@code Authorization: Bearer <token>} header, or null
     * @param basicCredentials decoded value from {@code Authorization: Basic <b64>} header, or null
     * @param peerCertificates client certificates from TLS handshake (HTTPS + mTLS), or null
     */
    public HttpAuthenticationContext(
            InetAddress clientAddress,
            String listenerName,
            SecurityProtocol securityProtocol,
            String bearerToken,
            String basicCredentials,
            X509Certificate[] peerCertificates) {
        this.clientAddress = Objects.requireNonNull(clientAddress, "clientAddress must not be null");
        this.listenerName = Objects.requireNonNull(listenerName, "listenerName must not be null");
        if (!securityProtocol.isHttp()) {
            throw new IllegalArgumentException(
                "securityProtocol must be HTTP or HTTPS, got " + securityProtocol);
        }
        this.securityProtocol = securityProtocol;
        this.bearerToken = bearerToken;
        this.basicCredentials = basicCredentials;
        this.peerCertificates = peerCertificates;
    }

    @Override
    public SecurityProtocol securityProtocol() {
        return securityProtocol;
    }

    @Override
    public InetAddress clientAddress() {
        return clientAddress;
    }

    @Override
    public String listenerName() {
        return listenerName;
    }

    /**
     * Returns the bearer token from the {@code Authorization: Bearer <token>} header,
     * or {@code null} if no bearer token was present.
     */
    public String bearerToken() {
        return bearerToken;
    }

    /**
     * Returns the decoded credentials from the {@code Authorization: Basic <b64>} header,
     * or {@code null} if no basic auth header was present. Format: {@code "username:password"}.
     */
    public String basicCredentials() {
        return basicCredentials;
    }

    /**
     * Returns the client's X.509 certificate chain from the TLS handshake,
     * or {@code null} if no client certificates were presented (no mTLS).
     * The first element is the leaf (client) certificate.
     */
    public X509Certificate[] peerCertificates() {
        return peerCertificates;
    }
}
