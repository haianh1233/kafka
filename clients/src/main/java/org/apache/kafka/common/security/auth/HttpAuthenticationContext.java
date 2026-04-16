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

import java.net.InetAddress;
import java.security.cert.X509Certificate;
import java.util.Optional;

/**
 * Authentication context for HTTP requests.
 *
 * Carries all possible authentication data extracted from the HTTP request:
 * <ul>
 *   <li>Client certificates (mTLS over HTTPS)</li>
 *   <li>Bearer token (Authorization: Bearer header)</li>
 *   <li>Basic credentials (Authorization: Basic header)</li>
 *   <li>Client IP address (always present)</li>
 * </ul>
 *
 * A configured {@link KafkaPrincipalBuilder} inspects this context to determine the principal.
 * The default builder handles mTLS certificates and falls back to ANONYMOUS. Custom builders
 * handle Bearer and Basic auth.
 *
 * Placed in the {@code clients} module alongside other {@link AuthenticationContext} implementations
 * to avoid circular dependencies between {@code http-server} and {@code clients}.
 *
 * @see AuthenticationContext
 * @see org.apache.kafka.common.security.authenticator.DefaultKafkaPrincipalBuilder
 */
public class HttpAuthenticationContext implements AuthenticationContext {

    private final InetAddress clientAddress;
    private final SecurityProtocol securityProtocol;
    private final X509Certificate[] peerCertificates;
    private final String bearerToken;
    private final String basicUsername;
    private final String basicPassword;
    private final String listenerName;

    private HttpAuthenticationContext(Builder builder) {
        this.clientAddress = builder.clientAddress;
        this.securityProtocol = builder.securityProtocol;
        this.peerCertificates = builder.peerCertificates;
        this.bearerToken = builder.bearerToken;
        this.basicUsername = builder.basicUsername;
        this.basicPassword = builder.basicPassword;
        this.listenerName = builder.listenerName;
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
     * Client certificates from mTLS handshake.
     * Present only when listener uses HTTPS and client presents a certificate.
     */
    public Optional<X509Certificate[]> peerCertificates() {
        return Optional.ofNullable(peerCertificates);
    }

    /**
     * Bearer token from Authorization header.
     * Present only when the request has "Authorization: Bearer ..." header.
     */
    public Optional<String> bearerToken() {
        return Optional.ofNullable(bearerToken);
    }

    /**
     * Basic auth username.
     * Present only when the request has "Authorization: Basic ..." header.
     */
    public Optional<String> basicUsername() {
        return Optional.ofNullable(basicUsername);
    }

    /**
     * Basic auth password.
     * Present only when the request has "Authorization: Basic ..." header.
     */
    public Optional<String> basicPassword() {
        return Optional.ofNullable(basicPassword);
    }

    /**
     * Whether this context has any authentication credentials.
     */
    public boolean hasCredentials() {
        return peerCertificates != null || bearerToken != null || basicUsername != null;
    }

    public static class Builder {
        private InetAddress clientAddress;
        private SecurityProtocol securityProtocol = SecurityProtocol.PLAINTEXT;
        private X509Certificate[] peerCertificates;
        private String bearerToken;
        private String basicUsername;
        private String basicPassword;
        private String listenerName = "HTTP";

        public Builder clientAddress(InetAddress clientAddress) {
            this.clientAddress = clientAddress;
            return this;
        }

        public Builder securityProtocol(SecurityProtocol securityProtocol) {
            this.securityProtocol = securityProtocol;
            return this;
        }

        public Builder peerCertificates(X509Certificate[] certs) {
            this.peerCertificates = certs;
            return this;
        }

        public Builder bearerToken(String token) {
            this.bearerToken = token;
            return this;
        }

        public Builder basicCredentials(String username, String password) {
            this.basicUsername = username;
            this.basicPassword = password;
            return this;
        }

        public Builder listenerName(String listenerName) {
            this.listenerName = listenerName;
            return this;
        }

        public HttpAuthenticationContext build() {
            if (clientAddress == null)
                throw new IllegalArgumentException("clientAddress is required");
            return new HttpAuthenticationContext(this);
        }
    }
}
