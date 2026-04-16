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

import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.ApplicationProtocolNames;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;

import javax.net.ssl.SSLException;

import java.io.File;

/**
 * Builds a Netty {@link SslContext} for the HTTPS listener with ALPN support
 * for HTTP/2 negotiation.
 *
 * The resulting SslContext advertises both {@code h2} and {@code http/1.1} via ALPN.
 * Clients that support HTTP/2 will negotiate h2; others fall back to HTTP/1.1.
 *
 * <ul>
 *   <li>{@code SelectorFailureBehavior.NO_ADVERTISE} -- server does not advertise unsupported protocols</li>
 *   <li>{@code SelectedListenerFailureBehavior.ACCEPT} -- connection proceeds even if the client's
 *       preferred protocol is not supported</li>
 * </ul>
 */
public class HttpSslContextBuilder {

    /**
     * Build an SslContext with HTTP/2 ALPN support.
     *
     * @param certChainFile    PEM-encoded certificate chain
     * @param privateKeyFile   PEM-encoded private key
     * @param trustCertFile    PEM-encoded trusted CA certificates (for mTLS), or null
     * @param clientAuth       whether to require client certificates
     * @return SslContext configured for HTTP/2 + HTTP/1.1 ALPN
     * @throws SSLException if the SSL context cannot be built
     */
    public static SslContext build(
            File certChainFile,
            File privateKeyFile,
            File trustCertFile,
            boolean clientAuth) throws SSLException {

        SslContextBuilder builder = SslContextBuilder.forServer(certChainFile, privateKeyFile);

        if (trustCertFile != null) {
            builder.trustManager(trustCertFile);
        }

        if (clientAuth) {
            builder.clientAuth(ClientAuth.REQUIRE);
        } else {
            builder.clientAuth(ClientAuth.OPTIONAL);
        }

        builder.sslProvider(SslProvider.JDK)
            .applicationProtocolConfig(new ApplicationProtocolConfig(
                ApplicationProtocolConfig.Protocol.ALPN,
                ApplicationProtocolConfig.SelectorFailureBehavior.NO_ADVERTISE,
                ApplicationProtocolConfig.SelectedListenerFailureBehavior.ACCEPT,
                ApplicationProtocolNames.HTTP_2,
                ApplicationProtocolNames.HTTP_1_1
            ));

        return builder.build();
    }
}
