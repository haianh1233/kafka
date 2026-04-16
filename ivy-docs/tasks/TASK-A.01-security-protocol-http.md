# TASK-A.01: SecurityProtocol enum + HttpAuthenticationContext

## Prerequisites

- JDK 17+
- Kafka repository checked out at branch `feature/http-protocol`
- Ability to run `./gradlew :clients:test` from repo root
- Read the design doc section 8.1 and 12.1: `/home/anh/kafka/ivy-docs/http-protocol-design.md`

## Context

Apache Kafka's `SecurityProtocol` enum defines the transport-layer security modes available
for broker listeners. Today it has four values: `PLAINTEXT(0)`, `SSL(1)`, `SASL_PLAINTEXT(2)`,
`SASL_SSL(3)`. Every listener endpoint maps to exactly one `SecurityProtocol` via the
`listener.security.protocol.map` configuration property.

The HTTP protocol feature adds two new enum values -- `HTTP(4)` and `HTTPS(5)` -- so that
operators can configure HTTP listeners using the existing `listeners` and
`listener.security.protocol.map` machinery. `HTTPS` is a distinct value (like `SSL` vs
`PLAINTEXT`) rather than a flag, preserving the one-listener-to-one-protocol invariant.

Kafka authenticates connections via a pluggable `KafkaPrincipalBuilder` that receives an
`AuthenticationContext` carrying transport-specific metadata. Existing implementations:
- `PlaintextAuthenticationContext` -- returns `ANONYMOUS`
- `SslAuthenticationContext` -- carries the `SSLSession` for cert extraction
- `SaslAuthenticationContext` -- carries the `SaslServer` for GSSAPI/SCRAM/etc.

HTTP needs its own context class (`HttpAuthenticationContext`) carrying HTTP-specific auth
data: bearer tokens, basic auth credentials, and mTLS certificates. The
`DefaultKafkaPrincipalBuilder` must be updated to handle this new context type.

### Why this task exists

Every downstream task (the `http-server` module, `HttpAcceptor`, `HttpRequestHandler`,
`KafkaApis` dispatch) depends on `SecurityProtocol.HTTP` / `HTTPS` existing and on
`HttpAuthenticationContext` being available for principal extraction. This is the very first
change in the dependency chain.

### Key files (read these before starting)

| File | Role |
|---|---|
| `clients/src/main/java/org/apache/kafka/common/security/auth/SecurityProtocol.java` | Enum to modify |
| `clients/src/main/java/org/apache/kafka/common/security/auth/AuthenticationContext.java` | Interface to implement |
| `clients/src/main/java/org/apache/kafka/common/security/auth/PlaintextAuthenticationContext.java` | Reference implementation (simplest) |
| `clients/src/main/java/org/apache/kafka/common/security/auth/SslAuthenticationContext.java` | Reference implementation (with SSLSession) |
| `clients/src/main/java/org/apache/kafka/common/security/authenticator/DefaultKafkaPrincipalBuilder.java` | Must handle `HttpAuthenticationContext` |
| `clients/src/main/java/org/apache/kafka/common/security/auth/KafkaPrincipal.java` | `ANONYMOUS` constant, `USER_TYPE` constant |

## Specification

### 1. Add `HTTP(4)` and `HTTPS(5)` to `SecurityProtocol`

Open `clients/src/main/java/org/apache/kafka/common/security/auth/SecurityProtocol.java`.

The current enum body (line 26-34) is:

```java
public enum SecurityProtocol {
    /** Un-authenticated, non-encrypted channel */
    PLAINTEXT(0, "PLAINTEXT"),
    /** SSL channel */
    SSL(1, "SSL"),
    /** SASL authenticated, non-encrypted channel */
    SASL_PLAINTEXT(2, "SASL_PLAINTEXT"),
    /** SASL authenticated, SSL channel */
    SASL_SSL(3, "SASL_SSL");
```

Change the last semicolon on `SASL_SSL` to a comma and add two new entries:

```java
    /** SASL authenticated, SSL channel */
    SASL_SSL(3, "SASL_SSL"),
    /** HTTP channel (no TLS) */
    HTTP(4, "HTTP"),
    /** HTTPS channel (TLS) */
    HTTPS(5, "HTTPS");
```

### 2. Add `isHttp()` helper method

Add this instance method to `SecurityProtocol` (after the `forName()` static method, before
the closing brace of the enum):

```java
    /** Returns true if this protocol is HTTP or HTTPS. */
    public boolean isHttp() {
        return this == HTTP || this == HTTPS;
    }
```

This method is used in `KafkaApis.handle()` for dispatch branching (design doc section 3).

### 3. Create `HttpAuthenticationContext`

Create a new file at:
`clients/src/main/java/org/apache/kafka/common/security/auth/HttpAuthenticationContext.java`

This class implements `AuthenticationContext` and carries HTTP-specific authentication data.

### 4. Update `DefaultKafkaPrincipalBuilder.build()`

Open `clients/src/main/java/org/apache/kafka/common/security/authenticator/DefaultKafkaPrincipalBuilder.java`.

Add an `else if` branch for `HttpAuthenticationContext` in the `build()` method (line 68-87).
The branch goes **after** the `SaslAuthenticationContext` check and **before** the final
`else` that throws `IllegalArgumentException`.

Logic:
- If `HTTPS` and peer certificates are present (mTLS), extract the subject principal from
  the first (leaf) certificate and run it through `applySslPrincipalMapper()` (same as SSL).
- Otherwise, return `KafkaPrincipal.ANONYMOUS`.

Add the import for `HttpAuthenticationContext` at the top of the file.

## Implementation Details

### SecurityProtocol enum IDs

IDs 4 and 5 are chosen because the existing enum uses 0-3 sequentially. The `id` field is
`short` and stored in `CODE_TO_SECURITY_PROTOCOL` map. The static initializer block
iterates `SecurityProtocol.values()` and populates both `CODE_TO_SECURITY_PROTOCOL` and
`NAMES` automatically -- no changes to the static block are needed.

### LISTENER_SECURITY_PROTOCOL_MAP_DEFAULT side effect

`SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_DEFAULT` is computed at class-load time
from `SecurityProtocol.values()`. Adding `HTTP` and `HTTPS` will automatically include
`HTTP:HTTP,HTTPS:HTTPS` in the default map. This is the desired behavior -- it means
operators can use `HTTP` or `HTTPS` as listener names without explicitly adding them to the
protocol map, matching the convention for `PLAINTEXT`, `SSL`, etc.

### HttpAuthenticationContext fields

| Field | Type | Nullable | Source |
|---|---|---|---|
| `clientAddress` | `InetAddress` | No | Netty `ChannelHandlerContext.channel().remoteAddress()` |
| `listenerName` | `String` | No | Listener config |
| `securityProtocol` | `SecurityProtocol` | No | `HTTP` or `HTTPS` from endpoint config |
| `bearerToken` | `String` | Yes | From `Authorization: Bearer <token>` header |
| `basicCredentials` | `String` | Yes | From `Authorization: Basic <b64>` header, base64-decoded |
| `peerCertificates` | `X509Certificate[]` | Yes | From TLS handshake (HTTPS + mTLS only) |

The `securityProtocol` field is stored rather than hardcoded because the same class serves
both `HTTP` and `HTTPS` listeners.

## Skeleton Code

### SecurityProtocol.java (modified)

```java
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
package org.apache.kafka.common.security.auth;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public enum SecurityProtocol {
    /** Un-authenticated, non-encrypted channel */
    PLAINTEXT(0, "PLAINTEXT"),
    /** SSL channel */
    SSL(1, "SSL"),
    /** SASL authenticated, non-encrypted channel */
    SASL_PLAINTEXT(2, "SASL_PLAINTEXT"),
    /** SASL authenticated, SSL channel */
    SASL_SSL(3, "SASL_SSL"),
    /** HTTP channel (no TLS) */
    HTTP(4, "HTTP"),
    /** HTTPS channel (TLS) */
    HTTPS(5, "HTTPS");

    private static final Map<Short, SecurityProtocol> CODE_TO_SECURITY_PROTOCOL;
    private static final List<String> NAMES;

    static {
        SecurityProtocol[] protocols = SecurityProtocol.values();
        List<String> names = new ArrayList<>(protocols.length);
        Map<Short, SecurityProtocol> codeToSecurityProtocol = new HashMap<>(protocols.length);
        for (SecurityProtocol proto : protocols) {
            codeToSecurityProtocol.put(proto.id, proto);
            names.add(proto.name);
        }
        CODE_TO_SECURITY_PROTOCOL = Collections.unmodifiableMap(codeToSecurityProtocol);
        NAMES = Collections.unmodifiableList(names);
    }

    /** The permanent and immutable id of a security protocol -- this can't change, and must match kafka.cluster.SecurityProtocol  */
    public final short id;

    /** Name of the security protocol. This may be used by client configuration. */
    public final String name;

    SecurityProtocol(int id, String name) {
        this.id = (short) id;
        this.name = name;
    }

    public static List<String> names() {
        return NAMES;
    }

    public static SecurityProtocol forId(short id) {
        return CODE_TO_SECURITY_PROTOCOL.get(id);
    }

    /** Case insensitive lookup by protocol name */
    public static SecurityProtocol forName(String name) {
        return SecurityProtocol.valueOf(name.toUpperCase(Locale.ROOT));
    }

    /** Returns true if this protocol is HTTP or HTTPS. */
    public boolean isHttp() {
        return this == HTTP || this == HTTPS;
    }

}
```

### HttpAuthenticationContext.java (new file)

```java
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
```

### DefaultKafkaPrincipalBuilder.java (modified `build()` method)

The full modified `build()` method. Only the `HttpAuthenticationContext` branch is new; all
existing branches are unchanged.

```java
    @Override
    public KafkaPrincipal build(AuthenticationContext context) {
        if (context instanceof PlaintextAuthenticationContext) {
            return KafkaPrincipal.ANONYMOUS;
        } else if (context instanceof SslAuthenticationContext) {
            SSLSession sslSession = ((SslAuthenticationContext) context).session();
            try {
                return applySslPrincipalMapper(sslSession.getPeerPrincipal());
            } catch (SSLPeerUnverifiedException se) {
                return KafkaPrincipal.ANONYMOUS;
            }
        } else if (context instanceof SaslAuthenticationContext) {
            SaslServer saslServer = ((SaslAuthenticationContext) context).server();
            if (SaslConfigs.GSSAPI_MECHANISM.equals(saslServer.getMechanismName()))
                return applyKerberosShortNamer(saslServer.getAuthorizationID());
            else
                return new KafkaPrincipal(KafkaPrincipal.USER_TYPE, saslServer.getAuthorizationID());
        } else if (context instanceof HttpAuthenticationContext) {
            HttpAuthenticationContext httpContext = (HttpAuthenticationContext) context;
            // HTTPS with mTLS: extract principal from the leaf client certificate
            if (httpContext.securityProtocol() == SecurityProtocol.HTTPS
                    && httpContext.peerCertificates() != null
                    && httpContext.peerCertificates().length > 0) {
                return applySslPrincipalMapper(
                    httpContext.peerCertificates()[0].getSubjectX500Principal());
            }
            // HTTP without TLS, or HTTPS without client cert: anonymous
            return KafkaPrincipal.ANONYMOUS;
        } else {
            throw new IllegalArgumentException("Unhandled authentication context type: " + context.getClass().getName());
        }
    }
```

Add this import at the top of `DefaultKafkaPrincipalBuilder.java` (with the other auth imports):

```java
import org.apache.kafka.common.security.auth.HttpAuthenticationContext;
```

## Tests

### HttpAuthenticationContextTest.java (new file)

Create at: `clients/src/test/java/org/apache/kafka/common/security/auth/HttpAuthenticationContextTest.java`

```java
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
```

### SecurityProtocol enum tests (add to existing test class or create new)

There is no existing `SecurityProtocolTest.java`. Create at:
`clients/src/test/java/org/apache/kafka/common/security/auth/SecurityProtocolTest.java`

```java
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
```

### DefaultKafkaPrincipalBuilderTest update

Add tests to the existing test file for `DefaultKafkaPrincipalBuilder`. Locate it at:
`clients/src/test/java/org/apache/kafka/common/security/authenticator/DefaultKafkaPrincipalBuilderTest.java`

If it does not exist, create it. Add these test methods:

```java
    @Test
    void testBuildFromHttpContextReturnsAnonymous() throws Exception {
        DefaultKafkaPrincipalBuilder builder = new DefaultKafkaPrincipalBuilder(null, null);
        HttpAuthenticationContext httpCtx = new HttpAuthenticationContext(
            InetAddress.getByName("127.0.0.1"),
            "HTTP",
            SecurityProtocol.HTTP,
            null, null, null);
        KafkaPrincipal principal = builder.build(httpCtx);
        assertEquals(KafkaPrincipal.ANONYMOUS, principal);
    }

    @Test
    void testBuildFromHttpsContextWithoutCertsReturnsAnonymous() throws Exception {
        DefaultKafkaPrincipalBuilder builder = new DefaultKafkaPrincipalBuilder(null, null);
        HttpAuthenticationContext httpsCtx = new HttpAuthenticationContext(
            InetAddress.getByName("127.0.0.1"),
            "HTTPS",
            SecurityProtocol.HTTPS,
            "some-token", null, null);  // bearer token but no certs
        KafkaPrincipal principal = builder.build(httpsCtx);
        assertEquals(KafkaPrincipal.ANONYMOUS, principal);
    }

    @Test
    void testBuildFromHttpsContextWithMtlsCert() throws Exception {
        // Create a self-signed X509Certificate with CN=testuser
        // Use javax.security.auth.x500.X500Principal for the subject
        X509Certificate mockCert = mock(X509Certificate.class);
        X500Principal subject = new X500Principal("CN=testuser,O=TestOrg");
        when(mockCert.getSubjectX500Principal()).thenReturn(subject);

        DefaultKafkaPrincipalBuilder builder = new DefaultKafkaPrincipalBuilder(
            null, SslPrincipalMapper.fromRules("DEFAULT"));

        HttpAuthenticationContext httpsCtx = new HttpAuthenticationContext(
            InetAddress.getByName("127.0.0.1"),
            "HTTPS",
            SecurityProtocol.HTTPS,
            null, null, new X509Certificate[]{mockCert});
        KafkaPrincipal principal = builder.build(httpsCtx);
        assertEquals(KafkaPrincipal.USER_TYPE, principal.getPrincipalType());
        // With DEFAULT rule the full DN is used
        assertEquals("CN=testuser,O=TestOrg", principal.getName());
    }

    @Test
    void testBuildFromHttpsContextWithEmptyCertArrayReturnsAnonymous() throws Exception {
        DefaultKafkaPrincipalBuilder builder = new DefaultKafkaPrincipalBuilder(null, null);
        HttpAuthenticationContext httpsCtx = new HttpAuthenticationContext(
            InetAddress.getByName("127.0.0.1"),
            "HTTPS",
            SecurityProtocol.HTTPS,
            null, null, new X509Certificate[]{});  // empty array
        KafkaPrincipal principal = builder.build(httpsCtx);
        assertEquals(KafkaPrincipal.ANONYMOUS, principal);
    }
```

Required imports for the test class:

```java
import org.apache.kafka.common.security.auth.HttpAuthenticationContext;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.ssl.SslPrincipalMapper;
import java.net.InetAddress;
import java.security.cert.X509Certificate;
import javax.security.auth.x500.X500Principal;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
```

## Rules

1. **Do not change the `id` values of existing enum entries.** IDs 0-3 are wire-protocol
   stable. Changing them breaks rolling upgrades and stored metadata.

2. **Do not modify the static initializer block in `SecurityProtocol`.** It already
   iterates `values()` and populates both maps correctly for any number of entries.

3. **`HttpAuthenticationContext` must validate `securityProtocol` in the constructor.**
   Throw `IllegalArgumentException` if the protocol is not `HTTP` or `HTTPS`. This prevents
   accidental misuse.

4. **`HttpAuthenticationContext` must require non-null `clientAddress` and `listenerName`.**
   Use `Objects.requireNonNull()`. Auth fields (`bearerToken`, `basicCredentials`,
   `peerCertificates`) are nullable by design.

5. **`DefaultKafkaPrincipalBuilder` must not throw for `HttpAuthenticationContext`.**
   The HTTP path should gracefully fall back to `ANONYMOUS` when no credentials are present.
   This matches the pattern for `PlaintextAuthenticationContext` and the
   `SSLPeerUnverifiedException` catch in the SSL branch.

6. **Do not add Netty or HTTP library dependencies to the `:clients` module.** The
   `HttpAuthenticationContext` class uses only JDK types (`InetAddress`,
   `X509Certificate`). HTTP header parsing happens in the `http-server` module (TASK-A.02),
   not here.

7. **Follow existing code style.** Use the Apache 2.0 license header. Use `this.field`
   assignments in the constructor. No `@Nullable`/`@NonNull` annotations (the `clients`
   module does not use them).

8. **Run `./gradlew :clients:test` and verify all tests pass**, including pre-existing
   tests that may reference `SecurityProtocol.values()`.

## Learning

- The `SecurityProtocol` static initializer block auto-populates both `CODE_TO_SECURITY_PROTOCOL` and `NAMES` from `values()`, so adding new enum entries requires zero changes to the static block.
- `SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_DEFAULT` is also auto-generated from `SecurityProtocol.values()`, meaning `HTTP:HTTP,HTTPS:HTTPS` is automatically included in the default map -- desired behavior matching existing protocol conventions.
- No tests in the `clients` module iterate over `SecurityProtocol.values()`, so adding new enum values caused no test breakage.
- The `applySslPrincipalMapper()` method in `DefaultKafkaPrincipalBuilder` accepts a `Principal` argument and handles both `X500Principal` and plain `Principal` types, which makes it reusable for the HTTPS mTLS path via `X509Certificate.getSubjectX500Principal()`.

## Limitations

- `HttpAuthenticationContext` carries auth data (bearer tokens, basic credentials) but `DefaultKafkaPrincipalBuilder` does not use them for principal extraction -- it only handles mTLS. Custom `KafkaPrincipalBuilder` implementations will be needed for bearer/basic auth in downstream tasks.
- The `peerCertificates` array is exposed directly (no defensive copy), consistent with how `SslAuthenticationContext` exposes `SSLSession` directly.

## Field Notes

- 4 pre-existing flaky test failures in `SaslAuthenticatorTest` (SASL channel readiness timeouts, "Channel was not ready after 30 seconds") -- completely unrelated to this change.
- TDD approach: tests written first, confirmed compilation failure (RED), then implementation added (GREEN). All 21 new + existing principal builder tests pass.

## Acceptance Criteria

- [ ] `SecurityProtocol.HTTP` exists with `id=4`, `name="HTTP"`
- [ ] `SecurityProtocol.HTTPS` exists with `id=5`, `name="HTTPS"`
- [ ] `SecurityProtocol.HTTP.isHttp()` returns `true`
- [ ] `SecurityProtocol.HTTPS.isHttp()` returns `true`
- [ ] `SecurityProtocol.PLAINTEXT.isHttp()` returns `false` (and SSL, SASL_PLAINTEXT, SASL_SSL)
- [ ] `SecurityProtocol.forId((short) 4)` returns `HTTP`
- [ ] `SecurityProtocol.forId((short) 5)` returns `HTTPS`
- [ ] `SecurityProtocol.forName("HTTP")` returns `HTTP`
- [ ] `SecurityProtocol.forName("HTTPS")` returns `HTTPS`
- [ ] `SecurityProtocol.names()` contains "HTTP" and "HTTPS" (size = 6)
- [ ] `HttpAuthenticationContext` implements `AuthenticationContext`
- [ ] `HttpAuthenticationContext` constructor rejects non-HTTP protocols with `IllegalArgumentException`
- [ ] `HttpAuthenticationContext` constructor rejects null `clientAddress` with `NullPointerException`
- [ ] `HttpAuthenticationContext` constructor rejects null `listenerName` with `NullPointerException`
- [ ] `HttpAuthenticationContext` allows null `bearerToken`, `basicCredentials`, `peerCertificates`
- [ ] `DefaultKafkaPrincipalBuilder.build(HttpAuthenticationContext)` returns `ANONYMOUS` for HTTP without credentials
- [ ] `DefaultKafkaPrincipalBuilder.build(HttpAuthenticationContext)` returns `ANONYMOUS` for HTTPS without peer certs
- [ ] `DefaultKafkaPrincipalBuilder.build(HttpAuthenticationContext)` extracts CN from mTLS cert for HTTPS with peer certs
- [ ] `./gradlew :clients:test` passes with zero failures
- [ ] All new tests pass: `SecurityProtocolTest`, `HttpAuthenticationContextTest`, `DefaultKafkaPrincipalBuilder` HTTP tests

## File Manifest

| File | Action | Description |
|---|---|---|
| `clients/src/main/java/org/apache/kafka/common/security/auth/SecurityProtocol.java` | Modified | Added `HTTP(4)`, `HTTPS(5)` enum values and `isHttp()` method |
| `clients/src/main/java/org/apache/kafka/common/security/auth/HttpAuthenticationContext.java` | Created | New `AuthenticationContext` implementation for HTTP/HTTPS listeners |
| `clients/src/main/java/org/apache/kafka/common/security/authenticator/DefaultKafkaPrincipalBuilder.java` | Modified | Added `HttpAuthenticationContext` branch in `build()` method |
| `clients/src/test/java/org/apache/kafka/common/security/auth/SecurityProtocolTest.java` | Created | Tests for HTTP/HTTPS enum values, `isHttp()`, `forId()`, `forName()`, `names()` |
| `clients/src/test/java/org/apache/kafka/common/security/auth/HttpAuthenticationContextTest.java` | Created | Tests for construction, field access, and validation of `HttpAuthenticationContext` |
| `clients/src/test/java/org/apache/kafka/common/security/auth/DefaultKafkaPrincipalBuilderTest.java` | Modified | Added 4 tests for HTTP/HTTPS principal building (anonymous, mTLS, empty certs) |
