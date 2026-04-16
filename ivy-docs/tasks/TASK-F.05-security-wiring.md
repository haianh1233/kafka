# TASK-F.05: mTLS + Bearer + Basic Auth Wiring

## Prerequisites

| Task | What it provides |
|------|-----------------|
| TASK-A.01 | `HttpAuthenticationContext` -- implements `AuthenticationContext` for HTTP |
| TASK-B.05 | `HttpRequestHandler` -- Netty handler where authentication is extracted |

Both must be merged and passing CI before this task begins.

---

## Context

The HTTP protocol design (section 12.1) specifies authentication via the existing
`KafkaPrincipalBuilder` interface. Rather than parsing HTTP headers ad-hoc, the handler
injects `HttpAuthenticationContext` into `KafkaPrincipalBuilder.build()` and receives a
`KafkaPrincipal` that flows through the standard `Authorizer` path.

This task wires the four authentication methods:

| Auth method | Transport | What HttpAuthenticationContext carries |
|---|---|---|
| **mTLS** | HTTPS listener | `X509Certificate[]` from Netty's `SslHandler` |
| **Bearer token** | Any | `Authorization: Bearer <token>` header value |
| **Basic auth** | Any | `Authorization: Basic <b64>` decoded credentials |
| **No auth** | HTTP listener | Remote IP address |

The resulting `KafkaPrincipal` is set on `RequestContext` and flows unchanged through
the existing `Authorizer` path in `KafkaApis` -- zero changes to authorization logic.

This task also updates `DefaultKafkaPrincipalBuilder.build()` to handle the new
`HttpAuthenticationContext` type.

---

## Specification

### Authentication Flow

```
HTTP request arrives
  |
  +-- Extract auth context from request:
  |     1. Check for client certificate (HTTPS + mTLS)
  |     2. Check for Authorization header (Bearer or Basic)
  |     3. Fall back to ANONYMOUS
  |
  +-- Build KafkaPrincipal:
  |     principalBuilder.build(HttpAuthenticationContext)
  |
  +-- Set on RequestContext:
        requestContext.principal = principal
```

### mTLS Authentication (HTTPS listener)

When the listener uses `HTTPS` security protocol and the client presents a certificate:
1. Extract `X509Certificate[]` from Netty's `SslHandler` via `engine().getSession().getPeerCertificates()`
2. Store in `HttpAuthenticationContext.peerCertificates()`
3. `DefaultKafkaPrincipalBuilder` extracts CN/SAN from the leaf certificate

If no client certificate is presented (one-way TLS), fall through to Bearer/Basic/ANONYMOUS.

### Bearer Token Authentication

When the request has `Authorization: Bearer <token>`:
1. Extract the token string
2. Store in `HttpAuthenticationContext.bearerToken()`
3. A custom `KafkaPrincipalBuilder` validates the token (JWT verification, OAuth introspection)
4. `DefaultKafkaPrincipalBuilder` does NOT handle Bearer tokens -- returns ANONYMOUS

### Basic Authentication

When the request has `Authorization: Basic <base64>`:
1. Base64-decode to get `username:password`
2. Store in `HttpAuthenticationContext.basicCredentials()`
3. A custom `KafkaPrincipalBuilder` validates against configured credentials
4. `DefaultKafkaPrincipalBuilder` does NOT handle Basic auth -- returns ANONYMOUS

### No Authentication

When no auth credentials are present:
1. `HttpAuthenticationContext` carries only the client IP address
2. `DefaultKafkaPrincipalBuilder` returns `KafkaPrincipal.ANONYMOUS`

---

## Implementation Details

### 1. HttpAuthenticationContext -- Full Implementation

The class implements `AuthenticationContext` and carries all possible auth data.

### 2. Authentication Extraction in HttpRequestHandler

The handler must extract auth context before building the `RequestContext`. The extraction
order matters: mTLS > Bearer > Basic > Anonymous.

### 3. DefaultKafkaPrincipalBuilder Update

Add a new branch in `build()` for `HttpAuthenticationContext`:
- If it has peer certificates, delegate to `applySslPrincipalMapper()` (same as SslAuthenticationContext)
- Otherwise, return `KafkaPrincipal.ANONYMOUS`

Custom builders handle Bearer and Basic auth.

---

## Skeleton Code

### HttpAuthenticationContext.java

```java
// http-server/src/main/java/kafka/server/http/HttpAuthenticationContext.java

package kafka.server.http;

import org.apache.kafka.common.security.auth.AuthenticationContext;
import org.apache.kafka.common.security.auth.SecurityProtocol;

import java.net.InetAddress;
import java.security.cert.X509Certificate;
import java.util.Optional;

/**
 * Authentication context for HTTP requests.
 *
 * Carries all possible authentication data extracted from the HTTP request:
 * - Client certificates (mTLS over HTTPS)
 * - Bearer token (Authorization: Bearer header)
 * - Basic credentials (Authorization: Basic header)
 * - Client IP address (always present)
 *
 * A configured {@link org.apache.kafka.common.security.auth.KafkaPrincipalBuilder}
 * inspects this context to determine the principal. The default builder handles
 * mTLS certificates and falls back to ANONYMOUS. Custom builders handle Bearer
 * and Basic auth.
 *
 * @see org.apache.kafka.common.security.auth.AuthenticationContext
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
        private SecurityProtocol securityProtocol = SecurityProtocol.HTTP;
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
```

### HttpRequestHandler.scala -- Authentication Extraction

```scala
// http-server/src/main/scala/kafka/network/HttpRequestHandler.scala

package kafka.network

import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.http.{FullHttpRequest, HttpHeaderNames}
import io.netty.handler.ssl.SslHandler
import kafka.server.http.HttpAuthenticationContext
import org.apache.kafka.common.security.auth.{KafkaPrincipal, KafkaPrincipalBuilder, SecurityProtocol}

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.security.cert.X509Certificate
import java.util.Base64

class HttpRequestHandler(
  requestChannel: RequestChannel,
  principalBuilder: KafkaPrincipalBuilder,
  securityProtocol: SecurityProtocol,
  config: KafkaConfig
) extends SimpleChannelInboundHandler[FullHttpRequest] {

  /**
   * Extract authentication context from the HTTP request and Netty channel.
   *
   * Priority order:
   * 1. Client certificates (mTLS) from SslHandler
   * 2. Bearer token from Authorization header
   * 3. Basic auth from Authorization header
   * 4. Anonymous (no credentials)
   */
  private def extractAuthContext(
    ctx: ChannelHandlerContext,
    req: FullHttpRequest
  ): HttpAuthenticationContext = {
    val remoteAddr = ctx.channel().remoteAddress().asInstanceOf[InetSocketAddress]
    val builder = new HttpAuthenticationContext.Builder()
      .clientAddress(remoteAddr.getAddress)
      .securityProtocol(securityProtocol)

    // 1. Try mTLS (client certificate)
    val sslHandler = ctx.pipeline().get(classOf[SslHandler])
    if (sslHandler != null) {
      try {
        val session = sslHandler.engine().getSession
        val peerCerts = session.getPeerCertificates
        if (peerCerts != null && peerCerts.nonEmpty) {
          val x509Certs = peerCerts.collect { case c: X509Certificate => c }
          if (x509Certs.nonEmpty) {
            builder.peerCertificates(x509Certs)
            return builder.build()
          }
        }
      } catch {
        case _: javax.net.ssl.SSLPeerUnverifiedException =>
          // No client certificate -- fall through
      }
    }

    // 2. Try Authorization header
    val authHeader = req.headers().get(HttpHeaderNames.AUTHORIZATION)
    if (authHeader != null) {
      if (authHeader.regionMatches(true, 0, "Bearer ", 0, 7)) {
        // Bearer token
        val token = authHeader.substring(7).trim
        if (token.nonEmpty) {
          builder.bearerToken(token)
          return builder.build()
        }
      } else if (authHeader.regionMatches(true, 0, "Basic ", 0, 6)) {
        // Basic auth
        val encoded = authHeader.substring(6).trim
        try {
          val decoded = new String(Base64.getDecoder.decode(encoded), StandardCharsets.UTF_8)
          val colonIndex = decoded.indexOf(':')
          if (colonIndex > 0) {
            val username = decoded.substring(0, colonIndex)
            val password = decoded.substring(colonIndex + 1)
            builder.basicCredentials(username, password)
            return builder.build()
          }
        } catch {
          case _: IllegalArgumentException =>
            // Invalid base64 -- fall through to anonymous
        }
      }
    }

    // 3. Anonymous
    builder.build()
  }

  /**
   * Build KafkaPrincipal from the authentication context.
   */
  private def buildPrincipal(ctx: ChannelHandlerContext, req: FullHttpRequest): KafkaPrincipal = {
    val authContext = extractAuthContext(ctx, req)
    principalBuilder.build(authContext)
  }

  // In channelRead0():
  // val principal = buildPrincipal(ctx, req)
  // val requestContext = new RequestContext(
  //   ...,
  //   principal = principal,
  //   ...
  // )
}
```

### DefaultKafkaPrincipalBuilder.java -- Update

```java
// clients/src/main/java/org/apache/kafka/common/security/authenticator/DefaultKafkaPrincipalBuilder.java

// Add to the build() method:

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
        // ... existing SASL handling ...
    } else if (context instanceof HttpAuthenticationContext httpContext) {
        // Handle HTTP authentication context
        // mTLS: extract principal from client certificate
        if (httpContext.peerCertificates().isPresent()) {
            X509Certificate[] certs = httpContext.peerCertificates().get();
            if (certs.length > 0) {
                Principal leafPrincipal = certs[0].getSubjectX500Principal();
                try {
                    return applySslPrincipalMapper(leafPrincipal);
                } catch (IOException e) {
                    // Fall through to ANONYMOUS
                }
            }
        }
        // Bearer and Basic auth require custom KafkaPrincipalBuilder implementations.
        // DefaultKafkaPrincipalBuilder returns ANONYMOUS for these.
        return KafkaPrincipal.ANONYMOUS;
    } else {
        // Unknown context type -- return ANONYMOUS
        return KafkaPrincipal.ANONYMOUS;
    }
}
```

---

## Tests

### Unit Tests -- HttpAuthenticationContext

```java
// http-server/src/test/java/kafka/server/http/HttpAuthenticationContextTest.java

package kafka.server.http;

import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;

import static org.junit.jupiter.api.Assertions.*;

class HttpAuthenticationContextTest {

    @Test
    void testAnonymousContext() throws Exception {
        HttpAuthenticationContext ctx = new HttpAuthenticationContext.Builder()
            .clientAddress(InetAddress.getLoopbackAddress())
            .securityProtocol(SecurityProtocol.HTTP)
            .build();

        assertFalse(ctx.hasCredentials());
        assertTrue(ctx.peerCertificates().isEmpty());
        assertTrue(ctx.bearerToken().isEmpty());
        assertTrue(ctx.basicUsername().isEmpty());
        assertEquals(SecurityProtocol.HTTP, ctx.securityProtocol());
    }

    @Test
    void testBearerTokenContext() throws Exception {
        HttpAuthenticationContext ctx = new HttpAuthenticationContext.Builder()
            .clientAddress(InetAddress.getLoopbackAddress())
            .securityProtocol(SecurityProtocol.HTTPS)
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
            .securityProtocol(SecurityProtocol.HTTPS)
            .basicCredentials("admin", "secret")
            .build();

        assertTrue(ctx.hasCredentials());
        assertTrue(ctx.basicUsername().isPresent());
        assertEquals("admin", ctx.basicUsername().get());
        assertTrue(ctx.basicPassword().isPresent());
        assertEquals("secret", ctx.basicPassword().get());
    }

    @Test
    void testMtlsContext() throws Exception {
        // Use a self-signed test certificate
        java.security.cert.X509Certificate[] certs = createTestCertificates();

        HttpAuthenticationContext ctx = new HttpAuthenticationContext.Builder()
            .clientAddress(InetAddress.getLoopbackAddress())
            .securityProtocol(SecurityProtocol.HTTPS)
            .peerCertificates(certs)
            .build();

        assertTrue(ctx.hasCredentials());
        assertTrue(ctx.peerCertificates().isPresent());
        assertEquals(1, ctx.peerCertificates().get().length);
    }

    @Test
    void testBuilderRequiresClientAddress() {
        assertThrows(IllegalArgumentException.class, () -> {
            new HttpAuthenticationContext.Builder()
                .securityProtocol(SecurityProtocol.HTTP)
                .build();
        });
    }

    @Test
    void testDefaultSecurityProtocol() throws Exception {
        HttpAuthenticationContext ctx = new HttpAuthenticationContext.Builder()
            .clientAddress(InetAddress.getLoopbackAddress())
            .build();

        assertEquals(SecurityProtocol.HTTP, ctx.securityProtocol());
    }

    private java.security.cert.X509Certificate[] createTestCertificates() {
        // Generate a self-signed certificate for testing
        // This would use java.security.KeyPairGenerator and sun.security.x509
        // or a test utility like BouncyCastle
        return new java.security.cert.X509Certificate[0]; // placeholder
    }
}
```

### Unit Tests -- Auth Extraction

```scala
// http-server/src/test/scala/kafka/network/HttpAuthExtractionTest.scala

package kafka.network

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions._

class HttpAuthExtractionTest {

  @Test
  def testParseBearerToken(): Unit = {
    val header = "Bearer eyJhbGciOiJSUzI1NiJ9.test.sig"
    assertTrue(header.regionMatches(true, 0, "Bearer ", 0, 7))
    val token = header.substring(7).trim
    assertEquals("eyJhbGciOiJSUzI1NiJ9.test.sig", token)
  }

  @Test
  def testParseBearerTokenCaseInsensitive(): Unit = {
    val header = "bearer my-token"
    assertTrue(header.regionMatches(true, 0, "Bearer ", 0, 7))
    val token = header.substring(7).trim
    assertEquals("my-token", token)
  }

  @Test
  def testParseBasicAuth(): Unit = {
    val header = "Basic " + java.util.Base64.getEncoder.encodeToString("user:pass".getBytes)
    assertTrue(header.regionMatches(true, 0, "Basic ", 0, 6))
    val encoded = header.substring(6).trim
    val decoded = new String(java.util.Base64.getDecoder.decode(encoded))
    assertEquals("user:pass", decoded)
    val colonIdx = decoded.indexOf(':')
    assertEquals("user", decoded.substring(0, colonIdx))
    assertEquals("pass", decoded.substring(colonIdx + 1))
  }

  @Test
  def testParseBasicAuthWithColonInPassword(): Unit = {
    val header = "Basic " + java.util.Base64.getEncoder.encodeToString("user:p:a:ss".getBytes)
    val encoded = header.substring(6).trim
    val decoded = new String(java.util.Base64.getDecoder.decode(encoded))
    val colonIdx = decoded.indexOf(':')
    assertEquals("user", decoded.substring(0, colonIdx))
    assertEquals("p:a:ss", decoded.substring(colonIdx + 1))
  }

  @Test
  def testParseBasicAuthInvalidBase64(): Unit = {
    val header = "Basic not-valid-base64!!!"
    val encoded = header.substring(6).trim
    assertThrows(classOf[IllegalArgumentException], () => {
      java.util.Base64.getDecoder.decode(encoded)
    })
  }

  @Test
  def testNoAuthHeaderReturnsAnonymous(): Unit = {
    // When no Authorization header is present, the context should have no credentials
    val ctx = new kafka.server.http.HttpAuthenticationContext.Builder()
      .clientAddress(java.net.InetAddress.getLoopbackAddress)
      .build()
    assertFalse(ctx.hasCredentials())
  }
}
```

### DefaultKafkaPrincipalBuilder Test

```java
// clients/src/test/java/org/apache/kafka/common/security/auth/DefaultKafkaPrincipalBuilderHttpTest.java

package org.apache.kafka.common.security.auth;

import kafka.server.http.HttpAuthenticationContext;
import org.apache.kafka.common.security.authenticator.DefaultKafkaPrincipalBuilder;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;

import static org.junit.jupiter.api.Assertions.*;

class DefaultKafkaPrincipalBuilderHttpTest {

    @Test
    void testAnonymousHttpContext() throws Exception {
        DefaultKafkaPrincipalBuilder builder = new DefaultKafkaPrincipalBuilder(null, null);
        HttpAuthenticationContext ctx = new HttpAuthenticationContext.Builder()
            .clientAddress(InetAddress.getLoopbackAddress())
            .securityProtocol(SecurityProtocol.HTTP)
            .build();

        KafkaPrincipal principal = builder.build(ctx);
        assertEquals(KafkaPrincipal.ANONYMOUS, principal);
    }

    @Test
    void testBearerTokenReturnAnonymousFromDefaultBuilder() throws Exception {
        DefaultKafkaPrincipalBuilder builder = new DefaultKafkaPrincipalBuilder(null, null);
        HttpAuthenticationContext ctx = new HttpAuthenticationContext.Builder()
            .clientAddress(InetAddress.getLoopbackAddress())
            .securityProtocol(SecurityProtocol.HTTPS)
            .bearerToken("some-token")
            .build();

        // Default builder does NOT handle Bearer tokens
        KafkaPrincipal principal = builder.build(ctx);
        assertEquals(KafkaPrincipal.ANONYMOUS, principal);
    }

    @Test
    void testBasicAuthReturnAnonymousFromDefaultBuilder() throws Exception {
        DefaultKafkaPrincipalBuilder builder = new DefaultKafkaPrincipalBuilder(null, null);
        HttpAuthenticationContext ctx = new HttpAuthenticationContext.Builder()
            .clientAddress(InetAddress.getLoopbackAddress())
            .securityProtocol(SecurityProtocol.HTTPS)
            .basicCredentials("admin", "secret")
            .build();

        // Default builder does NOT handle Basic auth
        KafkaPrincipal principal = builder.build(ctx);
        assertEquals(KafkaPrincipal.ANONYMOUS, principal);
    }
}
```

---

## Rules

- Authentication extraction priority: mTLS > Bearer > Basic > Anonymous. Once a method is found, stop checking lower-priority methods.
- Do NOT validate Bearer tokens or Basic credentials in the HTTP handler. That is the `KafkaPrincipalBuilder`'s responsibility.
- `DefaultKafkaPrincipalBuilder` handles only mTLS certificates (via `applySslPrincipalMapper`). All other HTTP auth methods return ANONYMOUS from the default builder.
- The `HttpAuthenticationContext` class must be in the `http-server` module but must be visible to `clients` module for `DefaultKafkaPrincipalBuilder`. If a circular dependency arises, define an interface in `clients` and implement in `http-server`.
- Base64 decoding of Basic auth must handle passwords containing colons (split on first colon only).
- Bearer token parsing is case-insensitive for the "Bearer" prefix.
- If the Authorization header is malformed (invalid base64, no colon in decoded Basic), fall through to ANONYMOUS rather than returning an error.
- The `SslHandler` may not be present in the pipeline for HTTP (non-HTTPS) listeners. Always null-check.

---

## Learning

_To be filled by the executing agent._

## Limitations

_To be filled by the executing agent._

## Field Notes

_To be filled by the executing agent._

---

## Acceptance Criteria

- [ ] `HttpAuthenticationContext` carries all four auth types (mTLS, Bearer, Basic, Anonymous)
- [ ] `HttpRequestHandler` extracts auth context with correct priority order
- [ ] mTLS: client certificate from `SslHandler` is passed to `KafkaPrincipalBuilder`
- [ ] Bearer: token from `Authorization: Bearer <token>` header is extracted
- [ ] Basic: username/password from `Authorization: Basic <b64>` header is decoded
- [ ] Anonymous: no credentials present, `ANONYMOUS` principal is returned
- [ ] `DefaultKafkaPrincipalBuilder` handles `HttpAuthenticationContext` for mTLS
- [ ] `DefaultKafkaPrincipalBuilder` returns `ANONYMOUS` for Bearer/Basic (custom builders handle these)
- [ ] Password with colons is handled correctly in Basic auth
- [ ] Invalid base64 in Basic auth falls through to ANONYMOUS
- [ ] All unit tests pass
- [ ] No circular dependency between `http-server` and `clients` modules

---

## File Manifest

| File | Status |
|------|--------|
| `http-server/src/main/java/kafka/server/http/HttpAuthenticationContext.java` | |
| `http-server/src/main/scala/kafka/network/HttpRequestHandler.scala` | |
| `clients/src/main/java/org/apache/kafka/common/security/authenticator/DefaultKafkaPrincipalBuilder.java` | |
| `http-server/src/test/java/kafka/server/http/HttpAuthenticationContextTest.java` | |
| `http-server/src/test/scala/kafka/network/HttpAuthExtractionTest.scala` | |
| `clients/src/test/java/org/apache/kafka/common/security/auth/DefaultKafkaPrincipalBuilderHttpTest.java` | |
