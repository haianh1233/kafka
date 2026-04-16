# TASK-F.04: CORS Support

## Prerequisites

| Task | What it provides |
|------|-----------------|
| TASK-B.03 | `HttpChannelInitializer` -- Netty pipeline where the CORS handler is inserted |

TASK-B.03 must be merged and passing CI before this task begins.

---

## Context

The HTTP protocol design (section 10, config table) specifies CORS support via the
configuration property `http.cors.allowed.origins`. When non-empty, a CORS handler is
added to the Netty pipeline to allow browser-based JavaScript clients to make cross-origin
requests to the Kafka HTTP API.

Netty provides a built-in `CorsHandler` with `CorsConfigBuilder` that handles all CORS
negotiation (preflight OPTIONS requests, Access-Control-Allow-Origin headers, etc.). This
task wires the Netty CORS handler into the `HttpChannelInitializer` pipeline when the
config is set.

---

## Specification

### Configuration

| Property | Default | Description |
|---|---|---|
| `http.cors.allowed.origins` | `""` (disabled) | Comma-separated CORS allowed origins. `*` for all. Empty string disables CORS. |

### Behavior

When `http.cors.allowed.origins` is non-empty:
- Add `CorsHandler` to the Netty pipeline after `HttpServerCodec` and before `HttpObjectAggregator`
- Handle preflight `OPTIONS` requests automatically (Netty's `CorsHandler` does this)
- Set `Access-Control-Allow-Origin` on all responses matching the configured origins
- Allow the following HTTP methods: GET, POST, OPTIONS
- Allow the following request headers: `Content-Type`, `Authorization`, `X-Kafka-Client-ID`, `X-Request-ID`
- Expose the following response headers: `X-Kafka-Request-ID`, `X-Kafka-MaxWait-Applied`, `Retry-After`
- Set `Access-Control-Max-Age` to 3600 seconds (1 hour) for preflight caching

When `http.cors.allowed.origins` is empty (default):
- No CORS handler in the pipeline
- No CORS-related response headers
- Browser cross-origin requests will be blocked by the browser's same-origin policy

### Security Note

Setting `http.cors.allowed.origins=*` allows any website to make requests to the Kafka
broker on behalf of authenticated users. This is a security risk if the HTTP listener uses
cookie-based or ambient authentication. With Bearer token auth (the recommended approach),
wildcard origins are generally safe because the browser cannot automatically attach tokens.

---

## Implementation Details

### 1. Parse CORS Config

In `KafkaConfig` (or the HTTP config handler), parse `http.cors.allowed.origins`:

```scala
val corsAllowedOrigins: Seq[String] = {
  val raw = config.getString("http.cors.allowed.origins")
  if (raw == null || raw.trim.isEmpty) Seq.empty
  else raw.split(",").map(_.trim).filter(_.nonEmpty).toSeq
}
```

### 2. Build CorsConfig

Use Netty's `CorsConfigBuilder` to build the CORS configuration:

- If origins contains `"*"`: use `CorsConfigBuilder.forAnyOrigin()`
- Otherwise: use `CorsConfigBuilder.forOrigins(origins: _*)`
- Add allowed methods, headers, and exposed headers
- Set max age for preflight caching

### 3. Add to Pipeline

Insert `CorsHandler` in the Netty pipeline at the correct position:
- After `HttpServerCodec` (needs HTTP message objects)
- Before `HttpObjectAggregator` (CORS preflight should be handled before aggregation)

---

## Skeleton Code

### HttpChannelInitializer.scala -- CORS addition

```scala
// http-server/src/main/scala/kafka/network/HttpChannelInitializer.scala

package kafka.network

import io.netty.channel.ChannelInitializer
import io.netty.channel.socket.SocketChannel
import io.netty.handler.codec.http._
import io.netty.handler.codec.http.cors.{CorsConfig, CorsConfigBuilder, CorsHandler}
import io.netty.handler.ssl.SslContext
import io.netty.handler.stream.ChunkedWriteHandler
import io.netty.handler.timeout.IdleStateHandler
import kafka.server.KafkaConfig
import kafka.server.http.HttpMetrics

import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters._

class HttpChannelInitializer(
  config: KafkaConfig,
  requestChannel: RequestChannel,
  sslContext: Option[SslContext],
  httpMetrics: HttpMetrics
) extends ChannelInitializer[SocketChannel] {

  private val corsConfig: Option[CorsConfig] = buildCorsConfig()

  override def initChannel(ch: SocketChannel): Unit = {
    val pipeline = ch.pipeline()

    // Optional TLS (for HTTPS)
    sslContext.foreach { ssl =>
      pipeline.addLast("ssl", ssl.newHandler(ch.alloc()))
    }

    // HTTP/1.1 codec
    pipeline.addLast("http-codec", new HttpServerCodec())

    // CORS handler (if configured) -- must be after codec, before aggregator
    corsConfig.foreach { cc =>
      pipeline.addLast("cors", new CorsHandler(cc))
    }

    // Aggregate chunked requests into FullHttpRequest
    pipeline.addLast("http-aggregator",
      new HttpObjectAggregator(config.httpRequestMaxBytes))

    // Compression support
    pipeline.addLast("compressor", new HttpContentCompressor())

    // Idle connection timeout
    pipeline.addLast("idle-handler", new IdleStateHandler(
      0, 0, config.httpConnectionIdleTimeoutMs, TimeUnit.MILLISECONDS))
    pipeline.addLast("idle-closer",
      new IdleStateCloseHandler(httpMetrics))

    // Kafka HTTP handler
    pipeline.addLast("kafka-handler", new HttpRequestHandler(
      requestChannel, config))
  }

  /**
   * Build Netty CorsConfig from Kafka configuration.
   * Returns None if CORS is not configured (empty origins).
   */
  private def buildCorsConfig(): Option[CorsConfig] = {
    val origins = parseCorsOrigins(config)
    if (origins.isEmpty) return None

    val builder = if (origins.contains("*")) {
      CorsConfigBuilder.forAnyOrigin()
    } else {
      CorsConfigBuilder.forOrigins(origins: _*)
    }

    Some(
      builder
        .allowedRequestMethods(
          io.netty.handler.codec.http.HttpMethod.GET,
          io.netty.handler.codec.http.HttpMethod.POST,
          io.netty.handler.codec.http.HttpMethod.OPTIONS
        )
        .allowedRequestHeaders(
          "Content-Type",
          "Authorization",
          "X-Kafka-Client-ID",
          "X-Request-ID"
        )
        .exposeHeaders(
          "X-Kafka-Request-ID",
          "X-Kafka-MaxWait-Applied",
          "Retry-After"
        )
        .maxAge(3600)  // 1 hour preflight cache
        .allowNullOrigin()  // some browsers send "null" origin for file:// or data: URIs
        .build()
    )
  }

  /**
   * Parse comma-separated CORS origins from config.
   */
  private def parseCorsOrigins(config: KafkaConfig): Array[String] = {
    val raw = config.getString("http.cors.allowed.origins")
    if (raw == null || raw.trim.isEmpty) Array.empty
    else raw.split(",").map(_.trim).filter(_.nonEmpty)
  }
}
```

### KafkaConfig addition

```scala
// core/src/main/scala/kafka/server/KafkaConfig.scala
// Add to HTTP config section:

val HttpCorsAllowedOriginsProp = "http.cors.allowed.origins"
val HttpCorsAllowedOriginsDoc = "Comma-separated list of allowed CORS origins. " +
  "Use '*' to allow all origins. Empty string (default) disables CORS."

// In config defs:
.define(HttpCorsAllowedOriginsProp, STRING, "", MEDIUM, HttpCorsAllowedOriginsDoc)
```

---

## Tests

### Unit Tests

```scala
// http-server/src/test/scala/kafka/network/HttpCorsTest.scala

package kafka.network

import io.netty.handler.codec.http.cors.{CorsConfig, CorsConfigBuilder}
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions._

class HttpCorsTest {

  @Test
  def testCorsDisabledByDefault(): Unit = {
    val origins = parseCorsOrigins("")
    assertTrue(origins.isEmpty)
  }

  @Test
  def testCorsSingleOrigin(): Unit = {
    val origins = parseCorsOrigins("https://example.com")
    assertEquals(1, origins.length)
    assertEquals("https://example.com", origins(0))
  }

  @Test
  def testCorsMultipleOrigins(): Unit = {
    val origins = parseCorsOrigins("https://example.com, https://other.com")
    assertEquals(2, origins.length)
    assertEquals("https://example.com", origins(0))
    assertEquals("https://other.com", origins(1))
  }

  @Test
  def testCorsWildcard(): Unit = {
    val origins = parseCorsOrigins("*")
    assertEquals(1, origins.length)
    assertEquals("*", origins(0))
  }

  @Test
  def testCorsTrimsWhitespace(): Unit = {
    val origins = parseCorsOrigins("  https://a.com ,  https://b.com  ")
    assertEquals(2, origins.length)
    assertEquals("https://a.com", origins(0))
    assertEquals("https://b.com", origins(1))
  }

  @Test
  def testCorsIgnoresEmptyEntries(): Unit = {
    val origins = parseCorsOrigins("https://a.com,,https://b.com,")
    assertEquals(2, origins.length)
  }

  @Test
  def testCorsConfigBuilderForAnyOrigin(): Unit = {
    val config = CorsConfigBuilder.forAnyOrigin()
      .allowedRequestMethods(io.netty.handler.codec.http.HttpMethod.GET)
      .maxAge(3600)
      .build()
    assertTrue(config.isAnyOriginSupported)
  }

  @Test
  def testCorsConfigBuilderForSpecificOrigins(): Unit = {
    val config = CorsConfigBuilder.forOrigins("https://example.com")
      .allowedRequestMethods(io.netty.handler.codec.http.HttpMethod.GET)
      .maxAge(3600)
      .build()
    assertFalse(config.isAnyOriginSupported)
    assertTrue(config.origins().contains("https://example.com"))
  }

  private def parseCorsOrigins(raw: String): Array[String] = {
    if (raw == null || raw.trim.isEmpty) Array.empty
    else raw.split(",").map(_.trim).filter(_.nonEmpty)
  }
}
```

### Integration Tests

```scala
// http-server/src/test/scala/kafka/network/HttpCorsIntegrationTest.scala

package kafka.network

import org.junit.jupiter.api.{AfterAll, BeforeAll, Test, TestInstance}
import org.junit.jupiter.api.Assertions._
import kafka.server.http.HttpIntegrationTestHarness

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class HttpCorsIntegrationTest extends HttpIntegrationTestHarness {

  private val httpClient = HttpClient.newHttpClient()

  override def corsAllowedOrigins: String = "https://example.com,https://other.com"

  @BeforeAll
  def setUp(): Unit = {
    startCluster()
  }

  @AfterAll
  def tearDown(): Unit = {
    stopCluster()
  }

  @Test
  def testPreflightRequest(): Unit = {
    val request = HttpRequest.newBuilder()
      .uri(URI.create(s"$httpBaseUrl/v1/health"))
      .method("OPTIONS", HttpRequest.BodyPublishers.noBody())
      .header("Origin", "https://example.com")
      .header("Access-Control-Request-Method", "GET")
      .build()
    val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
    assertEquals(200, response.statusCode())
    assertTrue(response.headers().firstValue("access-control-allow-origin").isPresent)
    assertEquals("https://example.com",
      response.headers().firstValue("access-control-allow-origin").get())
  }

  @Test
  def testCorsHeaderOnNormalRequest(): Unit = {
    val request = HttpRequest.newBuilder()
      .uri(URI.create(s"$httpBaseUrl/v1/health"))
      .header("Origin", "https://example.com")
      .GET().build()
    val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
    assertEquals(200, response.statusCode())
    assertTrue(response.headers().firstValue("access-control-allow-origin").isPresent)
  }

  @Test
  def testCorsRejectedForUnknownOrigin(): Unit = {
    val request = HttpRequest.newBuilder()
      .uri(URI.create(s"$httpBaseUrl/v1/health"))
      .header("Origin", "https://evil.com")
      .GET().build()
    val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
    // The request still succeeds (CORS is enforced browser-side), but the
    // Access-Control-Allow-Origin header should NOT be present for unknown origins
    assertFalse(
      response.headers().firstValue("access-control-allow-origin")
        .map(_.contains("evil.com")).orElse(false),
      "CORS header should not include unknown origin")
  }

  @Test
  def testPreflightAllowedMethods(): Unit = {
    val request = HttpRequest.newBuilder()
      .uri(URI.create(s"$httpBaseUrl/v1/health"))
      .method("OPTIONS", HttpRequest.BodyPublishers.noBody())
      .header("Origin", "https://example.com")
      .header("Access-Control-Request-Method", "POST")
      .build()
    val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
    assertEquals(200, response.statusCode())
    val allowedMethods = response.headers().firstValue("access-control-allow-methods").orElse("")
    assertTrue(allowedMethods.contains("GET") || allowedMethods.contains("POST"),
      "Should allow GET and POST methods")
  }

  @Test
  def testPreflightExposedHeaders(): Unit = {
    val request = HttpRequest.newBuilder()
      .uri(URI.create(s"$httpBaseUrl/v1/health"))
      .method("OPTIONS", HttpRequest.BodyPublishers.noBody())
      .header("Origin", "https://example.com")
      .header("Access-Control-Request-Method", "GET")
      .build()
    val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
    val exposedHeaders = response.headers().firstValue("access-control-expose-headers").orElse("")
    assertTrue(exposedHeaders.contains("X-Kafka-Request-ID") ||
      exposedHeaders.contains("Retry-After"),
      "Should expose Kafka-specific response headers")
  }

  @Test
  def testPreflightMaxAge(): Unit = {
    val request = HttpRequest.newBuilder()
      .uri(URI.create(s"$httpBaseUrl/v1/health"))
      .method("OPTIONS", HttpRequest.BodyPublishers.noBody())
      .header("Origin", "https://example.com")
      .header("Access-Control-Request-Method", "GET")
      .build()
    val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
    val maxAge = response.headers().firstValue("access-control-max-age").orElse("0")
    assertEquals("3600", maxAge)
  }
}
```

### Disabled CORS Test

```scala
// http-server/src/test/scala/kafka/network/HttpCorsDisabledIntegrationTest.scala

package kafka.network

import org.junit.jupiter.api.{AfterAll, BeforeAll, Test, TestInstance}
import org.junit.jupiter.api.Assertions._
import kafka.server.http.HttpIntegrationTestHarness

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class HttpCorsDisabledIntegrationTest extends HttpIntegrationTestHarness {

  private val httpClient = HttpClient.newHttpClient()

  override def corsAllowedOrigins: String = ""  // CORS disabled (default)

  @BeforeAll
  def setUp(): Unit = {
    startCluster()
  }

  @AfterAll
  def tearDown(): Unit = {
    stopCluster()
  }

  @Test
  def testNoCorsHeadersWhenDisabled(): Unit = {
    val request = HttpRequest.newBuilder()
      .uri(URI.create(s"$httpBaseUrl/v1/health"))
      .header("Origin", "https://example.com")
      .GET().build()
    val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
    assertEquals(200, response.statusCode())
    assertFalse(response.headers().firstValue("access-control-allow-origin").isPresent,
      "No CORS headers when CORS is disabled")
  }
}
```

---

## Rules

- CORS must be opt-in: disabled by default with empty `http.cors.allowed.origins`.
- Use Netty's built-in `CorsHandler` and `CorsConfigBuilder` -- do NOT implement CORS manually.
- The `CorsHandler` must be placed in the pipeline AFTER `HttpServerCodec` and BEFORE `HttpObjectAggregator`.
- Allow only GET, POST, OPTIONS methods (not PUT, DELETE, PATCH -- not used by the API).
- Expose Kafka-specific response headers so JavaScript clients can read them.
- Set `Access-Control-Max-Age: 3600` to reduce preflight request frequency.
- Document the security implications of wildcard origins in config documentation.

---

## Learning

_To be filled by the executing agent._

## Limitations

_To be filled by the executing agent._

## Field Notes

_To be filled by the executing agent._

---

## Acceptance Criteria

- [ ] CORS is disabled by default (no CORS headers when `http.cors.allowed.origins` is empty)
- [ ] CORS preflight (OPTIONS) returns correct headers for configured origins
- [ ] `Access-Control-Allow-Origin` is set on normal responses for allowed origins
- [ ] Unknown origins do not get `Access-Control-Allow-Origin` header
- [ ] Allowed methods include GET, POST, OPTIONS
- [ ] Allowed request headers include Content-Type, Authorization, X-Kafka-Client-ID, X-Request-ID
- [ ] Exposed response headers include X-Kafka-Request-ID, X-Kafka-MaxWait-Applied, Retry-After
- [ ] `Access-Control-Max-Age` is 3600
- [ ] Wildcard `*` origin works
- [ ] Multiple specific origins work
- [ ] All unit tests pass
- [ ] Integration tests pass with CORS enabled and disabled

---

## File Manifest

| File | Status |
|------|--------|
| `http-server/src/main/scala/kafka/network/HttpChannelInitializer.scala` | |
| `core/src/main/scala/kafka/server/KafkaConfig.scala` | |
| `http-server/src/test/scala/kafka/network/HttpCorsTest.scala` | |
| `http-server/src/test/scala/kafka/network/HttpCorsIntegrationTest.scala` | |
| `http-server/src/test/scala/kafka/network/HttpCorsDisabledIntegrationTest.scala` | |
