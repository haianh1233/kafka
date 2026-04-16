/**
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

// Time: Created - TASK-F.04
package kafka.network

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions._

/**
 * Unit tests for CORS origin parsing and CorsConfig building logic
 * in [[HttpChannelInitializer]].
 *
 * Tests cover:
 *   - Parsing comma-separated origins
 *   - Handling of whitespace, empty entries, null input
 *   - Wildcard (*) origin support
 *   - CorsConfig construction for any-origin vs specific-origin modes
 *   - CORS disabled by default (empty string)
 */
class HttpCorsTest {

  // --- parseCorsOrigins tests ---

  @Test
  def testCorsDisabledByDefault(): Unit = {
    val origins = HttpChannelInitializer.parseCorsOrigins("")
    assertTrue(origins.isEmpty, "Empty string should produce no origins (CORS disabled)")
  }

  @Test
  def testCorsDisabledForNull(): Unit = {
    val origins = HttpChannelInitializer.parseCorsOrigins(null)
    assertTrue(origins.isEmpty, "Null input should produce no origins (CORS disabled)")
  }

  @Test
  def testCorsDisabledForWhitespaceOnly(): Unit = {
    val origins = HttpChannelInitializer.parseCorsOrigins("   ")
    assertTrue(origins.isEmpty, "Whitespace-only input should produce no origins")
  }

  @Test
  def testCorsSingleOrigin(): Unit = {
    val origins = HttpChannelInitializer.parseCorsOrigins("https://example.com")
    assertEquals(1, origins.length)
    assertEquals("https://example.com", origins(0))
  }

  @Test
  def testCorsMultipleOrigins(): Unit = {
    val origins = HttpChannelInitializer.parseCorsOrigins("https://example.com, https://other.com")
    assertEquals(2, origins.length)
    assertEquals("https://example.com", origins(0))
    assertEquals("https://other.com", origins(1))
  }

  @Test
  def testCorsWildcard(): Unit = {
    val origins = HttpChannelInitializer.parseCorsOrigins("*")
    assertEquals(1, origins.length)
    assertEquals("*", origins(0))
  }

  @Test
  def testCorsTrimsWhitespace(): Unit = {
    val origins = HttpChannelInitializer.parseCorsOrigins("  https://a.com ,  https://b.com  ")
    assertEquals(2, origins.length)
    assertEquals("https://a.com", origins(0))
    assertEquals("https://b.com", origins(1))
  }

  @Test
  def testCorsIgnoresEmptyEntries(): Unit = {
    val origins = HttpChannelInitializer.parseCorsOrigins("https://a.com,,https://b.com,")
    assertEquals(2, origins.length)
    assertEquals("https://a.com", origins(0))
    assertEquals("https://b.com", origins(1))
  }

  // --- buildCorsConfig tests ---

  @Test
  def testBuildCorsConfigReturnsNoneWhenDisabled(): Unit = {
    assertTrue(HttpChannelInitializer.buildCorsConfig("").isEmpty,
      "Empty origins should return None (CORS disabled)")
    assertTrue(HttpChannelInitializer.buildCorsConfig(null).isEmpty,
      "Null origins should return None (CORS disabled)")
    assertTrue(HttpChannelInitializer.buildCorsConfig("   ").isEmpty,
      "Whitespace origins should return None (CORS disabled)")
  }

  @Test
  def testBuildCorsConfigForAnyOrigin(): Unit = {
    val configOpt = HttpChannelInitializer.buildCorsConfig("*")
    assertTrue(configOpt.isDefined, "Wildcard should produce a CorsConfig")
    val config = configOpt.get
    assertTrue(config.isAnyOriginSupported, "Wildcard should enable any-origin mode")
  }

  @Test
  def testBuildCorsConfigForSpecificOrigins(): Unit = {
    val configOpt = HttpChannelInitializer.buildCorsConfig("https://example.com")
    assertTrue(configOpt.isDefined, "Specific origin should produce a CorsConfig")
    val config = configOpt.get
    assertFalse(config.isAnyOriginSupported, "Specific origin should not enable any-origin mode")
    assertTrue(config.origins().contains("https://example.com"),
      "CorsConfig should contain the configured origin")
  }

  @Test
  def testBuildCorsConfigForMultipleOrigins(): Unit = {
    val configOpt = HttpChannelInitializer.buildCorsConfig("https://a.com,https://b.com")
    assertTrue(configOpt.isDefined, "Multiple origins should produce a CorsConfig")
    val config = configOpt.get
    assertFalse(config.isAnyOriginSupported)
    assertTrue(config.origins().contains("https://a.com"))
    assertTrue(config.origins().contains("https://b.com"))
  }

  @Test
  def testBuildCorsConfigMaxAge(): Unit = {
    val configOpt = HttpChannelInitializer.buildCorsConfig("https://example.com")
    assertTrue(configOpt.isDefined)
    val config = configOpt.get
    assertEquals(3600, config.maxAge(), "Max age should be 3600 seconds")
  }

  @Test
  def testBuildCorsConfigAllowedMethods(): Unit = {
    val configOpt = HttpChannelInitializer.buildCorsConfig("https://example.com")
    assertTrue(configOpt.isDefined)
    val config = configOpt.get
    val methods = config.allowedRequestMethods()
    assertTrue(methods.contains(io.netty.handler.codec.http.HttpMethod.GET),
      "Should allow GET")
    assertTrue(methods.contains(io.netty.handler.codec.http.HttpMethod.POST),
      "Should allow POST")
    assertTrue(methods.contains(io.netty.handler.codec.http.HttpMethod.OPTIONS),
      "Should allow OPTIONS")
  }

  @Test
  def testBuildCorsConfigAllowedHeaders(): Unit = {
    val configOpt = HttpChannelInitializer.buildCorsConfig("https://example.com")
    assertTrue(configOpt.isDefined)
    val config = configOpt.get
    val headers = config.allowedRequestHeaders()
    assertTrue(headers.contains("Content-Type"), "Should allow Content-Type header")
    assertTrue(headers.contains("Authorization"), "Should allow Authorization header")
    assertTrue(headers.contains("X-Kafka-Client-ID"), "Should allow X-Kafka-Client-ID header")
    assertTrue(headers.contains("X-Request-ID"), "Should allow X-Request-ID header")
  }

  @Test
  def testBuildCorsConfigExposedHeaders(): Unit = {
    val configOpt = HttpChannelInitializer.buildCorsConfig("https://example.com")
    assertTrue(configOpt.isDefined)
    val config = configOpt.get
    val exposed = config.exposedHeaders()
    assertTrue(exposed.contains("X-Kafka-Request-ID"),
      "Should expose X-Kafka-Request-ID header")
    assertTrue(exposed.contains("X-Kafka-MaxWait-Applied"),
      "Should expose X-Kafka-MaxWait-Applied header")
    assertTrue(exposed.contains("Retry-After"),
      "Should expose Retry-After header")
  }

  @Test
  def testBuildCorsConfigNullOriginAllowed(): Unit = {
    val configOpt = HttpChannelInitializer.buildCorsConfig("https://example.com")
    assertTrue(configOpt.isDefined)
    val config = configOpt.get
    assertTrue(config.isNullOriginAllowed, "Null origin should be allowed for file:// URIs")
  }
}
