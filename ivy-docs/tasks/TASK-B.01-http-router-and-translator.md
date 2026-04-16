# TASK-B.01: HttpRouter + HttpRequestTranslator

## Prerequisites

- **TASK-A.01** (SecurityProtocol.HTTP/HTTPS) — Adds `HTTP(4)` and `HTTPS(5)` to `SecurityProtocol` enum with `isHttp()` helper. This task needs `SecurityProtocol.HTTP` to exist for `RequestContext` construction.
- **TASK-A.02** (http-server module, HttpServerConfigs) — Creates the `http-server` Gradle submodule with Netty and Jackson dependencies. This task creates files inside that module. Also provides `HttpServerConfigs` with constants like `httpRequestMaxBytes`, `httpProduceMaxRecords`, `httpConsumeMaxWaitMs`.

---

## Context

The HTTP protocol layer for Apache Kafka translates incoming HTTP requests (JSON bodies, URL paths, query params) into the internal Kafka protocol format (binary `ProduceRequest`, `FetchRequest`, `MetadataRequest`, etc.) so they can be placed on the shared `RequestChannel` and processed by the existing `KafkaApis` handler pool.

This task implements two classes that form the request ingress pipeline:

1. **`HttpRouter`** — parses the HTTP method + URI path, extracts path parameters (topic name, partition, consumer group), validates them, and determines which Kafka `ApiKeys` handler the request maps to. The router is called first for every incoming HTTP request.

2. **`HttpRequestTranslator`** — takes the routing result and the HTTP request body (JSON), deserializes it with Jackson, and produces a `(ApiKeys, ByteBuffer)` pair. The `ByteBuffer` contains the serialized Kafka wire-protocol request that `RequestChannel.Request` will parse. For `PRODUCE`, this includes partition assignment logic (murmur2 hash for keyed records, batch-sticky round-robin for keyless records). For `FETCH`, this includes clamping `maxWaitMs` to the server-side cap.

The design document (HTTP Protocol Design, sections 4, 5.3, 8.5, 14.4) specifies the exact route table, validation rules, and partition assignment algorithm. All validation runs before any Kafka code is called — invalid requests are rejected with HTTP 400/422 immediately without touching the `RequestChannel`.

---

## Specification

### `HttpRouter` — `kafka.server.http.HttpRouter`

```java
/**
 * Maps HTTP method + URI path to a Kafka handler type.
 * Validates topic names and client IDs before returning a route result.
 */
public final class HttpRouter {

    public enum HandlerType {
        PRODUCE,          // POST /v1/topics/{t}/records
        FETCH,            // POST /v1/topics/{t}/records:fetch
        METADATA_TOPIC,   // GET  /v1/topics/{t}
        METADATA_ALL,     // GET  /v1/topics
        LIST_OFFSETS,     // GET  /v1/topics/{t}/partitions/{p}/offsets
        CONSUMER_LAG,     // GET  /v1/consumer-groups/{g}/lags
        HEALTH            // GET  /v1/health
    }

    /**
     * Route result containing the handler type and extracted path parameters.
     */
    public record RouteResult(
        HandlerType handlerType,
        String topicName,        // null for METADATA_ALL, HEALTH
        Integer partition,       // non-null only for LIST_OFFSETS
        String consumerGroup,    // non-null only for CONSUMER_LAG
        Map<String, String> queryParams  // parsed query string
    ) {}

    /**
     * Routes an HTTP request to a handler type.
     *
     * @param method HTTP method (GET, POST, etc.)
     * @param uri    raw URI path with query string (e.g., "/v1/topics/my-topic/records")
     * @return RouteResult with handler type and extracted params
     * @throws InvalidRequestException if the URI does not match any route
     * @throws InvalidTopicException   if the topic name fails validation
     */
    public RouteResult route(HttpMethod method, String uri);

    /**
     * URL-decodes a path segment, rejects path traversal chars, then validates
     * via Topic.validate().
     *
     * @param rawSegment URL-encoded topic name from the URI path
     * @return validated, decoded topic name
     * @throws InvalidRequestException if topic contains /, \, or \0
     * @throws InvalidTopicException   if Topic.validate() fails
     */
    static String validateTopicName(String rawSegment);

    /**
     * Validates the X-Kafka-Client-ID header.
     * Returns "http-client" if absent or empty.
     *
     * @param clientId raw header value
     * @return validated client ID
     * @throws InvalidRequestException if clientId contains illegal characters
     */
    static String validateClientId(String clientId);
}
```

### `HttpRequestTranslator` — `kafka.server.http.HttpRequestTranslator`

```java
/**
 * Translates JSON HTTP request body into a serialized Kafka protocol request.
 * All validation runs here — invalid requests throw before touching RequestChannel.
 */
public final class HttpRequestTranslator {

    /**
     * Result of translating an HTTP request to a Kafka protocol request.
     */
    public record TranslationResult(
        ApiKeys apiKey,
        short apiVersion,
        ByteBuffer serializedRequest
    ) {}

    /**
     * Translates an HTTP request body + route result into a Kafka wire-format request.
     *
     * @param routeResult result from HttpRouter.route()
     * @param body        raw JSON body bytes (may be empty for GET routes)
     * @param config      server configuration for defaults/limits
     * @param metadataSupplier supplier for topic metadata (partition count)
     * @return TranslationResult with ApiKeys and serialized ByteBuffer
     * @throws InvalidRequestException on validation failure (400/422)
     */
    public TranslationResult translate(
        HttpRouter.RouteResult routeResult,
        byte[] body,
        HttpServerConfigs config,
        Function<String, Integer> metadataSupplier
    );
}
```

### Behavioral contracts

- `HttpRouter.route()` throws `InvalidRequestException` for unrecognized paths (maps to HTTP 404).
- `validateTopicName()` URL-decodes first, then rejects `\0`, `/`, `\\`, then delegates to `org.apache.kafka.common.internals.Topic.validate()`.
- `validateClientId()` accepts only `[a-zA-Z0-9._-]{1,128}` or returns `"http-client"` if absent/empty.
- `HttpRequestTranslator.translate()` rejects: empty body on POST routes, empty `records` array, records count > `config.httpProduceMaxRecords`, negative partition, invalid acks string, `timeoutMs <= 0`, invalid base64 in BINARY data.
- Partition assignment for keyless records: `(roundRobinCounter.getAndIncrement() & 0x7fffffff) % partitionCount`. All keyless records in one request get the same partition (batch-sticky).
- Partition assignment for keyed records: `(Utils.murmur2(keyBytes) & 0x7fffffff) % partitionCount` (matches the Java producer client).
- `maxWaitMs` for fetch: clamped to `Math.min(requestMaxWaitMs, config.httpConsumeMaxWaitMs)`.
- Jackson `ObjectMapper` configured with `StreamReadConstraints`: `maxNestingDepth=20`, `maxStringLength=1_048_576`, `maxNumberLength=100`.

---

## Implementation Details

**Module:** `http-server`

**Files to study:**

| File | Why |
|------|-----|
| `clients/src/main/java/org/apache/kafka/common/internals/Topic.java` | Topic name validation — `validate()` method we delegate to |
| `clients/src/main/java/org/apache/kafka/common/utils/Utils.java` | `murmur2(byte[])` — the partition hash function used by the Java producer |
| `clients/src/main/java/org/apache/kafka/common/requests/ProduceRequest.java` | How to build a `ProduceRequest` — constructor, builder, serialization |
| `clients/src/main/java/org/apache/kafka/common/requests/FetchRequest.java` | How to build a `FetchRequest` |
| `clients/src/main/java/org/apache/kafka/common/requests/MetadataRequest.java` | How to build a `MetadataRequest` |
| `clients/src/main/java/org/apache/kafka/common/requests/ListOffsetsRequest.java` | How to build a `ListOffsetsRequest` |

```java
// From clients/src/main/java/org/apache/kafka/common/internals/Topic.java lines 41-45:
public static void validate(String topic) {
    validate(topic, "Topic name", message -> {
        throw new InvalidTopicException(message);
    });
}
```

```java
// From clients/src/main/java/org/apache/kafka/common/utils/Utils.java — murmur2 hash:
// This is the same hash used by DefaultPartitioner in the Java client.
public static int murmur2(final byte[] data) { /* ... */ }
```

**Files to create:**

| File | What it does |
|------|--------------|
| `http-server/src/main/java/kafka/server/http/HttpRouter.java` | URI routing and path parameter extraction with validation |
| `http-server/src/main/java/kafka/server/http/HttpRequestTranslator.java` | JSON body deserialization and Kafka request construction |

> **CRITICAL:** The `murmur2` hash must use `& 0x7fffffff` before modulo to avoid negative partition indexes when the hash result is `Integer.MIN_VALUE`. The Java producer client does this too — match it exactly.

> **CRITICAL:** URL-decode the topic name BEFORE validation. `%2F` decodes to `/` which must be rejected. `%00` decodes to `\0` which must be rejected. Without URL-decoding first, an attacker can bypass path traversal checks.

> **CRITICAL:** Jackson `ObjectMapper` must be a shared static final instance configured once. Do NOT create a new ObjectMapper per request — it is expensive.

**Implementation order:**
1. Implement `HttpRouter` with route matching and validation methods
2. Implement `HttpRequestTranslator` with the Jackson ObjectMapper and translation logic
3. Start with PRODUCE translation (most complex — includes partition assignment)
4. Add FETCH translation (simpler — partition list from body, clamp maxWaitMs)
5. Add METADATA/LIST_OFFSETS translations (simplest — from URL params only)

---

## Skeleton Code

### `HttpRouter.java`

```java
package kafka.server.http;

import io.netty.handler.codec.http.HttpMethod;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.internals.Topic;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Routes HTTP method + URI path to a Kafka handler type and extracts path parameters.
 * All topic name and client ID validation is performed here before any Kafka code runs.
 *
 * Route table:
 *   POST /v1/topics/{t}/records         -> PRODUCE
 *   POST /v1/topics/{t}/records:fetch   -> FETCH
 *   GET  /v1/topics/{t}                 -> METADATA_TOPIC
 *   GET  /v1/topics                     -> METADATA_ALL
 *   GET  /v1/topics/{t}/partitions/{p}/offsets -> LIST_OFFSETS
 *   GET  /v1/consumer-groups/{g}/lags   -> CONSUMER_LAG
 *   GET  /v1/health                     -> HEALTH
 *
 * // Time: Created - TASK-B.01
 */
public final class HttpRouter {

    // --- Handler type enum ---
    public enum HandlerType {
        PRODUCE,
        FETCH,
        METADATA_TOPIC,
        METADATA_ALL,
        LIST_OFFSETS,
        CONSUMER_LAG,
        HEALTH
    }

    // --- Route result record ---
    public record RouteResult(
        HandlerType handlerType,
        String topicName,
        Integer partition,
        String consumerGroup,
        Map<String, String> queryParams
    ) {}

    // --- URI patterns (precompiled) ---
    // Matches: /v1/topics/{topicName}/records
    private static final Pattern PRODUCE_PATTERN =
        Pattern.compile("^/v1/topics/([^/?]+)/records$");

    // Matches: /v1/topics/{topicName}/records:fetch
    private static final Pattern FETCH_PATTERN =
        Pattern.compile("^/v1/topics/([^/?]+)/records:fetch$");

    // Matches: /v1/topics/{topicName}/partitions/{partition}/offsets
    private static final Pattern LIST_OFFSETS_PATTERN =
        Pattern.compile("^/v1/topics/([^/?]+)/partitions/(\\d+)/offsets$");

    // Matches: /v1/topics/{topicName}
    private static final Pattern METADATA_TOPIC_PATTERN =
        Pattern.compile("^/v1/topics/([^/?]+)$");

    // Matches: /v1/topics
    private static final Pattern METADATA_ALL_PATTERN =
        Pattern.compile("^/v1/topics$");

    // Matches: /v1/consumer-groups/{group}/lags
    private static final Pattern CONSUMER_LAG_PATTERN =
        Pattern.compile("^/v1/consumer-groups/([^/?]+)/lags$");

    // Matches: /v1/health
    private static final Pattern HEALTH_PATTERN =
        Pattern.compile("^/v1/health$");

    // --- Client ID validation ---
    private static final Pattern CLIENT_ID_PATTERN =
        Pattern.compile("^[a-zA-Z0-9._-]{1,128}$");

    private static final String DEFAULT_CLIENT_ID = "http-client";

    /**
     * Routes an HTTP request to a handler type.
     *
     * @param method HTTP method (GET, POST)
     * @param uri    raw URI with possible query string, e.g. "/v1/topics/orders/records"
     * @return RouteResult with handler type and extracted parameters
     * @throws InvalidRequestException if the URI does not match any known route
     * @throws InvalidTopicException   if the extracted topic name is invalid
     */
    public RouteResult route(HttpMethod method, String uri) {
        Objects.requireNonNull(method, "method");
        Objects.requireNonNull(uri, "uri");

        // TODO: Split URI into path and query string
        // TODO: Parse query string into Map<String, String>
        // TODO: Match path against patterns in order (most specific first)
        // TODO: For POST routes, verify method is POST; for GET routes, verify method is GET
        // TODO: Extract and validate topic name, partition, consumer group as needed
        // TODO: Return RouteResult or throw InvalidRequestException("No route found for ...")
        throw new UnsupportedOperationException("Not yet implemented");
    }

    /**
     * URL-decodes a path segment, rejects path traversal characters, then validates
     * via {@link Topic#validate(String)}.
     *
     * Validation order:
     * 1. URL-decode (catches %2F = '/', %00 = null byte)
     * 2. Reject path traversal: '/', '\', '\0'
     * 3. Kafka Topic.validate() — checks [a-zA-Z0-9._-], max 249 chars, rejects "." and ".."
     *
     * @param rawSegment URL-encoded topic name from the URI path
     * @return validated, decoded topic name
     * @throws InvalidRequestException if topic contains illegal characters (/, \, \0)
     * @throws InvalidTopicException   if Kafka's Topic.validate() rejects it
     */
    static String validateTopicName(String rawSegment) {
        // TODO: 1. URLDecoder.decode(rawSegment, StandardCharsets.UTF_8)
        // TODO: 2. Check for '\0', '/', '\\' — throw InvalidRequestException
        // TODO: 3. Topic.validate(decoded) — throws InvalidTopicException on failure
        // TODO: 4. Return decoded topic name
        throw new UnsupportedOperationException("Not yet implemented");
    }

    /**
     * Validates the X-Kafka-Client-ID header value.
     * Must match [a-zA-Z0-9._-]{1,128}. Returns "http-client" if absent or empty.
     *
     * @param clientId raw header value (may be null)
     * @return validated client ID string
     * @throws InvalidRequestException if clientId contains illegal characters
     */
    static String validateClientId(String clientId) {
        // TODO: Return DEFAULT_CLIENT_ID if null or empty
        // TODO: Match against CLIENT_ID_PATTERN
        // TODO: Throw InvalidRequestException if no match
        throw new UnsupportedOperationException("Not yet implemented");
    }

    /**
     * Splits a URI into path and query string components, then parses query params.
     */
    private static Map<String, String> parseQueryParams(String queryString) {
        // TODO: Split on '&', then split each on '=' (URL-decode both key and value)
        // TODO: Return unmodifiable map
        throw new UnsupportedOperationException("Not yet implemented");
    }
}
```

### `HttpRequestTranslator.java`

```java
package kafka.server.http;

import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.ListOffsetsRequestData;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.utils.Utils;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * Translates JSON HTTP request bodies into serialized Kafka wire-protocol requests.
 *
 * All input validation runs here — invalid requests throw exceptions that the caller
 * maps to HTTP 400/422 before anything touches RequestChannel.
 *
 * Jackson ObjectMapper is configured with StreamReadConstraints to prevent abuse:
 *   - maxNestingDepth = 20
 *   - maxStringLength = 1 MB
 *   - maxNumberLength = 100
 *
 * // Time: Created - TASK-B.01
 */
public final class HttpRequestTranslator {

    // --- Jackson ObjectMapper (shared, thread-safe) ---
    static final ObjectMapper MAPPER = new ObjectMapper()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        .setStreamReadConstraints(StreamReadConstraints.builder()
            .maxNestingDepth(20)
            .maxStringLength(1_048_576)   // 1 MB max per string field
            .maxNumberLength(100)
            .build());

    // --- Data type constants ---
    static final String TYPE_STRING = "STRING";
    static final String TYPE_BINARY = "BINARY";
    static final String TYPE_JSON = "JSON";
    static final String TYPE_NULL = "NULL";

    // --- ACK mapping ---
    static final short ACKS_NONE = 0;
    static final short ACKS_LEADER = 1;
    static final short ACKS_ALL = -1;

    // --- ListOffsets timestamp constants ---
    static final long EARLIEST_TIMESTAMP = -2L;
    static final long LATEST_TIMESTAMP = -1L;
    static final long MAX_TIMESTAMP = -3L;

    // --- Default produce settings ---
    static final int DEFAULT_PRODUCE_TIMEOUT_MS = 30_000;
    static final String DEFAULT_ACKS = "all";

    // --- Default fetch settings ---
    static final int DEFAULT_FETCH_MAX_WAIT_MS = 500;
    static final int DEFAULT_FETCH_MIN_BYTES = 1;
    static final int DEFAULT_FETCH_MAX_BYTES = 10_485_760;       // 10 MB
    static final int DEFAULT_FETCH_MAX_BYTES_PER_PARTITION = 1_048_576;  // 1 MB

    // --- Batch-sticky partition counter (per topic, but AtomicInteger for simplicity in phase 1) ---
    private final AtomicInteger roundRobinCounter = new AtomicInteger(0);

    /**
     * Result of translating an HTTP request to a Kafka wire-format request.
     */
    public record TranslationResult(
        ApiKeys apiKey,
        short apiVersion,
        ByteBuffer serializedRequest
    ) {}

    /**
     * Translates an HTTP request body + route result into a Kafka wire-format request.
     *
     * @param routeResult      from HttpRouter.route()
     * @param body             raw JSON body bytes (may be empty/null for GET routes)
     * @param config           server config for limits (httpProduceMaxRecords, httpConsumeMaxWaitMs)
     * @param metadataSupplier given a topic name, returns partition count (or throws)
     * @return TranslationResult with apiKey, version, and serialized ByteBuffer
     * @throws InvalidRequestException on any validation failure
     */
    public TranslationResult translate(
            HttpRouter.RouteResult routeResult,
            byte[] body,
            HttpServerConfigs config,
            Function<String, Integer> metadataSupplier) {
        Objects.requireNonNull(routeResult, "routeResult");

        // TODO: switch on routeResult.handlerType():
        //   PRODUCE       -> translateProduce(routeResult, body, config, metadataSupplier)
        //   FETCH         -> translateFetch(routeResult, body, config)
        //   METADATA_TOPIC -> translateMetadataTopic(routeResult)
        //   METADATA_ALL  -> translateMetadataAll()
        //   LIST_OFFSETS  -> translateListOffsets(routeResult)
        //   CONSUMER_LAG  -> translateConsumerLag(routeResult)
        //   HEALTH        -> throw (should never reach translator — handled directly)
        throw new UnsupportedOperationException("Not yet implemented");
    }

    /**
     * Translates a produce request. Parses records array, assigns partitions
     * (murmur2 for keyed, batch-sticky for keyless), builds ProduceRequest.
     *
     * Validation:
     * - Body must not be null/empty
     * - "records" array must be present and non-empty
     * - records.size() <= config.httpProduceMaxRecords (default 10,000)
     * - partition (if present) must be >= 0
     * - acks must be "all", "leader", or "none"
     * - timeoutMs must be > 0
     * - BINARY data must be valid base64
     */
    TranslationResult translateProduce(
            HttpRouter.RouteResult routeResult,
            byte[] body,
            HttpServerConfigs config,
            Function<String, Integer> metadataSupplier) {
        // TODO: 1. Parse body as JSON
        // TODO: 2. Extract "records" array — validate non-null, non-empty, <= max
        // TODO: 3. Extract "acks" (default "all") — map to short
        // TODO: 4. Extract "timeoutMs" (default 30000) — validate > 0
        // TODO: 5. Get partition count from metadataSupplier
        // TODO: 6. Compute batchStickyPartition for keyless records:
        //          (roundRobinCounter.getAndIncrement() & 0x7fffffff) % partitionCount
        // TODO: 7. For each record:
        //          - parse key DataObject → byte[]
        //          - parse value DataObject → byte[]
        //          - parse headers → Header[]
        //          - assign partition: explicit, murmur2(key) & 0x7fffffff % partitionCount, or batchSticky
        //          - group by partition into Map<Integer, List<SimpleRecord>>
        // TODO: 8. Build MemoryRecords per partition
        // TODO: 9. Build ProduceRequestData and serialize
        // TODO: 10. Return TranslationResult(ApiKeys.PRODUCE, latestVersion, buffer)
        throw new UnsupportedOperationException("Not yet implemented");
    }

    /**
     * Translates a fetch request. Parses partitions array, clamps maxWaitMs.
     *
     * Validation:
     * - Body must not be null/empty
     * - "partitions" array must be present and non-empty
     * - Each partition entry must have "partition" (int >= 0) and "offset" (long >= 0)
     * - maxWaitMs >= 0 before clamping
     */
    TranslationResult translateFetch(
            HttpRouter.RouteResult routeResult,
            byte[] body,
            HttpServerConfigs config) {
        // TODO: 1. Parse body as JSON
        // TODO: 2. Extract "partitions" array — validate
        // TODO: 3. Extract maxWaitMs (default 500), clamp to config.httpConsumeMaxWaitMs
        // TODO: 4. Extract minBytes, maxBytes, maxBytesPerPartition, isolationLevel
        // TODO: 5. Build FetchRequestData and serialize
        // TODO: 6. Return TranslationResult(ApiKeys.FETCH, latestVersion, buffer)
        throw new UnsupportedOperationException("Not yet implemented");
    }

    /**
     * Translates a single-topic metadata request from GET /v1/topics/{topic}.
     */
    TranslationResult translateMetadataTopic(HttpRouter.RouteResult routeResult) {
        // TODO: Build MetadataRequest for single topic
        throw new UnsupportedOperationException("Not yet implemented");
    }

    /**
     * Translates an all-topics metadata request from GET /v1/topics.
     */
    TranslationResult translateMetadataAll() {
        // TODO: Build MetadataRequest with empty topics list (returns all topics)
        throw new UnsupportedOperationException("Not yet implemented");
    }

    /**
     * Translates a list-offsets request from GET /v1/topics/{t}/partitions/{p}/offsets.
     *
     * Query param "timestamp" maps to:
     *   "earliest" -> -2 (EARLIEST_TIMESTAMP)
     *   "latest"   -> -1 (LATEST_TIMESTAMP)
     *   "max"      -> -3 (MAX_TIMESTAMP)
     *   numeric    -> parsed as long (epoch millis)
     */
    TranslationResult translateListOffsets(HttpRouter.RouteResult routeResult) {
        // TODO: Extract topic, partition, timestamp from routeResult
        // TODO: Map timestamp string to long constant
        // TODO: Build ListOffsetsRequestData and serialize
        throw new UnsupportedOperationException("Not yet implemented");
    }

    /**
     * Translates a consumer lag request. This produces TWO requests internally
     * (OFFSET_FETCH + LIST_OFFSETS), but for phase 1 we build just the OFFSET_FETCH.
     * The lag computation is handled in HttpResponseSerializer.
     */
    TranslationResult translateConsumerLag(HttpRouter.RouteResult routeResult) {
        // TODO: Build OffsetFetchRequest for the consumer group
        throw new UnsupportedOperationException("Not yet implemented");
    }

    // --- DataObject deserialization helpers ---

    /**
     * Deserializes a DataObject JSON node to byte[].
     *
     * { "type": "STRING", "data": "text" }     -> text.getBytes(UTF_8)
     * { "type": "BINARY", "data": "base64..." } -> Base64.decode(data)
     * { "type": "JSON",   "data": {...} }       -> data.toString().getBytes(UTF_8)
     * { "type": "NULL" }                        -> null
     *
     * @throws InvalidRequestException if type is unknown or BINARY data is invalid base64
     */
    static byte[] deserializeDataObject(JsonNode node) {
        // TODO: implement
        throw new UnsupportedOperationException("Not yet implemented");
    }

    /**
     * Maps acks string to Kafka wire-protocol short value.
     * "none" -> 0, "leader" -> 1, "all" -> -1
     *
     * @throws InvalidRequestException if acks is not one of the three valid values
     */
    static short parseAcks(String acks) {
        // TODO: implement
        throw new UnsupportedOperationException("Not yet implemented");
    }

    /**
     * Maps timestamp query param to ListOffsets constant.
     * "earliest" -> -2, "latest" -> -1, "max" -> -3, numeric -> parsed long
     *
     * @throws InvalidRequestException if value is not recognized and not a valid long
     */
    static long parseTimestamp(String value) {
        // TODO: implement
        throw new UnsupportedOperationException("Not yet implemented");
    }
}
```

### Test class — `HttpRouterTest.java`

```java
package kafka.server.http;

import io.netty.handler.codec.http.HttpMethod;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * // Time: Created - TASK-B.01
 */
class HttpRouterTest {

    private HttpRouter router;

    @BeforeEach
    void setUp() {
        router = new HttpRouter();
    }

    // --- Route matching ---

    @Test
    void route_producePost_returnsProduceHandler() {
        // Arrange / Act
        var result = router.route(HttpMethod.POST, "/v1/topics/my-topic/records");
        // Assert
        assertEquals(HttpRouter.HandlerType.PRODUCE, result.handlerType());
        assertEquals("my-topic", result.topicName());
    }

    @Test
    void route_fetchPost_returnsFetchHandler() {
        var result = router.route(HttpMethod.POST, "/v1/topics/my-topic/records:fetch");
        assertEquals(HttpRouter.HandlerType.FETCH, result.handlerType());
        assertEquals("my-topic", result.topicName());
    }

    @Test
    void route_metadataTopic_returnsMetadataTopicHandler() {
        var result = router.route(HttpMethod.GET, "/v1/topics/my-topic");
        assertEquals(HttpRouter.HandlerType.METADATA_TOPIC, result.handlerType());
        assertEquals("my-topic", result.topicName());
    }

    @Test
    void route_metadataAll_returnsMetadataAllHandler() {
        var result = router.route(HttpMethod.GET, "/v1/topics");
        assertEquals(HttpRouter.HandlerType.METADATA_ALL, result.handlerType());
        assertNull(result.topicName());
    }

    @Test
    void route_listOffsets_extractsPartition() {
        var result = router.route(HttpMethod.GET, "/v1/topics/orders/partitions/0/offsets?timestamp=latest");
        assertEquals(HttpRouter.HandlerType.LIST_OFFSETS, result.handlerType());
        assertEquals("orders", result.topicName());
        assertEquals(0, result.partition());
        assertEquals("latest", result.queryParams().get("timestamp"));
    }

    @Test
    void route_consumerLag_extractsGroup() {
        var result = router.route(HttpMethod.GET, "/v1/consumer-groups/my-group/lags");
        assertEquals(HttpRouter.HandlerType.CONSUMER_LAG, result.handlerType());
        assertEquals("my-group", result.consumerGroup());
    }

    @Test
    void route_health_returnsHealthHandler() {
        var result = router.route(HttpMethod.GET, "/v1/health");
        assertEquals(HttpRouter.HandlerType.HEALTH, result.handlerType());
    }

    @Test
    void route_unknownPath_throws() {
        assertThrows(InvalidRequestException.class,
            () -> router.route(HttpMethod.GET, "/v1/unknown"));
    }

    @Test
    void route_wrongMethod_throws() {
        assertThrows(InvalidRequestException.class,
            () -> router.route(HttpMethod.GET, "/v1/topics/my-topic/records"));
    }

    // --- Topic name validation ---

    @Test
    void validateTopicName_validName_succeeds() {
        assertEquals("my-topic", HttpRouter.validateTopicName("my-topic"));
    }

    @Test
    void validateTopicName_urlEncodedSlash_rejects() {
        assertThrows(InvalidRequestException.class,
            () -> HttpRouter.validateTopicName("my%2Ftopic"));
    }

    @Test
    void validateTopicName_nullByte_rejects() {
        assertThrows(InvalidRequestException.class,
            () -> HttpRouter.validateTopicName("my%00topic"));
    }

    @Test
    void validateTopicName_backslash_rejects() {
        assertThrows(InvalidRequestException.class,
            () -> HttpRouter.validateTopicName("my\\topic"));
    }

    @Test
    void validateTopicName_dot_rejectsViaKafka() {
        assertThrows(InvalidTopicException.class,
            () -> HttpRouter.validateTopicName("."));
    }

    @Test
    void validateTopicName_tooLong_rejectsViaKafka() {
        String longName = "a".repeat(250);
        assertThrows(InvalidTopicException.class,
            () -> HttpRouter.validateTopicName(longName));
    }

    // --- Client ID validation ---

    @Test
    void validateClientId_null_returnsDefault() {
        assertEquals("http-client", HttpRouter.validateClientId(null));
    }

    @Test
    void validateClientId_empty_returnsDefault() {
        assertEquals("http-client", HttpRouter.validateClientId(""));
    }

    @Test
    void validateClientId_validId_returnsAsIs() {
        assertEquals("my-client_1.0", HttpRouter.validateClientId("my-client_1.0"));
    }

    @Test
    void validateClientId_illegalChars_throws() {
        assertThrows(InvalidRequestException.class,
            () -> HttpRouter.validateClientId("my client!"));
    }

    @Test
    void validateClientId_tooLong_throws() {
        assertThrows(InvalidRequestException.class,
            () -> HttpRouter.validateClientId("a".repeat(129)));
    }
}
```

### Test class — `HttpRequestTranslatorTest.java`

```java
package kafka.server.http;

import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.protocol.ApiKeys;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * // Time: Created - TASK-B.01
 */
class HttpRequestTranslatorTest {

    private HttpRequestTranslator translator;
    private HttpServerConfigs config;

    @BeforeEach
    void setUp() {
        translator = new HttpRequestTranslator();
        // TODO: create config with defaults (httpProduceMaxRecords=10000, httpConsumeMaxWaitMs=5000)
    }

    // --- Produce translation ---

    @Test
    void translateProduce_singleRecord_buildsProduceRequest() {
        // Arrange
        String json = """
            { "records": [{"value": {"type": "STRING", "data": "hello"}}] }
            """;
        var route = new HttpRouter.RouteResult(
            HttpRouter.HandlerType.PRODUCE, "my-topic", null, null, Map.of());
        // Act
        var result = translator.translate(route, json.getBytes(StandardCharsets.UTF_8),
            config, topic -> 3);
        // Assert
        assertEquals(ApiKeys.PRODUCE, result.apiKey());
        assertNotNull(result.serializedRequest());
    }

    @Test
    void translateProduce_keyedRecord_usesMurmur2() {
        // TODO: Verify keyed record goes to murmur2(key) & 0x7fffffff % partitionCount
    }

    @Test
    void translateProduce_keylessRecords_allSamePartition() {
        // TODO: Verify all keyless records in same request get same partition (batch-sticky)
    }

    @Test
    void translateProduce_emptyBody_throws() {
        var route = new HttpRouter.RouteResult(
            HttpRouter.HandlerType.PRODUCE, "my-topic", null, null, Map.of());
        assertThrows(InvalidRequestException.class,
            () -> translator.translate(route, null, config, topic -> 3));
    }

    @Test
    void translateProduce_tooManyRecords_throws() {
        // TODO: records.size() > config.httpProduceMaxRecords -> throw
    }

    @Test
    void translateProduce_negativePartition_throws() {
        String json = """
            { "records": [{"partition": -1, "value": {"type": "STRING", "data": "x"}}] }
            """;
        var route = new HttpRouter.RouteResult(
            HttpRouter.HandlerType.PRODUCE, "my-topic", null, null, Map.of());
        assertThrows(InvalidRequestException.class,
            () -> translator.translate(route, json.getBytes(StandardCharsets.UTF_8),
                config, topic -> 3));
    }

    @Test
    void translateProduce_invalidAcks_throws() {
        String json = """
            { "records": [{"value": {"type": "STRING", "data": "x"}}], "acks": "two" }
            """;
        var route = new HttpRouter.RouteResult(
            HttpRouter.HandlerType.PRODUCE, "my-topic", null, null, Map.of());
        assertThrows(InvalidRequestException.class,
            () -> translator.translate(route, json.getBytes(StandardCharsets.UTF_8),
                config, topic -> 3));
    }

    // --- Fetch translation ---

    @Test
    void translateFetch_clampsMaxWaitMs() {
        // TODO: maxWaitMs=30000, config.httpConsumeMaxWaitMs=5000 -> effective=5000
    }

    @Test
    void translateFetch_emptyPartitions_throws() {
        String json = """
            { "partitions": [] }
            """;
        var route = new HttpRouter.RouteResult(
            HttpRouter.HandlerType.FETCH, "my-topic", null, null, Map.of());
        assertThrows(InvalidRequestException.class,
            () -> translator.translate(route, json.getBytes(StandardCharsets.UTF_8),
                config, topic -> 3));
    }

    // --- DataObject parsing ---

    @Test
    void deserializeDataObject_stringType_returnsUtf8Bytes() {
        var node = HttpRequestTranslator.MAPPER.createObjectNode()
            .put("type", "STRING").put("data", "hello");
        byte[] result = HttpRequestTranslator.deserializeDataObject(node);
        assertArrayEquals("hello".getBytes(StandardCharsets.UTF_8), result);
    }

    @Test
    void deserializeDataObject_binaryType_decodesBase64() {
        var node = HttpRequestTranslator.MAPPER.createObjectNode()
            .put("type", "BINARY").put("data", "aGVsbG8=");
        byte[] result = HttpRequestTranslator.deserializeDataObject(node);
        assertArrayEquals("hello".getBytes(StandardCharsets.UTF_8), result);
    }

    @Test
    void deserializeDataObject_nullType_returnsNull() {
        var node = HttpRequestTranslator.MAPPER.createObjectNode()
            .put("type", "NULL");
        assertNull(HttpRequestTranslator.deserializeDataObject(node));
    }

    @Test
    void deserializeDataObject_invalidBase64_throws() {
        var node = HttpRequestTranslator.MAPPER.createObjectNode()
            .put("type", "BINARY").put("data", "not-valid!!!");
        assertThrows(InvalidRequestException.class,
            () -> HttpRequestTranslator.deserializeDataObject(node));
    }

    // --- Acks parsing ---

    @Test
    void parseAcks_all_returnsMinusOne() {
        assertEquals((short) -1, HttpRequestTranslator.parseAcks("all"));
    }

    @Test
    void parseAcks_leader_returnsOne() {
        assertEquals((short) 1, HttpRequestTranslator.parseAcks("leader"));
    }

    @Test
    void parseAcks_none_returnsZero() {
        assertEquals((short) 0, HttpRequestTranslator.parseAcks("none"));
    }

    // --- Timestamp parsing ---

    @Test
    void parseTimestamp_earliest_returnsMinusTwo() {
        assertEquals(-2L, HttpRequestTranslator.parseTimestamp("earliest"));
    }

    @Test
    void parseTimestamp_latest_returnsMinusOne() {
        assertEquals(-1L, HttpRequestTranslator.parseTimestamp("latest"));
    }

    @Test
    void parseTimestamp_max_returnsMinusThree() {
        assertEquals(-3L, HttpRequestTranslator.parseTimestamp("max"));
    }

    @Test
    void parseTimestamp_epochMillis_parsesLong() {
        assertEquals(1713260400000L, HttpRequestTranslator.parseTimestamp("1713260400000"));
    }
}
```

### Existing pattern reference

```java
// From clients/src/main/java/org/apache/kafka/common/internals/Topic.java lines 41-72:
public static void validate(String topic) {
    validate(topic, "Topic name", message -> {
        throw new InvalidTopicException(message);
    });
}

private static String detectInvalidTopic(String name) {
    if (name.isEmpty())
        return "the empty string is not allowed";
    if (".".equals(name))
        return "'.' is not allowed";
    if ("..".equals(name))
        return "'..' is not allowed";
    if (name.length() > MAX_NAME_LENGTH)
        return "the length of '" + name + "' is longer than the max allowed length " + MAX_NAME_LENGTH;
    if (!containsValidPattern(name))
        return "'" + name + "' contains one or more characters other than " +
            "ASCII alphanumerics, '.', '_' and '-'";
    return null;
}
```

---

## Tests

**Test class:** `http-server/src/test/java/kafka/server/http/HttpRouterTest.java`

| Test method | What it verifies |
|-------------|-----------------|
| `route_producePost_returnsProduceHandler` | POST /v1/topics/{t}/records maps to PRODUCE |
| `route_fetchPost_returnsFetchHandler` | POST /v1/topics/{t}/records:fetch maps to FETCH |
| `route_metadataTopic_returnsMetadataTopicHandler` | GET /v1/topics/{t} maps to METADATA_TOPIC |
| `route_metadataAll_returnsMetadataAllHandler` | GET /v1/topics maps to METADATA_ALL |
| `route_listOffsets_extractsPartition` | GET /v1/topics/{t}/partitions/{p}/offsets extracts both topic and partition |
| `route_consumerLag_extractsGroup` | GET /v1/consumer-groups/{g}/lags extracts group name |
| `route_health_returnsHealthHandler` | GET /v1/health maps to HEALTH |
| `route_unknownPath_throws` | Unrecognized path throws InvalidRequestException |
| `route_wrongMethod_throws` | GET on a POST-only route throws InvalidRequestException |
| `validateTopicName_validName_succeeds` | Normal topic name passes |
| `validateTopicName_urlEncodedSlash_rejects` | %2F in topic name rejected |
| `validateTopicName_nullByte_rejects` | %00 in topic name rejected |
| `validateTopicName_backslash_rejects` | Backslash in topic name rejected |
| `validateTopicName_dot_rejectsViaKafka` | "." rejected by Kafka's Topic.validate() |

**Test class:** `http-server/src/test/java/kafka/server/http/HttpRequestTranslatorTest.java`

| Test method | What it verifies |
|-------------|-----------------|
| `translateProduce_singleRecord_buildsProduceRequest` | Happy path produce |
| `translateProduce_keyedRecord_usesMurmur2` | Key-based partition assignment |
| `translateProduce_keylessRecords_allSamePartition` | Batch-sticky assignment |
| `translateProduce_emptyBody_throws` | Null body rejected |
| `translateProduce_tooManyRecords_throws` | Exceeding httpProduceMaxRecords rejected |
| `translateProduce_negativePartition_throws` | partition < 0 rejected |
| `translateFetch_clampsMaxWaitMs` | maxWaitMs clamped to config cap |
| `deserializeDataObject_*` | All DataObject type variants |
| `parseAcks_*` | Acks string mapping |
| `parseTimestamp_*` | Timestamp query param mapping |

**Run command:**
```bash
./gradlew :http-server:test --tests "kafka.server.http.HttpRouterTest" --tests "kafka.server.http.HttpRequestTranslatorTest"
```

---

## Rules

- All input validation runs in HttpRouter/HttpRequestTranslator BEFORE any Kafka code is called. Return HTTP 400/422 immediately — do not enqueue to RequestChannel.
- The Jackson ObjectMapper must be a single shared `static final` instance — never create per-request.
- Murmur2 partition assignment MUST use `& 0x7fffffff` before modulo to prevent negative partition indexes.
- Topic names MUST be URL-decoded before validation to prevent path traversal bypass.
- The `X-Kafka-Client-ID` header is validated with `[a-zA-Z0-9._-]{1,128}` because it is used in metric label construction.

---

## Learning

_To be filled by the executing agent._

---

## Limitations

_To be filled by the executing agent._

---

## Field Notes

_To be filled by the executing agent._

---

## Acceptance Criteria

- [ ] `./gradlew :http-server:test --tests "kafka.server.http.HttpRouterTest"` exits 0
- [ ] `./gradlew :http-server:test --tests "kafka.server.http.HttpRequestTranslatorTest"` exits 0
- [ ] `HttpRouter.java` exists at `http-server/src/main/java/kafka/server/http/HttpRouter.java`
- [ ] `HttpRequestTranslator.java` exists at `http-server/src/main/java/kafka/server/http/HttpRequestTranslator.java`
- [ ] All 7 routes are handled (PRODUCE, FETCH, METADATA_TOPIC, METADATA_ALL, LIST_OFFSETS, CONSUMER_LAG, HEALTH)
- [ ] Topic name validation includes URL-decode, path traversal rejection, and Topic.validate()
- [ ] Client ID validation matches `[a-zA-Z0-9._-]{1,128}` pattern
- [ ] Jackson ObjectMapper configured with StreamReadConstraints (maxNestingDepth=20, maxStringLength=1MB)
- [ ] Murmur2 hash uses `& 0x7fffffff` before modulo
- [ ] Batch-sticky partition assignment uses `AtomicInteger` counter with `& 0x7fffffff`
- [ ] Learning section filled with at least one entry
- [ ] Limitations section filled (use "None" if truly none)
- [ ] File Manifest section updated after commit

---

## File Manifest

<!-- ### YYYY-MM-DD — <short description> (commit <hash>)
Created:
  - http-server/src/main/java/kafka/server/http/HttpRouter.java — URI routing and validation
  - http-server/src/main/java/kafka/server/http/HttpRequestTranslator.java — JSON to Kafka request translation
  - http-server/src/test/java/kafka/server/http/HttpRouterTest.java — Router unit tests
  - http-server/src/test/java/kafka/server/http/HttpRequestTranslatorTest.java — Translator unit tests
Modified:
  - (none)
-->
