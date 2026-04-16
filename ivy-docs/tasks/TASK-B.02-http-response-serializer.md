# TASK-B.02: HttpResponseSerializer

## Prerequisites

- **TASK-A.02** (http-server module) — Creates the `http-server` Gradle submodule with Netty and Jackson dependencies. This task creates files inside that module.

---

## Context

When `KafkaApis` finishes processing a request, it calls `requestChannel.sendResponse()` with a Kafka `AbstractResponse` object (e.g., `ProduceResponse`, `FetchResponse`, `MetadataResponse`). For binary protocol clients, this response is serialized directly into the Kafka wire format. For HTTP clients, the response must be converted to JSON.

`HttpResponseSerializer` is the single class responsible for this conversion. It handles:

1. **Kafka response to JSON serialization** — converting `AbstractResponse` subtypes into the JSON shapes defined in the HTTP API specification (design doc sections 4.1-4.3).
2. **Value type detection** (design doc section 14.7) — when serializing consumed records from a `FetchResponse`, the serializer must determine whether each record value is null, JSON, a clean UTF-8 string, or arbitrary binary. The algorithm is deterministic and documented.
3. **Error code to HTTP status mapping** (design doc section 11.1) — every Kafka `Errors` value maps to an HTTP status code. Some statuses include a `Retry-After` header.
4. **Partial failure handling** — when a multi-partition produce or fetch has mixed results (some partitions succeed, some fail), the HTTP status is `207 Multi-Status` with per-partition error info.
5. **Response headers** — `X-Kafka-Request-ID`, `X-Kafka-MaxWait-Applied`, `Content-Type: application/json`.

---

## Specification

### `HttpResponseSerializer` — `kafka.server.http.HttpResponseSerializer`

```java
/**
 * Serializes Kafka AbstractResponse objects into Netty FullHttpResponse with JSON body.
 * Handles error-code-to-HTTP-status mapping, value type detection for consumed records,
 * and partial failure (207 Multi-Status).
 */
public final class HttpResponseSerializer {

    /**
     * Serializes a Kafka AbstractResponse into a Netty FullHttpResponse.
     *
     * @param kafkaResponse  the Kafka protocol response
     * @param requestId      the X-Kafka-Request-ID to echo back
     * @param apiKey         the ApiKey of the original request (determines serialization shape)
     * @param effectiveMaxWaitMs  applied maxWaitMs cap (for X-Kafka-MaxWait-Applied header, -1 if N/A)
     * @return Netty FullHttpResponse with JSON body, correct HTTP status, and response headers
     */
    public static FullHttpResponse serialize(
        AbstractResponse kafkaResponse,
        String requestId,
        ApiKeys apiKey,
        int effectiveMaxWaitMs
    );

    /**
     * Maps a Kafka Errors enum value to an HTTP status code.
     *
     * @param error the Kafka error code
     * @return the corresponding HTTP status
     */
    static HttpResponseStatus mapErrorToHttpStatus(Errors error);

    /**
     * Returns the Retry-After header value in seconds for the given error, or -1 if none.
     *
     * @param error the Kafka error code
     * @return seconds for Retry-After header, or -1
     */
    static int retryAfterSeconds(Errors error);

    /**
     * Detects the value type of a record's byte[] payload and serializes it as a DataObject.
     *
     * Algorithm:
     * 1. null -> {"type":"NULL"}
     * 2. _content-type: application/json header -> {"type":"JSON","data":<parsed>}
     * 3. Valid UTF-8 without control chars -> {"type":"STRING","data":"..."}
     * 4. Fallback -> {"type":"BINARY","data":"<base64>"}
     *
     * @param valueBytes  raw record value (may be null)
     * @param headers     record headers (checked for _content-type)
     * @return ObjectNode representing the DataObject
     */
    static ObjectNode detectAndSerializeValue(byte[] valueBytes, Header[] headers);

    /**
     * Builds an error-only JSON response body.
     *
     * @param error   Kafka error
     * @param detail  optional detail message
     * @return JSON string: {"errorCode":N,"errorMessage":"...","detail":"..."}
     */
    static String buildErrorBody(Errors error, String detail);

    /**
     * Determines the overall HTTP status for a response that may contain partial failures.
     * If all partitions have the same error (or all NONE), use that status.
     * If mixed, return 207 Multi-Status.
     *
     * @param errors collection of per-partition Errors values
     * @return HTTP status code
     */
    static HttpResponseStatus determineOverallStatus(Collection<Errors> errors);
}
```

### Error code to HTTP status mapping (from design doc section 11.1)

| Kafka Error | HTTP Status | Retry-After (s) |
|---|---|---|
| `NONE` | 200 | - |
| `UNKNOWN_TOPIC_OR_PARTITION` | 404 | - |
| `LEADER_NOT_AVAILABLE` | 503 | 1 |
| `NOT_LEADER_OR_FOLLOWER` | 503 | 1 |
| `MESSAGE_TOO_LARGE` | 413 | - |
| `RECORD_LIST_TOO_LARGE` | 413 | - |
| `TOPIC_AUTHORIZATION_FAILED` | 403 | - |
| `CLUSTER_AUTHORIZATION_FAILED` | 403 | - |
| `INVALID_REQUEST` | 400 | - |
| `INVALID_TOPIC_EXCEPTION` | 400 | - |
| `NOT_ENOUGH_REPLICAS` | 503 | 5 |
| `NOT_ENOUGH_REPLICAS_AFTER_APPEND` | 503 | 5 |
| `REQUEST_TIMED_OUT` | 504 | 1 |
| `THROTTLING_QUOTA_EXCEEDED` | 429 | `ceil(throttleTimeMs/1000)` |
| `KAFKA_STORAGE_ERROR` | 500 | - |
| Everything else | 500 | - |

### Value type detection algorithm (from design doc section 14.7)

```
Input: byte[] valueBytes, Header[] headers

1. if valueBytes == null:
       -> { "type": "NULL" }

2. if headers contain "_content-type: application/json":
       parse valueBytes as UTF-8 JSON
       -> { "type": "JSON", "data": <parsed JsonNode> }
       on parse failure: fall through to step 3

3. try decode valueBytes as UTF-8:
       if decode fails -> go to step 4
       if decoded string contains any code point in [0x00-0x08, 0x0B, 0x0C, 0x0E-0x1F, 0x7F]
           (control chars except HT=0x09, LF=0x0A, CR=0x0D)
           -> go to step 4
       -> { "type": "STRING", "data": <decoded string> }

4. -> { "type": "BINARY", "data": <Base64.getEncoder().encodeToString(valueBytes)> }
```

---

## Implementation Details

**Module:** `http-server`

**Files to study:**

| File | Why |
|------|-----|
| `clients/src/main/java/org/apache/kafka/common/protocol/Errors.java` | The `Errors` enum — all Kafka error codes, their names, exception classes |
| `clients/src/main/java/org/apache/kafka/common/requests/ProduceResponse.java` | Structure of ProduceResponse — `PartitionResponse` per topic-partition |
| `clients/src/main/java/org/apache/kafka/common/requests/FetchResponse.java` | Structure of FetchResponse — `FetchablePartitionResponse` with `MemoryRecords` |
| `clients/src/main/java/org/apache/kafka/common/requests/MetadataResponse.java` | Structure of MetadataResponse — topic metadata with partition/leader/ISR |
| `clients/src/main/java/org/apache/kafka/common/record/DefaultRecord.java` | How to iterate records, access key/value/headers |

```java
// From Errors.java — each error has a code and message:
public enum Errors {
    UNKNOWN_SERVER_ERROR(-1, "The server experienced an unexpected error...", ...),
    NONE(0, null, ...),
    OFFSET_OUT_OF_RANGE(1, "...", ...),
    // etc.
    public short code() { return code; }
    public String message() { return message; }
}
```

**Files to create:**

| File | What it does |
|------|--------------|
| `http-server/src/main/java/kafka/server/http/HttpResponseSerializer.java` | Kafka response to JSON + HTTP status serialization |

> **CRITICAL:** Value type detection must check `_content-type` header (with underscore prefix) NOT `Content-Type`. The underscore prefix is a Kafka record header convention to avoid collision with HTTP headers.

> **CRITICAL:** The UTF-8 control character check in step 3 must EXCLUDE tab (0x09), newline (0x0A), and carriage return (0x0D) — these are valid in text content. Only reject other C0 control characters (0x00-0x08, 0x0B, 0x0C, 0x0E-0x1F) and DEL (0x7F).

> **CRITICAL:** When computing `Retry-After` for `THROTTLING_QUOTA_EXCEEDED`, use `Math.max(1, (int) Math.ceil(throttleTimeMs / 1000.0))` to ensure it's always at least 1 second.

**Implementation order:**
1. Implement error-to-HTTP-status mapping (static, pure function)
2. Implement value type detection (static, pure function)
3. Implement `ProduceResponse` serialization
4. Implement `FetchResponse` serialization (needs value type detection)
5. Implement `MetadataResponse` / `ListOffsetsResponse` serialization
6. Implement partial failure / 207 logic
7. Wire in response headers (`X-Kafka-Request-ID`, `X-Kafka-MaxWait-Applied`, `Content-Type`)

---

## Skeleton Code

### `HttpResponseSerializer.java`

```java
package kafka.server.http;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.ListOffsetsResponseData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.ListOffsetsResponse;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.ProduceResponse;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Serializes Kafka {@link AbstractResponse} objects into Netty {@link FullHttpResponse}
 * with JSON body.
 *
 * Responsibilities:
 * - Kafka error code -> HTTP status mapping (§11.1)
 * - Value type detection for consumed records (§14.7)
 * - Partial failure -> 207 Multi-Status (§11.2)
 * - Response headers: X-Kafka-Request-ID, X-Kafka-MaxWait-Applied, Retry-After
 *
 * // Time: Created - TASK-B.02
 */
public final class HttpResponseSerializer {

    // --- Jackson ObjectMapper (shared with HttpRequestTranslator if on classpath) ---
    private static final ObjectMapper MAPPER = new ObjectMapper();

    // --- HTTP header names ---
    static final String HEADER_REQUEST_ID = "X-Kafka-Request-ID";
    static final String HEADER_MAX_WAIT_APPLIED = "X-Kafka-MaxWait-Applied";
    static final String HEADER_RETRY_AFTER = "Retry-After";

    // --- Content-type record header name (underscore prefix to avoid HTTP header collision) ---
    static final String RECORD_HEADER_CONTENT_TYPE = "_content-type";
    static final String CONTENT_TYPE_JSON = "application/json";

    // --- DataObject type constants ---
    static final String TYPE_NULL = "NULL";
    static final String TYPE_JSON = "JSON";
    static final String TYPE_STRING = "STRING";
    static final String TYPE_BINARY = "BINARY";

    // --- HTTP 207 Multi-Status (not in Netty's HttpResponseStatus by default) ---
    static final HttpResponseStatus MULTI_STATUS = HttpResponseStatus.valueOf(207);

    private HttpResponseSerializer() {} // utility class

    /**
     * Serializes a Kafka AbstractResponse into a Netty FullHttpResponse.
     *
     * @param kafkaResponse     the Kafka protocol response (ProduceResponse, FetchResponse, etc.)
     * @param requestId         X-Kafka-Request-ID to echo back in response header
     * @param apiKey            the ApiKey of the original request (determines JSON shape)
     * @param effectiveMaxWaitMs applied maxWaitMs cap for FETCH responses (-1 if not applicable)
     * @return FullHttpResponse ready to write to Netty channel
     */
    public static FullHttpResponse serialize(
            AbstractResponse kafkaResponse,
            String requestId,
            ApiKeys apiKey,
            int effectiveMaxWaitMs) {
        Objects.requireNonNull(kafkaResponse, "kafkaResponse");
        Objects.requireNonNull(apiKey, "apiKey");

        // TODO: Switch on apiKey:
        //   PRODUCE      -> serializeProduceResponse((ProduceResponse) kafkaResponse)
        //   FETCH        -> serializeFetchResponse((FetchResponse) kafkaResponse)
        //   METADATA     -> serializeMetadataResponse((MetadataResponse) kafkaResponse)
        //   LIST_OFFSETS -> serializeListOffsetsResponse((ListOffsetsResponse) kafkaResponse)
        //   default      -> serializeGenericResponse(kafkaResponse)
        //
        // TODO: Build FullHttpResponse with:
        //   - HTTP status from determineOverallStatus() or mapErrorToHttpStatus()
        //   - Content-Type: application/json
        //   - X-Kafka-Request-ID: requestId
        //   - X-Kafka-MaxWait-Applied: effectiveMaxWaitMs (if >= 0 and apiKey == FETCH)
        //   - Retry-After: retryAfterSeconds(error) (if applicable)
        //   - Connection: keep-alive
        throw new UnsupportedOperationException("Not yet implemented");
    }

    /**
     * Serializes a ProduceResponse to JSON.
     *
     * Output shape:
     * {
     *   "offsets": [
     *     { "partition": 0, "offset": 10042, "errorCode": 0, "errorMessage": null }
     *   ]
     * }
     */
    static ObjectNode serializeProduceResponse(ProduceResponse response) {
        // TODO: Iterate response.data().responses() -> topicProduceResponse
        //       For each partition: extract partition, baseOffset, errorCode, errorMessage
        //       Build JSON array of offset entries
        throw new UnsupportedOperationException("Not yet implemented");
    }

    /**
     * Serializes a FetchResponse to JSON with value type detection.
     *
     * Output shape:
     * {
     *   "partitions": [
     *     {
     *       "partition": 0,
     *       "highWatermark": 10050,
     *       "records": [
     *         {
     *           "offset": 100,
     *           "timestamp": 1713260400000,
     *           "key": { "type": "STRING", "data": "order-123" },
     *           "value": { "type": "JSON", "data": { "amount": 42.0 } },
     *           "headers": [{ "name": "source", "value": "checkout-service" }]
     *         }
     *       ],
     *       "errorCode": 0, "errorMessage": null
     *     }
     *   ]
     * }
     */
    static ObjectNode serializeFetchResponse(FetchResponse response) {
        // TODO: Iterate response.data().responses() -> topicResponse -> partitions
        //       For each partition:
        //         - Extract highWatermark, errorCode
        //         - Iterate MemoryRecords -> RecordBatch -> Record
        //         - For each record: detectAndSerializeValue() for key and value
        //         - Serialize record headers
        throw new UnsupportedOperationException("Not yet implemented");
    }

    /**
     * Serializes a MetadataResponse to JSON.
     *
     * Output shape for single topic:
     * {
     *   "topic": "orders",
     *   "partitions": [
     *     {
     *       "partition": 0,
     *       "leader": { "brokerId": 1, "host": "broker1", "port": 9092 },
     *       "replicas": [1, 2, 3],
     *       "isr": [1, 2, 3]
     *     }
     *   ]
     * }
     *
     * Output shape for all topics:
     * {
     *   "topics": ["orders", "payments", "inventory"]
     * }
     */
    static ObjectNode serializeMetadataResponse(MetadataResponse response) {
        // TODO: implement
        throw new UnsupportedOperationException("Not yet implemented");
    }

    /**
     * Serializes a ListOffsetsResponse to JSON.
     *
     * Output shape:
     * { "partition": 0, "offset": 10042, "timestamp": 1713260400000 }
     */
    static ObjectNode serializeListOffsetsResponse(ListOffsetsResponse response) {
        // TODO: implement
        throw new UnsupportedOperationException("Not yet implemented");
    }

    // --- Error mapping ---

    /**
     * Maps a Kafka Errors enum value to an HTTP status code.
     *
     * NONE -> 200, UNKNOWN_TOPIC_OR_PARTITION -> 404, LEADER_NOT_AVAILABLE -> 503,
     * NOT_LEADER_OR_FOLLOWER -> 503, MESSAGE_TOO_LARGE -> 413, etc.
     *
     * @param error the Kafka error
     * @return corresponding HttpResponseStatus
     */
    static HttpResponseStatus mapErrorToHttpStatus(Errors error) {
        // TODO: Switch on error:
        //   NONE -> OK (200)
        //   UNKNOWN_TOPIC_OR_PARTITION -> NOT_FOUND (404)
        //   LEADER_NOT_AVAILABLE, NOT_LEADER_OR_FOLLOWER,
        //     NOT_ENOUGH_REPLICAS, NOT_ENOUGH_REPLICAS_AFTER_APPEND -> SERVICE_UNAVAILABLE (503)
        //   MESSAGE_TOO_LARGE, RECORD_LIST_TOO_LARGE -> REQUEST_ENTITY_TOO_LARGE (413)
        //   TOPIC_AUTHORIZATION_FAILED, CLUSTER_AUTHORIZATION_FAILED -> FORBIDDEN (403)
        //   INVALID_REQUEST, INVALID_TOPIC_EXCEPTION -> BAD_REQUEST (400)
        //   REQUEST_TIMED_OUT -> GATEWAY_TIMEOUT (504)
        //   THROTTLING_QUOTA_EXCEEDED -> TOO_MANY_REQUESTS (429)
        //   KAFKA_STORAGE_ERROR -> INTERNAL_SERVER_ERROR (500)
        //   default -> INTERNAL_SERVER_ERROR (500)
        throw new UnsupportedOperationException("Not yet implemented");
    }

    /**
     * Returns the Retry-After header value in seconds for retriable errors.
     * Returns -1 for non-retriable errors (no Retry-After header needed).
     *
     * LEADER_NOT_AVAILABLE, NOT_LEADER_OR_FOLLOWER -> 1
     * NOT_ENOUGH_REPLICAS, NOT_ENOUGH_REPLICAS_AFTER_APPEND -> 5
     * REQUEST_TIMED_OUT -> 1
     * THROTTLING_QUOTA_EXCEEDED -> computed from throttleTimeMs
     * All others -> -1
     */
    static int retryAfterSeconds(Errors error) {
        // TODO: implement
        throw new UnsupportedOperationException("Not yet implemented");
    }

    /**
     * Overload for THROTTLING_QUOTA_EXCEEDED that takes the throttle time.
     * Returns Math.max(1, (int) Math.ceil(throttleTimeMs / 1000.0))
     */
    static int retryAfterSeconds(Errors error, int throttleTimeMs) {
        // TODO: implement
        throw new UnsupportedOperationException("Not yet implemented");
    }

    // --- Value type detection ---

    /**
     * Detects the value type of a record's byte[] payload and serializes as DataObject.
     *
     * Algorithm (§14.7):
     * 1. null -> {"type":"NULL"}
     * 2. _content-type: application/json header -> {"type":"JSON","data":<parsed>}
     * 3. Valid UTF-8 without control chars -> {"type":"STRING","data":"..."}
     * 4. Fallback -> {"type":"BINARY","data":"<base64>"}
     *
     * @param valueBytes raw record value bytes (may be null)
     * @param headers    record headers (checked for _content-type)
     * @return ObjectNode representing the DataObject JSON
     */
    static ObjectNode detectAndSerializeValue(byte[] valueBytes, Header[] headers) {
        ObjectNode node = MAPPER.createObjectNode();

        // TODO: Step 1 — null check
        // TODO: Step 2 — check headers for _content-type: application/json
        //                 try parse as JSON; on failure fall through
        // TODO: Step 3 — try UTF-8 decode, check for control chars
        //                 [0x00-0x08, 0x0B, 0x0C, 0x0E-0x1F, 0x7F] -> binary
        //                 allowed: 0x09 (tab), 0x0A (newline), 0x0D (CR)
        // TODO: Step 4 — base64 encode as BINARY
        throw new UnsupportedOperationException("Not yet implemented");
    }

    /**
     * Checks if a string contains C0/C1 control characters that make it unsuitable
     * for STRING type. Allows HT (0x09), LF (0x0A), CR (0x0D).
     *
     * @param s decoded UTF-8 string
     * @return true if the string contains disallowed control characters
     */
    static boolean containsControlChars(String s) {
        // TODO: iterate code points, check ranges
        //       disallowed: 0x00-0x08, 0x0B, 0x0C, 0x0E-0x1F, 0x7F
        throw new UnsupportedOperationException("Not yet implemented");
    }

    /**
     * Checks record headers for a specific header name and returns its value.
     */
    static String getRecordHeader(Header[] headers, String name) {
        // TODO: iterate headers, match by name, return value as UTF-8 string
        throw new UnsupportedOperationException("Not yet implemented");
    }

    // --- Partial failure ---

    /**
     * Determines the overall HTTP status for a response with potentially mixed errors.
     *
     * - All NONE -> 200
     * - All same non-NONE error -> that error's HTTP status
     * - Mixed (some success, some failure) -> 207 Multi-Status
     *
     * @param errors collection of per-partition Errors values
     * @return the appropriate HTTP status
     */
    static HttpResponseStatus determineOverallStatus(Collection<Errors> errors) {
        // TODO: implement
        throw new UnsupportedOperationException("Not yet implemented");
    }

    // --- Error body builder ---

    /**
     * Builds a standalone JSON error response body.
     *
     * { "errorCode": 3, "errorMessage": "UNKNOWN_TOPIC_OR_PARTITION", "detail": "..." }
     */
    static String buildErrorBody(Errors error, String detail) {
        // TODO: implement
        throw new UnsupportedOperationException("Not yet implemented");
    }

    // --- Response builder helper ---

    /**
     * Builds a FullHttpResponse from a JSON body and status, with standard headers.
     */
    static FullHttpResponse buildResponse(HttpResponseStatus status, String jsonBody,
                                          String requestId, int effectiveMaxWaitMs) {
        // TODO:
        // 1. Create DefaultFullHttpResponse(HTTP_1_1, status, Unpooled.copiedBuffer(jsonBody, UTF_8))
        // 2. Set Content-Type: application/json
        // 3. Set Content-Length
        // 4. Set X-Kafka-Request-ID: requestId
        // 5. If effectiveMaxWaitMs >= 0: set X-Kafka-MaxWait-Applied
        // 6. Set Connection: keep-alive
        // 7. Return response
        throw new UnsupportedOperationException("Not yet implemented");
    }
}
```

### Test class — `HttpResponseSerializerTest.java`

```java
package kafka.server.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.protocol.Errors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * // Time: Created - TASK-B.02
 */
class HttpResponseSerializerTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // --- Error to HTTP status mapping ---

    @Test
    void mapError_none_returns200() {
        assertEquals(HttpResponseStatus.OK,
            HttpResponseSerializer.mapErrorToHttpStatus(Errors.NONE));
    }

    @Test
    void mapError_unknownTopicOrPartition_returns404() {
        assertEquals(HttpResponseStatus.NOT_FOUND,
            HttpResponseSerializer.mapErrorToHttpStatus(Errors.UNKNOWN_TOPIC_OR_PARTITION));
    }

    @Test
    void mapError_leaderNotAvailable_returns503() {
        assertEquals(HttpResponseStatus.SERVICE_UNAVAILABLE,
            HttpResponseSerializer.mapErrorToHttpStatus(Errors.LEADER_NOT_AVAILABLE));
    }

    @Test
    void mapError_messageTooLarge_returns413() {
        assertEquals(HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE,
            HttpResponseSerializer.mapErrorToHttpStatus(Errors.MESSAGE_TOO_LARGE));
    }

    @Test
    void mapError_topicAuthorizationFailed_returns403() {
        assertEquals(HttpResponseStatus.FORBIDDEN,
            HttpResponseSerializer.mapErrorToHttpStatus(Errors.TOPIC_AUTHORIZATION_FAILED));
    }

    @Test
    void mapError_requestTimedOut_returns504() {
        assertEquals(HttpResponseStatus.GATEWAY_TIMEOUT,
            HttpResponseSerializer.mapErrorToHttpStatus(Errors.REQUEST_TIMED_OUT));
    }

    @Test
    void mapError_throttlingQuotaExceeded_returns429() {
        assertEquals(HttpResponseStatus.TOO_MANY_REQUESTS,
            HttpResponseSerializer.mapErrorToHttpStatus(Errors.THROTTLING_QUOTA_EXCEEDED));
    }

    @Test
    void mapError_unknownError_returns500() {
        assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR,
            HttpResponseSerializer.mapErrorToHttpStatus(Errors.UNKNOWN_SERVER_ERROR));
    }

    // --- Retry-After ---

    @Test
    void retryAfter_leaderNotAvailable_returns1() {
        assertEquals(1, HttpResponseSerializer.retryAfterSeconds(Errors.LEADER_NOT_AVAILABLE));
    }

    @Test
    void retryAfter_notEnoughReplicas_returns5() {
        assertEquals(5, HttpResponseSerializer.retryAfterSeconds(Errors.NOT_ENOUGH_REPLICAS));
    }

    @Test
    void retryAfter_requestTimedOut_returns1() {
        assertEquals(1, HttpResponseSerializer.retryAfterSeconds(Errors.REQUEST_TIMED_OUT));
    }

    @Test
    void retryAfter_noRetry_returnsMinusOne() {
        assertEquals(-1, HttpResponseSerializer.retryAfterSeconds(Errors.INVALID_REQUEST));
    }

    @Test
    void retryAfter_throttling_ceilsToSeconds() {
        // 500ms -> ceil(0.5) = 1 second
        assertEquals(1, HttpResponseSerializer.retryAfterSeconds(
            Errors.THROTTLING_QUOTA_EXCEEDED, 500));
        // 1500ms -> ceil(1.5) = 2 seconds
        assertEquals(2, HttpResponseSerializer.retryAfterSeconds(
            Errors.THROTTLING_QUOTA_EXCEEDED, 1500));
        // 0ms -> max(1, 0) = 1 second minimum
        assertEquals(1, HttpResponseSerializer.retryAfterSeconds(
            Errors.THROTTLING_QUOTA_EXCEEDED, 0));
    }

    // --- Value type detection ---

    @Test
    void detectValue_null_returnsNullType() {
        ObjectNode result = HttpResponseSerializer.detectAndSerializeValue(null, new Header[0]);
        assertEquals("NULL", result.get("type").asText());
        assertFalse(result.has("data"));
    }

    @Test
    void detectValue_jsonContentType_returnsJsonType() {
        byte[] json = "{\"amount\":42.0}".getBytes(StandardCharsets.UTF_8);
        Header[] headers = { new RecordHeader("_content-type", "application/json".getBytes()) };
        ObjectNode result = HttpResponseSerializer.detectAndSerializeValue(json, headers);
        assertEquals("JSON", result.get("type").asText());
        assertEquals(42.0, result.get("data").get("amount").asDouble());
    }

    @Test
    void detectValue_jsonContentTypeInvalidJson_fallsToString() {
        byte[] notJson = "not json".getBytes(StandardCharsets.UTF_8);
        Header[] headers = { new RecordHeader("_content-type", "application/json".getBytes()) };
        ObjectNode result = HttpResponseSerializer.detectAndSerializeValue(notJson, headers);
        // Falls through to STRING since it's valid UTF-8
        assertEquals("STRING", result.get("type").asText());
        assertEquals("not json", result.get("data").asText());
    }

    @Test
    void detectValue_validUtf8_returnsStringType() {
        byte[] text = "Hello, world!".getBytes(StandardCharsets.UTF_8);
        ObjectNode result = HttpResponseSerializer.detectAndSerializeValue(text, new Header[0]);
        assertEquals("STRING", result.get("type").asText());
        assertEquals("Hello, world!", result.get("data").asText());
    }

    @Test
    void detectValue_utf8WithTab_returnsStringType() {
        byte[] text = "col1\tcol2".getBytes(StandardCharsets.UTF_8);
        ObjectNode result = HttpResponseSerializer.detectAndSerializeValue(text, new Header[0]);
        assertEquals("STRING", result.get("type").asText());
    }

    @Test
    void detectValue_utf8WithControlChars_returnsBinaryType() {
        byte[] data = new byte[] { 0x01, 0x02, 0x03 };  // control chars
        ObjectNode result = HttpResponseSerializer.detectAndSerializeValue(data, new Header[0]);
        assertEquals("BINARY", result.get("type").asText());
        assertEquals(Base64.getEncoder().encodeToString(data), result.get("data").asText());
    }

    @Test
    void detectValue_invalidUtf8_returnsBinaryType() {
        byte[] data = new byte[] { (byte) 0xFF, (byte) 0xFE };  // invalid UTF-8
        ObjectNode result = HttpResponseSerializer.detectAndSerializeValue(data, new Header[0]);
        assertEquals("BINARY", result.get("type").asText());
    }

    // --- Control char detection ---

    @Test
    void containsControlChars_normalText_returnsFalse() {
        assertFalse(HttpResponseSerializer.containsControlChars("Hello, world!"));
    }

    @Test
    void containsControlChars_withTab_returnsFalse() {
        assertFalse(HttpResponseSerializer.containsControlChars("col1\tcol2"));
    }

    @Test
    void containsControlChars_withNewline_returnsFalse() {
        assertFalse(HttpResponseSerializer.containsControlChars("line1\nline2"));
    }

    @Test
    void containsControlChars_withNullByte_returnsTrue() {
        assertTrue(HttpResponseSerializer.containsControlChars("hello\0world"));
    }

    @Test
    void containsControlChars_withBell_returnsTrue() {
        assertTrue(HttpResponseSerializer.containsControlChars("hello\u0007world"));
    }

    // --- Partial failure / 207 ---

    @Test
    void determineOverallStatus_allNone_returns200() {
        assertEquals(HttpResponseStatus.OK,
            HttpResponseSerializer.determineOverallStatus(List.of(Errors.NONE, Errors.NONE)));
    }

    @Test
    void determineOverallStatus_allSameError_returnsThatError() {
        assertEquals(HttpResponseStatus.NOT_FOUND,
            HttpResponseSerializer.determineOverallStatus(
                List.of(Errors.UNKNOWN_TOPIC_OR_PARTITION, Errors.UNKNOWN_TOPIC_OR_PARTITION)));
    }

    @Test
    void determineOverallStatus_mixed_returns207() {
        assertEquals(HttpResponseSerializer.MULTI_STATUS,
            HttpResponseSerializer.determineOverallStatus(
                List.of(Errors.NONE, Errors.UNKNOWN_TOPIC_OR_PARTITION)));
    }

    @Test
    void determineOverallStatus_singleSuccess_returns200() {
        assertEquals(HttpResponseStatus.OK,
            HttpResponseSerializer.determineOverallStatus(List.of(Errors.NONE)));
    }

    // --- Error body ---

    @Test
    void buildErrorBody_includesAllFields() throws Exception {
        String body = HttpResponseSerializer.buildErrorBody(
            Errors.UNKNOWN_TOPIC_OR_PARTITION, "Topic 'orders' not found");
        var node = MAPPER.readTree(body);
        assertEquals(3, node.get("errorCode").asInt());
        assertEquals("UNKNOWN_TOPIC_OR_PARTITION", node.get("errorMessage").asText());
        assertEquals("Topic 'orders' not found", node.get("detail").asText());
    }
}
```

### Existing pattern reference

```java
// From clients/src/main/java/org/apache/kafka/common/protocol/Errors.java — pattern for error code mapping:
public enum Errors {
    UNKNOWN_SERVER_ERROR(-1, "The server experienced an unexpected error when processing the request.", ...),
    NONE(0, null, ...),
    OFFSET_OUT_OF_RANGE(1, "The requested offset is not within the range of offsets...", ...),
    // Each entry: code (short), message (String), exceptionSupplier
    UNKNOWN_TOPIC_OR_PARTITION(3, "This server does not host this topic-partition.", ...),
    // ...
    public short code() { return code; }
    public String message() { return message; }
}
```

---

## Tests

**Test class:** `http-server/src/test/java/kafka/server/http/HttpResponseSerializerTest.java`

| Test method | What it verifies |
|-------------|-----------------|
| `mapError_none_returns200` | NONE -> 200 OK |
| `mapError_unknownTopicOrPartition_returns404` | Error 3 -> 404 |
| `mapError_leaderNotAvailable_returns503` | Transient error -> 503 |
| `mapError_messageTooLarge_returns413` | Size error -> 413 |
| `mapError_topicAuthorizationFailed_returns403` | Auth error -> 403 |
| `mapError_requestTimedOut_returns504` | Timeout -> 504 |
| `mapError_throttlingQuotaExceeded_returns429` | Throttle -> 429 |
| `retryAfter_*` | Retry-After header values for different errors |
| `detectValue_null_returnsNullType` | null -> NULL type |
| `detectValue_jsonContentType_returnsJsonType` | _content-type header -> JSON type |
| `detectValue_jsonContentTypeInvalidJson_fallsToString` | Bad JSON with header -> falls to STRING |
| `detectValue_validUtf8_returnsStringType` | Clean UTF-8 -> STRING type |
| `detectValue_utf8WithTab_returnsStringType` | Tab is allowed in STRING |
| `detectValue_utf8WithControlChars_returnsBinaryType` | Control chars -> BINARY type |
| `detectValue_invalidUtf8_returnsBinaryType` | Invalid UTF-8 -> BINARY type |
| `containsControlChars_*` | Control character detection edge cases |
| `determineOverallStatus_allNone_returns200` | All success -> 200 |
| `determineOverallStatus_mixed_returns207` | Mixed results -> 207 |
| `buildErrorBody_includesAllFields` | Error body has errorCode, errorMessage, detail |

**Run command:**
```bash
./gradlew :http-server:test --tests "kafka.server.http.HttpResponseSerializerTest"
```

---

## Rules

- Value type detection algorithm must follow the exact 4-step sequence from design doc section 14.7. Do not reorder or skip steps.
- The `_content-type` record header has an underscore prefix — do not confuse with HTTP `Content-Type`.
- UTF-8 control char check must ALLOW tab (0x09), newline (0x0A), carriage return (0x0D).
- `Retry-After` for `THROTTLING_QUOTA_EXCEEDED` uses `Math.max(1, (int) Math.ceil(throttleTimeMs / 1000.0))` — always at least 1 second.
- 207 Multi-Status is used ONLY when a multi-partition response has mixed success/failure. Single-partition responses use the direct HTTP status.

---

## Learning

- The Kafka `Record` interface returns `ByteBuffer` for key/value (not `byte[]`), requiring a `bufferToBytes()` conversion helper for the value type detection algorithm which operates on `byte[]`.
- The `http-server` module had no import-control checkstyle config and the package `kafka.server.http` is outside the default `org.apache.kafka` root package in `import-control.xml`. A new `import-control-http-server.xml` was needed.
- Checkstyle enforces `BooleanExpressionComplexity` max=5, so the control character range check (6 conditions) had to be extracted into a separate `isDisallowedControlChar()` helper method.
- The generated Kafka message data classes (e.g., `ProduceResponseData`, `FetchResponseData`) live in the build directory under `clients/build/generated/` and are not in the source tree.

---

## Limitations

- The `serialize()` method currently only handles PRODUCE, FETCH, METADATA, and LIST_OFFSETS api keys. Other api keys fall through to a generic serializer that only outputs apiKey name and throttleTimeMs.
- `ListOffsetsResponse` serialization returns only the first partition's data, assuming single-partition use for the HTTP API. Multi-partition ListOffsets responses will lose data.
- `FetchResponse` serialization casts `BaseRecords` to `MemoryRecords` and silently skips records if the cast fails (e.g., for `FileRecords` or other `BaseRecords` implementations).

---

## Field Notes

- The TASK-A.02 cherry-pick was required since this worktree branched off `trunk` where the http-server module did not exist yet. The commit `e749d41a3e` provided the Gradle submodule config.
- Checkstyle `NoWhitespaceAfter` rejects `{ new Foo() }` array initializer style; must use `{new Foo()}` with no spaces after `{`.
- Kafka `Errors.name()` returns the enum constant name (e.g., `"UNKNOWN_TOPIC_OR_PARTITION"`), while `Errors.message()` returns the human-readable description string. The error body uses `name()` for the `errorMessage` field per the task spec pattern.

---

## Acceptance Criteria

- [x] `./gradlew :http-server:test --tests "kafka.server.http.HttpResponseSerializerTest"` exits 0
- [x] `HttpResponseSerializer.java` exists at `http-server/src/main/java/kafka/server/http/HttpResponseSerializer.java`
- [x] All Kafka errors from the mapping table are handled in `mapErrorToHttpStatus()`
- [x] `Retry-After` headers are set for 429, 503, 504 responses
- [x] Value type detection follows the 4-step algorithm exactly
- [x] 207 Multi-Status is returned for mixed success/failure responses
- [x] `X-Kafka-Request-ID` header is set on every response
- [x] `X-Kafka-MaxWait-Applied` header is set on FETCH responses
- [x] Learning section filled with at least one entry
- [x] Limitations section filled (use "None" if truly none)
- [x] File Manifest section updated after commit

---

## File Manifest

### 2026-04-16 — Implement HttpResponseSerializer (commit ae0db830eb)
Created:
  - http-server/src/main/java/kafka/server/http/HttpResponseSerializer.java — Kafka response to JSON serialization with error mapping, value type detection, 207 Multi-Status
  - http-server/src/test/java/kafka/server/http/HttpResponseSerializerTest.java — 30 unit tests covering error mapping, retry-after, value type detection, control chars, partial failure, error body
  - checkstyle/import-control-http-server.xml — Checkstyle import control for kafka.server.http package
Modified:
  - build.gradle — Added checkstyle configProperties for http-server module
