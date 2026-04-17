# TASK-WS1.10: WsMessageDeserializer — Kafka ConsumerRecord to Deliver Frame

## Prerequisites

- None. WsMessageDeserializer is a pure transformation class with no dependencies on other WS classes.

---

## Context

When the broker pushes a message to a WebSocket subscriber, it must convert a Kafka `ConsumerRecord` into a JSON `deliver` frame. The `WsMessageDeserializer` performs this reverse transformation — it reconstructs the routing metadata and AMQP content properties from Kafka record headers.

This is the inverse of `WsMessageSerializer` (TASK-WS1.09). Together, they enable lossless round-trip: a message published via WebSocket → serialized to Kafka record → deserialized back to deliver frame with all original properties intact.

The design doc (§13.3) specifies the header mapping. Headers with the `_ws_*` prefix contain the original routing metadata. If these headers are absent (e.g., the Kafka record was produced by a Kafka binary client, not via WebSocket), the deserializer synthesizes reasonable defaults from available Kafka record metadata (topic name, key, timestamp).

The deliver frame JSON structure includes: type, subscriptionId, deliveryTag, redelivered flag, exchange, routingKey, the reconstructed message object, and Kafka-specific fields (partition, offset, kafkaTimestamp).

---

## Specification

### `WsMessageDeserializer` — `kafka.server.http.ws.WsMessageDeserializer`

```java
/**
 * Converts a Kafka ConsumerRecord into a deliver frame JSON structure.
 *
 * Reconstructs routing metadata and all 13 AMQP content properties from
 * _ws_* Kafka headers. If headers are absent (cross-protocol message),
 * synthesizes from available Kafka metadata.
 */
public final class WsMessageDeserializer {

    /**
     * Deserialized deliver frame components.
     */
    public record DeliverFrame(
        String exchange,          // from _ws_exchange header or ""
        String routingKey,        // from _ws_routing_key header or record key
        JsonNode message,         // reconstructed message object
        int partition,
        long offset,
        long kafkaTimestamp
    ) {}

    /**
     * Deserializes a Kafka record into deliver frame components.
     *
     * @param key       record key bytes (may be null)
     * @param value     record value bytes
     * @param headers   Kafka record headers
     * @param partition partition number
     * @param offset    record offset
     * @param timestamp Kafka record timestamp
     * @return DeliverFrame with all reconstructed fields
     */
    public DeliverFrame deserialize(byte[] key, byte[] value,
                                    Iterable<Header> headers,
                                    int partition, long offset, long timestamp);
}
```

### Deliver frame JSON structure (from design doc)

```json
{
  "type": "deliver",
  "subscriptionId": "sub-1",
  "deliveryTag": 42,
  "redelivered": false,
  "exchange": "events",
  "routingKey": "order.created",
  "message": {
    "body": { "orderId": "123" },
    "contentType": "application/json",
    "headers": { "source": "web" },
    "deliveryMode": 2,
    "correlationId": "req-123",
    "replyTo": "replies",
    "messageId": "msg-abc",
    "contentEncoding": "utf-8",
    "priority": 0,
    "type": "OrderCreated",
    "userId": "checkout-svc",
    "appId": "checkout-service",
    "expiration": "60000"
  },
  "partition": 0,
  "offset": 42,
  "kafkaTimestamp": 1713260400000
}
```

### Behavioral contracts

- If `_ws_exchange` header present: use its value as `exchange`. Otherwise: `""` (empty string).
- If `_ws_routing_key` header present: use its value as `routingKey`. Otherwise: use `key` as UTF-8 string if non-null, or `""`.
- Body deserialization:
  - If `_content-type` is `application/json`: parse value as JSON
  - If `_content-type` is `application/octet-stream`: Base64-encode value and return as string
  - Otherwise: return value as UTF-8 string
  - If value is null: body is JSON null
- All 13 AMQP properties reconstructed from `_ws_*` headers. Missing properties → null (omitted from JSON).
- `_ws_headers` (application headers): JSON-deserialized back to an object node.
- Cross-protocol synthesis: when no `_ws_*` headers present, produce a minimal message object with body only.

---

## Implementation Details

**Module:** `http-server`

**Files to study:**

| File | Why |
|------|-----|
| `ivy-docs/http-protocol-extend-design.md` §13.3 | Header mapping table |
| `ivy-docs/http-protocol-extend-design.md` §9 (§8.1) | Deliver frame structure |
| `http-server/src/main/java/kafka/server/http/HttpRequestTranslator.java` lines 76-88 | Jackson ObjectMapper pattern |

**Files to create:**

| File | What it does |
|------|--------------|
| `http-server/src/main/java/kafka/server/http/ws/WsMessageDeserializer.java` | Kafka record → deliver frame components |
| `http-server/src/test/java/kafka/server/http/ws/WsMessageDeserializerTest.java` | Unit tests |

> **CRITICAL:** When parsing the value as JSON (for `application/json` content type), wrap in try-catch. If JSON parsing fails, fall back to treating the value as a UTF-8 string. This prevents delivery failures for corrupted or non-JSON data.

> **CRITICAL:** The `_ws_headers` header value is a JSON string. Deserialize it to an `ObjectNode` using `MAPPER.readTree()`. If deserialization fails, set headers to empty object — do not fail the deliver.

**Implementation order:**
1. Create `WsMessageDeserializer.java` with header extraction helpers
2. Implement body deserialization (JSON, string, binary)
3. Implement AMQP property reconstruction from headers
4. Implement cross-protocol fallback (no `_ws_*` headers)
5. Write comprehensive tests

---

## Skeleton Code

### `WsMessageDeserializer.java`

```java
package kafka.server.http.ws;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import kafka.server.http.HttpRequestTranslator;
import org.apache.kafka.common.header.Header;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Objects;

/**
 * Converts a Kafka ConsumerRecord into deliver frame components.
 *
 * Reconstructs routing metadata and all 13 AMQP content properties from
 * _ws_* Kafka record headers. For cross-protocol records (no _ws_* headers),
 * synthesizes minimal fields from Kafka metadata.
 *
 * Thread-safe: stateless, uses shared ObjectMapper.
 *
 * // Time: Created - TASK-WS1.10
 */
public final class WsMessageDeserializer {

    private static final ObjectMapper MAPPER = HttpRequestTranslator.MAPPER;

    /**
     * Deserialized deliver frame components.
     */
    public record DeliverFrame(
        String exchange,
        String routingKey,
        JsonNode message,
        int partition,
        long offset,
        long kafkaTimestamp
    ) {}

    /**
     * Deserializes a Kafka record into deliver frame components.
     *
     * @param key       record key bytes (may be null)
     * @param value     record value bytes (may be null)
     * @param headers   Kafka record headers
     * @param partition partition number
     * @param offset    record offset
     * @param timestamp Kafka record timestamp
     * @return DeliverFrame with all reconstructed fields
     */
    public DeliverFrame deserialize(byte[] key, byte[] value,
                                    Iterable<Header> headers,
                                    int partition, long offset, long timestamp) {
        Objects.requireNonNull(headers, "headers");

        // TODO: 1. Extract all _ws_* headers into a local map for easy lookup
        // TODO: 2. Resolve exchange: _ws_exchange header or ""
        // TODO: 3. Resolve routingKey: _ws_routing_key header, or key as UTF-8, or ""
        // TODO: 4. Resolve contentType: _content-type header or null
        // TODO: 5. Deserialize body based on contentType:
        //          - application/json → parse as JSON
        //          - application/octet-stream → Base64 encode
        //          - null/other → UTF-8 string (try JSON parse first)
        //          - null value → NullNode
        // TODO: 6. Build message ObjectNode with body and all 13 AMQP properties from headers
        // TODO: 7. Reconstruct _ws_headers → "headers" ObjectNode
        // TODO: 8. Return DeliverFrame
        throw new UnsupportedOperationException("Not yet implemented");
    }

    /**
     * Extracts a header value as a UTF-8 string.
     *
     * @return header value or null if not found
     */
    private String extractHeader(Iterable<Header> headers, String name) {
        for (Header h : headers) {
            if (h.key().equals(name)) {
                return new String(h.value(), StandardCharsets.UTF_8);
            }
        }
        return null;
    }

    /**
     * Deserializes the record value to a JSON body node.
     */
    private JsonNode deserializeBody(byte[] value, String contentType) {
        // TODO: implement
        throw new UnsupportedOperationException("Not yet implemented");
    }

    /**
     * Adds a string property to the message node if the header value is non-null.
     */
    private void addIfPresent(ObjectNode message, String jsonField, String headerValue) {
        if (headerValue != null) {
            message.put(jsonField, headerValue);
        }
    }

    /**
     * Adds an integer property to the message node if the header value is non-null.
     */
    private void addIntIfPresent(ObjectNode message, String jsonField, String headerValue) {
        if (headerValue != null) {
            try {
                message.put(jsonField, Integer.parseInt(headerValue));
            } catch (NumberFormatException e) {
                message.put(jsonField, headerValue);
            }
        }
    }
}
```

### Test class — `WsMessageDeserializerTest.java`

```java
package kafka.server.http.ws;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.server.http.HttpRequestTranslator;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * // Time: Created - TASK-WS1.10
 */
class WsMessageDeserializerTest {

    private static final ObjectMapper MAPPER = HttpRequestTranslator.MAPPER;
    private WsMessageDeserializer deserializer;

    @BeforeEach
    void setUp() {
        deserializer = new WsMessageDeserializer();
    }

    // --- Routing metadata reconstruction ---

    @Test
    void deserialize_wsExchangeHeader_usedAsExchange() {
        List<Header> headers = new ArrayList<>();
        headers.add(header(WsMessageSerializer.HDR_EXCHANGE, "events"));
        headers.add(header(WsMessageSerializer.HDR_ROUTING_KEY, "order.created"));

        var result = deserializer.deserialize(
            "order.created".getBytes(), "{}".getBytes(), headers, 0, 42, 1000L);
        assertEquals("events", result.exchange());
        assertEquals("order.created", result.routingKey());
    }

    @Test
    void deserialize_noWsHeaders_synthesizesDefaults() {
        List<Header> headers = new ArrayList<>();

        var result = deserializer.deserialize(
            "my-key".getBytes(), "hello".getBytes(), headers, 0, 42, 1000L);
        assertEquals("", result.exchange());
        assertEquals("my-key", result.routingKey());
    }

    @Test
    void deserialize_noKeyNoHeaders_emptyRoutingKey() {
        List<Header> headers = new ArrayList<>();

        var result = deserializer.deserialize(
            null, "hello".getBytes(), headers, 0, 42, 1000L);
        assertEquals("", result.routingKey());
    }

    // --- Body deserialization ---

    @Test
    void deserialize_jsonContentType_parsesAsJson() {
        List<Header> headers = new ArrayList<>();
        headers.add(header(WsMessageSerializer.HDR_CONTENT_TYPE, "application/json"));

        var result = deserializer.deserialize(
            null, "{\"orderId\":\"123\"}".getBytes(), headers, 0, 42, 1000L);
        assertEquals("123", result.message().get("body").get("orderId").asText());
    }

    @Test
    void deserialize_octetStreamContentType_base64Encodes() {
        List<Header> headers = new ArrayList<>();
        headers.add(header(WsMessageSerializer.HDR_CONTENT_TYPE, "application/octet-stream"));
        byte[] binary = new byte[]{0x01, 0x02, 0x03};

        var result = deserializer.deserialize(null, binary, headers, 0, 42, 1000L);
        String bodyStr = result.message().get("body").asText();
        assertArrayEquals(binary, Base64.getDecoder().decode(bodyStr));
    }

    @Test
    void deserialize_noContentType_treatsAsString() {
        List<Header> headers = new ArrayList<>();

        var result = deserializer.deserialize(
            null, "plain text".getBytes(), headers, 0, 42, 1000L);
        assertEquals("plain text", result.message().get("body").asText());
    }

    @Test
    void deserialize_nullValue_bodyIsNull() {
        List<Header> headers = new ArrayList<>();

        var result = deserializer.deserialize(null, null, headers, 0, 42, 1000L);
        assertTrue(result.message().get("body").isNull());
    }

    @Test
    void deserialize_invalidJson_fallsBackToString() {
        List<Header> headers = new ArrayList<>();
        headers.add(header(WsMessageSerializer.HDR_CONTENT_TYPE, "application/json"));

        var result = deserializer.deserialize(
            null, "not json {".getBytes(), headers, 0, 42, 1000L);
        assertEquals("not json {", result.message().get("body").asText());
    }

    // --- AMQP property reconstruction ---

    @Test
    void deserialize_allAmqpProperties_reconstructed() {
        List<Header> headers = new ArrayList<>();
        headers.add(header(WsMessageSerializer.HDR_EXCHANGE, "events"));
        headers.add(header(WsMessageSerializer.HDR_ROUTING_KEY, "key"));
        headers.add(header(WsMessageSerializer.HDR_CONTENT_TYPE, "text/plain"));
        headers.add(header(WsMessageSerializer.HDR_CONTENT_ENCODING, "utf-8"));
        headers.add(header(WsMessageSerializer.HDR_DELIVERY_MODE, "2"));
        headers.add(header(WsMessageSerializer.HDR_PRIORITY, "5"));
        headers.add(header(WsMessageSerializer.HDR_CORRELATION_ID, "corr-1"));
        headers.add(header(WsMessageSerializer.HDR_REPLY_TO, "reply-q"));
        headers.add(header(WsMessageSerializer.HDR_EXPIRATION, "60000"));
        headers.add(header(WsMessageSerializer.HDR_MESSAGE_ID, "msg-1"));
        headers.add(header(WsMessageSerializer.HDR_TYPE, "OrderCreated"));
        headers.add(header(WsMessageSerializer.HDR_USER_ID, "svc-user"));
        headers.add(header(WsMessageSerializer.HDR_APP_ID, "checkout"));

        var result = deserializer.deserialize(
            null, "test".getBytes(), headers, 3, 100, 5000L);

        JsonNode msg = result.message();
        assertEquals("text/plain", msg.get("contentType").asText());
        assertEquals("utf-8", msg.get("contentEncoding").asText());
        assertEquals(2, msg.get("deliveryMode").asInt());
        assertEquals(5, msg.get("priority").asInt());
        assertEquals("corr-1", msg.get("correlationId").asText());
        assertEquals("reply-q", msg.get("replyTo").asText());
        assertEquals("60000", msg.get("expiration").asText());
        assertEquals("msg-1", msg.get("messageId").asText());
        assertEquals("OrderCreated", msg.get("type").asText());
        assertEquals("svc-user", msg.get("userId").asText());
        assertEquals("checkout", msg.get("appId").asText());
    }

    @Test
    void deserialize_applicationHeaders_reconstructed() throws Exception {
        List<Header> headers = new ArrayList<>();
        headers.add(header(WsMessageSerializer.HDR_HEADERS,
            "{\"source\":\"web\",\"priority\":\"high\"}"));

        var result = deserializer.deserialize(
            null, "test".getBytes(), headers, 0, 0, 0L);

        JsonNode appHeaders = result.message().get("headers");
        assertNotNull(appHeaders);
        assertEquals("web", appHeaders.get("source").asText());
        assertEquals("high", appHeaders.get("priority").asText());
    }

    @Test
    void deserialize_invalidApplicationHeaders_emptyObject() {
        List<Header> headers = new ArrayList<>();
        headers.add(header(WsMessageSerializer.HDR_HEADERS, "not valid json {"));

        var result = deserializer.deserialize(
            null, "test".getBytes(), headers, 0, 0, 0L);

        JsonNode appHeaders = result.message().get("headers");
        assertTrue(appHeaders == null || appHeaders.isEmpty());
    }

    // --- Kafka metadata ---

    @Test
    void deserialize_kafkaMetadata_preserved() {
        List<Header> headers = new ArrayList<>();

        var result = deserializer.deserialize(
            null, "test".getBytes(), headers, 5, 999, 1713260400000L);
        assertEquals(5, result.partition());
        assertEquals(999, result.offset());
        assertEquals(1713260400000L, result.kafkaTimestamp());
    }

    // --- Round-trip ---

    @Test
    void roundTrip_serializeThenDeserialize_lossless() throws Exception {
        var serializer = new WsMessageSerializer();
        ObjectMapper mapper = HttpRequestTranslator.MAPPER;

        var msgNode = mapper.createObjectNode();
        msgNode.putObject("body").put("orderId", "123");
        msgNode.put("contentType", "application/json");
        msgNode.put("correlationId", "corr-abc");
        msgNode.put("messageId", "msg-xyz");
        msgNode.put("deliveryMode", 2);

        var serialized = serializer.serialize("events", "order.created", msgNode, "/prod");

        var deserialized = deserializer.deserialize(
            serialized.key(), serialized.value(), serialized.headers(), 0, 100, 5000L);

        assertEquals("events", deserialized.exchange());
        assertEquals("order.created", deserialized.routingKey());
        assertEquals("corr-abc", deserialized.message().get("correlationId").asText());
        assertEquals("msg-xyz", deserialized.message().get("messageId").asText());
    }

    // --- Helper ---

    private static Header header(String name, String value) {
        return new RecordHeader(name, value.getBytes(StandardCharsets.UTF_8));
    }
}
```

### Existing pattern reference

```java
// Design doc §13.3 — Header mapping table:
// | Publish field         | Kafka record field               |
// |---|---|
// | message.body          | value                            |
// | routingKey             | key (for murmur2 hash)           |
// | exchange               | headers["_ws_exchange"]          |
// | routingKey             | headers["_ws_routing_key"]       |
// | message.contentType    | headers["_content-type"]         |
// | message.headers        | headers["_ws_headers"]           |
// | message.deliveryMode   | headers["_ws_delivery_mode"]     |
// | message.correlationId  | headers["_ws_correlation_id"]    |
// | message.replyTo        | headers["_ws_reply_to"]          |
// | message.messageId      | headers["_ws_message_id"]        |
// | message.contentEncoding| headers["_ws_content_encoding"]  |
// | message.priority       | headers["_ws_priority"]          |
// | message.type           | headers["_ws_type"]              |
// | message.userId         | headers["_ws_user_id"]           |
// | message.appId          | headers["_ws_app_id"]            |
// | message.expiration     | headers["_ws_expiration"]        |
// | vhost                  | headers["_ws_vhost"]             |
```

---

## Tests

**Test class:** `http-server/src/test/java/kafka/server/http/ws/WsMessageDeserializerTest.java`

| Test method | What it verifies |
|-------------|-----------------|
| `deserialize_wsExchangeHeader_usedAsExchange` | _ws_exchange → exchange field |
| `deserialize_noWsHeaders_synthesizesDefaults` | Cross-protocol fallback: key → routingKey |
| `deserialize_noKeyNoHeaders_emptyRoutingKey` | Null key + no headers → empty routing key |
| `deserialize_jsonContentType_parsesAsJson` | JSON body parsed to JsonNode |
| `deserialize_octetStreamContentType_base64Encodes` | Binary value → Base64 string |
| `deserialize_noContentType_treatsAsString` | Default to UTF-8 string |
| `deserialize_nullValue_bodyIsNull` | Null value → JSON null body |
| `deserialize_invalidJson_fallsBackToString` | Corrupted JSON → string fallback |
| `deserialize_allAmqpProperties_reconstructed` | All 13 AMQP properties from headers |
| `deserialize_applicationHeaders_reconstructed` | _ws_headers JSON → headers object |
| `deserialize_invalidApplicationHeaders_emptyObject` | Invalid _ws_headers → empty/null |
| `deserialize_kafkaMetadata_preserved` | partition, offset, timestamp passed through |
| `roundTrip_serializeThenDeserialize_lossless` | Serialize → deserialize preserves all data |

**Run command:**
```bash
./gradlew :http-server:test --tests "kafka.server.http.ws.WsMessageDeserializerTest"
```

---

## Rules

- Jackson ObjectMapper MUST be shared (reuse `HttpRequestTranslator.MAPPER`).
- Never throw from deserialize() on malformed data — fall back gracefully. A delivery failure due to bad deserialization would block the consumer.
- `_content-type` header uses no `_ws_` prefix (matches WsMessageSerializer).
- Cross-protocol compatibility: synthesize reasonable defaults when `_ws_*` headers absent.
- Thread-safe: stateless class, shared ObjectMapper is thread-safe.

---

## Learning

- **Mirror, don't duplicate, the header constants.** The deserializer must agree byte-for-byte with `WsMessageSerializer` on every header name, or round-trip silently corrupts data. I aliased the constants through package-private references (`HDR_EXCHANGE = WsMessageSerializer.HDR_EXCHANGE`, ...) rather than re-declaring string literals, which makes the compiler enforce the coupling at build time. Tests reference `WsMessageSerializer.HDR_*` directly so they see the one authoritative source.
- **Never throw on malformed payload.** Delivery-path deserialization sits in a hot loop; a thrown exception stops consumption for that subscription. I wrapped JSON body parsing and `_ws_headers` parsing in try/catch with sensible fallbacks (UTF-8 string for bodies; skip the `headers` field when invalid). A `deliveryMode` header that is not parseable as int is preserved as its raw string rather than silently dropped — the client at least sees the original wire value.
- **Single-pass header scan.** Walked the `Iterable<Header>` once into a `HashMap` keyed by header name, then looked up each field. Two passes (one to find exchange, another to find delivery-mode, etc.) would cost O(n*k) on large header lists.
- **NullNode for null payload.** Jackson distinguishes "field absent" from "field explicitly null"; using `NullNode.getInstance()` gives the frame a `body: null` entry, which round-trips cleanly and lets clients distinguish "empty payload" from "missing body".
- **Only `_ws_*` / `_content-type` are meaningful.** Arbitrary Kafka headers must not be promoted onto the message object — otherwise a producer injecting a header called `correlationId` would overwrite what WsMessageSerializer put under `_ws_correlation_id`. The `isWsHeader()` gate filters them out.

---

## Limitations

- **Binary-body round-trip re-encodes.** The serializer stores raw bytes in the record value; the deserializer re-base64s them for the deliver frame. That costs a Base64 encode per message on the hot path. An alternative would be to pass the raw bytes through the frame as a Netty `ByteBuf` attachment, but the JSON deliver frame contract expects a base64 string for binary.
- **Invalid JSON falls back silently.** When `_content-type: application/json` but the value is not valid JSON, we emit the UTF-8 string as the body. This is intentional (never block delivery) but the client cannot tell whether the producer sent valid JSON that survived in object form versus corrupted JSON that degraded to a string. A warning log would help observability but was out of scope.
- **`_ws_headers` must be a JSON object.** If the header value is a JSON array, number, or string, we omit the `headers` field entirely rather than surface it as-is. The serializer only ever writes objects, so this matches WsMessageSerializer's output contract, but a malicious producer (via the binary Kafka protocol) could silently drop headers by writing e.g. `"not an object"` into `_ws_headers`.
- **Numeric property coercion is best-effort.** `deliveryMode` / `priority` may end up as strings in the deliver frame when the header value is not parseable as int. Clients that assume these fields are always numeric will need to handle the string case.
- **No defensive copies.** The `byte[]` inputs (`key`, `value`) are read but not copied. Callers must not mutate those arrays after calling `deserialize(...)` — the JSON body node may reference the same UTF-8 view.

---

## Field Notes

- **Pre-existing scala compile errors on `feature/http-protocol`.** The HTTP module's scala sources (`HttpRequestHandler.scala`, `HttpCorsTest.scala`, `HttpGracefulShutdownTest.scala`, `HttpRequestHandlerTest.scala`) do not compile against the current Java sources — the main worktree only succeeds because its `build/classes/scala` directory is stale from before the Java interfaces changed. I verified this by running `./gradlew :http-server:compileScala --rerun-tasks` in `/home/anh/kafka` (the main checkout), which also fails with the same 23 errors. This is unrelated to WS1.10; tracking it separately is out of scope. To run the targeted tests I used `-x compileScala -x compileTestScala`.
- **Worktree was based on pre-http-server HEAD.** The harness created the worktree at commit `f95a1f995d`, which predates the entire `http-server/` module. I had to `git rebase feature/http-protocol` before any WS1.10 work was possible. WS1.09 author hit the same issue and worked in the main checkout instead; I chose to rebase and keep the worktree isolated.
- **WsMessageSerializer arrived mid-task.** Initially the `ws/` directory was empty. A parallel agent committed `f6e5479c12 feat: WS1.09 — WsMessageSerializer` during my rebase, which made the round-trip test viable. I added three round-trip tests (JSON, string, binary) on top of the task's specified suite to exercise the full serialize → deserialize loop.
- **Per project memory, I did NOT run `:http-server:test` in full.** Only `--tests 'kafka.server.http.ws.WsMessageDeserializerTest'`. All 21 tests pass.

---

## Acceptance Criteria

- [ ] `./gradlew :http-server:test --tests "kafka.server.http.ws.WsMessageDeserializerTest"` exits 0
- [ ] `WsMessageDeserializer.java` exists at `http-server/src/main/java/kafka/server/http/ws/WsMessageDeserializer.java`
- [ ] All 13 AMQP properties reconstructed from headers
- [ ] Cross-protocol fallback works when `_ws_*` headers absent
- [ ] JSON, string, and binary body deserialization all tested
- [ ] Invalid JSON body falls back to string (no exception)
- [ ] Round-trip test: serialize → deserialize is lossless
- [ ] Reuses `HttpRequestTranslator.MAPPER` (no new ObjectMapper created)
- [ ] Learning section filled with at least one entry
- [ ] Limitations section filled (use "None" if truly none)
- [ ] File Manifest section updated after commit

---

## File Manifest

> Filled by the executing agent after each commit.

### 2026-04-17 — WsMessageDeserializer initial implementation (commit eeee29346a)

Created:
  - `http-server/src/main/java/kafka/server/http/ws/WsMessageDeserializer.java` — Kafka record → deliver-frame components. Exposes the `DeliverFrame` record (exchange, routingKey, message JsonNode, partition, offset, kafkaTimestamp) and a single `deserialize(key, value, headers, partition, offset, timestamp)` entry point. Reconstructs routing metadata + all 13 AMQP properties from `_ws_*` headers, with cross-protocol synthesis when headers are absent. Body deserialization handles JSON / base64 / string / null; never throws on malformed input.
  - `http-server/src/test/java/kafka/server/http/ws/WsMessageDeserializerTest.java` — 21 JUnit 5 tests covering routing reconstruction, body encodings, all AMQP properties, missing / null / invalid headers, non-ws header filtering, Kafka metadata passthrough, and three round-trip tests against `WsMessageSerializer` (JSON / string / binary).

Modified:
  - `ivy-docs/tasks/TASK-WS1.10-ws-message-deserializer.md` — filled Learning / Limitations / Field Notes / File Manifest sections.
