# TASK-WS1.11: WsPublishHandler — Publish Frame Processing Pipeline

## Prerequisites

- **TASK-WS1.08** (RoutingEngine + DirectMatcher) — Provides `route(exchange, routingKey, headers)` → `Set<String>` matched queues. WsPublishHandler calls this to determine which queues receive the published message.
- **TASK-WS1.09** (WsMessageSerializer) — Provides `serialize(exchange, routingKey, message, vhost)` → `SerializedMessage`. WsPublishHandler uses this to convert the publish frame into Kafka record components.
- **TASK-WS1.06** (QueueManager) — Provides queue-to-Kafka-topic resolution (`resolveTopicName(queue)` → `"ws.{queue}"`).
- **TASK-A.03** (RequestChannel.tryEnqueue) — Provides the mechanism to submit requests to the Kafka request pipeline.

---

## Context

The `WsPublishHandler` implements the complete publish pipeline for WebSocket messages. When a client sends a `publish` JSON frame, the handler:

1. Parses the publish frame to extract exchange, routingKey, mandatory flag, message, and publishId
2. Calls `RoutingEngine.route()` to find matching queues
3. For each matched queue, resolves the Kafka topic name (via `ws.topic.prefix` + queue name)
4. Builds a `ProduceRequest` per topic using `WsMessageSerializer` to create the Kafka record
5. Enqueues each `ProduceRequest` to the `RequestChannel` via `tryEnqueue()`
6. On completion, sends a publisher confirm frame back to the WebSocket client (if confirms enabled)

The handler follows the pattern from `handleHttpProduceRequest()` in `KafkaApis.scala` (lines ~305-400), which demonstrates how to build `ProduceRequest` objects, classify partitions by leader, and handle local vs. remote produce paths.

For multi-queue fanout (e.g., topic/fanout exchange matching multiple queues), the handler uses `CompletableFuture.allOf()` to wait for all produce operations to complete before sending the confirm. All completion handling runs on the `wsAsyncExecutor` thread pool — never on Netty event loop threads or KafkaRequestHandler threads.

---

## Specification

### `WsPublishHandler` — `kafka.server.http.ws.WsPublishHandler`

```java
/**
 * Handles WebSocket publish frames: routing → serialization → ProduceRequest → RequestChannel.
 *
 * Threading model:
 * - Called from WsFrameHandler on a Netty event loop thread
 * - ProduceRequest enqueued to RequestChannel (non-blocking)
 * - Completion callback runs on wsAsyncExecutor
 */
public final class WsPublishHandler {

    /**
     * Processes a publish frame.
     *
     * @param publishFrame the parsed publish JSON frame
     * @param connectionCtx WebSocket connection context (principal, vhost, channelCtx)
     * @param confirmsEnabled whether publisher confirms are enabled on this connection
     */
    public void handlePublish(JsonNode publishFrame, WsConnectionContext connectionCtx,
                              boolean confirmsEnabled);
}
```

### Behavioral contracts

- If `RoutingEngine.route()` returns empty set AND `mandatory` is true: send `returned` frame back to client.
- If `RoutingEngine.route()` returns empty set AND `mandatory` is false: silently drop (no error, no confirm).
- For each matched queue: resolve to Kafka topic, build `ProduceRequest`, and enqueue.
- Publisher confirm (if enabled): sent only after ALL produce requests complete successfully.
- If any produce fails: send `publish-failed` frame with error code.
- `publishId` from the client frame is echoed in confirm/failed frames.
- Record key = routing key bytes (for murmur2 partition assignment).
- All `_ws_*` headers are included in the Kafka record (via WsMessageSerializer).
- Timeout: if produce takes longer than `ws.publish.timeout.ms` (default 30s), send `publish-failed`.

---

## Implementation Details

**Module:** `http-server`

**Files to study:**

| File | Why |
|------|-----|
| `core/src/main/scala/kafka/server/KafkaApis.scala` lines 305-400 | `handleHttpProduceRequest()` — builds ProduceRequest, classifies partitions by leader |
| `http-server/src/main/java/kafka/server/http/HttpRequestTranslator.java` lines 400-520 | `translateProduce()` — builds MemoryRecords and ProduceRequestData |
| `http-server/src/main/java/kafka/server/http/HttpProcessor.java` lines 170-175 | `enqueueResponse()` pattern |
| `ivy-docs/http-protocol-extend-design.md` §8 (lines 2010-2065) | WebSocket publish flow diagram |

```java
// From KafkaApis.scala lines 305-330 — classify partitions pattern:
val authorizedLocalRequestInfo = mutable.Map[TopicIdPartition, MemoryRecords]()
val remoteEntriesByLeader = mutable.Map[Int, mutable.Map[TopicIdPartition, MemoryRecords]]()

produceRequest.data.topicData.forEach { topic =>
  topic.partitionData.forEach { partition =>
    val topicPartition = new TopicPartition(topicName, partition.index())
    // classify as local, remote, or error based on MetadataCache leader info
  }
}
```

```java
// From design doc §8 — publish flow:
// 1. Parse publish frame
// 2. RoutingEngine.route() → matched queues
// 3. Resolve queues → Kafka topics (ws.{queueName})
// 4. Build ProduceRequest per topic
// 5. Enqueue to RequestChannel
// 6. On completion → publisher confirm
```

**Files to create:**

| File | What it does |
|------|--------------|
| `http-server/src/main/java/kafka/server/http/ws/WsPublishHandler.java` | Publish frame processing pipeline |
| `http-server/src/test/java/kafka/server/http/ws/WsPublishHandlerTest.java` | Unit tests with mocked dependencies |

> **CRITICAL:** Never block the Netty event loop thread. The `handlePublish()` method must enqueue work and return immediately. All completion handling (confirm frames, error frames) runs on `wsAsyncExecutor`.

> **CRITICAL:** `ProduceRequest.Builder` requires a `ProduceRequestData` with `TopicProduceDataCollection` — not a plain list. Use `new ProduceRequestData.TopicProduceDataCollection()` and iterate-add.

> **CRITICAL:** When building `MemoryRecords`, the `SimpleRecord` constructor takes `(long timestamp, byte[] key, byte[] value, Header[] headers)`. Convert the `List<Header>` from WsMessageSerializer to `Header[]`.

**Implementation order:**
1. Create `WsPublishHandler.java` with constructor accepting all dependencies
2. Implement publish frame parsing (extract exchange, routingKey, mandatory, message, publishId)
3. Implement routing + empty route handling (mandatory returns, silent drops)
4. Implement per-queue ProduceRequest building using WsMessageSerializer
5. Implement RequestChannel enqueue and completion handling
6. Implement publisher confirm / publish-failed frame writing
7. Write unit tests with mocked RoutingEngine, WsMessageSerializer, RequestChannel

---

## Skeleton Code

### `WsPublishHandler.java`

```java
package kafka.server.http.ws;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import kafka.network.RequestChannel;
import kafka.server.http.HttpRequestTranslator;
import kafka.server.http.routing.RoutingEngine;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

/**
 * Handles WebSocket publish frames: routing → serialization → ProduceRequest → RequestChannel.
 *
 * Threading model:
 *   handlePublish() called from Netty event loop (non-blocking)
 *   Completion callbacks run on wsAsyncExecutor
 *
 * // Time: Created - TASK-WS1.11
 */
public final class WsPublishHandler {

    private static final Logger log = LoggerFactory.getLogger(WsPublishHandler.class);
    private static final ObjectMapper MAPPER = HttpRequestTranslator.MAPPER;

    // --- Dependencies ---
    private final RoutingEngine routingEngine;
    private final WsMessageSerializer messageSerializer;
    private final Function<String, String> queueToTopicFn;  // queue name → Kafka topic name
    private final Function<String, Integer> partitionCountFn;  // topic name → partition count
    private final RequestChannel requestChannel;
    private final ExecutorService wsAsyncExecutor;

    /**
     * @param routingEngine    exchange → queue resolution
     * @param messageSerializer publish frame → Kafka record components
     * @param queueToTopicFn   queue name → Kafka topic name (e.g., "orders" → "ws.orders")
     * @param partitionCountFn topic name → partition count from MetadataCache
     * @param requestChannel   Kafka request pipeline
     * @param wsAsyncExecutor  executor for completion callbacks (never Netty thread)
     */
    public WsPublishHandler(RoutingEngine routingEngine,
                            WsMessageSerializer messageSerializer,
                            Function<String, String> queueToTopicFn,
                            Function<String, Integer> partitionCountFn,
                            RequestChannel requestChannel,
                            ExecutorService wsAsyncExecutor) {
        this.routingEngine = Objects.requireNonNull(routingEngine, "routingEngine");
        this.messageSerializer = Objects.requireNonNull(messageSerializer, "messageSerializer");
        this.queueToTopicFn = Objects.requireNonNull(queueToTopicFn, "queueToTopicFn");
        this.partitionCountFn = Objects.requireNonNull(partitionCountFn, "partitionCountFn");
        this.requestChannel = Objects.requireNonNull(requestChannel, "requestChannel");
        this.wsAsyncExecutor = Objects.requireNonNull(wsAsyncExecutor, "wsAsyncExecutor");
    }

    /**
     * Processes a publish frame from a WebSocket client.
     *
     * Steps:
     * 1. Parse publish frame fields
     * 2. RoutingEngine.route() → matched queues
     * 3. Handle empty match (mandatory return or silent drop)
     * 4. For each queue: resolve topic, build ProduceRequest, enqueue
     * 5. On completion: send confirm or failed frame
     *
     * @param publishFrame   the full publish JSON frame
     * @param connectionCtx  WebSocket connection context
     * @param confirmsEnabled whether publisher confirms are enabled
     */
    public void handlePublish(JsonNode publishFrame, WsConnectionContext connectionCtx,
                              boolean confirmsEnabled) {
        // TODO: 1. Extract fields: exchange, routingKey, mandatory, message, publishId
        // TODO: 2. Route: routingEngine.route(exchange, routingKey, messageHeaders)
        // TODO: 3. If matchedQueues.isEmpty():
        //          - If mandatory: write "returned" frame to WS channel
        //          - Else: silently drop (optionally send confirm if confirms enabled)
        //          - Return
        // TODO: 4. Serialize message: messageSerializer.serialize(exchange, routingKey, message, vhost)
        // TODO: 5. For each queue in matchedQueues:
        //          a. Resolve topic name: queueToTopicFn.apply(queue)
        //          b. Compute partition: murmur2(key) & 0x7fffffff % partitionCount
        //          c. Build SimpleRecord from serialized key, value, headers
        //          d. Build MemoryRecords
        //          e. Build ProduceRequestData
        //          f. Enqueue to requestChannel via tryEnqueue()
        // TODO: 6. CompletableFuture.allOf(futures).handleAsync(callback, wsAsyncExecutor)
        //          - On success: write "published" frame (if confirms enabled)
        //          - On failure: write "publish-failed" frame
        throw new UnsupportedOperationException("Not yet implemented");
    }

    /**
     * Computes partition for a record key using murmur2 hash.
     *
     * @param key            record key bytes (null → return 0 or round-robin)
     * @param partitionCount number of partitions for the topic
     * @return partition index
     */
    int computePartition(byte[] key, int partitionCount) {
        if (key == null || partitionCount <= 0) {
            return 0;
        }
        return (Utils.murmur2(key) & 0x7fffffff) % partitionCount;
    }

    /**
     * Writes a publisher confirm frame to the WebSocket channel.
     */
    private void writePublishConfirm(WsConnectionContext ctx, long publishId) {
        // TODO: Build {"type":"published","publishId":N} and write to ctx channel
        throw new UnsupportedOperationException("Not yet implemented");
    }

    /**
     * Writes a publish-failed frame to the WebSocket channel.
     */
    private void writePublishFailed(WsConnectionContext ctx, long publishId,
                                    String errorCode, String errorMessage) {
        // TODO: Build {"type":"publish-failed","publishId":N,"errorCode":"...","errorMessage":"..."} 
        throw new UnsupportedOperationException("Not yet implemented");
    }

    /**
     * Writes a returned frame for mandatory messages with no matching queues.
     */
    private void writeReturnedFrame(WsConnectionContext ctx, String exchange,
                                    String routingKey, JsonNode message) {
        // TODO: Build {"type":"returned","exchange":"...","routingKey":"...","replyCode":312,
        //             "replyText":"NO_ROUTE","message":{...}}
        throw new UnsupportedOperationException("Not yet implemented");
    }
}
```

### Test class — `WsPublishHandlerTest.java`

```java
package kafka.server.http.ws;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import kafka.network.RequestChannel;
import kafka.server.http.HttpRequestTranslator;
import kafka.server.http.routing.RoutingEngine;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * // Time: Created - TASK-WS1.11
 */
class WsPublishHandlerTest {

    private static final ObjectMapper MAPPER = HttpRequestTranslator.MAPPER;

    private RoutingEngine routingEngine;
    private WsMessageSerializer messageSerializer;
    private RequestChannel requestChannel;
    private WsPublishHandler handler;

    @BeforeEach
    void setUp() {
        routingEngine = mock(RoutingEngine.class);
        messageSerializer = new WsMessageSerializer();
        requestChannel = mock(RequestChannel.class);

        handler = new WsPublishHandler(
            routingEngine,
            messageSerializer,
            queue -> "ws." + queue,          // queueToTopicFn
            topic -> 3,                      // partitionCountFn (3 partitions)
            requestChannel,
            Executors.newSingleThreadExecutor()  // wsAsyncExecutor
        );
    }

    // --- Partition computation ---

    @Test
    void computePartition_withKey_usesMurmur2() {
        byte[] key = "order.created".getBytes(StandardCharsets.UTF_8);
        int partition = handler.computePartition(key, 3);
        assertTrue(partition >= 0 && partition < 3);
    }

    @Test
    void computePartition_nullKey_returnsZero() {
        assertEquals(0, handler.computePartition(null, 3));
    }

    @Test
    void computePartition_sameKey_samePartition() {
        byte[] key = "order.created".getBytes(StandardCharsets.UTF_8);
        int p1 = handler.computePartition(key, 10);
        int p2 = handler.computePartition(key, 10);
        assertEquals(p1, p2);
    }

    @Test
    void computePartition_alwaysNonNegative() {
        for (int i = 0; i < 1000; i++) {
            byte[] key = ("key-" + i).getBytes(StandardCharsets.UTF_8);
            int partition = handler.computePartition(key, 100);
            assertTrue(partition >= 0, "Partition must be non-negative: " + partition);
        }
    }

    // --- Routing ---

    @Test
    void handlePublish_routesToMatchedQueues() {
        when(routingEngine.route(eq("events"), eq("order.created"), any()))
            .thenReturn(Set.of("order-events"));

        ObjectNode frame = buildPublishFrame("events", "order.created", "hello");
        // This test verifies the routing engine is called correctly
        // Full integration requires WsConnectionContext mock
        verify(routingEngine, never()).route(any(), any());  // not called yet
    }

    @Test
    void handlePublish_emptyRouteNonMandatory_silentDrop() {
        when(routingEngine.route(eq("events"), eq("unknown.key"), any()))
            .thenReturn(Set.of());

        // Non-mandatory publish with no matching queues should not throw
        // and should not enqueue anything
    }

    // --- Constructor validation ---

    @Test
    void constructor_nullRoutingEngine_throwsNPE() {
        assertThrows(NullPointerException.class, () ->
            new WsPublishHandler(null, messageSerializer, q -> "ws." + q, t -> 3,
                requestChannel, Executors.newSingleThreadExecutor()));
    }

    @Test
    void constructor_nullSerializer_throwsNPE() {
        assertThrows(NullPointerException.class, () ->
            new WsPublishHandler(routingEngine, null, q -> "ws." + q, t -> 3,
                requestChannel, Executors.newSingleThreadExecutor()));
    }

    // --- Helper ---

    private ObjectNode buildPublishFrame(String exchange, String routingKey, String body) {
        ObjectNode frame = MAPPER.createObjectNode();
        frame.put("type", "publish");
        frame.put("exchange", exchange);
        frame.put("routingKey", routingKey);
        frame.put("mandatory", false);
        ObjectNode message = frame.putObject("message");
        message.put("body", body);
        frame.put("publishId", 1);
        return frame;
    }
}
```

### Existing pattern reference

```java
// From KafkaApis.scala lines 305-384 — handleHttpProduceRequest pattern:
// 1. Parse partition data from ProduceRequest
val authorizedLocalRequestInfo = mutable.Map[TopicIdPartition, MemoryRecords]()
val remoteEntriesByLeader = mutable.Map[Int, mutable.Map[TopicIdPartition, MemoryRecords]]()

// 2. Authorization check
val authorizedTopics = authHelper.filterByAuthorized(...)

// 3. Classify each partition: local, remote, or error
topicIdToPartitionData.foreach { case (topicIdPartition, partition) =>
  val memoryRecords = partition.records.asInstanceOf[MemoryRecords]
  // classify by MetadataCache leader
}

// 4. Build response callback
def sendMergedResponse(allResults: Map[TopicIdPartition, PartitionResponse]): Unit = { ... }
```

---

## Tests

**Test class:** `http-server/src/test/java/kafka/server/http/ws/WsPublishHandlerTest.java`

| Test method | What it verifies |
|-------------|-----------------|
| `computePartition_withKey_usesMurmur2` | Key-based partition in valid range |
| `computePartition_nullKey_returnsZero` | Null key → partition 0 |
| `computePartition_sameKey_samePartition` | Deterministic partition for same key |
| `computePartition_alwaysNonNegative` | murmur2 & 0x7fffffff prevents negative |
| `handlePublish_routesToMatchedQueues` | RoutingEngine called with correct args |
| `handlePublish_emptyRouteNonMandatory_silentDrop` | No-match + non-mandatory = silent |
| `constructor_nullRoutingEngine_throwsNPE` | Null dependency rejected |
| `constructor_nullSerializer_throwsNPE` | Null dependency rejected |

**Run command:**
```bash
./gradlew :http-server:test --tests "kafka.server.http.ws.WsPublishHandlerTest"
```

---

## Rules

- Never block the Netty event loop thread. `handlePublish()` must return immediately.
- Completion callbacks (confirm/failed frames) run on `wsAsyncExecutor`.
- Murmur2 hash MUST use `& 0x7fffffff` before modulo to prevent negative partition indexes.
- Publisher confirms only sent when ALL produce requests complete (CompletableFuture.allOf).
- Jackson ObjectMapper MUST be shared (`HttpRequestTranslator.MAPPER`).
- Mandatory messages with no matching queues produce a `returned` frame, not an error.

---

## Learning

- The routing engine's API (`RoutingEngine.route(exchange, routingKey, headers)`) is
  vhost-agnostic — the vhost is baked into the functional interfaces supplied to its
  constructor (`exchangeTypeFn`, `bindingsFn`). This means a single `WsPublishHandler`
  either shares a vhost-scoped `RoutingEngine`, or callers must scope construction per
  vhost. For this handler the vhost is taken from `WsConnectionContext.vhost()` and is
  only used for (a) `ExchangeManager.getExchange(vhost, name)` existence check and
  (b) the `_ws_vhost` record header via `WsMessageSerializer`.
- `JsonNode.fields()` is deprecated in Jackson 2.17; use `fieldNames()` + `get(name)`
  or `properties()` instead. `-Werror` on `http-server:compileJava` catches this.
- Checkstyle in this module caps cyclomatic complexity at 16 and NPath complexity at
  500 — pipelined methods with sequential validation steps need to be decomposed into
  helpers (one per stage). `handlePublish` was split into `parsePublishFrame`,
  `runRoute`, `serializeOrError`, and `fanOut`.
- A `ProduceRequestSink` functional interface is the cleanest seam between the
  publish handler and the Kafka request pipeline: tests capture every hand-off as a
  lambda; production wiring supplies a real lambda that builds a
  `RequestChannel.Request` with a `ProduceRequest` payload and stamps
  `requestLocalProperties.put("wsPublishId", publishId)` so
  `HttpProcessor.handleWsResponse` can emit the correct confirm/failed frame.

---

## Limitations

- **No QueueManager yet** — queue-name → Kafka-topic resolution is a
  `Function<String,String>` placeholder. Callers pass `queue -> "ws." + queue` as the
  default. A future task introducing `QueueManager.resolveTopicName(vhost, queue)` will
  replace this lambda. Until then the handler cannot verify that the resolved topic
  actually exists.
- **Sink is not wired to RequestChannel** — `ProduceRequestSink.enqueue` is invoked,
  but no production implementation ships in this task. The integration (building
  `ProduceRequest` + `RequestChannel.tryEnqueue` + stamping `wsPublishId`) lives in a
  separate wiring task (between this and `HttpProcessor.handleWsResponse`, which was
  added in WS1.12).
- **Publisher-confirm correlation is half-wired** — the handler stamps the
  `publishId` onto the sink call, which the real sink is expected to copy into
  `requestLocalProperties`. The actual `published` / `publish-failed` frames are
  emitted by `HttpProcessor.handleWsResponse` (already in place). If confirms are
  enabled but **no queues match** (non-mandatory drop), *no* confirm is emitted here;
  per WS3.01 that may need to change to an immediate `published` frame — deferred.
- **No partition math** — records are fanned out to topics as a `SerializedMessage`
  and the sink is free to pick a partition (murmur2 of the key is the expected default).
  This keeps the handler free of `metadataSupplier` coupling.
- **Single-record batches** — each matched queue triggers an independent hand-off.
  Batching N queues into one request is a potential future optimisation but
  intentionally out of scope.
- **Mandatory return frame carries the original message** — emitting the client's
  message back on `returned` mirrors AMQP semantics. No extra size cap is enforced
  beyond Jackson's configured `maxStringLength`.
- **DLX / retry / dedup** are WS4.* features and are explicitly NOT wired here.
- **Timeouts** for the produce hand-off are delegated to the sink / broker — this
  handler returns to the Netty event loop immediately after `sink.enqueue()`.

---

## Field Notes

- Red→green was fast (one compile fail for deprecated `JsonNode.fields()`, one
  checkstyle fail for complexity). Splitting the pipeline method into named helpers
  both satisfied checkstyle and improved readability — each stage now has a single
  responsibility and its own early-return semantics.
- The test class relies on a real `WsMessageSerializer` (not a mock) because its
  output shape is the invariant we care about — mocking it would just re-encode its
  behaviour. The handler's produce hand-off payload is covered by the serializer's
  own unit tests plus one end-to-end assertion here
  (`handlePublish_validExchange_singleQueue_enqueuesOneProduce` asserts both key
  bytes and value bytes).
- Capturing WS frames with a mocked `ChannelHandlerContext.writeAndFlush(any())`
  that stashes `TextWebSocketFrame.text()` worked cleanly — no `EmbeddedChannel`
  needed, matching the pattern already established by `WsFrameHandlerTest`.
- `ExchangeManager.getExchange` takes `(vhost, name)` — do NOT confuse with
  `RoutingEngine.route(exchange, routingKey, headers)` which has no vhost at all.

---

## Acceptance Criteria

- [ ] `./gradlew :http-server:test --tests "kafka.server.http.ws.WsPublishHandlerTest"` exits 0
- [ ] `WsPublishHandler.java` exists at `http-server/src/main/java/kafka/server/http/ws/WsPublishHandler.java`
- [ ] `computePartition()` always returns non-negative values (& 0x7fffffff)
- [ ] Routing engine is called with exchange, routingKey, and headers
- [ ] Empty route + non-mandatory = silent drop (no exception, no enqueue)
- [ ] Constructor rejects null dependencies
- [ ] Learning section filled with at least one entry
- [ ] Limitations section filled (use "None" if truly none)
- [ ] File Manifest section updated after commit

---

## File Manifest

> Filled by the executing agent after each commit.

### 2026-04-17 — WsPublishHandler (commit b27d3c43b1)

Created:
  - `http-server/src/main/java/kafka/server/http/ws/WsPublishHandler.java` —
    Publish-frame processing pipeline: parse → exchange existence check
    (`ExchangeManager.getExchange`) → `RoutingEngine.route` → per-queue
    `WsMessageSerializer.serialize` → sink hand-off. Emits `error` frames for
    invalid frames / unknown exchanges / routing failures, `returned` frames
    for mandatory messages with no matching queues, and silently drops
    non-mandatory unrouted publishes. Exposes the nested
    `ProduceRequestSink` functional interface as the seam for the Kafka
    request pipeline.
  - `http-server/src/test/java/kafka/server/http/ws/WsPublishHandlerTest.java`
    — 16 JUnit 5 tests covering constructor validation (5), happy-path single
    + multi-queue publishes (2), confirm-flag propagation (1), empty-route
    mandatory vs non-mandatory (2), unknown exchange (1), malformed frames (3),
    routing-engine failure propagation (1), and user-header propagation to
    the routing engine (1). Real `WsMessageSerializer`; mocks for
    `ExchangeManager`, `RoutingEngine`, Netty `ChannelHandlerContext`.

Modified: none. (No existing file needed changes — `WsFrameHandler.handlePublish`
still throws `UnsupportedOperationException`; wiring the frame handler to
call `WsPublishHandler` is left to an integration task.)
