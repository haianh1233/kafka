# TASK-WS1.12: HttpProcessor WebSocket Extension — WS Response Routing

## Prerequisites

- **TASK-B.04** (HttpProcessor) — The existing `HttpProcessor` class at `http-server/src/main/java/kafka/server/http/HttpProcessor.java` handles HTTP response routing. This task extends it to also handle WebSocket responses.
- **TASK-WS1.03** (WsConnectionContext) — Provides `WsConnectionContext` with per-connection state (principal, sessionId, channelCtx, vhost). HttpProcessor stores WsConnectionContext instances in its connection map.

---

## Context

The `HttpProcessor` currently handles only HTTP request-response cycles: it routes responses from `RequestChannel` back to Netty channels for HTTP clients. WebSocket connections need a different response path — instead of building full HTTP responses, the processor must write JSON frames (publisher confirms, error frames) to WebSocket channels.

The design doc (§12.4) specifies the extension: `processResponses()` checks whether a `connectionId` belongs to a WebSocket connection. If so, it delegates to a WebSocket-specific response handler instead of the HTTP response builder.

The key insight is that WebSocket `ProduceResponse` handling is simpler than HTTP: instead of serializing the full Kafka response to JSON, the processor writes a compact `{"type":"published","publishId":N}` frame (or `publish-failed` on error). `FetchResponse` is NOT handled through the processor — it flows directly through `WsConsumerFetchLoop`.

This task adds a `ConcurrentHashMap<String, WsConnectionContext>` to HttpProcessor and modifies `processResponses()` to check this map. It must NOT break the existing HTTP response flow.

---

## Specification

### Changes to `HttpProcessor` — `kafka.server.http.HttpProcessor`

```java
// New field:
private final ConcurrentHashMap<String, WsConnectionContext> wsConnections = new ConcurrentHashMap<>();

// New methods:
public void registerWsConnection(String connectionId, WsConnectionContext wsCtx);
public void unregisterWsConnection(String connectionId);
public boolean isWsConnection(String connectionId);
public int wsConnectionCount();

// Modified method: processResponses()
// Added: check wsConnections before HTTP handling
```

### `WsConnectionContext` — `kafka.server.http.ws.WsConnectionContext` (stub for this task)

The full `WsConnectionContext` is created in TASK-WS1.03. For this task, a minimal interface is needed:

```java
/**
 * Per-WebSocket-connection state. This task uses a minimal view —
 * the full implementation is in TASK-WS1.03.
 */
public class WsConnectionContext {
    private final ChannelHandlerContext channelCtx;
    private final String sessionId;
    private final boolean confirmsEnabled;

    public void writeFrame(String json);
    public void writePublishConfirm(ProduceResponse response, long publishId);
    public ChannelHandlerContext channelCtx();
    public String sessionId();
    public boolean confirmsEnabled();
}
```

### Behavioral contracts

- `registerWsConnection()` adds a WS connection context alongside the existing HTTP channel registration. Both maps are checked during response processing.
- `unregisterWsConnection()` removes the WS context. Called on WebSocket close.
- In `processResponses()`: for each response, check `wsConnections` FIRST. If the connectionId is a WS connection, route to `handleWsResponse()`. Otherwise, fall through to existing HTTP handling.
- `handleWsResponse()` for `SendResponse` with `ProduceResponse`: write publisher confirm frame if confirms enabled.
- `handleWsResponse()` for `SendResponse` with `FetchResponse`: no-op (handled by WsConsumerFetchLoop).
- `handleWsResponse()` for `CloseConnectionResponse`: close the WS channel and remove from maps.
- `handleWsResponse()` for `StartThrottlingResponse`: send throttle error frame to WS client.
- Must NOT break existing HTTP response flow — HTTP connections continue to work exactly as before.

---

## Implementation Details

**Module:** `http-server`

**Files to study:**

| File | Why |
|------|-----|
| `http-server/src/main/java/kafka/server/http/HttpProcessor.java` | Existing processResponses() to extend |
| `ivy-docs/http-protocol-extend-design.md` §12.4 (lines 3025-3057) | HttpProcessor extension design |

```java
// From HttpProcessor.java lines 183-210 — existing processResponses():
public void processResponses() {
    List<RequestChannel.Response> batch = new ArrayList<>();
    responseQueue.drainTo(batch);

    for (RequestChannel.Response response : batch) {
        String connectionId = response.request().context().connectionId();
        try {
            if (response instanceof RequestChannel.SendResponse) {
                handleSendResponse((RequestChannel.SendResponse) response, connectionId);
            } else if (response instanceof RequestChannel.NoOpResponse) {
                sendNoContentResponse(connectionId);
                safeUpdateMetrics(response);
            } else if (response instanceof RequestChannel.CloseConnectionResponse) {
                ChannelHandlerContext ctx = channels.remove(connectionId);
                if (ctx != null) {
                    ctx.close();
                }
                inFlightCount.decrementAndGet();
            }
            // ...
        } catch (Exception e) {
            log.error("Error processing response for connection {}", connectionId, e);
        }
    }
}
```

```java
// From design doc §12.4 — WS extension pattern:
public void processResponses() {
    for (RequestChannel.Response response : batch) {
        String connectionId = response.request().context().connectionId();

        if (wsConnections.containsKey(connectionId)) {
            WsConnectionContext wsCtx = wsConnections.get(connectionId);
            handleWsResponse(wsCtx, response);
        } else {
            // Existing HTTP response path (unchanged)
            handleHttpResponse(response);
        }
    }
}

private void handleWsResponse(WsConnectionContext wsCtx, RequestChannel.Response response) {
    if (response instanceof RequestChannel.SendResponse sendResp) {
        AbstractResponse kafkaResp = sendResp.response();
        if (kafkaResp instanceof ProduceResponse produceResp) {
            wsCtx.writePublishConfirm(produceResp);
        }
        // FetchResponse handled by WsConsumerFetchLoop directly
    }
}
```

**Files to modify:**

| File | What changes |
|------|--------------|
| `http-server/src/main/java/kafka/server/http/HttpProcessor.java` | Add wsConnections map, modify processResponses(), add register/unregister methods |

**Files to create:**

| File | What it does |
|------|--------------|
| `http-server/src/test/java/kafka/server/http/HttpProcessorWsTest.java` | Tests for WS extension (separate from existing HttpProcessorTest) |

> **CRITICAL:** The WS connection check MUST happen BEFORE the HTTP response handling in `processResponses()`. Use `wsConnections.get(connectionId)` — not `containsKey()` then `get()` (race condition). If the get returns non-null, handle as WS; otherwise fall through to HTTP.

> **CRITICAL:** Do NOT modify the existing `handleSendResponse()` method. Create a new `handleWsSendResponse()` method. This preserves the existing HTTP path and prevents regressions.

> **CRITICAL:** After handling a WS response, decrement `inFlightCount` just like HTTP responses do. Missing this causes the counter to leak and eventual connection rejection.

**Implementation order:**
1. Add `wsConnections` ConcurrentHashMap field to HttpProcessor
2. Add `registerWsConnection()`, `unregisterWsConnection()`, `isWsConnection()`, `wsConnectionCount()`
3. Modify `processResponses()` to check wsConnections first
4. Implement `handleWsResponse()` with ProduceResponse → confirm frame
5. Handle CloseConnectionResponse for WS connections
6. Write tests that verify both HTTP and WS paths still work

---

## Skeleton Code

### Changes to `HttpProcessor.java`

```java
// Add to existing HttpProcessor.java:

// --- WebSocket connection contexts: connectionId -> WsConnectionContext ---
private final ConcurrentHashMap<String, WsConnectionContext> wsConnections =
    new ConcurrentHashMap<>();

/**
 * Registers a WebSocket connection context. Called during WS upgrade.
 *
 * @param connectionId the connection ID (same as used for HTTP channels)
 * @param wsCtx        WebSocket connection context
 */
public void registerWsConnection(String connectionId, WsConnectionContext wsCtx) {
    Objects.requireNonNull(connectionId, "connectionId");
    Objects.requireNonNull(wsCtx, "wsCtx");
    wsConnections.put(connectionId, wsCtx);
}

/**
 * Unregisters a WebSocket connection. Called on WS close.
 */
public void unregisterWsConnection(String connectionId) {
    wsConnections.remove(connectionId);
}

/**
 * Returns true if the connection is a WebSocket connection.
 */
public boolean isWsConnection(String connectionId) {
    return wsConnections.containsKey(connectionId);
}

/**
 * Returns the number of active WebSocket connections.
 */
public int wsConnectionCount() {
    return wsConnections.size();
}

// Modify processResponses() — add WS check at the top of the loop body:
// In the for-loop, BEFORE the existing if-else chain:
//
//     WsConnectionContext wsCtx = wsConnections.get(connectionId);
//     if (wsCtx != null) {
//         handleWsResponse(wsCtx, response, connectionId);
//         continue;
//     }
//     // ... existing HTTP handling below ...

/**
 * Handles a response for a WebSocket connection.
 *
 * @param wsCtx        WebSocket connection context
 * @param response     the RequestChannel response
 * @param connectionId the connection ID
 */
private void handleWsResponse(WsConnectionContext wsCtx, RequestChannel.Response response,
                               String connectionId) {
    // TODO: 1. if SendResponse:
    //          - Extract AbstractResponse from request local properties
    //          - If ProduceResponse and confirms enabled: write confirm frame
    //          - If FetchResponse: no-op (WsConsumerFetchLoop handles)
    //          - Decrement inFlightCount
    //          - Update metrics
    // TODO: 2. if CloseConnectionResponse:
    //          - Close the WS channel
    //          - Remove from wsConnections and channels
    //          - Decrement inFlightCount
    // TODO: 3. if StartThrottlingResponse:
    //          - Write throttle error frame to WS
    //          - Decrement inFlightCount
    // TODO: 4. if NoOpResponse:
    //          - No-op for WS (acks=0 produce)
    //          - Decrement inFlightCount
    throw new UnsupportedOperationException("Not yet implemented");
}
```

### Test class — `HttpProcessorWsTest.java`

```java
package kafka.server.http;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Tests for HttpProcessor WebSocket extension.
 * Separate from HttpProcessorTest to isolate WS-specific behavior.
 *
 * // Time: Created - TASK-WS1.12
 */
class HttpProcessorWsTest {

    private HttpProcessor processor;

    @BeforeEach
    void setUp() {
        processor = new HttpProcessor(0);
    }

    // --- WS connection registration ---

    @Test
    void registerWsConnection_addsToMap() {
        // TODO: Create mock WsConnectionContext, register, verify isWsConnection returns true
    }

    @Test
    void unregisterWsConnection_removesFromMap() {
        // TODO: Register then unregister, verify isWsConnection returns false
    }

    @Test
    void wsConnectionCount_reflectsRegistrations() {
        assertEquals(0, processor.wsConnectionCount());
        // TODO: Register 2 WS connections, verify count is 2
    }

    @Test
    void isWsConnection_falseForHttpConnection() {
        assertFalse(processor.isWsConnection("http-conn-1"));
    }

    @Test
    void registerWsConnection_nullId_throwsNPE() {
        assertThrows(NullPointerException.class,
            () -> processor.registerWsConnection(null, mock(kafka.server.http.ws.WsConnectionContext.class)));
    }

    @Test
    void registerWsConnection_nullCtx_throwsNPE() {
        assertThrows(NullPointerException.class,
            () -> processor.registerWsConnection("conn-1", null));
    }

    // --- processResponses WS routing ---

    @Test
    void processResponses_wsConnection_routesToWsHandler() {
        // TODO: Register a WS connection, enqueue a SendResponse with ProduceResponse,
        //       call processResponses(), verify confirm frame written to WS channel
    }

    @Test
    void processResponses_httpConnection_routesToHttpHandler() {
        // TODO: Register an HTTP channel (not WS), enqueue a SendResponse,
        //       call processResponses(), verify HTTP response written to HTTP channel
    }

    @Test
    void processResponses_closeResponse_removesWsConnection() {
        // TODO: Register WS, enqueue CloseConnectionResponse,
        //       call processResponses(), verify connection removed
    }

    // --- close() cleanup ---

    @Test
    void close_clearsWsConnections() {
        // TODO: Register WS connection, call close(), verify wsConnectionCount is 0
    }

    // --- Existing HTTP path not broken ---

    @Test
    void processResponses_mixedConnections_routesCorrectly() {
        // TODO: Register one HTTP and one WS connection,
        //       enqueue responses for both, verify each routed correctly
    }
}
```

### Existing pattern reference

```java
// From HttpProcessor.java lines 183-210 — processResponses() to modify:
public void processResponses() {
    List<RequestChannel.Response> batch = new ArrayList<>();
    responseQueue.drainTo(batch);

    for (RequestChannel.Response response : batch) {
        String connectionId = response.request().context().connectionId();
        try {
            if (response instanceof RequestChannel.SendResponse) {
                handleSendResponse((RequestChannel.SendResponse) response, connectionId);
            } else if (response instanceof RequestChannel.NoOpResponse) {
                sendNoContentResponse(connectionId);
                safeUpdateMetrics(response);
            } else if (response instanceof RequestChannel.CloseConnectionResponse) {
                ChannelHandlerContext ctx = channels.remove(connectionId);
                if (ctx != null) {
                    ctx.close();
                }
                inFlightCount.decrementAndGet();
            } else if (response instanceof RequestChannel.StartThrottlingResponse) {
                handleStartThrottling(response, connectionId);
            } else if (response instanceof RequestChannel.EndThrottlingResponse) {
                // No-op for HTTP
            }
        } catch (Exception e) {
            log.error("Error processing response for connection {}", connectionId, e);
        }
    }
}
```

---

## Tests

**Test class:** `http-server/src/test/java/kafka/server/http/HttpProcessorWsTest.java`

| Test method | What it verifies |
|-------------|-----------------|
| `registerWsConnection_addsToMap` | WS connection appears in isWsConnection |
| `unregisterWsConnection_removesFromMap` | WS connection removed after unregister |
| `wsConnectionCount_reflectsRegistrations` | Count tracks registrations |
| `isWsConnection_falseForHttpConnection` | HTTP connections not flagged as WS |
| `registerWsConnection_nullId_throwsNPE` | Null ID rejected |
| `registerWsConnection_nullCtx_throwsNPE` | Null context rejected |
| `processResponses_wsConnection_routesToWsHandler` | WS response routed to WS handler |
| `processResponses_httpConnection_routesToHttpHandler` | HTTP response routed to HTTP handler |
| `processResponses_closeResponse_removesWsConnection` | Close cleans up WS state |
| `close_clearsWsConnections` | Shutdown clears all WS connections |
| `processResponses_mixedConnections_routesCorrectly` | Mixed HTTP+WS routed correctly |

**Run command:**
```bash
./gradlew :http-server:test --tests "kafka.server.http.HttpProcessorWsTest"
```

---

## Rules

- Must NOT break existing HTTP response flow — HTTP connections continue to work unchanged.
- Use `wsConnections.get()` (not containsKey + get) to avoid race conditions.
- Always decrement `inFlightCount` after handling any WS response type.
- `close()` must clear `wsConnections` in addition to existing cleanup.
- ProduceResponse confirm only written if `confirmsEnabled` on the WsConnectionContext.
- FetchResponse for WS connections is a no-op in the processor (handled by WsConsumerFetchLoop).

---

## Learning

- **`wsConnections.get()` vs `containsKey() + get()`**. Using a single `get()` (non-null check) is atomically race-free against concurrent `unregister` AND one map lookup cheaper per response. Design doc pseudo-code used `containsKey`; implementation uses `get` per the CRITICAL note at line 154.
- **WsConnectionContext (WS1.02 stub) only exposes `sendFrame(String)`** — not the `writePublishConfirm(ProduceResponse, long)` helper the task spec draft described. The processor therefore owns JSON frame construction (`buildPublishConfirmFrame`, `buildThrottleFrame`). Moving that formatting into `WsConnectionContext` later is an easy refactor once `WsFrameSerializer` (WS1.09) lands.
- **`ProduceResponseData` aggregation**. `ProduceResponse` carries an arbitrary number of topic/partition tuples; a single WS publish lifts this to one `publishId`. The processor picks the first non-NONE partition error as the response verdict, matching RabbitMQ's publisher-confirm semantics (any nack on any shard fails the publish).
- **`requestLocalProperties` marker keys**. The HTTP path stores the `AbstractResponse` under `httpAbstractResponse`. The WS publish-confirm path reuses this key *and* adds `wsPublishId` (to be set by `WsPublishHandler`, TASK-WS1.11) for the confirm frame's correlation ID. If `wsPublishId` is absent the processor defaults to 0 so tests that don't wire through WS1.11 still pass.
- **`inFlightCount` accounting parity**. Every WS branch in `handleWsResponse` decrements the shared counter in a `finally` block. Missing this was explicitly called out in the task as a failure mode (counter leak -> connection rejection). The `finally` placement guarantees the decrement even if a Netty write throws.

---

## Limitations

- Publisher confirms are written synchronously on the response-drainer thread via `ctx.writeAndFlush`. Netty serialises onto its event loop, so correctness is fine, but the drainer blocks on queueing. If WS produce throughput becomes a hot path, consider batching multiple confirm frames before flushing.
- `buildPublishConfirmFrame` reports only the *first* partition error. A batched produce across multiple partitions that partially succeeds is reported as a full failure. This matches RabbitMQ's "any nack -> publish-failed" model and is the behaviour WS1.11 expects, but is lossy compared to the full Kafka `ProduceResponse`.
- `StartThrottlingResponse` is expressed as a WS JSON frame rather than transport-level throttling. HTTP clients get a proper HTTP 429 with `Retry-After`; WS clients get a data frame they must interpret. Consistent with plain-WebSocket protocol (no transport backpressure beyond TCP) but clients must parse `"type":"throttled"` frames specifically.
- The in-house `escapeJson` helper is minimal — sufficient for server-generated error messages, but downstream tasks that embed arbitrary user-supplied strings in WS frames should switch to Jackson for correctness under all Unicode edge cases.

---

## Field Notes

- Checkstyle flagged switch-case single-line statements (`case X: sb.append(...); break;`) under the `OneStatementPerLine` rule. Multi-line case blocks are required in this repo.
- `org.apache.kafka.common.message.ProduceResponseData.TopicProduceResponseCollection` is constructed from an `Iterator<TopicProduceResponse>` — not a `List`. Subtle gotcha when building test fixtures.
- Mockito's default behaviour for `when(ctx.writeAndFlush(any())).thenReturn(writeFuture)` does not auto-stub `writeFuture.addListener(...)`; tests that chain listeners would NPE without an explicit `when(writeFuture.addListener(any())).thenReturn(writeFuture)`. Replicated the pattern from `HttpProcessorTest`'s `mockActiveCtx` helper.
- Worktree snapshot base was stale (`f95a1f995d`, main's trunk HEAD). First action was `git fetch && git reset --hard origin/feature/http-protocol` to align with the actual `feature/http-protocol` HEAD (`f89cbe575b`).

---

## Acceptance Criteria

- [ ] `./gradlew :http-server:test --tests "kafka.server.http.HttpProcessorWsTest"` exits 0
- [ ] Existing `HttpProcessorTest` still passes (no regressions)
- [ ] `wsConnections` ConcurrentHashMap added to HttpProcessor
- [ ] `registerWsConnection()` and `unregisterWsConnection()` implemented
- [ ] `processResponses()` checks wsConnections BEFORE HTTP handling
- [ ] `handleWsResponse()` handles ProduceResponse → confirm frame
- [ ] `close()` clears wsConnections
- [ ] inFlightCount decremented for all WS response types
- [ ] Learning section filled with at least one entry
- [ ] Limitations section filled (use "None" if truly none)
- [ ] File Manifest section updated after commit

---

## File Manifest

> Filled by the executing agent after each commit.

<!-- ### YYYY-MM-DD — <short description> (commit <hash>)
Created:
  - path/to/NewFile.java — <what it does>
Modified:
  - path/to/Existing.java — <what changed>
-->
