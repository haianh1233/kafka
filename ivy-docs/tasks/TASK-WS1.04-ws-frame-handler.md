# TASK-WS1.04: WebSocket Frame Handler

## Prerequisites

- **TASK-WS1.01 completed** — `WsConfigs.java` exists, providing config values.
- **TASK-WS1.02 completed** — `WsConnectionContext.java` exists, providing per-connection state.
- **TASK-WS1.03 completed** — `WsUpgradeOrHttpHandler.java` exists, which creates `WsFrameHandler` instances during pipeline switch.

---

## Context

After a successful WebSocket upgrade, all communication happens via JSON text frames. The `WsFrameHandler` is the central dispatcher that parses each incoming text frame, extracts the `type` field, and routes to the appropriate handler method.

From the design doc §5.2 (Control Messages), the WebSocket protocol defines 15 client→broker message types:

| Type | Purpose |
|---|---|
| `declare-exchange` | Create/assert an exchange |
| `delete-exchange` | Delete an exchange |
| `declare-queue` | Create/assert a queue |
| `delete-queue` | Delete a queue |
| `bind` | Bind queue to exchange |
| `unbind` | Remove binding |
| `publish` | Publish message to exchange |
| `subscribe` | Subscribe to a queue |
| `unsubscribe` | Cancel subscription |
| `get` | Pull one message from a queue |
| `purge-queue` | Remove all messages from a queue |
| `ack` | Acknowledge delivered message(s) |
| `nack` | Negative-acknowledge message(s) |
| `credits` | Grant delivery credits |
| `enable-confirms` | Enable publisher confirms |

In this task, the frame handler parses the JSON and dispatches by type. The actual handler methods are stubs (throw `UnsupportedOperationException`) — they will be implemented in later tasks when the routing engine, publish handler, and subscription manager are available.

### Error handling

- Malformed JSON → error frame with `PROTOCOL_ERROR`
- Missing `type` field → error frame with `PROTOCOL_ERROR`
- Unknown `type` → error frame with `UNKNOWN_MESSAGE_TYPE`
- Error frames include the `id` from the request (if present) for client-side correlation

From the design doc §18 (Error Handling):
```json
{
  "type": "error",
  "id": "req-1",
  "errorCode": "QUEUE_NOT_FOUND",
  "errorMessage": "Queue 'orders' does not exist",
  "detail": { "failingOperation": "subscribe", "queue": "orders" }
}
```

---

## Specification

**Package:** `kafka.server.http.ws`

```java
public class WsFrameHandler extends SimpleChannelInboundHandler<TextWebSocketFrame> {

    public WsFrameHandler(WsConnectionContext connectionContext, WsConfigs wsConfigs);

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, TextWebSocketFrame frame);

    @Override
    public void channelInactive(ChannelHandlerContext ctx);

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause);

    // --- Dispatch methods (stubs in this task) ---
    void handleDeclareExchange(WsConnectionContext ctx, com.fasterxml.jackson.databind.JsonNode msg);
    void handleDeleteExchange(WsConnectionContext ctx, com.fasterxml.jackson.databind.JsonNode msg);
    void handleDeclareQueue(WsConnectionContext ctx, com.fasterxml.jackson.databind.JsonNode msg);
    void handleDeleteQueue(WsConnectionContext ctx, com.fasterxml.jackson.databind.JsonNode msg);
    void handleBind(WsConnectionContext ctx, com.fasterxml.jackson.databind.JsonNode msg);
    void handleUnbind(WsConnectionContext ctx, com.fasterxml.jackson.databind.JsonNode msg);
    void handlePublish(WsConnectionContext ctx, com.fasterxml.jackson.databind.JsonNode msg);
    void handleSubscribe(WsConnectionContext ctx, com.fasterxml.jackson.databind.JsonNode msg);
    void handleUnsubscribe(WsConnectionContext ctx, com.fasterxml.jackson.databind.JsonNode msg);
    void handleGet(WsConnectionContext ctx, com.fasterxml.jackson.databind.JsonNode msg);
    void handlePurgeQueue(WsConnectionContext ctx, com.fasterxml.jackson.databind.JsonNode msg);
    void handleAck(WsConnectionContext ctx, com.fasterxml.jackson.databind.JsonNode msg);
    void handleNack(WsConnectionContext ctx, com.fasterxml.jackson.databind.JsonNode msg);
    void handleCredits(WsConnectionContext ctx, com.fasterxml.jackson.databind.JsonNode msg);
    void handleEnableConfirms(WsConnectionContext ctx, com.fasterxml.jackson.databind.JsonNode msg);
}
```

**Behavioral contracts:**
- JSON parsing uses Jackson `ObjectMapper` (shared singleton, thread-safe).
- Each frame is parsed to a `JsonNode`. The `type` field is extracted as text.
- Dispatch is a switch/if-else on the type string to the appropriate handler method.
- Error frames are sent via `connectionContext.sendFrame()`.
- `channelInactive()` logs disconnection and cleans up subscriptions.
- `exceptionCaught()` logs the error and closes the channel.
- Rate limiting of control messages is NOT implemented in this task (separate task).

---

## Implementation Details

**Module:** `http-server`

**Files to study:**

| File | Why |
|------|-----|
| `http-server/src/main/java/kafka/server/http/HttpRouter.java` | Pattern for request type dispatching |

```java
// From HttpRouter.java lines 50-66 — pattern for type dispatching:

public enum HandlerType {
    PRODUCE, FETCH, METADATA_TOPIC, METADATA_ALL,
    LIST_OFFSETS, CONSUMER_LAG, COMMIT_OFFSETS, FETCH_OFFSETS,
    SHARE_POLL, SHARE_ACKNOWLEDGE, HEALTH, OPENAPI_SPEC
}

// The router dispatches by matching HTTP method + URI pattern.
// WsFrameHandler dispatches by matching the "type" field in JSON.
```

**Files to create:**

| File | What it does |
|------|--------------|
| `http-server/src/main/java/kafka/server/http/ws/WsFrameHandler.java` | JSON frame parser + type dispatcher |

**Files to modify:**

None.

> **CRITICAL:** The `ObjectMapper` must be a `private static final` singleton — creating a new ObjectMapper per frame is a performance disaster. Use `ObjectMapper.readTree(String)` for parsing (returns `JsonNode`) rather than deserializing to a POJO, since message shapes vary by type.

> **CRITICAL:** Handle the case where `frame.text()` returns empty string or whitespace — this should produce a `PROTOCOL_ERROR`, not a NullPointerException from Jackson.

**Implementation order:**
1. Create `WsFrameHandler.java` with JSON parsing, type dispatch, and error frame sending
2. All 15 handler methods are stubs that throw `UnsupportedOperationException("Not yet implemented: <type>")`
3. Create `WsFrameHandlerTest.java`

---

## Skeleton Code

### Production class

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
// Time: Created - TASK-WS1.04
package kafka.server.http.ws;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * Parses incoming WebSocket JSON text frames and dispatches by message type.
 *
 * // Time: Created - TASK-WS1.04
 */
public class WsFrameHandler extends SimpleChannelInboundHandler<TextWebSocketFrame> {

    private static final Logger log = LoggerFactory.getLogger(WsFrameHandler.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final WsConnectionContext connectionContext;
    private final WsConfigs wsConfigs;

    public WsFrameHandler(WsConnectionContext connectionContext, WsConfigs wsConfigs) {
        this.connectionContext = Objects.requireNonNull(connectionContext, "connectionContext");
        this.wsConfigs = Objects.requireNonNull(wsConfigs, "wsConfigs");
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, TextWebSocketFrame frame) {
        // TODO: 1. Parse JSON
        // TODO: 2. Extract "type" field
        // TODO: 3. Switch on type, call appropriate handler
        // TODO: 4. On error, send error frame with id from request
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        // TODO: log disconnection, clean up subscriptions
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // TODO: log error, close channel
    }

    // --- Dispatch methods (stubs) ---
    void handleDeclareExchange(WsConnectionContext ctx, JsonNode msg) {
        throw new UnsupportedOperationException("Not yet implemented: declare-exchange");
    }

    void handleDeleteExchange(WsConnectionContext ctx, JsonNode msg) {
        throw new UnsupportedOperationException("Not yet implemented: delete-exchange");
    }

    void handleDeclareQueue(WsConnectionContext ctx, JsonNode msg) {
        throw new UnsupportedOperationException("Not yet implemented: declare-queue");
    }

    void handleDeleteQueue(WsConnectionContext ctx, JsonNode msg) {
        throw new UnsupportedOperationException("Not yet implemented: delete-queue");
    }

    void handleBind(WsConnectionContext ctx, JsonNode msg) {
        throw new UnsupportedOperationException("Not yet implemented: bind");
    }

    void handleUnbind(WsConnectionContext ctx, JsonNode msg) {
        throw new UnsupportedOperationException("Not yet implemented: unbind");
    }

    void handlePublish(WsConnectionContext ctx, JsonNode msg) {
        throw new UnsupportedOperationException("Not yet implemented: publish");
    }

    void handleSubscribe(WsConnectionContext ctx, JsonNode msg) {
        throw new UnsupportedOperationException("Not yet implemented: subscribe");
    }

    void handleUnsubscribe(WsConnectionContext ctx, JsonNode msg) {
        throw new UnsupportedOperationException("Not yet implemented: unsubscribe");
    }

    void handleGet(WsConnectionContext ctx, JsonNode msg) {
        throw new UnsupportedOperationException("Not yet implemented: get");
    }

    void handlePurgeQueue(WsConnectionContext ctx, JsonNode msg) {
        throw new UnsupportedOperationException("Not yet implemented: purge-queue");
    }

    void handleAck(WsConnectionContext ctx, JsonNode msg) {
        throw new UnsupportedOperationException("Not yet implemented: ack");
    }

    void handleNack(WsConnectionContext ctx, JsonNode msg) {
        throw new UnsupportedOperationException("Not yet implemented: nack");
    }

    void handleCredits(WsConnectionContext ctx, JsonNode msg) {
        throw new UnsupportedOperationException("Not yet implemented: credits");
    }

    void handleEnableConfirms(WsConnectionContext ctx, JsonNode msg) {
        throw new UnsupportedOperationException("Not yet implemented: enable-confirms");
    }

    // --- Error frame helpers ---

    void sendErrorFrame(String correlationId, String errorCode, String errorMessage) {
        // TODO: build error JSON and send via connectionContext.sendFrame()
        throw new UnsupportedOperationException("Not yet implemented");
    }

    void sendErrorFrame(String correlationId, String errorCode, String errorMessage,
                        String failingOperation, String detail) {
        // TODO: build error JSON with detail object and send
        throw new UnsupportedOperationException("Not yet implemented");
    }
}
```

### Test class

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
// Time: Created - TASK-WS1.04
package kafka.server.http.ws;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * // Time: Created - TASK-WS1.04
 */
class WsFrameHandlerTest {

    private WsConnectionContext connectionCtx;
    private WsConfigs wsConfigs;

    @BeforeEach
    void setUp() {
        wsConfigs = WsConfigs.withDefaults();
        // TODO: set up mock connection context and embedded channel
    }

    @Test
    void malformedJson_sendsErrorFrame() {
        // TODO: send "not json" frame, verify error frame sent
    }

    @Test
    void missingTypeField_sendsErrorFrame() {
        // TODO: send {"foo":"bar"} frame, verify PROTOCOL_ERROR
    }

    @Test
    void unknownType_sendsErrorFrame() {
        // TODO: send {"type":"bogus"}, verify UNKNOWN_MESSAGE_TYPE
    }

    @Test
    void declareExchange_dispatchesToHandler() {
        // TODO: send {"type":"declare-exchange",...}, verify handleDeclareExchange called
    }

    @Test
    void publish_dispatchesToHandler() {
        // TODO: send {"type":"publish",...}, verify handlePublish called
    }

    @Test
    void emptyFrame_sendsErrorFrame() {
        // TODO: send empty string frame, verify PROTOCOL_ERROR
    }

    @Test
    void errorFrame_includesCorrelationId() {
        // TODO: send {"type":"bogus","id":"req-1"}, verify error frame has id="req-1"
    }
}
```

### Existing pattern reference

```java
// From HttpRouter.java lines 50-66 — type dispatching pattern:

public enum HandlerType {
    PRODUCE, FETCH, METADATA_TOPIC, METADATA_ALL,
    LIST_OFFSETS, CONSUMER_LAG, COMMIT_OFFSETS, FETCH_OFFSETS,
    SHARE_POLL, SHARE_ACKNOWLEDGE, HEALTH, OPENAPI_SPEC
}

// WsFrameHandler uses String matching instead of enum routing because
// the message types are JSON string values, not HTTP method+path combos.
```

---

## Tests

**Test class:** `http-server/src/test/java/kafka/server/http/ws/WsFrameHandlerTest.java`

| Test method | What it verifies |
|-------------|-----------------|
| `malformedJson_sendsErrorFrame` | Invalid JSON produces PROTOCOL_ERROR |
| `missingTypeField_sendsErrorFrame` | Missing "type" produces PROTOCOL_ERROR |
| `unknownType_sendsErrorFrame` | Unknown type produces UNKNOWN_MESSAGE_TYPE |
| `declareExchange_dispatchesToHandler` | Type routing works for declare-exchange |
| `publish_dispatchesToHandler` | Type routing works for publish |
| `emptyFrame_sendsErrorFrame` | Empty string handled gracefully |
| `errorFrame_includesCorrelationId` | Error frame echoes request "id" |

**Run command:**
```bash
timeout 300 ./gradlew :http-server:test --tests "kafka.server.http.ws.WsFrameHandlerTest"
```

---

## Rules

- `ObjectMapper` must be a `private static final` singleton — never instantiate per-frame.
- Use `ObjectMapper.readTree(String)` for parsing, not POJO deserialization.
- All 15 handler methods are stubs in this task — throw `UnsupportedOperationException`.
- Error frames must include the `id` from the incoming request if present.
- Handle empty/whitespace frames gracefully (PROTOCOL_ERROR, not NPE).

---

## Learning

- **Checkstyle NPath complexity rule bites linear pipelines.** The original
  `channelRead0` chained empty-check, JSON parse, shape guard, type extract, and
  dispatch in a single method.  Checkstyle computed NPath = 543 (ceiling 500).
  The rule multiplies, not adds — every `||` inside an `if` still expands its
  product of paths.  Splitting parse-and-validate into `parseFrame()` and
  `routeAndDispatch()` (and lifting `extractType()` / `isBlank()` out) dropped
  each method comfortably under the ceiling without changing behaviour.
- **`ObjectMapper.readTree("null")` does not return Java `null`.** It returns a
  `NullNode` whose `isObject()` is `false`.  The `msg == null || !msg.isObject()`
  guard catches both cases — a useful double-check that saved a subtle bug
  (dispatching on a `NullNode.get("type")` throws NPE).
- **`SimpleChannelInboundHandler` auto-releases the text frame** after
  `channelRead0` returns, so we never call `frame.release()` ourselves.  In the
  unit test we invoke `handler.channelRead(...)` (not `channelRead0`) to keep
  the auto-release semantics and avoid leak warnings.
- **`Objects.requireNonNullElse` is the clean way** to default null code or
  message strings in `sendErrorFrame` — no need for a local `if` chain.

---

## Limitations

- **All 15 dispatch methods are stubs.**  Each throws
  `UnsupportedOperationException("Not yet implemented: <type>")`.  The frame
  handler catches that exception and converts it into an `INTERNAL_ERROR`
  frame with a helpful message so a half-built server does not silently drop
  client frames.  Later tasks will fill these in:
  - `handlePublish` → TASK-WS1.11 (`WsPublishHandler`)
  - `handleSubscribe` / `handleUnsubscribe` / `handleCredits` → WS1.14 (`WsCreditManager`)
    + WS1.15 (`WsSubscriptionManager`)
  - `handleAck` / `handleNack` → WS1.16 (`WsAckHandler`)
  - `handleDeclareExchange` / `handleDeleteExchange` → WS1.06 (`ExchangeManager`)
  - `handleBind` / `handleUnbind` → WS1.07 (`BindingManager`)
  - `handleDeclareQueue` / `handleDeleteQueue` / `handleGet` / `handlePurgeQueue`
    → later task (queue manager not yet scheduled in WS1).
  - `handleEnableConfirms` → WS3.01 (publisher confirms).
- **Rate limiting is out of scope** per the task spec ("Rate limiting of
  control messages is NOT implemented in this task — separate task").  The
  `maxControlMessagesPerSecond()` config value is read but not enforced here;
  enforcement will be added alongside the publisher/consumer implementations
  so it can see the whole per-connection traffic profile.
- **WsUpgradeOrHttpHandler is not modified.**  The task file explicitly says
  "Files to modify: None".  The upgrade handler still installs a no-op
  placeholder at pipeline name `ws-handler`; a follow-up task (or the
  integration test task WS1.17) will swap the placeholder for
  `new WsFrameHandler(connCtx, wsConfigs)`.  This isolation keeps WS1.03's
  test suite green and avoids touching the well-tested upgrade path before
  the dispatch handlers are wired in.
- **`channelInactive` clears the subscription map but does not yet drain
  in-flight deliveries.**  Subscription tear-down with credit / ack cleanup
  belongs in `WsSubscriptionManager` (WS1.15) and will be invoked here once
  that manager is reachable from the connection context.

---

## Field Notes

- Checkstyle NPath rule is worth remembering for future WS frame-heavy
  handlers: prefer decomposing parse pipelines into small named methods
  rather than chaining `if/return` guards in one body.
- Jackson `ObjectNode.put(String,String)` treats a `null` value as an explicit
  JSON `null`; we guarded the optional `detail` fields in `sendErrorFrame` by
  building the sub-object only when at least one of the two inputs was
  non-null.  That keeps the wire format minimal.
- The per-type dispatch test is parameterised via a helper
  (`assertDispatchedTo`) that overrides every handler and flips a single
  flag when the expected method is called.  This is ugly but obvious and
  avoids pulling in a test-only matcher library.  15 tests × 2 lines each is
  worth the duplication for the readability.
- Initial implementation wrote dispatch as a big `switch`.  Once the
  checkstyle NPath rule fired, the switch turned out to be innocent (it
  contributes linearly to NPath); the real culprit was the compound guards
  in `channelRead0`.  Moral: measure before refactoring.

---

## Acceptance Criteria

- [ ] `WsFrameHandler.java` exists at `http-server/src/main/java/kafka/server/http/ws/WsFrameHandler.java`
- [ ] Extends `SimpleChannelInboundHandler<TextWebSocketFrame>`
- [ ] Parses JSON using static `ObjectMapper` singleton
- [ ] Dispatches all 15 message types to handler methods
- [ ] Sends error frames for malformed JSON, missing type, unknown type
- [ ] Error frames include correlation `id` from request
- [ ] All 15 handler methods throw `UnsupportedOperationException`
- [ ] `channelInactive()` logs and cleans up
- [ ] `exceptionCaught()` logs and closes channel
- [ ] `timeout 300 ./gradlew :http-server:test --tests "kafka.server.http.ws.WsFrameHandlerTest"` exits 0
- [ ] Learning section filled with at least one entry
- [ ] Limitations section filled (use "None" if truly none)

---

## File Manifest

> Filled by the executing agent after each commit.
> Run: `git diff --name-status HEAD~1 HEAD -- '*.java' '*.xml' '*.json' '*.yaml' '*.yml'`

```
A  http-server/src/main/java/kafka/server/http/ws/WsFrameHandler.java
A  http-server/src/test/java/kafka/server/http/ws/WsFrameHandlerTest.java
```

**Test count:** 32 tests, all passing.

**Run command:**
```
timeout 300 ./gradlew :http-server:test --tests "kafka.server.http.ws.WsFrameHandlerTest"
```
