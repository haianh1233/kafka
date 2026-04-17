# TASK-WS1.02: WebSocket Connection Context

## Prerequisites

- **TASK-WS1.01 completed** — `WsConfigs.java` exists at `http-server/src/main/java/kafka/server/http/ws/WsConfigs.java` providing runtime config accessors.

---

## Context

Each WebSocket connection needs per-connection state that is accessed from multiple threads: the Netty I/O worker thread (frame reads/writes), the consumer executor threads (deliver frames), and the response drainer thread (publish confirms). This state must be thread-safe.

`WsConnectionContext` is a simple, self-contained value holder that encapsulates connection identity (session ID, principal, vhost), the Netty channel reference, active subscriptions, publish confirm mode, and connection metadata. It has no dependencies on other WS classes — it is a leaf node in the dependency graph, making it an ideal early task.

From the design doc §5 (WebSocket API Specification), after the WebSocket upgrade handshake, the broker sends a `connected` message containing the `sessionId`. The connection context holds this session ID and is the single source of truth for all per-connection state.

From the design doc §15.1 (Integration with Existing Infrastructure), the `WsConnectionContext` is created during the upgrade handshake in `WsUpgradeOrHttpHandler` and passed to `WsFrameHandler`. The same context is also registered in `HttpProcessor` for response routing.

### Thread safety requirements

- `sessionId`, `principal`, `vhost`, `connectTime`, `remoteAddress`: immutable after construction — safe for concurrent reads.
- `channel` (ChannelHandlerContext): Netty guarantees thread safety for write operations via `channel.writeAndFlush()`.
- `subscriptions`: ConcurrentHashMap — concurrent adds from Netty I/O thread, removes from consumer threads on unsubscribe/cancel.
- `publishConfirmsEnabled`: AtomicBoolean — set from Netty I/O thread on `enable-confirms`, read from response drainer.

---

## Specification

**Package:** `kafka.server.http.ws`

```java
public final class WsConnectionContext {

    // Constructor — all fields set at creation time
    public WsConnectionContext(
        String sessionId,
        KafkaPrincipal principal,
        String vhost,
        ChannelHandlerContext channel,
        InetSocketAddress remoteAddress);

    // --- Immutable accessors ---
    public String sessionId();
    public KafkaPrincipal principal();
    public String vhost();
    public ChannelHandlerContext channel();
    public Instant connectTime();
    public InetSocketAddress remoteAddress();

    // --- Mutable state ---
    public ConcurrentHashMap<String, Object> subscriptions();
    public boolean isPublishConfirmsEnabled();
    public void enablePublishConfirms();

    // --- Channel operations ---
    public void sendFrame(String jsonFrame);
    public void close(int code, String reason);
    public boolean isActive();
}
```

**Behavioral contracts:**
- `sessionId` format: `"ws-{brokerId}-{uuid8}"` (e.g., `"ws-3-af72b1c4"`)
- `vhost` defaults to `"/"` if not specified in the upgrade request
- `connectTime` is set to `Instant.now()` at construction
- `sendFrame()` writes a `TextWebSocketFrame` to the channel; safe to call from any thread
- `close()` sends a `CloseWebSocketFrame` and closes the channel
- `isActive()` delegates to `channel.channel().isActive()`
- `enablePublishConfirms()` is idempotent — calling it twice is a no-op
- Subscriptions map: key = subscription tag (String), value = subscription state object (Object for now, typed in later tasks)

---

## Implementation Details

**Module:** `http-server`

**Files to study:**

| File | Why |
|------|-----|
| `http-server/src/main/java/kafka/server/http/HttpProcessor.java` | Shows how Netty channel context is used for response writes |

```java
// From http-server/src/main/java/kafka/server/http/HttpProcessor.java lines 1-50
// Pattern: holding ChannelHandlerContext reference for async writes

// HttpProcessor uses ConcurrentHashMap<String, ChannelHandlerContext> to track connections
// and writes responses from a separate thread. WsConnectionContext follows the same pattern
// but encapsulates more per-connection state.
```

**Files to create:**

| File | What it does |
|------|--------------|
| `http-server/src/main/java/kafka/server/http/ws/WsConnectionContext.java` | Per-connection state holder |

**Files to modify:**

None.

> **CRITICAL:** The `subscriptions` map value type is `Object` in this task because the subscription state class is defined in a later task (WsSubscriptionManager). Do NOT create a concrete subscription type here — use `Object` as placeholder. Later tasks will refine this.

**Implementation order:**
1. Create `WsConnectionContext.java`
2. Create `WsConnectionContextTest.java`

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
// Time: Created - TASK-WS1.02
package kafka.server.http.ws;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Per-WebSocket-connection state. Thread-safe: accessed from Netty I/O,
 * consumer executor, and response drainer threads.
 *
 * // Time: Created - TASK-WS1.02
 */
public final class WsConnectionContext {

    private final String sessionId;
    private final KafkaPrincipal principal;
    private final String vhost;
    private final ChannelHandlerContext channel;
    private final Instant connectTime;
    private final InetSocketAddress remoteAddress;
    private final ConcurrentHashMap<String, Object> subscriptions;
    private final AtomicBoolean publishConfirmsEnabled;

    public WsConnectionContext(
            String sessionId,
            KafkaPrincipal principal,
            String vhost,
            ChannelHandlerContext channel,
            InetSocketAddress remoteAddress) {
        this.sessionId = Objects.requireNonNull(sessionId, "sessionId");
        this.principal = Objects.requireNonNull(principal, "principal");
        this.vhost = Objects.requireNonNull(vhost, "vhost");
        this.channel = Objects.requireNonNull(channel, "channel");
        this.remoteAddress = Objects.requireNonNull(remoteAddress, "remoteAddress");
        this.connectTime = Instant.now();
        this.subscriptions = new ConcurrentHashMap<>();
        this.publishConfirmsEnabled = new AtomicBoolean(false);
    }

    public String sessionId() { return sessionId; }
    public KafkaPrincipal principal() { return principal; }
    public String vhost() { return vhost; }
    public ChannelHandlerContext channel() { return channel; }
    public Instant connectTime() { return connectTime; }
    public InetSocketAddress remoteAddress() { return remoteAddress; }
    public ConcurrentHashMap<String, Object> subscriptions() { return subscriptions; }

    public boolean isPublishConfirmsEnabled() {
        return publishConfirmsEnabled.get();
    }

    public void enablePublishConfirms() {
        publishConfirmsEnabled.set(true);
    }

    public void sendFrame(String jsonFrame) {
        // TODO: write TextWebSocketFrame to channel
        throw new UnsupportedOperationException("Not yet implemented");
    }

    public void close(int code, String reason) {
        // TODO: send CloseWebSocketFrame and close channel
        throw new UnsupportedOperationException("Not yet implemented");
    }

    public boolean isActive() {
        return channel.channel().isActive();
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
// Time: Created - TASK-WS1.02
package kafka.server.http.ws;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * // Time: Created - TASK-WS1.02
 */
class WsConnectionContextTest {

    private WsConnectionContext ctx;
    private ChannelHandlerContext mockChannel;

    @BeforeEach
    void setUp() {
        mockChannel = mock(ChannelHandlerContext.class);
        // TODO: set up mock channel properly
        ctx = new WsConnectionContext(
            "ws-1-abc12345",
            new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "testuser"),
            "/",
            mockChannel,
            new InetSocketAddress("127.0.0.1", 54321));
    }

    @Test
    void constructor_setsImmutableFields() {
        assertEquals("ws-1-abc12345", ctx.sessionId());
        assertEquals("testuser", ctx.principal().getName());
        assertEquals("/", ctx.vhost());
        assertNotNull(ctx.connectTime());
        assertEquals(54321, ctx.remoteAddress().getPort());
    }

    @Test
    void subscriptions_startsEmpty() {
        assertTrue(ctx.subscriptions().isEmpty());
    }

    @Test
    void publishConfirms_defaultDisabled() {
        assertFalse(ctx.isPublishConfirmsEnabled());
    }

    @Test
    void enablePublishConfirms_setsFlag() {
        ctx.enablePublishConfirms();
        assertTrue(ctx.isPublishConfirmsEnabled());
    }

    @Test
    void enablePublishConfirms_idempotent() {
        ctx.enablePublishConfirms();
        ctx.enablePublishConfirms();
        assertTrue(ctx.isPublishConfirmsEnabled());
    }

    @Test
    void nullSessionId_throwsNPE() {
        assertThrows(NullPointerException.class, () -> new WsConnectionContext(
            null,
            new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "u"),
            "/", mockChannel, new InetSocketAddress("127.0.0.1", 1)));
    }

    @Test
    void nullPrincipal_throwsNPE() {
        assertThrows(NullPointerException.class, () -> new WsConnectionContext(
            "ws-1-x", null, "/", mockChannel, new InetSocketAddress("127.0.0.1", 1)));
    }

    @Test
    void nullVhost_throwsNPE() {
        assertThrows(NullPointerException.class, () -> new WsConnectionContext(
            "ws-1-x",
            new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "u"),
            null, mockChannel, new InetSocketAddress("127.0.0.1", 1)));
    }

    @Test
    void subscriptions_concurrentAdd() {
        ctx.subscriptions().put("sub-1", "placeholder");
        ctx.subscriptions().put("sub-2", "placeholder");
        assertEquals(2, ctx.subscriptions().size());
    }
}
```

### Existing pattern reference

```java
// From http-server/src/main/java/kafka/server/http/HttpProcessor.java lines 39-42
// Pattern: ConcurrentHashMap tracking per-connection state, AtomicBoolean for flags

private final ConcurrentHashMap<String, ChannelHandlerContext> channels = new ConcurrentHashMap<>();
private final AtomicBoolean draining;
private final AtomicInteger inFlightCount;
```

---

## Tests

**Test class:** `http-server/src/test/java/kafka/server/http/ws/WsConnectionContextTest.java`

| Test method | What it verifies |
|-------------|-----------------|
| `constructor_setsImmutableFields` | All constructor args accessible via getters |
| `subscriptions_startsEmpty` | Subscriptions map is empty on creation |
| `publishConfirms_defaultDisabled` | Publish confirms off by default |
| `enablePublishConfirms_setsFlag` | enablePublishConfirms() sets true |
| `enablePublishConfirms_idempotent` | Calling twice is safe |
| `nullSessionId_throwsNPE` | Constructor rejects null sessionId |
| `nullPrincipal_throwsNPE` | Constructor rejects null principal |
| `nullVhost_throwsNPE` | Constructor rejects null vhost |
| `subscriptions_concurrentAdd` | ConcurrentHashMap add works |

**Run command:**
```bash
timeout 300 ./gradlew :http-server:test --tests "kafka.server.http.ws.WsConnectionContextTest"
```

---

## Rules

- All constructor parameters that are objects must be null-checked with `Objects.requireNonNull`.
- Immutable fields (`sessionId`, `principal`, `vhost`, `connectTime`, `remoteAddress`) are final and set in constructor.
- Mutable state (`subscriptions`, `publishConfirmsEnabled`) uses concurrent data structures.
- The subscription value type is `Object` in this task — do not introduce a subscription state class.

---

## Learning

- `Objects.requireNonNull(arg, "arg")` is the idiomatic constructor null-check; the NPE message matches the parameter name, which makes failure diagnosis trivial.
- Netty's `ChannelHandlerContext.writeAndFlush(msg)` transfers ownership of the `ByteBuf`-backed frame to the pipeline. Tests that capture the frame via `ArgumentCaptor` MUST call `frame.release()` to avoid leak warnings.
- `CloseWebSocketFrame` is followed by `ChannelFutureListener.CLOSE` so the TCP close happens only after the close frame has been flushed — otherwise peers see a TCP RST and lose the close reason.
- `ConcurrentHashMap` + `AtomicBoolean` together cover all the mutable state needed for this class; no extra synchronisation blocks are required because each field's invariants are independent.
- Mockito `any()` matcher on `writeAndFlush` is required because `close()` chains `.addListener` on the returned `ChannelFuture` — stubbing is mandatory, otherwise the mocked return value is `null` and `.addListener` throws NPE.

---

## Limitations

- `subscriptions()` returns the live mutable `ConcurrentHashMap`, not a defensive copy. This is intentional (callers mutate it) but means a misbehaving caller could clear the map. Acceptable for an internal-only type.
- `sendFrame()` ignores the returned `ChannelFuture` — callers that need write-completion semantics (e.g. back-pressure on slow peers) will have to access `channel()` directly. Revisit once a WS back-pressure task lands.
- The subscription value type is `Object`; a typed `WsSubscription` is deferred to the subscription-manager task per the spec.

---

## Field Notes

- Worktree cross-contamination: the main repo (`/home/anh/kafka`) had untracked leftover files from other concurrent worktrees (`WsSubscriptionManager.java`, `WsConsumerFetchLoop.java`, `WsRoutingMetadataManager.java`, `ExchangeMetadata.java`, `QueueMetadata.java`, `BindingMetadata.java`, `SubscriptionContext.java`, plus matching tests) that broke `:http-server:compileJava` in the main repo. They did NOT affect this worktree once commands were run from the worktree directory. Running gradle from the worktree path (`/home/anh/kafka/.claude/worktrees/agent-ac9570ee`) sees only files tracked by this branch plus my new ones.
- Lesson: when writing files in a worktree session, ensure the absolute path is under the worktree root — `/home/anh/kafka/http-server/...` points to the *main* checkout, not the worktree.
- Test count: 19 methods, all green. Thread-safety smoke test spins 8 threads x 500 ops on the subscriptions map; completes well under a second.

---

## Acceptance Criteria

- [ ] `WsConnectionContext.java` exists at `http-server/src/main/java/kafka/server/http/ws/WsConnectionContext.java`
- [ ] All 8 fields accessible via typed getters
- [ ] `connectTime` is set automatically at construction (not passed as parameter)
- [ ] `subscriptions()` returns a `ConcurrentHashMap<String, Object>`
- [ ] `enablePublishConfirms()` is idempotent
- [ ] All 5 object constructor params are null-checked
- [ ] `sendFrame()` writes a `TextWebSocketFrame` to the Netty channel
- [ ] `close()` sends a `CloseWebSocketFrame` with the given code and reason
- [ ] `isActive()` delegates to the Netty channel
- [ ] `timeout 300 ./gradlew :http-server:test --tests "kafka.server.http.ws.WsConnectionContextTest"` exits 0
- [ ] Learning section filled with at least one entry
- [ ] Limitations section filled (use "None" if truly none)

---

## File Manifest

> Filled by the executing agent after each commit.
> Run: `git diff --name-status HEAD~1 HEAD -- '*.java' '*.xml' '*.json' '*.yaml' '*.yml'`
