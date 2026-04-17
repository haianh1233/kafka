# TASK-WS1.03: WebSocket Upgrade or HTTP Handler

## Prerequisites

- **TASK-WS1.01 completed** — `WsConfigs.java` exists, providing `maxFrameSize()` and `wsEnabled()`.
- **TASK-WS1.02 completed** — `WsConnectionContext.java` exists, providing per-connection state.

---

## Context

The existing Kafka HTTP pipeline processes every request through a Netty pipeline ending with `HttpRequestHandler`. To support WebSocket connections on the same port, we need a handler that detects WebSocket upgrade requests and switches the Netty pipeline from HTTP mode to WebSocket mode.

`WsUpgradeOrHttpHandler` sits at the end of the HTTP pipeline (replacing `HttpRequestHandler` as the terminal handler for the first request). It inspects each `FullHttpRequest`:
- If the request has `Upgrade: websocket` header AND the path is `/v1/ws`, it performs the WebSocket handshake, reconfigures the pipeline, creates a `WsConnectionContext`, and sends a `connected` message.
- Otherwise, it passes the request to the next handler (standard HTTP flow).

### Design doc §15.1 — Integration with Existing Infrastructure

```
// In HttpChannelInitializer.configureHttp11Pipeline():

// Existing HTTP pipeline (unchanged)
pipeline.addLast("http-codec",      new HttpServerCodec())
pipeline.addLast("http-aggregator", new HttpObjectAggregator(maxRequestBytes))
pipeline.addLast("compressor",      new HttpContentCompressor())
pipeline.addLast("idle-handler",    new IdleStateHandler(...))
pipeline.addLast("idle-closer",     new IdleStateCloseHandler())

// NEW: WebSocket upgrade detection + standard HTTP handler
pipeline.addLast("ws-or-http",      new WsUpgradeOrHttpHandler(...))
```

On upgrade, the handler:
1. Extracts authentication from the upgrade request (same `KafkaPrincipalBuilder` as HTTP).
2. Performs WebSocket handshake using `WebSocketServerHandshakerFactory`.
3. Replaces the pipeline: removes `http-aggregator`, `compressor`, and `ws-or-http`; adds `WebSocketFrameAggregator` and `WsFrameHandler`.
4. Sends a `connected` JSON message with `brokerId`, `clusterId`, `sessionId`, and `serverCapabilities`.

### Design doc §5 — Connection Metadata

After upgrade, the broker sends:
```json
{
  "type": "connected",
  "brokerId": 3,
  "clusterId": "abc123",
  "sessionId": "ws-3-af72b1c4",
  "serverCapabilities": ["exchange.direct", "exchange.topic", "exchange.fanout",
                         "exchange.headers", "publisher-confirms", "credits"]
}
```

---

## Specification

**Package:** `kafka.server.http.ws`

```java
public class WsUpgradeOrHttpHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

    public WsUpgradeOrHttpHandler(
        WsConfigs wsConfigs,
        int brokerId,
        String clusterId,
        KafkaPrincipalBuilder principalBuilder,
        SecurityProtocol securityProtocol);

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest req);

    // Visible for testing
    static boolean isWebSocketUpgrade(FullHttpRequest req);

    // Visible for testing
    static String extractVhost(FullHttpRequest req);
}
```

**Behavioral contracts:**
- `isWebSocketUpgrade(req)` returns true iff the request has `Upgrade: websocket` header (case-insensitive) AND `Connection: Upgrade` header AND path starts with `/v1/ws`.
- `extractVhost(req)` parses the `vhost` query parameter from the URI (e.g., `/v1/ws?vhost=/production` → `"/production"`). Returns `"/"` if absent.
- On successful upgrade: pipeline has `ws-frame-aggregator` and `ws-handler` handlers; `http-aggregator`, `compressor`, and `ws-or-http` are removed.
- On non-upgrade: request is retained and forwarded to the next handler via `ctx.fireChannelRead(req.retain())`.
- If `wsConfigs.wsEnabled()` is false, upgrade requests receive HTTP 404.
- Session ID format: `"ws-{brokerId}-{uuid8}"` where uuid8 is the first 8 chars of a random UUID.

---

## Implementation Details

**Module:** `http-server`

**Files to study:**

| File | Why |
|------|-----|
| `http-server/src/main/scala/kafka/network/HttpChannelInitializer.scala` | Pipeline configuration pattern (lines 98-115) |
| `http-server/src/main/scala/kafka/network/HttpRequestHandler.scala` | How existing handler processes FullHttpRequest |

```scala
// From HttpChannelInitializer.scala lines 98-115 — pipeline to modify:

private[network] def configureHttp11Pipeline(pipeline: ChannelPipeline): Unit = {
    pipeline.addLast("http-codec", new HttpServerCodec())
    corsConfig.foreach { cc =>
      pipeline.addLast("cors", new CorsHandler(cc))
    }
    pipeline.addLast("http-aggregator",
      new HttpObjectAggregator(maxRequestBytes))
    pipeline.addLast("compressor", new HttpContentCompressor())
    pipeline.addLast("idle-handler", new IdleStateHandler(
      0, 0, connectionIdleTimeoutMs, TimeUnit.MILLISECONDS))
    pipeline.addLast("idle-closer", new IdleStateCloseHandler(httpMetrics))
    pipeline.addLast("kafka-handler",
      new HttpRequestHandler(principalBuilder, securityProtocol, draining, inFlightCount,
        brokerId, clusterId, requestChannel, httpProcessor, metadataSupplier, topicIdSupplier, httpServerConfigs))
}
```

**Files to create:**

| File | What it does |
|------|--------------|
| `http-server/src/main/java/kafka/server/http/ws/WsUpgradeOrHttpHandler.java` | WebSocket upgrade detection + pipeline switch |

**Files to modify:**

None in this task. The wiring of `WsUpgradeOrHttpHandler` into `HttpChannelInitializer` is a separate integration task.

> **CRITICAL:** When removing pipeline handlers during upgrade, check that the handler name exists before removing (use `pipeline.get("name") != null`). The CORS handler may or may not be present depending on config. Do NOT attempt to remove "cors" handler — it is harmless for WebSocket and stays in the pipeline.

> **CRITICAL:** After `handshaker.handshake()`, the response is sent asynchronously. The pipeline modification and `connected` message must happen in the handshake future's completion listener, not synchronously after the call.

**Implementation order:**
1. Create `WsUpgradeOrHttpHandler.java`
2. Create `WsUpgradeOrHttpHandlerTest.java` using Netty `EmbeddedChannel`

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
// Time: Created - TASK-WS1.03
package kafka.server.http.ws;

import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrameAggregator;
import io.netty.handler.codec.http.websocketx.WebSocketServerHandshaker;
import io.netty.handler.codec.http.websocketx.WebSocketServerHandshakerFactory;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.KafkaPrincipalBuilder;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * Detects WebSocket upgrade requests on /v1/ws and switches the Netty pipeline.
 * Non-upgrade requests are passed to the next handler (standard HTTP flow).
 *
 * // Time: Created - TASK-WS1.03
 */
public class WsUpgradeOrHttpHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

    private static final Logger log = LoggerFactory.getLogger(WsUpgradeOrHttpHandler.class);
    private static final String WS_PATH = "/v1/ws";

    private final WsConfigs wsConfigs;
    private final int brokerId;
    private final String clusterId;
    private final KafkaPrincipalBuilder principalBuilder;
    private final SecurityProtocol securityProtocol;

    public WsUpgradeOrHttpHandler(
            WsConfigs wsConfigs,
            int brokerId,
            String clusterId,
            KafkaPrincipalBuilder principalBuilder,
            SecurityProtocol securityProtocol) {
        this.wsConfigs = Objects.requireNonNull(wsConfigs, "wsConfigs");
        this.brokerId = brokerId;
        this.clusterId = Objects.requireNonNull(clusterId, "clusterId");
        this.principalBuilder = Objects.requireNonNull(principalBuilder, "principalBuilder");
        this.securityProtocol = Objects.requireNonNull(securityProtocol, "securityProtocol");
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest req) {
        // TODO: implement — check isWebSocketUpgrade, then either upgradeToWebSocket or fireChannelRead
        throw new UnsupportedOperationException("Not yet implemented");
    }

    static boolean isWebSocketUpgrade(FullHttpRequest req) {
        // TODO: check Upgrade: websocket header + Connection: Upgrade + path /v1/ws
        throw new UnsupportedOperationException("Not yet implemented");
    }

    static String extractVhost(FullHttpRequest req) {
        // TODO: parse vhost query param, default "/"
        throw new UnsupportedOperationException("Not yet implemented");
    }

    private void upgradeToWebSocket(ChannelHandlerContext ctx, FullHttpRequest req) {
        // TODO: 1. Extract auth principal
        // TODO: 2. Perform WebSocket handshake
        // TODO: 3. In handshake completion listener: replace pipeline, create context, send connected
        throw new UnsupportedOperationException("Not yet implemented");
    }

    private String generateSessionId() {
        return "ws-" + brokerId + "-" + UUID.randomUUID().toString().substring(0, 8);
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
// Time: Created - TASK-WS1.03
package kafka.server.http.ws;

import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * // Time: Created - TASK-WS1.03
 */
class WsUpgradeOrHttpHandlerTest {

    @Test
    void isWebSocketUpgrade_validUpgrade_returnsTrue() {
        FullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/v1/ws");
        req.headers().set(HttpHeaderNames.UPGRADE, HttpHeaderValues.WEBSOCKET);
        req.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.UPGRADE);
        assertTrue(WsUpgradeOrHttpHandler.isWebSocketUpgrade(req));
        req.release();
    }

    @Test
    void isWebSocketUpgrade_noUpgradeHeader_returnsFalse() {
        FullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/v1/ws");
        assertFalse(WsUpgradeOrHttpHandler.isWebSocketUpgrade(req));
        req.release();
    }

    @Test
    void isWebSocketUpgrade_wrongPath_returnsFalse() {
        FullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/v1/topics");
        req.headers().set(HttpHeaderNames.UPGRADE, HttpHeaderValues.WEBSOCKET);
        req.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.UPGRADE);
        assertFalse(WsUpgradeOrHttpHandler.isWebSocketUpgrade(req));
        req.release();
    }

    @Test
    void extractVhost_present_returnsValue() {
        FullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/v1/ws?vhost=/production");
        assertEquals("/production", WsUpgradeOrHttpHandler.extractVhost(req));
        req.release();
    }

    @Test
    void extractVhost_absent_returnsDefault() {
        FullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/v1/ws");
        assertEquals("/", WsUpgradeOrHttpHandler.extractVhost(req));
        req.release();
    }

    @Test
    void isWebSocketUpgrade_withQueryParams_returnsTrue() {
        FullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/v1/ws?vhost=/dev");
        req.headers().set(HttpHeaderNames.UPGRADE, HttpHeaderValues.WEBSOCKET);
        req.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.UPGRADE);
        assertTrue(WsUpgradeOrHttpHandler.isWebSocketUpgrade(req));
        req.release();
    }

    // TODO: test channelRead0 with EmbeddedChannel for full pipeline switch
}
```

### Existing pattern reference

```scala
// From HttpChannelInitializer.scala lines 98-115 — pipeline handlers to interact with:

pipeline.addLast("http-codec", new HttpServerCodec())
pipeline.addLast("http-aggregator", new HttpObjectAggregator(maxRequestBytes))
pipeline.addLast("compressor", new HttpContentCompressor())
pipeline.addLast("idle-handler", new IdleStateHandler(0, 0, connectionIdleTimeoutMs, TimeUnit.MILLISECONDS))
pipeline.addLast("idle-closer", new IdleStateCloseHandler(httpMetrics))
pipeline.addLast("kafka-handler", new HttpRequestHandler(...))
```

---

## Tests

**Test class:** `http-server/src/test/java/kafka/server/http/ws/WsUpgradeOrHttpHandlerTest.java`

| Test method | What it verifies |
|-------------|-----------------|
| `isWebSocketUpgrade_validUpgrade_returnsTrue` | Correct headers + path detected |
| `isWebSocketUpgrade_noUpgradeHeader_returnsFalse` | Missing upgrade header rejected |
| `isWebSocketUpgrade_wrongPath_returnsFalse` | Non-ws path rejected |
| `extractVhost_present_returnsValue` | Query param parsed |
| `extractVhost_absent_returnsDefault` | Default "/" returned |
| `isWebSocketUpgrade_withQueryParams_returnsTrue` | Path with query params still matches |

**Run command:**
```bash
timeout 300 ./gradlew :http-server:test --tests "kafka.server.http.ws.WsUpgradeOrHttpHandlerTest"
```

---

## Rules

- Do NOT modify `HttpChannelInitializer.scala` in this task. Pipeline wiring is a separate integration task.
- After handshake, pipeline modification must happen in the handshake future's completion listener.
- `FullHttpRequest` must be `retain()`ed before `ctx.fireChannelRead()` for non-upgrade requests.
- Release `FullHttpRequest` properly to avoid Netty reference count leaks.

---

## Learning

- Netty's `WebSocketServerHandshaker.handshake()` writes an HTTP 101 `FullHttpResponse`.
  For this to succeed the pipeline needs an `HttpServerCodec` upstream of the upgrade
  handler — `HttpObjectAggregator` alone is not enough. Production satisfies this via
  `HttpChannelInitializer.configureHttp11Pipeline`, but tests must mirror it or the
  handshake future silently fails and the pipeline never rewires.
- `SimpleChannelInboundHandler` auto-releases the incoming `FullHttpRequest` after
  `channelRead0` returns. Forwarding non-upgrade requests therefore requires
  `ctx.fireChannelRead(req.retain())` — otherwise the downstream handler receives a
  released buffer.
- The `Connection` header is case-insensitive AND comma-separated (e.g.
  `keep-alive, Upgrade`); a simple `equalsIgnoreCase("Upgrade")` over the raw value
  misses real-world browser requests. Splitting on `,` and trimming each element is
  the robust check.
- `QueryStringDecoder.path()` is the right way to separate path from query string
  when validating the upgrade target — `FullHttpRequest.uri()` returns the raw URI.
- SpotBugs flagged `remote == null` after a non-null fallback: removed the
  defensive check and dropped the unused `FullHttpRequest` parameter from the
  private `buildPrincipal` helper.
- Added a small companion `WsAttributes` class with the `AttributeKey<WsConnectionContext>`
  so later tasks (WS1.04 frame handler, WS1.13 delivery-tag tracker, …) have a
  single well-known handoff point.

---

## Limitations

- The `ws-handler` placeholder is a no-op `ChannelInboundHandlerAdapter` — real
  frame dispatch lands in TASK-WS1.04.
- Authentication for upgrade requests currently runs through `KafkaPrincipalBuilder`
  with only the client address + security protocol. Authorization header parsing
  (Bearer/Basic) is not yet wired — production integration will either reuse
  `HttpRequestHandler.extractAuthContext` or extract it into a shared helper.
- The `connected` JSON payload is hand-built; if the schema evolves (e.g. more
  capabilities, localized strings) a Jackson-based serializer will be needed.
- This task does NOT modify `HttpChannelInitializer.scala`. Wiring the handler
  into the live pipeline is a separate integration task.

---

## Field Notes

- Initial red flag: the parallel agent started on commit `f95a1f995d` (not
  `feature/http-protocol`). `git reset --hard origin/feature/http-protocol` got
  the worktree onto `f89cbe575b`. Always check first.
- The first test run failed SpotBugs because of the redundant null-check on the
  `InetSocketAddress` returned by a fallback-returning helper. Lesson: if a helper
  has a non-null fallback, do not add a null-check at the call site.
- First EmbeddedChannel upgrade test failed because I omitted `HttpServerCodec`
  from the test pipeline. Added it to the test setup; all upgrade assertions
  then passed (pipeline rewire + connected frame + max-frame-size propagation).
- `HttpServerCodec` in the test pipeline encodes the 404 error response into a
  raw `ByteBuf`; the disabled-ws test had to collect bytes and assert the status
  line rather than reading back a `FullHttpResponse`.

---

## Acceptance Criteria

- [ ] `WsUpgradeOrHttpHandler.java` exists at `http-server/src/main/java/kafka/server/http/ws/WsUpgradeOrHttpHandler.java`
- [ ] `isWebSocketUpgrade()` correctly detects upgrade requests
- [ ] `extractVhost()` parses vhost query parameter with "/" default
- [ ] Non-upgrade requests are forwarded to next handler
- [ ] On upgrade: `http-aggregator`, `compressor` removed; `ws-frame-aggregator` and `ws-handler` added
- [ ] `connected` JSON message sent after successful handshake
- [ ] Session ID follows `ws-{brokerId}-{uuid8}` format
- [ ] `timeout 300 ./gradlew :http-server:test --tests "kafka.server.http.ws.WsUpgradeOrHttpHandlerTest"` exits 0
- [ ] Learning section filled with at least one entry
- [ ] Limitations section filled (use "None" if truly none)

---

## File Manifest

> Filled by the executing agent after each commit.
> Run: `git diff --name-status HEAD~1 HEAD -- '*.java' '*.xml' '*.json' '*.yaml' '*.yml'`

```
A  http-server/src/main/java/kafka/server/http/ws/WsAttributes.java
A  http-server/src/main/java/kafka/server/http/ws/WsUpgradeOrHttpHandler.java
A  http-server/src/test/java/kafka/server/http/ws/WsUpgradeOrHttpHandlerTest.java
M  ivy-docs/tasks/TASK-WS1.03-ws-upgrade-or-http-handler.md
```
