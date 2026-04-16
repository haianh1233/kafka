# TASK-B.06: SocketServer Wiring + KafkaApis Dispatch

## Prerequisites

- **TASK-B.03** (HttpAcceptor) — Provides `HttpAcceptor` class that manages Netty ServerBootstrap lifecycle. This task wires `HttpAcceptor` into `SocketServer.createDataPlaneAcceptorAndProcessors()` so that HTTP listeners get an HttpAcceptor instead of a DataPlaneAcceptor.
- **TASK-B.04** (HttpProcessor) — Provides `HttpProcessor` for response routing. Created alongside `HttpAcceptor` during SocketServer initialization.
- **TASK-B.05** (HttpRequestHandler) — Provides `HttpRequestHandler` which is the Netty handler added to the pipeline. Wired into `HttpChannelInitializer` during HttpAcceptor setup.
- **TASK-A.01** (SecurityProtocol.isHttp) — Adds `isHttp()` helper method to `SecurityProtocol` enum. Used in both the SocketServer branching and KafkaApis dispatch.

---

## Context

All the HTTP pipeline components (HttpAcceptor, HttpProcessor, HttpRequestHandler, HttpRouter, HttpRequestTranslator, HttpResponseSerializer) have been built in previous tasks. This task wires them together by modifying two existing core Kafka files:

1. **`SocketServer.scala`** — Currently, `createDataPlaneAcceptorAndProcessors()` creates a `DataPlaneAcceptor` (NIO-based) for every listener endpoint. We add a branch: if the endpoint's security protocol is `HTTP` or `HTTPS`, create an `HttpAcceptor` (Netty-based) instead. The `stopProcessingRequests()` method also needs HTTP drain integration.

2. **`KafkaApis.scala`** — The `handle()` method dispatches requests by `ApiKeys`. For `PRODUCE` and `FETCH`, HTTP requests need different handling than binary protocol requests (HTTP handlers do fan-out/forwarding, binary handlers don't). We add `if (securityProtocol.isHttp)` checks to route to `handleHttpProduceRequest()` and `handleHttpConsumeRequest()`.

This is the integration task that makes the HTTP listener functional end-to-end. After this task, starting a Kafka broker with `listeners=HTTP://0.0.0.0:9094` will accept HTTP connections and route them through the full Kafka pipeline.

**The existing code being modified:**

```scala
// SocketServer.scala line 215-228 — current implementation (unmodified):
private def createDataPlaneAcceptorAndProcessors(endpoint: Endpoint): Unit = synchronized {
    if (stopped) {
      throw new RuntimeException("Can't create new data plane acceptor and processors: SocketServer is stopped.")
    }
    val listenerName = ListenerName.normalised(endpoint.listener)
    val parsedConfigs = config.valuesFromThisConfigWithPrefixOverride(listenerName.configPrefix)
    connectionQuotas.addListener(config, listenerName)
    val isPrivilegedListener = config.interBrokerListenerName == listenerName
    val dataPlaneAcceptor = createDataPlaneAcceptor(endpoint, isPrivilegedListener, dataPlaneRequestChannel)
    config.addReconfigurable(dataPlaneAcceptor)
    dataPlaneAcceptor.configure(parsedConfigs)
    dataPlaneAcceptors.put(endpoint, dataPlaneAcceptor)
    info(s"Created data-plane acceptor and processors for endpoint : ${listenerName}")
}
```

```scala
// KafkaApis.scala line 170-172 — current dispatch (unmodified):
request.header.apiKey match {
    case ApiKeys.PRODUCE => handleProduceRequest(request, requestLocal)
    case ApiKeys.FETCH => handleFetchRequest(request)
    // ... other cases unchanged
}
```

---

## Specification

### SocketServer modifications

```scala
// Add new field to SocketServer class:
private val httpAcceptors = new ConcurrentHashMap[Endpoint, HttpAcceptor]

// Modified createDataPlaneAcceptorAndProcessors:
private def createDataPlaneAcceptorAndProcessors(endpoint: Endpoint): Unit = synchronized {
    if (stopped) {
      throw new RuntimeException("Can't create new data plane acceptor: SocketServer is stopped.")
    }
    val listenerName = ListenerName.normalised(endpoint.listener)

    endpoint.securityProtocol match {
      case SecurityProtocol.HTTP | SecurityProtocol.HTTPS =>
        // HTTP/HTTPS: create HttpAcceptor (Netty-based)
        val httpProcessor = new HttpProcessor(nextProcessorId())
        val httpAcceptor = new HttpAcceptor(
          this, endpoint, config, dataPlaneRequestChannel, httpProcessor, time)
        httpAcceptors.put(endpoint, httpAcceptor)
        info(s"Created HTTP acceptor for endpoint: $listenerName")

      case _ =>
        // Binary protocol: existing DataPlaneAcceptor path (unchanged)
        val parsedConfigs = config.valuesFromThisConfigWithPrefixOverride(listenerName.configPrefix)
        connectionQuotas.addListener(config, listenerName)
        val isPrivilegedListener = config.interBrokerListenerName == listenerName
        val dataPlaneAcceptor = createDataPlaneAcceptor(endpoint, isPrivilegedListener, dataPlaneRequestChannel)
        config.addReconfigurable(dataPlaneAcceptor)
        dataPlaneAcceptor.configure(parsedConfigs)
        dataPlaneAcceptors.put(endpoint, dataPlaneAcceptor)
        info(s"Created data-plane acceptor and processors for endpoint: $listenerName")
    }
}
```

```scala
// Modified stopProcessingRequests — add HTTP drain:
def stopProcessingRequests(): Unit = synchronized {
    if (!stopped) {
      stopped = true
      info("Stopping socket server request processors")

      // 1. Existing: shut down binary acceptors
      dataPlaneAcceptors.asScala.values.foreach(_.beginShutdown())
      dataPlaneAcceptors.asScala.values.foreach(_.close())

      // 2. NEW: drain HTTP in-flight requests (bounded window)
      httpAcceptors.asScala.values.foreach(_.beginDrain())
      val drainDeadline = time.milliseconds() + config.getLong("http.shutdown.drain.ms")
      httpAcceptors.asScala.values.foreach { acc =>
        val remaining = drainDeadline - time.milliseconds()
        if (remaining > 0) acc.awaitDrain(remaining)
      }
      httpAcceptors.asScala.values.foreach(_.close())

      // 3. Existing: clear request queue
      dataPlaneRequestChannel.clear()
      info("Stopped socket server request processors")
    }
}
```

### KafkaApis modifications

```scala
// Modified handle() dispatch — add HTTP branching for PRODUCE and FETCH:
request.header.apiKey match {
    case ApiKeys.PRODUCE =>
      if (request.context.securityProtocol.isHttp)
        handleHttpProduceRequest(request)
      else
        handleProduceRequest(request, requestLocal)

    case ApiKeys.FETCH =>
      if (request.context.securityProtocol.isHttp)
        handleHttpConsumeRequest(request)
      else
        handleFetchRequest(request)

    // All other cases remain EXACTLY as-is — no changes
    case ApiKeys.LIST_OFFSETS => handleListOffsetRequest(request)
    case ApiKeys.METADATA => handleTopicMetadataRequest(request)
    // ... etc.
}
```

```scala
// New method stubs in KafkaApis (Phase 2 implementation):
private def handleHttpProduceRequest(request: RequestChannel.Request): Unit = {
    // Phase 2: fan-out + forwarding via ProduceForwardManager
    // For now, delegate to handleProduceRequest for single-partition requests
    // where this broker is the leader (local path only)
    handleProduceRequest(request, requestLocal)
}

private def handleHttpConsumeRequest(request: RequestChannel.Request): Unit = {
    // Phase 2: fan-out + forwarding via FetchForwardManager
    // For now, delegate to handleFetchRequest for single-partition requests
    handleFetchRequest(request)
}
```

### Behavioral contracts

- When `endpoint.securityProtocol` is `HTTP` or `HTTPS`, `createDataPlaneAcceptorAndProcessors()` creates an `HttpAcceptor` instead of a `DataPlaneAcceptor`. The `HttpAcceptor` is stored in the new `httpAcceptors` map.
- The `httpAcceptors` map is separate from `dataPlaneAcceptors` to avoid type mixing.
- `stopProcessingRequests()` drains HTTP acceptors with a bounded window (`http.shutdown.drain.ms`, default 2000ms) before closing them. Binary acceptors are shut down first (existing behavior), then HTTP acceptors drain.
- `handle()` in `KafkaApis` checks `request.context.securityProtocol.isHttp` for PRODUCE and FETCH. For Phase 1, the HTTP-specific handler methods (`handleHttpProduceRequest`, `handleHttpConsumeRequest`) are stubs that delegate to the existing handlers. Phase 2 will add fan-out/forwarding logic.
- All other `ApiKeys` (METADATA, LIST_OFFSETS, OFFSET_FETCH, etc.) are handled by existing handlers unchanged. The HTTP response serialization happens in `HttpProcessor` on the response path, not in `KafkaApis`.
- The `inter.broker.listener.name` MUST NOT be an HTTP listener. If it is, startup should fail with a clear error message.

---

## Implementation Details

**Module:** `core` (modifying existing files)

**Files to study:**

| File | Why |
|------|-----|
| `core/src/main/scala/kafka/network/SocketServer.scala` lines 215-248 | The methods being modified (createDataPlane..., stopProcessingRequests) |
| `core/src/main/scala/kafka/network/SocketServer.scala` lines 135-213 | SocketServer class fields and enableRequestProcessing — understand lifecycle |
| `core/src/main/scala/kafka/server/KafkaApis.scala` lines 160-230 | `handle()` method — the dispatch table being modified |
| `core/src/main/scala/kafka/network/SocketServer.scala` lines 362-420 | DataPlaneAcceptor class — the existing binary acceptor that HttpAcceptor replaces |

```scala
// From SocketServer.scala lines 135-145 — class fields:
class SocketServer(val config: KafkaConfig,
                   val metrics: Metrics,
                   val time: Time,
                   val credentialProvider: CredentialProvider,
                   val apiVersionManager: ApiVersionManager)
  extends Logging with KafkaMetricsGroup with BrokerReconfigurable {

  private val maxQueuedRequests = config.queuedMaxRequests
  private val nodeId = config.brokerId
  // ... existing fields
  val dataPlaneAcceptors = new ConcurrentHashMap[Endpoint, DataPlaneAcceptor]
```

```scala
// From KafkaApis.scala lines 170-220 — the dispatch table:
request.header.apiKey match {
    case ApiKeys.PRODUCE => handleProduceRequest(request, requestLocal)
    case ApiKeys.FETCH => handleFetchRequest(request)
    case ApiKeys.LIST_OFFSETS => handleListOffsetRequest(request)
    case ApiKeys.METADATA => handleTopicMetadataRequest(request)
    case ApiKeys.OFFSET_COMMIT => handleOffsetCommitRequest(request, requestLocal).exceptionally(handleError)
    case ApiKeys.OFFSET_FETCH => handleOffsetFetchRequest(request).exceptionally(handleError)
    // ... ~50 more cases, all unchanged
}
```

**Files to modify:**

| File | What changes |
|------|--------------|
| `core/src/main/scala/kafka/network/SocketServer.scala` | Add `httpAcceptors` field, branch in `createDataPlaneAcceptorAndProcessors()`, add HTTP drain to `stopProcessingRequests()` |
| `core/src/main/scala/kafka/server/KafkaApis.scala` | Add `isHttp` check in PRODUCE/FETCH dispatch, add stub methods `handleHttpProduceRequest`/`handleHttpConsumeRequest` |

> **CRITICAL:** Do NOT modify ANY of the existing `DataPlaneAcceptor` code paths. The binary protocol path must remain 100% unchanged. The HTTP branch is an `else` case, not a replacement.

> **CRITICAL:** The `httpAcceptors` map MUST be a separate `ConcurrentHashMap[Endpoint, HttpAcceptor]`, not mixed into the existing `dataPlaneAcceptors: ConcurrentHashMap[Endpoint, DataPlaneAcceptor]`. The types are different and mixing them would require unsafe casts.

> **CRITICAL:** Add a startup validation check: if `config.interBrokerListenerName` resolves to an HTTP or HTTPS endpoint, throw `IllegalArgumentException("inter.broker.listener.name must not be an HTTP/HTTPS listener")`. The HTTP listener must never be used for inter-broker binary protocol communication.

> **CRITICAL:** The `enableRequestProcessing()` method (around line 190) calls `dataPlaneAcceptors.values()` to chain futures and start acceptors. This needs to also handle `httpAcceptors` — call `httpAcceptor.startup()` during the enable phase.

> **CRITICAL:** The Phase 1 `handleHttpProduceRequest` and `handleHttpConsumeRequest` stubs delegate to the existing handlers. This means Phase 1 only works correctly for single-partition requests where the receiving broker is the leader. Phase 2 adds proper fan-out. Document this limitation clearly.

**Implementation order:**
1. Add `httpAcceptors` field and import to `SocketServer.scala`
2. Modify `createDataPlaneAcceptorAndProcessors()` to branch on security protocol
3. Add HTTP acceptor startup to `enableRequestProcessing()`
4. Add HTTP drain to `stopProcessingRequests()`
5. Add inter-broker listener validation
6. Add `isHttp` check in `KafkaApis.handle()` for PRODUCE and FETCH
7. Add stub methods `handleHttpProduceRequest` and `handleHttpConsumeRequest`

---

## Skeleton Code

### SocketServer.scala modifications

```scala
// ADD to SocketServer class fields (after dataPlaneAcceptors declaration):

import kafka.network.HttpAcceptor
import kafka.server.http.HttpProcessor
import org.apache.kafka.common.security.auth.SecurityProtocol

// New field: HTTP acceptors (separate from binary DataPlaneAcceptors)
val httpAcceptors = new ConcurrentHashMap[Endpoint, HttpAcceptor]
```

```scala
// REPLACE createDataPlaneAcceptorAndProcessors (lines 215-228):

private def createDataPlaneAcceptorAndProcessors(endpoint: Endpoint): Unit = synchronized {
    if (stopped) {
      throw new RuntimeException("Can't create new data plane acceptor and processors: SocketServer is stopped.")
    }
    val listenerName = ListenerName.normalised(endpoint.listener)

    // TODO: Validate inter-broker listener is not HTTP
    // if (config.interBrokerListenerName == listenerName) {
    //   require(!endpoint.securityProtocol().isHttp,
    //     s"inter.broker.listener.name ($listenerName) must not be an HTTP/HTTPS listener")
    // }

    endpoint.securityProtocol() match {
      case SecurityProtocol.HTTP | SecurityProtocol.HTTPS =>
        // TODO: HTTP/HTTPS endpoint — create HttpAcceptor (Netty-based)
        // val httpProcessorId = nextProcessorId()  // or a dedicated counter
        // val httpProcessor = new HttpProcessor(httpProcessorId)
        // val httpAcceptor = new HttpAcceptor(
        //   this, endpoint, config, dataPlaneRequestChannel, httpProcessor, time)
        // httpAcceptors.put(endpoint, httpAcceptor)
        // info(s"Created HTTP acceptor for endpoint: $listenerName")
        throw new UnsupportedOperationException("Not yet implemented")

      case _ =>
        // Binary protocol: existing path (UNCHANGED)
        val parsedConfigs = config.valuesFromThisConfigWithPrefixOverride(listenerName.configPrefix)
        connectionQuotas.addListener(config, listenerName)
        val isPrivilegedListener = config.interBrokerListenerName == listenerName
        val dataPlaneAcceptor = createDataPlaneAcceptor(endpoint, isPrivilegedListener, dataPlaneRequestChannel)
        config.addReconfigurable(dataPlaneAcceptor)
        dataPlaneAcceptor.configure(parsedConfigs)
        dataPlaneAcceptors.put(endpoint, dataPlaneAcceptor)
        info(s"Created data-plane acceptor and processors for endpoint : ${listenerName}")
    }
}
```

```scala
// MODIFY enableRequestProcessing (around line 190-212):
// After the existing dataPlaneAcceptors futures, add HTTP acceptor startup:

// TODO: Add after existing dataPlaneAcceptors enable logic:
// httpAcceptors.asScala.values.foreach { acc =>
//   try {
//     acc.startup()
//   } catch {
//     case e: Exception =>
//       error(s"Failed to start HTTP acceptor", e)
//       acc.startedFuture.completeExceptionally(e)
//   }
// }
```

```scala
// MODIFY stopProcessingRequests (lines 239-248):

def stopProcessingRequests(): Unit = synchronized {
    if (!stopped) {
      stopped = true
      info("Stopping socket server request processors")

      // 1. Existing: shut down binary acceptors (UNCHANGED)
      dataPlaneAcceptors.asScala.values.foreach(_.beginShutdown())
      dataPlaneAcceptors.asScala.values.foreach(_.close())

      // TODO: 2. NEW: drain HTTP in-flight requests (bounded window)
      // httpAcceptors.asScala.values.foreach(_.beginDrain())
      // val httpDrainMs = 2000L  // TODO: config.getLong(HttpServerConfigs.HTTP_SHUTDOWN_DRAIN_MS_CONFIG)
      // val drainDeadline = time.milliseconds() + httpDrainMs
      // httpAcceptors.asScala.values.foreach { acc =>
      //   val remaining = drainDeadline - time.milliseconds()
      //   if (remaining > 0) acc.awaitDrain(remaining)
      // }
      // httpAcceptors.asScala.values.foreach(_.close())
      // info("Stopped HTTP acceptors")

      // 3. Existing: clear request queue (UNCHANGED)
      dataPlaneRequestChannel.clear()
      info("Stopped socket server request processors")
    }
}
```

### KafkaApis.scala modifications

```scala
// MODIFY handle() dispatch table (line 170+):
// Only PRODUCE and FETCH cases change. All other cases remain exactly as-is.

request.header.apiKey match {
    // MODIFIED: Add HTTP branching for PRODUCE
    case ApiKeys.PRODUCE =>
      if (request.context.securityProtocol.isHttp)
        handleHttpProduceRequest(request)
      else
        handleProduceRequest(request, requestLocal)

    // MODIFIED: Add HTTP branching for FETCH
    case ApiKeys.FETCH =>
      if (request.context.securityProtocol.isHttp)
        handleHttpConsumeRequest(request)
      else
        handleFetchRequest(request)

    // ALL OTHER CASES — EXACTLY UNCHANGED
    case ApiKeys.LIST_OFFSETS => handleListOffsetRequest(request)
    case ApiKeys.METADATA => handleTopicMetadataRequest(request)
    case ApiKeys.OFFSET_COMMIT => handleOffsetCommitRequest(request, requestLocal).exceptionally(handleError)
    case ApiKeys.OFFSET_FETCH => handleOffsetFetchRequest(request).exceptionally(handleError)
    // ... remainder unchanged ...
}
```

```scala
// ADD new methods to KafkaApis class:

/**
 * Handles HTTP produce requests. Phase 1: delegates to existing handleProduceRequest.
 * Phase 2 will add fan-out and forwarding via ProduceForwardManager.
 *
 * LIMITATION: Phase 1 only works correctly for single-partition requests
 * where the receiving broker is the partition leader. Multi-partition and
 * cross-broker requests will fail with LEADER_NOT_AVAILABLE for non-local
 * partitions. This is expected — Phase 2 adds proper forwarding.
 */
private def handleHttpProduceRequest(request: RequestChannel.Request): Unit = {
    // TODO: Phase 1 — delegate to existing handler
    // handleProduceRequest(request, requestLocal)
    //
    // Phase 2 will replace this with:
    //   - MetadataCache lookup to determine leaders per partition
    //   - Local append for partitions this broker leads
    //   - ProduceForwardManager.forward() for remote partitions
    //   - CompletableFuture.allOf().handleAsync() to merge results
    //   - requestHelper.sendMaybeThrottle() to send response
    throw new UnsupportedOperationException("Not yet implemented")
}

/**
 * Handles HTTP consume (fetch) requests. Phase 1: delegates to existing handleFetchRequest.
 * Phase 2 will add fan-out and forwarding via FetchForwardManager.
 *
 * LIMITATION: Phase 1 only works correctly for single-partition requests
 * where the receiving broker is the partition leader. Multi-partition and
 * cross-broker requests will fail with LEADER_NOT_AVAILABLE for non-local
 * partitions. This is expected — Phase 2 adds proper forwarding.
 */
private def handleHttpConsumeRequest(request: RequestChannel.Request): Unit = {
    // TODO: Phase 1 — delegate to existing handler
    // handleFetchRequest(request)
    //
    // Phase 2 will replace this with:
    //   - effectiveMaxWaitMs = min(request.maxWaitMs, config.httpConsumeMaxWaitMs)
    //   - MetadataCache lookup to determine leaders per partition
    //   - Local fetch for partitions this broker leads
    //   - FetchForwardManager.forward() for remote partitions
    //   - CompletableFuture.allOf().handleAsync() to merge results
    //   - Add X-Kafka-MaxWait-Applied response header
    throw new UnsupportedOperationException("Not yet implemented")
}
```

### Test class — `SocketServerHttpWiringTest.scala`

```scala
package kafka.network

import java.util.Properties

import kafka.server.{KafkaConfig => LegacyKafkaConfig}
import org.apache.kafka.common.Endpoint
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.MockTime
import org.apache.kafka.server.config.KafkaConfig
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.api.Assertions._

/**
 * Tests that SocketServer correctly creates HttpAcceptor for HTTP endpoints
 * and DataPlaneAcceptor for binary protocol endpoints.
 *
 * // Time: Created - TASK-B.06
 */
class SocketServerHttpWiringTest {

  // TODO: set up test SocketServer with both HTTP and PLAINTEXT listeners

  @Test
  def httpEndpoint_createsHttpAcceptor(): Unit = {
    // Arrange: config with listeners=PLAINTEXT://0.0.0.0:9092,HTTP://0.0.0.0:9094
    // Act: createDataPlaneAcceptorAndProcessors is called for HTTP endpoint
    // Assert: httpAcceptors contains the HTTP endpoint
    // Assert: dataPlaneAcceptors does NOT contain the HTTP endpoint
  }

  @Test
  def plaintextEndpoint_createsDataPlaneAcceptor(): Unit = {
    // Arrange: same config as above
    // Act: createDataPlaneAcceptorAndProcessors is called for PLAINTEXT endpoint
    // Assert: dataPlaneAcceptors contains the PLAINTEXT endpoint
    // Assert: httpAcceptors does NOT contain the PLAINTEXT endpoint
  }

  @Test
  def interBrokerListenerAsHttp_throwsOnStartup(): Unit = {
    // Arrange: config with inter.broker.listener.name=HTTP
    // Act / Assert: createDataPlaneAcceptorAndProcessors throws IllegalArgumentException
  }

  @Test
  def stopProcessingRequests_drainsHttpAcceptors(): Unit = {
    // Arrange: start HTTP acceptor
    // Act: stopProcessingRequests()
    // Assert: httpAcceptor.beginDrain() was called
    // Assert: httpAcceptor.close() was called
  }

  @Test
  def stopProcessingRequests_drainsBeforeBinaryShutdown(): Unit = {
    // Verify ordering: binary acceptors close first, then HTTP drain, then HTTP close
  }
}
```

### Test class — `KafkaApisHttpDispatchTest.scala`

```scala
package kafka.server

import kafka.network.RequestChannel
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.{ProduceRequest, FetchRequest, RequestContext}
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.junit.jupiter.api.{BeforeEach, Test}
import org.junit.jupiter.api.Assertions._
import org.mockito.Mockito._

/**
 * Tests that KafkaApis.handle() correctly dispatches PRODUCE and FETCH
 * to HTTP-specific handlers when securityProtocol is HTTP/HTTPS.
 *
 * // Time: Created - TASK-B.06
 */
class KafkaApisHttpDispatchTest {

  // TODO: set up KafkaApis with mocked dependencies

  @Test
  def produce_httpProtocol_callsHttpHandler(): Unit = {
    // Arrange: request with securityProtocol = HTTP, apiKey = PRODUCE
    // Act: kafkaApis.handle(request, requestLocal)
    // Assert: handleHttpProduceRequest was called (not handleProduceRequest)
  }

  @Test
  def produce_plaintextProtocol_callsExistingHandler(): Unit = {
    // Arrange: request with securityProtocol = PLAINTEXT, apiKey = PRODUCE
    // Act: kafkaApis.handle(request, requestLocal)
    // Assert: handleProduceRequest was called (not handleHttpProduceRequest)
  }

  @Test
  def fetch_httpProtocol_callsHttpHandler(): Unit = {
    // Arrange: request with securityProtocol = HTTP, apiKey = FETCH
    // Act: kafkaApis.handle(request, requestLocal)
    // Assert: handleHttpConsumeRequest was called (not handleFetchRequest)
  }

  @Test
  def fetch_plaintextProtocol_callsExistingHandler(): Unit = {
    // Arrange: request with securityProtocol = PLAINTEXT, apiKey = FETCH
    // Act: kafkaApis.handle(request, requestLocal)
    // Assert: handleFetchRequest was called (not handleHttpConsumeRequest)
  }

  @Test
  def metadata_httpProtocol_callsExistingHandler(): Unit = {
    // Arrange: request with securityProtocol = HTTP, apiKey = METADATA
    // Act: kafkaApis.handle(request, requestLocal)
    // Assert: handleTopicMetadataRequest was called (same as binary — no HTTP branch)
  }

  @Test
  def listOffsets_httpProtocol_callsExistingHandler(): Unit = {
    // Same pattern: HTTP LIST_OFFSETS goes to existing handler
  }
}
```

### Existing pattern reference

```scala
// From SocketServer.scala lines 215-228 — the method being modified:
private def createDataPlaneAcceptorAndProcessors(endpoint: Endpoint): Unit = synchronized {
    if (stopped) {
      throw new RuntimeException("Can't create new data plane acceptor and processors: SocketServer is stopped.")
    }
    val listenerName =  ListenerName.normalised(endpoint.listener)
    val parsedConfigs = config.valuesFromThisConfigWithPrefixOverride(listenerName.configPrefix)
    connectionQuotas.addListener(config, listenerName)
    val isPrivilegedListener = config.interBrokerListenerName == listenerName
    val dataPlaneAcceptor = createDataPlaneAcceptor(endpoint, isPrivilegedListener, dataPlaneRequestChannel)
    config.addReconfigurable(dataPlaneAcceptor)
    dataPlaneAcceptor.configure(parsedConfigs)
    dataPlaneAcceptors.put(endpoint, dataPlaneAcceptor)
    info(s"Created data-plane acceptor and processors for endpoint : ${listenerName}")
}
```

```scala
// From KafkaApis.scala lines 170-172 — the dispatch being modified:
request.header.apiKey match {
    case ApiKeys.PRODUCE => handleProduceRequest(request, requestLocal)
    case ApiKeys.FETCH => handleFetchRequest(request)
    case ApiKeys.LIST_OFFSETS => handleListOffsetRequest(request)
    case ApiKeys.METADATA => handleTopicMetadataRequest(request)
    // ... 50+ more cases
}
```

```scala
// From SocketServer.scala lines 239-248 — shutdown being modified:
def stopProcessingRequests(): Unit = synchronized {
    if (!stopped) {
      stopped = true
      info("Stopping socket server request processors")
      dataPlaneAcceptors.asScala.values.foreach(_.beginShutdown())
      dataPlaneAcceptors.asScala.values.foreach(_.close())
      dataPlaneRequestChannel.clear()
      info("Stopped socket server request processors")
    }
}
```

---

## Tests

**Test class:** `http-server/src/test/scala/kafka/network/SocketServerHttpWiringTest.scala`

| Test method | What it verifies |
|-------------|-----------------|
| `httpEndpoint_createsHttpAcceptor` | HTTP endpoints get HttpAcceptor, not DataPlaneAcceptor |
| `plaintextEndpoint_createsDataPlaneAcceptor` | PLAINTEXT endpoints still get DataPlaneAcceptor (unchanged) |
| `interBrokerListenerAsHttp_throwsOnStartup` | inter.broker.listener.name=HTTP is rejected |
| `stopProcessingRequests_drainsHttpAcceptors` | HTTP drain called during shutdown |
| `stopProcessingRequests_drainsBeforeBinaryShutdown` | Shutdown ordering is correct |

**Test class:** `http-server/src/test/scala/kafka/server/KafkaApisHttpDispatchTest.scala`

| Test method | What it verifies |
|-------------|-----------------|
| `produce_httpProtocol_callsHttpHandler` | HTTP PRODUCE dispatches to handleHttpProduceRequest |
| `produce_plaintextProtocol_callsExistingHandler` | Binary PRODUCE unchanged |
| `fetch_httpProtocol_callsHttpHandler` | HTTP FETCH dispatches to handleHttpConsumeRequest |
| `fetch_plaintextProtocol_callsExistingHandler` | Binary FETCH unchanged |
| `metadata_httpProtocol_callsExistingHandler` | HTTP METADATA uses existing handler |
| `listOffsets_httpProtocol_callsExistingHandler` | HTTP LIST_OFFSETS uses existing handler |

**Run command:**
```bash
./gradlew :core:test --tests "kafka.network.SocketServerHttpWiringTest" --tests "kafka.server.KafkaApisHttpDispatchTest"
```

---

## Rules

- Do NOT modify any existing `DataPlaneAcceptor` code paths. The binary protocol path must remain 100% unchanged. The HTTP branch is purely additive.
- Only PRODUCE and FETCH get HTTP-specific dispatch in `KafkaApis.handle()`. All other ApiKeys (METADATA, LIST_OFFSETS, OFFSET_FETCH, etc.) use existing handlers unchanged.
- The `inter.broker.listener.name` MUST NOT be set to an HTTP/HTTPS listener. Validate at startup.
- The `httpAcceptors` map is separate from `dataPlaneAcceptors` — do not mix them.
- HTTP drain in `stopProcessingRequests()` is bounded by `http.shutdown.drain.ms` (default 2000ms).
- Phase 1 HTTP produce/fetch handlers are stubs that delegate to existing handlers. Document this limitation.

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

- [ ] `./gradlew :core:test --tests "kafka.network.SocketServerHttpWiringTest"` exits 0
- [ ] `./gradlew :core:test --tests "kafka.server.KafkaApisHttpDispatchTest"` exits 0
- [ ] `SocketServer.scala` has `httpAcceptors` field of type `ConcurrentHashMap[Endpoint, HttpAcceptor]`
- [ ] `createDataPlaneAcceptorAndProcessors()` branches on `SecurityProtocol.HTTP`/`HTTPS`
- [ ] Existing `DataPlaneAcceptor` path is unchanged (binary protocol not affected)
- [ ] `stopProcessingRequests()` includes HTTP drain with bounded timeout
- [ ] `KafkaApis.handle()` dispatches PRODUCE to `handleHttpProduceRequest` when `securityProtocol.isHttp`
- [ ] `KafkaApis.handle()` dispatches FETCH to `handleHttpConsumeRequest` when `securityProtocol.isHttp`
- [ ] METADATA, LIST_OFFSETS, and all other ApiKeys are NOT branched — existing handlers used
- [ ] `inter.broker.listener.name` set to HTTP/HTTPS is rejected at startup
- [ ] `handleHttpProduceRequest` and `handleHttpConsumeRequest` stubs exist and delegate to existing handlers
- [ ] Learning section filled with at least one entry
- [ ] Limitations section filled (use "None" if truly none)
- [ ] File Manifest section updated after commit

---

## File Manifest

<!-- ### YYYY-MM-DD — <short description> (commit <hash>)
Created:
  - (none — this task only modifies existing files)
Modified:
  - core/src/main/scala/kafka/network/SocketServer.scala — Add httpAcceptors, branch on HTTP, add drain
  - core/src/main/scala/kafka/server/KafkaApis.scala — Add HTTP dispatch for PRODUCE/FETCH
  - http-server/src/test/scala/kafka/network/SocketServerHttpWiringTest.scala — Wiring tests
  - http-server/src/test/scala/kafka/server/KafkaApisHttpDispatchTest.scala — Dispatch tests
-->
