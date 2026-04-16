# TASK-0.02: Deduplicate Classes Across core and http-server Modules

## Prerequisites

| Task | What it provides |
|------|-----------------|
| TASK-0.01 | Checkstyle fixes must land first so checkstyleMain passes cleanly |

---

## Context

During the HTTP protocol implementation, several classes were created as stubs in the `core` module (for SocketServer wiring) and then fully implemented in the `http-server` module. Both versions ended up on the classpath, causing `NoSuchMethodError` at runtime because the JVM loads the wrong class. There are also two competing `HttpRequestHandler` implementations within the `http-server` module itself.

### Duplicate 1: `kafka.network.HttpAcceptor`

| Location | Origin | Type |
|----------|--------|------|
| `core/src/main/scala/kafka/network/HttpAcceptor.scala` | TASK-B.06 stub | Lightweight stub with `(SocketServer, Endpoint, KafkaConfig, RequestChannel, HttpProcessor, Time)` constructor |
| `http-server/src/main/scala/kafka/network/HttpAcceptor.scala` | TASK-B.03 real impl | Full Netty ServerBootstrap with `(Endpoint, Int, Int, Long, Time, String)` constructor |

**Problem:** Both compile to `kafka.network.HttpAcceptor.class`. At runtime, the classpath order determines which loads. `SocketServer.scala` (line 256) calls the core stub constructor, which does not match the http-server constructor signature.

### Duplicate 2: `kafka.network.HttpProcessor`

| Location | Origin | Type |
|----------|--------|------|
| `core/src/main/scala/kafka/network/HttpProcessor.scala` | TASK-B.04 stub | Minimal stub with `(id: Int)` constructor, just logs |
| `http-server/src/main/java/kafka/server/http/HttpProcessor.java` | TASK-B.04 real impl | Full response processor with Netty channel management |

**Problem:** Although the packages differ (`kafka.network` vs `kafka.server.http`), `SocketServer.scala` line 255 creates `new HttpProcessor(httpProcessorId)` which resolves to the core stub (same package). The real processor in `kafka.server.http.HttpProcessor` is never used by `SocketServer`.

### Duplicate 3: `HttpRequestHandler`

| Location | Origin | Type |
|----------|--------|------|
| `http-server/src/main/java/kafka/server/http/HttpRequestHandler.java` | TASK-F.07 stub | Simple stub with `(AtomicBoolean, AtomicInteger, HttpMetrics, int)` constructor |
| `http-server/src/main/scala/kafka/network/HttpRequestHandler.scala` | TASK-B.05/F.06 real impl | Full handler with auth context extraction, drain check, principal builder |

**Problem:** Both compile to classes named `HttpRequestHandler` (different packages). The `HttpChannelInitializer` must reference exactly one. The Java stub has a simpler constructor but no auth logic. The Scala version is the real implementation.

---

## Specification

### Resolution Strategy

1. **Delete** `core/src/main/scala/kafka/network/HttpAcceptor.scala` (the stub).
2. **Delete** `core/src/main/scala/kafka/network/HttpProcessor.scala` (the stub).
3. **Delete** `http-server/src/main/java/kafka/server/http/HttpRequestHandler.java` (the F.07 stub).
4. **Update** `SocketServer.scala` to use the real `http-server` HttpAcceptor and HttpProcessor via proper imports and constructor calls.

### SocketServer.scala Changes

The `SocketServer.scala` file is in `package kafka.network`. Currently it creates:

```scala
// Line 255
val httpProcessor = new HttpProcessor(httpProcessorId)
// Line 256-257
val httpAcceptor = new HttpAcceptor(
  this, endpoint, config, dataPlaneRequestChannel, httpProcessor, time)
```

After deleting the core stubs, `HttpProcessor` will no longer exist in `kafka.network` -- it will only exist as `kafka.server.http.HttpProcessor`. And `HttpAcceptor` will only exist as the real http-server version in `kafka.network` (same package, different module).

**Changes needed:**

1. The http-server `HttpAcceptor` constructor is `(Endpoint, Int, Int, Long, Time, String)`. The SocketServer must be updated to pass the correct arguments:
   - `endpoint` -- the `Endpoint` instance
   - `config.numHttpNetworkThreads` -- number of worker threads
   - `config.httpRequestMaxBytes` -- max request size
   - `config.httpConnectionIdleTimeoutMs` -- idle timeout
   - `time` -- Time instance
   - `config.httpCorsAllowedOrigins` -- CORS origins (or empty string)

2. The `HttpProcessor` creation must use an explicit import of `kafka.server.http.HttpProcessor` and the 1-arg or 2-arg constructor.

3. The `httpAcceptors` map type annotation `ConcurrentHashMap[Endpoint, HttpAcceptor]` will now refer to the http-server `HttpAcceptor` (which implements `Closeable`).

---

## Implementation Details

### Step 1: Delete stub files

```bash
rm core/src/main/scala/kafka/network/HttpAcceptor.scala
rm core/src/main/scala/kafka/network/HttpProcessor.scala
rm http-server/src/main/java/kafka/server/http/HttpRequestHandler.java
```

### Step 2: Update SocketServer.scala

**File:** `core/src/main/scala/kafka/network/SocketServer.scala`

Add import near the top of the file (after existing imports):

BEFORE (no import for http-server HttpProcessor):
```scala
import kafka.server.{BrokerReconfigurable, KafkaConfig}
```

AFTER:
```scala
import kafka.server.{BrokerReconfigurable, KafkaConfig}
import kafka.server.http.{HttpProcessor => HttpResponseProcessor}
```

Note: We alias it to `HttpResponseProcessor` to avoid ambiguity with any future `HttpProcessor` in `kafka.network`.

**Update the `createDataPlaneAcceptorAndProcessors` method (around lines 254-258):**

BEFORE:
```scala
      case SecurityProtocol.HTTP | SecurityProtocol.HTTPS =>
        // HTTP/HTTPS endpoint -- create HttpAcceptor (Netty-based)
        val httpProcessorId = nextProcessorId()
        val httpProcessor = new HttpProcessor(httpProcessorId)
        val httpAcceptor = new HttpAcceptor(
          this, endpoint, config, dataPlaneRequestChannel, httpProcessor, time)
        httpAcceptors.put(endpoint, httpAcceptor)
        info(s"Created HTTP acceptor for endpoint: $listenerName")
```

AFTER:
```scala
      case SecurityProtocol.HTTP | SecurityProtocol.HTTPS =>
        // HTTP/HTTPS endpoint -- create HttpAcceptor (Netty-based)
        val httpAcceptor = new HttpAcceptor(
          endpoint,
          config.numHttpNetworkThreads,
          config.httpRequestMaxBytes,
          config.httpConnectionIdleTimeoutMs,
          time,
          config.httpCorsAllowedOrigins
        )
        httpAcceptors.put(endpoint, httpAcceptor)
        info(s"Created HTTP acceptor for endpoint: $listenerName")
```

Note: If `config.numHttpNetworkThreads`, `config.httpRequestMaxBytes`, etc. are not yet defined on `KafkaConfig`, use the defaults from `HttpServerConfigs`:
```scala
        val httpAcceptor = new HttpAcceptor(
          endpoint,
          org.apache.kafka.network.HttpServerConfigs.NUM_HTTP_NETWORK_THREADS_DEFAULT,
          org.apache.kafka.network.HttpServerConfigs.HTTP_REQUEST_MAX_BYTES_DEFAULT,
          org.apache.kafka.network.HttpServerConfigs.HTTP_CONNECTION_IDLE_TIMEOUT_MS_DEFAULT,
          time
        )
```

**Update the `addListeners` method (around lines 351-364):**

The `httpAcceptor.startup()` call should still work since the http-server `HttpAcceptor` also has a `startup()` method.

**Update the shutdown sequence (search for `httpAcceptors.asScala`):**

The http-server `HttpAcceptor` uses `beginDrain()`, `awaitDrain()`, and `close()` methods. Verify these are called correctly. The method signatures match.

### Step 3: Verify HttpChannelInitializer uses the correct HttpRequestHandler

**File:** `http-server/src/main/scala/kafka/network/HttpChannelInitializer.scala`

Check which `HttpRequestHandler` is referenced. After deleting the Java stub, only the Scala version at `kafka.network.HttpRequestHandler` remains. If the initializer imports `kafka.server.http.HttpRequestHandler`, update the import to `kafka.network.HttpRequestHandler`.

---

## Skeleton Code

### SocketServer.scala -- Updated HTTP acceptor creation

```scala
// core/src/main/scala/kafka/network/SocketServer.scala

case SecurityProtocol.HTTP | SecurityProtocol.HTTPS =>
  // HTTP/HTTPS endpoint -- create HttpAcceptor (Netty-based)
  val httpAcceptor = new HttpAcceptor(
    endpoint,
    org.apache.kafka.network.HttpServerConfigs.NUM_HTTP_NETWORK_THREADS_DEFAULT,
    org.apache.kafka.network.HttpServerConfigs.HTTP_REQUEST_MAX_BYTES_DEFAULT,
    org.apache.kafka.network.HttpServerConfigs.HTTP_CONNECTION_IDLE_TIMEOUT_MS_DEFAULT,
    time
  )
  httpAcceptors.put(endpoint, httpAcceptor)
  info(s"Created HTTP acceptor for endpoint: $listenerName")
```

---

## Tests

### Compilation Check

```bash
./gradlew :core:compileScala
```

This must pass. It verifies that `SocketServer.scala` compiles correctly with the http-server `HttpAcceptor` on the compile classpath and without the deleted stubs.

```bash
./gradlew :http-server:compileScala :http-server:compileJava
```

This verifies no references to the deleted `HttpRequestHandler.java` stub remain.

### Existing Tests

All existing tests that used the stubs must be checked:
- Tests in `http-server/src/test/` that reference `HttpAcceptor` should use the http-server version (which they already do, since they're in the http-server module).
- Tests in `core/src/test/` that reference `HttpAcceptor` or `HttpProcessor` from `kafka.network` -- these will fail if they depended on the stub constructors and must be updated.

### Targeted Test Run

```bash
./gradlew :http-server:test --tests 'kafka.server.http.HttpProcessorTest'
./gradlew :http-server:test --tests 'kafka.network.HttpAcceptorTest*'
./gradlew :http-server:test --tests 'kafka.network.HttpRequestHandlerTest*'
```

---

## Rules

- Delete the stubs cleanly -- do not leave behind empty files or commented-out code.
- The `SocketServer.scala` constructor call must match the http-server `HttpAcceptor` constructor signature exactly.
- If `KafkaConfig` does not yet have the HTTP config properties, use `HttpServerConfigs.*_DEFAULT` constants directly.
- Do NOT modify the http-server `HttpAcceptor` class -- it is the canonical implementation.
- Do NOT modify the http-server `HttpRequestHandler.scala` -- it is the canonical implementation.
- Verify `core` depends on `http-server` at compile time (check `build.gradle` for the dependency declaration).

---

## Learning

(empty)

## Limitations

(empty)

## Field Notes

(empty)

---

## Acceptance Criteria

- [ ] `core/src/main/scala/kafka/network/HttpAcceptor.scala` is deleted
- [ ] `core/src/main/scala/kafka/network/HttpProcessor.scala` is deleted
- [ ] `http-server/src/main/java/kafka/server/http/HttpRequestHandler.java` is deleted
- [ ] `SocketServer.scala` updated to construct the http-server `HttpAcceptor` with correct arguments
- [ ] `./gradlew :core:compileScala` passes
- [ ] `./gradlew :http-server:compileScala :http-server:compileJava` passes
- [ ] No `NoSuchMethodError` at runtime -- constructor signatures match
- [ ] Only one `HttpAcceptor` class, one `HttpProcessor` class, and one `HttpRequestHandler` class remain on the classpath

---

## File Manifest

(empty -- to be filled after implementation)
