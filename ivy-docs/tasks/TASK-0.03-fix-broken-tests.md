# TASK-0.03: Fix or Remove .broken Test Files

## Prerequisites

| Task | What it provides |
|------|-----------------|
| TASK-0.01 | Checkstyle passes -- tests can be compiled |
| TASK-0.02 | Deduplicated classes -- correct `HttpAcceptor`, `HttpProcessor`, `HttpRequestHandler` on classpath |

Both must be merged before this task begins, since the broken tests reference classes that may have been duplicated or stubbed.

---

## Context

Five test files in the `http-server` module were renamed to `.broken` to unblock CI. Each file has compilation or runtime errors caused by API mismatches, missing methods, or references to classes/methods that do not exist in the current codebase. This task analyzes each file, determines whether it can be fixed or should be deleted, and takes the appropriate action.

### File Inventory

| # | File | Package | Origin | Lines |
|---|------|---------|--------|-------|
| 1 | `http-server/src/test/scala/kafka/network/HttpAcceptorDrainTest.scala.broken` | `kafka.network` | TASK-F.06 | 232 |
| 2 | `http-server/src/test/scala/kafka/network/HttpGracefulShutdownTest.scala.broken` | `kafka.network` | TASK-F.06 | 211 |
| 3 | `http-server/src/test/scala/kafka/server/http/HttpRequestTranslatorShareGroupTest.scala.broken` | `kafka.server.http` | TASK-G.01/G.02 | 160 |
| 4 | `http-server/src/test/scala/kafka/server/http/HttpRouterShareGroupTest.scala.broken` | `kafka.server.http` | TASK-G.01/G.02 | 92 |
| 5 | `http-server/src/test/scala/kafka/server/http/ShareGroupApiContractTest.scala.broken` | `kafka.server.http` | TASK-G.02 | 214 |

---

## Specification

### Analysis of Each File

#### 1. `HttpAcceptorDrainTest.scala.broken` -- FIXABLE

**What it tests:** Drain lifecycle of `HttpAcceptor` -- `beginDrain()`, `awaitDrain()`, `isDraining`, `isAccepting`, `incrementPending()`, `decrementPending()`, `pendingRequestCount`, `close()`, `startup()`, `startedFuture`, `draining` (AtomicBoolean field), `inFlightCount` (AtomicInteger field).

**Why it broke:** After TASK-0.02 deduplication, only the http-server `HttpAcceptor` remains. This test constructs the acceptor as:
```scala
acceptor = new HttpAcceptor(
  endpoint,
  HttpConfigs.NUM_HTTP_NETWORK_THREADS_DEFAULT,
  HttpConfigs.HTTP_REQUEST_MAX_BYTES_DEFAULT,
  HttpConfigs.HTTP_CONNECTION_IDLE_TIMEOUT_MS_DEFAULT,
  time
)
```
This matches the real http-server `HttpAcceptor` constructor (without the optional `corsAllowedOrigins` parameter -- Scala defaults apply).

**Problem:** The test calls `acceptor.startup()` which tries to bind a real Netty server socket. In CI, port 0 binding may succeed, but some tests (`testBeginDrainSetsDrainingFlag`, `testBeginDrainIsIdempotent`, etc.) call `startup()` followed by `beginDrain()` which closes the server channel. The MockTime object passed as `time` should work since the real `HttpAcceptor` only uses `time.milliseconds()` in `awaitDrain()`.

**Fix:** The test should compile and work correctly against the http-server `HttpAcceptor` after deduplication. The constructor signature and all method names (`isDraining`, `isAccepting`, `incrementPending`, `decrementPending`, `pendingRequestCount`, `beginDrain`, `awaitDrain`, `close`, `startup`, `startedFuture`, `draining`, `inFlightCount`) all exist on the http-server version.

**Action:** Rename back to `.scala` (remove `.broken` suffix). No code changes needed.

#### 2. `HttpGracefulShutdownTest.scala.broken` -- FIXABLE

**What it tests:** Drain check in `HttpRequestHandler` -- sends requests to an `EmbeddedChannel` with the Scala `HttpRequestHandler` and verifies 503 responses during drain.

**Why it broke:** References `HttpRequestHandler` from `kafka.network` package, which is the Scala version. After deduplication (TASK-0.02), only this Scala version remains, so the import is unambiguous.

**Problem:** The test constructs `HttpRequestHandler(principalBuilder, SecurityProtocol.HTTP, draining, inFlightCount)`. This matches the Scala `HttpRequestHandler` constructor at `http-server/src/main/scala/kafka/network/HttpRequestHandler.scala` line 48.

**Fix:** Rename back to `.scala`. The test compiles correctly against the Scala `HttpRequestHandler`.

**Action:** Rename back to `.scala` (remove `.broken` suffix). No code changes needed.

#### 3. `HttpRequestTranslatorShareGroupTest.scala.broken` -- DELETE

**What it tests:** Share group poll/acknowledge translation methods on `HttpRequestTranslator`.

**Why it broke:** The test calls methods that do not exist in the current `HttpRequestTranslator.java`:
- `HttpRequestTranslator.translateShareGroupPoll(group, json)` -- does not exist
- `HttpRequestTranslator.translateShareGroupAcknowledge(group, json)` -- does not exist
- `HttpRequestTranslator.mapAcknowledgeType(type)` -- does not exist

These methods were planned for TASK-G.01 (Share Group Endpoints) but were never implemented in the Java `HttpRequestTranslator`. The test was written speculatively.

**Action:** Delete the file. The share group translation methods do not exist, and the test cannot compile. When TASK-G.01 is implemented, new tests should be written alongside the actual methods.

#### 4. `HttpRouterShareGroupTest.scala.broken` -- DELETE

**What it tests:** Share group routing in `HttpRouter` -- `ShareGroupPollRoute`, `ShareGroupAcknowledgeRoute`, `NotFoundRoute`.

**Why it broke:** The test calls `HttpRouter.route("POST", "/v1/share-groups/my-share-group/records")` and pattern-matches on `HttpRouter.ShareGroupPollRoute(group)`, `HttpRouter.ShareGroupAcknowledgeRoute(group)`, and `HttpRouter.NotFoundRoute`. None of these types exist in the current `HttpRouter.java`:
- `HttpRouter.route()` is an instance method (not static), takes `(HttpMethod, String)` (Netty `HttpMethod`, not `String`), and returns `RouteResult` (not a sealed trait hierarchy)
- `ShareGroupPollRoute`, `ShareGroupAcknowledgeRoute`, `NotFoundRoute` are not defined anywhere in `HttpRouter.java`

The current `HttpRouter` uses an enum-based `HandlerType` system, not Scala case classes. The test was written against a planned Scala-style router that was never implemented.

**Action:** Delete the file. The route types it tests do not exist. When share group routes are added to `HttpRouter.java`, they will use `HandlerType` enum entries and `RouteResult`, and new tests should match that API.

#### 5. `ShareGroupApiContractTest.scala.broken` -- PARTIALLY FIXABLE, DELETE

**What it tests:** Share group API JSON contract -- request/response shapes, plus serialization via `HttpResponseSerializer.serializeShareFetchResponse()`, `HttpResponseSerializer.serializeShareAcknowledgeResponse()`, and `HttpResponseSerializer.serializeError()`.

**Why it broke:** The test references:
- `HttpResponseSerializer.serializeShareFetchResponse(response, topicNames)` -- exists in `HttpResponseSerializer.scala` (Scala object)
- `HttpResponseSerializer.serializeShareAcknowledgeResponse(response)` -- exists in `HttpResponseSerializer.scala`
- `HttpResponseSerializer.serializeError(statusCode, message)` -- exists in `HttpResponseSerializer.scala`
- `ShareFetchResponse.of(...)` -- the test uses a 5-arg `ShareFetchResponse.of()` which may not match the current method signature

**Problem:** There is a Scala `HttpResponseSerializer` object and a Java `HttpResponseSerializer` class in the same package (`kafka.server.http`). This is itself a duplicate class conflict. The Scala object has the share group methods; the Java class does not. At compile time, the Scala test would see both and get an ambiguous reference error.

Additionally, `ShareFetchResponse.of()` may have a different signature in the current codebase than what the test expects.

**Action:** Delete the file. The `HttpResponseSerializer` duplicate (Java vs Scala) must be resolved first (out of scope for this task), and the `ShareFetchResponse.of()` API may not match. The contract tests for JSON shapes (lines 34-102) are trivial JSON parsing tests that do not test any actual code. When the `HttpResponseSerializer` duplicate is resolved and share group support is complete, write proper tests.

---

## Implementation Details

### Step 1: Rename fixable tests

```bash
cd /home/anh/kafka

# Rename fixable tests back to .scala
mv http-server/src/test/scala/kafka/network/HttpAcceptorDrainTest.scala.broken \
   http-server/src/test/scala/kafka/network/HttpAcceptorDrainTest.scala

mv http-server/src/test/scala/kafka/network/HttpGracefulShutdownTest.scala.broken \
   http-server/src/test/scala/kafka/network/HttpGracefulShutdownTest.scala
```

### Step 2: Delete unfixable tests

```bash
rm http-server/src/test/scala/kafka/server/http/HttpRequestTranslatorShareGroupTest.scala.broken
rm http-server/src/test/scala/kafka/server/http/HttpRouterShareGroupTest.scala.broken
rm http-server/src/test/scala/kafka/server/http/ShareGroupApiContractTest.scala.broken
```

### Step 3: Verify restored tests compile

```bash
./gradlew :http-server:compileTestScala
```

If `HttpAcceptorDrainTest.scala` or `HttpGracefulShutdownTest.scala` fails to compile, diagnose the error:

**Possible issue in HttpAcceptorDrainTest:** The import `org.apache.kafka.network.{HttpServerConfigs => HttpConfigs}` may not resolve if the `HttpServerConfigs` class is not on the test compile classpath. Fix: check `http-server/build.gradle` for `implementation project(':server-common')` or similar dependency that provides `HttpServerConfigs`.

**Possible issue in HttpGracefulShutdownTest:** The Mockito import `org.mockito.Mockito._` requires Mockito on the test classpath. Verify `http-server/build.gradle` includes `testImplementation libs.mockitoCore` or equivalent.

### Step 4: Run restored tests

```bash
./gradlew :http-server:test --tests 'kafka.network.HttpAcceptorDrainTest'
./gradlew :http-server:test --tests 'kafka.network.HttpGracefulShutdownTest'
```

If any tests fail at runtime (e.g., port binding issues in `HttpAcceptorDrainTest`), fix the test setup to use port 0 or skip the startup() call where not needed.

---

## Skeleton Code

No new code -- this task only renames and deletes files.

---

## Tests

The restored test files ARE the tests:

1. `HttpAcceptorDrainTest.scala` -- 12 test methods covering drain lifecycle, in-flight tracking, idempotency, and close safety
2. `HttpGracefulShutdownTest.scala` -- 6 test methods covering 503 drain response, JSON format, Retry-After header, multiple requests, in-flight count tracking

### Verification Commands

```bash
./gradlew :http-server:test --tests 'kafka.network.HttpAcceptorDrainTest'
./gradlew :http-server:test --tests 'kafka.network.HttpGracefulShutdownTest'
```

---

## Rules

- Only restore tests that compile and pass against the current API. Do not hack tests to make them compile with stub methods.
- Delete tests that reference non-existent APIs (share group routes, share group translation methods). Do not leave dead test code.
- Do not create new stub methods in production code to satisfy broken tests -- that defeats the purpose of deduplication.
- After restoring tests, run them immediately. If they fail, investigate and fix. Do not leave tests that are renamed but still broken.

---

## Learning

(empty)

## Limitations

(empty)

## Field Notes

(empty)

---

## Acceptance Criteria

- [ ] `HttpAcceptorDrainTest.scala` restored and all 12 tests pass
- [ ] `HttpGracefulShutdownTest.scala` restored and all 6 tests pass
- [ ] `HttpRequestTranslatorShareGroupTest.scala.broken` deleted
- [ ] `HttpRouterShareGroupTest.scala.broken` deleted
- [ ] `ShareGroupApiContractTest.scala.broken` deleted
- [ ] No `.broken` files remain in the repository
- [ ] `./gradlew :http-server:compileTestScala` passes

---

## File Manifest

(empty -- to be filled after implementation)
