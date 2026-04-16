# TASK-A.03: RequestChannel.tryEnqueue() method

## Prerequisites

- JDK 17+
- Kafka repository checked out at branch `feature/http-protocol`
- Ability to run `./gradlew :core:test` from repo root
- Read the design doc section 14.3: `/home/anh/kafka/ivy-docs/http-protocol-design.md`

## Context

`RequestChannel` is the central queue that connects Kafka's network layer (acceptors /
processors) to the request handler thread pool (`KafkaRequestHandler`). Every request --
binary protocol and (soon) HTTP -- passes through this queue.

The existing `sendRequest()` method uses `requestQueue.put(request)`, which is a **blocking
put** on an `ArrayBlockingQueue`. For the binary protocol this is acceptable: the binary
`Processor` calls `selector.mute(connectionId)` immediately after enqueuing, so it stops
reading that connection and avoids queue overflow. The blocking `put()` is brief because
the selector loop pauses per-connection flow control.

Netty has no equivalent per-connection mute mechanism at the Kafka level. If a Netty worker
thread calls the blocking `put()` and the queue is full, the thread blocks inside Kafka,
unable to read HTTP frames or write responses on **any** of the connections it multiplexes.
This is a head-of-line blocking hazard that can freeze the entire HTTP listener.

### The fix

Add a non-blocking `tryEnqueue()` method that uses `ArrayBlockingQueue.offer()` instead of
`put()`. `offer()` returns `false` immediately if the queue is full, allowing the Netty
worker thread to respond with HTTP 503 and continue processing other connections.

This is a one-line addition to a critical shared class. The existing `sendRequest()` method
is **not modified** -- binary protocol callers continue using the blocking path unchanged.

### Why this task exists

`HttpRequestHandler` (to be implemented in a subsequent task) must call
`requestChannel.tryEnqueue(kafkaRequest)` instead of `requestChannel.sendRequest(kafkaRequest)`.
Without `tryEnqueue()`, no HTTP request can be safely enqueued.

### Key files

| File | Role |
|---|---|
| `core/src/main/scala/kafka/network/RequestChannel.scala` | Class to modify |
| `core/src/test/scala/unit/kafka/network/RequestChannelTest.scala` | Existing test class |

## Specification

### 1. Add `tryEnqueue()` method to `RequestChannel`

Open `/home/anh/kafka/core/src/main/scala/kafka/network/RequestChannel.scala`.

The `RequestChannel` class is defined at line 344. The `requestQueue` field is at line 354:

```scala
private val requestQueue = new ArrayBlockingQueue[BaseRequest](queueSize)
```

The existing `sendRequest()` method is at lines 381-383:

```scala
/** Send a request to be handled, potentially blocking until there is room in the queue for the request */
def sendRequest(request: RequestChannel.Request): Unit = {
    requestQueue.put(request)
}
```

Add the following method **immediately after** `sendRequest()` (at line 384):

```scala
/**
 * Attempt to enqueue a request without blocking. Returns true if the request was
 * accepted, false if the queue is full. This is used by the HTTP listener (Netty)
 * where blocking would stall the event loop and cause head-of-line blocking across
 * all HTTP connections on that worker thread.
 *
 * Callers that receive false should return an immediate error response (e.g. HTTP 503)
 * rather than retrying or blocking.
 *
 * @see #sendRequest(RequestChannel.Request) for the blocking variant used by the binary protocol
 */
def tryEnqueue(request: RequestChannel.Request): Boolean =
    requestQueue.offer(request)
```

That is the complete change. No other lines in `RequestChannel.scala` are modified.

## Implementation Details

### Why `offer()` is safe

`ArrayBlockingQueue.offer(e)` is a thread-safe, non-blocking operation. It returns `true`
if the element was added, `false` if the queue is at capacity. It does not throw exceptions
for a full queue (unlike `add()` which throws `IllegalStateException`).

The `requestQueue` is already accessed concurrently by multiple binary `Processor` threads
calling `sendRequest()` (via `put()`) and by `KafkaRequestHandler` threads calling
`receiveRequest()` (via `poll()` or `take()`). Adding another concurrent accessor via
`offer()` is safe -- `ArrayBlockingQueue` is designed for exactly this.

### Method signature choice

The method takes `RequestChannel.Request` (not `BaseRequest`) because:
1. HTTP requests are always `Request` objects, never `ShutdownRequest`
2. The method is specifically for HTTP callers where type safety matters
3. `sendRequest()` also takes `Request`, maintaining API consistency

### What the caller does on `false`

When `tryEnqueue()` returns `false`, the HTTP handler (`HttpRequestHandler`) should:
1. Immediately write an HTTP 503 response with `Retry-After: 1` header
2. Close or release the Netty channel context as appropriate
3. Increment the `http.queue.full.rate` metric (implemented in a later task)

This keeps the Netty worker thread free to handle other connections.

### Thread safety analysis

| Thread | Operation | Blocking? |
|---|---|---|
| Binary `Processor` | `sendRequest()` -> `put()` | Yes (blocks if full) |
| HTTP Netty worker | `tryEnqueue()` -> `offer()` | No (returns false if full) |
| `KafkaRequestHandler` | `receiveRequest()` -> `take()` or `poll()` | Yes (blocks if empty) |
| Callback thread | `sendCallbackRequest()` -> `callbackQueue.put()` | Separate queue |

All operations use `ArrayBlockingQueue`'s built-in `ReentrantLock`. No additional
synchronization is needed.

## Skeleton Code

### RequestChannel.scala (addition only)

The full context around the change. Lines before and after are shown for placement reference.
Only the `tryEnqueue` method is new.

```scala
  /** Send a request to be handled, potentially blocking until there is room in the queue for the request */
  def sendRequest(request: RequestChannel.Request): Unit = {
    requestQueue.put(request)
  }

  /**
   * Attempt to enqueue a request without blocking. Returns true if the request was
   * accepted, false if the queue is full. This is used by the HTTP listener (Netty)
   * where blocking would stall the event loop and cause head-of-line blocking across
   * all HTTP connections on that worker thread.
   *
   * Callers that receive false should return an immediate error response (e.g. HTTP 503)
   * rather than retrying or blocking.
   *
   * @see #sendRequest(RequestChannel.Request) for the blocking variant used by the binary protocol
   */
  def tryEnqueue(request: RequestChannel.Request): Boolean =
    requestQueue.offer(request)

  def closeConnection(
    request: RequestChannel.Request,
    errorCounts: java.util.Map[Errors, Integer]
  ): Unit = {
```

## Tests

### Add tests to existing `RequestChannelTest.scala`

Open `/home/anh/kafka/core/src/test/scala/unit/kafka/network/RequestChannelTest.scala`.

Add the following test methods to the `RequestChannelTest` class. These tests verify the
`tryEnqueue()` method's behavior in normal and full-queue scenarios.

You will need to add a helper method that creates a `RequestChannel` with a specific queue
size, and a helper that creates a minimal `RequestChannel.Request` object. Check the
existing test class for patterns -- it likely already has `request()` helpers.

```scala
  @Test
  def testTryEnqueueSuccess(): Unit = {
    // Create a RequestChannel with a small queue
    val channel = new RequestChannel(
      queueSize = 5,
      new MockTime(),
      requestChannelMetrics
    )
    try {
      val req = buildMinimalRequest()
      val result = channel.tryEnqueue(req)
      assertTrue(result, "tryEnqueue should return true when queue has space")

      // Verify the request is in the queue by receiving it
      val received = channel.receiveRequest(timeout = 1000)
      assertNotNull(received, "Should receive the enqueued request")
    } finally {
      channel.shutdown()
    }
  }

  @Test
  def testTryEnqueueReturnsFalseWhenQueueFull(): Unit = {
    // Create a RequestChannel with a queue size of 1
    val channel = new RequestChannel(
      queueSize = 1,
      new MockTime(),
      requestChannelMetrics
    )
    try {
      // Fill the queue
      val req1 = buildMinimalRequest()
      val firstResult = channel.tryEnqueue(req1)
      assertTrue(firstResult, "First tryEnqueue should succeed")

      // Second enqueue should fail (queue full)
      val req2 = buildMinimalRequest()
      val secondResult = channel.tryEnqueue(req2)
      assertFalse(secondResult, "tryEnqueue should return false when queue is full")
    } finally {
      channel.shutdown()
    }
  }

  @Test
  def testTryEnqueueDoesNotBlock(): Unit = {
    // Create a RequestChannel with queue size 1, fill it, then verify tryEnqueue
    // returns within a short timeout (proving it does not block)
    val channel = new RequestChannel(
      queueSize = 1,
      new MockTime(),
      requestChannelMetrics
    )
    try {
      // Fill the queue
      val req1 = buildMinimalRequest()
      channel.tryEnqueue(req1)

      // tryEnqueue on full queue should return immediately
      val startNanos = System.nanoTime()
      val req2 = buildMinimalRequest()
      val result = channel.tryEnqueue(req2)
      val elapsedMs = (System.nanoTime() - startNanos) / 1_000_000

      assertFalse(result, "tryEnqueue should return false on full queue")
      assertTrue(elapsedMs < 100, s"tryEnqueue should not block; took ${elapsedMs}ms")
    } finally {
      channel.shutdown()
    }
  }

  @Test
  def testTryEnqueueAfterDrain(): Unit = {
    // Fill queue, drain one, then tryEnqueue should succeed again
    val channel = new RequestChannel(
      queueSize = 1,
      new MockTime(),
      requestChannelMetrics
    )
    try {
      val req1 = buildMinimalRequest()
      assertTrue(channel.tryEnqueue(req1))

      // Queue is full
      val req2 = buildMinimalRequest()
      assertFalse(channel.tryEnqueue(req2))

      // Drain one request
      channel.receiveRequest(timeout = 1000)

      // Now there should be space
      val req3 = buildMinimalRequest()
      assertTrue(channel.tryEnqueue(req3), "tryEnqueue should succeed after draining")
    } finally {
      channel.shutdown()
    }
  }
```

### Helper method

The existing `RequestChannelTest` class likely already has a `request()` helper that builds
a `RequestChannel.Request`. If it does, use it. If not, create a `buildMinimalRequest()`
helper:

```scala
  private def buildMinimalRequest(): RequestChannel.Request = {
    // Use the pattern from the existing request() helper in this test class.
    // Build a minimal CreateTopicsRequest or MetadataRequest -- the simplest
    // request type that can be constructed without complex dependencies.
    //
    // Example using MetadataRequest (requires fewest arguments):
    val metadataRequest = new MetadataRequest.Builder(
      java.util.Collections.emptyList(), true).build()
    val buffer = metadataRequest.serialize()

    val header = new RequestHeader(
      ApiKeys.METADATA,
      metadataRequest.version(),
      "test-client",
      0
    )

    val context = new RequestContext(
      header,
      "test-connection",
      InetAddress.getLoopbackAddress,
      Optional.of(1234),
      KafkaPrincipal.ANONYMOUS,
      new ListenerName("PLAINTEXT"),
      SecurityProtocol.PLAINTEXT,
      ClientInformation.EMPTY,
      false
    )

    new RequestChannel.Request(
      processor = 0,
      context = context,
      startTimeNanos = System.nanoTime(),
      memoryPool = MemoryPool.NONE,
      buffer = buffer,
      metrics = requestChannelMetrics,
      envelope = None
    )
  }
```

**Important:** Check the existing test file for how `Request` objects are constructed. The
exact constructor arguments may vary depending on the Kafka version. Match the existing
pattern. The existing test class has a `request()` method near the bottom -- use the same
approach. If the existing helper takes an `AbstractRequest`, create a `MetadataRequest` and
pass it in.

### Required imports for new tests

```scala
import org.apache.kafka.common.utils.MockTime
import org.junit.jupiter.api.Assertions.{assertTrue, assertFalse, assertNotNull}
```

Check which imports are already in the file and only add what is missing.

## Rules

1. **Do not modify `sendRequest()`.** The binary protocol path must continue using the
   blocking `put()`. Binary `Processor` threads have their own backpressure via
   `selector.mute()`.

2. **Do not change the `requestQueue` type or capacity.** The queue remains an
   `ArrayBlockingQueue[BaseRequest]` with capacity `queueSize` (from `queued.max.requests`
   config, default 500).

3. **The method must be named `tryEnqueue`** (not `offerRequest`, `enqueue`, etc.). The
   design doc and all downstream HTTP handler code reference this exact name.

4. **The method must take `RequestChannel.Request`** (not `BaseRequest`). HTTP requests are
   always `Request` objects. This provides compile-time type safety for HTTP callers.

5. **The method must return `Boolean`** (Scala) / `boolean` (from Java callers). `true` means
   the request was accepted. `false` means the queue is full.

6. **Include a Scaladoc comment** on the method. Explain that it is non-blocking, what callers
   should do on `false`, and reference `sendRequest()` as the blocking alternative.

7. **Do not add any metrics in this task.** The `http.queue.full.rate` metric will be added
   in the `HttpRequestHandler` task. This task is purely the queue-level primitive.

8. **Run `./gradlew :core:test` to verify no existing tests break.** The `RequestChannelTest`
   class exercises the existing `sendRequest()` and `receiveRequest()` paths. Adding
   `tryEnqueue()` must not interfere.

## Learning

- The Gradle build cache in a git worktree does not automatically invalidate when the source
  file changes -- editing files in the wrong directory (main repo vs worktree) causes silent
  stale builds. Always use full absolute worktree paths when editing.
- Scala's `ArrayBlockingQueue.offer()` is the correct non-blocking counterpart to `put()`.
  It returns `Boolean` directly, no wrapper needed.
- The existing `RequestChannel.Request` constructor requires `RequestChannelMetrics` (not a
  mock of `RequestChannel`), and the test pattern uses `mock(classOf[RequestChannelMetrics])`
  for the metrics parameter.
- `RequestChannel.shutdown()` calls `metrics.close()` on the metrics object, which works
  fine with mocked metrics in tests.

## Limitations

- `tryEnqueue()` provides no backpressure signal beyond the boolean return value. The caller
  (HTTP handler) must implement its own retry/503 logic.
- No metric is emitted when `tryEnqueue()` returns false -- that is deferred to TASK-F.03
  (HttpMetrics) where `http.queue.full.rate` will be tracked.
- The method does not distinguish between "queue is full" and other potential failure modes
  of `offer()` (though in practice `ArrayBlockingQueue.offer()` only returns false for
  capacity reasons).

## Field Notes

- TDD RED/GREEN confirmed: 8 compilation errors when `tryEnqueue` was absent, all 11 tests
  (7 existing + 4 new) pass after adding the one-method implementation.
- The `buildMinimalRequest()` helper constructs a `MetadataRequest` with empty topic list,
  which is the lightest-weight request type available. The existing test helper `request()`
  follows a similar pattern but uses `serializeWithHeader` + `RequestHeader.parse`.
- `RequestChannel` constructor takes `(queueSize: Int, time: Time, metrics: RequestChannelMetrics)`.
  Tests use `MockTime()` for the time parameter and a mockito mock for metrics.

## Acceptance Criteria

- [x] `RequestChannel` has a `tryEnqueue(request: RequestChannel.Request): Boolean` method
- [x] `tryEnqueue` uses `requestQueue.offer(request)` internally
- [x] `tryEnqueue` returns `true` when the queue has available capacity
- [x] `tryEnqueue` returns `false` when the queue is full
- [x] `tryEnqueue` does NOT block the calling thread (completes in < 1ms on a full queue)
- [x] `tryEnqueue` has a Scaladoc comment explaining its purpose and non-blocking behavior
- [x] The existing `sendRequest()` method is NOT modified
- [x] The `requestQueue` field type and capacity are NOT changed
- [x] Test: `testTryEnqueueSuccess` -- enqueue succeeds and request is receivable
- [x] Test: `testTryEnqueueReturnsFalseWhenQueueFull` -- returns false when queue is at capacity
- [x] Test: `testTryEnqueueDoesNotBlock` -- completes within 100ms on a full queue
- [x] Test: `testTryEnqueueAfterDrain` -- succeeds after draining a full queue
- [x] `./gradlew :core:test` passes with zero new failures
- [x] No other methods in `RequestChannel` are modified

## File Manifest

| File | Change |
|---|---|
| `core/src/main/scala/kafka/network/RequestChannel.scala` | Added `tryEnqueue()` method (lines 385-398) |
| `core/src/test/scala/unit/kafka/network/RequestChannelTest.scala` | Added 4 test methods + `buildMinimalRequest()` helper, `MockTime` import |
| `ivy-docs/tasks/TASK-A.03-request-channel-try-enqueue.md` | Updated Learning, Limitations, Field Notes, Acceptance Criteria, File Manifest |
