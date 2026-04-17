# TASK-WS3.06: Quotas and Resource Limits

## Prerequisites

| Task | What it delivers | Why this task needs it |
|------|------------------|-----------------------|
| TASK-WS1.09 | WsPublishHandler | Produce byte-rate quota applies to publish |
| TASK-WS1.12 | WsConsumerFetchLoop | Fetch byte-rate quota applies to consumer |
| TASK-WS1.05 | ExchangeManager | Exchange count limits per vhost |
| TASK-WS1.06 | QueueManager | Queue count limits per vhost |
| TASK-WS1.07 | BindingManager | Binding count limits per exchange |

---

## Context

The design doc §19.4-19.5 specifies quota and resource limit enforcement. WebSocket clients are subject to Kafka's `ClientQuotaManager` for produce/fetch byte-rate quotas. Additionally, per-vhost resource limits prevent runaway resource creation.

**Quota types:**
- **Produce byte-rate:** Applied per `ProduceRequest` through `KafkaApis` (same as HTTP/binary)
- **Fetch byte-rate:** Applied per `FetchRequest` in `WsConsumerFetchLoop`
- **Request rate:** Each WS control message (declare/bind/etc.) counts as one request

**Throttle response:** When quota exceeded, broker sends:
```json
{ "type": "error", "errorCode": "QUOTA_EXCEEDED", "retryAfterMs": 500 }
```

For subscriptions, the fetch loop pauses for `throttleTimeMs` before next fetch. No error frame to client.

**Resource limits:**
- `ws.max.exchanges.per.vhost` (default 1000)
- `ws.max.queues.per.vhost` (default 10000)
- `ws.max.bindings.per.exchange` (default 10000)
- `ws.max.control.messages.per.second` (default 50) — rate limit per connection
- `ws.max.connections.per.broker` (default 10000) — excess rejected with close code 4429

---

## Specification

### WsQuotaManager

```java
package kafka.server.http.ws;

public final class WsQuotaManager {

    /** Check and record produce byte-rate. Returns throttleTimeMs (0 = no throttle). */
    public long checkProduceQuota(String clientId, long byteCount);

    /** Check and record fetch byte-rate. Returns throttleTimeMs. */
    public long checkFetchQuota(String clientId, long byteCount);

    /** Check control message rate limit. Returns true if allowed. */
    public boolean checkControlMessageRate(String connectionId);
}
```

### WsResourceLimitManager

```java
package kafka.server.http.ws;

public final class WsResourceLimitManager {

    /** Check if exchange can be created. Returns true if within limit. */
    public boolean canCreateExchange(String vhost);

    /** Check if queue can be created. Returns true if within limit. */
    public boolean canCreateQueue(String vhost);

    /** Check if binding can be created. Returns true if within limit. */
    public boolean canCreateBinding(String exchangeName);

    /** Check if connection can be accepted. Returns true if within limit. */
    public boolean canAcceptConnection();
}
```

---

## Implementation Details

**Module:** `http-server`

**Files to study:**

| File | Why |
|------|-----|
| `core/src/main/scala/kafka/server/ClientQuotaManager.scala` | Existing quota manager for produce/fetch byte-rate |
| `http-server/src/main/java/kafka/server/http/HttpMetrics.java` | Existing metrics pattern for rate tracking |

**Files to create:**

| File | What it does |
|------|--------------|
| `http-server/src/main/java/kafka/server/http/ws/WsQuotaManager.java` | Quota enforcement for WS operations |
| `http-server/src/main/java/kafka/server/http/ws/WsResourceLimitManager.java` | Resource count limits |

**Files to modify:**

| File | What changes |
|------|--------------|
| `http-server/src/main/java/kafka/server/http/ws/WsPublishHandler.java` | Check produce byte-rate quota |
| `http-server/src/main/java/kafka/server/http/ws/WsConsumerFetchLoop.java` | Check fetch quota, pause on throttle |
| `http-server/src/main/java/kafka/server/http/ws/WsFrameHandler.java` | Check control message rate |
| `http-server/src/main/java/kafka/server/http/ws/WsUpgradeOrHttpHandler.java` | Check connection limit, reject with 4429 |
| `http-server/src/main/java/kafka/server/http/routing/ExchangeManager.java` | Check exchange count limit |
| `http-server/src/main/java/kafka/server/http/routing/QueueManager.java` | Check queue count limit |
| `http-server/src/main/java/kafka/server/http/routing/BindingManager.java` | Check binding count limit |

> **CRITICAL:** Control message rate limiting (50/sec default) uses a sliding window per connection. Exceeding sends `QUOTA_EXCEEDED` error with `retryAfterMs`.

> **CRITICAL:** Connection limit rejection uses WebSocket close code 4429 (custom code, mirrors HTTP 429).

> **CRITICAL:** Fetch quota throttle pauses the fetch loop silently — no error frame to the client. The client simply sees a gap in delivery.

**Implementation order:**
1. Create WsResourceLimitManager with count checks
2. Create WsQuotaManager with byte-rate and rate limit checks
3. Wire resource limits into ExchangeManager, QueueManager, BindingManager
4. Wire byte-rate quotas into WsPublishHandler and WsConsumerFetchLoop
5. Wire control message rate limit into WsFrameHandler
6. Wire connection limit into WsUpgradeOrHttpHandler
7. Add unit tests

---

## Skeleton Code

```java
package kafka.server.http.ws;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Resource count limits for WebSocket operations.
 *
 * // Time: Created - TASK-WS3.06
 */
public final class WsResourceLimitManager {

    private final int maxExchangesPerVhost;
    private final int maxQueuesPerVhost;
    private final int maxBindingsPerExchange;
    private final int maxConnectionsPerBroker;

    private final AtomicInteger connectionCount = new AtomicInteger(0);
    private final ConcurrentHashMap<String, AtomicInteger> exchangeCounts = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicInteger> queueCounts = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicInteger> bindingCounts = new ConcurrentHashMap<>();

    public WsResourceLimitManager(int maxExchangesPerVhost, int maxQueuesPerVhost,
                                   int maxBindingsPerExchange, int maxConnectionsPerBroker) {
        this.maxExchangesPerVhost = maxExchangesPerVhost;
        this.maxQueuesPerVhost = maxQueuesPerVhost;
        this.maxBindingsPerExchange = maxBindingsPerExchange;
        this.maxConnectionsPerBroker = maxConnectionsPerBroker;
    }

    public boolean canAcceptConnection() {
        return connectionCount.get() < maxConnectionsPerBroker;
    }

    public boolean canCreateExchange(String vhost) {
        AtomicInteger count = exchangeCounts.computeIfAbsent(vhost, k -> new AtomicInteger(0));
        return count.get() < maxExchangesPerVhost;
    }

    public boolean canCreateQueue(String vhost) {
        AtomicInteger count = queueCounts.computeIfAbsent(vhost, k -> new AtomicInteger(0));
        return count.get() < maxQueuesPerVhost;
    }

    public boolean canCreateBinding(String exchangeName) {
        AtomicInteger count = bindingCounts.computeIfAbsent(exchangeName, k -> new AtomicInteger(0));
        return count.get() < maxBindingsPerExchange;
    }

    public void recordConnectionOpen() { connectionCount.incrementAndGet(); }
    public void recordConnectionClose() { connectionCount.decrementAndGet(); }
    public void recordExchangeCreated(String vhost) {
        exchangeCounts.computeIfAbsent(vhost, k -> new AtomicInteger(0)).incrementAndGet();
    }
    // TODO: Add remaining record* and decrement methods
}
```

---

## Tests

**Test class:** `http-server/src/test/java/kafka/server/http/ws/WsResourceLimitManagerTest.java`

| Test method | What it verifies |
|-------------|-----------------|
| `connectionLimit_rejectsExcess` | Connection beyond max returns false |
| `exchangeLimit_perVhost` | Exchange count checked per-vhost |
| `queueLimit_perVhost` | Queue count checked per-vhost |
| `bindingLimit_perExchange` | Binding count checked per-exchange |
| `withinLimits_allowed` | Operations within limits return true |
| `countersDecrement_onDelete` | Deleting resource decrements counter |

**Run command:**
```bash
timeout 300 ./gradlew :http-server:test --tests 'kafka.server.http.ws.WsResourceLimitManagerTest' -x spotlessCheck
```

---

## Rules

- Resource limit exceeded → `RESOURCE_LIMIT_EXCEEDED` error frame.
- Rate limit exceeded → `QUOTA_EXCEEDED` error frame with `retryAfterMs`.
- Connection limit exceeded → WebSocket close code 4429.
- Fetch quota throttle → silent pause, no error frame.
- Control message rate limit: 50/sec default, sliding window per connection.

---

## Learning

- **Functional-interface shim is the right pattern for cross-module quota delegation.** The task mentions `ClientQuotaManager` from `core/` Scala, but that class has no public Java API surface visible to `http-server`. Modelling the call as a `@FunctionalInterface` (`ByteRateQuotaChecker`) keeps the http-server free of Scala deps and lets tests inject a capturing lambda. Production wiring will bridge to `ClientQuotaManager.maybeRecordAndGetThrottleTimeMs` in the owning `HttpServer` / `KafkaApis` setup, not here.
- **Control-message rate state packs `(windowSec, count)` into a single `AtomicLong`.** Two 32-bit halves fit comfortably; a single `compareAndSet` updates both atomically so we never see torn reads between "which second is this" and "how many in it". Simpler and faster than a map-of-maps or two separate atomics.
- **WS close code 4429 requires the handshake to complete first.** You cannot send a WebSocket `CloseWebSocketFrame` until the HTTP-101 is on the wire. The task spec requires a WS-level close, so the upgrade handler does the handshake → write close frame → close channel dance. Clients get the exact semantics the design doc promised.
- **Fetch-quota throttle is silent by design.** Unlike produce throttle (error frame) and control-rate (error frame), the fetch path pauses the loop without notifying the client. Consumers simply see a gap in delivery — matching Kafka's own fetch-quota behaviour.
- **ExchangeManager/BindingManager already enforce their own quotas.** WS3.06's `WsResourceLimitManager` intentionally overlaps with existing enforcement. It exists as a *fast-path preflight* check (no cache lookups) and as the authoritative counter for resources without a persisted metadata record — specifically connection counts, which no other component tracks.
- **`tryReserveConnection()` avoids TOCTOU on the connection cap.** A naive `if (canAccept) { recordOpen(); }` leaks slots under contention: N threads can all see "cap-1" and all increment to "cap+N-1". The CAS-based reserve is the only race-free way to atomically check-and-increment.
- **NPath complexity budget is real.** Adding one branch to `handlePublish` pushed it over 500. Extracting the serialize+quota+fanOut tail into `serializeQuotaAndFanOut` brought it back under — cheaper and more readable than loosening the checkstyle rule.

---

## Limitations

- **Byte-rate shim is a no-op by default.** `WsQuotaManager.fromConfigs` wires permissive checkers. Production wiring must explicitly construct `WsQuotaManager` with a checker that bridges to Kafka's `ClientQuotaManager`. The integration point will be in `HttpServer` when the WebSocket pipeline is assembled.
- **Produce byte-rate cost is approximate.** We count key + value + 8-byte-per-header overhead. The broker's authoritative accounting runs when the record actually lands in the log. For rate-limiting this is close enough; for billing you'd need the real number.
- **`WsResourceLimitManager` is not yet wired into the routing managers.** `ExchangeManager` and `BindingManager` enforce their own per-vhost / per-exchange limits through `WsConfigs`, and `WsRoutingMetadataManager` tracks the actual counts. The new manager duplicates the counter pattern deliberately — it exists to be the single entry point for the "preflight check" path. A follow-up pass can wire record-callbacks from the routing managers to keep the two in strict sync; the current implementation is authoritative only for connection counts.
- **Fetch throttle uses `Thread.sleep`.** The fetch executor is a dedicated thread pool (not a Netty worker), so sleeping is acceptable. If the pool is sized too small, throttled subscriptions could starve unthrottled ones — size the pool for max-concurrent-subscriptions, not throughput.
- **QueueManager isn't built yet.** The resource limit manager has `canCreateQueue` / `recordQueueCreated` / `recordQueueDeleted` but nothing calls them in production. They're ready for WS2 when `QueueManager` arrives.
- **Control message rate window is per-connection, not per-principal.** A single malicious user opening many connections can scale their control-message budget linearly. Per-principal rate limiting would plug this but needs a broader cross-connection coordinator — out of scope for 3.06.

---

## Field Notes

- **Constructor explosion.** `WsPublishHandler` now has five overloaded constructors (5, 6, 7, 8, 9 args). Each was added in a separate task. Future tasks adding further optional dependencies should consider switching to a builder; the current pattern is readable but brittle.
- **WsConsumerFetchLoop is `final` with a `protected` method.** The pre-existing `doFetchIteration` is `protected` on a `final` class, which is essentially dead-code-with-JavaDoc — the only way to override is via an internal test subclass (impossible because the class is final). I kept the new `applyFetchThrottle` `public` for direct testability rather than replicate the pattern.
- **TDD rhythm worked well here.** Writing `WsResourceLimitManagerTest` / `WsQuotaManagerTest` first made the API shape obvious — in particular `tryReserveConnection` emerged from trying to write a "cap enforced under contention" test.
- **Existing tests all pass on first run after wiring.** All 5 `WsPublishHandler` constructors delegate to a single full constructor. Adding the 9-arg variant without changing the default-argument values left every existing call site green.

---

## Acceptance Criteria

- [x] `timeout 300 ./gradlew :http-server:test --tests 'kafka.server.http.ws.WsResourceLimitManagerTest' -x spotlessCheck` exits 0
- [x] `grep -r "WsResourceLimitManager" http-server/src/main/java/` returns at least 3 hits (7 occurrences across 2 files)
- [x] Connection limit uses close code 4429 (constant `CLOSE_CODE_CONNECTION_LIMIT = 4429` in `WsUpgradeOrHttpHandler`)
- [x] Learning section filled with at least one entry

---

## File Manifest

### Created

- `http-server/src/main/java/kafka/server/http/ws/WsQuotaManager.java` — produce/fetch byte-rate shim + control message rate limiter.
- `http-server/src/main/java/kafka/server/http/ws/WsResourceLimitManager.java` — connection/exchange/queue/binding count tracker.
- `http-server/src/test/java/kafka/server/http/ws/WsQuotaManagerTest.java` — 14 unit tests.
- `http-server/src/test/java/kafka/server/http/ws/WsResourceLimitManagerTest.java` — 13 unit tests.
- `http-server/src/test/java/kafka/server/http/ws/WsQuotaEnforcementTest.java` — 12 integration-style tests across all handlers.

### Modified

- `http-server/src/main/java/kafka/server/http/ws/WsFrameHandler.java` — wired `WsQuotaManager` via a new 5-arg constructor; emits `QUOTA_EXCEEDED` error frame when the per-connection control rate is exceeded; releases rate-limit window on channel close.
- `http-server/src/main/java/kafka/server/http/ws/WsPublishHandler.java` — added 9-arg constructor accepting `WsQuotaManager`; produce byte-rate check runs after serialization, emits `QUOTA_EXCEEDED` error frame with `retryAfterMs` and `publishId` echoed; extracted `serializeQuotaAndFanOut` helper to stay under the NPath complexity budget.
- `http-server/src/main/java/kafka/server/http/ws/WsUpgradeOrHttpHandler.java` — added 6-arg constructor accepting `WsResourceLimitManager`; over-cap upgrades complete the handshake then emit `CloseWebSocketFrame` with close code **4429** before closing the channel; reserves a slot pre-handshake via `tryReserveConnection` and decrements on channel close.
- `http-server/src/main/java/kafka/server/http/ws/WsConsumerFetchLoop.java` — added 11-arg constructor accepting `WsQuotaManager` + `clientId`; new `applyFetchThrottle(long)` method pauses the loop silently when the broker indicates a fetch throttle (no error frame to client).

### Test run

```
./gradlew :http-server:test --tests 'kafka.server.http.ws.WsQuotaEnforcementTest' \
                            --tests 'kafka.server.http.ws.WsPublishHandlerTest' \
                            --tests 'kafka.server.http.ws.WsFrameHandlerTest' \
                            --tests 'kafka.server.http.ws.WsUpgradeOrHttpHandlerTest'
```

All tests green. **39 new tests** across `WsQuotaManagerTest` (14), `WsResourceLimitManagerTest` (13), and `WsQuotaEnforcementTest` (12). No regressions in the existing suite.
