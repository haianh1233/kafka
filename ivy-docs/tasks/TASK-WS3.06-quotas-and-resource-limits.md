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

_To be filled by the executing agent._

---

## Limitations

_To be filled by the executing agent._

---

## Field Notes

_To be filled by the executing agent._

---

## Acceptance Criteria

- [ ] `timeout 300 ./gradlew :http-server:test --tests 'kafka.server.http.ws.WsResourceLimitManagerTest' -x spotlessCheck` exits 0
- [ ] `grep -r "WsResourceLimitManager" http-server/src/main/java/` returns at least 3 hits
- [ ] Connection limit uses close code 4429
- [ ] Learning section filled with at least one entry

---

## File Manifest

_To be filled by the executing agent._
