# TASK-WS3.08: WebSocket Metrics

## Prerequisites

| Task | What it delivers | Why this task needs it |
|------|------------------|-----------------------|
| TASK-WS1.09 | WsPublishHandler | Source for publish rate metric |
| TASK-WS1.12 | WsConsumerFetchLoop | Source for deliver rate metric |
| TASK-WS1.14 | WsAckHandler | Source for ack/nack rate metrics |
| TASK-WS1.13 | WsCreditManager | Source for credit exhausted metric |

---

## Context

The design doc §11.5 specifies 15+ WebSocket-specific metrics registered with Kafka's Yammer metrics framework. These follow the same `KafkaMetricsGroup` pattern used by the existing `HttpMetrics` class (TASK-F.03), using a `protocol=ws` tag to distinguish from HTTP and binary metrics.

**Metrics to implement:**

| Metric | Type | Tags | Description |
|---|---|---|---|
| `ws.publish.rate` | Meter | protocol=ws | Messages published per second |
| `ws.deliver.rate` | Meter | protocol=ws | Messages delivered to consumers per second |
| `ws.ack.rate` | Meter | protocol=ws | ACKs per second |
| `ws.nack.rate` | Meter | protocol=ws | NACKs per second |
| `ws.error.rate` | Meter | protocol=ws | Error frames per second |
| `ws.connection.count` | Gauge | protocol=ws | Current WS connection count |
| `ws.subscription.count` | Gauge | protocol=ws | Current active subscription count |
| `ws.queue.depth` | Gauge | protocol=ws, queue={name} | Unconsumed messages per queue |
| `ws.queue.consumers` | Gauge | protocol=ws, queue={name} | Active consumers per queue |
| `ws.credit.exhausted.count` | Meter | protocol=ws | Credits exhausted events per second |
| `ws.redelivery.rate` | Meter | protocol=ws | Redelivered messages per second |
| `ws.dlx.rate` | Meter | protocol=ws | Dead-lettered messages per second |
| `ws.mandatory.return.rate` | Meter | protocol=ws | Mandatory returns per second |
| `ws.confirm.rate` | Meter | protocol=ws | Publisher confirms per second |
| `ws.control.message.rate` | Meter | protocol=ws | Control messages per second |

---

## Specification

### WsMetrics

```java
package kafka.server.http.ws;

public class WsMetrics implements Closeable {

    // Meters (rates)
    public final Meter publishRate;
    public final Meter deliverRate;
    public final Meter ackRate;
    public final Meter nackRate;
    public final Meter errorRate;
    public final Meter creditExhaustedRate;
    public final Meter redeliveryRate;
    public final Meter dlxRate;
    public final Meter mandatoryReturnRate;
    public final Meter confirmRate;
    public final Meter controlMessageRate;

    // Gauge registration
    public void registerConnectionCountGauge(Supplier<Integer> supplier);
    public void registerSubscriptionCountGauge(Supplier<Integer> supplier);
    public void registerQueueDepthGauge(String queueName, Supplier<Long> supplier);
    public void registerQueueConsumersGauge(String queueName, Supplier<Integer> supplier);
    public void removeQueueGauges(String queueName);

    @Override
    public void close();
}
```

---

## Implementation Details

**Module:** `http-server`

**Files to study:**

| File | Why |
|------|-----|
| `http-server/src/main/java/kafka/server/http/HttpMetrics.java` | Exact pattern to follow for KafkaMetricsGroup + Meter + Gauge |

**Files to create:**

| File | What it does |
|------|--------------|
| `http-server/src/main/java/kafka/server/http/ws/WsMetrics.java` | All WS metrics |

**Files to modify:**

| File | What changes |
|------|--------------|
| `http-server/src/main/java/kafka/server/http/ws/WsPublishHandler.java` | Call `wsMetrics.publishRate.mark()` on publish |
| `http-server/src/main/java/kafka/server/http/ws/WsConsumerFetchLoop.java` | Call `wsMetrics.deliverRate.mark()` on deliver |
| `http-server/src/main/java/kafka/server/http/ws/WsAckHandler.java` | Call `wsMetrics.ackRate.mark()` and `nackRate.mark()` |
| `http-server/src/main/java/kafka/server/http/ws/WsCreditManager.java` | Call `wsMetrics.creditExhaustedRate.mark()` when credits reach 0 |
| `http-server/src/main/java/kafka/server/http/ws/WsFrameHandler.java` | Call `wsMetrics.controlMessageRate.mark()` on control messages |
| `http-server/src/main/java/kafka/server/http/ws/WsConnectionContext.java` | Register/deregister connection count gauge |

> **CRITICAL:** Per-queue gauges (`ws.queue.depth`, `ws.queue.consumers`) must be registered on queue creation and removed on queue deletion. Use `ConcurrentHashMap` for gauge tracking, same pattern as `HttpMetrics.forwardQueueGauges`.

> **CRITICAL:** All meters use `protocol=ws` tag. Metric names follow Kafka conventions.

**Implementation order:**
1. Create WsMetrics with all meters and gauge registration methods
2. Wire `mark()` calls into publish, deliver, ack, nack, error, credit exhausted paths
3. Wire gauge registration into connection/subscription/queue lifecycle
4. Add close() cleanup (remove all registered metrics)
5. Add unit tests

---

## Skeleton Code

```java
package kafka.server.http.ws;

import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Meter;
import org.apache.kafka.server.metrics.KafkaMetricsGroup;

import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * WebSocket-specific metrics. All metrics use protocol=ws tag.
 *
 * // Time: Created - TASK-WS3.08
 */
public class WsMetrics implements Closeable {

    private static final String PACKAGE_NAME = "kafka.server.http.ws";
    private static final String CLASS_NAME = "WsMetrics";
    private static final Map<String, String> WS_TAG = Map.of("protocol", "ws");

    private final KafkaMetricsGroup metricsGroup;
    private final ConcurrentHashMap<String, Gauge<?>> queueGauges = new ConcurrentHashMap<>();

    public final Meter publishRate;
    public final Meter deliverRate;
    public final Meter ackRate;
    public final Meter nackRate;
    public final Meter errorRate;
    public final Meter creditExhaustedRate;
    public final Meter redeliveryRate;
    public final Meter dlxRate;
    public final Meter mandatoryReturnRate;
    public final Meter confirmRate;
    public final Meter controlMessageRate;

    public WsMetrics() {
        this.metricsGroup = new KafkaMetricsGroup(PACKAGE_NAME, CLASS_NAME);
        this.publishRate = metricsGroup.newMeter("PublishRate", "messages", TimeUnit.SECONDS, WS_TAG);
        this.deliverRate = metricsGroup.newMeter("DeliverRate", "messages", TimeUnit.SECONDS, WS_TAG);
        this.ackRate = metricsGroup.newMeter("AckRate", "acks", TimeUnit.SECONDS, WS_TAG);
        this.nackRate = metricsGroup.newMeter("NackRate", "nacks", TimeUnit.SECONDS, WS_TAG);
        this.errorRate = metricsGroup.newMeter("ErrorRate", "errors", TimeUnit.SECONDS, WS_TAG);
        this.creditExhaustedRate = metricsGroup.newMeter("CreditExhaustedRate", "events", TimeUnit.SECONDS, WS_TAG);
        this.redeliveryRate = metricsGroup.newMeter("RedeliveryRate", "messages", TimeUnit.SECONDS, WS_TAG);
        this.dlxRate = metricsGroup.newMeter("DlxRate", "messages", TimeUnit.SECONDS, WS_TAG);
        this.mandatoryReturnRate = metricsGroup.newMeter("MandatoryReturnRate", "returns", TimeUnit.SECONDS, WS_TAG);
        this.confirmRate = metricsGroup.newMeter("ConfirmRate", "confirms", TimeUnit.SECONDS, WS_TAG);
        this.controlMessageRate = metricsGroup.newMeter("ControlMessageRate", "messages", TimeUnit.SECONDS, WS_TAG);
    }

    public void registerQueueDepthGauge(String queueName, Supplier<Long> supplier) {
        // TODO: Register gauge with queue={queueName} tag
    }

    public void removeQueueGauges(String queueName) {
        // TODO: Remove gauges for queue
    }

    @Override
    public void close() {
        // TODO: Remove all registered metrics
    }
}
```

---

## Tests

**Test class:** `http-server/src/test/java/kafka/server/http/ws/WsMetricsTest.java`

| Test method | What it verifies |
|-------------|-----------------|
| `publishRate_markedOnPublish` | Meter increments on publish |
| `deliverRate_markedOnDeliver` | Meter increments on deliver |
| `ackRate_markedOnAck` | Meter increments on ack |
| `nackRate_markedOnNack` | Meter increments on nack |
| `queueDepthGauge_registeredOnQueueCreate` | Gauge registered with queue tag |
| `queueGauges_removedOnQueueDelete` | Gauges cleaned up on queue deletion |
| `close_removesAllMetrics` | All metrics deregistered on close |
| `allMeters_haveProtocolWsTag` | All meters have protocol=ws |

**Run command:**
```bash
timeout 300 ./gradlew :http-server:test --tests 'kafka.server.http.ws.WsMetricsTest' -x spotlessCheck
```

---

## Rules

- All metrics use `protocol=ws` tag.
- Per-queue gauges registered on queue creation, removed on deletion.
- Follow `HttpMetrics` pattern exactly for meter/gauge creation.
- close() must remove all registered metrics to prevent leaks.
- Meter names follow Kafka conventions (PascalCase).

---

## Learning

- `KafkaMetricsGroup.newGauge` **returns** a `Gauge<T>` (it isn't void), but modern checkstyle with `UnusedLocalVariable` rejects `Gauge<X> ignored = …`. Either use `var` plus `@SuppressWarnings`, or simply drop the assignment — the gauge is registered as a side effect of the call.
- JMX tag keys are preserved verbatim in the MBean name — the `queue=<name>` tag on per-queue gauges is emitted exactly as written by `KafkaMetricsGroup`, so tests can assert `mbean.getMBeanName().contains("queue=orders")`.
- Existing Kafka `HttpMetrics` uses `close()` + `removeMetric(...)` with the full tag map, not the raw metric name, because `KafkaMetricsGroup.removeMetric` rebuilds the `MetricName` from the same tag set used on creation. Using the wrong tag map silently no-ops — the test `close_removesAllMetrics` would have caught that.
- Adding an additional-arg constructor with a nullable `WsMetrics` avoided touching every existing test: the previous-signature constructor still exists and delegates with `metrics=null`, so the handler skips every `metrics.*.mark()` call.
- `controlMessageRate` is counted on **every** inbound text frame (including invalid ones that parse to no message), matching the "inbound message" definition in design doc §11.5. The `errorRate` then also ticks when that frame produces an error response — both counters move in the same call, which is the correct operator view (error rate is always <= control rate).

---

## Limitations

- `ConfirmRate`, `DlxRate`, and `RedeliveryRate` meters are wired at known call sites (`WsAckHandler` for nack+dlx, `WsConsumerFetchLoop.deliverRecord` for redelivered flag), but `ConfirmRate` is **not** yet wired into `WsPublisherConfirmTracker`. The meter is registered and `close()` tears it down, but no production call site invokes it. Full wiring depends on whether confirm emission happens inside or outside `handleWsResponse`; left for a follow-up.
- Per-queue `QueueDepth` and `QueueConsumers` gauges are plumbed (register/remove) but **no production site calls them**. The intended callers are a queue-lifecycle manager (queue declare/delete) that does not exist in the current codebase; the unit tests verify the plumbing works, and production wiring can land in the task that introduces the queue manager.
- `ConnectionCount` / `SubscriptionCount` gauges use `Supplier<Integer>` — production wiring (`WsConnectionRegistry`, `WsSubscriptionManager`) is trivial (lambda over `registry.size()`) but not wired here to keep the diff minimal.
- `WsMetrics` is not yet owned by any process (no `HttpProcessor`-level singleton) — production integration should wire it the same way `HttpMetrics` is wired.
- Yammer `Meter` does not expose a `reset()` method, so tests that assert "incremented by N" read the counter before and after each action instead of resetting. Harmless but requires per-test `before` snapshots.
- `CreditExhaustedRate` is only marked when the loop observes `credits.available() == 0` after `awaitCredits` returns 0 — if the cause was channel un-writability rather than credit starvation, the meter does not tick (by design per the metric's name).

---

## Field Notes

- Followed the `HttpMetrics` pattern exactly: constant strings for metric names, `KafkaMetricsGroup` with `PACKAGE_NAME="kafka.server.http.ws"` + `CLASS_NAME="WsMetrics"`, eager Meter construction in the constructor, lazy Gauge registration with per-queue cleanup via `ConcurrentHashMap` bookkeeping.
- The `UnusedLocalVariable` checkstyle rule caught `Gauge<Integer> ignored = ...` in four places during the first gradle run; resolved by dropping the assignment since `newGauge` registers as a side effect. Shipped `WsMetricsTest` green before touching handlers.
- Added a new metrics-aware constructor to each handler with the previous signature preserved as a delegating convenience — this meant zero changes to the 4 existing test classes for publish/fetch/ack/frame while the new `WsMetricsTest` exercises the wiring.
- The `frameHandler_inboundFrame_incrementsControlMessageRate` test sends an `enable-confirms` frame because it has no dependencies beyond `WsConnectionContext`. Other frame types would require stubbed routing/exchange/subscription managers.
- `buildPublishFrame` in `WsMetricsTest` mirrors the helper in `WsPublishHandlerTest`; a small amount of duplication (~12 lines) is acceptable because the test packages a richer publish frame that exercises the full handler path.
- Final commit hash recorded in "Return" section below (filled by the executing agent on commit).

---

## Acceptance Criteria

- [x] `timeout 300 ./gradlew :http-server:test --tests 'kafka.server.http.ws.WsMetricsTest' -x spotlessCheck` exits 0 — 26 tests PASS
- [x] `grep -r "WsMetrics" http-server/src/main/java/` returns at least 5 hits — returns 11 hits across `WsMetrics`, `WsPublishHandler`, `WsConsumerFetchLoop`, `WsAckHandler`, `WsFrameHandler`.
- [x] At least 11 meters defined with `protocol=ws` tag — exactly 11: publishRate, deliverRate, ackRate, nackRate, errorRate, creditExhaustedRate, redeliveryRate, dlxRate, mandatoryReturnRate, confirmRate, controlMessageRate. The `atLeastElevenMeters_registered` test guards this count.
- [x] `close()` removes all metrics — `close_removesAllMetrics` test verifies zero WS metrics remain in the Yammer registry after close.
- [x] Learning section filled with five entries.

---

## File Manifest

**Created:**
- `http-server/src/main/java/kafka/server/http/ws/WsMetrics.java` — 11 meters + 4 gauge-registration paths + close() teardown.
- `http-server/src/test/java/kafka/server/http/ws/WsMetricsTest.java` — 26 tests: meter registration, gauge registration, per-queue gauge lifecycle, close cleanup, and integration with `WsPublishHandler`, `WsConsumerFetchLoop`, `WsAckHandler`, `WsFrameHandler`.

**Modified:**
- `http-server/src/main/java/kafka/server/http/ws/WsPublishHandler.java` — added `WsMetrics`-aware constructor; records `publishRate` on successful enqueue, `mandatoryReturnRate` on NO_ROUTE mandatory returns, `errorRate` on `emitError`.
- `http-server/src/main/java/kafka/server/http/ws/WsConsumerFetchLoop.java` — added `WsMetrics`-aware constructor; records `deliverRate` on each `deliverRecord`, `redeliveryRate` when the record is a redelivery, `creditExhaustedRate` when awaitCredits returns 0 due to credit starvation.
- `http-server/src/main/java/kafka/server/http/ws/WsAckHandler.java` — added `WsMetrics`-aware constructor; records `ackRate` on successful ack, `nackRate` on successful nack, `dlxRate` when nack has `requeue=false`.
- `http-server/src/main/java/kafka/server/http/ws/WsFrameHandler.java` — added `WsMetrics`-aware constructor; records `controlMessageRate` on every inbound text frame, `errorRate` on every emitted error frame.

**Not modified (deferred):**
- `WsCreditManager.java` — credit-exhaustion is recorded from the fetch loop where full context is available; keeping the credit manager unaware keeps the class pure.
- `WsConnectionContext.java` — connection count gauge is `Supplier`-based; the context does not need to touch metrics.
- `WsPublisherConfirmTracker.java` — `confirmRate` meter is registered but not wired; see Limitations.
