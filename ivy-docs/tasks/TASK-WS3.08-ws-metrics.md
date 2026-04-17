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

_To be filled by the executing agent._

---

## Limitations

_To be filled by the executing agent._

---

## Field Notes

_To be filled by the executing agent._

---

## Acceptance Criteria

- [ ] `timeout 300 ./gradlew :http-server:test --tests 'kafka.server.http.ws.WsMetricsTest' -x spotlessCheck` exits 0
- [ ] `grep -r "WsMetrics" http-server/src/main/java/` returns at least 5 hits (used in multiple handlers)
- [ ] At least 11 meters defined with protocol=ws tag
- [ ] close() removes all metrics
- [ ] Learning section filled with at least one entry

---

## File Manifest

_To be filled by the executing agent._
