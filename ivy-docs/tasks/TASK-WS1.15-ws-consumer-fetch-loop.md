# TASK-WS1.15: WsConsumerFetchLoop and WsSubscriptionManager — WebSocket Push Delivery Engine

## Prerequisites

| Task | What it delivers | Why this task needs it |
|------|------------------|-----------------------|
| TASK-WS1.13 | `WsDeliveryTagTracker` | Fetch loop assigns delivery tags via tracker for each delivered record |
| TASK-WS1.14 | `WsCreditManager` | Fetch loop calls `awaitCredits()` before fetching and `consume()` after delivery |

---

## Context

The `WsConsumerFetchLoop` is the core engine for WebSocket push delivery. When a client sends a `subscribe` frame, the broker starts a fetch loop on a dedicated `wsConsumerExecutor` thread pool that continuously:

1. Waits for credits (`WsCreditManager.awaitCredits()`)
2. Builds a `FetchRequest` and submits to `RequestChannel`
3. On response, converts records to deliver frames
4. Writes deliver frames to the Netty channel
5. Assigns delivery tags via `WsDeliveryTagTracker`
6. Decrements credits

From **design doc §9 (Consume Path)**:

```
WsConsumerFetchLoop (consumer executor):
  loop:
    if credits <= 0: wait for credits msg
    budget = min(credits, maxFetchRecords)

    FetchRequest(partitions, offsets, maxWait=500ms, maxRecords=budget)
    → RequestChannel → KafkaApis
    ← FetchResponse with records

    for each record:
      deliveryTag = nextTag++
      pendingAcks[tag] = (tp, offset)
      credits--
      write deliver frame to WS channel
```

From **design doc §21.1**: `WsConsumerFetchLoop` runs on a dedicated `wsConsumerExecutor`, NOT on Netty workers. `channel.writeAndFlush()` from a non-Netty thread is safe — Netty schedules the write on the channel's event loop internally.

The `WsSubscriptionManager` manages the lifecycle of subscriptions per connection: register, cancel, clear-on-disconnect.

---

## Specification

```java
package kafka.server.http.ws;

import io.netty.channel.Channel;
import org.apache.kafka.common.TopicPartition;
import java.util.Map;
import java.util.Set;

/**
 * Manages all subscriptions for a single WebSocket connection.
 */
public final class WsSubscriptionManager {

    /**
     * Registers a new subscription and starts the fetch loop.
     *
     * @param subscriptionId  client-chosen subscription identifier
     * @param queueName       logical queue name
     * @param topic           resolved Kafka topic (ws.{queueName})
     * @param partitions      assigned partitions
     * @param startOffsets    starting offset per partition
     * @param initialCredits  initial credit count
     * @param noAck           if true, auto-ack (no delivery tags)
     * @param channel         the Netty channel for writing deliver frames
     */
    public void subscribe(String subscriptionId, String queueName, String topic,
                          Set<TopicPartition> partitions, Map<TopicPartition, Long> startOffsets,
                          int initialCredits, boolean noAck, Channel channel);

    /**
     * Cancels a subscription and stops its fetch loop.
     * Returns the committable offsets for final flush.
     */
    public Map<TopicPartition, Long> unsubscribe(String subscriptionId);

    /**
     * Grants additional credits to a subscription.
     */
    public void grantCredits(String subscriptionId, int credits);

    /**
     * Returns the subscription context, or null if not found.
     */
    public SubscriptionContext getSubscription(String subscriptionId);

    /**
     * Cancels all subscriptions (connection close).
     * Returns all uncommitted offsets for final commit.
     */
    public Map<TopicPartition, Long> cancelAll();

    /**
     * Returns active subscription count.
     */
    public int activeCount();
}

/**
 * Per-subscription state container.
 */
public final class SubscriptionContext {
    public String subscriptionId();
    public String queueName();
    public String topic();
    public WsDeliveryTagTracker deliveryTagTracker();
    public WsCreditManager creditManager();
    public WsConsumerFetchLoop fetchLoop();
}

/**
 * Runnable fetch loop per subscription, runs on wsConsumerExecutor.
 */
public final class WsConsumerFetchLoop implements Runnable {

    /**
     * @param subscriptionId subscription identifier
     * @param topic          Kafka topic to fetch from
     * @param partitions     assigned partitions with start offsets
     * @param creditManager  credit tracker
     * @param tagTracker     delivery tag tracker
     * @param channel        Netty channel for writing deliver frames
     * @param noAck          auto-ack mode
     */
    public WsConsumerFetchLoop(String subscriptionId, String topic,
                                Map<TopicPartition, Long> startOffsets,
                                WsCreditManager creditManager,
                                WsDeliveryTagTracker tagTracker,
                                Channel channel, boolean noAck);

    /** Stops the fetch loop gracefully. */
    public void stop();

    /** Returns true if the loop is still active. */
    public boolean isActive();
}
```

---

## Implementation Details

**Module:** `http-server`

**Files to study:**

| File | Why |
|------|-----|
| `ivy-docs/http-protocol-extend-design.md` §9 | Complete consume path flow |
| `ivy-docs/http-protocol-extend-design.md` §21.1 | CRITICAL: fetch loop must NOT block Netty |
| `ivy-docs/http-protocol-extend-design.md` §21.2 | Credit-based backpressure |
| `ivy-docs/http-protocol-extend-design.md` §12.4 | Per-message TTL check at delivery time |
| `references/ivy-ref/ivy-server/src/main/java/com/ivy/server/handler/amqp091/AmqpQueueDispatcher.java` lines 1-100 | Consumer dispatch pattern |

```java
// From AmqpQueueDispatcher.java lines 44-65 — consumer slot management:
public AmqpQueueDispatcher(String queueName, AtomicLong deliveryTagCounter) {
    this.queueName = Objects.requireNonNull(queueName, "queueName");
    this.deliveryTagCounter = Objects.requireNonNull(deliveryTagCounter, "deliveryTagCounter");
}

public void addConsumer(String consumerTag, short amqpChannel,
                        Channel nettyChannel, short prefetchCount) {
    consumers.add(new ConsumerSlot(consumerTag, amqpChannel, nettyChannel, prefetchCount));
    drainBuffers();
}

public boolean removeConsumer(String consumerTag) {
    // ... requeues unacked entries from removed consumer
}
```

**Files to create:**

| File | What it does |
|------|--------------|
| `http-server/src/main/java/kafka/server/http/ws/WsConsumerFetchLoop.java` | Per-subscription fetch loop running on wsConsumerExecutor |
| `http-server/src/main/java/kafka/server/http/ws/WsSubscriptionManager.java` | Per-connection subscription lifecycle management |
| `http-server/src/main/java/kafka/server/http/ws/SubscriptionContext.java` | Per-subscription state container |
| `http-server/src/test/java/kafka/server/http/ws/WsSubscriptionManagerTest.java` | Unit tests |
| `http-server/src/test/java/kafka/server/http/ws/WsConsumerFetchLoopTest.java` | Unit tests |

**Files to modify:**

None.

> **CRITICAL:** The fetch loop must run on `wsConsumerExecutor`, NEVER on Netty workers. This is a dedicated `ScheduledExecutorService` with `num.ws.consumer.threads` threads (default 8). Each subscription gets a `Runnable` scheduled on this pool.

> **CRITICAL:** `channel.writeAndFlush()` from a non-Netty thread is safe (Netty schedules internally). This is a documented Netty pattern. Do NOT use `channel.eventLoop().execute()` for every write.

> **EDGE CASE:** Per-message TTL check at delivery time (§12.4): if `_ws_expiration` header present and message expired, skip the record (do not deliver, do not assign delivery tag, but do advance offset).

> **GOTCHA:** The fetch loop currently uses a simplified direct-fetch approach. Full `RequestChannel → KafkaApis` integration will be wired in TASK-WS1.16 and integration tests. This task focuses on the loop structure, credit management, and delivery tag assignment.

**Implementation order:**
1. Create `SubscriptionContext` record
2. Create `WsConsumerFetchLoop` with the loop skeleton (credit wait → fetch → deliver → tag assign)
3. Create `WsSubscriptionManager` managing subscription lifecycle
4. Write unit tests with mocked components
5. Verify thread safety of subscribe/unsubscribe concurrent with fetch loop

---

## Skeleton Code

### SubscriptionContext

```java
package kafka.server.http.ws;

import java.util.Objects;

/**
 * Per-subscription state container.
 *
 * // Time: Created - TASK-WS1.15
 */
public final class SubscriptionContext {

    private final String subscriptionId;
    private final String queueName;
    private final String topic;
    private final WsDeliveryTagTracker deliveryTagTracker;
    private final WsCreditManager creditManager;
    private final WsConsumerFetchLoop fetchLoop;

    public SubscriptionContext(String subscriptionId, String queueName, String topic,
                                WsDeliveryTagTracker deliveryTagTracker,
                                WsCreditManager creditManager,
                                WsConsumerFetchLoop fetchLoop) {
        this.subscriptionId = Objects.requireNonNull(subscriptionId);
        this.queueName = Objects.requireNonNull(queueName);
        this.topic = Objects.requireNonNull(topic);
        this.deliveryTagTracker = Objects.requireNonNull(deliveryTagTracker);
        this.creditManager = Objects.requireNonNull(creditManager);
        this.fetchLoop = Objects.requireNonNull(fetchLoop);
    }

    public String subscriptionId() { return subscriptionId; }
    public String queueName() { return queueName; }
    public String topic() { return topic; }
    public WsDeliveryTagTracker deliveryTagTracker() { return deliveryTagTracker; }
    public WsCreditManager creditManager() { return creditManager; }
    public WsConsumerFetchLoop fetchLoop() { return fetchLoop; }
}
```

### WsConsumerFetchLoop

```java
package kafka.server.http.ws;

import io.netty.channel.Channel;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Per-subscription fetch loop. Runs on wsConsumerExecutor, NOT on Netty workers.
 *
 * // Time: Created - TASK-WS1.15
 */
public final class WsConsumerFetchLoop implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(WsConsumerFetchLoop.class);
    private static final long CREDIT_WAIT_TIMEOUT_MS = 500;
    private static final int MAX_FETCH_RECORDS = 100;

    private final String subscriptionId;
    private final String topic;
    private final Map<TopicPartition, Long> currentOffsets;
    private final WsCreditManager creditManager;
    private final WsDeliveryTagTracker tagTracker;
    private final Channel channel;
    private final boolean noAck;
    private final AtomicBoolean active = new AtomicBoolean(true);

    public WsConsumerFetchLoop(String subscriptionId, String topic,
                                Map<TopicPartition, Long> startOffsets,
                                WsCreditManager creditManager,
                                WsDeliveryTagTracker tagTracker,
                                Channel channel, boolean noAck) {
        this.subscriptionId = Objects.requireNonNull(subscriptionId);
        this.topic = Objects.requireNonNull(topic);
        this.currentOffsets = new ConcurrentHashMap<>(startOffsets);
        this.creditManager = Objects.requireNonNull(creditManager);
        this.tagTracker = Objects.requireNonNull(tagTracker);
        this.channel = Objects.requireNonNull(channel);
        this.noAck = noAck;
    }

    @Override
    public void run() {
        log.debug("Fetch loop started: sub={} topic={}", subscriptionId, topic);
        try {
            while (active.get() && channel.isOpen()) {
                // 1. Wait for credits
                int budget = creditManager.awaitCredits(CREDIT_WAIT_TIMEOUT_MS);
                if (budget <= 0) continue;
                budget = Math.min(budget, MAX_FETCH_RECORDS);

                // 2. Build and submit fetch request
                // TODO: Wire to RequestChannel for real fetch
                // For now, this is the structural skeleton.
                // The actual fetch integration will be completed when
                // WsFrameHandler wires everything together.

                // 3. On response, for each record:
                //    - Check TTL (skip expired records)
                //    - Assign delivery tag via tagTracker
                //    - Build deliver frame JSON
                //    - writeAndFlush to channel (thread-safe)
                //    - Consume credit

                // Placeholder: sleep to prevent busy-wait in skeleton
                if (active.get()) {
                    Thread.sleep(CREDIT_WAIT_TIMEOUT_MS);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            if (active.get()) {
                log.error("Fetch loop error: sub={}", subscriptionId, e);
            }
        } finally {
            log.debug("Fetch loop stopped: sub={}", subscriptionId);
        }
    }

    /**
     * Delivers a single record to the WebSocket channel.
     * Called from the fetch loop after receiving records.
     */
    void deliverRecord(TopicPartition tp, long offset, String exchange,
                       String routingKey, String messageJson, boolean redelivered) {
        long tag = noAck ? 0 : tagTracker.assign(tp, offset);

        String frame = String.format(
            "{\"type\":\"deliver\",\"subscriptionId\":\"%s\",\"deliveryTag\":%d," +
            "\"redelivered\":%b,\"exchange\":\"%s\",\"routingKey\":\"%s\"," +
            "\"message\":%s,\"partition\":%d,\"offset\":%d}",
            subscriptionId, tag, redelivered,
            exchange, routingKey, messageJson,
            tp.partition(), offset);

        channel.writeAndFlush(new TextWebSocketFrame(frame));
        creditManager.consume();

        currentOffsets.put(tp, offset + 1);
    }

    public void stop() {
        active.set(false);
    }

    public boolean isActive() {
        return active.get();
    }

    public Map<TopicPartition, Long> currentOffsets() {
        return Map.copyOf(currentOffsets);
    }
}
```

### WsSubscriptionManager

```java
package kafka.server.http.ws;

import io.netty.channel.Channel;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

/**
 * Manages all subscriptions for a single WebSocket connection.
 *
 * // Time: Created - TASK-WS1.15
 */
public final class WsSubscriptionManager {

    private static final Logger log = LoggerFactory.getLogger(WsSubscriptionManager.class);

    private final ConcurrentHashMap<String, SubscriptionContext> subscriptions = new ConcurrentHashMap<>();
    private final ExecutorService wsConsumerExecutor;

    public WsSubscriptionManager(ExecutorService wsConsumerExecutor) {
        this.wsConsumerExecutor = Objects.requireNonNull(wsConsumerExecutor);
    }

    public void subscribe(String subscriptionId, String queueName, String topic,
                          Set<TopicPartition> partitions, Map<TopicPartition, Long> startOffsets,
                          int initialCredits, boolean noAck, Channel channel) {
        if (subscriptions.containsKey(subscriptionId)) {
            throw new IllegalStateException("Subscription already exists: " + subscriptionId);
        }

        WsDeliveryTagTracker tagTracker = new WsDeliveryTagTracker();
        WsCreditManager creditManager = new WsCreditManager(initialCredits, channel);
        WsConsumerFetchLoop fetchLoop = new WsConsumerFetchLoop(
            subscriptionId, topic, startOffsets, creditManager, tagTracker, channel, noAck);

        SubscriptionContext ctx = new SubscriptionContext(
            subscriptionId, queueName, topic, tagTracker, creditManager, fetchLoop);
        subscriptions.put(subscriptionId, ctx);

        wsConsumerExecutor.submit(fetchLoop);
        log.debug("Subscription started: id={} queue={} topic={} credits={}",
            subscriptionId, queueName, topic, initialCredits);
    }

    public Map<TopicPartition, Long> unsubscribe(String subscriptionId) {
        SubscriptionContext ctx = subscriptions.remove(subscriptionId);
        if (ctx == null) return Map.of();

        ctx.fetchLoop().stop();
        Map<TopicPartition, Long> offsets = ctx.deliveryTagTracker().getCommittableOffsets();
        ctx.deliveryTagTracker().clear();
        ctx.creditManager().reset();

        log.debug("Subscription stopped: id={}", subscriptionId);
        return offsets;
    }

    public void grantCredits(String subscriptionId, int credits) {
        SubscriptionContext ctx = subscriptions.get(subscriptionId);
        if (ctx != null) {
            ctx.creditManager().grant(credits);
        }
    }

    public SubscriptionContext getSubscription(String subscriptionId) {
        return subscriptions.get(subscriptionId);
    }

    public Map<TopicPartition, Long> cancelAll() {
        Map<TopicPartition, Long> allOffsets = new HashMap<>();
        for (String subId : subscriptions.keySet()) {
            allOffsets.putAll(unsubscribe(subId));
        }
        return allOffsets;
    }

    public int activeCount() {
        return subscriptions.size();
    }
}
```

### Test class

```java
package kafka.server.http.ws;

import io.netty.channel.Channel;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * // Time: Created - TASK-WS1.15
 */
class WsSubscriptionManagerTest {

    private WsSubscriptionManager manager;
    private ExecutorService executor;
    private Channel channel;
    private final TopicPartition tp0 = new TopicPartition("ws.orders", 0);

    @BeforeEach
    void setUp() {
        executor = Executors.newSingleThreadExecutor();
        channel = mock(Channel.class);
        when(channel.isWritable()).thenReturn(true);
        when(channel.isOpen()).thenReturn(true);
        manager = new WsSubscriptionManager(executor);
    }

    @Test
    void subscribe_createsSubscription() {
        manager.subscribe("sub-1", "orders", "ws.orders",
            Set.of(tp0), Map.of(tp0, 0L), 100, false, channel);
        assertEquals(1, manager.activeCount());
        assertNotNull(manager.getSubscription("sub-1"));
    }

    @Test
    void subscribe_duplicateId_throwsIllegalState() {
        manager.subscribe("sub-1", "orders", "ws.orders",
            Set.of(tp0), Map.of(tp0, 0L), 100, false, channel);
        assertThrows(IllegalStateException.class, () ->
            manager.subscribe("sub-1", "orders", "ws.orders",
                Set.of(tp0), Map.of(tp0, 0L), 100, false, channel));
    }

    @Test
    void unsubscribe_stopsAndRemoves() {
        manager.subscribe("sub-1", "orders", "ws.orders",
            Set.of(tp0), Map.of(tp0, 0L), 100, false, channel);
        manager.unsubscribe("sub-1");
        assertEquals(0, manager.activeCount());
        assertNull(manager.getSubscription("sub-1"));
    }

    @Test
    void unsubscribe_unknownId_returnsEmptyMap() {
        Map<TopicPartition, Long> offsets = manager.unsubscribe("nonexistent");
        assertTrue(offsets.isEmpty());
    }

    @Test
    void grantCredits_forwardsToManager() {
        manager.subscribe("sub-1", "orders", "ws.orders",
            Set.of(tp0), Map.of(tp0, 0L), 10, false, channel);
        manager.grantCredits("sub-1", 50);
        assertEquals(60, manager.getSubscription("sub-1").creditManager().available());
    }

    @Test
    void cancelAll_stopsAllSubscriptions() {
        manager.subscribe("sub-1", "orders", "ws.orders",
            Set.of(tp0), Map.of(tp0, 0L), 100, false, channel);
        manager.subscribe("sub-2", "events", "ws.events",
            Set.of(new TopicPartition("ws.events", 0)),
            Map.of(new TopicPartition("ws.events", 0), 0L), 50, false, channel);

        manager.cancelAll();
        assertEquals(0, manager.activeCount());
    }
}
```

---

## Tests

**Test class:** `http-server/src/test/java/kafka/server/http/ws/WsSubscriptionManagerTest.java`

| Test method | What it verifies |
|-------------|-----------------|
| `subscribe_createsSubscription` | Subscription registered and fetch loop started |
| `subscribe_duplicateId_throwsIllegalState` | Duplicate subscription ID rejected |
| `unsubscribe_stopsAndRemoves` | Fetch loop stopped, context removed |
| `unsubscribe_unknownId_returnsEmptyMap` | No-op for unknown ID |
| `grantCredits_forwardsToManager` | Credits forwarded to subscription's credit manager |
| `cancelAll_stopsAllSubscriptions` | All subscriptions cancelled on disconnect |

**Test class:** `http-server/src/test/java/kafka/server/http/ws/WsConsumerFetchLoopTest.java`

| Test method | What it verifies |
|-------------|-----------------|
| `stop_setsInactive` | stop() terminates the loop |
| `deliverRecord_assignsTagAndWritesFrame` | Delivery produces correct JSON frame |
| `deliverRecord_noAck_skipsTagAssignment` | noAck mode uses tag 0 |

**Run command:**
```bash
cd /home/anh/kafka && ./gradlew :http-server:test --tests 'kafka.server.http.ws.WsSubscriptionManagerTest' --tests 'kafka.server.http.ws.WsConsumerFetchLoopTest'
```

---

## Rules

- Fetch loop runs on wsConsumerExecutor, NEVER on Netty workers (§21.1)
- channel.writeAndFlush() from non-Netty thread is safe (documented Netty pattern)
- Credit-based backpressure prevents OOM (§21.2)
- Per-message TTL check at delivery time (§12.4)

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

- [ ] `cd /home/anh/kafka && ./gradlew :http-server:test --tests 'kafka.server.http.ws.WsSubscriptionManagerTest'` exits 0
- [ ] `grep -r "WsConsumerFetchLoop" http-server/src/main/java/` returns at least 1 hit
- [ ] `grep -r "WsSubscriptionManager" http-server/src/main/java/` returns at least 1 hit
- [ ] Fetch loop can be stopped gracefully without blocking
- [ ] Learning section filled with at least one entry
- [ ] Limitations section filled (use "None" if truly none)
- [ ] File Manifest section updated after commit

---

## File Manifest

> Filled by the executing agent after each commit.
> Run: `git diff --name-status HEAD~1 HEAD -- '*.java' '*.xml' '*.json' '*.yaml' '*.yml'`

<!-- ### YYYY-MM-DD — <short description> (commit <hash>)
Created:
  - path/to/NewFile.java — <what it does>
Modified:
  - path/to/Existing.java — <what changed>
-->
