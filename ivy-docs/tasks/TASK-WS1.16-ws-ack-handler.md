# TASK-WS1.16: WsAckHandler — ACK/NACK Processing and Offset Commit Batching

## Prerequisites

| Task | What it delivers | Why this task needs it |
|------|------------------|-----------------------|
| TASK-WS1.13 | `WsDeliveryTagTracker` | ACK handler resolves delivery tags to (tp, offset) via the tracker |
| TASK-WS1.15 | `WsSubscriptionManager` | ACK handler looks up the subscription context to find the correct tracker |

---

## Context

When a WebSocket client sends `ack` or `nack` frames, the `WsAckHandler` resolves the delivery tag to a `(TopicPartition, offset)` pair and manages offset commits. This is the consumer-side counterpart to the delivery tag assignment in `WsConsumerFetchLoop`.

From **design doc §13.4**:

> ACK: resolve deliveryTag → (tp, offset) via WsDeliveryTagTracker, batch offset commits via GroupCoordinator.commitOffsets()

> Batch commit optimization: offsets are flushed every `ws.ack.commit.interval.ms` (default 1000ms), on unsubscribe, and on WebSocket close.

From **design doc §5.9** (edge cases):
- Unknown tag → PRECONDITION_FAILED error response
- Double-ack → idempotent (ignored)
- Ack after unsubscribe → ignored

NACK behavior from **design doc §12.2-12.3**:
- `requeue=true`: offset NOT committed, fetch loop re-fetches from last committed offset
- `requeue=false` with DLX: trigger dead-letter publish (§12.3), then commit original offset
- `requeue=false` without DLX: commit offset (message lost)

The ivy-ref `Amqp091AckHandler` (lines 174-250) shows the established pattern for cumulative vs single ack, requeue tracking, and DLX routing.

---

## Specification

```java
package kafka.server.http.ws;

import org.apache.kafka.common.TopicPartition;
import java.util.Map;

/**
 * Handles ack and nack JSON frames for WebSocket subscriptions.
 * Batches offset commits for efficiency.
 */
public final class WsAckHandler {

    /**
     * @param subscriptionManager  subscription lookup
     * @param commitIntervalMs     how often to flush offset commits (default 1000ms)
     */
    public WsAckHandler(WsSubscriptionManager subscriptionManager,
                        long commitIntervalMs);

    /**
     * Handles an ack frame.
     *
     * @param subscriptionId  the subscription that acked
     * @param deliveryTag     the tag being acknowledged
     * @param multiple        if true, ack all tags up to and including deliveryTag
     * @return null on success, or an error message string
     */
    public String handleAck(String subscriptionId, long deliveryTag, boolean multiple);

    /**
     * Handles a nack frame.
     *
     * @param subscriptionId  the subscription that nacked
     * @param deliveryTag     the tag being negatively acknowledged
     * @param requeue         if true, message will be redelivered; if false, DLX or discard
     * @param multiple        if true, nack all tags up to and including deliveryTag
     * @return null on success, or an error message string
     */
    public String handleNack(String subscriptionId, long deliveryTag,
                             boolean requeue, boolean multiple);

    /**
     * Flushes all pending offset commits immediately.
     * Called on unsubscribe and connection close.
     *
     * @return committable offsets per partition
     */
    public Map<TopicPartition, Long> flush();

    /**
     * Starts the periodic commit timer.
     */
    public void start();

    /**
     * Stops the periodic commit timer and flushes remaining offsets.
     */
    public void stop();
}
```

---

## Implementation Details

**Module:** `http-server`

**Files to study:**

| File | Why |
|------|-----|
| `references/ivy-ref/ivy-server/src/main/java/com/ivy/server/handler/amqp091/Amqp091AckHandler.java` lines 174-250 | ACK/NACK pattern to follow |
| `ivy-docs/http-protocol-extend-design.md` §13.4 | Offset commit with gaps |
| `ivy-docs/http-protocol-extend-design.md` §12.2 | NACK with requeue |
| `ivy-docs/http-protocol-extend-design.md` §12.3 | NACK without requeue — DLX |
| `ivy-docs/http-protocol-extend-design.md` §5.9 | Edge cases |

```java
// From Amqp091AckHandler.java lines 174-199 — ACK pattern to follow:
private void handleBasicAck(ChannelHandlerContext ctx, ByteBuf buf) {
    AmqpFrame frame = Amqp091Codec.parseFrame(buf);
    try {
        AmqpConfirmData data = Amqp091Codec.decodeBasicAck(frame.payload());
        if (data.multiple()) {
            for (long tag : Set.copyOf(unackedDeliveries)) {
                if (tag <= data.deliveryTag()) {
                    unackedDeliveries.remove(tag);
                    ackedDeliveries.add(tag);
                    pendingDeliveries.remove(tag);
                }
            }
        } else {
            unackedDeliveries.remove(data.deliveryTag());
            ackedDeliveries.add(data.deliveryTag());
            pendingDeliveries.remove(data.deliveryTag());
        }
    } finally {
        frame.close();
        ReferenceCountUtil.safeRelease(buf);
    }
}
```

**Files to create:**

| File | What it does |
|------|--------------|
| `http-server/src/main/java/kafka/server/http/ws/WsAckHandler.java` | ACK/NACK processing with batched offset commits |
| `http-server/src/test/java/kafka/server/http/ws/WsAckHandlerTest.java` | Unit tests |

**Files to modify:**

None.

> **CRITICAL:** The commit interval timer must be a `ScheduledExecutorService` task, not a Thread.sleep loop. Use single-threaded `ScheduledExecutorService` with `scheduleAtFixedRate`.

> **CRITICAL:** `handleNack` with `requeue=false` must trigger DLX publish BEFORE committing the original offset (atomicity invariant from §12.3). For this task, the DLX publish is a TODO placeholder — actual DLX integration will be wired later. But the commit must happen only after DLX (or skip if no DLX).

> **EDGE CASE:** Unknown tag → return "PRECONDITION_FAILED: unknown delivery tag". Double-ack → silently succeed (idempotent). Ack on unknown subscription → return error.

**Implementation order:**
1. Create `WsAckHandler` with subscription manager reference and commit interval
2. Implement `handleAck()` — single and multiple modes
3. Implement `handleNack()` — requeue vs discard/DLX
4. Implement batched commit timer with `ScheduledExecutorService`
5. Implement `flush()` — collect committable offsets from all subscriptions
6. Implement `start()` / `stop()` lifecycle
7. Write tests

---

## Skeleton Code

### Production class

```java
package kafka.server.http.ws;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Handles ACK/NACK frames and batches offset commits.
 *
 * // Time: Created - TASK-WS1.16
 */
public final class WsAckHandler {

    private static final Logger log = LoggerFactory.getLogger(WsAckHandler.class);

    private final WsSubscriptionManager subscriptionManager;
    private final long commitIntervalMs;
    private final ScheduledExecutorService scheduler;
    private volatile ScheduledFuture<?> commitTask;

    public WsAckHandler(WsSubscriptionManager subscriptionManager, long commitIntervalMs) {
        this.subscriptionManager = Objects.requireNonNull(subscriptionManager);
        this.commitIntervalMs = commitIntervalMs;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "ws-ack-commit-timer");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Handles an ack frame. Returns null on success, or error message.
     */
    public String handleAck(String subscriptionId, long deliveryTag, boolean multiple) {
        SubscriptionContext ctx = subscriptionManager.getSubscription(subscriptionId);
        if (ctx == null) {
            return "PRECONDITION_FAILED: unknown subscription " + subscriptionId;
        }

        WsDeliveryTagTracker tracker = ctx.deliveryTagTracker();
        boolean success;
        if (multiple) {
            success = tracker.ackMultiple(deliveryTag);
        } else {
            success = tracker.ack(deliveryTag);
        }

        if (!success) {
            // Unknown tag — could be already acked (idempotent) or truly unknown
            log.debug("ACK for unknown/already-acked tag: sub={} tag={}", subscriptionId, deliveryTag);
        }
        return null;
    }

    /**
     * Handles a nack frame. Returns null on success, or error message.
     */
    public String handleNack(String subscriptionId, long deliveryTag,
                             boolean requeue, boolean multiple) {
        SubscriptionContext ctx = subscriptionManager.getSubscription(subscriptionId);
        if (ctx == null) {
            return "PRECONDITION_FAILED: unknown subscription " + subscriptionId;
        }

        WsDeliveryTagTracker tracker = ctx.deliveryTagTracker();

        if (multiple) {
            // NACK multiple not yet supported — nack tags individually
            // For now, nack just the specified tag
            WsDeliveryTagTracker.PendingDelivery pd = tracker.nack(deliveryTag, requeue);
            if (pd == null) {
                log.debug("NACK for unknown tag: sub={} tag={}", subscriptionId, deliveryTag);
            } else if (!requeue) {
                handleDlxOrDiscard(ctx, pd);
            }
        } else {
            WsDeliveryTagTracker.PendingDelivery pd = tracker.nack(deliveryTag, requeue);
            if (pd == null) {
                log.debug("NACK for unknown tag: sub={} tag={}", subscriptionId, deliveryTag);
            } else if (!requeue) {
                handleDlxOrDiscard(ctx, pd);
            }
        }
        return null;
    }

    /**
     * Handle DLX publish or discard for nack-without-requeue.
     * DLX integration is wired in a later task — this is the hook point.
     */
    private void handleDlxOrDiscard(SubscriptionContext ctx,
                                     WsDeliveryTagTracker.PendingDelivery pd) {
        // TODO: Check if queue has DLX configured
        // If DLX: publish to DLX exchange, then offset is committed normally
        // If no DLX: offset is committed (message lost)
        log.debug("NACK without requeue: sub={} tp={} offset={} (DLX not yet wired)",
            ctx.subscriptionId(), pd.topicPartition(), pd.offset());
    }

    /**
     * Flushes all pending offset commits.
     */
    public Map<TopicPartition, Long> flush() {
        Map<TopicPartition, Long> allOffsets = new HashMap<>();
        // Iterate all active subscriptions and collect committable offsets
        // The actual commit to __consumer_offsets will be done by the caller
        // This method just collects what CAN be committed
        return allOffsets;
    }

    /**
     * Starts the periodic commit timer.
     */
    public void start() {
        if (commitIntervalMs > 0) {
            commitTask = scheduler.scheduleAtFixedRate(
                this::periodicCommit,
                commitIntervalMs, commitIntervalMs, TimeUnit.MILLISECONDS);
            log.debug("ACK commit timer started: interval={}ms", commitIntervalMs);
        }
    }

    /**
     * Stops the periodic commit timer and flushes remaining offsets.
     */
    public void stop() {
        if (commitTask != null) {
            commitTask.cancel(false);
        }
        flush();
        scheduler.shutdown();
        log.debug("ACK handler stopped");
    }

    private void periodicCommit() {
        try {
            Map<TopicPartition, Long> offsets = flush();
            if (!offsets.isEmpty()) {
                // TODO: Call GroupCoordinator.commitOffsets() with collected offsets
                log.debug("Periodic commit: {} partitions", offsets.size());
            }
        } catch (Exception e) {
            log.error("Periodic commit failed", e);
        }
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
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * // Time: Created - TASK-WS1.16
 */
class WsAckHandlerTest {

    private WsSubscriptionManager subscriptionManager;
    private WsAckHandler ackHandler;
    private Channel channel;
    private final TopicPartition tp0 = new TopicPartition("ws.orders", 0);

    @BeforeEach
    void setUp() {
        channel = mock(Channel.class);
        when(channel.isWritable()).thenReturn(true);
        when(channel.isOpen()).thenReturn(true);

        subscriptionManager = new WsSubscriptionManager(Executors.newSingleThreadExecutor());
        subscriptionManager.subscribe("sub-1", "orders", "ws.orders",
            Set.of(tp0), Map.of(tp0, 0L), 100, false, channel);

        ackHandler = new WsAckHandler(subscriptionManager, 1000);
    }

    @Test
    void handleAck_singleTag_succeeds() {
        // Assign a delivery tag first
        SubscriptionContext ctx = subscriptionManager.getSubscription("sub-1");
        ctx.deliveryTagTracker().assign(tp0, 100);

        String error = ackHandler.handleAck("sub-1", 1, false);
        assertNull(error);
    }

    @Test
    void handleAck_unknownSubscription_returnsError() {
        String error = ackHandler.handleAck("nonexistent", 1, false);
        assertNotNull(error);
        assertTrue(error.contains("PRECONDITION_FAILED"));
    }

    @Test
    void handleAck_multiple_acksRange() {
        SubscriptionContext ctx = subscriptionManager.getSubscription("sub-1");
        ctx.deliveryTagTracker().assign(tp0, 100);
        ctx.deliveryTagTracker().assign(tp0, 101);
        ctx.deliveryTagTracker().assign(tp0, 102);

        String error = ackHandler.handleAck("sub-1", 2, true);
        assertNull(error);
        assertEquals(1, ctx.deliveryTagTracker().pendingCount()); // tag 3 still pending
    }

    @Test
    void handleNack_withRequeue_doesNotCommit() {
        SubscriptionContext ctx = subscriptionManager.getSubscription("sub-1");
        ctx.deliveryTagTracker().assign(tp0, 100);

        String error = ackHandler.handleNack("sub-1", 1, true, false);
        assertNull(error);

        // The offset should NOT be committable (it's nacked with requeue)
        Map<TopicPartition, Long> offsets = ctx.deliveryTagTracker().getCommittableOffsets();
        assertTrue(offsets.isEmpty());
    }

    @Test
    void handleNack_withoutRequeue_allowsCommit() {
        SubscriptionContext ctx = subscriptionManager.getSubscription("sub-1");
        ctx.deliveryTagTracker().assign(tp0, 100);

        String error = ackHandler.handleNack("sub-1", 1, false, false);
        assertNull(error);

        Map<TopicPartition, Long> offsets = ctx.deliveryTagTracker().getCommittableOffsets();
        assertEquals(101L, offsets.get(tp0)); // offset+1 committed
    }

    @Test
    void handleNack_unknownSubscription_returnsError() {
        String error = ackHandler.handleNack("nonexistent", 1, true, false);
        assertNotNull(error);
        assertTrue(error.contains("PRECONDITION_FAILED"));
    }

    @Test
    void handleAck_doubleAck_isIdempotent() {
        SubscriptionContext ctx = subscriptionManager.getSubscription("sub-1");
        ctx.deliveryTagTracker().assign(tp0, 100);

        assertNull(ackHandler.handleAck("sub-1", 1, false));
        assertNull(ackHandler.handleAck("sub-1", 1, false)); // idempotent
    }

    @Test
    void startAndStop_lifecycle() {
        ackHandler.start();
        ackHandler.stop();
        // No exception means lifecycle is clean
    }
}
```

### Existing pattern reference

```java
// From Amqp091AckHandler.java lines 200-233 — NACK pattern to follow:
private void handleBasicNack(ChannelHandlerContext ctx, ByteBuf buf) {
    AmqpFrame frame = Amqp091Codec.parseFrame(buf);
    try {
        AmqpBasicNackData data = Amqp091Codec.decodeBasicNack(frame.payload());
        if (data.multiple()) {
            for (long tag : Set.copyOf(unackedDeliveries)) {
                if (tag <= data.deliveryTag()) {
                    unackedDeliveries.remove(tag);
                    nackedDeliveries.add(tag);
                    if (data.requeue()) {
                        requeuedDeliveries.add(tag);
                    } else {
                        routeToDlx(tag, "nack");
                    }
                }
            }
        } else {
            unackedDeliveries.remove(data.deliveryTag());
            nackedDeliveries.add(data.deliveryTag());
            if (data.requeue()) {
                requeuedDeliveries.add(data.deliveryTag());
            } else {
                routeToDlx(data.deliveryTag(), "nack");
            }
        }
    } finally {
        frame.close();
        ReferenceCountUtil.safeRelease(buf);
    }
}
```

---

## Tests

**Test class:** `http-server/src/test/java/kafka/server/http/ws/WsAckHandlerTest.java`

| Test method | What it verifies |
|-------------|-----------------|
| `handleAck_singleTag_succeeds` | Single ack resolves correctly |
| `handleAck_unknownSubscription_returnsError` | Unknown subscription → error |
| `handleAck_multiple_acksRange` | Range ack clears tags |
| `handleNack_withRequeue_doesNotCommit` | Requeue prevents commit |
| `handleNack_withoutRequeue_allowsCommit` | Discard allows commit |
| `handleNack_unknownSubscription_returnsError` | Unknown subscription → error |
| `handleAck_doubleAck_isIdempotent` | Double ack is no-op |
| `startAndStop_lifecycle` | Clean start/stop |

**Run command:**
```bash
cd /home/anh/kafka && ./gradlew :http-server:test --tests 'kafka.server.http.ws.WsAckHandlerTest'
```

---

## Rules

- Batch commit interval configurable via `ws.ack.commit.interval.ms` (default 1000ms)
- Unknown tag → PRECONDITION_FAILED (§5.9)
- Double-ack → idempotent (§5.9)
- NACK without requeue + DLX → DLX publish before commit (§12.3 atomicity invariant)
- ScheduledExecutorService for commit timer, not Thread.sleep

---

## Learning

- `WsDeliveryTagTracker` exposes `ack`/`ackMultiple`/`nack` as state-transition APIs. Distinguishing "unknown tag" (PRECONDITION_FAILED per §5.9) from "already-acked tag" (idempotent success per §5.9) requires knowing the range of assigned tags. Added a `currentTagCounter()` accessor to the tracker; single-range check `tag >= 1 && tag <= currentTagCounter()` is the precise test.
- The `OffsetCommitSink` pattern (injected functional interface) keeps the handler testable without pulling in a `RequestChannel` mock. Tests use a `RecordingSink` that captures commit calls and assert on the resulting watermark sequence.
- Batched commits vs flush-on-ack is a single mode toggle via `commitIntervalMs`: `0` = flush-each, `>0` = scheduled flush. Both paths go through the same `flushSubscription` helper so behaviour is uniform and easy to reason about.
- `WsSubscriptionManager` needed an `activeSubscriptionIds()` accessor so the scheduled flusher can iterate without reaching into the internal map; returned a `Set.copyOf` snapshot for concurrent safety.
- For `nack(multiple=true)` the tracker only exposes single-tag nack, so we iterate `[1, deliveryTag]`. Pre-validating the range via `isTagWithinAssignedRange` avoids walking the loop for an unknown tag before surfacing the error.
- The tracker's ack-bitmap guarantees that `nack(requeue=true)` holds the commit watermark back (offset is `NACKED_REQUEUE`, not terminal), so the redelivery hook can remain a log-only placeholder until the fetch loop exposes an active redeliver API: the fetch loop re-reads from the uncommitted offset on its next iteration anyway.

---

## Limitations

- **RequestChannel wiring is deferred.** The `OffsetCommitSink` functional interface is a placeholder for the eventual `OffsetCommit` path through the broker's group coordinator. Integration with `RequestChannel` will be wired when the WebSocket frame handler (WS1.04) and full fetch-loop integration are in place.
- **Redelivery trigger is passive.** `nack(requeue=true)` currently relies on the fetch loop re-reading the uncommitted offset on its next fetch iteration; no active redeliver call is made because `WsConsumerFetchLoop` does not yet expose such a hook. Semantics are still correct (offset stays uncommitted), only the timing is delayed by one fetch cycle.
- **DLX path is a placeholder.** `nack(requeue=false)` currently logs and lets the tracker's `NACKED_DISCARD` state advance the watermark — the message is effectively dropped. Wiring to the DLX exchange publish is deferred to TASK-WS4.01.
- **`multiple=true` nack is O(n) in the tag range.** For very large fan-out subscriptions a smarter bulk-nack on the tracker would help, but single-range scans are bounded by the number of in-flight (unacked) records which is credit-limited anyway.

---

## Field Notes

- Checkstyle caught one unused import on the first compile — worth running `:http-server:checkstyleTest` as part of the tight feedback loop when authoring new tests.
- `@Timeout(5)` on the batched-commit test uses a 50ms interval and polls with a 10ms sleep up to 2s — this is generous enough for CI and tight enough for local dev.
- The concurrency test drives 500 tags across 8 threads; it completes in well under a second locally and gives good confidence that the lock in the tracker serialises mutations correctly under contention.

---

## Acceptance Criteria

- [ ] `cd /home/anh/kafka && ./gradlew :http-server:test --tests 'kafka.server.http.ws.WsAckHandlerTest'` exits 0
- [ ] `grep -r "WsAckHandler" http-server/src/main/java/` returns at least 1 hit
- [ ] Single ack, multiple ack, nack with requeue, nack without requeue all tested
- [ ] Commit timer starts and stops cleanly
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
