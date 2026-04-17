# TASK-WS1.13: WsDeliveryTagTracker — Per-Subscription Delivery Tag and Offset Commit Tracking

## Prerequisites

| Task | What it delivers | Why this task needs it |
|------|------------------|-----------------------|
| None | — | This is a standalone data structure with no upstream dependencies |

---

## Context

When the broker pushes messages to a WebSocket subscriber, each message gets a monotonically increasing **delivery tag** (starting from 1). The subscriber acknowledges or rejects messages by referencing these tags. The broker must track which delivery tags map to which `(TopicPartition, offset)` so that ACKs can be translated to Kafka offset commits.

The critical correctness invariant comes from **design doc §13.4 (Offset commit with gaps)**:

> When `multiple=true` ACK encounters a gap (a NACKed-with-requeue tag within the range), the offset commit advances only to the **lowest uncommitted offset** per partition. The broker maintains a per-partition **ack bitmap** — only contiguous acked offsets from the low watermark are committed. This prevents a `multiple` ACK from skipping over a requeued message.

Example from the design doc:

```
Delivered: tag=1 (offset=40), tag=2 (offset=41), tag=3 (offset=42),
           tag=4 (offset=43), tag=5 (offset=44)

Client NACKs tag=3 with requeue=true (offset 42 not committed)
Client ACKs tag=5 with multiple=true

Offset commit: tp0 → 42 (NOT 45!)
  Tags 1,2 → committed (offsets 40,41 < 42)
  Tag 3 → gap (offset 42, requeued, not committed)
  Tags 4,5 → acked in memory, but offsets 43,44 NOT committed yet

After tag=3 is redelivered and acked:
  Offset commit: tp0 → 45 (gap filled, safe to advance past all 5)
```

Thread safety is required: the consumer fetch loop assigns delivery tags (on wsConsumerExecutor thread), while ACK/NACK frames arrive on the Netty event loop.

The ivy-ref `Amqp091AckHandler` (lines 174-250) shows the ack/nack pattern — cumulative vs single, requeue tracking, and DLX routing on nack-without-requeue. This task adapts that pattern to Kafka offset semantics.

---

## Specification

```java
package kafka.server.http.ws;

import org.apache.kafka.common.TopicPartition;
import java.util.Map;

/**
 * Tracks delivery tags for a single WebSocket subscription.
 *
 * Delivery tags are per-subscription, monotonically increasing starting from 1.
 * Each tag maps to a (TopicPartition, offset) pair. The tracker maintains an
 * ack bitmap per partition to support offset commit with gaps.
 *
 * Thread-safe: accessed from consumer fetch loop (assign) and Netty event loop (ack/nack).
 */
public final class WsDeliveryTagTracker {

    /**
     * Assigns a delivery tag for a record at the given partition and offset.
     *
     * @param tp     the topic partition
     * @param offset the record offset
     * @return the assigned delivery tag (monotonically increasing from 1)
     */
    public long assign(TopicPartition tp, long offset);

    /**
     * Acknowledges a single delivery tag.
     * Unknown tag → returns false. Double-ack → idempotent (returns true).
     * Ack after clear → ignored (returns false).
     *
     * @param deliveryTag the tag to acknowledge
     * @return true if the tag was known and acknowledged
     */
    public boolean ack(long deliveryTag);

    /**
     * Acknowledges all delivery tags up to and including the given tag.
     * Tags that were NACKed-with-requeue are skipped (gap preserved).
     * Unknown upToTag → returns false.
     *
     * @param upToTag acknowledge all tags from 1..upToTag
     * @return true if upToTag was a known tag
     */
    public boolean ackMultiple(long upToTag);

    /**
     * Negatively acknowledges a delivery tag.
     *
     * @param deliveryTag the tag to nack
     * @param requeue     if true, the offset is NOT committed (will be redelivered);
     *                    if false, the offset is marked for commit (message discarded or DLX'd)
     * @return the PendingDelivery for the nacked tag, or null if unknown
     */
    public PendingDelivery nack(long deliveryTag, boolean requeue);

    /**
     * Returns the committable offsets: for each partition, the highest contiguous
     * offset that can be safely committed (lowest uncommitted offset).
     *
     * Only returns partitions where there are new offsets to commit since the last call.
     *
     * @return map of TopicPartition → offset+1 (the offset to commit, per Kafka convention)
     */
    public Map<TopicPartition, Long> getCommittableOffsets();

    /**
     * Clears all state. Called on unsubscribe or connection close.
     * After clear, ack/nack calls are ignored.
     */
    public void clear();

    /**
     * Returns the number of pending (unacked) deliveries.
     */
    public int pendingCount();

    /**
     * Immutable record representing a pending delivery.
     */
    public record PendingDelivery(TopicPartition topicPartition, long offset) {}
}
```

---

## Implementation Details

**Module:** `http-server`

**Files to study:**

| File | Why |
|------|-----|
| `references/ivy-ref/ivy-server/src/main/java/com/ivy/server/handler/amqp091/Amqp091AckHandler.java` lines 174-250 | ACK/NACK pattern — cumulative ack, requeue tracking |
| `ivy-docs/http-protocol-extend-design.md` §13.4 | Offset commit with gaps invariant |
| `ivy-docs/http-protocol-extend-design.md` §21.3 | Delivery tag memory bound |

```java
// From Amqp091AckHandler.java lines 174-199 — ACK pattern:
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
| `http-server/src/main/java/kafka/server/http/ws/WsDeliveryTagTracker.java` | Per-subscription delivery tag ↔ offset mapping with ack bitmap |
| `http-server/src/test/java/kafka/server/http/ws/WsDeliveryTagTrackerTest.java` | Unit tests |

**Files to modify:**

None.

> **CRITICAL:** The ack bitmap must track per-partition state. Use a `TreeMap<Long, Boolean>` (offset → acked) per partition, where the committable offset is found by scanning from the low watermark until the first un-acked offset. This ensures `multiple=true` ACK with a NACKed-with-requeue gap does NOT advance past the gap.

> **CRITICAL:** Thread safety — use `synchronized` on a private lock object (not `this`). The critical sections are short (map lookups and updates), so contention is minimal. Do NOT use `ConcurrentHashMap` for the tag-to-delivery map — the `ackMultiple` operation requires atomic iteration over a range.

> **EDGE CASE:** Double-ack must be idempotent (return true, no state change). Ack after clear must be silently ignored (return false).

**Implementation order:**
1. Create `PendingDelivery` record
2. Implement `assign()` — ConcurrentHashMap for tag→PendingDelivery, AtomicLong for tag counter
3. Implement per-partition ack bitmap (TreeMap<Long, AckState> per TopicPartition)
4. Implement `ack()` — single tag acknowledgment
5. Implement `ackMultiple()` — range ack with gap preservation
6. Implement `nack()` — mark as nacked, optionally mark for requeue
7. Implement `getCommittableOffsets()` — scan ack bitmap for contiguous committed range
8. Implement `clear()` — reset all state
9. Write tests

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
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Per-subscription delivery tag tracker with ack bitmap for gap-safe offset commits.
 *
 * // Time: Created - TASK-WS1.13
 */
public final class WsDeliveryTagTracker {

    private static final Logger log = LoggerFactory.getLogger(WsDeliveryTagTracker.class);

    public record PendingDelivery(TopicPartition topicPartition, long offset) {}

    private enum AckState { PENDING, ACKED, NACKED_REQUEUE, NACKED_DISCARD }

    private final AtomicLong tagCounter = new AtomicLong(0);
    private final Object lock = new Object();

    // tag → PendingDelivery
    private final Map<Long, PendingDelivery> pendingDeliveries = new HashMap<>();

    // partition → (offset → AckState) — TreeMap for ordered iteration
    private final Map<TopicPartition, TreeMap<Long, AckState>> partitionAckMaps = new HashMap<>();

    // partition → last committed offset (exclusive, Kafka convention)
    private final Map<TopicPartition, Long> lastCommittedOffsets = new HashMap<>();

    private volatile boolean cleared = false;

    /**
     * Assigns a delivery tag for a record at the given partition and offset.
     */
    public long assign(TopicPartition tp, long offset) {
        long tag = tagCounter.incrementAndGet();
        synchronized (lock) {
            if (cleared) return tag;
            pendingDeliveries.put(tag, new PendingDelivery(tp, offset));
            partitionAckMaps
                .computeIfAbsent(tp, k -> new TreeMap<>())
                .put(offset, AckState.PENDING);
        }
        return tag;
    }

    /**
     * Acknowledges a single delivery tag.
     */
    public boolean ack(long deliveryTag) {
        synchronized (lock) {
            if (cleared) return false;
            PendingDelivery delivery = pendingDeliveries.remove(deliveryTag);
            if (delivery == null) {
                // Check if already acked (idempotent)
                return false;
            }
            TreeMap<Long, AckState> ackMap = partitionAckMaps.get(delivery.topicPartition());
            if (ackMap != null) {
                AckState current = ackMap.get(delivery.offset());
                if (current == AckState.PENDING) {
                    ackMap.put(delivery.offset(), AckState.ACKED);
                }
                // Already ACKED = idempotent
            }
            return true;
        }
    }

    /**
     * Acknowledges all delivery tags up to and including upToTag.
     */
    public boolean ackMultiple(long upToTag) {
        synchronized (lock) {
            if (cleared) return false;
            boolean found = false;
            // Iterate all pending deliveries with tag <= upToTag
            var iterator = pendingDeliveries.entrySet().iterator();
            while (iterator.hasNext()) {
                var entry = iterator.next();
                if (entry.getKey() <= upToTag) {
                    found = true;
                    PendingDelivery delivery = entry.getValue();
                    TreeMap<Long, AckState> ackMap = partitionAckMaps.get(delivery.topicPartition());
                    if (ackMap != null) {
                        AckState current = ackMap.get(delivery.offset());
                        // Skip NACKED_REQUEUE entries (gap preservation)
                        if (current == AckState.PENDING) {
                            ackMap.put(delivery.offset(), AckState.ACKED);
                        }
                    }
                    iterator.remove();
                }
            }
            return found || upToTag <= tagCounter.get();
        }
    }

    /**
     * Negatively acknowledges a delivery tag.
     */
    public PendingDelivery nack(long deliveryTag, boolean requeue) {
        synchronized (lock) {
            if (cleared) return null;
            PendingDelivery delivery = pendingDeliveries.remove(deliveryTag);
            if (delivery == null) return null;
            TreeMap<Long, AckState> ackMap = partitionAckMaps.get(delivery.topicPartition());
            if (ackMap != null) {
                ackMap.put(delivery.offset(),
                    requeue ? AckState.NACKED_REQUEUE : AckState.NACKED_DISCARD);
            }
            return delivery;
        }
    }

    /**
     * Returns committable offsets: per partition, the highest contiguous acked offset + 1.
     */
    public Map<TopicPartition, Long> getCommittableOffsets() {
        Map<TopicPartition, Long> result = new HashMap<>();
        synchronized (lock) {
            if (cleared) return result;
            for (var entry : partitionAckMaps.entrySet()) {
                TopicPartition tp = entry.getKey();
                TreeMap<Long, AckState> ackMap = entry.getValue();
                long lastCommitted = lastCommittedOffsets.getOrDefault(tp, Long.MIN_VALUE);

                // Find the highest contiguous acked offset from the low watermark
                long committableOffset = -1;
                for (var offsetEntry : ackMap.entrySet()) {
                    long offset = offsetEntry.getKey();
                    AckState state = offsetEntry.getValue();

                    if (state == AckState.ACKED || state == AckState.NACKED_DISCARD) {
                        committableOffset = offset + 1; // Kafka commits offset+1
                    } else {
                        // Gap found (PENDING or NACKED_REQUEUE) — stop here
                        break;
                    }
                }

                if (committableOffset > 0 && committableOffset > lastCommitted) {
                    result.put(tp, committableOffset);
                    lastCommittedOffsets.put(tp, committableOffset);

                    // Clean up acked entries below the new commit point
                    ackMap.headMap(committableOffset).clear();
                }
            }
        }
        return result;
    }

    /**
     * Clears all state. After clear, ack/nack calls are ignored.
     */
    public void clear() {
        synchronized (lock) {
            cleared = true;
            pendingDeliveries.clear();
            partitionAckMaps.clear();
            lastCommittedOffsets.clear();
        }
    }

    /**
     * Returns the number of pending (unacked) deliveries.
     */
    public int pendingCount() {
        synchronized (lock) {
            return pendingDeliveries.size();
        }
    }
}
```

### Test class

```java
package kafka.server.http.ws;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * // Time: Created - TASK-WS1.13
 */
class WsDeliveryTagTrackerTest {

    private WsDeliveryTagTracker tracker;
    private final TopicPartition tp0 = new TopicPartition("ws.orders", 0);
    private final TopicPartition tp1 = new TopicPartition("ws.orders", 1);

    @BeforeEach
    void setUp() {
        tracker = new WsDeliveryTagTracker();
    }

    @Test
    void assign_returnsMonotonicallyIncreasingTags() {
        long tag1 = tracker.assign(tp0, 100);
        long tag2 = tracker.assign(tp0, 101);
        long tag3 = tracker.assign(tp1, 50);
        assertEquals(1, tag1);
        assertEquals(2, tag2);
        assertEquals(3, tag3);
        assertEquals(3, tracker.pendingCount());
    }

    @Test
    void ack_singleTag_removesFromPending() {
        long tag = tracker.assign(tp0, 100);
        assertTrue(tracker.ack(tag));
        assertEquals(0, tracker.pendingCount());
    }

    @Test
    void ack_unknownTag_returnsFalse() {
        assertFalse(tracker.ack(999));
    }

    @Test
    void ack_doubleAck_isIdempotent() {
        long tag = tracker.assign(tp0, 100);
        assertTrue(tracker.ack(tag));
        assertFalse(tracker.ack(tag)); // already acked
    }

    @Test
    void ackMultiple_acksAllTagsUpToGiven() {
        tracker.assign(tp0, 100);
        tracker.assign(tp0, 101);
        tracker.assign(tp0, 102);
        assertTrue(tracker.ackMultiple(2));
        assertEquals(1, tracker.pendingCount()); // tag 3 still pending
    }

    @Test
    void ackMultiple_withGap_preservesNackedRequeue() {
        // Design doc §13.4 scenario
        tracker.assign(tp0, 40); // tag 1
        tracker.assign(tp0, 41); // tag 2
        tracker.assign(tp0, 42); // tag 3
        tracker.assign(tp0, 43); // tag 4
        tracker.assign(tp0, 44); // tag 5

        // NACK tag 3 with requeue
        tracker.nack(3, true);

        // ACK multiple up to tag 5
        tracker.ackMultiple(5);

        // Committable: only up to offset 42 (not 45!)
        Map<TopicPartition, Long> offsets = tracker.getCommittableOffsets();
        assertEquals(42L, offsets.get(tp0)); // offset 42 is the gap
    }

    @Test
    void nack_withRequeue_createsGap() {
        long tag = tracker.assign(tp0, 100);
        WsDeliveryTagTracker.PendingDelivery pd = tracker.nack(tag, true);
        assertNotNull(pd);
        assertEquals(tp0, pd.topicPartition());
        assertEquals(100, pd.offset());

        // Offset should NOT be committable
        Map<TopicPartition, Long> offsets = tracker.getCommittableOffsets();
        assertTrue(offsets.isEmpty());
    }

    @Test
    void nack_withoutRequeue_allowsCommit() {
        long tag = tracker.assign(tp0, 100);
        tracker.nack(tag, false); // discard (DLX or lost)

        Map<TopicPartition, Long> offsets = tracker.getCommittableOffsets();
        assertEquals(101L, offsets.get(tp0)); // offset+1 per Kafka convention
    }

    @Test
    void nack_unknownTag_returnsNull() {
        assertNull(tracker.nack(999, true));
    }

    @Test
    void getCommittableOffsets_contiguousAcks() {
        tracker.assign(tp0, 100); // tag 1
        tracker.assign(tp0, 101); // tag 2
        tracker.assign(tp0, 102); // tag 3
        tracker.ack(1);
        tracker.ack(2);
        // tag 3 still pending

        Map<TopicPartition, Long> offsets = tracker.getCommittableOffsets();
        assertEquals(102L, offsets.get(tp0)); // can commit up to 102 (exclusive)
    }

    @Test
    void getCommittableOffsets_multiplePartitions() {
        tracker.assign(tp0, 10); // tag 1
        tracker.assign(tp1, 20); // tag 2
        tracker.ack(1);
        tracker.ack(2);

        Map<TopicPartition, Long> offsets = tracker.getCommittableOffsets();
        assertEquals(11L, offsets.get(tp0));
        assertEquals(21L, offsets.get(tp1));
    }

    @Test
    void getCommittableOffsets_returnsEmpty_whenNothingNew() {
        tracker.assign(tp0, 100);
        tracker.ack(1);
        tracker.getCommittableOffsets(); // consume the offset

        Map<TopicPartition, Long> offsets = tracker.getCommittableOffsets();
        assertTrue(offsets.isEmpty()); // nothing new to commit
    }

    @Test
    void clear_resetsAllState() {
        tracker.assign(tp0, 100);
        tracker.clear();
        assertEquals(0, tracker.pendingCount());
        assertFalse(tracker.ack(1)); // ignored after clear
    }

    @Test
    void ackAfterClear_isIgnored() {
        long tag = tracker.assign(tp0, 100);
        tracker.clear();
        assertFalse(tracker.ack(tag));
        assertNull(tracker.nack(tag, true));
    }

    @Test
    void gapFilled_advancesCommitOffset() {
        // Full §13.4 scenario with gap resolution
        tracker.assign(tp0, 40); // tag 1
        tracker.assign(tp0, 41); // tag 2
        tracker.assign(tp0, 42); // tag 3
        tracker.assign(tp0, 43); // tag 4
        tracker.assign(tp0, 44); // tag 5

        tracker.nack(3, true);  // gap at offset 42
        tracker.ackMultiple(5);

        Map<TopicPartition, Long> offsets1 = tracker.getCommittableOffsets();
        assertEquals(42L, offsets1.get(tp0));

        // Redelivery: tag 3 was requeued, now redelivered as tag 6
        tracker.assign(tp0, 42); // tag 6 (redelivered)
        tracker.ack(6);

        Map<TopicPartition, Long> offsets2 = tracker.getCommittableOffsets();
        assertEquals(45L, offsets2.get(tp0)); // gap filled, advance to end
    }
}
```

### Existing pattern reference

```java
// From Amqp091AckHandler.java lines 200-233 — NACK pattern:
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

**Test class:** `http-server/src/test/java/kafka/server/http/ws/WsDeliveryTagTrackerTest.java`

| Test method | What it verifies |
|-------------|-----------------|
| `assign_returnsMonotonicallyIncreasingTags` | Tags start at 1 and increment |
| `ack_singleTag_removesFromPending` | Single ack removes delivery |
| `ack_unknownTag_returnsFalse` | Unknown tag returns false |
| `ack_doubleAck_isIdempotent` | Double ack does not throw |
| `ackMultiple_acksAllTagsUpToGiven` | Range ack clears all tags in range |
| `ackMultiple_withGap_preservesNackedRequeue` | §13.4 gap invariant |
| `nack_withRequeue_createsGap` | Requeued nack blocks commit |
| `nack_withoutRequeue_allowsCommit` | Discarded nack allows commit |
| `nack_unknownTag_returnsNull` | Unknown tag returns null |
| `getCommittableOffsets_contiguousAcks` | Contiguous acks produce correct offset |
| `getCommittableOffsets_multiplePartitions` | Multi-partition tracking |
| `getCommittableOffsets_returnsEmpty_whenNothingNew` | No double-report |
| `clear_resetsAllState` | Clear empties everything |
| `ackAfterClear_isIgnored` | Post-clear ops are no-ops |
| `gapFilled_advancesCommitOffset` | Full §13.4 gap-fill scenario |

**Run command:**
```bash
cd /home/anh/kafka && ./gradlew :http-server:test --tests 'kafka.server.http.ws.WsDeliveryTagTrackerTest'
```

---

## Rules

- Thread-safe: accessed from consumer fetch loop + Netty event loop for ack/nack (design doc §21.3)
- No unbounded collections: credits naturally bound pending acks (design doc §21.3 — max ~2MB per connection)
- Per-partition ack bitmap for gap-safe offset commits (design doc §13.4)
- No `ConcurrentHashMap` for the tag map — `ackMultiple` needs atomic range iteration

---

## Learning

- The §13.4 "gap-fill" invariant has a subtle redelivery wrinkle: after a `NACK(requeue=true)` marks an offset as `NACKED_REQUEUE`, the caller will eventually redeliver that same offset under a new tag. The ack bitmap must promote the existing `NACKED_REQUEUE` entry back to `PENDING` at assign time, or else the later `ack()` finds a non-PENDING state and refuses to transition it to `ACKED`, leaving the gap forever. Resolved via a `Map.merge` on `assign`: only `NACKED_REQUEUE → PENDING` transitions are allowed; terminal states (`ACKED`, `NACKED_DISCARD`) are preserved to catch double-delivery by the caller.
- `TreeMap.headMap(k).clear()` is exclusive of `k` — convenient for pruning "committed" entries because the Kafka commit offset `k = lastOffset + 1` is *itself* not yet observed, so pruning everything strictly below `k` is correct and keeps the watermark computation O(pending) rather than O(all-time).
- Idempotence semantics in the spec: `ack(tag)` returns `false` on the *second* call because the tag is no longer in `pendingDeliveries`. The "idempotent" guarantee is state-level (no corruption, no exception), not return-level. This matches `Map.remove` semantics and mirrors `Amqp091AckHandler.unackedDeliveries.remove`.
- A single `synchronized (lock)` block keeps `pendingDeliveries`, `partitionAckMaps`, and `lastCommittedOffsets` in a consistent view during `ackMultiple` and `getCommittableOffsets`. The spec explicitly rules out `ConcurrentHashMap` for `pendingDeliveries` because atomic range iteration is required. The `cleared` flag is volatile purely as a fast-path signal; the lock still serialises all mutations.
- The concurrency smoke tests exercise the realistic threading model (one assigner, many ack-ers) and verified the final watermark equals `lastOffset+1` after 500 parallel acks across 8 threads.

---

## Limitations

- Only the `NACKED_REQUEUE → PENDING` transition is allowed on re-assign. A caller that erroneously re-delivers an already-`ACKED` offset will silently leave the `ACKED` state unchanged and the new pending delivery will never clear the pending map (because `ack` for the new tag will find the offset already `ACKED`). This is defensive (we refuse to un-ack) but is not surfaced via an exception — the caller is expected to coordinate via the consumer fetch loop contract.
- Memory pruning happens inside `getCommittableOffsets`. If a caller never invokes that method, the ack bitmap grows unbounded. Design doc §21.3 bounds pending deliveries via credit flow-control, but the ack bitmap entries for `ACKED`/`NACKED_DISCARD` offsets linger until the next `getCommittableOffsets` call. Callers should poll at least once per ack batch.
- `assign` still increments the tag counter even after `clear()` — the tag returned is "detached" and will never match any later ack. This matches the spec ("After clear, ack/nack calls are ignored") and avoids a race between a concurrent assign and clear leaving the counter in an inconsistent state.
- No per-connection / per-subscription memory cap enforced here; that is the responsibility of the credit manager (TASK-WS1.14). This tracker assumes credits bound the live set.

---

## Field Notes

- The `feature/http-protocol` branch HEAD has a pre-existing scala compile break in `http-server/src/main/scala/kafka/network/HttpRequestHandler.scala` (references `HttpRouter.validateClientId`, `HttpRouter.HandlerType`, `HttpRequestTranslator.translateCommitOffsets`, `HttpResponseSerializer.serialize`, etc., which do not exist on the branch). This blocked `./gradlew :http-server:test` until I skipped scala explicitly: `-x compileScala -x compileTestScala`. Verified by checking out a fresh clone of `feature/http-protocol` into `/tmp` — the break is not mine. Flagging for a follow-up task or a sibling agent's fix.
- With `-x compileScala -x compileTestScala`, all 20 tests pass in ~0.05s wall (including two concurrency smoke tests with 8-thread executors and 500/200 tags respectively).
- Checkstyle passes on both main and test sources.
- The worktree started from an older upstream commit `f95a1f995d` (no `http-server` module). Reset to `feature/http-protocol` HEAD (`ee602708bd`) before writing code, as instructed by CLAUDE.md workflow.

---

## Acceptance Criteria

- [ ] `cd /home/anh/kafka && ./gradlew :http-server:test --tests 'kafka.server.http.ws.WsDeliveryTagTrackerTest'` exits 0
- [ ] `grep -r "WsDeliveryTagTracker" http-server/src/main/java/` returns at least 1 hit
- [ ] `grep -r "getCommittableOffsets" http-server/src/main/java/` returns at least 1 hit
- [ ] The §13.4 gap scenario test passes (ackMultiple with nacked-requeue gap commits only to lowest uncommitted)
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
