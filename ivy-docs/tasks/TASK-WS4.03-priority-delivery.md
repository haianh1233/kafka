# TASK-WS4.03: Priority Delivery Ordering

## Prerequisites

| Task | What it delivers | Why this task needs it |
|------|------------------|-----------------------|
| TASK-WS1.12 | WsConsumerFetchLoop | Priority sorting applied to fetch batch |
| TASK-WS1.15 | WsMessageSerializer | Writes _ws_priority header |

---

## Context

The design doc §12.5 specifies priority-aware delivery ordering. Queues declared with `x-max-priority: N` enable priority-sorted delivery. The `WsConsumerFetchLoop` fetches a batch of records, sorts them by priority (highest first), and delivers in priority order.

**Limitation:** Priority ordering is **within each fetch batch**, not globally across the entire queue. A low-priority record already in the batch will be delivered before a high-priority record that arrives in the next fetch.

```
Fetch batch (unsorted):
  offset=10, priority=3
  offset=11, priority=9
  offset=12, priority=5

Deliver order (sorted):
  deliveryTag=1, offset=11, priority=9  ← first
  deliveryTag=2, offset=12, priority=5
  deliveryTag=3, offset=10, priority=3  ← last
```

---

## Specification

### Priority sorting in WsConsumerFetchLoop

```java
private List<ConsumerRecord<byte[], byte[]>> sortByPriority(
        List<ConsumerRecord<byte[], byte[]>> records, int maxPriority) {
    if (maxPriority <= 0) return records;
    records.sort((a, b) -> {
        int pa = extractPriority(a);
        int pb = extractPriority(b);
        return Integer.compare(pb, pa); // descending: highest first
    });
    return records;
}

private int extractPriority(ConsumerRecord<byte[], byte[]> record) {
    Header h = record.headers().lastHeader("_ws_priority");
    if (h == null) return 0;
    return Integer.parseInt(new String(h.value(), StandardCharsets.UTF_8));
}
```

---

## Implementation Details

**Module:** `http-server`

**Files to modify:**

| File | What changes |
|------|--------------|
| `http-server/src/main/java/kafka/server/http/ws/WsConsumerFetchLoop.java` | Sort fetch batch by _ws_priority before delivery |
| `http-server/src/main/java/kafka/server/http/ws/WsMessageSerializer.java` | Write _ws_priority header from publish message.priority |
| `http-server/src/main/java/kafka/server/http/ws/WsMessageDeserializer.java` | Read _ws_priority for deliver frame |

> **CRITICAL:** Only sort when queue has `x-max-priority > 0`. For queues without priority, skip sorting (preserve offset order).

> **CRITICAL:** Priority value clamped to [0, x-max-priority]. Values above max are capped.

> **CRITICAL:** Messages without a priority header default to priority 0 (lowest).

**Implementation order:**
1. Ensure _ws_priority header written on publish
2. Add priority sorting logic to WsConsumerFetchLoop (conditional on queue config)
3. Add unit tests

---

## Skeleton Code

```java
// In WsConsumerFetchLoop — add after fetching records, before delivering

private List<ConsumerRecord<byte[], byte[]>> maybeSortByPriority(
        List<ConsumerRecord<byte[], byte[]>> records) {
    if (queueMaxPriority <= 0) return records;
    List<ConsumerRecord<byte[], byte[]>> sorted = new ArrayList<>(records);
    sorted.sort((a, b) -> Integer.compare(extractPriority(b), extractPriority(a)));
    return sorted;
}

private int extractPriority(ConsumerRecord<byte[], byte[]> record) {
    Header h = record.headers().lastHeader("_ws_priority");
    if (h == null) return 0;
    int p = Integer.parseInt(new String(h.value(), StandardCharsets.UTF_8));
    return Math.min(p, queueMaxPriority);
}
```

---

## Tests

**Test class:** `http-server/src/test/java/kafka/server/http/ws/WsPriorityDeliveryTest.java`

| Test method | What it verifies |
|-------------|-----------------|
| `priorityQueue_highestDeliveredFirst` | Priority 9 delivered before priority 3 |
| `priorityQueue_samePriority_offsetOrder` | Equal priority preserves offset order |
| `noPriorityQueue_offsetOrder` | Non-priority queue delivers in offset order |
| `missingPriorityHeader_defaultsToZero` | No header → priority 0 |
| `priorityExceedsMax_capped` | Priority above x-max-priority capped |
| `batchLocalOrdering` | Priority ordering is per-fetch-batch only |
| `priorityHeader_roundTrip` | _ws_priority written on publish, read on fetch |

**Run command:**
```bash
timeout 300 ./gradlew :http-server:test --tests 'kafka.server.http.ws.WsPriorityDeliveryTest' -x spotlessCheck
```

---

## Rules

- Sort by priority only when queue has `x-max-priority > 0`.
- Descending order: highest priority first.
- Missing priority header defaults to 0.
- Priority capped at x-max-priority.
- Ordering is batch-local only — not global across the queue.
- Same-priority records preserve offset order (stable sort).

---

## Learning

- **Header plumbing already existed.** `_ws_priority` was plumbed end-to-end by WS1.09 / WS1.10 (`WsMessageSerializer.HDR_PRIORITY` writes it, `WsMessageDeserializer` reads it back into `message.priority` on deliver frames). All that was missing was the batch sort + the `x-max-priority` toggle on the fetch loop. Keeping this task additive (no signature changes to existing `deliverRecord` / `maybeDeliverRecord`) meant WS1.15 / WS4.02 tests stayed green untouched.
- **Mirrored the WsMessageTtl shape for WsPriority.** Two sibling pure utilities (`WsMessageTtl` + `WsPriority`) both live off the same serializer header constants, expose a `fromQueueArgs` parser, and stay side-effect-free so the fetch loop owns the downstream action (skip vs. sort). This avoids a god class on the loop and keeps each helper trivially unit-testable.
- **Stable descending sort via index decoration.** `Collections.sort` is documented stable but index-based decoration + `Integer[]` sort is explicit about the tiebreaker (input index) and dodges the comparator-contract traps that come with `Integer.compare(b,a)`-style "reverse" comparators. Pre-computing priorities into an `int[]` once also keeps the sort at O(n log n) comparisons without re-parsing `_ws_priority` bytes per comparison.
- **Clamp-first semantics match the spec’s "fail open" theme.** Missing / malformed / negative headers all collapse to priority 0, and values above `x-max-priority` are capped — same defensive stance as the TTL helper and the rest of the WS stack. Tests explicitly assert each edge case so downstream re-users cannot regress this quietly.
- **Priority ordering is deliberately batch-local.** The spec (§12.5) and skeleton both call this out, and the test `batchLocalOrdering` pins the guarantee down: once a batch is sorted and dispatched, a later fetch that brings a higher-priority record does NOT preempt already-dispatched lower-priority deliveries. Any future per-queue "global priority" is out of scope and would require a different fetch strategy (separate partition ranges per priority, design doc §15.8).

---

## Limitations

- **Ordering is per-fetch-batch, not global.** A low-priority record fetched in batch N will be delivered before a high-priority record that arrives in batch N+1. Global priority ordering would require partition-per-priority or an in-memory priority queue across batches (design doc §15.8 hints at the partition-per-priority model but that is out of scope here).
- **Fetch-integration wiring is still open.** The hook `WsConsumerFetchLoop.doFetchIteration(int)` remains a no-op pending the Kafka fetch integration. When WS1.16 (or follow-up) wires real `ConsumerRecord<byte[], byte[]>` batches, the integration should:
    1. Convert the fetched records to a mutable `List<ConsumerRecord<...>>`.
    2. Call `loop.maybeSortByPriority(records, rec -> rec.headers())`.
    3. Iterate the sorted list and call `maybeDeliverRecord(...)` per record.
  The helper accepts any `Function<T, Iterable<Header>>` so no extra adapter is needed — tests use a tiny `PriRec` POJO to prove that.
- **Priority value range is clamped to `[0, 255]`.** AMQP `priority` is an octet, so `x-max-priority` values above 255 cap at 255 and negatives collapse to "non-priority". Going beyond that would break the over-the-wire frame contract and is not exposed.
- **No metric emission for priority reordering.** Unlike TTL (which records a drop rate), priority sorting does not currently emit a metric. Easy to add later via `WsMetrics` if operators want visibility into how often priority actually moves a record.
- **No change to existing `WsConsumerFetchLoop` constructors.** The new `int queueMaxPriority` parameter has two defaults in the metrics-aware and non-metrics-aware constructors (both default to 0 = non-priority). Callers opt into priority sorting explicitly via either the full 9-arg constructor or the `withMaxPriority` static factory, which keeps existing call sites (WS1.15 tests, WS4.02 tests) binary-compatible.

---

## Field Notes

- **RED → GREEN:** Wrote `WsPriorityDeliveryTest` first; initial compile failure produced ~38 symbol-lookup errors (as expected — no `WsPriority`, no `maybeSortByPriority`, no `withMaxPriority`). Added `WsPriority` utility + minimal constructor/factory + `maybeSortByPriority` on the loop → 30/30 green first run.
- **Regression sweep:** re-ran `WsConsumerFetchLoopTest` (13 tests) and `WsMessageTtlTest` (19 tests) alongside the new suite — all 62 tests green. No existing test required any edit; added code is purely additive (new utility class, new constructor overload, new static factory, new instance method).
- **Acceptance pre-flight:**
    - `grep -r "_ws_priority" http-server/src/main/java/` → **3 hits** (`WsMessageSerializer.java`, `WsMessageDeserializer.java` via HDR_PRIORITY constant + Javadoc). Meets "≥ 2".
    - Priority sort gated on `queueMaxPriority > 0` — non-priority queues return the input list unchanged (same reference), verified by `noPriorityQueue_offsetOrder`.
    - `timeout 300 ./gradlew :http-server:test --tests 'kafka.server.http.ws.WsPriorityDeliveryTest' -x spotlessCheck` → BUILD SUCCESSFUL.
- **Design checkpoint:** The task skeleton used `ConsumerRecord<byte[],byte[]>` directly. I generalized to a `Function<T, Iterable<Header>>` because (a) the fetch loop currently does not see `ConsumerRecord` yet (WS1.16 isn’t wired), (b) tests can use plain POJOs, and (c) the production call site can pass `ConsumerRecord::headers` verbatim when integration lands. This avoids forcing the loop to depend on `kafka-clients` internals for a header accessor.

---

## File Manifest

**Added:**
- `http-server/src/main/java/kafka/server/http/ws/WsPriority.java` — pure utility: `extractPriority`, `maxPriorityFromQueueArgs`, stable descending `sortByPriority`, `HDR_PRIORITY` / `QUEUE_ARG_X_MAX_PRIORITY` / `MAX_PRIORITY_CAP` constants.
- `http-server/src/test/java/kafka/server/http/ws/WsPriorityDeliveryTest.java` — 30 tests covering `WsPriority` semantics + loop integration (`priorityQueue_highestDeliveredFirst`, `priorityQueue_samePriority_offsetOrder`, `noPriorityQueue_offsetOrder`, `missingPriorityHeader_defaultsToZero`, `priorityExceedsMax_capped`, `batchLocalOrdering`, `priorityHeader_roundTrip`, plus edge cases: null/malformed/empty/duplicate headers, single/empty batches, etc.).

**Modified:**
- `http-server/src/main/java/kafka/server/http/ws/WsConsumerFetchLoop.java` — added `queueMaxPriority` field, new 9-arg constructor, `withMaxPriority(...)` static factory, `queueMaxPriority()` accessor, and generic `maybeSortByPriority(List<T>, Function<T, Iterable<Header>>)` that delegates to `WsPriority.sortByPriority`. Existing constructors preserved; they now default `queueMaxPriority` to 0.

**Unchanged but verified for regressions:**
- `http-server/src/main/java/kafka/server/http/ws/WsMessageSerializer.java` — already writes `_ws_priority` on publish.
- `http-server/src/main/java/kafka/server/http/ws/WsMessageDeserializer.java` — already reads `_ws_priority` into `message.priority` on deliver.
- `http-server/src/test/java/kafka/server/http/ws/WsConsumerFetchLoopTest.java` — 13 tests, still green.
- `http-server/src/test/java/kafka/server/http/ws/WsMessageTtlTest.java` — 19 tests, still green.

---

## Acceptance Criteria

- [x] `timeout 300 ./gradlew :http-server:test --tests 'kafka.server.http.ws.WsPriorityDeliveryTest' -x spotlessCheck` exits 0
- [x] `grep -r "_ws_priority" http-server/src/main/java/` returns at least 2 hits (3 hits observed)
- [x] Priority sorting conditional on queue config (`queueMaxPriority <= 0` → no-op, list returned unchanged)
- [x] Learning section filled with at least one entry

---

## File Manifest

_To be filled by the executing agent._
