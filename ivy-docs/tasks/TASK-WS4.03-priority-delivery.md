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

_To be filled by the executing agent._

---

## Limitations

_To be filled by the executing agent._

---

## Field Notes

_To be filled by the executing agent._

---

## Acceptance Criteria

- [ ] `timeout 300 ./gradlew :http-server:test --tests 'kafka.server.http.ws.WsPriorityDeliveryTest' -x spotlessCheck` exits 0
- [ ] `grep -r "_ws_priority" http-server/src/main/java/` returns at least 2 hits
- [ ] Priority sorting conditional on queue config
- [ ] Learning section filled with at least one entry

---

## File Manifest

_To be filled by the executing agent._
