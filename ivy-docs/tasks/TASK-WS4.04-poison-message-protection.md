# TASK-WS4.04: Poison Message Protection

## Prerequisites

| Task | What it delivers | Why this task needs it |
|------|------------------|-----------------------|
| TASK-WS1.12 | WsConsumerFetchLoop | Delivery count incremented on each redelivery |
| TASK-WS4.01 | WsDeadLetterHandler | Auto-DLX when max retries exceeded |

---

## Context

The design doc §12.6 specifies poison message protection. A message that repeatedly fails processing (consumer NACKs with `requeue: true` every time) can cause an infinite redelivery loop. The broker tracks delivery count via the `_ws_delivery_count` header and auto-dead-letters when the count exceeds `ws.max.redelivery.count` (default 10).

**Flow:**
1. Message delivered with `_ws_delivery_count: 1`
2. Consumer NACKs with requeue=true
3. Message redelivered with `_ws_delivery_count: 2`
4. ... repeat until count reaches `ws.max.redelivery.count`
5. On next NACK with requeue=true, broker auto-DLXs instead of requeuing
6. x-death reason = "max-retries-exceeded"

The per-queue `x-max-retries` argument can override the global `ws.max.redelivery.count`.

---

## Specification

### Delivery count tracking

```java
// On each redelivery in WsConsumerFetchLoop:
int deliveryCount = getDeliveryCount(record);
deliveryCount++;
setDeliveryCount(record, deliveryCount);

// On NACK with requeue=true:
if (deliveryCount >= maxRetries) {
    deadLetterHandler.deadLetter(record, queueName, "max-retries-exceeded");
} else {
    // Normal requeue — do not commit offset
}
```

---

## Implementation Details

**Module:** `http-server`

**Files to modify:**

| File | What changes |
|------|--------------|
| `http-server/src/main/java/kafka/server/http/ws/WsConsumerFetchLoop.java` | Read and increment `_ws_delivery_count` header |
| `http-server/src/main/java/kafka/server/http/ws/WsAckHandler.java` | On NACK requeue=true, check delivery count vs max retries |
| `http-server/src/main/java/kafka/server/http/ws/WsMessageSerializer.java` | Write updated `_ws_delivery_count` header |

> **CRITICAL:** The delivery count header `_ws_delivery_count` must be incremented BEFORE checking max retries. First delivery = count 1, not 0.

> **CRITICAL:** When auto-DLXing, the x-death reason is "max-retries-exceeded" (not "rejected").

> **CRITICAL:** If no DLX configured on the queue and max retries exceeded, commit offset (message lost). Log a warning.

**Implementation order:**
1. Read `_ws_delivery_count` from record headers on delivery
2. Increment count and include in deliver frame
3. On NACK requeue=true, check count against max retries
4. If exceeded, auto-DLX with reason "max-retries-exceeded"
5. Add unit tests

---

## Skeleton Code

```java
// In WsConsumerFetchLoop — delivery count tracking

private int getDeliveryCount(ConsumerRecord<byte[], byte[]> record) {
    Header h = record.headers().lastHeader("_ws_delivery_count");
    if (h == null) return 0;
    return Integer.parseInt(new String(h.value(), StandardCharsets.UTF_8));
}

// In WsAckHandler — NACK requeue=true path

private void handleNackRequeue(ConsumerRecord<byte[], byte[]> record,
                                String queueName, int deliveryCount) {
    int maxRetries = getMaxRetries(queueName); // queue x-max-retries or global default
    if (deliveryCount >= maxRetries) {
        deadLetterHandler.deadLetter(record.key(), record.value(),
            record.headers(), queueName, "max-retries-exceeded");
    } else {
        // Normal requeue: do not commit offset
    }
}
```

---

## Tests

**Test class:** `http-server/src/test/java/kafka/server/http/ws/WsPoisonMessageProtectionTest.java`

| Test method | What it verifies |
|-------------|-----------------|
| `deliveryCount_incrementsOnRedelivery` | Count goes from 1 to 2 on redelivery |
| `maxRetries_exceeded_autoDlx` | Auto-DLX when count >= max retries |
| `maxRetries_notExceeded_requeued` | Normal requeue when below max |
| `autoDlx_reasonIsMaxRetriesExceeded` | x-death reason = "max-retries-exceeded" |
| `perQueueMaxRetries_overridesGlobal` | Queue x-max-retries used instead of global |
| `noDlx_maxRetriesExceeded_discards` | No DLX → discard, commit offset, log warning |
| `firstDelivery_countIsOne` | First delivery has count = 1 |
| `deliveryCountHeader_persistsAcrossRedeliveries` | Header value persists in Kafka record |

**Run command:**
```bash
timeout 300 ./gradlew :http-server:test --tests 'kafka.server.http.ws.WsPoisonMessageProtectionTest' -x spotlessCheck
```

---

## Rules

- First delivery count = 1 (not 0).
- Auto-DLX when `deliveryCount >= maxRetries`.
- x-death reason: "max-retries-exceeded".
- Per-queue `x-max-retries` overrides global `ws.max.redelivery.count`.
- No DLX + max retries exceeded → discard message, commit offset, log warning.

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

- [ ] `timeout 300 ./gradlew :http-server:test --tests 'kafka.server.http.ws.WsPoisonMessageProtectionTest' -x spotlessCheck` exits 0
- [ ] `grep -r "_ws_delivery_count" http-server/src/main/java/` returns at least 2 hits
- [ ] Auto-DLX triggered when delivery count exceeds max retries
- [ ] Learning section filled with at least one entry

---

## File Manifest

_To be filled by the executing agent._
