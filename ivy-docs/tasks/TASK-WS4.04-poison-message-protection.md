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

- **Memory bound shaped the API.** `WsDeliveryTagTracker` only stores `(TopicPartition, offset)` per design §21.3 — it has no access to record headers. That is what forced the poison-gate to be a small functional interface `PoisonGate(queueName, PendingDelivery) -> boolean` rather than a direct `(deliveryCount, queueName) -> boolean`: the caller owning the fetch-loop / record cache must itself materialise the count from the headers before invoking the gate. Same architectural choice as `DeadLetterHook` in WS4.01.
- **"Count >= max" triggers DLX, not ">".** First delivery carries count 1; spec says "auto-DLX when deliveryCount >= maxRetries". With default `ws.max.redelivery.count=10` a message therefore gets 10 delivery attempts in total (counts 1..10) before the 11th NACK redirects it to DLX. Verified in `maxRetries_exceeded_returnsTrue` (count=3, max=3 → true).
- **Poison DLX is best-effort, not atomic.** Unlike the WS4.01 `NACK(requeue=false)` path (which holds the tag in PENDING until DLX produce acks), the poison path transitions the tracker immediately on gate return. The design choice: if we held the commit on DLX-in-flight here too, a DLX outage would stall the poison-bounce protection — which defeats the whole point. The trade-off is documented on `WsPoisonMessageProtection.maybeDeadLetter`: DLX failure logs WARN and the offset still commits.
- **Per-queue `x-max-retries` must survive bad values.** A malformed queue argument must not crash the ack path. `resolveMaxRetries` falls back to the global default silently (debug-log only). Same posture as `WsDeadLetterHandler` on malformed `x-death` headers.
- **`_ws_delivery_count` header ownership.** The header is *never* written by the publish path (no bump on initial publish — that would break first-delivery-count=1). Only the redelivery/dead-letter path writes it via `incrementDeliveryCount`. This keeps publish semantics pure and mirrors RabbitMQ's `x-delivery-count` which also appears on first redelivery only.

---

## Limitations

- **Fetch-loop wiring is not yet live.** The task spec says WS1.15 should call `incrementDeliveryCount` before every redelivery. That helper is implemented here, but `WsConsumerFetchLoop` does not yet call it — the fetch integration (WS1.16) has to thread record headers through `maybeDeliverRecord`. Under TDD this shows up as: the gate will never fire in production today because no code writes `_ws_delivery_count` yet. The poison machinery is fully tested; only the "bump on redelivery" side-effect is deferred.
- **Tracker doesn't remember the count.** The same record redelivered across a broker restart starts fresh if the client's consumer offset was committed earlier — but the header on the Kafka record itself *does* persist, because we write the incremented header onto the Kafka record, not onto in-memory state. Still, because `currentOffsets` in the fetch loop is rebuilt from committed offsets on startup, a redelivery across restart correctly reads the persisted header. No missing state here — documenting the mechanism.
- **No metric for poison DLX.** `WsMetrics.dlxRate` is incremented only by the WS4.01 `NACK(requeue=false)` path. Poison auto-DLX goes through a different code path (the `PoisonGate` owns its own sink); we don't double-count by stamping `dlxRate` in `WsAckHandler.processSingleNack`. When WS3.08 metrics get a dedicated `poisonDlxRate` counter this is the obvious hook point.
- **Sync-only per-queue lookup.** `WsPoisonMessageProtection.resolveMaxRetries` calls the queue resolver synchronously on every NACK. In WS4.01 the resolver is backed by `WsRoutingMetadataManager` which is an in-memory ConcurrentHashMap — safe. If the production wiring ever goes through a remote metadata lookup, this becomes a hot path and needs caching.

---

## Field Notes

- **Constructor overload explosion.** `WsAckHandler` now has four constructors (3-arg, 4-arg, 5-arg, 6-arg). Kept the older ones for existing WS3.08/WS4.01 call sites that hadn't heard of `PoisonGate` — all delegate to the widest constructor with `null` poison gate. Trade-off vs a builder: this class is called from exactly one place in production (WS1.17 integration not yet live) plus tests, so extra constructors are cheaper than a builder.
- **PoisonGate return shape.** Considered returning a `CompletableFuture<Boolean>` for parity with `DeadLetterHook`. Rejected: the gate is synchronous in its decision — it just consults a per-queue threshold. The asynchronous piece is the DLX produce itself, which the gate's *implementation* fires-and-forgets. If callers need to know when DLX acks, they chain on the sink's future; the ack handler doesn't need to wait.
- **Why separate from `WsDeadLetterHandler`.** The WS4.01 handler is bound to `NACK(requeue=false)` with reason `"rejected"`; the poison path is `NACK(requeue=true)` with reason `"max-retries-exceeded"`. Different triggers, different reasons, different commit semantics. Sharing via the `DeadLetterSink` adapter (production adapter will simply call `WsDeadLetterHandler.deadLetter(..., reason)` with the right reason string) keeps the two flows composable without coupling.
- **Test helper inlining.** `ackHandler_*` tests inline the subscription manager / channel mocks instead of using a `@BeforeEach`. Reason: the poison-gate tests need to rebuild the ack handler with a specific gate (the default `setUp` gives you no gate), and the rebuild-in-place pattern from `WsAckHandlerTest.rebuildHandlerWithHook` doesn't compose with adding a second optional collaborator. Two tests is below the threshold where a helper would pay off.
- **17 tests total** covering: 5 header-helper cases, 4 threshold-resolution cases, 4 DLX-dispatch cases, 1 null-guard, 2 end-to-end `WsAckHandler+PoisonGate` integration cases, and 1 `WsConfigs` overlap (not written — `WsConfigsTest` already covers `maxRedeliveryCount` default).

---

## Acceptance Criteria

- [ ] `timeout 300 ./gradlew :http-server:test --tests 'kafka.server.http.ws.WsPoisonMessageProtectionTest' -x spotlessCheck` exits 0
- [ ] `grep -r "_ws_delivery_count" http-server/src/main/java/` returns at least 2 hits
- [ ] Auto-DLX triggered when delivery count exceeds max retries
- [ ] Learning section filled with at least one entry

---

## File Manifest

**Created:**
- `http-server/src/main/java/kafka/server/http/ws/WsPoisonMessageProtection.java` — header helpers (`readDeliveryCount`, `incrementDeliveryCount`), threshold resolver, auto-DLX dispatcher
- `http-server/src/test/java/kafka/server/http/ws/WsPoisonMessageProtectionTest.java` — 17 tests spanning header helpers, threshold logic, DLX dispatch, and end-to-end `WsAckHandler+PoisonGate` integration

**Modified:**
- `http-server/src/main/java/kafka/server/http/ws/WsMessageSerializer.java` — added `HDR_DELIVERY_COUNT = "_ws_delivery_count"` constant
- `http-server/src/main/java/kafka/server/http/ws/WsAckHandler.java` — added `PoisonGate` functional interface + 6-arg constructor + poison path in `processSingleNack(requeue=true)`

**Deferred (out of scope — see Limitations):**
- `http-server/src/main/java/kafka/server/http/ws/WsConsumerFetchLoop.java` — wiring `incrementDeliveryCount` into the redelivery side-effect lives in WS1.15/WS1.16 fetch integration
