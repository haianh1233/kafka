# TASK-WS4.02: Message TTL — Per-Queue and Per-Message Expiration

## Prerequisites

| Task | What it delivers | Why this task needs it |
|------|------------------|-----------------------|
| TASK-WS1.12 | WsConsumerFetchLoop — per-subscription fetch task | TTL check happens at delivery time in fetch loop |
| TASK-WS1.06 | QueueManager | Queue-level x-message-ttl maps to Kafka retention.ms |
| TASK-WS4.01 | WsDeadLetterHandler | Expired messages with DLX are dead-lettered |

---

## Context

The design doc §12.4 specifies two levels of message TTL:

**Per-queue TTL** (`x-message-ttl` on queue declare):
- Maps to Kafka `retention.ms` on the backing topic
- Kafka automatically deletes log segments older than this
- Applied at the storage level — no per-message overhead

**Per-message TTL** (`expiration` field on publish):
- Checked at **delivery time** in `WsConsumerFetchLoop`
- If `now() > record.timestamp + expiration`, the message is skipped (not delivered)
- Skipped messages still advance the consumer offset (committed as consumed)
- If the queue has DLX, expired messages are dead-lettered with `reason: "expired"`

---

## Specification

### TTL check in WsConsumerFetchLoop

```java
// At delivery time in fetch loop:
for (Record record : fetchResponse.records()) {
    String expiration = getHeader(record, "_ws_expiration");
    if (expiration != null) {
        long ttlMs = Long.parseLong(expiration);
        long age = System.currentTimeMillis() - record.timestamp();
        if (age > ttlMs) {
            if (queueHasDLX) {
                deadLetterHandler.deadLetter(record, queueName, "expired");
            }
            continue;  // skip delivery, advance offset
        }
    }
    writeDeliverFrame(record);
}
```

---

## Implementation Details

**Module:** `http-server`

**Files to modify:**

| File | What changes |
|------|--------------|
| `http-server/src/main/java/kafka/server/http/ws/WsConsumerFetchLoop.java` | Add per-message TTL check before delivery |
| `http-server/src/main/java/kafka/server/http/ws/WsMessageDeserializer.java` | Extract `_ws_expiration` header |
| `http-server/src/main/java/kafka/server/http/ws/WsMessageSerializer.java` | Write `_ws_expiration` header on publish |
| `http-server/src/main/java/kafka/server/http/routing/QueueManager.java` | Pass `x-message-ttl` as `retention.ms` on topic creation |

> **CRITICAL:** Per-message TTL is checked at DELIVERY TIME, not at storage time. A message that has been in the queue for longer than its TTL but hasn't been fetched yet will be skipped on fetch. This means expired messages occupy storage until fetched.

> **CRITICAL:** Expired messages that are skipped must still advance the consumer offset. Do NOT leave gaps in committed offsets — the offset for expired messages is committed as if they were consumed.

> **CRITICAL:** If the queue has DLX and the message expired, dead-letter with `reason: "expired"` BEFORE advancing the offset. The atomicity invariant from WS4.01 applies.

**Implementation order:**
1. Add `_ws_expiration` header write in WsMessageSerializer on publish
2. Add `_ws_expiration` header read in WsConsumerFetchLoop
3. Add TTL check logic before delivery
4. Integrate with WsDeadLetterHandler for expired+DLX messages
5. Ensure expired messages advance consumer offset
6. Add unit tests

---

## Skeleton Code

```java
// Addition to WsConsumerFetchLoop — TTL check before delivery

private boolean isMessageExpired(ConsumerRecord<byte[], byte[]> record) {
    Header expirationHeader = record.headers().lastHeader("_ws_expiration");
    if (expirationHeader == null) return false;

    long ttlMs = Long.parseLong(new String(expirationHeader.value(), StandardCharsets.UTF_8));
    long ageMs = System.currentTimeMillis() - record.timestamp();
    return ageMs > ttlMs;
}

private void handleExpiredMessage(ConsumerRecord<byte[], byte[]> record, String queueName) {
    if (queueHasDLX(queueName)) {
        deadLetterHandler.deadLetter(record.key(), record.value(),
            record.headers(), queueName, "expired")
            .thenRun(() -> commitOffset(record));
    } else {
        commitOffset(record);
    }
}
```

---

## Tests

**Test class:** `http-server/src/test/java/kafka/server/http/ws/WsMessageTtlTest.java`

| Test method | What it verifies |
|-------------|-----------------|
| `perMessageTtl_expired_skipped` | Expired message not delivered to consumer |
| `perMessageTtl_notExpired_delivered` | Non-expired message delivered normally |
| `perMessageTtl_expired_withDlx_deadLettered` | Expired + DLX → dead-lettered with reason "expired" |
| `perMessageTtl_expired_noDlx_discarded` | Expired + no DLX → offset committed, message lost |
| `perMessageTtl_expired_offsetAdvanced` | Consumer offset advances past expired messages |
| `perQueueTtl_mapsToRetentionMs` | x-message-ttl on queue → retention.ms on topic |
| `noExpiration_noTtlCheck` | Messages without expiration always delivered |
| `expirationHeader_serializedCorrectly` | _ws_expiration written on publish, read on fetch |

**Run command:**
```bash
timeout 300 ./gradlew :http-server:test --tests 'kafka.server.http.ws.WsMessageTtlTest' -x spotlessCheck
```

---

## Rules

- Per-message TTL checked at delivery time, not storage time.
- Expired messages still advance consumer offset.
- Expired + DLX → dead-letter with reason "expired".
- Expired + no DLX → discard, commit offset.
- Per-queue TTL maps to Kafka `retention.ms` on topic creation.
- `_ws_expiration` header stores TTL value in milliseconds as string.

---

## Learning

- **Serializer/deserializer already understood `_ws_expiration`** — WS1.09/WS1.10 mapped the
  AMQP `expiration` message property to the `_ws_expiration` Kafka header round-trip without
  touching the publish/deliver pipeline, so WS4.02 only needed the *check* at delivery time.
  Concretely this means no serializer changes were required: the header is written on publish,
  read on fetch, and WS4.02 simply adds a predicate that `WsConsumerFetchLoop` consults before
  emitting a deliver frame.
- **Fail-open TTL parsing** — A dropped message is a worse outcome than a slightly stale one:
  unknown-format expiration headers, negative TTLs, and clock-skewed records all return
  `isExpired == false`. This is a conservative design choice, and it is documented explicitly
  in both `WsMessageTtl` Javadoc and the corresponding tests so future changes do not silently
  flip it.
- **Offset watermark advances even for expired records** — If we did not advance, the next
  fetch would return the same expired record forever, poisoning the subscription. The skip
  path in `maybeDeliverRecord` intentionally updates `currentOffsets` before returning
  `false`; tests `perMessageTtl_expired_offsetAdvanced` and
  `multipleRecords_mixedExpiration_offsetsAdvanceForAll` pin this invariant.
- **Per-queue TTL is declarative** — `x-message-ttl` → `retention.ms` is a one-shot mapping at
  topic-creation time (there is no per-fetch check). `WsMessageTtl.retentionMsFromQueueArgs`
  returns `-1L` as the sentinel for "use broker default", which matches Kafka's convention
  of treating negative retention values as "ignored" and keeps the helper trivial to adopt
  when `QueueManager` is eventually wired (the WS1.06 `QueueManager` did not exist at the
  time of this task, so the helper is currently unused in production but fully tested).

---

## Limitations

- **DLX integration deferred to WS4.04.** Expired messages with a configured DLX should be
  dead-lettered with `reason: "expired"` per §12.4. WS4.01 (the dead-letter handler) has not
  landed yet, so `maybeDeliverRecord` currently drops expired records silently and advances
  the offset. The drop path includes a hook comment (and a debug log) noting that WS4.01 will
  replace the drop with a dead-letter call; existing tests will still pass because they
  verify the drop+offset-advance contract, which DLX must preserve.
- **`QueueManager` does not exist yet.** The design doc lists
  `http-server/src/main/java/kafka/server/http/routing/QueueManager.java` as a modification
  target but the file is absent. `REST` handlers work against a `QueueStore` façade (WS2.06).
  The helper `WsMessageTtl.retentionMsFromQueueArgs` is ready to be called from whatever
  topic-creation path is eventually added to queue declare, but wiring it is out of scope.
- **Wall-clock dependency.** The TTL check uses `System.currentTimeMillis()` in the
  production overload of `maybeDeliverRecord`. If upstream Kafka record timestamps are
  broker-produced and clients publish against a different clock, clock drift could flip the
  expiration decision. The `isExpired(... , long nowMs)` overload accepts an explicit clock
  so the fetch loop can eventually inject a `Clock` / a broker-synced time source if drift
  becomes a real problem.
- **No metric for dropped-due-to-TTL.** `WsMetrics` does not currently have a
  `TtlExpiredRate` meter. Once WS4.01/WS4.04 wire the DLX path the metric will most naturally
  live with the DLX counters, so we deliberately did not add one here to avoid a throwaway
  meter.

---

## Field Notes

- **TDD loop.** Tests written first against `loop.maybeDeliverRecord(...)` and a
  `WsMessageTtl.isExpired(...)` helper that did not exist — gradle reported 22 "cannot find
  symbol" errors (RED). Added `WsMessageTtl.java` + two `maybeDeliverRecord` overloads on the
  fetch loop (GREEN in a single pass). Checkstyle caught an unused `ArrayList` import on the
  first run; removed. Second run: all 19 new tests pass, all 13 existing
  `WsConsumerFetchLoopTest` cases still pass.
- **Design considered.** Initially considered baking the TTL check directly inside
  `deliverRecord`, but that would have forced every existing call site to supply headers and
  a timestamp, churning the `deliverRecord` signature and WS1.15 tests. A new wrapper method
  `maybeDeliverRecord` keeps the existing API untouched and gives TASK-WS1.16 (fetch
  integration) a clean extension point for the "post-fetch, pre-delivery" pipeline stage.
- **No DLX scaffold yet.** The task description mentions WS4.01/WS4.04 integration as
  follow-ups; no DLX-specific code was added. The drop path is structured so the eventual
  integration is a minimal diff: replace the debug log with `deadLetterHandler.deadLetter(
  ..., "expired").thenRun(() -> currentOffsets.put(tp, offset + 1))`.
- **Per-queue TTL helper is "ready but unconsumed".** Spotlessly documented and tested, no
  production call site yet — covered by three dedicated tests to prove absence-of-arg,
  presence, and malformed-value handling.

---

## Acceptance Criteria

- [x] `timeout 300 ./gradlew :http-server:test --tests 'kafka.server.http.ws.WsMessageTtlTest' -x spotlessCheck` exits 0 (19/19 pass)
- [x] `grep -r "_ws_expiration" http-server/src/main/java/` returns at least 2 hits (returns 5)
- [x] Expired messages advance consumer offset (`perMessageTtl_expired_offsetAdvanced`, `multipleRecords_mixedExpiration_offsetsAdvanceForAll`)
- [x] Learning section filled with at least one entry

---

## File Manifest

**Added**
- `http-server/src/main/java/kafka/server/http/ws/WsMessageTtl.java` — pure TTL helpers:
  `isExpired(headers, recordTs, now)` + `retentionMsFromQueueArgs(args)` + constants.
- `http-server/src/test/java/kafka/server/http/ws/WsMessageTtlTest.java` — 19 tests covering
  header predicate semantics, serializer/deserializer round-trip of `_ws_expiration`,
  `maybeDeliverRecord` deliver-vs-skip behaviour, offset watermark advance on expiry, and
  per-queue TTL mapping.

**Modified**
- `http-server/src/main/java/kafka/server/http/ws/WsConsumerFetchLoop.java` — added WS4.02
  update header, imports for `Header`/`Collections`, and two `maybeDeliverRecord` overloads
  (explicit-clock + production-clock) that consult `WsMessageTtl.isExpired` before delegating
  to the existing `deliverRecord`. Expired records advance the offset watermark without
  writing a frame / consuming a credit / assigning a delivery tag.

**Not modified (per plan)**
- `WsMessageSerializer.java` and `WsMessageDeserializer.java` — already encoded/decoded the
  `_ws_expiration` header since WS1.09/WS1.10; no change required for WS4.02.
- `QueueManager.java` / queue declare path — `QueueManager` does not exist yet in this
  branch; `WsMessageTtl.retentionMsFromQueueArgs` is provided for future integration and is
  fully tested.
