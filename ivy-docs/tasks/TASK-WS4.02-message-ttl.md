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

_To be filled by the executing agent._

---

## Limitations

_To be filled by the executing agent._

---

## Field Notes

_To be filled by the executing agent._

---

## Acceptance Criteria

- [ ] `timeout 300 ./gradlew :http-server:test --tests 'kafka.server.http.ws.WsMessageTtlTest' -x spotlessCheck` exits 0
- [ ] `grep -r "_ws_expiration" http-server/src/main/java/` returns at least 2 hits
- [ ] Expired messages advance consumer offset
- [ ] Learning section filled with at least one entry

---

## File Manifest

_To be filled by the executing agent._
