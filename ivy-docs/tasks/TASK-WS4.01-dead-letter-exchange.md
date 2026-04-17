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

_To be filled by the executing agent._

---

## Limitations

_To be filled by the executing agent._

---

## Field Notes

_To be filled by the executing agent._

---

## Acceptance Criteria

- [ ] `timeout 300 ./gradlew :http-server:test --tests 'kafka.server.http.ws.WsDeadLetterHandlerTest' -x spotlessCheck` exits 0
- [ ] `grep -r "WsDeadLetterHandler" http-server/src/main/java/` returns at least 3 hits
- [ ] DLX produce completes before offset commit (atomicity invariant)
- [ ] x-death header accumulates across hops
- [ ] Learning section filled with at least one entry

---

## File Manifest

_To be filled by the executing agent._
