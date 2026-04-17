# TASK-WS4.08: Server-Initiated Cancel and Rebalance Notifications

## Prerequisites

| Task | What it delivers | Why this task needs it |
|------|------------------|-----------------------|
| TASK-WS1.11 | WsSubscriptionManager | Subscription cancellation path |
| TASK-WS3.03 | WsConsumerGroupCoordinator | Rebalance events trigger notifications |
| TASK-WS1.02 | WsFrameHandler | Writes notification frames to WebSocket channel |

---

## Context

The design doc §5.3 and §5.8 specify server-initiated notifications to WebSocket subscribers:

**subscription-cancelled frame:** Sent when:
- The queue is deleted (by another client or auto-delete trigger)
- An exclusive consumer is evicted by queue deletion
- The queue's backing topic is deleted externally
- An admin force-cancels the consumer via REST API

```json
{
  "type": "subscription-cancelled",
  "subscriptionId": "sub-1",
  "reason": "QUEUE_DELETED",
  "queue": "order-events"
}
```

**rebalance frame:** Sent when GroupCoordinator reassigns partitions:
```json
{
  "type": "rebalance",
  "subscriptionId": "sub-1",
  "assignedPartitions": [0, 1, 3],
  "revokedPartitions": [2]
}
```

When partitions are revoked, all pending delivery tags for records from those partitions are invalidated. ACKs for invalidated tags are silently ignored.

---

## Specification

### WsServerNotificationWriter

```java
package kafka.server.http.ws;

public final class WsServerNotificationWriter {

    /** Send subscription-cancelled to the owning connection. */
    public void sendSubscriptionCancelled(String connectionId, String subscriptionId,
                                           String reason, String queueName);

    /** Send rebalance notification with assigned and revoked partitions. */
    public void sendRebalance(String connectionId, String subscriptionId,
                               Collection<Integer> assignedPartitions,
                               Collection<Integer> revokedPartitions);
}
```

### Cancel reasons

| Reason | Trigger |
|---|---|
| `QUEUE_DELETED` | Queue deleted by another client or auto-delete |
| `EXCLUSIVE_EVICTION` | Exclusive consumer's queue deleted |
| `TOPIC_DELETED` | Backing Kafka topic deleted externally |
| `ADMIN_CANCEL` | Admin force-cancel via REST API |

---

## Implementation Details

**Module:** `http-server`

**Files to create:**

| File | What it does |
|------|--------------|
| `http-server/src/main/java/kafka/server/http/ws/WsServerNotificationWriter.java` | Writes server-initiated frames to WS connections |

**Files to modify:**

| File | What changes |
|------|--------------|
| `http-server/src/main/java/kafka/server/http/routing/QueueManager.java` | On queue delete, notify affected subscribers |
| `http-server/src/main/java/kafka/server/http/ws/WsConsumerGroupCoordinator.java` | On rebalance, call notification writer |
| `http-server/src/main/java/kafka/server/http/ws/WsDeliveryTagTracker.java` | Invalidate tags for revoked partitions |

> **CRITICAL:** subscription-cancelled is a TERMINAL event. After sending it, no more deliver frames for that subscriptionId. The fetch loop must stop.

> **CRITICAL:** Revoked partitions in rebalance frame invalidate delivery tags. ACKs for invalidated tags must be silently ignored (not errors).

> **CRITICAL:** The notification must be sent to the CORRECT connection. Use the WsConnectionRegistry to look up the connection that owns the subscription.

**Implementation order:**
1. Create WsServerNotificationWriter
2. Wire queue deletion → subscription-cancelled for all affected subscribers
3. Wire rebalance → rebalance frame
4. Wire admin cancel (REST) → subscription-cancelled
5. Ensure delivery tag invalidation on revoked partitions
6. Add unit tests

---

## Skeleton Code

```java
package kafka.server.http.ws;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;

import java.util.Collection;

/**
 * Writes server-initiated notification frames to WebSocket connections.
 *
 * // Time: Created - TASK-WS4.08
 */
public final class WsServerNotificationWriter {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // TODO: WsConnectionRegistry dependency

    public void sendSubscriptionCancelled(String connectionId, String subscriptionId,
                                           String reason, String queueName) {
        ObjectNode frame = MAPPER.createObjectNode();
        frame.put("type", "subscription-cancelled");
        frame.put("subscriptionId", subscriptionId);
        frame.put("reason", reason);
        frame.put("queue", queueName);
        // TODO: Look up connection channel, write frame
    }

    public void sendRebalance(String connectionId, String subscriptionId,
                               Collection<Integer> assignedPartitions,
                               Collection<Integer> revokedPartitions) {
        ObjectNode frame = MAPPER.createObjectNode();
        frame.put("type", "rebalance");
        frame.put("subscriptionId", subscriptionId);
        ArrayNode assigned = frame.putArray("assignedPartitions");
        assignedPartitions.forEach(assigned::add);
        ArrayNode revoked = frame.putArray("revokedPartitions");
        revokedPartitions.forEach(revoked::add);
        // TODO: Look up connection channel, write frame
    }
}
```

---

## Tests

**Test class:** `http-server/src/test/java/kafka/server/http/ws/WsServerNotificationWriterTest.java`

| Test method | What it verifies |
|-------------|-----------------|
| `queueDeleted_sendsSubscriptionCancelled` | subscription-cancelled with QUEUE_DELETED |
| `adminCancel_sendsSubscriptionCancelled` | subscription-cancelled with ADMIN_CANCEL |
| `rebalance_sendsAssignedAndRevoked` | Rebalance frame with both partition lists |
| `revokedPartitions_invalidateDeliveryTags` | Tags for revoked partitions invalidated |
| `cancelledSubscription_noMoreDelivers` | Fetch loop stops after cancel |
| `acksForInvalidatedTags_silentlyIgnored` | ACKs for revoked tags not errors |
| `multipleSubscribers_onlyAffectedNotified` | Only affected subscriber receives cancel |

**Run command:**
```bash
timeout 300 ./gradlew :http-server:test --tests 'kafka.server.http.ws.WsServerNotificationWriterTest' -x spotlessCheck
```

---

## Rules

- subscription-cancelled is terminal — no more deliver frames after.
- Revoked partitions invalidate delivery tags.
- ACKs for invalidated tags silently ignored (not errors).
- Cancel reasons: QUEUE_DELETED, EXCLUSIVE_EVICTION, TOPIC_DELETED, ADMIN_CANCEL.
- Notification sent to correct connection via WsConnectionRegistry.

---

## Learning

- `WsConnectionContext.sendFrame(String)` is the canonical way to push a server-initiated frame — Netty serialises writes per channel, so the writer does not need additional synchronisation. The registry lookup is `ConcurrentHashMap#get`, also lock-free.
- `WsConsumerGroupCoordinator` already fires `RebalanceListener.onPartitionsRevoked` *before* `onPartitionsAssigned` for members that are losing partitions, so the adapter can buffer the revoked set and emit a single `rebalance` frame on the assign callback.
- `WsDeliveryTagTracker.invalidatePartitions` is the load-bearing piece for the "ACKs for revoked partitions silently ignored" contract. After invalidation, subsequent `ack(tag)` returns `false` and — crucially — `peek(tag)` also returns `null`, which gives the rebalance-adapter / ack-handler a quick way to detect "this tag was revoked, do nothing".
- The spec mentioned `QueueManager.java` in the files-to-modify list but the project stores queue metadata via `WsRoutingMetadataManager`; the queue-deletion notification wiring will need a follow-up task that subscribes a listener to `WsRoutingMetadataManager.deleteQueue` tombstones.
- Checkstyle is stricter than I expected — unused imports cause the whole `:http-server:test` task to fail. Always clean imports after stubbing out test code.

---

## Limitations

- **Not wired into queue deletion:** `WsRoutingMetadataManager.deleteQueue` does not yet emit `subscription-cancelled` frames for subscribers of the deleted queue. A follow-up task should introduce a `QueueDeletionListener` that walks `WsSubscriptionManager` instances per connection and calls `WsServerNotificationWriter.sendSubscriptionCancelled(..., REASON_QUEUE_DELETED, ...)` + `unsubscribe`.
- **Not wired into `WsConsumerGroupCoordinator` by default:** the coordinator's `RebalanceListener` is supplied per-`joinGroup` call. TASK-WS4.08 only proves the adapter contract in a test (`WsServerCancelTest`). Production wiring (inject the adapter from the subscription-manager glue) lives in the next wiring task, because `WsSubscriptionManager.subscribe` does not currently call `joinGroup` — TASK-WS3.03 left that wiring to a future integration task.
- **Exclusive-eviction reason not yet fired:** `ExclusiveConsumerManager.onConnectionClose` cascades queue deletion per design §11.3, but the cascade does not yet call the notification writer. Same follow-up as queue deletion.
- **Topic-deleted reason not yet fired:** there is no Kafka-topic-deletion listener on the HTTP server side; needs an admin-client hook.

---

## Field Notes

- The `WsServerNotificationWriter` is intentionally a *stateless* output class. It has no knowledge of subscriptions or delivery tags; callers are expected to have already invalidated tracker state before sending a rebalance frame. This keeps the dependency graph shallow and the class trivially testable.
- Chose `Collection<Integer>` rather than `Set<Integer>` for the rebalance partition lists because coordinator callbacks deliver ordered `Collection<TopicPartition>` — no point forcing a re-copy into a `Set`. The frame serialisation preserves iteration order.
- `null` partition entries are defensively skipped in the rebalance serialiser — this is belt-and-braces since the coordinator never produces null entries, but it keeps the writer robust to third-party listener plumbing.
- Inactive-channel silent-no-op matters in the teardown race: the admin REST DELETE can arrive microseconds after the WebSocket channel closed. Without the guard we'd log a stack trace on every racing force-cancel.
- Kept the existing `ConsumerRestHandler` 2-arg constructor to preserve source compatibility with the broker wiring — added a 3-arg overload that injects a shared notification writer.

---

---

## Acceptance Criteria

- [ ] `timeout 300 ./gradlew :http-server:test --tests 'kafka.server.http.ws.WsServerNotificationWriterTest' -x spotlessCheck` exits 0
- [ ] `grep -r "WsServerNotificationWriter" http-server/src/main/java/` returns at least 3 hits
- [ ] subscription-cancelled frame includes reason and queue name
- [ ] rebalance frame includes assignedPartitions and revokedPartitions arrays
- [ ] Learning section filled with at least one entry

---

## File Manifest

**Created:**
- `http-server/src/main/java/kafka/server/http/ws/WsServerNotificationWriter.java` — stateless writer for `subscription-cancelled` and `rebalance` frames; looks up the target connection via `WsConnectionRegistry`.
- `http-server/src/test/java/kafka/server/http/ws/WsServerNotificationWriterTest.java` — 17 unit tests (frame shape, all 4 cancel reasons, empty / unknown / inactive connection paths, null-arg rejection).
- `http-server/src/test/java/kafka/server/http/ws/WsServerCancelTest.java` — 7 integration-style tests that exercise the rebalance-adapter contract and confirm ACKs for invalidated tags are silently ignored.

**Modified:**
- `http-server/src/main/java/kafka/server/http/rest/ConsumerRestHandler.java` — delegates `subscription-cancelled` frame construction to `WsServerNotificationWriter` instead of the previous inline `cancelledFrameJson` helper. New 3-arg constructor overload accepts an injected writer.

**Untouched but verified still green:**
- `WsFrameHandlerTest`, `WsSubscriptionManagerTest`, `WsConsumerGroupCoordinatorTest`, `ConsumerRestHandlerTest` (117 passing test methods across the targeted classes).
