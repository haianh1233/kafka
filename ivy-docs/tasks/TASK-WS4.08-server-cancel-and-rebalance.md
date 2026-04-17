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

_To be filled by the executing agent._

---

## Limitations

_To be filled by the executing agent._

---

## Field Notes

_To be filled by the executing agent._

---

## Acceptance Criteria

- [ ] `timeout 300 ./gradlew :http-server:test --tests 'kafka.server.http.ws.WsServerNotificationWriterTest' -x spotlessCheck` exits 0
- [ ] `grep -r "WsServerNotificationWriter" http-server/src/main/java/` returns at least 3 hits
- [ ] subscription-cancelled frame includes reason and queue name
- [ ] rebalance frame includes assignedPartitions and revokedPartitions arrays
- [ ] Learning section filled with at least one entry

---

## File Manifest

_To be filled by the executing agent._
