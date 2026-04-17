# TASK-WS3.03: Competing Consumers via GroupCoordinator

## Prerequisites

| Task | What it delivers | Why this task needs it |
|------|------------------|-----------------------|
| TASK-WS1.11 | WsSubscriptionManager — subscription lifecycle | Manages subscriber registration |
| TASK-WS1.12 | WsConsumerFetchLoop — per-subscription fetch task | Fetch loop needs partition assignment from GroupCoordinator |
| TASK-WS1.13 | WsDeliveryTagTracker | Delivery tags must be invalidated on rebalance |

---

## Context

The design doc §9.2 and §11.4 specify competing consumer support. When multiple WebSocket clients subscribe to the same queue, they form a Kafka consumer group. The `GroupCoordinator` handles partition assignment automatically.

**Consumer group name:** `ws.{queueName}` — all WebSocket subscribers to the same queue join the same consumer group.

**Partition assignment flow:**
1. Client A subscribes to `queue="orders"` → joins group `ws.orders`
2. Client B subscribes to `queue="orders"` → joins group `ws.orders`
3. GroupCoordinator assigns: Client A → partitions [0,1], Client B → partitions [2,3]
4. Client B disconnects → rebalance: Client A → partitions [0,1,2,3]

**Rebalance notification (§5.8):** When partitions are reassigned, the broker sends:
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

### WsConsumerGroupCoordinator

```java
package kafka.server.http.ws;

public final class WsConsumerGroupCoordinator {

    /** Register a subscription as a member of queue's consumer group. */
    public CompletableFuture<PartitionAssignment> joinGroup(
        String queueName, String subscriptionId, WsConsumerFetchLoop fetchLoop);

    /** Remove a subscription from the consumer group (unsubscribe or disconnect). */
    public CompletableFuture<Void> leaveGroup(String queueName, String subscriptionId);

    /** Handle rebalance callback from GroupCoordinator. */
    public void onPartitionsAssigned(String subscriptionId,
                                     Collection<TopicPartition> assigned);

    /** Handle partition revocation during rebalance. */
    public void onPartitionsRevoked(String subscriptionId,
                                    Collection<TopicPartition> revoked);
}
```

---

## Implementation Details

**Module:** `http-server`

**Files to study:**

| File | Why |
|------|-----|
| `core/src/main/scala/kafka/coordinator/group/GroupCoordinator.scala` | Understand join/leave/rebalance protocol |
| `core/src/main/scala/kafka/server/KafkaApis.scala` | handleJoinGroup, handleSyncGroup patterns |

**Files to create:**

| File | What it does |
|------|--------------|
| `http-server/src/main/java/kafka/server/http/ws/WsConsumerGroupCoordinator.java` | Bridges WS subscriptions to Kafka consumer groups |

**Files to modify:**

| File | What changes |
|------|--------------|
| `http-server/src/main/java/kafka/server/http/ws/WsSubscriptionManager.java` | Use WsConsumerGroupCoordinator for subscribe/unsubscribe |
| `http-server/src/main/java/kafka/server/http/ws/WsConsumerFetchLoop.java` | Fetch from assigned partitions only, handle reassignment |
| `http-server/src/main/java/kafka/server/http/ws/WsDeliveryTagTracker.java` | Invalidate tags for revoked partitions |
| `http-server/src/main/java/kafka/server/http/ws/WsFrameHandler.java` | Write rebalance frame to client |

> **CRITICAL:** The rebalance notification must be sent to the affected subscriber. Revoked partitions invalidate delivery tags — ACKs for those tags must be silently ignored, not return errors.

> **CRITICAL:** The GroupCoordinator join/sync protocol is asynchronous. The subscribe frame response (`subscribed`) should be sent after initial partition assignment completes.

> **CRITICAL:** Consumer group name is `ws.{queueName}` — consistent across all subscribers regardless of connection or broker.

**Implementation order:**
1. Create WsConsumerGroupCoordinator with join/leave/rebalance callbacks
2. Modify WsSubscriptionManager to use group coordinator on subscribe/unsubscribe
3. Modify WsConsumerFetchLoop to fetch only from assigned partitions
4. Modify WsDeliveryTagTracker to invalidate tags on revoked partitions
5. Add rebalance frame writing to WsFrameHandler
6. Add unit tests

---

## Skeleton Code

```java
package kafka.server.http.ws;

import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Bridges WebSocket subscriptions to Kafka consumer groups.
 * Group name = "ws.{queueName}".
 *
 * // Time: Created - TASK-WS3.03
 */
public final class WsConsumerGroupCoordinator {

    private static final String GROUP_PREFIX = "ws.";

    private final ConcurrentHashMap<String, SubscriptionGroupMember> members =
        new ConcurrentHashMap<>();

    public CompletableFuture<Collection<TopicPartition>> joinGroup(
            String queueName, String subscriptionId, WsConsumerFetchLoop fetchLoop) {
        String groupId = GROUP_PREFIX + queueName;
        // TODO: Build JoinGroupRequest, submit to RequestChannel
        // TODO: Handle SyncGroupResponse with partition assignment
        // TODO: Return assigned partitions
        throw new UnsupportedOperationException("Not yet implemented");
    }

    public CompletableFuture<Void> leaveGroup(String queueName, String subscriptionId) {
        String groupId = GROUP_PREFIX + queueName;
        // TODO: Build LeaveGroupRequest, submit to RequestChannel
        throw new UnsupportedOperationException("Not yet implemented");
    }

    public void onPartitionsAssigned(String subscriptionId,
                                     Collection<TopicPartition> assigned) {
        // TODO: Update fetch loop with new partition assignment
        // TODO: Send rebalance frame to WebSocket client
        throw new UnsupportedOperationException("Not yet implemented");
    }

    public void onPartitionsRevoked(String subscriptionId,
                                    Collection<TopicPartition> revoked) {
        // TODO: Invalidate delivery tags for revoked partitions
        // TODO: Stop fetching from revoked partitions
        // TODO: Include revoked partitions in rebalance frame
        throw new UnsupportedOperationException("Not yet implemented");
    }

    private record SubscriptionGroupMember(
        String subscriptionId, WsConsumerFetchLoop fetchLoop,
        Collection<TopicPartition> assignedPartitions) {}
}
```

---

## Tests

**Test class:** `http-server/src/test/java/kafka/server/http/ws/WsConsumerGroupCoordinatorTest.java`

| Test method | What it verifies |
|-------------|-----------------|
| `joinGroup_generatesCorrectGroupName` | Group name is "ws.{queueName}" |
| `twoSubscribers_partitionsDistributed` | Partitions split between two members |
| `leaveGroup_triggersRebalance` | Remaining member gets all partitions |
| `rebalance_sendsNotificationFrame` | Rebalance frame sent with assigned/revoked |
| `revokedPartitions_invalidateDeliveryTags` | ACKs for revoked tags silently ignored |
| `multipleSubscriptions_sameQueue_sameGroup` | All subscribers in same consumer group |

**Run command:**
```bash
timeout 300 ./gradlew :http-server:test --tests 'kafka.server.http.ws.WsConsumerGroupCoordinatorTest' -x spotlessCheck
```

---

## Rules

- Consumer group name: `ws.{queueName}` — never include vhost in group name (topic already has vhost prefix).
- Rebalance invalidates delivery tags for revoked partitions.
- ACKs for invalidated tags are silently ignored (not errors).
- `subscribed` response sent after initial partition assignment completes.

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

- [ ] `timeout 300 ./gradlew :http-server:test --tests 'kafka.server.http.ws.WsConsumerGroupCoordinatorTest' -x spotlessCheck` exits 0
- [ ] `grep -r "WsConsumerGroupCoordinator" http-server/src/main/java/` returns at least 3 hits
- [ ] Group name format: "ws.{queueName}"
- [ ] Learning section filled with at least one entry

---

## File Manifest

_To be filled by the executing agent._
