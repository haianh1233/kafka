# TASK-WS3.05: ACL Authorization for WebSocket Operations

## Prerequisites

| Task | What it delivers | Why this task needs it |
|------|------------------|-----------------------|
| TASK-WS1.03 | WsConnectionContext with KafkaPrincipal | Principal created during WS upgrade, used for ACL checks |
| TASK-WS1.09 | WsPublishHandler | Publish needs TOPIC:WRITE check per target queue |
| TASK-WS1.11 | WsSubscriptionManager | Subscribe needs TOPIC:READ check |
| TASK-WS1.05 | ExchangeManager | declare-exchange needs CLUSTER:ALTER check |
| TASK-WS1.06 | QueueManager | declare-queue needs TOPIC:CREATE check |
| TASK-WS1.07 | BindingManager | bind needs TOPIC:ALTER check |

---

## Context

The design doc §19.2 specifies per-operation ACL checks for all WebSocket operations. The authenticated principal from the WS upgrade request is used for all authorization decisions on that connection.

**ACL mapping:**

| WebSocket Operation | Kafka ACL Resource | Kafka ACL Operation |
|---|---|---|
| `publish` (after routing) | `TOPIC:ws.{queue}` | `WRITE` |
| `subscribe` | `TOPIC:ws.{queue}` | `READ` |
| `declare-exchange` | `CLUSTER` | `ALTER` |
| `delete-exchange` | `CLUSTER` | `ALTER` |
| `declare-queue` | `TOPIC:ws.{queue}` | `CREATE` |
| `delete-queue` | `TOPIC:ws.{queue}` | `DELETE` |
| `bind` / `unbind` | `TOPIC:ws.{queue}` | `ALTER` |

**Per-message authorization on publish:** When a single `publish` routes to multiple queues (fanout), the broker checks `WRITE` permission on EACH target queue's backing topic. If any check fails, the publish fails entirely (no partial routing).

**userId validation:** The `message.userId` field in publish frames is validated against the authenticated principal. If it doesn't match, the publish is rejected with `ACCESS_REFUSED`.

---

## Specification

### WsAuthorizationHelper

```java
package kafka.server.http.ws;

public final class WsAuthorizationHelper {

    public WsAuthorizationHelper(Authorizer authorizer);

    /** Check WRITE on each target queue topic. Returns unauthorized queue names. */
    public Set<String> filterUnauthorizedQueues(KafkaPrincipal principal,
                                                 Set<String> queueTopicNames,
                                                 AclOperation operation);

    /** Check single resource authorization. Returns true if authorized. */
    public boolean authorize(KafkaPrincipal principal,
                             ResourceType resourceType, String resourceName,
                             AclOperation operation);

    /** Validate userId field matches authenticated principal. */
    public boolean validateUserId(KafkaPrincipal principal, String userId);
}
```

---

## Implementation Details

**Module:** `http-server`

**Files to study:**

| File | Why |
|------|-----|
| `core/src/main/scala/kafka/server/AuthHelper.scala` | Existing authorization helper patterns |
| `http-server/src/main/java/kafka/server/http/HttpErrorMapper.java` | TOPIC_AUTHORIZATION_FAILED → 403 mapping |

**Files to create:**

| File | What it does |
|------|--------------|
| `http-server/src/main/java/kafka/server/http/ws/WsAuthorizationHelper.java` | ACL check helper for WS operations |

**Files to modify:**

| File | What changes |
|------|--------------|
| `http-server/src/main/java/kafka/server/http/ws/WsPublishHandler.java` | Add WRITE check per target queue, userId validation |
| `http-server/src/main/java/kafka/server/http/ws/WsSubscriptionManager.java` | Add READ check on subscribe |
| `http-server/src/main/java/kafka/server/http/ws/WsFrameHandler.java` | Add CLUSTER:ALTER check on declare/delete-exchange, TOPIC checks on queue/binding ops |

> **CRITICAL:** Multi-queue publish authorization: check WRITE on ALL target queues. If ANY fails, reject ENTIRE publish. Do not partially route.

> **CRITICAL:** userId validation: if `message.userId` is set and doesn't match `principal.getName()`, reject with `ACCESS_REFUSED`.

> **CRITICAL:** Authorization failures return error frame with `errorCode: "ACCESS_REFUSED"`, not HTTP 403 (that's for REST; WS uses error frames).

**Implementation order:**
1. Create WsAuthorizationHelper with resource check methods
2. Add publish authorization (WRITE per target queue + userId validation)
3. Add subscribe authorization (READ on queue topic)
4. Add declare/delete authorization (CLUSTER:ALTER for exchanges, TOPIC:CREATE/DELETE for queues)
5. Add bind/unbind authorization (TOPIC:ALTER)
6. Add unit tests

---

## Skeleton Code

```java
package kafka.server.http.ws;

import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.server.authorizer.Authorizer;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.AuthorizationResult;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * ACL authorization helper for WebSocket operations.
 *
 * // Time: Created - TASK-WS3.05
 */
public final class WsAuthorizationHelper {

    private final Authorizer authorizer;

    public WsAuthorizationHelper(Authorizer authorizer) {
        this.authorizer = Objects.requireNonNull(authorizer, "authorizer");
    }

    public Set<String> filterUnauthorizedQueues(KafkaPrincipal principal,
                                                 Set<String> queueTopicNames,
                                                 AclOperation operation) {
        // TODO: Check each queue topic for the given operation
        // TODO: Return set of unauthorized queue names
        throw new UnsupportedOperationException("Not yet implemented");
    }

    public boolean authorize(KafkaPrincipal principal,
                             ResourceType resourceType, String resourceName,
                             AclOperation operation) {
        // TODO: Build Action, call authorizer.authorize(), check result
        throw new UnsupportedOperationException("Not yet implemented");
    }

    public boolean validateUserId(KafkaPrincipal principal, String userId) {
        if (userId == null || userId.isEmpty()) return true;
        return principal.getName().equals(userId);
    }
}
```

---

## Tests

**Test class:** `http-server/src/test/java/kafka/server/http/ws/WsAuthorizationHelperTest.java`

| Test method | What it verifies |
|-------------|-----------------|
| `publish_authorized_allQueues` | All target queues pass WRITE check |
| `publish_unauthorized_oneQueue_rejectsAll` | One unauthorized queue fails entire publish |
| `subscribe_authorized` | READ check on queue topic passes |
| `subscribe_unauthorized_returnsAccessRefused` | READ check fails → ACCESS_REFUSED error |
| `declareExchange_requiresClusterAlter` | CLUSTER:ALTER check for exchange operations |
| `declareQueue_requiresTopicCreate` | TOPIC:CREATE check for queue operations |
| `bind_requiresTopicAlter` | TOPIC:ALTER check for bind/unbind |
| `userId_matchesPrincipal_allowed` | userId matches → publish proceeds |
| `userId_mismatch_rejected` | userId doesn't match → ACCESS_REFUSED |
| `userId_null_allowed` | No userId field → skip validation |

**Run command:**
```bash
timeout 300 ./gradlew :http-server:test --tests 'kafka.server.http.ws.WsAuthorizationHelperTest' -x spotlessCheck
```

---

## Rules

- Multi-queue publish: ALL queues must pass WRITE check. Any failure rejects entire publish.
- userId field validated against authenticated principal. Mismatch → ACCESS_REFUSED.
- Error code for auth failures: `ACCESS_REFUSED` (in error frame), not HTTP status.
- Exchange operations (declare/delete) require CLUSTER:ALTER.
- Queue operations (declare/delete) require TOPIC:CREATE/DELETE on backing topic.
- Bind/unbind requires TOPIC:ALTER on the queue's backing topic.

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

- [ ] `timeout 300 ./gradlew :http-server:test --tests 'kafka.server.http.ws.WsAuthorizationHelperTest' -x spotlessCheck` exits 0
- [ ] `grep -r "WsAuthorizationHelper" http-server/src/main/java/` returns at least 4 hits
- [ ] Multi-queue publish authorization checks ALL target queues
- [ ] userId validation rejects mismatch with ACCESS_REFUSED
- [ ] Learning section filled with at least one entry

---

## File Manifest

_To be filled by the executing agent._
