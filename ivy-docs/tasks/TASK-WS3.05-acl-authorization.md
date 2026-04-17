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

- The spec file names `WsAuthorizationHelperTest` but the parent message wanted `WsAclAuthorizationTest`; aligned on the latter to match the run-command, while using the same ACL-helper focus.
- Kafka's `Authorizer.authorize(AuthorizableRequestContext, List<Action>)` is the ONLY public entry point — no `authorize(principal, resource, op)` shortcut exists. The helper synthesises a minimal `AuthorizableRequestContext` backed by the WS connection's principal + a fixed `"WS"` listener name (stays consistent with Kafka's listener-name-aware audit logging).
- `ResourcePattern` must be built with `PatternType.LITERAL` for the authorizer to do exact-match lookups against stored LITERAL ACLs; wildcard/prefix patterns are reserved for filter-style ACL queries.
- The cyclomatic/NPath complexity rule on `WsPublishHandler.handlePublish` is tight: each new `if` on the main path roughly doubles NPath. Splitting each added branch into a private method (`isUserIdAuthorized`, `isAuthorizedForAllQueues`, `isDedupHit`, `hasQueuesToRouteTo`) was necessary to keep the file under the checkstyle budget AND ended up improving readability of the publish pipeline.
- Keeping the helper/field `null`able in handlers matters — existing tests across WS1.04 / WS1.11 / WS1.15 never pass an authorizer, so the "no helper → permissive" degenerate path must be a hot path without changing observable behaviour.

---

## Limitations

- The `AuthorizableRequestContext` built by the helper uses placeholder values for `clientAddress` (0.0.0.0), `requestType`/`requestVersion` (`-1`), and `clientId` ("ws"). This is fine for `StandardAuthorizer` (only principal + host matters) but authorizers that audit these fields will see placeholder data for every WS call. Production wiring should derive the real peer address from `WsConnectionContext.remoteAddress()` — left for the integration task that plumbs `WsAuthorizationHelper` through `WsUpgradeOrHttpHandler`.
- `WsFrameHandler.requireExchangeAdmin` / `requireQueueAccess` are hooks — they are NOT called from the existing `handleDeclareExchange` / `handleDeclareQueue` / etc. stubs (those still throw `UnsupportedOperationException`). Wiring them into the real implementations belongs to the per-operation tasks (WS2.06 REST + later WS frame-handler fleshing).
- `WsSubscriptionManager.checkSubscribeAuthorized` is a static utility; the subscribe call sites (frame handler's `handleSubscribe`) do not yet call it — same reasoning as above.
- Group-level `GROUP:READ` check for subscribe (design §19.2) is not implemented — the WS consumer-group coordinator is still under construction (WS3.03); adding the check before the coordinator exists would require a stub `consumerGroupId` that isn't meaningful. Deferred to the subscribe-handler implementation task.
- `bind`/`unbind` maps to `TOPIC:ALTER` on the backing topic `ws.<queue>` per the task-file mapping. The design doc also contemplates an exchange-level ALTER check for the exchange side of the binding; the skeleton task spec only lists the queue side, so the helper models exactly that.

---

## Field Notes

- Wired authorization through backward-compatible constructors: each impacted class (`WsPublishHandler`, `WsFrameHandler`) gained a new constructor overload taking a nullable `WsAuthorizationHelper`; the existing overloads delegate with `null`. Zero existing test had to change.
- `WsPublishHandler.handlePublish` drifted over the NPath complexity budget (512) the moment the second ACL branch (`isAuthorizedForAllQueues`) landed. Consolidating unrelated existing branches (`runRoute` → `null`/empty) into a `hasQueuesToRouteTo` helper bought back the budget while also making the main method read as a linear sequence of guards.
- `isAuthorizedForAllQueues` resolves queue names → topic names VIA the injected `queueToTopicFn` before asking the authorizer, so the ACL check runs on exactly the same resource string the Kafka core layer will authorize later. Any queue whose topic resolution is `null`/empty is silently omitted — matches the `fanOut` behaviour below.
- `FakeAuthorizer` in the test file is ~60 lines and suffices for every scenario; avoided bringing in `MockedStatic` / `StandardAuthorizer` to keep the tests pure unit-level. The fake records every context + action for assertions (`authorize_passesPrincipalThroughRequestContext`, `authorize_buildsLiteralResourcePattern`).
- Mandatory worktree reset: worktree was at `f95a1f995d`; reset to `origin/feature/http-protocol` @ `417ea132fc` before starting. All subsequent work is on top of that HEAD.

---

## Acceptance Criteria

- [x] `timeout 300 ./gradlew :http-server:test --tests 'kafka.server.http.ws.WsAclAuthorizationTest' -x spotlessCheck` exits 0 (32/32 passed)
- [x] `grep -r "WsAuthorizationHelper" http-server/src/main/java/` returns at least 4 hits (15 hits across 4 files)
- [x] Multi-queue publish authorization checks ALL target queues (`publishHandler_multiQueueOneDenied_rejectsEntirePublish`)
- [x] userId validation rejects mismatch with ACCESS_REFUSED (`publishHandler_userIdMismatch_emitsAccessRefusedFrame`)
- [x] Learning section filled with at least one entry

---

## File Manifest

**Created:**
- `http-server/src/main/java/kafka/server/http/ws/WsAuthorizationHelper.java` — ACL helper class with `authorize`, `filterUnauthorizedQueues`, `validateUserId` methods; bundles a minimal `AuthorizableRequestContext` impl (`WsAuthorizableRequestContext`) carrying the connection's principal into the authorizer call.
- `http-server/src/test/java/kafka/server/http/ws/WsAclAuthorizationTest.java` — 32 unit tests: helper-level per-operation authorized/denied coverage + integration tests against `WsPublishHandler` (userId + multi-queue), `WsFrameHandler` (requireExchangeAdmin / requireQueueAccess hooks), `WsSubscriptionManager.checkSubscribeAuthorized`.

**Modified:**
- `http-server/src/main/java/kafka/server/http/ws/WsPublishHandler.java` — added nullable `authorizationHelper` field, new 8-arg constructor, `FIELD_USER_ID`/`userId` in `ParsedPublish`, userId + multi-queue WRITE checks in `handlePublish`, extracted `isUserIdAuthorized` / `isAuthorizedForAllQueues` / `isDedupHit` / `hasQueuesToRouteTo` helpers.
- `http-server/src/main/java/kafka/server/http/ws/WsFrameHandler.java` — added nullable `authorizationHelper` field + 4-arg constructor overload; added `ERR_ACCESS_REFUSED` / `CLUSTER_RESOURCE_NAME` / `WS_TOPIC_PREFIX` constants and `requireAuthorized` / `requireExchangeAdmin` / `requireQueueAccess` hooks for later-task handler impls.
- `http-server/src/main/java/kafka/server/http/ws/WsSubscriptionManager.java` — added static `checkSubscribeAuthorized(helper, principal, topic)` utility (TOPIC:READ check).

**Commit:** _(filled after commit)_
