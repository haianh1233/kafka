# TASK-WS2.09: Virtual Host (Vhost) Support

## Prerequisites

| Task | What it delivers | Why this task needs it |
|------|------------------|-----------------------|
| TASK-WS1.04 | WsRoutingMetadataManager — metadata storage + replay | Vhost metadata stored in __ws_routing_metadata keyed by vhost |
| TASK-WS1.05 | ExchangeManager | Per-vhost exchange instances |
| TASK-WS1.06 | QueueManager | Per-vhost topic prefix mapping |
| TASK-WS1.08 | RoutingEngine | Per-vhost RoutingEngine instances |
| TASK-WS2.06 | REST handlers | REST vhost CRUD + X-Vhost header extraction |

---

## Context

The design doc §11.6 specifies virtual host support for namespace isolation in multi-tenant deployments. Each vhost has its own set of exchanges, queues, and bindings — completely invisible to other vhosts.

**Vhost selection:**
- WebSocket: `GET /v1/ws?vhost=/production` query parameter on upgrade
- REST: `X-Vhost: /production` header on each request (default `/`)

**Topic prefix mapping:**

| Vhost | Topic Prefix | Queue "orders" → Topic |
|---|---|---|
| `/` (default) | `ws.` | `ws.orders` |
| `/production` | `ws.production.` | `ws.production.orders` |
| `/staging` | `ws.staging.` | `ws.staging.orders` |

**Metadata isolation:** Records in `__ws_routing_metadata` are keyed by vhost: `exchange:/production:orders`, `queue:/staging:inbox`. The in-memory routing cache maintains separate `RoutingEngine` instances per vhost.

**REST vhost CRUD (§5.2):**
- `GET /v1/vhosts` — list virtual hosts with exchange/queue counts
- `PUT /v1/vhosts/{vhost}` — create vhost (pre-declared exchanges auto-created)
- `DELETE /v1/vhosts/{vhost}` — delete vhost and ALL its resources (requires no active connections)

---

## Specification

### VhostManager

```java
package kafka.server.http.routing;

public final class VhostManager {

    /** Get or create the RoutingEngine for a vhost. */
    public RoutingEngine getRoutingEngine(String vhost);

    /** Create a new vhost with pre-declared exchanges. */
    public void createVhost(String vhost);

    /** Delete a vhost and all its exchanges, queues, bindings, and backing topics. */
    public void deleteVhost(String vhost);

    /** List all vhosts with exchange/queue counts. */
    public List<VhostInfo> listVhosts();

    /** Resolve queue name to Kafka topic name using vhost prefix. */
    public String resolveTopicName(String vhost, String queueName);

    /** Check if vhost exists. */
    public boolean exists(String vhost);
}
```

### VhostRestHandler

```java
package kafka.server.http.rest;

public final class VhostRestHandler {

    public FullHttpResponse handleList();
    public FullHttpResponse handleCreate(String vhostName);
    public FullHttpResponse handleDelete(String vhostName);
}
```

### Topic name resolution

```java
public String resolveTopicName(String vhost, String queueName) {
    if ("/".equals(vhost)) {
        return "ws." + queueName;
    }
    String normalizedVhost = vhost.startsWith("/") ? vhost.substring(1) : vhost;
    return "ws." + normalizedVhost + "." + queueName;
}
```

---

## Implementation Details

**Module:** `http-server`

**Files to study:**

| File | Why |
|------|-----|
| `http-server/src/main/java/kafka/server/http/routing/RoutingEngine.java` | Need per-vhost instances |
| `http-server/src/main/java/kafka/server/http/ws/WsConnectionContext.java` | Stores vhost for connection |

**Files to create:**

| File | What it does |
|------|--------------|
| `http-server/src/main/java/kafka/server/http/routing/VhostManager.java` | Vhost lifecycle, per-vhost RoutingEngine map |
| `http-server/src/main/java/kafka/server/http/rest/VhostRestHandler.java` | REST vhost CRUD |

**Files to modify:**

| File | What changes |
|------|--------------|
| `http-server/src/main/java/kafka/server/http/routing/QueueManager.java` | Use VhostManager.resolveTopicName() for topic creation |
| `http-server/src/main/java/kafka/server/http/ws/WsUpgradeOrHttpHandler.java` | Extract `?vhost=` query param on WS upgrade |
| `http-server/src/main/java/kafka/server/http/ws/WsConnectionContext.java` | Store vhost field |
| `http-server/src/main/java/kafka/server/http/HttpRouter.java` | Add vhost REST routes |
| `http-server/src/main/java/kafka/server/http/routing/WsRoutingMetadataManager.java` | Vhost-prefixed metadata keys |

> **CRITICAL:** The default vhost `/` must always exist and cannot be deleted. Pre-declared exchanges (`""`, `amq.direct`, `amq.topic`, `amq.fanout`, `amq.headers`) must be auto-created for each new vhost.

> **CRITICAL:** Deleting a vhost requires no active connections to that vhost. Return 409 Conflict if connections exist.

> **CRITICAL:** Vhost deletion must cascade: delete all exchanges, queues, bindings, and backing Kafka topics in that vhost.

**Implementation order:**
1. Create VhostManager with per-vhost RoutingEngine map
2. Modify QueueManager to use vhost-aware topic name resolution
3. Modify WsUpgradeOrHttpHandler to extract vhost query param
4. Modify WsRoutingMetadataManager for vhost-prefixed keys
5. Create VhostRestHandler
6. Add vhost routes to HttpRouter
7. Add unit tests

---

## Skeleton Code

### VhostManager

```java
package kafka.server.http.routing;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages virtual hosts — namespace isolation for exchanges, queues, bindings.
 *
 * // Time: Created - TASK-WS2.09
 */
public final class VhostManager {

    private static final String DEFAULT_VHOST = "/";

    private final ConcurrentHashMap<String, RoutingEngine> routingEngines =
        new ConcurrentHashMap<>();

    public VhostManager() {
        createVhost(DEFAULT_VHOST);
    }

    public RoutingEngine getRoutingEngine(String vhost) {
        RoutingEngine engine = routingEngines.get(vhost);
        if (engine == null) {
            throw new IllegalArgumentException("Vhost not found: " + vhost);
        }
        return engine;
    }

    public void createVhost(String vhost) {
        // TODO: Create RoutingEngine with pre-declared exchanges
        // TODO: Store in __ws_routing_metadata
        throw new UnsupportedOperationException("Not yet implemented");
    }

    public void deleteVhost(String vhost) {
        // TODO: Reject deletion of default vhost "/"
        // TODO: Check no active connections
        // TODO: Delete all exchanges, queues, bindings, backing topics
        // TODO: Remove from routingEngines map
        throw new UnsupportedOperationException("Not yet implemented");
    }

    public String resolveTopicName(String vhost, String queueName) {
        if (DEFAULT_VHOST.equals(vhost)) {
            return "ws." + queueName;
        }
        String normalized = vhost.startsWith("/") ? vhost.substring(1) : vhost;
        return "ws." + normalized + "." + queueName;
    }

    public boolean exists(String vhost) {
        return routingEngines.containsKey(vhost);
    }

    public List<VhostInfo> listVhosts() {
        // TODO: Return vhost name + exchange/queue counts
        throw new UnsupportedOperationException("Not yet implemented");
    }

    public record VhostInfo(String name, int exchangeCount, int queueCount) {}
}
```

---

## Tests

**Test class:** `http-server/src/test/java/kafka/server/http/routing/VhostManagerTest.java`

| Test method | What it verifies |
|-------------|-----------------|
| `defaultVhost_existsOnStartup` | Default "/" vhost auto-created |
| `createVhost_autoCreatesPreDeclaredExchanges` | 5 pre-declared exchanges in new vhost |
| `resolveTopicName_defaultVhost` | "/" → "ws.orders" |
| `resolveTopicName_namedVhost` | "/production" → "ws.production.orders" |
| `deleteDefaultVhost_rejected` | Cannot delete "/" vhost |
| `deleteVhost_cascadesResources` | All exchanges, queues, bindings removed |
| `deleteVhost_activeConnections_returns409` | Rejects deletion with active connections |
| `getRoutingEngine_unknownVhost_throws` | Non-existent vhost throws |
| `vhostIsolation_separateNamespaces` | Same queue name in different vhosts resolves to different topics |

**Run command:**
```bash
timeout 300 ./gradlew :http-server:test --tests 'kafka.server.http.routing.VhostManagerTest' -x spotlessCheck
```

---

## Rules

- Default vhost `/` always exists and cannot be deleted.
- Pre-declared exchanges: `""` (direct), `amq.direct`, `amq.topic`, `amq.fanout`, `amq.headers`.
- Topic prefix: `ws.` for `/`, `ws.{vhost_without_leading_slash}.` for named vhosts.
- Metadata keys in `__ws_routing_metadata` include vhost: `exchange:{vhost}:{name}`.
- Vhost deletion requires zero active connections (409 if violated).

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

- [ ] `timeout 300 ./gradlew :http-server:test --tests 'kafka.server.http.routing.VhostManagerTest' -x spotlessCheck` exits 0
- [ ] `grep -r "VhostManager" http-server/src/main/java/` returns at least 3 hits
- [ ] Default vhost "/" auto-created on VhostManager construction
- [ ] Topic name resolution: "/" + "orders" → "ws.orders"
- [ ] Topic name resolution: "/production" + "orders" → "ws.production.orders"
- [ ] Learning section filled with at least one entry

---

## File Manifest

_To be filled by the executing agent._
