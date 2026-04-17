# TASK-WS1.06: Exchange Manager

## Prerequisites

- **TASK-WS1.01 completed** — `WsConfigs.java` exists, providing `maxExchangesPerVhost()`.
- **TASK-WS1.05 completed** — `WsRoutingMetadataManager.java` exists with `writeExchange()`, `deleteExchange()`, `getExchange()`, `listExchanges()`, `exchangeCount()`.
- **TASK-WS1.05 completed** — `ExchangeMetadata.java` exists as the exchange value object.

---

## Context

The `ExchangeManager` is the domain-level manager for exchange lifecycle operations. It handles the business logic for declare, delete, get, and list operations, delegating persistence to `WsRoutingMetadataManager`. It also manages the 5 pre-declared default exchanges that always exist and cannot be deleted.

### Design doc §5.4.1 — Declare Exchange

Declaring an exchange that already exists with the **same type** is a no-op (idempotent). Declaring with a **different type** returns `EXCHANGE_TYPE_MISMATCH` error.

**Passive declare:** When `passive: true`, the broker checks if the exchange exists and returns success if it does, or `EXCHANGE_NOT_FOUND` error if it does not. No exchange is created.

**Pre-declared exchanges** (always exist, cannot be deleted):

| Name | Type | Description |
|---|---|---|
| `""` (empty) | direct | Default exchange — routing key = queue name |
| `amq.direct` | direct | Standard direct exchange |
| `amq.topic` | topic | Standard topic exchange |
| `amq.fanout` | fanout | Standard fanout exchange |
| `amq.headers` | headers | Standard headers exchange |

### Design doc §5.4.2 — Delete Exchange

Deleting a pre-declared exchange returns `EXCHANGE_PROTECTED` error. Deleting a non-existent exchange is a no-op (idempotent). If `ifUnused: true`, the exchange is only deleted if it has no bindings.

### Limit enforcement

The `ws.max.exchanges.per.vhost` config limits the number of exchanges per virtual host. The 5 default exchanges count toward this limit.

---

## Specification

**Package:** `kafka.server.http.routing`

```java
public class ExchangeManager {

    public ExchangeManager(WsRoutingMetadataManager metadataManager, WsConfigs wsConfigs);

    // Called once during startup — registers default exchanges in cache
    public void initializeDefaults(String vhost);

    // --- Operations ---
    public ExchangeMetadata declareExchange(
        String vhost, String name, String type, boolean durable,
        boolean autoDelete, boolean passive, boolean internal,
        Map<String, String> arguments)
        throws ExchangeException;

    public void deleteExchange(String vhost, String name, boolean ifUnused)
        throws ExchangeException;

    public ExchangeMetadata getExchange(String vhost, String name);

    public Collection<ExchangeMetadata> listExchanges(String vhost);

    public boolean isDefaultExchange(String name);
}
```

### Exception class

```java
public class ExchangeException extends Exception {
    public enum ErrorCode {
        EXCHANGE_NOT_FOUND,
        EXCHANGE_TYPE_MISMATCH,
        EXCHANGE_PROTECTED,
        EXCHANGE_IN_USE,
        EXCHANGE_LIMIT_EXCEEDED
    }

    public ExchangeException(ErrorCode code, String message);
    public ErrorCode errorCode();
}
```

**Behavioral contracts:**
- `initializeDefaults(vhost)`: registers the 5 default exchanges in `WsRoutingMetadataManager`'s cache. Does NOT write to the metadata topic (defaults are synthetic).
- `declareExchange()` with `passive=true`: returns existing exchange or throws `EXCHANGE_NOT_FOUND`.
- `declareExchange()` with `passive=false` and exchange exists with same type: returns existing exchange (no-op).
- `declareExchange()` with `passive=false` and exchange exists with different type: throws `EXCHANGE_TYPE_MISMATCH`.
- `declareExchange()` when exchange count >= `maxExchangesPerVhost`: throws `EXCHANGE_LIMIT_EXCEEDED`.
- `deleteExchange()` on default exchange: throws `EXCHANGE_PROTECTED`.
- `deleteExchange()` on non-existent exchange: no-op (returns normally).
- `deleteExchange()` with `ifUnused=true` and exchange has bindings: throws `EXCHANGE_IN_USE`.
- `isDefaultExchange()` returns true for `""`, `"amq.direct"`, `"amq.topic"`, `"amq.fanout"`, `"amq.headers"`.

---

## Implementation Details

**Module:** `http-server`

**Files to study:**

| File | Why |
|------|-----|
| Design doc §5.4.1-5.4.2 | Exchange declare/delete semantics |
| `http-server/src/main/java/kafka/server/http/ws/WsRoutingMetadataManager.java` | Persistence layer to delegate to |

**Files to create:**

| File | What it does |
|------|--------------|
| `http-server/src/main/java/kafka/server/http/routing/ExchangeManager.java` | Exchange lifecycle management |
| `http-server/src/main/java/kafka/server/http/routing/ExchangeException.java` | Typed exception with error codes |

**Files to modify:**

None.

> **CRITICAL:** Default exchanges are NOT written to the metadata topic — they are synthesized at startup. This means they survive even if the metadata topic is empty or unavailable. They are registered directly in the in-memory cache via `WsRoutingMetadataManager`.

> **CRITICAL:** The empty-string exchange (`""`) is the default exchange. It has special routing behavior (routing key = queue name), but that routing logic belongs in the routing engine, not here. The `ExchangeManager` just manages the exchange's existence.

> **CRITICAL:** The 5 default exchanges must be `durable=true`, `autoDelete=false`, `internal=false` (except `amq.headers` which is also not internal). They all have empty arguments.

**Implementation order:**
1. Create `ExchangeException.java`
2. Create `ExchangeManager.java` with defaults initialization, declare, delete, get, list
3. Create `ExchangeManagerTest.java`

---

## Skeleton Code

### Production class — ExchangeException.java

```java
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// Time: Created - TASK-WS1.06
package kafka.server.http.routing;

/**
 * Exception for exchange operations with typed error codes.
 *
 * // Time: Created - TASK-WS1.06
 */
public class ExchangeException extends Exception {

    public enum ErrorCode {
        EXCHANGE_NOT_FOUND,
        EXCHANGE_TYPE_MISMATCH,
        EXCHANGE_PROTECTED,
        EXCHANGE_IN_USE,
        EXCHANGE_LIMIT_EXCEEDED
    }

    private final ErrorCode errorCode;

    public ExchangeException(ErrorCode errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    public ErrorCode errorCode() {
        return errorCode;
    }
}
```

### Production class — ExchangeManager.java

```java
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// Time: Created - TASK-WS1.06
package kafka.server.http.routing;

import kafka.server.http.ws.ExchangeMetadata;
import kafka.server.http.ws.WsConfigs;
import kafka.server.http.ws.WsRoutingMetadataManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Manages exchange lifecycle: declare, delete, get, list.
 * Delegates persistence to {@link WsRoutingMetadataManager}.
 *
 * // Time: Created - TASK-WS1.06
 */
public class ExchangeManager {

    private static final Logger log = LoggerFactory.getLogger(ExchangeManager.class);

    private static final Set<String> DEFAULT_EXCHANGE_NAMES = Set.of(
        "", "amq.direct", "amq.topic", "amq.fanout", "amq.headers");

    private final WsRoutingMetadataManager metadataManager;
    private final WsConfigs wsConfigs;

    public ExchangeManager(WsRoutingMetadataManager metadataManager, WsConfigs wsConfigs) {
        this.metadataManager = Objects.requireNonNull(metadataManager, "metadataManager");
        this.wsConfigs = Objects.requireNonNull(wsConfigs, "wsConfigs");
    }

    public void initializeDefaults(String vhost) {
        // TODO: register 5 default exchanges in metadataManager cache
        // Do NOT write to metadata topic — defaults are synthetic
        throw new UnsupportedOperationException("Not yet implemented");
    }

    public ExchangeMetadata declareExchange(
            String vhost, String name, String type, boolean durable,
            boolean autoDelete, boolean passive, boolean internal,
            Map<String, String> arguments) throws ExchangeException {
        // TODO: implement declare logic
        // 1. Passive declare: check existence only
        // 2. Existing exchange: check type match
        // 3. New exchange: check limit, write to metadata
        throw new UnsupportedOperationException("Not yet implemented");
    }

    public void deleteExchange(String vhost, String name, boolean ifUnused) throws ExchangeException {
        // TODO: implement delete logic
        // 1. Protected check
        // 2. Existence check (no-op if not found)
        // 3. If ifUnused: check bindings
        // 4. Delete from metadata
        throw new UnsupportedOperationException("Not yet implemented");
    }

    public ExchangeMetadata getExchange(String vhost, String name) {
        return metadataManager.getExchange(vhost, name);
    }

    public Collection<ExchangeMetadata> listExchanges(String vhost) {
        return metadataManager.listExchanges(vhost);
    }

    public boolean isDefaultExchange(String name) {
        return DEFAULT_EXCHANGE_NAMES.contains(name);
    }
}
```

### Test class

```java
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// Time: Created - TASK-WS1.06
package kafka.server.http.routing;

import kafka.server.http.ws.ExchangeMetadata;
import kafka.server.http.ws.WsConfigs;
import kafka.server.http.ws.WsRoutingMetadataManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * // Time: Created - TASK-WS1.06
 */
class ExchangeManagerTest {

    private ExchangeManager exchangeManager;
    private WsRoutingMetadataManager metadataManager;

    @BeforeEach
    void setUp() {
        metadataManager = new WsRoutingMetadataManager(
            WsConfigs.withDefaults(),
            (key, value) -> {});
        exchangeManager = new ExchangeManager(metadataManager, WsConfigs.withDefaults());
    }

    @Test
    void initializeDefaults_creates5Exchanges() {
        exchangeManager.initializeDefaults("/");
        assertNotNull(exchangeManager.getExchange("/", ""));
        assertNotNull(exchangeManager.getExchange("/", "amq.direct"));
        assertNotNull(exchangeManager.getExchange("/", "amq.topic"));
        assertNotNull(exchangeManager.getExchange("/", "amq.fanout"));
        assertNotNull(exchangeManager.getExchange("/", "amq.headers"));
    }

    @Test
    void initializeDefaults_correctTypes() {
        exchangeManager.initializeDefaults("/");
        assertEquals("direct", exchangeManager.getExchange("/", "").type());
        assertEquals("direct", exchangeManager.getExchange("/", "amq.direct").type());
        assertEquals("topic", exchangeManager.getExchange("/", "amq.topic").type());
        assertEquals("fanout", exchangeManager.getExchange("/", "amq.fanout").type());
        assertEquals("headers", exchangeManager.getExchange("/", "amq.headers").type());
    }

    @Test
    void declareExchange_newExchange_succeeds() throws ExchangeException {
        exchangeManager.initializeDefaults("/");
        ExchangeMetadata result = exchangeManager.declareExchange(
            "/", "events", "topic", true, false, false, false, Collections.emptyMap());
        assertNotNull(result);
        assertEquals("topic", result.type());
    }

    @Test
    void declareExchange_sameType_idempotent() throws ExchangeException {
        exchangeManager.initializeDefaults("/");
        exchangeManager.declareExchange("/", "events", "topic", true, false, false, false, Collections.emptyMap());
        ExchangeMetadata result = exchangeManager.declareExchange(
            "/", "events", "topic", true, false, false, false, Collections.emptyMap());
        assertNotNull(result);
    }

    @Test
    void declareExchange_differentType_throwsMismatch() throws ExchangeException {
        exchangeManager.initializeDefaults("/");
        exchangeManager.declareExchange("/", "events", "topic", true, false, false, false, Collections.emptyMap());
        ExchangeException ex = assertThrows(ExchangeException.class, () ->
            exchangeManager.declareExchange("/", "events", "direct", true, false, false, false, Collections.emptyMap()));
        assertEquals(ExchangeException.ErrorCode.EXCHANGE_TYPE_MISMATCH, ex.errorCode());
    }

    @Test
    void declareExchange_passive_existingExchange_succeeds() throws ExchangeException {
        exchangeManager.initializeDefaults("/");
        ExchangeMetadata result = exchangeManager.declareExchange(
            "/", "amq.direct", "direct", true, false, true, false, Collections.emptyMap());
        assertNotNull(result);
    }

    @Test
    void declareExchange_passive_nonExistent_throwsNotFound() {
        exchangeManager.initializeDefaults("/");
        ExchangeException ex = assertThrows(ExchangeException.class, () ->
            exchangeManager.declareExchange("/", "nonexistent", "direct", true, false, true, false, Collections.emptyMap()));
        assertEquals(ExchangeException.ErrorCode.EXCHANGE_NOT_FOUND, ex.errorCode());
    }

    @Test
    void deleteExchange_defaultExchange_throwsProtected() {
        exchangeManager.initializeDefaults("/");
        ExchangeException ex = assertThrows(ExchangeException.class, () ->
            exchangeManager.deleteExchange("/", "amq.direct", false));
        assertEquals(ExchangeException.ErrorCode.EXCHANGE_PROTECTED, ex.errorCode());
    }

    @Test
    void deleteExchange_nonExistent_noOp() throws ExchangeException {
        exchangeManager.initializeDefaults("/");
        assertDoesNotThrow(() -> exchangeManager.deleteExchange("/", "nonexistent", false));
    }

    @Test
    void deleteExchange_existingExchange_removesFromCache() throws ExchangeException {
        exchangeManager.initializeDefaults("/");
        exchangeManager.declareExchange("/", "events", "topic", true, false, false, false, Collections.emptyMap());
        exchangeManager.deleteExchange("/", "events", false);
        assertNull(exchangeManager.getExchange("/", "events"));
    }

    @Test
    void isDefaultExchange_trueForDefaults() {
        assertTrue(exchangeManager.isDefaultExchange(""));
        assertTrue(exchangeManager.isDefaultExchange("amq.direct"));
        assertTrue(exchangeManager.isDefaultExchange("amq.topic"));
        assertTrue(exchangeManager.isDefaultExchange("amq.fanout"));
        assertTrue(exchangeManager.isDefaultExchange("amq.headers"));
    }

    @Test
    void isDefaultExchange_falseForUserExchanges() {
        assertFalse(exchangeManager.isDefaultExchange("events"));
        assertFalse(exchangeManager.isDefaultExchange("my-exchange"));
    }

    @Test
    void declareExchange_exceedsLimit_throwsLimitExceeded() throws ExchangeException {
        // Create a manager with very low limit for testing
        WsConfigs lowLimitConfigs = new WsConfigs(
            true, 1048576, 256, 100, 10000,
            "ws.", 1, "__ws_routing_metadata", 3,
            1000, "latest", 8, 500, 1048576,
            30000, 600000L, 5000, 10, false,
            10000, 60000L, 10000,
            6, // maxExchangesPerVhost = 6 (5 defaults + 1)
            10000, 10000, 50, 300000L, 30000L);
        ExchangeManager limitedManager = new ExchangeManager(
            new WsRoutingMetadataManager(lowLimitConfigs, (k, v) -> {}), lowLimitConfigs);
        limitedManager.initializeDefaults("/");
        // 5 defaults + 1 user exchange = 6 (at limit)
        limitedManager.declareExchange("/", "one", "direct", true, false, false, false, Collections.emptyMap());
        // 7th should fail
        ExchangeException ex = assertThrows(ExchangeException.class, () ->
            limitedManager.declareExchange("/", "two", "direct", true, false, false, false, Collections.emptyMap()));
        assertEquals(ExchangeException.ErrorCode.EXCHANGE_LIMIT_EXCEEDED, ex.errorCode());
    }
}
```

### Existing pattern reference

```java
// Design doc §5.4.1 — Declare exchange semantics:
//
// Idempotency: same name + same type → no-op
// Mismatch: same name + different type → EXCHANGE_TYPE_MISMATCH
// Passive: check existence only, no creation
// Pre-declared: "", amq.direct, amq.topic, amq.fanout, amq.headers — always exist, cannot be deleted
```

---

## Tests

**Test class:** `http-server/src/test/java/kafka/server/http/routing/ExchangeManagerTest.java`

| Test method | What it verifies |
|-------------|-----------------|
| `initializeDefaults_creates5Exchanges` | 5 default exchanges exist after init |
| `initializeDefaults_correctTypes` | Default exchanges have correct types |
| `declareExchange_newExchange_succeeds` | New exchange created successfully |
| `declareExchange_sameType_idempotent` | Re-declare with same type is no-op |
| `declareExchange_differentType_throwsMismatch` | Type mismatch error |
| `declareExchange_passive_existingExchange_succeeds` | Passive declare on existing exchange |
| `declareExchange_passive_nonExistent_throwsNotFound` | Passive declare on missing exchange |
| `deleteExchange_defaultExchange_throwsProtected` | Cannot delete default exchanges |
| `deleteExchange_nonExistent_noOp` | Deleting missing exchange is silent |
| `deleteExchange_existingExchange_removesFromCache` | Delete removes from cache |
| `isDefaultExchange_trueForDefaults` | 5 defaults identified correctly |
| `isDefaultExchange_falseForUserExchanges` | User exchanges not marked as default |
| `declareExchange_exceedsLimit_throwsLimitExceeded` | Limit enforcement works |

**Run command:**
```bash
timeout 300 ./gradlew :http-server:test --tests "kafka.server.http.routing.ExchangeManagerTest"
```

---

## Rules

- Default exchanges are NOT written to the metadata topic — they are synthesized at startup.
- Idempotent declare: same name + same type = no-op. Different type = error.
- Default exchanges cannot be deleted (EXCHANGE_PROTECTED).
- Delete on non-existent exchange is a no-op, not an error.
- `ws.max.exchanges.per.vhost` is enforced on declare. Default exchanges count toward the limit.
- The empty-string exchange `""` is a valid exchange name (the default exchange).

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

- [ ] `ExchangeManager.java` exists at `http-server/src/main/java/kafka/server/http/routing/ExchangeManager.java`
- [ ] `ExchangeException.java` exists at `http-server/src/main/java/kafka/server/http/routing/ExchangeException.java`
- [ ] `initializeDefaults("/")` creates 5 exchanges: `""`, `amq.direct`, `amq.topic`, `amq.fanout`, `amq.headers`
- [ ] Passive declare on existing exchange returns it; on missing exchange throws `EXCHANGE_NOT_FOUND`
- [ ] Idempotent re-declare with same type succeeds; different type throws `EXCHANGE_TYPE_MISMATCH`
- [ ] Deleting default exchange throws `EXCHANGE_PROTECTED`
- [ ] Deleting non-existent exchange is a no-op
- [ ] `ws.max.exchanges.per.vhost` limit enforced
- [ ] `isDefaultExchange()` returns true for all 5 defaults
- [ ] `timeout 300 ./gradlew :http-server:test --tests "kafka.server.http.routing.ExchangeManagerTest"` exits 0
- [ ] Learning section filled with at least one entry
- [ ] Limitations section filled (use "None" if truly none)

---

## File Manifest

> Filled by the executing agent after each commit.
> Run: `git diff --name-status HEAD~1 HEAD -- '*.java' '*.xml' '*.json' '*.yaml' '*.yml'`
