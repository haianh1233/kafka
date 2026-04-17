# TASK-WS1.05: WebSocket Routing Metadata Manager

## Prerequisites

- **TASK-WS1.01 completed** — `WsConfigs.java` exists, providing `metadataTopic()`, `metadataReplicationFactor()`, `metadataStartupTimeoutMs()`.

---

## Context

The routing engine (exchanges, queues, bindings) needs durable persistence across broker restarts and consistency across all brokers in the cluster. Following the same pattern as Kafka's `GroupCoordinator` replaying `__consumer_offsets`, the `WsRoutingMetadataManager` uses an internal compacted Kafka topic (`__ws_routing_metadata`) to store all routing metadata.

### Design doc §14 — Metadata Storage

A single compacted Kafka topic stores all routing metadata:

```
Topic: __ws_routing_metadata
Partitions: 1 (total ordering for metadata operations)
Cleanup policy: compact
Replication factor: min(3, cluster size)

Record key formats:
  "exchange:{vhost}:{name}"
  "queue:{vhost}:{name}"
  "binding:{vhost}:{exchange}:{queue}:{routingKey}:{argsHash}"
  "e2e:{vhost}:{source}:{destination}:{routingKey}:{argsHash}"

Record value:
  JSON metadata (exchange type, queue arguments, binding arguments)
  null value = tombstone (entity deleted)
```

### In-memory cache (§14.2)

`WsRoutingMetadataManager` maintains concurrent in-memory structures:

```java
ConcurrentHashMap<String, Exchange> exchanges;        // vhost:name → exchange config
ConcurrentHashMap<String, Queue> queues;              // vhost:name → queue config
ConcurrentHashMap<String, CopyOnWriteArrayList<Binding>> bindings;  // vhost:exchange → bindings
```

All mutations: (1) write to `__ws_routing_metadata` (durable), then (2) apply to in-memory cache (immediate).

### Startup replay (§14.3)

On broker startup, `WsRoutingMetadataManager` replays `__ws_routing_metadata` from offset 0 to high watermark, rebuilding the in-memory cache. The WebSocket endpoint does not accept connections until replay is complete (same pattern as `GroupCoordinator` replaying `__consumer_offsets`).

If the replay does not complete within `ws.metadata.startup.timeout.ms`, the broker starts with only the 5 default exchanges and logs a warning.

### In this task

This task creates the `WsRoutingMetadataManager` class with:
- Internal model classes: `Exchange`, `Queue`, `Binding` (simple records/value objects)
- Key formatting and parsing
- In-memory cache with concurrent data structures
- Write methods: `writeExchange()`, `writeQueue()`, `writeBinding()`, `deleteExchange()`, `deleteQueue()`, `deleteBinding()`
- Replay method: `replayFromTopic()` (accepts records, applies to cache)

The actual Kafka producer/consumer integration (creating the topic, producing records, consuming for replay) is **stubbed** in this task. The manager accepts a `java.util.function.Consumer<ProducerRecord>` for writes and a `List<ConsumerRecord>` for replay, so it can be tested without a running Kafka cluster.

---

## Specification

**Package:** `kafka.server.http.ws`

### Model classes (inner classes or separate files)

```java
public record ExchangeMetadata(
    String name, String vhost, String type, boolean durable,
    boolean autoDelete, boolean internal, Map<String, String> arguments);

public record QueueMetadata(
    String name, String vhost, boolean durable, boolean exclusive,
    boolean autoDelete, Map<String, String> arguments);

public record BindingMetadata(
    String vhost, String exchange, String queue, String routingKey,
    Map<String, String> arguments);
```

### Manager class

```java
public class WsRoutingMetadataManager {

    public WsRoutingMetadataManager(WsConfigs wsConfigs);

    // --- Write methods (persist + cache update) ---
    public void writeExchange(ExchangeMetadata exchange);
    public void writeQueue(QueueMetadata queue);
    public void writeBinding(BindingMetadata binding);
    public void deleteExchange(String vhost, String name);
    public void deleteQueue(String vhost, String name);
    public void deleteBinding(String vhost, String exchange, String queue,
                              String routingKey, Map<String, String> arguments);

    // --- Read methods (from cache) ---
    public ExchangeMetadata getExchange(String vhost, String name);
    public QueueMetadata getQueue(String vhost, String name);
    public List<BindingMetadata> getBindings(String vhost, String exchange);
    public Collection<ExchangeMetadata> listExchanges(String vhost);
    public Collection<QueueMetadata> listQueues(String vhost);
    public int exchangeCount(String vhost);
    public int queueCount(String vhost);
    public int bindingCount(String vhost, String exchange);

    // --- Replay (called during startup) ---
    public void applyRecord(String key, byte[] value);
    public boolean isReplayComplete();

    // --- Key formatting ---
    static String exchangeKey(String vhost, String name);
    static String queueKey(String vhost, String name);
    static String bindingKey(String vhost, String exchange, String queue,
                             String routingKey, Map<String, String> arguments);
    static int argsHash(Map<String, String> arguments);
}
```

**Behavioral contracts:**
- `writeExchange()`: creates key `exchange:{vhost}:{name}`, serializes metadata to JSON, sends to writer, updates cache.
- `deleteExchange()`: creates same key, sends null value (tombstone), removes from cache.
- `applyRecord()`: parses key prefix to determine entity type, deserializes or removes from cache.
- `argsHash()`: deterministic hash of sorted argument entries (for binding key uniqueness).
- Cache lookups are O(1) for exchanges and queues, O(K) for bindings where K = bindings on the exchange.
- Null value in `applyRecord()` means deletion (tombstone).

---

## Implementation Details

**Module:** `http-server`

**Files to study:**

| File | Why |
|------|-----|
| None directly — the pattern is described in the design doc §14 | See context above |

**Files to create:**

| File | What it does |
|------|--------------|
| `http-server/src/main/java/kafka/server/http/ws/WsRoutingMetadataManager.java` | Metadata persistence + cache |
| `http-server/src/main/java/kafka/server/http/ws/ExchangeMetadata.java` | Exchange value object |
| `http-server/src/main/java/kafka/server/http/ws/QueueMetadata.java` | Queue value object |
| `http-server/src/main/java/kafka/server/http/ws/BindingMetadata.java` | Binding value object |

**Files to modify:**

None.

> **CRITICAL:** The `argsHash()` method must be deterministic: sort arguments by key, concatenate `key=value` pairs, and compute a hash. Two bindings with the same exchange, queue, routing key, and arguments (in any order) must produce the same key. Use a simple algorithm: `Objects.hash(sortedEntries.toString())`.

> **CRITICAL:** For replay, null values (tombstones) must REMOVE the entity from the cache. The replay processes records in order, so a later tombstone correctly overrides an earlier write.

> **CRITICAL:** The model classes should be plain Java classes (not records) for JDK 11 compatibility if the project requires it. Check the project's source level. If JDK 17+ is confirmed, records are fine.

**Implementation order:**
1. Create model classes (`ExchangeMetadata`, `QueueMetadata`, `BindingMetadata`)
2. Create `WsRoutingMetadataManager` with key formatting, cache, write/read methods
3. Create test class

---

## Skeleton Code

### Production class — ExchangeMetadata.java

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
// Time: Created - TASK-WS1.05
package kafka.server.http.ws;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * Immutable exchange metadata. Stored in routing metadata topic and in-memory cache.
 *
 * // Time: Created - TASK-WS1.05
 */
public final class ExchangeMetadata {

    private final String name;
    private final String vhost;
    private final String type; // "direct", "topic", "fanout", "headers"
    private final boolean durable;
    private final boolean autoDelete;
    private final boolean internal;
    private final Map<String, String> arguments;

    public ExchangeMetadata(String name, String vhost, String type, boolean durable,
                            boolean autoDelete, boolean internal, Map<String, String> arguments) {
        this.name = Objects.requireNonNull(name, "name");
        this.vhost = Objects.requireNonNull(vhost, "vhost");
        this.type = Objects.requireNonNull(type, "type");
        this.durable = durable;
        this.autoDelete = autoDelete;
        this.internal = internal;
        this.arguments = arguments == null ? Collections.emptyMap() : Collections.unmodifiableMap(arguments);
    }

    public String name() { return name; }
    public String vhost() { return vhost; }
    public String type() { return type; }
    public boolean durable() { return durable; }
    public boolean autoDelete() { return autoDelete; }
    public boolean internal() { return internal; }
    public Map<String, String> arguments() { return arguments; }
}
```

### Production class — WsRoutingMetadataManager.java

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
// Time: Created - TASK-WS1.05
package kafka.server.http.ws;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

/**
 * Manages routing metadata (exchanges, queues, bindings) backed by the
 * __ws_routing_metadata compacted Kafka topic.
 *
 * // Time: Created - TASK-WS1.05
 */
public class WsRoutingMetadataManager {

    private static final Logger log = LoggerFactory.getLogger(WsRoutingMetadataManager.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String EXCHANGE_PREFIX = "exchange:";
    private static final String QUEUE_PREFIX = "queue:";
    private static final String BINDING_PREFIX = "binding:";

    private final WsConfigs wsConfigs;
    private final AtomicBoolean replayComplete = new AtomicBoolean(false);

    // In-memory cache
    private final ConcurrentHashMap<String, ExchangeMetadata> exchanges = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, QueueMetadata> queues = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, CopyOnWriteArrayList<BindingMetadata>> bindings = new ConcurrentHashMap<>();

    // Pluggable writer for testability (key, value bytes — null value = tombstone)
    private final BiConsumer<String, byte[]> recordWriter;

    public WsRoutingMetadataManager(WsConfigs wsConfigs, BiConsumer<String, byte[]> recordWriter) {
        this.wsConfigs = Objects.requireNonNull(wsConfigs, "wsConfigs");
        this.recordWriter = Objects.requireNonNull(recordWriter, "recordWriter");
    }

    // --- Write methods ---
    public void writeExchange(ExchangeMetadata exchange) {
        // TODO: serialize to JSON, write via recordWriter, update cache
        throw new UnsupportedOperationException("Not yet implemented");
    }

    public void deleteExchange(String vhost, String name) {
        // TODO: write tombstone, remove from cache
        throw new UnsupportedOperationException("Not yet implemented");
    }

    public void writeQueue(QueueMetadata queue) {
        // TODO: serialize to JSON, write via recordWriter, update cache
        throw new UnsupportedOperationException("Not yet implemented");
    }

    public void deleteQueue(String vhost, String name) {
        // TODO: write tombstone, remove from cache
        throw new UnsupportedOperationException("Not yet implemented");
    }

    public void writeBinding(BindingMetadata binding) {
        // TODO: serialize to JSON, write via recordWriter, update cache
        throw new UnsupportedOperationException("Not yet implemented");
    }

    public void deleteBinding(String vhost, String exchange, String queue,
                              String routingKey, Map<String, String> arguments) {
        // TODO: write tombstone, remove from bindings list
        throw new UnsupportedOperationException("Not yet implemented");
    }

    // --- Read methods ---
    public ExchangeMetadata getExchange(String vhost, String name) {
        return exchanges.get(vhost + ":" + name);
    }

    public QueueMetadata getQueue(String vhost, String name) {
        return queues.get(vhost + ":" + name);
    }

    public List<BindingMetadata> getBindings(String vhost, String exchange) {
        CopyOnWriteArrayList<BindingMetadata> list = bindings.get(vhost + ":" + exchange);
        return list == null ? Collections.emptyList() : Collections.unmodifiableList(list);
    }

    public Collection<ExchangeMetadata> listExchanges(String vhost) {
        // TODO: filter by vhost
        throw new UnsupportedOperationException("Not yet implemented");
    }

    public Collection<QueueMetadata> listQueues(String vhost) {
        // TODO: filter by vhost
        throw new UnsupportedOperationException("Not yet implemented");
    }

    public int exchangeCount(String vhost) {
        // TODO: count exchanges in vhost
        throw new UnsupportedOperationException("Not yet implemented");
    }

    public int queueCount(String vhost) {
        // TODO: count queues in vhost
        throw new UnsupportedOperationException("Not yet implemented");
    }

    public int bindingCount(String vhost, String exchange) {
        List<BindingMetadata> list = bindings.get(vhost + ":" + exchange);
        return list == null ? 0 : list.size();
    }

    // --- Replay ---
    public void applyRecord(String key, byte[] value) {
        // TODO: parse key prefix, deserialize or remove from cache
        throw new UnsupportedOperationException("Not yet implemented");
    }

    public void markReplayComplete() {
        replayComplete.set(true);
    }

    public boolean isReplayComplete() {
        return replayComplete.get();
    }

    // --- Key formatting ---
    static String exchangeKey(String vhost, String name) {
        return EXCHANGE_PREFIX + vhost + ":" + name;
    }

    static String queueKey(String vhost, String name) {
        return QUEUE_PREFIX + vhost + ":" + name;
    }

    static String bindingKey(String vhost, String exchange, String queue,
                             String routingKey, Map<String, String> arguments) {
        return BINDING_PREFIX + vhost + ":" + exchange + ":" + queue + ":"
                + routingKey + ":" + argsHash(arguments);
    }

    static int argsHash(Map<String, String> arguments) {
        if (arguments == null || arguments.isEmpty()) return 0;
        TreeMap<String, String> sorted = new TreeMap<>(arguments);
        return Objects.hash(sorted.toString());
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
// Time: Created - TASK-WS1.05
package kafka.server.http.ws;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * // Time: Created - TASK-WS1.05
 */
class WsRoutingMetadataManagerTest {

    private WsRoutingMetadataManager manager;
    private List<Map.Entry<String, byte[]>> writtenRecords;

    @BeforeEach
    void setUp() {
        writtenRecords = new ArrayList<>();
        manager = new WsRoutingMetadataManager(
            WsConfigs.withDefaults(),
            (key, value) -> writtenRecords.add(Map.entry(key, value != null ? value : new byte[0])));
    }

    @Test
    void writeExchange_addsToCache() {
        ExchangeMetadata ex = new ExchangeMetadata("events", "/", "topic", true, false, false, Collections.emptyMap());
        manager.writeExchange(ex);
        assertNotNull(manager.getExchange("/", "events"));
        assertEquals("topic", manager.getExchange("/", "events").type());
    }

    @Test
    void deleteExchange_removesFromCache() {
        ExchangeMetadata ex = new ExchangeMetadata("events", "/", "topic", true, false, false, Collections.emptyMap());
        manager.writeExchange(ex);
        manager.deleteExchange("/", "events");
        assertNull(manager.getExchange("/", "events"));
    }

    @Test
    void writeBinding_addsToBindingsList() {
        BindingMetadata b = new BindingMetadata("/", "events", "q1", "order.#", Collections.emptyMap());
        manager.writeBinding(b);
        assertEquals(1, manager.getBindings("/", "events").size());
    }

    @Test
    void exchangeKey_format() {
        assertEquals("exchange:/:/events", WsRoutingMetadataManager.exchangeKey("/", "events"));
    }

    @Test
    void queueKey_format() {
        assertEquals("queue:/:/orders", WsRoutingMetadataManager.queueKey("/", "orders"));
    }

    @Test
    void argsHash_deterministic() {
        Map<String, String> args1 = new HashMap<>();
        args1.put("b", "2");
        args1.put("a", "1");
        Map<String, String> args2 = new HashMap<>();
        args2.put("a", "1");
        args2.put("b", "2");
        assertEquals(WsRoutingMetadataManager.argsHash(args1), WsRoutingMetadataManager.argsHash(args2));
    }

    @Test
    void argsHash_emptyReturnsZero() {
        assertEquals(0, WsRoutingMetadataManager.argsHash(Collections.emptyMap()));
        assertEquals(0, WsRoutingMetadataManager.argsHash(null));
    }

    @Test
    void applyRecord_exchangeWrite_populatesCache() {
        // TODO: create JSON bytes for an exchange, call applyRecord, verify cache
    }

    @Test
    void applyRecord_tombstone_removesFromCache() {
        // TODO: write exchange, then apply null value, verify removed
    }

    @Test
    void replayComplete_defaultFalse() {
        assertFalse(manager.isReplayComplete());
    }

    @Test
    void markReplayComplete_setsTrue() {
        manager.markReplayComplete();
        assertTrue(manager.isReplayComplete());
    }

    @Test
    void writeExchange_writesToRecordWriter() {
        ExchangeMetadata ex = new ExchangeMetadata("test", "/", "direct", true, false, false, Collections.emptyMap());
        manager.writeExchange(ex);
        assertFalse(writtenRecords.isEmpty());
        assertTrue(writtenRecords.get(0).getKey().startsWith("exchange:"));
    }
}
```

### Existing pattern reference

```java
// Design doc §14.2 — In-memory cache structure:

ConcurrentHashMap<String, Exchange> exchanges;
ConcurrentHashMap<String, Queue> queues;
ConcurrentHashMap<String, CopyOnWriteArrayList<Binding>> bindings;

// All mutations: (1) write to __ws_routing_metadata, (2) apply to cache
```

---

## Tests

**Test class:** `http-server/src/test/java/kafka/server/http/ws/WsRoutingMetadataManagerTest.java`

| Test method | What it verifies |
|-------------|-----------------|
| `writeExchange_addsToCache` | Exchange write updates in-memory cache |
| `deleteExchange_removesFromCache` | Tombstone removes from cache |
| `writeBinding_addsToBindingsList` | Binding added to exchange's binding list |
| `exchangeKey_format` | Key format: `exchange:{vhost}:{name}` |
| `queueKey_format` | Key format: `queue:{vhost}:{name}` |
| `argsHash_deterministic` | Same args in different order → same hash |
| `argsHash_emptyReturnsZero` | Empty/null args → 0 |
| `applyRecord_exchangeWrite_populatesCache` | Replay applies exchange to cache |
| `applyRecord_tombstone_removesFromCache` | Replay removes on null value |
| `replayComplete_defaultFalse` | Replay not complete by default |
| `markReplayComplete_setsTrue` | Can mark replay as complete |
| `writeExchange_writesToRecordWriter` | Writer receives the record |

**Run command:**
```bash
timeout 300 ./gradlew :http-server:test --tests "kafka.server.http.ws.WsRoutingMetadataManagerTest"
```

---

## Rules

- `argsHash()` must be deterministic: sort by key, then hash.
- Null value in `applyRecord()` = tombstone = delete from cache.
- The record writer is a `BiConsumer<String, byte[]>` for testability — actual Kafka producer integration is a separate task.
- Model classes are immutable — arguments map wrapped in `Collections.unmodifiableMap()`.
- Thread safety via ConcurrentHashMap and CopyOnWriteArrayList.

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

- [ ] `WsRoutingMetadataManager.java` exists at `http-server/src/main/java/kafka/server/http/ws/WsRoutingMetadataManager.java`
- [ ] `ExchangeMetadata.java`, `QueueMetadata.java`, `BindingMetadata.java` exist in same package
- [ ] Key format: `exchange:{vhost}:{name}`, `queue:{vhost}:{name}`, `binding:{vhost}:{exchange}:{queue}:{routingKey}:{argsHash}`
- [ ] `writeExchange()` persists to writer AND updates cache
- [ ] `deleteExchange()` writes tombstone (null) AND removes from cache
- [ ] `applyRecord()` handles both write and tombstone cases
- [ ] `argsHash()` is deterministic regardless of map iteration order
- [ ] `isReplayComplete()` / `markReplayComplete()` work correctly
- [ ] `timeout 300 ./gradlew :http-server:test --tests "kafka.server.http.ws.WsRoutingMetadataManagerTest"` exits 0
- [ ] Learning section filled with at least one entry
- [ ] Limitations section filled (use "None" if truly none)

---

## File Manifest

> Filled by the executing agent after each commit.
> Run: `git diff --name-status HEAD~1 HEAD -- '*.java' '*.xml' '*.json' '*.yaml' '*.yml'`
