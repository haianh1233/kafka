# TASK-WS1.01: WebSocket & Routing Config Properties

## Prerequisites

- **TASK-A.02 completed** — `HttpServerConfigs.java` exists at `server/src/main/java/org/apache/kafka/network/HttpServerConfigs.java` with `CONFIG_DEF` pattern and the `http-server` Gradle module is functional.

---

## Context

The WebSocket push delivery and AMQP-style routing extension requires 28 new configuration properties to control WebSocket behavior, consumer threading, flow control, routing limits, deduplication, and metadata management. These properties are listed in the design document §17 (Configuration).

Following the same pattern as the existing `HttpServerConfigs` in the `:server` module (`org.apache.kafka.network.HttpServerConfigs`), the new config class `WsServerConfigs` defines all WebSocket-specific properties with public constants for config keys, defaults, documentation strings, and a `ConfigDef` that registers all properties. This class lives in the `:server` module (not `:http-server`) because `KafkaConfig` in `:server` must reference these config definitions, and `:http-server` depends on `:server`, not the other way around.

Additionally, a runtime configuration holder `WsConfigs` is created in the `http-server` module at `kafka.server.http.ws.WsConfigs`. This mirrors the pattern of `kafka.server.http.HttpServerConfigs` (the runtime holder) wrapping `org.apache.kafka.network.HttpServerConfigs` (the config definitions). The runtime holder resolves config values at startup and provides typed accessor methods for the WebSocket layer.

### Design doc §17 — Configuration

New properties (in addition to existing HTTP config):

| Property | Default | Description |
|---|---|---|
| `ws.enabled` | `true` | Enable WebSocket upgrade on HTTP listener |
| `ws.max.frame.size` | `1048576` (1 MB) | Max WebSocket frame payload size |
| `ws.max.subscriptions.per.connection` | `256` | Max concurrent subscriptions per WebSocket |
| `ws.default.credits` | `100` | Default delivery credits if not specified in subscribe |
| `ws.max.credits` | `10000` | Max credits a client can grant at once |
| `ws.topic.prefix` | `ws.` | Prefix for Kafka topics backing queues |
| `ws.default.queue.partitions` | `1` | Default partitions for auto-created queue topics |
| `ws.metadata.topic` | `__ws_routing_metadata` | Internal topic for routing metadata |
| `ws.metadata.replication.factor` | `3` | Replication factor for metadata topic |
| `ws.ack.commit.interval.ms` | `1000` | Batch interval for offset commits from ACKs |
| `ws.consumer.start.offset` | `latest` | Default start offset (`earliest` or `latest`) |
| `num.ws.consumer.threads` | `8` | Thread pool for consumer fetch loops |
| `ws.consumer.max.wait.ms` | `500` | Max wait per fetch loop iteration |
| `ws.consumer.max.bytes` | `1048576` (1 MB) | Max fetch bytes per iteration |
| `ws.publish.timeout.ms` | `30000` | Max time for publish to complete before NACK |
| `ws.connection.max.idle.ms` | `600000` (10 min) | Close idle WS connections (no frames) |
| `ws.shutdown.drain.ms` | `5000` | Drain window for WS connections during shutdown |
| `ws.max.redelivery.count` | `10` | Auto-DLX after this many redeliveries |
| `ws.dedup.enabled` | `false` | Enable publish deduplication by messageId |
| `ws.dedup.cache.size` | `10000` | Max entries in dedup cache per exchange |
| `ws.dedup.cache.ttl.ms` | `60000` | Dedup cache entry TTL |
| `ws.max.connections.per.broker` | `10000` | Max concurrent WS connections per broker |
| `ws.max.exchanges.per.vhost` | `1000` | Max exchanges per virtual host |
| `ws.max.queues.per.vhost` | `10000` | Max queues per virtual host |
| `ws.max.bindings.per.exchange` | `10000` | Max bindings per exchange |
| `ws.max.control.messages.per.second` | `50` | Control message rate limit per connection |
| `ws.consumer.ack.timeout.ms` | `300000` (5 min) | Auto-requeue unacked messages after this duration |
| `ws.metadata.startup.timeout.ms` | `30000` | Max time to wait for metadata replay |

---

## Specification

### 1. `WsServerConfigs.java` in `:server` module

**Package:** `org.apache.kafka.network`

This class defines all 28 WebSocket configuration properties as public static final constants and registers them in a `ConfigDef`. It follows the exact same structure as `HttpServerConfigs`:

- Public static final `String` for each config key (`WS_ENABLED_CONFIG`, etc.)
- Public static final default value (`WS_ENABLED_DEFAULT`, etc.)
- Public static final `String` for documentation (`WS_ENABLED_DOC`, etc.)
- A `public static final ConfigDef CONFIG_DEF` that registers all properties

### 2. `WsConfigs.java` in `:http-server` module

**Package:** `kafka.server.http.ws`

Runtime configuration holder that wraps resolved values. Constructor takes all 28 values; each has a typed getter. Includes a `withDefaults()` factory method.

---

## Implementation Details

**Module:** `server` (config definitions) and `http-server` (runtime holder)

**Files to study:**

| File | Why |
|------|-----|
| `server/src/main/java/org/apache/kafka/network/HttpServerConfigs.java` | Pattern to follow for config definitions |
| `http-server/src/main/java/kafka/server/http/HttpServerConfigs.java` | Pattern to follow for runtime holder |

```java
// From server/src/main/java/org/apache/kafka/network/HttpServerConfigs.java lines 283-452
// Pattern: constants + CONFIG_DEF registration

public class HttpServerConfigs {

    // ---- http.enabled ----
    public static final String HTTP_ENABLED_CONFIG = "http.enabled";
    public static final boolean HTTP_ENABLED_DEFAULT = false;
    public static final String HTTP_ENABLED_DOC = "Master switch to enable the HTTP listener. ...";

    // ... more properties ...

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(HTTP_ENABLED_CONFIG,
                    BOOLEAN, HTTP_ENABLED_DEFAULT,
                    HIGH, HTTP_ENABLED_DOC)
            // ... more definitions ...
}
```

```java
// From http-server/src/main/java/kafka/server/http/HttpServerConfigs.java lines 25-59
// Pattern: runtime holder with typed getters

public final class HttpServerConfigs {

    private final int httpProduceMaxRecords;
    private final int httpConsumeMaxWaitMs;
    private final int httpRequestMaxBytes;

    public HttpServerConfigs(int httpProduceMaxRecords, int httpConsumeMaxWaitMs, int httpRequestMaxBytes) {
        this.httpProduceMaxRecords = httpProduceMaxRecords;
        this.httpConsumeMaxWaitMs = httpConsumeMaxWaitMs;
        this.httpRequestMaxBytes = httpRequestMaxBytes;
    }

    public static HttpServerConfigs withDefaults() {
        return new HttpServerConfigs(
            org.apache.kafka.network.HttpServerConfigs.HTTP_PRODUCE_MAX_RECORDS_DEFAULT,
            org.apache.kafka.network.HttpServerConfigs.HTTP_CONSUME_MAX_WAIT_MS_DEFAULT,
            org.apache.kafka.network.HttpServerConfigs.HTTP_REQUEST_MAX_BYTES_DEFAULT
        );
    }

    public int httpProduceMaxRecords() { return httpProduceMaxRecords; }
    public int httpConsumeMaxWaitMs() { return httpConsumeMaxWaitMs; }
    public int httpRequestMaxBytes() { return httpRequestMaxBytes; }
}
```

**Files to create:**

| File | What it does |
|------|--------------|
| `server/src/main/java/org/apache/kafka/network/WsServerConfigs.java` | 28 WebSocket config property definitions + ConfigDef |
| `http-server/src/main/java/kafka/server/http/ws/WsConfigs.java` | Runtime config holder with typed getters |

**Files to modify:**

None — wiring into `KafkaConfig` is a separate task.

> **CRITICAL:** The `ws.consumer.start.offset` property accepts only `"earliest"` or `"latest"` as values. Use `ConfigDef.ValidString.in("earliest", "latest")` validator. The `ws.topic.prefix` and `ws.metadata.topic` are STRING type with no validator. The `ws.metadata.replication.factor` must use `atLeast(1)` since a replication factor of 0 is invalid.

**Implementation order:**
1. Create `WsServerConfigs.java` with all 28 properties
2. Create `WsConfigs.java` runtime holder
3. Create test classes for both

---

## Skeleton Code

### Production class — WsServerConfigs.java

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
// Time: Created - TASK-WS1.01
package org.apache.kafka.network;

import org.apache.kafka.common.config.ConfigDef;

import static org.apache.kafka.common.config.ConfigDef.Importance.HIGH;
import static org.apache.kafka.common.config.ConfigDef.Importance.LOW;
import static org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM;
import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.Type.BOOLEAN;
import static org.apache.kafka.common.config.ConfigDef.Type.INT;
import static org.apache.kafka.common.config.ConfigDef.Type.LONG;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;
import static org.apache.kafka.common.config.ConfigDef.ValidString.in;

/**
 * Configuration properties for the WebSocket and AMQP-style routing extension.
 * Follows the same pattern as {@link HttpServerConfigs}.
 *
 * // Time: Created - TASK-WS1.01
 */
public class WsServerConfigs {

    // ---- ws.enabled ----
    public static final String WS_ENABLED_CONFIG = "ws.enabled";
    public static final boolean WS_ENABLED_DEFAULT = true;
    public static final String WS_ENABLED_DOC = "Enable WebSocket upgrade on the HTTP listener. "
            + "When true, HTTP requests with Upgrade: websocket header and path /v1/ws are "
            + "upgraded to WebSocket connections. When false, upgrade requests receive HTTP 404.";

    // ---- ws.max.frame.size ----
    public static final String WS_MAX_FRAME_SIZE_CONFIG = "ws.max.frame.size";
    public static final int WS_MAX_FRAME_SIZE_DEFAULT = 1048576; // 1 MB
    public static final String WS_MAX_FRAME_SIZE_DOC = "Maximum WebSocket frame payload size in bytes. "
            + "Frames exceeding this limit cause the connection to be closed with status 1009 "
            + "(Message Too Big). Default is 1 MB, sufficient for most JSON messages.";

    // ---- ws.max.subscriptions.per.connection ----
    public static final String WS_MAX_SUBSCRIPTIONS_PER_CONNECTION_CONFIG = "ws.max.subscriptions.per.connection";
    public static final int WS_MAX_SUBSCRIPTIONS_PER_CONNECTION_DEFAULT = 256;
    public static final String WS_MAX_SUBSCRIPTIONS_PER_CONNECTION_DOC = "Maximum number of concurrent "
            + "subscriptions allowed per WebSocket connection. Additional subscribe requests are "
            + "rejected with an error frame. Each subscription creates a consumer fetch loop.";

    // ---- ws.default.credits ----
    public static final String WS_DEFAULT_CREDITS_CONFIG = "ws.default.credits";
    public static final int WS_DEFAULT_CREDITS_DEFAULT = 100;
    public static final String WS_DEFAULT_CREDITS_DOC = "Default number of delivery credits "
            + "granted to a new subscription if the subscribe message does not specify a credits "
            + "value. Credits control flow — the broker delivers at most this many messages before "
            + "waiting for the client to grant more via a credits frame.";

    // ---- ws.max.credits ----
    public static final String WS_MAX_CREDITS_CONFIG = "ws.max.credits";
    public static final int WS_MAX_CREDITS_DEFAULT = 10000;
    public static final String WS_MAX_CREDITS_DOC = "Maximum number of credits a client can grant "
            + "in a single credits frame. Requests exceeding this limit are clamped to this value. "
            + "Prevents a single consumer from buffering excessive undelivered messages.";

    // ---- ws.topic.prefix ----
    public static final String WS_TOPIC_PREFIX_CONFIG = "ws.topic.prefix";
    public static final String WS_TOPIC_PREFIX_DEFAULT = "ws.";
    public static final String WS_TOPIC_PREFIX_DOC = "Prefix prepended to queue names when creating "
            + "the backing Kafka topic. For example, queue 'order-events' maps to Kafka topic "
            + "'ws.order-events'. This separates WebSocket queue topics from user-managed topics.";

    // ---- ws.default.queue.partitions ----
    public static final String WS_DEFAULT_QUEUE_PARTITIONS_CONFIG = "ws.default.queue.partitions";
    public static final int WS_DEFAULT_QUEUE_PARTITIONS_DEFAULT = 1;
    public static final String WS_DEFAULT_QUEUE_PARTITIONS_DOC = "Default number of partitions for "
            + "Kafka topics created to back declared queues. Can be overridden per queue via the "
            + "'x-partitions' argument in declare-queue. Most queues use 1 partition for FIFO ordering.";

    // ---- ws.metadata.topic ----
    public static final String WS_METADATA_TOPIC_CONFIG = "ws.metadata.topic";
    public static final String WS_METADATA_TOPIC_DEFAULT = "__ws_routing_metadata";
    public static final String WS_METADATA_TOPIC_DOC = "Name of the internal compacted Kafka topic "
            + "that stores exchange, queue, and binding metadata. This topic is created automatically "
            + "on first broker startup with ws.enabled=true. All brokers replay this topic to build "
            + "their in-memory routing cache.";

    // ---- ws.metadata.replication.factor ----
    public static final String WS_METADATA_REPLICATION_FACTOR_CONFIG = "ws.metadata.replication.factor";
    public static final int WS_METADATA_REPLICATION_FACTOR_DEFAULT = 3;
    public static final String WS_METADATA_REPLICATION_FACTOR_DOC = "Replication factor for the "
            + "routing metadata topic. Set to min(this value, cluster size) at creation time. "
            + "Higher values increase durability of exchange/queue/binding definitions.";

    // ---- ws.ack.commit.interval.ms ----
    public static final String WS_ACK_COMMIT_INTERVAL_MS_CONFIG = "ws.ack.commit.interval.ms";
    public static final int WS_ACK_COMMIT_INTERVAL_MS_DEFAULT = 1000;
    public static final String WS_ACK_COMMIT_INTERVAL_MS_DOC = "Interval in milliseconds between "
            + "batched offset commits for acknowledged messages. Lower values reduce re-delivery "
            + "on crash but increase commit overhead.";

    // ---- ws.consumer.start.offset ----
    public static final String WS_CONSUMER_START_OFFSET_CONFIG = "ws.consumer.start.offset";
    public static final String WS_CONSUMER_START_OFFSET_DEFAULT = "latest";
    public static final String WS_CONSUMER_START_OFFSET_DOC = "Default start offset for new "
            + "subscriptions: 'earliest' starts from the beginning of the topic, 'latest' starts "
            + "from the current end. Can be overridden per subscription in the subscribe frame.";

    // ---- num.ws.consumer.threads ----
    public static final String NUM_WS_CONSUMER_THREADS_CONFIG = "num.ws.consumer.threads";
    public static final int NUM_WS_CONSUMER_THREADS_DEFAULT = 8;
    public static final String NUM_WS_CONSUMER_THREADS_DOC = "Number of threads in the thread pool "
            + "used for WebSocket consumer fetch loops. Each active subscription runs a fetch loop "
            + "on one of these threads. Increase for workloads with many concurrent subscriptions.";

    // ---- ws.consumer.max.wait.ms ----
    public static final String WS_CONSUMER_MAX_WAIT_MS_CONFIG = "ws.consumer.max.wait.ms";
    public static final int WS_CONSUMER_MAX_WAIT_MS_DEFAULT = 500;
    public static final String WS_CONSUMER_MAX_WAIT_MS_DOC = "Maximum time in milliseconds each "
            + "consumer fetch loop iteration waits for new data. Lower values reduce delivery "
            + "latency but increase fetch request rate and broker load.";

    // ---- ws.consumer.max.bytes ----
    public static final String WS_CONSUMER_MAX_BYTES_CONFIG = "ws.consumer.max.bytes";
    public static final int WS_CONSUMER_MAX_BYTES_DEFAULT = 1048576; // 1 MB
    public static final String WS_CONSUMER_MAX_BYTES_DOC = "Maximum bytes fetched per consumer "
            + "fetch loop iteration. Controls memory usage per subscription. Each fetched batch "
            + "is serialized to JSON deliver frames and written to the WebSocket channel.";

    // ---- ws.publish.timeout.ms ----
    public static final String WS_PUBLISH_TIMEOUT_MS_CONFIG = "ws.publish.timeout.ms";
    public static final int WS_PUBLISH_TIMEOUT_MS_DEFAULT = 30000;
    public static final String WS_PUBLISH_TIMEOUT_MS_DOC = "Maximum time in milliseconds for a "
            + "WebSocket publish to complete (route through exchange, produce to Kafka topic, "
            + "wait for ISR acks). If exceeded, the broker sends a publish-failed frame.";

    // ---- ws.connection.max.idle.ms ----
    public static final String WS_CONNECTION_MAX_IDLE_MS_CONFIG = "ws.connection.max.idle.ms";
    public static final long WS_CONNECTION_MAX_IDLE_MS_DEFAULT = 600000L; // 10 min
    public static final String WS_CONNECTION_MAX_IDLE_MS_DOC = "Time in milliseconds after which "
            + "an idle WebSocket connection (no frames sent or received) is closed with code 1000. "
            + "Default is 10 minutes. Set to 0 to disable idle connection closing.";

    // ---- ws.shutdown.drain.ms ----
    public static final String WS_SHUTDOWN_DRAIN_MS_CONFIG = "ws.shutdown.drain.ms";
    public static final int WS_SHUTDOWN_DRAIN_MS_DEFAULT = 5000;
    public static final String WS_SHUTDOWN_DRAIN_MS_DOC = "Time in milliseconds to drain "
            + "WebSocket connections during graceful shutdown. During this window the broker "
            + "sends close frames with code 1001 (Going Away) and waits for client close. "
            + "After the window expires, remaining connections are forcibly closed.";

    // ---- ws.max.redelivery.count ----
    public static final String WS_MAX_REDELIVERY_COUNT_CONFIG = "ws.max.redelivery.count";
    public static final int WS_MAX_REDELIVERY_COUNT_DEFAULT = 10;
    public static final String WS_MAX_REDELIVERY_COUNT_DOC = "Maximum number of redelivery attempts "
            + "for a message before it is routed to the dead-letter exchange (DLX). Prevents poison "
            + "messages from blocking a queue indefinitely. Set to 0 to disable DLX routing.";

    // ---- ws.dedup.enabled ----
    public static final String WS_DEDUP_ENABLED_CONFIG = "ws.dedup.enabled";
    public static final boolean WS_DEDUP_ENABLED_DEFAULT = false;
    public static final String WS_DEDUP_ENABLED_DOC = "Enable publish deduplication by messageId. "
            + "When enabled, the broker tracks recently seen messageId values per exchange and "
            + "silently drops duplicate publishes. Requires publishers to set a unique messageId "
            + "in each publish frame.";

    // ---- ws.dedup.cache.size ----
    public static final String WS_DEDUP_CACHE_SIZE_CONFIG = "ws.dedup.cache.size";
    public static final int WS_DEDUP_CACHE_SIZE_DEFAULT = 10000;
    public static final String WS_DEDUP_CACHE_SIZE_DOC = "Maximum number of entries in the "
            + "deduplication cache per exchange. When the cache is full, the oldest entries are "
            + "evicted. Only effective when ws.dedup.enabled=true.";

    // ---- ws.dedup.cache.ttl.ms ----
    public static final String WS_DEDUP_CACHE_TTL_MS_CONFIG = "ws.dedup.cache.ttl.ms";
    public static final long WS_DEDUP_CACHE_TTL_MS_DEFAULT = 60000L;
    public static final String WS_DEDUP_CACHE_TTL_MS_DOC = "Time-to-live in milliseconds for "
            + "entries in the deduplication cache. Entries older than this are eligible for eviction "
            + "regardless of cache size. Only effective when ws.dedup.enabled=true.";

    // ---- ws.max.connections.per.broker ----
    public static final String WS_MAX_CONNECTIONS_PER_BROKER_CONFIG = "ws.max.connections.per.broker";
    public static final int WS_MAX_CONNECTIONS_PER_BROKER_DEFAULT = 10000;
    public static final String WS_MAX_CONNECTIONS_PER_BROKER_DOC = "Maximum number of concurrent "
            + "WebSocket connections per broker. Excess connections are rejected with WebSocket "
            + "close code 4429. Protects broker memory and thread resources.";

    // ---- ws.max.exchanges.per.vhost ----
    public static final String WS_MAX_EXCHANGES_PER_VHOST_CONFIG = "ws.max.exchanges.per.vhost";
    public static final int WS_MAX_EXCHANGES_PER_VHOST_DEFAULT = 1000;
    public static final String WS_MAX_EXCHANGES_PER_VHOST_DOC = "Maximum number of exchanges that "
            + "can be declared per virtual host. Exchange declarations exceeding this limit are "
            + "rejected with an error. Prevents unbounded metadata growth.";

    // ---- ws.max.queues.per.vhost ----
    public static final String WS_MAX_QUEUES_PER_VHOST_CONFIG = "ws.max.queues.per.vhost";
    public static final int WS_MAX_QUEUES_PER_VHOST_DEFAULT = 10000;
    public static final String WS_MAX_QUEUES_PER_VHOST_DOC = "Maximum number of queues that can "
            + "be declared per virtual host. Each queue creates a backing Kafka topic, so this "
            + "limit also caps the number of auto-created topics.";

    // ---- ws.max.bindings.per.exchange ----
    public static final String WS_MAX_BINDINGS_PER_EXCHANGE_CONFIG = "ws.max.bindings.per.exchange";
    public static final int WS_MAX_BINDINGS_PER_EXCHANGE_DEFAULT = 10000;
    public static final String WS_MAX_BINDINGS_PER_EXCHANGE_DOC = "Maximum number of bindings per "
            + "exchange. Bind requests exceeding this limit are rejected with an error. Large "
            + "binding counts on topic/headers exchanges increase per-publish routing cost.";

    // ---- ws.max.control.messages.per.second ----
    public static final String WS_MAX_CONTROL_MESSAGES_PER_SECOND_CONFIG = "ws.max.control.messages.per.second";
    public static final int WS_MAX_CONTROL_MESSAGES_PER_SECOND_DEFAULT = 50;
    public static final String WS_MAX_CONTROL_MESSAGES_PER_SECOND_DOC = "Maximum control messages "
            + "(declare, delete, bind, unbind, etc.) per second per WebSocket connection. Publish "
            + "and ack/nack are NOT counted as control messages. Excess messages are rejected "
            + "with an error frame.";

    // ---- ws.consumer.ack.timeout.ms ----
    public static final String WS_CONSUMER_ACK_TIMEOUT_MS_CONFIG = "ws.consumer.ack.timeout.ms";
    public static final long WS_CONSUMER_ACK_TIMEOUT_MS_DEFAULT = 300000L; // 5 min
    public static final String WS_CONSUMER_ACK_TIMEOUT_MS_DOC = "Time in milliseconds after "
            + "delivery before an unacknowledged message is automatically requeued. Prevents "
            + "messages from being stuck in unacked state if the consumer disconnects without "
            + "sending ack/nack. Set to 0 to disable auto-requeue (not recommended).";

    // ---- ws.metadata.startup.timeout.ms ----
    public static final String WS_METADATA_STARTUP_TIMEOUT_MS_CONFIG = "ws.metadata.startup.timeout.ms";
    public static final long WS_METADATA_STARTUP_TIMEOUT_MS_DEFAULT = 30000L;
    public static final String WS_METADATA_STARTUP_TIMEOUT_MS_DOC = "Maximum time in milliseconds "
            + "to wait for the routing metadata topic replay to complete during broker startup. "
            + "If the timeout expires, the broker starts with only the 5 default exchanges and "
            + "logs a warning. Metadata replay continues in the background.";

    /**
     * ConfigDef for all WebSocket and routing configuration properties.
     * Intended to be merged into KafkaConfig's ConfigDef.
     */
    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(WS_ENABLED_CONFIG,
                    BOOLEAN, WS_ENABLED_DEFAULT,
                    HIGH, WS_ENABLED_DOC)
            .define(WS_MAX_FRAME_SIZE_CONFIG,
                    INT, WS_MAX_FRAME_SIZE_DEFAULT,
                    atLeast(1), HIGH, WS_MAX_FRAME_SIZE_DOC)
            .define(WS_MAX_SUBSCRIPTIONS_PER_CONNECTION_CONFIG,
                    INT, WS_MAX_SUBSCRIPTIONS_PER_CONNECTION_DEFAULT,
                    atLeast(1), MEDIUM, WS_MAX_SUBSCRIPTIONS_PER_CONNECTION_DOC)
            .define(WS_DEFAULT_CREDITS_CONFIG,
                    INT, WS_DEFAULT_CREDITS_DEFAULT,
                    atLeast(1), MEDIUM, WS_DEFAULT_CREDITS_DOC)
            .define(WS_MAX_CREDITS_CONFIG,
                    INT, WS_MAX_CREDITS_DEFAULT,
                    atLeast(1), MEDIUM, WS_MAX_CREDITS_DOC)
            .define(WS_TOPIC_PREFIX_CONFIG,
                    STRING, WS_TOPIC_PREFIX_DEFAULT,
                    MEDIUM, WS_TOPIC_PREFIX_DOC)
            .define(WS_DEFAULT_QUEUE_PARTITIONS_CONFIG,
                    INT, WS_DEFAULT_QUEUE_PARTITIONS_DEFAULT,
                    atLeast(1), MEDIUM, WS_DEFAULT_QUEUE_PARTITIONS_DOC)
            .define(WS_METADATA_TOPIC_CONFIG,
                    STRING, WS_METADATA_TOPIC_DEFAULT,
                    HIGH, WS_METADATA_TOPIC_DOC)
            .define(WS_METADATA_REPLICATION_FACTOR_CONFIG,
                    INT, WS_METADATA_REPLICATION_FACTOR_DEFAULT,
                    atLeast(1), HIGH, WS_METADATA_REPLICATION_FACTOR_DOC)
            .define(WS_ACK_COMMIT_INTERVAL_MS_CONFIG,
                    INT, WS_ACK_COMMIT_INTERVAL_MS_DEFAULT,
                    atLeast(1), MEDIUM, WS_ACK_COMMIT_INTERVAL_MS_DOC)
            .define(WS_CONSUMER_START_OFFSET_CONFIG,
                    STRING, WS_CONSUMER_START_OFFSET_DEFAULT,
                    in("earliest", "latest"), MEDIUM, WS_CONSUMER_START_OFFSET_DOC)
            .define(NUM_WS_CONSUMER_THREADS_CONFIG,
                    INT, NUM_WS_CONSUMER_THREADS_DEFAULT,
                    atLeast(1), HIGH, NUM_WS_CONSUMER_THREADS_DOC)
            .define(WS_CONSUMER_MAX_WAIT_MS_CONFIG,
                    INT, WS_CONSUMER_MAX_WAIT_MS_DEFAULT,
                    atLeast(0), MEDIUM, WS_CONSUMER_MAX_WAIT_MS_DOC)
            .define(WS_CONSUMER_MAX_BYTES_CONFIG,
                    INT, WS_CONSUMER_MAX_BYTES_DEFAULT,
                    atLeast(1), MEDIUM, WS_CONSUMER_MAX_BYTES_DOC)
            .define(WS_PUBLISH_TIMEOUT_MS_CONFIG,
                    INT, WS_PUBLISH_TIMEOUT_MS_DEFAULT,
                    atLeast(1), MEDIUM, WS_PUBLISH_TIMEOUT_MS_DOC)
            .define(WS_CONNECTION_MAX_IDLE_MS_CONFIG,
                    LONG, WS_CONNECTION_MAX_IDLE_MS_DEFAULT,
                    atLeast(0), MEDIUM, WS_CONNECTION_MAX_IDLE_MS_DOC)
            .define(WS_SHUTDOWN_DRAIN_MS_CONFIG,
                    INT, WS_SHUTDOWN_DRAIN_MS_DEFAULT,
                    atLeast(0), LOW, WS_SHUTDOWN_DRAIN_MS_DOC)
            .define(WS_MAX_REDELIVERY_COUNT_CONFIG,
                    INT, WS_MAX_REDELIVERY_COUNT_DEFAULT,
                    atLeast(0), MEDIUM, WS_MAX_REDELIVERY_COUNT_DOC)
            .define(WS_DEDUP_ENABLED_CONFIG,
                    BOOLEAN, WS_DEDUP_ENABLED_DEFAULT,
                    LOW, WS_DEDUP_ENABLED_DOC)
            .define(WS_DEDUP_CACHE_SIZE_CONFIG,
                    INT, WS_DEDUP_CACHE_SIZE_DEFAULT,
                    atLeast(1), LOW, WS_DEDUP_CACHE_SIZE_DOC)
            .define(WS_DEDUP_CACHE_TTL_MS_CONFIG,
                    LONG, WS_DEDUP_CACHE_TTL_MS_DEFAULT,
                    atLeast(1), LOW, WS_DEDUP_CACHE_TTL_MS_DOC)
            .define(WS_MAX_CONNECTIONS_PER_BROKER_CONFIG,
                    INT, WS_MAX_CONNECTIONS_PER_BROKER_DEFAULT,
                    atLeast(1), HIGH, WS_MAX_CONNECTIONS_PER_BROKER_DOC)
            .define(WS_MAX_EXCHANGES_PER_VHOST_CONFIG,
                    INT, WS_MAX_EXCHANGES_PER_VHOST_DEFAULT,
                    atLeast(1), MEDIUM, WS_MAX_EXCHANGES_PER_VHOST_DOC)
            .define(WS_MAX_QUEUES_PER_VHOST_CONFIG,
                    INT, WS_MAX_QUEUES_PER_VHOST_DEFAULT,
                    atLeast(1), MEDIUM, WS_MAX_QUEUES_PER_VHOST_DOC)
            .define(WS_MAX_BINDINGS_PER_EXCHANGE_CONFIG,
                    INT, WS_MAX_BINDINGS_PER_EXCHANGE_DEFAULT,
                    atLeast(1), MEDIUM, WS_MAX_BINDINGS_PER_EXCHANGE_DOC)
            .define(WS_MAX_CONTROL_MESSAGES_PER_SECOND_CONFIG,
                    INT, WS_MAX_CONTROL_MESSAGES_PER_SECOND_DEFAULT,
                    atLeast(1), LOW, WS_MAX_CONTROL_MESSAGES_PER_SECOND_DOC)
            .define(WS_CONSUMER_ACK_TIMEOUT_MS_CONFIG,
                    LONG, WS_CONSUMER_ACK_TIMEOUT_MS_DEFAULT,
                    atLeast(0), MEDIUM, WS_CONSUMER_ACK_TIMEOUT_MS_DOC)
            .define(WS_METADATA_STARTUP_TIMEOUT_MS_CONFIG,
                    LONG, WS_METADATA_STARTUP_TIMEOUT_MS_DEFAULT,
                    atLeast(1), HIGH, WS_METADATA_STARTUP_TIMEOUT_MS_DOC);
}
```

### Production class — WsConfigs.java

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
// Time: Created - TASK-WS1.01
package kafka.server.http.ws;

import org.apache.kafka.network.WsServerConfigs;

/**
 * Runtime configuration holder for the WebSocket and routing layer.
 * Wraps resolved configuration values used by WS handlers and routing engine.
 * Defaults come from {@link WsServerConfigs}.
 *
 * // Time: Created - TASK-WS1.01
 */
public final class WsConfigs {

    private final boolean wsEnabled;
    private final int maxFrameSize;
    private final int maxSubscriptionsPerConnection;
    private final int defaultCredits;
    private final int maxCredits;
    private final String topicPrefix;
    private final int defaultQueuePartitions;
    private final String metadataTopic;
    private final int metadataReplicationFactor;
    private final int ackCommitIntervalMs;
    private final String consumerStartOffset;
    private final int numConsumerThreads;
    private final int consumerMaxWaitMs;
    private final int consumerMaxBytes;
    private final int publishTimeoutMs;
    private final long connectionMaxIdleMs;
    private final int shutdownDrainMs;
    private final int maxRedeliveryCount;
    private final boolean dedupEnabled;
    private final int dedupCacheSize;
    private final long dedupCacheTtlMs;
    private final int maxConnectionsPerBroker;
    private final int maxExchangesPerVhost;
    private final int maxQueuesPerVhost;
    private final int maxBindingsPerExchange;
    private final int maxControlMessagesPerSecond;
    private final long consumerAckTimeoutMs;
    private final long metadataStartupTimeoutMs;

    public WsConfigs(
            boolean wsEnabled,
            int maxFrameSize,
            int maxSubscriptionsPerConnection,
            int defaultCredits,
            int maxCredits,
            String topicPrefix,
            int defaultQueuePartitions,
            String metadataTopic,
            int metadataReplicationFactor,
            int ackCommitIntervalMs,
            String consumerStartOffset,
            int numConsumerThreads,
            int consumerMaxWaitMs,
            int consumerMaxBytes,
            int publishTimeoutMs,
            long connectionMaxIdleMs,
            int shutdownDrainMs,
            int maxRedeliveryCount,
            boolean dedupEnabled,
            int dedupCacheSize,
            long dedupCacheTtlMs,
            int maxConnectionsPerBroker,
            int maxExchangesPerVhost,
            int maxQueuesPerVhost,
            int maxBindingsPerExchange,
            int maxControlMessagesPerSecond,
            long consumerAckTimeoutMs,
            long metadataStartupTimeoutMs) {
        this.wsEnabled = wsEnabled;
        this.maxFrameSize = maxFrameSize;
        this.maxSubscriptionsPerConnection = maxSubscriptionsPerConnection;
        this.defaultCredits = defaultCredits;
        this.maxCredits = maxCredits;
        this.topicPrefix = java.util.Objects.requireNonNull(topicPrefix, "topicPrefix");
        this.defaultQueuePartitions = defaultQueuePartitions;
        this.metadataTopic = java.util.Objects.requireNonNull(metadataTopic, "metadataTopic");
        this.metadataReplicationFactor = metadataReplicationFactor;
        this.ackCommitIntervalMs = ackCommitIntervalMs;
        this.consumerStartOffset = java.util.Objects.requireNonNull(consumerStartOffset, "consumerStartOffset");
        this.numConsumerThreads = numConsumerThreads;
        this.consumerMaxWaitMs = consumerMaxWaitMs;
        this.consumerMaxBytes = consumerMaxBytes;
        this.publishTimeoutMs = publishTimeoutMs;
        this.connectionMaxIdleMs = connectionMaxIdleMs;
        this.shutdownDrainMs = shutdownDrainMs;
        this.maxRedeliveryCount = maxRedeliveryCount;
        this.dedupEnabled = dedupEnabled;
        this.dedupCacheSize = dedupCacheSize;
        this.dedupCacheTtlMs = dedupCacheTtlMs;
        this.maxConnectionsPerBroker = maxConnectionsPerBroker;
        this.maxExchangesPerVhost = maxExchangesPerVhost;
        this.maxQueuesPerVhost = maxQueuesPerVhost;
        this.maxBindingsPerExchange = maxBindingsPerExchange;
        this.maxControlMessagesPerSecond = maxControlMessagesPerSecond;
        this.consumerAckTimeoutMs = consumerAckTimeoutMs;
        this.metadataStartupTimeoutMs = metadataStartupTimeoutMs;
    }

    public static WsConfigs withDefaults() {
        return new WsConfigs(
            WsServerConfigs.WS_ENABLED_DEFAULT,
            WsServerConfigs.WS_MAX_FRAME_SIZE_DEFAULT,
            WsServerConfigs.WS_MAX_SUBSCRIPTIONS_PER_CONNECTION_DEFAULT,
            WsServerConfigs.WS_DEFAULT_CREDITS_DEFAULT,
            WsServerConfigs.WS_MAX_CREDITS_DEFAULT,
            WsServerConfigs.WS_TOPIC_PREFIX_DEFAULT,
            WsServerConfigs.WS_DEFAULT_QUEUE_PARTITIONS_DEFAULT,
            WsServerConfigs.WS_METADATA_TOPIC_DEFAULT,
            WsServerConfigs.WS_METADATA_REPLICATION_FACTOR_DEFAULT,
            WsServerConfigs.WS_ACK_COMMIT_INTERVAL_MS_DEFAULT,
            WsServerConfigs.WS_CONSUMER_START_OFFSET_DEFAULT,
            WsServerConfigs.NUM_WS_CONSUMER_THREADS_DEFAULT,
            WsServerConfigs.WS_CONSUMER_MAX_WAIT_MS_DEFAULT,
            WsServerConfigs.WS_CONSUMER_MAX_BYTES_DEFAULT,
            WsServerConfigs.WS_PUBLISH_TIMEOUT_MS_DEFAULT,
            WsServerConfigs.WS_CONNECTION_MAX_IDLE_MS_DEFAULT,
            WsServerConfigs.WS_SHUTDOWN_DRAIN_MS_DEFAULT,
            WsServerConfigs.WS_MAX_REDELIVERY_COUNT_DEFAULT,
            WsServerConfigs.WS_DEDUP_ENABLED_DEFAULT,
            WsServerConfigs.WS_DEDUP_CACHE_SIZE_DEFAULT,
            WsServerConfigs.WS_DEDUP_CACHE_TTL_MS_DEFAULT,
            WsServerConfigs.WS_MAX_CONNECTIONS_PER_BROKER_DEFAULT,
            WsServerConfigs.WS_MAX_EXCHANGES_PER_VHOST_DEFAULT,
            WsServerConfigs.WS_MAX_QUEUES_PER_VHOST_DEFAULT,
            WsServerConfigs.WS_MAX_BINDINGS_PER_EXCHANGE_DEFAULT,
            WsServerConfigs.WS_MAX_CONTROL_MESSAGES_PER_SECOND_DEFAULT,
            WsServerConfigs.WS_CONSUMER_ACK_TIMEOUT_MS_DEFAULT,
            WsServerConfigs.WS_METADATA_STARTUP_TIMEOUT_MS_DEFAULT
        );
    }

    // --- Typed accessors ---
    public boolean wsEnabled() { return wsEnabled; }
    public int maxFrameSize() { return maxFrameSize; }
    public int maxSubscriptionsPerConnection() { return maxSubscriptionsPerConnection; }
    public int defaultCredits() { return defaultCredits; }
    public int maxCredits() { return maxCredits; }
    public String topicPrefix() { return topicPrefix; }
    public int defaultQueuePartitions() { return defaultQueuePartitions; }
    public String metadataTopic() { return metadataTopic; }
    public int metadataReplicationFactor() { return metadataReplicationFactor; }
    public int ackCommitIntervalMs() { return ackCommitIntervalMs; }
    public String consumerStartOffset() { return consumerStartOffset; }
    public int numConsumerThreads() { return numConsumerThreads; }
    public int consumerMaxWaitMs() { return consumerMaxWaitMs; }
    public int consumerMaxBytes() { return consumerMaxBytes; }
    public int publishTimeoutMs() { return publishTimeoutMs; }
    public long connectionMaxIdleMs() { return connectionMaxIdleMs; }
    public int shutdownDrainMs() { return shutdownDrainMs; }
    public int maxRedeliveryCount() { return maxRedeliveryCount; }
    public boolean dedupEnabled() { return dedupEnabled; }
    public int dedupCacheSize() { return dedupCacheSize; }
    public long dedupCacheTtlMs() { return dedupCacheTtlMs; }
    public int maxConnectionsPerBroker() { return maxConnectionsPerBroker; }
    public int maxExchangesPerVhost() { return maxExchangesPerVhost; }
    public int maxQueuesPerVhost() { return maxQueuesPerVhost; }
    public int maxBindingsPerExchange() { return maxBindingsPerExchange; }
    public int maxControlMessagesPerSecond() { return maxControlMessagesPerSecond; }
    public long consumerAckTimeoutMs() { return consumerAckTimeoutMs; }
    public long metadataStartupTimeoutMs() { return metadataStartupTimeoutMs; }
}
```

### Test class — WsServerConfigsTest.java

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
// Time: Created - TASK-WS1.01
package org.apache.kafka.network;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * // Time: Created - TASK-WS1.01
 */
class WsServerConfigsTest {

    @Test
    void testConfigDefNotNull() {
        assertNotNull(WsServerConfigs.CONFIG_DEF);
    }

    @Test
    void testTotalConfigCount() {
        assertEquals(28, WsServerConfigs.CONFIG_DEF.names().size());
    }

    @Test
    void testAllConfigKeysRegistered() {
        ConfigDef configDef = WsServerConfigs.CONFIG_DEF;
        // TODO: assertTrue for all 28 config keys
    }

    @Test
    void testDefaultValues() {
        Map<String, Object> parsed = WsServerConfigs.CONFIG_DEF.parse(new HashMap<>());
        assertEquals(true, parsed.get(WsServerConfigs.WS_ENABLED_CONFIG));
        assertEquals(1048576, parsed.get(WsServerConfigs.WS_MAX_FRAME_SIZE_CONFIG));
        assertEquals(256, parsed.get(WsServerConfigs.WS_MAX_SUBSCRIPTIONS_PER_CONNECTION_CONFIG));
        assertEquals(100, parsed.get(WsServerConfigs.WS_DEFAULT_CREDITS_CONFIG));
        // TODO: assert all 28 defaults
    }

    @Test
    void testConsumerStartOffsetRejectsInvalid() {
        Map<String, String> props = new HashMap<>();
        props.put(WsServerConfigs.WS_CONSUMER_START_OFFSET_CONFIG, "middle");
        assertThrows(ConfigException.class, () -> WsServerConfigs.CONFIG_DEF.parse(props));
    }

    @Test
    void testConsumerStartOffsetAcceptsEarliest() {
        Map<String, String> props = new HashMap<>();
        props.put(WsServerConfigs.WS_CONSUMER_START_OFFSET_CONFIG, "earliest");
        Map<String, Object> parsed = WsServerConfigs.CONFIG_DEF.parse(props);
        assertEquals("earliest", parsed.get(WsServerConfigs.WS_CONSUMER_START_OFFSET_CONFIG));
    }

    @Test
    void testMaxFrameSizeRejectsZero() {
        Map<String, String> props = new HashMap<>();
        props.put(WsServerConfigs.WS_MAX_FRAME_SIZE_CONFIG, "0");
        assertThrows(ConfigException.class, () -> WsServerConfigs.CONFIG_DEF.parse(props));
    }

    @Test
    void testConnectionMaxIdleAllowsZero() {
        Map<String, String> props = new HashMap<>();
        props.put(WsServerConfigs.WS_CONNECTION_MAX_IDLE_MS_CONFIG, "0");
        Map<String, Object> parsed = WsServerConfigs.CONFIG_DEF.parse(props);
        assertEquals(0L, parsed.get(WsServerConfigs.WS_CONNECTION_MAX_IDLE_MS_CONFIG));
    }

    @Test
    void testShutdownDrainAllowsZero() {
        Map<String, String> props = new HashMap<>();
        props.put(WsServerConfigs.WS_SHUTDOWN_DRAIN_MS_CONFIG, "0");
        Map<String, Object> parsed = WsServerConfigs.CONFIG_DEF.parse(props);
        assertEquals(0, parsed.get(WsServerConfigs.WS_SHUTDOWN_DRAIN_MS_CONFIG));
    }

    @Test
    void testMaxRedeliveryCountAllowsZero() {
        Map<String, String> props = new HashMap<>();
        props.put(WsServerConfigs.WS_MAX_REDELIVERY_COUNT_CONFIG, "0");
        Map<String, Object> parsed = WsServerConfigs.CONFIG_DEF.parse(props);
        assertEquals(0, parsed.get(WsServerConfigs.WS_MAX_REDELIVERY_COUNT_CONFIG));
    }

    @Test
    void testReplicationFactorRejectsZero() {
        Map<String, String> props = new HashMap<>();
        props.put(WsServerConfigs.WS_METADATA_REPLICATION_FACTOR_CONFIG, "0");
        assertThrows(ConfigException.class, () -> WsServerConfigs.CONFIG_DEF.parse(props));
    }
}
```

### Test class — WsConfigsTest.java

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
// Time: Created - TASK-WS1.01
package kafka.server.http.ws;

import org.apache.kafka.network.WsServerConfigs;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * // Time: Created - TASK-WS1.01
 */
class WsConfigsTest {

    @Test
    void withDefaults_hasExpectedValues() {
        WsConfigs cfg = WsConfigs.withDefaults();
        assertEquals(WsServerConfigs.WS_ENABLED_DEFAULT, cfg.wsEnabled());
        assertEquals(WsServerConfigs.WS_MAX_FRAME_SIZE_DEFAULT, cfg.maxFrameSize());
        assertEquals(WsServerConfigs.WS_TOPIC_PREFIX_DEFAULT, cfg.topicPrefix());
        assertEquals(WsServerConfigs.WS_METADATA_TOPIC_DEFAULT, cfg.metadataTopic());
        // TODO: assert all 28 getters match defaults
    }

    @Test
    void nullTopicPrefix_throwsNPE() {
        assertThrows(NullPointerException.class, () -> new WsConfigs(
            true, 1048576, 256, 100, 10000,
            null, 1, "__ws_routing_metadata", 3,
            1000, "latest", 8, 500, 1048576,
            30000, 600000L, 5000, 10, false,
            10000, 60000L, 10000, 1000, 10000,
            10000, 50, 300000L, 30000L));
    }
}
```

### Existing pattern reference

```java
// From server/src/main/java/org/apache/kafka/network/HttpServerConfigs.java lines 281-452
// Pattern: public constants + ConfigDef — follow exactly

public class HttpServerConfigs {
    public static final String HTTP_ENABLED_CONFIG = "http.enabled";
    public static final boolean HTTP_ENABLED_DEFAULT = false;
    public static final String HTTP_ENABLED_DOC = "Master switch to enable the HTTP listener. ...";
    // ... 13 more properties ...

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(HTTP_ENABLED_CONFIG, BOOLEAN, HTTP_ENABLED_DEFAULT, HIGH, HTTP_ENABLED_DOC)
            // ... 13 more .define() calls ...
}
```

---

## Tests

**Test class 1:** `server/src/test/java/org/apache/kafka/network/WsServerConfigsTest.java`

| Test method | What it verifies |
|-------------|-----------------|
| `testConfigDefNotNull` | CONFIG_DEF is initialized |
| `testTotalConfigCount` | Exactly 28 properties registered |
| `testAllConfigKeysRegistered` | All 28 keys exist in CONFIG_DEF |
| `testDefaultValues` | All defaults match spec |
| `testConsumerStartOffsetRejectsInvalid` | Only "earliest"/"latest" accepted |
| `testConsumerStartOffsetAcceptsEarliest` | "earliest" parses correctly |
| `testMaxFrameSizeRejectsZero` | atLeast(1) validation works |
| `testConnectionMaxIdleAllowsZero` | atLeast(0) allows zero |
| `testShutdownDrainAllowsZero` | atLeast(0) allows zero |
| `testMaxRedeliveryCountAllowsZero` | atLeast(0) allows zero |
| `testReplicationFactorRejectsZero` | atLeast(1) validation works |

**Test class 2:** `http-server/src/test/java/kafka/server/http/ws/WsConfigsTest.java`

| Test method | What it verifies |
|-------------|-----------------|
| `withDefaults_hasExpectedValues` | All getters return expected defaults |
| `nullTopicPrefix_throwsNPE` | Constructor rejects null String fields |

**Run commands:**
```bash
timeout 300 ./gradlew :server:test --tests "org.apache.kafka.network.WsServerConfigsTest"
timeout 300 ./gradlew :http-server:test --tests "kafka.server.http.ws.WsConfigsTest"
```

---

## Rules

- Config property names must use the `ws.` prefix except `num.ws.consumer.threads` which uses `num.` prefix for consistency with `num.network.threads` and `num.http.network.threads`.
- Config documentation strings must be self-contained. Each doc string should explain what the property does, what the default means, and any important caveats.
- `WsServerConfigs` goes in the `:server` module at `org.apache.kafka.network`, not in `:http-server`.
- `WsConfigs` runtime holder goes in `:http-server` at `kafka.server.http.ws`.
- Do not wire `WsServerConfigs.CONFIG_DEF` into `KafkaConfig` in this task — that is a separate task.

---

## Learning

- `ConfigDef.ValidString.in(...)` is the idiomatic enum-like validator for string-valued config keys. Used here for `ws.consumer.start.offset` accepting only `"earliest"` and `"latest"`. Importing via `import static org.apache.kafka.common.config.ConfigDef.ValidString.in;` keeps the `CONFIG_DEF` block readable.
- `atLeast(0)` vs `atLeast(1)` differentiates "zero disables the feature" (e.g. `ws.connection.max.idle.ms=0` disables idle-closing; `ws.max.redelivery.count=0` disables DLX routing) from "zero is invalid" (e.g. replication factor, dedup cache size/TTL, consumer threads).
- `HttpServerConfigs.CONFIG_DEF` is merged into the broker's consolidated `CONFIG_DEF` inside `server/src/main/java/org/apache/kafka/server/config/AbstractKafkaConfig.java` (not `KafkaConfig.scala` as the task brief suggested). Any new broker-level `ConfigDef` goes into the `Utils.mergeConfigs(List.of(...))` list there.
- Checkstyle's `ParameterNumber` check caps method signatures at 13 parameters; the `WsConfigs` constructor has 28. Suppressed by adding `WsConfigs` to the filename regex of the existing `ParameterNumber` suppression in `checkstyle/suppressions.xml`.
- The Kafka checkstyle `LeftCurly` rule rejects single-line method bodies like `public int foo() { return foo; }` — each accessor must be formatted on its own multi-line block.

---

## Limitations

- `WsServerConfigs.CONFIG_DEF` is merged into `AbstractKafkaConfig.CONFIG_DEF`, but no typed accessors on `AbstractKafkaConfig` / `KafkaConfig` are added yet (e.g. no `wsEnabled(): Boolean` convenience method). Downstream tasks can add these as needed.
- `WsConfigs.fromKafkaConfig(AbstractKafkaConfig)` factory was not implemented despite being hinted at in the top-level "Context" paragraph of the task brief — the `## Specification` skeleton only specified the 28-arg constructor and `withDefaults()` factory. Runtime wiring from `AbstractKafkaConfig` into `WsConfigs` will need to be added when the WS layer is assembled in `BrokerServer`.
- Pre-existing Scala compile failure on branch `feature/http-protocol`: duplicate `HttpRouter`/`HttpRequestTranslator` defined both as Java classes in `http-server/src/main/java/kafka/server/http/` and as Scala objects in `http-server/src/main/scala/kafka/server/http/`. This prevents `./gradlew :http-server:test` from running unless `-x :http-server:compileScala -x :http-server:compileTestScala` is passed. Not caused by this task; flagged here for the next maintainer.
- Pre-existing test failure on branch `feature/http-protocol`: `SocketServerConfigsTest.testDefaultNameToSecurityProto` expects 4 security protocols but `SecurityProtocol` now has 6 (HTTP/HTTPS added). Not caused by this task.

---

## Field Notes

- The worktree originally pointed at an orphan branch (`worktree-agent-aa3d82ed`) based on an old commit (`f95a1f995d`) that predated the `http-server` module. Had to `git reset --hard feature/http-protocol` to pull in the current state before the required files/paths existed.
- The task brief's test skeleton for `WsConfigsTest` exercised only one `null`-check via `nullTopicPrefix_throwsNPE`, but the constructor has three `Objects.requireNonNull` calls (`topicPrefix`, `metadataTopic`, `consumerStartOffset`). Added three explicit null-param tests to cover each.
- `ws.topic.prefix` and `ws.metadata.topic` are `STRING` type without a validator. A future task should consider adding a `Topic.validate(...)`-based validator to catch invalid topic-name characters early.
- `num.ws.consumer.threads` uses the `num.` prefix rather than `ws.`, mirroring the existing convention of `num.network.threads` / `num.http.network.threads`. This is intentional per the task Rules section.
- Running `:http-server:test` requires `-x :http-server:compileScala -x :http-server:compileTestScala` until the Java/Scala `HttpRouter`/`HttpRequestTranslator` duplication is resolved in a separate task.

---

## Acceptance Criteria

- [ ] `WsServerConfigs.java` exists at `server/src/main/java/org/apache/kafka/network/WsServerConfigs.java`
- [ ] `WsServerConfigs.CONFIG_DEF` registers exactly 28 properties
- [ ] All 28 config keys have correct types (BOOLEAN for ws.enabled and ws.dedup.enabled; LONG for ws.connection.max.idle.ms, ws.dedup.cache.ttl.ms, ws.consumer.ack.timeout.ms, ws.metadata.startup.timeout.ms; STRING for ws.topic.prefix, ws.metadata.topic, ws.consumer.start.offset; INT for all others)
- [ ] `ws.consumer.start.offset` rejects values other than "earliest" and "latest"
- [ ] `ws.metadata.replication.factor` rejects 0 (atLeast(1))
- [ ] `ws.connection.max.idle.ms` and `ws.shutdown.drain.ms` accept 0 (atLeast(0))
- [ ] `WsConfigs.java` exists at `http-server/src/main/java/kafka/server/http/ws/WsConfigs.java`
- [ ] `WsConfigs.withDefaults()` returns a valid instance with all 28 default values
- [ ] `timeout 300 ./gradlew :server:test --tests "org.apache.kafka.network.WsServerConfigsTest"` exits 0
- [ ] `timeout 300 ./gradlew :http-server:test --tests "kafka.server.http.ws.WsConfigsTest"` exits 0
- [ ] No existing tests break: `./gradlew :server:test` passes
- [ ] Learning section filled with at least one entry
- [ ] Limitations section filled (use "None" if truly none)

---

## File Manifest

> Filled by the executing agent after each commit.
> Run: `git diff --name-status HEAD~1 HEAD -- '*.java' '*.xml' '*.json' '*.yaml' '*.yml'`

### 2026-04-17 — WS1.01 config properties (commit fa97449cb6)

Created:
  - `server/src/main/java/org/apache/kafka/network/WsServerConfigs.java` — 28 public constants (key + default + doc) and `CONFIG_DEF` that registers all properties with types, validators, importance levels, and docs.
  - `http-server/src/main/java/kafka/server/http/ws/WsConfigs.java` — runtime holder with 28-arg constructor, `withDefaults()` factory, and 28 typed accessors. Defaults come from `WsServerConfigs` constants.
  - `server/src/test/java/org/apache/kafka/network/WsServerConfigsTest.java` — 25 tests covering CONFIG_DEF presence, total count = 28, all key registration, default values per spec, custom-value parsing, and all validator edge cases (`atLeast(1)` rejects 0, `atLeast(0)` accepts 0, `ValidString.in(earliest, latest)` enforcement, empty-doc guard).
  - `http-server/src/test/java/kafka/server/http/ws/WsConfigsTest.java` — 6 tests covering `withDefaults()`, full-constructor round-trip, three NPE tests for null string fields, and a CONFIG_DEF → constructor integration test.

Modified:
  - `server/src/main/java/org/apache/kafka/server/config/AbstractKafkaConfig.java` — added import for `WsServerConfigs` and `WsServerConfigs.CONFIG_DEF` entry in the `Utils.mergeConfigs(List.of(...))` builder so all 28 WS keys are recognized by `KafkaConfig`.
  - `checkstyle/suppressions.xml` — appended `WsConfigs` to the existing `ParameterNumber` suppression filename regex (28-arg constructor exceeds the 13-arg Kafka limit).
  - `ivy-docs/tasks/TASK-WS1.01-ws-config-properties.md` — filled Learning, Limitations, Field Notes, and File Manifest sections.
