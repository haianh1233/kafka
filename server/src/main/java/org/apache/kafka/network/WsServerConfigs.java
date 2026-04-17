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
 * Configuration properties for the WebSocket push delivery and AMQP-style routing extension.
 * <p>
 * Follows the same pattern as {@link HttpServerConfigs}: public constants for config keys,
 * defaults, and documentation strings, plus a {@link ConfigDef} that registers all properties.
 * <p>
 * This class is intended to be merged into the broker's {@code AbstractKafkaConfig.CONFIG_DEF}
 * alongside the existing HTTP, socket, and replication config definitions.
 *
 * @see HttpServerConfigs
 */
public class WsServerConfigs {

    // ---- ws.enabled ----
    public static final String WS_ENABLED_CONFIG = "ws.enabled";
    public static final boolean WS_ENABLED_DEFAULT = true;
    public static final String WS_ENABLED_DOC = "Enable WebSocket upgrade on the HTTP listener. " +
            "When true, HTTP requests with Upgrade: websocket header and path /v1/ws are " +
            "upgraded to WebSocket connections. When false, upgrade requests receive HTTP 404.";

    // ---- ws.max.frame.size ----
    public static final String WS_MAX_FRAME_SIZE_CONFIG = "ws.max.frame.size";
    public static final int WS_MAX_FRAME_SIZE_DEFAULT = 1048576; // 1 MB
    public static final String WS_MAX_FRAME_SIZE_DOC = "Maximum WebSocket frame payload size in bytes. " +
            "Frames exceeding this limit cause the connection to be closed with status 1009 " +
            "(Message Too Big). Default is 1 MB, sufficient for most JSON messages.";

    // ---- ws.max.subscriptions.per.connection ----
    public static final String WS_MAX_SUBSCRIPTIONS_PER_CONNECTION_CONFIG = "ws.max.subscriptions.per.connection";
    public static final int WS_MAX_SUBSCRIPTIONS_PER_CONNECTION_DEFAULT = 256;
    public static final String WS_MAX_SUBSCRIPTIONS_PER_CONNECTION_DOC = "Maximum number of concurrent " +
            "subscriptions allowed per WebSocket connection. Additional subscribe requests are " +
            "rejected with an error frame. Each subscription creates a consumer fetch loop.";

    // ---- ws.default.credits ----
    public static final String WS_DEFAULT_CREDITS_CONFIG = "ws.default.credits";
    public static final int WS_DEFAULT_CREDITS_DEFAULT = 100;
    public static final String WS_DEFAULT_CREDITS_DOC = "Default number of delivery credits " +
            "granted to a new subscription if the subscribe message does not specify a credits " +
            "value. Credits control flow — the broker delivers at most this many messages before " +
            "waiting for the client to grant more via a credits frame.";

    // ---- ws.max.credits ----
    public static final String WS_MAX_CREDITS_CONFIG = "ws.max.credits";
    public static final int WS_MAX_CREDITS_DEFAULT = 10000;
    public static final String WS_MAX_CREDITS_DOC = "Maximum number of credits a client can grant " +
            "in a single credits frame. Requests exceeding this limit are clamped to this value. " +
            "Prevents a single consumer from buffering excessive undelivered messages.";

    // ---- ws.topic.prefix ----
    public static final String WS_TOPIC_PREFIX_CONFIG = "ws.topic.prefix";
    public static final String WS_TOPIC_PREFIX_DEFAULT = "ws.";
    public static final String WS_TOPIC_PREFIX_DOC = "Prefix prepended to queue names when creating " +
            "the backing Kafka topic. For example, queue 'order-events' maps to Kafka topic " +
            "'ws.order-events'. This separates WebSocket queue topics from user-managed topics.";

    // ---- ws.default.queue.partitions ----
    public static final String WS_DEFAULT_QUEUE_PARTITIONS_CONFIG = "ws.default.queue.partitions";
    public static final int WS_DEFAULT_QUEUE_PARTITIONS_DEFAULT = 1;
    public static final String WS_DEFAULT_QUEUE_PARTITIONS_DOC = "Default number of partitions for " +
            "Kafka topics created to back declared queues. Can be overridden per queue via the " +
            "'x-partitions' argument in declare-queue. Most queues use 1 partition for FIFO ordering.";

    // ---- ws.metadata.topic ----
    public static final String WS_METADATA_TOPIC_CONFIG = "ws.metadata.topic";
    public static final String WS_METADATA_TOPIC_DEFAULT = "__ws_routing_metadata";
    public static final String WS_METADATA_TOPIC_DOC = "Name of the internal compacted Kafka topic " +
            "that stores exchange, queue, and binding metadata. This topic is created automatically " +
            "on first broker startup with ws.enabled=true. All brokers replay this topic to build " +
            "their in-memory routing cache.";

    // ---- ws.metadata.replication.factor ----
    public static final String WS_METADATA_REPLICATION_FACTOR_CONFIG = "ws.metadata.replication.factor";
    public static final int WS_METADATA_REPLICATION_FACTOR_DEFAULT = 3;
    public static final String WS_METADATA_REPLICATION_FACTOR_DOC = "Replication factor for the " +
            "routing metadata topic. Set to min(this value, cluster size) at creation time. " +
            "Higher values increase durability of exchange/queue/binding definitions.";

    // ---- ws.ack.commit.interval.ms ----
    public static final String WS_ACK_COMMIT_INTERVAL_MS_CONFIG = "ws.ack.commit.interval.ms";
    public static final int WS_ACK_COMMIT_INTERVAL_MS_DEFAULT = 1000;
    public static final String WS_ACK_COMMIT_INTERVAL_MS_DOC = "Interval in milliseconds between " +
            "batched offset commits for acknowledged messages. Lower values reduce re-delivery " +
            "on crash but increase commit overhead.";

    // ---- ws.consumer.start.offset ----
    public static final String WS_CONSUMER_START_OFFSET_CONFIG = "ws.consumer.start.offset";
    public static final String WS_CONSUMER_START_OFFSET_DEFAULT = "latest";
    public static final String WS_CONSUMER_START_OFFSET_DOC = "Default start offset for new " +
            "subscriptions: 'earliest' starts from the beginning of the topic, 'latest' starts " +
            "from the current end. Can be overridden per subscription in the subscribe frame.";

    // ---- num.ws.consumer.threads ----
    public static final String NUM_WS_CONSUMER_THREADS_CONFIG = "num.ws.consumer.threads";
    public static final int NUM_WS_CONSUMER_THREADS_DEFAULT = 8;
    public static final String NUM_WS_CONSUMER_THREADS_DOC = "Number of threads in the thread pool " +
            "used for WebSocket consumer fetch loops. Each active subscription runs a fetch loop " +
            "on one of these threads. Increase for workloads with many concurrent subscriptions.";

    // ---- ws.consumer.max.wait.ms ----
    public static final String WS_CONSUMER_MAX_WAIT_MS_CONFIG = "ws.consumer.max.wait.ms";
    public static final int WS_CONSUMER_MAX_WAIT_MS_DEFAULT = 500;
    public static final String WS_CONSUMER_MAX_WAIT_MS_DOC = "Maximum time in milliseconds each " +
            "consumer fetch loop iteration waits for new data. Lower values reduce delivery " +
            "latency but increase fetch request rate and broker load.";

    // ---- ws.consumer.max.bytes ----
    public static final String WS_CONSUMER_MAX_BYTES_CONFIG = "ws.consumer.max.bytes";
    public static final int WS_CONSUMER_MAX_BYTES_DEFAULT = 1048576; // 1 MB
    public static final String WS_CONSUMER_MAX_BYTES_DOC = "Maximum bytes fetched per consumer " +
            "fetch loop iteration. Controls memory usage per subscription. Each fetched batch " +
            "is serialized to JSON deliver frames and written to the WebSocket channel.";

    // ---- ws.publish.timeout.ms ----
    public static final String WS_PUBLISH_TIMEOUT_MS_CONFIG = "ws.publish.timeout.ms";
    public static final int WS_PUBLISH_TIMEOUT_MS_DEFAULT = 30000;
    public static final String WS_PUBLISH_TIMEOUT_MS_DOC = "Maximum time in milliseconds for a " +
            "WebSocket publish to complete (route through exchange, produce to Kafka topic, " +
            "wait for ISR acks). If exceeded, the broker sends a publish-failed frame.";

    // ---- ws.connection.max.idle.ms ----
    public static final String WS_CONNECTION_MAX_IDLE_MS_CONFIG = "ws.connection.max.idle.ms";
    public static final long WS_CONNECTION_MAX_IDLE_MS_DEFAULT = 600000L; // 10 min
    public static final String WS_CONNECTION_MAX_IDLE_MS_DOC = "Time in milliseconds after which " +
            "an idle WebSocket connection (no frames sent or received) is closed with code 1000. " +
            "Default is 10 minutes. Set to 0 to disable idle connection closing.";

    // ---- ws.shutdown.drain.ms ----
    public static final String WS_SHUTDOWN_DRAIN_MS_CONFIG = "ws.shutdown.drain.ms";
    public static final int WS_SHUTDOWN_DRAIN_MS_DEFAULT = 5000;
    public static final String WS_SHUTDOWN_DRAIN_MS_DOC = "Time in milliseconds to drain " +
            "WebSocket connections during graceful shutdown. During this window the broker " +
            "sends close frames with code 1001 (Going Away) and waits for client close. " +
            "After the window expires, remaining connections are forcibly closed.";

    // ---- ws.max.redelivery.count ----
    public static final String WS_MAX_REDELIVERY_COUNT_CONFIG = "ws.max.redelivery.count";
    public static final int WS_MAX_REDELIVERY_COUNT_DEFAULT = 10;
    public static final String WS_MAX_REDELIVERY_COUNT_DOC = "Maximum number of redelivery attempts " +
            "for a message before it is routed to the dead-letter exchange (DLX). Prevents poison " +
            "messages from blocking a queue indefinitely. Set to 0 to disable DLX routing.";

    // ---- ws.dedup.enabled ----
    public static final String WS_DEDUP_ENABLED_CONFIG = "ws.dedup.enabled";
    public static final boolean WS_DEDUP_ENABLED_DEFAULT = false;
    public static final String WS_DEDUP_ENABLED_DOC = "Enable publish deduplication by messageId. " +
            "When enabled, the broker tracks recently seen messageId values per exchange and " +
            "silently drops duplicate publishes. Requires publishers to set a unique messageId " +
            "in each publish frame.";

    // ---- ws.dedup.cache.size ----
    public static final String WS_DEDUP_CACHE_SIZE_CONFIG = "ws.dedup.cache.size";
    public static final int WS_DEDUP_CACHE_SIZE_DEFAULT = 10000;
    public static final String WS_DEDUP_CACHE_SIZE_DOC = "Maximum number of entries in the " +
            "deduplication cache per exchange. When the cache is full, the oldest entries are " +
            "evicted. Only effective when ws.dedup.enabled=true.";

    // ---- ws.dedup.cache.ttl.ms ----
    public static final String WS_DEDUP_CACHE_TTL_MS_CONFIG = "ws.dedup.cache.ttl.ms";
    public static final long WS_DEDUP_CACHE_TTL_MS_DEFAULT = 60000L;
    public static final String WS_DEDUP_CACHE_TTL_MS_DOC = "Time-to-live in milliseconds for " +
            "entries in the deduplication cache. Entries older than this are eligible for eviction " +
            "regardless of cache size. Only effective when ws.dedup.enabled=true.";

    // ---- ws.max.connections.per.broker ----
    public static final String WS_MAX_CONNECTIONS_PER_BROKER_CONFIG = "ws.max.connections.per.broker";
    public static final int WS_MAX_CONNECTIONS_PER_BROKER_DEFAULT = 10000;
    public static final String WS_MAX_CONNECTIONS_PER_BROKER_DOC = "Maximum number of concurrent " +
            "WebSocket connections per broker. Excess connections are rejected with WebSocket " +
            "close code 4429. Protects broker memory and thread resources.";

    // ---- ws.max.exchanges.per.vhost ----
    public static final String WS_MAX_EXCHANGES_PER_VHOST_CONFIG = "ws.max.exchanges.per.vhost";
    public static final int WS_MAX_EXCHANGES_PER_VHOST_DEFAULT = 1000;
    public static final String WS_MAX_EXCHANGES_PER_VHOST_DOC = "Maximum number of exchanges that " +
            "can be declared per virtual host. Exchange declarations exceeding this limit are " +
            "rejected with an error. Prevents unbounded metadata growth.";

    // ---- ws.max.queues.per.vhost ----
    public static final String WS_MAX_QUEUES_PER_VHOST_CONFIG = "ws.max.queues.per.vhost";
    public static final int WS_MAX_QUEUES_PER_VHOST_DEFAULT = 10000;
    public static final String WS_MAX_QUEUES_PER_VHOST_DOC = "Maximum number of queues that can " +
            "be declared per virtual host. Each queue creates a backing Kafka topic, so this " +
            "limit also caps the number of auto-created topics.";

    // ---- ws.max.bindings.per.exchange ----
    public static final String WS_MAX_BINDINGS_PER_EXCHANGE_CONFIG = "ws.max.bindings.per.exchange";
    public static final int WS_MAX_BINDINGS_PER_EXCHANGE_DEFAULT = 10000;
    public static final String WS_MAX_BINDINGS_PER_EXCHANGE_DOC = "Maximum number of bindings per " +
            "exchange. Bind requests exceeding this limit are rejected with an error. Large " +
            "binding counts on topic/headers exchanges increase per-publish routing cost.";

    // ---- ws.max.control.messages.per.second ----
    public static final String WS_MAX_CONTROL_MESSAGES_PER_SECOND_CONFIG = "ws.max.control.messages.per.second";
    public static final int WS_MAX_CONTROL_MESSAGES_PER_SECOND_DEFAULT = 50;
    public static final String WS_MAX_CONTROL_MESSAGES_PER_SECOND_DOC = "Maximum control messages " +
            "(declare, delete, bind, unbind, etc.) per second per WebSocket connection. Publish " +
            "and ack/nack are NOT counted as control messages. Excess messages are rejected " +
            "with an error frame.";

    // ---- ws.consumer.ack.timeout.ms ----
    public static final String WS_CONSUMER_ACK_TIMEOUT_MS_CONFIG = "ws.consumer.ack.timeout.ms";
    public static final long WS_CONSUMER_ACK_TIMEOUT_MS_DEFAULT = 300000L; // 5 min
    public static final String WS_CONSUMER_ACK_TIMEOUT_MS_DOC = "Time in milliseconds after " +
            "delivery before an unacknowledged message is automatically requeued. Prevents " +
            "messages from being stuck in unacked state if the consumer disconnects without " +
            "sending ack/nack. Set to 0 to disable auto-requeue (not recommended).";

    // ---- ws.metadata.startup.timeout.ms ----
    public static final String WS_METADATA_STARTUP_TIMEOUT_MS_CONFIG = "ws.metadata.startup.timeout.ms";
    public static final long WS_METADATA_STARTUP_TIMEOUT_MS_DEFAULT = 30000L;
    public static final String WS_METADATA_STARTUP_TIMEOUT_MS_DOC = "Maximum time in milliseconds " +
            "to wait for the routing metadata topic replay to complete during broker startup. " +
            "If the timeout expires, the broker starts with only the 5 default exchanges and " +
            "logs a warning. Metadata replay continues in the background.";

    /**
     * ConfigDef for all WebSocket and routing configuration properties.
     * <p>
     * Intended to be merged into the broker's {@code AbstractKafkaConfig.CONFIG_DEF}
     * via {@code Utils.mergeConfigs(...)}.
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
