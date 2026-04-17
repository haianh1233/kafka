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
// Time: Created - TASK-A.02
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

/**
 * Configuration properties for the HTTP/HTTPS listener.
 * <p>
 * Follows the same pattern as {@link SocketServerConfigs}: public constants for config keys,
 * defaults, and documentation strings, plus a {@link ConfigDef} that registers all properties.
 *
 * @see SocketServerConfigs
 */
public class HttpServerConfigs {

    // ---- http.enabled ----
    public static final String HTTP_ENABLED_CONFIG = "http.enabled";
    public static final boolean HTTP_ENABLED_DEFAULT = false;
    public static final String HTTP_ENABLED_DOC = "Master switch to enable the HTTP listener. " +
            "HTTP/HTTPS listeners are configured through the standard 'listeners' property using the " +
            "HTTP:// or HTTPS:// URI scheme. Example: listeners=PLAINTEXT://0.0.0.0:9092,HTTP://0.0.0.0:9094. " +
            "The listener.security.protocol.map automatically includes HTTP:HTTP and HTTPS:HTTPS mappings. " +
            "Adding an HTTP:// or HTTPS:// entry to 'listeners' implicitly enables the HTTP listener " +
            "regardless of this setting. The inter.broker.listener.name must remain a binary protocol " +
            "listener (PLAINTEXT, SSL, SASL_PLAINTEXT, or SASL_SSL).";

    // ---- num.http.network.threads ----
    public static final String NUM_HTTP_NETWORK_THREADS_CONFIG = "num.http.network.threads";
    public static final int NUM_HTTP_NETWORK_THREADS_DEFAULT = 4;
    public static final String NUM_HTTP_NETWORK_THREADS_DOC = "The number of Netty worker threads " +
            "used by the HTTP listener for reading requests from the network and sending responses. " +
            "These threads handle HTTP codec operations (frame parsing, aggregation, compression) " +
            "and must not block. Increase for high-concurrency HTTP workloads.";

    // ---- http.request.max.bytes ----
    public static final String HTTP_REQUEST_MAX_BYTES_CONFIG = "http.request.max.bytes";
    public static final int HTTP_REQUEST_MAX_BYTES_DEFAULT = 10 * 1024 * 1024; // 10 MB
    public static final String HTTP_REQUEST_MAX_BYTES_DOC = "The maximum size in bytes of an " +
            "HTTP request body. Requests exceeding this limit are rejected with HTTP 413 " +
            "(Payload Too Large) before the handler is invoked. This limit is enforced by Netty's " +
            "HttpObjectAggregator. Default is 10 MB, which covers 10,000 records of ~1 KB each. " +
            "Raise for workloads with larger record sizes, but be aware that large values increase " +
            "the risk of Netty worker thread head-of-line blocking.";

    // ---- http.produce.max.records ----
    public static final String HTTP_PRODUCE_MAX_RECORDS_CONFIG = "http.produce.max.records";
    public static final int HTTP_PRODUCE_MAX_RECORDS_DEFAULT = 10000;
    public static final String HTTP_PRODUCE_MAX_RECORDS_DOC = "The maximum number of records " +
            "allowed in a single HTTP produce request. Requests with more records are rejected " +
            "with HTTP 413 before deserialization to prevent out-of-memory conditions. " +
            "This is checked after JSON parsing of the records array length.";

    // ---- http.response.timeout.ms ----
    public static final String HTTP_RESPONSE_TIMEOUT_MS_CONFIG = "http.response.timeout.ms";
    public static final int HTTP_RESPONSE_TIMEOUT_MS_DEFAULT = 30000;
    public static final String HTTP_RESPONSE_TIMEOUT_MS_DOC = "The maximum time in milliseconds " +
            "the broker will wait before returning an HTTP 504 (Gateway Timeout) response to the " +
            "client. This is the outer timeout for the entire request lifecycle, including " +
            "forwarding to remote brokers and waiting for ISR acknowledgements.";

    // ---- http.consume.max.wait.ms ----
    public static final String HTTP_CONSUME_MAX_WAIT_MS_CONFIG = "http.consume.max.wait.ms";
    public static final int HTTP_CONSUME_MAX_WAIT_MS_DEFAULT = 5000;
    public static final String HTTP_CONSUME_MAX_WAIT_MS_DOC = "Server-side cap on the " +
            "'maxWaitMs' parameter in HTTP consume (fetch) requests. Any client-supplied value " +
            "above this cap is silently clamped. This keeps HTTP connections short enough to " +
            "survive load-balancer idle timeouts (typically 30-60 seconds). The applied cap is " +
            "echoed back in the 'X-Kafka-MaxWait-Applied' response header so clients can adjust.";

    // ---- http.consume.max.bytes ----
    public static final String HTTP_CONSUME_MAX_BYTES_CONFIG = "http.consume.max.bytes";
    public static final int HTTP_CONSUME_MAX_BYTES_DEFAULT = 1048576; // 1 MB
    public static final String HTTP_CONSUME_MAX_BYTES_DOC = "The maximum total response bytes " +
            "for HTTP consume responses. This is separate from the binary protocol's 'maxBytes' " +
            "parameter and controls JSON serialization memory usage. A 1 MB compressed batch " +
            "may expand to 5 MB of JSON, so this cap prevents excessive memory allocation for " +
            "consume response buffers.";

    // ---- http.internal.forwarding.timeout.ms ----
    public static final String HTTP_INTERNAL_FORWARDING_TIMEOUT_MS_CONFIG = "http.internal.forwarding.timeout.ms";
    public static final int HTTP_INTERNAL_FORWARDING_TIMEOUT_MS_DEFAULT = 10000;
    public static final String HTTP_INTERNAL_FORWARDING_TIMEOUT_MS_DOC = "Timeout in " +
            "milliseconds for broker-to-broker forwarding calls used by the HTTP produce and " +
            "consume paths. When the receiving broker is not the leader for a partition, it " +
            "forwards the request to the correct leader over the inter-broker binary protocol. " +
            "If the leader does not respond within this timeout, the affected partitions return " +
            "HTTP 504 (Gateway Timeout).";

    // ---- http.internal.forwarding.retries ----
    public static final String HTTP_INTERNAL_FORWARDING_RETRIES_CONFIG = "http.internal.forwarding.retries";
    public static final int HTTP_INTERNAL_FORWARDING_RETRIES_DEFAULT = 1;
    public static final String HTTP_INTERNAL_FORWARDING_RETRIES_DOC = "Maximum number of retry " +
            "attempts per forwarded partition group on retriable errors (e.g., leader change, " +
            "connection failure). Set to 0 to disable retries. Each retry refreshes the metadata " +
            "cache and re-routes to the new leader. Retries are bounded by " +
            "'" + HTTP_INTERNAL_FORWARDING_TIMEOUT_MS_CONFIG + "'.";

    // ---- http.internal.forwarding.queue.size ----
    public static final String HTTP_INTERNAL_FORWARDING_QUEUE_SIZE_CONFIG = "http.internal.forwarding.queue.size";
    public static final int HTTP_INTERNAL_FORWARDING_QUEUE_SIZE_DEFAULT = 10000;
    public static final String HTTP_INTERNAL_FORWARDING_QUEUE_SIZE_DOC = "Bounded queue capacity " +
            "per ProduceForwardThread / FetchForwardThread (one thread per remote broker). When " +
            "the queue is full, new forwarding requests are immediately rejected with HTTP 503 " +
            "(Service Unavailable). Monitor 'http.forward.queue.size' metric per broker.";

    // ---- http.cors.allowed.origins ----
    public static final String HTTP_CORS_ALLOWED_ORIGINS_CONFIG = "http.cors.allowed.origins";
    public static final String HTTP_CORS_ALLOWED_ORIGINS_DEFAULT = "";
    public static final String HTTP_CORS_ALLOWED_ORIGINS_DOC = "Comma-separated list of allowed " +
            "CORS origins. Set to '*' to allow all origins. An empty string (default) disables " +
            "CORS entirely. When set, the broker adds 'Access-Control-Allow-Origin' and related " +
            "headers to HTTP responses and handles OPTIONS preflight requests.";

    // ---- http.connection.idle.timeout.ms ----
    public static final String HTTP_CONNECTION_IDLE_TIMEOUT_MS_CONFIG = "http.connection.idle.timeout.ms";
    public static final long HTTP_CONNECTION_IDLE_TIMEOUT_MS_DEFAULT = 60000L;
    public static final String HTTP_CONNECTION_IDLE_TIMEOUT_MS_DOC = "Time in milliseconds " +
            "after which an idle HTTP keep-alive connection is closed. Netty's IdleStateHandler " +
            "fires the timeout. Default (60 seconds) matches common load-balancer idle timeouts " +
            "(e.g., AWS ALB = 60s). Set to 0 to disable idle connection closing.";

    // ---- http.shutdown.drain.ms ----
    public static final String HTTP_SHUTDOWN_DRAIN_MS_CONFIG = "http.shutdown.drain.ms";
    public static final int HTTP_SHUTDOWN_DRAIN_MS_DEFAULT = 2000;
    public static final String HTTP_SHUTDOWN_DRAIN_MS_DOC = "Time in milliseconds to drain " +
            "in-flight HTTP requests during graceful shutdown. During this window the broker " +
            "stops accepting new connections but continues processing requests already in the " +
            "RequestChannel queue. After the window expires, remaining connections are forcibly " +
            "closed.";

    // ---- num.http.async.threads ----
    public static final String NUM_HTTP_ASYNC_THREADS_CONFIG = "num.http.async.threads";
    public static final int NUM_HTTP_ASYNC_THREADS_DEFAULT = 4;
    public static final String NUM_HTTP_ASYNC_THREADS_DOC = "Number of threads in the dedicated " +
            "executor used for CompletableFuture.handleAsync() callbacks in the HTTP produce and " +
            "consume paths. These threads merge forwarding results and call " +
            "requestChannel.sendResponse(). They must be separate from KafkaRequestHandler threads " +
            "and Netty worker threads to avoid blocking either pool.";

    /**
     * ConfigDef for all HTTP server configuration properties.
     * <p>
     * This is intended to be merged into KafkaConfig's ConfigDef via
     * {@code CONFIG_DEF.define(HttpServerConfigs.CONFIG_DEF)}.
     */
    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(HTTP_ENABLED_CONFIG,
                    BOOLEAN, HTTP_ENABLED_DEFAULT,
                    HIGH, HTTP_ENABLED_DOC)
            .define(NUM_HTTP_NETWORK_THREADS_CONFIG,
                    INT, NUM_HTTP_NETWORK_THREADS_DEFAULT,
                    atLeast(1), HIGH, NUM_HTTP_NETWORK_THREADS_DOC)
            .define(HTTP_REQUEST_MAX_BYTES_CONFIG,
                    INT, HTTP_REQUEST_MAX_BYTES_DEFAULT,
                    atLeast(1), HIGH, HTTP_REQUEST_MAX_BYTES_DOC)
            .define(HTTP_PRODUCE_MAX_RECORDS_CONFIG,
                    INT, HTTP_PRODUCE_MAX_RECORDS_DEFAULT,
                    atLeast(1), MEDIUM, HTTP_PRODUCE_MAX_RECORDS_DOC)
            .define(HTTP_RESPONSE_TIMEOUT_MS_CONFIG,
                    INT, HTTP_RESPONSE_TIMEOUT_MS_DEFAULT,
                    atLeast(1), MEDIUM, HTTP_RESPONSE_TIMEOUT_MS_DOC)
            .define(HTTP_CONSUME_MAX_WAIT_MS_CONFIG,
                    INT, HTTP_CONSUME_MAX_WAIT_MS_DEFAULT,
                    atLeast(0), MEDIUM, HTTP_CONSUME_MAX_WAIT_MS_DOC)
            .define(HTTP_CONSUME_MAX_BYTES_CONFIG,
                    INT, HTTP_CONSUME_MAX_BYTES_DEFAULT,
                    atLeast(1), MEDIUM, HTTP_CONSUME_MAX_BYTES_DOC)
            .define(HTTP_INTERNAL_FORWARDING_TIMEOUT_MS_CONFIG,
                    INT, HTTP_INTERNAL_FORWARDING_TIMEOUT_MS_DEFAULT,
                    atLeast(1), MEDIUM, HTTP_INTERNAL_FORWARDING_TIMEOUT_MS_DOC)
            .define(HTTP_INTERNAL_FORWARDING_RETRIES_CONFIG,
                    INT, HTTP_INTERNAL_FORWARDING_RETRIES_DEFAULT,
                    atLeast(0), LOW, HTTP_INTERNAL_FORWARDING_RETRIES_DOC)
            .define(HTTP_INTERNAL_FORWARDING_QUEUE_SIZE_CONFIG,
                    INT, HTTP_INTERNAL_FORWARDING_QUEUE_SIZE_DEFAULT,
                    atLeast(1), MEDIUM, HTTP_INTERNAL_FORWARDING_QUEUE_SIZE_DOC)
            .define(HTTP_CORS_ALLOWED_ORIGINS_CONFIG,
                    STRING, HTTP_CORS_ALLOWED_ORIGINS_DEFAULT,
                    LOW, HTTP_CORS_ALLOWED_ORIGINS_DOC)
            .define(HTTP_CONNECTION_IDLE_TIMEOUT_MS_CONFIG,
                    LONG, HTTP_CONNECTION_IDLE_TIMEOUT_MS_DEFAULT,
                    atLeast(0), MEDIUM, HTTP_CONNECTION_IDLE_TIMEOUT_MS_DOC)
            .define(HTTP_SHUTDOWN_DRAIN_MS_CONFIG,
                    INT, HTTP_SHUTDOWN_DRAIN_MS_DEFAULT,
                    atLeast(0), LOW, HTTP_SHUTDOWN_DRAIN_MS_DOC)
            .define(NUM_HTTP_ASYNC_THREADS_CONFIG,
                    INT, NUM_HTTP_ASYNC_THREADS_DEFAULT,
                    atLeast(1), MEDIUM, NUM_HTTP_ASYNC_THREADS_DOC);
}
