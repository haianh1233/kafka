# TASK-A.02: http-server Gradle submodule + HTTP config properties

## Prerequisites

- JDK 17+
- Kafka repository checked out at branch `feature/http-protocol`
- **TASK-A.01 completed** (SecurityProtocol.HTTP and HTTPS must exist)
- Ability to run `./gradlew :http-server:compileJava` and `./gradlew :server:test` from repo root
- Read design doc sections 9, 10: `/home/anh/kafka/ivy-docs/http-protocol-design.md`

## Context

The HTTP protocol implementation lives in a new Gradle submodule called `http-server`, keeping
HTTP-specific code (Netty handlers, JSON translation, forwarding threads) out of the existing
`core` and `clients` modules. This task creates the module structure and its build configuration.

Additionally, this task adds the `HttpServerConfigs` class in the `:server` module (not
`:http-server`) because config property definitions must be available to `KafkaConfig` which
lives in `:server`. This follows the same pattern as `SocketServerConfigs` which defines
network-related config properties in the `:server` module.

### Why this task exists

Every HTTP-layer class (`HttpAcceptor`, `HttpChannelInitializer`, `HttpRequestHandler`,
`ProduceForwardThread`, `FetchForwardThread`, `HttpProcessor`) will live in the `http-server`
module. Without the module skeleton and config properties, no HTTP code can be compiled or
configured. This task is a dependency for every Phase 1 implementation task.

### Key files to read before starting

| File | Role |
|---|---|
| `/home/anh/kafka/settings.gradle` | Submodule include list (line 66-127) |
| `/home/anh/kafka/build.gradle` | Top-level build with per-project dependency blocks |
| `/home/anh/kafka/gradle/dependencies.gradle` | Centralized version catalog |
| `/home/anh/kafka/server/src/main/java/org/apache/kafka/network/SocketServerConfigs.java` | Pattern to follow for config class |

### Architecture note

The `http-server` module depends on `:core`, `:clients`, `:server-common`, and `:server`.
It does NOT depend on `:metadata`, `:storage`, or `:raft`. The HTTP layer is a thin
translation shell -- it translates HTTP requests into existing Kafka protocol objects and
submits them to `RequestChannel`, which is defined in `:core`.

## Specification

### 1. Add Netty version to `gradle/dependencies.gradle`

Open `/home/anh/kafka/gradle/dependencies.gradle`.

In the `versions` block (around line 51-142), add a `netty` entry. Insert it alphabetically
(after the `mockito` line, before `opentelemetryProto`):

```groovy
  netty: "4.1.118.Final",
```

In the `libs` block (starts around line 145), add the Netty library reference. Insert it
alphabetically:

```groovy
  nettyAll: "io.netty:netty-all:$versions.netty",
```

### 2. Register `http-server` in `settings.gradle`

Open `/home/anh/kafka/settings.gradle`.

The include list starts at line 66. Add `'http-server'` after the `'group-coordinator:group-coordinator-api'` line (around line 82). Alphabetical placement:

```gradle
include 'clients',
    'clients:clients-integration-tests',
    'connect:api',
    'connect:basic-auth-extension',
    'connect:file',
    'connect:json',
    'connect:mirror',
    'connect:mirror-client',
    'connect:runtime',
    'connect:test-plugins',
    'connect:transforms',
    'coordinator-common',
    'core',
    'examples',
    'generator',
    'group-coordinator',
    'group-coordinator:group-coordinator-api',
    'http-server',
    'jmh-benchmarks',
    ...
```

### 3. Add `http-server` project block in `build.gradle`

Open `/home/anh/kafka/build.gradle`.

Add a new `project(':http-server')` block. Place it after the `project(':group-coordinator')`
block and before the `project(':jmh-benchmarks')` block (maintaining alphabetical order).

The block must:
- Set `archivesName` to `"kafka-http-server"`
- Declare dependencies on `:core`, `:clients`, `:server-common`, `:server`
- Add Netty dependency (`libs.nettyAll`)
- Add Jackson dependencies (already in `libs`: `jacksonDatabind`)
- Add test dependencies: JUnit 5, Mockito, and test outputs from `:core`, `:clients`, `:server-common`

### 4. Create `http-server/` directory structure

Create the following directory tree:

```
http-server/
  src/
    main/
      java/
        kafka/
          server/
            http/
              .gitkeep
      resources/
    test/
      java/
        kafka/
          server/
            http/
              .gitkeep
```

The `.gitkeep` files are placeholders so Git tracks the empty directories. They will be
replaced by actual source files in subsequent tasks.

### 5. Create `HttpServerConfigs.java` in the `:server` module

Create a new file at:
`/home/anh/kafka/server/src/main/java/org/apache/kafka/network/HttpServerConfigs.java`

This class defines all HTTP-specific configuration properties using the same pattern as
`SocketServerConfigs`:
- Public static final String constants for config keys
- Public static final default values
- Public static final String constants for documentation
- A `public static final ConfigDef CONFIG_DEF` that registers all properties

## Implementation Details

### Netty version selection

Use Netty 4.1.x (latest stable in the 4.1 line). Netty 4.1 is the standard for
production JVM HTTP servers and provides the `HttpServerCodec`, `HttpObjectAggregator`,
`HttpContentCompressor`, and `IdleStateHandler` classes referenced in the design doc.

The `netty-all` artifact is used (instead of individual `netty-handler`, `netty-codec-http`,
etc.) for simplicity. This is acceptable because the HTTP server uses codec, handler,
transport, and buffer components. A future optimization can narrow to specific artifacts.

### Config property details (from design doc section 10)

| Property | Type | Default | Importance | Validator | Description |
|---|---|---|---|---|---|
| `http.enabled` | BOOLEAN | `false` | HIGH | -- | Master switch for HTTP listener |
| `num.http.network.threads` | INT | `4` | HIGH | atLeast(1) | Netty worker thread count |
| `http.request.max.bytes` | INT | `10485760` (10 MB) | HIGH | atLeast(1) | Max HTTP request body size |
| `http.produce.max.records` | INT | `10000` | MEDIUM | atLeast(1) | Max records per produce request |
| `http.response.timeout.ms` | INT | `30000` | MEDIUM | atLeast(1) | Max time before 504 to HTTP client |
| `http.consume.max.wait.ms` | INT | `5000` | MEDIUM | atLeast(0) | Server-side cap on consume maxWaitMs |
| `http.consume.max.bytes` | INT | `1048576` (1 MB) | MEDIUM | atLeast(1) | Max response bytes for consume |
| `http.internal.forwarding.timeout.ms` | INT | `10000` | MEDIUM | atLeast(1) | Timeout for broker-to-broker forwarding |
| `http.internal.forwarding.retries` | INT | `1` | LOW | atLeast(0) | Max retries per forwarded group |
| `http.internal.forwarding.queue.size` | INT | `10000` | MEDIUM | atLeast(1) | Bounded queue per forward thread |
| `http.cors.allowed.origins` | STRING | `""` | LOW | -- | CORS allowed origins (comma-separated) |
| `http.connection.idle.timeout.ms` | LONG | `60000` | MEDIUM | atLeast(0) | Idle HTTP keep-alive connection timeout |
| `http.shutdown.drain.ms` | INT | `2000` | LOW | atLeast(0) | Graceful shutdown drain window |
| `num.http.async.threads` | INT | `4` | MEDIUM | atLeast(1) | Thread pool for async future completion |

### ConfigDef pattern

Follow the exact pattern from `SocketServerConfigs` (lines 155-171):

```java
public static final ConfigDef CONFIG_DEF = new ConfigDef()
    .define(HTTP_ENABLED_CONFIG, BOOLEAN, HTTP_ENABLED_DEFAULT, HIGH, HTTP_ENABLED_DOC)
    .define(NUM_HTTP_NETWORK_THREADS_CONFIG, INT, NUM_HTTP_NETWORK_THREADS_DEFAULT, atLeast(1), HIGH, NUM_HTTP_NETWORK_THREADS_DOC)
    // ... all other properties
```

Use static imports for `ConfigDef.Importance.*`, `ConfigDef.Range.*`, `ConfigDef.Type.*`.

## Skeleton Code

### gradle/dependencies.gradle (additions only)

In the `versions` block, add:

```groovy
  netty: "4.1.118.Final",
```

In the `libs` block, add:

```groovy
  nettyAll: "io.netty:netty-all:$versions.netty",
```

### settings.gradle (addition only)

Add `'http-server'` to the include list between `'group-coordinator:group-coordinator-api'`
and `'jmh-benchmarks'`.

### build.gradle (new project block)

```groovy
project(':http-server') {
  base {
    archivesName = "kafka-http-server"
  }

  dependencies {
    implementation project(':core')
    implementation project(':clients')
    implementation project(':server-common')
    implementation project(':server')

    implementation libs.nettyAll
    implementation libs.jacksonDatabind
    implementation libs.slf4jApi

    testImplementation project(':core').sourceSets.test.output
    testImplementation project(':clients').sourceSets.test.output
    testImplementation project(':server-common').sourceSets.test.output

    testImplementation libs.mockitoCore
    testImplementation libs.junitJupiter
    testImplementation testLog4j2Libs

    testRuntimeOnly runtimeTestLibs
  }
}
```

### HttpServerConfigs.java (full file)

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
            "When set to true, the broker starts an HTTP listener on the port specified in the " +
            "'listeners' configuration. Alternatively, adding an HTTP:// or HTTPS:// entry to " +
            "'listeners' implicitly enables the HTTP listener regardless of this setting.";

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
```

### Directory structure to create

```bash
mkdir -p http-server/src/main/java/kafka/server/http
mkdir -p http-server/src/main/resources
mkdir -p http-server/src/test/java/kafka/server/http
touch http-server/src/main/java/kafka/server/http/.gitkeep
touch http-server/src/test/java/kafka/server/http/.gitkeep
```

## Tests

### HttpServerConfigsTest.java (new file)

Create at:
`/home/anh/kafka/server/src/test/java/org/apache/kafka/network/HttpServerConfigsTest.java`

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
package org.apache.kafka.network;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HttpServerConfigsTest {

    @Test
    void testConfigDefNotNull() {
        assertNotNull(HttpServerConfigs.CONFIG_DEF);
    }

    @Test
    void testAllConfigKeysRegistered() {
        ConfigDef configDef = HttpServerConfigs.CONFIG_DEF;
        assertTrue(configDef.names().contains(HttpServerConfigs.HTTP_ENABLED_CONFIG));
        assertTrue(configDef.names().contains(HttpServerConfigs.NUM_HTTP_NETWORK_THREADS_CONFIG));
        assertTrue(configDef.names().contains(HttpServerConfigs.HTTP_REQUEST_MAX_BYTES_CONFIG));
        assertTrue(configDef.names().contains(HttpServerConfigs.HTTP_PRODUCE_MAX_RECORDS_CONFIG));
        assertTrue(configDef.names().contains(HttpServerConfigs.HTTP_RESPONSE_TIMEOUT_MS_CONFIG));
        assertTrue(configDef.names().contains(HttpServerConfigs.HTTP_CONSUME_MAX_WAIT_MS_CONFIG));
        assertTrue(configDef.names().contains(HttpServerConfigs.HTTP_CONSUME_MAX_BYTES_CONFIG));
        assertTrue(configDef.names().contains(HttpServerConfigs.HTTP_INTERNAL_FORWARDING_TIMEOUT_MS_CONFIG));
        assertTrue(configDef.names().contains(HttpServerConfigs.HTTP_INTERNAL_FORWARDING_RETRIES_CONFIG));
        assertTrue(configDef.names().contains(HttpServerConfigs.HTTP_INTERNAL_FORWARDING_QUEUE_SIZE_CONFIG));
        assertTrue(configDef.names().contains(HttpServerConfigs.HTTP_CORS_ALLOWED_ORIGINS_CONFIG));
        assertTrue(configDef.names().contains(HttpServerConfigs.HTTP_CONNECTION_IDLE_TIMEOUT_MS_CONFIG));
        assertTrue(configDef.names().contains(HttpServerConfigs.HTTP_SHUTDOWN_DRAIN_MS_CONFIG));
        assertTrue(configDef.names().contains(HttpServerConfigs.NUM_HTTP_ASYNC_THREADS_CONFIG));
    }

    @Test
    void testTotalConfigCount() {
        // Ensure we have exactly 14 config properties
        assertEquals(14, HttpServerConfigs.CONFIG_DEF.names().size());
    }

    @Test
    void testDefaultValues() {
        Map<String, Object> parsedDefaults = HttpServerConfigs.CONFIG_DEF.parse(new HashMap<>());

        assertEquals(false, parsedDefaults.get(HttpServerConfigs.HTTP_ENABLED_CONFIG));
        assertEquals(4, parsedDefaults.get(HttpServerConfigs.NUM_HTTP_NETWORK_THREADS_CONFIG));
        assertEquals(10 * 1024 * 1024, parsedDefaults.get(HttpServerConfigs.HTTP_REQUEST_MAX_BYTES_CONFIG));
        assertEquals(10000, parsedDefaults.get(HttpServerConfigs.HTTP_PRODUCE_MAX_RECORDS_CONFIG));
        assertEquals(30000, parsedDefaults.get(HttpServerConfigs.HTTP_RESPONSE_TIMEOUT_MS_CONFIG));
        assertEquals(5000, parsedDefaults.get(HttpServerConfigs.HTTP_CONSUME_MAX_WAIT_MS_CONFIG));
        assertEquals(1048576, parsedDefaults.get(HttpServerConfigs.HTTP_CONSUME_MAX_BYTES_CONFIG));
        assertEquals(10000, parsedDefaults.get(HttpServerConfigs.HTTP_INTERNAL_FORWARDING_TIMEOUT_MS_CONFIG));
        assertEquals(1, parsedDefaults.get(HttpServerConfigs.HTTP_INTERNAL_FORWARDING_RETRIES_CONFIG));
        assertEquals(10000, parsedDefaults.get(HttpServerConfigs.HTTP_INTERNAL_FORWARDING_QUEUE_SIZE_CONFIG));
        assertEquals("", parsedDefaults.get(HttpServerConfigs.HTTP_CORS_ALLOWED_ORIGINS_CONFIG));
        assertEquals(60000L, parsedDefaults.get(HttpServerConfigs.HTTP_CONNECTION_IDLE_TIMEOUT_MS_CONFIG));
        assertEquals(2000, parsedDefaults.get(HttpServerConfigs.HTTP_SHUTDOWN_DRAIN_MS_CONFIG));
        assertEquals(4, parsedDefaults.get(HttpServerConfigs.NUM_HTTP_ASYNC_THREADS_CONFIG));
    }

    @Test
    void testCustomValues() {
        Map<String, String> props = new HashMap<>();
        props.put(HttpServerConfigs.HTTP_ENABLED_CONFIG, "true");
        props.put(HttpServerConfigs.NUM_HTTP_NETWORK_THREADS_CONFIG, "8");
        props.put(HttpServerConfigs.HTTP_REQUEST_MAX_BYTES_CONFIG, "67108864"); // 64 MB
        props.put(HttpServerConfigs.HTTP_CONSUME_MAX_WAIT_MS_CONFIG, "10000");
        props.put(HttpServerConfigs.HTTP_CORS_ALLOWED_ORIGINS_CONFIG, "https://example.com");
        props.put(HttpServerConfigs.HTTP_CONNECTION_IDLE_TIMEOUT_MS_CONFIG, "120000");

        Map<String, Object> parsed = HttpServerConfigs.CONFIG_DEF.parse(props);

        assertEquals(true, parsed.get(HttpServerConfigs.HTTP_ENABLED_CONFIG));
        assertEquals(8, parsed.get(HttpServerConfigs.NUM_HTTP_NETWORK_THREADS_CONFIG));
        assertEquals(67108864, parsed.get(HttpServerConfigs.HTTP_REQUEST_MAX_BYTES_CONFIG));
        assertEquals(10000, parsed.get(HttpServerConfigs.HTTP_CONSUME_MAX_WAIT_MS_CONFIG));
        assertEquals("https://example.com", parsed.get(HttpServerConfigs.HTTP_CORS_ALLOWED_ORIGINS_CONFIG));
        assertEquals(120000L, parsed.get(HttpServerConfigs.HTTP_CONNECTION_IDLE_TIMEOUT_MS_CONFIG));
    }

    @Test
    void testNetworkThreadsRejectsZero() {
        Map<String, String> props = new HashMap<>();
        props.put(HttpServerConfigs.NUM_HTTP_NETWORK_THREADS_CONFIG, "0");
        assertThrows(ConfigException.class, () -> HttpServerConfigs.CONFIG_DEF.parse(props));
    }

    @Test
    void testNetworkThreadsRejectsNegative() {
        Map<String, String> props = new HashMap<>();
        props.put(HttpServerConfigs.NUM_HTTP_NETWORK_THREADS_CONFIG, "-1");
        assertThrows(ConfigException.class, () -> HttpServerConfigs.CONFIG_DEF.parse(props));
    }

    @Test
    void testRequestMaxBytesRejectsZero() {
        Map<String, String> props = new HashMap<>();
        props.put(HttpServerConfigs.HTTP_REQUEST_MAX_BYTES_CONFIG, "0");
        assertThrows(ConfigException.class, () -> HttpServerConfigs.CONFIG_DEF.parse(props));
    }

    @Test
    void testForwardingRetriesAllowsZero() {
        Map<String, String> props = new HashMap<>();
        props.put(HttpServerConfigs.HTTP_INTERNAL_FORWARDING_RETRIES_CONFIG, "0");
        Map<String, Object> parsed = HttpServerConfigs.CONFIG_DEF.parse(props);
        assertEquals(0, parsed.get(HttpServerConfigs.HTTP_INTERNAL_FORWARDING_RETRIES_CONFIG));
    }

    @Test
    void testConsumeMaxWaitAllowsZero() {
        Map<String, String> props = new HashMap<>();
        props.put(HttpServerConfigs.HTTP_CONSUME_MAX_WAIT_MS_CONFIG, "0");
        Map<String, Object> parsed = HttpServerConfigs.CONFIG_DEF.parse(props);
        assertEquals(0, parsed.get(HttpServerConfigs.HTTP_CONSUME_MAX_WAIT_MS_CONFIG));
    }

    @Test
    void testIdleTimeoutAllowsZero() {
        Map<String, String> props = new HashMap<>();
        props.put(HttpServerConfigs.HTTP_CONNECTION_IDLE_TIMEOUT_MS_CONFIG, "0");
        Map<String, Object> parsed = HttpServerConfigs.CONFIG_DEF.parse(props);
        assertEquals(0L, parsed.get(HttpServerConfigs.HTTP_CONNECTION_IDLE_TIMEOUT_MS_CONFIG));
    }

    @Test
    void testShutdownDrainAllowsZero() {
        Map<String, String> props = new HashMap<>();
        props.put(HttpServerConfigs.HTTP_SHUTDOWN_DRAIN_MS_CONFIG, "0");
        Map<String, Object> parsed = HttpServerConfigs.CONFIG_DEF.parse(props);
        assertEquals(0, parsed.get(HttpServerConfigs.HTTP_SHUTDOWN_DRAIN_MS_CONFIG));
    }

    @Test
    void testAsyncThreadsRejectsZero() {
        Map<String, String> props = new HashMap<>();
        props.put(HttpServerConfigs.NUM_HTTP_ASYNC_THREADS_CONFIG, "0");
        assertThrows(ConfigException.class, () -> HttpServerConfigs.CONFIG_DEF.parse(props));
    }
}
```

### Gradle build verification

After all files are created, run these commands to verify:

```bash
# Verify the module is recognized by Gradle
./gradlew projects | grep http-server

# Verify the http-server module compiles (empty src, but dependencies resolve)
./gradlew :http-server:compileJava

# Verify HttpServerConfigs compiles and tests pass
./gradlew :server:compileJava
./gradlew :server:test --tests "org.apache.kafka.network.HttpServerConfigsTest"
```

## Rules

1. **Do not modify existing submodule dependencies.** Only add the new `project(':http-server')`
   block. Do not change `:core`, `:server`, `:clients`, or any other project block.

2. **Netty version must be pinned in `gradle/dependencies.gradle`.** Do not hardcode the
   version in `build.gradle`. Use `libs.nettyAll` (or equivalent reference) in the dependency
   declaration.

3. **`HttpServerConfigs` goes in the `:server` module**, not `:http-server`. This is because
   `KafkaConfig` (in `:server`) must reference these config definitions. The `:http-server`
   module depends on `:server`, not the other way around.

4. **All config property names must use the `http.` prefix** to avoid collision with existing
   config properties. The only exceptions are `num.http.network.threads` and
   `num.http.async.threads` which use the `num.` prefix for consistency with
   `num.network.threads` and `num.io.threads`.

5. **Config documentation strings must be self-contained.** Each doc string should explain
   what the property does, what the default means, and any important caveats. A developer
   reading `kafka-configs.sh --describe` should understand the property without looking at
   the design doc.

6. **Do not add HTTP-specific code to `KafkaConfig` yet.** This task only creates the
   `ConfigDef` and the module skeleton. Wiring `HttpServerConfigs.CONFIG_DEF` into
   `KafkaConfig` is a separate task.

7. **Follow the Kafka build convention: one project block per submodule in `build.gradle`.**
   The block must set `archivesName` and declare all dependencies.

8. **Run `./gradlew :server:test` to verify no existing tests break** from the addition of
   `HttpServerConfigs`.

## Learning

- The task skeleton included `assertFalse` in the test imports but no test used it. Checkstyle caught the unused import; removed it.
- Kafka's `build.gradle` project blocks are NOT strictly alphabetical -- they follow a logical grouping. The `http-server` block was placed after `group-coordinator` (line 1566) and before `test-common` (line 1569).
- Worktree files are independent copies; edits must target the worktree path, not the main repo path.

## Limitations

- The `http-server` module has no source files yet (only `.gitkeep`), so `:http-server:compileJava` reports `NO-SOURCE`. This is expected -- subsequent tasks will add real source files.
- `HttpServerConfigs.CONFIG_DEF` is not yet wired into `KafkaConfig`; that is a separate task.

## Field Notes

- All 13 tests in `HttpServerConfigsTest` pass (13 test methods, not 14 -- there are 14 config properties but 13 test methods covering registration, defaults, custom values, and validation).
- Netty 4.1.118.Final resolves successfully from Maven Central.
- The `// Time: Created - TASK-A.02` comment was placed after the license header, before the `package` declaration, matching the convention for new files in this task series.

## Acceptance Criteria

- [x] `settings.gradle` includes `'http-server'` in the project list
- [x] `./gradlew projects` output lists `:http-server`
- [x] `gradle/dependencies.gradle` has `netty` version entry and `nettyAll` lib entry
- [x] `build.gradle` has `project(':http-server')` block with correct dependencies
- [x] `http-server/src/main/java/kafka/server/http/` directory exists
- [x] `http-server/src/test/java/kafka/server/http/` directory exists
- [x] `./gradlew :http-server:compileJava` succeeds (dependencies resolve)
- [x] `HttpServerConfigs.java` exists at `server/src/main/java/org/apache/kafka/network/HttpServerConfigs.java`
- [x] `HttpServerConfigs.CONFIG_DEF` registers exactly 14 properties
- [x] All 14 config keys have correct types: `http.enabled` is BOOLEAN, `http.connection.idle.timeout.ms` is LONG, all others are INT or STRING as specified
- [x] All config keys have correct default values matching the specification table
- [x] Validation works: `num.http.network.threads=0` throws `ConfigException`
- [x] Validation works: `http.internal.forwarding.retries=0` is accepted (atLeast(0))
- [x] Validation works: `http.consume.max.wait.ms=0` is accepted (atLeast(0))
- [x] `HttpServerConfigsTest` passes all assertions
- [x] `./gradlew :server:test` passes with zero new failures
- [x] No existing submodule build files are modified (only `build.gradle`, `settings.gradle`, and `gradle/dependencies.gradle` at root)

## File Manifest

| File | Action |
|---|---|
| `gradle/dependencies.gradle` | Modified -- added `netty: "4.1.118.Final"` to versions, `nettyAll` to libs |
| `settings.gradle` | Modified -- added `'http-server'` to include list |
| `build.gradle` | Modified -- added `project(':http-server')` block |
| `http-server/src/main/java/kafka/server/http/.gitkeep` | Created |
| `http-server/src/main/resources/` | Created (empty directory) |
| `http-server/src/test/java/kafka/server/http/.gitkeep` | Created |
| `server/src/main/java/org/apache/kafka/network/HttpServerConfigs.java` | Created -- 14 HTTP config properties |
| `server/src/test/java/org/apache/kafka/network/HttpServerConfigsTest.java` | Created -- 13 test methods |
