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
// Time: Created - TASK-F.03
package kafka.server.http;

import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Meter;
import org.apache.kafka.server.metrics.KafkaMetricsGroup;

import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * HTTP-specific metrics registered with Kafka's Yammer metrics framework.
 *
 * All meters use a "protocol=http" tag to distinguish from binary protocol metrics.
 * These are additive to existing per-ApiKey metrics -- they do not replace them.
 *
 * Metric naming convention follows existing Kafka patterns:
 * - Meters: "RequestsPerSec" with different tag values
 * - Gauges: descriptive name with broker.id tag
 *
 * @see org.apache.kafka.server.metrics.KafkaMetricsGroup
 */
public class HttpMetrics implements Closeable {

    private static final String PACKAGE_NAME = "kafka.server.http";
    private static final String CLASS_NAME = "HttpMetrics";

    private final KafkaMetricsGroup metricsGroup;
    private final ConcurrentHashMap<Integer, Gauge<Integer>> forwardQueueGauges;

    // --- Meters ---

    /** Rate of HTTP produce requests received. */
    public final Meter produceRequestRate;

    /** Rate of HTTP consume (fetch) requests received. */
    public final Meter consumeRequestRate;

    /** Rate of requests forwarded to remote brokers. */
    public final Meter forwardRequestRate;

    /** Rate of forwarding errors (connection failure, timeout, etc.). */
    public final Meter forwardErrorRate;

    /** Rate of requests rejected because RequestChannel queue is full. */
    public final Meter queueFullRate;

    /** Rate of idle HTTP connections closed by the server. */
    public final Meter idleConnectionsClosedRate;

    public HttpMetrics() {
        this.metricsGroup = new KafkaMetricsGroup(PACKAGE_NAME, CLASS_NAME);
        this.forwardQueueGauges = new ConcurrentHashMap<>();

        this.produceRequestRate = metricsGroup.newMeter(
            "RequestsPerSec",
            "requests",
            TimeUnit.SECONDS,
            Map.of("protocol", "http", "request", "produce")
        );

        this.consumeRequestRate = metricsGroup.newMeter(
            "RequestsPerSec",
            "requests",
            TimeUnit.SECONDS,
            Map.of("protocol", "http", "request", "consume")
        );

        this.forwardRequestRate = metricsGroup.newMeter(
            "RequestsPerSec",
            "requests",
            TimeUnit.SECONDS,
            Map.of("protocol", "http", "request", "forward")
        );

        this.forwardErrorRate = metricsGroup.newMeter(
            "RequestsPerSec",
            "requests",
            TimeUnit.SECONDS,
            Map.of("protocol", "http", "request", "forward-error")
        );

        this.queueFullRate = metricsGroup.newMeter(
            "RequestsPerSec",
            "requests",
            TimeUnit.SECONDS,
            Map.of("protocol", "http", "request", "queue-full")
        );

        this.idleConnectionsClosedRate = metricsGroup.newMeter(
            "IdleConnectionsClosedPerSec",
            "connections",
            TimeUnit.SECONDS,
            Map.of("protocol", "http")
        );
    }

    /**
     * Register a forward queue size gauge for a target broker.
     * Called when a new ProduceForwardThread is created.
     *
     * @param brokerId   target broker ID
     * @param sizeSupplier supplier that returns current queue size
     */
    public void registerForwardQueueGauge(int brokerId, Supplier<Integer> sizeSupplier) {
        Gauge<Integer> gauge = metricsGroup.newGauge(
            "ForwardQueueSize",
            sizeSupplier,
            Map.of("protocol", "http", "broker.id", String.valueOf(brokerId))
        );
        forwardQueueGauges.put(brokerId, gauge);
    }

    /**
     * Remove a forward queue size gauge for a target broker.
     * Called when a ProduceForwardThread is cleaned up.
     *
     * @param brokerId target broker ID
     */
    public void removeForwardQueueGauge(int brokerId) {
        forwardQueueGauges.remove(brokerId);
        metricsGroup.removeMetric("ForwardQueueSize",
            Map.of("protocol", "http", "broker.id", String.valueOf(brokerId)));
    }

    @Override
    public void close() {
        metricsGroup.removeMetric("RequestsPerSec",
            Map.of("protocol", "http", "request", "produce"));
        metricsGroup.removeMetric("RequestsPerSec",
            Map.of("protocol", "http", "request", "consume"));
        metricsGroup.removeMetric("RequestsPerSec",
            Map.of("protocol", "http", "request", "forward"));
        metricsGroup.removeMetric("RequestsPerSec",
            Map.of("protocol", "http", "request", "forward-error"));
        metricsGroup.removeMetric("RequestsPerSec",
            Map.of("protocol", "http", "request", "queue-full"));
        metricsGroup.removeMetric("IdleConnectionsClosedPerSec",
            Map.of("protocol", "http"));
        forwardQueueGauges.keySet().forEach(this::removeForwardQueueGauge);
    }
}
