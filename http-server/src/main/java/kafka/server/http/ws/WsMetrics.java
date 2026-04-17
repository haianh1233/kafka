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
// Time: Created - TASK-WS3.08
package kafka.server.http.ws;

import com.yammer.metrics.core.Meter;
import org.apache.kafka.server.metrics.KafkaMetricsGroup;

import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * WebSocket-specific metrics registered with Kafka's Yammer metrics framework.
 *
 * <p>All meters and gauges carry a {@code protocol=ws} tag so operators can
 * distinguish WebSocket protocol traffic from the binary and HTTP protocols.
 * Per-queue gauges additionally carry a {@code queue=<name>} tag — these are
 * registered on queue creation and removed on deletion to avoid leaks.
 *
 * <p>The class follows the exact pattern of
 * {@link kafka.server.http.HttpMetrics} (TASK-F.03): construct the meters
 * eagerly in the constructor, register gauges lazily via the
 * {@code registerXxxGauge} methods, and tear everything down in {@link #close()}.
 *
 * <h3>Meters (rates)</h3>
 * <ul>
 *   <li>{@code PublishRate} — messages published per second (WS1.11)</li>
 *   <li>{@code DeliverRate} — messages delivered to consumers per second (WS1.15)</li>
 *   <li>{@code AckRate} — acks per second (WS1.16)</li>
 *   <li>{@code NackRate} — nacks per second (WS1.16)</li>
 *   <li>{@code ErrorRate} — error frames per second (WS1.04)</li>
 *   <li>{@code CreditExhaustedRate} — credit-exhaustion events (WS1.14)</li>
 *   <li>{@code RedeliveryRate} — redelivered messages (WS3.x)</li>
 *   <li>{@code DlxRate} — DLX-routed messages (WS4.x)</li>
 *   <li>{@code MandatoryReturnRate} — mandatory returns (WS3.02)</li>
 *   <li>{@code ConfirmRate} — publisher confirms (WS3.01)</li>
 *   <li>{@code ControlMessageRate} — inbound control messages (WS1.04)</li>
 * </ul>
 *
 * <h3>Gauges</h3>
 * <ul>
 *   <li>{@code ConnectionCount} — active WebSocket connections</li>
 *   <li>{@code SubscriptionCount} — active subscriptions across all connections</li>
 *   <li>{@code QueueDepth} per-queue — unconsumed message count</li>
 *   <li>{@code QueueConsumers} per-queue — active consumers on the queue</li>
 * </ul>
 *
 * <p>All gauges are registered via {@link Supplier}s so the metrics class does
 * not own the underlying state — callers (e.g., {@code WsConnectionRegistry},
 * {@code WsSubscriptionManager}, queue lifecycle managers) own the counters
 * and merely expose read access through the supplier.
 */
public class WsMetrics implements Closeable {

    private static final String PACKAGE_NAME = "kafka.server.http.ws";
    private static final String CLASS_NAME = "WsMetrics";
    private static final String PROTOCOL_TAG_KEY = "protocol";
    private static final String PROTOCOL_TAG_VALUE = "ws";
    private static final Map<String, String> WS_TAG =
        Map.of(PROTOCOL_TAG_KEY, PROTOCOL_TAG_VALUE);

    // --- Metric names (PascalCase per Kafka convention) ---

    private static final String METER_PUBLISH_RATE = "PublishRate";
    private static final String METER_DELIVER_RATE = "DeliverRate";
    private static final String METER_ACK_RATE = "AckRate";
    private static final String METER_NACK_RATE = "NackRate";
    private static final String METER_ERROR_RATE = "ErrorRate";
    private static final String METER_CREDIT_EXHAUSTED_RATE = "CreditExhaustedRate";
    private static final String METER_REDELIVERY_RATE = "RedeliveryRate";
    private static final String METER_DLX_RATE = "DlxRate";
    private static final String METER_MANDATORY_RETURN_RATE = "MandatoryReturnRate";
    private static final String METER_CONFIRM_RATE = "ConfirmRate";
    private static final String METER_CONTROL_MESSAGE_RATE = "ControlMessageRate";

    private static final String GAUGE_CONNECTION_COUNT = "ConnectionCount";
    private static final String GAUGE_SUBSCRIPTION_COUNT = "SubscriptionCount";
    private static final String GAUGE_QUEUE_DEPTH = "QueueDepth";
    private static final String GAUGE_QUEUE_CONSUMERS = "QueueConsumers";

    private final KafkaMetricsGroup metricsGroup;

    // Tracks queue names whose gauges are currently registered so close() can
    // tear them all down cleanly without walking the global registry.
    private final ConcurrentHashMap<String, Boolean> registeredQueues;
    private volatile boolean connectionCountGaugeRegistered;
    private volatile boolean subscriptionCountGaugeRegistered;

    // --- Meters (rates) ---

    public final Meter publishRate;
    public final Meter deliverRate;
    public final Meter ackRate;
    public final Meter nackRate;
    public final Meter errorRate;
    public final Meter creditExhaustedRate;
    public final Meter redeliveryRate;
    public final Meter dlxRate;
    public final Meter mandatoryReturnRate;
    public final Meter confirmRate;
    public final Meter controlMessageRate;

    public WsMetrics() {
        this.metricsGroup = new KafkaMetricsGroup(PACKAGE_NAME, CLASS_NAME);
        this.registeredQueues = new ConcurrentHashMap<>();
        this.publishRate = metricsGroup.newMeter(METER_PUBLISH_RATE, "messages", TimeUnit.SECONDS, WS_TAG);
        this.deliverRate = metricsGroup.newMeter(METER_DELIVER_RATE, "messages", TimeUnit.SECONDS, WS_TAG);
        this.ackRate = metricsGroup.newMeter(METER_ACK_RATE, "acks", TimeUnit.SECONDS, WS_TAG);
        this.nackRate = metricsGroup.newMeter(METER_NACK_RATE, "nacks", TimeUnit.SECONDS, WS_TAG);
        this.errorRate = metricsGroup.newMeter(METER_ERROR_RATE, "errors", TimeUnit.SECONDS, WS_TAG);
        this.creditExhaustedRate = metricsGroup.newMeter(METER_CREDIT_EXHAUSTED_RATE, "events", TimeUnit.SECONDS, WS_TAG);
        this.redeliveryRate = metricsGroup.newMeter(METER_REDELIVERY_RATE, "messages", TimeUnit.SECONDS, WS_TAG);
        this.dlxRate = metricsGroup.newMeter(METER_DLX_RATE, "messages", TimeUnit.SECONDS, WS_TAG);
        this.mandatoryReturnRate = metricsGroup.newMeter(METER_MANDATORY_RETURN_RATE, "returns", TimeUnit.SECONDS, WS_TAG);
        this.confirmRate = metricsGroup.newMeter(METER_CONFIRM_RATE, "confirms", TimeUnit.SECONDS, WS_TAG);
        this.controlMessageRate = metricsGroup.newMeter(METER_CONTROL_MESSAGE_RATE, "messages", TimeUnit.SECONDS, WS_TAG);
    }

    // ------------------------------------------------------------------
    //  Gauge registration (connection-level)
    // ------------------------------------------------------------------

    /**
     * Register a gauge reporting the number of currently-active WebSocket
     * connections. Typically wired to the size of {@code WsConnectionRegistry}.
     *
     * <p>Idempotent — a second call re-registers the gauge, replacing any prior
     * supplier.
     *
     * @param supplier returns the current connection count
     */
    public void registerConnectionCountGauge(Supplier<Integer> supplier) {
        metricsGroup.newGauge(GAUGE_CONNECTION_COUNT, supplier, WS_TAG);
        connectionCountGaugeRegistered = true;
    }

    /**
     * Register a gauge reporting the number of currently-active WS subscriptions
     * across all connections. Typically wired to the size of
     * {@code WsSubscriptionManager}.
     *
     * <p>Idempotent — a second call re-registers the gauge.
     *
     * @param supplier returns the current subscription count
     */
    public void registerSubscriptionCountGauge(Supplier<Integer> supplier) {
        metricsGroup.newGauge(GAUGE_SUBSCRIPTION_COUNT, supplier, WS_TAG);
        subscriptionCountGaugeRegistered = true;
    }

    // ------------------------------------------------------------------
    //  Gauge registration (per-queue)
    // ------------------------------------------------------------------

    /**
     * Register a {@code QueueDepth} gauge for the given queue. Called when a
     * queue is declared. Paired with {@link #removeQueueGauges(String)}.
     *
     * @param queueName the queue identifier (used as the {@code queue=...}
     *                  JMX tag; must not be empty)
     * @param supplier  returns the current unconsumed-message count
     */
    public void registerQueueDepthGauge(String queueName, Supplier<Long> supplier) {
        metricsGroup.newGauge(GAUGE_QUEUE_DEPTH, supplier, queueTags(queueName));
        registeredQueues.put(queueName, Boolean.TRUE);
    }

    /**
     * Register a {@code QueueConsumers} gauge for the given queue. Called when
     * a queue is declared. Paired with {@link #removeQueueGauges(String)}.
     *
     * @param queueName the queue identifier
     * @param supplier  returns the current consumer count for this queue
     */
    public void registerQueueConsumersGauge(String queueName, Supplier<Integer> supplier) {
        metricsGroup.newGauge(GAUGE_QUEUE_CONSUMERS, supplier, queueTags(queueName));
        registeredQueues.put(queueName, Boolean.TRUE);
    }

    /**
     * Remove both per-queue gauges ({@code QueueDepth}, {@code QueueConsumers})
     * for the given queue. Called when a queue is deleted. Idempotent — safe to
     * call when no gauges were ever registered.
     *
     * @param queueName the queue identifier
     */
    public void removeQueueGauges(String queueName) {
        metricsGroup.removeMetric(GAUGE_QUEUE_DEPTH, queueTags(queueName));
        metricsGroup.removeMetric(GAUGE_QUEUE_CONSUMERS, queueTags(queueName));
        registeredQueues.remove(queueName);
    }

    private static Map<String, String> queueTags(String queueName) {
        return Map.of(PROTOCOL_TAG_KEY, PROTOCOL_TAG_VALUE, "queue", queueName);
    }

    // ------------------------------------------------------------------
    //  Cleanup
    // ------------------------------------------------------------------

    /**
     * Deregister every WS metric this instance created. Safe to call multiple
     * times — subsequent calls are no-ops because the registry returns no
     * matching metrics to remove.
     */
    @Override
    public void close() {
        metricsGroup.removeMetric(METER_PUBLISH_RATE, WS_TAG);
        metricsGroup.removeMetric(METER_DELIVER_RATE, WS_TAG);
        metricsGroup.removeMetric(METER_ACK_RATE, WS_TAG);
        metricsGroup.removeMetric(METER_NACK_RATE, WS_TAG);
        metricsGroup.removeMetric(METER_ERROR_RATE, WS_TAG);
        metricsGroup.removeMetric(METER_CREDIT_EXHAUSTED_RATE, WS_TAG);
        metricsGroup.removeMetric(METER_REDELIVERY_RATE, WS_TAG);
        metricsGroup.removeMetric(METER_DLX_RATE, WS_TAG);
        metricsGroup.removeMetric(METER_MANDATORY_RETURN_RATE, WS_TAG);
        metricsGroup.removeMetric(METER_CONFIRM_RATE, WS_TAG);
        metricsGroup.removeMetric(METER_CONTROL_MESSAGE_RATE, WS_TAG);

        if (connectionCountGaugeRegistered) {
            metricsGroup.removeMetric(GAUGE_CONNECTION_COUNT, WS_TAG);
            connectionCountGaugeRegistered = false;
        }
        if (subscriptionCountGaugeRegistered) {
            metricsGroup.removeMetric(GAUGE_SUBSCRIPTION_COUNT, WS_TAG);
            subscriptionCountGaugeRegistered = false;
        }
        // Iterate via keySet() — removeQueueGauges mutates the map, so snapshot first.
        for (String q : registeredQueues.keySet().toArray(new String[0])) {
            removeQueueGauges(q);
        }
    }
}
