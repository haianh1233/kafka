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

import org.apache.kafka.server.metrics.KafkaYammerMetrics;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HttpMetricsTest {

    private HttpMetrics metrics;

    @BeforeEach
    void setUp() {
        metrics = new HttpMetrics();
    }

    @AfterEach
    void tearDown() {
        metrics.close();
    }

    @Test
    void testProduceRequestRateRegistered() {
        assertNotNull(metrics.produceRequestRate);
        metrics.produceRequestRate.mark();
        assertTrue(metrics.produceRequestRate.count() > 0);
    }

    @Test
    void testConsumeRequestRateRegistered() {
        assertNotNull(metrics.consumeRequestRate);
        metrics.consumeRequestRate.mark();
        assertTrue(metrics.consumeRequestRate.count() > 0);
    }

    @Test
    void testForwardRequestRateRegistered() {
        assertNotNull(metrics.forwardRequestRate);
        metrics.forwardRequestRate.mark();
        metrics.forwardRequestRate.mark();
        assertEquals(2, metrics.forwardRequestRate.count());
    }

    @Test
    void testForwardErrorRateRegistered() {
        assertNotNull(metrics.forwardErrorRate);
    }

    @Test
    void testQueueFullRateRegistered() {
        assertNotNull(metrics.queueFullRate);
    }

    @Test
    void testIdleConnectionsClosedRateRegistered() {
        assertNotNull(metrics.idleConnectionsClosedRate);
    }

    @Test
    void testForwardQueueGaugeRegistration() {
        metrics.registerForwardQueueGauge(1, () -> 42);
        // Verify gauge is registered in Yammer registry
        boolean found = KafkaYammerMetrics.defaultRegistry().allMetrics().entrySet().stream()
            .anyMatch(e -> e.getKey().getName().equals("ForwardQueueSize")
                && e.getKey().getMBeanName().contains("broker.id=1"));
        assertTrue(found, "ForwardQueueSize gauge for broker 1 should be registered");
    }

    @Test
    void testForwardQueueGaugeRemoval() {
        metrics.registerForwardQueueGauge(2, () -> 10);
        metrics.removeForwardQueueGauge(2);
        boolean found = KafkaYammerMetrics.defaultRegistry().allMetrics().entrySet().stream()
            .anyMatch(e -> e.getKey().getName().equals("ForwardQueueSize")
                && e.getKey().getMBeanName().contains("broker.id=2"));
        assertFalse(found, "ForwardQueueSize gauge for broker 2 should be removed");
    }

    @Test
    void testCloseRemovesAllMetrics() {
        metrics.registerForwardQueueGauge(3, () -> 0);
        metrics.close();
        // After close, no HTTP metrics should remain
        long httpMetricCount = KafkaYammerMetrics.defaultRegistry().allMetrics().entrySet().stream()
            .filter(e -> e.getKey().getMBeanName().contains("protocol=http"))
            .count();
        assertEquals(0, httpMetricCount,
            "All HTTP metrics should be deregistered after close()");
    }

    @Test
    void testMultipleForwardQueueGauges() {
        metrics.registerForwardQueueGauge(1, () -> 10);
        metrics.registerForwardQueueGauge(2, () -> 20);
        metrics.registerForwardQueueGauge(3, () -> 30);

        long gaugeCount = KafkaYammerMetrics.defaultRegistry().allMetrics().entrySet().stream()
            .filter(e -> e.getKey().getName().equals("ForwardQueueSize"))
            .count();
        assertEquals(3, gaugeCount);
    }
}
