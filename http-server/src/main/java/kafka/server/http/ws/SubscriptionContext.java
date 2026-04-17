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

// Time: Created - TASK-WS1.15

package kafka.server.http.ws;

import java.util.Objects;

/**
 * Per-subscription state container held inside a {@link WsSubscriptionManager}.
 *
 * <p>Bundles the identity of the subscription, the Kafka topic mapping, and the
 * three cooperating state objects that together implement credit-based push
 * delivery on a WebSocket connection:
 *
 * <ul>
 *   <li>{@link WsDeliveryTagTracker} — assigns delivery tags and tracks ack state
 *       for gap-safe offset commits.</li>
 *   <li>{@link WsCreditManager} — credit-based flow control gate consulted by the
 *       fetch loop between fetch iterations.</li>
 *   <li>{@link WsConsumerFetchLoop} — the {@link Runnable} running on the
 *       {@code wsConsumerExecutor} that actually produces deliver frames.</li>
 * </ul>
 *
 * <p>Instances are constructed only by {@link WsSubscriptionManager#subscribe} and
 * are effectively immutable once published; the three cooperating components each
 * carry their own thread-safety guarantees.
 */
public final class SubscriptionContext {

    private final String subscriptionId;
    private final String queueName;
    private final String topic;
    private final WsDeliveryTagTracker deliveryTagTracker;
    private final WsCreditManager creditManager;
    private final WsConsumerFetchLoop fetchLoop;

    public SubscriptionContext(String subscriptionId,
                               String queueName,
                               String topic,
                               WsDeliveryTagTracker deliveryTagTracker,
                               WsCreditManager creditManager,
                               WsConsumerFetchLoop fetchLoop) {
        this.subscriptionId = Objects.requireNonNull(subscriptionId, "subscriptionId");
        this.queueName = Objects.requireNonNull(queueName, "queueName");
        this.topic = Objects.requireNonNull(topic, "topic");
        this.deliveryTagTracker = Objects.requireNonNull(deliveryTagTracker, "deliveryTagTracker");
        this.creditManager = Objects.requireNonNull(creditManager, "creditManager");
        this.fetchLoop = Objects.requireNonNull(fetchLoop, "fetchLoop");
    }

    public String subscriptionId() {
        return subscriptionId;
    }

    public String queueName() {
        return queueName;
    }

    public String topic() {
        return topic;
    }

    public WsDeliveryTagTracker deliveryTagTracker() {
        return deliveryTagTracker;
    }

    public WsCreditManager creditManager() {
        return creditManager;
    }

    public WsConsumerFetchLoop fetchLoop() {
        return fetchLoop;
    }
}
