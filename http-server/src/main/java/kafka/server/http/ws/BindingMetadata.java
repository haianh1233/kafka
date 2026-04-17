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
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Immutable binding metadata. Persisted to the {@code __ws_routing_metadata} topic
 * and held in the in-memory cache of {@link WsRoutingMetadataManager}.
 *
 * // Time: Created - TASK-WS1.05
 */
public final class BindingMetadata {

    private final String vhost;
    private final String exchange;
    private final String queue;
    private final String routingKey;
    private final Map<String, String> arguments;

    public BindingMetadata(String vhost,
                           String exchange,
                           String queue,
                           String routingKey,
                           Map<String, String> arguments) {
        this.vhost = Objects.requireNonNull(vhost, "vhost");
        this.exchange = Objects.requireNonNull(exchange, "exchange");
        this.queue = Objects.requireNonNull(queue, "queue");
        this.routingKey = Objects.requireNonNull(routingKey, "routingKey");
        this.arguments = arguments == null
            ? Collections.emptyMap()
            : Collections.unmodifiableMap(new HashMap<>(arguments));
    }

    public String vhost() {
        return vhost;
    }

    public String exchange() {
        return exchange;
    }

    public String queue() {
        return queue;
    }

    public String routingKey() {
        return routingKey;
    }

    public Map<String, String> arguments() {
        return arguments;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BindingMetadata)) return false;
        BindingMetadata that = (BindingMetadata) o;
        return vhost.equals(that.vhost)
            && exchange.equals(that.exchange)
            && queue.equals(that.queue)
            && routingKey.equals(that.routingKey)
            && arguments.equals(that.arguments);
    }

    @Override
    public int hashCode() {
        return Objects.hash(vhost, exchange, queue, routingKey, arguments);
    }

    @Override
    public String toString() {
        return "BindingMetadata{vhost='" + vhost + "', exchange='" + exchange
            + "', queue='" + queue + "', routingKey='" + routingKey
            + "', arguments=" + arguments + '}';
    }
}
