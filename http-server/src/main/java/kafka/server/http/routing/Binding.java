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

// Time: Created - TASK-WS1.07

package kafka.server.http.routing;

import java.util.Map;
import java.util.Objects;

/**
 * Immutable binding record connecting a queue to an exchange.
 *
 * <p>Equality uses {@code exchange}, {@code queue}, {@code routingKey}, and the
 * full {@code arguments} map — not just (queue, routingKey) — so headers-exchange
 * bindings that differ only by their match arguments are treated as distinct.
 *
 * // Time: Created - TASK-WS1.07
 */
public record Binding(
    String exchange,
    String queue,
    String routingKey,
    Map<String, String> arguments
) {
    public Binding {
        Objects.requireNonNull(exchange, "exchange");
        Objects.requireNonNull(queue, "queue");
        Objects.requireNonNull(routingKey, "routingKey");
        arguments = arguments == null ? Map.of() : Map.copyOf(arguments);
    }
}
