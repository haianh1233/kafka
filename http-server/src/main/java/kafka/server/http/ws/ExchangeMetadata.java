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
 * Immutable exchange metadata. Persisted to the {@code __ws_routing_metadata} topic
 * and held in the in-memory cache of {@link WsRoutingMetadataManager}.
 *
 * // Time: Created - TASK-WS1.05
 */
public final class ExchangeMetadata {

    private final String name;
    private final String vhost;
    private final String type; // "direct", "topic", "fanout", "headers"
    private final boolean durable;
    private final boolean autoDelete;
    private final boolean internal;
    private final Map<String, String> arguments;

    public ExchangeMetadata(String name,
                            String vhost,
                            String type,
                            boolean durable,
                            boolean autoDelete,
                            boolean internal,
                            Map<String, String> arguments) {
        this.name = Objects.requireNonNull(name, "name");
        this.vhost = Objects.requireNonNull(vhost, "vhost");
        this.type = Objects.requireNonNull(type, "type");
        this.durable = durable;
        this.autoDelete = autoDelete;
        this.internal = internal;
        this.arguments = arguments == null
            ? Collections.emptyMap()
            : Collections.unmodifiableMap(new HashMap<>(arguments));
    }

    public String name() {
        return name;
    }

    public String vhost() {
        return vhost;
    }

    public String type() {
        return type;
    }

    public boolean durable() {
        return durable;
    }

    public boolean autoDelete() {
        return autoDelete;
    }

    public boolean internal() {
        return internal;
    }

    public Map<String, String> arguments() {
        return arguments;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ExchangeMetadata)) return false;
        ExchangeMetadata that = (ExchangeMetadata) o;
        return durable == that.durable
            && autoDelete == that.autoDelete
            && internal == that.internal
            && name.equals(that.name)
            && vhost.equals(that.vhost)
            && type.equals(that.type)
            && arguments.equals(that.arguments);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, vhost, type, durable, autoDelete, internal, arguments);
    }

    @Override
    public String toString() {
        return "ExchangeMetadata{name='" + name + "', vhost='" + vhost + "', type='" + type
            + "', durable=" + durable + ", autoDelete=" + autoDelete + ", internal=" + internal
            + ", arguments=" + arguments + '}';
    }
}
