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
// Time: Created - TASK-WS2.07
package kafka.server.http.ws;

import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Broker-wide registry of active WebSocket connections, keyed by connection id
 * (typically {@code ctx.channel().id().asLongText()}). Provides the read and
 * lifecycle surface needed by the REST admin endpoints
 * (see TASK-WS2.07) without exposing the full {@code HttpProcessor} internals.
 *
 * <p>Thread-safe via {@link ConcurrentHashMap}. Register / unregister / lookup
 * are all O(1).
 *
 * <p>The {@code HttpProcessor} is the authoritative owner of a
 * {@code wsConnections} map today; this class is designed so that at wire-up
 * time a single shared registry instance can be passed to both the processor
 * and the REST handlers — keeping the single source of truth on the processor
 * while keeping the admin handlers decoupled from HTTP plumbing.
 *
 * // Time: Created - TASK-WS2.07
 */
public final class WsConnectionRegistry {

    private final ConcurrentHashMap<String, WsConnectionContext> connections = new ConcurrentHashMap<>();

    /** Registers a connection. Overwrites any previous entry with the same id. */
    public void register(String connectionId, WsConnectionContext ctx) {
        Objects.requireNonNull(connectionId, "connectionId");
        Objects.requireNonNull(ctx, "ctx");
        connections.put(connectionId, ctx);
    }

    /** Removes the connection with the given id. No-op if unknown. */
    public WsConnectionContext unregister(String connectionId) {
        if (connectionId == null) return null;
        return connections.remove(connectionId);
    }

    /** @return the context for the given id, or {@code null} if unknown. */
    public WsConnectionContext get(String connectionId) {
        if (connectionId == null) return null;
        return connections.get(connectionId);
    }

    /** @return whether a connection with the given id is currently registered. */
    public boolean contains(String connectionId) {
        if (connectionId == null) return false;
        return connections.containsKey(connectionId);
    }

    /**
     * Snapshot of connection ids. Safe to iterate concurrently with register /
     * unregister; reflects the state at call time.
     */
    public Collection<String> connectionIds() {
        return Collections.unmodifiableCollection(connections.keySet());
    }

    /**
     * Snapshot of {@code (id, ctx)} entries. Iteration is lock-free — entries
     * added or removed after the snapshot returns may or may not appear.
     */
    public Collection<WsConnectionContext> contexts() {
        return Collections.unmodifiableCollection(connections.values());
    }

    /** @return number of currently registered connections. */
    public int size() {
        return connections.size();
    }
}
