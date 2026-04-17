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
// Time: Update - TASK-WS3.07 - added drain support
package kafka.server.http.ws;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private static final Logger log = LoggerFactory.getLogger(WsConnectionRegistry.class);

    /** Close-frame code used during graceful shutdown. RFC 6455 §7.4.1: "Going Away". */
    static final int CLOSE_CODE_GOING_AWAY = 1001;

    /** Close-frame reason broadcast to clients at drain time. */
    static final String CLOSE_REASON_DRAINING = "server shutting down";

    /** Poll interval while waiting for connections to clear during drain. */
    private static final long DRAIN_POLL_MS = 25L;

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

    /**
     * Graceful drain (TASK-WS3.07). Sends a WebSocket close frame with code
     * {@value #CLOSE_CODE_GOING_AWAY} ("{@value #CLOSE_REASON_DRAINING}") to
     * every registered connection, then polls until the registry is empty or
     * {@code timeoutMs} elapses.
     *
     * <p>Thread-safe. Calling {@code drain} on an empty registry returns
     * {@code true} immediately. Connections register/unregister lock-free, so
     * the pass over {@link #contexts()} is a snapshot — any connections
     * registered after the close-frame broadcast are not signalled and will
     * cause {@code drain} to time out.
     *
     * <p>The method does <em>not</em> force-close connections after the
     * timeout: it simply reports {@code false} to the caller. The caller is
     * expected to decide whether to proceed with shutdown anyway.
     *
     * @param timeoutMs hard upper bound on how long to wait for the registry
     *                  to empty, measured from the moment drain starts.
     *                  {@code <= 0} means "check once and return".
     * @return {@code true} if every connection cleared within the timeout;
     *         {@code false} if connections remained when the timeout expired.
     */
    public boolean drain(long timeoutMs) {
        // Snapshot before signalling — any connection that registers after this
        // point will not receive the close frame, which is fine: it either
        // finishes its own handshake and stays connected (caller's problem) or
        // times out here.
        Collection<WsConnectionContext> snapshot = contexts();
        if (snapshot.isEmpty()) {
            return true;
        }

        int count = snapshot.size();
        log.info("Draining {} WebSocket connection(s) with close code {}",
                count, CLOSE_CODE_GOING_AWAY);

        for (WsConnectionContext ctx : snapshot) {
            try {
                ctx.close(CLOSE_CODE_GOING_AWAY, CLOSE_REASON_DRAINING);
            } catch (RuntimeException e) {
                // Never let one broken connection stop the broadcast.
                log.warn("Failed to send drain close frame to session {}", ctx.sessionId(), e);
            }
        }

        long deadline = System.nanoTime() + Math.max(0L, timeoutMs) * 1_000_000L;
        while (!connections.isEmpty()) {
            long remainingNs = deadline - System.nanoTime();
            if (remainingNs <= 0L) {
                log.warn("Drain timed out after {} ms with {} connection(s) still registered",
                        timeoutMs, connections.size());
                return false;
            }
            long sleepMs = Math.min(DRAIN_POLL_MS, remainingNs / 1_000_000L + 1L);
            try {
                Thread.sleep(sleepMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Drain interrupted after {} ms with {} connection(s) still registered",
                        timeoutMs, connections.size());
                return false;
            }
        }
        return true;
    }
}
