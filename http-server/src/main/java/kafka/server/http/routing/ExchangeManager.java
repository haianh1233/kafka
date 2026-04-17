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

// Time: Created - TASK-WS1.06

package kafka.server.http.routing;

import kafka.server.http.ws.ExchangeMetadata;
import kafka.server.http.ws.WsConfigs;
import kafka.server.http.ws.WsRoutingMetadataManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Domain-level manager for AMQP-style exchange lifecycle operations. Validates
 * business rules (type compatibility on re-declare, protection of built-in
 * exchanges, per-vhost quota) and delegates persistence to
 * {@link WsRoutingMetadataManager}.
 *
 * <p>On startup {@link #initializeDefaults(String)} registers the five built-in
 * exchanges ({@code ""}, {@code amq.direct}, {@code amq.topic}, {@code amq.fanout},
 * {@code amq.headers}). These are synthesized directly into the in-memory cache
 * via the package-private {@link WsRoutingMetadataManager#applyRecord} path and
 * are NOT persisted to the {@code __ws_routing_metadata} topic — they always
 * exist for every vhost, even on a freshly-created broker with an empty metadata
 * topic.
 *
 * <p>Thread-safety: all mutating operations are serialized per-vhost via a
 * {@link java.util.concurrent.ConcurrentHashMap} of per-vhost locks so that
 * racing declare/delete calls cannot corrupt the cache or emit duplicate
 * metadata records. Reads hit the underlying manager directly and are lock-free.
 *
 * // Time: Created - TASK-WS1.06
 */
public class ExchangeManager {

    private static final Logger log = LoggerFactory.getLogger(ExchangeManager.class);

    /** Names of the 5 pre-declared built-in exchanges. */
    public static final Set<String> DEFAULT_EXCHANGE_NAMES = Set.of(
        "", "amq.direct", "amq.topic", "amq.fanout", "amq.headers");

    /** Must match {@code WsRoutingMetadataManager.EXCHANGE_PREFIX}. */
    private static final String EXCHANGE_KEY_PREFIX = "exchange:";

    /**
     * Default exchange name → type, in AMQP spec order. A {@link LinkedHashMap}
     * is used so {@link #initializeDefaults(String)} registers them in a
     * predictable order (useful for deterministic log output).
     */
    private static final Map<String, String> DEFAULT_EXCHANGE_TYPES;
    static {
        Map<String, String> m = new LinkedHashMap<>();
        m.put("", "direct");
        m.put("amq.direct", "direct");
        m.put("amq.topic", "topic");
        m.put("amq.fanout", "fanout");
        m.put("amq.headers", "headers");
        DEFAULT_EXCHANGE_TYPES = Collections.unmodifiableMap(m);
    }

    private final WsRoutingMetadataManager metadataManager;
    private final WsConfigs wsConfigs;

    /** Per-vhost monitors guarding declare/delete critical sections. */
    private final ConcurrentHashMap<String, Object> vhostLocks = new ConcurrentHashMap<>();

    public ExchangeManager(WsRoutingMetadataManager metadataManager, WsConfigs wsConfigs) {
        this.metadataManager = Objects.requireNonNull(metadataManager, "metadataManager");
        this.wsConfigs = Objects.requireNonNull(wsConfigs, "wsConfigs");
    }

    // ------------------------------------------------------------------
    // Initialization
    // ------------------------------------------------------------------

    /**
     * Registers the 5 built-in exchanges for {@code vhost} directly in the
     * metadata manager's in-memory cache without writing to the metadata topic.
     * Safe to call multiple times — subsequent calls are no-ops because the
     * exchanges are already present.
     */
    public void initializeDefaults(String vhost) {
        Objects.requireNonNull(vhost, "vhost");
        synchronized (lockFor(vhost)) {
            for (Map.Entry<String, String> entry : DEFAULT_EXCHANGE_TYPES.entrySet()) {
                String name = entry.getKey();
                String type = entry.getValue();
                if (metadataManager.getExchange(vhost, name) != null) {
                    continue; // already initialized
                }
                ExchangeMetadata synthetic = new ExchangeMetadata(
                    name, vhost, type,
                    /* durable */ true,
                    /* autoDelete */ false,
                    /* internal */ false,
                    Collections.emptyMap());
                // Synthesize via applyRecord so the entry enters the cache WITHOUT
                // invoking the recordWriter — defaults never hit the metadata topic.
                // Key format mirrors WsRoutingMetadataManager.exchangeKey(vhost, name):
                // "exchange:" + vhost + ":" + name.
                metadataManager.applyRecord(
                    EXCHANGE_KEY_PREFIX + vhost + ":" + name,
                    serializeSynthetic(synthetic));
            }
            log.debug("Initialized {} default exchanges for vhost={}", DEFAULT_EXCHANGE_TYPES.size(), vhost);
        }
    }

    // ------------------------------------------------------------------
    // Declare / delete
    // ------------------------------------------------------------------

    /**
     * Declares an exchange. See class Javadoc / task spec for semantics.
     *
     * @param passive if {@code true}, only check existence; never create or modify.
     */
    public ExchangeMetadata declareExchange(
            String vhost, String name, String type, boolean durable,
            boolean autoDelete, boolean passive, boolean internal,
            Map<String, String> arguments) throws ExchangeException {
        Objects.requireNonNull(vhost, "vhost");
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(type, "type");
        Map<String, String> safeArgs = arguments == null ? Collections.emptyMap() : arguments;

        synchronized (lockFor(vhost)) {
            ExchangeMetadata existing = metadataManager.getExchange(vhost, name);

            if (passive) {
                if (existing == null) {
                    throw new ExchangeException(
                        ExchangeException.ErrorCode.EXCHANGE_NOT_FOUND,
                        "Exchange '" + name + "' does not exist in vhost '" + vhost + "'");
                }
                return existing;
            }

            if (existing != null) {
                // Re-declare: same type → idempotent no-op (return existing, no write).
                //             different type → mismatch error.
                if (!existing.type().equals(type)) {
                    throw new ExchangeException(
                        ExchangeException.ErrorCode.EXCHANGE_TYPE_MISMATCH,
                        "Exchange '" + name + "' already exists with type '" + existing.type()
                            + "', cannot redeclare as '" + type + "'");
                }
                return existing;
            }

            // New exchange: enforce per-vhost quota.
            int currentCount = metadataManager.exchangeCount(vhost);
            if (currentCount >= wsConfigs.maxExchangesPerVhost()) {
                throw new ExchangeException(
                    ExchangeException.ErrorCode.EXCHANGE_LIMIT_EXCEEDED,
                    "Vhost '" + vhost + "' already has " + currentCount + " exchanges, "
                        + "limit is " + wsConfigs.maxExchangesPerVhost());
            }

            ExchangeMetadata created = new ExchangeMetadata(
                name, vhost, type, durable, autoDelete, internal, safeArgs);
            metadataManager.writeExchange(created);
            log.debug("Declared exchange vhost={} name={} type={} durable={} autoDelete={} internal={}",
                vhost, name, type, durable, autoDelete, internal);
            return created;
        }
    }

    /**
     * Deletes an exchange. Built-in exchanges are protected; deleting a
     * non-existent exchange is a no-op. When {@code ifUnused=true} the delete
     * fails with {@link ExchangeException.ErrorCode#EXCHANGE_IN_USE} if the
     * exchange has bindings.
     */
    public void deleteExchange(String vhost, String name, boolean ifUnused) throws ExchangeException {
        Objects.requireNonNull(vhost, "vhost");
        Objects.requireNonNull(name, "name");

        if (isDefaultExchange(name)) {
            throw new ExchangeException(
                ExchangeException.ErrorCode.EXCHANGE_PROTECTED,
                "Exchange '" + name + "' is a built-in pre-declared exchange and cannot be deleted");
        }

        synchronized (lockFor(vhost)) {
            ExchangeMetadata existing = metadataManager.getExchange(vhost, name);
            if (existing == null) {
                // No-op on missing exchange per AMQP delete semantics.
                return;
            }
            if (ifUnused && metadataManager.bindingCount(vhost, name) > 0) {
                throw new ExchangeException(
                    ExchangeException.ErrorCode.EXCHANGE_IN_USE,
                    "Exchange '" + name + "' has bindings; cannot delete with ifUnused=true");
            }
            // Delegates tombstone emission + cache removal (and binding cascade) to the manager.
            metadataManager.deleteExchange(vhost, name);
            log.debug("Deleted exchange vhost={} name={}", vhost, name);
        }
    }

    // ------------------------------------------------------------------
    // Read-only helpers (delegated, lock-free)
    // ------------------------------------------------------------------

    public ExchangeMetadata getExchange(String vhost, String name) {
        return metadataManager.getExchange(vhost, name);
    }

    public Collection<ExchangeMetadata> listExchanges(String vhost) {
        return metadataManager.listExchanges(vhost);
    }

    public boolean isDefaultExchange(String name) {
        return DEFAULT_EXCHANGE_NAMES.contains(name);
    }

    // ------------------------------------------------------------------
    // Internal
    // ------------------------------------------------------------------

    private Object lockFor(String vhost) {
        return vhostLocks.computeIfAbsent(vhost, v -> new Object());
    }

    /**
     * Serializes a default exchange for injection via {@link WsRoutingMetadataManager#applyRecord}.
     * We build the same JSON payload the manager itself would produce for a normal write,
     * so the replay path populates the cache with the right object.
     */
    private static byte[] serializeSynthetic(ExchangeMetadata ex) {
        StringBuilder sb = new StringBuilder(128);
        sb.append("{\"name\":\"").append(jsonEscape(ex.name())).append('\"');
        sb.append(",\"vhost\":\"").append(jsonEscape(ex.vhost())).append('\"');
        sb.append(",\"type\":\"").append(jsonEscape(ex.type())).append('\"');
        sb.append(",\"durable\":").append(ex.durable());
        sb.append(",\"autoDelete\":").append(ex.autoDelete());
        sb.append(",\"internal\":").append(ex.internal());
        sb.append(",\"arguments\":{}}");
        return sb.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    private static String jsonEscape(String s) {
        // Default exchange names contain only ASCII letters, '.', and '' (empty).
        // We still guard against '"' and '\' for forward-compat.
        StringBuilder out = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '"' || c == '\\') {
                out.append('\\');
            }
            out.append(c);
        }
        return out.toString();
    }
}
