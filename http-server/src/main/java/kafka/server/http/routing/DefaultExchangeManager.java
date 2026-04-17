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

// Time: Created - TASK-WS2.05

package kafka.server.http.routing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * Manages the default exchange ({@code ""}) and implicit queue bindings.
 *
 * <p>The default exchange is a pre-declared direct exchange (per AMQP 0-9-1
 * §3.1.3.3). Every declared queue is automatically bound to it with
 * {@code routingKey = queueName}, enabling simple publish-to-queue-by-name
 * without explicit binding setup:
 *
 * <pre>
 *   Publish: exchange="", routingKey="orders"
 *     → direct match against implicit binding (key="orders")
 *     → delivered to queue "orders"
 * </pre>
 *
 * <p>Integration points:
 * <ul>
 *   <li>{@link #initialize(ExchangeManager, String)} — called once per vhost
 *       during broker startup / vhost bootstrap; delegates to
 *       {@link ExchangeManager#initializeDefaults(String)}, which also
 *       registers the four {@code amq.*} pre-declared exchanges. Idempotent.</li>
 *   <li>{@link #onQueueDeclared(BindingManager, String)} — called by the queue
 *       manager on queue declare. Adds an implicit binding via
 *       {@link BindingManager#bind(String, String, String)}. Idempotent:
 *       {@code BindingManager} deduplicates on
 *       {@code (exchange, queue, routingKey, arguments)}.</li>
 *   <li>{@link #onQueueDeleted(BindingManager, String)} — called by the queue
 *       manager on queue delete. Removes the implicit binding. Silent no-op
 *       if the binding is already absent. Note: if the queue is being deleted
 *       for good, the caller typically also calls
 *       {@link BindingManager#removeAllForQueue(String)} to cascade across all
 *       exchanges; this hook targets only the default exchange's implicit
 *       binding so it can be used standalone when cascading isn't appropriate.</li>
 * </ul>
 *
 * <p><b>Protection.</b> The default exchange name ({@code ""}) is part of
 * {@link ExchangeManager#DEFAULT_EXCHANGE_NAMES}, so
 * {@link ExchangeManager#deleteExchange(String, String, boolean)} already
 * rejects attempts to delete it with
 * {@link ExchangeException.ErrorCode#EXCHANGE_PROTECTED}.
 *
 * <p><b>Design note.</b> This class is a stateless utility. It does not hold
 * any mutable state — it's a thin facade composing {@link ExchangeManager}
 * and {@link BindingManager} so that callers don't have to remember the
 * empty-string exchange name and the {@code routingKey = queueName}
 * convention.
 *
 * // Time: Created - TASK-WS2.05
 */
public final class DefaultExchangeManager {

    private static final Logger log = LoggerFactory.getLogger(DefaultExchangeManager.class);

    /** The default exchange name, per AMQP 0-9-1 §3.1.3.3 (empty string). */
    public static final String DEFAULT_EXCHANGE_NAME = "";

    private DefaultExchangeManager() {
        // utility class
    }

    /**
     * Ensures the default exchange (and the other {@code amq.*} pre-declared
     * exchanges) are registered for the given vhost. Idempotent: safe to call
     * multiple times.
     *
     * @param exchangeManager the exchange manager (not {@code null})
     * @param vhost           the virtual host (not {@code null}; typically
     *                        {@code "/"} for the default vhost)
     * @throws NullPointerException if any argument is {@code null}
     */
    public static void initialize(ExchangeManager exchangeManager, String vhost) {
        Objects.requireNonNull(exchangeManager, "exchangeManager");
        Objects.requireNonNull(vhost, "vhost");
        exchangeManager.initializeDefaults(vhost);
        log.debug("Default exchange initialized for vhost='{}'", vhost);
    }

    /**
     * Adds an implicit binding for a newly declared queue. The binding uses
     * {@code routingKey = queueName}, so publishing to
     * {@code exchange=""} with {@code routingKey="myQueue"} routes to queue
     * {@code "myQueue"}.
     *
     * <p>Idempotent: re-declaring a queue that already has an implicit binding
     * is a silent no-op (BindingManager deduplicates).
     *
     * @param bindingManager the binding manager (not {@code null})
     * @param queueName      the queue name (not {@code null}); used both as
     *                       the binding target and the routing key
     * @throws NullPointerException     if any argument is {@code null}
     * @throws IllegalArgumentException if the default exchange has not been
     *                                  initialized, or the queue does not
     *                                  exist (per {@code BindingManager}'s
     *                                  existence predicates)
     */
    public static void onQueueDeclared(BindingManager bindingManager, String queueName) {
        Objects.requireNonNull(bindingManager, "bindingManager");
        Objects.requireNonNull(queueName, "queueName");
        bindingManager.bind(DEFAULT_EXCHANGE_NAME, queueName, queueName);
        log.debug("Implicit binding added: default exchange -> queue '{}' (key='{}')",
            queueName, queueName);
    }

    /**
     * Removes the implicit binding for a deleted queue. Silent no-op if the
     * binding is already absent (e.g. the exchange never had any bindings).
     *
     * @param bindingManager the binding manager (not {@code null})
     * @param queueName      the queue name (not {@code null})
     * @throws NullPointerException if any argument is {@code null}
     */
    public static void onQueueDeleted(BindingManager bindingManager, String queueName) {
        Objects.requireNonNull(bindingManager, "bindingManager");
        Objects.requireNonNull(queueName, "queueName");
        bindingManager.unbind(DEFAULT_EXCHANGE_NAME, queueName, queueName);
        log.debug("Implicit binding removed: default exchange -> queue '{}'", queueName);
    }
}
