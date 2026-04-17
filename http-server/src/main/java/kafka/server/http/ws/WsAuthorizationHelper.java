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
// Time: Created - TASK-WS3.05
package kafka.server.http.ws;

import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.AuthorizationResult;
import org.apache.kafka.server.authorizer.Authorizer;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;

/**
 * ACL authorization helper for WebSocket operations (design doc §19.2).
 *
 * <p>Single point of integration between the WS layer and Kafka's pluggable
 * {@link Authorizer}. Handlers call these check methods before executing a
 * privileged operation and surface denials as WS {@code error} frames with
 * {@code errorCode: "ACCESS_REFUSED"}.
 *
 * <h3>Operation → (resource, operation) mapping</h3>
 * <table>
 *   <caption>WS to ACL mapping</caption>
 *   <tr><th>WS operation</th><th>Resource</th><th>Operation</th></tr>
 *   <tr><td>{@code publish}</td><td>{@code TOPIC:ws.{queue}}</td><td>{@code WRITE}</td></tr>
 *   <tr><td>{@code subscribe}</td><td>{@code TOPIC:ws.{queue}}</td><td>{@code READ}</td></tr>
 *   <tr><td>{@code declare-exchange}</td><td>{@code CLUSTER}</td><td>{@code ALTER}</td></tr>
 *   <tr><td>{@code delete-exchange}</td><td>{@code CLUSTER}</td><td>{@code ALTER}</td></tr>
 *   <tr><td>{@code declare-queue}</td><td>{@code TOPIC:ws.{queue}}</td><td>{@code CREATE}</td></tr>
 *   <tr><td>{@code delete-queue}</td><td>{@code TOPIC:ws.{queue}}</td><td>{@code DELETE}</td></tr>
 *   <tr><td>{@code bind}/{@code unbind}</td><td>{@code TOPIC:ws.{queue}}</td><td>{@code ALTER}</td></tr>
 * </table>
 *
 * <h3>Multi-queue publish</h3>
 * <p>When a single {@code publish} fans out to N queues, {@link #filterUnauthorizedQueues}
 * returns the set of queues that failed the WRITE check; the caller must reject the ENTIRE
 * publish if the returned set is non-empty (no partial routing per design §19.2).
 *
 * <h3>userId validation</h3>
 * <p>{@link #validateUserId} enforces that an (optional) {@code message.userId} field on a
 * publish frame matches the authenticated principal name — an AMQP-inherited safety check
 * that prevents a client from impersonating another user at the application layer.
 *
 * <h3>Testability</h3>
 * <p>The helper depends only on the {@link Authorizer} interface. Tests wire a tiny
 * fake that returns {@link AuthorizationResult#ALLOWED} / {@link AuthorizationResult#DENIED}
 * per {@code (principal, resource, operation)} tuple; no real broker or ACL store is
 * needed.
 *
 * // Time: Created - TASK-WS3.05
 */
public final class WsAuthorizationHelper {

    /** AMQP error code emitted on any denial. */
    public static final String ACCESS_REFUSED = "ACCESS_REFUSED";

    private final Authorizer authorizer;

    /** Supplier used to translate the callsite connection into an {@link AuthorizableRequestContext}. */
    private final BiFunction<KafkaPrincipal, String, AuthorizableRequestContext> contextFactory;

    public WsAuthorizationHelper(Authorizer authorizer) {
        this(authorizer, WsAuthorizationHelper::defaultContext);
    }

    /**
     * Package-visible constructor used by tests that need to inspect the
     * {@link AuthorizableRequestContext} passed to the authorizer.
     */
    WsAuthorizationHelper(Authorizer authorizer,
                          BiFunction<KafkaPrincipal, String, AuthorizableRequestContext> contextFactory) {
        this.authorizer = Objects.requireNonNull(authorizer, "authorizer");
        this.contextFactory = Objects.requireNonNull(contextFactory, "contextFactory");
    }

    // ------------------------------------------------------------------
    //  Primary check — single resource
    // ------------------------------------------------------------------

    /**
     * Returns {@code true} iff the authorizer returns
     * {@link AuthorizationResult#ALLOWED} for the given resource/operation pair.
     *
     * @param principal     authenticated WS connection principal (non-null)
     * @param resourceType  e.g. {@link ResourceType#TOPIC}, {@link ResourceType#CLUSTER}
     * @param resourceName  resource name — for {@code CLUSTER} this is conventionally
     *                      {@code "kafka-cluster"}; for {@code TOPIC} the full topic name
     *                      (e.g. {@code "ws.orders"})
     * @param operation     e.g. {@link AclOperation#WRITE}, {@link AclOperation#READ}
     */
    public boolean authorize(KafkaPrincipal principal,
                             ResourceType resourceType, String resourceName,
                             AclOperation operation) {
        Objects.requireNonNull(principal, "principal");
        Objects.requireNonNull(resourceType, "resourceType");
        Objects.requireNonNull(resourceName, "resourceName");
        Objects.requireNonNull(operation, "operation");

        ResourcePattern pattern = new ResourcePattern(resourceType, resourceName, PatternType.LITERAL);
        Action action = new Action(operation, pattern, 1, true, true);
        AuthorizableRequestContext ctx = contextFactory.apply(principal, resourceName);
        List<AuthorizationResult> results = authorizer.authorize(ctx, Collections.singletonList(action));
        return !results.isEmpty() && results.get(0) == AuthorizationResult.ALLOWED;
    }

    // ------------------------------------------------------------------
    //  Multi-queue publish filter
    // ------------------------------------------------------------------

    /**
     * Returns the subset of {@code queueTopicNames} for which the principal is
     * NOT authorized for the given {@code operation} on resource type
     * {@link ResourceType#TOPIC}. An empty return value means every queue passed
     * the check.
     *
     * <p>The caller is expected to reject the entire publish/subscribe/etc. if
     * the return value is non-empty — multi-queue operations are atomic w.r.t.
     * authorization (design §19.2).
     *
     * @param queueTopicNames fully-qualified backing topic names (e.g. {@code "ws.orders"})
     */
    public Set<String> filterUnauthorizedQueues(KafkaPrincipal principal,
                                                Set<String> queueTopicNames,
                                                AclOperation operation) {
        Objects.requireNonNull(principal, "principal");
        Objects.requireNonNull(queueTopicNames, "queueTopicNames");
        Objects.requireNonNull(operation, "operation");

        if (queueTopicNames.isEmpty()) {
            return Collections.emptySet();
        }
        Set<String> denied = new LinkedHashSet<>();
        for (String topic : queueTopicNames) {
            if (!authorize(principal, ResourceType.TOPIC, topic, operation)) {
                denied.add(topic);
            }
        }
        return denied;
    }

    // ------------------------------------------------------------------
    //  userId field validation
    // ------------------------------------------------------------------

    /**
     * Validates an optional {@code message.userId} field against the authenticated
     * principal. An absent / empty userId is ALWAYS allowed (publisher opt-out).
     *
     * <p>Called from the publish path. On a {@code false} return the caller MUST
     * reject the publish with {@link #ACCESS_REFUSED} and NOT route the message.
     *
     * @return {@code true} if the userId matches the principal name, is absent, or is empty
     */
    public boolean validateUserId(KafkaPrincipal principal, String userId) {
        Objects.requireNonNull(principal, "principal");
        if (userId == null || userId.isEmpty()) {
            return true;
        }
        return principal.getName().equals(userId);
    }

    // ------------------------------------------------------------------
    //  Minimal AuthorizableRequestContext used for the call
    // ------------------------------------------------------------------

    /**
     * Default {@link AuthorizableRequestContext} built from the connection's
     * principal. The rest of the fields are populated with values appropriate
     * for a WebSocket control-plane request: the {@code wsListenerName} for the
     * listener; PLAINTEXT for the security protocol (the actual transport
     * security was validated at the upgrade handshake); {@code -1} for request
     * type/version to indicate this is not a Kafka protocol request.
     */
    static AuthorizableRequestContext defaultContext(KafkaPrincipal principal, String ignoredResource) {
        return new WsAuthorizableRequestContext(principal);
    }

    /**
     * Thin {@link AuthorizableRequestContext} implementation used for WS ACL
     * checks. The principal and listener name are the meaningful fields; other
     * getters return placeholder values that every {@code Authorizer}
     * implementation in tree tolerates.
     */
    static final class WsAuthorizableRequestContext implements AuthorizableRequestContext {
        private static final String WS_LISTENER_NAME = "WS";
        private final KafkaPrincipal principal;

        WsAuthorizableRequestContext(KafkaPrincipal principal) {
            this.principal = Objects.requireNonNull(principal, "principal");
        }

        @Override
        public String listenerName() {
            return WS_LISTENER_NAME;
        }

        @Override
        public org.apache.kafka.common.security.auth.SecurityProtocol securityProtocol() {
            return org.apache.kafka.common.security.auth.SecurityProtocol.PLAINTEXT;
        }

        @Override
        public KafkaPrincipal principal() {
            return principal;
        }

        @Override
        public java.net.InetAddress clientAddress() {
            try {
                return java.net.InetAddress.getByAddress(new byte[]{0, 0, 0, 0});
            } catch (java.net.UnknownHostException e) {
                // 0.0.0.0 is always a valid 4-byte address; the checked exception is unreachable.
                throw new IllegalStateException(e);
            }
        }

        @Override
        public int requestType() {
            return -1;
        }

        @Override
        public int requestVersion() {
            return -1;
        }

        @Override
        public String clientId() {
            return "ws";
        }

        @Override
        public int correlationId() {
            return -1;
        }
    }
}
