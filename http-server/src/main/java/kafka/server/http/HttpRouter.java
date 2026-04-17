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
// Time: Created - TASK-B.01
// Time: Update - TASK-WS2.06 - added exchange/queue/binding REST routes
// Time: Update - TASK-WS2.07 - added connection/consumer REST routes
// Time: Update - TASK-WS2.08 - added message-ops REST routes
// Time: Update - TASK-WS2.09 - added vhost scoping (vhost REST routes)
package kafka.server.http;

import io.netty.handler.codec.http.HttpMethod;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.internals.Topic;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Routes HTTP method + URI path to a Kafka handler type and extracts path parameters.
 * All topic name and client ID validation is performed here before any Kafka code runs.
 *
 * Route table:
 *   POST /v1/topics/{t}/records         -> PRODUCE
 *   POST /v1/topics/{t}/records:fetch   -> FETCH
 *   GET  /v1/topics/{t}                 -> METADATA_TOPIC
 *   GET  /v1/topics                     -> METADATA_ALL
 *   GET  /v1/topics/{t}/partitions/{p}/offsets -> LIST_OFFSETS
 *   GET  /v1/consumer-groups/{g}/lags   -> CONSUMER_LAG
 *   POST /v1/consumer-groups/{g}/offsets -> COMMIT_OFFSETS
 *   GET  /v1/consumer-groups/{g}/offsets -> FETCH_OFFSETS
 *   GET  /v1/health                     -> HEALTH
 *   GET  /v1/openapi.yaml              -> OPENAPI_SPEC
 */
public final class HttpRouter {

    // --- Handler type enum ---
    public enum HandlerType {
        PRODUCE,
        FETCH,
        METADATA_TOPIC,
        METADATA_ALL,
        LIST_OFFSETS,
        CONSUMER_LAG,
        COMMIT_OFFSETS,
        FETCH_OFFSETS,
        SHARE_POLL,
        SHARE_ACKNOWLEDGE,
        HEALTH,
        OPENAPI_SPEC,
        // --- WS2.06: REST routing CRUD ---
        DECLARE_EXCHANGE,
        GET_EXCHANGE,
        LIST_EXCHANGES,
        DELETE_EXCHANGE,
        DECLARE_QUEUE,
        GET_QUEUE,
        LIST_QUEUES,
        PATCH_QUEUE,
        DELETE_QUEUE,
        PURGE_QUEUE,
        CREATE_BINDING,
        LIST_BINDINGS,
        DELETE_BINDING,
        // --- WS2.07: Connection / consumer management ---
        LIST_CONNECTIONS,
        GET_CONNECTION,
        FORCE_CLOSE_CONNECTION,
        LIST_CONSUMERS,
        FORCE_CANCEL_CONSUMER,
        // --- WS2.08: REST message operations ---
        PUBLISH_VIA_EXCHANGE,
        QUEUE_GET,
        QUEUE_ACK,
        QUEUE_NACK,
        // --- WS2.09: Vhost admin ---
        LIST_VHOSTS,
        CREATE_VHOST,
        DELETE_VHOST
    }

    // --- Route result ---
    public static final class RouteResult {
        private final HandlerType handlerType;
        private final String topicName;
        private final Integer partition;
        private final String consumerGroup;
        private final String groupId;
        private final String resourceName; // WS2.06: exchange/queue name for REST routes
        private final Map<String, String> queryParams;

        public RouteResult(
            HandlerType handlerType,
            String topicName,
            Integer partition,
            String consumerGroup,
            Map<String, String> queryParams
        ) {
            this(handlerType, topicName, partition, consumerGroup, null, null, queryParams);
        }

        public RouteResult(
            HandlerType handlerType,
            String topicName,
            Integer partition,
            String consumerGroup,
            String groupId,
            Map<String, String> queryParams
        ) {
            this(handlerType, topicName, partition, consumerGroup, groupId, null, queryParams);
        }

        public RouteResult(
            HandlerType handlerType,
            String topicName,
            Integer partition,
            String consumerGroup,
            String groupId,
            String resourceName,
            Map<String, String> queryParams
        ) {
            this.handlerType = handlerType;
            this.topicName = topicName;
            this.partition = partition;
            this.consumerGroup = consumerGroup;
            this.groupId = groupId;
            this.resourceName = resourceName;
            this.queryParams = queryParams;
        }

        public HandlerType handlerType() {
            return handlerType;
        }

        public String topicName() {
            return topicName;
        }

        public Integer partition() {
            return partition;
        }

        public String consumerGroup() {
            return consumerGroup;
        }

        public String groupId() {
            return groupId;
        }

        /** WS2.06: exchange or queue name extracted from /v1/exchanges/{name} or /v1/queues/{name}. */
        public String resourceName() {
            return resourceName;
        }

        public Map<String, String> queryParams() {
            return queryParams;
        }
    }

    // --- URI patterns (precompiled) ---
    // Order matters: more specific patterns must be tried before less specific ones.

    // Matches: /v1/topics/{topicName}/records:fetch
    private static final Pattern FETCH_PATTERN =
        Pattern.compile("^/v1/topics/([^/?]+)/records:fetch$");

    // Matches: /v1/topics/{topicName}/records
    private static final Pattern PRODUCE_PATTERN =
        Pattern.compile("^/v1/topics/([^/?]+)/records$");

    // Matches: /v1/topics/{topicName}/partitions/{partition}/offsets
    private static final Pattern LIST_OFFSETS_PATTERN =
        Pattern.compile("^/v1/topics/([^/?]+)/partitions/(\\d+)/offsets$");

    // Matches: /v1/topics/{topicName}
    private static final Pattern METADATA_TOPIC_PATTERN =
        Pattern.compile("^/v1/topics/([^/?]+)$");

    // Matches: /v1/topics
    private static final Pattern METADATA_ALL_PATTERN =
        Pattern.compile("^/v1/topics$");

    // Matches: /v1/share-groups/{group}/records
    private static final Pattern SHARE_POLL_PATTERN =
        Pattern.compile("^/v1/share-groups/([^/?]+)/records$");

    // Matches: /v1/share-groups/{group}/acknowledge
    private static final Pattern SHARE_ACKNOWLEDGE_PATTERN =
        Pattern.compile("^/v1/share-groups/([^/?]+)/acknowledge$");

    // Matches: /v1/consumer-groups/{group}/lags
    private static final Pattern CONSUMER_LAG_PATTERN =
        Pattern.compile("^/v1/consumer-groups/([^/?]+)/lags$");

    // Matches: /v1/consumer-groups/{group}/offsets
    private static final Pattern CONSUMER_GROUP_OFFSETS_PATTERN =
        Pattern.compile("^/v1/consumer-groups/([^/?]+)/offsets$");

    // Matches: /v1/health
    private static final Pattern HEALTH_PATTERN =
        Pattern.compile("^/v1/health$");

    // Matches: /v1/openapi.yaml
    private static final Pattern OPENAPI_SPEC_PATTERN =
        Pattern.compile("^/v1/openapi\\.yaml$");

    // --- WS2.06 routing CRUD patterns ---

    // --- WS2.08 message operations (must precede the bare {name} patterns) ---

    // Matches: /v1/exchanges/{name}/publish — must precede EXCHANGE_PATTERN
    private static final Pattern EXCHANGE_PUBLISH_PATTERN =
        Pattern.compile("^/v1/exchanges/([^/?]+)/publish$");

    // Matches: /v1/queues/{name}/get — must precede QUEUE_PATTERN
    private static final Pattern QUEUE_GET_PATTERN =
        Pattern.compile("^/v1/queues/([^/?]+)/get$");

    // Matches: /v1/queues/{name}/ack — must precede QUEUE_PATTERN
    private static final Pattern QUEUE_ACK_PATTERN =
        Pattern.compile("^/v1/queues/([^/?]+)/ack$");

    // Matches: /v1/queues/{name}/nack — must precede QUEUE_PATTERN
    private static final Pattern QUEUE_NACK_PATTERN =
        Pattern.compile("^/v1/queues/([^/?]+)/nack$");

    // Matches: /v1/exchanges/{name}
    private static final Pattern EXCHANGE_PATTERN =
        Pattern.compile("^/v1/exchanges/([^/?]+)$");

    // Matches: /v1/exchanges
    private static final Pattern EXCHANGES_LIST_PATTERN =
        Pattern.compile("^/v1/exchanges$");

    // Matches: /v1/queues/{name}/messages — must precede QUEUE_PATTERN
    private static final Pattern QUEUE_MESSAGES_PATTERN =
        Pattern.compile("^/v1/queues/([^/?]+)/messages$");

    // Matches: /v1/queues/{name}
    private static final Pattern QUEUE_PATTERN =
        Pattern.compile("^/v1/queues/([^/?]+)$");

    // Matches: /v1/queues
    private static final Pattern QUEUES_LIST_PATTERN =
        Pattern.compile("^/v1/queues$");

    // Matches: /v1/bindings
    private static final Pattern BINDINGS_PATTERN =
        Pattern.compile("^/v1/bindings$");

    // --- WS2.09: vhost admin ---

    // Matches: /v1/vhosts
    private static final Pattern VHOSTS_LIST_PATTERN =
        Pattern.compile("^/v1/vhosts$");

    // Matches: /v1/vhosts/{name}. The name segment may be URL-encoded — path
    // traversal characters ('/', '\0') are rejected in validateResourceName.
    private static final Pattern VHOST_DETAIL_PATTERN =
        Pattern.compile("^/v1/vhosts/([^/?]+)$");

    // --- WS2.07 connection / consumer management patterns ---

    // Matches: /v1/connections
    private static final Pattern CONNECTIONS_LIST_PATTERN =
        Pattern.compile("^/v1/connections$");

    // Matches: /v1/connections/{connectionId}
    private static final Pattern CONNECTION_DETAIL_PATTERN =
        Pattern.compile("^/v1/connections/([^/?]+)$");

    // Matches: /v1/consumers
    private static final Pattern CONSUMERS_LIST_PATTERN =
        Pattern.compile("^/v1/consumers$");

    // Matches: /v1/consumers/{connectionId}/{subscriptionId}
    private static final Pattern CONSUMER_CANCEL_PATTERN =
        Pattern.compile("^/v1/consumers/([^/?]+)/([^/?]+)$");

    // --- Client ID validation ---
    private static final Pattern CLIENT_ID_PATTERN =
        Pattern.compile("^[a-zA-Z0-9._-]{1,128}$");

    private static final String DEFAULT_CLIENT_ID = "http-client";

    /**
     * Routes an HTTP request to a handler type.
     *
     * @param method HTTP method (GET, POST)
     * @param uri    raw URI with possible query string, e.g. "/v1/topics/orders/records"
     * @return RouteResult with handler type and extracted parameters
     * @throws InvalidRequestException if the URI does not match any known route
     * @throws InvalidTopicException   if the extracted topic name is invalid
     */
    public RouteResult route(HttpMethod method, String uri) {
        Objects.requireNonNull(method, "method");
        Objects.requireNonNull(uri, "uri");

        // Split URI into path and query string
        String path;
        String queryString;
        int queryStart = uri.indexOf('?');
        if (queryStart >= 0) {
            path = uri.substring(0, queryStart);
            queryString = uri.substring(queryStart + 1);
        } else {
            path = uri;
            queryString = null;
        }

        Map<String, String> queryParams = parseQueryParams(queryString);

        // Try topic routes first, then share-group, consumer-group, then utility routes
        RouteResult result = matchTopicRoutes(method, path, queryParams);
        if (result != null) return result;

        result = matchShareGroupRoutes(method, path, queryParams);
        if (result != null) return result;

        result = matchConsumerGroupRoutes(method, path, queryParams);
        if (result != null) return result;

        result = matchRoutingRoutes(method, path, queryParams);
        if (result != null) return result;

        result = matchVhostRoutes(method, path, queryParams);
        if (result != null) return result;

        result = matchAdminRoutes(method, path, queryParams);
        if (result != null) return result;

        result = matchUtilityRoutes(method, path, queryParams);
        if (result != null) return result;

        throw new InvalidRequestException("No route found for " + method + " " + path);
    }

    /**
     * Matches topic-related routes: FETCH, PRODUCE, LIST_OFFSETS, METADATA_TOPIC, METADATA_ALL.
     */
    private RouteResult matchTopicRoutes(HttpMethod method, String path, Map<String, String> queryParams) {
        Matcher matcher;

        // FETCH must be checked before PRODUCE because /records:fetch is more specific than /records
        matcher = FETCH_PATTERN.matcher(path);
        if (matcher.matches()) {
            requireMethod(method, HttpMethod.POST, path);
            String topicName = validateTopicName(matcher.group(1));
            return new RouteResult(HandlerType.FETCH, topicName, null, null, queryParams);
        }

        matcher = PRODUCE_PATTERN.matcher(path);
        if (matcher.matches()) {
            requireMethod(method, HttpMethod.POST, path);
            String topicName = validateTopicName(matcher.group(1));
            return new RouteResult(HandlerType.PRODUCE, topicName, null, null, queryParams);
        }

        matcher = LIST_OFFSETS_PATTERN.matcher(path);
        if (matcher.matches()) {
            requireMethod(method, HttpMethod.GET, path);
            String topicName = validateTopicName(matcher.group(1));
            int partition = Integer.parseInt(matcher.group(2));
            return new RouteResult(HandlerType.LIST_OFFSETS, topicName, partition, null, queryParams);
        }

        matcher = METADATA_TOPIC_PATTERN.matcher(path);
        if (matcher.matches()) {
            requireMethod(method, HttpMethod.GET, path);
            String topicName = validateTopicName(matcher.group(1));
            return new RouteResult(HandlerType.METADATA_TOPIC, topicName, null, null, queryParams);
        }

        matcher = METADATA_ALL_PATTERN.matcher(path);
        if (matcher.matches()) {
            requireMethod(method, HttpMethod.GET, path);
            return new RouteResult(HandlerType.METADATA_ALL, null, null, null, queryParams);
        }

        return null;
    }

    /**
     * Matches share-group routes: SHARE_POLL, SHARE_ACKNOWLEDGE.
     */
    private RouteResult matchShareGroupRoutes(HttpMethod method, String path, Map<String, String> queryParams) {
        Matcher matcher;

        matcher = SHARE_POLL_PATTERN.matcher(path);
        if (matcher.matches()) {
            requireMethod(method, HttpMethod.POST, path);
            String groupId = validateGroupId(matcher.group(1));
            return new RouteResult(HandlerType.SHARE_POLL, null, null, null, groupId, queryParams);
        }

        matcher = SHARE_ACKNOWLEDGE_PATTERN.matcher(path);
        if (matcher.matches()) {
            requireMethod(method, HttpMethod.POST, path);
            String groupId = validateGroupId(matcher.group(1));
            return new RouteResult(HandlerType.SHARE_ACKNOWLEDGE, null, null, null, groupId, queryParams);
        }

        return null;
    }

    /**
     * Matches consumer-group routes: CONSUMER_LAG, COMMIT_OFFSETS, FETCH_OFFSETS.
     */
    private RouteResult matchConsumerGroupRoutes(HttpMethod method, String path, Map<String, String> queryParams) {
        Matcher matcher;

        matcher = CONSUMER_LAG_PATTERN.matcher(path);
        if (matcher.matches()) {
            requireMethod(method, HttpMethod.GET, path);
            String consumerGroup = validateGroupId(matcher.group(1));
            return new RouteResult(HandlerType.CONSUMER_LAG, null, null, consumerGroup, queryParams);
        }

        matcher = CONSUMER_GROUP_OFFSETS_PATTERN.matcher(path);
        if (matcher.matches()) {
            String consumerGroup = validateGroupId(matcher.group(1));
            if (method.equals(HttpMethod.POST)) {
                return new RouteResult(HandlerType.COMMIT_OFFSETS, null, null, consumerGroup, queryParams);
            } else if (method.equals(HttpMethod.GET)) {
                return new RouteResult(HandlerType.FETCH_OFFSETS, null, null, consumerGroup, queryParams);
            } else {
                throw new InvalidRequestException(
                    "Method " + method + " not allowed for " + path + "; expected POST or GET");
            }
        }

        return null;
    }

    /**
     * WS2.06 — Matches REST routing CRUD routes for exchanges, queues, and bindings.
     *
     * Order matters: the more specific {@code /v1/queues/{name}/messages} must come
     * before the general {@code /v1/queues/{name}} to avoid being swallowed.
     */
    private RouteResult matchRoutingRoutes(HttpMethod method, String path, Map<String, String> queryParams) {
        RouteResult result = matchExchangeRoutes(method, path, queryParams);
        if (result != null) return result;

        result = matchQueueRoutes(method, path, queryParams);
        if (result != null) return result;

        return matchBindingRoutes(method, path, queryParams);
    }

    /** /v1/exchanges, /v1/exchanges/{name}, and /v1/exchanges/{name}/publish. */
    private RouteResult matchExchangeRoutes(HttpMethod method, String path, Map<String, String> queryParams) {
        // WS2.08: /v1/exchanges/{name}/publish must precede the bare {name} pattern.
        Matcher matcher = EXCHANGE_PUBLISH_PATTERN.matcher(path);
        if (matcher.matches()) {
            requireMethod(method, HttpMethod.POST, path);
            String name = validateResourceName(matcher.group(1), "exchange");
            return new RouteResult(HandlerType.PUBLISH_VIA_EXCHANGE,
                null, null, null, null, name, queryParams);
        }
        matcher = EXCHANGE_PATTERN.matcher(path);
        if (matcher.matches()) {
            String name = validateResourceName(matcher.group(1), "exchange");
            HandlerType ht = exchangeMethodToHandler(method, path);
            return new RouteResult(ht, null, null, null, null, name, queryParams);
        }
        matcher = EXCHANGES_LIST_PATTERN.matcher(path);
        if (matcher.matches()) {
            requireMethod(method, HttpMethod.GET, path);
            return new RouteResult(HandlerType.LIST_EXCHANGES, null, null, null, null, null, queryParams);
        }
        return null;
    }

    /** /v1/queues, /v1/queues/{name}, /v1/queues/{name}/messages, plus WS2.08 get/ack/nack. */
    private RouteResult matchQueueRoutes(HttpMethod method, String path, Map<String, String> queryParams) {
        // More specific paths (/messages, /get, /ack, /nack) must come first.
        Matcher matcher = QUEUE_MESSAGES_PATTERN.matcher(path);
        if (matcher.matches()) {
            requireMethod(method, HttpMethod.DELETE, path);
            String name = validateResourceName(matcher.group(1), "queue");
            return new RouteResult(HandlerType.PURGE_QUEUE, null, null, null, null, name, queryParams);
        }
        // WS2.08: /v1/queues/{name}/get
        matcher = QUEUE_GET_PATTERN.matcher(path);
        if (matcher.matches()) {
            requireMethod(method, HttpMethod.POST, path);
            String name = validateResourceName(matcher.group(1), "queue");
            return new RouteResult(HandlerType.QUEUE_GET, null, null, null, null, name, queryParams);
        }
        // WS2.08: /v1/queues/{name}/ack
        matcher = QUEUE_ACK_PATTERN.matcher(path);
        if (matcher.matches()) {
            requireMethod(method, HttpMethod.POST, path);
            String name = validateResourceName(matcher.group(1), "queue");
            return new RouteResult(HandlerType.QUEUE_ACK, null, null, null, null, name, queryParams);
        }
        // WS2.08: /v1/queues/{name}/nack
        matcher = QUEUE_NACK_PATTERN.matcher(path);
        if (matcher.matches()) {
            requireMethod(method, HttpMethod.POST, path);
            String name = validateResourceName(matcher.group(1), "queue");
            return new RouteResult(HandlerType.QUEUE_NACK, null, null, null, null, name, queryParams);
        }
        matcher = QUEUE_PATTERN.matcher(path);
        if (matcher.matches()) {
            String name = validateResourceName(matcher.group(1), "queue");
            HandlerType ht = queueMethodToHandler(method, path);
            return new RouteResult(ht, null, null, null, null, name, queryParams);
        }
        matcher = QUEUES_LIST_PATTERN.matcher(path);
        if (matcher.matches()) {
            requireMethod(method, HttpMethod.GET, path);
            return new RouteResult(HandlerType.LIST_QUEUES, null, null, null, null, null, queryParams);
        }
        return null;
    }

    /** /v1/bindings. */
    private RouteResult matchBindingRoutes(HttpMethod method, String path, Map<String, String> queryParams) {
        Matcher matcher = BINDINGS_PATTERN.matcher(path);
        if (!matcher.matches()) return null;
        HandlerType ht;
        if (method.equals(HttpMethod.POST)) {
            ht = HandlerType.CREATE_BINDING;
        } else if (method.equals(HttpMethod.GET)) {
            ht = HandlerType.LIST_BINDINGS;
        } else if (method.equals(HttpMethod.DELETE)) {
            ht = HandlerType.DELETE_BINDING;
        } else {
            throw new InvalidRequestException(
                "Method " + method + " not allowed for " + path + "; expected POST, GET or DELETE");
        }
        return new RouteResult(ht, null, null, null, null, null, queryParams);
    }

    /**
     * WS2.09 — Matches vhost admin routes.
     *
     * <p>Routes:
     * <ul>
     *   <li>{@code GET /v1/vhosts} → {@link HandlerType#LIST_VHOSTS}</li>
     *   <li>{@code PUT /v1/vhosts/{name}} → {@link HandlerType#CREATE_VHOST}</li>
     *   <li>{@code DELETE /v1/vhosts/{name}} → {@link HandlerType#DELETE_VHOST}</li>
     * </ul>
     *
     * <p>The vhost name is extracted into {@link RouteResult#resourceName()}.
     * Callers must URL-encode leading slashes (e.g. {@code %2Fproduction}) or
     * pass the bare name (e.g. {@code production}); both forms are validated
     * via {@link #validateResourceName(String, String)}.
     */
    private RouteResult matchVhostRoutes(HttpMethod method, String path, Map<String, String> queryParams) {
        Matcher matcher = VHOST_DETAIL_PATTERN.matcher(path);
        if (matcher.matches()) {
            String name = validateResourceName(matcher.group(1), "vhost");
            HandlerType ht;
            if (method.equals(HttpMethod.PUT)) {
                ht = HandlerType.CREATE_VHOST;
            } else if (method.equals(HttpMethod.DELETE)) {
                ht = HandlerType.DELETE_VHOST;
            } else {
                throw new InvalidRequestException(
                    "Method " + method + " not allowed for " + path + "; expected PUT or DELETE");
            }
            return new RouteResult(ht, null, null, null, null, name, queryParams);
        }
        matcher = VHOSTS_LIST_PATTERN.matcher(path);
        if (matcher.matches()) {
            requireMethod(method, HttpMethod.GET, path);
            return new RouteResult(HandlerType.LIST_VHOSTS, null, null, null, null, null, queryParams);
        }
        return null;
    }

    /**
     * WS2.07 — Matches connection and consumer management routes.
     *
     * <p>Consumer cancel uses a two-segment identifier path — connectionId and
     * subscriptionId — so we reuse {@link RouteResult#consumerGroup()} for the
     * connection id and {@link RouteResult#resourceName()} for the
     * subscriptionId to avoid adding yet another dedicated field.
     */
    private RouteResult matchAdminRoutes(HttpMethod method, String path, Map<String, String> queryParams) {
        Matcher matcher;

        // /v1/consumers/{connectionId}/{subscriptionId} — must come before the list pattern.
        matcher = CONSUMER_CANCEL_PATTERN.matcher(path);
        if (matcher.matches()) {
            requireMethod(method, HttpMethod.DELETE, path);
            String connectionId = validateResourceName(matcher.group(1), "connection");
            String subscriptionId = validateResourceName(matcher.group(2), "subscription");
            return new RouteResult(HandlerType.FORCE_CANCEL_CONSUMER,
                null, null, connectionId, null, subscriptionId, queryParams);
        }

        matcher = CONSUMERS_LIST_PATTERN.matcher(path);
        if (matcher.matches()) {
            requireMethod(method, HttpMethod.GET, path);
            return new RouteResult(HandlerType.LIST_CONSUMERS, null, null, null, null, null, queryParams);
        }

        matcher = CONNECTION_DETAIL_PATTERN.matcher(path);
        if (matcher.matches()) {
            String connectionId = validateResourceName(matcher.group(1), "connection");
            HandlerType ht;
            if (method.equals(HttpMethod.GET)) {
                ht = HandlerType.GET_CONNECTION;
            } else if (method.equals(HttpMethod.DELETE)) {
                ht = HandlerType.FORCE_CLOSE_CONNECTION;
            } else {
                throw new InvalidRequestException(
                    "Method " + method + " not allowed for " + path + "; expected GET or DELETE");
            }
            return new RouteResult(ht, null, null, null, null, connectionId, queryParams);
        }

        matcher = CONNECTIONS_LIST_PATTERN.matcher(path);
        if (matcher.matches()) {
            requireMethod(method, HttpMethod.GET, path);
            return new RouteResult(HandlerType.LIST_CONNECTIONS, null, null, null, null, null, queryParams);
        }

        return null;
    }

    private static HandlerType exchangeMethodToHandler(HttpMethod method, String path) {
        if (method.equals(HttpMethod.PUT)) return HandlerType.DECLARE_EXCHANGE;
        if (method.equals(HttpMethod.GET)) return HandlerType.GET_EXCHANGE;
        if (method.equals(HttpMethod.DELETE)) return HandlerType.DELETE_EXCHANGE;
        throw new InvalidRequestException(
            "Method " + method + " not allowed for " + path + "; expected PUT, GET or DELETE");
    }

    private static HandlerType queueMethodToHandler(HttpMethod method, String path) {
        if (method.equals(HttpMethod.PUT)) return HandlerType.DECLARE_QUEUE;
        if (method.equals(HttpMethod.GET)) return HandlerType.GET_QUEUE;
        if (method.equals(HttpMethod.PATCH)) return HandlerType.PATCH_QUEUE;
        if (method.equals(HttpMethod.DELETE)) return HandlerType.DELETE_QUEUE;
        throw new InvalidRequestException(
            "Method " + method + " not allowed for " + path + "; expected PUT, GET, PATCH or DELETE");
    }

    /**
     * Matches utility routes: HEALTH, OPENAPI_SPEC.
     */
    private RouteResult matchUtilityRoutes(HttpMethod method, String path, Map<String, String> queryParams) {
        Matcher matcher = HEALTH_PATTERN.matcher(path);
        if (matcher.matches()) {
            requireMethod(method, HttpMethod.GET, path);
            return new RouteResult(HandlerType.HEALTH, null, null, null, queryParams);
        }

        matcher = OPENAPI_SPEC_PATTERN.matcher(path);
        if (matcher.matches()) {
            requireMethod(method, HttpMethod.GET, path);
            return new RouteResult(HandlerType.OPENAPI_SPEC, null, null, null, queryParams);
        }

        return null;
    }

    /**
     * URL-decodes a path segment, rejects path traversal characters, then validates
     * via {@link Topic#validate(String)}.
     *
     * Validation order:
     * 1. URL-decode (catches %2F = '/', %00 = null byte)
     * 2. Reject path traversal: '/', '\', '\0'
     * 3. Kafka Topic.validate() -- checks [a-zA-Z0-9._-], max 249 chars, rejects "." and ".."
     *
     * @param rawSegment URL-encoded topic name from the URI path
     * @return validated, decoded topic name
     * @throws InvalidRequestException if topic contains illegal characters (/, \, \0)
     * @throws InvalidTopicException   if Kafka's Topic.validate() rejects it
     */
    static String validateTopicName(String rawSegment) {
        // 1. URL-decode
        String decoded = URLDecoder.decode(rawSegment, StandardCharsets.UTF_8);

        // 2. Check for path traversal characters
        if (decoded.indexOf('\0') >= 0) {
            throw new InvalidRequestException(
                "Topic name contains illegal null byte character");
        }
        if (decoded.indexOf('/') >= 0) {
            throw new InvalidRequestException(
                "Topic name contains illegal '/' character");
        }
        if (decoded.indexOf('\\') >= 0) {
            throw new InvalidRequestException(
                "Topic name contains illegal '\\' character");
        }

        // 3. Kafka validation (checks valid chars, length, ".", "..")
        Topic.validate(decoded);

        return decoded;
    }

    /**
     * URL-decodes and validates a consumer group ID from the URI path.
     * Group IDs must be non-empty and at most 255 characters after decoding.
     *
     * @param rawSegment URL-encoded group ID from the URI path
     * @return validated, decoded group ID
     * @throws InvalidRequestException if the group ID is empty or too long
     */
    static String validateGroupId(String rawSegment) {
        String decoded = URLDecoder.decode(rawSegment, StandardCharsets.UTF_8);
        if (decoded.isEmpty()) {
            throw new InvalidRequestException("Consumer group ID must not be empty");
        }
        if (decoded.length() > 255) {
            throw new InvalidRequestException(
                "Consumer group ID must not exceed 255 characters, got " + decoded.length());
        }
        return decoded;
    }

    /**
     * URL-decodes and validates an exchange or queue name from the URI path.
     * Rejects path-traversal characters (null byte, '/', '\'). Allows all other
     * printable characters so AMQP names like {@code amq.direct} and empty-string
     * default exchange survive (callers use an empty path segment for the latter,
     * which the regex rejects — use a non-empty placeholder and translate at the
     * handler layer).
     *
     * // Time: Created - TASK-WS2.06
     *
     * @param rawSegment URL-encoded resource name from the URI path
     * @param kind       "exchange" or "queue" — used in the error message only
     * @return validated, decoded resource name
     * @throws InvalidRequestException if the name is empty, too long, or contains
     *                                 illegal characters
     */
    static String validateResourceName(String rawSegment, String kind) {
        String decoded = URLDecoder.decode(rawSegment, StandardCharsets.UTF_8);
        if (decoded.isEmpty()) {
            throw new InvalidRequestException(kind + " name must not be empty");
        }
        if (decoded.length() > 255) {
            throw new InvalidRequestException(
                kind + " name must not exceed 255 characters, got " + decoded.length());
        }
        if (decoded.indexOf('\0') >= 0) {
            throw new InvalidRequestException(kind + " name contains illegal null byte character");
        }
        if (decoded.indexOf('/') >= 0) {
            throw new InvalidRequestException(kind + " name contains illegal '/' character");
        }
        if (decoded.indexOf('\\') >= 0) {
            throw new InvalidRequestException(kind + " name contains illegal '\\' character");
        }
        return decoded;
    }

    /**
     * Validates the X-Kafka-Client-ID header value.
     * Must match [a-zA-Z0-9._-]{1,128}. Returns "http-client" if absent or empty.
     *
     * @param clientId raw header value (may be null)
     * @return validated client ID string
     * @throws InvalidRequestException if clientId contains illegal characters
     */
    public static String validateClientId(String clientId) {
        if (clientId == null || clientId.isEmpty()) {
            return DEFAULT_CLIENT_ID;
        }
        if (!CLIENT_ID_PATTERN.matcher(clientId).matches()) {
            throw new InvalidRequestException(
                "Client ID must match [a-zA-Z0-9._-]{1,128}, got: '" + clientId + "'");
        }
        return clientId;
    }

    /**
     * Splits a query string into a map of key-value pairs. URL-decodes both key and value.
     */
    private static Map<String, String> parseQueryParams(String queryString) {
        if (queryString == null || queryString.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, String> params = new HashMap<>();
        String[] pairs = queryString.split("&");
        for (String pair : pairs) {
            if (pair.isEmpty()) {
                continue;
            }
            int eqIdx = pair.indexOf('=');
            if (eqIdx >= 0) {
                String key = URLDecoder.decode(pair.substring(0, eqIdx), StandardCharsets.UTF_8);
                String value = URLDecoder.decode(pair.substring(eqIdx + 1), StandardCharsets.UTF_8);
                params.put(key, value);
            } else {
                String key = URLDecoder.decode(pair, StandardCharsets.UTF_8);
                params.put(key, "");
            }
        }
        return Collections.unmodifiableMap(params);
    }

    /**
     * Verifies the HTTP method matches the expected method for a route.
     */
    private static void requireMethod(HttpMethod actual, HttpMethod expected, String path) {
        if (!actual.equals(expected)) {
            throw new InvalidRequestException(
                "Method " + actual + " not allowed for " + path + "; expected " + expected);
        }
    }
}
