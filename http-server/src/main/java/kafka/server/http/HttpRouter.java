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
        HEALTH
    }

    // --- Route result record ---
    public record RouteResult(
        HandlerType handlerType,
        String topicName,
        Integer partition,
        String consumerGroup,
        Map<String, String> queryParams
    ) { }

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

    // Matches: /v1/consumer-groups/{group}/lags
    private static final Pattern CONSUMER_LAG_PATTERN =
        Pattern.compile("^/v1/consumer-groups/([^/?]+)/lags$");

    // Matches: /v1/consumer-groups/{group}/offsets
    private static final Pattern CONSUMER_GROUP_OFFSETS_PATTERN =
        Pattern.compile("^/v1/consumer-groups/([^/?]+)/offsets$");

    // Matches: /v1/health
    private static final Pattern HEALTH_PATTERN =
        Pattern.compile("^/v1/health$");

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

        // Try topic routes first, then consumer-group routes, then utility routes
        RouteResult result = matchTopicRoutes(method, path, queryParams);
        if (result != null) return result;

        result = matchConsumerGroupRoutes(method, path, queryParams);
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
     * Matches consumer-group routes: CONSUMER_LAG, COMMIT_OFFSETS, FETCH_OFFSETS.
     */
    private RouteResult matchConsumerGroupRoutes(HttpMethod method, String path, Map<String, String> queryParams) {
        Matcher matcher;

        matcher = CONSUMER_LAG_PATTERN.matcher(path);
        if (matcher.matches()) {
            requireMethod(method, HttpMethod.GET, path);
            String consumerGroup = matcher.group(1);
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
     * Matches utility routes: HEALTH.
     */
    private RouteResult matchUtilityRoutes(HttpMethod method, String path, Map<String, String> queryParams) {
        Matcher matcher = HEALTH_PATTERN.matcher(path);
        if (matcher.matches()) {
            requireMethod(method, HttpMethod.GET, path);
            return new RouteResult(HandlerType.HEALTH, null, null, null, queryParams);
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
