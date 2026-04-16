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
        Matcher matcher;

        // Match path against patterns in order (most specific first)

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

        matcher = CONSUMER_LAG_PATTERN.matcher(path);
        if (matcher.matches()) {
            requireMethod(method, HttpMethod.GET, path);
            String consumerGroup = matcher.group(1);
            return new RouteResult(HandlerType.CONSUMER_LAG, null, null, consumerGroup, queryParams);
        }

        matcher = HEALTH_PATTERN.matcher(path);
        if (matcher.matches()) {
            requireMethod(method, HttpMethod.GET, path);
            return new RouteResult(HandlerType.HEALTH, null, null, null, queryParams);
        }

        throw new InvalidRequestException("No route found for " + method + " " + path);
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
     * Validates the X-Kafka-Client-ID header value.
     * Must match [a-zA-Z0-9._-]{1,128}. Returns "http-client" if absent or empty.
     *
     * @param clientId raw header value (may be null)
     * @return validated client ID string
     * @throws InvalidRequestException if clientId contains illegal characters
     */
    static String validateClientId(String clientId) {
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
