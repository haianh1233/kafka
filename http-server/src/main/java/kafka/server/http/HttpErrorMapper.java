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
package kafka.server.http;

import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.kafka.common.protocol.Errors;

import java.util.EnumMap;
import java.util.Map;
import java.util.OptionalInt;

/**
 * Maps Kafka {@link Errors} to HTTP status codes and Retry-After values.
 *
 * This is the single source of truth for all error-to-HTTP mappings
 * in the HTTP protocol layer. Both {@link HttpResponseSerializer} and the
 * forwarding retry logic delegate to this class.
 *
 * <h3>Error Tables (design doc section 5.5)</h3>
 * <ul>
 *   <li>Request validation errors (400, 413, 422)</li>
 *   <li>Kafka broker errors (403, 404, 413, 500, 503, 504)</li>
 *   <li>Forwarding-specific (503, 504 with Retry-After)</li>
 * </ul>
 *
 * // Time: Created - TASK-F.02
 */
public final class HttpErrorMapper {

    /** HTTP 207 Multi-Status (not in standard Netty enum). */
    public static final HttpResponseStatus MULTI_STATUS = HttpResponseStatus.valueOf(207);

    private static final Map<Errors, HttpResponseStatus> STATUS_MAP = new EnumMap<>(Errors.class);
    private static final Map<Errors, Integer> RETRY_AFTER_MAP = new EnumMap<>(Errors.class);

    static {
        // 2xx
        STATUS_MAP.put(Errors.NONE, HttpResponseStatus.OK);

        // 4xx -- client errors
        STATUS_MAP.put(Errors.UNKNOWN_TOPIC_OR_PARTITION, HttpResponseStatus.NOT_FOUND);
        STATUS_MAP.put(Errors.TOPIC_AUTHORIZATION_FAILED, HttpResponseStatus.FORBIDDEN);
        STATUS_MAP.put(Errors.CLUSTER_AUTHORIZATION_FAILED, HttpResponseStatus.FORBIDDEN);
        STATUS_MAP.put(Errors.GROUP_AUTHORIZATION_FAILED, HttpResponseStatus.FORBIDDEN);
        STATUS_MAP.put(Errors.INVALID_TOPIC_EXCEPTION, HttpResponseStatus.BAD_REQUEST);
        STATUS_MAP.put(Errors.INVALID_REQUEST, HttpResponseStatus.BAD_REQUEST);
        STATUS_MAP.put(Errors.MESSAGE_TOO_LARGE, HttpResponseStatus.valueOf(413));
        STATUS_MAP.put(Errors.RECORD_LIST_TOO_LARGE, HttpResponseStatus.valueOf(413));
        STATUS_MAP.put(Errors.THROTTLING_QUOTA_EXCEEDED, HttpResponseStatus.TOO_MANY_REQUESTS);

        // 5xx -- server errors
        STATUS_MAP.put(Errors.LEADER_NOT_AVAILABLE, HttpResponseStatus.SERVICE_UNAVAILABLE);
        STATUS_MAP.put(Errors.NOT_LEADER_OR_FOLLOWER, HttpResponseStatus.SERVICE_UNAVAILABLE);
        STATUS_MAP.put(Errors.NOT_ENOUGH_REPLICAS, HttpResponseStatus.SERVICE_UNAVAILABLE);
        STATUS_MAP.put(Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND, HttpResponseStatus.SERVICE_UNAVAILABLE);
        STATUS_MAP.put(Errors.KAFKA_STORAGE_ERROR, HttpResponseStatus.INTERNAL_SERVER_ERROR);
        STATUS_MAP.put(Errors.REQUEST_TIMED_OUT, HttpResponseStatus.GATEWAY_TIMEOUT);

        // Retry-After values (seconds) per design doc section 5.5
        RETRY_AFTER_MAP.put(Errors.LEADER_NOT_AVAILABLE, 1);
        RETRY_AFTER_MAP.put(Errors.NOT_LEADER_OR_FOLLOWER, 1);
        RETRY_AFTER_MAP.put(Errors.NOT_ENOUGH_REPLICAS, 5);
        RETRY_AFTER_MAP.put(Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND, 5);
        RETRY_AFTER_MAP.put(Errors.REQUEST_TIMED_OUT, 1);
    }

    /**
     * Map a Kafka error to an HTTP status code.
     * Unknown errors default to 500 Internal Server Error.
     */
    public static HttpResponseStatus httpStatus(Errors error) {
        return STATUS_MAP.getOrDefault(error, HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }

    /**
     * Get the Retry-After header value for a given error, if applicable.
     * Returns empty if no Retry-After should be set.
     */
    public static OptionalInt retryAfterSeconds(Errors error) {
        Integer value = RETRY_AFTER_MAP.get(error);
        return value != null ? OptionalInt.of(value) : OptionalInt.empty();
    }

    /**
     * Compute Retry-After for throttle quota exceeded.
     *
     * @param throttleTimeMs the throttle time from the Kafka response
     * @return seconds to include in Retry-After header (at least 1)
     */
    public static int throttleRetryAfter(long throttleTimeMs) {
        return (int) Math.ceil(throttleTimeMs / 1000.0);
    }

    /**
     * Determine if a Kafka error is retriable for forwarding purposes.
     * Used by the forwarding retry logic (section 5.5C).
     *
     * <p>Retriable errors: NOT_LEADER_OR_FOLLOWER, LEADER_NOT_AVAILABLE,
     * REQUEST_TIMED_OUT.</p>
     */
    public static boolean isRetriableForForwarding(Errors error) {
        return error == Errors.NOT_LEADER_OR_FOLLOWER
            || error == Errors.LEADER_NOT_AVAILABLE
            || error == Errors.REQUEST_TIMED_OUT;
    }

    /**
     * Compute the aggregate HTTP status for a multi-partition response.
     *
     * <ul>
     *   <li>All NONE -> 200 OK</li>
     *   <li>All same error -> that error's HTTP status</li>
     *   <li>Mixed success/failure or different errors -> 207 Multi-Status</li>
     * </ul>
     *
     * @param errorCodes collection of per-partition error codes (short values)
     * @return the appropriate aggregate HTTP status
     */
    public static HttpResponseStatus aggregateStatus(Iterable<Short> errorCodes) {
        boolean hasSuccess = false;
        boolean hasError = false;
        Errors singleError = null;
        boolean uniformError = true;

        for (short code : errorCodes) {
            Errors error = Errors.forCode(code);
            if (error == Errors.NONE) {
                hasSuccess = true;
            } else {
                hasError = true;
                if (singleError == null) {
                    singleError = error;
                } else if (singleError != error) {
                    uniformError = false;
                }
            }
        }

        if (!hasError) return HttpResponseStatus.OK;
        if (!hasSuccess && uniformError) return httpStatus(singleError);
        return MULTI_STATUS;
    }

    private HttpErrorMapper() {} // utility class
}
