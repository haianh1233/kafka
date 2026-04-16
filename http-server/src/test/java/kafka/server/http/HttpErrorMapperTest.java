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
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.OptionalInt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link HttpErrorMapper} -- centralized Kafka error to HTTP status mapping.
 *
 * // Time: Created - TASK-F.02
 */
class HttpErrorMapperTest {

    // --- httpStatus tests ---

    @Test
    void testNoneMapsTo200() {
        assertEquals(HttpResponseStatus.OK, HttpErrorMapper.httpStatus(Errors.NONE));
    }

    @Test
    void testUnknownTopicMapsTo404() {
        assertEquals(HttpResponseStatus.NOT_FOUND,
            HttpErrorMapper.httpStatus(Errors.UNKNOWN_TOPIC_OR_PARTITION));
    }

    @Test
    void testTopicAuthorizationMapsTo403() {
        assertEquals(HttpResponseStatus.FORBIDDEN,
            HttpErrorMapper.httpStatus(Errors.TOPIC_AUTHORIZATION_FAILED));
    }

    @Test
    void testClusterAuthorizationMapsTo403() {
        assertEquals(HttpResponseStatus.FORBIDDEN,
            HttpErrorMapper.httpStatus(Errors.CLUSTER_AUTHORIZATION_FAILED));
    }

    @Test
    void testGroupAuthorizationMapsTo403() {
        assertEquals(HttpResponseStatus.FORBIDDEN,
            HttpErrorMapper.httpStatus(Errors.GROUP_AUTHORIZATION_FAILED));
    }

    @Test
    void testInvalidTopicMapsTo400() {
        assertEquals(HttpResponseStatus.BAD_REQUEST,
            HttpErrorMapper.httpStatus(Errors.INVALID_TOPIC_EXCEPTION));
    }

    @Test
    void testInvalidRequestMapsTo400() {
        assertEquals(HttpResponseStatus.BAD_REQUEST,
            HttpErrorMapper.httpStatus(Errors.INVALID_REQUEST));
    }

    @Test
    void testMessageTooLargeMapsTo413() {
        assertEquals(HttpResponseStatus.valueOf(413),
            HttpErrorMapper.httpStatus(Errors.MESSAGE_TOO_LARGE));
    }

    @Test
    void testRecordListTooLargeMapsTo413() {
        assertEquals(HttpResponseStatus.valueOf(413),
            HttpErrorMapper.httpStatus(Errors.RECORD_LIST_TOO_LARGE));
    }

    @Test
    void testThrottlingMapsTo429() {
        assertEquals(HttpResponseStatus.TOO_MANY_REQUESTS,
            HttpErrorMapper.httpStatus(Errors.THROTTLING_QUOTA_EXCEEDED));
    }

    @Test
    void testLeaderNotAvailableMapsTo503() {
        assertEquals(HttpResponseStatus.SERVICE_UNAVAILABLE,
            HttpErrorMapper.httpStatus(Errors.LEADER_NOT_AVAILABLE));
    }

    @Test
    void testNotLeaderOrFollowerMapsTo503() {
        assertEquals(HttpResponseStatus.SERVICE_UNAVAILABLE,
            HttpErrorMapper.httpStatus(Errors.NOT_LEADER_OR_FOLLOWER));
    }

    @Test
    void testNotEnoughReplicasMapsTo503() {
        assertEquals(HttpResponseStatus.SERVICE_UNAVAILABLE,
            HttpErrorMapper.httpStatus(Errors.NOT_ENOUGH_REPLICAS));
    }

    @Test
    void testNotEnoughReplicasAfterAppendMapsTo503() {
        assertEquals(HttpResponseStatus.SERVICE_UNAVAILABLE,
            HttpErrorMapper.httpStatus(Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND));
    }

    @Test
    void testKafkaStorageErrorMapsTo500() {
        assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR,
            HttpErrorMapper.httpStatus(Errors.KAFKA_STORAGE_ERROR));
    }

    @Test
    void testTimeoutMapsTo504() {
        assertEquals(HttpResponseStatus.GATEWAY_TIMEOUT,
            HttpErrorMapper.httpStatus(Errors.REQUEST_TIMED_OUT));
    }

    @Test
    void testUnknownErrorMapsTo500() {
        assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR,
            HttpErrorMapper.httpStatus(Errors.UNKNOWN_SERVER_ERROR));
    }

    // --- retryAfterSeconds tests ---

    @Test
    void testRetryAfterForLeaderNotAvailable() {
        OptionalInt retryAfter = HttpErrorMapper.retryAfterSeconds(Errors.LEADER_NOT_AVAILABLE);
        assertTrue(retryAfter.isPresent());
        assertEquals(1, retryAfter.getAsInt());
    }

    @Test
    void testRetryAfterForNotLeaderOrFollower() {
        OptionalInt retryAfter = HttpErrorMapper.retryAfterSeconds(Errors.NOT_LEADER_OR_FOLLOWER);
        assertTrue(retryAfter.isPresent());
        assertEquals(1, retryAfter.getAsInt());
    }

    @Test
    void testRetryAfterForNotEnoughReplicas() {
        OptionalInt retryAfter = HttpErrorMapper.retryAfterSeconds(Errors.NOT_ENOUGH_REPLICAS);
        assertTrue(retryAfter.isPresent());
        assertEquals(5, retryAfter.getAsInt());
    }

    @Test
    void testRetryAfterForNotEnoughReplicasAfterAppend() {
        OptionalInt retryAfter = HttpErrorMapper.retryAfterSeconds(Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND);
        assertTrue(retryAfter.isPresent());
        assertEquals(5, retryAfter.getAsInt());
    }

    @Test
    void testRetryAfterForTimeout() {
        OptionalInt retryAfter = HttpErrorMapper.retryAfterSeconds(Errors.REQUEST_TIMED_OUT);
        assertTrue(retryAfter.isPresent());
        assertEquals(1, retryAfter.getAsInt());
    }

    @Test
    void testNoRetryAfterFor404() {
        OptionalInt retryAfter = HttpErrorMapper.retryAfterSeconds(Errors.UNKNOWN_TOPIC_OR_PARTITION);
        assertFalse(retryAfter.isPresent());
    }

    @Test
    void testNoRetryAfterFor403() {
        assertFalse(HttpErrorMapper.retryAfterSeconds(Errors.TOPIC_AUTHORIZATION_FAILED).isPresent());
    }

    @Test
    void testNoRetryAfterForNone() {
        assertFalse(HttpErrorMapper.retryAfterSeconds(Errors.NONE).isPresent());
    }

    // --- throttleRetryAfter tests ---

    @Test
    void testThrottleRetryAfter500ms() {
        assertEquals(1, HttpErrorMapper.throttleRetryAfter(500));
    }

    @Test
    void testThrottleRetryAfter1000ms() {
        assertEquals(1, HttpErrorMapper.throttleRetryAfter(1000));
    }

    @Test
    void testThrottleRetryAfter1500ms() {
        assertEquals(2, HttpErrorMapper.throttleRetryAfter(1500));
    }

    @Test
    void testThrottleRetryAfter2001ms() {
        assertEquals(3, HttpErrorMapper.throttleRetryAfter(2001));
    }

    // --- isRetriableForForwarding tests ---

    @Test
    void testNotLeaderOrFollowerIsRetriable() {
        assertTrue(HttpErrorMapper.isRetriableForForwarding(Errors.NOT_LEADER_OR_FOLLOWER));
    }

    @Test
    void testLeaderNotAvailableIsRetriable() {
        assertTrue(HttpErrorMapper.isRetriableForForwarding(Errors.LEADER_NOT_AVAILABLE));
    }

    @Test
    void testRequestTimedOutIsRetriable() {
        assertTrue(HttpErrorMapper.isRetriableForForwarding(Errors.REQUEST_TIMED_OUT));
    }

    @Test
    void testUnknownTopicNotRetriable() {
        assertFalse(HttpErrorMapper.isRetriableForForwarding(Errors.UNKNOWN_TOPIC_OR_PARTITION));
    }

    @Test
    void testAuthorizationNotRetriable() {
        assertFalse(HttpErrorMapper.isRetriableForForwarding(Errors.TOPIC_AUTHORIZATION_FAILED));
    }

    @Test
    void testMessageTooLargeNotRetriable() {
        assertFalse(HttpErrorMapper.isRetriableForForwarding(Errors.MESSAGE_TOO_LARGE));
    }

    @Test
    void testNoneNotRetriable() {
        assertFalse(HttpErrorMapper.isRetriableForForwarding(Errors.NONE));
    }

    // --- aggregateStatus tests ---

    @Test
    void testAggregateStatusAllSuccess() {
        List<Short> codes = List.of((short) 0, (short) 0, (short) 0);
        assertEquals(HttpResponseStatus.OK, HttpErrorMapper.aggregateStatus(codes));
    }

    @Test
    void testAggregateStatusAllSameError() {
        // UNKNOWN_TOPIC_OR_PARTITION = error code 3
        List<Short> codes = List.of((short) 3, (short) 3);
        assertEquals(HttpResponseStatus.NOT_FOUND, HttpErrorMapper.aggregateStatus(codes));
    }

    @Test
    void testAggregateStatusMixedSuccessAndError() {
        // NONE (0) + UNKNOWN_TOPIC_OR_PARTITION (3) -> 207
        List<Short> codes = List.of((short) 0, (short) 3);
        assertEquals(HttpErrorMapper.MULTI_STATUS, HttpErrorMapper.aggregateStatus(codes));
    }

    @Test
    void testAggregateStatusMixedDifferentErrors() {
        // UNKNOWN_TOPIC_OR_PARTITION (3) + TOPIC_AUTHORIZATION_FAILED (29) -> 207
        List<Short> codes = List.of((short) 3, (short) 29);
        assertEquals(HttpErrorMapper.MULTI_STATUS, HttpErrorMapper.aggregateStatus(codes));
    }

    @Test
    void testAggregateStatusSingleSuccess() {
        List<Short> codes = List.of((short) 0);
        assertEquals(HttpResponseStatus.OK, HttpErrorMapper.aggregateStatus(codes));
    }

    @Test
    void testAggregateStatusSingleError() {
        // LEADER_NOT_AVAILABLE = error code 5
        List<Short> codes = List.of((short) 5);
        assertEquals(HttpResponseStatus.SERVICE_UNAVAILABLE, HttpErrorMapper.aggregateStatus(codes));
    }

    @Test
    void testAggregateStatusEmpty() {
        List<Short> codes = List.of();
        assertEquals(HttpResponseStatus.OK, HttpErrorMapper.aggregateStatus(codes));
    }

    @Test
    void testMultiStatusIs207() {
        assertEquals(207, HttpErrorMapper.MULTI_STATUS.code());
    }
}
