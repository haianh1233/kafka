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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class HttpRouterTest {

    private HttpRouter router;

    @BeforeEach
    void setUp() {
        router = new HttpRouter();
    }

    // --- Route matching ---

    @Test
    void route_producePost_returnsProduceHandler() {
        var result = router.route(HttpMethod.POST, "/v1/topics/my-topic/records");
        assertEquals(HttpRouter.HandlerType.PRODUCE, result.handlerType());
        assertEquals("my-topic", result.topicName());
    }

    @Test
    void route_fetchPost_returnsFetchHandler() {
        var result = router.route(HttpMethod.POST, "/v1/topics/my-topic/records:fetch");
        assertEquals(HttpRouter.HandlerType.FETCH, result.handlerType());
        assertEquals("my-topic", result.topicName());
    }

    @Test
    void route_metadataTopic_returnsMetadataTopicHandler() {
        var result = router.route(HttpMethod.GET, "/v1/topics/my-topic");
        assertEquals(HttpRouter.HandlerType.METADATA_TOPIC, result.handlerType());
        assertEquals("my-topic", result.topicName());
    }

    @Test
    void route_metadataAll_returnsMetadataAllHandler() {
        var result = router.route(HttpMethod.GET, "/v1/topics");
        assertEquals(HttpRouter.HandlerType.METADATA_ALL, result.handlerType());
        assertNull(result.topicName());
    }

    @Test
    void route_listOffsets_extractsPartition() {
        var result = router.route(HttpMethod.GET, "/v1/topics/orders/partitions/0/offsets?timestamp=latest");
        assertEquals(HttpRouter.HandlerType.LIST_OFFSETS, result.handlerType());
        assertEquals("orders", result.topicName());
        assertEquals(0, result.partition());
        assertEquals("latest", result.queryParams().get("timestamp"));
    }

    @Test
    void route_consumerLag_extractsGroup() {
        var result = router.route(HttpMethod.GET, "/v1/consumer-groups/my-group/lags");
        assertEquals(HttpRouter.HandlerType.CONSUMER_LAG, result.handlerType());
        assertEquals("my-group", result.consumerGroup());
    }

    @Test
    void route_health_returnsHealthHandler() {
        var result = router.route(HttpMethod.GET, "/v1/health");
        assertEquals(HttpRouter.HandlerType.HEALTH, result.handlerType());
    }

    @Test
    void route_unknownPath_throws() {
        assertThrows(InvalidRequestException.class,
            () -> router.route(HttpMethod.GET, "/v1/unknown"));
    }

    @Test
    void route_wrongMethod_throws() {
        assertThrows(InvalidRequestException.class,
            () -> router.route(HttpMethod.GET, "/v1/topics/my-topic/records"));
    }

    // --- Topic name validation ---

    @Test
    void validateTopicName_validName_succeeds() {
        assertEquals("my-topic", HttpRouter.validateTopicName("my-topic"));
    }

    @Test
    void validateTopicName_urlEncodedSlash_rejects() {
        assertThrows(InvalidRequestException.class,
            () -> HttpRouter.validateTopicName("my%2Ftopic"));
    }

    @Test
    void validateTopicName_nullByte_rejects() {
        assertThrows(InvalidRequestException.class,
            () -> HttpRouter.validateTopicName("my%00topic"));
    }

    @Test
    void validateTopicName_backslash_rejects() {
        assertThrows(InvalidRequestException.class,
            () -> HttpRouter.validateTopicName("my\\topic"));
    }

    @Test
    void validateTopicName_dot_rejectsViaKafka() {
        assertThrows(InvalidTopicException.class,
            () -> HttpRouter.validateTopicName("."));
    }

    @Test
    void validateTopicName_tooLong_rejectsViaKafka() {
        String longName = "a".repeat(250);
        assertThrows(InvalidTopicException.class,
            () -> HttpRouter.validateTopicName(longName));
    }

    // --- Client ID validation ---

    @Test
    void validateClientId_null_returnsDefault() {
        assertEquals("http-client", HttpRouter.validateClientId(null));
    }

    @Test
    void validateClientId_empty_returnsDefault() {
        assertEquals("http-client", HttpRouter.validateClientId(""));
    }

    @Test
    void validateClientId_validId_returnsAsIs() {
        assertEquals("my-client_1.0", HttpRouter.validateClientId("my-client_1.0"));
    }

    @Test
    void validateClientId_illegalChars_throws() {
        assertThrows(InvalidRequestException.class,
            () -> HttpRouter.validateClientId("my client!"));
    }

    @Test
    void validateClientId_tooLong_throws() {
        assertThrows(InvalidRequestException.class,
            () -> HttpRouter.validateClientId("a".repeat(129)));
    }
}
