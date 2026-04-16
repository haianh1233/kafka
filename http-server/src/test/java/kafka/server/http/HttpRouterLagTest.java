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

import io.netty.handler.codec.http.HttpMethod;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Router tests specific to the consumer group lag endpoint.
 * Verifies route matching, group ID extraction, URL-decoding, and validation.
 */
class HttpRouterLagTest {

    private HttpRouter router;

    @BeforeEach
    void setUp() {
        router = new HttpRouter();
    }

    @Test
    void testRouteConsumerGroupLag() {
        var result = router.route(HttpMethod.GET, "/v1/consumer-groups/checkout-consumer/lags");
        assertEquals(HttpRouter.HandlerType.CONSUMER_LAG, result.handlerType());
        assertEquals("checkout-consumer", result.consumerGroup());
    }

    @Test
    void testRouteConsumerGroupLagUrlEncoded() {
        // URL-encoded group name: "my%20group" -> "my group"
        var result = router.route(HttpMethod.GET, "/v1/consumer-groups/my%20group/lags");
        assertEquals(HttpRouter.HandlerType.CONSUMER_LAG, result.handlerType());
        // Router URL-decodes and validates the group ID
        assertEquals("my group", result.consumerGroup());
    }

    @Test
    void testRouteConsumerGroupLagWithDots() {
        var result = router.route(HttpMethod.GET, "/v1/consumer-groups/my.group.v2/lags");
        assertEquals(HttpRouter.HandlerType.CONSUMER_LAG, result.handlerType());
        assertEquals("my.group.v2", result.consumerGroup());
    }

    @Test
    void testRouteConsumerGroupLagWithHyphens() {
        var result = router.route(HttpMethod.GET, "/v1/consumer-groups/my-consumer-group/lags");
        assertEquals(HttpRouter.HandlerType.CONSUMER_LAG, result.handlerType());
        assertEquals("my-consumer-group", result.consumerGroup());
    }

    @Test
    void testRouteConsumerGroupLagWrongMethod() {
        assertThrows(InvalidRequestException.class, () ->
            router.route(HttpMethod.POST, "/v1/consumer-groups/my-group/lags"));
    }

    @Test
    void testRouteConsumerGroupLagNoTrailingLags() {
        // /v1/consumer-groups/my-group (missing /lags) -> should not match consumer lag
        assertThrows(InvalidRequestException.class, () ->
            router.route(HttpMethod.GET, "/v1/consumer-groups/my-group"));
    }
}
