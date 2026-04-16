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
// Time: Created - TASK-F.01
package kafka.server.http;

import io.netty.handler.codec.http.HttpMethod;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HttpRouterOffsetsTest {

    private HttpRouter router;

    @BeforeEach
    void setUp() {
        router = new HttpRouter();
    }

    @Test
    void route_commitOffsets_returnsCommitOffsetsHandler() {
        var result = router.route(HttpMethod.POST, "/v1/consumer-groups/my-group/offsets");
        assertEquals(HttpRouter.HandlerType.COMMIT_OFFSETS, result.handlerType());
        assertEquals("my-group", result.consumerGroup());
    }

    @Test
    void route_fetchOffsets_returnsFetchOffsetsHandler() {
        var result = router.route(HttpMethod.GET, "/v1/consumer-groups/my-group/offsets");
        assertEquals(HttpRouter.HandlerType.FETCH_OFFSETS, result.handlerType());
        assertEquals("my-group", result.consumerGroup());
        assertTrue(result.queryParams().isEmpty());
    }

    @Test
    void route_fetchOffsetsWithTopicFilter_extractsTopicParam() {
        var result = router.route(HttpMethod.GET, "/v1/consumer-groups/my-group/offsets?topic=orders");
        assertEquals(HttpRouter.HandlerType.FETCH_OFFSETS, result.handlerType());
        assertEquals("my-group", result.consumerGroup());
        assertEquals("orders", result.queryParams().get("topic"));
    }

    @Test
    void route_commitOffsets_wrongMethodGet_throws() {
        assertThrows(InvalidRequestException.class,
            () -> router.route(HttpMethod.DELETE, "/v1/consumer-groups/my-group/offsets"));
    }

    @Test
    void route_commitOffsets_emptyGroup_throws() {
        assertThrows(InvalidRequestException.class,
            () -> router.route(HttpMethod.POST, "/v1/consumer-groups//offsets"));
    }

    @Test
    void route_commitOffsets_urlDecodesGroup() {
        var result = router.route(HttpMethod.POST, "/v1/consumer-groups/my-group-1/offsets");
        assertEquals("my-group-1", result.consumerGroup());
    }

    @Test
    void route_fetchOffsets_groupTooLong_throws() {
        String longGroup = "a".repeat(256);
        assertThrows(InvalidRequestException.class,
            () -> router.route(HttpMethod.GET, "/v1/consumer-groups/" + longGroup + "/offsets"));
    }
}
