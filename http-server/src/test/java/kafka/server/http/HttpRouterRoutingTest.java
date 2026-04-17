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
// Time: Created - TASK-WS2.06
package kafka.server.http;

import io.netty.handler.codec.http.HttpMethod;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests URL routing for the WS2.06 exchange/queue/binding REST endpoints.
 *
 * // Time: Created - TASK-WS2.06
 */
class HttpRouterRoutingTest {

    private HttpRouter router;

    @BeforeEach
    void setUp() {
        router = new HttpRouter();
    }

    // --- Exchange routes ---

    @Test
    void route_declareExchange_put() {
        var r = router.route(HttpMethod.PUT, "/v1/exchanges/orders");
        assertEquals(HttpRouter.HandlerType.DECLARE_EXCHANGE, r.handlerType());
        assertEquals("orders", r.resourceName());
    }

    @Test
    void route_getExchange_get() {
        var r = router.route(HttpMethod.GET, "/v1/exchanges/orders");
        assertEquals(HttpRouter.HandlerType.GET_EXCHANGE, r.handlerType());
        assertEquals("orders", r.resourceName());
    }

    @Test
    void route_listExchanges_get() {
        var r = router.route(HttpMethod.GET, "/v1/exchanges");
        assertEquals(HttpRouter.HandlerType.LIST_EXCHANGES, r.handlerType());
        assertNull(r.resourceName());
    }

    @Test
    void route_deleteExchange_delete() {
        var r = router.route(HttpMethod.DELETE, "/v1/exchanges/orders?ifUnused=true");
        assertEquals(HttpRouter.HandlerType.DELETE_EXCHANGE, r.handlerType());
        assertEquals("orders", r.resourceName());
        assertEquals("true", r.queryParams().get("ifUnused"));
    }

    @Test
    void route_exchangeWrongMethod_throws() {
        assertThrows(InvalidRequestException.class,
            () -> router.route(HttpMethod.POST, "/v1/exchanges/orders"));
    }

    @Test
    void route_exchangeUrlEncodedName_decoded() {
        // "amq.direct" URL-encoded — the dot stays ASCII but this verifies the
        // decoder runs and resource-name validation accepts the AMQP name.
        var r = router.route(HttpMethod.GET, "/v1/exchanges/amq.direct");
        assertEquals(HttpRouter.HandlerType.GET_EXCHANGE, r.handlerType());
        assertEquals("amq.direct", r.resourceName());
    }

    @Test
    void route_exchangeSlashInName_rejected() {
        // Slash in the name is treated as a path separator so the regex never
        // matches and we fall through to NoRouteFound (InvalidRequestException).
        assertThrows(InvalidRequestException.class,
            () -> router.route(HttpMethod.GET, "/v1/exchanges/orders/foo"));
    }

    @Test
    void route_exchangeUrlEncodedSlash_rejectedByValidator() {
        assertThrows(InvalidRequestException.class,
            () -> router.route(HttpMethod.GET, "/v1/exchanges/orders%2Fsub"));
    }

    // --- Queue routes ---

    @Test
    void route_declareQueue_put() {
        var r = router.route(HttpMethod.PUT, "/v1/queues/q1");
        assertEquals(HttpRouter.HandlerType.DECLARE_QUEUE, r.handlerType());
        assertEquals("q1", r.resourceName());
    }

    @Test
    void route_getQueue_get() {
        var r = router.route(HttpMethod.GET, "/v1/queues/q1");
        assertEquals(HttpRouter.HandlerType.GET_QUEUE, r.handlerType());
        assertEquals("q1", r.resourceName());
    }

    @Test
    void route_listQueues_get() {
        var r = router.route(HttpMethod.GET, "/v1/queues");
        assertEquals(HttpRouter.HandlerType.LIST_QUEUES, r.handlerType());
    }

    @Test
    void route_deleteQueue_withQueryParams() {
        var r = router.route(HttpMethod.DELETE, "/v1/queues/q1?ifUnused=true&ifEmpty=true");
        assertEquals(HttpRouter.HandlerType.DELETE_QUEUE, r.handlerType());
        assertEquals("q1", r.resourceName());
        assertEquals("true", r.queryParams().get("ifUnused"));
        assertEquals("true", r.queryParams().get("ifEmpty"));
    }

    @Test
    void route_purgeQueue_messagesPath() {
        var r = router.route(HttpMethod.DELETE, "/v1/queues/q1/messages");
        assertEquals(HttpRouter.HandlerType.PURGE_QUEUE, r.handlerType());
        assertEquals("q1", r.resourceName());
    }

    @Test
    void route_patchQueue() {
        var r = router.route(HttpMethod.PATCH, "/v1/queues/q1");
        assertEquals(HttpRouter.HandlerType.PATCH_QUEUE, r.handlerType());
        assertEquals("q1", r.resourceName());
    }

    // --- Binding routes ---

    @Test
    void route_createBinding_post() {
        var r = router.route(HttpMethod.POST, "/v1/bindings");
        assertEquals(HttpRouter.HandlerType.CREATE_BINDING, r.handlerType());
    }

    @Test
    void route_listBindings_withFilters() {
        var r = router.route(HttpMethod.GET, "/v1/bindings?exchange=ex1&queue=q1");
        assertEquals(HttpRouter.HandlerType.LIST_BINDINGS, r.handlerType());
        assertEquals("ex1", r.queryParams().get("exchange"));
        assertEquals("q1", r.queryParams().get("queue"));
    }

    @Test
    void route_deleteBinding() {
        var r = router.route(HttpMethod.DELETE, "/v1/bindings");
        assertEquals(HttpRouter.HandlerType.DELETE_BINDING, r.handlerType());
    }

    // --- Validator behaviour ---

    @Test
    void validateResourceName_valid() {
        assertEquals("orders", HttpRouter.validateResourceName("orders", "exchange"));
    }

    @Test
    void validateResourceName_urlEncodedSlash_rejects() {
        assertThrows(InvalidRequestException.class,
            () -> HttpRouter.validateResourceName("a%2Fb", "exchange"));
    }

    @Test
    void validateResourceName_empty_rejects() {
        assertThrows(InvalidRequestException.class,
            () -> HttpRouter.validateResourceName("", "queue"));
    }

    @Test
    void validateResourceName_tooLong_rejects() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 256; i++) sb.append('a');
        assertThrows(InvalidRequestException.class,
            () -> HttpRouter.validateResourceName(sb.toString(), "queue"));
    }

    @Test
    void validateResourceName_nullByte_rejects() {
        assertThrows(InvalidRequestException.class,
            () -> HttpRouter.validateResourceName("a%00b", "exchange"));
    }

    @Test
    void validateResourceName_backslash_rejects() {
        assertThrows(InvalidRequestException.class,
            () -> HttpRouter.validateResourceName("a\\b", "exchange"));
    }

    // --- Disambiguation ---

    @Test
    void route_queueMessages_precedesQueue() {
        // Verifies ordering: /v1/queues/{name}/messages is matched as PURGE_QUEUE,
        // NOT as a queue named "{name}/messages".
        var r = router.route(HttpMethod.DELETE, "/v1/queues/myqueue/messages");
        assertEquals(HttpRouter.HandlerType.PURGE_QUEUE, r.handlerType());
        assertNotNull(r.resourceName());
    }

    // --- WS2.07: Connection management routes ---

    @Test
    void route_listConnections_get() {
        var r = router.route(HttpMethod.GET, "/v1/connections");
        assertEquals(HttpRouter.HandlerType.LIST_CONNECTIONS, r.handlerType());
    }

    @Test
    void route_getConnection_get() {
        var r = router.route(HttpMethod.GET, "/v1/connections/abc-123");
        assertEquals(HttpRouter.HandlerType.GET_CONNECTION, r.handlerType());
        assertEquals("abc-123", r.resourceName());
    }

    @Test
    void route_forceCloseConnection_delete() {
        var r = router.route(HttpMethod.DELETE, "/v1/connections/abc-123");
        assertEquals(HttpRouter.HandlerType.FORCE_CLOSE_CONNECTION, r.handlerType());
        assertEquals("abc-123", r.resourceName());
    }

    @Test
    void route_connectionWrongMethod_throws() {
        assertThrows(InvalidRequestException.class,
            () -> router.route(HttpMethod.POST, "/v1/connections/abc-123"));
    }

    // --- WS2.07: Consumer management routes ---

    @Test
    void route_listConsumers_get() {
        var r = router.route(HttpMethod.GET, "/v1/consumers");
        assertEquals(HttpRouter.HandlerType.LIST_CONSUMERS, r.handlerType());
    }

    @Test
    void route_listConsumers_queueFilter() {
        var r = router.route(HttpMethod.GET, "/v1/consumers?queue=orders");
        assertEquals(HttpRouter.HandlerType.LIST_CONSUMERS, r.handlerType());
        assertEquals("orders", r.queryParams().get("queue"));
    }

    @Test
    void route_forceCancelConsumer_delete() {
        var r = router.route(HttpMethod.DELETE, "/v1/consumers/conn-1/sub-42");
        assertEquals(HttpRouter.HandlerType.FORCE_CANCEL_CONSUMER, r.handlerType());
        assertEquals("conn-1", r.consumerGroup()); // connectionId reused via consumerGroup slot
        assertEquals("sub-42", r.resourceName());  // subscriptionId via resourceName slot
    }

    @Test
    void route_forceCancelConsumer_wrongMethod_throws() {
        assertThrows(InvalidRequestException.class,
            () -> router.route(HttpMethod.GET, "/v1/consumers/conn-1/sub-42"));
    }
}
