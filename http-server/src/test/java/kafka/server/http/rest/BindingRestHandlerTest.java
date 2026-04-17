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
package kafka.server.http.rest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import kafka.server.http.routing.BindingManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * // Time: Created - TASK-WS2.06
 */
class BindingRestHandlerTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String VHOST = "/";

    private BindingManager bindingManager;
    private BindingRestHandler handler;
    private Set<String> existingExchanges;
    private Set<String> existingQueues;

    @BeforeEach
    void setUp() {
        existingExchanges = new HashSet<>();
        existingQueues = new HashSet<>();
        bindingManager = new BindingManager(existingExchanges::contains, existingQueues::contains);
        handler = new BindingRestHandler(bindingManager);
    }

    @Test
    void createBinding_queueToExchange_returns201() throws Exception {
        existingExchanges.add("ex1");
        existingQueues.add("q1");

        FullHttpResponse resp = handler.handleCreate(VHOST, requestBody(
            "{\"exchange\":\"ex1\",\"queue\":\"q1\",\"routingKey\":\"orders\"}"));

        assertEquals(HttpResponseStatus.CREATED, resp.status());
        JsonNode body = parseBody(resp);
        assertEquals("ex1", body.get("exchange").asText());
        assertEquals("q1", body.get("queue").asText());
        assertEquals("orders", body.get("routingKey").asText());
        assertEquals(1, bindingManager.bindingCount("ex1"));
    }

    @Test
    void createBinding_missingFields_returns400() {
        FullHttpResponse resp = handler.handleCreate(VHOST, requestBody("{\"queue\":\"q1\"}"));
        assertEquals(HttpResponseStatus.BAD_REQUEST, resp.status());
    }

    @Test
    void createBinding_unknownExchange_returns404() {
        existingQueues.add("q1");
        FullHttpResponse resp = handler.handleCreate(VHOST, requestBody(
            "{\"exchange\":\"missing\",\"queue\":\"q1\",\"routingKey\":\"rk\"}"));
        assertEquals(HttpResponseStatus.NOT_FOUND, resp.status());
    }

    @Test
    void createBinding_malformedJson_returns400() {
        FullHttpResponse resp = handler.handleCreate(VHOST, requestBody("{not json"));
        assertEquals(HttpResponseStatus.BAD_REQUEST, resp.status());
    }

    @Test
    void listBindings_filterByExchange() throws Exception {
        existingExchanges.add("ex1");
        existingQueues.add("q1");
        existingQueues.add("q2");
        bindingManager.bind("ex1", "q1", "a");
        bindingManager.bind("ex1", "q2", "b");

        FullHttpResponse resp = handler.handleList(VHOST, "ex1", null);
        assertEquals(HttpResponseStatus.OK, resp.status());
        JsonNode body = parseBody(resp);
        assertEquals(2, body.get("bindings").size());
    }

    @Test
    void listBindings_filterByQueue() throws Exception {
        existingExchanges.add("ex1");
        existingExchanges.add("ex2");
        existingQueues.add("q1");
        bindingManager.bind("ex1", "q1", "rk");
        bindingManager.bind("ex2", "q1", "rk");

        FullHttpResponse resp = handler.handleList(VHOST, null, "q1");
        assertEquals(HttpResponseStatus.OK, resp.status());
        JsonNode body = parseBody(resp);
        assertEquals(2, body.get("bindings").size());
    }

    @Test
    void listBindings_combinedFilter_narrowsResults() throws Exception {
        existingExchanges.add("ex1");
        existingExchanges.add("ex2");
        existingQueues.add("q1");
        existingQueues.add("q2");
        bindingManager.bind("ex1", "q1", "rk");
        bindingManager.bind("ex1", "q2", "rk");
        bindingManager.bind("ex2", "q1", "rk");

        FullHttpResponse resp = handler.handleList(VHOST, "ex1", "q2");
        JsonNode body = parseBody(resp);
        assertEquals(1, body.get("bindings").size());
        assertEquals("q2", body.get("bindings").get(0).get("queue").asText());
    }

    @Test
    void listBindings_noFilter_returnsEmptyArray() throws Exception {
        existingExchanges.add("ex1");
        existingQueues.add("q1");
        bindingManager.bind("ex1", "q1", "rk");

        FullHttpResponse resp = handler.handleList(VHOST, null, null);
        JsonNode body = parseBody(resp);
        assertEquals(0, body.get("bindings").size());
    }

    @Test
    void deleteBinding_returns204() throws Exception {
        existingExchanges.add("ex1");
        existingQueues.add("q1");
        bindingManager.bind("ex1", "q1", "rk");

        FullHttpResponse resp = handler.handleDelete(VHOST, requestBody(
            "{\"exchange\":\"ex1\",\"queue\":\"q1\",\"routingKey\":\"rk\"}"));
        assertEquals(HttpResponseStatus.NO_CONTENT, resp.status());
        assertEquals(0, bindingManager.bindingCount("ex1"));
    }

    @Test
    void deleteBinding_missingFields_returns400() {
        FullHttpResponse resp = handler.handleDelete(VHOST, requestBody("{}"));
        assertEquals(HttpResponseStatus.BAD_REQUEST, resp.status());
    }

    @Test
    void deleteBinding_nonExistent_stillReturns204() throws Exception {
        // unbind() is a silent no-op per AMQP semantics.
        FullHttpResponse resp = handler.handleDelete(VHOST, requestBody(
            "{\"exchange\":\"missing\",\"queue\":\"q\",\"routingKey\":\"rk\"}"));
        assertEquals(HttpResponseStatus.NO_CONTENT, resp.status());
    }

    @Test
    void response_contentTypeIsJson() throws Exception {
        existingExchanges.add("ex1");
        existingQueues.add("q1");
        FullHttpResponse resp = handler.handleCreate(VHOST, requestBody(
            "{\"exchange\":\"ex1\",\"queue\":\"q1\",\"routingKey\":\"rk\"}"));
        assertTrue(resp.headers().get("Content-Type").startsWith("application/json"));
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    private static FullHttpRequest requestBody(String json) {
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        return new DefaultFullHttpRequest(
            HttpVersion.HTTP_1_1, HttpMethod.POST, "/v1/bindings",
            Unpooled.wrappedBuffer(bytes));
    }

    private static JsonNode parseBody(FullHttpResponse resp) throws Exception {
        byte[] bytes = new byte[resp.content().readableBytes()];
        resp.content().getBytes(resp.content().readerIndex(), bytes);
        if (bytes.length == 0) return MAPPER.readTree("{}");
        return MAPPER.readTree(bytes);
    }
}
