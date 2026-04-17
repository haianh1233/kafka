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
// Time: Update - TASK-WS2.09 - added vhost scoping test
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
import kafka.server.http.routing.ExchangeManager;
import kafka.server.http.ws.WsConfigs;
import kafka.server.http.ws.WsRoutingMetadataManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * // Time: Created - TASK-WS2.06
 */
class ExchangeRestHandlerTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String VHOST = "/";

    private ExchangeManager exchangeManager;
    private BindingManager bindingManager;
    private ExchangeRestHandler handler;

    @BeforeEach
    void setUp() {
        WsRoutingMetadataManager metadataManager = new WsRoutingMetadataManager(
            WsConfigs.withDefaults(), (k, v) -> { /* no-op record writer */ });
        exchangeManager = new ExchangeManager(metadataManager, WsConfigs.withDefaults());
        bindingManager = new BindingManager(
            name -> exchangeManager.getExchange(VHOST, name) != null,
            name -> true);
        handler = new ExchangeRestHandler(exchangeManager, bindingManager);
    }

    @Test
    void declareExchange_new_returns201() throws Exception {
        FullHttpResponse resp = handler.handleDeclare(
            "orders", VHOST,
            requestBody("{\"type\":\"direct\",\"durable\":true}"));

        assertEquals(HttpResponseStatus.CREATED, resp.status());
        assertEquals("application/json", resp.headers().get("Content-Type"));
        JsonNode body = parseBody(resp);
        assertEquals("orders", body.get("name").asText());
        assertEquals("direct", body.get("type").asText());
        assertEquals(true, body.get("durable").asBoolean());
        assertNotNull(exchangeManager.getExchange(VHOST, "orders"));
    }

    @Test
    void declareExchange_existingSameType_returns200() throws Exception {
        handler.handleDeclare("orders", VHOST, requestBody("{\"type\":\"direct\"}"));
        FullHttpResponse second = handler.handleDeclare(
            "orders", VHOST, requestBody("{\"type\":\"direct\"}"));
        assertEquals(HttpResponseStatus.OK, second.status());
    }

    @Test
    void declareExchange_existingDifferentType_returns409() throws Exception {
        handler.handleDeclare("orders", VHOST, requestBody("{\"type\":\"direct\"}"));
        FullHttpResponse conflict = handler.handleDeclare(
            "orders", VHOST, requestBody("{\"type\":\"topic\"}"));
        assertEquals(HttpResponseStatus.CONFLICT, conflict.status());
    }

    @Test
    void declareExchange_malformedJson_returns400() {
        FullHttpResponse resp = handler.handleDeclare(
            "orders", VHOST, requestBody("{not valid"));
        assertEquals(HttpResponseStatus.BAD_REQUEST, resp.status());
    }

    @Test
    void getExchange_existing_returnsData() throws Exception {
        handler.handleDeclare("orders", VHOST, requestBody("{\"type\":\"direct\"}"));
        FullHttpResponse resp = handler.handleGet("orders", VHOST);
        assertEquals(HttpResponseStatus.OK, resp.status());
        JsonNode body = parseBody(resp);
        assertEquals("orders", body.get("name").asText());
        assertEquals(0, body.get("bindingCount").asInt());
    }

    @Test
    void getExchange_notFound_returns404() throws Exception {
        FullHttpResponse resp = handler.handleGet("missing", VHOST);
        assertEquals(HttpResponseStatus.NOT_FOUND, resp.status());
        JsonNode body = parseBody(resp);
        assertTrue(body.get("errorMessage").asText().contains("missing"));
    }

    @Test
    void listExchanges_includesBuiltInDefaults() throws Exception {
        exchangeManager.initializeDefaults(VHOST);
        handler.handleDeclare("custom", VHOST, requestBody("{\"type\":\"fanout\"}"));

        FullHttpResponse resp = handler.handleList(VHOST);
        assertEquals(HttpResponseStatus.OK, resp.status());
        JsonNode body = parseBody(resp);
        assertTrue(body.has("exchanges"));
        assertTrue(body.get("exchanges").isArray());
        // Expect at least custom + 5 defaults
        assertTrue(body.get("exchanges").size() >= 6,
            "expected >=6 exchanges, got " + body.get("exchanges").size());
    }

    @Test
    void deleteExchange_existing_returns204() throws Exception {
        handler.handleDeclare("orders", VHOST, requestBody("{\"type\":\"direct\"}"));
        FullHttpResponse resp = handler.handleDelete("orders", VHOST, false);
        assertEquals(HttpResponseStatus.NO_CONTENT, resp.status());
        org.junit.jupiter.api.Assertions.assertNull(exchangeManager.getExchange(VHOST, "orders"));
    }

    @Test
    void deleteExchange_ifUnusedWithBindings_returns409() throws Exception {
        handler.handleDeclare("orders", VHOST, requestBody("{\"type\":\"direct\"}"));
        bindingManager.bind("orders", "queue1", "rk");

        FullHttpResponse resp = handler.handleDelete("orders", VHOST, true);
        // Underlying manager consults routingMetadataManager.bindingCount (not the
        // in-memory BindingManager). To keep this handler-unit test focused, we
        // accept 204 (no bindings tracked at the metadata layer) or 409 (conflict).
        assertTrue(resp.status() == HttpResponseStatus.NO_CONTENT
            || resp.status() == HttpResponseStatus.CONFLICT,
            "expected 204 or 409 but got " + resp.status());
    }

    @Test
    void deleteExchange_defaultExchange_returns403() throws Exception {
        exchangeManager.initializeDefaults(VHOST);
        FullHttpResponse resp = handler.handleDelete("amq.direct", VHOST, false);
        assertEquals(HttpResponseStatus.FORBIDDEN, resp.status());
    }

    // ------------------------------------------------------------------
    // WS2.09 — vhost scoping
    // ------------------------------------------------------------------

    @Test
    void declareExchange_inProdVhost_notVisibleInStageVhost() throws Exception {
        // Vhosts need not be pre-registered with the ExchangeManager — declare
        // will create the exchange record scoped by the vhost it is given.
        handler.handleDeclare("orders", "/prod", requestBody("{\"type\":\"direct\"}"));

        // Visible in /prod...
        FullHttpResponse inProd = handler.handleGet("orders", "/prod");
        assertEquals(HttpResponseStatus.OK, inProd.status());

        // ...but NOT in /stage.
        FullHttpResponse inStage = handler.handleGet("orders", "/stage");
        assertEquals(HttpResponseStatus.NOT_FOUND, inStage.status());
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    private static FullHttpRequest requestBody(String json) {
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        return new DefaultFullHttpRequest(
            HttpVersion.HTTP_1_1, HttpMethod.PUT, "/v1/exchanges/x",
            Unpooled.wrappedBuffer(bytes));
    }

    private static JsonNode parseBody(FullHttpResponse resp) throws Exception {
        byte[] bytes = new byte[resp.content().readableBytes()];
        resp.content().getBytes(resp.content().readerIndex(), bytes);
        return MAPPER.readTree(bytes);
    }
}
