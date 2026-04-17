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

// Time: Created - TASK-WS2.09

package kafka.server.http.rest;

import kafka.server.http.routing.BindingManager;
import kafka.server.http.routing.ExchangeManager;
import kafka.server.http.routing.VhostManager;
import kafka.server.http.ws.WsConfigs;
import kafka.server.http.ws.WsRoutingMetadataManager;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link VhostRestHandler}.
 *
 * // Time: Created - TASK-WS2.09
 */
class VhostRestHandlerTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private VhostManager vhostManager;
    private VhostRestHandler handler;
    private AtomicBoolean hasActiveConnections;

    @BeforeEach
    void setUp() {
        WsRoutingMetadataManager metadataManager = new WsRoutingMetadataManager(
            WsConfigs.withDefaults(), (k, v) -> { /* no-op writer */ });
        ExchangeManager exchangeManager = new ExchangeManager(metadataManager, WsConfigs.withDefaults());
        BindingManager bindingManager = new BindingManager(name -> true, name -> true);
        hasActiveConnections = new AtomicBoolean(false);
        vhostManager = new VhostManager(
            metadataManager, exchangeManager, bindingManager,
            v -> hasActiveConnections.get());
        handler = new VhostRestHandler(vhostManager);
    }

    // ------------------------------------------------------------------
    // List
    // ------------------------------------------------------------------

    @Test
    void handleList_defaultVhostOnly_returnsOne() throws Exception {
        FullHttpResponse resp = handler.handleList();
        assertEquals(HttpResponseStatus.OK, resp.status());
        JsonNode body = parseBody(resp);
        JsonNode vhosts = body.get("vhosts");
        assertEquals(1, vhosts.size());
        assertEquals("/", vhosts.get(0).get("name").asText());
        assertEquals(5, vhosts.get(0).get("exchangeCount").asInt());
        assertEquals(0, vhosts.get(0).get("queueCount").asInt());
    }

    @Test
    void handleList_multipleVhosts_returnsAll() throws Exception {
        vhostManager.createVhost("/prod");
        vhostManager.createVhost("/stage");

        FullHttpResponse resp = handler.handleList();
        assertEquals(HttpResponseStatus.OK, resp.status());
        JsonNode body = parseBody(resp);
        assertEquals(3, body.get("vhosts").size());
    }

    // ------------------------------------------------------------------
    // Create
    // ------------------------------------------------------------------

    @Test
    void handleCreate_newVhost_returns201() throws Exception {
        FullHttpResponse resp = handler.handleCreate("/production");
        assertEquals(HttpResponseStatus.CREATED, resp.status());
        JsonNode body = parseBody(resp);
        assertEquals("/production", body.get("name").asText());
        assertEquals(5, body.get("exchangeCount").asInt());
        assertTrue(vhostManager.exists("/production"));
    }

    @Test
    void handleCreate_nameWithoutLeadingSlash_prepended() throws Exception {
        FullHttpResponse resp = handler.handleCreate("staging");
        assertEquals(HttpResponseStatus.CREATED, resp.status());
        JsonNode body = parseBody(resp);
        assertEquals("/staging", body.get("name").asText());
        assertTrue(vhostManager.exists("/staging"));
    }

    @Test
    void handleCreate_existingVhost_returns200() throws Exception {
        vhostManager.createVhost("/already");
        FullHttpResponse resp = handler.handleCreate("/already");
        assertEquals(HttpResponseStatus.OK, resp.status());
    }

    @Test
    void handleCreate_emptyName_returns400() throws Exception {
        FullHttpResponse resp = handler.handleCreate("");
        assertEquals(HttpResponseStatus.BAD_REQUEST, resp.status());
    }

    // ------------------------------------------------------------------
    // Delete
    // ------------------------------------------------------------------

    @Test
    void handleDelete_existingVhost_returns204() {
        vhostManager.createVhost("/tmp");
        FullHttpResponse resp = handler.handleDelete("/tmp");
        assertEquals(HttpResponseStatus.NO_CONTENT, resp.status());
        assertTrue(!vhostManager.exists("/tmp"));
    }

    @Test
    void handleDelete_unknownVhost_returns404() {
        FullHttpResponse resp = handler.handleDelete("/never");
        assertEquals(HttpResponseStatus.NOT_FOUND, resp.status());
    }

    @Test
    void handleDelete_defaultVhost_returns403() {
        FullHttpResponse resp = handler.handleDelete("/");
        assertEquals(HttpResponseStatus.FORBIDDEN, resp.status());
        assertTrue(vhostManager.exists("/"));
    }

    @Test
    void handleDelete_activeConnections_returns409() {
        vhostManager.createVhost("/busy");
        hasActiveConnections.set(true);
        FullHttpResponse resp = handler.handleDelete("/busy");
        assertEquals(HttpResponseStatus.CONFLICT, resp.status());
        // Vhost still present because deletion was rejected.
        assertTrue(vhostManager.exists("/busy"));
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    private static JsonNode parseBody(FullHttpResponse resp) throws Exception {
        byte[] bytes = new byte[resp.content().readableBytes()];
        resp.content().getBytes(resp.content().readerIndex(), bytes);
        return MAPPER.readTree(new String(bytes, StandardCharsets.UTF_8));
    }
}
