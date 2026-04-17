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
import kafka.server.http.routing.ExchangeManager;
import kafka.server.http.ws.QueueMetadata;
import kafka.server.http.ws.WsConfigs;
import kafka.server.http.ws.WsRoutingMetadataManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Cross-handler integration test that verifies exchange -> binding -> delete
 * flow through the REST handlers without going through the Netty pipeline.
 *
 * // Time: Created - TASK-WS2.06
 */
class RoutingRestHandlerTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String VHOST = "/";

    private ExchangeManager exchangeManager;
    private BindingManager bindingManager;
    private Map<String, QueueMetadata> queueStoreMap;
    private ExchangeRestHandler exchangeHandler;
    private QueueRestHandler queueHandler;
    private BindingRestHandler bindingHandler;

    @BeforeEach
    void setUp() {
        WsRoutingMetadataManager metadataManager = new WsRoutingMetadataManager(
            WsConfigs.withDefaults(), (k, v) -> { /* no-op record writer */ });
        exchangeManager = new ExchangeManager(metadataManager, WsConfigs.withDefaults());
        queueStoreMap = new LinkedHashMap<>();
        bindingManager = new BindingManager(
            name -> exchangeManager.getExchange(VHOST, name) != null,
            name -> queueStoreMap.containsKey(VHOST + ":" + name));
        exchangeHandler = new ExchangeRestHandler(exchangeManager, bindingManager);
        queueHandler = new QueueRestHandler(new MapBackedQueueStore(queueStoreMap));
        bindingHandler = new BindingRestHandler(bindingManager);
    }

    @Test
    void fullFlow_declareExchange_declareQueue_bind_list_delete() throws Exception {
        // 1. Declare exchange.
        FullHttpResponse ex = exchangeHandler.handleDeclare(
            "ex1", VHOST, makeRequest("{\"type\":\"direct\"}"));
        assertEquals(HttpResponseStatus.CREATED, ex.status());

        // 2. Declare queue.
        FullHttpResponse q = queueHandler.handleDeclare(
            "q1", VHOST, makeRequest("{}"));
        assertEquals(HttpResponseStatus.CREATED, q.status());

        // 3. Bind.
        FullHttpResponse bind = bindingHandler.handleCreate(VHOST, makeRequest(
            "{\"exchange\":\"ex1\",\"queue\":\"q1\",\"routingKey\":\"orders\"}"));
        assertEquals(HttpResponseStatus.CREATED, bind.status());

        // 4. List bindings for exchange.
        FullHttpResponse listed = bindingHandler.handleList(VHOST, "ex1", null);
        assertEquals(HttpResponseStatus.OK, listed.status());
        JsonNode listedBody = parseBody(listed);
        assertEquals(1, listedBody.get("bindings").size());
        assertEquals("orders", listedBody.get("bindings").get(0).get("routingKey").asText());

        // 5. Get exchange — bindingCount should reflect the bind.
        FullHttpResponse getEx = exchangeHandler.handleGet("ex1", VHOST);
        JsonNode getExBody = parseBody(getEx);
        assertEquals(1, getExBody.get("bindingCount").asInt());

        // 6. Delete binding.
        FullHttpResponse unbind = bindingHandler.handleDelete(VHOST, makeRequest(
            "{\"exchange\":\"ex1\",\"queue\":\"q1\",\"routingKey\":\"orders\"}"));
        assertEquals(HttpResponseStatus.NO_CONTENT, unbind.status());
        assertEquals(0, bindingManager.bindingCount("ex1"));

        // 7. Delete queue and exchange.
        assertEquals(HttpResponseStatus.NO_CONTENT,
            queueHandler.handleDelete("q1", VHOST, false, false).status());
        assertEquals(HttpResponseStatus.NO_CONTENT,
            exchangeHandler.handleDelete("ex1", VHOST, false).status());
    }

    @Test
    void allJsonResponses_haveCorrectContentType() throws Exception {
        assertTrue(exchangeHandler.handleList(VHOST).headers().get("Content-Type")
            .startsWith("application/json"));
        assertTrue(queueHandler.handleList(VHOST).headers().get("Content-Type")
            .startsWith("application/json"));
        assertTrue(bindingHandler.handleList(VHOST, null, null).headers().get("Content-Type")
            .startsWith("application/json"));
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    private static FullHttpRequest makeRequest(String json) {
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        return new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST,
            "/v1/any", Unpooled.wrappedBuffer(bytes));
    }

    private static JsonNode parseBody(FullHttpResponse resp) throws Exception {
        byte[] bytes = new byte[resp.content().readableBytes()];
        resp.content().getBytes(resp.content().readerIndex(), bytes);
        if (bytes.length == 0) return MAPPER.readTree("{}");
        return MAPPER.readTree(bytes);
    }

    /** Minimal store backed by a shared map; also exposes queue existence to BindingManager. */
    private static final class MapBackedQueueStore implements QueueRestHandler.QueueStore {
        private final Map<String, QueueMetadata> queues;

        MapBackedQueueStore(Map<String, QueueMetadata> queues) {
            this.queues = queues;
        }

        @Override public QueueMetadata get(String vhost, String name) {
            return queues.get(vhost + ":" + name);
        }

        @Override public QueueMetadata declare(String vhost, String name, boolean durable,
                boolean exclusive, boolean autoDelete,
                Map<String, String> arguments) {
            QueueMetadata q = new QueueMetadata(name, vhost, durable, exclusive, autoDelete,
                arguments == null ? Collections.emptyMap() : new HashMap<>(arguments));
            queues.put(vhost + ":" + name, q);
            return q;
        }

        @Override public Collection<QueueMetadata> list(String vhost) {
            Collection<QueueMetadata> out = new ArrayList<>();
            for (QueueMetadata q : queues.values()) {
                if (q.vhost().equals(vhost)) out.add(q);
            }
            return out;
        }

        @Override public void delete(String vhost, String name, boolean ifUnused, boolean ifEmpty) {
            queues.remove(vhost + ":" + name);
        }
    }
}
