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
import kafka.server.http.ws.QueueMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * // Time: Created - TASK-WS2.06
 */
class QueueRestHandlerTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String VHOST = "/";

    private InMemoryQueueStore store;
    private QueueRestHandler handler;

    @BeforeEach
    void setUp() {
        store = new InMemoryQueueStore();
        handler = new QueueRestHandler(store);
    }

    @Test
    void declareQueue_new_returns201() throws Exception {
        FullHttpResponse resp = handler.handleDeclare(
            "q1", VHOST, requestBody("{\"durable\":true}"));
        assertEquals(HttpResponseStatus.CREATED, resp.status());
        JsonNode body = parseBody(resp);
        assertEquals("q1", body.get("name").asText());
        assertEquals(true, body.get("durable").asBoolean());
        assertNotNull(store.get(VHOST, "q1"));
    }

    @Test
    void declareQueue_idempotent_returns200() throws Exception {
        handler.handleDeclare("q1", VHOST, requestBody("{\"durable\":true}"));
        FullHttpResponse second = handler.handleDeclare(
            "q1", VHOST, requestBody("{\"durable\":true}"));
        assertEquals(HttpResponseStatus.OK, second.status());
    }

    @Test
    void declareQueue_flagsMismatch_returns409() throws Exception {
        handler.handleDeclare("q1", VHOST,
            requestBody("{\"durable\":true,\"exclusive\":false}"));
        FullHttpResponse conflict = handler.handleDeclare(
            "q1", VHOST, requestBody("{\"durable\":false}"));
        assertEquals(HttpResponseStatus.CONFLICT, conflict.status());
    }

    @Test
    void getQueue_existing_returnsDataWithConsumersArray() throws Exception {
        handler.handleDeclare("q1", VHOST, requestBody("{}"));
        FullHttpResponse resp = handler.handleGet("q1", VHOST);
        assertEquals(HttpResponseStatus.OK, resp.status());
        JsonNode body = parseBody(resp);
        assertEquals("q1", body.get("name").asText());
        assertTrue(body.has("consumers"));
        assertTrue(body.get("consumers").isArray());
    }

    @Test
    void getQueue_notFound_returns404() throws Exception {
        FullHttpResponse resp = handler.handleGet("missing", VHOST);
        assertEquals(HttpResponseStatus.NOT_FOUND, resp.status());
    }

    @Test
    void listQueues_returnsAll() throws Exception {
        handler.handleDeclare("q1", VHOST, requestBody("{}"));
        handler.handleDeclare("q2", VHOST, requestBody("{}"));
        FullHttpResponse resp = handler.handleList(VHOST);
        assertEquals(HttpResponseStatus.OK, resp.status());
        JsonNode body = parseBody(resp);
        assertEquals(2, body.get("queues").size());
    }

    @Test
    void deleteQueue_existing_returns204() throws Exception {
        handler.handleDeclare("q1", VHOST, requestBody("{}"));
        FullHttpResponse resp = handler.handleDelete("q1", VHOST, false, false);
        assertEquals(HttpResponseStatus.NO_CONTENT, resp.status());
        assertNull(store.get(VHOST, "q1"));
    }

    @Test
    void deleteQueue_ifUnused_withConsumers_returns409() throws Exception {
        handler.handleDeclare("q1", VHOST, requestBody("{}"));
        store.markInUse("q1");
        FullHttpResponse resp = handler.handleDelete("q1", VHOST, true, false);
        assertEquals(HttpResponseStatus.CONFLICT, resp.status());
    }

    @Test
    void declareQueue_malformedJson_returns400() {
        FullHttpResponse resp = handler.handleDeclare(
            "q1", VHOST, requestBody("{not valid"));
        assertEquals(HttpResponseStatus.BAD_REQUEST, resp.status());
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    private static FullHttpRequest requestBody(String json) {
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        return new DefaultFullHttpRequest(
            HttpVersion.HTTP_1_1, HttpMethod.PUT, "/v1/queues/x",
            Unpooled.wrappedBuffer(bytes));
    }

    private static JsonNode parseBody(FullHttpResponse resp) throws Exception {
        byte[] bytes = new byte[resp.content().readableBytes()];
        resp.content().getBytes(resp.content().readerIndex(), bytes);
        if (bytes.length == 0) return MAPPER.readTree("{}");
        return MAPPER.readTree(bytes);
    }

    /**
     * Lightweight {@link QueueRestHandler.QueueStore} implementation used to
     * unit-test the handler without any QueueManager dependency.
     */
    private static final class InMemoryQueueStore implements QueueRestHandler.QueueStore {
        private final Map<String, QueueMetadata> queues = new LinkedHashMap<>();
        private final java.util.Set<String> inUse = new java.util.HashSet<>();

        @Override public QueueMetadata get(String vhost, String name) {
            return queues.get(key(vhost, name));
        }

        @Override public QueueMetadata declare(String vhost, String name, boolean durable,
                boolean exclusive, boolean autoDelete,
                Map<String, String> arguments) throws QueueRestHandler.QueueConflict {
            QueueMetadata existing = queues.get(key(vhost, name));
            if (existing != null) {
                if (existing.durable() != durable
                    || existing.exclusive() != exclusive
                    || existing.autoDelete() != autoDelete) {
                    throw new QueueRestHandler.QueueConflict(
                        "Queue '" + name + "' flags mismatch");
                }
                return existing;
            }
            QueueMetadata q = new QueueMetadata(name, vhost, durable, exclusive, autoDelete,
                arguments == null ? Collections.emptyMap() : arguments);
            queues.put(key(vhost, name), q);
            return q;
        }

        @Override public Collection<QueueMetadata> list(String vhost) {
            Collection<QueueMetadata> out = new ArrayList<>();
            for (QueueMetadata q : queues.values()) {
                if (q.vhost().equals(vhost)) out.add(q);
            }
            return out;
        }

        @Override public void delete(String vhost, String name, boolean ifUnused, boolean ifEmpty)
                throws QueueRestHandler.QueueConflict {
            if (ifUnused && inUse.contains(name)) {
                throw new QueueRestHandler.QueueConflict(
                    "Queue '" + name + "' has active consumers");
            }
            queues.remove(key(vhost, name));
        }

        void markInUse(String name) {
            inUse.add(name);
        }

        private static String key(String vhost, String name) {
            return vhost + ":" + name;
        }
    }
}
