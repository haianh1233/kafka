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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link ConsumerGroupLagHandler} lag computation logic.
 *
 * These tests exercise buildLagResponse() directly with pre-computed offset maps,
 * verifying the merge logic, clamping, sorting, and edge cases without requiring
 * a running broker or RequestChannel.
 */
class ConsumerGroupLagHandlerTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private JsonNode parseResponse(FullHttpResponse response) throws Exception {
        byte[] bytes = new byte[response.content().readableBytes()];
        response.content().readBytes(bytes);
        return MAPPER.readTree(bytes);
    }

    @Test
    void testLagCalculationNormal() throws Exception {
        Map<TopicPartition, Long> committed = new LinkedHashMap<>();
        committed.put(new TopicPartition("orders", 0), 8500L);
        committed.put(new TopicPartition("orders", 1), 5000L);

        Map<TopicPartition, Long> logEnd = new LinkedHashMap<>();
        logEnd.put(new TopicPartition("orders", 0), 10043L);
        logEnd.put(new TopicPartition("orders", 1), 5500L);

        // lag(P0) = 10043 - 8500 = 1543
        // lag(P1) = 5500 - 5000 = 500
        // totalLag = 2043
        ConsumerGroupLagHandler handler = new ConsumerGroupLagHandler();
        FullHttpResponse response = handler.buildLagResponse("test-group", committed, logEnd);

        assertEquals(HttpResponseStatus.OK, response.status());

        JsonNode json = parseResponse(response);
        assertEquals("test-group", json.get("group").asText());
        assertEquals(2043L, json.get("totalLag").asLong());
        assertEquals(2, json.get("partitions").size());
    }

    @Test
    void testLagClampedToZero() throws Exception {
        // Due to concurrent sampling, committed might briefly exceed logEnd
        Map<TopicPartition, Long> committed = Map.of(
            new TopicPartition("orders", 0), 10050L
        );
        Map<TopicPartition, Long> logEnd = Map.of(
            new TopicPartition("orders", 0), 10043L
        );
        // lag = max(0, 10043 - 10050) = 0

        ConsumerGroupLagHandler handler = new ConsumerGroupLagHandler();
        FullHttpResponse response = handler.buildLagResponse("test-group", committed, logEnd);

        JsonNode json = parseResponse(response);
        assertEquals(0L, json.get("totalLag").asLong());
        assertEquals(0L, json.get("partitions").get(0).get("lag").asLong());
    }

    @Test
    void testLagMissingLogEndOffset() throws Exception {
        // logEndOffset missing for a partition (e.g., authorization failed on LIST_OFFSETS)
        Map<TopicPartition, Long> committed = Map.of(
            new TopicPartition("orders", 0), 8500L
        );
        Map<TopicPartition, Long> logEnd = Map.of(); // no log-end data

        ConsumerGroupLagHandler handler = new ConsumerGroupLagHandler();
        FullHttpResponse response = handler.buildLagResponse("test-group", committed, logEnd);

        JsonNode json = parseResponse(response);
        // lag should be 0 when logEnd is unknown
        assertEquals(0L, json.get("partitions").get(0).get("lag").asLong());
        assertEquals(-1L, json.get("partitions").get(0).get("logEndOffset").asLong());
    }

    @Test
    void testEmptyGroupHasZeroLag() throws Exception {
        Map<TopicPartition, Long> committed = Map.of();
        Map<TopicPartition, Long> logEnd = Map.of();

        ConsumerGroupLagHandler handler = new ConsumerGroupLagHandler();
        FullHttpResponse response = handler.buildLagResponse("empty-group", committed, logEnd);

        JsonNode json = parseResponse(response);
        assertEquals(0L, json.get("totalLag").asLong());
        assertEquals(0, json.get("partitions").size());
    }

    @Test
    void testPartitionsSortedByTopicThenPartition() throws Exception {
        Map<TopicPartition, Long> committed = new LinkedHashMap<>();
        committed.put(new TopicPartition("orders", 1), 100L);
        committed.put(new TopicPartition("events", 0), 200L);
        committed.put(new TopicPartition("orders", 0), 300L);

        Map<TopicPartition, Long> logEnd = new LinkedHashMap<>();
        logEnd.put(new TopicPartition("orders", 1), 150L);
        logEnd.put(new TopicPartition("events", 0), 250L);
        logEnd.put(new TopicPartition("orders", 0), 350L);

        ConsumerGroupLagHandler handler = new ConsumerGroupLagHandler();
        FullHttpResponse response = handler.buildLagResponse("test-group", committed, logEnd);

        JsonNode json = parseResponse(response);
        JsonNode partitions = json.get("partitions");

        // Should be sorted: events/0, orders/0, orders/1
        assertEquals("events", partitions.get(0).get("topic").asText());
        assertEquals(0, partitions.get(0).get("partition").asInt());
        assertEquals("orders", partitions.get(1).get("topic").asText());
        assertEquals(0, partitions.get(1).get("partition").asInt());
        assertEquals("orders", partitions.get(2).get("topic").asText());
        assertEquals(1, partitions.get(2).get("partition").asInt());
    }

    @Test
    void testTotalLagSumsAllPartitions() throws Exception {
        Map<TopicPartition, Long> committed = new LinkedHashMap<>();
        committed.put(new TopicPartition("topic-a", 0), 10L);
        committed.put(new TopicPartition("topic-a", 1), 20L);
        committed.put(new TopicPartition("topic-b", 0), 30L);

        Map<TopicPartition, Long> logEnd = new LinkedHashMap<>();
        logEnd.put(new TopicPartition("topic-a", 0), 100L);
        logEnd.put(new TopicPartition("topic-a", 1), 120L);
        logEnd.put(new TopicPartition("topic-b", 0), 130L);

        ConsumerGroupLagHandler handler = new ConsumerGroupLagHandler();
        FullHttpResponse response = handler.buildLagResponse("sum-group", committed, logEnd);

        JsonNode json = parseResponse(response);
        // lag = (100-10) + (120-20) + (130-30) = 90 + 100 + 100 = 290
        assertEquals(290L, json.get("totalLag").asLong());
        assertEquals(3, json.get("partitions").size());
    }

    @Test
    void testNegativeCommittedOffsetTreatedAsZeroLag() throws Exception {
        // committedOffset = -1 means unknown/never committed
        Map<TopicPartition, Long> committed = Map.of(
            new TopicPartition("orders", 0), -1L
        );
        Map<TopicPartition, Long> logEnd = Map.of(
            new TopicPartition("orders", 0), 100L
        );

        ConsumerGroupLagHandler handler = new ConsumerGroupLagHandler();
        FullHttpResponse response = handler.buildLagResponse("test-group", committed, logEnd);

        JsonNode json = parseResponse(response);
        // When committedOffset < 0, lag should be 0
        assertEquals(0L, json.get("partitions").get(0).get("lag").asLong());
    }

    @Test
    void testGroupFieldMatchesRequest() throws Exception {
        String groupId = "my-special-group-123";
        Map<TopicPartition, Long> committed = Map.of(
            new TopicPartition("t", 0), 50L
        );
        Map<TopicPartition, Long> logEnd = Map.of(
            new TopicPartition("t", 0), 60L
        );

        ConsumerGroupLagHandler handler = new ConsumerGroupLagHandler();
        FullHttpResponse response = handler.buildLagResponse(groupId, committed, logEnd);

        JsonNode json = parseResponse(response);
        assertEquals(groupId, json.get("group").asText());
    }

    @Test
    void testResponseIncludesAllFields() throws Exception {
        Map<TopicPartition, Long> committed = Map.of(
            new TopicPartition("orders", 0), 8500L
        );
        Map<TopicPartition, Long> logEnd = Map.of(
            new TopicPartition("orders", 0), 10043L
        );

        ConsumerGroupLagHandler handler = new ConsumerGroupLagHandler();
        FullHttpResponse response = handler.buildLagResponse("test-group", committed, logEnd);

        JsonNode json = parseResponse(response);
        JsonNode partition = json.get("partitions").get(0);

        assertTrue(partition.has("topic"));
        assertTrue(partition.has("partition"));
        assertTrue(partition.has("committedOffset"));
        assertTrue(partition.has("logEndOffset"));
        assertTrue(partition.has("lag"));

        assertEquals("orders", partition.get("topic").asText());
        assertEquals(0, partition.get("partition").asInt());
        assertEquals(8500L, partition.get("committedOffset").asLong());
        assertEquals(10043L, partition.get("logEndOffset").asLong());
        assertEquals(1543L, partition.get("lag").asLong());
    }
}
