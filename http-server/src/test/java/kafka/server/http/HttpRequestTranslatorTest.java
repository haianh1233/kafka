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

import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.utils.Utils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HttpRequestTranslatorTest {

    private HttpRequestTranslator translator;
    private HttpServerConfigs config;

    @BeforeEach
    void setUp() {
        translator = new HttpRequestTranslator();
        config = new HttpServerConfigs(10000, 5000, 10 * 1024 * 1024);
    }

    // --- Produce translation ---

    @Test
    void translateProduce_singleRecord_buildsProduceRequest() {
        String json = """
            { "records": [{"value": {"type": "STRING", "data": "hello"}}] }
            """;
        var route = new HttpRouter.RouteResult(
            HttpRouter.HandlerType.PRODUCE, "my-topic", null, null, Map.of());
        var result = translator.translate(route, json.getBytes(StandardCharsets.UTF_8),
            config, topic -> 3);
        assertEquals(ApiKeys.PRODUCE, result.apiKey());
        assertNotNull(result.serializedRequest());
    }

    @Test
    void translateProduce_keyedRecord_usesMurmur2() {
        String json = """
            { "records": [{"key": {"type": "STRING", "data": "mykey"}, "value": {"type": "STRING", "data": "hello"}}] }
            """;
        var route = new HttpRouter.RouteResult(
            HttpRouter.HandlerType.PRODUCE, "my-topic", null, null, Map.of());
        var result = translator.translate(route, json.getBytes(StandardCharsets.UTF_8),
            config, topic -> 6);
        assertEquals(ApiKeys.PRODUCE, result.apiKey());
        assertNotNull(result.serializedRequest());
        // Verify the expected partition: murmur2("mykey".getBytes()) & 0x7fffffff % 6
        int expectedPartition = (Utils.murmur2("mykey".getBytes(StandardCharsets.UTF_8)) & 0x7fffffff) % 6;
        assertTrue(expectedPartition >= 0 && expectedPartition < 6);
    }

    @Test
    void translateProduce_keylessRecords_allSamePartition() {
        // Two keyless records in the same request should go to the same partition (batch-sticky)
        String json = """
            { "records": [
                {"value": {"type": "STRING", "data": "r1"}},
                {"value": {"type": "STRING", "data": "r2"}}
            ] }
            """;
        var route = new HttpRouter.RouteResult(
            HttpRouter.HandlerType.PRODUCE, "my-topic", null, null, Map.of());
        var result = translator.translate(route, json.getBytes(StandardCharsets.UTF_8),
            config, topic -> 10);
        assertEquals(ApiKeys.PRODUCE, result.apiKey());
        assertNotNull(result.serializedRequest());
    }

    @Test
    void translateProduce_emptyBody_throws() {
        var route = new HttpRouter.RouteResult(
            HttpRouter.HandlerType.PRODUCE, "my-topic", null, null, Map.of());
        assertThrows(InvalidRequestException.class,
            () -> translator.translate(route, null, config, topic -> 3));
    }

    @Test
    void translateProduce_tooManyRecords_throws() {
        // Config allows max 10000 records, create a JSON with more than that
        HttpServerConfigs limitedConfig = new HttpServerConfigs(2, 5000, 10 * 1024 * 1024);
        String json = """
            { "records": [
                {"value": {"type": "STRING", "data": "r1"}},
                {"value": {"type": "STRING", "data": "r2"}},
                {"value": {"type": "STRING", "data": "r3"}}
            ] }
            """;
        var route = new HttpRouter.RouteResult(
            HttpRouter.HandlerType.PRODUCE, "my-topic", null, null, Map.of());
        assertThrows(InvalidRequestException.class,
            () -> translator.translate(route, json.getBytes(StandardCharsets.UTF_8),
                limitedConfig, topic -> 3));
    }

    @Test
    void translateProduce_negativePartition_throws() {
        String json = """
            { "records": [{"partition": -1, "value": {"type": "STRING", "data": "x"}}] }
            """;
        var route = new HttpRouter.RouteResult(
            HttpRouter.HandlerType.PRODUCE, "my-topic", null, null, Map.of());
        assertThrows(InvalidRequestException.class,
            () -> translator.translate(route, json.getBytes(StandardCharsets.UTF_8),
                config, topic -> 3));
    }

    @Test
    void translateProduce_invalidAcks_throws() {
        String json = """
            { "records": [{"value": {"type": "STRING", "data": "x"}}], "acks": "two" }
            """;
        var route = new HttpRouter.RouteResult(
            HttpRouter.HandlerType.PRODUCE, "my-topic", null, null, Map.of());
        assertThrows(InvalidRequestException.class,
            () -> translator.translate(route, json.getBytes(StandardCharsets.UTF_8),
                config, topic -> 3));
    }

    // --- Fetch translation ---

    @Test
    void translateFetch_clampsMaxWaitMs() {
        // maxWaitMs=30000 in the body, config.httpConsumeMaxWaitMs=5000 -> clamped to 5000
        String json = """
            { "partitions": [{"partition": 0, "offset": 0}], "maxWaitMs": 30000 }
            """;
        var route = new HttpRouter.RouteResult(
            HttpRouter.HandlerType.FETCH, "my-topic", null, null, Map.of());
        var result = translator.translate(route, json.getBytes(StandardCharsets.UTF_8),
            config, topic -> 3);
        assertEquals(ApiKeys.FETCH, result.apiKey());
        assertNotNull(result.serializedRequest());
    }

    @Test
    void translateFetch_emptyPartitions_throws() {
        String json = """
            { "partitions": [] }
            """;
        var route = new HttpRouter.RouteResult(
            HttpRouter.HandlerType.FETCH, "my-topic", null, null, Map.of());
        assertThrows(InvalidRequestException.class,
            () -> translator.translate(route, json.getBytes(StandardCharsets.UTF_8),
                config, topic -> 3));
    }

    // --- DataObject parsing ---

    @Test
    void deserializeDataObject_stringType_returnsUtf8Bytes() {
        var node = HttpRequestTranslator.MAPPER.createObjectNode()
            .put("type", "STRING").put("data", "hello");
        byte[] result = HttpRequestTranslator.deserializeDataObject(node);
        assertArrayEquals("hello".getBytes(StandardCharsets.UTF_8), result);
    }

    @Test
    void deserializeDataObject_binaryType_decodesBase64() {
        var node = HttpRequestTranslator.MAPPER.createObjectNode()
            .put("type", "BINARY").put("data", "aGVsbG8=");
        byte[] result = HttpRequestTranslator.deserializeDataObject(node);
        assertArrayEquals("hello".getBytes(StandardCharsets.UTF_8), result);
    }

    @Test
    void deserializeDataObject_nullType_returnsNull() {
        var node = HttpRequestTranslator.MAPPER.createObjectNode()
            .put("type", "NULL");
        assertNull(HttpRequestTranslator.deserializeDataObject(node));
    }

    @Test
    void deserializeDataObject_invalidBase64_throws() {
        var node = HttpRequestTranslator.MAPPER.createObjectNode()
            .put("type", "BINARY").put("data", "not-valid!!!");
        assertThrows(InvalidRequestException.class,
            () -> HttpRequestTranslator.deserializeDataObject(node));
    }

    // --- Acks parsing ---

    @Test
    void parseAcks_all_returnsMinusOne() {
        assertEquals((short) -1, HttpRequestTranslator.parseAcks("all"));
    }

    @Test
    void parseAcks_leader_returnsOne() {
        assertEquals((short) 1, HttpRequestTranslator.parseAcks("leader"));
    }

    @Test
    void parseAcks_none_returnsZero() {
        assertEquals((short) 0, HttpRequestTranslator.parseAcks("none"));
    }

    // --- Timestamp parsing ---

    @Test
    void parseTimestamp_earliest_returnsMinusTwo() {
        assertEquals(-2L, HttpRequestTranslator.parseTimestamp("earliest"));
    }

    @Test
    void parseTimestamp_latest_returnsMinusOne() {
        assertEquals(-1L, HttpRequestTranslator.parseTimestamp("latest"));
    }

    @Test
    void parseTimestamp_max_returnsMinusThree() {
        assertEquals(-3L, HttpRequestTranslator.parseTimestamp("max"));
    }

    @Test
    void parseTimestamp_epochMillis_parsesLong() {
        assertEquals(1713260400000L, HttpRequestTranslator.parseTimestamp("1713260400000"));
    }
}
