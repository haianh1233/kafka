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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.message.ListOffsetsRequestData;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetFetchRequestData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.OffsetFetchRequest;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.utils.Utils;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * Translates JSON HTTP request bodies into serialized Kafka wire-protocol requests.
 *
 * All input validation runs here -- invalid requests throw exceptions that the caller
 * maps to HTTP 400/422 before anything touches RequestChannel.
 *
 * Jackson ObjectMapper is configured with StreamReadConstraints to prevent abuse:
 *   - maxNestingDepth = 20
 *   - maxStringLength = 1 MB
 *   - maxNumberLength = 100
 */
public final class HttpRequestTranslator {

    // --- Jackson ObjectMapper (shared, thread-safe) ---
    public static final ObjectMapper MAPPER;
    static {
        JsonFactory factory = JsonFactory.builder()
            .streamReadConstraints(StreamReadConstraints.builder()
                .maxNestingDepth(20)
                .maxStringLength(1_048_576)   // 1 MB max per string field
                .maxNumberLength(100)
                .build())
            .build();
        MAPPER = new ObjectMapper(factory)
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    // --- Data type constants ---
    static final String TYPE_STRING = "STRING";
    static final String TYPE_BINARY = "BINARY";
    static final String TYPE_JSON = "JSON";
    static final String TYPE_NULL = "NULL";

    // --- ACK mapping ---
    static final short ACKS_NONE = 0;
    static final short ACKS_LEADER = 1;
    static final short ACKS_ALL = -1;

    // --- ListOffsets timestamp constants ---
    static final long EARLIEST_TIMESTAMP = -2L;
    static final long LATEST_TIMESTAMP = -1L;
    static final long MAX_TIMESTAMP = -3L;

    // --- Default produce settings ---
    static final int DEFAULT_PRODUCE_TIMEOUT_MS = 30_000;
    static final String DEFAULT_ACKS = "all";

    // --- Default fetch settings ---
    static final int DEFAULT_FETCH_MAX_WAIT_MS = 500;
    static final int DEFAULT_FETCH_MIN_BYTES = 1;
    static final int DEFAULT_FETCH_MAX_BYTES = 10_485_760;       // 10 MB
    static final int DEFAULT_FETCH_MAX_BYTES_PER_PARTITION = 1_048_576;  // 1 MB

    // --- Batch-sticky partition counter ---
    private final AtomicInteger roundRobinCounter = new AtomicInteger(0);

    /**
     * Result of translating an HTTP request to a Kafka wire-format request.
     */
    public static final class TranslationResult {
        private final ApiKeys apiKey;
        private final short apiVersion;
        private final ByteBuffer serializedRequest;

        public TranslationResult(ApiKeys apiKey, short apiVersion, ByteBuffer serializedRequest) {
            this.apiKey = apiKey;
            this.apiVersion = apiVersion;
            this.serializedRequest = serializedRequest;
        }

        public ApiKeys apiKey() {
            return apiKey;
        }

        public short apiVersion() {
            return apiVersion;
        }

        public ByteBuffer serializedRequest() {
            return serializedRequest;
        }
    }

    /**
     * Translates an HTTP request body + route result into a Kafka wire-format request.
     *
     * @param routeResult      from HttpRouter.route()
     * @param body             raw JSON body bytes (may be empty/null for GET routes)
     * @param config           server config for limits (httpProduceMaxRecords, httpConsumeMaxWaitMs)
     * @param metadataSupplier given a topic name, returns partition count (or throws)
     * @return TranslationResult with apiKey, version, and serialized ByteBuffer
     * @throws InvalidRequestException on any validation failure
     */
    public TranslationResult translate(
            HttpRouter.RouteResult routeResult,
            byte[] body,
            HttpServerConfigs config,
            Function<String, Integer> metadataSupplier) {
        Objects.requireNonNull(routeResult, "routeResult");

        return switch (routeResult.handlerType()) {
            case PRODUCE -> translateProduce(routeResult, body, config, metadataSupplier);
            case FETCH -> translateFetch(routeResult, body, config);
            case METADATA_TOPIC -> translateMetadataTopic(routeResult);
            case METADATA_ALL -> translateMetadataAll();
            case LIST_OFFSETS -> translateListOffsets(routeResult);
            case CONSUMER_LAG -> translateConsumerLag(routeResult);
            case COMMIT_OFFSETS, FETCH_OFFSETS -> throw new InvalidRequestException(
                "Offset commit/fetch requests use dedicated translation methods, not the generic translate()");
            case SHARE_POLL, SHARE_ACKNOWLEDGE -> throw new InvalidRequestException(
                "Share group requests use dedicated translation methods, not the generic translate()");
            case HEALTH -> throw new InvalidRequestException(
                "HEALTH requests should be handled directly, not translated");
            case OPENAPI_SPEC -> throw new InvalidRequestException(
                "OPENAPI_SPEC requests should be handled directly, not translated");
            // WS2.06 REST routing handlers — dispatched by HttpRequestHandler
            // directly; never reach the generic translator.
            case DECLARE_EXCHANGE, GET_EXCHANGE, LIST_EXCHANGES, DELETE_EXCHANGE,
                 DECLARE_QUEUE, GET_QUEUE, LIST_QUEUES, PATCH_QUEUE, DELETE_QUEUE,
                 PURGE_QUEUE, CREATE_BINDING, LIST_BINDINGS, DELETE_BINDING ->
                throw new InvalidRequestException(
                    "REST routing requests are handled directly, not translated");
        };
    }

    /**
     * Translates a produce request. Parses records array, assigns partitions
     * (murmur2 for keyed, batch-sticky for keyless), builds ProduceRequest.
     */
    TranslationResult translateProduce(
            HttpRouter.RouteResult routeResult,
            byte[] body,
            HttpServerConfigs config,
            Function<String, Integer> metadataSupplier) {

        JsonNode root = parseAndValidateBody(body, "produce");
        ArrayNode records = extractRecordsArray(root, config);

        // Extract acks (default "all") and timeoutMs (default 30000)
        String acksStr = root.has("acks") ? root.get("acks").asText() : DEFAULT_ACKS;
        short acks = parseAcks(acksStr);
        int timeoutMs = extractTimeoutMs(root);

        // Get partition count and compute batch-sticky partition
        String topicName = routeResult.topicName();
        int partitionCount = metadataSupplier.apply(topicName);
        int batchStickyPartition = (roundRobinCounter.getAndIncrement() & 0x7fffffff) % partitionCount;

        // Parse records and group by partition
        Map<Integer, List<SimpleRecord>> recordsByPartition = groupRecordsByPartition(
            records, partitionCount, batchStickyPartition);

        // Build and serialize ProduceRequest
        return buildProduceRequest(topicName, acks, timeoutMs, recordsByPartition);
    }

    /**
     * Translates a fetch request. Parses partitions array, clamps maxWaitMs.
     */
    TranslationResult translateFetch(
            HttpRouter.RouteResult routeResult,
            byte[] body,
            HttpServerConfigs config) {

        JsonNode root = parseAndValidateBody(body, "fetch");
        ArrayNode partitions = extractPartitionsArray(root);

        // Extract and clamp settings
        int maxWaitMs = extractClampedMaxWaitMs(root, config);
        int minBytes = getIntOrDefault(root, "minBytes", DEFAULT_FETCH_MIN_BYTES);
        int maxBytes = getIntOrDefault(root, "maxBytes", DEFAULT_FETCH_MAX_BYTES);
        int maxBytesPerPartition = getIntOrDefault(root, "maxBytesPerPartition", DEFAULT_FETCH_MAX_BYTES_PER_PARTITION);

        // Build fetch data from partitions array
        String topicName = routeResult.topicName();
        Map<TopicPartition, FetchRequest.PartitionData> fetchData =
            buildFetchPartitionData(partitions, topicName, maxBytesPerPartition);

        // Build and serialize FetchRequest
        // Use version 12 (not 13+) because version 13 (KIP-516) replaces topic names with
        // topic IDs. HTTP requests use topic names, so we need a version that supports names.
        short version = (short) Math.min(12, ApiKeys.FETCH.latestVersion());
        FetchRequest.Builder builder = FetchRequest.Builder.forConsumer(
            version, maxWaitMs, minBytes, fetchData);
        builder.setMaxBytes(maxBytes);
        FetchRequest request = builder.build(version);
        ByteBuffer buffer = request.serialize().buffer();

        return new TranslationResult(ApiKeys.FETCH, version, buffer);
    }

    // --- Produce helper methods ---

    private static ArrayNode extractRecordsArray(JsonNode root, HttpServerConfigs config) {
        JsonNode recordsNode = root.get("records");
        if (recordsNode == null || !recordsNode.isArray()) {
            throw new InvalidRequestException("'records' array is required in produce request body");
        }
        ArrayNode records = (ArrayNode) recordsNode;
        if (records.isEmpty()) {
            throw new InvalidRequestException("'records' array must not be empty");
        }
        if (records.size() > config.httpProduceMaxRecords()) {
            throw new InvalidRequestException(
                "Too many records: " + records.size() + " exceeds maximum " + config.httpProduceMaxRecords());
        }
        return records;
    }

    private static int extractTimeoutMs(JsonNode root) {
        int timeoutMs = root.has("timeoutMs") ? root.get("timeoutMs").asInt() : DEFAULT_PRODUCE_TIMEOUT_MS;
        if (timeoutMs <= 0) {
            throw new InvalidRequestException("timeoutMs must be > 0, got: " + timeoutMs);
        }
        return timeoutMs;
    }

    private Map<Integer, List<SimpleRecord>> groupRecordsByPartition(
            ArrayNode records, int partitionCount, int batchStickyPartition) {
        Map<Integer, List<SimpleRecord>> recordsByPartition = new HashMap<>();
        for (int i = 0; i < records.size(); i++) {
            JsonNode recordNode = records.get(i);
            byte[] keyBytes = extractDataObjectField(recordNode, "key");
            byte[] valueBytes = extractDataObjectField(recordNode, "value");
            Header[] headers = parseHeaders(recordNode);

            int partition = assignPartition(recordNode, keyBytes, partitionCount, batchStickyPartition, i);
            long timestamp = extractTimestamp(recordNode);

            SimpleRecord simpleRecord = new SimpleRecord(timestamp, keyBytes, valueBytes, headers);
            recordsByPartition.computeIfAbsent(partition, k -> new ArrayList<>()).add(simpleRecord);
        }
        return recordsByPartition;
    }

    private static byte[] extractDataObjectField(JsonNode recordNode, String fieldName) {
        if (recordNode.has(fieldName) && !recordNode.get(fieldName).isNull()) {
            return deserializeDataObject(recordNode.get(fieldName));
        }
        return null;
    }

    private static int assignPartition(
            JsonNode recordNode, byte[] keyBytes, int partitionCount, int batchStickyPartition, int index) {
        if (recordNode.has("partition") && !recordNode.get("partition").isNull()) {
            int partition = recordNode.get("partition").asInt();
            if (partition < 0) {
                throw new InvalidRequestException(
                    "partition must be >= 0, got: " + partition + " at record index " + index);
            }
            return partition;
        } else if (keyBytes != null) {
            return (Utils.murmur2(keyBytes) & 0x7fffffff) % partitionCount;
        } else {
            return batchStickyPartition;
        }
    }

    private static long extractTimestamp(JsonNode recordNode) {
        if (recordNode.has("timestamp") && !recordNode.get("timestamp").isNull()) {
            return recordNode.get("timestamp").asLong();
        }
        return RecordBatch.NO_TIMESTAMP;
    }

    private static TranslationResult buildProduceRequest(
            String topicName, short acks, int timeoutMs,
            Map<Integer, List<SimpleRecord>> recordsByPartition) {
        ProduceRequestData data = new ProduceRequestData()
            .setAcks(acks)
            .setTimeoutMs(timeoutMs);

        ProduceRequestData.TopicProduceData topicData = new ProduceRequestData.TopicProduceData()
            .setName(topicName);

        List<ProduceRequestData.PartitionProduceData> partitionDataList = new ArrayList<>();
        for (Map.Entry<Integer, List<SimpleRecord>> entry : recordsByPartition.entrySet()) {
            MemoryRecords memoryRecords = MemoryRecords.withRecords(
                Compression.NONE,
                entry.getValue().toArray(new SimpleRecord[0])
            );
            partitionDataList.add(new ProduceRequestData.PartitionProduceData()
                .setIndex(entry.getKey())
                .setRecords(memoryRecords));
        }
        topicData.setPartitionData(partitionDataList);
        data.setTopicData(new ProduceRequestData.TopicProduceDataCollection(
            Collections.singletonList(topicData).iterator()));

        // Use version 9 because later versions may have features the HTTP layer
        // doesn't fully support (e.g., version 13 replaces topic names with topic IDs).
        // Version 9 is the first flexible version with full topic name support.
        short version = 9;
        ProduceRequest request = ProduceRequest.builder(data).build(version);
        ByteBuffer buffer = request.serialize().buffer();

        return new TranslationResult(ApiKeys.PRODUCE, version, buffer);
    }

    // --- Fetch helper methods ---

    private static ArrayNode extractPartitionsArray(JsonNode root) {
        JsonNode partitionsNode = root.get("partitions");
        if (partitionsNode == null || !partitionsNode.isArray()) {
            throw new InvalidRequestException("'partitions' array is required in fetch request body");
        }
        ArrayNode partitions = (ArrayNode) partitionsNode;
        if (partitions.isEmpty()) {
            throw new InvalidRequestException("'partitions' array must not be empty");
        }
        return partitions;
    }

    private static int extractClampedMaxWaitMs(JsonNode root, HttpServerConfigs config) {
        int maxWaitMs = root.has("maxWaitMs") ? root.get("maxWaitMs").asInt() : DEFAULT_FETCH_MAX_WAIT_MS;
        if (maxWaitMs < 0) {
            throw new InvalidRequestException("maxWaitMs must be >= 0, got: " + maxWaitMs);
        }
        return Math.min(maxWaitMs, config.httpConsumeMaxWaitMs());
    }

    private static int getIntOrDefault(JsonNode root, String field, int defaultValue) {
        return root.has(field) ? root.get(field).asInt() : defaultValue;
    }

    private static Map<TopicPartition, FetchRequest.PartitionData> buildFetchPartitionData(
            ArrayNode partitions, String topicName, int maxBytesPerPartition) {
        Map<TopicPartition, FetchRequest.PartitionData> fetchData = new HashMap<>();
        for (int i = 0; i < partitions.size(); i++) {
            JsonNode partNode = partitions.get(i);
            validateFetchPartitionEntry(partNode, i);
            int partition = partNode.get("partition").asInt();
            long offset = partNode.get("offset").asLong();

            TopicPartition tp = new TopicPartition(topicName, partition);
            fetchData.put(tp, new FetchRequest.PartitionData(
                Uuid.ZERO_UUID, offset, FetchRequest.INVALID_LOG_START_OFFSET,
                maxBytesPerPartition, Optional.empty()
            ));
        }
        return fetchData;
    }

    private static void validateFetchPartitionEntry(JsonNode partNode, int index) {
        if (!partNode.has("partition")) {
            throw new InvalidRequestException(
                "'partition' field is required in each partition entry at index " + index);
        }
        int partition = partNode.get("partition").asInt();
        if (partition < 0) {
            throw new InvalidRequestException(
                "partition must be >= 0, got: " + partition + " at index " + index);
        }
        if (!partNode.has("offset")) {
            throw new InvalidRequestException(
                "'offset' field is required in each partition entry at index " + index);
        }
        long offset = partNode.get("offset").asLong();
        if (offset < 0) {
            throw new InvalidRequestException(
                "offset must be >= 0, got: " + offset + " at index " + index);
        }
    }

    // --- Common helpers ---

    private static JsonNode parseAndValidateBody(byte[] body, String requestType) {
        if (body == null || body.length == 0) {
            throw new InvalidRequestException("Request body is required for " + requestType + " requests");
        }
        try {
            return MAPPER.readTree(body);
        } catch (Exception e) {
            throw new InvalidRequestException("Invalid JSON in request body: " + e.getMessage());
        }
    }

    /**
     * Translates a single-topic metadata request from GET /v1/topics/{topic}.
     */
    TranslationResult translateMetadataTopic(HttpRouter.RouteResult routeResult) {
        MetadataRequestData data = new MetadataRequestData();
        data.topics().add(new MetadataRequestData.MetadataRequestTopic()
            .setName(routeResult.topicName()));
        data.setAllowAutoTopicCreation(false);

        short version = ApiKeys.METADATA.latestVersion();
        var request = new org.apache.kafka.common.requests.MetadataRequest(data, version);
        ByteBuffer buffer = request.serialize().buffer();

        return new TranslationResult(ApiKeys.METADATA, version, buffer);
    }

    /**
     * Translates an all-topics metadata request from GET /v1/topics.
     */
    TranslationResult translateMetadataAll() {
        MetadataRequestData data = new MetadataRequestData()
            .setTopics(null)
            .setAllowAutoTopicCreation(false);

        short version = ApiKeys.METADATA.latestVersion();
        var request = new org.apache.kafka.common.requests.MetadataRequest(data, version);
        ByteBuffer buffer = request.serialize().buffer();

        return new TranslationResult(ApiKeys.METADATA, version, buffer);
    }

    /**
     * Translates a list-offsets request from GET /v1/topics/{t}/partitions/{p}/offsets.
     *
     * Query param "timestamp" maps to:
     *   "earliest" -> -2 (EARLIEST_TIMESTAMP)
     *   "latest"   -> -1 (LATEST_TIMESTAMP)
     *   "max"      -> -3 (MAX_TIMESTAMP)
     *   numeric    -> parsed as long (epoch millis)
     */
    TranslationResult translateListOffsets(HttpRouter.RouteResult routeResult) {
        String topicName = routeResult.topicName();
        int partition = routeResult.partition();

        // Parse timestamp from query params (default to "latest")
        String timestampStr = routeResult.queryParams().getOrDefault("timestamp", "latest");
        long timestamp = parseTimestamp(timestampStr);

        ListOffsetsRequestData.ListOffsetsTopic topic = new ListOffsetsRequestData.ListOffsetsTopic()
            .setName(topicName)
            .setPartitions(Collections.singletonList(
                new ListOffsetsRequestData.ListOffsetsPartition()
                    .setPartitionIndex(partition)
                    .setTimestamp(timestamp)
                    .setCurrentLeaderEpoch(-1)
            ));

        short version = ApiKeys.LIST_OFFSETS.latestVersion();
        ListOffsetsRequest.Builder builder = ListOffsetsRequest.Builder.forConsumer(
            true, IsolationLevel.READ_UNCOMMITTED);
        builder.setTargetTimes(Collections.singletonList(topic));
        ListOffsetsRequest request = builder.build(version);
        ByteBuffer buffer = request.serialize().buffer();

        return new TranslationResult(ApiKeys.LIST_OFFSETS, version, buffer);
    }

    /**
     * Translates a consumer lag request. For phase 1, this builds an OffsetFetchRequest
     * for the consumer group. The lag computation is handled downstream.
     */
    TranslationResult translateConsumerLag(HttpRouter.RouteResult routeResult) {
        // Phase 1: consumer lag is a placeholder -- full implementation requires
        // OFFSET_FETCH + LIST_OFFSETS composition which will come in a later task.
        throw new InvalidRequestException(
            "Consumer lag endpoint is not yet implemented (phase 1)");
    }

    // --- DataObject deserialization helpers ---

    /**
     * Deserializes a DataObject JSON node to byte[].
     *
     * { "type": "STRING", "data": "text" }     -> text.getBytes(UTF_8)
     * { "type": "BINARY", "data": "base64..." } -> Base64.decode(data)
     * { "type": "JSON",   "data": {...} }       -> data.toString().getBytes(UTF_8)
     * { "type": "NULL" }                        -> null
     *
     * @throws InvalidRequestException if type is unknown or BINARY data is invalid base64
     */
    static byte[] deserializeDataObject(JsonNode node) {
        if (node == null || node.isNull()) {
            return null;
        }
        if (!node.has("type")) {
            throw new InvalidRequestException("DataObject must have a 'type' field");
        }
        String type = node.get("type").asText();

        return switch (type) {
            case TYPE_STRING -> {
                if (!node.has("data")) {
                    throw new InvalidRequestException("STRING DataObject must have a 'data' field");
                }
                yield node.get("data").asText().getBytes(StandardCharsets.UTF_8);
            }
            case TYPE_BINARY -> {
                if (!node.has("data")) {
                    throw new InvalidRequestException("BINARY DataObject must have a 'data' field");
                }
                String base64Data = node.get("data").asText();
                try {
                    yield Base64.getDecoder().decode(base64Data);
                } catch (IllegalArgumentException e) {
                    throw new InvalidRequestException(
                        "Invalid base64 in BINARY DataObject: " + e.getMessage());
                }
            }
            case TYPE_JSON -> {
                if (!node.has("data")) {
                    throw new InvalidRequestException("JSON DataObject must have a 'data' field");
                }
                yield node.get("data").toString().getBytes(StandardCharsets.UTF_8);
            }
            case TYPE_NULL -> null;
            default -> throw new InvalidRequestException("Unknown DataObject type: '" + type + "'");
        };
    }

    /**
     * Maps acks string to Kafka wire-protocol short value.
     * "none" -> 0, "leader" -> 1, "all" -> -1
     *
     * @throws InvalidRequestException if acks is not one of the three valid values
     */
    static short parseAcks(String acks) {
        return switch (acks) {
            case "none" -> ACKS_NONE;
            case "leader" -> ACKS_LEADER;
            case "all" -> ACKS_ALL;
            default -> throw new InvalidRequestException(
                "Invalid acks value: '" + acks + "'; must be 'all', 'leader', or 'none'");
        };
    }

    /**
     * Maps timestamp query param to ListOffsets constant.
     * "earliest" -> -2, "latest" -> -1, "max" -> -3, numeric -> parsed long
     *
     * @throws InvalidRequestException if value is not recognized and not a valid long
     */
    static long parseTimestamp(String value) {
        return switch (value) {
            case "earliest" -> EARLIEST_TIMESTAMP;
            case "latest" -> LATEST_TIMESTAMP;
            case "max" -> MAX_TIMESTAMP;
            default -> {
                try {
                    yield Long.parseLong(value);
                } catch (NumberFormatException e) {
                    throw new InvalidRequestException(
                        "Invalid timestamp value: '" + value + "'; must be 'earliest', 'latest', 'max', or a numeric epoch millis");
                }
            }
        };
    }

    /**
     * Parses headers from a record JSON node.
     */
    private Header[] parseHeaders(JsonNode recordNode) {
        if (!recordNode.has("headers") || recordNode.get("headers").isNull()) {
            return new Header[0];
        }
        JsonNode headersNode = recordNode.get("headers");
        if (!headersNode.isArray()) {
            throw new InvalidRequestException("'headers' must be an array");
        }
        List<Header> headers = new ArrayList<>();
        for (JsonNode headerNode : headersNode) {
            if (!headerNode.has("key")) {
                throw new InvalidRequestException("Each header must have a 'key' field");
            }
            String key = headerNode.get("key").asText();
            byte[] value = null;
            if (headerNode.has("value") && !headerNode.get("value").isNull()) {
                value = deserializeDataObject(headerNode.get("value"));
            }
            headers.add(new RecordHeader(key, value));
        }
        return headers.toArray(new Header[0]);
    }

    // --- Consumer Group Offset Commit/Fetch (TASK-F.01) ---

    /**
     * Result of translating an offset commit HTTP request.
     */
    public static final class OffsetCommitTranslationResult {
        private final ApiKeys apiKey;
        private final OffsetCommitRequest.Builder builder;

        public OffsetCommitTranslationResult(ApiKeys apiKey, OffsetCommitRequest.Builder builder) {
            this.apiKey = apiKey;
            this.builder = builder;
        }

        public ApiKeys apiKey() {
            return apiKey;
        }

        public OffsetCommitRequest.Builder builder() {
            return builder;
        }
    }

    /**
     * Result of translating an offset fetch HTTP request.
     */
    public static final class OffsetFetchTranslationResult {
        private final ApiKeys apiKey;
        private final OffsetFetchRequest.Builder builder;

        public OffsetFetchTranslationResult(ApiKeys apiKey, OffsetFetchRequest.Builder builder) {
            this.apiKey = apiKey;
            this.builder = builder;
        }

        public ApiKeys apiKey() {
            return apiKey;
        }

        public OffsetFetchRequest.Builder builder() {
            return builder;
        }
    }

    /**
     * Translate JSON body into an OffsetCommitRequest builder.
     *
     * Uses generationId=-1 and memberId="" (simple consumer mode).
     * The group does not need to have active members for this to work.
     *
     * @param group consumer group ID from URL path
     * @param body  parsed JSON body
     * @return OffsetCommitTranslationResult with apiKey and builder
     * @throws InvalidRequestException if validation fails
     */
    public static OffsetCommitTranslationResult translateCommitOffsets(String group, JsonNode body) {
        JsonNode offsetsNode = body.get("offsets");
        if (offsetsNode == null || !offsetsNode.isArray() || offsetsNode.isEmpty()) {
            throw new InvalidRequestException("'offsets' array is required and must not be empty");
        }

        // Group offsets by topic using LinkedHashMap to preserve insertion order
        LinkedHashMap<String, List<OffsetCommitRequestData.OffsetCommitRequestPartition>> byTopic =
            new LinkedHashMap<>();

        for (JsonNode entry : offsetsNode) {
            String topic = requireStringField(entry, "topic");
            int partition = requireIntField(entry, "partition");
            long offset = requireLongField(entry, "offset");
            String metadata = optionalStringField(entry, "metadata", "");

            if (partition < 0) {
                throw new InvalidRequestException("Partition must be non-negative, got " + partition);
            }
            if (offset < 0) {
                throw new InvalidRequestException("Offset must be non-negative, got " + offset);
            }

            OffsetCommitRequestData.OffsetCommitRequestPartition partitionData =
                new OffsetCommitRequestData.OffsetCommitRequestPartition()
                    .setPartitionIndex(partition)
                    .setCommittedOffset(offset)
                    .setCommittedMetadata(metadata);

            byTopic.computeIfAbsent(topic, k -> new ArrayList<>()).add(partitionData);
        }

        List<OffsetCommitRequestData.OffsetCommitRequestTopic> topics = new ArrayList<>();
        byTopic.forEach((topicName, partitions) ->
            topics.add(new OffsetCommitRequestData.OffsetCommitRequestTopic()
                .setName(topicName)
                .setPartitions(partitions)));

        OffsetCommitRequestData data = new OffsetCommitRequestData()
            .setGroupId(group)
            .setMemberId("")
            .setGenerationIdOrMemberEpoch(-1)
            .setTopics(topics);

        return new OffsetCommitTranslationResult(
            ApiKeys.OFFSET_COMMIT,
            OffsetCommitRequest.Builder.forTopicNames(data));
    }

    /**
     * Translate into an OffsetFetchRequest builder.
     *
     * @param group       consumer group ID from URL path
     * @param topicFilter optional topic name filter from query string (null for all topics)
     * @return OffsetFetchTranslationResult with apiKey and builder
     */
    public static OffsetFetchTranslationResult translateFetchOffsets(String group, String topicFilter) {
        OffsetFetchRequestData.OffsetFetchRequestGroup groupData =
            new OffsetFetchRequestData.OffsetFetchRequestGroup()
                .setGroupId(group);

        if (topicFilter != null && !topicFilter.isEmpty()) {
            // Fetch offsets for a specific topic (all partitions)
            OffsetFetchRequestData.OffsetFetchRequestTopics topicData =
                new OffsetFetchRequestData.OffsetFetchRequestTopics()
                    .setName(topicFilter)
                    .setPartitionIndexes(Collections.emptyList());
            groupData.setTopics(Collections.singletonList(topicData));
        } else {
            // Fetch offsets for all topics in the group (null = all topics)
            groupData.setTopics(null);
        }

        OffsetFetchRequestData data = new OffsetFetchRequestData()
            .setGroups(Collections.singletonList(groupData));

        return new OffsetFetchTranslationResult(
            ApiKeys.OFFSET_FETCH,
            OffsetFetchRequest.Builder.forTopicNames(data, false));
    }

    // --- JSON field extraction helpers for offset requests ---

    private static String requireStringField(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || value.isNull()) {
            throw new InvalidRequestException("Required field '" + field + "' is missing");
        }
        return value.asText();
    }

    private static int requireIntField(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || value.isNull()) {
            throw new InvalidRequestException("Required field '" + field + "' is missing");
        }
        if (!value.isNumber()) {
            throw new InvalidRequestException("Field '" + field + "' must be an integer");
        }
        return value.asInt();
    }

    private static long requireLongField(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || value.isNull()) {
            throw new InvalidRequestException("Required field '" + field + "' is missing");
        }
        if (!value.isNumber()) {
            throw new InvalidRequestException("Field '" + field + "' must be a number");
        }
        return value.asLong();
    }

    private static String optionalStringField(JsonNode node, String field, String defaultValue) {
        JsonNode value = node.get(field);
        if (value == null || value.isNull()) {
            return defaultValue;
        }
        return value.asText();
    }
}
