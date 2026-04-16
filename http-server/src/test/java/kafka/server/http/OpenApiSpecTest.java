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
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Validates the OpenAPI 3.0 specification file is present, well-formed,
 * and covers all endpoints and schemas required by the HTTP protocol design.
 */
class OpenApiSpecTest {

    private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());

    @Test
    void testSpecExistsOnClasspath() throws Exception {
        try (InputStream stream = getClass().getClassLoader().getResourceAsStream("openapi.yaml")) {
            assertNotNull(stream, "openapi.yaml must exist on classpath");
        }
    }

    @Test
    void testSpecIsValidYaml() throws Exception {
        try (InputStream stream = getClass().getClassLoader().getResourceAsStream("openapi.yaml")) {
            JsonNode tree = YAML_MAPPER.readTree(stream);
            assertNotNull(tree);
        }
    }

    @Test
    void testSpecVersion() throws Exception {
        try (InputStream stream = getClass().getClassLoader().getResourceAsStream("openapi.yaml")) {
            JsonNode tree = YAML_MAPPER.readTree(stream);
            assertEquals("3.0.3", tree.get("openapi").asText());
        }
    }

    @Test
    void testInfoSection() throws Exception {
        try (InputStream stream = getClass().getClassLoader().getResourceAsStream("openapi.yaml")) {
            JsonNode tree = YAML_MAPPER.readTree(stream);
            JsonNode info = tree.get("info");
            assertNotNull(info, "info section must be present");
            assertNotNull(info.get("title"), "info.title must be present");
            assertNotNull(info.get("version"), "info.version must be present");
            assertNotNull(info.get("description"), "info.description must be present");
        }
    }

    @Test
    void testAllEndpointsPresent() throws Exception {
        try (InputStream stream = getClass().getClassLoader().getResourceAsStream("openapi.yaml")) {
            JsonNode tree = YAML_MAPPER.readTree(stream);
            JsonNode paths = tree.get("paths");
            assertNotNull(paths);

            // Phase 1 endpoints
            assertTrue(paths.has("/health"), "Missing /health");
            assertTrue(paths.has("/topics/{topic}/records"), "Missing /topics/{topic}/records");
            assertTrue(paths.has("/topics/{topic}/records:fetch"), "Missing /topics/{topic}/records:fetch");

            // Phase 2 endpoints
            assertTrue(paths.has("/topics"), "Missing /topics");
            assertTrue(paths.has("/topics/{topic}"), "Missing /topics/{topic}");
            assertTrue(paths.has("/topics/{topic}/partitions/{partition}/offsets"),
                "Missing offsets endpoint");
            assertTrue(paths.has("/consumer-groups/{group}/lags"), "Missing lag endpoint");

            // Phase 3 endpoints
            assertTrue(paths.has("/consumer-groups/{group}/offsets"), "Missing offsets endpoint");

            // Phase 4 endpoints
            assertTrue(paths.has("/share-groups/{group}/records"), "Missing share group poll");
            assertTrue(paths.has("/share-groups/{group}/acknowledge"), "Missing share group ack");
        }
    }

    @Test
    void testAllSchemasPresent() throws Exception {
        try (InputStream stream = getClass().getClassLoader().getResourceAsStream("openapi.yaml")) {
            JsonNode tree = YAML_MAPPER.readTree(stream);
            JsonNode schemas = tree.get("components").get("schemas");

            String[] requiredSchemas = {
                "ErrorResponse", "HealthStatus", "DataObject", "TopicList", "TopicMetadata",
                "ProduceRequest", "ProduceRecord", "ProduceResponse",
                "FetchRequest", "FetchResponse", "OffsetInfo",
                "ConsumerGroupLag", "OffsetCommitRequest", "OffsetCommitResponse",
                "OffsetFetchResponse", "ShareFetchRequest", "ShareFetchResponse",
                "ShareAcknowledgeRequest", "ShareAcknowledgeResponse"
            };

            for (String name : requiredSchemas) {
                assertTrue(schemas.has(name), "Missing schema: " + name);
            }
        }
    }

    @Test
    void testProduceEndpointHasPostMethod() throws Exception {
        try (InputStream stream = getClass().getClassLoader().getResourceAsStream("openapi.yaml")) {
            JsonNode tree = YAML_MAPPER.readTree(stream);
            JsonNode producePath = tree.get("paths").get("/topics/{topic}/records");
            assertTrue(producePath.has("post"), "Produce endpoint must have POST method");
        }
    }

    @Test
    void testFetchEndpointHasPostMethod() throws Exception {
        try (InputStream stream = getClass().getClassLoader().getResourceAsStream("openapi.yaml")) {
            JsonNode tree = YAML_MAPPER.readTree(stream);
            JsonNode fetchPath = tree.get("paths").get("/topics/{topic}/records:fetch");
            assertTrue(fetchPath.has("post"), "Fetch endpoint must have POST method");
        }
    }

    @Test
    void testHealthEndpointHasGetMethod() throws Exception {
        try (InputStream stream = getClass().getClassLoader().getResourceAsStream("openapi.yaml")) {
            JsonNode tree = YAML_MAPPER.readTree(stream);
            JsonNode healthPath = tree.get("paths").get("/health");
            assertTrue(healthPath.has("get"), "Health endpoint must have GET method");
        }
    }

    @Test
    void testConsumerGroupOffsetsHasBothMethods() throws Exception {
        try (InputStream stream = getClass().getClassLoader().getResourceAsStream("openapi.yaml")) {
            JsonNode tree = YAML_MAPPER.readTree(stream);
            JsonNode offsetsPath = tree.get("paths").get("/consumer-groups/{group}/offsets");
            assertTrue(offsetsPath.has("post"), "Consumer group offsets must have POST (commit)");
            assertTrue(offsetsPath.has("get"), "Consumer group offsets must have GET (fetch)");
        }
    }

    @Test
    void testErrorResponseSchemaHasRequiredFields() throws Exception {
        try (InputStream stream = getClass().getClassLoader().getResourceAsStream("openapi.yaml")) {
            JsonNode tree = YAML_MAPPER.readTree(stream);
            JsonNode errorSchema = tree.get("components").get("schemas").get("ErrorResponse");
            JsonNode required = errorSchema.get("required");
            assertNotNull(required);
            Set<String> requiredFields = new HashSet<>();
            for (int i = 0; i < required.size(); i++) {
                requiredFields.add(required.get(i).asText());
            }
            assertTrue(requiredFields.contains("errorCode"));
            assertTrue(requiredFields.contains("errorMessage"));
        }
    }

    @Test
    void testDataObjectSchemaHasTypeEnum() throws Exception {
        try (InputStream stream = getClass().getClassLoader().getResourceAsStream("openapi.yaml")) {
            JsonNode tree = YAML_MAPPER.readTree(stream);
            JsonNode dataObject = tree.get("components").get("schemas").get("DataObject");
            JsonNode typeEnum = dataObject.get("properties").get("type").get("enum");
            assertNotNull(typeEnum, "DataObject.type must have an enum");

            Set<String> enumValues = new HashSet<>();
            for (int i = 0; i < typeEnum.size(); i++) {
                enumValues.add(typeEnum.get(i).asText());
            }
            assertTrue(enumValues.contains("STRING"));
            assertTrue(enumValues.contains("BINARY"));
            assertTrue(enumValues.contains("JSON"));
            assertTrue(enumValues.contains("NULL"));
        }
    }

    @Test
    void testReusableParametersPresent() throws Exception {
        try (InputStream stream = getClass().getClassLoader().getResourceAsStream("openapi.yaml")) {
            JsonNode tree = YAML_MAPPER.readTree(stream);
            JsonNode parameters = tree.get("components").get("parameters");
            assertNotNull(parameters, "components/parameters must be present");
            assertTrue(parameters.has("TopicName"), "Missing TopicName parameter");
            assertTrue(parameters.has("PartitionId"), "Missing PartitionId parameter");
            assertTrue(parameters.has("GroupId"), "Missing GroupId parameter");
        }
    }

    @Test
    void testReusableResponsesPresent() throws Exception {
        try (InputStream stream = getClass().getClassLoader().getResourceAsStream("openapi.yaml")) {
            JsonNode tree = YAML_MAPPER.readTree(stream);
            JsonNode responses = tree.get("components").get("responses");
            assertNotNull(responses, "components/responses must be present");
            assertTrue(responses.has("BadRequest"), "Missing BadRequest response");
            assertTrue(responses.has("Forbidden"), "Missing Forbidden response");
            assertTrue(responses.has("NotFound"), "Missing NotFound response");
            assertTrue(responses.has("PayloadTooLarge"), "Missing PayloadTooLarge response");
        }
    }

    @Test
    void testServersSection() throws Exception {
        try (InputStream stream = getClass().getClassLoader().getResourceAsStream("openapi.yaml")) {
            JsonNode tree = YAML_MAPPER.readTree(stream);
            JsonNode servers = tree.get("servers");
            assertNotNull(servers, "servers section must be present");
            assertTrue(servers.isArray() && servers.size() > 0, "servers must contain at least one entry");
        }
    }
}
