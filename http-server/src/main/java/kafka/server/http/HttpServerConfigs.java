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

/**
 * Runtime configuration holder for the HTTP server layer.
 * Wraps resolved configuration values that are used by the translator and handler components.
 * Defaults come from {@link org.apache.kafka.network.HttpServerConfigs}.
 */
public final class HttpServerConfigs {

    private final int httpProduceMaxRecords;
    private final int httpConsumeMaxWaitMs;
    private final int httpRequestMaxBytes;

    public HttpServerConfigs(int httpProduceMaxRecords, int httpConsumeMaxWaitMs, int httpRequestMaxBytes) {
        this.httpProduceMaxRecords = httpProduceMaxRecords;
        this.httpConsumeMaxWaitMs = httpConsumeMaxWaitMs;
        this.httpRequestMaxBytes = httpRequestMaxBytes;
    }

    /**
     * Creates an instance with default values from the network config constants.
     */
    public static HttpServerConfigs withDefaults() {
        return new HttpServerConfigs(
            org.apache.kafka.network.HttpServerConfigs.HTTP_PRODUCE_MAX_RECORDS_DEFAULT,
            org.apache.kafka.network.HttpServerConfigs.HTTP_CONSUME_MAX_WAIT_MS_DEFAULT,
            org.apache.kafka.network.HttpServerConfigs.HTTP_REQUEST_MAX_BYTES_DEFAULT
        );
    }

    public int httpProduceMaxRecords() {
        return httpProduceMaxRecords;
    }

    public int httpConsumeMaxWaitMs() {
        return httpConsumeMaxWaitMs;
    }

    public int httpRequestMaxBytes() {
        return httpRequestMaxBytes;
    }
}
