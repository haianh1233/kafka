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

/**
 * Configuration constants for the HTTP server module.
 *
 * // Time: Created - TASK-A.02 (stub for TASK-B.03)
 */
public class HttpServerConfigs {

    public static final String NUM_HTTP_NETWORK_THREADS_CONFIG = "num.http.network.threads";
    public static final int NUM_HTTP_NETWORK_THREADS_DEFAULT = 4;

    public static final String HTTP_REQUEST_MAX_BYTES_CONFIG = "http.request.max.bytes";
    public static final int HTTP_REQUEST_MAX_BYTES_DEFAULT = 10_485_760; // 10 MB

    public static final String HTTP_CONNECTION_IDLE_TIMEOUT_MS_CONFIG = "http.connection.idle.timeout.ms";
    public static final long HTTP_CONNECTION_IDLE_TIMEOUT_MS_DEFAULT = 60_000L; // 60 seconds

    private HttpServerConfigs() {
        // Utility class — not instantiable
    }
}
