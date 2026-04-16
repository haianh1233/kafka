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
 * Placeholder processor for translating HTTP requests into Kafka requests.
 * Full implementation provided by TASK-B.05.
 *
 * // Time: Created - TASK-A.02 (stub for TASK-B.03)
 */
public class HttpProcessor {

    private final int id;

    public HttpProcessor(int id) {
        this.id = id;
    }

    public int id() {
        return id;
    }
}
