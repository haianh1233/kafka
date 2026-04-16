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
package org.apache.kafka.server.network;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.requests.ProduceResponse;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Interface for forwarding produce requests to remote leader brokers.
 *
 * <p>This interface lives in server-common so that {@code KafkaApis} (in core)
 * can reference the forwarder without depending on the http-server module.
 * The concrete implementation ({@code ProduceForwardManager}) lives in http-server.</p>
 *
 * @see org.apache.kafka.server.network.FetchForwarder
 * // Time: Created - TASK-D.04
 */
public interface ProduceForwarder {

    /**
     * Forward produce request to the leader broker for the given partitions.
     *
     * @param leaderId     the leader broker ID
     * @param entries      per-partition records to produce
     * @param requiredAcks required acknowledgments (0, 1, or -1)
     * @param timeoutMs    produce timeout in milliseconds
     * @return future that completes with per-partition produce responses
     */
    CompletableFuture<Map<TopicIdPartition, ProduceResponse.PartitionResponse>> forward(
            int leaderId,
            Map<TopicIdPartition, MemoryRecords> entries,
            short requiredAcks,
            int timeoutMs);
}
