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
// Time: Created - TASK-D.01
package kafka.server.http;

import org.apache.kafka.clients.RequestCompletionHandler;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.server.storage.log.FetchPartitionData;
import org.apache.kafka.server.util.InterBrokerSendThread;
import org.apache.kafka.server.util.RequestAndCompletionHandler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

/**
 * One instance per remote leader broker. Drains a queue of pending fetch
 * entries and forwards them as binary FetchRequests over the inter-broker
 * listener.
 *
 * <p>Key difference from ProduceForwardThread: the {@code maxWaitMs} budget
 * is passed through to the remote broker's FetchRequest so the remote
 * broker's DelayedFetch purgatory operates within the same time budget.</p>
 *
 * @see org.apache.kafka.server.util.InterBrokerSendThread
 * @see ProduceForwardThread
 */
public class FetchForwardThread extends InterBrokerSendThread {

    private final Node destination;
    private final Time time;
    private final BlockingQueue<PendingFetch> pendingQueue;

    /**
     * @param destination      the remote broker node to forward fetch requests to
     * @param networkClient    the network client for this broker (one per thread)
     * @param requestTimeoutMs timeout for individual requests on the wire
     * @param queueCapacity    max pending entries before rejection (config:
     *                         http.internal.forwarding.queue.size, default 10000)
     * @param time             time source
     */
    public FetchForwardThread(
            Node destination,
            KafkaClient networkClient,
            int requestTimeoutMs,
            int queueCapacity,
            Time time) {
        super("FetchForwardThread-" + destination.id(), networkClient, requestTimeoutMs, time);
        this.destination = destination;
        this.time = time;
        this.pendingQueue = new LinkedBlockingQueue<>(queueCapacity);
    }

    /**
     * Returns the destination broker node.
     */
    public Node destination() {
        return destination;
    }

    /**
     * Called by KafkaApis handler thread (via FetchForwardManager).
     * Returns a CompletableFuture that resolves when the remote broker's
     * FetchResponse arrives.
     *
     * @param fetchSpecs  per-partition fetch specifications
     * @param maxWaitMs   effective max wait time (already capped by section 6.1)
     * @param minBytes    minimum bytes before responding
     * @param maxBytes    maximum total bytes across all partitions
     * @return future that completes with per-partition fetch data
     */
    public CompletableFuture<Map<TopicIdPartition, FetchPartitionData>> enqueue(
            Map<TopicIdPartition, FetchRequest.PartitionData> fetchSpecs,
            int maxWaitMs,
            int minBytes,
            int maxBytes) {

        CompletableFuture<Map<TopicIdPartition, FetchPartitionData>> future =
                new CompletableFuture<>();

        PendingFetch pending = new PendingFetch(
                fetchSpecs, maxWaitMs, minBytes, maxBytes, future);

        if (!pendingQueue.offer(pending)) {
            future.completeExceptionally(
                    new KafkaException(
                            "Fetch forward queue full for broker " + destination.id()));
            return future;
        }

        wakeup();  // interrupt the poll sleep
        return future;
    }

    /**
     * Called each poll cycle. Drains the pending queue and builds
     * FetchRequest for each entry.
     */
    @Override
    public Collection<RequestAndCompletionHandler> generateRequests() {
        List<PendingFetch> batch = new ArrayList<>();
        pendingQueue.drainTo(batch);

        return batch.stream().map(pending -> {
            // Build FetchRequest with the same maxWaitMs budget
            // so the remote broker's DelayedFetch purgatory uses the same deadline
            FetchRequest.Builder requestBuilder = FetchRequest.Builder.forConsumer(
                    ApiKeys.FETCH.latestVersion(),
                    pending.maxWaitMs,
                    pending.minBytes,
                    toFetchRequestPartitionDataMap(pending.fetchSpecs)
            ).setMaxBytes(pending.maxBytes);

            RequestCompletionHandler handler = response -> {
                if (response.wasDisconnected()) {
                    pending.future.completeExceptionally(
                            new DisconnectException(
                                    "Lost connection to broker " + destination.id()));
                } else {
                    try {
                        FetchResponse fetchResponse =
                                (FetchResponse) response.responseBody();
                        pending.future.complete(
                                parseFetchPartitionData(fetchResponse));
                    } catch (Exception e) {
                        pending.future.completeExceptionally(e);
                    }
                }
            };

            return new RequestAndCompletionHandler(
                    time.milliseconds(), destination, requestBuilder, handler);
        }).collect(Collectors.toList());
    }

    /**
     * Returns the current queue size (for metrics).
     */
    public int queueSize() {
        return pendingQueue.size();
    }

    // ------------------------------------------------------------------
    // Helper: Convert Map<TopicIdPartition, PartitionData> to LinkedHashMap
    // ------------------------------------------------------------------

    private LinkedHashMap<TopicPartition, FetchRequest.PartitionData> toFetchRequestPartitionDataMap(
            Map<TopicIdPartition, FetchRequest.PartitionData> fetchSpecs) {

        LinkedHashMap<TopicPartition, FetchRequest.PartitionData> result = new LinkedHashMap<>();
        for (Map.Entry<TopicIdPartition, FetchRequest.PartitionData> entry : fetchSpecs.entrySet()) {
            result.put(entry.getKey().topicPartition(), entry.getValue());
        }
        return result;
    }

    // ------------------------------------------------------------------
    // Helper: Parse FetchResponse into per-partition FetchPartitionData
    // ------------------------------------------------------------------

    private Map<TopicIdPartition, FetchPartitionData> parseFetchPartitionData(
            FetchResponse response) {

        Map<TopicIdPartition, FetchPartitionData> result = new HashMap<>();

        response.data().responses().forEach(topicResponse -> {
            String topicName = topicResponse.topic();
            Uuid topicId = topicResponse.topicId();

            topicResponse.partitions().forEach(partData -> {
                TopicPartition tp = new TopicPartition(topicName, partData.partitionIndex());
                TopicIdPartition tidp = new TopicIdPartition(topicId, tp);

                Errors error = Errors.forCode(partData.errorCode());
                MemoryRecords records = (MemoryRecords) partData.records();

                FetchPartitionData fpd = new FetchPartitionData(
                        error,
                        partData.highWatermark(),
                        partData.lastStableOffset(),
                        records,
                        Optional.empty(),     // divergingEpoch
                        OptionalLong.empty(),  // lastStableOffset
                        Optional.empty(),     // abortedTransactions
                        OptionalInt.empty(),  // preferredReadReplica
                        false                 // isReassignmentFetch
                );
                result.put(tidp, fpd);
            });
        });

        return result;
    }

    // ------------------------------------------------------------------
    // Inner class: PendingFetch
    // ------------------------------------------------------------------

    /**
     * Holds a pending fetch request and the future to complete when
     * the remote broker responds.
     */
    static class PendingFetch {
        final Map<TopicIdPartition, FetchRequest.PartitionData> fetchSpecs;
        final int maxWaitMs;
        final int minBytes;
        final int maxBytes;
        final CompletableFuture<Map<TopicIdPartition, FetchPartitionData>> future;

        PendingFetch(
                Map<TopicIdPartition, FetchRequest.PartitionData> fetchSpecs,
                int maxWaitMs,
                int minBytes,
                int maxBytes,
                CompletableFuture<Map<TopicIdPartition, FetchPartitionData>> future) {
            this.fetchSpecs = fetchSpecs;
            this.maxWaitMs = maxWaitMs;
            this.minBytes = minBytes;
            this.maxBytes = maxBytes;
            this.future = future;
        }
    }
}
