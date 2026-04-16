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
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.server.util.InterBrokerSendThread;
import org.apache.kafka.server.util.RequestAndCompletionHandler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

/**
 * One instance per remote leader broker. Drains a queue of pending produce
 * entries and forwards them as binary ProduceRequests over the inter-broker
 * listener. Follows the same pattern as TransactionMarkerChannelManager.
 *
 * <p>Thread lifecycle: created by {@code ProduceForwardManager#forward} via
 * {@code computeIfAbsent}, started immediately, and shut down when the
 * broker is no longer alive or during graceful shutdown.</p>
 *
 * @see org.apache.kafka.server.util.InterBrokerSendThread
 */
public class ProduceForwardThread extends InterBrokerSendThread {

    private final Node destination;
    private final Time time;
    private final BlockingQueue<PendingProduce> pendingQueue;

    /**
     * @param destination      the remote broker node to forward requests to
     * @param networkClient    the network client for this broker (one per thread)
     * @param requestTimeoutMs timeout for individual requests on the wire
     * @param queueCapacity    max pending entries before rejection (config:
     *                         http.internal.forwarding.queue.size, default 10000)
     * @param time             time source
     */
    public ProduceForwardThread(
            Node destination,
            KafkaClient networkClient,
            int requestTimeoutMs,
            int queueCapacity,
            Time time) {
        super("ProduceForwardThread-" + destination.id(), networkClient, requestTimeoutMs, time);
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
     * Called by KafkaApis handler thread (via ProduceForwardManager).
     * Returns a CompletableFuture that resolves when the remote broker's
     * ProduceResponse arrives.
     *
     * <p>Uses {@code offer()} (non-blocking). If the queue is full, the
     * future completes exceptionally immediately.</p>
     *
     * @param entriesPerPartition records to forward, keyed by TopicIdPartition
     * @param requiredAcks        acks level (-1, 1, or 0)
     * @param timeoutMs           produce timeout from the HTTP request
     * @return future that completes with per-partition responses
     */
    public CompletableFuture<Map<TopicIdPartition, ProduceResponse.PartitionResponse>> enqueue(
            Map<TopicIdPartition, MemoryRecords> entriesPerPartition,
            short requiredAcks,
            int timeoutMs) {

        CompletableFuture<Map<TopicIdPartition, ProduceResponse.PartitionResponse>> future =
                new CompletableFuture<>();

        PendingProduce pending = new PendingProduce(
                entriesPerPartition, requiredAcks, timeoutMs, future);

        if (!pendingQueue.offer(pending)) {
            future.completeExceptionally(
                    new org.apache.kafka.common.errors.NotEnoughReplicasException(
                            "Produce forward queue full for broker " + destination.id()
                                    + " (capacity: " + pendingQueue.remainingCapacity() + ")"));
            return future;
        }

        wakeup();  // interrupt the poll sleep so the request is sent immediately
        return future;
    }

    /**
     * Called each poll cycle by InterBrokerSendThread.doWork() -> pollOnce().
     * Drains the pending queue and builds ProduceRequest for each entry.
     */
    @Override
    public Collection<RequestAndCompletionHandler> generateRequests() {
        List<PendingProduce> batch = new ArrayList<>();
        pendingQueue.drainTo(batch);

        return batch.stream().map(pending -> {
            // Build ProduceRequest from the pending entry
            ProduceRequestData data = new ProduceRequestData()
                    .setAcks(pending.requiredAcks)
                    .setTimeoutMs(pending.timeoutMs);

            // Convert entriesPerPartition to ProduceRequestData.TopicProduceDataCollection
            data.setTopicData(toTopicProduceData(pending.entriesPerPartition));

            ProduceRequest.Builder requestBuilder = ProduceRequest.builder(data);

            // Completion handler: parse response and complete future
            RequestCompletionHandler handler = response -> {
                if (response.wasDisconnected()) {
                    pending.future.completeExceptionally(
                            new DisconnectException(
                                    "Lost connection to broker " + destination.id()));
                } else {
                    try {
                        ProduceResponse produceResponse =
                                (ProduceResponse) response.responseBody();
                        pending.future.complete(
                                parsePartitionResponses(produceResponse, pending.entriesPerPartition));
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
    // Helper: Convert Map<TopicIdPartition, MemoryRecords> to protocol data
    // ------------------------------------------------------------------

    private ProduceRequestData.TopicProduceDataCollection toTopicProduceData(
            Map<TopicIdPartition, MemoryRecords> entries) {

        // Group by topic
        Map<String, List<Map.Entry<TopicIdPartition, MemoryRecords>>> byTopic = new HashMap<>();
        for (Map.Entry<TopicIdPartition, MemoryRecords> entry : entries.entrySet()) {
            byTopic.computeIfAbsent(entry.getKey().topic(), k -> new ArrayList<>()).add(entry);
        }

        ProduceRequestData.TopicProduceDataCollection collection =
                new ProduceRequestData.TopicProduceDataCollection();

        for (Map.Entry<String, List<Map.Entry<TopicIdPartition, MemoryRecords>>> topicEntry
                : byTopic.entrySet()) {

            String topicName = topicEntry.getKey();
            Uuid topicId = topicEntry.getValue().get(0).getKey().topicId();

            ProduceRequestData.TopicProduceData topicData =
                    new ProduceRequestData.TopicProduceData()
                            .setName(topicName)
                            .setTopicId(topicId);

            for (Map.Entry<TopicIdPartition, MemoryRecords> partEntry : topicEntry.getValue()) {
                topicData.partitionData().add(
                        new ProduceRequestData.PartitionProduceData()
                                .setIndex(partEntry.getKey().partition())
                                .setRecords(partEntry.getValue()));
            }

            collection.add(topicData);
        }

        return collection;
    }

    // ------------------------------------------------------------------
    // Helper: Parse ProduceResponse into per-partition results
    // ------------------------------------------------------------------

    private Map<TopicIdPartition, ProduceResponse.PartitionResponse> parsePartitionResponses(
            ProduceResponse response,
            Map<TopicIdPartition, MemoryRecords> originalEntries) {

        Map<TopicIdPartition, ProduceResponse.PartitionResponse> result = new HashMap<>();

        response.data().responses().forEach(topicResponse -> {
            String topicName = topicResponse.name();
            Uuid topicId = topicResponse.topicId();

            topicResponse.partitionResponses().forEach(partResp -> {
                TopicPartition tp = new TopicPartition(topicName, partResp.index());
                TopicIdPartition tidp = new TopicIdPartition(topicId, tp);

                ProduceResponse.PartitionResponse pr = new ProduceResponse.PartitionResponse(
                        Errors.forCode(partResp.errorCode()),
                        partResp.baseOffset(),
                        partResp.logAppendTimeMs(),
                        partResp.logStartOffset());
                result.put(tidp, pr);
            });
        });

        return result;
    }

    // ------------------------------------------------------------------
    // Inner class: PendingProduce
    // ------------------------------------------------------------------

    /**
     * Holds a pending produce request and the future to complete when
     * the remote broker responds.
     */
    static class PendingProduce {
        final Map<TopicIdPartition, MemoryRecords> entriesPerPartition;
        final short requiredAcks;
        final int timeoutMs;
        final CompletableFuture<Map<TopicIdPartition, ProduceResponse.PartitionResponse>> future;

        PendingProduce(
                Map<TopicIdPartition, MemoryRecords> entriesPerPartition,
                short requiredAcks,
                int timeoutMs,
                CompletableFuture<Map<TopicIdPartition, ProduceResponse.PartitionResponse>> future) {
            this.entriesPerPartition = entriesPerPartition;
            this.requiredAcks = requiredAcks;
            this.timeoutMs = timeoutMs;
            this.future = future;
        }
    }
}
