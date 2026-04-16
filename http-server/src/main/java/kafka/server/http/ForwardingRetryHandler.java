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

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.metadata.LeaderAndIsr;
import org.apache.kafka.metadata.MetadataCache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Handles retry logic for produce forwarding when the remote broker returns a
 * retriable error (NOT_LEADER_OR_FOLLOWER, LEADER_NOT_AVAILABLE, REQUEST_TIMED_OUT).
 *
 * <p>Retry budget: at most 1 retry per forwarded group, bounded by
 * {@code http.internal.forwarding.timeout.ms}.</p>
 *
 * <p>This class is a utility that wraps {@link ProduceForwardManager} with
 * single-retry semantics. It separates retry policy from the forwarding I/O
 * layer, making both independently testable.</p>
 *
 * // Time: Created - TASK-F.02
 *
 * @see HttpErrorMapper#isRetriableForForwarding(Errors)
 * @see ProduceForwardManager
 */
public class ForwardingRetryHandler {

    private static final Logger log = LoggerFactory.getLogger(ForwardingRetryHandler.class);

    private final ProduceForwardManager forwardManager;
    private final MetadataCache metadataCache;
    private final int maxRetries;
    private final long forwardTimeoutMs;
    private final Time time;

    /**
     * @param forwardManager   the produce forward manager for sending requests
     * @param metadataCache    for looking up current partition leaders on retry
     * @param maxRetries       max retry attempts per forwarded group (default 1)
     * @param forwardTimeoutMs overall forwarding timeout budget in ms
     * @param time             time source for elapsed-time tracking
     */
    public ForwardingRetryHandler(
            ProduceForwardManager forwardManager,
            MetadataCache metadataCache,
            int maxRetries,
            long forwardTimeoutMs,
            Time time) {
        this.forwardManager = forwardManager;
        this.metadataCache = metadataCache;
        this.maxRetries = maxRetries;
        this.forwardTimeoutMs = forwardTimeoutMs;
        this.time = time;
    }

    /**
     * Convenience constructor that uses system time.
     */
    public ForwardingRetryHandler(
            ProduceForwardManager forwardManager,
            MetadataCache metadataCache,
            int maxRetries,
            long forwardTimeoutMs) {
        this(forwardManager, metadataCache, maxRetries, forwardTimeoutMs, Time.SYSTEM);
    }

    /**
     * Forward produce entries to a remote leader with retry on retriable errors.
     *
     * <p>On the initial forward, if any partition returns a retriable error
     * (per {@link HttpErrorMapper#isRetriableForForwarding}), the handler
     * refreshes metadata, re-buckets by new leader, and forwards once more
     * -- provided the retry budget and timeout budget allow it.</p>
     *
     * @param leaderId  target leader broker ID
     * @param entries   partition data to forward
     * @param acks      required acks
     * @param timeoutMs produce timeout from client request
     * @return future with per-partition results (includes both non-retriable
     *         results from the first attempt and results from the retry)
     */
    @SuppressWarnings("unchecked")
    public CompletableFuture<Map<TopicIdPartition, PartitionResponse>> forwardWithRetry(
            int leaderId,
            Map<TopicIdPartition, MemoryRecords> entries,
            short acks,
            int timeoutMs) {

        long startMs = time.milliseconds();

        return forwardManager.forward(leaderId, entries, acks, timeoutMs)
            .thenCompose(results -> {
                // Separate retriable errors from final results
                Map<TopicIdPartition, MemoryRecords> retriableEntries = new HashMap<>();
                Map<TopicIdPartition, PartitionResponse> finalResults = new HashMap<>(results);

                for (Map.Entry<TopicIdPartition, PartitionResponse> entry : results.entrySet()) {
                    Errors error = entry.getValue().error;
                    if (HttpErrorMapper.isRetriableForForwarding(error)) {
                        MemoryRecords originalRecords = entries.get(entry.getKey());
                        if (originalRecords != null) {
                            retriableEntries.put(entry.getKey(), originalRecords);
                        }
                    }
                }

                if (retriableEntries.isEmpty() || maxRetries <= 0) {
                    return CompletableFuture.completedFuture(finalResults);
                }

                // Check timeout budget
                long elapsedMs = time.milliseconds() - startMs;
                long remainingMs = forwardTimeoutMs - elapsedMs;
                if (remainingMs <= 0) {
                    log.debug("Forwarding retry skipped: timeout budget exhausted "
                        + "(elapsed={}ms, budget={}ms)", elapsedMs, forwardTimeoutMs);
                    return CompletableFuture.completedFuture(finalResults);
                }

                log.debug("Retrying {} partitions after retriable errors "
                    + "(remainingMs={}, maxRetries={})", retriableEntries.size(),
                    remainingMs, maxRetries);

                // Re-bucket retriable entries by refreshed leader
                Map<Integer, Map<TopicIdPartition, MemoryRecords>> rebucketed =
                    rebucketByLeader(retriableEntries);

                // Forward each rebucketed group
                CompletableFuture<Map<TopicIdPartition, PartitionResponse>>[] retryFutures =
                    rebucketed.entrySet().stream()
                        .map(e -> forwardManager.forward(
                            e.getKey(), e.getValue(), acks,
                            Math.min(timeoutMs, (int) remainingMs)))
                        .toArray(CompletableFuture[]::new);

                return CompletableFuture.allOf(retryFutures)
                    .orTimeout(remainingMs, TimeUnit.MILLISECONDS)
                    .thenApply(v -> {
                        for (CompletableFuture<Map<TopicIdPartition, PartitionResponse>> f : retryFutures) {
                            finalResults.putAll(f.join());
                        }
                        return finalResults;
                    })
                    .exceptionally(ex -> {
                        // On timeout or exception, keep original error results
                        log.warn("Forwarding retry failed: {}", ex.getMessage());
                        return finalResults;
                    });
            });
    }

    /**
     * Re-bucket entries by looking up the current leader in MetadataCache.
     * Partitions whose leader is unknown are bucketed under leader ID -1.
     */
    Map<Integer, Map<TopicIdPartition, MemoryRecords>> rebucketByLeader(
            Map<TopicIdPartition, MemoryRecords> entries) {
        Map<Integer, Map<TopicIdPartition, MemoryRecords>> buckets = new HashMap<>();
        for (Map.Entry<TopicIdPartition, MemoryRecords> entry : entries.entrySet()) {
            TopicIdPartition tp = entry.getKey();
            int newLeader = lookupLeader(tp);
            buckets.computeIfAbsent(newLeader, k -> new HashMap<>())
                   .put(tp, entry.getValue());
        }
        return buckets;
    }

    /**
     * Look up the current leader for a partition from MetadataCache.
     * Returns -1 if leader is unknown.
     */
    int lookupLeader(TopicIdPartition tp) {
        if (metadataCache == null) {
            return -1;
        }
        Optional<LeaderAndIsr> leaderAndIsr = metadataCache.getLeaderAndIsr(
            tp.topicPartition().topic(), tp.topicPartition().partition());
        return leaderAndIsr.map(lai -> lai.leader()).orElse(-1);
    }
}
