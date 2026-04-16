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
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;
import org.apache.kafka.metadata.LeaderAndIsr;
import org.apache.kafka.metadata.MetadataCache;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyShort;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link ForwardingRetryHandler} -- forwarding retry logic.
 *
 * // Time: Created - TASK-F.02
 */
class ForwardingRetryHandlerTest {

    private final Uuid topicId = Uuid.randomUuid();
    private final TopicIdPartition tp0 = new TopicIdPartition(topicId, 0, "test");

    @Test
    void testNoRetryOnSuccess() throws Exception {
        ProduceForwardManager mockManager = mock(ProduceForwardManager.class);
        Map<TopicIdPartition, PartitionResponse> successResult = Map.of(
            tp0, new PartitionResponse(Errors.NONE, 100L, -1L, -1L)
        );
        when(mockManager.forward(anyInt(), anyMap(), anyShort(), anyInt()))
            .thenReturn(CompletableFuture.completedFuture(successResult));

        ForwardingRetryHandler handler = new ForwardingRetryHandler(
            mockManager, null, 1, 10000);

        Map<TopicIdPartition, PartitionResponse> result =
            handler.forwardWithRetry(1, new HashMap<>(), (short) -1, 5000).get();
        assertEquals(Errors.NONE, result.get(tp0).error);
        verify(mockManager, times(1)).forward(anyInt(), anyMap(), anyShort(), anyInt());
    }

    @Test
    void testRetryOnNotLeaderOrFollower() throws Exception {
        ProduceForwardManager mockManager = mock(ProduceForwardManager.class);

        // Prepare entries -- need actual entries for retry to find them
        Map<TopicIdPartition, MemoryRecords> entries = new HashMap<>();
        entries.put(tp0, MemoryRecords.EMPTY);

        Map<TopicIdPartition, PartitionResponse> errorResult = new HashMap<>();
        errorResult.put(tp0, new PartitionResponse(Errors.NOT_LEADER_OR_FOLLOWER));

        Map<TopicIdPartition, PartitionResponse> successResult = new HashMap<>();
        successResult.put(tp0, new PartitionResponse(Errors.NONE, 100L, -1L, -1L));

        when(mockManager.forward(anyInt(), anyMap(), anyShort(), anyInt()))
            .thenReturn(CompletableFuture.completedFuture(errorResult))
            .thenReturn(CompletableFuture.completedFuture(successResult));

        // MetadataCache returns a new leader
        MetadataCache mockCache = mock(MetadataCache.class);
        when(mockCache.getLeaderAndIsr("test", 0))
            .thenReturn(Optional.of(new LeaderAndIsr(2, List.of(1, 2, 3))));

        ForwardingRetryHandler handler = new ForwardingRetryHandler(
            mockManager, mockCache, 1, 10000);

        handler.forwardWithRetry(1, entries, (short) -1, 5000).get();

        // After retry, forward should have been called at least 2 times
        verify(mockManager, atLeast(2)).forward(anyInt(), anyMap(), anyShort(), anyInt());
    }

    @Test
    void testRetryOnLeaderNotAvailable() throws Exception {
        ProduceForwardManager mockManager = mock(ProduceForwardManager.class);

        Map<TopicIdPartition, MemoryRecords> entries = new HashMap<>();
        entries.put(tp0, MemoryRecords.EMPTY);

        Map<TopicIdPartition, PartitionResponse> errorResult = new HashMap<>();
        errorResult.put(tp0, new PartitionResponse(Errors.LEADER_NOT_AVAILABLE));

        Map<TopicIdPartition, PartitionResponse> successResult = new HashMap<>();
        successResult.put(tp0, new PartitionResponse(Errors.NONE, 200L, -1L, -1L));

        when(mockManager.forward(anyInt(), anyMap(), anyShort(), anyInt()))
            .thenReturn(CompletableFuture.completedFuture(errorResult))
            .thenReturn(CompletableFuture.completedFuture(successResult));

        MetadataCache mockCache = mock(MetadataCache.class);
        when(mockCache.getLeaderAndIsr("test", 0))
            .thenReturn(Optional.of(new LeaderAndIsr(3, List.of(1, 2, 3))));

        ForwardingRetryHandler handler = new ForwardingRetryHandler(
            mockManager, mockCache, 1, 10000);

        handler.forwardWithRetry(1, entries, (short) -1, 5000).get();

        verify(mockManager, atLeast(2)).forward(anyInt(), anyMap(), anyShort(), anyInt());
    }

    @Test
    void testNoRetryOnNonRetriableError() throws Exception {
        ProduceForwardManager mockManager = mock(ProduceForwardManager.class);
        Map<TopicIdPartition, PartitionResponse> errorResult = Map.of(
            tp0, new PartitionResponse(Errors.TOPIC_AUTHORIZATION_FAILED)
        );
        when(mockManager.forward(anyInt(), anyMap(), anyShort(), anyInt()))
            .thenReturn(CompletableFuture.completedFuture(errorResult));

        ForwardingRetryHandler handler = new ForwardingRetryHandler(
            mockManager, null, 1, 10000);

        Map<TopicIdPartition, PartitionResponse> result =
            handler.forwardWithRetry(1, new HashMap<>(), (short) -1, 5000).get();
        assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED, result.get(tp0).error);
        verify(mockManager, times(1)).forward(anyInt(), anyMap(), anyShort(), anyInt());
    }

    @Test
    void testNoRetryOnMessageTooLarge() throws Exception {
        ProduceForwardManager mockManager = mock(ProduceForwardManager.class);
        Map<TopicIdPartition, PartitionResponse> errorResult = Map.of(
            tp0, new PartitionResponse(Errors.MESSAGE_TOO_LARGE)
        );
        when(mockManager.forward(anyInt(), anyMap(), anyShort(), anyInt()))
            .thenReturn(CompletableFuture.completedFuture(errorResult));

        ForwardingRetryHandler handler = new ForwardingRetryHandler(
            mockManager, null, 1, 10000);

        Map<TopicIdPartition, PartitionResponse> result =
            handler.forwardWithRetry(1, new HashMap<>(), (short) -1, 5000).get();
        assertEquals(Errors.MESSAGE_TOO_LARGE, result.get(tp0).error);
        verify(mockManager, times(1)).forward(anyInt(), anyMap(), anyShort(), anyInt());
    }

    @Test
    void testNoRetryWhenMaxRetriesIsZero() throws Exception {
        ProduceForwardManager mockManager = mock(ProduceForwardManager.class);

        Map<TopicIdPartition, MemoryRecords> entries = new HashMap<>();
        entries.put(tp0, MemoryRecords.EMPTY);

        Map<TopicIdPartition, PartitionResponse> errorResult = new HashMap<>();
        errorResult.put(tp0, new PartitionResponse(Errors.NOT_LEADER_OR_FOLLOWER));

        when(mockManager.forward(anyInt(), anyMap(), anyShort(), anyInt()))
            .thenReturn(CompletableFuture.completedFuture(errorResult));

        // maxRetries = 0 means no retry allowed
        ForwardingRetryHandler handler = new ForwardingRetryHandler(
            mockManager, null, 0, 10000);

        Map<TopicIdPartition, PartitionResponse> result =
            handler.forwardWithRetry(1, entries, (short) -1, 5000).get();
        assertEquals(Errors.NOT_LEADER_OR_FOLLOWER, result.get(tp0).error);
        verify(mockManager, times(1)).forward(anyInt(), anyMap(), anyShort(), anyInt());
    }

    @Test
    void testLookupLeaderReturnsMinusOneWhenCacheIsNull() {
        ForwardingRetryHandler handler = new ForwardingRetryHandler(
            mock(ProduceForwardManager.class), null, 1, 10000);

        assertEquals(-1, handler.lookupLeader(tp0));
    }

    @Test
    void testLookupLeaderReturnsMinusOneWhenPartitionNotFound() {
        MetadataCache mockCache = mock(MetadataCache.class);
        when(mockCache.getLeaderAndIsr("test", 0))
            .thenReturn(Optional.empty());

        ForwardingRetryHandler handler = new ForwardingRetryHandler(
            mock(ProduceForwardManager.class), mockCache, 1, 10000);

        assertEquals(-1, handler.lookupLeader(tp0));
    }

    @Test
    void testLookupLeaderReturnsLeaderId() {
        MetadataCache mockCache = mock(MetadataCache.class);
        when(mockCache.getLeaderAndIsr("test", 0))
            .thenReturn(Optional.of(new LeaderAndIsr(5, List.of(1, 2, 5))));

        ForwardingRetryHandler handler = new ForwardingRetryHandler(
            mock(ProduceForwardManager.class), mockCache, 1, 10000);

        assertEquals(5, handler.lookupLeader(tp0));
    }

    @Test
    void testRebucketByLeaderGroupsCorrectly() {
        MetadataCache mockCache = mock(MetadataCache.class);
        TopicIdPartition tp1 = new TopicIdPartition(topicId, 1, "test");
        TopicIdPartition tp2 = new TopicIdPartition(topicId, 2, "test");

        // p0 -> leader 1, p1 -> leader 2, p2 -> leader 1
        when(mockCache.getLeaderAndIsr("test", 0))
            .thenReturn(Optional.of(new LeaderAndIsr(1, List.of(1, 2))));
        when(mockCache.getLeaderAndIsr("test", 1))
            .thenReturn(Optional.of(new LeaderAndIsr(2, List.of(2, 3))));
        when(mockCache.getLeaderAndIsr("test", 2))
            .thenReturn(Optional.of(new LeaderAndIsr(1, List.of(1, 3))));

        ForwardingRetryHandler handler = new ForwardingRetryHandler(
            mock(ProduceForwardManager.class), mockCache, 1, 10000);

        Map<TopicIdPartition, MemoryRecords> entries = new HashMap<>();
        entries.put(tp0, MemoryRecords.EMPTY);
        entries.put(tp1, MemoryRecords.EMPTY);
        entries.put(tp2, MemoryRecords.EMPTY);

        Map<Integer, Map<TopicIdPartition, MemoryRecords>> buckets =
            handler.rebucketByLeader(entries);

        assertEquals(2, buckets.size());
        assertNotNull(buckets.get(1));
        assertNotNull(buckets.get(2));
        assertEquals(2, buckets.get(1).size()); // tp0 and tp2
        assertEquals(1, buckets.get(2).size()); // tp1
    }
}
