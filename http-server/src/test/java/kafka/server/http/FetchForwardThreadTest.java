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

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.server.storage.log.FetchPartitionData;
import org.apache.kafka.server.util.RequestAndCompletionHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class FetchForwardThreadTest {

    private static final Node DESTINATION = new Node(2, "broker2.example.com", 9092);
    private static final int QUEUE_CAPACITY = 100;
    private static final int REQUEST_TIMEOUT_MS = 30000;

    private KafkaClient networkClient;
    private MockTime time;
    private FetchForwardThread thread;

    @BeforeEach
    void setUp() {
        networkClient = mock(KafkaClient.class);
        when(networkClient.active()).thenReturn(true);
        time = new MockTime();
        thread = new FetchForwardThread(
                DESTINATION, networkClient, REQUEST_TIMEOUT_MS, QUEUE_CAPACITY, time);
    }

    @AfterEach
    void tearDown() throws Exception {
        thread.initiateShutdown();
    }

    @Test
    void testEnqueueReturnsFuture() {
        Map<TopicIdPartition, FetchRequest.PartitionData> specs = singlePartitionFetchSpecs();
        CompletableFuture<Map<TopicIdPartition, FetchPartitionData>> future =
                thread.enqueue(specs, 5000, 1, 10485760);
        assertNotNull(future);
        assertFalse(future.isDone());
    }

    @Test
    void testGenerateRequestsDrainsQueue() {
        thread.enqueue(singlePartitionFetchSpecs(), 5000, 1, 10485760);
        thread.enqueue(singlePartitionFetchSpecs(), 3000, 1, 5242880);

        Collection<RequestAndCompletionHandler> requests = thread.generateRequests();
        assertEquals(2, requests.size());
        assertEquals(0, thread.queueSize());
    }

    @Test
    void testMaxWaitMsPassedThrough() {
        // Verify the FetchRequest built by generateRequests uses the
        // maxWaitMs from the enqueue call (same budget for remote purgatory)
        thread.enqueue(singlePartitionFetchSpecs(), 3500, 1, 10485760);

        Collection<RequestAndCompletionHandler> requests = thread.generateRequests();
        RequestAndCompletionHandler rach = requests.iterator().next();
        @SuppressWarnings("unchecked")
        FetchRequest.Builder builder = (FetchRequest.Builder) rach.request;
        FetchRequest built = builder.build();
        assertEquals(3500, built.maxWait());
    }

    @Test
    void testQueueFullRejectsWithException() {
        for (int i = 0; i < QUEUE_CAPACITY; i++) {
            thread.enqueue(singlePartitionFetchSpecs(), 5000, 1, 10485760);
        }

        CompletableFuture<?> future = thread.enqueue(
                singlePartitionFetchSpecs(), 5000, 1, 10485760);
        assertTrue(future.isCompletedExceptionally());
    }

    @Test
    void testDisconnectCompletesExceptionally() {
        CompletableFuture<Map<TopicIdPartition, FetchPartitionData>> future =
                thread.enqueue(singlePartitionFetchSpecs(), 5000, 1, 10485760);
        Collection<RequestAndCompletionHandler> requests = thread.generateRequests();

        // Simulate disconnect
        RequestAndCompletionHandler rach = requests.iterator().next();
        ClientResponse disconnectResponse = mock(ClientResponse.class);
        when(disconnectResponse.wasDisconnected()).thenReturn(true);
        rach.handler.onComplete(disconnectResponse);

        // The future should have been completed exceptionally
        assertTrue(future.isCompletedExceptionally());
    }

    @Test
    void testDestination() {
        assertEquals(DESTINATION, thread.destination());
    }

    private Map<TopicIdPartition, FetchRequest.PartitionData> singlePartitionFetchSpecs() {
        Map<TopicIdPartition, FetchRequest.PartitionData> specs = new HashMap<>();
        TopicIdPartition tp = new TopicIdPartition(
                Uuid.randomUuid(),
                new TopicPartition("test-topic", 0));
        specs.put(tp, new FetchRequest.PartitionData(
                tp.topicId(), 0L, -1L, 1048576, Optional.empty()));
        return specs;
    }
}
