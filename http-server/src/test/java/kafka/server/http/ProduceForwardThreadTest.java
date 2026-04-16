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
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.server.util.RequestAndCompletionHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ProduceForwardThreadTest {

    private static final Node DESTINATION = new Node(1, "broker1.example.com", 9092);
    private static final int QUEUE_CAPACITY = 100;
    private static final int REQUEST_TIMEOUT_MS = 30000;

    private KafkaClient networkClient;
    private MockTime time;
    private ProduceForwardThread thread;

    @BeforeEach
    void setUp() {
        networkClient = mock(KafkaClient.class);
        when(networkClient.active()).thenReturn(true);
        time = new MockTime();
        thread = new ProduceForwardThread(
                DESTINATION, networkClient, REQUEST_TIMEOUT_MS, QUEUE_CAPACITY, time);
    }

    @AfterEach
    void tearDown() throws Exception {
        thread.initiateShutdown();
    }

    @Test
    void testEnqueueReturnsFuture() {
        Map<TopicIdPartition, MemoryRecords> entries = singlePartitionEntries();
        CompletableFuture<Map<TopicIdPartition, ProduceResponse.PartitionResponse>> future =
                thread.enqueue(entries, (short) -1, 5000);
        assertNotNull(future);
        assertFalse(future.isDone());
    }

    @Test
    void testGenerateRequestsDrainsQueue() {
        thread.enqueue(singlePartitionEntries(), (short) -1, 5000);
        thread.enqueue(singlePartitionEntries(), (short) 1, 3000);

        Collection<RequestAndCompletionHandler> requests = thread.generateRequests();
        assertEquals(2, requests.size());

        // Queue should be empty after drain
        assertEquals(0, thread.queueSize());
    }

    @Test
    void testGenerateRequestsReturnsEmptyWhenQueueEmpty() {
        Collection<RequestAndCompletionHandler> requests = thread.generateRequests();
        assertTrue(requests.isEmpty());
    }

    @Test
    void testQueueFullRejectsWithException() {
        // Fill the queue
        for (int i = 0; i < QUEUE_CAPACITY; i++) {
            thread.enqueue(singlePartitionEntries(), (short) -1, 5000);
        }

        // Next enqueue should fail
        CompletableFuture<?> future = thread.enqueue(
                singlePartitionEntries(), (short) -1, 5000);
        assertTrue(future.isCompletedExceptionally());
    }

    @Test
    void testDisconnectCompletesExceptionally() {
        CompletableFuture<Map<TopicIdPartition, ProduceResponse.PartitionResponse>> future =
                thread.enqueue(singlePartitionEntries(), (short) -1, 5000);
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

    private Map<TopicIdPartition, MemoryRecords> singlePartitionEntries() {
        Map<TopicIdPartition, MemoryRecords> entries = new HashMap<>();
        TopicIdPartition tp = new TopicIdPartition(
                Uuid.randomUuid(),
                new TopicPartition("test-topic", 0));
        entries.put(tp, MemoryRecords.withRecords(
                Compression.NONE,
                new SimpleRecord("key".getBytes(), "value".getBytes())));
        return entries;
    }
}
