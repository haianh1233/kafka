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
// Time: Created - TASK-D.02
package kafka.server.http;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.metadata.MetadataCache;
import org.apache.kafka.server.config.AbstractKafkaConfig;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyShort;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ProduceForwardManagerTest {

    private static final ListenerName LISTENER = ListenerName.forSecurityProtocol(
            org.apache.kafka.common.security.auth.SecurityProtocol.PLAINTEXT);
    private static final Node NODE_1 = new Node(1, "host1", 9092);
    private static final Node NODE_2 = new Node(2, "host2", 9092);

    private MetadataCache metadataCache;
    private AbstractKafkaConfig config;
    private MockTime time;
    private Metrics metrics;
    private ProduceForwardManager manager;

    @BeforeEach
    void setUp() {
        metadataCache = mock(MetadataCache.class);
        config = mock(AbstractKafkaConfig.class);
        when(config.interBrokerListenerName()).thenReturn(LISTENER);
        when(config.requestTimeoutMs()).thenReturn(30000);
        when(config.brokerId()).thenReturn(0);

        time = new MockTime();
        metrics = new Metrics();
    }

    @AfterEach
    void tearDown() {
        if (manager != null) manager.close();
        metrics.close();
    }

    @SuppressWarnings("unchecked")
    private ProduceForwardThread mockThread(Node node) {
        ProduceForwardThread thread = mock(ProduceForwardThread.class);
        when(thread.destination()).thenReturn(node);
        when(thread.enqueue(any(Map.class), anyShort(), anyInt()))
                .thenReturn(new CompletableFuture<>());
        return thread;
    }

    private ProduceForwardManager createSpiedManager() {
        ProduceForwardManager mgr = spy(new ProduceForwardManager(
                metadataCache, config, time, metrics, new LogContext(), 10000));
        return mgr;
    }

    @Test
    void testForwardCreatesThreadForNewBroker() {
        when(metadataCache.getAliveBrokerNode(1, LISTENER)).thenReturn(Optional.of(NODE_1));

        manager = createSpiedManager();
        ProduceForwardThread mockThread = mockThread(NODE_1);
        doReturn(mockThread).when(manager).createThread(1);

        TopicIdPartition tip = new TopicIdPartition(Uuid.randomUuid(),
                new TopicPartition("topic1", 0));
        Map<TopicIdPartition, MemoryRecords> entries = new HashMap<>();
        entries.put(tip, mock(MemoryRecords.class));

        manager.forward(1, entries, (short) -1, 5000);

        verify(manager).createThread(1);
        assertEquals(1, manager.activeThreadCount());
    }

    @Test
    void testForwardReusesExistingThread() {
        when(metadataCache.getAliveBrokerNode(1, LISTENER)).thenReturn(Optional.of(NODE_1));

        manager = createSpiedManager();
        ProduceForwardThread mockThread = mockThread(NODE_1);
        doReturn(mockThread).when(manager).createThread(1);

        TopicIdPartition tip = new TopicIdPartition(Uuid.randomUuid(),
                new TopicPartition("topic1", 0));
        Map<TopicIdPartition, MemoryRecords> entries = new HashMap<>();
        entries.put(tip, mock(MemoryRecords.class));

        manager.forward(1, entries, (short) -1, 5000);
        manager.forward(1, entries, (short) -1, 5000);

        // createThread should only be called once
        verify(manager, times(1)).createThread(1);
        assertEquals(1, manager.activeThreadCount());
    }

    @Test
    void testForwardToDifferentBrokersCreatesSeparateThreads() {
        when(metadataCache.getAliveBrokerNode(1, LISTENER)).thenReturn(Optional.of(NODE_1));
        when(metadataCache.getAliveBrokerNode(2, LISTENER)).thenReturn(Optional.of(NODE_2));

        manager = createSpiedManager();
        doReturn(mockThread(NODE_1)).when(manager).createThread(1);
        doReturn(mockThread(NODE_2)).when(manager).createThread(2);

        TopicIdPartition tip = new TopicIdPartition(Uuid.randomUuid(),
                new TopicPartition("topic1", 0));
        Map<TopicIdPartition, MemoryRecords> entries = new HashMap<>();
        entries.put(tip, mock(MemoryRecords.class));

        manager.forward(1, entries, (short) -1, 5000);
        manager.forward(2, entries, (short) -1, 5000);

        verify(manager).createThread(1);
        verify(manager).createThread(2);
        assertEquals(2, manager.activeThreadCount());
    }

    @Test
    void testForwardToUnknownBrokerThrows() {
        when(metadataCache.getAliveBrokerNode(eq(99), any())).thenReturn(Optional.empty());

        manager = createSpiedManager();

        TopicIdPartition tip = new TopicIdPartition(Uuid.randomUuid(),
                new TopicPartition("topic1", 0));
        Map<TopicIdPartition, MemoryRecords> entries = new HashMap<>();
        entries.put(tip, mock(MemoryRecords.class));

        assertThrows(ProduceForwardManager.BrokerNotFoundException.class,
                () -> manager.forward(99, entries, (short) -1, 5000));
    }

    @Test
    void testCleanupRemovesThreadForDeadBroker() {
        when(metadataCache.getAliveBrokerNode(1, LISTENER)).thenReturn(Optional.of(NODE_1));

        manager = createSpiedManager();
        ProduceForwardThread mockThread = mockThread(NODE_1);
        when(mockThread.initiateShutdown()).thenReturn(true);
        doReturn(mockThread).when(manager).createThread(1);

        TopicIdPartition tip = new TopicIdPartition(Uuid.randomUuid(),
                new TopicPartition("topic1", 0));
        Map<TopicIdPartition, MemoryRecords> entries = new HashMap<>();
        entries.put(tip, mock(MemoryRecords.class));

        manager.forward(1, entries, (short) -1, 5000);
        assertEquals(1, manager.activeThreadCount());

        // Broker 1 is now dead
        when(metadataCache.getAliveBrokerNode(1, LISTENER)).thenReturn(Optional.empty());
        manager.cleanupStaleThreads();

        assertEquals(0, manager.activeThreadCount());
        verify(mockThread).initiateShutdown();
    }

    @Test
    void testCleanupRemovesThreadForRelocatedBroker() {
        when(metadataCache.getAliveBrokerNode(1, LISTENER)).thenReturn(Optional.of(NODE_1));

        manager = createSpiedManager();
        ProduceForwardThread mockThread = mockThread(NODE_1);
        when(mockThread.initiateShutdown()).thenReturn(true);
        doReturn(mockThread).when(manager).createThread(1);

        TopicIdPartition tip = new TopicIdPartition(Uuid.randomUuid(),
                new TopicPartition("topic1", 0));
        Map<TopicIdPartition, MemoryRecords> entries = new HashMap<>();
        entries.put(tip, mock(MemoryRecords.class));

        manager.forward(1, entries, (short) -1, 5000);
        assertEquals(1, manager.activeThreadCount());

        // Broker 1 relocated to different port
        Node relocatedNode = new Node(1, "host1", 9093);
        when(metadataCache.getAliveBrokerNode(1, LISTENER)).thenReturn(Optional.of(relocatedNode));
        manager.cleanupStaleThreads();

        assertEquals(0, manager.activeThreadCount());
        verify(mockThread).initiateShutdown();
    }

    @Test
    void testCleanupKeepsAliveThreads() {
        when(metadataCache.getAliveBrokerNode(1, LISTENER)).thenReturn(Optional.of(NODE_1));

        manager = createSpiedManager();
        ProduceForwardThread mockThread = mockThread(NODE_1);
        doReturn(mockThread).when(manager).createThread(1);

        TopicIdPartition tip = new TopicIdPartition(Uuid.randomUuid(),
                new TopicPartition("topic1", 0));
        Map<TopicIdPartition, MemoryRecords> entries = new HashMap<>();
        entries.put(tip, mock(MemoryRecords.class));

        manager.forward(1, entries, (short) -1, 5000);

        // Broker 1 still alive at same address
        manager.cleanupStaleThreads();

        assertEquals(1, manager.activeThreadCount());
        verify(mockThread, never()).initiateShutdown();
    }

    @Test
    void testCloseShutdownsAllThreads() throws InterruptedException {
        when(metadataCache.getAliveBrokerNode(1, LISTENER)).thenReturn(Optional.of(NODE_1));
        when(metadataCache.getAliveBrokerNode(2, LISTENER)).thenReturn(Optional.of(NODE_2));

        manager = createSpiedManager();
        ProduceForwardThread mockThread1 = mockThread(NODE_1);
        ProduceForwardThread mockThread2 = mockThread(NODE_2);
        when(mockThread1.initiateShutdown()).thenReturn(true);
        when(mockThread2.initiateShutdown()).thenReturn(true);
        doReturn(mockThread1).when(manager).createThread(1);
        doReturn(mockThread2).when(manager).createThread(2);

        TopicIdPartition tip = new TopicIdPartition(Uuid.randomUuid(),
                new TopicPartition("topic1", 0));
        Map<TopicIdPartition, MemoryRecords> entries = new HashMap<>();
        entries.put(tip, mock(MemoryRecords.class));

        manager.forward(1, entries, (short) -1, 5000);
        manager.forward(2, entries, (short) -1, 5000);
        assertEquals(2, manager.activeThreadCount());

        manager.close();

        assertEquals(0, manager.activeThreadCount());
        verify(mockThread1).initiateShutdown();
        verify(mockThread2).initiateShutdown();
        verify(mockThread1).awaitShutdown();
        verify(mockThread2).awaitShutdown();

        // Prevent double-close in tearDown
        manager = null;
    }
}
