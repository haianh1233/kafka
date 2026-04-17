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

// Time: Created - TASK-WS1.13

package kafka.server.http.ws;

import org.apache.kafka.common.TopicPartition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Per-subscription delivery tag tracker with ack bitmap for gap-safe offset commits.
 *
 * <p>Delivery tags are per-subscription, monotonically increasing starting from 1.
 * Each tag maps to a {@code (TopicPartition, offset)} pair. The tracker maintains a
 * per-partition ack bitmap ({@link AckState} per offset) to support {@code multiple=true}
 * acknowledgement while preserving gaps introduced by {@code NACK(requeue=true)}.
 *
 * <p><b>Thread-safety:</b> guarded by a private lock object. The consumer fetch loop
 * assigns tags (single writer) while multiple Netty event-loop threads may ack/nack
 * concurrently. Critical sections are short (map lookups and updates).
 *
 * <p>See design doc §13.4 (offset commit with gaps) and §21.3 (delivery tag memory bound).
 */
public final class WsDeliveryTagTracker {

    private static final Logger log = LoggerFactory.getLogger(WsDeliveryTagTracker.class);

    /**
     * Immutable record representing a pending delivery.
     */
    public record PendingDelivery(TopicPartition topicPartition, long offset) { }

    /**
     * Per-offset ack state used to compute committable offsets.
     *
     * <ul>
     *   <li>{@link #PENDING} — delivered but not yet acked/nacked; blocks commit advance</li>
     *   <li>{@link #ACKED} — client acked; commit may advance past this offset</li>
     *   <li>{@link #NACKED_REQUEUE} — client nacked with requeue=true; offset must NOT
     *       be committed until the message is redelivered and acked (gap preserved)</li>
     *   <li>{@link #NACKED_DISCARD} — client nacked with requeue=false; offset is
     *       treated as terminal (DLX or dropped) and commit may advance past it</li>
     * </ul>
     */
    private enum AckState { PENDING, ACKED, NACKED_REQUEUE, NACKED_DISCARD }

    private final AtomicLong tagCounter = new AtomicLong(0);
    private final Object lock = new Object();

    /** tag → PendingDelivery. Guarded by {@link #lock}. */
    private final Map<Long, PendingDelivery> pendingDeliveries = new HashMap<>();

    /** partition → (offset → AckState). TreeMap for ordered iteration. Guarded by {@link #lock}. */
    private final Map<TopicPartition, TreeMap<Long, AckState>> partitionAckMaps = new HashMap<>();

    /**
     * partition → next offset to commit (Kafka convention: offset+1 of last committed record).
     * Guarded by {@link #lock}. Absent key means "never committed".
     */
    private final Map<TopicPartition, Long> lastCommittedOffsets = new HashMap<>();

    /**
     * Volatile so ack/nack can fast-path without entering the lock once cleared,
     * while the lock still serialises all mutations.
     */
    private volatile boolean cleared = false;

    /**
     * Assigns a delivery tag for a record at the given partition and offset.
     *
     * @param tp     the topic partition
     * @param offset the record offset
     * @return the assigned delivery tag (monotonically increasing from 1);
     *         tags are allocated even after {@link #clear()} but carry no tracking state
     */
    public long assign(TopicPartition tp, long offset) {
        long tag = tagCounter.incrementAndGet();
        synchronized (lock) {
            if (cleared) {
                return tag;
            }
            pendingDeliveries.put(tag, new PendingDelivery(tp, offset));
            // Redelivery semantics: if the offset was previously NACKED_REQUEUE, the caller
            // has redelivered it — reset to PENDING so a new ack can advance the watermark.
            // If it is currently PENDING or a terminal state (ACKED/NACKED_DISCARD), leave it:
            // a concurrent assign of an already-terminal offset indicates an over-delivery
            // which the caller is responsible for preventing.
            partitionAckMaps
                .computeIfAbsent(tp, k -> new TreeMap<>())
                .merge(offset, AckState.PENDING, (oldState, newState) ->
                    oldState == AckState.NACKED_REQUEUE ? AckState.PENDING : oldState);
        }
        return tag;
    }

    /**
     * Acknowledges a single delivery tag.
     *
     * <p>Unknown tag (never assigned, or already acked/nacked) → returns {@code false}.
     * Ack after {@link #clear()} → silently ignored, returns {@code false}.
     *
     * @param deliveryTag the tag to acknowledge
     * @return true if the tag was pending and has now been acknowledged
     */
    public boolean ack(long deliveryTag) {
        synchronized (lock) {
            if (cleared) {
                return false;
            }
            PendingDelivery delivery = pendingDeliveries.remove(deliveryTag);
            if (delivery == null) {
                return false;
            }
            TreeMap<Long, AckState> ackMap = partitionAckMaps.get(delivery.topicPartition());
            if (ackMap != null) {
                AckState current = ackMap.get(delivery.offset());
                if (current == AckState.PENDING) {
                    ackMap.put(delivery.offset(), AckState.ACKED);
                }
                // else: already transitioned (ACKED / NACKED_*); leave as-is for idempotence
            }
            return true;
        }
    }

    /**
     * Acknowledges all delivery tags from 1 up to and including {@code upToTag}.
     *
     * <p>Tags already transitioned to a NACK state are <b>not</b> re-acked (gap preservation).
     * Unknown {@code upToTag} (outside [1, currentCounter]) → returns {@code false}.
     *
     * @param upToTag the highest tag to acknowledge
     * @return true if {@code upToTag} is within the assigned tag range
     */
    public boolean ackMultiple(long upToTag) {
        synchronized (lock) {
            if (cleared) {
                return false;
            }
            // "Known" means the tag has been allocated at some point.
            if (upToTag < 1 || upToTag > tagCounter.get()) {
                return false;
            }
            Iterator<Map.Entry<Long, PendingDelivery>> it = pendingDeliveries.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<Long, PendingDelivery> entry = it.next();
                if (entry.getKey() <= upToTag) {
                    PendingDelivery delivery = entry.getValue();
                    TreeMap<Long, AckState> ackMap = partitionAckMaps.get(delivery.topicPartition());
                    if (ackMap != null) {
                        AckState current = ackMap.get(delivery.offset());
                        // Only flip PENDING → ACKED; preserve NACKED_REQUEUE and NACKED_DISCARD.
                        if (current == AckState.PENDING) {
                            ackMap.put(delivery.offset(), AckState.ACKED);
                        }
                    }
                    it.remove();
                }
            }
            return true;
        }
    }

    /**
     * Negatively acknowledges a delivery tag.
     *
     * @param deliveryTag the tag to nack
     * @param requeue     if {@code true}, the offset is marked {@link AckState#NACKED_REQUEUE}
     *                    and the commit watermark will NOT advance past it (message will be
     *                    redelivered). If {@code false}, marked {@link AckState#NACKED_DISCARD}
     *                    and the commit watermark may advance past it (message is dropped
     *                    or routed to a DLX by caller).
     * @return the {@link PendingDelivery} for the nacked tag, or {@code null} if the tag
     *         is unknown or the tracker is cleared
     */
    public PendingDelivery nack(long deliveryTag, boolean requeue) {
        synchronized (lock) {
            if (cleared) {
                return null;
            }
            PendingDelivery delivery = pendingDeliveries.remove(deliveryTag);
            if (delivery == null) {
                return null;
            }
            TreeMap<Long, AckState> ackMap = partitionAckMaps.get(delivery.topicPartition());
            if (ackMap != null) {
                ackMap.put(delivery.offset(), requeue ? AckState.NACKED_REQUEUE : AckState.NACKED_DISCARD);
            }
            return delivery;
        }
    }

    /**
     * Returns the committable offsets: for each partition, the highest contiguous
     * offset that can be safely committed (lowest uncommitted offset + 1, per Kafka convention).
     *
     * <p>A partition appears in the returned map only when its commit watermark has
     * advanced since the previous call. Entries below the new watermark are pruned from
     * the internal ack bitmap to bound memory.
     *
     * <p>Scan semantics: starting from the lowest offset in the ack bitmap, advance while
     * the state is {@link AckState#ACKED} or {@link AckState#NACKED_DISCARD}. Stop at the
     * first {@link AckState#PENDING} or {@link AckState#NACKED_REQUEUE} — that offset is
     * the gap and must remain uncommitted.
     *
     * @return map of {@code TopicPartition} → next offset to commit
     */
    public Map<TopicPartition, Long> getCommittableOffsets() {
        Map<TopicPartition, Long> result = new LinkedHashMap<>();
        synchronized (lock) {
            if (cleared) {
                return result;
            }
            for (Map.Entry<TopicPartition, TreeMap<Long, AckState>> entry : partitionAckMaps.entrySet()) {
                TopicPartition tp = entry.getKey();
                TreeMap<Long, AckState> ackMap = entry.getValue();
                if (ackMap.isEmpty()) {
                    continue;
                }

                // Scan from the lowest recorded offset. Advance the watermark while we see
                // terminal ack states (ACKED or NACKED_DISCARD). Stop at PENDING or
                // NACKED_REQUEUE — this preserves the §13.4 gap invariant.
                long committable = -1L;
                for (Map.Entry<Long, AckState> offsetEntry : ackMap.entrySet()) {
                    AckState state = offsetEntry.getValue();
                    if (state == AckState.ACKED || state == AckState.NACKED_DISCARD) {
                        committable = offsetEntry.getKey() + 1L;
                    } else {
                        break;
                    }
                }

                if (committable < 0) {
                    continue;
                }
                Long previous = lastCommittedOffsets.get(tp);
                if (previous == null || committable > previous) {
                    result.put(tp, committable);
                    lastCommittedOffsets.put(tp, committable);
                    // Prune entries strictly below the new watermark — they are committed
                    // and cannot affect future scans. This bounds memory per design §21.3.
                    ackMap.headMap(committable).clear();
                }
            }
        }
        return result;
    }

    /**
     * Clears all state. Called on unsubscribe or connection close.
     * After clear, {@link #ack}, {@link #ackMultiple}, {@link #nack} and
     * {@link #getCommittableOffsets} are no-ops.
     */
    public void clear() {
        synchronized (lock) {
            cleared = true;
            pendingDeliveries.clear();
            partitionAckMaps.clear();
            lastCommittedOffsets.clear();
        }
        log.debug("WsDeliveryTagTracker cleared");
    }

    /**
     * Returns the number of pending (unacked, un-nacked) deliveries.
     */
    public int pendingCount() {
        synchronized (lock) {
            return pendingDeliveries.size();
        }
    }

    /**
     * Returns the highest delivery tag ever assigned by {@link #assign}. Tags are
     * monotonically allocated from 1, so a tag {@code t} is "known" iff
     * {@code 1 <= t <= currentTagCounter()}. Used by callers (notably
     * {@link WsAckHandler}) to distinguish unknown tags (PRECONDITION_FAILED) from
     * already-transitioned tags (idempotent success) per design doc §5.9.
     */
    public long currentTagCounter() {
        return tagCounter.get();
    }
}
