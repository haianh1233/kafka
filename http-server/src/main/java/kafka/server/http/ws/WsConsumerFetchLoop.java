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

// Time: Created - TASK-WS1.15
// Time: Update - TASK-WS3.08 - added metrics recording
// Time: Update - TASK-WS4.02 - added per-message TTL check at delivery

package kafka.server.http.ws;

import io.netty.channel.Channel;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Per-subscription WebSocket push-delivery fetch loop.
 *
 * <p>Runs as a {@link Runnable} on the shared {@code wsConsumerExecutor} thread pool
 * (see design doc §21.1). MUST NOT be executed on a Netty worker thread: the loop
 * calls {@link WsCreditManager#awaitCredits} which parks the thread, and would
 * otherwise stall all I/O on that event loop.
 *
 * <p>Loop shape (design doc §9):
 * <pre>
 *   while (active &amp;&amp; channel.isOpen()) {
 *     credits = awaitCredits(timeout)
 *     if (credits &lt;= 0) continue
 *     // (Kafka fetch integration arrives in TASK-WS1.16 — hook lives at doFetchIteration())
 *     // For each returned record: deliverRecord(...)
 *   }
 * </pre>
 *
 * <p>Frame delivery is done via {@link Channel#writeAndFlush} from the consumer
 * executor thread. That is safe: Netty schedules the actual write on the
 * channel's event loop internally (design doc §21.1).
 *
 * <p>Cancellation: {@link #stop} flips an {@link AtomicBoolean} that the loop
 * checks at each iteration. Because {@link WsCreditManager#awaitCredits} returns
 * within its timeout, the loop exits within at most one {@code timeoutMs} after
 * {@code stop()} is called.
 *
 * <p>Per-message TTL check (design doc §12.4) is the caller's responsibility: when
 * a record is known to be expired, the caller simply skips the call to
 * {@link #deliverRecord} while still advancing its own read offset so the record
 * is never redelivered. This class does not reach into record headers directly —
 * it is fed already-decoded payloads by the TASK-WS1.16 integration.
 */
public final class WsConsumerFetchLoop implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(WsConsumerFetchLoop.class);

    /** Credit wait timeout — bounded so cancellation is always observed promptly. */
    static final long CREDIT_WAIT_TIMEOUT_MS = 500L;

    /** Upper bound on records requested per fetch iteration — soft budget. */
    static final int MAX_FETCH_RECORDS = 100;

    private final String subscriptionId;
    private final String topic;
    private final Map<TopicPartition, Long> currentOffsets;
    private final WsCreditManager creditManager;
    private final WsDeliveryTagTracker tagTracker;
    private final Channel channel;
    private final boolean noAck;
    private final WsMetrics metrics;
    private final AtomicBoolean active = new AtomicBoolean(true);

    /**
     * Convenience constructor with no metrics — equivalent to passing {@code null}
     * to the metrics-aware constructor. Used by unit tests that don't care about
     * metric emission.
     */
    public WsConsumerFetchLoop(String subscriptionId,
                               String topic,
                               Map<TopicPartition, Long> startOffsets,
                               WsCreditManager creditManager,
                               WsDeliveryTagTracker tagTracker,
                               Channel channel,
                               boolean noAck) {
        this(subscriptionId, topic, startOffsets, creditManager, tagTracker, channel, noAck, null);
    }

    public WsConsumerFetchLoop(String subscriptionId,
                               String topic,
                               Map<TopicPartition, Long> startOffsets,
                               WsCreditManager creditManager,
                               WsDeliveryTagTracker tagTracker,
                               Channel channel,
                               boolean noAck,
                               WsMetrics metrics) {
        this.subscriptionId = Objects.requireNonNull(subscriptionId, "subscriptionId");
        this.topic = Objects.requireNonNull(topic, "topic");
        Objects.requireNonNull(startOffsets, "startOffsets");
        this.currentOffsets = new ConcurrentHashMap<>(startOffsets);
        this.creditManager = Objects.requireNonNull(creditManager, "creditManager");
        this.tagTracker = Objects.requireNonNull(tagTracker, "tagTracker");
        this.channel = Objects.requireNonNull(channel, "channel");
        this.noAck = noAck;
        this.metrics = metrics; // nullable; null means no-op metric recording
    }

    @Override
    public void run() {
        log.debug("Fetch loop started: sub={} topic={}", subscriptionId, topic);
        try {
            while (active.get() && channel.isOpen()) {
                int budget = creditManager.awaitCredits(CREDIT_WAIT_TIMEOUT_MS);
                if (budget <= 0) {
                    // Either no credits or channel not writable. awaitCredits has already
                    // parked for up to CREDIT_WAIT_TIMEOUT_MS — loop re-checks active flag.
                    // Record credit-exhausted only when credits are the blocker (not when
                    // the channel is simply not writable or we're being shut down).
                    if (metrics != null && active.get() && creditManager.available() == 0) {
                        metrics.creditExhaustedRate.mark();
                    }
                    continue;
                }
                if (!active.get()) {
                    break;
                }

                int fetchBudget = Math.min(budget, MAX_FETCH_RECORDS);
                // Hook: TASK-WS1.16 will wire doFetchIteration into RequestChannel/KafkaApis.
                // Until then, the loop skeleton is safe to run — it will park each iteration
                // in awaitCredits, observe cancellation, and exit.
                doFetchIteration(fetchBudget);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            if (active.get()) {
                log.error("Fetch loop error: sub={}", subscriptionId, e);
            }
        } finally {
            active.set(false);
            log.debug("Fetch loop stopped: sub={}", subscriptionId);
        }
    }

    /**
     * Hook for the real Kafka fetch integration wired in TASK-WS1.16.
     *
     * <p>Until that integration is live, this method is a no-op but must not
     * busy-spin. The outer loop already parks in {@link WsCreditManager#awaitCredits},
     * which costs &lt;= {@link #CREDIT_WAIT_TIMEOUT_MS} per iteration — so this
     * hook being empty does not produce a spin.
     *
     * @param fetchBudget maximum number of records to request this iteration
     *                    (subclass override wiring will honour this when calling
     *                    into RequestChannel)
     * @throws InterruptedException if interrupted during a blocking fetch
     */
    protected void doFetchIteration(int fetchBudget) throws InterruptedException {
        // Intentionally empty — TASK-WS1.16 wires the fetch integration.
    }

    /**
     * Delivers a single Kafka record as a {@code deliver} WebSocket frame.
     *
     * <p>Steps:
     * <ol>
     *   <li>Assign a delivery tag via the tracker (or {@code 0} in no-ack mode).</li>
     *   <li>Write a {@link TextWebSocketFrame} to the channel via
     *       {@link Channel#writeAndFlush}.</li>
     *   <li>Consume one credit.</li>
     *   <li>Advance the per-partition offset watermark to {@code offset + 1}.</li>
     * </ol>
     *
     * <p>The {@code messageJson} parameter is inserted raw into the frame so callers
     * can forward already-serialised JSON produced by {@code WsMessageDeserializer}
     * without a redundant parse/serialise round-trip.
     *
     * @param tp           topic-partition of the record
     * @param offset       record offset
     * @param exchange     exchange name recorded by the publisher (or
     *                     {@code "(default)"} for direct-to-queue publish)
     * @param routingKey   routing key recorded by the publisher
     * @param messageJson  message payload as pre-serialised JSON
     * @param redelivered  whether this record is a redelivery
     */
    void deliverRecord(TopicPartition tp,
                       long offset,
                       String exchange,
                       String routingKey,
                       String messageJson,
                       boolean redelivered) {
        long tag = noAck ? 0L : tagTracker.assign(tp, offset);

        String frame = String.format(
            "{\"type\":\"deliver\",\"subscriptionId\":\"%s\",\"deliveryTag\":%d,"
                + "\"redelivered\":%b,\"exchange\":\"%s\",\"routingKey\":\"%s\","
                + "\"message\":%s,\"partition\":%d,\"offset\":%d}",
            subscriptionId, tag, redelivered,
            exchange, routingKey, messageJson,
            tp.partition(), offset);

        channel.writeAndFlush(new TextWebSocketFrame(frame));
        creditManager.consume();
        currentOffsets.put(tp, offset + 1);
        if (metrics != null) {
            metrics.deliverRate.mark();
            if (redelivered) {
                metrics.redeliveryRate.mark();
            }
        }
    }

    /**
     * Delivery entry point that applies the per-message TTL check
     * (design doc §12.4) before handing off to {@link #deliverRecord}.
     *
     * <p>If the record has a {@code _ws_expiration} header and the record is
     * older than the TTL, the record is <b>silently skipped</b>: no deliver
     * frame is written, no credit is consumed, no delivery tag is assigned,
     * but the per-partition offset watermark still advances so the record
     * will not be redelivered on the next fetch. The WS4.01 dead-letter
     * integration will eventually route expired records to the configured
     * DLX before skipping — that integration lives on top of this method.
     *
     * <p>Non-expired records are delivered exactly as through
     * {@link #deliverRecord}. The TTL check is cheap: a single iteration
     * over the record's headers looking for {@code _ws_expiration}.
     *
     * @param tp               topic-partition of the record
     * @param offset           record offset
     * @param exchange         exchange name recorded by the publisher
     * @param routingKey       routing key recorded by the publisher
     * @param messageJson      message payload as pre-serialised JSON
     * @param redelivered      whether this record is a redelivery
     * @param headers          Kafka record headers (non-null; may be empty)
     * @param recordTimestampMs  Kafka record timestamp (ms since epoch)
     * @param nowMs            wall-clock "now" in ms since epoch
     * @return {@code true} if the record was delivered, {@code false} if it
     *         was skipped as expired (caller may emit a drop metric)
     */
    boolean maybeDeliverRecord(TopicPartition tp,
                               long offset,
                               String exchange,
                               String routingKey,
                               String messageJson,
                               boolean redelivered,
                               Iterable<Header> headers,
                               long recordTimestampMs,
                               long nowMs) {
        Iterable<Header> safeHeaders = headers == null ? Collections.emptyList() : headers;
        if (WsMessageTtl.isExpired(safeHeaders, recordTimestampMs, nowMs)) {
            // Expired: do not deliver. Do not consume a credit. Do not assign
            // a tag. But DO advance the watermark so the next fetch skips this
            // record. WS4.01 will hook dead-letter routing in here.
            if (log.isDebugEnabled()) {
                log.debug("Skipping expired record: sub={} tp={} offset={} ts={} now={}",
                    subscriptionId, tp, offset, recordTimestampMs, nowMs);
            }
            currentOffsets.put(tp, offset + 1);
            return false;
        }
        deliverRecord(tp, offset, exchange, routingKey, messageJson, redelivered);
        return true;
    }

    /**
     * Convenience overload that uses {@link System#currentTimeMillis()} as the
     * reference "now" — the production path. Tests inject an explicit clock
     * via {@link #maybeDeliverRecord(TopicPartition, long, String, String, String, boolean, Iterable, long, long)}.
     */
    boolean maybeDeliverRecord(TopicPartition tp,
                               long offset,
                               String exchange,
                               String routingKey,
                               String messageJson,
                               boolean redelivered,
                               Iterable<Header> headers,
                               long recordTimestampMs) {
        return maybeDeliverRecord(tp, offset, exchange, routingKey, messageJson,
            redelivered, headers, recordTimestampMs, System.currentTimeMillis());
    }

    /**
     * Signals the loop to stop at the next iteration. Idempotent.
     *
     * <p>The loop observes the flag on the next {@link WsCreditManager#awaitCredits}
     * return, so exit latency is bounded by {@link #CREDIT_WAIT_TIMEOUT_MS}.
     */
    public void stop() {
        active.set(false);
    }

    /**
     * @return {@code true} while the loop is still running or waiting to run.
     *         Becomes {@code false} after {@link #stop} or natural completion.
     */
    public boolean isActive() {
        return active.get();
    }

    /**
     * @return an immutable snapshot of per-partition next-fetch offsets.
     *         Safe to call from any thread.
     */
    public Map<TopicPartition, Long> currentOffsets() {
        return Map.copyOf(currentOffsets);
    }

    /** Subscription identifier — used by subclasses/tests. */
    public String subscriptionId() {
        return subscriptionId;
    }

    /** Kafka topic being fetched — used by subclasses/tests. */
    public String topic() {
        return topic;
    }
}
