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
// Time: Created - TASK-WS3.01
package kafka.server.http.ws;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Tracks publisher confirms for a single WebSocket connection.
 *
 * <p>When enabled via {@link #enable()}, the tracker records an in-flight set
 * of publishIds via {@link #recordPending(long)} and emits a {@code published}
 * or {@code publish-failed} JSON frame through the owning
 * {@link WsConnectionContext} when {@link #confirmSuccess(long)} or
 * {@link #confirmFailure(long, String, String)} is called.
 *
 * <p>Confirming a publishId that was never recorded is a no-op — this makes
 * it safe for the produce callback to blindly call confirmSuccess/Failure
 * without first checking whether confirms were enabled at publish time.
 *
 * <h3>Threading</h3>
 * <p>All public methods are safe to call concurrently. {@link #recordPending}
 * runs on the Netty I/O thread (from
 * {@link WsPublishHandler#handlePublish}); the confirm methods run on the
 * produce-completion callback thread (the HTTP async executor in production).
 * Frame writes go through {@link WsConnectionContext#sendFrame(String)} which
 * delegates to Netty's thread-safe {@code writeAndFlush}.
 *
 * // Time: Created - TASK-WS3.01
 */
public final class WsPublisherConfirmTracker {

    private static final Logger log = LoggerFactory.getLogger(WsPublisherConfirmTracker.class);

    /** Shared, thread-safe mapper for frame serialisation. */
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final WsConnectionContext ctx;
    private final AtomicBoolean enabled = new AtomicBoolean(false);

    /**
     * Set of publishIds awaiting a confirm. Using a map (with a constant
     * value) rather than a set because {@link ConcurrentHashMap#newKeySet()}
     * backs onto the same representation and {@code remove(key)} returning
     * the previous value makes the confirm path's "unknown id" guard a
     * single atomic op.
     */
    private final ConcurrentHashMap<Long, Boolean> pending = new ConcurrentHashMap<>();

    public WsPublisherConfirmTracker(WsConnectionContext ctx) {
        this.ctx = Objects.requireNonNull(ctx, "ctx");
    }

    /**
     * Idempotent — subsequent calls are no-ops.
     */
    public void enable() {
        enabled.set(true);
    }

    public boolean isEnabled() {
        return enabled.get();
    }

    /**
     * Record a pending publish. No-op if confirms are not enabled on this
     * connection.
     */
    public void recordPending(long publishId) {
        if (enabled.get()) {
            pending.put(publishId, Boolean.TRUE);
        }
    }

    /**
     * Mark the publish as succeeded, removing it from pending and emitting a
     * {@code {"type":"published","publishId":N}} frame to the client. Confirming
     * a publishId that was never recorded (or that has already been confirmed)
     * is silently ignored.
     */
    public void confirmSuccess(long publishId) {
        if (pending.remove(publishId) == null) {
            return;
        }
        ObjectNode frame = MAPPER.createObjectNode();
        frame.put("type", "published");
        frame.put("publishId", publishId);
        sendFrame(frame);
    }

    /**
     * Mark the publish as failed, removing it from pending and emitting a
     * {@code publish-failed} frame carrying the given error code and message.
     * A {@code null} {@code errorMessage} is serialised as an empty string.
     * Confirming a publishId that was never recorded is silently ignored.
     */
    public void confirmFailure(long publishId, String errorCode, String errorMessage) {
        if (pending.remove(publishId) == null) {
            return;
        }
        ObjectNode frame = MAPPER.createObjectNode();
        frame.put("type", "publish-failed");
        frame.put("publishId", publishId);
        frame.put("errorCode", Objects.requireNonNullElse(errorCode, ""));
        frame.put("errorMessage", Objects.requireNonNullElse(errorMessage, ""));
        sendFrame(frame);
    }

    /** Count of publishIds currently awaiting a confirm. */
    public int pendingCount() {
        return pending.size();
    }

    private void sendFrame(ObjectNode frame) {
        try {
            ctx.sendFrame(MAPPER.writeValueAsString(frame));
        } catch (JsonProcessingException e) {
            // Serialising a freshly-built ObjectNode should never fail; log and
            // move on so we don't propagate exceptions into the produce callback
            // thread.
            log.warn("Failed to serialise publisher-confirm frame for session {}",
                ctx.sessionId(), e);
        }
    }
}
