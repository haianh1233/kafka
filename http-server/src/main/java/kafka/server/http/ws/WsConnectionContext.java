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
// Time: Created - TASK-WS1.02
package kafka.server.http.ws;

import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Per-WebSocket-connection state. Thread-safe holder accessed from the
 * Netty I/O worker thread, consumer executor threads, and the response
 * drainer thread.
 *
 * <h3>Thread-safety model</h3>
 * <ul>
 *   <li>{@code sessionId}, {@code principal}, {@code vhost}, {@code connectTime},
 *       {@code remoteAddress}: immutable after construction, safe for concurrent reads.</li>
 *   <li>{@code channel}: Netty guarantees thread-safe writes via
 *       {@link ChannelHandlerContext#writeAndFlush(Object)}.</li>
 *   <li>{@code subscriptions}: backed by a {@link ConcurrentHashMap}.</li>
 *   <li>{@code publishConfirmsEnabled}: backed by an {@link AtomicBoolean}.</li>
 * </ul>
 *
 * <p>The {@code subscriptions} map value type is {@link Object} here because
 * the concrete subscription state class is introduced in a later task
 * ({@code WsSubscriptionManager}).</p>
 *
 * // Time: Created - TASK-WS1.02
 */
public final class WsConnectionContext {

    private final String sessionId;
    private final KafkaPrincipal principal;
    private final String vhost;
    private final ChannelHandlerContext channel;
    private final Instant connectTime;
    private final InetSocketAddress remoteAddress;
    private final ConcurrentHashMap<String, Object> subscriptions;
    private final AtomicBoolean publishConfirmsEnabled;

    public WsConnectionContext(
            String sessionId,
            KafkaPrincipal principal,
            String vhost,
            ChannelHandlerContext channel,
            InetSocketAddress remoteAddress) {
        this.sessionId = Objects.requireNonNull(sessionId, "sessionId");
        this.principal = Objects.requireNonNull(principal, "principal");
        this.vhost = Objects.requireNonNull(vhost, "vhost");
        this.channel = Objects.requireNonNull(channel, "channel");
        this.remoteAddress = Objects.requireNonNull(remoteAddress, "remoteAddress");
        this.connectTime = Instant.now();
        this.subscriptions = new ConcurrentHashMap<>();
        this.publishConfirmsEnabled = new AtomicBoolean(false);
    }

    // --- Immutable accessors ---

    public String sessionId() {
        return sessionId;
    }

    public KafkaPrincipal principal() {
        return principal;
    }

    public String vhost() {
        return vhost;
    }

    public ChannelHandlerContext channel() {
        return channel;
    }

    public Instant connectTime() {
        return connectTime;
    }

    public InetSocketAddress remoteAddress() {
        return remoteAddress;
    }

    // --- Mutable state ---

    public ConcurrentHashMap<String, Object> subscriptions() {
        return subscriptions;
    }

    public boolean isPublishConfirmsEnabled() {
        return publishConfirmsEnabled.get();
    }

    /**
     * Idempotent — calling more than once is a no-op.
     */
    public void enablePublishConfirms() {
        publishConfirmsEnabled.set(true);
    }

    // --- Channel operations ---

    /**
     * Write a {@link TextWebSocketFrame} carrying the given JSON payload.
     * Safe to call from any thread: Netty serialises writes onto the channel's
     * event loop.
     */
    public void sendFrame(String jsonFrame) {
        Objects.requireNonNull(jsonFrame, "jsonFrame");
        channel.writeAndFlush(new TextWebSocketFrame(jsonFrame));
    }

    /**
     * Send a {@link CloseWebSocketFrame} with the given close {@code code} and
     * {@code reason}, then close the channel once the frame has been flushed.
     */
    public void close(int code, String reason) {
        Objects.requireNonNull(reason, "reason");
        channel.writeAndFlush(new CloseWebSocketFrame(code, reason))
            .addListener(ChannelFutureListener.CLOSE);
    }

    /**
     * Delegates to the underlying Netty channel's active state.
     */
    public boolean isActive() {
        return channel.channel().isActive();
    }
}
