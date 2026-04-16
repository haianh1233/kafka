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

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import kafka.network.RequestChannel;
import kafka.network.ResponseProcessor;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.AbstractResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Response routing bridge between {@link RequestChannel} and Netty.
 *
 * Mirrors the binary protocol's {@code Processor} response handling pattern
 * (SocketServer.scala lines 935-973) but routes to Netty channels instead of
 * NIO channels.
 *
 * <h3>Threading model:</h3>
 * <ul>
 *   <li>{@link #enqueueResponse} is called from KafkaRequestHandler threads (via RequestChannel)</li>
 *   <li>{@link #processResponses} is called from a dedicated response-drainer thread</li>
 *   <li>{@link #registerChannel} is called from Netty worker threads</li>
 * </ul>
 *
 * All three paths are thread-safe via ConcurrentHashMap and LinkedBlockingDeque.
 *
 * <h3>Response type handling:</h3>
 * <ul>
 *   <li>{@code SendResponse} -> serialize response data, write to Netty channel</li>
 *   <li>{@code CloseConnectionResponse} -> close the Netty channel</li>
 *   <li>{@code StartThrottlingResponse} -> HTTP 429 + Retry-After header</li>
 *   <li>{@code EndThrottlingResponse} -> no-op (HTTP has no mute/unmute)</li>
 * </ul>
 *
 * // Time: Created - TASK-B.04
 */
public class HttpProcessor implements ResponseProcessor {

    private static final Logger log = LoggerFactory.getLogger(HttpProcessor.class);

    // --- Processor ID (registered with RequestChannel) ---
    private final int id;

    // --- In-flight request count (shared with HttpRequestHandler for drain coordination) ---
    private final AtomicInteger inFlightCount;

    // --- Response queue (unbounded, non-blocking add) ---
    private final LinkedBlockingDeque<RequestChannel.Response> responseQueue =
        new LinkedBlockingDeque<>();

    // --- Active Netty channels: connectionId -> ChannelHandlerContext ---
    private final ConcurrentHashMap<String, ChannelHandlerContext> channels =
        new ConcurrentHashMap<>();

    // --- Drainer thread state ---
    private final AtomicBoolean running = new AtomicBoolean(false);
    private volatile Thread drainerThread;

    // --- Polling interval for the drainer thread when queue is empty ---
    static final long POLL_INTERVAL_MS = 10;

    // --- Throttle response JSON template ---
    private static final String THROTTLE_BODY_TEMPLATE =
        "{\"errorCode\":%d,\"errorMessage\":\"%s\",\"throttleTimeMs\":%d}";

    // --- HTTP 429 status ---
    private static final HttpResponseStatus TOO_MANY_REQUESTS =
        HttpResponseStatus.valueOf(429, "Too Many Requests");

    // --- Throttle error code (THROTTLING_QUOTA_EXCEEDED = 89) ---
    private static final int THROTTLE_ERROR_CODE = 89;
    private static final String THROTTLE_ERROR_MESSAGE = "THROTTLING_QUOTA_EXCEEDED";

    /**
     * Creates a new HttpProcessor with a shared in-flight counter.
     *
     * @param id            unique processor ID for RequestChannel registration
     * @param inFlightCount shared counter for tracking in-flight requests (owned by HttpAcceptor)
     */
    public HttpProcessor(int id, AtomicInteger inFlightCount) {
        this.id = id;
        this.inFlightCount = Objects.requireNonNull(inFlightCount, "inFlightCount");
    }

    /**
     * Creates a new HttpProcessor with its own in-flight counter.
     * Convenience constructor for tests and backward compatibility.
     *
     * @param id unique processor ID for RequestChannel registration
     */
    public HttpProcessor(int id) {
        this(id, new AtomicInteger(0));
    }

    /**
     * Returns the processor ID used by RequestChannel for response routing.
     */
    public int id() {
        return id;
    }

    /**
     * Registers a Netty channel for a connection. Called by HttpRequestHandler
     * when a new HTTP request is received. A close listener automatically removes
     * the entry when the channel closes (client disconnect).
     *
     * @param connectionId Netty channel long text ID (ctx.channel().id().asLongText())
     * @param ctx          the Netty ChannelHandlerContext for writing responses
     */
    public void registerChannel(String connectionId, ChannelHandlerContext ctx) {
        Objects.requireNonNull(connectionId, "connectionId");
        Objects.requireNonNull(ctx, "ctx");

        channels.put(connectionId, ctx);

        // Add close listener to auto-remove the channel entry on disconnect
        ctx.channel().closeFuture().addListener((ChannelFutureListener) future ->
            channels.remove(connectionId));
    }

    /**
     * Unregisters a Netty channel for a connection. Called by HttpRequestHandler
     * when enqueue fails and the channel registration needs to be cleaned up.
     *
     * @param connectionId Netty channel long text ID
     */
    public void unregisterChannel(String connectionId) {
        channels.remove(connectionId);
    }

    /**
     * Enqueues a response for delivery to the Netty channel.
     * Non-blocking — called from KafkaRequestHandler threads via RequestChannel.sendResponse().
     *
     * @param response the response to deliver
     */
    public void enqueueResponse(RequestChannel.Response response) {
        Objects.requireNonNull(response, "response");
        responseQueue.add(response);  // non-blocking on unbounded deque
    }

    /**
     * Drains and processes all queued responses. For each response:
     * - Looks up the Netty channel by connectionId
     * - Dispatches based on response type (SendResponse, CloseConnection, etc.)
     * - Handles exceptions per-response to prevent one failure from blocking others
     *
     * Called by the response-drainer thread.
     */
    public void processResponses() {
        List<RequestChannel.Response> batch = new ArrayList<>();
        responseQueue.drainTo(batch);

        for (RequestChannel.Response response : batch) {
            String connectionId = response.request().context().connectionId();
            try {
                if (response instanceof RequestChannel.SendResponse) {
                    handleSendResponse((RequestChannel.SendResponse) response, connectionId);
                } else if (response instanceof RequestChannel.NoOpResponse) {
                    sendNoContentResponse(connectionId);
                    safeUpdateMetrics(response);
                } else if (response instanceof RequestChannel.CloseConnectionResponse) {
                    ChannelHandlerContext ctx = channels.remove(connectionId);
                    if (ctx != null) {
                        ctx.close();
                    }
                    inFlightCount.decrementAndGet();
                } else if (response instanceof RequestChannel.StartThrottlingResponse) {
                    handleStartThrottling(response, connectionId);
                } else if (response instanceof RequestChannel.EndThrottlingResponse) {
                    // No-op for HTTP — no mute/unmute mechanism
                }
            } catch (Exception e) {
                log.error("Error processing response for connection {}", connectionId, e);
            }
        }
    }

    /**
     * Handles a SendResponse by serializing to HTTP and writing to the Netty channel.
     */
    private void handleSendResponse(RequestChannel.SendResponse sendResp, String connectionId) {
        ChannelHandlerContext ctx = channels.get(connectionId);
        if (ctx != null && ctx.channel().isActive()) {
            FullHttpResponse httpResponse = buildHttpResponse(sendResp, connectionId);

            // Section 12.3: If throttleTimeMs > 0, add Retry-After header
            long throttleTimeMs = sendResp.request().apiThrottleTimeMs();
            if (throttleTimeMs > 0) {
                int retryAfterSeconds = Math.max(1, (int) Math.ceil(throttleTimeMs / 1000.0));
                httpResponse.headers().setInt(HttpHeaderNames.RETRY_AFTER, retryAfterSeconds);
            }

            ctx.writeAndFlush(httpResponse).addListener(f -> inFlightCount.decrementAndGet());
        } else {
            log.debug("Channel closed before response could be sent: {}", connectionId);
            inFlightCount.decrementAndGet();
        }
        safeUpdateMetrics(sendResp);
        channels.remove(connectionId);
    }

    /**
     * Builds a FullHttpResponse from a SendResponse.
     */
    private FullHttpResponse buildHttpResponse(RequestChannel.SendResponse sendResp, String connectionId) {
        ApiKeys apiKey = sendResp.request().header().apiKey();
        String requestId = connectionId + "-" + sendResp.request().header().correlationId();

        java.util.concurrent.ConcurrentHashMap<String, Object> props =
            sendResp.request().requestLocalProperties();
        if (props != null) {
            Object storedResponse = props.get("httpAbstractResponse");
            if (storedResponse instanceof AbstractResponse) {
                AbstractResponse abstractResponse = (AbstractResponse) storedResponse;
                Object maxWaitObj = props.get("httpMaxWaitApplied");
                int effectiveMaxWaitMs = maxWaitObj instanceof Integer ? (Integer) maxWaitObj : -1;
                return HttpResponseSerializer.serialize(abstractResponse, requestId, apiKey, effectiveMaxWaitMs);
            }
        }
        // Fallback: use response log or minimal response
        byte[] body = serializeSendResponse(sendResp);
        FullHttpResponse httpResponse = new DefaultFullHttpResponse(
            HttpVersion.HTTP_1_1, HttpResponseStatus.OK, Unpooled.wrappedBuffer(body));
        httpResponse.headers().set(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON);
        httpResponse.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, body.length);
        return httpResponse;
    }

    /**
     * Handles a StartThrottlingResponse by sending 429 to the client.
     */
    private void handleStartThrottling(RequestChannel.Response response, String connectionId) {
        ChannelHandlerContext ctx = channels.get(connectionId);
        if (ctx != null && ctx.channel().isActive()) {
            long throttleTimeMs = response.request().apiThrottleTimeMs();
            sendThrottleResponse(ctx, throttleTimeMs);
        }
        inFlightCount.decrementAndGet();
        channels.remove(connectionId);
    }

    /**
     * Safely updates request metrics, catching any exceptions to avoid
     * breaking the response flow.
     */
    private void safeUpdateMetrics(RequestChannel.Response response) {
        try {
            response.request().responseDequeueTimeNanos_$eq(
                org.apache.kafka.common.utils.Time.SYSTEM.nanoseconds());
            response.request().updateRequestMetrics(0L, response);
        } catch (Exception e) {
            log.debug("Failed to update request metrics: {}", e.getMessage());
        }
    }

    /**
     * Serializes a SendResponse into a JSON byte array for the HTTP response body.
     * This is a preliminary implementation — the full HttpResponseSerializer (future task)
     * will handle proper Kafka response-to-JSON conversion.
     *
     * @param sendResp the SendResponse to serialize
     * @return JSON bytes for the HTTP response body
     */
    private byte[] serializeSendResponse(RequestChannel.SendResponse sendResp) {
        // Use the response log JSON if available (it's a Jackson JsonNode)
        scala.Option<com.fasterxml.jackson.databind.JsonNode> logOpt = sendResp.responseLog();
        if (logOpt.isDefined()) {
            return logOpt.get().toString().getBytes(StandardCharsets.UTF_8);
        }
        // Fallback: minimal JSON indicating the response was sent
        String apiKey = sendResp.request().header().apiKey().name();
        return String.format("{\"apiKey\":\"%s\",\"status\":\"ok\"}", apiKey)
            .getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Sends an HTTP 429 Too Many Requests response for throttled requests.
     *
     * @param ctx            Netty channel context
     * @param throttleTimeMs throttle duration in milliseconds
     */
    private void sendThrottleResponse(ChannelHandlerContext ctx, long throttleTimeMs) {
        String body = String.format(THROTTLE_BODY_TEMPLATE,
            THROTTLE_ERROR_CODE, THROTTLE_ERROR_MESSAGE, throttleTimeMs);
        byte[] bodyBytes = body.getBytes(StandardCharsets.UTF_8);

        FullHttpResponse httpResponse = new DefaultFullHttpResponse(
            HttpVersion.HTTP_1_1,
            TOO_MANY_REQUESTS,
            Unpooled.wrappedBuffer(bodyBytes));

        // Retry-After header: ceiling of throttleTimeMs in seconds, minimum 1
        int retryAfterSeconds = Math.max(1, (int) Math.ceil(throttleTimeMs / 1000.0));
        httpResponse.headers().setInt(HttpHeaderNames.RETRY_AFTER, retryAfterSeconds);
        httpResponse.headers().set(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON);
        httpResponse.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, bodyBytes.length);

        ctx.writeAndFlush(httpResponse);
    }

    /**
     * Sends an HTTP 204 No Content response for NoOp responses (acks=0 produce).
     * HTTP clients need an explicit response; unlike the binary protocol, there's
     * no "mute" mechanism.
     *
     * @param connectionId the connection ID for the channel lookup
     */
    private void sendNoContentResponse(String connectionId) {
        ChannelHandlerContext ctx = channels.get(connectionId);
        if (ctx != null && ctx.channel().isActive()) {
            FullHttpResponse httpResponse = new DefaultFullHttpResponse(
                HttpVersion.HTTP_1_1, HttpResponseStatus.NO_CONTENT);
            httpResponse.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, 0);
            ctx.writeAndFlush(httpResponse).addListener(f -> inFlightCount.decrementAndGet());
        } else {
            inFlightCount.decrementAndGet();
        }
        channels.remove(connectionId);
    }

    /**
     * Starts the response-drainer thread. This thread polls processResponses()
     * in a loop, sleeping briefly between empty polls.
     */
    public void startDrainer() {
        if (!running.compareAndSet(false, true)) {
            log.warn("Drainer thread already running for processor {}", id);
            return;
        }

        drainerThread = new Thread(() -> {
            while (running.get()) {
                try {
                    processResponses();
                    if (responseQueue.isEmpty()) {
                        Thread.sleep(POLL_INTERVAL_MS);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    log.error("Error in HTTP response drainer for processor {}", id, e);
                }
            }
        }, "http-response-drainer-" + id);
        drainerThread.setDaemon(true);
        drainerThread.start();
    }

    /**
     * Shuts down the response-drainer thread and clears all state.
     * This method is idempotent — calling it multiple times is safe.
     */
    public void close() {
        running.set(false);
        Thread thread = drainerThread;
        if (thread != null) {
            thread.interrupt();
            try {
                thread.join(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            drainerThread = null;
        }
        channels.clear();
        responseQueue.clear();
    }

    /**
     * Returns the number of registered (active) channels.
     * Useful for monitoring and drain logic.
     */
    public int channelCount() {
        return channels.size();
    }

    /**
     * Returns the current response queue size.
     * Useful for monitoring.
     */
    public int responseQueueSize() {
        return responseQueue.size();
    }
}
