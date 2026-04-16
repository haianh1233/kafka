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

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpContentCompressor;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http2.Http2FrameCodecBuilder;
import io.netty.handler.codec.http2.Http2MultiplexHandler;
import io.netty.handler.codec.http2.Http2StreamFrameToHttpObjectCodec;
import io.netty.handler.ssl.ApplicationProtocolNames;
import io.netty.handler.ssl.ApplicationProtocolNegotiationHandler;
import io.netty.handler.timeout.IdleStateHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * ALPN-based protocol negotiation handler for HTTPS listeners.
 *
 * After TLS handshake, inspects the negotiated ALPN protocol:
 * <ul>
 *   <li>{@code "h2"}       -- configures HTTP/2 pipeline with {@link Http2MultiplexHandler}</li>
 *   <li>{@code "http/1.1"} -- configures standard HTTP/1.1 pipeline</li>
 *   <li>fallback            -- HTTP/1.1</li>
 * </ul>
 *
 * HTTP/2 uses child channels per stream, so each stream gets its own
 * request handler instance via the supplied factory. HTTP/1.1 is the same
 * as the existing pipeline from TASK-B.03.
 *
 * // Time: Modified - TASK-0.02 (use handler factory instead of direct HttpRequestHandler construction)
 */
public class HttpProtocolNegotiationHandler extends ApplicationProtocolNegotiationHandler {

    private static final Logger log = LoggerFactory.getLogger(HttpProtocolNegotiationHandler.class);

    private final AtomicBoolean draining;
    private final AtomicInteger inFlightCount;
    private final HttpMetrics httpMetrics;
    private final int maxContentLength;
    private final long connectionIdleTimeoutMs;
    private final Supplier<ChannelHandler> requestHandlerFactory;

    /**
     * Create a new ALPN-based protocol negotiation handler with a request handler factory.
     *
     * @param draining                flag indicating whether the server is draining
     * @param inFlightCount           counter tracking in-flight requests
     * @param httpMetrics             metrics recorder for the HTTP layer
     * @param maxContentLength        maximum allowed HTTP request body size in bytes
     * @param connectionIdleTimeoutMs idle timeout for connections in milliseconds
     * @param requestHandlerFactory   factory that creates a new ChannelHandler for each pipeline
     */
    public HttpProtocolNegotiationHandler(
            AtomicBoolean draining,
            AtomicInteger inFlightCount,
            HttpMetrics httpMetrics,
            int maxContentLength,
            long connectionIdleTimeoutMs,
            Supplier<ChannelHandler> requestHandlerFactory) {
        super(ApplicationProtocolNames.HTTP_1_1);  // fallback protocol
        this.draining = draining;
        this.inFlightCount = inFlightCount;
        this.httpMetrics = httpMetrics;
        this.maxContentLength = maxContentLength;
        this.connectionIdleTimeoutMs = connectionIdleTimeoutMs;
        this.requestHandlerFactory = requestHandlerFactory;
    }

    /**
     * Backward-compatible constructor without request handler factory.
     * The factory defaults to throwing UnsupportedOperationException.
     *
     * @param draining                flag indicating whether the server is draining
     * @param inFlightCount           counter tracking in-flight requests
     * @param httpMetrics             metrics recorder for the HTTP layer
     * @param maxContentLength        maximum allowed HTTP request body size in bytes
     * @param connectionIdleTimeoutMs idle timeout for connections in milliseconds
     */
    public HttpProtocolNegotiationHandler(
            AtomicBoolean draining,
            AtomicInteger inFlightCount,
            HttpMetrics httpMetrics,
            int maxContentLength,
            long connectionIdleTimeoutMs) {
        this(draining, inFlightCount, httpMetrics, maxContentLength, connectionIdleTimeoutMs,
            () -> {
                throw new UnsupportedOperationException(
                    "requestHandlerFactory must be provided");
            });
    }

    @Override
    protected void configurePipeline(ChannelHandlerContext ctx, String protocol) throws Exception {
        ChannelPipeline pipeline = ctx.pipeline();

        if (ApplicationProtocolNames.HTTP_2.equals(protocol)) {
            log.debug("ALPN negotiated HTTP/2 for channel {}", ctx.channel());
            configureH2Pipeline(pipeline);
        } else if (ApplicationProtocolNames.HTTP_1_1.equals(protocol)) {
            log.debug("ALPN negotiated HTTP/1.1 for channel {}", ctx.channel());
            configureHttp11Pipeline(pipeline);
        } else {
            throw new IllegalStateException("Unknown ALPN protocol: " + protocol);
        }
    }

    /**
     * Configure HTTP/2 pipeline.
     *
     * Uses {@link Http2MultiplexHandler} which creates a child channel per stream.
     * Each child channel gets its own request handler via the factory.
     */
    void configureH2Pipeline(ChannelPipeline pipeline) {
        pipeline.addLast("h2-frame-codec",
            Http2FrameCodecBuilder.forServer().build());

        pipeline.addLast("h2-multiplex",
            new Http2MultiplexHandler(new ChannelInitializer<>() {
                @Override
                protected void initChannel(io.netty.channel.Channel ch) {
                    ChannelPipeline streamPipeline = ch.pipeline();
                    streamPipeline.addLast("h2-to-http",
                        new Http2StreamFrameToHttpObjectCodec(true));
                    streamPipeline.addLast("http-aggregator",
                        new HttpObjectAggregator(maxContentLength));
                    streamPipeline.addLast("kafka-handler",
                        requestHandlerFactory.get());
                }
            }));

        pipeline.addLast("idle-handler", new IdleStateHandler(
            0, 0, connectionIdleTimeoutMs, TimeUnit.MILLISECONDS));
        pipeline.addLast("idle-closer", new IdleStateCloseHandler(httpMetrics));
    }

    /**
     * Configure HTTP/1.1 pipeline.
     */
    void configureHttp11Pipeline(ChannelPipeline pipeline) {
        pipeline.addLast("http-codec", new HttpServerCodec());
        pipeline.addLast("http-aggregator",
            new HttpObjectAggregator(maxContentLength));
        pipeline.addLast("compressor", new HttpContentCompressor());
        pipeline.addLast("idle-handler", new IdleStateHandler(
            0, 0, connectionIdleTimeoutMs, TimeUnit.MILLISECONDS));
        pipeline.addLast("idle-closer", new IdleStateCloseHandler(httpMetrics));
        pipeline.addLast("kafka-handler",
            requestHandlerFactory.get());
    }
}
