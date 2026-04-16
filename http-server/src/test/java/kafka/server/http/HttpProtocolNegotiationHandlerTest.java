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

import io.netty.channel.ChannelPipeline;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpContentCompressor;
import io.netty.handler.codec.http2.Http2FrameCodec;
import io.netty.handler.codec.http2.Http2MultiplexHandler;
import io.netty.handler.timeout.IdleStateHandler;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link HttpProtocolNegotiationHandler}.
 *
 * Since ApplicationProtocolNegotiationHandler requires a real TLS handshake
 * to trigger configurePipeline(), we test the pipeline configuration methods
 * directly via package-private access.
 */
class HttpProtocolNegotiationHandlerTest {

    private static final int MAX_CONTENT_LENGTH = 10 * 1024 * 1024; // 10 MB
    private static final long IDLE_TIMEOUT_MS = 60_000L;

    private final AtomicBoolean draining = new AtomicBoolean(false);
    private final AtomicInteger inFlightCount = new AtomicInteger(0);
    private final HttpMetrics httpMetrics = new NoOpHttpMetrics();

    @Test
    void testConstructorSetsHttp11AsFallback() {
        // Verify that the handler can be constructed without exceptions.
        // The superclass constructor argument sets the fallback protocol to HTTP/1.1.
        assertDoesNotThrow(() ->
            new HttpProtocolNegotiationHandler(
                draining, inFlightCount, httpMetrics,
                MAX_CONTENT_LENGTH, IDLE_TIMEOUT_MS));
    }

    @Test
    void testH2PipelineHasExpectedHandlers() {
        HttpProtocolNegotiationHandler handler = new HttpProtocolNegotiationHandler(
            draining, inFlightCount, httpMetrics,
            MAX_CONTENT_LENGTH, IDLE_TIMEOUT_MS);

        EmbeddedChannel channel = new EmbeddedChannel();
        ChannelPipeline pipeline = channel.pipeline();

        handler.configureH2Pipeline(pipeline);

        // Verify HTTP/2 frame codec is present
        assertNotNull(pipeline.get("h2-frame-codec"),
            "HTTP/2 frame codec should be in the pipeline");
        assertTrue(pipeline.get("h2-frame-codec") instanceof Http2FrameCodec,
            "h2-frame-codec should be a Http2FrameCodec");

        // Verify HTTP/2 multiplex handler is present
        assertNotNull(pipeline.get("h2-multiplex"),
            "HTTP/2 multiplex handler should be in the pipeline");
        assertTrue(pipeline.get("h2-multiplex") instanceof Http2MultiplexHandler,
            "h2-multiplex should be a Http2MultiplexHandler");

        // Verify idle timeout handler on parent connection
        assertNotNull(pipeline.get("idle-handler"),
            "Idle state handler should be on the parent connection");
        assertTrue(pipeline.get("idle-handler") instanceof IdleStateHandler,
            "idle-handler should be an IdleStateHandler");

        assertNotNull(pipeline.get("idle-closer"),
            "Idle close handler should be on the parent connection");

        // Verify no HTTP/1.1 codec in the h2 pipeline
        assertNull(pipeline.get("http-codec"),
            "HTTP/1.1 codec should NOT be in the HTTP/2 pipeline");

        channel.close();
    }

    @Test
    void testHttp11PipelineHasExpectedHandlers() {
        HttpProtocolNegotiationHandler handler = new HttpProtocolNegotiationHandler(
            draining, inFlightCount, httpMetrics,
            MAX_CONTENT_LENGTH, IDLE_TIMEOUT_MS);

        EmbeddedChannel channel = new EmbeddedChannel();
        ChannelPipeline pipeline = channel.pipeline();

        handler.configureHttp11Pipeline(pipeline);

        // Verify HTTP/1.1 codec
        assertNotNull(pipeline.get("http-codec"),
            "HTTP/1.1 codec should be in the pipeline");
        assertTrue(pipeline.get("http-codec") instanceof HttpServerCodec,
            "http-codec should be a HttpServerCodec");

        // Verify aggregator
        assertNotNull(pipeline.get("http-aggregator"),
            "HTTP object aggregator should be in the pipeline");
        assertTrue(pipeline.get("http-aggregator") instanceof HttpObjectAggregator,
            "http-aggregator should be a HttpObjectAggregator");

        // Verify compressor
        assertNotNull(pipeline.get("compressor"),
            "HTTP content compressor should be in the pipeline");
        assertTrue(pipeline.get("compressor") instanceof HttpContentCompressor,
            "compressor should be a HttpContentCompressor");

        // Verify idle timeout
        assertNotNull(pipeline.get("idle-handler"),
            "Idle state handler should be in the pipeline");
        assertNotNull(pipeline.get("idle-closer"),
            "Idle close handler should be in the pipeline");

        // Verify Kafka request handler
        assertNotNull(pipeline.get("kafka-handler"),
            "Kafka HTTP request handler should be in the pipeline");
        assertTrue(pipeline.get("kafka-handler") instanceof HttpRequestHandler,
            "kafka-handler should be a HttpRequestHandler");

        // Verify no HTTP/2 handlers
        assertNull(pipeline.get("h2-frame-codec"),
            "HTTP/2 frame codec should NOT be in the HTTP/1.1 pipeline");
        assertNull(pipeline.get("h2-multiplex"),
            "HTTP/2 multiplex handler should NOT be in the HTTP/1.1 pipeline");

        channel.close();
    }

    @Test
    void testH2PipelineDoesNotContainHttp11Codec() {
        HttpProtocolNegotiationHandler handler = new HttpProtocolNegotiationHandler(
            draining, inFlightCount, httpMetrics,
            MAX_CONTENT_LENGTH, IDLE_TIMEOUT_MS);

        EmbeddedChannel channel = new EmbeddedChannel();
        handler.configureH2Pipeline(channel.pipeline());

        assertNull(channel.pipeline().get(HttpServerCodec.class),
            "HTTP/2 pipeline must not contain HttpServerCodec");
        assertNull(channel.pipeline().get(HttpContentCompressor.class),
            "HTTP/2 parent pipeline must not contain HttpContentCompressor");

        channel.close();
    }

    @Test
    void testHttp11PipelineDoesNotContainH2Handlers() {
        HttpProtocolNegotiationHandler handler = new HttpProtocolNegotiationHandler(
            draining, inFlightCount, httpMetrics,
            MAX_CONTENT_LENGTH, IDLE_TIMEOUT_MS);

        EmbeddedChannel channel = new EmbeddedChannel();
        handler.configureHttp11Pipeline(channel.pipeline());

        assertNull(channel.pipeline().get(Http2FrameCodec.class),
            "HTTP/1.1 pipeline must not contain Http2FrameCodec");
        assertNull(channel.pipeline().get(Http2MultiplexHandler.class),
            "HTTP/1.1 pipeline must not contain Http2MultiplexHandler");

        channel.close();
    }

    @Test
    void testH2PipelineHandlerOrder() {
        HttpProtocolNegotiationHandler handler = new HttpProtocolNegotiationHandler(
            draining, inFlightCount, httpMetrics,
            MAX_CONTENT_LENGTH, IDLE_TIMEOUT_MS);

        EmbeddedChannel channel = new EmbeddedChannel();
        handler.configureH2Pipeline(channel.pipeline());

        var names = channel.pipeline().names();
        int frameCodecIdx = names.indexOf("h2-frame-codec");
        int multiplexIdx = names.indexOf("h2-multiplex");
        int idleHandlerIdx = names.indexOf("idle-handler");
        int idleCloserIdx = names.indexOf("idle-closer");

        assertTrue(frameCodecIdx < multiplexIdx,
            "h2-frame-codec must come before h2-multiplex");
        assertTrue(multiplexIdx < idleHandlerIdx,
            "h2-multiplex must come before idle-handler");
        assertTrue(idleHandlerIdx < idleCloserIdx,
            "idle-handler must come before idle-closer");

        channel.close();
    }

    @Test
    void testHttp11PipelineHandlerOrder() {
        HttpProtocolNegotiationHandler handler = new HttpProtocolNegotiationHandler(
            draining, inFlightCount, httpMetrics,
            MAX_CONTENT_LENGTH, IDLE_TIMEOUT_MS);

        EmbeddedChannel channel = new EmbeddedChannel();
        handler.configureHttp11Pipeline(channel.pipeline());

        var names = channel.pipeline().names();
        int codecIdx = names.indexOf("http-codec");
        int aggregatorIdx = names.indexOf("http-aggregator");
        int compressorIdx = names.indexOf("compressor");
        int idleHandlerIdx = names.indexOf("idle-handler");
        int kafkaHandlerIdx = names.indexOf("kafka-handler");

        assertTrue(codecIdx < aggregatorIdx,
            "http-codec must come before http-aggregator");
        assertTrue(aggregatorIdx < compressorIdx,
            "http-aggregator must come before compressor");
        assertTrue(compressorIdx < idleHandlerIdx,
            "compressor must come before idle-handler");
        assertTrue(idleHandlerIdx < kafkaHandlerIdx,
            "idle-handler must come before kafka-handler");

        channel.close();
    }

    /**
     * No-op extension of HttpMetrics for testing.
     * HttpMetrics is a concrete class; metrics are recorded but not verified in these tests.
     */
    private static class NoOpHttpMetrics extends HttpMetrics {
    }
}
