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

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Netty channel handler that processes HTTP requests and routes them
 * to the Kafka request processing pipeline.
 *
 * This handler is used for both HTTP/1.1 and HTTP/2 pipelines.
 * For HTTP/2, each stream gets its own instance via Http2MultiplexHandler
 * child channels. Http2StreamFrameToHttpObjectCodec converts HTTP/2 frames
 * to standard FullHttpRequest objects, so this handler works unchanged.
 */
public class HttpRequestHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

    private final AtomicBoolean draining;
    private final AtomicInteger inFlightCount;
    private final HttpMetrics httpMetrics;
    private final int maxRequestBytes;

    public HttpRequestHandler(
            AtomicBoolean draining,
            AtomicInteger inFlightCount,
            HttpMetrics httpMetrics,
            int maxRequestBytes) {
        this.draining = draining;
        this.inFlightCount = inFlightCount;
        this.httpMetrics = httpMetrics;
        this.maxRequestBytes = maxRequestBytes;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) {
        if (draining.get()) {
            FullHttpResponse response = new DefaultFullHttpResponse(
                HttpVersion.HTTP_1_1, HttpResponseStatus.SERVICE_UNAVAILABLE);
            ctx.writeAndFlush(response);
            return;
        }

        inFlightCount.incrementAndGet();
        try {
            // Placeholder: actual request routing will be implemented in TASK-B.04
            FullHttpResponse response = new DefaultFullHttpResponse(
                request.protocolVersion(), HttpResponseStatus.OK);
            ctx.writeAndFlush(response);
        } finally {
            inFlightCount.decrementAndGet();
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        ctx.close();
    }
}
