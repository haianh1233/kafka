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

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleStateEvent;

/**
 * Closes connections that have been idle beyond the configured timeout.
 * Records idle connection close events via {@link HttpMetrics}.
 */
public class IdleStateCloseHandler extends ChannelDuplexHandler {

    private final HttpMetrics httpMetrics;

    public IdleStateCloseHandler(HttpMetrics httpMetrics) {
        this.httpMetrics = httpMetrics;
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            if (httpMetrics != null) {
                httpMetrics.idleConnectionsClosedRate.mark();
            }
            ctx.close();
        } else {
            super.userEventTriggered(ctx, evt);
        }
    }
}
