/**
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

package kafka.network

import java.util.concurrent.TimeUnit

import io.netty.channel.{Channel, ChannelDuplexHandler, ChannelHandlerContext, ChannelInitializer}
import io.netty.handler.codec.http.{HttpContentCompressor, HttpObjectAggregator, HttpServerCodec}
import io.netty.handler.ssl.SslContext
import io.netty.handler.timeout.{IdleStateEvent, IdleStateHandler}
import kafka.utils.Logging
import org.apache.kafka.common.Endpoint

/**
 * Configures the Netty pipeline for each new HTTP connection.
 *
 * Pipeline order:
 *   [ssl]           - SslHandler (HTTPS only)
 *   http-codec      - HttpServerCodec (HTTP/1.1 request/response encoding/decoding)
 *   http-aggregator - HttpObjectAggregator (aggregate chunked -> FullHttpRequest)
 *   compressor      - HttpContentCompressor (gzip/deflate response compression)
 *   idle-handler    - IdleStateHandler (detect idle connections)
 *   idle-closer     - IdleStateCloseHandler (close on idle timeout)
 *
 * The HttpObjectAggregator is bounded by http.request.max.bytes (default 10 MB).
 * Requests exceeding this limit receive automatic HTTP 413 from Netty.
 *
 * Note: kafka-handler (HttpRequestHandler) is NOT added in this task.
 * It will be fully wired in TASK-B.05.
 *
 * // Time: Created - TASK-B.03
 */
class HttpChannelInitializer(
    endpoint: Endpoint,
    httpRequestMaxBytes: Int,
    httpConnectionIdleTimeoutMs: Long,
    sslContext: Option[SslContext]
) extends ChannelInitializer[Channel] with Logging {

  override def initChannel(ch: Channel): Unit = {
    val pipeline = ch.pipeline()

    // 1. Optional SSL (for HTTPS listeners)
    sslContext.foreach { ctx =>
      pipeline.addLast("ssl", ctx.newHandler(ch.alloc()))
    }

    // 2. HTTP/1.1 codec - decodes HTTP frames, encodes responses
    pipeline.addLast("http-codec", new HttpServerCodec())

    // 3. Aggregate chunked requests into FullHttpRequest
    //    Netty returns 413 automatically if body exceeds limit
    pipeline.addLast("http-aggregator", new HttpObjectAggregator(httpRequestMaxBytes))

    // 4. Response compression (gzip/deflate based on Accept-Encoding)
    pipeline.addLast("compressor", new HttpContentCompressor())

    // 5. Idle connection detection
    //    Uses allIdle timeout - fires if no read OR write for the configured duration
    //    IMPORTANT: use the TimeUnit overload, not the seconds-only constructor
    pipeline.addLast("idle-handler",
      new IdleStateHandler(0, 0, httpConnectionIdleTimeoutMs, TimeUnit.MILLISECONDS))

    // 6. Close channel on idle timeout
    pipeline.addLast("idle-closer", new IdleStateCloseHandler())

    // Note: kafka-handler (HttpRequestHandler) will be added by TASK-B.05
  }
}

/**
 * Closes the channel when Netty's IdleStateHandler fires an IdleStateEvent.
 * This reclaims keep-alive HTTP connections that have been idle longer than
 * http.connection.idle.timeout.ms (default 60 seconds).
 *
 * // Time: Created - TASK-B.03
 */
class IdleStateCloseHandler extends ChannelDuplexHandler with Logging {

  override def userEventTriggered(ctx: ChannelHandlerContext, evt: Any): Unit = {
    evt match {
      case _: IdleStateEvent =>
        debug(s"Closing idle HTTP connection: ${ctx.channel().remoteAddress()}")
        ctx.close()
      case _ =>
        super.userEventTriggered(ctx, evt)
    }
  }
}
