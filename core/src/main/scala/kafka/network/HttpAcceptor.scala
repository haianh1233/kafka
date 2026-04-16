/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import java.util.concurrent.CompletableFuture
import kafka.server.KafkaConfig
import kafka.utils.Logging
import org.apache.kafka.common.Endpoint
import org.apache.kafka.common.utils.Time

/**
 * Netty-based acceptor for HTTP/HTTPS listeners. Manages the Netty
 * ServerBootstrap lifecycle and routes incoming HTTP requests through
 * the Kafka request pipeline.
 *
 * Phase 1 stub: minimal implementation for wiring into SocketServer.
 * Full Netty integration will be added in Phase 2.
 *
 * // Time: Created - TASK-B.03
 */
class HttpAcceptor(
  val socketServer: SocketServer,
  val endPoint: Endpoint,
  val config: KafkaConfig,
  val requestChannel: RequestChannel,
  val httpProcessor: HttpProcessor,
  val time: Time
) extends Logging {

  val startedFuture = new CompletableFuture[Void]()

  /**
   * Start the Netty ServerBootstrap and bind to the endpoint port.
   */
  def startup(): Unit = {
    info(s"Starting HTTP acceptor for ${endPoint.listener}:${endPoint.port}")
    httpProcessor.startup()
    startedFuture.complete(null)
    info(s"Started HTTP acceptor for ${endPoint.listener}:${endPoint.port}")
  }

  /**
   * Begin draining in-flight HTTP requests. New requests are rejected
   * with 503 Service Unavailable.
   */
  def beginDrain(): Unit = {
    info(s"Beginning drain for HTTP acceptor ${endPoint.listener}")
  }

  /**
   * Block until all in-flight requests are drained or the timeout expires.
   *
   * @param timeoutMs maximum time to wait for drain completion
   */
  def awaitDrain(timeoutMs: Long): Unit = {
    info(s"Awaiting drain for HTTP acceptor ${endPoint.listener}, timeout=${timeoutMs}ms")
  }

  /**
   * Close this acceptor and release all resources.
   */
  def close(): Unit = {
    info(s"Closing HTTP acceptor for ${endPoint.listener}")
    httpProcessor.close()
  }
}
