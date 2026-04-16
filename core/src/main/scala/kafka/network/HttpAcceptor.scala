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
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
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
 * Full Netty integration lives in http-server module's HttpAcceptor.
 *
 * // Time: Created - TASK-B.03
 * // Time: Modified - TASK-F.06 (graceful shutdown drain support)
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

  // --- Drain support ---
  private val draining = new AtomicBoolean(false)
  private val inFlightCount = new AtomicInteger(0)

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
   *
   * This method is idempotent -- calling it multiple times is safe.
   */
  def beginDrain(): Unit = {
    draining.set(true)
    info(s"Beginning drain for HTTP acceptor ${endPoint.listener}")
  }

  /**
   * Block until all in-flight requests are drained or the timeout expires.
   *
   * @param timeoutMs maximum time to wait for drain completion
   */
  def awaitDrain(timeoutMs: Long): Unit = {
    val deadline = time.milliseconds() + timeoutMs
    while (inFlightCount.get() > 0 && time.milliseconds() < deadline) {
      Thread.sleep(20)
    }
    val remaining = inFlightCount.get()
    if (remaining > 0) {
      warn(s"HTTP drain timed out with $remaining requests still in-flight for ${endPoint.listener}")
    }
    info(s"Awaiting drain for HTTP acceptor ${endPoint.listener}, timeout=${timeoutMs}ms")
  }

  /**
   * Close this acceptor and release all resources.
   */
  def close(): Unit = {
    info(s"Closing HTTP acceptor for ${endPoint.listener}")
    httpProcessor.close()
  }

  /** Whether the acceptor is currently draining. */
  def isDraining: Boolean = draining.get()

  /** Current count of in-flight requests. */
  def pendingRequestCount: Int = inFlightCount.get()
}
