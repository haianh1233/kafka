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
import org.apache.kafka.common.Endpoint

/**
 * Abstract contract for HTTP/HTTPS acceptors.
 *
 * SocketServer references this trait for lifecycle management of HTTP
 * listeners. The concrete Netty-based implementation lives in the
 * http-server module ({@code kafka.network.HttpAcceptor}).
 *
 * This trait exists in core because SocketServer (core) cannot depend
 * on http-server (which already depends on core).
 *
 * // Time: Created - TASK-B.03
 * // Time: Modified - TASK-0.02 (converted from concrete stub to abstract trait)
 * // Time: Modified - TASK-B.06 (added setRequestChannel + setMetadataSupplier for pipeline wiring)
 */
trait HttpAcceptorLike {

  /** The endpoint this acceptor is bound to. */
  def endpoint: Endpoint

  /** Future that completes when startup succeeds (or fails). */
  def startedFuture: CompletableFuture[Void]

  /** Start the HTTP server and bind to the endpoint port. */
  def startup(): Unit

  /**
   * Begin draining in-flight HTTP requests. New requests are rejected
   * with 503 Service Unavailable. Idempotent.
   */
  def beginDrain(): Unit

  /**
   * Block until all in-flight requests are drained or the timeout expires.
   *
   * @param timeoutMs maximum time to wait for drain completion
   */
  def awaitDrain(timeoutMs: Long): Unit

  /** Returns the actual bound port (-1 if not yet started). */
  def boundPort: Int

  /** Close this acceptor and release all resources. */
  def close(): Unit

  /** Whether the acceptor is currently draining. */
  def isDraining: Boolean

  /** Current count of in-flight requests. */
  def pendingRequestCount: Int

  /**
   * Injects the shared RequestChannel so HTTP requests can be enqueued for
   * processing by KafkaApis. Must be called before startup().
   * Default implementation is a no-op for backward compatibility.
   */
  def setRequestChannel(requestChannel: RequestChannel): Unit = {}

  /**
   * Injects a metadata supplier that maps topic names to partition counts.
   * Used by the produce translator for partition assignment.
   * Must be called before startup().
   * Default implementation is a no-op for backward compatibility.
   */
  def setMetadataSupplier(supplier: java.util.function.Function[String, Integer]): Unit = {}

  /**
   * Injects a topic ID supplier that maps topic names to topic UUIDs.
   * Used by share group request translation (ShareFetch requires topic IDs).
   * Must be called before startup().
   * Default implementation is a no-op for backward compatibility.
   */
  def setTopicIdSupplier(supplier: java.util.function.Function[String, org.apache.kafka.common.Uuid]): Unit = {}

  /**
   * T8: inject a Kafka bootstrap servers supplier. Called lazily at first use
   * (after binary listeners have bound so ports are known). Null supplier or
   * empty result disables the WS data plane.
   */
  def setBootstrapServersSupplier(supplier: java.util.function.Supplier[String]): Unit = {}
}
