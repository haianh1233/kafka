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
// Time: Created - TASK-B.05 (stub for HttpRequestHandler dependency; full impl in TASK-B.03)
package kafka.network

import java.util.concurrent.atomic.AtomicInteger

/**
 * Minimal stub for HttpAcceptor providing the interface needed by HttpRequestHandler.
 * The full implementation (Netty ServerBootstrap lifecycle) will be provided by TASK-B.03.
 *
 * Methods used by HttpRequestHandler:
 *   - isAccepting: whether the server is accepting new requests (false during drain)
 *   - incrementPending / decrementPending: track in-flight request count for graceful shutdown
 */
class HttpAcceptor {

  @volatile private var accepting: Boolean = true
  private val pendingCount = new AtomicInteger(0)

  def isAccepting: Boolean = accepting

  def setAccepting(value: Boolean): Unit = {
    accepting = value
  }

  def incrementPending(): Unit = {
    pendingCount.incrementAndGet()
  }

  def decrementPending(): Unit = {
    pendingCount.decrementAndGet()
  }

  def pendingRequests: Int = pendingCount.get()
}
