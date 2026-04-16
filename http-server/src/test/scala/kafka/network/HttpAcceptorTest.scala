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

import org.apache.kafka.network.{HttpServerConfigs => HttpConfigs}
import org.apache.kafka.common.Endpoint
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.MockTime
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.api.Assertions._

/**
 * Tests for HttpAcceptor lifecycle management.
 *
 * // Time: Created - TASK-B.03
 */
class HttpAcceptorTest {

  private var acceptor: HttpAcceptor = _
  private val time = new MockTime()

  @BeforeEach
  def setUp(): Unit = {
    // Create endpoint with port 0 (auto-assign)
    val endpoint = new Endpoint("HTTP", SecurityProtocol.HTTP, "localhost", 0)
    acceptor = new HttpAcceptor(
      endpoint,
      HttpConfigs.NUM_HTTP_NETWORK_THREADS_DEFAULT,
      HttpConfigs.HTTP_REQUEST_MAX_BYTES_DEFAULT,
      HttpConfigs.HTTP_CONNECTION_IDLE_TIMEOUT_MS_DEFAULT,
      time
    )
  }

  @AfterEach
  def tearDown(): Unit = {
    if (acceptor != null) acceptor.close()
  }

  @Test
  def startup_completesStartedFuture(): Unit = {
    acceptor.startup()
    // startedFuture should complete without exception
    assertDoesNotThrow(() => acceptor.startedFuture.get(5, TimeUnit.SECONDS))
  }

  @Test
  def startup_bindsToPort(): Unit = {
    acceptor.startup()
    acceptor.startedFuture.get(5, TimeUnit.SECONDS)

    // The server should be accepting connections
    // We verify this by checking the future completed successfully
    assertTrue(acceptor.startedFuture.isDone)
    assertFalse(acceptor.startedFuture.isCompletedExceptionally)
  }

  @Test
  def close_isIdempotent(): Unit = {
    acceptor.startup()
    acceptor.startedFuture.get(5, TimeUnit.SECONDS)

    // Close multiple times should not throw
    acceptor.close()
    acceptor.close()
    // If we get here without exception, the test passes
  }

  @Test
  def beginDrain_setsAcceptingFalse(): Unit = {
    acceptor.startup()
    acceptor.startedFuture.get(5, TimeUnit.SECONDS)

    assertTrue(acceptor.isAccepting, "Should be accepting before drain")

    acceptor.beginDrain()

    assertFalse(acceptor.isAccepting, "Should not be accepting after drain")
  }

  @Test
  def awaitDrain_returnsImmediately_whenNoPending(): Unit = {
    acceptor.startup()
    acceptor.startedFuture.get(5, TimeUnit.SECONDS)

    val start = System.currentTimeMillis()
    acceptor.awaitDrain(1000)
    val elapsed = System.currentTimeMillis() - start

    // Should return very quickly since there are no pending connections
    assertTrue(elapsed < 500, s"awaitDrain should return immediately when no pending, took ${elapsed}ms")
  }

  @Test
  def pendingCount_incrementAndDecrement(): Unit = {
    acceptor.startup()
    acceptor.startedFuture.get(5, TimeUnit.SECONDS)

    // Increment 3 times, decrement once => 2 pending
    acceptor.incrementPending()
    acceptor.incrementPending()
    acceptor.incrementPending()
    acceptor.decrementPending()

    // awaitDrain with a short timeout should return after timeout (since pending > 0)
    val start = System.currentTimeMillis()
    acceptor.awaitDrain(100)
    val elapsed = System.currentTimeMillis() - start

    // Should have waited approximately the timeout since there are pending connections
    assertTrue(elapsed >= 80, s"awaitDrain should wait when pending > 0, but took only ${elapsed}ms")

    // Clear pending
    acceptor.decrementPending()
    acceptor.decrementPending()

    // Now awaitDrain should return immediately
    val start2 = System.currentTimeMillis()
    acceptor.awaitDrain(1000)
    val elapsed2 = System.currentTimeMillis() - start2
    assertTrue(elapsed2 < 500, s"awaitDrain should return immediately when no pending, took ${elapsed2}ms")
  }

  @Test
  def isAccepting_defaultsToTrue(): Unit = {
    assertTrue(acceptor.isAccepting, "Should default to accepting")
  }
}
