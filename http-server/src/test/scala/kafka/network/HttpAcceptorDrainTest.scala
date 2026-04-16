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

import org.apache.kafka.common.Endpoint
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.MockTime
import org.apache.kafka.network.{HttpServerConfigs => HttpConfigs}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.api.Assertions._

/**
 * Tests for HttpAcceptor graceful shutdown drain support.
 *
 * // Time: Created - TASK-F.06
 */
class HttpAcceptorDrainTest {

  private var acceptor: HttpAcceptor = _
  private val time = new MockTime()

  @BeforeEach
  def setUp(): Unit = {
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
  def testBeginDrainSetsDrainingFlag(): Unit = {
    acceptor.startup()
    acceptor.startedFuture.get(5, TimeUnit.SECONDS)

    assertFalse(acceptor.isDraining, "Should not be draining before beginDrain()")
    assertTrue(acceptor.isAccepting, "Should be accepting before beginDrain()")

    acceptor.beginDrain()

    assertTrue(acceptor.isDraining, "Should be draining after beginDrain()")
    assertFalse(acceptor.isAccepting, "Should not be accepting after beginDrain()")
  }

  @Test
  def testBeginDrainIsIdempotent(): Unit = {
    acceptor.startup()
    acceptor.startedFuture.get(5, TimeUnit.SECONDS)

    // Calling beginDrain multiple times should not throw
    acceptor.beginDrain()
    acceptor.beginDrain()
    acceptor.beginDrain()

    assertTrue(acceptor.isDraining)
  }

  @Test
  def testAwaitDrainReturnsImmediatelyWhenNoInFlight(): Unit = {
    acceptor.startup()
    acceptor.startedFuture.get(5, TimeUnit.SECONDS)

    val startTime = System.currentTimeMillis()
    acceptor.awaitDrain(2000)
    val elapsed = System.currentTimeMillis() - startTime

    assertTrue(elapsed < 100, s"Should return immediately when no in-flight, took ${elapsed}ms")
  }

  @Test
  def testAwaitDrainWaitsForInFlightRequests(): Unit = {
    acceptor.startup()
    acceptor.startedFuture.get(5, TimeUnit.SECONDS)

    // Simulate an in-flight request
    acceptor.incrementPending()
    assertEquals(1, acceptor.pendingRequestCount)

    // Schedule the request to "complete" after 200ms
    new Thread(() => {
      Thread.sleep(200)
      acceptor.decrementPending()
    }).start()

    val startTime = System.currentTimeMillis()
    acceptor.awaitDrain(2000)
    val elapsed = System.currentTimeMillis() - startTime

    assertTrue(elapsed >= 150, s"Should wait for in-flight request, took ${elapsed}ms")
    assertTrue(elapsed < 1000, s"Should not wait too long, took ${elapsed}ms")
    assertEquals(0, acceptor.pendingRequestCount)
  }

  @Test
  def testAwaitDrainTimesOut(): Unit = {
    acceptor.startup()
    acceptor.startedFuture.get(5, TimeUnit.SECONDS)

    // Simulate 3 stuck in-flight requests
    acceptor.incrementPending()
    acceptor.incrementPending()
    acceptor.incrementPending()

    val timeoutMs = 200L
    val startTime = System.currentTimeMillis()
    acceptor.awaitDrain(timeoutMs)
    val elapsed = System.currentTimeMillis() - startTime

    assertTrue(elapsed >= timeoutMs - 50, s"Should wait at least close to the timeout, took ${elapsed}ms")
    assertEquals(3, acceptor.pendingRequestCount, "In-flight count should not change on timeout")
  }

  @Test
  def testInFlightCountTracking(): Unit = {
    // Simulate 3 requests arriving
    acceptor.incrementPending()
    acceptor.incrementPending()
    acceptor.incrementPending()
    assertEquals(3, acceptor.pendingRequestCount)

    // Complete 2 requests
    acceptor.decrementPending()
    acceptor.decrementPending()
    assertEquals(1, acceptor.pendingRequestCount)

    // Complete last request
    acceptor.decrementPending()
    assertEquals(0, acceptor.pendingRequestCount)
  }

  @Test
  def testDrainingFlagDefaultsFalse(): Unit = {
    assertFalse(acceptor.isDraining, "Should default to not draining")
    assertTrue(acceptor.isAccepting, "Should default to accepting")
  }

  @Test
  def testInFlightCountDefaultsToZero(): Unit = {
    assertEquals(0, acceptor.pendingRequestCount, "In-flight count should default to 0")
  }

  @Test
  def testCloseIsIdempotent(): Unit = {
    acceptor.startup()
    acceptor.startedFuture.get(5, TimeUnit.SECONDS)

    // Close multiple times should not throw
    acceptor.close()
    acceptor.close()
    acceptor.close()
    // set to null so tearDown doesn't close again
    acceptor = null
  }

  @Test
  def testCloseWithoutStartup(): Unit = {
    // close() must be safe to call even if startup() was never called
    acceptor.close()
    acceptor = null
  }

  @Test
  def testFullDrainLifecycle(): Unit = {
    acceptor.startup()
    acceptor.startedFuture.get(5, TimeUnit.SECONDS)

    // Simulate 2 in-flight requests
    acceptor.incrementPending()
    acceptor.incrementPending()

    // Begin drain
    acceptor.beginDrain()
    assertTrue(acceptor.isDraining)

    // Complete the requests
    acceptor.decrementPending()
    acceptor.decrementPending()

    // Await drain should return immediately
    val startTime = System.currentTimeMillis()
    acceptor.awaitDrain(2000)
    val elapsed = System.currentTimeMillis() - startTime
    assertTrue(elapsed < 100, s"Drain should complete immediately when all requests are done, took ${elapsed}ms")

    // Close
    acceptor.close()
    acceptor = null
  }

  @Test
  def testDrainingFlagIsVisibleFromAtomicBoolean(): Unit = {
    // Verify the draining AtomicBoolean is accessible for HttpRequestHandler integration
    assertFalse(acceptor.draining.get())
    acceptor.beginDrain()
    assertTrue(acceptor.draining.get())
  }

  @Test
  def testInFlightCountIsVisibleFromAtomicInteger(): Unit = {
    // Verify the inFlightCount AtomicInteger is accessible for HttpProcessor integration
    assertEquals(0, acceptor.inFlightCount.get())
    acceptor.incrementPending()
    assertEquals(1, acceptor.inFlightCount.get())
    acceptor.decrementPending()
    assertEquals(0, acceptor.inFlightCount.get())
  }
}
