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
// Time: Created - TASK-WS1.17
package kafka.http

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}

import java.net.URI
import java.net.http.WebSocket.Listener
import java.net.http.{HttpClient, WebSocket}
import java.util.concurrent.{CompletableFuture, CompletionStage, LinkedBlockingQueue, TimeUnit}
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

/**
 * WebSocket test client for integration tests. Thin wrapper around the JDK 11+
 * `java.net.http.WebSocket` API. Follows the pattern from [[HttpTestClient]].
 *
 * <p>We intentionally use the JDK client rather than adding a Jetty WebSocket
 * dependency — the JDK client is adequate for integration tests, keeps the
 * dependency footprint minimal, and works identically on every supported JVM.
 *
 * <p>Usage:
 * {{{
 *   val wsClient = new WsTestClient(httpBaseUrl)
 *   wsClient.connect()
 *   try {
 *     wsClient.declareExchange("events", "direct")
 *     wsClient.declareQueue("orders")
 *     wsClient.bind("orders", "events", "order.created")
 *     wsClient.subscribe("orders", "sub-1")
 *     wsClient.publish("events", "order.created", Map("orderId" -> "123"))
 *     val deliver = wsClient.waitForDeliver()
 *     wsClient.ack("sub-1", deliver.get("deliveryTag").asLong())
 *   } finally {
 *     wsClient.close()
 *   }
 * }}}
 *
 * Note: when the WebSocket pipeline (WsUpgradeOrHttpHandler, WsFrameHandler)
 * is not yet wired into the broker's HTTP listener, `connect()` will fail at
 * the upgrade handshake. Tests that need live WS should be `@Disabled` until
 * wiring is complete.
 *
 * // Time: Created - TASK-WS1.17
 */
class WsTestClient(httpBaseUrl: String) extends AutoCloseable {

  private val mapper = new ObjectMapper()
  private val httpClient = HttpClient.newHttpClient()
  private val inbox = new LinkedBlockingQueue[JsonNode]()
  private var session: WebSocket = _

  /** Connect to the WebSocket endpoint and wait for the "connected" frame. */
  def connect(): Unit = {
    val wsUri = URI.create(httpBaseUrl.replaceFirst("^http://", "ws://") + "/v1/ws")
    val listener = new WsTestListener(inbox, mapper)
    session = httpClient.newWebSocketBuilder()
      .buildAsync(wsUri, listener)
      .get(10, TimeUnit.SECONDS)
    val connected = waitForType("connected", 5.seconds)
    require(connected != null, "Did not receive connected message")
  }

  /** Send a raw JSON frame. */
  def send(json: String): Unit = {
    if (session == null) throw new IllegalStateException("WebSocket not connected")
    session.sendText(json, true).get(5, TimeUnit.SECONDS)
  }

  /**
   * Block until a frame with the given `type` arrives, or throw if the timeout elapses.
   * Frames received before the matching one remain in the inbox in FIFO order — any
   * non-matching frames encountered here are dropped. Tests that need to inspect
   * specific intermediate frames should use [[takeNext]] instead.
   */
  def waitForType(messageType: String, timeout: Duration): JsonNode = {
    val deadline = System.nanoTime() + timeout.toNanos
    while (System.nanoTime() < deadline) {
      val remainingMs = Math.max((deadline - System.nanoTime()) / 1_000_000L, 1L)
      val msg = inbox.poll(remainingMs, TimeUnit.MILLISECONDS)
      if (msg != null) {
        val typeNode = msg.get("type")
        if (typeNode != null && typeNode.isTextual && typeNode.asText() == messageType) {
          return msg
        }
        // else: drop non-matching frame (tests care only about the target type)
      }
    }
    throw new AssertionError(s"Timed out waiting for '$messageType' after $timeout")
  }

  /** Take the next frame off the inbox, or null on timeout. */
  def takeNext(timeout: Duration): JsonNode = {
    inbox.poll(timeout.toMillis, TimeUnit.MILLISECONDS)
  }

  // ------------------------------------------------------------------
  //  Convenience — control plane
  // ------------------------------------------------------------------

  def declareExchange(name: String, exchangeType: String = "direct"): JsonNode = {
    send(s"""{"type":"declare-exchange","id":"de-$name","exchange":"$name","exchangeType":"$exchangeType"}""")
    waitForType("exchange-declared", 5.seconds)
  }

  def declareQueue(name: String, args: Map[String, Any] = Map.empty): JsonNode = {
    val argsJson = mapper.writeValueAsString(args.asJava)
    send(s"""{"type":"declare-queue","id":"dq-$name","queue":"$name","arguments":$argsJson}""")
    waitForType("queue-declared", 5.seconds)
  }

  def bind(queue: String, exchange: String, routingKey: String): JsonNode = {
    send(s"""{"type":"bind","id":"b-$queue-$exchange","queue":"$queue","exchange":"$exchange","routingKey":"$routingKey"}""")
    waitForType("bound", 5.seconds)
  }

  def subscribe(queue: String, subId: String, credits: Int = 100,
                startOffset: String = "earliest", noAck: Boolean = false): JsonNode = {
    send(s"""{"type":"subscribe","id":"s-$subId","queue":"$queue","subscriptionId":"$subId","credits":$credits,"startOffset":"$startOffset","noAck":$noAck}""")
    waitForType("subscribed", 5.seconds)
  }

  def publish(exchange: String, routingKey: String, body: Any,
              headers: Map[String, String] = Map.empty, publishId: Long = -1): Unit = {
    val headersJson = mapper.writeValueAsString(headers.asJava)
    val bodyJson = mapper.writeValueAsString(body)
    val pidField = if (publishId >= 0) s""","publishId":$publishId""" else ""
    send(s"""{"type":"publish","exchange":"$exchange","routingKey":"$routingKey","message":{"body":$bodyJson,"headers":$headersJson}$pidField}""")
  }

  def waitForDeliver(timeout: Duration = 5.seconds): JsonNode =
    waitForType("deliver", timeout)

  def ack(subId: String, deliveryTag: Long, multiple: Boolean = false): Unit =
    send(s"""{"type":"ack","subscriptionId":"$subId","deliveryTag":$deliveryTag,"multiple":$multiple}""")

  def nack(subId: String, deliveryTag: Long, requeue: Boolean = true, multiple: Boolean = false): Unit =
    send(s"""{"type":"nack","subscriptionId":"$subId","deliveryTag":$deliveryTag,"requeue":$requeue,"multiple":$multiple}""")

  def enableConfirms(): JsonNode = {
    send("""{"type":"enable-confirms","id":"ec"}""")
    waitForType("confirms-enabled", 5.seconds)
  }

  def grantCredits(subId: String, credits: Int): Unit =
    send(s"""{"type":"credits","subscriptionId":"$subId","credits":$credits}""")

  def unsubscribe(subId: String): JsonNode = {
    send(s"""{"type":"unsubscribe","id":"us-$subId","subscriptionId":"$subId"}""")
    waitForType("unsubscribed", 5.seconds)
  }

  override def close(): Unit = {
    if (session != null) {
      try {
        session.sendClose(WebSocket.NORMAL_CLOSURE, "bye").get(5, TimeUnit.SECONDS)
      } catch {
        case _: Throwable => // best effort
      }
      session.abort()
      session = null
    }
  }
}

/**
 * JDK WebSocket listener that converts inbound text frames into JsonNode and
 * places them on the inbox queue for the test to consume.
 *
 * // Time: Created - TASK-WS1.17
 */
private class WsTestListener(inbox: LinkedBlockingQueue[JsonNode], mapper: ObjectMapper)
  extends Listener {

  private val buffer = new StringBuilder()

  override def onText(webSocket: WebSocket, data: CharSequence, last: Boolean): CompletionStage[_] = {
    buffer.append(data)
    if (last) {
      val text = buffer.toString()
      buffer.setLength(0)
      try {
        inbox.add(mapper.readTree(text))
      } catch {
        case _: Exception => // ignore malformed frames in tests
      }
    }
    webSocket.request(1)
    CompletableFuture.completedFuture(null)
  }

  override def onError(webSocket: WebSocket, error: Throwable): Unit = {
    // Tests treat errors as timeouts at the inbox level. Logging to stderr is
    // enough for diagnostic purposes.
    System.err.println(s"WebSocket error: ${error.getMessage}")
  }

  override def onClose(webSocket: WebSocket, statusCode: Int, reason: String): CompletionStage[_] = {
    CompletableFuture.completedFuture(null)
  }
}
