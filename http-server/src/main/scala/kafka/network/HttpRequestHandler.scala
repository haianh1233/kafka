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
// Time: Created - TASK-F.05
// Time: Modified - TASK-F.06 (drain check + in-flight tracking)
package kafka.network

import io.netty.buffer.Unpooled
import io.netty.channel.{ChannelFutureListener, ChannelHandlerContext, SimpleChannelInboundHandler}
import io.netty.handler.codec.http.{DefaultFullHttpResponse, FullHttpRequest, HttpHeaderNames, HttpResponseStatus, HttpVersion}
import io.netty.handler.ssl.SslHandler
import org.apache.kafka.common.security.auth.{HttpAuthenticationContext, KafkaPrincipal, KafkaPrincipalBuilder, SecurityProtocol}

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.security.cert.X509Certificate
import java.util.Base64
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

/**
 * Netty channel handler for HTTP requests that extracts authentication context
 * and delegates to KafkaPrincipalBuilder for principal resolution.
 *
 * Authentication extraction priority: mTLS > Bearer > Basic > Anonymous.
 * Once a method is found, lower-priority methods are not checked.
 *
 * This handler also implements drain-phase request rejection: when the broker
 * is shutting down, new requests receive 503 Service Unavailable with a
 * Retry-After header directing clients to retry on another broker.
 *
 * In-flight request tracking: the handler increments the in-flight counter
 * when a request passes the drain check. The counter is decremented when the
 * response is written (handled by HttpProcessor) or on error paths.
 */
class HttpRequestHandler(
  principalBuilder: KafkaPrincipalBuilder,
  securityProtocol: SecurityProtocol,
  draining: AtomicBoolean,
  inFlightCount: AtomicInteger
) extends SimpleChannelInboundHandler[FullHttpRequest] {

  /** Convenience constructor for backward compatibility (no drain support). */
  def this(principalBuilder: KafkaPrincipalBuilder, securityProtocol: SecurityProtocol) =
    this(principalBuilder, securityProtocol, new AtomicBoolean(false), new AtomicInteger(0))

  /** JSON body for 503 drain response */
  private val DrainingResponseBody =
    """{"errorCode":-1,"errorMessage":"Broker is shutting down","detail":"SERVICE_UNAVAILABLE"}"""

  private[network] def extractAuthContext(
    ctx: ChannelHandlerContext,
    req: FullHttpRequest
  ): HttpAuthenticationContext = {
    val remoteAddr = ctx.channel().remoteAddress().asInstanceOf[InetSocketAddress]
    val builder = new HttpAuthenticationContext.Builder()
      .clientAddress(remoteAddr.getAddress)
      .securityProtocol(securityProtocol)

    val sslHandler = ctx.pipeline().get(classOf[SslHandler])
    if (sslHandler != null) {
      try {
        val session = sslHandler.engine().getSession
        val peerCerts = session.getPeerCertificates
        if (peerCerts != null && peerCerts.nonEmpty) {
          val x509Certs = peerCerts.collect { case c: X509Certificate => c }
          if (x509Certs.nonEmpty) {
            builder.peerCertificates(x509Certs)
            return builder.build()
          }
        }
      } catch {
        case _: javax.net.ssl.SSLPeerUnverifiedException =>
      }
    }

    val authHeader = req.headers().get(HttpHeaderNames.AUTHORIZATION)
    if (authHeader != null) {
      if (authHeader.regionMatches(true, 0, "Bearer ", 0, 7)) {
        val token = authHeader.substring(7).trim
        if (token.nonEmpty) {
          builder.bearerToken(token)
          return builder.build()
        }
      } else if (authHeader.regionMatches(true, 0, "Basic ", 0, 6)) {
        val encoded = authHeader.substring(6).trim
        try {
          val decoded = new String(Base64.getDecoder.decode(encoded), StandardCharsets.UTF_8)
          val colonIndex = decoded.indexOf(':')
          if (colonIndex > 0) {
            val username = decoded.substring(0, colonIndex)
            val password = decoded.substring(colonIndex + 1)
            builder.basicCredentials(username, password)
            return builder.build()
          }
        } catch {
          case _: IllegalArgumentException =>
        }
      }
    }

    builder.build()
  }

  private[network] def buildPrincipal(ctx: ChannelHandlerContext, req: FullHttpRequest): KafkaPrincipal = {
    val authContext = extractAuthContext(ctx, req)
    principalBuilder.build(authContext)
  }

  override def channelRead0(ctx: ChannelHandlerContext, req: FullHttpRequest): Unit = {
    if (draining.get()) {
      sendDrainingResponse(ctx)
      return
    }

    inFlightCount.incrementAndGet()

    try {
      buildPrincipal(ctx, req)
      ctx.fireChannelRead(req.retain())
    } catch {
      case e: Exception =>
        inFlightCount.decrementAndGet()
        throw e
    }
  }

  private[network] def sendDrainingResponse(ctx: ChannelHandlerContext): Unit = {
    val body = DrainingResponseBody.getBytes(StandardCharsets.UTF_8)
    val response = new DefaultFullHttpResponse(
      HttpVersion.HTTP_1_1,
      HttpResponseStatus.SERVICE_UNAVAILABLE,
      Unpooled.wrappedBuffer(body))
    response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json")
    response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, body.length)
    response.headers().set("Retry-After", "5")
    response.headers().set(HttpHeaderNames.CONNECTION, "close")
    ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE)
  }
}
