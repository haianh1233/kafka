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
package kafka.network

import io.netty.channel.{ChannelHandlerContext, SimpleChannelInboundHandler}
import io.netty.handler.codec.http.{FullHttpRequest, HttpHeaderNames}
import io.netty.handler.ssl.SslHandler
import org.apache.kafka.common.security.auth.{HttpAuthenticationContext, KafkaPrincipal, KafkaPrincipalBuilder, SecurityProtocol}

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.security.cert.X509Certificate
import java.util.Base64

/**
 * Netty channel handler for HTTP requests that extracts authentication context
 * and delegates to KafkaPrincipalBuilder for principal resolution.
 *
 * Authentication extraction priority: mTLS > Bearer > Basic > Anonymous.
 * Once a method is found, lower-priority methods are not checked.
 *
 * This handler does NOT validate Bearer tokens or Basic credentials -- that is
 * the KafkaPrincipalBuilder's responsibility.
 */
class HttpRequestHandler(
  principalBuilder: KafkaPrincipalBuilder,
  securityProtocol: SecurityProtocol
) extends SimpleChannelInboundHandler[FullHttpRequest] {

  /**
   * Extract authentication context from the HTTP request and Netty channel.
   *
   * Priority order:
   * 1. Client certificates (mTLS) from SslHandler
   * 2. Bearer token from Authorization header
   * 3. Basic auth from Authorization header
   * 4. Anonymous (no credentials)
   */
  private[network] def extractAuthContext(
    ctx: ChannelHandlerContext,
    req: FullHttpRequest
  ): HttpAuthenticationContext = {
    val remoteAddr = ctx.channel().remoteAddress().asInstanceOf[InetSocketAddress]
    val builder = new HttpAuthenticationContext.Builder()
      .clientAddress(remoteAddr.getAddress)
      .securityProtocol(securityProtocol)

    // 1. Try mTLS (client certificate) -- only if SslHandler is in the pipeline
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
          // No client certificate presented -- fall through to header-based auth
      }
    }

    // 2. Try Authorization header (Bearer or Basic)
    val authHeader = req.headers().get(HttpHeaderNames.AUTHORIZATION)
    if (authHeader != null) {
      if (authHeader.regionMatches(true, 0, "Bearer ", 0, 7)) {
        // Bearer token -- case-insensitive prefix match
        val token = authHeader.substring(7).trim
        if (token.nonEmpty) {
          builder.bearerToken(token)
          return builder.build()
        }
      } else if (authHeader.regionMatches(true, 0, "Basic ", 0, 6)) {
        // Basic auth -- decode base64, split on first colon only
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
            // Invalid base64 -- fall through to anonymous
        }
      }
    }

    // 3. Anonymous -- no credentials found
    builder.build()
  }

  /**
   * Build KafkaPrincipal from the authentication context.
   * Delegates to the configured KafkaPrincipalBuilder.
   */
  private[network] def buildPrincipal(ctx: ChannelHandlerContext, req: FullHttpRequest): KafkaPrincipal = {
    val authContext = extractAuthContext(ctx, req)
    principalBuilder.build(authContext)
  }

  override def channelRead0(ctx: ChannelHandlerContext, req: FullHttpRequest): Unit = {
    // Build principal for RequestContext construction (used by downstream integration)
    buildPrincipal(ctx, req)
    ctx.fireChannelRead(req.retain())
  }
}
