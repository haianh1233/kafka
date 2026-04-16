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
package kafka.server.http

import org.apache.kafka.common.errors.InvalidRequestException

/**
 * HTTP request router that maps URI patterns to route results.
 *
 * Dispatches incoming HTTP requests based on method and URI path segments.
 * Each route result carries the extracted path parameters needed by downstream
 * request translators.
 */
object HttpRouter {

  /** Supported HTTP methods for routing. */
  object HttpMethod extends Enumeration {
    type HttpMethod = Value
    val GET, POST, PUT, DELETE = Value

    def fromString(method: String): HttpMethod = {
      method.toUpperCase match {
        case "GET" => GET
        case "POST" => POST
        case "PUT" => PUT
        case "DELETE" => DELETE
        case _ => throw new InvalidRequestException(s"Unsupported HTTP method: $method")
      }
    }
  }

  /** Base trait for all route matching results. */
  sealed trait RouteResult

  /** No matching route was found. */
  case object NotFoundRoute extends RouteResult

  /** Share group poll: POST /v1/share-groups/{group}/records */
  case class ShareGroupPollRoute(group: String) extends RouteResult

  /** Share group acknowledge: POST /v1/share-groups/{group}/acknowledge */
  case class ShareGroupAcknowledgeRoute(group: String) extends RouteResult

  /**
   * Route an HTTP request to the appropriate handler.
   *
   * @param method the HTTP method (GET, POST, etc.)
   * @param uri    the request URI path (e.g. "/v1/share-groups/my-group/records")
   * @return the matching RouteResult
   */
  def route(method: String, uri: String): RouteResult = {
    val httpMethod = HttpMethod.fromString(method)
    val path = uri.split('?').head // strip query string
    val segments = path.stripPrefix("/").split('/').toList

    (httpMethod, segments) match {
      case (HttpMethod.POST, List("v1", "share-groups", rawGroup, "records")) =>
        val group = validateGroupId(rawGroup)
        ShareGroupPollRoute(group)

      case (HttpMethod.POST, List("v1", "share-groups", rawGroup, "acknowledge")) =>
        val group = validateGroupId(rawGroup)
        ShareGroupAcknowledgeRoute(group)

      case _ =>
        NotFoundRoute
    }
  }

  /**
   * Validate and sanitize a group ID extracted from the URL path.
   *
   * @param rawGroup the raw group ID from the URL
   * @return the validated group ID
   * @throws InvalidRequestException if the group ID is empty or invalid
   */
  def validateGroupId(rawGroup: String): String = {
    if (rawGroup == null || rawGroup.trim.isEmpty) {
      throw new InvalidRequestException("Group ID must not be empty")
    }
    val decoded = java.net.URLDecoder.decode(rawGroup, "UTF-8")
    if (decoded.trim.isEmpty) {
      throw new InvalidRequestException("Group ID must not be empty")
    }
    decoded
  }
}
