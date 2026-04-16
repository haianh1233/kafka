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
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

class HttpRouterShareGroupTest {

  @Test
  def testRouteShareGroupPoll(): Unit = {
    val result = HttpRouter.route("POST", "/v1/share-groups/my-share-group/records")
    result match {
      case HttpRouter.ShareGroupPollRoute(group) =>
        assertEquals("my-share-group", group)
      case _ => fail(s"Expected ShareGroupPollRoute, got $result")
    }
  }

  @Test
  def testRouteShareGroupAcknowledge(): Unit = {
    val result = HttpRouter.route("POST", "/v1/share-groups/my-share-group/acknowledge")
    result match {
      case HttpRouter.ShareGroupAcknowledgeRoute(group) =>
        assertEquals("my-share-group", group)
      case _ => fail(s"Expected ShareGroupAcknowledgeRoute, got $result")
    }
  }

  @Test
  def testRouteShareGroupPollInvalidGroup(): Unit = {
    // Empty group segment in URL path
    assertThrows(classOf[InvalidRequestException], () => {
      HttpRouter.route("POST", "/v1/share-groups//records")
    })
  }

  @Test
  def testRouteShareGroupPollWithUrlEncodedGroup(): Unit = {
    val result = HttpRouter.route("POST", "/v1/share-groups/my%20group/records")
    result match {
      case HttpRouter.ShareGroupPollRoute(group) =>
        assertEquals("my group", group)
      case _ => fail(s"Expected ShareGroupPollRoute, got $result")
    }
  }

  @Test
  def testRouteShareGroupPollWithQueryString(): Unit = {
    val result = HttpRouter.route("POST", "/v1/share-groups/my-group/records?timeout=5000")
    result match {
      case HttpRouter.ShareGroupPollRoute(group) =>
        assertEquals("my-group", group)
      case _ => fail(s"Expected ShareGroupPollRoute, got $result")
    }
  }

  @Test
  def testRouteNotFound(): Unit = {
    val result = HttpRouter.route("GET", "/v1/nonexistent")
    assertEquals(HttpRouter.NotFoundRoute, result)
  }

  @Test
  def testRouteWrongMethodForShareGroupPoll(): Unit = {
    // GET is not valid for share group poll
    val result = HttpRouter.route("GET", "/v1/share-groups/my-group/records")
    assertEquals(HttpRouter.NotFoundRoute, result)
  }

  @Test
  def testRouteWrongMethodForShareGroupAcknowledge(): Unit = {
    // PUT is not valid for share group acknowledge
    val result = HttpRouter.route("PUT", "/v1/share-groups/my-group/acknowledge")
    assertEquals(HttpRouter.NotFoundRoute, result)
  }
}
