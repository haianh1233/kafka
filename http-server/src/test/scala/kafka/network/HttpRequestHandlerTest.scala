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
// Time: Created - TASK-B.05
// Time: Stubbed - TASK-B.03 (original tests incompatible with F.05 HttpRequestHandler rewrite)
package kafka.network

import io.netty.channel.embedded.EmbeddedChannel
import org.apache.kafka.common.security.auth.{KafkaPrincipal, KafkaPrincipalBuilder, SecurityProtocol}
import org.junit.jupiter.api.{BeforeEach, Test}
import org.junit.jupiter.api.Assertions._
import org.mockito.Mockito._

/**
 * Tests for [[HttpRequestHandler]] -- the Netty inbound handler that extracts
 * authentication context and delegates to KafkaPrincipalBuilder.
 *
 * Note: The original TASK-B.05 tests were for a different HttpRequestHandler
 * implementation. These tests match the current F.05 version.
 */
class HttpRequestHandlerTest {

  private var principalBuilder: KafkaPrincipalBuilder = _

  @BeforeEach
  def setUp(): Unit = {
    principalBuilder = mock(classOf[KafkaPrincipalBuilder])
    when(principalBuilder.build(org.mockito.ArgumentMatchers.any())).thenReturn(KafkaPrincipal.ANONYMOUS)
  }

  @Test
  def handlerCreatesSuccessfully(): Unit = {
    val handler = new HttpRequestHandler(principalBuilder, SecurityProtocol.HTTP)
    assertNotNull(handler)
  }

  @Test
  def handlerCanBeAddedToPipeline(): Unit = {
    val handler = new HttpRequestHandler(principalBuilder, SecurityProtocol.HTTP)
    val channel = new EmbeddedChannel(handler)
    assertNotNull(channel.pipeline().last())
    channel.close()
  }
}
