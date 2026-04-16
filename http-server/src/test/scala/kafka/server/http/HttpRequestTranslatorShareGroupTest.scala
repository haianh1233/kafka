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

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.common.errors.InvalidRequestException
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

class HttpRequestTranslatorShareGroupTest {

  private val mapper = new ObjectMapper()

  @Test
  def testTranslateShareGroupPoll(): Unit = {
    val json = mapper.readTree("""{"topics":["orders","payments"],"maxRecords":50,"maxWaitMs":3000}""")
    val builder = HttpRequestTranslator.translateShareGroupPoll("my-group", json)
    val request = builder.build(1.toShort)
    val data = request.data()
    assertEquals("my-group", data.groupId())
    assertEquals(3000, data.maxWaitMs())
    assertEquals(50, data.maxRecords())
  }

  @Test
  def testTranslateShareGroupPollDefaults(): Unit = {
    val json = mapper.readTree("""{"topics":["orders"]}""")
    val builder = HttpRequestTranslator.translateShareGroupPoll("my-group", json)
    val request = builder.build(1.toShort)
    val data = request.data()
    assertEquals(5000, data.maxWaitMs())
    assertEquals(100, data.maxRecords())
  }

  @Test
  def testTranslateShareGroupPollEmptyTopics(): Unit = {
    val json = mapper.readTree("""{"topics":[]}""")
    assertThrows(classOf[InvalidRequestException], () => {
      HttpRequestTranslator.translateShareGroupPoll("my-group", json)
    })
  }

  @Test
  def testTranslateShareGroupPollMissingTopics(): Unit = {
    val json = mapper.readTree("""{"maxRecords":50}""")
    assertThrows(classOf[InvalidRequestException], () => {
      HttpRequestTranslator.translateShareGroupPoll("my-group", json)
    })
  }

  @Test
  def testTranslateShareGroupAcknowledge(): Unit = {
    val json = mapper.readTree("""{"acknowledgements":[
      {"acquireId":"acq-123","type":"ACCEPT"},
      {"acquireId":"acq-456","type":"REJECT"},
      {"acquireId":"acq-789","type":"RELEASE"}
    ]}""")
    // Should not throw
    assertDoesNotThrow(() => {
      HttpRequestTranslator.translateShareGroupAcknowledge("my-group", json)
    })
  }

  @Test
  def testTranslateShareGroupAcknowledgeInvalidType(): Unit = {
    val json = mapper.readTree("""{"acknowledgements":[
      {"acquireId":"acq-123","type":"INVALID_TYPE"}
    ]}""")
    assertThrows(classOf[InvalidRequestException], () => {
      HttpRequestTranslator.translateShareGroupAcknowledge("my-group", json)
    })
  }

  @Test
  def testTranslateShareGroupAcknowledgeEmptyArray(): Unit = {
    val json = mapper.readTree("""{"acknowledgements":[]}""")
    assertThrows(classOf[InvalidRequestException], () => {
      HttpRequestTranslator.translateShareGroupAcknowledge("my-group", json)
    })
  }

  @Test
  def testTranslateShareGroupAcknowledgeMissingArray(): Unit = {
    val json = mapper.readTree("""{"other":"data"}""")
    assertThrows(classOf[InvalidRequestException], () => {
      HttpRequestTranslator.translateShareGroupAcknowledge("my-group", json)
    })
  }

  @Test
  def testTranslateShareGroupAcknowledgeCaseInsensitiveType(): Unit = {
    val json = mapper.readTree("""{"acknowledgements":[
      {"acquireId":"acq-123","type":"accept"}
    ]}""")
    // Should work with lowercase
    assertDoesNotThrow(() => {
      HttpRequestTranslator.translateShareGroupAcknowledge("my-group", json)
    })
  }

  @Test
  def testTranslateShareGroupAcknowledgeMixedCaseType(): Unit = {
    val json = mapper.readTree("""{"acknowledgements":[
      {"acquireId":"acq-123","type":"Accept"},
      {"acquireId":"acq-456","type":"rEJECT"},
      {"acquireId":"acq-789","type":"Release"}
    ]}""")
    assertDoesNotThrow(() => {
      HttpRequestTranslator.translateShareGroupAcknowledge("my-group", json)
    })
  }

  @Test
  def testMapAcknowledgeTypeValues(): Unit = {
    assertEquals(1.toByte, HttpRequestTranslator.mapAcknowledgeType("ACCEPT"))
    assertEquals(2.toByte, HttpRequestTranslator.mapAcknowledgeType("RELEASE"))
    assertEquals(3.toByte, HttpRequestTranslator.mapAcknowledgeType("REJECT"))
  }

  @Test
  def testMapAcknowledgeTypeCaseInsensitive(): Unit = {
    assertEquals(1.toByte, HttpRequestTranslator.mapAcknowledgeType("accept"))
    assertEquals(2.toByte, HttpRequestTranslator.mapAcknowledgeType("release"))
    assertEquals(3.toByte, HttpRequestTranslator.mapAcknowledgeType("reject"))
  }

  @Test
  def testTranslateShareGroupAcknowledgeMissingAcquireId(): Unit = {
    val json = mapper.readTree("""{"acknowledgements":[
      {"type":"ACCEPT"}
    ]}""")
    assertThrows(classOf[InvalidRequestException], () => {
      HttpRequestTranslator.translateShareGroupAcknowledge("my-group", json)
    })
  }

  @Test
  def testTranslateShareGroupAcknowledgeMissingType(): Unit = {
    val json = mapper.readTree("""{"acknowledgements":[
      {"acquireId":"acq-123"}
    ]}""")
    assertThrows(classOf[InvalidRequestException], () => {
      HttpRequestTranslator.translateShareGroupAcknowledge("my-group", json)
    })
  }
}
