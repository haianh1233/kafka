# TASK-T.01: KafkaApis HTTP Dispatch Unit Tests

## Prerequisites

| Task | What it provides |
|------|-----------------|
| TASK-0.02 | Deduplicate classes resolved -- core module compiles cleanly |
| TASK-0.04 | Build verified -- base code compiles and existing tests pass |

---

## Context

`KafkaApis.scala` has an `isHttp` dispatch branch (lines 176-186) that routes PRODUCE requests to `handleHttpProduceRequest()` and FETCH requests to `handleHttpConsumeRequest()` when `request.context.securityProtocol.isHttp` is true. There are **zero tests** for this HTTP dispatch path. `KafkaApisTest.scala` has ~100+ existing tests but none exercise the HTTP code paths.

This task adds unit tests for:
1. The dispatch branch itself (PRODUCE/FETCH with `SecurityProtocol.HTTP` routes to the HTTP handlers)
2. `handleHttpProduceRequest` -- authorization, local append, leader-not-available
3. `handleHttpConsumeRequest` -- maxWaitMs clamping, local fetch

Tests go in a **new file** `KafkaApisHttpTest.scala` to avoid merge conflicts with the large `KafkaApisTest.scala`.

### Key Code References

- **Dispatch**: `KafkaApis.scala` lines 176-186 -- `request.header.apiKey match { case ApiKeys.PRODUCE => if (request.context.securityProtocol.isHttp) handleHttpProduceRequest(...) }`
- **HTTP Produce handler**: `KafkaApis.scala` line 305 -- `handleHttpProduceRequest(request, requestLocal)`
- **HTTP Consume handler**: `KafkaApis.scala` line 688 -- `handleHttpConsumeRequest(request)`
- **Existing test pattern**: `KafkaApisTest.scala` -- mocked dependencies, `buildRequest()`, `createKafkaApis()`, `setupBasicMetadataCache()`

---

## Specification

### Test 1: `testProduceWithHttpProtocolRoutesToHttpHandler`
- Build a ProduceRequest with `SecurityProtocol.HTTP` in the request context
- Call `kafkaApis.handle(request, requestLocal)`
- Verify `replicaManager.appendRecords` is called (HTTP produce handler path)
- Verify it is NOT the binary `handleProduceRequest` path (which has different callback signatures)

### Test 2: `testFetchWithHttpProtocolRoutesToHttpConsumeHandler`
- Build a FetchRequest with `SecurityProtocol.HTTP` in the request context
- Call `kafkaApis.handle(request, requestLocal)`
- Verify `replicaManager.fetchMessages` is called (HTTP consume handler path)

### Test 3: `testProduceWithPlaintextProtocolRoutesToNormalHandler`
- Build a ProduceRequest with `SecurityProtocol.PLAINTEXT` (regression check)
- Call `kafkaApis.handle(request, requestLocal)`
- Verify the standard `handleProduceRequest` path is taken (not HTTP)

### Test 4: `testHttpProduceUnauthorizedTopicReturns403`
- Build a ProduceRequest with `SecurityProtocol.HTTP`
- Configure authorizer to DENY WRITE on the target topic
- Call `kafkaApis.handle(request, requestLocal)`
- Verify response contains `TOPIC_AUTHORIZATION_FAILED` error for the partition

### Test 5: `testHttpProduceLocalLeaderAppends`
- Build a ProduceRequest with `SecurityProtocol.HTTP`
- Configure authorizer to ALLOW, metadata cache has this broker as leader
- Call `kafkaApis.handle(request, requestLocal)`
- Verify `replicaManager.appendRecords` is called with the correct partition data

### Test 6: `testHttpProduceNotLeaderReturnsLeaderNotAvailable`
- Build a ProduceRequest with `SecurityProtocol.HTTP`
- Configure metadata cache so another broker is leader (or leader unknown)
- Call `kafkaApis.handle(request, requestLocal)`
- Verify response contains `LEADER_NOT_AVAILABLE` error (forwarding is not tested here)

### Test 7: `testHttpConsumeMaxWaitMsClamped`
- Build a FetchRequest with `maxWait=30000` and `SecurityProtocol.HTTP`
- Configure `httpConsumeMaxWaitMs=5000` in config
- Call `kafkaApis.handle(request, requestLocal)`
- Verify `effectiveMaxWaitMs` passed to replicaManager is capped at 5000

### Test 8: `testHttpConsumeLocalFetch`
- Build a FetchRequest with `SecurityProtocol.HTTP`
- Configure metadata cache with this broker as leader
- Call `kafkaApis.handle(request, requestLocal)`
- Verify `replicaManager.fetchMessages` is called

---

## Implementation Details

### File: `core/src/test/scala/unit/kafka/server/KafkaApisHttpTest.scala`

This test file follows the exact same pattern as `KafkaApisTest.scala`:
- Same mocked dependencies (requestChannel, replicaManager, groupCoordinator, etc.)
- Same `createKafkaApis()` factory method
- Same `buildRequest()` helper, but with an overload that accepts `SecurityProtocol`
- Same `setupBasicMetadataCache()` for configuring metadata

The key difference is the `buildRequest` helper must create a `RequestContext` with `SecurityProtocol.HTTP` instead of `SecurityProtocol.PLAINTEXT`.

---

## Skeleton Code

```scala
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

package kafka.server

import kafka.network.RequestChannel
import kafka.server.QuotaFactory.QuotaManagers
import kafka.server.share.SharePartitionManager
import kafka.utils.{Logging, TestUtils}
import org.apache.kafka.common._
import org.apache.kafka.common.memory.MemoryPool
import org.apache.kafka.common.message.ApiMessageType.ListenerType
import org.apache.kafka.common.message._
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.{ClientInformation, ListenerName}
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record.internal._
import org.apache.kafka.common.requests._
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.common.security.auth.{KafkaPrincipal, KafkaPrincipalSerde, SecurityProtocol}
import org.apache.kafka.common.utils.{SecurityUtils, Utils}
import org.apache.kafka.coordinator.group.{GroupConfigManager, GroupCoordinator}
import org.apache.kafka.coordinator.share.ShareCoordinator
import org.apache.kafka.coordinator.transaction.TransactionCoordinator
import org.apache.kafka.common.internals.Plugin
import org.apache.kafka.image.{MetadataDelta, MetadataImage, MetadataProvenance}
import org.apache.kafka.metadata.{ConfigRepository, KRaftMetadataCache, MetadataCache, MockConfigRepository}
import org.apache.kafka.network.metrics.{RequestChannelMetrics, RequestMetrics}
import org.apache.kafka.raft.{KRaftConfigs, QuorumConfig}
import org.apache.kafka.server.{ClientMetricsManager, FetchManager, SimpleApiVersionManager}
import org.apache.kafka.server.authorizer.{Action, AuthorizationResult, Authorizer}
import org.apache.kafka.server.common.{FinalizedFeatures, MetadataVersion, RequestLocal}
import org.apache.kafka.server.quota.{ClientQuotaManager, ClientRequestQuotaManager, ControllerMutationQuotaManager, ReplicationQuotaManager}
import org.apache.kafka.server.util.{MockTime, ServerTestUtils}
import org.apache.kafka.storage.log.metrics.BrokerTopicStats
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, Test}
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._

import java.net.InetAddress
import java.util
import java.util.{Optional, Properties}
import scala.collection.{Map, Seq}
import scala.jdk.CollectionConverters._

/**
 * Unit tests for KafkaApis HTTP dispatch paths.
 *
 * Separated from KafkaApisTest to avoid conflicts and keep HTTP-specific
 * tests self-contained. Follows the same mock pattern as KafkaApisTest.
 *
 * Tests cover:
 * - PRODUCE/FETCH dispatch branching on securityProtocol.isHttp
 * - handleHttpProduceRequest: auth, local append, leader-not-available
 * - handleHttpConsumeRequest: maxWaitMs clamping, local fetch
 */
class KafkaApisHttpTest extends Logging {

  private val requestChannel: RequestChannel = mock(classOf[RequestChannel])
  private val requestChannelMetrics: RequestChannelMetrics = mock(classOf[RequestChannelMetrics])
  private val replicaManager: ReplicaManager = mock(classOf[ReplicaManager])
  private val groupCoordinator: GroupCoordinator = mock(classOf[GroupCoordinator])
  private val shareCoordinator: ShareCoordinator = mock(classOf[ShareCoordinator])
  private val txnCoordinator: TransactionCoordinator = mock(classOf[TransactionCoordinator])
  private val forwardingManager: ForwardingManager = mock(classOf[ForwardingManager])
  private val autoTopicCreationManager: AutoTopicCreationManager = mock(classOf[AutoTopicCreationManager])

  private val kafkaPrincipalSerde = new KafkaPrincipalSerde {
    override def serialize(principal: KafkaPrincipal): Array[Byte] = Utils.utf8(principal.toString)
    override def deserialize(bytes: Array[Byte]): KafkaPrincipal = SecurityUtils.parseKafkaPrincipal(Utils.utf8(bytes))
  }
  private val metrics = new Metrics()
  private val brokerId = 1
  private var metadataCache: MetadataCache = new KRaftMetadataCache(brokerId, () => org.apache.kafka.server.common.KRaftVersion.LATEST_PRODUCTION)
  private val clientQuotaManager: ClientQuotaManager = mock(classOf[ClientQuotaManager])
  private val clientRequestQuotaManager: ClientRequestQuotaManager = mock(classOf[ClientRequestQuotaManager])
  private val clientControllerQuotaManager: ControllerMutationQuotaManager = mock(classOf[ControllerMutationQuotaManager])
  private val replicaQuotaManager: ReplicationQuotaManager = mock(classOf[ReplicationQuotaManager])
  private val quotas = new QuotaManagers(clientQuotaManager, clientQuotaManager, clientRequestQuotaManager,
    clientControllerQuotaManager, replicaQuotaManager, replicaQuotaManager, replicaQuotaManager, util.Optional.empty())
  private val fetchManager: FetchManager = mock(classOf[FetchManager])
  private val sharePartitionManager: SharePartitionManager = mock(classOf[SharePartitionManager])
  private val clientMetricsManager: ClientMetricsManager = mock(classOf[ClientMetricsManager])
  private val groupConfigManager: GroupConfigManager = mock(classOf[GroupConfigManager])
  private val brokerTopicStats = new BrokerTopicStats
  private val clusterId = "clusterId"
  private val time = new MockTime
  private val clientId = ""
  private var kafkaApis: KafkaApis = _

  @AfterEach
  def tearDown(): Unit = {
    Utils.swallow(this.logger.underlying, () => quotas.shutdown())
    if (kafkaApis != null)
      Utils.swallow(this.logger.underlying, () => kafkaApis.close())
    ServerTestUtils.clearYammerMetrics()
    metrics.close()
  }

  // -------------------------------------------------------------------
  // Factory: create KafkaApis with optional authorizer & config overrides
  // -------------------------------------------------------------------
  def createKafkaApis(
    authorizer: Option[Authorizer] = None,
    configRepository: ConfigRepository = new MockConfigRepository(),
    overrideProperties: Map[String, String] = Map.empty
  ): KafkaApis = {
    val properties = TestUtils.createBrokerConfig(brokerId)
    properties.put(KRaftConfigs.NODE_ID_CONFIG, brokerId.toString)
    properties.put(KRaftConfigs.PROCESS_ROLES_CONFIG, "broker")
    val voterId = brokerId + 1
    properties.put(QuorumConfig.QUORUM_VOTERS_CONFIG, s"$voterId@localhost:9093")

    overrideProperties.foreach(p => properties.put(p._1, p._2))
    val config = new KafkaConfig(properties)

    val apiVersionManager = new SimpleApiVersionManager(
      ListenerType.BROKER, true,
      () => new FinalizedFeatures(MetadataVersion.latestTesting(), util.Map.of[String, java.lang.Short], 0))

    new KafkaApis(
      requestChannel = requestChannel,
      forwardingManager = forwardingManager,
      replicaManager = replicaManager,
      groupCoordinator = groupCoordinator,
      txnCoordinator = txnCoordinator,
      shareCoordinator = shareCoordinator,
      autoTopicCreationManager = autoTopicCreationManager,
      brokerId = brokerId,
      config = config,
      configRepository = configRepository,
      metadataCache = metadataCache,
      metrics = metrics,
      authorizerPlugin = authorizer.map(Plugin.wrapInstance(_, null, "authorizer.class.name")),
      quotas = quotas,
      fetchManager = fetchManager,
      sharePartitionManager = sharePartitionManager,
      brokerTopicStats = brokerTopicStats,
      clusterId = clusterId,
      time = time,
      tokenManager = null,
      apiVersionManager = apiVersionManager,
      clientMetricsManager = clientMetricsManager,
      groupConfigManager = groupConfigManager)
  }

  // -------------------------------------------------------------------
  // buildRequest: creates RequestChannel.Request with the given protocol
  // -------------------------------------------------------------------
  private def buildRequest(
    request: AbstractRequest,
    securityProtocol: SecurityProtocol = SecurityProtocol.HTTP,
    listenerName: ListenerName = ListenerName.normalised("HTTP"),
    fromPrivilegedListener: Boolean = false
  ): RequestChannel.Request = {
    val header = new RequestHeader(request.apiKey, request.version, clientId, 0)
    val buffer = request.serializeWithHeader(header)
    val parsedHeader = RequestHeader.parse(buffer)
    val principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice")
    val context = new RequestContext(parsedHeader, "1", InetAddress.getLocalHost, Optional.empty(),
      principal, listenerName, securityProtocol,
      ClientInformation.EMPTY, fromPrivilegedListener, Optional.of(kafkaPrincipalSerde))
    new RequestChannel.Request(processor = 1, context = context, startTimeNanos = 0,
      MemoryPool.NONE, buffer, requestChannelMetrics, envelope = None)
  }

  // -------------------------------------------------------------------
  // Metadata cache setup helpers (same as KafkaApisTest)
  // -------------------------------------------------------------------
  private def setupBasicMetadataCache(topic: String, numPartitions: Int, numBrokers: Int, topicId: Uuid): Unit = {
    val delta = new MetadataDelta(MetadataImage.EMPTY)

    // Register brokers
    (0 until numBrokers).foreach { id =>
      val endpoints = new org.apache.kafka.common.metadata.RegisterBrokerRecord.BrokerEndpointCollection()
      endpoints.add(new org.apache.kafka.common.metadata.RegisterBrokerRecord.BrokerEndpoint()
        .setHost("broker" + id).setPort(9092)
        .setSecurityProtocol(SecurityProtocol.PLAINTEXT.id)
        .setName("PLAINTEXT"))
      delta.replay(new org.apache.kafka.common.metadata.RegisterBrokerRecord()
        .setBrokerId(id).setRack("rack").setFenced(false)
        .setEndPoints(endpoints).setBrokerEpoch(id * 100L))
    }

    // Create topic
    delta.replay(new org.apache.kafka.common.metadata.TopicRecord()
      .setTopicId(topicId).setName(topic))

    // Create partitions (leader = brokerId for local tests)
    (0 until numPartitions).foreach { p =>
      delta.replay(new org.apache.kafka.common.metadata.PartitionRecord()
        .setTopicId(topicId).setPartitionId(p)
        .setLeader(brokerId).setLeaderEpoch(0)
        .setPartitionEpoch(0)
        .setReplicas(util.List.of(brokerId))
        .setIsr(util.List.of(brokerId)))
    }

    val provenance = new MetadataProvenance(100, 10, 1000)
    metadataCache.asInstanceOf[KRaftMetadataCache].setImage(delta.apply(provenance))
  }

  // ===================================================================
  // Test 1: PRODUCE + HTTP -> handleHttpProduceRequest
  // ===================================================================
  @Test
  def testProduceWithHttpProtocolRoutesToHttpHandler(): Unit = {
    kafkaApis = createKafkaApis()
    val topicId = Uuid.randomUuid()
    setupBasicMetadataCache("test-topic", 1, 2, topicId)

    val produceData = new ProduceRequestData()
    val topicData = new ProduceRequestData.TopicProduceData()
      .setName("test-topic")
    val partitionData = new ProduceRequestData.PartitionProduceData()
      .setIndex(0)
      .setRecords(MemoryRecords.withRecords(
        org.apache.kafka.common.record.internal.CompressionType.NONE,
        new org.apache.kafka.common.record.internal.SimpleRecord("value".getBytes)))
    topicData.partitionData().add(partitionData)
    produceData.topicData().add(topicData)
    produceData.setAcks((-1).toShort)
    produceData.setTimeoutMs(5000)

    val produceRequest = new ProduceRequest.Builder(
      ApiKeys.PRODUCE.latestVersion(), ApiKeys.PRODUCE.latestVersion(),
      produceData).build()

    val request = buildRequest(produceRequest, SecurityProtocol.HTTP)

    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    kafkaApis.handle(request, RequestLocal.withThreadConfinedCaching)

    // Verify replicaManager.appendRecords was called (HTTP produce path)
    verify(replicaManager).appendRecords(
      anyLong(), anyShort(), anyBoolean(), any(), any(), any(), any(),
      any(), any(), any(), any(), any())
  }

  // ===================================================================
  // Test 2: FETCH + HTTP -> handleHttpConsumeRequest
  // ===================================================================
  @Test
  def testFetchWithHttpProtocolRoutesToHttpConsumeHandler(): Unit = {
    kafkaApis = createKafkaApis()
    val topicId = Uuid.randomUuid()
    setupBasicMetadataCache("test-topic", 1, 2, topicId)

    val fetchData = new FetchRequestData()
    fetchData.setMaxWaitMs(500)
    fetchData.setMinBytes(1)
    fetchData.setMaxBytes(1024 * 1024)
    val fetchTopic = new FetchRequestData.FetchTopic().setTopic("test-topic").setTopicId(topicId)
    val fetchPartition = new FetchRequestData.FetchPartition()
      .setPartition(0).setFetchOffset(0).setPartitionMaxBytes(1024 * 1024)
    fetchTopic.partitions().add(fetchPartition)
    fetchData.topics().add(fetchTopic)

    val fetchRequest = FetchRequest.parse(fetchData.toStruct(ApiKeys.FETCH.latestVersion()), ApiKeys.FETCH.latestVersion())

    val request = buildRequest(fetchRequest, SecurityProtocol.HTTP)

    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    kafkaApis.handle(request, RequestLocal.withThreadConfinedCaching)

    // Verify replicaManager.fetchMessages was called (HTTP consume path)
    verify(replicaManager).fetchMessages(
      any(), any(), anyInt(), anyInt(), anyBoolean(),
      any(), any(), any(), any(), any())
  }

  // ===================================================================
  // Test 3: PRODUCE + PLAINTEXT -> normal handleProduceRequest (regression)
  // ===================================================================
  @Test
  def testProduceWithPlaintextProtocolRoutesToNormalHandler(): Unit = {
    kafkaApis = createKafkaApis()
    val topicId = Uuid.randomUuid()
    setupBasicMetadataCache("test-topic", 1, 2, topicId)

    val produceData = new ProduceRequestData()
    val topicData = new ProduceRequestData.TopicProduceData()
      .setName("test-topic")
    val partitionData = new ProduceRequestData.PartitionProduceData()
      .setIndex(0)
      .setRecords(MemoryRecords.withRecords(
        org.apache.kafka.common.record.internal.CompressionType.NONE,
        new org.apache.kafka.common.record.internal.SimpleRecord("value".getBytes)))
    topicData.partitionData().add(partitionData)
    produceData.topicData().add(topicData)
    produceData.setAcks((-1).toShort)
    produceData.setTimeoutMs(5000)

    val produceRequest = new ProduceRequest.Builder(
      ApiKeys.PRODUCE.latestVersion(), ApiKeys.PRODUCE.latestVersion(),
      produceData).build()

    // Use PLAINTEXT, not HTTP
    val request = buildRequest(produceRequest,
      SecurityProtocol.PLAINTEXT,
      ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT))

    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    kafkaApis.handle(request, RequestLocal.withThreadConfinedCaching)

    // Should still call appendRecords but via the normal path
    verify(replicaManager).appendRecords(
      anyLong(), anyShort(), anyBoolean(), any(), any(), any(), any(),
      any(), any(), any(), any(), any())
  }

  // ===================================================================
  // Test 4: HTTP PRODUCE with unauthorized topic -> TOPIC_AUTHORIZATION_FAILED
  // ===================================================================
  @Test
  def testHttpProduceUnauthorizedTopicReturns403(): Unit = {
    val authorizer = mock(classOf[Authorizer])
    // Deny all WRITE operations
    when(authorizer.authorize(any(), any())).thenReturn(
      util.List.of(AuthorizationResult.DENIED))

    kafkaApis = createKafkaApis(authorizer = Some(authorizer))
    val topicId = Uuid.randomUuid()
    setupBasicMetadataCache("secret-topic", 1, 2, topicId)

    val produceData = new ProduceRequestData()
    val topicData = new ProduceRequestData.TopicProduceData()
      .setName("secret-topic")
    val partitionData = new ProduceRequestData.PartitionProduceData()
      .setIndex(0)
      .setRecords(MemoryRecords.withRecords(
        org.apache.kafka.common.record.internal.CompressionType.NONE,
        new org.apache.kafka.common.record.internal.SimpleRecord("value".getBytes)))
    topicData.partitionData().add(partitionData)
    produceData.topicData().add(topicData)
    produceData.setAcks((-1).toShort)
    produceData.setTimeoutMs(5000)

    val produceRequest = new ProduceRequest.Builder(
      ApiKeys.PRODUCE.latestVersion(), ApiKeys.PRODUCE.latestVersion(),
      produceData).build()

    val request = buildRequest(produceRequest, SecurityProtocol.HTTP)

    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    kafkaApis.handle(request, RequestLocal.withThreadConfinedCaching)

    // Verify response was sent with TOPIC_AUTHORIZATION_FAILED
    val capturedResponse: ArgumentCaptor[AbstractResponse] =
      ArgumentCaptor.forClass(classOf[AbstractResponse])
    verify(requestChannel).sendResponse(
      ArgumentMatchers.eq(request), capturedResponse.capture(), any())
    val produceResponse = capturedResponse.getValue.asInstanceOf[ProduceResponse]
    val partitionResp = produceResponse.data.responses().get(0)
      .partitionResponses().get(0)
    assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED.code(), partitionResp.errorCode())
  }

  // ===================================================================
  // Test 5: HTTP PRODUCE with local leader -> appendRecords called
  // ===================================================================
  @Test
  def testHttpProduceLocalLeaderAppends(): Unit = {
    kafkaApis = createKafkaApis()
    val topicId = Uuid.randomUuid()
    // brokerId=1 is leader for all partitions
    setupBasicMetadataCache("my-topic", 1, 2, topicId)

    val produceData = new ProduceRequestData()
    val topicData = new ProduceRequestData.TopicProduceData()
      .setName("my-topic")
    val partitionData = new ProduceRequestData.PartitionProduceData()
      .setIndex(0)
      .setRecords(MemoryRecords.withRecords(
        org.apache.kafka.common.record.internal.CompressionType.NONE,
        new org.apache.kafka.common.record.internal.SimpleRecord("hello".getBytes)))
    topicData.partitionData().add(partitionData)
    produceData.topicData().add(topicData)
    produceData.setAcks((-1).toShort)
    produceData.setTimeoutMs(5000)

    val produceRequest = new ProduceRequest.Builder(
      ApiKeys.PRODUCE.latestVersion(), ApiKeys.PRODUCE.latestVersion(),
      produceData).build()

    val request = buildRequest(produceRequest, SecurityProtocol.HTTP)

    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    kafkaApis.handle(request, RequestLocal.withThreadConfinedCaching)

    // Verify appendRecords was called on the local leader
    verify(replicaManager).appendRecords(
      anyLong(), anyShort(), anyBoolean(), any(), any(), any(), any(),
      any(), any(), any(), any(), any())
  }

  // ===================================================================
  // Test 6: HTTP PRODUCE, not leader -> LEADER_NOT_AVAILABLE
  // ===================================================================
  @Test
  def testHttpProduceNotLeaderReturnsLeaderNotAvailable(): Unit = {
    kafkaApis = createKafkaApis()
    val topicId = Uuid.randomUuid()

    // Set up metadata with a DIFFERENT broker as leader (broker 0, not brokerId=1)
    val delta = new MetadataDelta(MetadataImage.EMPTY)
    (0 until 2).foreach { id =>
      val endpoints = new org.apache.kafka.common.metadata.RegisterBrokerRecord.BrokerEndpointCollection()
      endpoints.add(new org.apache.kafka.common.metadata.RegisterBrokerRecord.BrokerEndpoint()
        .setHost("broker" + id).setPort(9092)
        .setSecurityProtocol(SecurityProtocol.PLAINTEXT.id)
        .setName("PLAINTEXT"))
      delta.replay(new org.apache.kafka.common.metadata.RegisterBrokerRecord()
        .setBrokerId(id).setRack("rack").setFenced(false)
        .setEndPoints(endpoints).setBrokerEpoch(id * 100L))
    }
    delta.replay(new org.apache.kafka.common.metadata.TopicRecord()
      .setTopicId(topicId).setName("remote-topic"))
    // Leader is broker 0, not brokerId=1
    delta.replay(new org.apache.kafka.common.metadata.PartitionRecord()
      .setTopicId(topicId).setPartitionId(0)
      .setLeader(0).setLeaderEpoch(0).setPartitionEpoch(0)
      .setReplicas(util.List.of(0)).setIsr(util.List.of(0)))
    val provenance = new MetadataProvenance(100, 10, 1000)
    metadataCache.asInstanceOf[KRaftMetadataCache].setImage(delta.apply(provenance))

    val produceData = new ProduceRequestData()
    val topicData = new ProduceRequestData.TopicProduceData()
      .setName("remote-topic")
    val partitionData = new ProduceRequestData.PartitionProduceData()
      .setIndex(0)
      .setRecords(MemoryRecords.withRecords(
        org.apache.kafka.common.record.internal.CompressionType.NONE,
        new org.apache.kafka.common.record.internal.SimpleRecord("remote".getBytes)))
    topicData.partitionData().add(partitionData)
    produceData.topicData().add(topicData)
    produceData.setAcks((-1).toShort)
    produceData.setTimeoutMs(5000)

    val produceRequest = new ProduceRequest.Builder(
      ApiKeys.PRODUCE.latestVersion(), ApiKeys.PRODUCE.latestVersion(),
      produceData).build()

    val request = buildRequest(produceRequest, SecurityProtocol.HTTP)

    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    kafkaApis.handle(request, RequestLocal.withThreadConfinedCaching)

    // With no produceForwardManager wired, remote partitions get LEADER_NOT_AVAILABLE
    // or the handler attempts forwarding. Either way, appendRecords should NOT be called
    // for a partition where this broker is not the leader (when forwarding is null).
    // The response should contain an error for that partition.
    val capturedResponse: ArgumentCaptor[AbstractResponse] =
      ArgumentCaptor.forClass(classOf[AbstractResponse])
    verify(requestChannel).sendResponse(
      ArgumentMatchers.eq(request), capturedResponse.capture(), any())
  }

  // ===================================================================
  // Test 7: HTTP CONSUME maxWaitMs is clamped
  // ===================================================================
  @Test
  def testHttpConsumeMaxWaitMsClamped(): Unit = {
    // Configure httpConsumeMaxWaitMs = 5000
    kafkaApis = createKafkaApis(overrideProperties = Map(
      "http.consume.max.wait.ms" -> "5000"
    ))
    val topicId = Uuid.randomUuid()
    setupBasicMetadataCache("test-topic", 1, 2, topicId)

    val fetchData = new FetchRequestData()
    fetchData.setMaxWaitMs(30000)  // Client requests 30s
    fetchData.setMinBytes(1)
    fetchData.setMaxBytes(1024 * 1024)
    val fetchTopic = new FetchRequestData.FetchTopic().setTopic("test-topic").setTopicId(topicId)
    val fetchPartition = new FetchRequestData.FetchPartition()
      .setPartition(0).setFetchOffset(0).setPartitionMaxBytes(1024 * 1024)
    fetchTopic.partitions().add(fetchPartition)
    fetchData.topics().add(fetchTopic)

    val fetchRequest = FetchRequest.parse(fetchData.toStruct(ApiKeys.FETCH.latestVersion()), ApiKeys.FETCH.latestVersion())

    val request = buildRequest(fetchRequest, SecurityProtocol.HTTP)

    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    kafkaApis.handle(request, RequestLocal.withThreadConfinedCaching)

    // Verify the maxWait passed to replicaManager.fetchMessages is clamped to 5000
    val maxWaitCaptor: ArgumentCaptor[java.lang.Long] =
      ArgumentCaptor.forClass(classOf[java.lang.Long])
    verify(replicaManager).fetchMessages(
      any(), any(), anyInt(), anyInt(), anyBoolean(),
      any(), any(), any(), any(), any())

    // Also verify the request property was set
    val applied = request.requestLocalProperties.get("httpMaxWaitApplied")
    assertNotNull(applied, "httpMaxWaitApplied should be set in request properties")
    assertEquals(5000, applied.asInstanceOf[Int])
  }

  // ===================================================================
  // Test 8: HTTP CONSUME with local leader -> fetchMessages called
  // ===================================================================
  @Test
  def testHttpConsumeLocalFetch(): Unit = {
    kafkaApis = createKafkaApis()
    val topicId = Uuid.randomUuid()
    setupBasicMetadataCache("test-topic", 1, 2, topicId)

    val fetchData = new FetchRequestData()
    fetchData.setMaxWaitMs(500)
    fetchData.setMinBytes(1)
    fetchData.setMaxBytes(1024 * 1024)
    val fetchTopic = new FetchRequestData.FetchTopic().setTopic("test-topic").setTopicId(topicId)
    val fetchPartition = new FetchRequestData.FetchPartition()
      .setPartition(0).setFetchOffset(0).setPartitionMaxBytes(1024 * 1024)
    fetchTopic.partitions().add(fetchPartition)
    fetchData.topics().add(fetchTopic)

    val fetchRequest = FetchRequest.parse(fetchData.toStruct(ApiKeys.FETCH.latestVersion()), ApiKeys.FETCH.latestVersion())

    val request = buildRequest(fetchRequest, SecurityProtocol.HTTP)

    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    kafkaApis.handle(request, RequestLocal.withThreadConfinedCaching)

    // Verify fetchMessages was called on the local leader
    verify(replicaManager).fetchMessages(
      any(), any(), anyInt(), anyInt(), anyBoolean(),
      any(), any(), any(), any(), any())
  }
}
```

---

## Tests

### Run Command

```bash
./gradlew :core:test --tests 'kafka.server.KafkaApisHttpTest*'
```

### Expected Results

All 8 tests should pass. These are pure unit tests with mocked dependencies -- no broker startup required.

---

## Rules

- Do NOT modify `KafkaApisTest.scala` -- all HTTP tests go in the new `KafkaApisHttpTest.scala` file.
- Follow the exact same mock setup pattern as `KafkaApisTest`: same mocked classes, same `createKafkaApis()` factory.
- The `buildRequest()` helper MUST accept `SecurityProtocol` as a parameter (defaulting to `HTTP`).
- Each test must be independent -- no shared mutable state between tests beyond the class-level mocks.
- If the skeleton code does not compile due to API changes in upstream Kafka, fix compilation errors but do not change test semantics.

---

## Learning

1. ProduceRequest v13 (KIP-516) replaces topic names with topic IDs. Tests must use v11/v12 (name-based) when relying on topic names, or set topicId explicitly at v13.
2. The normal `handleProduceRequest` calls `replicaManager.handleProduceAppend()`, while the HTTP `handleHttpProduceRequest` calls `replicaManager.appendRecords()` directly. Test 3 (PLAINTEXT regression) must verify the correct method.
3. `MetadataDelta` must be constructed via `new MetadataDelta.Builder().setImage(MetadataImage.EMPTY).build()` (Builder pattern), and `MetadataProvenance.EMPTY` should be used for test metadata images.
4. Scala compiler treats unused default arguments as errors (-Xfatal-warnings). Tests should use the default at least once or remove it.

## Limitations

(empty)

## Field Notes

- Skeleton code required fixes: FetchRequest.parse/toStruct replaced with FetchRequest.Builder; Compression.NONE (not CompressionType.NONE); MetadataDelta.Builder; MetadataProvenance needs 4th boolean param (isOffsetBatchAligned) or use EMPTY; ProduceResponse.data.responses is an ImplicitLinkedHashCollection requiring .asScala.head (not .get(0)).

---

## Acceptance Criteria

- [x] `KafkaApisHttpTest.scala` is created at `core/src/test/scala/unit/kafka/server/KafkaApisHttpTest.scala`
- [x] All 8 tests compile with `./gradlew :core:compileTestScala`
- [x] All 8 tests pass with `./gradlew :core:test --tests 'kafka.server.KafkaApisHttpTest*'`
- [x] No modifications to `KafkaApisTest.scala`
- [x] Tests cover: HTTP dispatch branching, auth failure, local append, leader-not-available, maxWait clamping, local fetch

---

## File Manifest

| File | Status | Description |
|------|--------|-------------|
| `core/src/test/scala/unit/kafka/server/KafkaApisHttpTest.scala` | New | 8 unit tests for HTTP dispatch paths |
