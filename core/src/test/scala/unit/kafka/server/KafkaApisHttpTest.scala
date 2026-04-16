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

import kafka.coordinator.transaction.TransactionCoordinator
import kafka.network.RequestChannel
import kafka.server.QuotaFactory.QuotaManagers
import kafka.server.share.SharePartitionManager
import kafka.utils.{Logging, TestUtils}
import org.apache.kafka.common._
import org.apache.kafka.common.compress.Compression
import org.apache.kafka.common.memory.MemoryPool
import org.apache.kafka.common.message.ApiMessageType.ListenerType
import org.apache.kafka.common.message._
import org.apache.kafka.common.metadata.{PartitionRecord, RegisterBrokerRecord, TopicRecord}
import org.apache.kafka.common.metadata.RegisterBrokerRecord.{BrokerEndpoint, BrokerEndpointCollection}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.{ClientInformation, ListenerName}
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record.internal._
import org.apache.kafka.common.requests._
import org.apache.kafka.common.security.auth.{KafkaPrincipal, KafkaPrincipalSerde, SecurityProtocol}
import org.apache.kafka.common.utils.{SecurityUtils, Utils}
import org.apache.kafka.coordinator.group.{GroupConfigManager, GroupCoordinator}
import org.apache.kafka.coordinator.share.ShareCoordinator
import org.apache.kafka.common.internals.Plugin
import org.apache.kafka.image.{MetadataDelta, MetadataImage, MetadataProvenance}
import org.apache.kafka.metadata.{ConfigRepository, KRaftMetadataCache, MockConfigRepository}
import org.apache.kafka.network.metrics.RequestChannelMetrics
import org.apache.kafka.raft.{KRaftConfigs, QuorumConfig}
import org.apache.kafka.server.{ClientMetricsManager, FetchManager, SimpleApiVersionManager}
import org.apache.kafka.server.authorizer.{AuthorizationResult, Authorizer}
import org.apache.kafka.server.common.{FinalizedFeatures, KRaftVersion, MetadataVersion, RequestLocal}
import org.apache.kafka.server.quota.{ClientQuotaManager, ControllerMutationQuotaManager, ReplicationQuotaManager}
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
import java.util.Optional
import scala.collection.Map
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
  private val metadataCache = new KRaftMetadataCache(brokerId, () => KRaftVersion.LATEST_PRODUCTION)
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
    listenerName: ListenerName = ListenerName.normalised("HTTP")
  ): RequestChannel.Request = {
    val header = new RequestHeader(request.apiKey, request.version, clientId, 0)
    val buffer = request.serializeWithHeader(header)
    val parsedHeader = RequestHeader.parse(buffer)
    val principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice")
    val context = new RequestContext(parsedHeader, "1", InetAddress.getLocalHost, Optional.empty(),
      principal, listenerName, securityProtocol,
      ClientInformation.EMPTY, false, Optional.of(kafkaPrincipalSerde))
    new RequestChannel.Request(processor = 1, context = context, startTimeNanos = 0,
      MemoryPool.NONE, buffer, requestChannelMetrics, envelope = None)
  }

  // -------------------------------------------------------------------
  // Metadata cache setup: registers brokers, creates topic + partitions
  // with leader = brokerId (this broker) so local-leader tests work.
  // -------------------------------------------------------------------
  private def setupLocalLeaderMetadataCache(topic: String, numPartitions: Int, numBrokers: Int, topicId: Uuid): Unit = {
    val delta = new MetadataDelta.Builder().setImage(MetadataImage.EMPTY).build()

    // Register brokers
    (0 until numBrokers).foreach { id =>
      val endpoints = new BrokerEndpointCollection()
      endpoints.add(new BrokerEndpoint()
        .setHost("broker" + id).setPort(9092)
        .setSecurityProtocol(SecurityProtocol.PLAINTEXT.id)
        .setName("PLAINTEXT"))
      delta.replay(new RegisterBrokerRecord()
        .setBrokerId(id).setRack("rack").setFenced(false)
        .setEndPoints(endpoints).setBrokerEpoch(id * 100L))
    }

    // Create topic
    delta.replay(new TopicRecord()
      .setTopicId(topicId).setName(topic))

    // Create partitions (leader = brokerId for local tests)
    (0 until numPartitions).foreach { p =>
      delta.replay(new PartitionRecord()
        .setTopicId(topicId).setPartitionId(p)
        .setLeader(brokerId).setLeaderEpoch(0)
        .setPartitionEpoch(0)
        .setReplicas(util.List.of(brokerId))
        .setIsr(util.List.of(brokerId)))
    }

    metadataCache.setImage(delta.apply(MetadataProvenance.EMPTY))
  }

  // -------------------------------------------------------------------
  // Helper: build a ProduceRequest for the given topic
  // -------------------------------------------------------------------
  // Use version 11 (name-based, not topic-ID-based which is v13+)
  private val produceVersion: Short = 11

  private def buildProduceRequest(topicName: String): ProduceRequest = {
    val produceData = new ProduceRequestData()
    val topicData = new ProduceRequestData.TopicProduceData()
      .setName(topicName)
    val partitionData = new ProduceRequestData.PartitionProduceData()
      .setIndex(0)
      .setRecords(MemoryRecords.withRecords(
        Compression.NONE,
        new SimpleRecord("value".getBytes)))
    topicData.partitionData().add(partitionData)
    produceData.topicData().add(topicData)
    produceData.setAcks((-1).toShort)
    produceData.setTimeoutMs(5000)

    new ProduceRequest.Builder(
      produceVersion, produceVersion,
      produceData).build()
  }

  // -------------------------------------------------------------------
  // Helper: build a FetchRequest for the given topic
  // -------------------------------------------------------------------
  private def buildFetchRequest(topicName: String, topicId: Uuid, maxWait: Int = 500): FetchRequest = {
    val tp = new TopicPartition(topicName, 0)
    val fetchData = util.Map.of(tp, new FetchRequest.PartitionData(topicId, 0, 0, 1024 * 1024, Optional.empty()))
    new FetchRequest.Builder(
      ApiKeys.FETCH.latestVersion(), ApiKeys.FETCH.latestVersion(),
      -1, -1, maxWait, 1, fetchData).build()
  }

  // ===================================================================
  // Test 1: PRODUCE + HTTP -> handleHttpProduceRequest
  // ===================================================================
  @Test
  def testProduceWithHttpProtocolRoutesToHttpHandler(): Unit = {
    kafkaApis = createKafkaApis()
    val topicId = Uuid.randomUuid()
    setupLocalLeaderMetadataCache("test-topic", 1, 2, topicId)

    val produceRequest = buildProduceRequest("test-topic")
    val request = buildRequest(produceRequest) // default SecurityProtocol.HTTP

    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    kafkaApis.handle(request, RequestLocal.withThreadConfinedCaching)

    // Verify replicaManager.appendRecords was called (HTTP produce path)
    verify(replicaManager).appendRecords(
      anyLong(), anyShort(), anyBoolean(), any(), any(), any(),
      any(), any(), any(), any())
  }

  // ===================================================================
  // Test 2: FETCH + HTTP -> handleHttpConsumeRequest
  // ===================================================================
  @Test
  def testFetchWithHttpProtocolRoutesToHttpConsumeHandler(): Unit = {
    kafkaApis = createKafkaApis()
    val topicId = Uuid.randomUuid()
    setupLocalLeaderMetadataCache("test-topic", 1, 2, topicId)

    val fetchRequest = buildFetchRequest("test-topic", topicId)
    val request = buildRequest(fetchRequest) // default SecurityProtocol.HTTP

    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    kafkaApis.handle(request, RequestLocal.withThreadConfinedCaching)

    // Verify replicaManager.fetchMessages was called (HTTP consume path)
    verify(replicaManager).fetchMessages(any(), any(), any(), any())
  }

  // ===================================================================
  // Test 3: PRODUCE + PLAINTEXT -> normal handleProduceRequest (regression)
  // ===================================================================
  @Test
  def testProduceWithPlaintextProtocolRoutesToNormalHandler(): Unit = {
    kafkaApis = createKafkaApis()
    val topicId = Uuid.randomUuid()
    setupLocalLeaderMetadataCache("test-topic", 1, 2, topicId)

    val produceRequest = buildProduceRequest("test-topic")

    // Use PLAINTEXT, not HTTP
    val request = buildRequest(produceRequest,
      SecurityProtocol.PLAINTEXT,
      ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT))

    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    kafkaApis.handle(request, RequestLocal.withThreadConfinedCaching)

    // Normal path goes through handleProduceAppend, not appendRecords
    verify(replicaManager).handleProduceAppend(
      anyLong(), anyShort(), anyBoolean(), any(), any(), any(),
      any(), any(), any())
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
    setupLocalLeaderMetadataCache("secret-topic", 1, 2, topicId)

    val produceRequest = buildProduceRequest("secret-topic")
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
    val topicProduceResponse = produceResponse.data.responses.asScala.head
    val partitionResp = topicProduceResponse.partitionResponses.asScala.head
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
    setupLocalLeaderMetadataCache("my-topic", 1, 2, topicId)

    val produceRequest = buildProduceRequest("my-topic")
    val request = buildRequest(produceRequest, SecurityProtocol.HTTP)

    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    kafkaApis.handle(request, RequestLocal.withThreadConfinedCaching)

    // Verify appendRecords was called on the local leader
    verify(replicaManager).appendRecords(
      anyLong(), anyShort(), anyBoolean(), any(), any(), any(),
      any(), any(), any(), any())
  }

  // ===================================================================
  // Test 6: HTTP PRODUCE, not leader -> response sent (not appendRecords)
  // ===================================================================
  @Test
  def testHttpProduceNotLeaderReturnsLeaderNotAvailable(): Unit = {
    kafkaApis = createKafkaApis()
    val topicId = Uuid.randomUuid()

    // Set up metadata with a DIFFERENT broker as leader (broker 0, not brokerId=1)
    val delta = new MetadataDelta.Builder().setImage(MetadataImage.EMPTY).build()
    (0 until 2).foreach { id =>
      val endpoints = new BrokerEndpointCollection()
      endpoints.add(new BrokerEndpoint()
        .setHost("broker" + id).setPort(9092)
        .setSecurityProtocol(SecurityProtocol.PLAINTEXT.id)
        .setName("PLAINTEXT"))
      delta.replay(new RegisterBrokerRecord()
        .setBrokerId(id).setRack("rack").setFenced(false)
        .setEndPoints(endpoints).setBrokerEpoch(id * 100L))
    }
    delta.replay(new TopicRecord()
      .setTopicId(topicId).setName("remote-topic"))
    // Leader is broker 0, not brokerId=1
    delta.replay(new PartitionRecord()
      .setTopicId(topicId).setPartitionId(0)
      .setLeader(0).setLeaderEpoch(0).setPartitionEpoch(0)
      .setReplicas(util.List.of(0)).setIsr(util.List.of(0)))
    metadataCache.setImage(delta.apply(MetadataProvenance.EMPTY))

    val produceRequest = buildProduceRequest("remote-topic")
    val request = buildRequest(produceRequest, SecurityProtocol.HTTP)

    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    kafkaApis.handle(request, RequestLocal.withThreadConfinedCaching)

    // With no produceForwardManager wired, remote partitions get LEADER_NOT_AVAILABLE.
    // The response should be sent directly (appendRecords should NOT be called).
    val capturedResponse: ArgumentCaptor[AbstractResponse] =
      ArgumentCaptor.forClass(classOf[AbstractResponse])
    verify(requestChannel).sendResponse(
      ArgumentMatchers.eq(request), capturedResponse.capture(), any())

    // appendRecords should not be called for a partition where this broker is not the leader
    verify(replicaManager, never()).appendRecords(
      anyLong(), anyShort(), anyBoolean(), any(), any(), any(),
      any(), any(), any(), any())
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
    setupLocalLeaderMetadataCache("test-topic", 1, 2, topicId)

    // Client requests 30s maxWait, but config caps at 5s
    val fetchRequest = buildFetchRequest("test-topic", topicId, maxWait = 30000)
    val request = buildRequest(fetchRequest, SecurityProtocol.HTTP)

    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    kafkaApis.handle(request, RequestLocal.withThreadConfinedCaching)

    // Verify the request property was set to the clamped value
    val applied = request.requestLocalProperties.get("httpMaxWaitApplied")
    assertNotNull(applied, "httpMaxWaitApplied should be set in request properties")
    assertEquals(5000, applied.asInstanceOf[Int])

    // Verify fetchMessages was called (the handler proceeded)
    verify(replicaManager).fetchMessages(any(), any(), any(), any())
  }

  // ===================================================================
  // Test 8: HTTP CONSUME with local leader -> fetchMessages called
  // ===================================================================
  @Test
  def testHttpConsumeLocalFetch(): Unit = {
    kafkaApis = createKafkaApis()
    val topicId = Uuid.randomUuid()
    setupLocalLeaderMetadataCache("test-topic", 1, 2, topicId)

    val fetchRequest = buildFetchRequest("test-topic", topicId)
    val request = buildRequest(fetchRequest, SecurityProtocol.HTTP)

    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    kafkaApis.handle(request, RequestLocal.withThreadConfinedCaching)

    // Verify fetchMessages was called on the local leader
    verify(replicaManager).fetchMessages(any(), any(), any(), any())
  }
}
