# HTTP Protocol for Apache Kafka — Design Document

## Table of Contents

1. [Overview](#1-overview)
2. [Goals & Non-Goals](#2-goals--non-goals)
3. [Architecture Overview](#3-architecture-overview)
4. [HTTP API Specification](#4-http-api-specification)
   - 4.1 Produce
   - 4.2 Consume
   - 4.3 Additional Operations (Metadata, Offsets, Lag, Health)
5. [Produce Path](#5-produce-path)
6. [Consume Path](#6-consume-path)
7. [Broker-to-Broker Internal Forwarding](#7-broker-to-broker-internal-forwarding)
8. [Integration with Existing Kafka Infrastructure](#8-integration-with-existing-kafka-infrastructure)
9. [New Module: `http-server`](#9-new-module-http-server)
10. [Configuration](#10-configuration)
11. [Error Handling](#11-error-handling)
12. [Security](#12-security)
13. [Implementation Plan](#13-implementation-plan)
14. [Implementation Concerns](#14-implementation-concerns)

---

## 1. Overview

This document describes the design for adding native HTTP protocol support to Apache Kafka brokers. The feature allows clients to **produce** and **consume** Kafka records over plain HTTP/1.1 or HTTP/2 without the Kafka binary protocol or a separate gateway.

### Key Properties

| Property | Behavior |
|---|---|
| **Produce** | Client sends HTTP POST to any broker → broker forwards internally to the correct partition leader → returns offset |
| **Consume** | Client sends HTTP POST to any broker → broker fans out fetch requests to the correct leaders → aggregates all partition data → returns in single response |
| **Protocol** | HTTP/1.1 with JSON body; HTTP/2 as a future upgrade |
| **Integration** | Plugs into the existing `SocketServer → RequestChannel → KafkaApis` pipeline |
| **Forwarding** | Broker-to-broker forwarding uses the existing Kafka binary protocol (PRODUCE / FETCH API) to keep the forwarding path lean and leverage existing ack/ISR semantics |

### Motivation

- Enable polyglot clients (JavaScript, cURL, mobile) without a separate gateway or proxy
- Leverage existing broker auth (mTLS, SASL) for HTTP endpoints from day one

---

## 2. Goals & Non-Goals

### Goals

- HTTP produce: POST a batch of records for one or more topic-partitions; broker forwards to correct leader transparently
- HTTP consume: POST a fetch request for one or more topic-partitions; broker aggregates from correct leaders and returns unified response
- First-class error mapping: every Kafka `Errors` value maps to an appropriate HTTP status + machine-readable error body
- Configurable via standard `listeners` / `listener.security.protocol.map` mechanism

### Non-Goals

- WebSocket streaming (future work)
- HTTP/2 push (future work)
- Schema Registry integration (out of scope; let clients handle serialization)
- Admin operations over HTTP (topic creation, ACL management) — phase 2
- Replacing the Kafka binary protocol

---

## 3. Architecture Overview

```
┌──────────────────────────────────────────────────────────────────────────┐
│                           Kafka Broker                                   │
│                                                                          │
│  Port 9094 (HTTP)              Port 9092 (PLAINTEXT — unchanged)         │
│  ┌───────────────────────┐     ┌──────────────────────────────────────┐  │
│  │     HttpAcceptor      │     │         DataPlaneAcceptor            │  │
│  │  (Netty ServerBootstrap)    │         (existing NIO Acceptor)      │  │
│  │                       │     │                                      │  │
│  │  ┌─────────────────┐  │     └──────────────────┬───────────────────┘  │
│  │  │ HttpServerCodec │  │                        │                      │
│  │  │ ObjectAggregator│  │                        │                      │
│  │  │ RequestHandler  │  │                        │                      │
│  │  └────────┬────────┘  │                        │                      │
│  └───────────┼───────────┘                        │                      │
│              │                                    │                      │
│              └──────────────┬─────────────────────┘                      │
│                             ▼                                             │
│                   ┌──────────────────────┐                               │
│                   │    RequestChannel    │  ◄── shared, protocol-agnostic│
│                   └──────────┬───────────┘                               │
│                              │                                            │
│                   ┌──────────▼───────────┐                               │
│                   │  KafkaRequestHandler │  ◄── thread pool, unchanged   │
│                   │     thread pool      │                                │
│                   └──────────┬───────────┘                               │
│                              │                                            │
│                   ┌──────────▼───────────────────────────────────────┐   │
│                   │                    KafkaApis                     │   │
│                   │   handleHttpProduceRequest()                     │   │
│                   │   handleHttpConsumeRequest()                     │   │
│                   └───────┬───────────────────────┬──────────────────┘   │
│                           │                       │                       │
│               MetadataCache lookup          is this broker leader?        │
│                           │              YES ──────────┐                  │
│                           │                            │     NO           │
│                           │              ┌─────────────▼──┐  ┌──────────┐│
│                           │              │  ReplicaManager │  │ Forward  ││
│                           │              │  appendRecords()│  │ Manager  ││
│                           │              │  fetchMessages()│  │(§7)      ││
│                           │              └─────────────┬──┘  └────┬─────┘│
│                           │                            │           │      │
└───────────────────────────┼────────────────────────────┼───────────┼──────┘
                            │                            │           │
                  CompletableFuture merge         local log    binary PRODUCE/FETCH
                  → HTTP response                 + ISR        → remote brokers
```

HTTP requests and binary-protocol requests share the **same `RequestChannel` queue and the same
`KafkaRequestHandler` thread pool**. This is intentional: it avoids a separate handler pool,
reuses all existing quota, metrics, and throttling hooks, and keeps the footprint minimal.
If priority separation is ever needed, a dedicated queue can be introduced later without
changing the HTTP layer.

### Thread Model

```
                      ┌─────────────────────────────────────────────┐
  Port 9094           │  Netty Boss Thread (1)                      │
  HTTP requests ─────►│  accepts TCP connections                    │
                      └───────────────────┬─────────────────────────┘
                                          │ distributes connections
                      ┌───────────────────▼─────────────────────────┐
                      │  Netty Worker Threads (num.http.network.threads, default=4) │
                      │  • read HTTP frames off the socket           │
                      │  • HttpObjectAggregator buffers full body    │
                      │  • HttpRequestHandler translates → Request   │
                      │  • writes HTTP responses back to socket      │
                      └───────────────────┬─────────────────────────┘
                                          │ RequestChannel.sendRequest()
                      ┌───────────────────▼─────────────────────────┐
                      │  RequestChannel queue (shared)               │
                      │  ← also receives binary protocol requests    │
                      └───────────────────┬─────────────────────────┘
                                          │ receiveRequest() blocking dequeue
                      ┌───────────────────▼─────────────────────────┐
                      │  KafkaRequestHandler pool (num.io.threads)   │
                      │  • runs KafkaApis.handle()                   │
                      │  • routes by ApiKey (PRODUCE/FETCH/METADATA…)│
                      │  • calls ReplicaManager (local leader path)  │
                      │  • enqueues into ProduceForwardThread /      │
                      │    FetchForwardThread (remote leader path)   │
                      │  • waits on CompletableFuture (async)        │
                      └──────────┬───────────────────┬──────────────┘
              enqueue()          │                   │  enqueue()
              ┌──────────────────▼──┐           ┌───▼──────────────────┐
              │  ProduceForwardThread│           │  ProduceForwardThread│
              │  extends             │    ...    │  extends             │
              │  InterBrokerSendThread│          │  InterBrokerSendThread│
              │  → Broker 1          │          │  → Broker N          │
              │  NetworkClient       │          │  NetworkClient       │
              │  generateRequests()  │          │  generateRequests()  │
              │  poll loop           │          │  poll loop           │
              └──────────┬──────────┘           └───────────┬──────────┘
                         │  future.complete()                │ future.complete()
                         └──────────────────┬────────────────┘
                                            │ CompletableFuture resolved
                                            ▼
                              KafkaRequestHandler resumes,
                              serializes response → HTTP reply
                              via Netty worker thread
```

---

## 4. HTTP API Specification

Base path: `/v1`

All requests and responses use `Content-Type: application/json`.

### 4.1 Produce

```
POST /v1/topics/{topicName}/records
```

#### Request Body

```json
{
  "records": [
    {
      "partition": 0,
      "key":   { "type": "STRING", "data": "order-123" },
      "value": { "type": "BINARY", "data": "<base64>" },
      "headers": [
        { "name": "source", "value": "checkout-service" }
      ]
    },
    {
      "value": { "type": "JSON", "data": { "amount": 42.0 } }
    }
  ],
  "acks": "all",
  "timeoutMs": 5000
}
```

| Field | Type | Default | Description |
|---|---|---|---|
| `records` | array | required | One or more records to produce |
| `records[].partition` | int | null | Target partition. Null → broker picks via sticky partitioner |
| `records[].key` | DataObject | null | Record key |
| `records[].value` | DataObject | null | Record value |
| `records[].headers` | array | [] | List of `{name, value}` string pairs |
| `acks` | `"all"` \| `"leader"` \| `"none"` | `"all"` | Durability level (maps to Kafka acks -1/1/0) |
| `timeoutMs` | int | 30000 | Max time to wait for broker acknowledgement |

**DataObject** — exactly one type field is set:

```json
{ "type": "STRING", "data": "text value" }
{ "type": "BINARY", "data": "<base64-encoded bytes>" }
{ "type": "JSON",   "data": { ...any JSON... } }
{ "type": "NULL" }
```

#### Response Body (200 OK)

```json
{
  "offsets": [
    { "partition": 0, "offset": 10042, "errorCode": 0, "errorMessage": null },
    { "partition": 2, "offset": 3318,  "errorCode": 0, "errorMessage": null }
  ]
}
```

#### Error Response (4xx / 5xx)

```json
{
  "errorCode": 3,
  "errorMessage": "This server is not the leader for that topic-partition.",
  "detail": "UNKNOWN_TOPIC_OR_PARTITION"
}
```

---

### 4.2 Consume (Fetch)

```
POST /v1/topics/{topicName}/records:fetch
```

Using POST (not GET) because the fetch request carries substantial per-partition offset state
that does not fit naturally in a GET query string.

#### Request Body

```json
{
  "partitions": [
    { "partition": 0, "offset": 100 },
    { "partition": 1, "offset": 50  }
  ],
  "maxWaitMs":    500,
  "minBytes":     1,
  "maxBytes":     10485760,
  "maxBytesPerPartition": 1048576,
  "isolationLevel": "READ_COMMITTED"
}
```

| Field | Type | Default | Description |
|---|---|---|---|
| `partitions` | array | required | List of `{partition, offset}` pairs to fetch |
| `partitions[].partition` | int | required | Partition id |
| `partitions[].offset` | long | required | Fetch from this offset (inclusive) |
| `maxWaitMs` | int | 500 | Max wait if fewer than `minBytes` available — capped server-side (see §6.1) |
| `minBytes` | int | 1 | Min bytes to accumulate before responding |
| `maxBytes` | int | 10485760 | Max total bytes across all partitions |
| `maxBytesPerPartition` | int | 1048576 | Per-partition byte cap |
| `isolationLevel` | `"READ_COMMITTED"` \| `"READ_UNCOMMITTED"` | `"READ_UNCOMMITTED"` | Transaction isolation |

#### Response Body (200 OK)

```json
{
  "partitions": [
    {
      "partition": 0,
      "highWatermark": 10050,
      "records": [
        {
          "offset": 100,
          "timestamp": 1713260400000,
          "key":   { "type": "STRING", "data": "order-123" },
          "value": { "type": "JSON",   "data": { "amount": 42.0 } },
          "headers": [{ "name": "source", "value": "checkout-service" }]
        }
      ],
      "errorCode": 0,
      "errorMessage": null
    }
  ]
}
```

---

### 4.3 Additional Operations

Beyond produce and consume the HTTP listener exposes four supporting operations.
Admin-plane operations (topic create/delete, broker configs, ACLs, partition reassignments)
are intentionally excluded — they belong on the binary protocol / `AdminClient` channel,
not a data-plane port.

---

#### 4.3.1 Topic Metadata — `GET /v1/topics/{topic}`

```
GET /v1/topics/orders
```

```json
{
  "topic": "orders",
  "partitions": [
    {
      "partition": 0,
      "leader":   { "brokerId": 1, "host": "broker1.example.com", "port": 9092 },
      "replicas": [1, 2, 3],
      "isr":      [1, 2, 3]
    },
    {
      "partition": 1,
      "leader":   { "brokerId": 2, "host": "broker2.example.com", "port": 9092 },
      "replicas": [2, 3, 1],
      "isr":      [2, 3]
    }
  ]
}
```

**Kafka API used:** `METADATA` (ApiKey id=3)

`HttpRequestTranslator` builds a `MetadataRequest` for the named topic and places it on
`RequestChannel` with `apiKey = ApiKeys.METADATA`. `KafkaApis.handleTopicMetadataRequest()`
handles it identically to a binary-protocol metadata request — authorization check
(`DESCRIBE` on topic), `MetadataCache.getTopicMetadata()` lookup, ISR/leader resolution —
then writes a `MetadataResponse` back. `HttpResponseSerializer` projects the response into the
JSON shape above.

This means the metadata endpoint gets authorization, metrics, and error handling for free with
zero new broker-side logic.

---

#### 4.3.2 List Offsets — `GET /v1/topics/{topic}/partitions/{partition}/offsets`

Clients use this to know where to start consuming.

```
GET /v1/topics/orders/partitions/0/offsets?timestamp=earliest
GET /v1/topics/orders/partitions/0/offsets?timestamp=latest
GET /v1/topics/orders/partitions/0/offsets?timestamp=max
GET /v1/topics/orders/partitions/0/offsets?timestamp=1713260400000
```

`timestamp` values map to `ListOffsetsRequest` constants:

| Query value | Wire value | Meaning |
|---|---|---|
| `earliest` | `-2` (`EARLIEST_TIMESTAMP`) | First available offset |
| `latest` | `-1` (`LATEST_TIMESTAMP`) | Log-end offset |
| `max` | `-3` (`MAX_TIMESTAMP`) | Offset of the record with the largest timestamp |
| `<epoch ms>` | positive long | First offset at or after the given epoch millisecond |

```json
{
  "partition": 0,
  "offset": 10042,
  "timestamp": 1713260400000
}
```

**Kafka API used:** `LIST_OFFSETS` (ApiKey id=2)

`HttpRequestTranslator` builds a `ListOffsetsRequest` and routes it through
`KafkaApis.handleListOffsetRequest()`. The handler calls
`ReplicaManager.fetchOffset()` → `fetchOffsetForTimestamp(TopicPartition, timestamp,
isolationLevel, currentLeaderEpoch, fetchOnlyFromLeader)` on the partition leader.
Authorization (`READ` on topic) and leader-routing are handled inside the existing handler.

---

#### 4.3.3 Consumer Group Lag — `GET /v1/consumer-groups/{group}/lags`

Returns committed offset + log-end offset per partition, so the client can compute lag.

```
GET /v1/consumer-groups/checkout-consumer/lags
```

```json
{
  "group": "checkout-consumer",
  "totalLag": 1543,
  "partitions": [
    {
      "topic": "orders",
      "partition": 0,
      "committedOffset": 8500,
      "logEndOffset":   10043,
      "lag":            1543
    }
  ]
}
```

**Kafka APIs used — both routed through existing `KafkaApis` handlers:**

1. `OFFSET_FETCH` (ApiKey id=9) → `KafkaApis.handleOffsetFetchRequest()` →
   `GroupCoordinator.fetchOffsets()` — returns committed offsets per partition for the group
2. `LIST_OFFSETS` (ApiKey id=2) → `KafkaApis.handleListOffsetRequest()` with
   `timestamp = -1` (LATEST) — returns log-end offset per partition

Both calls are made concurrently via `CompletableFuture`. Lag = logEndOffset − committedOffset,
computed in `HttpResponseSerializer` before serializing the JSON. Authorization (`DESCRIBE` on
group + `READ` on each topic partition) is enforced inside the existing handlers.

---

#### 4.3.4 Health Check — `GET /v1/health`

```json
{ "status": "RUNNING", "brokerId": 3, "clusterId": "abc123" }
```

Returns 200 if the broker is in `RUNNING` state, 503 otherwise. Used by load balancers.

Served directly (no `RequestChannel` round-trip needed — it is a zero-dependency local read):

```scala
// BrokerServer.scala (KRaft mode)
val state: BrokerState = lifecycleManager.state
// BrokerState.RUNNING → 200, anything else → 503
```

`BrokerState` is an enum: `NOT_RUNNING`, `STARTING`, `RECOVERY`, `RUNNING`, `PENDING_CONTROLLED_SHUTDOWN`, `SHUTTING_DOWN`.

---

#### 4.3.5 List Topics — `GET /v1/topics`

```json
{
  "topics": ["orders", "payments", "inventory"]
}
```

Served from `MetadataCache.getAllTopics()`. No I/O.

---

## 5. Produce Path

A single `POST /v1/topics/{topic}/records` request may carry records for **multiple partitions**
of the same topic. Each record carries an optional `partition` field; the broker assigns one
(via key-hash or batch-sticky round-robin — see §5.3) if omitted. Different partitions may have different leaders across the
cluster, so the receiving broker fans out the batch concurrently — appending locally for
partitions it leads and forwarding over the binary protocol for the rest.

### 5.1 End-to-end: Happy Path

Scenario: topic `orders`, 3 partitions. Client hits **Broker 3**.
Broker 3 is leader for P2 (local). Broker 1 leads P0. Broker 2 leads P1.

```
HTTP Client          Broker 3                                    Broker 1        Broker 2
     │                  │                                            │               │
     │  POST /v1/topics/orders/records                               │               │
     │  { records: [                                                 │               │
     │      {P0, rec0},   ← will forward to B1                       │               │
     │      {P1, rec1},   ← will forward to B2                       │               │
     │      {P2, rec2},   ← local (B3 is leader)                     │               │
     │      {P1, rec3}    ← will forward to B2                       │               │
     │  ], acks:"all" }                                              │               │
     │──────────────────►│                                           │               │
     │                   │                                           │               │
     │             ┌─────▼──────────────────────────────────┐       │               │
     │             │  HttpAcceptor (Netty)                   │       │               │
     │             │  HttpObjectAggregator buffers body      │       │               │
     │             │  HttpRequestHandler → RequestChannel    │       │               │
     │             └─────┬──────────────────────────────────┘       │               │
     │                   │                                           │               │
     │             ┌─────▼──────────────────────────────────┐       │               │
     │             │  KafkaRequestHandler (thread pool)      │       │               │
     │             │  KafkaApis.handleHttpProduceRequest()   │       │               │
     │             │                                         │       │               │
     │             │  MetadataCache lookup:                  │       │               │
     │             │    P0 → leader = Broker 1               │       │               │
     │             │    P1 → leader = Broker 2               │       │               │
     │             │    P2 → leader = Broker 3 (local ✓)    │       │               │
     │             │                                         │       │               │
     │             │  Bucket by leader:                      │       │               │
     │             │    LOCAL   : { P2: [rec2] }             │       │               │
     │             │    Broker 1: { P0: [rec0] }             │       │               │
     │             │    Broker 2: { P1: [rec1, rec3] }       │       │               │
     │             └─────┬──────────────────────────────────┘       │               │
     │                   │                                           │               │
     │         ┌─────────┼──────────────────┐                       │               │
     │         │         │                  │                        │               │
     │    ┌────▼────┐    │           ┌──────▼───────┐               │               │
     │    │ Replica │    │           │ProduceForward│               │               │
     │    │ Manager │    │           │   Manager    │               │               │
     │    │ (local) │    │           └──┬───────┬───┘               │               │
     │    │ append  │    │              │       │                    │               │
     │    │ P2:rec2 │    │    Thread-1◄─┘       └─►Thread-2         │               │
     │    │ ISR ack │    │  (→ Broker1)           (→ Broker2)       │               │
     │    └────┬────┘    │       │                     │            │               │
     │         │         │       │  binary PRODUCE     │            │               │
     │         │         │       │  { P0:[rec0] }      │  binary PRODUCE            │
     │         │         │       │──────────────────────────────────►  {P1:[rec1,rec3]}
     │         │         │       │                     │────────────────────────────►
     │         │         │       │                     │            │               │
     │         │         │       │  ProduceResponse    │  ISR ack   │  ISR ack      │
     │         │         │       │  { P0: offset=1042 }│            │               │
     │         │         │       │◄─────────────────────────────────│               │
     │         │         │       │                     │  ProduceResponse            │
     │         │         │       │                     │  { P1: offset=887,888 }     │
     │         │         │       │                     │◄───────────────────────────│
     │         │         │       │                     │            │               │
     │    future_local   │  future1.complete()    future2.complete()│               │
     │    {P2:offset=319}│  {P0:offset=1042}      {P1:offset=887,888}               │
     │         │         │       │                     │            │               │
     │         └─────────┘       └──────────┬──────────┘            │               │
     │                   │                  │                        │               │
     │             ┌─────▼──────────────────▼────────────────┐      │               │
     │             │  CompletableFuture.allOf(                │      │               │
     │             │    future_local, future1, future2         │      │               │
     │             │  ).orTimeout(timeoutMs)                  │      │               │
     │             │                                          │      │               │
     │             │  merge: P0→1042, P1→887, P1→888, P2→319 │      │               │
     │             └─────────────────────────────────────────┘      │               │
     │                   │                                           │               │
     │  HTTP 200         │                                           │               │
     │  { "offsets": [   │                                           │               │
     │    {P0, 1042},    │                                           │               │
     │    {P1, 887 },    │                                           │               │
     │    {P2, 319 },    │                                           │               │
     │    {P1, 888 }     │                                           │               │
     │  ]}               │                                           │               │
     │◄──────────────────│                                           │               │
```

### 5.2 End-to-end: Timeout / Partial Failure

Broker 2 does not respond within `timeoutMs = 5000ms`.

```
HTTP Client          Broker 3                           Broker 1        Broker 2
     │                  │                                   │          (unresponsive)
     │  POST /v1/topics/orders/records                      │               │
     │  { records: [{P0,rec0},{P1,rec1}], timeoutMs:5000 }  │               │
     │──────────────────►│                                  │               │
     │                   │                                  │               │
     │             KafkaApis: P0→B1, P1→B2                 │               │
     │                   │                                  │               │
     │         ┌─────────┴──────────────┐                  │               │
     │         │                        │                   │               │
     │   ProduceForwardThread-1    ProduceForwardThread-2   │               │
     │         │                        │                   │               │
     │         │  binary ProduceRequest │  binary ProduceRequest            │
     │         │─────────────────────────────────────────►  │               │
     │         │                        │──────────────────────────────────►│
     │         │                        │                   │               │
     │         │  ProduceResponse       │                   │               X  (no reply)
     │         │  {P0: offset=1042}     │                   │               │
     │         │◄──────────────────────────────────────────│               │
     │         │                        │                   │               │
     │  future1.complete({P0:1042})     │                   │               │
     │     t = 120ms ✓                  │                   │               │
     │                                  │ waiting...        │               │
     │                             t = 5000ms               │               │
     │                   │                                  │               │
     │             orTimeout fires                          │               │
     │             future2.completeExceptionally(TimeoutException)          │
     │                   │                                  │               │
     │  HTTP 207 Multi-Status                               │               │
     │  { "offsets": [                                      │               │
     │    { "partition":0, "offset":1042, "errorCode":0  },│               │
     │    { "partition":1, "offset":null, "errorCode":7,   │               │
     │      "errorMessage":"REQUEST_TIMED_OUT" }            │               │
     │  ]}                                                  │               │
     │◄──────────────────│                                  │               │
```

### 5.3 Partition assignment when no partition is specified

The broker does not have a `StickyPartitioner` — that is a client-side abstraction. For HTTP
requests where `partition` is omitted, `HttpRequestTranslator` assigns partitions before
building the `ProduceRequest`, using the following logic:

```
partitionCount = MetadataCache.getTopicMetadata(topic).partitionCount

for each record in batch:
    if record.key != null:
        partition = murmur2(serialize(record.key)) % partitionCount   // matches Java client
    else:
        partition = batchStickyPartition                              // same for all keyless
                                                                       // records in this request

batchStickyPartition = roundRobinCounter.getAndIncrement() % partitionCount
```

Rules:
- Keyed records: deterministic hash (`murmur2`, same algorithm as the Java producer), so the
  same key always lands on the same partition.
- Keyless records: all records in **the same HTTP request** land on the same partition
  (batch-sticky). The sticky partition advances per request via an `AtomicInteger` counter on
  the topic, giving balanced distribution across requests.
- The assigned partition is always echoed back in the response `offsets[].partition` field so
  the client knows where the record landed.

### 5.4 ACK semantics mapping

| HTTP `acks` | Kafka `required_acks` |
|---|---|
| `"none"` | 0 |
| `"leader"` | 1 |
| `"all"` (default) | -1 |

### 5.5 Edge Cases

Grouped by when the error is detected.

#### A. Request validation (before touching Kafka)

| Condition | HTTP status | errorCode | Notes |
|---|---|---|---|
| Body missing or empty | 400 | `INVALID_REQUEST` | |
| `records` array null or empty | 400 | `INVALID_REQUEST` | |
| `records` exceeds `http.produce.max.records` (default 10 000) | 413 | `BATCH_TOO_LARGE` | Prevents OOM on deserialization before any broker work starts |
| `partition` < 0 | 422 | `INVALID_PARTITION` | Value is syntactically present but semantically invalid |
| `acks` not one of `"all"`, `"leader"`, `"none"` | 422 | `INVALID_REQUEST` | |
| `timeoutMs` ≤ 0 | 422 | `INVALID_REQUEST` | |
| Record serialized size > `max.message.bytes` | 413 | `MESSAGE_TOO_LARGE` | Checked before sending; avoids a round-trip to the log |
| BINARY value is not valid base64 | 422 | `INVALID_DATA` | |
| Total request body > `http.request.max.bytes` | 413 | `REQUEST_TOO_LARGE` | Rejected by `HttpObjectAggregator` before handler is called |

#### B. Kafka errors — local append path

These come back from `ReplicaManager.appendRecords()` as `PartitionResponse.error`.

| Kafka error | HTTP status | Notes |
|---|---|---|
| `UNKNOWN_TOPIC_OR_PARTITION` | 404 | Topic or partition does not exist |
| `TOPIC_AUTHORIZATION_FAILED` | 403 | Principal lacks WRITE on topic |
| `CLUSTER_AUTHORIZATION_FAILED` | 403 | Internal topic write without admin privilege |
| `INVALID_TOPIC_EXCEPTION` | 400 | Illegal topic name characters |
| `MESSAGE_TOO_LARGE` | 413 | Record exceeds broker `max.message.bytes` after compression |
| `RECORD_LIST_TOO_LARGE` | 413 | Batch byte size exceeds broker limit |
| `NOT_ENOUGH_REPLICAS` | 503 | ISR < `min.insync.replicas` at time of append |
| `NOT_ENOUGH_REPLICAS_AFTER_APPEND` | 503 | ISR shrank after append but before ack |
| `KAFKA_STORAGE_ERROR` | 500 | Local disk error |
| `REQUEST_TIMED_OUT` | 504 | Broker waited for ISR ack past `timeoutMs` |
| Any other `Errors` value | 500 | Catch-all; log and surface `errorCode` + `errorMessage` |

For single-partition requests the HTTP status maps directly. For multi-partition batches the
status is **207 Multi-Status** and each partition entry carries its own `errorCode`
(same table applies per entry).

#### C. Forwarding-specific edge cases

Unlike a client-side producer, `ProduceForwardThread` owns the full forwarding lifecycle —
leader lookup, connection management, and retry are all explicit responsibilities.

| Condition | Detection point | Handling |
|---|---|---|
| **Leader unknown in MetadataCache** | Before enqueue | Return 503 `LEADER_NOT_AVAILABLE` with `Retry-After: 1` for that partition. Other partitions in the batch proceed normally. |
| **Forward connection failure** (broker unreachable) | `ProduceForwardThread.onComplete(disconnected=true)` | `future.completeExceptionally`. Broker refreshes metadata for affected partitions via `MetadataCache`. Returns 503 for those partitions. |
| **Leader changed between lookup and arrival** | Remote broker returns `NOT_LEADER_OR_FOLLOWER` | **Retry once**: refresh MetadataCache, re-bucket the affected partitions, re-enqueue. If the second attempt also fails, return 503. |
| **Forward timeout** | `orTimeout(http.internal.forwarding.timeout.ms)` fires | 504 `GATEWAY_TIMEOUT` for affected partitions. Other partitions that resolved are still returned. |
| **Remote broker returns retriable error** (`LEADER_NOT_AVAILABLE`, `REQUEST_TIMED_OUT`) | `ProduceForwardThread.onComplete` | Same as leader-changed: retry once. Avoids surfacing transient leader elections to the HTTP client. |
| **Remote broker returns non-retriable error** | `ProduceForwardThread.onComplete` | Map to HTTP status per table B above. No retry. |

**Retry budget:** at most **1 retry per forwarded group**, bounded by `http.internal.forwarding.timeout.ms`.
No retry for local appends (Kafka's own ISR/purgatory already handles that internally).

#### D. Idempotency

HTTP `POST` is not idempotent by spec. `enable.idempotence=true` is explicitly **not** set
on the internal producer used by `ProduceForwardThread`. Clients that need exactly-once
semantics must use the binary Kafka protocol with transactions.

---

## 6. Consume Path

### 6.1 Why `maxWaitMs` must be capped server-side

In the binary protocol the client owns a persistent TCP socket and can hold a fetch open for
minutes with no intermediary interference. Over HTTP this assumption breaks:

| Intermediary | Typical idle-connection timeout |
|---|---|
| AWS ALB | 60 s |
| nginx (proxy_read_timeout) | 60 s |
| Cloudflare | 100 s |
| HAProxy (timeout tunnel) | 3600 s but defaults vary |
| Corporate HTTP proxies | 30–60 s |

A client that passes `maxWaitMs=120000` would have its connection silently dropped long before
the broker responds. The broker would eventually write a response to a dead socket, wasting a
Netty channel, a `KafkaRequestHandler` thread slot, and potentially a `DelayedFetch` purgatory
entry for the entire wait period.

**Rule:** The broker clamps `effectiveMaxWaitMs = min(request.maxWaitMs, http.consume.max.wait.ms)`.
The default cap is **5000 ms**. The broker signals the applied value back in the response header
`X-Kafka-MaxWait-Applied: <ms>` so clients can self-correct.

Clients that need to wait longer than the cap implement a **client-side poll loop** — they
receive an empty-records response, advance no offsets, and immediately re-issue the same fetch.

### 6.2 Fan-out and aggregation

```
HttpAcceptor
  │
  ├─ Parse HTTP body → per-partition fetch specs
  ├─ effectiveMaxWaitMs = min(request.maxWaitMs, http.consume.max.wait.ms)  ← cap enforced here
  │
KafkaApis.handleHttpConsumeRequest(request)
  │
  ├─ For each requested partition:
  │    leader = metadataCache.getLeaderAndIsr(topic, partition)
  │    Bucket into: local (leader == this.brokerId) or remote (leader == otherBrokerId)
  │
  ├─ LOCAL partitions:
  │    replicaManager.fetchMessages(
  │      params = FetchParams(effectiveMaxWaitMs, minBytes, maxBytes, isolationLevel),
  │      fetchInfos = localPartitionFetchSpecs,
  │      responseCallback = { data → store in resultMap }
  │    )
  │
  ├─ REMOTE partitions (grouped by leader broker):
  │    For each (leaderId → partitions) group:
  │      fetchForwardManager.forward(leaderId, partitions, effectiveMaxWaitMs)
  │      → FetchForwardThread sends binary FetchRequest with maxWait=effectiveMaxWaitMs
  │      → remote broker's own DelayedFetch purgatory runs within the same budget
  │      → returns Seq[(TopicIdPartition, FetchPartitionData)]
  │      → merge into resultMap
  │
  ├─ Wait for all futures (bounded by effectiveMaxWaitMs)
  │
  └─ Aggregate resultMap → JSON FetchResponse
     → set header X-Kafka-MaxWait-Applied: <effectiveMaxWaitMs>
     → requestChannel.sendResponse(httpResponse)
```

### 6.3 Timeout budget

```
t=0ms
  ├─ start local fetch (DelayedFetch purgatory, capped at effectiveMaxWaitMs)
  └─ start all remote fetch futures concurrently (each capped at effectiveMaxWaitMs)

t≤effectiveMaxWaitMs
  └─ futures resolve as data becomes available

t=effectiveMaxWaitMs  ← hard ceiling, guaranteed HTTP response by here
  └─ collect whatever arrived; partitions with no data → records:[], errorCode:0
     (empty is not an error — client simply polls again)
```

### 6.4 Client poll loop (reference pattern)

```
offsets = { P0: 100, P2: 50 }

loop:
    response = POST /v1/topics/orders/records:fetch
               { partitions: offsets.entries(), maxWaitMs: 5000 }

    for partition in response.partitions:
        for record in partition.records:
            process(record)
            offsets[partition.id] = record.offset + 1

    // response.header["X-Kafka-MaxWait-Applied"] tells you the actual cap used
    // no explicit sleep needed — the server already waited up to maxWaitMs
```

### 6.5 Delayed fetch interaction

When a local partition has no data yet, `ReplicaManager.fetchMessages()` creates a
`DelayedFetch` entry in the purgatory watching that partition. The `effectiveMaxWaitMs` cap
means the purgatory entry's TTL is at most 5 s (default), not unbounded. The Netty channel
stays open for at most that long before the broker writes a response (empty or not).

### 6.6 Fetch semantics — intentional simplifications

The HTTP fetch path deliberately omits two advanced binary-protocol features:

**No leader epoch checks.** The binary `FetchRequest` carries `currentLeaderEpoch` per
partition so the broker can reject stale fetches from consumers that haven't caught up to
a leader election. HTTP clients are stateless and do not track epochs. The HTTP fetch
simply omits the epoch field (`currentLeaderEpoch = -1` / not set), which the broker treats
as "any epoch accepted". This is safe for consumption — the worst outcome is reading from a
slightly stale leader during an election window, which self-corrects on the next poll.
Epoch support can be added in phase 3 if strict fencing is required.

**No preferred read replica.** In the binary protocol the leader can redirect a consumer to
a preferred replica (e.g. a closer rack) by returning a `preferredReadReplica` in the fetch
response. The HTTP consume path always fetches from the leader (or forwards to the leader).
Rack-aware reads can be introduced in phase 3 by forwarding to the preferred replica instead
of the leader when the consumer's `FetchRequest` carries client rack information.

### 6.6 End-to-end: Consume with maxWaitMs cap and delayed fetch

Client requests `maxWaitMs:30000` but broker cap is `http.consume.max.wait.ms=5000`.
P2 has no new data → hits `DelayedFetch` purgatory, fires at t=3200ms.

```
HTTP Client          Broker 3                                         Broker 1
     │                  │                                                 │
     │  POST /v1/topics/orders/records:fetch                              │
     │  { partitions: [                                                   │
     │      { partition:0, offset:100 },  ← leader = Broker 1            │
     │      { partition:2, offset:50  }   ← leader = Broker 3 (local)    │
     │  ], maxWaitMs:30000, minBytes:1 }  ← client asks for 30s          │
     │──────────────────►│                                                │
     │                   │                                                │
     │             ┌─────▼────────────────────────────────────┐          │
     │             │  KafkaApis.handleHttpConsumeRequest()     │          │
     │             │                                           │          │
     │             │  effectiveMaxWaitMs =                     │          │
     │             │    min(30000, 5000) = 5000 ms  ← cap      │          │
     │             │                                           │          │
     │             │  MetadataCache lookup:                    │          │
     │             │    P0 → leader = Broker 1  (remote)       │          │
     │             │    P2 → leader = Broker 3  (local ✓)      │          │
     │             └─────┬──────────────────────────────────┬──┘          │
     │                   │                                  │             │
     │         ┌─────────▼──────────┐         ┌────────────▼──────────┐  │
     │         │  ReplicaManager    │         │  FetchForwardManager  │  │
     │         │  .fetchMessages()  │         │  FetchForwardThread-1 │  │
     │         │  { P2: offset=50,  │         └────────────┬──────────┘  │
     │         │    maxWait=5000ms }│                      │             │
     │         └─────────┬──────────┘              binary FetchRequest   │
     │                   │                         { P0: offset=100,     │
     │              no data yet                      maxWait=5000ms }    │
     │                   │                                  │─────────────►
     │         ┌─────────▼──────────┐                      │             │
     │         │  DelayedFetch      │                  B1 fetchMessages()│
     │         │  Purgatory (B3)    │                  (own purgatory    │
     │         │  watching P2       │                   if needed)       │
     │         │  TTL = 5000ms      │                      │             │
     │         └─────────┬──────────┘                 t=180ms: B1 data  │
     │                   │                                  │  binary FetchResponse
     │                   │                                  │  { P0: records[100..115] }
     │                   │                                  │◄────────────│
     │                   │                    future1.complete({P0: records})
     │                   │  t=3200ms: new records arrive on P2            │
     │         ┌─────────▼──────────┐                      │             │
     │         │  DelayedFetch      │                      │             │
     │         │  fires ✓           │                      │             │
     │         │  {P2: records[50..58]}                    │             │
     │         └─────────┬──────────┘                      │             │
     │         future_local.complete({P2: records})         │             │
     │                   │                                  │             │
     │                   └──────────────────┬───────────────┘             │
     │                                      │                             │
     │             ┌────────────────────────▼─────────────────────┐      │
     │             │  CompletableFuture.allOf(future_local,future1)│      │
     │             │  .orTimeout(5000ms)  ← both done at t=3200ms  │      │
     │             │  aggregate: P0→records[100..115], P2→[50..58] │      │
     │             └───────────────────────────────────────────────┘      │
     │                   │                                                 │
     │  HTTP 200         │                                                 │
     │  X-Kafka-MaxWait-Applied: 5000                                      │
     │  { "partitions": [│                                                 │
     │    { "partition":0, "highWatermark":10050,                         │
     │      "records":[{offset:100,...},...] },                           │
     │    { "partition":2, "highWatermark":330,                           │
     │      "records":[{offset:50,...},...] }                             │
     │  ]}               │                                                 │
     │◄──────────────────│                                                 │
```

### 6.7 End-to-end: Cap expires, no data (empty poll)

P2 still has no data when the 5 s cap expires.
Broker returns immediately with `records: []` — client polls again.

```
HTTP Client          Broker 3                                         Broker 1
     │                  │                                                 │
     │  POST /v1/topics/orders/records:fetch                              │
     │  { partitions:[{P0,offset:100},{P2,offset:50}], maxWaitMs:30000 } │
     │──────────────────►│                                                │
     │                   │                                                │
     │             effectiveMaxWaitMs = 5000ms (cap)                      │
     │                   │                                                │
     │         ┌─────────┴──────────────┐                                │
     │  DelayedFetch (P2, TTL=5000ms)   │  binary FetchRequest (P0)      │
     │  watching for new data           │──────────────────────────────►  │
     │                   │             │  binary FetchResponse            │
     │                   │             │  { P0: records[100..115] }       │
     │                   │             │◄─────────────────────────────── │
     │                   │             │  future1.complete ✓ (t=180ms)    │
     │                   │                                                │
     │               t = 5000ms: DelayedFetch TTL expires                 │
     │               future_local.complete({P2: records=[]}) ← empty      │
     │                   │                                                │
     │             ┌─────▼──────────────────────────────────┐            │
     │             │  merge: P0→records[100..115], P2→[]    │            │
     │             └─────────────────────────────────────────┘            │
     │                   │                                                 │
     │  HTTP 200         │                                                 │
     │  X-Kafka-MaxWait-Applied: 5000                                      │
     │  { "partitions": [│                                                 │
     │    { "partition":0, "records":[{offset:100},...] },                │
     │    { "partition":2, "records":[] }  ← empty, not an error         │
     │  ]}               │                                                 │
     │◄──────────────────│                                                 │
     │                   │                                                 │
     │  (client advances P0 offset to 115, keeps P2 at 50, polls again)   │
     │  POST /v1/topics/orders/records:fetch                              │
     │  { partitions:[{P0,offset:115},{P2,offset:50}], maxWaitMs:30000 } │
     │──────────────────►│                                                │
```

---

## 7. Broker-to-Broker Internal Forwarding

### 7.1 Design choice: `InterBrokerSendThread` (existing Kafka internal)

Rather than building a custom `NetworkClient` pool, forwarding is built on **`InterBrokerSendThread`**
(`server-common/src/main/java/org/apache/kafka/server/util/InterBrokerSendThread.java`).

This is an existing Kafka abstract class already used for broker-to-broker binary protocol
communication. The proven precedent is `TransactionMarkerChannelManager`, which subclasses it
to fan out `WriteTxnMarkersRequest` to multiple partition-leader brokers concurrently — exactly
the same pattern we need for produce forwarding.

**Why this is the right base:**

| Concern | How `InterBrokerSendThread` handles it |
|---|---|
| Connection management | `NetworkClient` created once per thread; reconnects automatically |
| Parallel sends to N brokers | One `ProduceForwardThread` instance per remote broker |
| Request queuing | Per-broker `BlockingQueue`; `generateRequests()` drains it each poll cycle |
| Timeout & retry | Built into the `ShutdownableThread` + `NetworkClient` poll loop |
| Auth | Shares `inter.broker.listener.name` security context |
| No new primitives | Zero custom networking code |

`ForwardingManager` and `NodeToControllerChannelManager` were considered and rejected — both
are hardcoded to the active KRaft controller and wrap requests in `EnvelopeRequest`, making them
unsuitable for data-plane forwarding to arbitrary brokers.

### 7.2 `ProduceForwardThread` — class design

```java
// http-server/src/main/java/kafka/server/http/ProduceForwardThread.java

/**
 * One instance per remote leader broker. Drains a queue of pending produce
 * entries and forwards them as binary ProduceRequests over the inter-broker
 * listener. Follows the same pattern as TransactionMarkerChannelManager.
 *
 * @see org.apache.kafka.server.util.InterBrokerSendThread
 * @see kafka.coordinator.transaction.TransactionMarkerChannelManager
 */
public class ProduceForwardThread extends InterBrokerSendThread {

    private final BlockingQueue<PendingProduce> pendingQueue = new LinkedBlockingQueue<>();

    public ProduceForwardThread(Node destination, NetworkClient networkClient,
                                int requestTimeoutMs, Time time) {
        super("ProduceForwardThread-" + destination.id(), networkClient, requestTimeoutMs, time);
    }

    /**
     * Called by KafkaApis handler thread. Returns a CompletableFuture that
     * resolves when the remote broker's ProduceResponse arrives.
     */
    public CompletableFuture<Map<TopicIdPartition, PartitionResponse>> enqueue(
            Map<TopicIdPartition, MemoryRecords> entriesPerPartition,
            short requiredAcks,
            int timeoutMs) {
        CompletableFuture<Map<TopicIdPartition, PartitionResponse>> future = new CompletableFuture<>();
        pendingQueue.add(new PendingProduce(entriesPerPartition, requiredAcks, timeoutMs, future));
        wakeup();  // interrupt the poll sleep so the request is sent immediately
        return future;
    }

    @Override
    public Collection<RequestAndCompletionHandler> generateRequests() {
        List<PendingProduce> batch = new ArrayList<>();
        pendingQueue.drainTo(batch);

        return batch.stream().map(pending -> {
            ProduceRequest.Builder request = ProduceRequest.builder(
                new ProduceRequestData()
                    .setAcks(pending.requiredAcks)
                    .setTimeoutMs(pending.timeoutMs)
                    .setTopicData(toTopicProduceData(pending.entriesPerPartition))
            );

            RequestCompletionHandler handler = response -> {
                if (response.disconnected()) {
                    pending.future.completeExceptionally(
                        new DisconnectException("Lost connection to broker " + destination().id()));
                } else {
                    ProduceResponse produceResponse = (ProduceResponse) response.responseBody();
                    pending.future.complete(parsePartitionResponses(produceResponse));
                }
            };

            return new RequestAndCompletionHandler(
                time.milliseconds(), destination(), request, handler);
        }).collect(toList());
    }
}
```

### 7.3 `ProduceForwardManager` — thread pool per broker

```java
// http-server/src/main/java/kafka/server/http/ProduceForwardManager.java

public class ProduceForwardManager implements Closeable {

    private final ConcurrentHashMap<Integer, ProduceForwardThread> threads =
        new ConcurrentHashMap<>();

    public CompletableFuture<Map<TopicIdPartition, PartitionResponse>> forward(
            int leaderId,
            Map<TopicIdPartition, MemoryRecords> entries,
            short requiredAcks,
            int timeoutMs) {

        ProduceForwardThread thread = threads.computeIfAbsent(leaderId, id -> {
            Node node = metadataCache.getAliveBrokerNode(id, interBrokerListenerName)
                .orElseThrow(() -> new BrokerNotFoundException(id));
            NetworkClient client = buildNetworkClient(node, config);
            ProduceForwardThread t = new ProduceForwardThread(node, client, config.requestTimeoutMs(), time);
            t.start();
            return t;
        });

        return thread.enqueue(entries, requiredAcks, timeoutMs);
    }

    @Override
    public void close() {
        threads.values().forEach(ProduceForwardThread::initiateShutdown);
        threads.values().forEach(t -> { try { t.awaitShutdown(); } catch (Exception e) { /* log */ } });
    }
}
```

### 7.4 Forwarding internals diagram

```
KafkaApis.handleHttpProduceRequest()
  │
  ├─ LOCAL partitions → ReplicaManager.appendRecords()   (direct, no thread hop)
  │
  └─ REMOTE partitions, grouped by leaderId:
       │
       ├─ ProduceForwardManager.forward(leaderId=1, {P0,P2}, ...)
       │       │
       │       └─ ProduceForwardThread-1  (extends InterBrokerSendThread)
       │               BlockingQueue ← PendingProduce{P0,P2, future1}
       │               generateRequests() → RequestAndCompletionHandler
       │               NetworkClient.send(ProduceRequest) ──► Broker 1
       │               onComplete(ProduceResponse) → future1.complete(...)
       │
       └─ ProduceForwardManager.forward(leaderId=2, {P1}, ...)
               │
               └─ ProduceForwardThread-2  (extends InterBrokerSendThread)
                       BlockingQueue ← PendingProduce{P1, future2}
                       generateRequests() → RequestAndCompletionHandler
                       NetworkClient.send(ProduceRequest) ──► Broker 2
                       onComplete(ProduceResponse) → future2.complete(...)

  CompletableFuture.allOf(localFuture, future1, future2)
    .orTimeout(timeoutMs)
    .thenApply(__ -> mergeResults(...))
    .thenAccept(result -> requestChannel.sendResponse(httpResponse))
```

### 7.5 Forwarding decision matrix

```
Produce request arrives at broker B for partition P:

is B the leader for P?
  ├── YES → ReplicaManager.appendRecords() directly (no forwarding)
  └── NO  → does metadataCache know the leader?
             ├── YES (leaderId = L) → ProduceForwardManager.forward(L, ...)
             └── NO (leader unknown) → return HTTP 503 with errorCode=LEADER_NOT_AVAILABLE
```

```
Consume request arrives at broker B for partition P:

is B the leader for P?
  ├── YES → ReplicaManager.fetchMessages() directly
  └── NO  → does metadataCache know the leader?
             ├── YES (leaderId = L) → FetchForwardManager.forward(L, ...)  [same pattern]
             └── NO (leader unknown) → partition entry in response: errorCode=LEADER_NOT_AVAILABLE, records=[]
```

A single HTTP request may result in **mixed** results: some partitions served locally, others
forwarded. The broker always returns a unified HTTP response.

---

## 8. Integration with Existing Kafka Infrastructure

### 8.1 New security protocol: `HTTP`

Add `HTTP` to `SecurityProtocol` enum (id = 4):

```java
// clients/src/main/java/org/apache/kafka/common/security/auth/SecurityProtocol.java
HTTP(4, "HTTP");
```

`listener.security.protocol.map` example:
```properties
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,SSL:SSL,HTTP:HTTP,HTTPS:HTTPS
listeners=PLAINTEXT://0.0.0.0:9092,HTTP://0.0.0.0:9094
advertised.listeners=PLAINTEXT://broker1.example.com:9092,HTTP://broker1.example.com:9094
```

**`inter.broker.listener.name` must remain a binary-protocol listener** (e.g. `PLAINTEXT` or
`SSL`). The HTTP listener must never be set as the inter-broker listener. `ProduceForwardManager`
and `FetchForwardManager` use the inter-broker listener name to resolve leader endpoints for
internal forwarding — they send binary Kafka protocol requests, not HTTP.

### 8.2 HttpAcceptor — plugging into SocketServer

`SocketServer.createDataPlaneAcceptorAndProcessors()` today creates a `DataPlaneAcceptor`
(extends `Acceptor`) for each endpoint. We add a branch:

```scala
// core/src/main/scala/kafka/network/SocketServer.scala

def createDataPlaneAcceptorAndProcessors(endpoint: Endpoint): Unit = {
  endpoint.securityProtocol match {
    case SecurityProtocol.HTTP | SecurityProtocol.HTTPS =>
      val httpAcceptor = new HttpAcceptor(this, endpoint, config, ...)
      httpAcceptors.put(endpoint, httpAcceptor)
    case _ =>
      val dataPlaneAcceptor = new DataPlaneAcceptor(this, endpoint, config, ...)
      dataPlaneAcceptors.put(endpoint, dataPlaneAcceptor)
  }
}
```

### 8.3 HttpAcceptor — Netty-based server

```scala
// core/src/main/scala/kafka/network/HttpAcceptor.scala

class HttpAcceptor(
  socketServer:   SocketServer,
  endpoint:       Endpoint,
  config:         KafkaConfig,
  requestChannel: RequestChannel,
  ...
) extends Closeable {

  private val bossGroup:   EventLoopGroup = new NioEventLoopGroup(1)
  private val workerGroup: EventLoopGroup = new NioEventLoopGroup(config.numHttpNetworkThreads)
  private var channel:     Channel = _

  def startup(): Unit = {
    val bootstrap = new ServerBootstrap()
    bootstrap
      .group(bossGroup, workerGroup)
      .channel(classOf[NioServerSocketChannel])
      .childHandler(new HttpChannelInitializer(config, requestChannel, ...))
    channel = bootstrap.bind(endpoint.host, endpoint.port).sync().channel()
  }

  override def close(): Unit = {
    channel.close().sync()
    workerGroup.shutdownGracefully()
    bossGroup.shutdownGracefully()
  }
}
```

### 8.4 HttpChannelInitializer — Netty pipeline

```scala
class HttpChannelInitializer(config: KafkaConfig, requestChannel: RequestChannel, ...)
    extends ChannelInitializer[SocketChannel] {

  override def initChannel(ch: SocketChannel): Unit = {
    val pipeline = ch.pipeline()
    // Optional TLS (for HTTPS)
    if (endpoint.securityProtocol == SecurityProtocol.HTTPS)
      pipeline.addLast("ssl", sslContext.newHandler(ch.alloc()))
    // HTTP/1.1 codec
    pipeline.addLast("http-codec",      new HttpServerCodec())
    // Aggregate chunked requests into FullHttpRequest (max 64 MB)
    pipeline.addLast("http-aggregator", new HttpObjectAggregator(config.httpMaxRequestSize))
    // Compression support (optional)
    pipeline.addLast("compressor",      new HttpContentCompressor())
    // Our handler: translate HTTP → RequestChannel
    pipeline.addLast("kafka-handler",   new HttpRequestHandler(requestChannel, config, ...))
  }
}
```

### 8.5 HttpRequestHandler — Netty ChannelInboundHandler

The handler translates each HTTP request into a standard Kafka protocol request object and
places it on `RequestChannel`. No new `ApiKeys` are introduced — every HTTP endpoint maps to
an existing wire-protocol request type, so `KafkaApis.handle()` routes it without modification.

| HTTP endpoint | Kafka ApiKey | Request type |
|---|---|---|
| `POST /v1/topics/{t}/records` | `PRODUCE` (0) | `ProduceRequest` |
| `POST /v1/topics/{t}/records:fetch` | `FETCH` (1) | `FetchRequest` |
| `GET /v1/topics/{t}` | `METADATA` (3) | `MetadataRequest` |
| `GET /v1/topics/{t}/partitions/{p}/offsets` | `LIST_OFFSETS` (2) | `ListOffsetsRequest` |
| `GET /v1/consumer-groups/{g}/lags` | `OFFSET_FETCH` (9) + `LIST_OFFSETS` (2) | two requests, merged |
| `GET /v1/topics` | `METADATA` (3) | `MetadataRequest` (empty topics → all) |
| `GET /v1/health` | — | direct `BrokerServer.lifecycleManager.state` read, no `RequestChannel` |

```scala
// http-server/src/main/scala/kafka/network/HttpRequestHandler.scala

class HttpRequestHandler(
  requestChannel:   RequestChannel,
  principalBuilder: KafkaPrincipalBuilder,   // pluggable — see §12.1
  config:           KafkaConfig,
  ...
) extends SimpleChannelInboundHandler[FullHttpRequest] {

  // HTTP_PROCESSOR_ID is a unique Int registered with RequestChannel.addProcessor()
  // at HttpAcceptor startup so the response queue is wired up correctly.

  override def channelRead0(ctx: ChannelHandlerContext, req: FullHttpRequest): Unit = {
    // Special case: health check never touches RequestChannel
    if (req.uri() == "/v1/health") { sendHealthResponse(ctx); return }

    // 1. Translate HTTP → Kafka protocol request (serialised into ByteBuffer)
    val (apiKey, requestBuffer) = HttpRequestTranslator.translate(req)

    // 2. Build RequestContext — principal comes from KafkaPrincipalBuilder (§12.1)
    val principal = principalBuilder.build(new HttpAuthenticationContext(req))
    val requestContext = new RequestContext(
      new RequestHeader(apiKey, apiKey.latestVersion, clientId, correlationId),
      connectionId      = ctx.channel().id().asLongText(),
      clientAddress     = remoteAddress(ctx),
      principal         = principal,
      listenerName      = ListenerName.normalised("HTTP"),
      securityProtocol  = SecurityProtocol.HTTP,
      clientInformation = ClientInformation.EMPTY,
      fromPrivilegedListener = false
    )

    // 3. Stash Netty ctx keyed by correlationId for the response write-back
    val kafkaRequest = new RequestChannel.Request(
      processor      = HTTP_PROCESSOR_ID,
      context        = requestContext,
      startTimeNanos = System.nanoTime(),
      memoryPool     = MemoryPool.NONE,
      buffer         = requestBuffer,
      metrics        = requestChannel.metrics,
      envelope       = None
    )
    pendingRequests.put(kafkaRequest.context.correlationId, ctx)

    // 4. Hand off to KafkaRequestHandler thread pool — identical path to binary protocol
    requestChannel.sendRequest(kafkaRequest)
  }
}
```

**Processor registration** — `HttpAcceptor.startup()` must register the HTTP processor with
`RequestChannel` before accepting connections, so `RequestChannel.sendResponse()` can route
responses back to the correct Netty thread:

```scala
// In HttpAcceptor.startup():
requestChannel.addProcessor(httpProcessor)   // httpProcessor.id == HTTP_PROCESSOR_ID
```

`RequestChannel.sendResponse()` routes by `response.processor` →
`processors.get(response.processor).enqueueResponse(response)` →
the Netty worker thread dequeues and writes the JSON response to the socket.

### 8.6 Response path

`KafkaApis` calls `requestChannel.sendResponse(request, response)` as usual — it is entirely
unaware that the request came from HTTP. `HttpResponseSerializer` runs in the Netty worker thread
when dequeuing the response:

1. Match response type to HTTP endpoint (by correlationId → original URI)
2. Serialize `AbstractResponse` → JSON (e.g. `MetadataResponse` → topic metadata JSON)
3. Write `FullHttpResponse` through the `ChannelHandlerContext` stashed in step 8.5

---

## 9. New Module: `http-server`

To keep the existing `core` and `clients` modules clean, all HTTP-specific code lives in a new
Gradle subproject:

```
kafka/
└── http-server/
    ├── build.gradle
    └── src/
        ├── main/
        │   └── scala/kafka/
        │       ├── network/
        │       │   ├── HttpAcceptor.scala
        │       │   ├── HttpChannelInitializer.scala
        │       │   └── HttpRequestHandler.scala
        │       └── server/
        │           └── http/
        │               ├── ProduceForwardThread.java
        │               ├── ProduceForwardManager.java
        │               ├── FetchForwardThread.java
        │               ├── FetchForwardManager.java
        │               ├── HttpRouter.scala
        │               ├── HttpRequestTranslator.scala
        │               └── HttpResponseSerializer.scala
        └── test/
            └── scala/kafka/
                ├── network/
                │   └── HttpAcceptorTest.scala
                └── server/http/
                    ├── ProduceForwardThreadTest.scala
                    ├── HttpProduceIntegrationTest.scala
                    └── HttpConsumeIntegrationTest.scala
```

### 9.1 `http-server/build.gradle` dependencies

```groovy
dependencies {
  implementation project(':core')
  implementation project(':clients')
  implementation project(':server-common')
  implementation "io.netty:netty-all:${versions.netty}"
  implementation "com.fasterxml.jackson.core:jackson-databind:${versions.jackson}"
  implementation "com.fasterxml.jackson.module:jackson-module-scala_${versions.scala213}:${versions.jackson}"

  testImplementation project(':core').sourceSets.test.output
  testImplementation "org.apache.kafka:kafka-clients:${version}:test"
}
```

---

## 10. Configuration

New properties added to `KafkaConfig`:

| Property | Default | Description |
|---|---|---|
| `http.enabled` | `false` | Master switch (can also just add `HTTP://...` to `listeners`) |
| `num.http.network.threads` | `4` | Netty worker thread count for HTTP listener |
| `http.request.max.bytes` | `10485760` (10 MB) | Max aggregated HTTP request body size. Capped low to prevent Netty worker HOL blocking (§14.5); raise to 64 MB only if large batches are needed. |
| `http.produce.max.records` | `10000` | Max records per produce request; excess rejected with 413 before deserialization |
| `http.response.timeout.ms` | `30000` | Max time before broker returns a 504 to HTTP client |
| `http.consume.max.wait.ms` | `5000` | Server-side cap on `maxWaitMs` in consume requests. Any client value above this is silently clamped. Keeps HTTP connections short enough to survive load-balancer idle timeouts. Echoed back in `X-Kafka-MaxWait-Applied` response header. |
| `http.internal.forwarding.timeout.ms` | `10000` | Timeout for broker-to-broker forwarding calls |
| `http.internal.forwarding.retries` | `1` | Max retries per forwarded group on retriable errors (leader change, connection failure) |
| `http.cors.allowed.origins` | `""` (disabled) | Comma-separated CORS allowed origins (`*` for all) |

Listener registration (no new config needed beyond standard listener machinery):

```properties
listeners=PLAINTEXT://0.0.0.0:9092,HTTP://0.0.0.0:9094
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,HTTP:HTTP
advertised.listeners=PLAINTEXT://broker1.example.com:9092,HTTP://broker1.example.com:9094
```

---

## 11. Error Handling

### 11.1 HTTP status code mapping

| Kafka Error | HTTP Status | Notes |
|---|---|---|
| `NONE` | 200 OK | |
| `UNKNOWN_TOPIC_OR_PARTITION` | 404 Not Found | |
| `LEADER_NOT_AVAILABLE` | 503 Service Unavailable | `Retry-After: 1` |
| `NOT_LEADER_OR_FOLLOWER` | 503 Service Unavailable | forwarding failed after retry |
| `MESSAGE_TOO_LARGE` | 413 Payload Too Large | |
| `RECORD_LIST_TOO_LARGE` | 413 Payload Too Large | |
| `TOPIC_AUTHORIZATION_FAILED` | 403 Forbidden | |
| `CLUSTER_AUTHORIZATION_FAILED` | 403 Forbidden | |
| `INVALID_REQUEST` | 400 Bad Request | JSON parse error or bad field value |
| `INVALID_TOPIC_EXCEPTION` | 400 Bad Request | |
| `NOT_ENOUGH_REPLICAS` | 503 Service Unavailable | ISR below minimum |
| `NOT_ENOUGH_REPLICAS_AFTER_APPEND` | 503 Service Unavailable | |
| `REQUEST_TIMED_OUT` | 504 Gateway Timeout | |
| `KAFKA_STORAGE_ERROR` | 500 Internal Server Error | |
| Everything else | 500 Internal Server Error | |

### 11.2 Partial failure on multi-partition requests

When a produce or consume request spans multiple partitions and some succeed while others fail,
the HTTP status is **207 Multi-Status**. Each partition entry in the response body has its own
`errorCode` and `errorMessage`.

Example 207 produce response:

```json
{
  "offsets": [
    { "partition": 0, "offset": 1042, "errorCode": 0,  "errorMessage": null },
    { "partition": 1, "offset": null, "errorCode": 3,  "errorMessage": "UNKNOWN_TOPIC_OR_PARTITION" }
  ]
}
```

---

## 12. Security

### 12.1 Authentication via `KafkaPrincipalBuilder`

Kafka already provides a pluggable `KafkaPrincipalBuilder` interface
(`clients/src/main/java/org/apache/kafka/common/security/auth/KafkaPrincipalBuilder.java`):

```java
public interface KafkaPrincipalBuilder {
    KafkaPrincipal build(AuthenticationContext context);
}
```

Rather than parsing HTTP headers ad-hoc in `HttpRequestHandler`, we introduce
`HttpAuthenticationContext implements AuthenticationContext` and inject a configured
`KafkaPrincipalBuilder` into the handler. Operators supply their own implementation or use
the built-in ones:

| Auth method | `HttpAuthenticationContext` carries | Built-in builder behaviour |
|---|---|---|
| **mTLS** (`HTTPS` listener) | `X509Certificate[]` from Netty's `SslHandler` | `DefaultKafkaPrincipalBuilder` extracts CN/SAN from the leaf cert |
| **Bearer token** | `Authorization: Bearer <token>` header value | Custom builder validates token (OAuth introspection or JWT verification) → returns principal |
| **Basic Auth** | `Authorization: Basic <b64>` decoded credentials | Custom builder validates against `PlainLoginModule` → returns principal |
| **No auth** (`HTTP` listener) | Remote IP address | `DefaultKafkaPrincipalBuilder` returns `ANONYMOUS` |

The resulting `KafkaPrincipal` is set on `RequestContext` and flows unchanged through the
existing `Authorizer` path in `KafkaApis` — zero changes to authorization logic.

### 12.2 Authorization

HTTP produce/consume requests go through the same `AclAuthorizer` checks as binary protocol
requests. The resource type is `TOPIC` with `WRITE` (produce) or `READ` (consume) operations.
No changes to the authorization layer are needed.

### 12.3 Quota enforcement

The existing `ClientQuotaManager` is keyed on `(clientId, user)`. HTTP requests set `clientId`
from the `X-Kafka-Client-ID` header (or `"http-client"` as default). Throttling responses:

```json
{ "errorCode": 89, "errorMessage": "REQUEST_TIMED_OUT", "throttleTimeMs": 500 }
```

HTTP status 429 with `Retry-After: 1` header.

---

## 13. Implementation Plan

### Phase 1 — HTTP listener + core data plane

1. Add `HTTP` / `HTTPS` to `SecurityProtocol` enum
2. Create `http-server` Gradle submodule with Netty dependency
3. Implement `HttpAcceptor`, `HttpChannelInitializer`, `HttpRequestHandler`
4. Wire `HttpAcceptor` into `SocketServer.createDataPlaneAcceptorAndProcessors()`
5. Implement `HttpRouter` (URI → handler routing)
6. Implement `HttpRequestTranslator` (JSON body → `ProduceRequest` / `FetchRequest`)
7. Implement `HttpResponseSerializer` (Kafka response → JSON)
8. Add new config properties to `KafkaConfig`
9. `GET /v1/health` — health check (zero dependency, needed by load balancers from day 1)
10. Integration test: single-partition produce/consume on a local broker (leader path only)

### Phase 2 — Forwarding + observability operations

11. Implement `ProduceForwardThread` / `FetchForwardThread` (subclass `InterBrokerSendThread`)
12. Implement `ProduceForwardManager` / `FetchForwardManager`
13. Add forwarding logic to `handleHttpProduceRequest` / `handleHttpConsumeRequest`
14. Fan-out + aggregation for multi-partition consume with `effectiveMaxWaitMs` cap
15. `GET /v1/topics` — list topics (from `MetadataCache`)
16. `GET /v1/topics/{topic}` — topic metadata with partition/leader/ISR info
17. `GET /v1/topics/{topic}/partitions/{partition}/offsets` — list offsets
18. `GET /v1/consumer-groups/{group}/lags` — committed offset + log-end offset + lag
19. Integration tests: produce/consume/metadata hitting a follower broker

### Phase 3 — Robustness, security & polish

20. Full produce edge case handling: §5.5 error tables, forwarding retry on leader change
21. Metrics: `http.produce.request.rate`, `http.consume.request.rate`, `http.forward.request.rate`, `http.forward.error.rate`, `http.metadata.request.rate`
22. CORS support (`http.cors.allowed.origins`)
23. mTLS / Bearer token / Basic Auth wiring into `KafkaPrincipal`
24. HTTP/2 upgrade via Netty `ApplicationProtocolNegotiationHandler` (ALPN)
25. Performance benchmarking (produce/consume throughput and latency under load)

## 14. Implementation Concerns

Issues that must be resolved during implementation, with concrete solutions derived from
existing Kafka code patterns.

---

### 14.1 CRITICAL — Async completion: use `.handleAsync` with a dedicated executor

**Problem.** `KafkaRequestHandler.run()` is a tight blocking loop. If `handleHttpProduceRequest`
or `handleHttpConsumeRequest` ever calls `.get()`, `.join()`, or any blocking wait on a
`CompletableFuture`, the handler thread is held for the entire forwarding RTT, starving the
shared `KafkaRequestHandler` pool for all protocols.

There is a second, subtler risk: `CompletableFuture.handle()` (without an executor) runs the
callback on whichever thread completed the last future. For local appends, `ReplicaManager`
may call `responseCallback` **synchronously on the handler thread** (acks=1 fast path). In
that case `.handle()` would also run on the handler thread — same problem.

**How existing Kafka async handlers solve this.**
Handlers like `handleOffsetFetchRequest` (`KafkaApis.scala` line 1019) return a
`CompletableFuture[Unit]` immediately and chain a `.handle[Unit]` callback. The handler
thread returns before any future completes. Inside the callback, `requestHelper.sendMaybeThrottle()`
→ `requestChannel.sendResponse()` → `processor.enqueueResponse()` enqueues into a
`LinkedBlockingDeque` (unbounded, non-blocking), then calls `wakeup()`. The callback thread
is never blocked.

**Required pattern for HTTP handlers.**

```scala
// In KafkaApis.handleHttpProduceRequest():

// 1. Wrap the local-append callback in a CompletableFuture so it is always async
val localFuture = new CompletableFuture[Map[TopicIdPartition, PartitionResponse]]()
replicaManager.appendRecords(
  ...,
  responseCallback = results => localFuture.complete(results)  // may be called synchronously
)

// 2. Forward to remote leaders (already returns CompletableFuture)
val remoteFutures: Seq[CompletableFuture[...]] = remoteBuckets.map { (leaderId, entries) =>
  produceForwardManager.forward(leaderId, entries, ...)
}

// 3. Join all futures; callback ALWAYS runs on httpAsyncExecutor — never on handler thread
// or InterBrokerSendThread poll loop
CompletableFuture.allOf((localFuture +: remoteFutures).toArray: _*)
  .orTimeout(effectiveTimeoutMs, MILLISECONDS)
  .handleAsync({ (_, ex) =>
    val response = mergeResults(localFuture, remoteFutures, ex)
    requestHelper.sendMaybeThrottle(request, response)
    ()
  }: java.util.function.BiFunction[Void, Throwable, Unit],
  httpAsyncExecutor)  // ← always specify; never use plain .handle() here

// 4. Handler thread returns immediately — KafkaApis.handle() loops back to dequeue
```

**`httpAsyncExecutor`** is a `ScheduledExecutorService` with a fixed pool (new config:
`num.http.async.threads`, default 4), created in `HttpAcceptor.startup()` and shut down in
`HttpAcceptor.close()`. It is entirely separate from `KafkaRequestHandler` threads and
Netty worker threads.

**Why `requestChannel.sendResponse()` is safe from `httpAsyncExecutor`.**
`RequestChannel.sendResponse()` looks up the processor by ID, then calls
`processor.enqueueResponse()` which does `responseQueue.put(response)` on a
`LinkedBlockingDeque` (unbounded → non-blocking) followed by `wakeup()`. No lock contention,
no blocking.

---

### 14.2 CRITICAL — `pendingRequests` map: key by `connectionId`, add channel-close cleanup

**Problem.** The original design keyed `pendingRequests` by `correlationId` (Int). This
creates two bugs: (a) potential collision between HTTP and binary-protocol processors since
they generate correlationIds independently; (b) client disconnection leaks map entries.

**What binary protocol actually does.**
After researching `SocketServer.scala`, the existing `Processor` class does **not** map
by `correlationId` at all. It uses `connectionId` (Netty channel ID, format
`localAddr:localPort-remoteAddr:remotePort-index`) as the key in `inflightResponses`.
`correlationId` is embedded in the response frame itself and matched client-side. Since
HTTP/1.1 connections are sequential (one outstanding request per connection by default),
`connectionId` is sufficient to route a response back to the right channel.

**Fix: key by `connectionId`, one entry per active HTTP/1.1 connection.**

```scala
// In HttpRequestHandler (one instance per Netty ChannelPipeline, i.e. per connection):
private var pendingCtx: ChannelHandlerContext = _  // at most one in-flight per HTTP/1.1 conn

override def channelRead0(ctx: ChannelHandlerContext, req: FullHttpRequest): Unit = {
  pendingCtx = ctx

  // Register close listener — cleans up if client disconnects mid-request
  ctx.channel().closeFuture().addListener { (_: ChannelFuture) =>
    pendingCtx = null
  }

  val kafkaRequest = buildKafkaRequest(ctx, req)
  requestChannel.sendRequest(kafkaRequest)           // or tryEnqueue (§14.3)
}

// Called from response poller (httpAsyncExecutor) when RequestChannel delivers response:
def sendHttpResponse(response: AbstractResponse, correlationId: Int): Unit = {
  val ctx = pendingCtx
  if (ctx != null && ctx.channel().isActive) {
    val json = HttpResponseSerializer.serialize(response)
    ctx.writeAndFlush(buildFullHttpResponse(json))
  }
  pendingCtx = null
}
```

`connectionId` in `RequestContext` is set to `ctx.channel().id().asLongText()`. The response
poller matches responses back to `HttpRequestHandler` instances via the same channel ID.

For future HTTP/2 or HTTP/1.1 pipelining support, promote `pendingCtx` to a
`ConcurrentHashMap[Int, ChannelHandlerContext]` keyed by `correlationId` (generated by an
`AtomicInteger` counter local to the handler, restarting at 0 each new connection).

---

### 14.3 HIGH — RequestChannel backpressure: `offer()` not `put()`

**Problem.** `RequestChannel.sendRequest()` calls `requestQueue.put(request)` — a **blocking
put** on an `ArrayBlockingQueue(queuedMaxRequests)` (default 500). When the queue is full,
the Netty worker thread calling `sendRequest()` blocks inside Kafka, unable to read more
frames or write responses on any connection it multiplexes.

**How binary protocol handles it.** The binary `Processor` calls `requestChannel.sendRequest(req)`
then immediately `selector.mute(connectionId)` — so it gives up reading that connection until
the handler dequeues the request. The blocking `put()` is brief because the selector loop
pauses that connection before overflowing.

Netty has no equivalent mute mechanism per-connection at the Kafka level, so the `put()` can
stall the entire worker thread.

**Fix: add `tryEnqueue()` to `RequestChannel` using `offer()` (one-line change).**

```scala
// RequestChannel.scala — new method alongside sendRequest():
def tryEnqueue(request: RequestChannel.Request): Boolean =
  requestQueue.offer(request)  // non-blocking; returns false if full
```

In `HttpRequestHandler`:
```scala
if (!requestChannel.tryEnqueue(kafkaRequest)) {
  val body = """{"errorCode":"QUEUE_FULL","errorMessage":"Broker request queue saturated, retry later"}"""
  val resp = new DefaultFullHttpResponse(HTTP_1_1, SERVICE_UNAVAILABLE,
    Unpooled.copiedBuffer(body, UTF_8))
  resp.headers().set(CONTENT_TYPE, "application/json")
  resp.headers().set(RETRY_AFTER, "1")
  ctx.writeAndFlush(resp)
}
```

**Monitoring.** Expose `http.queue.full.rate` metric to alert when this rejection path is hit.
A sustained non-zero rate means `num.io.threads` or `queued.max.requests` needs tuning.

---

### 14.4 HIGH — Input validation

All validation runs in `HttpRequestTranslator.translate()` before any Kafka code is called.
Return HTTP 400/422 immediately — do not enqueue to `RequestChannel`.

**Topic name (URL path segment)**

```java
// HttpRouter.java — called before any handler
static String validateTopicName(String rawSegment) {
    // 1. URL-decode (catches %2F = '/', %00 = null byte, etc.)
    String topic = URLDecoder.decode(rawSegment, StandardCharsets.UTF_8);

    // 2. Fast-reject path traversal and separators before calling Kafka
    if (topic.contains("\0") || topic.contains("/") || topic.contains("\\"))
        throw new InvalidRequestException("Topic name contains illegal characters");

    // 3. Kafka's own validator — checks [a-zA-Z0-9._-], max 249, rejects "." and ".."
    //    (Topic.java: containsValidPattern() + detectInvalidTopic())
    Topic.validate(topic);   // throws InvalidTopicException on failure
    return topic;
}
```

**`X-Kafka-Client-ID` header**

Kafka's own `clientId` config has **no character validation** (defined as a plain `STRING` in
`CommonClientConfigs.java`). The HTTP layer must add it:

```java
static String validateClientId(String clientId) {
    if (clientId == null || clientId.isEmpty()) return "http-client";
    // Same whitelist as topic names; metric labels are built from clientId
    if (!clientId.matches("^[a-zA-Z0-9._-]{1,128}$"))
        throw new InvalidRequestException("X-Kafka-Client-ID contains illegal characters");
    return clientId;
}
```

**`maxWaitMs`** — must be `>= 0` before `min()` is applied:
```java
if (maxWaitMs < 0)
    throw new InvalidRequestException("maxWaitMs must be non-negative, got " + maxWaitMs);
int effective = Math.min(maxWaitMs, config.httpConsumeMaxWaitMs());
```

**JSON deserialization depth (Jackson 2.21.2 — `StreamReadConstraints` available)**

Kafka's existing Jackson usage pattern (`JsonDeserializer.java` in `connect/json`) uses
`objectMapper.enable(feature)`. Follow the same style but add `StreamReadConstraints`:

```java
// HttpRequestTranslator.java — shared ObjectMapper
private static final ObjectMapper MAPPER = new ObjectMapper()
    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    .setStreamReadConstraints(StreamReadConstraints.builder()
        .maxNestingDepth(20)          // prevents stack overflow on deeply nested JSON
        .maxStringLength(1_048_576)   // 1 MB max per string field
        .maxNumberLength(100)         // prevents huge numeric literals
        .build());
```

---

### 14.5 HIGH — `HttpObjectAggregator` size and Netty HOL blocking

When `HttpObjectAggregator` receives a request body that exceeds its `maxContentLength`,
Netty automatically returns HTTP 413 and closes the connection — no handler code needed.
The risk is choosing too high a limit: one slow upload of 64 MB ties up a Netty worker
thread, starving all connections it multiplexes.

**Default reduced to 10 MB.** This covers 10,000 × 1 KB records comfortably. Operators
with large record sizes raise `http.request.max.bytes`; the corresponding `HttpObjectAggregator`
is configured to the same value in `HttpChannelInitializer`:

```scala
pipeline.addLast("http-aggregator",
  new HttpObjectAggregator(config.httpRequestMaxBytes))  // default 10 MB
```

The §10 config table default has been updated accordingly.

---

### 14.6 MEDIUM — Bound `ProduceForwardThread` queue and add per-broker metric

`TransactionMarkerChannelManager` (the existing precedent) also uses an unbounded
`LinkedBlockingQueue`. For transaction markers this is acceptable because marker
throughput is bounded by transaction rate. HTTP produce has no such bound.

**Fix: bounded queue with immediate rejection.**

```java
// config-driven capacity; default 10_000 entries per broker
private final int capacity;
private final BlockingQueue<PendingProduce> pendingQueue;

public ProduceForwardThread(Node destination, NetworkClient client,
                            int requestTimeoutMs, int queueCapacity, Time time) {
    super("ProduceForwardThread-" + destination.id(), client, requestTimeoutMs, time);
    this.capacity = queueCapacity;
    this.pendingQueue = new LinkedBlockingQueue<>(queueCapacity);
}

public CompletableFuture<...> enqueue(Map<TopicIdPartition, MemoryRecords> entries,
                                       short requiredAcks, int timeoutMs) {
    CompletableFuture<...> future = new CompletableFuture<>();
    PendingProduce pending = new PendingProduce(entries, requiredAcks, timeoutMs, future);
    if (!pendingQueue.offer(pending)) {
        future.completeExceptionally(
            new NotEnoughReplicasException("Forward queue full for broker " + destination().id()));
    }
    wakeup();
    return future;
}
```

New config: `http.internal.forwarding.queue.size` (default 10 000). Add a gauge metric
`http.forward.queue.size` tagged by `broker.id` so operators can see per-leader pressure.
Use `KafkaMetricsGroup.newGauge(name, () -> pendingQueue.size(), Map("broker.id" -> id.toString))`.

---

### 14.7 MEDIUM — `HttpResponseSerializer` value type detection algorithm

The serializer determines `DataObject.type` when reading consumed `MemoryRecords` bytes.
Algorithm must be deterministic and documented so client authors know what to expect:

```
Input: byte[] valueBytes

1. if valueBytes == null:
       → { "type": "NULL" }

2. if record.headers contains "_content-type: application/json":
       parse valueBytes as UTF-8 JSON
       → { "type": "JSON", "data": <parsed JsonNode> }
       on parse failure: fall through to step 3

3. try decode valueBytes as UTF-8:
       if decode fails → go to step 4
       if decoded string contains any code point in [0x00–0x08, 0x0B, 0x0C, 0x0E–0x1F, 0x7F]
           (i.e. C0/C1 control chars except HT=0x09, LF=0x0A, CR=0x0D)
           → go to step 4
       → { "type": "STRING", "data": <decoded string> }

4. → { "type": "BINARY", "data": <Base64.getEncoder().encodeToString(valueBytes)> }
```

**Recommendation for producers:** always set a `_content-type` record header when producing
to eliminate step-3 ambiguity on the consume side. The HTTP produce handler SHOULD propagate
the `Content-Type` field from the produce request body's `DataObject` into this header
automatically, so round-trip type fidelity is preserved without client coordination.

---

### 14.8 MEDIUM — Graceful shutdown: integrate into `SocketServer.stopProcessingRequests()`

**What existing shutdown does.** `SocketServer.stopProcessingRequests()` calls:
1. `acceptor.beginShutdown()` — stops accepting new TCP connections
2. `acceptor.close()` — closes the listener socket
3. `requestChannel.clear()` — **immediately drops all queued requests** (no drain)

In-flight requests already dispatched to `KafkaRequestHandler` threads are abandoned;
their responses are discarded when `Processor.closeAll()` closes all sockets.

**HTTP needs a short drain window** because HTTP clients hold the TCP connection open
waiting for the response — unlike binary clients that can detect disconnect and retry.

**Integration:**

```scala
// SocketServer.scala — add to stopProcessingRequests() after existing acceptor shutdown:
def stopProcessingRequests(): Unit = synchronized {
  if (!stopped) {
    stopped = true

    // 1. Existing: shut down binary acceptors
    dataPlaneAcceptors.asScala.values.foreach(_.beginShutdown())
    dataPlaneAcceptors.asScala.values.foreach(_.close())

    // 2. NEW: drain HTTP in-flight requests (bounded window)
    httpAcceptors.asScala.values.foreach(_.beginDrain())
    val drainDeadline = time.milliseconds() + config.httpShutdownDrainMs  // default 2000ms
    httpAcceptors.asScala.values.foreach { acc =>
      val remaining = drainDeadline - time.milliseconds()
      if (remaining > 0) acc.awaitDrain(remaining)
    }
    httpAcceptors.asScala.values.foreach(_.close())

    // 3. Existing: clear request queue
    dataPlaneRequestChannel.clear()
  }
}
```

```scala
// HttpAcceptor — drain support:
def beginDrain(): Unit = accepting.set(false)   // stop reading from Netty (no new sendRequest calls)

def awaitDrain(timeoutMs: Long): Unit = {
  val deadline = System.currentTimeMillis() + timeoutMs
  while (pendingConnectionCount.get() > 0 && System.currentTimeMillis() < deadline)
    Thread.sleep(20)
}

override def close(): Unit = {
  channel.close().sync()
  produceForwardManager.close()    // awaits ProduceForwardThread shutdown
  fetchForwardManager.close()
  httpAsyncExecutor.shutdown()
  httpAsyncExecutor.awaitTermination(2, SECONDS)
  workerGroup.shutdownGracefully(100, 500, MILLISECONDS).sync()
  bossGroup.shutdownGracefully(100, 200, MILLISECONDS).sync()
}
```

New config: `http.shutdown.drain.ms` (default 2000). During the drain window, in-progress
requests that have already been dispatched to handler threads can still complete and send
responses. New incoming HTTP requests are rejected with 503 immediately after
`beginDrain()` is called.

---

### 14.9 LOW — Metric tagging: distinguish HTTP from binary protocol traffic

HTTP requests flowing through `KafkaRequestHandler` are counted by existing per-ApiKey
metrics (`produce-request-rate`, `fetch-request-rate`), conflating HTTP and binary traffic.

Use `KafkaMetricsGroup` with a `protocol` tag — this tag already exists in Kafka's own
SSL cipher metrics (`Selector.java`):

```scala
// In HttpAcceptor or a dedicated HttpMetrics class:
private val metricsGroup = new KafkaMetricsGroup(getClass)

val httpProduceRate = metricsGroup.newMeter(
  "RequestsPerSec",
  "requests",
  TimeUnit.SECONDS,
  Map("protocol" -> "http", "request" -> "produce").asJava
)

val httpConsumeRate = metricsGroup.newMeter(
  "RequestsPerSec",
  "requests",
  TimeUnit.SECONDS,
  Map("protocol" -> "http", "request" -> "consume").asJava
)
```

These are **additive** — they do not replace the existing per-ApiKey metrics. Monitoring
dashboards can filter by `protocol=http` for HTTP-only views or omit the tag for the
aggregate (binary + HTTP).

---

*Document version: 0.5 — 2026-04-16*
*Branch: feature/http-protocol*
