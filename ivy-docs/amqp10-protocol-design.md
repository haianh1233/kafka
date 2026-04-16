# AMQP 1.0 Protocol for Apache Kafka — Design Document

## Table of Contents

1. [Overview](#1-overview)
2. [Goals & Non-Goals](#2-goals--non-goals)
3. [Architecture Overview](#3-architecture-overview)
4. [AMQP 1.0 Protocol Specification](#4-amqp-10-protocol-specification)
   - 4.1 Connection Lifecycle
   - 4.2 Session Management
   - 4.3 Sender Links (Produce)
   - 4.4 Receiver Links (Consume)
   - 4.5 Flow Control
   - 4.6 Settlement & Disposition
   - 4.7 Transactions
5. [Produce Path](#5-produce-path)
6. [Consume Path](#6-consume-path)
7. [Broker-to-Broker Internal Forwarding](#7-broker-to-broker-internal-forwarding)
8. [Integration with Existing Kafka Infrastructure](#8-integration-with-existing-kafka-infrastructure)
9. [New Module: `amqp-server`](#9-new-module-amqp-server)
10. [Configuration](#10-configuration)
11. [Error Handling](#11-error-handling)
12. [Security](#12-security)
13. [Address Format & Topic Mapping](#13-address-format--topic-mapping)
14. [Message Conversion](#14-message-conversion)
15. [Implementation Plan](#15-implementation-plan)
16. [Implementation Concerns](#16-implementation-concerns)
17. [Comparison with Existing AMQP-Kafka Bridges](#17-comparison-with-existing-amqp-kafka-bridges)

---

## 1. Overview

This document describes the design for adding native AMQP 1.0 protocol support to Apache Kafka brokers. The feature allows AMQP 1.0 clients (Apache Qpid, RabbitMQ clients, Azure Event Hubs SDKs) to **produce** and **consume** Kafka records using the AMQP 1.0 wire protocol without the Kafka binary protocol, a separate gateway, or a protocol translation proxy.

### Key Properties

| Property | Behavior |
|---|---|
| **Produce** | Client opens a Sender Link to an address (topic:partition) → sends Transfer frames → broker appends to partition leader → returns Disposition(Accepted) |
| **Consume** | Client opens a Receiver Link to an address → broker pushes Transfer frames as records arrive → client settles via Disposition |
| **Protocol** | AMQP 1.0 (OASIS Standard, ISO 19464:2014) over TCP, with optional SASL and TLS |
| **Integration** | Plugs into the existing `SocketServer → RequestChannel → KafkaApis` pipeline via Netty handlers |
| **Forwarding** | Broker-to-broker forwarding uses the existing Kafka binary protocol (PRODUCE / FETCH API) — AMQP is a client-facing protocol only |
| **Flow Control** | AMQP 1.0 link-level credit with session-level windowing; maps to Kafka fetch quota semantics |
| **Transactions** | AMQP 1.0 transaction coordinator links map to Kafka transactional produce (idempotent + two-phase commit) |

### Motivation

- Enable enterprise messaging clients (JMS via Qpid, .NET via AMQP.Net Lite, Azure SDKs) to produce/consume Kafka topics natively
- Provide credit-based flow control and per-message acknowledgement — features that Kafka's binary protocol lacks at the wire level
- Support AMQP 1.0 transactions mapped to Kafka's exactly-once semantics
- Leverage existing broker auth (mTLS, SASL PLAIN/SCRAM) for AMQP connections from day one

---

## 2. Goals & Non-Goals

### Goals

- AMQP 1.0 produce: client sends Transfer frames on Sender Links; broker forwards to correct partition leader transparently
- AMQP 1.0 consume: client attaches Receiver Links; broker pushes records as Transfer frames respecting link credit
- SASL PLAIN authentication using Kafka's existing credential stores (JAAS, SCRAM)
- Credit-based flow control with drain mode support
- Per-message settlement: Accepted (ack), Rejected (DLQ), Released (redeliver), Modified (redeliver with annotations)
- AMQP 1.0 transactions: Declare/Discharge mapped to Kafka transactional produce
- Multi-frame transfer support for messages exceeding max frame size
- Configurable via standard `listeners` / `listener.security.protocol.map` mechanism

### Non-Goals

- AMQP 0-9-1 support (different wire protocol; separate design if needed)
- AMQP management operations (topic creation, ACL management) — admin operations belong on the Kafka binary protocol / `AdminClient`
- AMQP dynamic link creation (server-named sources/targets) — phase 2
- Durable terminus persistence (PostgreSQL-backed unsettled delivery tracking) — phase 3
- AMQP 1.0 connection redirect for cluster rebalancing — phase 3
- Replacing the Kafka binary protocol
- JMS-level semantics (selectors, message groups, priority queues) — out of scope; these are JMS abstractions above the wire protocol

---

## 3. Architecture Overview

```
┌──────────────────────────────────────────────────────────────────────────┐
│                           Kafka Broker                                   │
│                                                                          │
│  Port 5672 (AMQP)              Port 9092 (PLAINTEXT — unchanged)         │
│  ┌───────────────────────┐     ┌──────────────────────────────────────┐  │
│  │    Amqp10Acceptor     │     │         DataPlaneAcceptor            │  │
│  │  (Netty ServerBootstrap)    │         (existing NIO Acceptor)      │  │
│  │                       │     │                                      │  │
│  │  ┌─────────────────┐  │     └──────────────────┬───────────────────┘  │
│  │  │Amqp10FrameDecoder│ │                        │                      │
│  │  │ConnectionHandler│  │                        │                      │
│  │  │ SessionHandler  │  │                        │                      │
│  │  │  LinkHandler    │  │                        │                      │
│  │  │TransferHandler  │  │                        │                      │
│  │  │  FlowHandler    │  │                        │                      │
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
│                   │  if (securityProtocol == AMQP/AMQPS):            │   │
│                   │    handleAmqpProduceRequest()                    │   │
│                   │    handleAmqpConsumeRequest()                    │   │
│                   │  else:                                           │   │
│                   │    handleProduceRequest()   (existing, unchanged)│   │
│                   │    handleFetchRequest()     (existing, unchanged)│   │
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
                  Disposition(Accepted)            local log    binary PRODUCE/FETCH
                  → AMQP client                    + ISR        → remote brokers
```

AMQP 1.0 requests and binary-protocol requests share the **same `RequestChannel` queue and the
same `KafkaRequestHandler` thread pool**. This is intentional: it reuses all existing quota,
metrics, and throttling hooks, and keeps the footprint minimal.

**Dispatch model.** No new `ApiKeys` are introduced. AMQP Transfer frames on Sender Links map to
`PRODUCE` (ApiKey 0). Receiver Link credit grants trigger `FETCH` (ApiKey 1). Transaction
Declare/Discharge maps to `INIT_PRODUCER_ID` (ApiKey 22) + `ADD_PARTITIONS_TO_TXN` (ApiKey 24) +
`END_TXN` (ApiKey 26). `KafkaApis.handle()` dispatches to **dedicated AMQP handler methods**
when `request.context.securityProtocol == AMQP || AMQPS`.

```scala
// KafkaApis.handle() — dispatch addition (no changes to existing cases):
case ApiKeys.PRODUCE =>
  if (request.context.securityProtocol.isAmqp)
    handleAmqpProduceRequest(request)   // single-message or txn batch (§5)
  else
    handleProduceRequest(request)       // existing binary handler (unchanged)

case ApiKeys.FETCH =>
  if (request.context.securityProtocol.isAmqp)
    handleAmqpConsumeRequest(request)   // credit-driven push (§6)
  else
    handleFetchRequest(request)         // existing binary handler (unchanged)
```

### Thread Model

```
                      ┌─────────────────────────────────────────────┐
  Port 5672           │  Netty Boss Thread (1)                      │
  AMQP connections ──►│  accepts TCP connections                    │
                      └───────────────────┬─────────────────────────┘
                                          │ distributes connections
                      ┌───────────────────▼─────────────────────────┐
                      │  Netty Worker Threads (num.amqp.network.threads, default=4)  │
                      │  • SASL handshake + Open/Close lifecycle     │
                      │  • Session Begin/End                         │
                      │  • Link Attach/Detach                        │
                      │  • Frame decode + performative dispatch      │
                      │  • Transfer/Disposition frame encoding       │
                      │  • Flow control credit tracking              │
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
                      │  • routes by ApiKey (PRODUCE/FETCH/...)      │
                      │  • calls ReplicaManager (local leader path)  │
                      │  • enqueues into ProduceForwardThread /      │
                      │    FetchForwardThread (remote leader path)   │
                      └──────────┬───────────────────┬──────────────┘
              enqueue()          │                   │  enqueue()
              ┌──────────────────▼──┐           ┌───▼──────────────────┐
              │  ProduceForwardThread│           │  FetchForwardThread  │
              │  → Broker 1          │    ...    │  → Broker N          │
              │  NetworkClient       │          │  NetworkClient       │
              └──────────┬──────────┘           └───────────┬──────────┘
                         │  future.complete()                │ future.complete()
                         └──────────────────┬────────────────┘
                                            │ CompletableFuture resolved
                                            ▼
                              amqpAsyncExecutor resumes,
                              encodes Disposition → AMQP frame
                              via Netty worker thread
```

### AMQP 1.0 Concepts → Kafka Mapping

| AMQP 1.0 Concept | Kafka Equivalent | Notes |
|---|---|---|
| Connection | TCP connection to broker | One per client, multiplexes sessions |
| Session | Channel within connection | Maps to transfer ID sequences (up to 65535 sessions) |
| Sender Link | Producer | Client→broker; target address = topic[:partition] |
| Receiver Link | Consumer | Broker→client; source address = topic[:partition] |
| Transfer | ProduceRequest record / FetchResponse record | Carries AMQP message payload |
| Disposition(Accepted) | Offset commit (consumer) / Produce ack (producer) | Settlement confirmation |
| Disposition(Rejected) | Dead letter / skip | Message cannot be processed |
| Disposition(Released) | Requeue / redeliver | Temporary failure; try again |
| Flow (link credit) | fetch.max.records / consumer prefetch | Controls delivery rate |
| Transaction Coordinator | Kafka transactional producer | Declare=InitProducerId, Discharge=EndTxn |
| Address | topic:partition | Colon-separated; partition optional (default 0) |

---

## 4. AMQP 1.0 Protocol Specification

### 4.1 Connection Lifecycle

AMQP 1.0 connections follow a strict sequence:

```
Client                                    Broker
  │                                          │
  │  SASL Header (AMQP\x03\x01\x00\x00)     │  ← SASL protocol ID = 0x03
  │─────────────────────────────────────────►│
  │                                          │
  │  SaslMechanisms [PLAIN, SCRAM-SHA-256]   │
  │◄─────────────────────────────────────────│
  │                                          │
  │  SaslInit (mechanism=PLAIN,              │
  │    initial-response=\0user\0pass)        │
  │─────────────────────────────────────────►│
  │                                          │
  │  SaslOutcome (code=0, OK)               │
  │◄─────────────────────────────────────────│
  │                                          │
  │  AMQP Header (AMQP\x00\x01\x00\x00)     │  ← AMQP protocol ID = 0x00
  │─────────────────────────────────────────►│
  │                                          │
  │  AMQP Header (echo)                     │
  │◄─────────────────────────────────────────│
  │                                          │
  │  Open (container-id="client-1",          │
  │    hostname="broker1.example.com",       │
  │    max-frame-size=262144)                │
  │─────────────────────────────────────────►│
  │                                          │
  │  Open (container-id="kafka-amqp-<id>",   │
  │    max-frame-size=262144)                │
  │◄─────────────────────────────────────────│
  │                                          │
  │  ══════ Connection OPEN ══════           │
  │                                          │
  │  Close (error=null)                      │
  │─────────────────────────────────────────►│
  │                                          │
  │  Close (error=null)                      │
  │◄─────────────────────────────────────────│
```

**Protocol detection.** The AMQP 1.0 protocol header is 8 bytes: `AMQP` + protocol-id (1 byte) +
major (1 byte) + minor (1 byte) + revision (1 byte). Two variants:

| Header bytes | Meaning |
|---|---|
| `AMQP\x00\x01\x00\x00` | AMQP 1.0 bare connection (no SASL) |
| `AMQP\x03\x01\x00\x00` | AMQP 1.0 with SASL layer |

The `Amqp10FrameDecoder` detects the 4-byte `AMQP` prefix and extracts the 8-byte header.
This is distinct from AMQP 0-9-1 which uses `AMQP\x00\x00\x09\x01`.

**Connection parameters negotiated in Open:**

| Field | Client sends | Broker responds | Default |
|---|---|---|---|
| `container-id` | Client identifier (required) | `kafka-amqp-<channel-id>` | — |
| `hostname` | Virtual host / SNI | null (not used) | — |
| `max-frame-size` | Client's max frame size | `262144` (256 KB) | 262144 |
| `channel-max` | Max session channels | `65535` | 65535 |
| `idle-time-out` | Heartbeat interval (ms) | `60000` (60s) | 60000 |

**Heartbeats.** AMQP 1.0 uses empty frames (8 bytes: size=8, doff=2, type=0, channel=0) as
heartbeats. The `idle-time-out` field in Open advertises the maximum allowed idle period.
Either peer must send a frame within half this interval to prevent timeout. Netty's
`IdleStateHandler` fires on read-idle; the handler sends an empty frame as keepalive.

**Connection states:**

```
AWAITING_SASL_HEADER → AWAITING_SASL_INIT → AWAITING_AMQP_HEADER →
AWAITING_OPEN → OPEN → CLOSED
```

Not `@Sharable` — one handler instance per connection holds mutable state.

---

### 4.2 Session Management

Sessions multiplex transfers within a single connection. Each session has its own channel number
(0–65535) and independent flow control windows.

```
Client                                    Broker
  │                                          │
  │  Begin (channel=0,                       │
  │    next-outgoing-id=0,                   │
  │    incoming-window=2048,                 │
  │    outgoing-window=2048)                 │
  │─────────────────────────────────────────►│
  │                                          │
  │  Begin (channel=0,                       │
  │    remote-channel=0,                     │
  │    next-outgoing-id=0,                   │
  │    incoming-window=2048,                 │
  │    outgoing-window=2048)                 │
  │◄─────────────────────────────────────────│
  │                                          │
  │  ══════ Session OPEN on channel 0 ══════ │
  │                                          │
  │  End (error=null)                        │
  │─────────────────────────────────────────►│
  │                                          │
  │  End (error=null)                        │
  │◄─────────────────────────────────────────│
```

**Session-level flow control:**

| Field | Direction | Purpose |
|---|---|---|
| `next-outgoing-id` | Both | Sequence number of next transfer to send |
| `incoming-window` | Both | Max number of unacknowledged incoming transfers |
| `outgoing-window` | Both | Max number of unacknowledged outgoing transfers |

The session flow controller tracks:
- **Outgoing:** `nextOutgoingId` incremented per transfer sent; `outgoingWindow` decremented
- **Incoming:** `nextIncomingId` incremented per transfer received; `incomingWindow` decremented
- **Peer updates:** received via Flow frames; recalculates `outgoingWindow = peerNextIncomingId + peerIncomingWindow - nextOutgoingId`

A transfer can only be sent when `outgoingWindow > 0`. If the window is exhausted, the broker
buffers deliveries until the client sends a Flow granting more capacity.

**State per session:** `ConcurrentHashMap<Short, SessionState>` where `SessionState` is a record
containing `(remoteChannel, nextOutgoingId, incomingWindow, outgoingWindow)`.

---

### 4.3 Sender Links (Produce)

A Sender Link allows the client to send messages (produce records) to a Kafka topic.

```
Client                                    Broker
  │                                          │
  │  Attach (name="sender-1",               │
  │    handle=0,                             │
  │    role=false,          ← sender         │
  │    target={address="orders:0"})          │
  │─────────────────────────────────────────►│
  │                                          │  Parse address → topic="orders", partition=0
  │                                          │  Verify topic exists in MetadataCache
  │                                          │  Authorize: WRITE on topic "orders"
  │  Attach (name="sender-1",               │
  │    handle=0,                             │
  │    role=true,           ← receiver       │
  │    target={address="orders:0"})          │
  │◄─────────────────────────────────────────│
  │                                          │
  │  ══════ Link ATTACHED ══════             │
  │                                          │
  │  Transfer (handle=0,                     │
  │    delivery-id=0,                        │
  │    delivery-tag=<8 bytes>,               │
  │    message-format=0,                     │
  │    settled=false,                        │
  │    payload=<AMQP message sections>)      │
  │─────────────────────────────────────────►│
  │                                          │  Decode payload → Kafka record
  │                                          │  ReplicaManager.appendRecords()
  │                                          │  Wait for ISR ack
  │  Disposition (role=true,                 │
  │    first=0, last=0,                      │
  │    settled=true,                         │
  │    state=Accepted{})                     │
  │◄─────────────────────────────────────────│
  │                                          │
  │  Detach (handle=0, closed=true)          │
  │─────────────────────────────────────────►│
  │                                          │
  │  Detach (handle=0, closed=true)          │
  │◄─────────────────────────────────────────│
```

**Address format:** `topic[:partition]` — see §13 for full specification.

**Pre-settled transfers (fire-and-forget).** If the client sets `settled=true` on Transfer,
the broker appends the record with `acks=1` (leader only) and does not send a Disposition.
This maps to Kafka's `acks=1` semantics — faster but no delivery guarantee confirmation.

**Unsettled transfers (at-least-once).** If `settled=false`, the broker tracks the delivery,
appends with `acks=all` (ISR quorum), and sends `Disposition(Accepted, settled=true)` after
the ISR acknowledges. This maps to Kafka's `acks=-1` semantics.

**Partition assignment for unpartitioned addresses.** When the client attaches to `orders`
(no partition suffix), the broker assigns a partition using:

```
if record has a key:
    partition = murmur2(key) % partitionCount    // matches Java client
else:
    partition = stickyPartition(linkHandle)       // round-robin per link
```

---

### 4.4 Receiver Links (Consume)

A Receiver Link allows the client to receive messages (consume records) from a Kafka topic.

```
Client                                    Broker
  │                                          │
  │  Attach (name="receiver-1",              │
  │    handle=1,                             │
  │    role=true,           ← receiver       │
  │    source={address="orders:0"})          │
  │─────────────────────────────────────────►│
  │                                          │  Parse address → topic="orders", partition=0
  │                                          │  Verify topic exists
  │                                          │  Authorize: READ on topic "orders"
  │                                          │  Create subscription cursor
  │  Attach (name="receiver-1",              │
  │    handle=1,                             │
  │    role=false,          ← sender         │
  │    source={address="orders:0"})          │
  │◄─────────────────────────────────────────│
  │                                          │
  │  ══════ Link ATTACHED ══════             │
  │                                          │
  │  Flow (handle=1,                         │
  │    link-credit=100,                      │
  │    delivery-count=0)                     │
  │─────────────────────────────────────────►│
  │                                          │  Credit granted → start fetching
  │                                          │  replicaManager.fetchMessages()
  │                                          │
  │  Transfer (handle=1,                     │
  │    delivery-id=0,                        │
  │    delivery-tag=<8 bytes>,               │
  │    settled=false,                        │
  │    payload=<AMQP message>)               │
  │◄─────────────────────────────────────────│
  │                                          │
  │  Transfer (handle=1, delivery-id=1, ...) │
  │◄─────────────────────────────────────────│
  │                                          │
  │  ... (up to credit=100 deliveries)       │
  │                                          │
  │  Disposition (role=false,                │
  │    first=0, last=49,                     │
  │    settled=true,                         │
  │    state=Accepted{})                     │
  │─────────────────────────────────────────►│
  │                                          │  Commit offsets 0..49
  │                                          │
  │  Flow (handle=1,                         │
  │    link-credit=100,                      │
  │    delivery-count=50)                    │
  │─────────────────────────────────────────►│
  │                                          │  Replenish credit → continue fetching
```

**Credit-based flow control.** The client grants link credit via Flow frames. The broker
delivers at most `linkCredit` messages before pausing. The client replenishes credit after
processing and acknowledging a batch. This naturally implements backpressure — a slow
consumer simply stops granting credit.

**Drain mode.** The client can set `drain=true` on a Flow to request the broker to send all
available messages (up to credit) and then advance `delivery-count` to consume remaining
credit. The broker responds with `Flow(drain=true, linkCredit=0)` after draining:

```
Client                                    Broker
  │                                          │
  │  Flow (handle=1, link-credit=100,        │
  │    drain=true)                           │
  │─────────────────────────────────────────►│
  │                                          │  Send all available (say 30 messages)
  │  Transfer × 30                           │
  │◄─────────────────────────────────────────│
  │                                          │
  │  Flow (handle=1, link-credit=0,          │
  │    delivery-count=30, drain=true)        │  ← "no more to send"
  │◄─────────────────────────────────────────│
```

**Consume offset.** By default, a newly attached Receiver Link starts consuming from the
latest offset (log-end). Clients can specify a starting offset via link properties:

| Link property | Value | Kafka equivalent |
|---|---|---|
| `kafka-offset` | `<long>` | Start from specific offset |
| `kafka-offset` | `earliest` | Start from first available offset |
| `kafka-offset` | `latest` (default) | Start from log-end offset |

---

### 4.5 Flow Control

AMQP 1.0 provides two levels of flow control:

#### Session-level flow control

Session windows limit the total number of unacknowledged transfers across all links in a session.

```
Amqp10SessionFlowControl:
  ┌─ Outgoing: nextOutgoingId, outgoingWindow
  │    • canSendOutgoingTransfer() → outgoingWindow > 0
  │    • recordOutgoingTransfer()  → nextOutgoingId++, outgoingWindow--
  │
  └─ Incoming: nextIncomingId, incomingWindow
       • canAcceptIncomingTransfer() → incomingWindow > 0
       • recordIncomingTransfer()    → nextIncomingId++, incomingWindow--
       • handlePeerFlow()            → recalculate outgoingWindow from peer's advertised windows
```

Default window size: 2048 transfers in each direction.

#### Link-level credit

Link credit is the primary flow control mechanism. Per-link state:

```java
record CreditState(long handle, long linkCredit, long deliveryCount)
```

- **Sender links (produce):** The broker grants initial credit on Attach. The client can
  send `linkCredit` transfers. The broker replenishes credit after processing.
- **Receiver links (consume):** The client grants credit via Flow. The broker pushes up to
  `linkCredit` records. When credit reaches 0, the broker pauses delivery.

**Credit exhaustion handling:**

```
Client credit reaches 0 for receiver link:
  ├── Broker buffers pending deliveries (bounded by amqp.delivery.buffer.size, default 1000)
  ├── If buffer full → broker pauses fetch for this partition
  └── When client sends Flow(linkCredit > 0) → drain buffer first, then resume fetch
```

**Echo mode.** The client can set `echo=true` on a Flow to request the broker to echo its
current flow state without any implicit state change. The broker responds with a Flow
containing its current `delivery-count` and `link-credit` (without setting `echo` to prevent
infinite loops).

---

### 4.6 Settlement & Disposition

Settlement is how AMQP 1.0 achieves message acknowledgement. Four outcomes are supported:

| Outcome | AMQP Type | Kafka Mapping | Behavior |
|---|---|---|---|
| **Accepted** | `Accepted{}` | Offset commit | Record successfully processed; advance cursor |
| **Rejected** | `Rejected{error}` | Dead-letter + skip | Permanently failed; route to DLQ (if configured), advance cursor past this offset |
| **Released** | `Released{}` | Redeliver | Temporary failure; redeliver with same `delivery-count` |
| **Modified** | `Modified{delivery-failed, undeliverable-here}` | Redeliver with annotations | Temporary failure; increment `delivery-count`, optionally mark `undeliverable-here` |

**Settlement modes:**

| Transfer.settled | Mode | Kafka equivalent |
|---|---|---|
| `true` | Pre-settled (fire-and-forget) | `acks=1` for produce; auto-commit for consume |
| `false` | Unsettled (explicit ack) | `acks=all` for produce; manual commit for consume |

**Range disposition.** A single Disposition frame can settle a contiguous range of deliveries
`[first, last]`. This maps naturally to Kafka's contiguous offset commit model — committing
offset N implicitly acknowledges all offsets < N.

**Settlement manager state machine per delivery:**

```
UNSETTLED ──Transfer──► DELIVERED ──Disposition(Accepted)──► SETTLED (offset committed)
                            │
                            ├── Disposition(Rejected) ──► DEAD_LETTERED (DLQ + skip)
                            │
                            ├── Disposition(Released)  ──► REDELIVERED (same delivery-count)
                            │
                            └── Disposition(Modified)  ──► REDELIVERED (delivery-count++)
```

---

### 4.7 Transactions

AMQP 1.0 transactions use a special **coordinator link** with target address `coordinator`.
The transaction protocol is:

```
Client                                    Broker
  │                                          │
  │  Attach (name="txn-1",                  │
  │    handle=2,                             │
  │    role=false,                           │
  │    target={type=Coordinator})            │
  │─────────────────────────────────────────►│
  │                                          │
  │  Attach (name="txn-1",                  │
  │    handle=2, role=true,                  │
  │    target={type=Coordinator})            │
  │◄─────────────────────────────────────────│
  │                                          │
  │  ═══ Coordinator link ATTACHED ═══       │
  │                                          │
  │  Transfer (handle=2,                     │
  │    delivery-id=100,                      │
  │    payload=Declare{})                    │
  │─────────────────────────────────────────►│
  │                                          │  Allocate txnId (UUID)
  │                                          │  InitProducerId → Kafka
  │  Disposition (first=100,                 │
  │    state=Declared{txn-id=<16 bytes>})    │
  │◄─────────────────────────────────────────│
  │                                          │
  │  ═══ Transaction ACTIVE ═══              │
  │                                          │
  │  Transfer (handle=0,                     │  ← normal sender link
  │    delivery-id=101,                      │
  │    state=TransactionalState{             │
  │      txn-id=<16 bytes>})                 │
  │─────────────────────────────────────────►│
  │                                          │  Buffer write (not committed yet)
  │                                          │
  │  Transfer (handle=0,                     │
  │    delivery-id=102,                      │
  │    state=TransactionalState{             │
  │      txn-id=<16 bytes>})                 │
  │─────────────────────────────────────────►│
  │                                          │  Buffer write
  │                                          │
  │  Transfer (handle=2,                     │  ← coordinator link
  │    delivery-id=103,                      │
  │    payload=Discharge{                    │
  │      txn-id=<16 bytes>,                  │
  │      fail=false})        ← COMMIT        │
  │─────────────────────────────────────────►│
  │                                          │  Flush buffered writes to ReplicaManager
  │                                          │  AddPartitionsToTxn → EndTxn(COMMIT)
  │  Disposition (first=103,                 │
  │    state=Accepted{})                     │
  │◄─────────────────────────────────────────│
```

**Transaction mapping to Kafka:**

| AMQP Operation | Kafka Operation | Notes |
|---|---|---|
| Declare | `InitProducerId` (ApiKey 22) | Allocates producer ID + epoch |
| Transfer with `TransactionalState` | Buffer in-memory | Not written until Discharge |
| Discharge(fail=false) | `AddPartitionsToTxn` + `appendRecords` + `EndTxn(COMMIT)` | Atomic commit |
| Discharge(fail=true) | `EndTxn(ABORT)` | Discard buffered writes |

**Transaction producer ID allocation.** AMQP transaction producer IDs are allocated from a
separate range (`AMQP_PRODUCER_ID_BASE = 2_000_000`) to avoid collision with Kafka's own
producer IDs (which start from 0). The producer epoch is always 0 for AMQP transactions.

**Transaction timeout.** Default 60 seconds. If a transaction is not discharged within this
window, it is automatically aborted.

**Buffered writes.** Between Declare and Discharge, Transfer frames with `TransactionalState`
are accumulated in `Map<txnId, List<BufferedTxnWrite>>`. On Discharge(fail=false), all
buffered writes are flushed atomically via `ReplicaManager.appendRecords()` with the
transactional producer state. On Discharge(fail=true), the buffer is discarded.

---

## 5. Produce Path

### 5.1 End-to-end: Single-message produce (happy path)

Client sends one Transfer on a Sender Link to `orders:0`. Broker 3 receives it.
Broker 1 is leader for partition 0.

```
AMQP Client           Broker 3                                    Broker 1
     │                   │                                            │
     │  [Link already attached to "orders:0"]                         │
     │                                                                │
     │  Transfer (handle=0,                                           │
     │    delivery-id=42,                                             │
     │    delivery-tag=<8B>,                                          │
     │    settled=false,                                              │
     │    payload=<AMQP message>)                                     │
     │──────────────────►│                                            │
     │                   │                                            │
     │             ┌─────▼──────────────────────────────────┐        │
     │             │  Amqp10TransferHandler                  │        │
     │             │                                         │        │
     │             │  1. Decode Transfer performative         │        │
     │             │  2. Extract payload sections             │        │
     │             │  3. Convert AMQP message → Kafka record  │        │
     │             │     (§14: key, value, headers, timestamp)│        │
     │             │  4. Resolve target: topic=orders, P=0    │        │
     │             │  5. Track unsettled delivery (id=42)     │        │
     │             └─────┬──────────────────────────────────┘        │
     │                   │                                            │
     │             ┌─────▼──────────────────────────────────┐        │
     │             │  MetadataCache lookup:                  │        │
     │             │    P0 → leader = Broker 1 (remote)      │        │
     │             └─────┬──────────────────────────────────┘        │
     │                   │                                            │
     │             ┌─────▼──────────────────────────────────┐        │
     │             │  ProduceForwardManager.forward(          │        │
     │             │    leaderId=1,                           │        │
     │             │    {orders-0: MemoryRecords},            │        │
     │             │    acks=-1, timeout=30000)               │        │
     │             └─────┬──────────────────────────────────┘        │
     │                   │                                            │
     │            ProduceForwardThread-1                              │
     │                   │  binary ProduceRequest                     │
     │                   │  { orders-0: [record] }                    │
     │                   │────────────────────────────────────────────►
     │                   │                                            │
     │                   │                              ISR ack       │
     │                   │                                            │
     │                   │  ProduceResponse                           │
     │                   │  { orders-0: offset=1042 }                 │
     │                   │◄────────────────────────────────────────────
     │                   │                                            │
     │             future.complete({orders-0: offset=1042})           │
     │                   │                                            │
     │             ┌─────▼──────────────────────────────────┐        │
     │             │  amqpAsyncExecutor:                     │        │
     │             │  Encode Disposition(Accepted, settled)   │        │
     │             │  Remove from unsettled tracking          │        │
     │             └─────┬──────────────────────────────────┘        │
     │                   │                                            │
     │  Disposition (role=true,                                       │
     │    first=42, last=42,                                          │
     │    settled=true,                                               │
     │    state=Accepted{})                                           │
     │◄──────────────────│                                            │
```

### 5.2 End-to-end: Pre-settled (fire-and-forget)

When the client sets `settled=true`, the broker uses `acks=1` and skips Disposition:

```
AMQP Client           Broker 3                           Broker 1
     │                   │                                    │
     │  Transfer (handle=0,                                   │
     │    delivery-id=43,                                     │
     │    settled=true,      ← fire-and-forget                │
     │    payload=<message>)                                  │
     │──────────────────►│                                    │
     │                   │                                    │
     │             forward with acks=1                        │
     │             no Disposition sent                        │
     │             (client doesn't expect one)                │
     │                   │  binary ProduceRequest (acks=1)    │
     │                   │────────────────────────────────────►
     │                   │                                    │
     │                   │  ProduceResponse (leader ack only) │
     │                   │◄────────────────────────────────────
     │                   │                                    │
     │  (client continues sending without waiting)            │
```

### 5.3 End-to-end: Leader is local

When the receiving broker is the partition leader, no forwarding is needed:

```
AMQP Client           Broker 1 (leader for orders:0)
     │                   │
     │  Transfer (...)   │
     │──────────────────►│
     │                   │
     │             ReplicaManager.appendRecords()
     │             (direct — no forwarding)
     │             ISR ack
     │                   │
     │  Disposition(Accepted, settled=true)
     │◄──────────────────│
```

### 5.4 Multi-frame transfers

When a message exceeds `max-frame-size`, it is split across multiple Transfer frames:

```
AMQP Client           Broker
     │                   │
     │  Transfer (handle=0,                    │
     │    delivery-id=44,                      │
     │    delivery-tag=<8B>,                   │
     │    message-format=0,                    │
     │    more=true,        ← continuation     │
     │    payload=<first chunk>)               │
     │─────────────────────────────────────────►
     │                                          │
     │  Transfer (handle=0,                    │
     │    more=true,                           │
     │    payload=<middle chunk>)              │
     │─────────────────────────────────────────►
     │                                          │
     │  Transfer (handle=0,                    │  ← no more flag = final frame
     │    payload=<last chunk>)                │
     │─────────────────────────────────────────►
     │                                          │
     │                   Reassemble payload     │
     │                   Convert + append       │
     │                                          │
     │  Disposition(Accepted)                  │
     │◄─────────────────────────────────────────│
```

Continuation Transfer frames carry only `handle` (no `delivery-id`, `delivery-tag`, or
`message-format`). The `more` flag indicates additional frames follow. The broker reassembles
the payload from all frames before converting and appending.

### 5.5 Edge Cases

#### A. Link attach validation (before any Transfer)

| Condition | AMQP Error | Notes |
|---|---|---|
| Target address null or empty | `amqp:invalid-field` | Detach with error |
| Topic does not exist | `amqp:not-found` | Detach with error |
| Topic authorization failed | `amqp:unauthorized-access` | Detach with error |
| Partition out of range | `amqp:invalid-field` | Detach with error |
| Max links per session exceeded | `amqp:resource-limit-exceeded` | Detach with error |

#### B. Transfer-time errors

| Condition | AMQP Disposition | Kafka error | Notes |
|---|---|---|---|
| Message too large (> `max.message.bytes`) | `Rejected{amqp:resource-limit-exceeded}` | `MESSAGE_TOO_LARGE` | Checked before forwarding |
| Topic authorization revoked | `Rejected{amqp:unauthorized-access}` | `TOPIC_AUTHORIZATION_FAILED` | ACL changed mid-session |
| ISR below minimum | `Rejected{amqp:internal-error}` | `NOT_ENOUGH_REPLICAS` | Transient; client should retry |
| Request timeout | `Released{}` | `REQUEST_TIMED_OUT` | Broker waited past timeout; Released so client retries |
| Unknown topic (deleted mid-session) | `Rejected{amqp:not-found}` | `UNKNOWN_TOPIC_OR_PARTITION` | Topic removed after link attached |
| Kafka storage error | `Rejected{amqp:internal-error}` | `KAFKA_STORAGE_ERROR` | Local disk failure |

#### C. Forwarding-specific edge cases

| Condition | Handling |
|---|---|
| **Leader unknown** | `Disposition(Released{})` with `delivery-failed=true`. Broker refreshes metadata. Client retries. |
| **Forward connection failure** | `Disposition(Released{})`. Broker refreshes metadata for affected partitions. |
| **Leader changed** | Retry once with refreshed metadata. If second attempt fails, `Disposition(Rejected{amqp:internal-error})`. |
| **Forward timeout** | `Disposition(Released{})` — client can retry delivery. |

#### D. Idempotency

AMQP 1.0 delivery tags provide a natural deduplication mechanism. However, phase 1 does **not**
enable Kafka's `enable.idempotence` for non-transactional AMQP produces. The AMQP delivery tag
is used only for settlement correlation, not for broker-side deduplication. Clients that need
exactly-once semantics must use AMQP transactions (§4.7) which map to Kafka's transactional
produce with full idempotency.

---

## 6. Consume Path

### 6.1 Credit-driven fetch model

Unlike HTTP's request-response fetch, AMQP consume is **push-based**: the broker actively
delivers records to the client as they arrive, bounded by the client's granted link credit.

```
Credit grant                    Broker fetch loop
─────────────                   ─────────────────
Flow(credit=100)  ──────────►   fetchMessages(maxRecords=100)
                                ├── records available? ──YES──► Transfer × N (N ≤ credit)
                                │                               credit -= N
                                └── no records? ──► wait (DelayedFetch purgatory)
                                                    ├── records arrive → Transfer
                                                    └── timeout → do nothing, wait for next credit
```

**The broker never sends more than `linkCredit` transfers.** When credit reaches 0, the
broker stops fetching. The client replenishes credit (typically after processing a batch)
by sending a new Flow frame.

### 6.2 Delayed fetch interaction

When the partition has no new data, the broker creates a `DelayedFetch` entry in the purgatory.
Unlike HTTP (which must respond within `maxWaitMs`), AMQP connections are persistent — the
fetch can wait indefinitely until either:
1. New records arrive → broker delivers and decrements credit
2. Client sends new Flow → resets credit / triggers drain
3. Connection closes → cleanup

However, to avoid holding purgatory entries indefinitely, the broker caps the delayed fetch
at `amqp.consume.max.wait.ms` (default 30 s). When the timeout fires with no data, the broker
simply does nothing — the link remains open, waiting for the next credit grant or data arrival.

### 6.3 Push delivery handler

The `Amqp10PushDeliveryHandler` manages per-link delivery state:

```java
// Per-link state:
class LinkDeliveryState {
    long credit;                    // Available credit
    Queue<byte[]> pending;          // Buffered entries when credit exhausted
    // Methods: addCredit(), consumeCredit(), buffer(), dequeue()
}

// Global state:
ConcurrentHashMap<Long, LinkDeliveryState> linkStates;  // handle → state
AtomicLong deliveryIdCounter;                            // global delivery ID sequence
```

**Delivery flow:**

```
onPushEntries(channel, handle, entries):
  for each entry:
    if (credit > 0):
      deliverTransfer(ctx, channel, handle, entry)
      credit--
    else:
      buffer(entry)          // bounded by amqp.delivery.buffer.size

onCreditGranted(channel, handle, newCredit):
  credit += newCredit
  while (credit > 0 && hasBuffered()):
    deliverTransfer(ctx, channel, handle, dequeue())
    credit--
```

**Delivery encoding:**

```java
void deliverTransfer(ctx, channel, handle, payload) {
    long deliveryId = deliveryIdCounter.getAndIncrement();
    byte[] tag = longToBytes(deliveryId);   // 8-byte big-endian
    ByteBuf frame = ctx.alloc().buffer();
    Amqp10Codec.encodeTransfer(frame, channel, handle, deliveryId, tag,
        /* messageFormat */ 0, /* settled */ false, /* more */ false,
        payload, MAX_FRAME_SIZE);
    ctx.write(frame);   // caller flushes after batch
}
```

### 6.4 Header injection at delivery time

When delivering a record to a Receiver Link, the broker injects an AMQP Header section
containing delivery metadata:

| Header field | Value | Source |
|---|---|---|
| `delivery-count` | Redelivery count (0 = first delivery) | Tracked per delivery; incremented on Modified/Released |
| `first-acquirer` | `true` if first delivery, `false` otherwise | Derived from delivery-count |
| `durable` | Preserved from original message | Producer-set |
| `priority` | Preserved from original message | Producer-set |
| `ttl` | Preserved from original message | Producer-set |

The `Amqp10HeaderInjector` updates the Header section without re-encoding the entire message —
it splices a fresh Header at the front of the message sections byte array.

### 6.5 End-to-end: Credit grant, fetch, deliver, settle

```
AMQP Client           Broker 3 (leader for orders:0)
     │                   │
     │  [Receiver Link attached to "orders:0"]
     │                   │
     │  Flow (handle=1,  │
     │    link-credit=50,│
     │    delivery-count=0)
     │──────────────────►│
     │                   │
     │             replicaManager.fetchMessages(
     │               topic=orders, partition=0,
     │               offset=lastCommitted,
     │               maxRecords=50)
     │                   │
     │             ┌─────▼──────────────────┐
     │             │  30 records available   │
     │             │  (less than credit=50)  │
     │             └─────┬──────────────────┘
     │                   │
     │  Transfer (delivery-id=0, payload=record[0])
     │◄──────────────────│
     │  Transfer (delivery-id=1, payload=record[1])
     │◄──────────────────│
     │  ...              │
     │  Transfer (delivery-id=29, payload=record[29])
     │◄──────────────────│
     │                   │  credit remaining = 50 - 30 = 20
     │                   │
     │             ┌─────▼──────────────────┐
     │             │  No more records.       │
     │             │  Enter DelayedFetch     │
     │             │  purgatory (30s TTL).   │
     │             └─────────────────────────┘
     │                   │
     │  (client processes records 0..29)
     │                   │
     │  Disposition (role=false,
     │    first=0, last=29,
     │    settled=true,
     │    state=Accepted{})
     │──────────────────►│
     │                   │  Commit offset 30 via OffsetCommit
     │                   │
     │  Flow (handle=1,  │
     │    link-credit=50,│
     │    delivery-count=30)
     │──────────────────►│
     │                   │  Credit replenished → resume fetch
     │                   │  from offset 30
```

### 6.6 End-to-end: Rejected delivery → dead letter

```
AMQP Client           Broker
     │                   │
     │  Transfer (delivery-id=5, payload=record[5])
     │◄──────────────────│
     │                   │
     │  (client cannot process record — poison message)
     │                   │
     │  Disposition (first=5, last=5,
     │    settled=true,
     │    state=Rejected{
     │      error={condition="amqp:internal-error",
     │             description="deserialization failure"}})
     │──────────────────►│
     │                   │
     │             ┌─────▼──────────────────┐
     │             │  DeadLetterService:     │
     │             │  Route record to DLQ    │
     │             │  topic (if configured)  │
     │             │                         │
     │             │  Advance cursor past    │
     │             │  offset 5               │
     │             └─────────────────────────┘
```

### 6.7 Consume offset management

AMQP Disposition(Accepted) maps to Kafka offset commit. The mapping is:

```
Disposition(first=F, last=L, state=Accepted{}):
  → OffsetCommit(topic, partition, offset = L + 1)
    (Kafka commits the NEXT offset to read, not the last consumed)
```

For range dispositions, the broker commits the highest offset in the range + 1.

**Consumer group integration.** The AMQP consumer's group ID is derived from:
1. Link property `kafka-group-id` (if set) — client explicitly joins a group
2. Connection's `container-id` + link name (default) — per-client offset tracking

Offsets are committed to Kafka's `__consumer_offsets` topic using the same
`GroupCoordinator.commitOffsets()` path as binary protocol consumers.

---

## 7. Broker-to-Broker Internal Forwarding

### 7.1 Reuse of existing forwarding infrastructure

AMQP produce and consume forwarding reuses the **same `ProduceForwardManager` and
`FetchForwardManager`** designed for the HTTP protocol (see http-protocol-design.md §7).
The forwarding layer is protocol-agnostic — it accepts `MemoryRecords` for produce and
`FetchRequest.PartitionData` for fetch, regardless of the originating protocol.

```
AMQP Transfer arrives at Broker B for partition P:

is B the leader for P?
  ├── YES → ReplicaManager.appendRecords() directly
  └── NO  → does MetadataCache know the leader?
             ├── YES (leaderId = L) → ProduceForwardManager.forward(L, ...)
             │     → binary ProduceRequest to leader
             │     → future.complete() → Disposition(Accepted)
             └── NO → Disposition(Released{}) — client retries
```

```
AMQP Receiver Link credit arrives at Broker B for partition P:

is B the leader for P?
  ├── YES → ReplicaManager.fetchMessages() → push Transfer frames
  └── NO  → FetchForwardManager.forward(L, ...)
             → binary FetchRequest to leader
             → convert FetchResponse → push Transfer frames
```

### 7.2 Forwarding decision matrix

```
Produce (Transfer on Sender Link):
  ├── Local leader → appendRecords() → Disposition(Accepted)
  ├── Remote leader → forward(leaderId, records) → wait → Disposition(Accepted)
  ├── Leader unknown → Disposition(Released{}) + metadata refresh
  └── Forward timeout → Disposition(Released{})

Consume (credit on Receiver Link):
  ├── Local leader → fetchMessages() → push Transfer frames
  ├── Remote leader → forward(leaderId, fetchSpecs) → push Transfer frames
  ├── Leader unknown → wait for metadata refresh, then retry
  └── Forward timeout → no deliveries (credit preserved, retry on next fetch cycle)
```

### 7.3 Key difference from HTTP forwarding

HTTP forwarding must return all results in a single response — it uses
`CompletableFuture.allOf()` to merge local and remote results before responding.

AMQP forwarding is **streaming**: each record can be delivered individually as a Transfer
frame. There is no "response aggregation" step. This means:
- Local records are delivered immediately (no waiting for remote)
- Remote records are delivered as they arrive from forwarded fetches
- Credit is decremented per delivery, not per batch

---

## 8. Integration with Existing Kafka Infrastructure

### 8.1 New security protocol: `AMQP`

Add `AMQP` and `AMQPS` to `SecurityProtocol` enum (ids 6 and 7):

```java
// clients/src/main/java/org/apache/kafka/common/security/auth/SecurityProtocol.java
// Existing: PLAINTEXT(0), SSL(1), SASL_PLAINTEXT(2), SASL_SSL(3), HTTP(4), HTTPS(5)
AMQP(6, "AMQP"),
AMQPS(7, "AMQPS");
```

Add a helper method:

```java
public boolean isAmqp() { return this == AMQP || this == AMQPS; }
```

`listener.security.protocol.map` example:

```properties
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,AMQP:AMQP,AMQPS:AMQPS
listeners=PLAINTEXT://0.0.0.0:9092,AMQP://0.0.0.0:5672
advertised.listeners=PLAINTEXT://broker1.example.com:9092,AMQP://broker1.example.com:5672
```

**`inter.broker.listener.name` must remain a binary-protocol listener.** AMQP is a
client-facing protocol only; inter-broker forwarding uses the Kafka binary protocol.

### 8.2 Amqp10Acceptor — plugging into SocketServer

```scala
// core/src/main/scala/kafka/network/SocketServer.scala

def createDataPlaneAcceptorAndProcessors(endpoint: Endpoint): Unit = {
  endpoint.securityProtocol match {
    case SecurityProtocol.AMQP | SecurityProtocol.AMQPS =>
      val amqpAcceptor = new Amqp10Acceptor(this, endpoint, config, ...)
      amqpAcceptors.put(endpoint, amqpAcceptor)
    case SecurityProtocol.HTTP | SecurityProtocol.HTTPS =>
      // ... (HTTP handling)
    case _ =>
      val dataPlaneAcceptor = new DataPlaneAcceptor(this, endpoint, config, ...)
      dataPlaneAcceptors.put(endpoint, dataPlaneAcceptor)
  }
}
```

### 8.3 Amqp10Acceptor — Netty-based server

```scala
class Amqp10Acceptor(
  socketServer:   SocketServer,
  endpoint:       Endpoint,
  config:         KafkaConfig,
  requestChannel: RequestChannel,
  ...
) extends Closeable {

  private val bossGroup:   EventLoopGroup = new NioEventLoopGroup(1)
  private val workerGroup: EventLoopGroup = new NioEventLoopGroup(config.numAmqpNetworkThreads)
  private var channel:     Channel = _

  def startup(): Unit = {
    val bootstrap = new ServerBootstrap()
    bootstrap
      .group(bossGroup, workerGroup)
      .channel(classOf[NioServerSocketChannel])
      .option(ChannelOption.TCP_NODELAY, true)   // low-latency for small frames
      .childHandler(new Amqp10ChannelInitializer(config, requestChannel, ...))
    channel = bootstrap.bind(endpoint.host, endpoint.port).sync().channel()
  }

  override def close(): Unit = {
    channel.close().sync()
    workerGroup.shutdownGracefully(100, 500, MILLISECONDS).sync()
    bossGroup.shutdownGracefully(100, 200, MILLISECONDS).sync()
  }
}
```

### 8.4 Amqp10ChannelInitializer — Netty pipeline

```scala
class Amqp10ChannelInitializer(config: KafkaConfig, requestChannel: RequestChannel, ...)
    extends ChannelInitializer[SocketChannel] {

  override def initChannel(ch: SocketChannel): Unit = {
    val pipeline = ch.pipeline()
    // Optional TLS (for AMQPS)
    if (endpoint.securityProtocol == SecurityProtocol.AMQPS)
      pipeline.addLast("tls", sslContext.newHandler(ch.alloc()))
    // Idle connection detection (heartbeat timeout)
    pipeline.addLast("idle-handler",
      new IdleStateHandler(config.amqpIdleTimeoutMs, 0, 0, MILLISECONDS))
    // AMQP 1.0 frame decoder (ByteToMessageDecoder)
    pipeline.addLast("amqp-frame-decoder",   new Amqp10FrameDecoder(config.amqpMaxFrameSize))
    // Connection lifecycle (SASL + Open/Close)
    pipeline.addLast("amqp-connection",      new Amqp10ConnectionHandler(authenticator))
    // Session lifecycle (Begin/End)
    pipeline.addLast("amqp-session",         new Amqp10SessionHandler())
    // Link lifecycle (Attach/Detach)
    pipeline.addLast("amqp-link",            new Amqp10LinkHandler())
    // Transfer/Disposition processing
    pipeline.addLast("amqp-transfer",        new Amqp10TransferHandler(requestChannel, ...))
    // Flow control (link credit + drain/echo)
    pipeline.addLast("amqp-flow",            new Amqp10FlowHandler())
    // Push delivery (broker → client)
    pipeline.addLast("amqp-push-delivery",   new Amqp10PushDeliveryHandler())
  }
}
```

**Handler pipeline order rationale:**

| Position | Handler | Processes | Forwards |
|---|---|---|---|
| 1 | `Amqp10FrameDecoder` | Raw bytes → frames | Frames |
| 2 | `Amqp10ConnectionHandler` | SASL, Open, Close | Post-Open frames |
| 3 | `Amqp10SessionHandler` | Begin, End | Post-Begin frames |
| 4 | `Amqp10LinkHandler` | Attach, Detach, Flow (link-level) | Transfer, Disposition, session Flow |
| 5 | `Amqp10TransferHandler` | Transfer, Disposition | — |
| 6 | `Amqp10FlowHandler` | Flow (credit tracking, drain, echo) | — |
| 7 | `Amqp10PushDeliveryHandler` | Outbound delivery push | — |

Each handler is **not `@Sharable`** — one instance per connection, holding mutable per-connection
state in `ConcurrentHashMap` fields.

### 8.5 Amqp10Processor — response path

Like the HTTP `HttpProcessor` (http-protocol-design.md §8.6), the `Amqp10Processor` bridges
`RequestChannel` responses back to the correct Netty channel:

```java
public class Amqp10Processor {

    private final int id;
    private final LinkedBlockingDeque<RequestChannel.Response> responseQueue =
        new LinkedBlockingDeque<>();
    private final ConcurrentHashMap<String, Amqp10ResponseContext> channels =
        new ConcurrentHashMap<>();   // connectionId → response context

    // connectionId → { ChannelHandlerContext, deliveryId, handle, channel }
    public void registerPendingDelivery(String connectionId, Amqp10ResponseContext ctx) {
        channels.put(connectionId, ctx);
    }

    public void enqueueResponse(RequestChannel.Response response) {
        responseQueue.add(response);
    }

    public void processResponses() {
        List<RequestChannel.Response> batch = new ArrayList<>();
        responseQueue.drainTo(batch);

        for (RequestChannel.Response response : batch) {
            String connectionId = response.request().context().connectionId();
            Amqp10ResponseContext ctx = channels.remove(connectionId);

            if (response instanceof RequestChannel.SendResponse sendResp) {
                if (ctx != null && ctx.nettyCtx().channel().isActive()) {
                    // Encode Disposition based on Kafka response
                    AbstractResponse kafkaResponse = sendResp.response();
                    ByteBuf disposition = Amqp10Codec.encodeDisposition(
                        ctx.nettyCtx().alloc().buffer(),
                        ctx.channel(), /* role=true (receiver) */ true,
                        ctx.deliveryId(), ctx.deliveryId(),
                        /* settled */ true,
                        mapKafkaOutcome(kafkaResponse));
                    ctx.nettyCtx().writeAndFlush(disposition);
                }
            }
        }
    }
}
```

---

## 9. New Module: `amqp-server`

```
kafka/
└── amqp-server/
    ├── build.gradle
    └── src/
        ├── main/
        │   ├── java/kafka/server/amqp/
        │   │   ├── codec/
        │   │   │   ├── Amqp10Codec.java
        │   │   │   ├── Amqp10FrameHeader.java
        │   │   │   └── Amqp10HeaderInjector.java
        │   │   ├── handler/
        │   │   │   ├── Amqp10FrameDecoder.java
        │   │   │   ├── Amqp10ConnectionHandler.java
        │   │   │   ├── Amqp10SessionHandler.java
        │   │   │   ├── Amqp10LinkHandler.java
        │   │   │   ├── Amqp10TransferHandler.java
        │   │   │   ├── Amqp10FlowHandler.java
        │   │   │   ├── Amqp10PushDeliveryHandler.java
        │   │   │   ├── Amqp10DispositionHandler.java
        │   │   │   ├── Amqp10TransactionHandler.java
        │   │   │   ├── Amqp10SessionFlowControl.java
        │   │   │   ├── Amqp10SettlementManager.java
        │   │   │   └── Amqp10CreditFlowIntegrator.java
        │   │   └── Amqp10Processor.java
        │   ├── scala/kafka/
        │   │   └── network/
        │   │       ├── Amqp10Acceptor.scala
        │   │       ├── Amqp10ChannelInitializer.scala
        │   │       └── Amqp10AddressParser.scala
        │   └── resources/
        └── test/
            ├── java/kafka/server/amqp/
            │   ├── codec/
            │   │   ├── Amqp10CodecTest.java
            │   │   └── Amqp10HeaderInjectorTest.java
            │   └── handler/
            │       ├── Amqp10ConnectionHandlerTest.java
            │       ├── Amqp10SessionHandlerTest.java
            │       ├── Amqp10LinkHandlerTest.java
            │       ├── Amqp10TransferHandlerTest.java
            │       ├── Amqp10FlowHandlerTest.java
            │       ├── Amqp10TransactionHandlerTest.java
            │       ├── Amqp10DispositionHandlerTest.java
            │       ├── Amqp10SessionFlowControlTest.java
            │       └── Amqp10SettlementManagerTest.java
            └── scala/kafka/
                └── server/amqp/
                    ├── Amqp10ProduceIntegrationTest.scala
                    ├── Amqp10ConsumeIntegrationTest.scala
                    ├── Amqp10TransactionIntegrationTest.scala
                    └── Amqp10MultiFrameIntegrationTest.scala
```

### 9.1 `amqp-server/build.gradle` dependencies

```groovy
dependencies {
  implementation project(':core')
  implementation project(':clients')
  implementation project(':server-common')
  implementation "io.netty:netty-all:${versions.netty}"
  implementation "org.apache.qpid:protonj2:1.1.0"   // AMQP 1.0 type system + codec

  testImplementation project(':core').sourceSets.test.output
  testImplementation "org.apache.qpid:qpid-jms-client:${versions.qpidJms}"   // JMS test client
  testImplementation "org.apache.kafka:kafka-clients:${version}:test"
}
```

**Why protonj2?** Apache Qpid protonj2 provides:
- Complete AMQP 1.0 type system (symbols, described types, performatives)
- Efficient binary codec (ProtonBuffer ↔ AMQP wire format)
- Performative classes (Open, Begin, Attach, Transfer, Flow, Disposition, Close, etc.)
- SASL frame types (SaslMechanisms, SaslInit, SaslOutcome)
- No dependency on a transport layer (codec-only usage)

---

## 10. Configuration

New properties added to `KafkaConfig`:

| Property | Default | Description |
|---|---|---|
| `amqp.enabled` | `false` | Master switch (can also just add `AMQP://...` to `listeners`) |
| `num.amqp.network.threads` | `4` | Netty worker thread count for AMQP listener |
| `amqp.max.frame.size` | `262144` (256 KB) | Max AMQP frame size; advertised in Open. Larger messages use multi-frame transfers. |
| `amqp.idle.timeout.ms` | `60000` (60 s) | AMQP heartbeat idle timeout; advertised in Open. Connection closed if no frame received within this window. |
| `amqp.consume.max.wait.ms` | `30000` (30 s) | Max time a fetch waits in DelayedFetch purgatory per credit-driven fetch cycle |
| `amqp.delivery.buffer.size` | `1000` | Per-link delivery buffer for records awaiting credit |
| `amqp.max.links.per.session` | `256` | Max concurrent links per session |
| `amqp.max.sessions.per.connection` | `64` | Max concurrent sessions per connection |
| `amqp.transaction.timeout.ms` | `60000` (60 s) | Max time between Declare and Discharge before auto-abort |
| `amqp.sasl.mechanisms` | `PLAIN` | Comma-separated list of supported SASL mechanisms |
| `amqp.session.window.size` | `2048` | Default session-level incoming/outgoing window |
| `amqp.shutdown.drain.ms` | `5000` | Drain window during graceful shutdown — AMQP connections are long-lived so need a longer drain than HTTP |
| `num.amqp.async.threads` | `4` | Thread pool for async produce/consume completion |

Listener registration:

```properties
listeners=PLAINTEXT://0.0.0.0:9092,AMQP://0.0.0.0:5672
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,AMQP:AMQP
advertised.listeners=PLAINTEXT://broker1.example.com:9092,AMQP://broker1.example.com:5672
```

---

## 11. Error Handling

### 11.1 AMQP error condition mapping

AMQP 1.0 errors are reported either as:
- **Disposition outcomes** (Rejected, Released, Modified) — per-message errors
- **Detach with error** — link-level errors
- **End with error** — session-level errors
- **Close with error** — connection-level errors

#### Disposition-level error mapping (produce failures)

| Kafka Error | AMQP Outcome | Error Condition | Notes |
|---|---|---|---|
| `NONE` | `Accepted{}` | — | Success |
| `UNKNOWN_TOPIC_OR_PARTITION` | `Rejected{}` | `amqp:not-found` | Topic deleted mid-session |
| `LEADER_NOT_AVAILABLE` | `Released{}` | — | Transient; client retries delivery |
| `NOT_LEADER_OR_FOLLOWER` | `Released{}` | — | Leader election; retry |
| `MESSAGE_TOO_LARGE` | `Rejected{}` | `amqp:resource-limit-exceeded` | Not retriable |
| `RECORD_LIST_TOO_LARGE` | `Rejected{}` | `amqp:resource-limit-exceeded` | Not retriable |
| `TOPIC_AUTHORIZATION_FAILED` | `Rejected{}` | `amqp:unauthorized-access` | Not retriable |
| `NOT_ENOUGH_REPLICAS` | `Released{}` | — | ISR below minimum; transient |
| `REQUEST_TIMED_OUT` | `Released{}` | — | Client can retry |
| `KAFKA_STORAGE_ERROR` | `Rejected{}` | `amqp:internal-error` | Disk failure |
| Everything else | `Rejected{}` | `amqp:internal-error` | Catch-all |

**Rationale for Released vs Rejected:** `Released` tells the client "try again" — the message
is not permanently failed. `Rejected` tells the client "this message will never succeed" — don't
retry. Transient Kafka errors (leader election, ISR, timeout) map to Released. Permanent errors
(auth, topic not found, message too large) map to Rejected.

#### Link-level errors (Detach with error)

| Condition | AMQP Error | When |
|---|---|---|
| Topic not found at Attach | `amqp:not-found` | Address resolution failure |
| Authorization failure at Attach | `amqp:unauthorized-access` | ACL check |
| Invalid address format | `amqp:invalid-field` | Malformed address string |
| Partition out of range | `amqp:invalid-field` | Partition doesn't exist |
| Too many links | `amqp:resource-limit-exceeded` | Per-session link limit |

#### Session-level errors (End with error)

| Condition | AMQP Error | When |
|---|---|---|
| Window violation | `amqp:window-violation` | Transfer sent outside window |
| Too many sessions | `amqp:resource-limit-exceeded` | Per-connection session limit |

#### Connection-level errors (Close with error)

| Condition | AMQP Error | When |
|---|---|---|
| SASL authentication failure | `amqp:unauthorized-access` | Bad credentials |
| Idle timeout | `amqp:resource-limit-exceeded` | No frames within idle-time-out |
| Frame too large | `amqp:frame-size-too-small` | Frame exceeds negotiated max |
| Internal error | `amqp:internal-error` | Unrecoverable broker error |

### 11.2 Dead letter queue routing

When a consumer sends `Disposition(Rejected)`, the broker routes the message to a dead letter
queue (DLQ) if configured. DLQ topic naming:

```
Original topic: orders
DLQ topic:      orders.dlq     (configurable via topic config property)
```

The DLQ record includes annotations:
- `x-dead-letter-reason`: "REJECTED"
- `x-original-topic`: source topic name
- `x-original-partition`: source partition
- `x-original-offset`: source offset
- `x-death-timestamp`: when the message was dead-lettered

---

## 12. Security

### 12.1 SASL Authentication

AMQP 1.0 has built-in SASL negotiation as part of the connection handshake. The broker
supports SASL mechanisms that map to Kafka's existing credential stores:

| SASL Mechanism | Implementation | Kafka credential store |
|---|---|---|
| `PLAIN` | Username/password in `\0<authcid>\0<passwd>` format | JAAS `PlainLoginModule` |
| `SCRAM-SHA-256` | Challenge-response | Kafka SCRAM credential store |
| `SCRAM-SHA-512` | Challenge-response | Kafka SCRAM credential store |

**SASL PLAIN flow** (most common):

```java
// Amqp10ConnectionHandler.parseSaslPlain():
// Format: \0<authcid>\0<passwd>  (authzid is empty before first NUL)
byte[] responseBytes = saslInit.getInitialResponse();
// Split by NUL: [0] = empty (authzid), [1] = username, [2] = password
```

The `Amqp10Authenticator` functional interface delegates to Kafka's `CredentialProvider`:

```java
@FunctionalInterface
public interface Amqp10Authenticator {
    AuthResult authenticate(String username, String password);
}
```

The resulting `KafkaPrincipal` is set on `RequestContext` and flows through the existing
`Authorizer` path — zero changes to authorization logic.

### 12.2 TLS

The `AMQPS` security protocol adds TLS. Standard AMQP TLS port is 5671.
TLS handler is the first in the Netty pipeline (§8.4). Uses the same
`SslContext` configuration as Kafka's existing SSL listener.

### 12.3 Authorization

AMQP operations map to existing Kafka ACL operations:

| AMQP Operation | Kafka ACL | Resource |
|---|---|---|
| Attach Sender Link | `WRITE` | Topic |
| Attach Receiver Link | `READ` | Topic |
| Transfer (produce) | `WRITE` | Topic (already checked at Attach) |
| Disposition (consume ack) | `READ` | Topic (already checked at Attach) |
| Transaction Declare | `WRITE` + `IDEMPOTENT_WRITE` | Cluster |
| Transaction Discharge(commit) | `WRITE` | Topic (per partition in transaction) |

Authorization is checked at **link Attach time** — not per Transfer. If ACLs change
mid-session, the next Transfer/Disposition gets a rejection; the broker does not
proactively detach links.

### 12.4 Quota enforcement

AMQP clients are subject to the same `ClientQuotaManager` as binary protocol clients.
The `clientId` is derived from the AMQP connection's `container-id` field. The `user` is
derived from the SASL-authenticated principal.

When quota is exceeded, the broker responds with `Released{}` (throttle) instead of
`Accepted{}` for the Transfer that triggered the quota violation. The client's existing
retry logic handles backoff. Additionally, the broker can reduce the effective link credit
to slow down the client.

---

## 13. Address Format & Topic Mapping

### 13.1 Address syntax

AMQP 1.0 link addresses map to Kafka topics and partitions:

```
address := topic [ ":" partition ]

topic     := Kafka topic name (validated by Topic.validate())
partition := non-negative integer (0-based)
```

**Examples:**

| AMQP Address | Kafka Topic | Kafka Partition | Notes |
|---|---|---|---|
| `orders` | `orders` | (assigned by broker) | Broker picks partition |
| `orders:0` | `orders` | `0` | Explicit partition |
| `orders:2` | `orders` | `2` | Explicit partition |

### 13.2 Address resolution

```java
// Amqp10AddressParser.java
public record ParsedAddress(String topic, OptionalInt partition) {

    public static ParsedAddress parse(String address) {
        if (address == null || address.isEmpty())
            throw new AmqpException("amqp:invalid-field", "Address is required");

        int colonIdx = address.lastIndexOf(':');
        if (colonIdx < 0)
            return new ParsedAddress(address, OptionalInt.empty());

        String topicPart = address.substring(0, colonIdx);
        String partitionPart = address.substring(colonIdx + 1);

        try {
            int partition = Integer.parseInt(partitionPart);
            if (partition < 0)
                throw new AmqpException("amqp:invalid-field",
                    "Partition must be non-negative: " + partition);
            return new ParsedAddress(topicPart, OptionalInt.of(partition));
        } catch (NumberFormatException e) {
            // Colon is part of topic name (e.g. "namespace:topic")
            return new ParsedAddress(address, OptionalInt.empty());
        }
    }
}
```

**Edge case: topic names containing colons.** If the string after the last colon is not a
valid integer, the entire address is treated as the topic name. This allows topic names
like `my-namespace:orders` (no partition) while supporting `orders:2` (explicit partition).

### 13.3 Partition assignment for unpartitioned addresses

When no partition is specified:

```
Sender Link (produce):
  if record key != null:
    partition = murmur2(key) % partitionCount     // deterministic, matches Java client
  else:
    partition = stickyCounter.getAndIncrement() % partitionCount  // per-link round-robin

Receiver Link (consume):
  All partitions consumed via partition assignment from GroupCoordinator
  OR single partition 0 if no group is configured
```

---

## 14. Message Conversion

### 14.1 AMQP → Kafka (produce path)

When a Transfer frame arrives on a Sender Link, the AMQP message must be converted to a
Kafka `ProducerRecord`:

| AMQP Section | Kafka Record Field | Conversion |
|---|---|---|
| Properties.message-id | Header: `amqp-message-id` | String representation |
| Properties.correlation-id | Header: `amqp-correlation-id` | String representation |
| Properties.content-type | Header: `amqp-content-type` | String |
| Properties.subject | Header: `amqp-subject` | String |
| Properties.to | (ignored — address is on the link) | — |
| Properties.reply-to | Header: `amqp-reply-to` | String |
| Properties.creation-time | Record timestamp | Epoch milliseconds |
| Application-Properties | Record headers | Each entry → header key:value |
| Data section | Record value | Raw bytes |
| AmqpValue section | Record value | Serialized to bytes (type-dependent) |
| AmqpSequence section | Record value | Serialized to bytes |

**Key extraction.** AMQP 1.0 has no built-in concept of a message key (unlike Kafka).
The key is extracted from:
1. Application property `kafka-key` (if present) — explicit key
2. Properties.message-id (fallback if `kafka-key` not set) — natural dedup key
3. null (if neither set) — keyless record

**Timestamp.** If `Properties.creation-time` is set, it is used as the Kafka record timestamp.
Otherwise, the broker uses `System.currentTimeMillis()` (append time).

### 14.2 Kafka → AMQP (consume path)

When delivering a Kafka record as a Transfer frame to a Receiver Link:

| Kafka Record Field | AMQP Section | Conversion |
|---|---|---|
| Record key | Application property: `kafka-key` | Base64 if binary, UTF-8 if string |
| Record value | Data section | Raw bytes wrapped in AMQP Data descriptor |
| Record headers | Application-Properties | Each header → property entry |
| Record timestamp | Properties.creation-time | Epoch milliseconds |
| Record offset | Message-Annotations: `x-kafka-offset` | Long |
| Record partition | Message-Annotations: `x-kafka-partition` | Integer |
| Record topic | Message-Annotations: `x-kafka-topic` | String |

**Why message annotations for Kafka metadata?** Message annotations (§3.2.2 of the AMQP spec)
are for infrastructure use — the sending/receiving application typically does not see them, but
intermediaries (brokers) can use them for routing. Kafka offset/partition/topic are broker
metadata, not application data. Using annotations keeps them separate from application
properties.

### 14.3 Message format

AMQP 1.0 messages consist of ordered sections:

```
┌──────────────┐
│    Header     │  (delivery-count, priority, ttl, durable, first-acquirer)
├──────────────┤
│ Delivery-    │  (optional annotations for intermediaries)
│ Annotations  │
├──────────────┤
│  Message-    │  (x-kafka-offset, x-kafka-partition, x-kafka-topic)
│ Annotations  │
├──────────────┤
│  Properties  │  (message-id, creation-time, content-type, etc.)
├──────────────┤
│ Application- │  (kafka-key, user-defined headers)
│ Properties   │
├──────────────┤
│    Body      │  Data section (raw bytes) OR AmqpValue OR AmqpSequence
├──────────────┤
│    Footer    │  (optional; not used)
└──────────────┘
```

The codec encodes the body as an AMQP Data section with descriptor `0x00 0x53 0x75` followed
by a vbin8 (≤254 bytes) or vbin32 (≤4GB) length-prefixed payload. This avoids any
interpretation of the payload bytes — Kafka record values are opaque binary.

---

## 15. Implementation Plan

### Phase 1 — AMQP listener + basic produce/consume

1. Add `AMQP(6)` / `AMQPS(7)` to `SecurityProtocol` enum with `isAmqp()` helper
2. Create `amqp-server` Gradle submodule with Netty + protonj2 dependencies
3. Implement `Amqp10Codec` — frame encoding/decoding using protonj2
4. Implement `Amqp10FrameDecoder` — ByteToMessageDecoder with frame size validation
5. Implement `Amqp10ConnectionHandler` — SASL PLAIN + Open/Close lifecycle
6. Implement `Amqp10SessionHandler` — Begin/End with flow control windows
7. Implement `Amqp10LinkHandler` — Attach/Detach with address resolution
8. Implement `Amqp10AddressParser` — topic:partition parsing
9. Implement `Amqp10Acceptor`, `Amqp10ChannelInitializer` — Netty bootstrap
10. Wire `Amqp10Acceptor` into `SocketServer.createDataPlaneAcceptorAndProcessors()`
11. Implement `Amqp10TransferHandler` — produce path (Sender Link → PRODUCE ApiKey)
12. Implement `Amqp10Processor` — response routing (RequestChannel → Disposition)
13. Add dispatch branch in `KafkaApis.handle()` for `securityProtocol.isAmqp`
14. Implement basic Receiver Link — credit-driven fetch with `Amqp10PushDeliveryHandler`
15. Implement `Amqp10SessionFlowControl` — session-level window tracking
16. Add new config properties to `KafkaConfig` (§10)
17. Message conversion: AMQP ↔ Kafka record (§14)
18. Integration test: single-partition produce/consume (local leader path, Qpid JMS client)

### Phase 2 — Forwarding + flow control + settlement

19. Reuse `ProduceForwardManager` / `FetchForwardManager` for AMQP → remote leader forwarding
20. Implement `Amqp10FlowHandler` — link credit tracking, drain mode, echo mode
21. Implement `Amqp10DispositionHandler` — Accepted/Rejected/Released/Modified outcomes
22. Implement `Amqp10SettlementManager` — unsettled delivery tracking
23. Implement `Amqp10CreditFlowIntegrator` — credit exhaustion → pause fetch, credit grant → resume
24. Range disposition support: batch settlement of `[first, last]`
25. Multi-frame transfer support: message splitting/reassembly for large messages
26. Consumer offset commit via Disposition(Accepted) → OffsetCommit
27. `Amqp10HeaderInjector` — delivery-count/first-acquirer injection at delivery time
28. Dead letter routing for Disposition(Rejected) — route to DLQ topic
29. Integration tests: multi-partition produce/consume hitting follower brokers
30. Integration tests: drain mode, flow control, settlement outcomes

### Phase 3 — Transactions + robustness + advanced features

31. Implement `Amqp10TransactionHandler` — Declare/Discharge coordinator links
32. Transaction → Kafka transactional produce mapping (InitProducerId + EndTxn)
33. Buffered writes: accumulate Transfers with TransactionalState, flush on Discharge
34. Transaction timeout: auto-abort after `amqp.transaction.timeout.ms`
35. SASL SCRAM-SHA-256/512 support (challenge-response, not single-init)
36. Consumer group integration: `kafka-group-id` link property
37. Metrics: `amqp.produce.rate`, `amqp.consume.rate`, `amqp.settlement.rate`, `amqp.flow.credit.total`
38. Graceful shutdown drain integration (§16.5)
39. Full error handling per §11 tables
40. Performance benchmarking (produce/consume throughput and latency under load)
41. Integration tests: transactions (commit + abort), SASL SCRAM, consumer groups

### Phase 4 — Durable terminus + cluster features (future)

42. Durable terminus: persist unsettled deliveries for link resumption across reconnects
43. Connection redirect: `connection:redirect` for partition rebalancing
44. Dynamic link creation (server-named sources/targets)
45. AMQP 1.0 connection multiplexing (HTTP/2-style session parallelism)
46. Integration tests: durable links, failover, reconnection with unsettled resume

---

## 16. Implementation Concerns

### 16.1 CRITICAL — Netty event loop safety: no blocking I/O on worker threads

**Problem.** AMQP handler methods run on Netty worker threads. Any blocking call (JDBC, DNS,
`Future.get()`) blocks all connections multiplexed on that thread.

**Solution.** All blocking operations are offloaded:
- **Produce forwarding:** `ProduceForwardManager.forward()` returns `CompletableFuture` immediately;
  the callback runs on `amqpAsyncExecutor` (not the worker thread)
- **Fetch forwarding:** Same pattern via `FetchForwardManager`
- **Offset commits:** `GroupCoordinator.commitOffsets()` is non-blocking (writes to `__consumer_offsets`
  via the same RequestChannel path)
- **Transaction operations:** `InitProducerId`, `EndTxn` are non-blocking RPCs via RequestChannel
- **DLQ routing:** Offloaded to virtual thread (`Thread.startVirtualThread()`)

```java
// In Amqp10TransferHandler — NEVER block on the Netty worker:
CompletableFuture<ProduceResponse> future = forwardManager.forward(leaderId, records, ...);
future.handleAsync((response, ex) -> {
    if (ex != null) {
        sendDisposition(ctx, channel, deliveryId, new Released());
    } else {
        sendDisposition(ctx, channel, deliveryId, new Accepted());
    }
    return null;
}, amqpAsyncExecutor);   // ← always specify executor
```

### 16.2 CRITICAL — ByteBuf lifecycle in handler chain

**Problem.** AMQP frames are `ByteBuf` instances passed through a chain of handlers. Each
handler must either consume (release) or forward (retain) the buffer. Missing a release
causes memory leaks; double-release causes SIGSEGV with direct buffers.

**Rules enforced per handler:**

```
channelRead(ctx, msg):
  try {
    if (msg instanceof ByteBuf frame) {
      if (isMyPerformative(frame)) {
        handlePerformative(ctx, frame);
        // Handler consumed the frame — release in finally
      } else {
        frame.retain();
        ctx.fireChannelRead(frame);  // Forward to next handler
        // Original frame released in finally; forwarded copy has its own refcount
      }
    }
  } finally {
    ReferenceCountUtil.safeRelease(msg);
  }
```

The `Amqp10FrameDecoder` uses `readRetainedSlice()` for zero-copy frame extraction.

### 16.3 HIGH — Settlement state memory bounds

**Problem.** A slow consumer that never settles deliveries causes the `unsettled` map in
`Amqp10SettlementManager` to grow unboundedly.

**Solution.** Bound unsettled deliveries per link:

```java
private static final int MAX_UNSETTLED_PER_LINK = 10_000;

void recordDelivery(long deliveryId, ...) {
    if (unsettled.size() >= MAX_UNSETTLED_PER_LINK) {
        // Pause delivery on this link (set credit to 0)
        // Resume when client settles outstanding deliveries
        pauseLink(handle);
        return;
    }
    unsettled.put(deliveryId, new UnsettledDelivery(...));
}
```

This naturally integrates with credit-based flow control — when the broker pauses delivery,
the client must settle existing deliveries before receiving more.

### 16.4 HIGH — Thread safety for per-connection state

**Problem.** Each handler holds per-connection state (`ConcurrentHashMap` of sessions, links,
deliveries). While Netty guarantees handler methods are called on the same event loop thread,
the `amqpAsyncExecutor` callbacks may access the same state from different threads.

**Solution.** State that is read/written only by handler methods uses
`ConcurrentHashMap<K, V>(initialCapacity, loadFactor, concurrencyLevel=1)` — low overhead
for single-writer access. State accessed by async callbacks (settlement manager, delivery
handler) uses standard concurrent data structures with atomic operations.

The `Amqp10SessionFlowControl` is explicitly **not thread-safe** — it is only accessed from
the Netty event loop thread for that connection. No synchronization overhead.

### 16.5 MEDIUM — Graceful shutdown

AMQP connections are long-lived (unlike HTTP request/response). Graceful shutdown must:

1. Stop accepting new connections (`Amqp10Acceptor.beginDrain()`)
2. Send Close to all open connections with `amqp:connection-forced` error condition
3. Wait up to `amqp.shutdown.drain.ms` (default 5 s) for clients to acknowledge Close
4. Force-close remaining connections
5. Shut down forward threads and async executor

```scala
def stopProcessingRequests(): Unit = synchronized {
  // ... existing binary + HTTP shutdown ...

  // AMQP shutdown:
  amqpAcceptors.asScala.values.foreach { acc =>
    acc.beginDrain()   // stop new connections, send Close to existing
  }
  val amqpDrainDeadline = time.milliseconds() + config.amqpShutdownDrainMs
  amqpAcceptors.asScala.values.foreach { acc =>
    val remaining = amqpDrainDeadline - time.milliseconds()
    if (remaining > 0) acc.awaitDrain(remaining)
  }
  amqpAcceptors.asScala.values.foreach(_.close())
}
```

### 16.6 MEDIUM — Heartbeat and idle connection management

AMQP 1.0 heartbeats are empty frames (8 bytes). The `idle-time-out` negotiated in Open
determines the interval. The implementation:

1. **Inbound:** Netty's `IdleStateHandler` detects read-idle. If no frame (including heartbeats)
   arrives within `idle-time-out`, close the connection with `amqp:resource-limit-exceeded`.
2. **Outbound:** A scheduled task sends empty frames at `idle-time-out / 2` intervals when
   no other frames are being written. This keeps the connection alive from the broker side.
3. **Reset on any frame:** The idle timer resets on ANY inbound frame, not just heartbeat frames.

### 16.7 MEDIUM — ProtonJ2 codec integration

The `Amqp10Codec` wraps protonj2's `ProtonBuffer` for encoding/decoding. Key patterns:

```java
// Encoding a performative into a frame:
static void encodeFrame(ByteBuf out, short channel, byte frameType, Object performative) {
    ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator()
        .allocate(INITIAL_ENCODE_CAPACITY);
    try {
        ProtonCodecFactory.getDefault().createEncoder()
            .writeObject(buffer, performative);
        byte[] body = new byte[buffer.getReadableBytes()];
        buffer.readBytes(body, 0, body.length);
        writeFrame(out, channel, frameType, body);
    } finally {
        buffer.close();
    }
}

// Decoding a performative from a frame:
static Object decodePerformative(ByteBuf frame, byte frameType) {
    byte[] bytes = new byte[frame.readableBytes()];
    frame.readBytes(bytes);
    ProtonBuffer buffer = ProtonBufferAllocator.defaultAllocator().wrap(bytes);
    try {
        Decoder decoder = (frameType == FRAME_TYPE_SASL)
            ? ProtonCodecFactory.getDefault().createSaslDecoder()
            : ProtonCodecFactory.getDefault().createDecoder();
        return decoder.readObject(buffer);
    } finally {
        buffer.close();
    }
}
```

**Buffer management.** ProtonJ2 buffers are always created in a try-with-resources scope and
closed immediately after use. The decoded performative objects (Open, Begin, etc.) are
lightweight POJOs that don't hold buffer references.

### 16.8 LOW — Metric tagging: AMQP traffic

```scala
val amqpProduceRate = metricsGroup.newMeter(
  "RequestsPerSec", "requests", TimeUnit.SECONDS,
  Map("protocol" -> "amqp", "request" -> "produce").asJava
)

val amqpConsumeRate = metricsGroup.newMeter(
  "RequestsPerSec", "requests", TimeUnit.SECONDS,
  Map("protocol" -> "amqp", "request" -> "consume").asJava
)

val amqpSettlementRate = metricsGroup.newMeter(
  "RequestsPerSec", "requests", TimeUnit.SECONDS,
  Map("protocol" -> "amqp", "request" -> "settlement").asJava
)

// Per-link credit gauge
val amqpCreditGauge = metricsGroup.newGauge(
  "LinkCredit", () -> totalLinkCredit(),
  Map("protocol" -> "amqp").asJava
)
```

---

## 17. Comparison with Existing AMQP-Kafka Bridges

### 17.1 Existing approaches

| Solution | Architecture | Limitations |
|---|---|---|
| **KoP (Kafka on Pulsar)** | Protocol handler plugin for Pulsar | Not native Kafka; requires Pulsar deployment |
| **Azure Event Hubs** | Native AMQP 1.0 on proprietary broker | Not open source; Azure-only |
| **Strimzi Kafka Bridge** | Sidecar proxy (HTTP + AMQP) | Extra hop; separate deployment; limited transaction support |
| **RabbitMQ Kafka Plugin** | Queue-level integration | Not wire-protocol compatible; RabbitMQ-specific |
| **AMQP Kafka Connect Connector** | Connect framework | Polling-based; high latency; no credit-based flow control |

### 17.2 Why native broker integration

| Concern | Sidecar/Proxy | Native (this design) |
|---|---|---|
| **Latency** | Extra network hop + serialization | Single hop to partition leader |
| **Forwarding** | Proxy must discover leaders | Broker has MetadataCache in-process |
| **Transactions** | Must proxy transaction state | Direct access to transaction coordinator |
| **Flow control** | Must buffer between AMQP and Kafka | Credit maps directly to fetch quota |
| **Auth** | Separate credential management | Same JAAS/SCRAM as binary protocol |
| **Deployment** | Additional process to operate | Single broker binary |
| **Metrics** | Separate monitoring surface | Same JMX/metrics infrastructure |

### 17.3 Azure Event Hubs comparison

Azure Event Hubs is the closest precedent for native AMQP 1.0 on a Kafka-compatible broker.
Key lessons adopted from their design:

| Event Hubs pattern | This design |
|---|---|
| Address = `<entity>` or `<entity>/Partitions/<N>` | Address = `topic[:partition]` (simpler) |
| Receiver Link filter for offset | Link property `kafka-offset` |
| Credit-based delivery | Same (AMQP 1.0 standard) |
| CBS (Claims-Based Security) tokens | SASL PLAIN/SCRAM (standard AMQP SASL) |
| Epoch-based ownership | Consumer group via `kafka-group-id` link property |

**Key difference:** Event Hubs is proprietary and cloud-only. This design is open-source,
runs on any Kafka deployment, and preserves full Kafka binary protocol compatibility.

---

*Document version: 0.1 — 2026-04-16*
*Branch: feature/http-protocol*
