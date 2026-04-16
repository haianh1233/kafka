# AMQP 0-9-1 Protocol for Apache Kafka — Design Document

## Table of Contents

1. [Overview](#1-overview)
2. [Goals & Non-Goals](#2-goals--non-goals)
3. [Architecture Overview](#3-architecture-overview)
4. [AMQP 0-9-1 Protocol Specification](#4-amqp-0-9-1-protocol-specification)
   - 4.1 Connection Lifecycle
   - 4.2 Channel Management
   - 4.3 Exchange Operations
   - 4.4 Queue Operations
   - 4.5 Binding Operations
   - 4.6 Basic.Publish (Produce)
   - 4.7 Basic.Consume / Basic.Deliver (Push Consume)
   - 4.8 Basic.Get (Pull Consume)
   - 4.9 Acknowledgements (ACK / NACK / Reject)
   - 4.10 Publisher Confirms
   - 4.11 QoS (Prefetch)
   - 4.12 Transactions
   - 4.13 Heartbeats
5. [Produce Path](#5-produce-path)
6. [Consume Path](#6-consume-path)
7. [Broker-to-Broker Internal Forwarding](#7-broker-to-broker-internal-forwarding)
8. [AMQP-to-Kafka Mapping](#8-amqp-to-kafka-mapping)
9. [Integration with Existing Kafka Infrastructure](#9-integration-with-existing-kafka-infrastructure)
10. [New Module: `amqp-server`](#10-new-module-amqp-server)
11. [Configuration](#11-configuration)
12. [Error Handling](#12-error-handling)
13. [Security](#13-security)
14. [Implementation Plan](#14-implementation-plan)
15. [Implementation Concerns](#15-implementation-concerns)
16. [Comparison with RabbitMQ](#16-comparison-with-rabbitmq)

---

## 1. Overview

This document describes the design for adding native AMQP 0-9-1 protocol support to Apache
Kafka brokers. The feature allows RabbitMQ-compatible clients to **publish** and **consume**
Kafka records using the AMQP 0-9-1 binary protocol without any application changes or
intermediary proxy.

### Key Properties

| Property | Behavior |
|---|---|
| **Publish** | AMQP client sends `Basic.Publish` to an exchange → broker routes via bindings to queues (backed by Kafka topics) → appends to partition leaders |
| **Consume** | AMQP client sends `Basic.Consume` → broker pushes records from Kafka topic partitions as `Basic.Deliver` frames, with per-message ACK tracking |
| **Protocol** | AMQP 0-9-1 binary wire protocol (8-byte header detection: `AMQP\x00\x00\x09\x01`) |
| **Integration** | Plugs into the existing `SocketServer → RequestChannel → KafkaApis` pipeline, reusing ReplicaManager, MetadataCache, and quota enforcement |
| **Forwarding** | Broker-to-broker forwarding uses the existing Kafka binary protocol (PRODUCE / FETCH API) — same mechanism as the HTTP protocol layer |
| **Compatibility** | Standard RabbitMQ client libraries (Java, Python pika, Go amqp091-go, Node amqplib) connect unmodified |

### Motivation

- Unlock the RabbitMQ ecosystem: thousands of existing applications and libraries speak AMQP 0-9-1
- Provide message-oriented semantics (exchanges, queues, bindings, ACKs) on top of Kafka's durable, replicated log
- Enable gradual migration from RabbitMQ to Kafka without rewriting producer/consumer code
- Leverage existing Kafka durability, replication, and horizontal scaling for AMQP workloads

### Relationship to the HTTP Protocol

The HTTP protocol (see `http-protocol-design.md`) provides stateless, request-response access
to Kafka. AMQP 0-9-1 is fundamentally different:

| Dimension | HTTP Protocol | AMQP 0-9-1 Protocol |
|---|---|---|
| Connection model | Stateless, per-request | Long-lived, stateful |
| Multiplexing | HTTP/2 streams (future) | AMQP channels within a connection |
| Consume model | Client polls (`POST :fetch`) | Server pushes (`Basic.Deliver`) |
| Routing | Topic name in URL path | Exchanges + bindings + routing keys |
| Acknowledgements | None (offset-based) | Per-message ACK/NACK/Reject |
| Backpressure | Client-controlled poll rate | QoS prefetch count |

Both protocols share the forwarding infrastructure (`ProduceForwardManager`,
`FetchForwardManager`) and the `RequestChannel → KafkaApis` pipeline.

---

## 2. Goals & Non-Goals

### Goals

- Full AMQP 0-9-1 connection lifecycle: protocol negotiation, SASL auth, channel management, heartbeats
- Exchange types: direct, topic, fanout, headers — with server-side routing
- Queue declaration with Kafka topic auto-creation
- Binding management: queue-to-exchange and exchange-to-exchange
- Push-based consumer delivery (`Basic.Consume` → `Basic.Deliver`) with per-message ACK
- Pull-based consumer delivery (`Basic.Get` → `Basic.GetOk`)
- Publisher confirms (`Confirm.Select` → `Basic.Ack`/`Basic.Nack`)
- QoS / prefetch (`Basic.Qos`) for consumer flow control
- AMQP transactions (`Tx.Select` / `Tx.Commit` / `Tx.Rollback`)
- Configurable via standard `listeners` / `listener.security.protocol.map`
- Wire-compatible with RabbitMQ Java client 5.x, Python pika 1.x, Go amqp091-go

### Non-Goals

- AMQP 1.0 support (different protocol, different design — future work)
- RabbitMQ management HTTP API compatibility (different concern)
- RabbitMQ plugin compatibility (Shovel, Federation, etc.)
- Queue mirroring / quorum queue semantics (Kafka's own replication replaces this)
- Replacing the Kafka binary protocol for existing Kafka clients
- RabbitMQ streams protocol (different wire protocol)

---

## 3. Architecture Overview

```
┌────────────────────────────────────────────────────────────────────────────────┐
│                              Kafka Broker                                      │
│                                                                                │
│  Port 5672 (AMQP)              Port 9094 (HTTP)          Port 9092 (PLAINTEXT) │
│  ┌──────────────────────┐     ┌──────────────────┐      ┌──────────────────┐  │
│  │     AmqpAcceptor     │     │   HttpAcceptor    │      │ DataPlaneAcceptor│  │
│  │ (Netty ServerBootstrap)    │ (Netty Bootstrap) │      │ (NIO Acceptor)   │  │
│  │                      │     │                   │      │                  │  │
│  │ ┌──────────────────┐ │     └────────┬──────────┘      └────────┬─────────┘  │
│  │ │Amqp091FrameDecoder│ │              │                          │            │
│  │ │Amqp091ConnHandler│ │              │                          │            │
│  │ │Amqp091ChanHandler│ │              │                          │            │
│  │ │Amqp091Heartbeat  │ │              │                          │            │
│  │ │Amqp091ReqHandler │ │              │                          │            │
│  │ └────────┬─────────┘ │              │                          │            │
│  └──────────┼───────────┘              │                          │            │
│             │                          │                          │            │
│             └──────────────────────────┼──────────────────────────┘            │
│                                        ▼                                       │
│                             ┌────────────────────┐                             │
│                             │   RequestChannel   │ ◄── shared, protocol-agnostic│
│                             └────────┬───────────┘                             │
│                                      │                                         │
│                             ┌────────▼───────────┐                             │
│                             │ KafkaRequestHandler │ ◄── thread pool, unchanged │
│                             │    thread pool      │                             │
│                             └────────┬───────────┘                             │
│                                      │                                         │
│                     ┌────────────────▼──────────────────────────┐              │
│                     │               KafkaApis                   │              │
│                     │  if (securityProtocol.isAmqp):            │              │
│                     │    handleAmqpProduceRequest()             │              │
│                     │    handleAmqpConsumeRequest()             │              │
│                     │  else if (securityProtocol.isHttp):       │              │
│                     │    handleHttpProduceRequest()             │              │
│                     │  else:                                    │              │
│                     │    handleProduceRequest()  (unchanged)    │              │
│                     │    handleFetchRequest()    (unchanged)    │              │
│                     └───────┬──────────────────┬────────────────┘              │
│                             │                  │                               │
│                  MetadataCache lookup     is this broker leader?               │
│                             │          YES ─────────┐                          │
│                             │                       │     NO                   │
│                             │         ┌─────────────▼──┐  ┌──────────┐        │
│                             │         │  ReplicaManager │  │ Forward  │        │
│                             │         │  appendRecords()│  │ Manager  │        │
│                             │         │  fetchMessages()│  │(§7)     │        │
│                             │         └─────────────┬──┘  └────┬─────┘        │
│                             │                       │          │               │
│                             │                       │          │               │
│  ┌──────────────────────────┼───────────────────────┼──────────┼──────────┐   │
│  │              AMQP Metadata Layer (§8)            │          │          │   │
│  │  ┌───────────────┐ ┌──────────────┐ ┌──────────┐│          │          │   │
│  │  │ExchangeManager│ │ QueueManager │ │ Binding  ││          │          │   │
│  │  │ (in-memory +  │ │ (in-memory + │ │ Manager  ││          │          │   │
│  │  │  __amqp_meta) │ │  Kafka topic)│ │          ││          │          │   │
│  │  └───────────────┘ └──────────────┘ └──────────┘│          │          │   │
│  └──────────────────────────────────────────────────┘          │          │   │
│                                                                │          │   │
└────────────────────────────────────────────────────────────────┼──────────┘   │
                                                                 │              │
                                                      binary PRODUCE/FETCH      │
                                                      → remote brokers          │
```

The AMQP acceptor runs alongside the HTTP acceptor and the binary-protocol acceptor. All
three share the **same `RequestChannel` queue and `KafkaRequestHandler` thread pool**. AMQP
publish operations map to Kafka `PRODUCE` ApiKey; consume operations map to `FETCH` ApiKey.

**Key architectural addition:** Unlike HTTP (which is stateless), AMQP requires a **metadata
layer** that tracks exchanges, queues, and bindings. This metadata is stored in an internal
Kafka topic (`__amqp_metadata`) and cached in-memory for fast routing lookups.

### Thread Model

```
                      ┌─────────────────────────────────────────────┐
  Port 5672           │  Netty Boss Thread (1)                      │
  AMQP connections ──►│  accepts TCP connections                    │
                      └───────────────────┬─────────────────────────┘
                                          │ distributes connections
                      ┌───────────────────▼─────────────────────────┐
                      │  Netty Worker Threads (num.amqp.network.threads, default=4)
                      │  • read AMQP frames off the socket          │
                      │  • Amqp091FrameDecoder parses binary frames │
                      │  • ConnectionHandler manages handshake      │
                      │  • ChannelHandler validates channel ops     │
                      │  • HeartbeatHandler sends/checks heartbeats │
                      │  • RequestHandler dispatches AMQP methods   │
                      └───────────────────┬─────────────────────────┘
                                          │
              ┌───────────────────────────┼──────────────────────────┐
              │                           │                          │
   ┌──────────▼──────────┐   ┌───────────▼──────────┐   ┌──────────▼──────────┐
   │ Publish path:       │   │ Consume path:         │   │ Metadata path:      │
   │ Multi-frame assembly│   │ ConsumerManager       │   │ Exchange/Queue/     │
   │ → exchange routing  │   │ → fetch loop          │   │ Binding ops         │
   │ → RequestChannel    │   │ → Basic.Deliver       │   │ → __amqp_metadata   │
   │   .tryEnqueue()     │   │   frame write         │   │   topic             │
   │ → KafkaApis         │   │                       │   │                     │
   └─────────────────────┘   └───────────────────────┘   └─────────────────────┘
```

The **consume path** is fundamentally different from HTTP. AMQP consumers subscribe via
`Basic.Consume` and receive records as `Basic.Deliver` frames pushed by the broker. The
`AmqpConsumerManager` runs a fetch loop per subscribed queue (backed by a Kafka topic),
converts fetched records to AMQP frames, and writes them to the Netty channel. This loop
runs on a dedicated `amqpConsumerExecutor` thread pool, separate from both Netty worker
threads and `KafkaRequestHandler` threads.

---

## 4. AMQP 0-9-1 Protocol Specification

### 4.1 Connection Lifecycle

AMQP 0-9-1 connections begin with an 8-byte protocol header followed by a multi-step
handshake. All handshake frames use channel 0.

#### 4.1.1 Protocol Detection

The first 8 bytes identify the protocol:

```
Bytes: 'A' 'M' 'Q' 'P' 0x00 0x00 0x09 0x01
       ─────────────────  ────  ────────────
       Protocol name       ID    Version 0-9-1
```

This distinguishes AMQP 0-9-1 from AMQP 1.0 (`AMQP 0x00 0x01 0x00 0x00`) at the acceptor
level, enabling future multi-version support on the same port.

#### 4.1.2 Handshake State Machine

```
Client                              Broker
  │                                   │
  │  "AMQP\x00\x00\x09\x01"         │
  │──────────────────────────────────►│  AWAITING_HEADER
  │                                   │
  │  Connection.Start                 │
  │  { versionMajor:0, minor:9,      │
  │    mechanisms:"PLAIN",            │
  │    capabilities:{                 │
  │      publisher_confirms:true,     │
  │      basic.nack:true,             │
  │      per_consumer_qos:true        │
  │    }}                             │
  │◄──────────────────────────────────│  AWAITING_START_OK
  │                                   │
  │  Connection.StartOk               │
  │  { mechanism:"PLAIN",             │
  │    response:"\0user\0pass" }      │
  │──────────────────────────────────►│
  │                                   │  → authenticate via SASL
  │  Connection.Tune                  │
  │  { channelMax:256,                │
  │    frameMax:1048576,              │
  │    heartbeat:60 }                 │
  │◄──────────────────────────────────│  AWAITING_TUNE_OK
  │                                   │
  │  Connection.TuneOk                │
  │  { channelMax:256,                │
  │    frameMax:1048576,              │
  │    heartbeat:60 }                 │
  │──────────────────────────────────►│  AWAITING_OPEN
  │                                   │
  │  Connection.Open                  │
  │  { virtualHost:"/" }             │
  │──────────────────────────────────►│
  │                                   │  → validate vhost (maps to namespace, §8.2)
  │  Connection.OpenOk                │
  │◄──────────────────────────────────│  OPEN
  │                                   │
  │  ← connection ready for           │
  │    channel operations →            │
```

**States:**

| State | Trigger | Action |
|---|---|---|
| `AWAITING_HEADER` | Recv 8-byte header | Validate `AMQP\x00\x00\x09\x01`; send `Connection.Start` |
| `AWAITING_START_OK` | Recv `Connection.StartOk` | Authenticate via SASL PLAIN; send `Connection.Tune` |
| `AWAITING_TUNE_OK` | Recv `Connection.TuneOk` | Store negotiated parameters |
| `AWAITING_OPEN` | Recv `Connection.Open` | Validate vhost; send `Connection.OpenOk` |
| `OPEN` | All frames | Dispatch to channel handlers |

**Negotiated Parameters:**

| Parameter | Server Default | Client Negotiation | Final Value |
|---|---|---|---|
| `channelMax` | `256` | `min(server, client)` or server if client sends 0 | Controls max concurrent channels |
| `frameMax` | `1048576` (1 MB) | `min(server, client)` or server if client sends 0 | Max frame payload size |
| `heartbeat` | `60` (seconds) | `min(server, client)` or server if client sends 0 | Heartbeat interval; 0 = disabled |

#### 4.1.3 Connection Close

Either side may initiate `Connection.Close`:

```
Initiator                              Responder
  │  Connection.Close                    │
  │  { replyCode:200,                    │
  │    replyText:"Normal shutdown",      │
  │    classId:0, methodId:0 }           │
  │──────────────────────────────────────►│
  │                                       │
  │  Connection.CloseOk                   │
  │◄──────────────────────────────────────│
  │                                       │
  ├── TCP close ──────────────────────────┤
```

**Error close reply codes:**

| Code | Name | Meaning |
|---|---|---|
| 200 | `REPLY_SUCCESS` | Normal shutdown |
| 311 | `CONTENT_TOO_LARGE` | Frame exceeds `frameMax` |
| 320 | `CONNECTION_FORCED` | Server-initiated (e.g. admin action) |
| 402 | `INVALID_PATH` | Unknown vhost |
| 403 | `ACCESS_REFUSED` | Authentication failed |
| 501 | `FRAME_ERROR` | Malformed frame |
| 502 | `SYNTAX_ERROR` | Invalid method arguments |
| 503 | `COMMAND_INVALID` | Method not allowed in current state |
| 504 | `CHANNEL_ERROR` | Invalid channel number |
| 505 | `UNEXPECTED_FRAME` | Wrong frame type for current state |
| 506 | `RESOURCE_ERROR` | Out of resources |
| 530 | `NOT_ALLOWED` | Operation not permitted |
| 540 | `NOT_IMPLEMENTED` | Method not implemented |
| 541 | `INTERNAL_ERROR` | Internal broker error |

---

### 4.2 Channel Management

Channels multiplex independent conversations over a single TCP connection. Channel 0 is
reserved for connection-level methods.

```
Connection (TCP socket)
├── Channel 0:  Connection.* methods only
├── Channel 1:  Exchange/Queue/Basic operations (independent state)
├── Channel 2:  Exchange/Queue/Basic operations (independent state)
└── Channel N:  (up to channelMax)
```

#### 4.2.1 Channel.Open / Channel.OpenOk

```
Client                          Broker
  │  Channel.Open (channel=1)    │
  │─────────────────────────────►│
  │  Channel.OpenOk              │
  │◄─────────────────────────────│
```

Each channel tracks independently:
- Active consumers (`consumerTag → subscription`)
- Pending acknowledgements (`deliveryTag → record offset`)
- QoS prefetch count
- Transaction state (if `Tx.Select` was called)
- Confirm mode state (if `Confirm.Select` was called)
- Unconfirmed publish sequence numbers

#### 4.2.2 Channel.Close / Channel.CloseOk

Closing a channel cancels all consumers on that channel and rolls back any pending
transaction. Unacknowledged messages are requeued.

```
Initiator                          Responder
  │  Channel.Close (channel=N)      │
  │  { replyCode:200,               │
  │    replyText:"Normal close" }   │
  │──────────────────────────────────►│
  │  Channel.CloseOk (channel=N)    │
  │◄──────────────────────────────────│
```

#### 4.2.3 Channel.Flow

Used for consumer-side flow control:

```
Client                          Broker
  │  Channel.Flow { active:false } │  ← pause delivery
  │────────────────────────────────►│
  │  Channel.FlowOk { active:false}│
  │◄────────────────────────────────│
  │                                 │
  │  Channel.Flow { active:true }  │  ← resume delivery
  │────────────────────────────────►│
  │  Channel.FlowOk { active:true} │
  │◄────────────────────────────────│
```

---

### 4.3 Exchange Operations

Exchanges are the routing layer. A published message goes to an exchange, which routes it
to zero or more queues based on the exchange type, the routing key, and the bindings.

#### 4.3.1 Exchange.Declare

```
Client                                         Broker
  │  Exchange.Declare {                          │
  │    exchange:"logs",                          │
  │    type:"topic",                             │
  │    durable:true,                             │
  │    autoDelete:false,                         │
  │    internal:false,                           │
  │    arguments:{} }                            │
  │──────────────────────────────────────────────►│
  │                                               │  → store in ExchangeManager (§8.1)
  │  Exchange.DeclareOk                           │  → persist to __amqp_metadata
  │◄──────────────────────────────────────────────│
```

**Exchange types:**

| Type | Routing Algorithm |
|---|---|
| `direct` | Exact match: `binding.routingKey == message.routingKey` |
| `topic` | Wildcard match: `.` separates words, `*` matches one word, `#` matches zero or more |
| `fanout` | Deliver to all bound queues (routing key ignored) |
| `headers` | Match message headers against binding arguments (`x-match: all\|any`) |

**Pre-declared exchanges (not deletable):**

| Name | Type | Purpose |
|---|---|---|
| `""` (empty string) | direct | Default exchange — routing key = queue name |
| `amq.direct` | direct | Standard direct exchange |
| `amq.topic` | topic | Standard topic exchange |
| `amq.fanout` | fanout | Standard fanout exchange |
| `amq.headers` | headers | Standard headers exchange |

#### 4.3.2 Exchange.Delete

Deletes an exchange. Fails if it has active bindings and `if-unused` is set.

#### 4.3.3 Exchange.Bind / Exchange.Unbind (Exchange-to-Exchange)

Allows chaining exchanges. Messages entering the source exchange that match the binding
are also routed to the destination exchange:

```
Client                                    Broker
  │  Exchange.Bind {                       │
  │    destination:"errors",               │
  │    source:"logs",                      │
  │    routingKey:"*.error" }              │
  │────────────────────────────────────────►│
  │  Exchange.BindOk                       │
  │◄────────────────────────────────────────│
```

The routing engine performs recursive routing with a **visited set** to prevent infinite
loops when exchanges form a cycle.

---

### 4.4 Queue Operations

#### 4.4.1 Queue.Declare

```
Client                                         Broker
  │  Queue.Declare {                             │
  │    queue:"order-events",                     │
  │    durable:true,                             │
  │    exclusive:false,                          │
  │    autoDelete:false,                         │
  │    arguments:{                               │
  │      "x-max-length": 1000000,               │
  │      "x-message-ttl": 86400000,             │
  │      "x-dead-letter-exchange": "dlx",       │
  │      "x-dead-letter-routing-key": "dead"    │
  │    }}                                        │
  │──────────────────────────────────────────────►│
  │                                               │  → create Kafka topic (§8.3)
  │  Queue.DeclareOk {                            │  → persist to __amqp_metadata
  │    queue:"order-events",                      │
  │    messageCount:0,                            │
  │    consumerCount:0 }                          │
  │◄──────────────────────────────────────────────│
```

**Queue arguments:**

| Argument | Type | Default | Kafka Mapping |
|---|---|---|---|
| `x-max-length` | int | unlimited | Cleanup policy with max records (§8.3) |
| `x-max-length-bytes` | int | unlimited | Topic `retention.bytes` |
| `x-message-ttl` | int (ms) | unlimited | Topic `retention.ms` |
| `x-max-priority` | int (0-255) | 0 | Separate partition ranges per priority (§15.8) |
| `x-dead-letter-exchange` | string | none | Dead-letter topic routing (§15.7) |
| `x-dead-letter-routing-key` | string | none | DLX routing key override |
| `x-overflow` | string | "drop-head" | `cleanup.policy=delete` (drop-head) or reject-publish |
| `x-queue-type` | string | "classic" | Ignored (all queues backed by Kafka log) |
| `x-expires` | int (ms) | none | Auto-delete topic after idle period |

**Server-generated queue names:** If `queue=""` (empty string), the broker generates a
unique name: `amq.gen-<UUID>`. This is used for temporary/exclusive queues.

#### 4.4.2 Queue.Bind

```
Client                                    Broker
  │  Queue.Bind {                          │
  │    queue:"order-events",               │
  │    exchange:"orders",                  │
  │    routingKey:"order.created",         │
  │    arguments:{} }                      │
  │────────────────────────────────────────►│
  │                                         │  → store binding in BindingManager
  │  Queue.BindOk                           │  → persist to __amqp_metadata
  │◄────────────────────────────────────────│
```

#### 4.4.3 Queue.Unbind

Removes a specific binding between a queue and an exchange.

#### 4.4.4 Queue.Delete

Deletes a queue. If `if-unused` is true, fails when consumers are active. If `if-empty` is
true, fails when messages exist.

#### 4.4.5 Queue.Purge

Removes all messages from a queue. Returns the number of messages purged.

---

### 4.5 Binding Operations

Bindings connect exchanges to queues (or exchanges to exchanges). The binding key is the
tuple `(exchange, queue, routingKey, arguments)`.

**Idempotency:** Declaring the same binding twice is a no-op (returns `BindOk`).

**Default exchange binding:** Every queue is implicitly bound to the default exchange (`""`)
with a routing key equal to the queue name. This binding cannot be removed.

---

### 4.6 Basic.Publish (Produce)

Publishing is a multi-frame operation:

```
Client                                    Broker
  │                                        │
  │  [Frame 1] Basic.Publish method        │  class=60, method=40, channel=N
  │  { exchange:"orders",                  │
  │    routingKey:"order.created",         │
  │    mandatory:false,                    │
  │    immediate:false }                   │  immediate is deprecated, always false
  │────────────────────────────────────────►│
  │                                         │
  │  [Frame 2] Content header              │  type=2, channel=N
  │  { classId:60, bodySize:1024,          │
  │    properties: {                       │
  │      contentType:"application/json",   │
  │      deliveryMode:2,                   │
  │      priority:0,                       │
  │      correlationId:"req-123",          │
  │      replyTo:"replies",               │
  │      expiration:"60000",              │
  │      messageId:"msg-abc",             │
  │      timestamp:1713260400,            │
  │      type:"OrderCreated",             │
  │      appId:"checkout-service"         │
  │    }}                                  │
  │────────────────────────────────────────►│
  │                                         │
  │  [Frame 3..N] Content body             │  type=3, channel=N
  │  { <raw bytes, up to frameMax-8> }     │  may span multiple frames
  │────────────────────────────────────────►│
  │                                         │
  │  (no response unless confirm mode)      │  → route via exchange → Kafka produce
```

#### Frame wire format

```
 0      1       3         7       7+size    7+size+1
 ┌──────┬───────┬─────────┬───────────────┬──────┐
 │ type │channel│  size   │   payload     │ 0xCE │
 │  (1) │  (2)  │  (4)    │   (size)      │  (1) │
 └──────┴───────┴─────────┴───────────────┴──────┘
```

- `type`: 1=METHOD, 2=HEADER, 3=BODY, 8=HEARTBEAT
- `channel`: 0 for connection methods, 1..N for channel methods
- `size`: payload byte count (0 for heartbeats)
- `0xCE`: frame-end marker (validates framing integrity)

#### Content property flags

Properties are marshaled with a 2-byte flags word where each bit indicates presence:

```
Bit 15 (0x8000): content-type      (shortstr)
Bit 14 (0x4000): content-encoding  (shortstr)
Bit 13 (0x2000): headers           (table)
Bit 12 (0x1000): delivery-mode     (octet: 1=non-persistent, 2=persistent)
Bit 11 (0x0800): priority          (octet: 0-9)
Bit 10 (0x0400): correlation-id    (shortstr)
Bit  9 (0x0200): reply-to          (shortstr)
Bit  8 (0x0100): expiration        (shortstr, milliseconds as string)
Bit  7 (0x0080): message-id        (shortstr)
Bit  6 (0x0040): timestamp         (long, unix epoch seconds)
Bit  5 (0x0020): type              (shortstr)
Bit  4 (0x0010): user-id           (shortstr)
Bit  3 (0x0008): app-id            (shortstr)
Bit  0 (0x0001): continuation      (more flag words follow)
```

#### Multi-frame assembly state machine

```
State: IDLE
  │
  ├── Recv METHOD frame (class=60, method=40) → Basic.Publish
  │   Parse: exchange, routingKey, mandatory, immediate
  │   Transition: AWAITING_HEADER
  │
State: AWAITING_HEADER
  │
  ├── Recv HEADER frame (type=2)
  │   Parse: bodySize, propertyFlags, properties
  │   if bodySize == 0:
  │     → route message (no body frames)
  │     Transition: IDLE
  │   else:
  │     allocate CompositeByteBuf for body
  │     receivedBodySize = 0
  │     Transition: AWAITING_BODY
  │
State: AWAITING_BODY
  │
  ├── Recv BODY frame (type=3)
  │   Append payload to CompositeByteBuf
  │   receivedBodySize += frame.payload.readableBytes()
  │   if receivedBodySize >= expectedBodySize:
  │     → route message (body complete)
  │     Transition: IDLE
  │   else:
  │     Transition: AWAITING_BODY (continue)
```

The `CompositeByteBuf` provides zero-copy body assembly — body frame payloads are retained
slices, not copied.

#### Mandatory message handling

When `mandatory=true` and the exchange routing finds **no matching queues**, the broker must
return the message to the publisher:

```
Broker                              Client
  │  Basic.Return {                   │
  │    replyCode:312,                 │
  │    replyText:"NO_ROUTE",          │
  │    exchange:"orders",             │
  │    routingKey:"order.xxx" }       │
  │───────────────────────────────────►│
  │                                    │
  │  Content header (original)         │
  │───────────────────────────────────►│
  │                                    │
  │  Content body (original)           │
  │───────────────────────────────────►│
```

---

### 4.7 Basic.Consume / Basic.Deliver (Push Consume)

This is the primary consumption model. The client subscribes to a queue and receives
messages pushed by the broker.

#### 4.7.1 Subscribe

```
Client                                    Broker
  │  Basic.Consume {                       │
  │    queue:"order-events",               │
  │    consumerTag:"consumer-1",           │
  │    noAck:false,                        │
  │    exclusive:false,                    │
  │    arguments:{} }                      │
  │────────────────────────────────────────►│
  │                                         │  → register consumer in ConsumerManager
  │  Basic.ConsumeOk {                      │  → start fetch loop for backing topic
  │    consumerTag:"consumer-1" }           │
  │◄────────────────────────────────────────│
```

#### 4.7.2 Message delivery

```
Broker                                    Client
  │  Basic.Deliver {                       │
  │    consumerTag:"consumer-1",           │
  │    deliveryTag:1,                      │  ← monotonic per-channel, used for ACK
  │    redelivered:false,                  │
  │    exchange:"orders",                  │
  │    routingKey:"order.created" }        │
  │────────────────────────────────────────►│
  │                                         │
  │  Content header                         │
  │────────────────────────────────────────►│
  │                                         │
  │  Content body                           │
  │────────────────────────────────────────►│
```

**`deliveryTag`** is a monotonically increasing 64-bit integer per channel. The client uses
it in `Basic.Ack`, `Basic.Nack`, or `Basic.Reject` to acknowledge specific messages.

The broker tracks the mapping: `deliveryTag → (topicPartition, offset)` per channel.

#### 4.7.3 Cancel subscription

```
Client                                    Broker
  │  Basic.Cancel {                        │
  │    consumerTag:"consumer-1" }          │
  │────────────────────────────────────────►│
  │                                         │  → stop fetch loop
  │  Basic.CancelOk {                       │  → commit outstanding offsets
  │    consumerTag:"consumer-1" }           │
  │◄────────────────────────────────────────│
```

---

### 4.8 Basic.Get (Pull Consume)

Single-message pull (synchronous fetch):

```
Client                                    Broker
  │  Basic.Get {                           │
  │    queue:"order-events",               │
  │    noAck:false }                       │
  │────────────────────────────────────────►│
  │                                         │
  │ (if messages available)                 │
  │  Basic.GetOk {                          │
  │    deliveryTag:42,                      │
  │    redelivered:false,                   │
  │    exchange:"orders",                   │
  │    routingKey:"order.created",          │
  │    messageCount:153 }                   │
  │◄────────────────────────────────────────│
  │  Content header + body                  │
  │◄────────────────────────────────────────│
  │                                         │
  │ (if no messages)                        │
  │  Basic.GetEmpty                         │
  │◄────────────────────────────────────────│
```

`Basic.Get` maps to a single-message `FetchRequest`. This is less efficient than
`Basic.Consume` for sustained consumption because each get is a full request-response
round trip.

---

### 4.9 Acknowledgements (ACK / NACK / Reject)

#### 4.9.1 Basic.Ack

```
Client                                    Broker
  │  Basic.Ack {                           │
  │    deliveryTag:5,                      │
  │    multiple:true }                     │  ← ack all messages up to and including tag 5
  │────────────────────────────────────────►│
  │                                         │  → commit offsets for tags 1..5
```

- `multiple=false`: acknowledge exactly one message
- `multiple=true`: acknowledge all messages up to and including `deliveryTag`

**Offset commit mapping:** When `Basic.Ack` is received, the broker resolves the delivery
tags to `(topicPartition, offset)` pairs and commits offsets to `__consumer_offsets` via
`GroupCoordinator.commitOffsets()`. The consumer group is
`amqp-{vhost}-{queue}-{connectionId}` (see §8.5).

#### 4.9.2 Basic.Nack (RabbitMQ extension)

```
Client                                    Broker
  │  Basic.Nack {                          │
  │    deliveryTag:5,                      │
  │    multiple:false,                     │
  │    requeue:true }                      │
  │────────────────────────────────────────►│
  │                                         │  if requeue: redeliver later (offset not committed)
  │                                         │  if !requeue: dead-letter (if DLX configured) or drop
```

#### 4.9.3 Basic.Reject

Same as `Basic.Nack` with `multiple=false`:

```
Client                                    Broker
  │  Basic.Reject {                        │
  │    deliveryTag:5,                      │
  │    requeue:false }                     │
  │────────────────────────────────────────►│
  │                                         │  → dead-letter or drop
```

---

### 4.10 Publisher Confirms

When confirm mode is enabled, the broker acknowledges each published message after it has
been durably written to the Kafka log.

#### 4.10.1 Enable confirm mode

```
Client                                    Broker
  │  Confirm.Select {                      │
  │    noWait:false }                      │
  │────────────────────────────────────────►│
  │  Confirm.SelectOk                      │  → channel enters confirm mode
  │◄────────────────────────────────────────│
```

#### 4.10.2 Confirm delivery

After each `Basic.Publish`, the broker sends `Basic.Ack` or `Basic.Nack` on the same channel:

```
Client                                    Broker
  │  Basic.Publish (seqNo=1)               │
  │────────────────────────────────────────►│
  │  Basic.Publish (seqNo=2)               │
  │────────────────────────────────────────►│
  │  Basic.Publish (seqNo=3)               │
  │────────────────────────────────────────►│
  │                                         │
  │  Basic.Ack { deliveryTag:2,            │  ← confirms 1 and 2 (multiple=true)
  │              multiple:true }            │
  │◄────────────────────────────────────────│
  │                                         │
  │  Basic.Nack { deliveryTag:3,           │  ← publish 3 failed (e.g. ISR below min)
  │               multiple:false }          │
  │◄────────────────────────────────────────│
```

**Mapping to Kafka:** The `CompletableFuture` returned by `ProduceForwardManager.forward()`
or `ReplicaManager.appendRecords()` resolves with the result for each partition. On success,
the broker sends `Basic.Ack` with the publish sequence number. On failure, it sends
`Basic.Nack`. Multiple sequential successes can be batched with `multiple=true`.

The publish sequence number is a per-channel monotonic counter starting at 1, incremented
for each `Basic.Publish` received.

---

### 4.11 QoS (Prefetch)

Controls how many unacknowledged messages the broker delivers to a consumer:

```
Client                                    Broker
  │  Basic.Qos {                           │
  │    prefetchSize:0,                     │  ← 0 means no size limit
  │    prefetchCount:100,                  │  ← max 100 unacked messages
  │    global:false }                      │  ← per-consumer (false) or per-channel (true)
  │────────────────────────────────────────►│
  │  Basic.QosOk                           │
  │◄────────────────────────────────────────│
```

**Implementation:** The consumer fetch loop (§6) pauses when the number of outstanding
(delivered but not yet ACK'd) messages reaches `prefetchCount`. When ACKs arrive, the
outstanding count decreases and the fetch loop resumes. This provides natural backpressure
without requiring `Channel.Flow`.

---

### 4.12 Transactions

AMQP transactions group publish and ack operations into atomic units:

```
Client                                    Broker
  │  Tx.Select                             │
  │────────────────────────────────────────►│
  │  Tx.SelectOk                           │  → channel enters transaction mode
  │◄────────────────────────────────────────│
  │                                         │
  │  Basic.Publish (buffered, not written)  │
  │────────────────────────────────────────►│
  │  Basic.Publish (buffered)               │
  │────────────────────────────────────────►│
  │  Basic.Ack deliveryTag:5 (buffered)     │
  │────────────────────────────────────────►│
  │                                         │
  │  Tx.Commit                              │
  │────────────────────────────────────────►│  → flush: produce all buffered messages
  │                                         │  → commit all buffered acks
  │  Tx.CommitOk                            │
  │◄────────────────────────────────────────│
  │                                         │
  │  Tx.Rollback                            │  (alternative: discard all buffered)
  │────────────────────────────────────────►│
  │  Tx.RollbackOk                          │
  │◄────────────────────────────────────────│
```

**Kafka mapping:** `Tx.Commit` translates to a Kafka transactional produce
(`initTransactions()` → `beginTransaction()` → `send()` → `commitTransaction()`). This
gives exactly-once semantics for the buffered publishes.

**Limitation:** Kafka transactions and AMQP transactions have different scope. AMQP
transactions are per-channel; Kafka transactions are per-transactional-id. The broker
assigns a transactional id per channel: `amqp-tx-{connectionId}-{channelNum}`.

**Phase 4 deliverable.** Full transaction support requires deep integration with Kafka's
transaction coordinator. Phase 1–3 can implement transactions as buffered batches
(all-or-nothing produce) without the Kafka transactional producer — this gives atomicity
of publish but not exactly-once semantics.

---

### 4.13 Heartbeats

Heartbeats detect dead connections:

```
 0      1       3         7       8
 ┌──────┬───────┬─────────┬──────┐
 │  8   │ 0x0000│0x00000000│ 0xCE │   ← heartbeat frame (8 bytes total)
 │(type)│(chan=0)│ (size=0) │(end) │
 └──────┴───────┴─────────┴──────┘
```

**Rules:**
- Interval negotiated during `Connection.Tune` (default 60s, configurable)
- **WRITER_IDLE**: if no frame sent within `heartbeat` seconds → send heartbeat
- **READER_IDLE**: if no frame received within `heartbeat` seconds → increment missed counter
- **Connection close**: after **3 consecutive** missed reader-idle events → close connection
- **Any frame resets the reader-idle counter** (not just heartbeats)
- Heartbeat interval of 0 = disabled (not recommended for production)

**Netty integration:** Use `IdleStateHandler(readerIdleTime=heartbeat, writerIdleTime=heartbeat)`:
- `WRITER_IDLE` event → write heartbeat frame
- `READER_IDLE` event → increment missed counter; close if ≥ 3

---

## 5. Produce Path

### 5.1 End-to-end: `Basic.Publish` → Kafka log

```
AMQP Client          Broker 3                                  Broker 1
     │                  │                                          │
     │  Basic.Publish   │                                          │
     │  {exchange:"orders", routingKey:"order.created"}            │
     │  Content header: bodySize=512, deliveryMode=2               │
     │  Content body: <512 bytes JSON payload>                     │
     │──────────────────►│                                         │
     │                   │                                         │
     │             ┌─────▼──────────────────────────────────┐      │
     │             │  Amqp091RequestHandler                  │      │
     │             │                                         │      │
     │             │  1. Multi-frame assembly:                │      │
     │             │     METHOD → HEADER → BODY (complete)   │      │
     │             │                                         │      │
     │             │  2. Exchange routing:                    │      │
     │             │     exchange="orders" (type=direct)      │      │
     │             │     routingKey="order.created"           │      │
     │             │     → bindings lookup:                   │      │
     │             │       queue="order-events" matches       │      │
     │             │                                         │      │
     │             │  3. Queue → Kafka topic resolution:      │      │
     │             │     queue="order-events"                 │      │
     │             │     → topic="amqp.order-events"         │      │
     │             │     → partition = murmur2(routingKey)    │      │
     │             │       % partitionCount                   │      │
     │             │                                         │      │
     │             │  4. Build ProduceRequest:                │      │
     │             │     topic="amqp.order-events"           │      │
     │             │     key=routingKey bytes                 │      │
     │             │     value=body bytes                     │      │
     │             │     headers=AMQP properties (§8.4)      │      │
     │             │                                         │      │
     │             │  5. MetadataCache: leader(P0) = Broker 1│      │
     │             │     → forward to Broker 1               │      │
     │             └─────┬──────────────────────────────────┘      │
     │                   │                                         │
     │             ProduceForwardManager.forward(leaderId=1, ...)   │
     │                   │                                         │
     │                   │  binary ProduceRequest                  │
     │                   │  { topic:"amqp.order-events",           │
     │                   │    partition:0, records:[...] }          │
     │                   │─────────────────────────────────────────►│
     │                   │                                         │
     │                   │  ProduceResponse { offset:42 }          │
     │                   │◄─────────────────────────────────────────│
     │                   │                                         │
     │  (if confirm mode)│                                         │
     │  Basic.Ack {      │                                         │
     │    deliveryTag:1, │                                         │
     │    multiple:false}│                                         │
     │◄──────────────────│                                         │
```

### 5.2 Exchange routing: multi-queue fanout

When an exchange routes to multiple queues (e.g., fanout or topic with multiple bindings),
the message must be produced to multiple Kafka topics. The broker fans out:

```
AMQP Client          Broker 3
     │                  │
     │  Basic.Publish   │
     │  {exchange:"events", routingKey:"order.created"}
     │──────────────────►│
     │                   │
     │             Exchange "events" (type=topic):
     │             Bindings:
     │               queue="orders"   routing_key="order.*"    → match ✓
     │               queue="audit"    routing_key="#"           → match ✓
     │               queue="shipping" routing_key="shipping.*"  → no match
     │                   │
     │             Fan-out produce to 2 topics:
     │               topic="amqp.orders"  → leader lookup → produce
     │               topic="amqp.audit"   → leader lookup → produce
     │                   │
     │             CompletableFuture.allOf(future_orders, future_audit)
     │                   │
     │  (if confirm mode and all succeed)
     │  Basic.Ack { deliveryTag:1 }
     │◄──────────────────│
     │                   │
     │  (if confirm mode and any fail)
     │  Basic.Nack { deliveryTag:1 }
     │◄──────────────────│
```

**Atomicity concern:** Fanout to multiple topics is not atomic — one topic's produce may
succeed while another fails. In confirm mode, `Basic.Nack` is returned if **any** target
fails. The client must republish. For topics that succeeded, the message is duplicated on
retry. This is acceptable for at-least-once semantics. Exactly-once fanout requires Kafka
transactions (phase 4, §4.12).

### 5.3 Partition assignment for AMQP publishes

AMQP does not have a partition concept. The broker assigns partitions:

```
topicPartitionCount = MetadataCache.getTopicMetadata(topic).partitionCount

if message.routingKey != null && !message.routingKey.isEmpty():
    partition = (murmur2(routingKey.getBytes(UTF_8)) & 0x7fffffff) % topicPartitionCount
else:
    partition = (stickyCounter.getAndIncrement() & 0x7fffffff) % topicPartitionCount
```

Using the routing key as the partition key gives locality: all messages with the same
routing key land on the same partition, preserving ordering per routing key.

### 5.4 Record format: AMQP properties → Kafka headers

AMQP content properties are preserved as Kafka record headers so they survive round-trip
through Kafka and can be consumed by AMQP clients:

| Kafka Header Key | Source | Format |
|---|---|---|
| `_amqp_exchange` | `Basic.Publish.exchange` | UTF-8 string |
| `_amqp_routing_key` | `Basic.Publish.routingKey` | UTF-8 string |
| `_amqp_properties` | Serialized AMQP property bytes | Raw bytes (compact, preserves all 13 fields) |
| `_amqp_mandatory` | `Basic.Publish.mandatory` | `0x01` or `0x00` |
| `_content-type` | `properties.contentType` | UTF-8 string (also used by HTTP serializer §14.7) |
| `_amqp_headers` | `properties.headers` (application table) | Serialized AMQP table bytes |

**Why raw bytes for `_amqp_properties`?** The 13 AMQP properties include types (octet,
shortstr, longstr, timestamp) that don't map cleanly to Kafka header strings. Preserving the
wire-format bytes ensures lossless round-trip for AMQP-to-AMQP flows. Non-AMQP consumers
can ignore these headers or use a provided deserializer utility class.

---

## 6. Consume Path

### 6.1 Consumer lifecycle

```
AMQP Client          Broker (AmqpConsumerManager)              Kafka Log
     │                  │                                          │
     │  Basic.Consume   │                                          │
     │  {queue:"orders",│                                          │
     │   consumerTag:   │                                          │
     │   "consumer-1",  │                                          │
     │   noAck:false}   │                                          │
     │──────────────────►│                                         │
     │                   │                                         │
     │             ┌─────▼──────────────────────────────────┐      │
     │             │  1. Resolve queue → topic:              │      │
     │             │     queue="orders"                      │      │
     │             │     → topic="amqp.orders"              │      │
     │             │                                         │      │
     │             │  2. Assign partitions:                   │      │
     │             │     If exclusive: all partitions          │      │
     │             │     If shared: use GroupCoordinator       │      │
     │             │     (consumer group = amqp-orders-{id}) │      │
     │             │                                         │      │
     │             │  3. Resolve start offset:                │      │
     │             │     Fetch from __consumer_offsets        │      │
     │             │     If none: start from earliest/latest  │      │
     │             │     (configurable, default=latest)       │      │
     │             │                                         │      │
     │             │  4. Start fetch loop on                  │      │
     │             │     amqpConsumerExecutor thread pool     │      │
     │             └─────┬──────────────────────────────────┘      │
     │                   │                                         │
     │  Basic.ConsumeOk  │                                         │
     │  {consumerTag:    │                                         │
     │   "consumer-1"}   │                                         │
     │◄──────────────────│                                         │
     │                   │                                         │
     │             ┌─────▼──────────────────────────────────┐      │
     │             │  Fetch loop (runs on consumer executor) │      │
     │             │                                         │      │
     │             │  while (active && !flowPaused):          │      │
     │             │    if (outstandingCount >= prefetch):    │      │
     │             │      wait for ACK signal                │      │
     │             │    FetchRequest → RequestChannel         │      │
     │             │    ← FetchResponse with records          │◄────│
     │             │    for each record:                      │      │
     │             │      deliveryTag = nextTag++             │      │
     │             │      pendingAcks[tag] = (tp, offset)    │      │
     │             │      outstandingCount++                  │      │
     │             │      write Basic.Deliver frame           │      │
     │             └─────┬──────────────────────────────────┘      │
     │                   │                                         │
     │  Basic.Deliver    │                                         │
     │  {consumerTag:    │                                         │
     │   "consumer-1",   │                                         │
     │   deliveryTag:1,  │                                         │
     │   exchange:"",    │                                         │
     │   routingKey:     │                                         │
     │   "orders"}       │                                         │
     │  Content header   │                                         │
     │  Content body     │                                         │
     │◄──────────────────│                                         │
     │                   │                                         │
     │  Basic.Ack {      │                                         │
     │   deliveryTag:1}  │                                         │
     │──────────────────►│                                         │
     │                   │  → commit offset to __consumer_offsets   │
     │                   │  → outstandingCount--                   │
     │                   │  → signal fetch loop to resume          │
```

### 6.2 Fetch loop internals

The consumer fetch loop runs on the `amqpConsumerExecutor` thread pool (separate from
Netty workers and `KafkaRequestHandler` threads). Each consumer subscription gets a
dedicated fetch task:

```java
// Simplified pseudo-code for AmqpConsumerFetchLoop

while (active && !flowPaused) {
    // 1. Respect prefetch limit
    while (outstandingCount.get() >= prefetchCount) {
        ackSignal.await(100, MILLISECONDS);  // wake on ACK or timeout
        if (!active) return;
    }

    // 2. Calculate fetch budget
    int fetchBudget = prefetchCount - outstandingCount.get();

    // 3. Build FetchRequest for assigned partitions
    FetchRequest fetchRequest = buildFetchRequest(
        assignedPartitions, currentOffsets,
        maxWaitMs = 500,    // short wait to stay responsive to cancel/flow
        maxBytes  = amqpConsumeMaxBytes,
        maxRecords = fetchBudget
    );

    // 4. Route through RequestChannel (reuses KafkaApis.handleAmqpConsumeRequest)
    CompletableFuture<FetchResponse> future = submitFetch(fetchRequest);
    FetchResponse response = future.get(fetchTimeoutMs, MILLISECONDS);

    // 5. Deliver records as AMQP frames
    for (PartitionData pd : response.partitions()) {
        for (Record record : pd.records()) {
            long deliveryTag = nextDeliveryTag.getAndIncrement();
            pendingAcks.put(deliveryTag, new PendingAck(pd.partition(), record.offset()));
            outstandingCount.incrementAndGet();

            // Convert Kafka record → AMQP frames and write to Netty channel
            writeBasicDeliver(channel, consumerTag, deliveryTag, record);
        }
        currentOffsets.put(pd.partition(), pd.lastOffset() + 1);
    }
}
```

### 6.3 Record conversion: Kafka → AMQP

When delivering a Kafka record as an AMQP `Basic.Deliver`, the broker converts:

```
Kafka Record                          AMQP Frames
─────────────                         ───────────
key bytes                             → routingKey (if _amqp_routing_key header absent)
value bytes                           → Content body
headers["_amqp_exchange"]             → Basic.Deliver.exchange
headers["_amqp_routing_key"]          → Basic.Deliver.routingKey
headers["_amqp_properties"]           → Content header properties (verbatim)
headers["_content-type"]              → properties.contentType (if no _amqp_properties)
timestamp                             → properties.timestamp
```

**Round-trip fidelity:** Messages published via AMQP and consumed via AMQP preserve all 13
content properties exactly (via the `_amqp_properties` header). Messages published via Kafka
binary protocol or HTTP and consumed via AMQP get synthesized properties from available
Kafka headers.

### 6.4 Multiple consumers on the same queue (competing consumers)

When multiple AMQP connections consume from the same queue, they form a **consumer group**.
The Kafka `GroupCoordinator` handles partition assignment:

```
Consumer A (connection 1)              Broker              Consumer B (connection 2)
     │                                   │                        │
     │  Basic.Consume queue="orders"     │                        │
     │──────────────────────────────────►│                        │
     │                                   │  Basic.Consume queue="orders"
     │                                   │◄────────────────────────│
     │                                   │                        │
     │  GroupCoordinator:                │                        │
     │  group="amqp.orders"             │                        │
     │  Consumer A → P0, P1              │                        │
     │  Consumer B → P2, P3              │                        │
     │                                   │                        │
     │  Basic.Deliver (from P0, P1)     │  Basic.Deliver (from P2, P3)
     │◄──────────────────────────────────│────────────────────────►│
```

The consumer group name is derived from the queue name: `amqp.{vhost}.{queueName}`.
Multiple consumers on the same queue share partitions via Kafka's standard rebalancing
protocol (`ConsumerGroupHeartbeat` in KRaft mode).

### 6.5 Exclusive consumers

When `Basic.Consume` sets `exclusive=true`, only one consumer can be active on that queue
at a time. The broker enforces this:

- If no consumer exists: register and assign all partitions
- If a consumer already exists: return `Channel.Close` with reply code 403 (`ACCESS_REFUSED`)

Exclusive consumers do not use the `GroupCoordinator` — they directly fetch from all
partitions of the backing topic.

---

## 7. Broker-to-Broker Internal Forwarding

AMQP forwarding reuses the same `ProduceForwardManager` and `FetchForwardManager`
infrastructure as the HTTP protocol. The forwarding path is identical because both protocols
ultimately produce `ProduceRequest` / `FetchRequest` objects that enter `KafkaApis`:

```
AMQP Client          Broker 3                                  Broker 1
     │                  │                                          │
     │  Basic.Publish   │                                          │
     │  (exchange routing                                          │
     │   resolves to    │                                          │
     │   topic P0)      │                                          │
     │──────────────────►│                                         │
     │                   │                                         │
     │   MetadataCache: P0 leader = Broker 1                       │
     │                   │                                         │
     │   ProduceForwardManager.forward(leaderId=1, ...)            │
     │                   │  binary ProduceRequest                  │
     │                   │─────────────────────────────────────────►│
     │                   │  ProduceResponse { offset:42 }          │
     │                   │◄─────────────────────────────────────────│
     │                   │                                         │
     │  Basic.Ack        │  (if confirm mode)                      │
     │◄──────────────────│                                         │
```

### 7.1 Forwarding decision

Same as HTTP protocol (see `http-protocol-design.md` §7.5):

```
Produce: is this broker the leader for target partition?
  YES → ReplicaManager.appendRecords() directly
  NO  → ProduceForwardManager.forward(leaderId, ...)

Consume: is this broker the leader for assigned partitions?
  YES → ReplicaManager.fetchMessages() directly
  NO  → FetchForwardManager.forward(leaderId, ...)
```

### 7.2 Multi-queue publish: fanout forwarding

When a single `Basic.Publish` routes to multiple queues (via fanout or topic exchange),
the broker may need to forward to multiple leaders simultaneously:

```
Basic.Publish → exchange "events" (fanout)
  → queue "orders"   → topic "amqp.orders"   P0 leader = Broker 1
  → queue "audit"    → topic "amqp.audit"    P0 leader = Broker 2
  → queue "metrics"  → topic "amqp.metrics"  P0 leader = Broker 3 (local)

Broker 3:
  LOCAL:    amqp.metrics/P0 → ReplicaManager.appendRecords()
  REMOTE:   amqp.orders/P0  → ProduceForwardManager.forward(1, ...)
  REMOTE:   amqp.audit/P0   → ProduceForwardManager.forward(2, ...)

  CompletableFuture.allOf(localFuture, remoteFuture1, remoteFuture2)
    .orTimeout(amqp.internal.forwarding.timeout.ms)
```

---

## 8. AMQP-to-Kafka Mapping

This section defines how AMQP concepts map to Kafka primitives.

### 8.1 Exchanges → Metadata records

Exchanges are routing configurations, not data stores. They exist only as metadata:

| AMQP Exchange Attribute | Storage |
|---|---|
| `name` | Key in `__amqp_metadata` topic |
| `type` (direct/topic/fanout/headers) | Value field |
| `durable` | If true, persisted to `__amqp_metadata` |
| `autoDelete` | Metadata flag; exchange deleted when last binding removed |
| `internal` | Metadata flag; rejects direct `Basic.Publish` |
| `arguments` | Serialized map (e.g. `alternate-exchange`) |

**In-memory cache:** `ExchangeManager` maintains a `ConcurrentHashMap<String, Exchange>`.
On broker startup, it replays the `__amqp_metadata` topic to rebuild the cache. All
exchange mutations (declare, delete) are written to `__amqp_metadata` and applied to the
cache atomically.

### 8.2 Virtual Hosts → Topic namespaces

AMQP virtual hosts provide isolation between tenants. They map to Kafka topic name prefixes:

```
vhost="/"           → topic prefix: "amqp."
vhost="/production" → topic prefix: "amqp.production."
vhost="/staging"    → topic prefix: "amqp.staging."
```

Queue "orders" in vhost "/production" → Kafka topic `amqp.production.orders`.
Queue "orders" in vhost "/" → Kafka topic `amqp.orders`.

The vhost is resolved during `Connection.Open` and stored as connection-scoped state. All
subsequent queue/exchange operations are scoped to this vhost.

### 8.3 Queues → Kafka topics

Each AMQP queue is backed by a Kafka topic:

| AMQP Queue Attribute | Kafka Topic Property |
|---|---|
| `queue` (name) | Topic name: `amqp.{vhost_prefix}{queueName}` |
| `durable:true` | Topic always persists (Kafka topics are durable by nature) |
| `durable:false` | Transient: topic created with `retention.ms=0` after last consumer disconnects |
| `exclusive` | Enforced at broker level (not a Kafka topic property) |
| `autoDelete` | Topic deleted when consumer count drops to 0 (tracked in metadata) |
| `x-message-ttl` | `retention.ms` |
| `x-max-length-bytes` | `retention.bytes` |
| `x-max-length` | Enforced at broker level (consumer lag check before publish, §15.8) |

**Topic auto-creation:** When `Queue.Declare` references a queue whose backing topic does
not exist, the broker creates it via an internal `CreateTopicsRequest` with:

```java
new NewTopic(
    topicName,
    numPartitions = config.amqpDefaultQueuePartitions,  // default 1
    replicationFactor = config.defaultReplicationFactor
)
.configs(Map.of(
    "retention.ms", String.valueOf(messageTtlMs),
    "retention.bytes", String.valueOf(maxLengthBytes),
    "cleanup.policy", "delete"
))
```

**Default partitions:** AMQP queues default to **1 partition** (matching RabbitMQ's
single-queue ordering guarantee). Operators can override with
`amqp.default.queue.partitions` for throughput. Note: with >1 partition, per-queue FIFO
ordering is relaxed to per-partition ordering.

### 8.4 Messages → Kafka records

| AMQP | Kafka Record Field | Notes |
|---|---|---|
| `Basic.Publish.routingKey` | `key` | Used for partition assignment |
| Content body bytes | `value` | Raw bytes, no transformation |
| Content properties | `headers["_amqp_properties"]` | Serialized property bytes for lossless round-trip |
| `properties.contentType` | `headers["_content-type"]` | Duplicated for cross-protocol consumption |
| `properties.headers` (app table) | `headers["_amqp_headers"]` | Application-defined headers |
| Exchange name | `headers["_amqp_exchange"]` | For `Basic.Deliver.exchange` reconstruction |
| Routing key | `headers["_amqp_routing_key"]` | For `Basic.Deliver.routingKey` reconstruction |
| `properties.timestamp` | `record.timestamp` | If present; else Kafka sets `CreateTime` |
| `properties.deliveryMode` | `headers["_amqp_delivery_mode"]` | 1=non-persistent, 2=persistent |

### 8.5 Consumer ACKs → Offset commits

AMQP acknowledgements map to Kafka consumer offset commits:

```
Basic.Ack(deliveryTag=5, multiple=true)
  → resolve delivery tags 1..5 to [(tp0, offset=42), (tp0, offset=43), ...]
  → commit offsets: tp0 → offset 44 (next offset to read)
  → via GroupCoordinator.commitOffsets(
      group = "amqp.{vhost}.{queueName}",
      memberId = "{connectionId}-{channelNum}",
      offsets = {tp0: 44}
    )
```

**Delivery tag tracking:** Per-channel `ConcurrentHashMap<Long, PendingDelivery>` maps
delivery tags to `(TopicPartition, offset)`. When `multiple=true`, all entries with
`tag <= deliveryTag` are committed.

**Batch commit optimization:** Rather than committing on every ACK, the broker batches
commits. Commits are flushed:
1. Every `amqp.ack.commit.interval.ms` (default 1000ms)
2. When a channel closes
3. When `Tx.Commit` is called (immediate flush)

### 8.6 Bindings → Metadata records

Bindings are stored in `__amqp_metadata` alongside exchanges and queues. Each binding is
a record with key `binding:{vhost}:{exchange}:{queue}:{routingKey}` and value containing
the binding arguments.

The `BindingManager` maintains an in-memory index:
- `exchangeToBindings: ConcurrentHashMap<String, CopyOnWriteArrayList<Binding>>`
- Used by the routing engine during `Basic.Publish`

### 8.7 `__amqp_metadata` topic design

A single compacted Kafka topic stores all AMQP metadata:

```
Topic: __amqp_metadata
Partitions: 1 (single partition for total ordering)
Cleanup policy: compact
Replication factor: min(3, cluster size)

Record key format:
  "exchange:{vhost}:{name}"
  "queue:{vhost}:{name}"
  "binding:{vhost}:{exchange}:{queue}:{routingKey}:{argumentsHash}"

Record value:
  JSON serialized metadata (exchange type, queue arguments, binding arguments)
  null value = tombstone (entity deleted)
```

**Startup replay:** On broker startup, the `AmqpMetadataManager` consumes the entire
`__amqp_metadata` topic from offset 0 to build the in-memory exchange, queue, and binding
caches. This follows the same pattern as `__consumer_offsets` replay in `GroupCoordinator`.

**Cross-broker consistency:** All brokers in the cluster read from `__amqp_metadata`, so
exchange/queue/binding declarations on one broker are visible to all brokers after a short
replication delay. Declarations are idempotent (write with `ON CONFLICT DO NOTHING` semantics
via compacted key).

---

## 9. Integration with Existing Kafka Infrastructure

### 9.1 New security protocol: `AMQP` and `AMQPS`

```java
// clients/src/main/java/org/apache/kafka/common/security/auth/SecurityProtocol.java
// Existing: PLAINTEXT(0), SSL(1), SASL_PLAINTEXT(2), SASL_SSL(3), HTTP(4), HTTPS(5)
AMQP(6, "AMQP"),
AMQPS(7, "AMQPS");
```

Helper method:

```java
public boolean isAmqp() { return this == AMQP || this == AMQPS; }
```

Listener configuration:

```properties
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,HTTP:HTTP,AMQP:AMQP
listeners=PLAINTEXT://0.0.0.0:9092,HTTP://0.0.0.0:9094,AMQP://0.0.0.0:5672
advertised.listeners=PLAINTEXT://broker1:9092,HTTP://broker1:9094,AMQP://broker1:5672
```

**`inter.broker.listener.name` must remain a binary-protocol listener** (same constraint
as HTTP).

### 9.2 AmqpAcceptor — plugging into SocketServer

```scala
// core/src/main/scala/kafka/network/SocketServer.scala

def createDataPlaneAcceptorAndProcessors(endpoint: Endpoint): Unit = {
  endpoint.securityProtocol match {
    case SecurityProtocol.HTTP | SecurityProtocol.HTTPS =>
      val httpAcceptor = new HttpAcceptor(this, endpoint, config, ...)
      httpAcceptors.put(endpoint, httpAcceptor)
    case SecurityProtocol.AMQP | SecurityProtocol.AMQPS =>
      val amqpAcceptor = new AmqpAcceptor(this, endpoint, config, ...)
      amqpAcceptors.put(endpoint, amqpAcceptor)
    case _ =>
      val dataPlaneAcceptor = new DataPlaneAcceptor(this, endpoint, config, ...)
      dataPlaneAcceptors.put(endpoint, dataPlaneAcceptor)
  }
}
```

### 9.3 AmqpAcceptor — Netty-based server

```scala
// amqp-server/src/main/scala/kafka/network/AmqpAcceptor.scala

class AmqpAcceptor(
  socketServer:   SocketServer,
  endpoint:       Endpoint,
  config:         KafkaConfig,
  requestChannel: RequestChannel,
  metadataCache:  MetadataCache,
  amqpMetadata:   AmqpMetadataManager,
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
      .childHandler(new AmqpChannelInitializer(config, requestChannel, amqpMetadata, ...))
    channel = bootstrap.bind(endpoint.host, endpoint.port).sync().channel()
  }
}
```

### 9.4 AmqpChannelInitializer — Netty pipeline

```scala
class AmqpChannelInitializer(config: KafkaConfig, ...)
    extends ChannelInitializer[SocketChannel] {

  override def initChannel(ch: SocketChannel): Unit = {
    val pipeline = ch.pipeline()

    // Optional TLS (for AMQPS)
    if (endpoint.securityProtocol == SecurityProtocol.AMQPS)
      pipeline.addLast("ssl", sslContext.newHandler(ch.alloc()))

    // AMQP binary frame decoder (handles 8-byte header + frames)
    pipeline.addLast("amqp-frame-decoder", new Amqp091FrameDecoder(config.amqpFrameMaxBytes))

    // Connection handshake handler (state machine, channel 0 only)
    pipeline.addLast("amqp-connection", new Amqp091ConnectionHandler(
      config, principalBuilder, amqpMetadata))

    // Channel lifecycle (open/close/flow)
    pipeline.addLast("amqp-channel", new Amqp091ChannelHandler(config))

    // Heartbeat (idle detection, keep-alive)
    // Note: added dynamically after Connection.Tune negotiates the heartbeat interval
    // pipeline.addLast("amqp-heartbeat", new Amqp091HeartbeatHandler(...))

    // Main request dispatcher
    pipeline.addLast("amqp-request", new Amqp091RequestHandler(
      config, requestChannel, amqpMetadata,
      produceForwardManager, fetchForwardManager,
      amqpConsumerManager, amqpAsyncExecutor))
  }
}
```

### 9.5 KafkaApis dispatch

```scala
// KafkaApis.handle() — dispatch addition:

case ApiKeys.PRODUCE =>
  if (request.context.securityProtocol.isAmqp)
    handleAmqpProduceRequest(request, requestLocal)
  else if (request.context.securityProtocol.isHttp)
    handleHttpProduceRequest(request, requestLocal)
  else
    handleProduceRequest(request, requestLocal)

case ApiKeys.FETCH =>
  if (request.context.securityProtocol.isAmqp)
    handleAmqpConsumeRequest(request)
  else if (request.context.securityProtocol.isHttp)
    handleHttpConsumeRequest(request)
  else
    handleFetchRequest(request)
```

**`handleAmqpProduceRequest`** follows the same pattern as `handleHttpProduceRequest` (§5
of the HTTP design):
1. Parse the produce request
2. Bucket partitions by leader (local vs. remote)
3. Local: `replicaManager.appendRecords()`
4. Remote: `produceForwardManager.forward()`
5. `.handleAsync()` on `amqpAsyncExecutor` — never block handler threads
6. On completion: write `Basic.Ack` or `Basic.Nack` to the AMQP channel (if confirm mode)

**`handleAmqpConsumeRequest`** is similar to `handleHttpConsumeRequest` but the response
path writes AMQP `Basic.Deliver` frames instead of JSON.

### 9.6 AmqpProcessor — response routing bridge

```java
// amqp-server/src/main/java/kafka/server/amqp/AmqpProcessor.java

public class AmqpProcessor {

    private final int id;
    private final LinkedBlockingDeque<RequestChannel.Response> responseQueue;
    private final ConcurrentHashMap<String, AmqpConnectionContext> connections;

    // Called by RequestChannel.sendResponse() — same contract as HttpProcessor
    public void enqueueResponse(RequestChannel.Response response) {
        responseQueue.add(response);
    }

    // Polled by dedicated response-drainer thread
    public void processResponses() {
        List<RequestChannel.Response> batch = new ArrayList<>();
        responseQueue.drainTo(batch);

        for (RequestChannel.Response response : batch) {
            String connectionId = response.request().context().connectionId();
            AmqpConnectionContext conn = connections.get(connectionId);

            if (response instanceof RequestChannel.SendResponse sendResp) {
                if (conn != null && conn.channel().isActive()) {
                    // Route to appropriate AMQP response writer:
                    // - Produce response → Basic.Ack/Nack (if confirm mode)
                    // - Fetch response → Basic.Deliver frames
                    // - Metadata response → queue/exchange/binding response frames
                    amqpResponseWriter.write(conn, sendResp);
                }
            } else if (response instanceof RequestChannel.CloseConnectionResponse) {
                if (conn != null) conn.close();
                connections.remove(connectionId);
            }
        }
    }
}
```

---

## 10. New Module: `amqp-server`

```
kafka/
└── amqp-server/
    ├── build.gradle
    └── src/
        ├── main/
        │   ├── java/kafka/server/amqp/
        │   │   ├── codec/
        │   │   │   ├── Amqp091Codec.java              — frame encoding/decoding (all methods)
        │   │   │   ├── Amqp091FrameDecoder.java        — Netty ByteToMessageDecoder
        │   │   │   ├── AmqpFrame.java                  — frame wrapper record
        │   │   │   ├── AmqpBasicPublishData.java       — parsed Basic.Publish fields
        │   │   │   ├── AmqpContentHeaderData.java      — parsed content properties
        │   │   │   ├── AmqpExchangeDeclareData.java    — parsed Exchange.Declare fields
        │   │   │   ├── AmqpQueueDeclareData.java       — parsed Queue.Declare fields
        │   │   │   ├── AmqpBasicConsumeData.java       — parsed Basic.Consume fields
        │   │   │   ├── AmqpBasicGetData.java           — parsed Basic.Get fields
        │   │   │   └── AmqpTableCodec.java             — AMQP table field encoding/decoding
        │   │   ├── handler/
        │   │   │   ├── Amqp091ConnectionHandler.java   — handshake state machine
        │   │   │   ├── Amqp091ChannelHandler.java      — channel lifecycle
        │   │   │   ├── Amqp091HeartbeatHandler.java    — idle detection / keep-alive
        │   │   │   ├── Amqp091RequestHandler.java      — main dispatcher
        │   │   │   ├── Amqp091PublishHandler.java       — multi-frame assembly + routing
        │   │   │   ├── Amqp091ConsumeHandler.java       — Basic.Consume registration
        │   │   │   ├── Amqp091AckHandler.java           — ACK/NACK/Reject processing
        │   │   │   ├── Amqp091GetHandler.java           — Basic.Get (pull)
        │   │   │   ├── Amqp091ExchangeHandler.java      — Exchange CRUD
        │   │   │   ├── Amqp091QueueHandler.java         — Queue CRUD
        │   │   │   ├── Amqp091TxHandler.java            — Transaction handling
        │   │   │   └── Amqp091ConfirmHandler.java       — Publisher confirms
        │   │   ├── routing/
        │   │   │   ├── AmqpRoutingEngine.java           — exchange → queue routing
        │   │   │   ├── DirectMatcher.java               — exact routing key match
        │   │   │   ├── TopicMatcher.java                — wildcard routing key match
        │   │   │   ├── FanoutMatcher.java               — deliver to all bindings
        │   │   │   └── HeadersMatcher.java              — header criteria matching
        │   │   ├── metadata/
        │   │   │   ├── AmqpMetadataManager.java         — exchange/queue/binding cache
        │   │   │   ├── ExchangeManager.java             — exchange CRUD + cache
        │   │   │   ├── QueueManager.java                — queue CRUD + topic creation
        │   │   │   └── BindingManager.java              — binding CRUD + index
        │   │   ├── consumer/
        │   │   │   ├── AmqpConsumerManager.java         — consumer lifecycle
        │   │   │   ├── AmqpConsumerFetchLoop.java       — per-consumer fetch task
        │   │   │   ├── DeliveryTagTracker.java          — deliveryTag → offset mapping
        │   │   │   └── PrefetchLimiter.java             — QoS enforcement
        │   │   ├── AmqpProcessor.java                   — RequestChannel response bridge
        │   │   ├── AmqpResponseWriter.java              — Kafka response → AMQP frame writer
        │   │   └── AmqpConnectionContext.java           — per-connection state
        │   └── scala/kafka/network/
        │       ├── AmqpAcceptor.scala                   — Netty server bootstrap
        │       └── AmqpChannelInitializer.scala         — pipeline configuration
        └── test/
            └── java/kafka/server/amqp/
                ├── codec/
                │   ├── Amqp091CodecTest.java            — frame encode/decode unit tests
                │   └── AmqpTableCodecTest.java          — table field tests
                ├── handler/
                │   ├── Amqp091ConnectionHandlerTest.java
                │   ├── Amqp091PublishHandlerTest.java
                │   └── Amqp091RoutingEngineTest.java
                ├── integration/
                │   ├── AmqpProduceIntegrationTest.java  — publish via RabbitMQ client
                │   ├── AmqpConsumeIntegrationTest.java  — consume via RabbitMQ client
                │   ├── AmqpExchangeIntegrationTest.java — exchange types
                │   └── AmqpCrossProtocolTest.java       — AMQP publish, Kafka consume
                └── routing/
                    ├── DirectMatcherTest.java
                    ├── TopicMatcherTest.java
                    └── HeadersMatcherTest.java
```

### 10.1 `amqp-server/build.gradle` dependencies

```groovy
dependencies {
  implementation project(':core')
  implementation project(':clients')
  implementation project(':server-common')
  implementation "io.netty:netty-all:${versions.netty}"
  // No external AMQP library — codec is implemented from scratch

  testImplementation project(':core').sourceSets.test.output
  testImplementation "com.rabbitmq:amqp-client:${versions.amqpClient}"  // 5.28.0
}
```

---

## 11. Configuration

New properties added to `KafkaConfig`:

| Property | Default | Description |
|---|---|---|
| `amqp.enabled` | `false` | Master switch (can also add `AMQP://...` to `listeners`) |
| `num.amqp.network.threads` | `4` | Netty worker thread count for AMQP listener |
| `amqp.frame.max.bytes` | `1048576` (1 MB) | Max AMQP frame payload size, negotiated with client |
| `amqp.channel.max` | `256` | Max channels per connection |
| `amqp.heartbeat.seconds` | `60` | Default heartbeat interval (negotiated with client) |
| `amqp.heartbeat.missed.max` | `3` | Close connection after this many missed heartbeats |
| `amqp.default.queue.partitions` | `1` | Default partition count for auto-created queue topics |
| `amqp.default.vhost` | `/` | Default virtual host |
| `amqp.topic.prefix` | `amqp.` | Prefix for Kafka topics backing AMQP queues |
| `amqp.metadata.topic` | `__amqp_metadata` | Internal topic for exchange/queue/binding metadata |
| `amqp.metadata.replication.factor` | `3` | Replication factor for `__amqp_metadata` |
| `amqp.consume.max.bytes` | `1048576` (1 MB) | Max bytes per fetch for AMQP consumers |
| `amqp.consume.max.wait.ms` | `500` | Max wait per fetch loop iteration |
| `amqp.ack.commit.interval.ms` | `1000` | Batch interval for offset commits from ACKs |
| `amqp.consumer.start.offset` | `latest` | Default start offset for new consumers (`earliest` or `latest`) |
| `num.amqp.consumer.threads` | `8` | Thread pool size for consumer fetch loops |
| `num.amqp.async.threads` | `4` | Thread pool for async produce/consume completion |
| `amqp.connection.idle.timeout.ms` | `600000` (10 min) | Close idle AMQP connections (no heartbeat, no frames) |
| `amqp.internal.forwarding.timeout.ms` | `10000` | Timeout for broker-to-broker forwarding |
| `amqp.internal.forwarding.queue.size` | `10000` | Bounded queue per forward thread |
| `amqp.shutdown.drain.ms` | `5000` | Drain window during graceful shutdown |
| `amqp.tx.buffer.max.bytes` | `10485760` (10 MB) | Max buffered bytes per transaction before rejection |
| `amqp.prefetch.default` | `0` (unlimited) | Default prefetch count if client does not set `Basic.Qos` |

Listener registration:

```properties
listeners=PLAINTEXT://0.0.0.0:9092,AMQP://0.0.0.0:5672
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,AMQP:AMQP
advertised.listeners=PLAINTEXT://broker1:9092,AMQP://broker1:5672
```

---

## 12. Error Handling

### 12.1 AMQP error frame mapping

AMQP errors are conveyed via `Channel.Close` (channel-level) or `Connection.Close`
(connection-level) frames, each carrying a reply code, reply text, and the class/method
that caused the error.

**Connection-level errors** (close the entire connection):

| Kafka Error | AMQP Reply Code | Reply Text | Notes |
|---|---|---|---|
| `KAFKA_STORAGE_ERROR` | 541 `INTERNAL_ERROR` | "Disk error" | |
| Authentication failure | 403 `ACCESS_REFUSED` | "Login refused" | |
| Unknown vhost | 402 `INVALID_PATH` | "No such vhost" | |
| Frame too large | 501 `FRAME_ERROR` | "Frame exceeds frameMax" | |
| Protocol violation | 501 `FRAME_ERROR` | "Malformed frame" | |
| Channel limit exceeded | 504 `CHANNEL_ERROR` | "channelMax exceeded" | |

**Channel-level errors** (close only the affected channel):

| Kafka Error | AMQP Reply Code | Reply Text | Notes |
|---|---|---|---|
| `UNKNOWN_TOPIC_OR_PARTITION` | 404 `NOT_FOUND` | "Queue not found" | Queue not declared |
| `TOPIC_AUTHORIZATION_FAILED` | 403 `ACCESS_REFUSED` | "Access denied" | |
| `NOT_ENOUGH_REPLICAS` | 541 `INTERNAL_ERROR` | "ISR below minimum" | |
| `REQUEST_TIMED_OUT` | 541 `INTERNAL_ERROR` | "Timeout" | |
| `MESSAGE_TOO_LARGE` | 311 `CONTENT_TOO_LARGE` | "Message too large" | |
| Exchange not found | 404 `NOT_FOUND` | "Exchange not found" | |
| Binding not found | 404 `NOT_FOUND` | "Binding not found" | |
| Exclusive consumer conflict | 403 `ACCESS_REFUSED` | "Exclusive consumer already active" | |
| Queue not empty (if-empty) | 406 `PRECONDITION_FAILED` | "Queue not empty" | Queue.Delete with ifEmpty |
| Queue in use (if-unused) | 406 `PRECONDITION_FAILED` | "Queue has consumers" | Queue.Delete with ifUnused |
| Exchange type mismatch | 406 `PRECONDITION_FAILED` | "Exchange type mismatch" | Redeclare with different type |

### 12.2 Error frame format

```
Channel.Close (channel=N):
  classId=20, methodId=40
  Fields: replyCode(short), replyText(shortstr), failingClassId(short), failingMethodId(short)

Connection.Close (channel=0):
  classId=10, methodId=50
  Fields: replyCode(short), replyText(shortstr), failingClassId(short), failingMethodId(short)
```

The `failingClassId` and `failingMethodId` identify which AMQP method triggered the error
(e.g., classId=60/methodId=40 = `Basic.Publish`).

### 12.3 Mandatory publish failure

When `mandatory=true` and routing finds no queues:

```
Basic.Return:
  classId=60, methodId=50
  Fields: replyCode=312, replyText="NO_ROUTE", exchange, routingKey
  Followed by: original Content header + Content body frames
```

This is NOT an error — the channel remains open. The client receives the returned message
and can decide what to do (retry, log, dead-letter).

---

## 13. Security

### 13.1 Authentication via SASL PLAIN

AMQP 0-9-1 authenticates during `Connection.StartOk`. The mechanism is `PLAIN` (RFC 4616):

```
Response field: "\0username\0password"  (NUL-separated)
```

The broker parses this and delegates to `KafkaPrincipalBuilder`:

```java
// AmqpAuthenticationContext implements AuthenticationContext
public class AmqpAuthenticationContext implements AuthenticationContext {
    private final String username;
    private final String password;
    private final InetAddress clientAddress;
    private final SecurityProtocol securityProtocol;
    // ... plus accessor for SslSession if AMQPS
}
```

The `KafkaPrincipalBuilder.build(AmqpAuthenticationContext)` returns a `KafkaPrincipal`
that flows through the standard `Authorizer` path in `KafkaApis`.

### 13.2 Authorization

AMQP operations map to Kafka ACL checks:

| AMQP Operation | Kafka ACL Resource | Kafka ACL Operation |
|---|---|---|
| `Basic.Publish` | `TOPIC:amqp.{queue}` | `WRITE` |
| `Basic.Consume` / `Basic.Get` | `TOPIC:amqp.{queue}` | `READ` |
| `Exchange.Declare` / `Exchange.Delete` | `CLUSTER` | `ALTER` |
| `Queue.Declare` / `Queue.Delete` | `TOPIC:amqp.{queue}` | `CREATE` / `DELETE` |
| `Queue.Bind` / `Queue.Unbind` | `TOPIC:amqp.{queue}` | `ALTER` |

### 13.3 TLS

The `AMQPS` security protocol provides TLS. Standard Kafka TLS configuration applies:

```properties
listener.security.protocol.map=AMQPS:AMQPS
ssl.keystore.location=/path/to/keystore.jks
ssl.truststore.location=/path/to/truststore.jks
```

The `AmqpChannelInitializer` adds Netty's `SslHandler` as the first pipeline handler when
the security protocol is `AMQPS`.

---

## 14. Implementation Plan

### Phase 1 — AMQP listener + connection lifecycle + basic publish

1. Add `AMQP(6)` / `AMQPS(7)` to `SecurityProtocol` enum with `isAmqp()` helper
2. Create `amqp-server` Gradle submodule with Netty dependency
3. Implement `Amqp091Codec` — frame encoding/decoding for all method classes
4. Implement `Amqp091FrameDecoder` — Netty `ByteToMessageDecoder`
5. Implement `Amqp091ConnectionHandler` — handshake state machine (§4.1)
6. Implement `Amqp091ChannelHandler` — channel open/close/flow (§4.2)
7. Implement `Amqp091HeartbeatHandler` — idle detection + heartbeat frames (§4.13)
8. Implement `AmqpAcceptor`, `AmqpChannelInitializer` — Netty server bootstrap
9. Wire `AmqpAcceptor` into `SocketServer.createDataPlaneAcceptorAndProcessors()`
10. Implement `AmqpProcessor` — response routing bridge (§9.6)
11. Add dispatch branch in `KafkaApis.handle()` for `securityProtocol.isAmqp` (§9.5)
12. Implement `AmqpMetadataManager` — `__amqp_metadata` topic + in-memory cache (§8.7)
13. Implement `ExchangeManager` — pre-declared exchanges + Exchange.Declare/Delete (§4.3)
14. Implement `QueueManager` — Queue.Declare/Delete with Kafka topic auto-creation (§4.4)
15. Implement `BindingManager` — Queue.Bind/Unbind (§4.5)
16. Implement `AmqpRoutingEngine` — direct exchange routing only (§4.3.1)
17. Implement `Amqp091PublishHandler` — multi-frame assembly + routing (§4.6)
18. Add new config properties to `KafkaConfig` (§11)
19. Integration test: RabbitMQ Java client connects, declares queue, publishes messages

### Phase 2 — All exchange types + consume + forwarding

20. Implement `TopicMatcher` — wildcard routing for topic exchanges
21. Implement `FanoutMatcher` — deliver-to-all-bindings routing
22. Implement `HeadersMatcher` — header criteria matching (x-match: all/any)
23. Implement exchange-to-exchange bindings with cycle detection
24. Implement `AmqpConsumerManager` + `AmqpConsumerFetchLoop` — push consumption (§6)
25. Implement `Amqp091ConsumeHandler` — Basic.Consume/Cancel registration (§4.7)
26. Implement `Amqp091GetHandler` — Basic.Get pull consume (§4.8)
27. Implement `Amqp091AckHandler` — ACK/NACK/Reject with offset commit (§4.9)
28. Implement `DeliveryTagTracker` — deliveryTag → (tp, offset) mapping (§8.5)
29. Implement `PrefetchLimiter` — QoS enforcement (§4.11)
30. Add AMQP produce forwarding via existing `ProduceForwardManager` (§7)
31. Add AMQP consume forwarding via existing `FetchForwardManager` (§7)
32. Integration tests: all 4 exchange types, multi-consumer competing, exclusive consumer

### Phase 3 — Publisher confirms + robustness + security

33. Implement `Amqp091ConfirmHandler` — Confirm.Select, Basic.Ack/Nack for publishes (§4.10)
34. Implement mandatory message handling — Basic.Return on no-route (§4.6)
35. Implement consumer group assignment via `GroupCoordinator` (§6.4)
36. Implement SASL PLAIN authentication (§13.1)
37. Implement ACL authorization mapping (§13.2)
38. Implement TLS support for `AMQPS` listener (§13.3)
39. Batch offset commit optimization (§8.5)
40. Implement Queue.Purge
41. Implement auto-delete queues and exchanges
42. Graceful shutdown drain (§15.6)
43. Metrics: `amqp.publish.rate`, `amqp.consume.rate`, `amqp.connection.count`, `amqp.channel.count`
44. Integration tests: publisher confirms, auth failures, cross-protocol (AMQP publish → Kafka consume)

### Phase 4 — Transactions + advanced features

45. Implement `Amqp091TxHandler` — Tx.Select/Commit/Rollback (§4.12)
46. Dead-letter exchange support: `x-dead-letter-exchange` queue argument (§15.7)
47. Message TTL: `x-message-ttl` enforcement at delivery time
48. Priority queues: `x-max-priority` with partitioned priority ranges (§15.8)
49. `x-max-length` enforcement with `x-overflow` policy (drop-head / reject-publish)
50. Server-generated queue names (`amq.gen-*`)
51. Exclusive queue cleanup on connection close
52. Performance benchmarking (throughput, latency, connection scalability)

---

## 15. Implementation Concerns

### 15.1 CRITICAL — Consumer fetch loop must not block Netty or handler threads

**Problem.** The AMQP consumer model requires the broker to push messages to clients. A
naive implementation might fetch from Kafka on a Netty worker thread or a
`KafkaRequestHandler` thread, blocking them.

**Solution.** The `AmqpConsumerFetchLoop` runs on a dedicated `amqpConsumerExecutor` thread
pool (configurable via `num.amqp.consumer.threads`, default 8). Each consumer subscription
gets its own task on this pool. The fetch loop:

1. Builds a `FetchRequest` for the consumer's assigned partitions
2. Enqueues it to `RequestChannel` via `tryEnqueue()` (non-blocking)
3. Receives the `FetchResponse` via a `CompletableFuture`
4. Converts Kafka records to AMQP `Basic.Deliver` frames
5. Writes frames to the Netty channel via `ctx.writeAndFlush()` (thread-safe in Netty)
6. Respects prefetch count before fetching more

```java
// Consumer fetch loop — runs on amqpConsumerExecutor, NOT on Netty or handler threads

while (active && !flowPaused) {
    // Wait for prefetch budget
    int budget = prefetchLimiter.awaitBudget(100, MILLISECONDS);
    if (budget <= 0) continue;

    // Non-blocking enqueue to RequestChannel
    CompletableFuture<FetchResponse> future = submitFetchRequest(budget);

    // Await response — this is on the consumer executor, OK to block briefly
    FetchResponse response = future.get(config.amqpConsumeMaxWaitMs + 1000, MILLISECONDS);

    // Write AMQP frames — Netty writeAndFlush is thread-safe
    for (Record record : response.records()) {
        writeBasicDeliver(ctx, record);
        prefetchLimiter.delivered();
    }
}
```

### 15.2 CRITICAL — Delivery tag tracking memory: bound per-channel pending ACKs

**Problem.** Each unacknowledged message consumes memory in the `DeliveryTagTracker` map
(`deliveryTag → (TopicPartition, offset)`). Without prefetch limits, a fast producer with
a slow consumer could accumulate millions of pending ACKs.

**Solution.** The `PrefetchLimiter` enforces a hard ceiling. If the client has not set
`Basic.Qos`, the broker uses `amqp.prefetch.default` (default 0 = unlimited). When prefetch
is unlimited, a **broker-side safety limit** of `amqp.max.unacked.per.channel` (default 65536)
prevents unbounded growth. Exceeding this limit pauses delivery (same as if prefetch were hit).

**Memory budget per channel:** 65536 entries × ~80 bytes/entry = ~5 MB. With 256 channels
per connection, worst case = ~1.3 GB per connection. In practice, most connections use 1–2
channels with prefetch=100.

### 15.3 CRITICAL — Multi-frame assembly: ByteBuf lifecycle

**Problem.** AMQP publishes span multiple frames (METHOD + HEADER + BODY*). The body is
assembled in a `CompositeByteBuf` using retained slices from each body frame. If the publish
fails or the connection drops mid-assembly, these slices must be released to avoid memory leaks.

**Solution.** The `Amqp091PublishHandler` tracks assembly state per channel:

```java
// Per-channel publish assembly state
private static class PublishAssembly {
    final AmqpBasicPublishData publishData;
    final AmqpContentHeaderData headerData;
    final CompositeByteBuf bodyAccumulator;
    long receivedBodySize;

    void release() {
        if (bodyAccumulator != null) {
            bodyAccumulator.release();
        }
    }
}

// In channelInactive() and exceptionCaught():
publishAssemblies.values().forEach(PublishAssembly::release);
publishAssemblies.clear();
```

Every code path that completes or abandons a publish must call `release()`. The handler's
`channelInactive()` cleans up all outstanding assemblies.

### 15.4 HIGH — Exchange routing hot path: pre-compile topic patterns

**Problem.** Topic exchange routing matches routing keys against wildcard patterns on every
`Basic.Publish`. For exchanges with hundreds of bindings, this is O(N) per publish.

**Solution.** `TopicMatcher` pre-compiles binding patterns into a trie structure on
`Queue.Bind`:

```
Trie for exchange "events":
  root
  ├── "order" → { "created" → [queue1], "updated" → [queue2], "*" → [queue3] }
  ├── "payment" → { "#" → [queue4] }
  └── "#" → [queue5]
```

Routing key `"order.created"` walks the trie: root → "order" → "created" → match [queue1].
Simultaneously check `*` (single-word wildcard) and `#` (multi-word wildcard) branches at
each level.

The trie is rebuilt on binding changes (rare) using `CopyOnWriteArrayList` for the binding
list, so reads during publish are lock-free.

### 15.5 HIGH — `__amqp_metadata` startup replay: block until caught up

**Problem.** On broker startup, the `AmqpMetadataManager` must replay `__amqp_metadata`
to build the in-memory exchange/queue/binding cache. If the AMQP listener starts accepting
connections before replay completes, clients may see missing exchanges/queues.

**Solution.** `AmqpAcceptor.startup()` blocks until `AmqpMetadataManager.isReady()` returns
true. The metadata manager signals readiness when it has consumed to the high watermark of
`__amqp_metadata`:

```java
// AmqpMetadataManager startup
public CompletableFuture<Void> startup() {
    return CompletableFuture.runAsync(() -> {
        // Consume __amqp_metadata from offset 0 to HW
        long highWatermark = getHighWatermark("__amqp_metadata", 0);
        while (currentOffset < highWatermark) {
            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, byte[]> record : records) {
                applyMetadataRecord(record);
                currentOffset = record.offset() + 1;
            }
        }
        ready.set(true);
    }, metadataExecutor);
}
```

### 15.6 MEDIUM — Graceful shutdown: drain AMQP consumers and in-flight publishes

**Drain sequence:**

```
1. Stop accepting new connections (Netty ServerChannel.close())
2. Send Connection.Close(320, "Broker shutting down") to all AMQP connections
3. Wait up to amqp.shutdown.drain.ms for:
   a. In-flight publishes to complete (pending produce futures)
   b. Consumer fetch loops to finish current iteration
   c. Pending ACK commits to flush
4. Force-close remaining connections
5. Shutdown Netty event loops
```

This follows the same pattern as `HttpAcceptor.beginDrain()` / `awaitDrain()` (§14.8 of
HTTP design).

### 15.7 MEDIUM — Dead-letter exchange (DLX) support

When a message is rejected (`Basic.Nack` with `requeue=false` or `Basic.Reject` with
`requeue=false`) and the queue has `x-dead-letter-exchange` set, the broker republishes
the message to the DLX:

```
1. Client sends Basic.Nack(deliveryTag=5, requeue=false)
2. Broker resolves deliveryTag 5 → queue "orders", offset 42
3. Queue "orders" has x-dead-letter-exchange = "dlx"
4. Broker republishes message to exchange "dlx":
   - routing key = x-dead-letter-routing-key (if set) or original routing key
   - additional header: x-death (array of death records)
5. Commit original offset (message consumed from "orders")
```

**Kafka mapping:** The DLX publish is a new `ProduceRequest` to the topic(s) resolved
through the DLX exchange's bindings. The `x-death` header records the death chain:

```json
[{
  "queue": "orders",
  "reason": "rejected",
  "count": 1,
  "exchange": "orders-exchange",
  "routing-keys": ["order.created"],
  "time": 1713260400
}]
```

### 15.8 LOW — Priority queues

AMQP priority queues (`x-max-priority` argument) are complex to map to Kafka's append-only
log. Two approaches:

**Option A: Separate partitions per priority level.**
A queue with `x-max-priority=10` gets 10 partitions (one per priority level 0–9). The
`AmqpConsumerFetchLoop` reads from the highest-priority partition first. This gives strict
priority ordering but requires partition-level consumer coordination.

**Option B: Consumer-side priority buffer.**
All messages go to the same partition. The `AmqpConsumerFetchLoop` fetches a batch, sorts
by the `priority` header, and delivers highest-priority messages first. This is simpler but
only provides batch-local priority ordering.

**Recommendation:** Option B for phase 4. It is simpler, works with any partition count,
and matches the common use case (priority within a batch). True strict priority ordering
across the entire queue would require a dedicated priority queue implementation outside
Kafka's log abstraction.

### 15.9 LOW — Connection and channel limits

**Per-broker limits:**
- `amqp.max.connections` (default 10000) — total AMQP connections per broker
- `amqp.max.channels.per.connection` — already covered by `amqp.channel.max` (default 256)

When the limit is reached, new `Connection.Open` receives `Connection.Close(506, "Too many connections")`.

New metric: `amqp.connection.count` — gauge of active connections. Alert when approaching limit.

### 15.10 LOW — `x-max-length` enforcement

AMQP's `x-max-length` limits the number of messages in a queue. Kafka topics don't have a
message-count-based retention policy. Implementation options:

**Option A: Produce-time rejection.**
Before appending, check the consumer lag (logEndOffset - committedOffset) for the queue's
backing topic. If lag exceeds `x-max-length` and `x-overflow=reject-publish`, reject the
publish with `Basic.Nack`. If `x-overflow=drop-head`, the oldest messages are effectively
"dropped" when consumed (the consumer skips past them via offset advancement).

**Option B: Cleanup thread.**
A background thread monitors queue lengths and deletes old segments when `x-max-length` is
exceeded. This is more complex and adds lag.

**Recommendation:** Option A for simplicity. `drop-head` semantics are approximated by
Kafka's retention policy; `reject-publish` is enforced at produce time via lag check.

---

## 16. Comparison with RabbitMQ

### 16.1 Architectural differences

| Concern | RabbitMQ | Kafka with AMQP layer |
|---|---|---|
| **Storage** | Per-node message store (Mnesia + disk) | Distributed, replicated log (ISR) |
| **Replication** | Quorum queues (Raft per queue) | ISR per partition (all queues) |
| **Ordering** | Per-queue FIFO | Per-partition FIFO (per-queue if 1 partition) |
| **Retention** | Messages deleted after ACK | Messages retained per retention policy |
| **Replay** | Not possible (ACK = delete) | Full replay from any offset |
| **Scalability** | Vertical (per-node queue capacity) | Horizontal (partitions across brokers) |
| **Consumer groups** | Not native (competing consumers) | Native Kafka consumer groups |
| **Transactions** | Per-channel, lightweight | Kafka transactions (heavier but exactly-once) |

### 16.2 What RabbitMQ clients gain

- **Durability:** Kafka's replicated log replaces RabbitMQ's quorum queues with simpler ops
- **Replay:** AMQP consumers can reset offsets to re-process messages (RabbitMQ deletes after ACK)
- **Horizontal scale:** Queues backed by partitioned topics scale across brokers
- **Cross-protocol:** Messages published via AMQP can be consumed by Kafka, HTTP, or other protocol clients

### 16.3 What RabbitMQ clients lose

- **Strict per-queue FIFO:** With >1 partition, ordering is per-partition only
- **Immediate message deletion on ACK:** Kafka retains messages per retention policy
- **Lightweight transactions:** Kafka transactions have higher overhead
- **Plugin ecosystem:** RabbitMQ plugins (Shovel, Federation, STOMP, MQTT) are not available
- **Management API:** RabbitMQ's HTTP management API is not replicated
- **Message priority:** Approximate priority only (§15.8)

### 16.4 Migration path from RabbitMQ

1. **Phase 1 — Shadow publish:** AMQP clients publish to both RabbitMQ and Kafka (dual-write)
2. **Phase 2 — Switch consumers:** Move consumers from RabbitMQ to Kafka's AMQP listener
3. **Phase 3 — Switch publishers:** Point publishers exclusively at Kafka's AMQP listener
4. **Phase 4 — Decommission RabbitMQ:** Remove RabbitMQ infrastructure

At each phase, the AMQP wire protocol is identical — no client code changes required. Only
connection endpoints change.

---

*Document version: 0.1 — 2026-04-16*
*Branch: feature/http-protocol*
