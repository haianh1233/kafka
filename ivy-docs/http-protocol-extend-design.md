# HTTP Protocol Extension: WebSocket Push & Message Routing — Design Document

## Table of Contents

1. [Overview](#1-overview)
2. [Goals & Non-Goals](#2-goals--non-goals)
3. [Core Design Principles](#3-core-design-principles)
   - 3.1 Cluster Transparency — Produce Anywhere, Consume Anywhere
   - 3.2 Queue as Logical View of a Topic
   - 3.3 Exchange Routing — Full AMQP Semantics
   - 3.4 Message Lifecycle Management
4. [Architecture Overview](#4-architecture-overview)
5. [WebSocket API Specification](#5-websocket-api-specification)
   - 5.1 Connection Lifecycle
   - 5.2 Control Messages (Client → Broker)
   - 5.3 Data Messages (Broker → Client)
   - 5.4 Exchange Operations
   - 5.5 Queue Operations
   - 5.6 Binding Operations
   - 5.7 Publish (Client → Broker → Routing → Queues)
   - 5.8 Subscribe / Unsubscribe
   - 5.9 Acknowledgements (ACK / NACK)
   - 5.10 Flow Control (Credits)
6. [HTTP REST API Extensions](#6-http-rest-api-extensions)
   - 6.1 Exchange Management
   - 6.2 Queue Management
   - 6.3 Binding Management
7. [Routing Engine](#7-routing-engine)
   - 7.1 Direct Exchange
   - 7.2 Topic Exchange (Wildcard)
   - 7.3 Fanout Exchange
   - 7.4 Headers Exchange
   - 7.5 Exchange-to-Exchange Routing
   - 7.6 Default Exchange
8. [Produce Path (WebSocket)](#8-produce-path-websocket)
9. [Consume Path (WebSocket Push)](#9-consume-path-websocket-push)
10. [Broker-to-Broker Forwarding — Produce & Consume Anywhere](#10-broker-to-broker-forwarding--produce--consume-anywhere)
11. [Queue: The Logical View](#11-queue-the-logical-view)
    - 11.1 Queue-to-Topic Mapping
    - 11.2 Queue Policies and Kafka Topic Configuration
    - 11.3 Queue Lifecycle (Auto-Create, Auto-Delete, Exclusive, Durable)
    - 11.4 Competing Consumers on a Queue
    - 11.5 Queue Depth and Observability
12. [Message Lifecycle](#12-message-lifecycle)
    - 12.1 Publish → Route → Enqueue → Deliver → ACK (Happy Path)
    - 12.2 NACK with Requeue — Redelivery
    - 12.3 NACK without Requeue — Dead-Letter Exchange (DLX)
    - 12.4 Message TTL — Per-Queue and Per-Message Expiration
    - 12.5 Priority Delivery
    - 12.6 Poison Message Protection
    - 12.7 Message Deduplication
13. [Routing-to-Kafka Mapping](#13-routing-to-kafka-mapping)
14. [Metadata Storage](#14-metadata-storage)
15. [Integration with Existing Infrastructure](#15-integration-with-existing-infrastructure)
16. [Module Changes: `http-server`](#16-module-changes-http-server)
17. [Configuration](#17-configuration)
18. [Error Handling](#18-error-handling)
19. [Security](#19-security)
20. [Implementation Plan](#20-implementation-plan)
21. [Implementation Concerns](#21-implementation-concerns)
22. [Comparison with Alternatives](#22-comparison-with-alternatives)

---

## 1. Overview

This document describes the design for extending the existing Kafka HTTP protocol layer with
**WebSocket push delivery** and **AMQP-style message routing** (exchanges, queues, bindings).
The extension runs on the **same HTTP port** (9094) — WebSocket connections are established
via the standard HTTP Upgrade mechanism, and routing management is accessible via both REST
endpoints and WebSocket control frames.

### Key Properties

| Property | Behavior |
|---|---|
| **Publish** | Client sends JSON `publish` frame over WebSocket → broker routes through exchange → matched queues (Kafka topics) → partition leaders |
| **Subscribe** | Client sends `subscribe` frame → broker starts pushing `deliver` frames from the backing Kafka topic as records arrive |
| **Routing** | AMQP-style: 4 exchange types (direct, topic/wildcard, fanout, headers) with queue bindings |
| **Transport** | WebSocket over HTTP/1.1 (same port as REST API, standard Upgrade handshake) |
| **Protocol** | JSON text frames for all control and data messages — no binary protocol |
| **REST API** | Exchange/queue/binding CRUD also available as HTTP REST endpoints |
| **Integration** | Same Netty pipeline, same `RequestChannel → KafkaApis` pipeline, same forwarding managers |
| **Compatibility** | Any WebSocket client (browser `WebSocket` API, wscat, Python websockets, Go gorilla/websocket) |

### Motivation

- **Push delivery:** The existing HTTP consume path is poll-based (`POST :fetch`). Clients
  must repeatedly poll, adding latency and wasted requests when no data is available. WebSocket
  gives server-push with sub-millisecond delivery latency.
- **Message routing:** Raw Kafka topics require producers to know partition topology. Exchange
  routing decouples publishers from consumers — publish to a logical exchange, subscribe to a
  logical queue, and the binding layer handles the routing.
- **Browser-native:** WebSocket is the only bidirectional push protocol available in browsers.
  Combined with JSON messages, this enables real-time dashboards, live feeds, and event-driven
  UIs consuming directly from Kafka.
- **Unified port:** No additional listener configuration. WebSocket and REST coexist on port 9094.

### Relationship to Existing HTTP Protocol

This is an **additive extension**, not a replacement. The entire existing HTTP API (§4 of
`http-protocol-design.md`) remains unchanged:

| Existing (unchanged) | New (this document) |
|---|---|
| `POST /v1/topics/{t}/records` — produce | `ws://broker:9094/v1/ws` — WebSocket endpoint |
| `POST /v1/topics/{t}/records:fetch` — poll consume | WebSocket `subscribe` + push `deliver` |
| `GET /v1/topics/{t}` — metadata | `POST /v1/exchanges` — exchange CRUD |
| `GET /v1/health` — health check | `POST /v1/queues` — queue CRUD |
| All existing endpoints | `POST /v1/bindings` — binding CRUD |

A client can mix HTTP REST and WebSocket freely. For example: publish via HTTP POST, consume
via WebSocket subscription. Or manage routing via REST, subscribe via WebSocket.

---

## 2. Goals & Non-Goals

### Goals

- WebSocket server-push consumption with per-message ACK/NACK
- Four AMQP-style exchange types: direct, topic (wildcard `*`/`#`), fanout, headers
- Queue declaration with automatic Kafka topic creation
- Binding management: queue-to-exchange and exchange-to-exchange
- Credit-based flow control (client controls delivery rate)
- REST API for exchange/queue/binding CRUD (manage routing without WebSocket)
- Cross-protocol interop: HTTP POST publish routed to WebSocket subscribers, and vice versa
- Zero new ports — WebSocket upgrade on existing HTTP listener
- Publisher confirms over WebSocket
- Wire-compatible with browser `WebSocket` API and standard WebSocket libraries

### Non-Goals

- Binary WebSocket frames (JSON only — keep it debuggable; binary can be added later)
- AMQP 0-9-1 wire protocol compatibility (use `amqp091-protocol-design.md` for that)
- WebSocket Compression Extension (permessage-deflate) in phase 1
- Consumer group rebalancing over WebSocket (use Kafka consumer groups or share groups)
- WebSocket multiplexing (one subscription set per connection; open multiple connections for isolation)
- Admin operations (topic create/delete, ACLs) — use binary protocol / `AdminClient`

---

## 3. Core Design Principles

### 3.1 Cluster Transparency — Produce Anywhere, Consume Anywhere

A client connects to **any broker** in the cluster and can publish to **any exchange** and
subscribe to **any queue**. The broker handles forwarding internally — the client never
needs to know which broker leads which partition.

```
                     ┌──────────────────────────────────────────────────┐
                     │               Kafka Cluster                      │
                     │                                                  │
  WS Client ──────►  Broker 3 (any)                                    │
  publish to         │                                                  │
  exchange="events"  │  RoutingEngine resolves:                         │
  routingKey=        │    exchange "events" (topic) →                   │
  "order.created"    │    queue "orders"      → topic ws.orders P0      │
                     │    queue "audit"       → topic ws.audit  P0      │
                     │                                                  │
                     │  ws.orders P0 leader = Broker 1   ─── FORWARD ──►│ Broker 1
                     │  ws.audit  P0 leader = Broker 3   ─── LOCAL      │
                     │                                                  │
  WS Client ──────►  Broker 2 (any)                                    │
  subscribe to       │                                                  │
  queue="orders"     │  ws.orders has P0 (Broker 1), P1 (Broker 3)     │
                     │  P0 ─── FetchForward to Broker 1 ───────────────►│ Broker 1
                     │  P1 ─── FetchForward to Broker 3 ───────────────►│ Broker 3
                     │  All partitions merged, pushed to WS client      │
                     │                                                  │
                     └──────────────────────────────────────────────────┘
```

**How it works:**
- **Publish:** `RoutingEngine` resolves exchange → queues → Kafka topics. For each topic-partition,
  `MetadataCache` identifies the leader. If the leader is this broker, `ReplicaManager.appendRecords()`
  handles it directly. If the leader is another broker, `ProduceForwardManager.forward()` sends a
  binary `ProduceRequest` to the correct leader. The client sees a single confirm.
- **Consume:** `WsConsumerFetchLoop` builds a `FetchRequest` for all assigned partitions.
  Local partitions are served by `ReplicaManager.fetchMessages()`. Remote partitions are fetched
  via `FetchForwardManager.forward()`. The client receives `deliver` frames from all partitions
  transparently.
- **Metadata:** Exchange/queue/binding definitions are stored in `__ws_routing_metadata` — a
  replicated Kafka topic. All brokers replay this topic on startup. A declaration on Broker 1
  is visible on Broker 3 within replication lag (~100ms).

**This means:**
- No client-side partition awareness needed
- No sticky routing to specific brokers
- Load balancers can distribute WebSocket connections freely across brokers
- Broker failures trigger automatic rebalance — clients reconnect to any surviving broker

### 3.2 Queue as Logical View of a Topic

A **queue** is the central abstraction that decouples AMQP semantics from Kafka internals.
The client thinks in queues; the broker thinks in topics.

```
┌─────────────────────────────────────────────────────────────────────┐
│                     Logical View (Client)                            │
│                                                                     │
│  Exchange "events"           Queue "orders"         WS Consumer     │
│  (topic type)                (logical name)         (subscribe)     │
│       │                          │                      │           │
│   publish ──► routing ──► enqueue ──► deliver ──► ack/nack          │
│   routingKey  (bindings)   (queue)    (push)      (per-msg)         │
│   "order.*"                                                         │
└─────────────────────────────────────────────────────────────────────┘
                              ║
                              ║  mapped by broker
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     Physical View (Kafka)                            │
│                                                                     │
│  ProduceRequest ──► Topic "ws.orders"  ──► FetchRequest ──► Offset  │
│  (key=routingKey)   Partition 0: B1        (consumer group           │
│                     Partition 1: B3         "ws.orders")             │
│                     Partition 2: B2                                  │
│                     retention.ms=86400000                            │
│                     retention.bytes=...                              │
└─────────────────────────────────────────────────────────────────────┘
```

**What the queue abstraction gives you:**

| Feature | Queue (logical) | Topic (physical) |
|---|---|---|
| **Name** | `"orders"` | `ws.orders` (prefixed) |
| **Subscribe** | `subscribe queue="orders"` | Consumer group, partition assignment |
| **Competing consumers** | Multiple subscribers auto-balance | Kafka consumer group rebalance |
| **Message TTL** | `x-message-ttl: 86400000` | `retention.ms=86400000` |
| **Dead-letter** | `x-dead-letter-exchange: "dlx"` | NACK → publish to DLX topic |
| **Max size** | `x-max-length-bytes: 1GB` | `retention.bytes=1073741824` |
| **Priority** | `x-max-priority: 10` | Sorted delivery within fetch batch |
| **Exclusive** | `exclusive: true` | Broker-enforced single-consumer |
| **Auto-delete** | `autoDelete: true` | Topic deleted on last unsubscribe |
| **Depth** | `messageCount` in declare response | `logEndOffset - committedOffset` |

**The client never sees topics, partitions, offsets, or consumer groups.** They declare a
queue, bind it to an exchange, subscribe, and receive messages. The broker manages all
Kafka machinery transparently.

### 3.3 Exchange Routing — Full AMQP Semantics

All four AMQP exchange types are first-class citizens, not approximations:

```
┌────────────────────────────────────────────────────────────────────────┐
│                        Exchange Routing Matrix                         │
│                                                                        │
│  ┌─────────┐     routingKey="order.created"                           │
│  │ DIRECT  │────► exact match: binding key == routing key              │
│  │         │     queue "orders" bound with "order.created" → ✓ MATCH  │
│  │         │     queue "payments" bound with "payment.done" → ✗       │
│  └─────────┘                                                          │
│                                                                        │
│  ┌─────────┐     routingKey="order.created"                           │
│  │ TOPIC   │────► wildcard match: * = 1 word, # = 0+ words            │
│  │(wildcard)│    queue "all-orders" bound with "order.#" → ✓ MATCH   │
│  │         │     queue "created" bound with "*.created" → ✓ MATCH    │
│  │         │     queue "payments" bound with "payment.#" → ✗         │
│  └─────────┘                                                          │
│                                                                        │
│  ┌─────────┐     routingKey ignored                                   │
│  │ FANOUT  │────► ALL bound queues receive the message                │
│  │         │     queue "service-a" → ✓                                │
│  │         │     queue "service-b" → ✓                                │
│  │         │     queue "service-c" → ✓                                │
│  └─────────┘                                                          │
│                                                                        │
│  ┌─────────┐     routingKey ignored, match on headers                 │
│  │ HEADERS │────► criteria matching with x-match: all|any              │
│  │         │     binding args: {priority:high, region:us, x-match:all}│
│  │         │     msg headers:  {priority:high, region:us} → ✓ MATCH  │
│  │         │     msg headers:  {priority:low,  region:us} → ✗        │
│  └─────────┘                                                          │
│                                                                        │
│  Exchange-to-Exchange:  source → destination (recursive, cycle-safe)  │
│  Default Exchange (""):  routingKey = queueName (implicit binding)    │
└────────────────────────────────────────────────────────────────────────┘
```

**One publish → multiple queues → multiple topics → multiple consumers:**

A single `publish` to a fanout exchange with 3 bound queues produces to 3 Kafka topics
simultaneously. Each topic may have consumers from different protocols (WebSocket, HTTP,
Kafka binary). The routing engine fans out, the forwarding layer handles leader routing,
and `CompletableFuture.allOf()` joins the results.

### 3.4 Message Lifecycle Management

Every message has a well-defined lifecycle from publish to final disposition:

```
                                    ┌──────────┐
                                    │  PUBLISH  │
                                    └─────┬────┘
                                          │
                                    ┌─────▼────┐
                                    │  ROUTE   │ exchange → bindings → queues
                                    └─────┬────┘
                                          │
                          ┌───────────────┼───────────────┐
                          ▼               ▼               ▼
                    ┌──────────┐   ┌──────────┐    ┌──────────┐
                    │ Queue A  │   │ Queue B  │    │ Queue C  │  (Kafka topics)
                    │ ws.a     │   │ ws.b     │    │ ws.c     │
                    └─────┬────┘   └──────────┘    └──────────┘
                          │
                    ┌─────▼────┐
                    │ DELIVER  │ push via WebSocket (credit-controlled)
                    └─────┬────┘
                          │
              ┌───────────┼───────────┐
              ▼           ▼           ▼
        ┌──────────┐ ┌────────┐ ┌────────────┐
        │   ACK    │ │ NACK   │ │   NACK     │
        │          │ │requeue │ │ no requeue │
        └─────┬────┘ └───┬────┘ └─────┬──────┘
              │          │            │
              ▼          ▼            ▼
        ┌──────────┐ ┌────────┐ ┌────────────────┐
        │  COMMIT  │ │REDELIVER│ │  DLX ROUTE    │ if x-dead-letter-exchange set
        │  offset  │ │  later │ │  or DISCARD   │ otherwise
        └──────────┘ └────────┘ └───────┬────────┘
                                        │
                                  ┌─────▼────────┐
                                  │ DLX Exchange  │
                                  │ → DLQ Queue   │ (with x-death headers)
                                  └──────────────┘
```

**Key lifecycle features:**

| Feature | How it works |
|---|---|
| **Publisher confirms** | Broker ACKs/NACKs each publish after Kafka write completes |
| **Mandatory routing** | Unroutable messages returned to publisher (not silently dropped) |
| **At-least-once delivery** | Messages delivered until ACK'd; redelivered on NACK/timeout |
| **Dead-letter exchange** | NACK without requeue → route to DLX → DLQ for investigation |
| **Message TTL** | Per-queue (`x-message-ttl`) maps to Kafka `retention.ms`; per-message (`expiration`) checked at delivery time |
| **Priority** | `x-max-priority` enables sorted delivery within fetch batches |
| **Poison message protection** | `x-delivery-count` header tracks redeliveries; auto-DLX after configurable max |
| **Credit flow control** | Client controls push rate — no broker-side OOM on slow consumers |
| **Redelivery** | NACK with `requeue: true` → message redelivered (potentially to different consumer) |
| **Offset commit batching** | ACKs batched into periodic offset commits (configurable interval) |

---

## 4. Architecture Overview

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                              Kafka Broker                                        │
│                                                                                  │
│  Port 9094 (HTTP + WebSocket)                                                    │
│  ┌────────────────────────────────────────────────────────────────┐              │
│  │                    HttpAcceptor (Netty)                         │              │
│  │                                                                 │              │
│  │  ┌─────────────────────────────────────────────────────────┐   │              │
│  │  │  HttpChannelInitializer                                  │   │              │
│  │  │  ┌───────────────┐ ┌──────────────┐ ┌───────────────┐  │   │              │
│  │  │  │HttpServerCodec│ │HttpAggregator│ │HttpReqHandler │  │   │              │
│  │  │  └───────┬───────┘ └──────┬───────┘ └───────┬───────┘  │   │              │
│  │  │          │                │                  │           │   │              │
│  │  │          │   ┌────────────▼──────────────┐   │           │   │              │
│  │  │          │   │  Is WebSocket Upgrade?     │   │           │   │              │
│  │  │          │   │  Upgrade: websocket        │   │           │   │              │
│  │  │          │   └─────┬──────────┬──────────┘   │           │   │              │
│  │  │          │    NO   │          │  YES          │           │   │              │
│  │  │          │         ▼          ▼               │           │   │              │
│  │  │          │   ┌──────────┐ ┌──────────────┐   │           │   │              │
│  │  │          │   │ Standard │ │ WS Protocol  │   │           │   │              │
│  │  │          │   │ HTTP flow│ │ Handler      │   │           │   │              │
│  │  │          │   │ (§4 of   │ │ (handshake)  │   │           │   │              │
│  │  │          │   │  base    │ │      │       │   │           │   │              │
│  │  │          │   │  design) │ │      ▼       │   │           │   │              │
│  │  │          │   │          │ │ WsFrameHandler│  │           │   │              │
│  │  │          │   │          │ │ (JSON frames) │  │           │   │              │
│  │  │          │   └──────────┘ └──────┬───────┘   │           │   │              │
│  │  └──────────────────────────────────┼───────────┘           │   │              │
│  └─────────────────────────────────────┼───────────────────────┘   │              │
│                                        │                            │              │
│         ┌──────────────────────────────┼────────────────┐          │              │
│         │         Routing Engine (§6)   │                │          │              │
│         │  ┌──────────┐ ┌──────────┐ ┌─▼────────────┐  │          │              │
│         │  │ Exchange  │ │ Binding  │ │ Subscription │  │          │              │
│         │  │ Manager   │ │ Manager  │ │ Manager      │  │          │              │
│         │  └──────────┘ └──────────┘ └──────────────┘  │          │              │
│         └──────────────────────────────┬────────────────┘          │              │
│                                        │                            │              │
│              ┌─────────────────────────┼──────────────┐            │              │
│              │                         ▼              │            │              │
│              │             ┌───────────────────┐      │            │              │
│              │             │  RequestChannel   │      │            │              │
│              │             └─────────┬─────────┘      │            │              │
│              │                       ▼                │            │              │
│              │             ┌───────────────────┐      │            │              │
│              │             │  KafkaApis        │      │            │              │
│              │             │  handleHttpProduce│      │            │              │
│              │             │  handleHttpConsume│      │            │              │
│              │             └────────┬──────────┘      │            │              │
│              │                      │                 │            │              │
│              │           ┌──────────▼──────────┐      │            │              │
│              │           │  ReplicaManager     │      │            │              │
│              │           │  + ForwardManagers  │      │            │              │
│              │           └─────────────────────┘      │            │              │
│              └────────────────────────────────────────┘            │              │
│                                                                    │              │
│  ┌────────────────────────────────────────┐                       │              │
│  │  Metadata Store: __ws_routing_metadata │                       │              │
│  │  (exchanges, queues, bindings)          │                       │              │
│  └────────────────────────────────────────┘                       │              │
└──────────────────────────────────────────────────────────────────────────────────┘
```

**Key architectural principle:** WebSocket connections are upgraded HTTP connections on the
**same port**. The `HttpChannelInitializer` detects the WebSocket upgrade request and
switches the Netty pipeline from HTTP frame handling to WebSocket frame handling. After
upgrade, the connection is a full-duplex WebSocket with JSON text frames.

### Thread Model

```
  Port 9094                      ┌─────────────────────────────────────┐
  HTTP + WS connections ────────►│  Netty Worker Threads                │
                                 │  (num.http.network.threads, default=4)
                                 │                                     │
                                 │  HTTP requests:                     │
                                 │    HttpRequestHandler → RequestChannel
                                 │                                     │
                                 │  WebSocket frames:                  │
                                 │    WsFrameHandler → dispatch:       │
                                 │      control msgs → RoutingEngine   │
                                 │      publish msgs → RequestChannel  │
                                 │      ack/nack     → OffsetManager   │
                                 └──────────────────┬──────────────────┘
                                                    │
                      ┌─────────────────────────────┼──────────────────────┐
                      │                             │                      │
           ┌──────────▼──────────┐   ┌──────────────▼────────────┐       │
           │ WsConsumerManager   │   │ KafkaRequestHandler pool  │       │
           │ (dedicated thread   │   │ (shared, unchanged)       │       │
           │  pool, pushes       │   │                           │       │
           │  deliver frames)    │   │ ReplicaManager.append()   │       │
           │                     │   │ ProduceForwardManager     │       │
           │ Per-subscription    │   │ FetchForwardManager       │       │
           │ fetch loop          │   │                           │       │
           └─────────────────────┘   └───────────────────────────┘       │
                                                                         │
                      ┌──────────────────────────────────────────────────┘
                      │
           ┌──────────▼──────────┐
           │ RoutingMetadata     │
           │ Manager             │
           │ (replays            │
           │  __ws_routing_meta  │
           │  on startup)        │
           └─────────────────────┘
```

The **WsConsumerManager** runs consumer fetch loops on a dedicated thread pool
(`num.ws.consumer.threads`, default 8). Each subscription gets a fetch task that pulls from
Kafka and pushes `deliver` frames to the WebSocket connection. This pool is separate from
Netty workers and `KafkaRequestHandler` threads — neither is ever blocked by consumer fetches.

---

## 4. WebSocket API Specification

### 4.1 Connection Lifecycle

#### 4.1.1 WebSocket Upgrade

```
GET /v1/ws HTTP/1.1
Host: broker1:9094
Upgrade: websocket
Connection: Upgrade
Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==
Sec-WebSocket-Version: 13
Authorization: Bearer <token>          ← optional, same auth as HTTP (§16)
```

```
HTTP/1.1 101 Switching Protocols
Upgrade: websocket
Connection: Upgrade
Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=
```

After the 101 response, the connection speaks WebSocket frames. Authentication is extracted
from the upgrade request (same `KafkaPrincipalBuilder` path as HTTP — see §12.1 of base
HTTP design).

#### 4.1.2 Connection Metadata

Immediately after upgrade, the broker sends a `connected` message:

```json
{
  "type": "connected",
  "brokerId": 3,
  "clusterId": "abc123",
  "sessionId": "ws-3-af72b1c4",
  "serverCapabilities": ["exchange.direct", "exchange.topic", "exchange.fanout",
                         "exchange.headers", "publisher-confirms", "credits"]
}
```

The `sessionId` uniquely identifies this WebSocket session for logging and debugging.

#### 4.1.3 Connection Close

Either side may close the WebSocket:

- **Normal close:** WebSocket close frame with code 1000 (Normal Closure)
- **Broker shutdown:** Close frame with code 1001 (Going Away), message "Broker shutting down"
- **Auth failure:** Close frame with code 4403, message "Access denied"
- **Protocol error:** Close frame with code 4400, message describing the error

On close, all subscriptions on this connection are cancelled, pending ACKs are discarded
(offsets not committed), and resources are released.

---

### 4.2 Control Messages (Client → Broker)

All messages are JSON text frames with a required `type` field:

```json
{ "type": "<message-type>", "id": "<optional-correlation-id>", ... }
```

The optional `id` field is an opaque string echoed back in the response for client-side
correlation. If omitted, the response has no `id`.

**Message types (client → broker):**

| Type | Purpose | Response |
|---|---|---|
| `declare-exchange` | Create/assert an exchange | `exchange-declared` |
| `delete-exchange` | Delete an exchange | `exchange-deleted` |
| `declare-queue` | Create/assert a queue | `queue-declared` |
| `delete-queue` | Delete a queue | `queue-deleted` |
| `bind` | Bind queue to exchange | `bound` |
| `unbind` | Remove binding | `unbound` |
| `publish` | Publish message to exchange | `published` (if confirms enabled) |
| `subscribe` | Subscribe to a queue | `subscribed` |
| `unsubscribe` | Cancel subscription | `unsubscribed` |
| `ack` | Acknowledge delivered message(s) | (none) |
| `nack` | Negative-acknowledge message(s) | (none) |
| `credits` | Grant delivery credits | (none) |
| `enable-confirms` | Enable publisher confirms | `confirms-enabled` |

### 4.3 Data Messages (Broker → Client)

| Type | Purpose | Notes |
|---|---|---|
| `connected` | Session established | Sent once after upgrade |
| `deliver` | Push a message to subscriber | One per record |
| `published` | Publisher confirm (success) | Only if confirms enabled |
| `publish-failed` | Publisher confirm (failure) | Only if confirms enabled |
| `returned` | Mandatory message unroutable | Only if `mandatory: true` |
| `error` | Error response to a control msg | Carries `id` from request |
| `exchange-declared` | Exchange declare response | |
| `queue-declared` | Queue declare response | |
| `bound` / `unbound` | Binding response | |
| `subscribed` / `unsubscribed` | Subscription response | |

---

### 4.4 Exchange Operations

#### 4.4.1 Declare Exchange

```json
{
  "type": "declare-exchange",
  "id": "req-1",
  "exchange": "events",
  "exchangeType": "topic",
  "durable": true,
  "autoDelete": false,
  "arguments": {}
}
```

Response:

```json
{
  "type": "exchange-declared",
  "id": "req-1",
  "exchange": "events"
}
```

| Field | Type | Default | Description |
|---|---|---|---|
| `exchange` | string | required | Exchange name |
| `exchangeType` | `"direct"` \| `"topic"` \| `"fanout"` \| `"headers"` | `"direct"` | Routing algorithm |
| `durable` | boolean | `true` | Survive broker restart |
| `autoDelete` | boolean | `false` | Delete when last binding removed |
| `arguments` | object | `{}` | Exchange-specific arguments (e.g., `alternate-exchange`) |

**Idempotency:** Declaring an exchange that already exists with the **same type** is a no-op.
Declaring with a **different type** returns an error.

**Pre-declared exchanges** (always exist, cannot be deleted):

| Name | Type | Description |
|---|---|---|
| `""` (empty) | direct | Default exchange — routing key = queue name |
| `amq.direct` | direct | Standard direct exchange |
| `amq.topic` | topic | Standard topic exchange |
| `amq.fanout` | fanout | Standard fanout exchange |
| `amq.headers` | headers | Standard headers exchange |

#### 4.4.2 Delete Exchange

```json
{
  "type": "delete-exchange",
  "id": "req-2",
  "exchange": "events",
  "ifUnused": false
}
```

---

### 4.5 Queue Operations

#### 4.5.1 Declare Queue

```json
{
  "type": "declare-queue",
  "id": "req-3",
  "queue": "order-events",
  "durable": true,
  "exclusive": false,
  "autoDelete": false,
  "arguments": {
    "x-message-ttl": 86400000,
    "x-dead-letter-exchange": "dlx",
    "x-dead-letter-routing-key": "dead"
  }
}
```

Response:

```json
{
  "type": "queue-declared",
  "id": "req-3",
  "queue": "order-events",
  "messageCount": 0,
  "consumerCount": 0
}
```

| Field | Type | Default | Description |
|---|---|---|---|
| `queue` | string | `""` → server-generated | Queue name (empty = auto-name `q.gen-<UUID>`) |
| `durable` | boolean | `true` | Survive broker restart |
| `exclusive` | boolean | `false` | Only this connection can consume |
| `autoDelete` | boolean | `false` | Delete when last subscriber disconnects |
| `arguments` | object | `{}` | Queue policies (see table below) |

**Queue arguments:**

| Key | Type | Kafka Mapping |
|---|---|---|
| `x-message-ttl` | int (ms) | Topic `retention.ms` |
| `x-max-length-bytes` | int | Topic `retention.bytes` |
| `x-dead-letter-exchange` | string | DLX routing on NACK (§18.6) |
| `x-dead-letter-routing-key` | string | DLX routing key override |
| `x-max-priority` | int (0-255) | Priority delivery ordering (§18.7) |

**Kafka topic creation:** `declare-queue` auto-creates a Kafka topic named
`ws.{queueName}` with `ws.default.queue.partitions` partitions (default 1).

#### 4.5.2 Delete Queue

```json
{
  "type": "delete-queue",
  "id": "req-4",
  "queue": "order-events",
  "ifUnused": false,
  "ifEmpty": false
}
```

---

### 4.6 Binding Operations

#### 4.6.1 Bind Queue to Exchange

```json
{
  "type": "bind",
  "id": "req-5",
  "queue": "order-events",
  "exchange": "events",
  "routingKey": "order.created",
  "arguments": {}
}
```

Response:

```json
{
  "type": "bound",
  "id": "req-5",
  "queue": "order-events",
  "exchange": "events",
  "routingKey": "order.created"
}
```

For **headers exchange**, the `arguments` object contains matching criteria:

```json
{
  "type": "bind",
  "id": "req-6",
  "queue": "important-orders",
  "exchange": "amq.headers",
  "routingKey": "",
  "arguments": {
    "x-match": "all",
    "priority": "high",
    "region": "us-east"
  }
}
```

#### 4.6.2 Bind Exchange to Exchange

```json
{
  "type": "bind",
  "id": "req-7",
  "source": "events",
  "destination": "errors",
  "routingKey": "*.error",
  "arguments": {}
}
```

When `source` and `destination` are both present (no `queue`), this creates an
exchange-to-exchange binding. Messages matching the binding on the source exchange are
also routed to the destination exchange.

#### 4.6.3 Unbind

```json
{
  "type": "unbind",
  "id": "req-8",
  "queue": "order-events",
  "exchange": "events",
  "routingKey": "order.created"
}
```

---

### 4.7 Publish (Client → Broker → Routing → Queues)

```json
{
  "type": "publish",
  "exchange": "events",
  "routingKey": "order.created",
  "mandatory": false,
  "message": {
    "body": { "orderId": "12345", "amount": 42.0 },
    "contentType": "application/json",
    "headers": {
      "source": "checkout-service",
      "priority": "high"
    },
    "deliveryMode": 2,
    "correlationId": "req-123",
    "replyTo": "replies",
    "expiration": "60000",
    "messageId": "msg-abc",
    "timestamp": 1713260400,
    "appId": "checkout-service"
  },
  "publishId": 1
}
```

| Field | Type | Default | Description |
|---|---|---|---|
| `exchange` | string | `""` (default) | Target exchange |
| `routingKey` | string | `""` | Routing key for exchange matching |
| `mandatory` | boolean | `false` | Return if unroutable |
| `message.body` | any | required | Message payload (JSON value, string, or base64 for binary) |
| `message.contentType` | string | `null` | MIME type |
| `message.headers` | object | `{}` | Application headers (used by headers exchange) |
| `message.deliveryMode` | int | `2` | 1=non-persistent, 2=persistent |
| `message.correlationId` | string | `null` | Correlation identifier |
| `message.replyTo` | string | `null` | Reply routing key |
| `message.expiration` | string | `null` | Per-message TTL in milliseconds |
| `message.messageId` | string | `null` | Application message ID |
| `message.timestamp` | long | `null` | Unix epoch seconds |
| `message.appId` | string | `null` | Application identifier |
| `publishId` | long | `null` | Sequence number for publisher confirms |

**Body encoding:**
- If `contentType` is `application/json` or `body` is a JSON object/array: stored as-is
- If `body` is a string: stored as UTF-8 bytes
- If `contentType` is `application/octet-stream`: `body` must be base64-encoded string

**Publisher confirm response** (when confirms enabled):

```json
{ "type": "published", "publishId": 1 }
```

or on failure:

```json
{ "type": "publish-failed", "publishId": 1, "errorCode": "NOT_ENOUGH_REPLICAS",
  "errorMessage": "ISR below minimum" }
```

**Mandatory return** (when `mandatory: true` and no queues match):

```json
{
  "type": "returned",
  "exchange": "events",
  "routingKey": "order.xxx",
  "replyCode": 312,
  "replyText": "NO_ROUTE",
  "message": { ... }
}
```

---

### 4.8 Subscribe / Unsubscribe

#### 4.8.1 Subscribe

```json
{
  "type": "subscribe",
  "id": "req-10",
  "queue": "order-events",
  "subscriptionId": "sub-1",
  "credits": 100,
  "startOffset": "latest",
  "noAck": false,
  "exclusive": false
}
```

| Field | Type | Default | Description |
|---|---|---|---|
| `queue` | string | required | Queue name to subscribe to |
| `subscriptionId` | string | auto-generated | Client-chosen ID for this subscription |
| `credits` | int | `100` | Initial delivery credits (flow control, §4.10) |
| `startOffset` | `"latest"` \| `"earliest"` \| number | `"latest"` | Where to start consuming |
| `noAck` | boolean | `false` | If true, messages don't require ACK (auto-ack) |
| `exclusive` | boolean | `false` | If true, only one subscriber allowed on this queue |

Response:

```json
{
  "type": "subscribed",
  "id": "req-10",
  "subscriptionId": "sub-1",
  "queue": "order-events"
}
```

After `subscribed`, the broker begins pushing `deliver` messages (§4.3).

**Multiple subscriptions:** A single WebSocket connection can have multiple active
subscriptions to different queues. Each subscription has its own `subscriptionId`,
credit counter, and delivery tag sequence.

#### 4.8.2 Unsubscribe

```json
{
  "type": "unsubscribe",
  "id": "req-11",
  "subscriptionId": "sub-1"
}
```

Cancels the subscription. Outstanding unacked messages for this subscription are discarded
(offsets not committed). The consumer fetch loop is stopped.

---

### 4.9 Acknowledgements (ACK / NACK)

#### 4.9.1 ACK

```json
{
  "type": "ack",
  "subscriptionId": "sub-1",
  "deliveryTag": 5,
  "multiple": true
}
```

| Field | Type | Default | Description |
|---|---|---|---|
| `subscriptionId` | string | required | Which subscription this ACK is for |
| `deliveryTag` | long | required | Tag from the `deliver` message |
| `multiple` | boolean | `false` | If true, ACK all tags ≤ `deliveryTag` |

**Effect:** Commits offsets to `__consumer_offsets` for the ACK'd messages. Releases
delivery credits (allowing more messages to be pushed).

#### 4.9.2 NACK

```json
{
  "type": "nack",
  "subscriptionId": "sub-1",
  "deliveryTag": 5,
  "multiple": false,
  "requeue": true
}
```

| Field | Type | Default | Description |
|---|---|---|---|
| `requeue` | boolean | `true` | If true, redeliver later. If false, dead-letter or discard. |

**`requeue: true`** — offset not committed; message will be redelivered on next fetch
iteration (delivery may go to a different consumer in competing-consumer setups).

**`requeue: false`** — if queue has `x-dead-letter-exchange`, republish to DLX. Otherwise,
commit offset (message lost). See §18.6.

---

### 4.10 Flow Control (Credits)

Credit-based flow control prevents the broker from overwhelming a slow consumer:

```json
{
  "type": "credits",
  "subscriptionId": "sub-1",
  "credits": 50
}
```

**How it works:**
1. On `subscribe`, the client specifies initial `credits` (default 100)
2. Each `deliver` message consumes 1 credit
3. When credits reach 0, the broker pauses delivery for that subscription
4. Client sends `credits` to grant more (additive to remaining)
5. `ack` with `multiple: true` does NOT auto-replenish credits (explicit `credits` required)

This gives the client precise control over delivery rate — critical for browser clients
that may process messages slower than Kafka can produce them.

**No-ack mode:** When `noAck: true`, credits still control delivery rate. The broker
auto-acks but respects the credit window.

---

## 5. HTTP REST API Extensions

These REST endpoints manage routing metadata. They complement the WebSocket control messages
— both operate on the same in-memory routing state and `__ws_routing_metadata` topic.

### 5.1 Exchange Management

#### Declare Exchange — `PUT /v1/exchanges/{exchange}`

```json
PUT /v1/exchanges/events

{
  "exchangeType": "topic",
  "durable": true,
  "autoDelete": false,
  "arguments": {}
}
```

Response (201 Created or 200 OK if already exists with same type):

```json
{
  "exchange": "events",
  "exchangeType": "topic",
  "durable": true,
  "autoDelete": false
}
```

#### List Exchanges — `GET /v1/exchanges`

```json
{
  "exchanges": [
    { "exchange": "", "exchangeType": "direct", "durable": true },
    { "exchange": "amq.direct", "exchangeType": "direct", "durable": true },
    { "exchange": "amq.topic", "exchangeType": "topic", "durable": true },
    { "exchange": "amq.fanout", "exchangeType": "fanout", "durable": true },
    { "exchange": "amq.headers", "exchangeType": "headers", "durable": true },
    { "exchange": "events", "exchangeType": "topic", "durable": true }
  ]
}
```

#### Delete Exchange — `DELETE /v1/exchanges/{exchange}?ifUnused=false`

Returns 204 No Content on success.

### 5.2 Queue Management

#### Declare Queue — `PUT /v1/queues/{queue}`

```json
PUT /v1/queues/order-events

{
  "durable": true,
  "arguments": {
    "x-message-ttl": 86400000
  }
}
```

Response (201 Created or 200 OK):

```json
{
  "queue": "order-events",
  "durable": true,
  "messageCount": 0,
  "consumerCount": 0
}
```

#### List Queues — `GET /v1/queues`

#### Delete Queue — `DELETE /v1/queues/{queue}?ifUnused=false&ifEmpty=false`

#### Purge Queue — `DELETE /v1/queues/{queue}/messages`

### 5.3 Binding Management

#### Create Binding — `POST /v1/bindings`

```json
{
  "exchange": "events",
  "queue": "order-events",
  "routingKey": "order.*",
  "arguments": {}
}
```

Response (201 Created):

```json
{
  "exchange": "events",
  "queue": "order-events",
  "routingKey": "order.*"
}
```

#### List Bindings — `GET /v1/bindings?exchange=events`

#### Delete Binding — `DELETE /v1/bindings`

```json
{
  "exchange": "events",
  "queue": "order-events",
  "routingKey": "order.*"
}
```

---

## 6. Routing Engine

The routing engine resolves `(exchange, routingKey, headers)` → `Set<String> matchedQueues`.
It is an in-memory data structure backed by the `__ws_routing_metadata` topic and follows
the patterns from the ivy-ref `Amqp091RoutingEngine`.

### 6.1 Direct Exchange

**Algorithm:** Exact string equality between `message.routingKey` and `binding.routingKey`.

```
Exchange "orders" (type=direct):
  Binding: queue="payment-events", routingKey="payment.completed"
  Binding: queue="order-events",   routingKey="order.created"

Publish: routingKey="order.created" → matches ["order-events"]
Publish: routingKey="order.updated" → matches [] (no match)
```

**Implementation:** `HashMap<String, List<String>>` keyed by routing key → list of queue
names. O(1) lookup.

### 6.2 Topic Exchange (Wildcard)

**Algorithm:** Dot-separated word matching with `*` (one word) and `#` (zero or more words).

```
Exchange "events" (type=topic):
  Binding: queue="all-orders",    routingKey="order.#"
  Binding: queue="order-created", routingKey="order.created"
  Binding: queue="any-created",   routingKey="*.created"
  Binding: queue="everything",    routingKey="#"

Publish: routingKey="order.created"
  → matches ["all-orders", "order-created", "any-created", "everything"]

Publish: routingKey="payment.completed"
  → matches ["everything"]

Publish: routingKey="order.payment.created"
  → matches ["all-orders", "everything"]
  (*.created does NOT match — * is exactly one word)
```

**Implementation (from ivy-ref pattern):** Per-publish word-by-word walk:

```java
boolean topicMatches(String[] patternWords, String[] routingWords) {
    int pi = 0, ri = 0;
    while (pi < patternWords.length) {
        if (patternWords[pi].equals("#")) {
            // # at end of pattern matches everything remaining
            if (pi == patternWords.length - 1) return true;
            // # in middle: try matching rest of pattern against remaining routing words
            pi++;
            while (ri < routingWords.length) {
                if (topicMatches(patternWords, pi, routingWords, ri)) return true;
                ri++;
            }
            return false;
        }
        if (ri >= routingWords.length) return false;
        if (!patternWords[pi].equals("*") && !patternWords[pi].equals(routingWords[ri]))
            return false;
        pi++; ri++;
    }
    return ri == routingWords.length;
}
```

**Optimization:** For exchanges with many bindings, pre-compile into a trie structure
indexed by the first word. This reduces the per-publish scan from O(N bindings) to
O(matching-prefix bindings). The trie is rebuilt on binding changes (rare).

### 6.3 Fanout Exchange

**Algorithm:** All bound queues receive the message. Routing key is ignored.

```
Exchange "broadcast" (type=fanout):
  Binding: queue="service-a"
  Binding: queue="service-b"
  Binding: queue="service-c"

Publish: routingKey="anything" → matches ["service-a", "service-b", "service-c"]
```

**Implementation:** Simple `List<String>` of queue names. O(K) where K = bound queues.

### 6.4 Headers Exchange

**Algorithm:** Match message headers against binding argument criteria.

```
Exchange "amq.headers" (type=headers):
  Binding: queue="priority-us",
           arguments={"x-match":"all", "priority":"high", "region":"us-east"}
  Binding: queue="any-high",
           arguments={"x-match":"any", "priority":"high", "region":"eu-west"}

Publish: headers={"priority":"high", "region":"us-east"}
  → "priority-us" matches (all criteria met)
  → "any-high" matches (priority=high matches, x-match=any requires only 1)
  → matches ["priority-us", "any-high"]

Publish: headers={"priority":"low", "region":"eu-west"}
  → "priority-us" does NOT match (priority≠high)
  → "any-high" matches (region=eu-west matches)
  → matches ["any-high"]
```

**Implementation (from ivy-ref `headersMatch` pattern):**

```java
boolean headersMatch(Map<String, String> criteria, Map<String, String> msgHeaders) {
    String xMatch = criteria.getOrDefault("x-match", "all");
    boolean matchAll = "all".equals(xMatch);

    int criteriaCount = 0, matchCount = 0;
    for (Map.Entry<String, String> entry : criteria.entrySet()) {
        if ("x-match".equals(entry.getKey())) continue;
        criteriaCount++;
        String msgVal = msgHeaders.get(entry.getKey());
        if (entry.getValue().equals(msgVal)) matchCount++;
    }

    if (criteriaCount == 0) return true;  // empty criteria matches everything
    return matchAll ? matchCount == criteriaCount : matchCount > 0;
}
```

### 6.5 Exchange-to-Exchange Routing

Messages entering a source exchange that match an e2e binding are also routed through the
destination exchange. Recursive routing with a **visited set** prevents infinite cycles:

```java
Set<String> routeRecursive(String exchange, String routingKey,
                           Map<String, String> headers,
                           Set<String> matchedQueues,
                           Set<String> visited) {
    if (!visited.add(exchange)) return matchedQueues;  // cycle guard

    ExchangeEntry entry = exchanges.get(exchange);
    // 1. Match queue bindings
    for (Binding b : entry.bindings) {
        if (matches(entry.type, b, routingKey, headers))
            matchedQueues.add(b.queue);
    }
    // 2. Match e2e bindings → recurse
    for (E2EBinding e2e : entry.e2eBindings) {
        if (matches(entry.type, e2e, routingKey, headers))
            routeRecursive(e2e.destination, routingKey, headers, matchedQueues, visited);
    }
    return matchedQueues;
}
```

### 6.6 Default Exchange

The default exchange (`""`) routes by queue name: `routingKey = queueName`. Every declared
queue is implicitly bound to the default exchange with `routingKey = queue.name`.

```
Publish: exchange="", routingKey="order-events"
  → direct match → queue "order-events"
```

This allows simple publish-to-queue-by-name without explicit binding setup.

---

## 7. Produce Path (WebSocket)

### 7.1 End-to-end: WebSocket publish → exchange routing → Kafka

```
WebSocket Client         Broker 3                                   Broker 1
     │                      │                                          │
     │  { "type":"publish",  │                                          │
     │    "exchange":"events",                                          │
     │    "routingKey":       │                                          │
     │    "order.created",    │                                          │
     │    "message":{...},    │                                          │
     │    "publishId": 1 }    │                                          │
     │──────────────────────►│                                          │
     │                       │                                          │
     │              ┌────────▼────────────────────────────────┐        │
     │              │  WsFrameHandler                          │        │
     │              │                                          │        │
     │              │  1. Parse JSON publish frame              │        │
     │              │                                          │        │
     │              │  2. RoutingEngine.route("events",         │        │
     │              │     "order.created", headers)             │        │
     │              │     → matched queues: ["order-events",    │        │
     │              │        "audit-log"]                       │        │
     │              │                                          │        │
     │              │  3. Resolve queues → Kafka topics:        │        │
     │              │     "order-events" → "ws.order-events"   │        │
     │              │     "audit-log"    → "ws.audit-log"      │        │
     │              │                                          │        │
     │              │  4. For each topic, determine partition:  │        │
     │              │     key = routingKey bytes                │        │
     │              │     partition = murmur2(key) % count      │        │
     │              │                                          │        │
     │              │  5. Build ProduceRequest for each topic   │        │
     │              │     (records include routing metadata     │        │
     │              │      as Kafka headers — see §10.3)       │        │
     │              │                                          │        │
     │              │  6. MetadataCache: ws.order-events P0     │        │
     │              │     leader = Broker 1 (remote)            │        │
     │              │     ws.audit-log P0                       │        │
     │              │     leader = Broker 3 (local)             │        │
     │              └────────┬────────────────────────────────┘        │
     │                       │                                          │
     │         ┌─────────────┼──────────────────────┐                  │
     │   LOCAL │             │               REMOTE │                   │
     │   ws.audit-log P0     │           ws.order-events P0            │
     │         │             │                      │                   │
     │   ReplicaManager      │    ProduceForwardManager.forward(1,...) │
     │   .appendRecords()    │                      │                   │
     │         │             │    binary ProduceRequest                 │
     │         │             │    ─────────────────────────────────────►│
     │         │             │                      │                   │
     │   future_local        │    ProduceResponse { offset:42 }        │
     │   .complete()         │    ◄────────────────────────────────────│
     │         │             │    future_remote.complete()              │
     │         └─────────────┼──────────────────────┘                  │
     │                       │                                          │
     │              CompletableFuture.allOf(local, remote)              │
     │              .handleAsync(wsAsyncExecutor)                       │
     │                       │                                          │
     │  (if confirms enabled)│                                          │
     │  {"type":"published", │                                          │
     │   "publishId":1}      │                                          │
     │◄──────────────────────│                                          │
```

### 7.2 HTTP POST publish with exchange routing

The existing `POST /v1/topics/{t}/records` endpoint is **unchanged**. But a new optional
header enables exchange routing for HTTP produces too:

```
POST /v1/topics/{exchange}/records:route
X-Routing-Key: order.created
```

This header tells the broker to treat the topic path as an exchange name and route through
the exchange bindings instead of producing directly to a topic.

### 7.3 Cross-protocol interop

| Publish via | Consume via | How it works |
|---|---|---|
| WebSocket `publish` | WebSocket `subscribe` | Publish routes through exchange → queue topic → WS consumer fetches |
| WebSocket `publish` | HTTP `POST :fetch` | Same: routing → topic → HTTP poll |
| WebSocket `publish` | Kafka binary consumer | Same: routing → topic → Kafka consumer |
| HTTP `POST records` | WebSocket `subscribe` | Direct topic produce → WS consumer on same topic |
| Kafka binary producer | WebSocket `subscribe` | Direct topic produce → WS consumer on same topic |

The key: the routing layer writes to standard Kafka topics. Any consumer protocol can
read from those topics.

---

## 8. Consume Path (WebSocket Push)

### 8.1 End-to-end: subscribe → fetch loop → deliver

```
WebSocket Client         Broker 3                                   Kafka Log
     │                      │                                          │
     │  {"type":"subscribe", │                                          │
     │   "queue":            │                                          │
     │   "order-events",     │                                          │
     │   "subscriptionId":   │                                          │
     │   "sub-1",            │                                          │
     │   "credits":100,      │                                          │
     │   "startOffset":      │                                          │
     │   "latest"}           │                                          │
     │──────────────────────►│                                          │
     │                       │                                          │
     │              ┌────────▼────────────────────────────────┐        │
     │              │  WsFrameHandler                          │        │
     │              │                                          │        │
     │              │  1. Resolve queue → topic:                │        │
     │              │     "order-events" → "ws.order-events"   │        │
     │              │                                          │        │
     │              │  2. Register subscription:                │        │
     │              │     subscriptionId = "sub-1"             │        │
     │              │     credits = 100                        │        │
     │              │     noAck = false                        │        │
     │              │                                          │        │
     │              │  3. Resolve start offsets:                │        │
     │              │     Fetch from __consumer_offsets         │        │
     │              │     group = "ws.order-events.{sessionId}"│        │
     │              │     if no committed: use "latest"         │        │
     │              │                                          │        │
     │              │  4. Start WsConsumerFetchLoop on          │        │
     │              │     wsConsumerExecutor thread pool        │        │
     │              └────────┬────────────────────────────────┘        │
     │                       │                                          │
     │  {"type":"subscribed",│                                          │
     │   "subscriptionId":   │                                          │
     │   "sub-1",            │                                          │
     │   "queue":            │                                          │
     │   "order-events"}     │                                          │
     │◄──────────────────────│                                          │
     │                       │                                          │
     │              ┌────────▼────────────────────────────────┐        │
     │              │  WsConsumerFetchLoop (consumer executor) │        │
     │              │                                          │        │
     │              │  loop:                                    │        │
     │              │    if credits <= 0: wait for credits msg  │        │
     │              │    budget = min(credits, maxFetchRecords) │        │
     │              │                                          │        │
     │              │    FetchRequest(partitions, offsets,       │        │
     │              │      maxWait=500ms, maxRecords=budget)    │        │
     │              │    → RequestChannel → KafkaApis           │────────►
     │              │    ← FetchResponse with records           │◄────────
     │              │                                          │        │
     │              │    for each record:                       │        │
     │              │      deliveryTag = nextTag++              │        │
     │              │      pendingAcks[tag] = (tp, offset)     │        │
     │              │      credits--                           │        │
     │              │      write deliver frame to WS channel   │        │
     │              └────────┬────────────────────────────────┘        │
     │                       │                                          │
     │  {"type":"deliver",   │                                          │
     │   "subscriptionId":   │                                          │
     │   "sub-1",            │                                          │
     │   "deliveryTag":1,    │                                          │
     │   "redelivered":false,│                                          │
     │   "exchange":"events",│                                          │
     │   "routingKey":       │                                          │
     │   "order.created",    │                                          │
     │   "message":{         │                                          │
     │     "body":{...},     │                                          │
     │     "contentType":    │                                          │
     │     "application/json"│                                          │
     │   },                  │                                          │
     │   "partition":0,      │                                          │
     │   "offset":42}        │                                          │
     │◄──────────────────────│                                          │
     │                       │                                          │
     │  {"type":"ack",       │                                          │
     │   "subscriptionId":   │                                          │
     │   "sub-1",            │                                          │
     │   "deliveryTag":1}    │                                          │
     │──────────────────────►│                                          │
     │                       │  → commit offset to __consumer_offsets   │
```

### 8.2 Competing consumers (multiple WebSocket subscribers on the same queue)

When multiple WebSocket connections subscribe to the same queue, they form a Kafka consumer
group. Partitions are assigned via `GroupCoordinator`:

```
WS Client A (sub-1)                Broker              WS Client B (sub-2)
     │                               │                        │
     │  subscribe queue="orders"     │                        │
     │──────────────────────────────►│                        │
     │                               │  subscribe queue="orders"
     │                               │◄────────────────────────│
     │                               │                        │
     │  GroupCoordinator:            │                        │
     │  group="ws.orders"            │                        │
     │  Client A → partitions [0,1]  │                        │
     │  Client B → partitions [2,3]  │                        │
     │                               │                        │
     │  deliver (from P0, P1)        │  deliver (from P2, P3) │
     │◄──────────────────────────────│────────────────────────►│
```

The consumer group name is: `ws.{queueName}`. All WebSocket subscribers to the same queue
join the same consumer group, getting partition-level load balancing automatically.

### 8.3 Deliver message format

```json
{
  "type": "deliver",
  "subscriptionId": "sub-1",
  "deliveryTag": 42,
  "redelivered": false,
  "exchange": "events",
  "routingKey": "order.created",
  "message": {
    "body": { "orderId": "12345", "amount": 42.0 },
    "contentType": "application/json",
    "headers": {
      "source": "checkout-service"
    },
    "deliveryMode": 2,
    "correlationId": "req-123",
    "messageId": "msg-abc",
    "timestamp": 1713260400,
    "appId": "checkout-service"
  },
  "partition": 0,
  "offset": 42,
  "kafkaTimestamp": 1713260400000
}
```

The `deliver` message includes both routing metadata (`exchange`, `routingKey`) reconstructed
from Kafka record headers (§10.3) and Kafka-native fields (`partition`, `offset`,
`kafkaTimestamp`) for clients that need them.

---

## 9. Broker-to-Broker Forwarding

WebSocket publish and consume **reuse the existing `ProduceForwardManager` and
`FetchForwardManager`** from the HTTP module. No new forwarding infrastructure is needed.

The flow is identical to HTTP forwarding (see `http-protocol-design.md` §7):

```
WS publish → exchange routing → queue topics → per-topic leader lookup
  → LOCAL: ReplicaManager.appendRecords()
  → REMOTE: ProduceForwardManager.forward(leaderId, entries, acks, timeout)
  → CompletableFuture.allOf().handleAsync(wsAsyncExecutor)

WS consume → fetch loop → per-partition leader lookup
  → LOCAL: ReplicaManager.fetchMessages()
  → REMOTE: FetchForwardManager.forward(leaderId, fetchSpecs, maxWait, ...)
  → CompletableFuture.allOf().handleAsync(wsAsyncExecutor)
```

---

## 10. Broker-to-Broker Forwarding — Produce & Consume Anywhere

See §3.1 for the design principle. This section details the mechanics.

### 10.1 Forwarding decision matrix

```
PUBLISH arrives at Broker B for topic-partition (T, P):

  Is B the leader for (T, P)?
    YES → ReplicaManager.appendRecords() directly (zero-hop)
    NO  → MetadataCache knows leader?
           YES (leader=L) → ProduceForwardManager.forward(L, ...)
                             → binary ProduceRequest to Broker L
                             → CompletableFuture resolves on ProduceResponse
           NO  → publish-failed: LEADER_NOT_AVAILABLE

SUBSCRIBE fetch loop on Broker B for partitions [P0, P1, P2]:

  For each partition:
    Is B the leader?
      YES → ReplicaManager.fetchMessages() (local fetch)
      NO  → FetchForwardManager.forward(leader, ...) (remote fetch)
  
  Merge all results → push deliver frames to WebSocket client
```

### 10.2 Multi-queue fanout forwarding

A single `publish` to a fanout exchange may route to queues whose topics have leaders on
different brokers. The broker fans out simultaneously:

```
publish → exchange "broadcast" (fanout) → 3 queues
  → ws.service-a P0 leader = Broker 1 → ProduceForwardManager.forward(1, ...)
  → ws.service-b P0 leader = Broker 3 → ReplicaManager.appendRecords() (local)
  → ws.service-c P0 leader = Broker 2 → ProduceForwardManager.forward(2, ...)

CompletableFuture.allOf(remote1, local, remote2)
  .orTimeout(ws.publish.timeout.ms)
  .handleAsync(wsAsyncExecutor)
  → single publisher confirm to client
```

### 10.3 Consumer partition assignment across brokers

When a WebSocket client subscribes to a queue with multiple partitions spread across
brokers, the fetch loop transparently fetches from all leaders:

```
subscribe queue="orders" → topic ws.orders (4 partitions)
  P0: Broker 1 (remote)  ─── FetchForwardManager.forward(1, ...) ───► Broker 1
  P1: Broker 2 (remote)  ─── FetchForwardManager.forward(2, ...) ───► Broker 2
  P2: Broker 3 (local)   ─── ReplicaManager.fetchMessages()
  P3: Broker 1 (remote)  ─── FetchForwardManager.forward(1, ...) ───► Broker 1

All results merged into deliver frames → pushed to WebSocket client
Client sees one stream of messages, unaware of 4 partitions on 3 brokers
```

---

## 11. Queue: The Logical View

The queue is the primary abstraction clients interact with. It is a **logical view** of a
Kafka topic — it hides partitions, offsets, consumer groups, and leader election behind a
simple name-based interface.

### 11.1 Queue-to-Topic Mapping

```
Queue name: "order-events"
  → Kafka topic: "ws.order-events"
  → Consumer group: "ws.order-events"
  → Metadata key: "queue:order-events" in __ws_routing_metadata
```

| Queue concept | Kafka implementation |
|---|---|
| Queue name | Topic name with `ws.` prefix |
| Subscribe to queue | Join consumer group `ws.{queueName}` |
| Queue depth (message count) | `sum(logEndOffset - committedOffset)` across partitions |
| Queue is empty | All partitions: `logEndOffset == committedOffset` |
| Queue ordering | Per-partition FIFO. Single partition (default) = strict FIFO |
| Queue durability | Kafka topic with configurable `retention.ms` / `retention.bytes` |

**Topic prefix** `ws.` (configurable via `ws.topic.prefix`) prevents collision with
user-created topics.

### 11.2 Queue Policies and Kafka Topic Configuration

When a queue is declared with arguments, the broker translates them to Kafka topic configs:

| Queue Argument | Kafka Topic Config | Behavior |
|---|---|---|
| `x-message-ttl: 86400000` | `retention.ms=86400000` | Messages expire after 24h |
| `x-max-length-bytes: 1073741824` | `retention.bytes=1073741824` | Max 1GB per partition |
| `x-max-length: 1000000` | Enforced at publish time (§12.6) | Max messages in queue |
| `x-dead-letter-exchange: "dlx"` | Queue metadata (not Kafka config) | Route NACKed messages |
| `x-dead-letter-routing-key: "dead"` | Queue metadata | Override routing key for DLX |
| `x-max-priority: 10` | Queue metadata | Enable priority-sorted delivery |
| `x-expires: 3600000` | Queue metadata | Auto-delete after 1h idle |
| `x-overflow: "reject-publish"` | Queue metadata | Reject publishes when full |

**Auto-creation:** `declare-queue` auto-creates a Kafka topic via internal `CreateTopicsRequest`:

```java
new NewTopic(
    "ws." + queueName,
    numPartitions = config.wsDefaultQueuePartitions,  // default 1
    replicationFactor = config.defaultReplicationFactor
).configs(Map.of(
    "retention.ms", String.valueOf(messageTtlMs),
    "retention.bytes", String.valueOf(maxLengthBytes),
    "cleanup.policy", "delete"
))
```

**Default 1 partition** preserves strict FIFO ordering (matching traditional queue behavior).
Operators can set `ws.default.queue.partitions` higher for throughput, accepting per-partition
ordering.

### 11.3 Queue Lifecycle (Auto-Create, Auto-Delete, Exclusive, Durable)

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Queue Lifecycle                              │
│                                                                     │
│  declare-queue ──► queue exists? ──► YES: assert same config (ok)  │
│       │                    │                                        │
│       │                    NO                                       │
│       │                    │                                        │
│       │              create Kafka topic                             │
│       │              store metadata in __ws_routing_metadata        │
│       │              if exclusive: lock to this connection          │
│       ▼                    ▼                                        │
│  QUEUE ACTIVE ◄────────────┘                                       │
│       │                                                             │
│  ┌────┴────────────────────────────────────────┐                   │
│  │                                              │                   │
│  ▼                                              ▼                   │
│  subscribe / publish                      delete-queue              │
│  (queue in use)                           (explicit delete)         │
│       │                                         │                   │
│       │  last subscriber disconnects            │                   │
│       │  AND autoDelete=true                    │                   │
│       │       │                                 │                   │
│       ▼       ▼                                 ▼                   │
│  AUTO-DELETE (topic deleted)           TOPIC DELETED                │
│                                                                     │
│  exclusive=true + connection closes → QUEUE DELETED                │
│  x-expires=N + no consumers for N ms → QUEUE DELETED               │
└─────────────────────────────────────────────────────────────────────┘
```

| Queue flag | Behavior |
|---|---|
| `durable: true` (default) | Queue + topic survive broker restart |
| `durable: false` | Transient: topic created with `retention.ms=0`, deleted on broker restart |
| `exclusive: true` | Only the declaring connection can subscribe; queue deleted on disconnect |
| `autoDelete: true` | Queue + topic deleted when last subscriber disconnects |
| `x-expires: N` | Queue + topic deleted after N ms with zero consumers |

### 11.4 Competing Consumers on a Queue

Multiple WebSocket clients subscribing to the same queue automatically form a **consumer
group**. Kafka's `GroupCoordinator` handles partition assignment:

```
WS Client A: subscribe queue="orders"  ──► group "ws.orders"
WS Client B: subscribe queue="orders"  ──► group "ws.orders"
WS Client C: subscribe queue="orders"  ──► group "ws.orders"

GroupCoordinator assignment:
  Client A → partitions [0, 1]   ← gets messages from P0, P1
  Client B → partitions [2, 3]   ← gets messages from P2, P3
  Client C → partitions [4]      ← gets messages from P4

If Client B disconnects:
  Rebalance:
  Client A → partitions [0, 1, 2]
  Client C → partitions [3, 4]
```

The client is completely unaware of partitions. It subscribes to `"orders"` and receives
messages. When another client subscribes to the same queue, Kafka automatically rebalances
partitions between them.

### 11.5 Queue Depth and Observability

Queue depth is exposed in multiple ways:

**On `declare-queue` response:**
```json
{
  "type": "queue-declared",
  "queue": "orders",
  "messageCount": 1543,
  "consumerCount": 2
}
```

Where:
- `messageCount` = `sum(logEndOffset - committedOffset)` across all partitions of `ws.orders`
- `consumerCount` = number of active subscribers in consumer group `ws.orders`

**Via REST API:**
```
GET /v1/queues/orders

{
  "queue": "orders",
  "durable": true,
  "messageCount": 1543,
  "consumerCount": 2,
  "partitions": 4,
  "arguments": { "x-message-ttl": 86400000 }
}
```

**Via metrics:**
- `ws.queue.depth` (gauge, per queue) — current unconsumed message count
- `ws.queue.consumers` (gauge, per queue) — active subscriber count
- `ws.queue.publish.rate` (meter, per queue) — messages entering the queue per second
- `ws.queue.deliver.rate` (meter, per queue) — messages delivered to consumers per second

---

## 12. Message Lifecycle

### 12.1 Publish → Route → Enqueue → Deliver → ACK (Happy Path)

```
Client A (publisher)           Broker              Client B (subscriber)
     │                           │                        │
     │  publish                  │                        │
     │  exchange="events"        │                        │
     │  routingKey="order.new"   │                        │
     │──────────────────────────►│                        │
     │                           │                        │
     │                    RoutingEngine:                   │
     │                    exchange "events" (topic)        │
     │                    binding: "order.*" → queue "orders"
     │                    → produce to ws.orders           │
     │                           │                        │
     │                    ReplicaManager.append()          │
     │                    offset = 42                      │
     │                           │                        │
     │  published (confirm)      │                        │
     │◄──────────────────────────│                        │
     │                           │                        │
     │                    WsConsumerFetchLoop:             │
     │                    fetch ws.orders from offset 42   │
     │                    record available                 │
     │                           │                        │
     │                           │  deliver               │
     │                           │  deliveryTag=1         │
     │                           │  exchange="events"     │
     │                           │  routingKey="order.new"│
     │                           │  message={body...}     │
     │                           │────────────────────────►│
     │                           │                        │
     │                           │  ack                   │
     │                           │  deliveryTag=1         │
     │                           │◄────────────────────────│
     │                           │                        │
     │                    commit offset 43 to             │
     │                    __consumer_offsets               │
     │                    group="ws.orders"                │
```

### 12.2 NACK with Requeue — Redelivery

When a consumer NACKs with `requeue: true`, the message is **not** committed and will be
redelivered on the next fetch iteration:

```
Client B (subscriber)           Broker
     │                           │
     │  deliver                  │
     │  deliveryTag=5            │
     │  offset=47                │
     │◄──────────────────────────│
     │                           │
     │  nack                     │
     │  deliveryTag=5            │
     │  requeue=true             │
     │──────────────────────────►│
     │                           │
     │                    offset 47 NOT committed
     │                    WsConsumerFetchLoop resets
     │                    fetch offset to 47
     │                    (or back to last committed)
     │                           │
     │  deliver                  │  ← redelivered
     │  deliveryTag=6            │
     │  redelivered=true         │  ← flag set
     │  offset=47                │
     │◄──────────────────────────│
```

In a competing-consumer setup, the redelivered message may go to a **different** consumer
(since partition rebalance may have occurred, or the same partition's fetch loop may deliver
to a different connection).

### 12.3 NACK without Requeue — Dead-Letter Exchange (DLX)

When a consumer NACKs with `requeue: false` and the queue has `x-dead-letter-exchange`:

```
Client B (subscriber)           Broker                     DLQ Consumer
     │                           │                              │
     │  deliver                  │                              │
     │  deliveryTag=5            │                              │
     │  offset=47                │                              │
     │◄──────────────────────────│                              │
     │                           │                              │
     │  nack                     │                              │
     │  deliveryTag=5            │                              │
     │  requeue=false            │                              │
     │──────────────────────────►│                              │
     │                           │                              │
     │                    Queue "orders" has:                    │
     │                    x-dead-letter-exchange = "dlx"         │
     │                    x-dead-letter-routing-key = "dead.orders"
     │                           │                              │
     │                    1. Build new message with:             │
     │                       - original body, headers            │
     │                       - routing key = "dead.orders"       │
     │                       - x-death header added:             │
     │                         [{                                │
     │                           "queue": "orders",              │
     │                           "reason": "rejected",           │
     │                           "count": 1,                     │
     │                           "exchange": "events",           │
     │                           "routing-keys": ["order.new"],  │
     │                           "time": 1713260400              │
     │                         }]                                │
     │                           │                              │
     │                    2. Route through DLX exchange:          │
     │                       exchange="dlx" (direct)             │
     │                       routingKey="dead.orders"            │
     │                       → queue "dead-letters"              │
     │                       → produce to ws.dead-letters        │
     │                           │                              │
     │                    3. Commit original offset 48            │
     │                       (message consumed from orders)      │
     │                           │                              │
     │                           │  deliver to DLQ subscriber   │
     │                           │  exchange="dlx"              │
     │                           │  routingKey="dead.orders"    │
     │                           │  x-death=[{queue:"orders"...}]
     │                           │──────────────────────────────►│
```

**DLX routing is a full publish** — the dead-lettered message goes through the DLX exchange's
routing engine. This means DLX messages can be routed to multiple queues via bindings, just
like any other publish.

**`x-death` header accumulates:** If a message is dead-lettered multiple times (e.g.,
DLQ → processed → NACK → DLX again), the `x-death` array gains additional entries,
creating an audit trail:

```json
{
  "_ws_headers": {
    "x-death": [
      { "queue": "retry-1", "reason": "rejected", "count": 1, "time": 1713260500 },
      { "queue": "orders",  "reason": "rejected", "count": 1, "time": 1713260400 }
    ]
  }
}
```

### 12.4 Message TTL — Per-Queue and Per-Message Expiration

**Per-queue TTL** (`x-message-ttl` on queue declare):
- Maps to Kafka `retention.ms` on the backing topic
- Kafka automatically deletes log segments older than this
- Applied at the storage level — no per-message overhead

**Per-message TTL** (`expiration` field on publish):
- Checked at **delivery time** in `WsConsumerFetchLoop`
- If `now() > record.timestamp + expiration`, the message is **skipped** (not delivered)
- Skipped messages still advance the consumer offset (committed as consumed)
- If the queue has DLX and `x-dead-letter-exchange`, expired messages are dead-lettered
  with `reason: "expired"`

```java
// WsConsumerFetchLoop — TTL check at delivery time
for (Record record : fetchResponse.records()) {
    String expiration = getHeader(record, "_ws_expiration");
    if (expiration != null) {
        long ttlMs = Long.parseLong(expiration);
        long age = System.currentTimeMillis() - record.timestamp();
        if (age > ttlMs) {
            // Message expired — skip or dead-letter
            if (queueHasDLX) deadLetter(record, "expired");
            continue;  // do not deliver
        }
    }
    // Deliver to client
    writeDeliverFrame(record);
}
```

### 12.5 Priority Delivery

Queues with `x-max-priority: N` enable priority-aware delivery ordering:

```json
{
  "type": "declare-queue",
  "queue": "tasks",
  "arguments": { "x-max-priority": 10 }
}
```

**Implementation:** The `WsConsumerFetchLoop` fetches a batch of records, **sorts them by
priority** (highest first, from `message.headers.priority`), and delivers in priority order.

```
Fetch batch (unsorted):
  offset=10, priority=3, body="low priority task"
  offset=11, priority=9, body="urgent task"
  offset=12, priority=5, body="medium task"

Deliver order (sorted):
  deliveryTag=1, offset=11, priority=9  ← delivered first
  deliveryTag=2, offset=12, priority=5
  deliveryTag=3, offset=10, priority=3  ← delivered last
```

**Limitation:** Priority ordering is **within each fetch batch**, not globally across the
entire queue. A record with priority=1 already in the batch will be delivered before a
priority=10 record that arrives in the next fetch. For strict global priority ordering,
use separate queues per priority level.

### 12.6 Poison Message Protection

A message that repeatedly fails processing (consumer NACKs with `requeue: true` every time)
can cause an infinite redelivery loop. Protection:

**`x-delivery-count` header:** The broker tracks how many times a message has been delivered
by storing a delivery count in a Kafka header. Each redelivery increments the count:

```json
{ "_ws_delivery_count": "3" }
```

**Auto-DLX on max retries:** When `x-delivery-count` exceeds `ws.max.redelivery.count`
(default 10, configurable per queue via `x-max-retries` argument), the broker automatically
dead-letters the message instead of redelivering:

```
deliver (deliveryTag=N, x-delivery-count=10)
  → client NACKs with requeue=true
  → broker checks: delivery_count (10) >= max_retries (10)
  → auto-DLX instead of requeue
  → x-death reason = "max-retries-exceeded"
```

This prevents poison messages from blocking the queue indefinitely.

### 12.7 Message Deduplication

AMQP `messageId` can be used for idempotent publish. When `ws.dedup.enabled=true`:

1. Publisher sets `message.messageId` on each publish
2. Broker maintains a bounded in-memory set of recent message IDs per exchange
   (size = `ws.dedup.cache.size`, default 10000, TTL = `ws.dedup.cache.ttl.ms`, default 60000)
3. If `messageId` already in the set → publish silently succeeds (duplicate suppressed),
   publisher confirm sent
4. If not in set → normal publish

This gives at-most-once semantics for publishers that retry on timeout, without requiring
Kafka transactions.

---

## 13. Routing-to-Kafka Mapping

### 13.1 Exchanges → Metadata records

Exchanges are routing configurations stored in `__ws_routing_metadata`. They have no Kafka
topic counterpart.

### 13.2 Queues → Kafka topics

See §11.1 and §11.2 for the complete mapping.

### 13.3 Messages → Kafka records

| Publish field | Kafka record field |
|---|---|
| `message.body` | `value` (JSON serialized or raw bytes) |
| `routingKey` | `key` (for partition assignment via murmur2 hash) |
| `exchange` | `headers["_ws_exchange"]` |
| `routingKey` | `headers["_ws_routing_key"]` |
| `message.contentType` | `headers["_content-type"]` |
| `message.headers` | `headers["_ws_headers"]` (JSON serialized) |
| `message.deliveryMode` | `headers["_ws_delivery_mode"]` |
| `message.correlationId` | `headers["_ws_correlation_id"]` |
| `message.replyTo` | `headers["_ws_reply_to"]` |
| `message.messageId` | `headers["_ws_message_id"]` |
| `message.timestamp` | `record.timestamp` (if present) |
| `message.appId` | `headers["_ws_app_id"]` |
| `message.expiration` | `headers["_ws_expiration"]` |
| delivery count | `headers["_ws_delivery_count"]` |
| priority | `headers["_ws_priority"]` |

All routing/message metadata is stored as Kafka record headers, enabling lossless
round-trip for WebSocket-to-WebSocket flows and cross-protocol consumption.

### 13.4 Consumer ACKs → Offset commits

```
ack(subscriptionId="sub-1", deliveryTag=5, multiple=true)
  → resolve tags 1..5 to [(tp0, offset=42), (tp0, offset=43), ...]
  → GroupCoordinator.commitOffsets(
      group = "ws.order-events",
      offsets = {tp0 → 44}
    )
```

Batch commit optimization: offsets are flushed every `ws.ack.commit.interval.ms`
(default 1000ms), on unsubscribe, and on WebSocket close.

---

## 14. Metadata Storage

### 11.1 `__ws_routing_metadata` topic

A single compacted Kafka topic stores all routing metadata:

```
Topic: __ws_routing_metadata
Partitions: 1 (total ordering for metadata operations)
Cleanup policy: compact
Replication factor: min(3, cluster size)

Record key formats:
  "exchange:{name}"
  "queue:{name}"
  "binding:{exchange}:{queue}:{routingKey}:{argsHash}"
  "e2e:{source}:{destination}:{routingKey}:{argsHash}"

Record value:
  JSON metadata (exchange type, queue arguments, binding arguments)
  null value = tombstone (entity deleted)
```

### 11.2 In-memory cache

`WsRoutingMetadataManager` maintains concurrent in-memory structures:

```java
ConcurrentHashMap<String, Exchange> exchanges;        // name → exchange config
ConcurrentHashMap<String, Queue> queues;              // name → queue config
ConcurrentHashMap<String, CopyOnWriteArrayList<Binding>> bindings;  // exchange → bindings
ConcurrentHashMap<String, CopyOnWriteArrayList<E2EBinding>> e2eBindings;  // exchange → e2e bindings
```

All mutations:
1. Write to `__ws_routing_metadata` (durable)
2. Apply to in-memory cache (immediate)

### 11.3 Startup replay

On broker startup, `WsRoutingMetadataManager` replays `__ws_routing_metadata` from offset 0
to high watermark, rebuilding the in-memory cache. The WebSocket endpoint does not accept
connections until replay is complete (same pattern as `GroupCoordinator` replaying
`__consumer_offsets`).

### 11.4 Cross-broker consistency

All brokers replay the same `__ws_routing_metadata` topic. Exchange/queue/binding
declarations on one broker are visible on all brokers after replication lag (typically <100ms
in a healthy cluster). Declarations are idempotent (compacted key deduplication).

---

## 12. Integration with Existing Infrastructure

### 12.1 WebSocket upgrade in HttpChannelInitializer

The existing `HttpChannelInitializer.configureHttp11Pipeline()` is extended to add WebSocket
upgrade detection:

```scala
// In HttpChannelInitializer.configureHttp11Pipeline():

// Existing HTTP pipeline (unchanged)
pipeline.addLast("http-codec",      new HttpServerCodec())
pipeline.addLast("http-aggregator", new HttpObjectAggregator(maxRequestBytes))
pipeline.addLast("compressor",      new HttpContentCompressor())
pipeline.addLast("idle-handler",    new IdleStateHandler(...))
pipeline.addLast("idle-closer",     new IdleStateCloseHandler())

// NEW: WebSocket upgrade detection + standard HTTP handler
pipeline.addLast("ws-or-http",      new WsUpgradeOrHttpHandler(
    wsRoutingManager, wsConsumerManager, wsAsyncExecutor,
    requestChannel, principalBuilder, securityProtocol,
    draining, inFlightCount))
```

`WsUpgradeOrHttpHandler` inspects each `FullHttpRequest`:

```java
public class WsUpgradeOrHttpHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest req) {
        if (isWebSocketUpgrade(req) && "/v1/ws".equals(req.uri())) {
            // Switch to WebSocket pipeline
            upgradeToWebSocket(ctx, req);
        } else {
            // Standard HTTP — delegate to existing HttpRequestHandler
            ctx.fireChannelRead(req.retain());
        }
    }

    private void upgradeToWebSocket(ChannelHandlerContext ctx, FullHttpRequest req) {
        // 1. Extract auth from upgrade request (same as HTTP)
        KafkaPrincipal principal = buildPrincipal(ctx, req);

        // 2. Perform WebSocket handshake
        WebSocketServerHandshakerFactory factory =
            new WebSocketServerHandshakerFactory(wsUrl(req), null, true, maxFrameSize);
        WebSocketServerHandshaker handshaker = factory.newHandshaker(req);
        handshaker.handshake(ctx.channel(), req);

        // 3. Replace pipeline: remove HTTP handlers, add WS handlers
        ChannelPipeline pipeline = ctx.pipeline();
        pipeline.remove("http-aggregator");
        pipeline.remove("compressor");
        pipeline.remove("ws-or-http");

        pipeline.addLast("ws-frame-aggregator",
            new WebSocketFrameAggregator(maxFrameSize));
        pipeline.addLast("ws-handler",
            new WsFrameHandler(principal, sessionId, wsRoutingManager,
                wsConsumerManager, wsAsyncExecutor, requestChannel));

        // 4. Send connected message
        sendConnectedMessage(ctx, sessionId);
    }
}
```

### 12.2 No new security protocol

WebSocket connections reuse the existing `HTTP` / `HTTPS` security protocol. No new enum
values are needed — the WebSocket upgrade happens within an HTTP connection, inheriting
its security context.

### 12.3 KafkaApis — no dispatch changes needed

WebSocket publishes are translated to standard `ProduceRequest` objects and enqueued to
`RequestChannel` with `securityProtocol = HTTP`. They flow through the existing
`handleHttpProduceRequest()` path. The response is routed back through `HttpProcessor`,
which detects the WebSocket session and writes the confirm frame instead of an HTTP response.

WebSocket consume uses a separate fetch loop (`WsConsumerFetchLoop`) that builds
`FetchRequest` objects and routes them through `RequestChannel` → `KafkaApis`. The
response path writes `deliver` frames to the WebSocket channel.

### 12.4 HttpProcessor extension

`HttpProcessor.processResponses()` is extended to handle WebSocket connections:

```java
public void processResponses() {
    // ... existing code ...

    for (RequestChannel.Response response : batch) {
        String connectionId = response.request().context().connectionId();

        if (wsConnections.containsKey(connectionId)) {
            // WebSocket response path
            WsConnectionContext wsCtx = wsConnections.get(connectionId);
            handleWsResponse(wsCtx, response);
        } else {
            // Existing HTTP response path (unchanged)
            handleHttpResponse(response);
        }
    }
}

private void handleWsResponse(WsConnectionContext wsCtx, RequestChannel.Response response) {
    if (response instanceof RequestChannel.SendResponse sendResp) {
        AbstractResponse kafkaResp = sendResp.response();
        if (kafkaResp instanceof ProduceResponse produceResp) {
            // Write publisher confirm frame
            wsCtx.writePublishConfirm(produceResp);
        }
        // FetchResponse handled by WsConsumerFetchLoop directly
    }
}
```

### 12.5 Reuse forwarding managers

WebSocket reuses the **existing** `ProduceForwardManager` and `FetchForwardManager`. These
managers are already instantiated in `HttpAcceptor.startup()` and passed to `KafkaApis`.
WebSocket produce/consume requests that enter `KafkaApis` via `RequestChannel` automatically
use the same forwarding infrastructure.

---

## 13. Module Changes: `http-server`

All WebSocket code lives in the existing `http-server` module. No new Gradle subproject.

```
http-server/src/main/java/kafka/server/http/
├── (existing files unchanged)
├── HttpRequestHandler.java
├── HttpProcessor.java
├── HttpRouter.java
├── ...
│
├── ws/                                  ← NEW: WebSocket package
│   ├── WsUpgradeOrHttpHandler.java      — Upgrade detection + pipeline switch
│   ├── WsFrameHandler.java             — JSON frame dispatcher
│   ├── WsPublishHandler.java           — Publish frame → routing → ProduceRequest
│   ├── WsSubscriptionManager.java      — Subscription lifecycle
│   ├── WsConsumerFetchLoop.java        — Per-subscription fetch task
│   ├── WsDeliveryTagTracker.java       — deliveryTag → (tp, offset) mapping
│   ├── WsCreditManager.java            — Per-subscription credit tracking
│   ├── WsAckHandler.java              — ACK/NACK → offset commit
│   ├── WsConnectionContext.java        — Per-connection state
│   ├── WsMessageSerializer.java        — Kafka record → deliver JSON
│   └── WsMessageDeserializer.java      — Publish JSON → Kafka record
│
├── routing/                             ← NEW: Routing engine package
│   ├── RoutingEngine.java              — Exchange → queue resolution
│   ├── DirectMatcher.java             — Exact routing key match
│   ├── TopicMatcher.java              — Wildcard pattern match
│   ├── FanoutMatcher.java             — All-bindings match
│   ├── HeadersMatcher.java            — Header criteria match
│   ├── ExchangeManager.java           — Exchange CRUD + cache
│   ├── QueueManager.java              — Queue CRUD + topic creation
│   ├── BindingManager.java            — Binding CRUD + index
│   └── WsRoutingMetadataManager.java  — __ws_routing_metadata replay + cache
│
├── rest/                                ← NEW: REST extensions for routing
│   ├── ExchangeRestHandler.java       — /v1/exchanges CRUD
│   ├── QueueRestHandler.java          — /v1/queues CRUD
│   └── BindingRestHandler.java        — /v1/bindings CRUD

http-server/src/test/java/kafka/server/http/
├── ws/
│   ├── WsFrameHandlerTest.java
│   ├── WsPublishHandlerTest.java
│   ├── WsConsumerFetchLoopTest.java
│   └── WsCreditManagerTest.java
├── routing/
│   ├── DirectMatcherTest.java
│   ├── TopicMatcherTest.java
│   ├── HeadersMatcherTest.java
│   └── RoutingEngineTest.java
└── integration/
    ├── WsPublishIntegrationTest.java
    ├── WsSubscribeIntegrationTest.java
    ├── WsExchangeRoutingTest.java
    └── WsCrossProtocolTest.java
```

### 13.1 New dependencies in `http-server/build.gradle`

```groovy
// No new external dependencies — Netty already includes WebSocket support:
// io.netty:netty-codec-http includes WebSocketServerHandshaker,
// WebSocketFrameAggregator, TextWebSocketFrame, etc.
```

---

## 14. Configuration

New properties (in addition to existing HTTP config from `http-protocol-design.md` §10):

| Property | Default | Description |
|---|---|---|
| `ws.enabled` | `true` | Enable WebSocket upgrade on HTTP listener |
| `ws.max.frame.size` | `1048576` (1 MB) | Max WebSocket frame payload size |
| `ws.max.subscriptions.per.connection` | `256` | Max concurrent subscriptions per WebSocket |
| `ws.default.credits` | `100` | Default delivery credits if not specified in subscribe |
| `ws.max.credits` | `10000` | Max credits a client can grant at once |
| `ws.topic.prefix` | `ws.` | Prefix for Kafka topics backing queues |
| `ws.default.queue.partitions` | `1` | Default partitions for auto-created queue topics |
| `ws.metadata.topic` | `__ws_routing_metadata` | Internal topic for routing metadata |
| `ws.metadata.replication.factor` | `3` | Replication factor for metadata topic |
| `ws.ack.commit.interval.ms` | `1000` | Batch interval for offset commits from ACKs |
| `ws.consumer.start.offset` | `latest` | Default start offset (`earliest` or `latest`) |
| `num.ws.consumer.threads` | `8` | Thread pool for consumer fetch loops |
| `ws.consumer.max.wait.ms` | `500` | Max wait per fetch loop iteration |
| `ws.consumer.max.bytes` | `1048576` (1 MB) | Max fetch bytes per iteration |
| `ws.publish.timeout.ms` | `30000` | Max time for publish to complete before NACK |
| `ws.connection.max.idle.ms` | `600000` (10 min) | Close idle WS connections (no frames) |
| `ws.shutdown.drain.ms` | `5000` | Drain window for WS connections during shutdown |
| `ws.max.redelivery.count` | `10` | Auto-DLX after this many redeliveries (poison message protection, §12.6) |
| `ws.dedup.enabled` | `false` | Enable publish deduplication by messageId (§12.7) |
| `ws.dedup.cache.size` | `10000` | Max entries in dedup cache per exchange |
| `ws.dedup.cache.ttl.ms` | `60000` | Dedup cache entry TTL |

No new listener configuration needed — WebSocket runs on the same HTTP listener port.

---

## 15. Error Handling

### 15.1 Error frame format

```json
{
  "type": "error",
  "id": "req-1",
  "errorCode": "QUEUE_NOT_FOUND",
  "errorMessage": "Queue 'orders' does not exist",
  "detail": {
    "failingOperation": "subscribe",
    "queue": "orders"
  }
}
```

### 15.2 Error codes

| Error Code | Meaning | Triggering Operations |
|---|---|---|
| `INVALID_REQUEST` | Malformed JSON or missing required fields | Any |
| `EXCHANGE_NOT_FOUND` | Exchange does not exist | publish, bind, unbind, delete-exchange |
| `QUEUE_NOT_FOUND` | Queue does not exist | subscribe, bind, unbind, delete-queue |
| `EXCHANGE_TYPE_MISMATCH` | Redeclare with different type | declare-exchange |
| `PRECONDITION_FAILED` | ifUnused/ifEmpty constraint violated | delete-queue, delete-exchange |
| `ACCESS_REFUSED` | Authorization failure | Any operation requiring ACLs |
| `EXCLUSIVE_CONSUMER` | Exclusive consumer already active | subscribe |
| `NO_ROUTE` | Mandatory publish, no matching queues | publish (→ `returned` frame) |
| `QUOTA_EXCEEDED` | Throttle limit reached | publish, subscribe |
| `NOT_ENOUGH_REPLICAS` | ISR below minimum | publish |
| `INTERNAL_ERROR` | Unexpected broker error | Any |
| `CONNECTION_FORCED` | Broker shutting down | (close frame, not error frame) |

### 15.3 WebSocket close codes

| Code | Meaning |
|---|---|
| 1000 | Normal closure |
| 1001 | Broker going away (shutdown) |
| 1008 | Policy violation |
| 1009 | Frame too large |
| 1011 | Unexpected error |
| 4400 | Protocol error (bad JSON, unknown type) |
| 4403 | Access denied |
| 4429 | Too many connections |

---

## 16. Security

### 16.1 Authentication

WebSocket inherits HTTP authentication. Credentials are extracted from the upgrade request:

| Method | Header/Source | Notes |
|---|---|---|
| mTLS | `SslHandler` peer certificate | HTTPS listener only |
| Bearer | `Authorization: Bearer <token>` | Upgrade request header |
| Basic | `Authorization: Basic <b64>` | Upgrade request header |
| Anonymous | (none) | HTTP listener with no auth config |

The `KafkaPrincipal` is created during upgrade and stored on the `WsConnectionContext`.
All subsequent operations on this WebSocket use the same principal.

### 16.2 Authorization

| WebSocket Operation | Kafka ACL Resource | Kafka ACL Operation |
|---|---|---|
| `publish` (after routing) | `TOPIC:ws.{queue}` | `WRITE` |
| `subscribe` | `TOPIC:ws.{queue}` | `READ` |
| `declare-exchange` | `CLUSTER` | `ALTER` |
| `delete-exchange` | `CLUSTER` | `ALTER` |
| `declare-queue` | `TOPIC:ws.{queue}` | `CREATE` |
| `delete-queue` | `TOPIC:ws.{queue}` | `DELETE` |
| `bind` / `unbind` | `TOPIC:ws.{queue}` | `ALTER` |

### 16.3 Per-message authorization on publish

When a single `publish` routes to multiple queues (fanout), the broker checks `WRITE`
permission on **each** target queue's backing topic. If any check fails, the publish fails
entirely (no partial routing).

---

## 17. Implementation Plan

### Phase 1 — WebSocket upgrade + basic publish/subscribe (direct exchange only)

1. Implement `WsUpgradeOrHttpHandler` — WebSocket upgrade detection + pipeline switch
2. Implement `WsFrameHandler` — JSON frame parsing and dispatch
3. Implement `WsConnectionContext` — per-connection state (principal, sessionId, subscriptions)
4. Implement `WsRoutingMetadataManager` — `__ws_routing_metadata` topic + in-memory cache (§11)
5. Implement `ExchangeManager` — pre-declared exchanges + declare/delete
6. Implement `QueueManager` — declare/delete with Kafka topic auto-creation
7. Implement `BindingManager` — bind/unbind with in-memory index
8. Implement `RoutingEngine` with `DirectMatcher` only
9. Implement `WsPublishHandler` — publish frame → routing → `ProduceRequest` → `RequestChannel`
10. Extend `HttpProcessor` to handle WebSocket connections (§12.4)
11. Implement `WsSubscriptionManager` + `WsConsumerFetchLoop` — push delivery
12. Implement `WsDeliveryTagTracker` — deliveryTag → (tp, offset) mapping
13. Implement `WsCreditManager` — credit-based flow control
14. Implement `WsAckHandler` — ACK/NACK → offset commit
15. Implement `WsMessageSerializer` / `WsMessageDeserializer` — record conversion
16. Add new config properties to `KafkaConfig` (§14)
17. Integration test: WebSocket connect, declare queue, publish, subscribe, deliver, ack

### Phase 2 — All exchange types + REST API

18. Implement `TopicMatcher` — wildcard pattern matching (`*`, `#`)
19. Implement `FanoutMatcher` — deliver-to-all-bindings
20. Implement `HeadersMatcher` — header criteria matching (x-match: all/any)
21. Implement exchange-to-exchange bindings with cycle detection
22. Implement `ExchangeRestHandler` — `PUT/GET/DELETE /v1/exchanges`
23. Implement `QueueRestHandler` — `PUT/GET/DELETE /v1/queues`
24. Implement `BindingRestHandler` — `POST/GET/DELETE /v1/bindings`
25. Extend `HttpRouter` to route new REST endpoints
26. Integration tests: all 4 exchange types, e2e bindings, REST management

### Phase 3 — Publisher confirms + robustness + security

27. Implement `enable-confirms` and publisher confirm flow (§4.10)
28. Implement mandatory message return (§4.7 mandatory handling)
29. Implement competing consumers via `GroupCoordinator` (§8.2)
30. Implement exclusive consumer enforcement (§4.8.1)
31. Implement ACL authorization for all operations (§16.2)
32. Batch offset commit optimization (§10.4)
33. Auto-delete queues and exchanges on last binding/subscriber removal
34. Graceful shutdown drain for WebSocket connections (§18.5)
35. Metrics: `ws.publish.rate`, `ws.deliver.rate`, `ws.connection.count`, `ws.subscription.count`
36. Integration tests: confirms, auth, cross-protocol (WS publish → Kafka consume)

### Phase 4 — Advanced features

37. Dead-letter exchange support (`x-dead-letter-exchange` on NACK, §18.6)
38. Message TTL: `x-message-ttl` + per-message `expiration` enforcement at delivery time
39. Priority delivery ordering (`x-max-priority`, §18.7)
40. HTTP POST with exchange routing (`POST /v1/topics/{exchange}/records:route`, §7.2)
41. WebSocket compression (permessage-deflate extension)
42. Server-generated queue names (`q.gen-*`)
43. Queue purge over WebSocket and REST
44. Performance benchmarking (connection count, publish throughput, delivery latency)

---

## 18. Implementation Concerns

### 18.1 CRITICAL — WsConsumerFetchLoop must not block Netty or handler threads

**Problem.** WebSocket push requires fetching records from Kafka and writing frames to the
WebSocket channel. This must never happen on Netty worker threads or `KafkaRequestHandler`
threads.

**Solution.** `WsConsumerFetchLoop` runs on a dedicated `wsConsumerExecutor`
(`ScheduledExecutorService`, configurable via `num.ws.consumer.threads`, default 8). Each
subscription gets a `Runnable` scheduled on this pool.

```java
// WsConsumerFetchLoop — runs on wsConsumerExecutor, NOT on Netty workers

while (active) {
    int budget = creditManager.awaitCredits(100, MILLISECONDS);
    if (budget <= 0) continue;

    // Build and submit fetch via RequestChannel (non-blocking enqueue)
    CompletableFuture<FetchResponse> future = submitFetch(budget);
    FetchResponse resp = future.get(fetchTimeoutMs, MILLISECONDS);

    for (Record record : resp.records()) {
        TextWebSocketFrame frame = serializer.toDeliverFrame(record, nextTag++);
        // Netty writeAndFlush is thread-safe (queued to event loop)
        channel.writeAndFlush(frame);
        creditManager.consumed();
    }
}
```

`channel.writeAndFlush()` from a non-Netty thread is safe — Netty schedules the write on
the channel's event loop internally. This is a documented Netty pattern.

### 18.2 CRITICAL — Credit-based backpressure prevents OOM on slow consumers

**Problem.** A slow WebSocket consumer (e.g., a browser on a bad connection) cannot process
messages as fast as Kafka produces them. Without flow control, the Netty write buffer grows
unboundedly until the broker OOMs.

**Solution.** Credits provide explicit, client-controlled flow control:

1. Client sets initial credits on `subscribe` (default 100)
2. Each `deliver` frame consumes 1 credit
3. When credits = 0, the fetch loop pauses (no more deliveries)
4. Client sends `credits` frame to replenish
5. Netty `ChannelOption.WRITE_BUFFER_WATER_MARK` acts as a safety net — if the write buffer
   exceeds the high water mark (default 64KB), `channel.isWritable()` returns false and the
   fetch loop pauses regardless of credit count

```java
// WsCreditManager — per-subscription credit tracking
private final AtomicInteger credits;

public int awaitCredits(long timeout, TimeUnit unit) {
    long deadline = System.nanoTime() + unit.toNanos(timeout);
    while (credits.get() <= 0) {
        long remaining = deadline - System.nanoTime();
        if (remaining <= 0) return 0;
        LockSupport.parkNanos(Math.min(remaining, MILLISECONDS.toNanos(10)));
    }
    return credits.get();
}

public void consumed() { credits.decrementAndGet(); }
public void grant(int additional) { credits.addAndGet(additional); }
```

### 18.3 CRITICAL — Delivery tag memory: bound per-subscription pending ACKs

**Problem.** Each unacked message occupies memory in `WsDeliveryTagTracker`. Without limits,
pending ACKs grow unboundedly.

**Solution.** The credit limit naturally bounds pending ACKs: at most `credits` messages
are outstanding per subscription. With default 100 credits × 256 subscriptions per
connection × ~80 bytes per entry = ~2 MB per connection. The hard ceiling is
`ws.max.credits` (default 10000).

### 18.4 HIGH — Routing engine hot path: lock-free data structures

**Problem.** The routing engine is called on every `publish` and must be lock-free.

**Solution (from ivy-ref patterns):**
- `ConcurrentHashMap` for exchange lookup (O(1))
- `CopyOnWriteArrayList` for binding lists (lock-free iteration, rare mutation)
- `TopicMatcher` uses manual dot-split (no regex in hot path)
- Pre-compiled trie for topic exchanges with many bindings (rebuilt on binding change)

### 18.5 MEDIUM — Graceful shutdown: drain WebSocket connections

```
1. Stop accepting new WebSocket upgrades (check draining flag)
2. Send close frame (1001, "Broker shutting down") to all WS connections
3. Wait up to ws.shutdown.drain.ms for:
   a. Consumer fetch loops to finish current iteration
   b. Pending ACK commits to flush
   c. Publisher confirm futures to resolve
4. Force-close remaining connections
```

Integrated into the existing `HttpAcceptor.beginDrain()` / `awaitDrain()` lifecycle.

### 18.6 See §12.3 — Dead-Letter Exchange (DLX) is now a core design section

### 18.7 See §12.5 — Priority Delivery is now a core design section

### 18.8 LOW — WebSocket ping/pong keepalive

Netty's `WebSocketServerProtocolHandler` automatically handles WebSocket ping/pong frames.
The `ws.connection.max.idle.ms` timeout is enforced by the existing `IdleStateHandler` +
`IdleStateCloseHandler` in the pipeline (added before the WebSocket upgrade).

After upgrade, idle detection shifts to WebSocket frame activity. The `WsFrameHandler`
resets the idle timer on every frame received (including pong frames).

---

## 19. Comparison with Alternatives

### 19.1 Why WebSocket + JSON instead of native AMQP 0-9-1

| Concern | AMQP 0-9-1 binary | WebSocket + JSON |
|---|---|---|
| **Client availability** | RabbitMQ clients only | Every language, every browser |
| **Debuggability** | Binary frames (Wireshark needed) | JSON text (browser DevTools) |
| **Learning curve** | AMQP spec (100+ pages) | REST + WebSocket (familiar to web devs) |
| **Browser support** | None (binary TCP) | Native `WebSocket` API |
| **Implementation** | Full binary codec (1500+ lines) | JSON parse (Jackson, 0 lines custom) |
| **Wire overhead** | Lower (binary encoding) | Higher (JSON text), but compressible |
| **Feature parity** | Full AMQP 0-9-1 | Exchange routing subset (4 types, bindings) |

**Recommendation:** Use WebSocket + JSON for new applications, browser clients, and polyglot
environments. Use native AMQP 0-9-1 (`amqp091-protocol-design.md`) only when
wire-compatibility with existing RabbitMQ deployments is required.

### 19.2 Why not Server-Sent Events (SSE)

| Concern | SSE | WebSocket |
|---|---|---|
| **Direction** | Server → client only | Bidirectional |
| **Publish** | Requires separate HTTP POST | Inline in same connection |
| **ACK/NACK** | Requires separate HTTP POST | Inline in same connection |
| **Flow control** | None (client must buffer) | Credits (explicit) |
| **Binary data** | Must base64 encode | Binary frames supported |
| **Connection** | HTTP keep-alive (proxy-sensitive) | Upgraded, long-lived |

WebSocket's bidirectional nature is essential for publish + subscribe + ack on a single
connection. SSE would require HTTP POST side-channel for publish and ack, doubling
connection count and adding correlation complexity.

### 19.3 Why extend HTTP module instead of new module

1. **Same port:** WebSocket upgrade requires an HTTP listener — reuse the existing one
2. **Same auth:** Upgrade request uses HTTP auth headers — reuse `KafkaPrincipalBuilder`
3. **Same pipeline:** Netty pipeline is already configured — just add upgrade detection
4. **Same forwarding:** `ProduceForwardManager` and `FetchForwardManager` already exist
5. **Shared routing:** REST endpoints and WebSocket control messages share the same
   `RoutingEngine` — putting them in the same module avoids cross-module coupling
6. **Single build artifact:** One JAR for all HTTP + WebSocket functionality

---

*Document version: 0.2 — 2026-04-16*
*Branch: feature/http-protocol*
