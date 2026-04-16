# MQTT 3.1.1 + 5.0 Protocol for Apache Kafka — Design Document

## Table of Contents

1. [Overview](#1-overview)
2. [Goals & Non-Goals](#2-goals--non-goals)
3. [Architecture Overview](#3-architecture-overview)
4. [MQTT Protocol Specification](#4-mqtt-protocol-specification)
   - 4.1 Packet Types
   - 4.2 CONNECT / CONNACK
   - 4.3 PUBLISH / PUBACK / PUBREC / PUBREL / PUBCOMP
   - 4.4 SUBSCRIBE / SUBACK
   - 4.5 UNSUBSCRIBE / UNSUBACK
   - 4.6 PINGREQ / PINGRESP
   - 4.7 DISCONNECT
   - 4.8 AUTH (MQTT 5.0)
5. [Topic Mapping: MQTT to Kafka](#5-topic-mapping-mqtt-to-kafka)
6. [Publish Path (Client → Broker → Kafka)](#6-publish-path-client--broker--kafka)
7. [Subscribe Path (Kafka → Broker → Client)](#7-subscribe-path-kafka--broker--client)
8. [Quality of Service (QoS)](#8-quality-of-service-qos)
9. [Session Management](#9-session-management)
10. [Will Messages](#10-will-messages)
11. [Retained Messages](#11-retained-messages)
12. [Shared Subscriptions (Consumer Groups)](#12-shared-subscriptions-consumer-groups)
13. [Broker-to-Broker Forwarding](#13-broker-to-broker-forwarding)
14. [Integration with Existing Kafka Infrastructure](#14-integration-with-existing-kafka-infrastructure)
15. [New Module: `mqtt-server`](#15-new-module-mqtt-server)
16. [Configuration](#16-configuration)
17. [Error Handling](#17-error-handling)
18. [Security](#18-security)
19. [Implementation Plan](#19-implementation-plan)
20. [Implementation Concerns](#20-implementation-concerns)
21. [MQTT 3.1.1 vs 5.0 Compatibility Matrix](#21-mqtt-311-vs-50-compatibility-matrix)
22. [Comparison with Existing MQTT-Kafka Bridges](#22-comparison-with-existing-mqtt-kafka-bridges)

---

## 1. Overview

This document describes the design for adding native MQTT protocol support to Apache Kafka
brokers. The feature allows MQTT clients to **publish** and **subscribe** to Kafka topics
using the MQTT 3.1.1 (RFC 7440 / OASIS v3.1.1) and MQTT 5.0 (OASIS v5.0) wire protocols
directly, without a separate bridge or gateway.

### Key Properties

| Property | Behavior |
|---|---|
| **Publish** | MQTT PUBLISH → broker resolves Kafka topic + partition → appends via `ReplicaManager` (local) or forwards to leader (remote) → PUBACK/PUBREC |
| **Subscribe** | MQTT SUBSCRIBE → broker registers push subscription → on new Kafka records, broker encodes and pushes PUBLISH frames to connected clients |
| **Protocol** | MQTT 3.1.1 (protocol level 4) and MQTT 5.0 (protocol level 5) over TCP |
| **QoS** | QoS 0 (at most once), QoS 1 (at least once), QoS 2 (exactly once) — mapped to Kafka acks semantics |
| **Sessions** | Persistent sessions with offline message queuing, subscription recovery on reconnect |
| **Integration** | Plugs into the existing `SocketServer → RequestChannel → KafkaApis` pipeline via a new `MqttAcceptor` |
| **Forwarding** | Broker-to-broker forwarding uses the existing Kafka binary protocol (PRODUCE / FETCH API) — same pattern as HTTP forwarding (§13) |

### Motivation

- Enable IoT and edge devices to publish directly to Kafka without a bridge process
- Leverage existing broker infrastructure (auth, quotas, replication, ISR) for MQTT traffic
- Support bidirectional streaming — MQTT's persistent TCP connection and server-push model
  maps naturally to Kafka's pub/sub semantics
- Unify MQTT and Kafka topic namespaces so a Kafka consumer can read MQTT-published messages
  and vice versa

### Why native, not a bridge?

| Concern | External bridge (e.g. Confluent MQTT Proxy, HiveMQ extension) | Native broker support |
|---|---|---|
| **Operational complexity** | Separate process to deploy, monitor, and scale | Zero additional processes |
| **Latency** | Bridge → Kafka producer → broker (2 network hops) | Client → broker (1 hop) |
| **Session management** | Bridge must persist sessions externally (Redis, DB) | Broker persists sessions in `__mqtt_sessions` internal topic |
| **Retained messages** | Bridge must maintain a separate store | Broker uses compacted internal topic |
| **Replication & HA** | Bridge is a SPOF unless clustered separately | Inherits Kafka's ISR-based replication |
| **Auth** | Separate auth stack or proxy-through | Same `KafkaPrincipalBuilder` + `Authorizer` path |

---

## 2. Goals & Non-Goals

### Goals

- Full MQTT 3.1.1 compliance: CONNECT, PUBLISH (QoS 0/1/2), SUBSCRIBE with wildcards (+, #), PINGREQ/PINGRESP, will messages, retained messages, clean session
- Full MQTT 5.0 compliance: enhanced reason codes, session expiry interval, will delay interval, maximum packet size negotiation, shared subscriptions ($share/), user properties, topic alias (phase 3)
- Transparent Kafka integration: MQTT-published messages appear as normal Kafka records; Kafka-produced records can be delivered to MQTT subscribers
- Support 100K+ concurrent MQTT connections per broker (IoT scale)
- Configurable via standard `listeners` / `listener.security.protocol.map` mechanism
- Session persistence surviving broker restarts via internal Kafka topic

### Non-Goals

- MQTT over WebSocket (phase 2 — requires HTTP upgrade handler in Netty pipeline)
- MQTT bridge mode (connecting to external MQTT brokers) — this is a native broker protocol
- Full MQTT 5.0 enhanced authentication multi-round-trip (phase 3) — phase 1 supports simple username/password via CONNECT
- Topic alias support (MQTT 5.0 §3.3.2.3.4) — phase 3 optimization
- Request/Response pattern (MQTT 5.0 §4.10) — phase 3
- Flow control via Receive Maximum (MQTT 5.0 §3.2.2.3.3) — phase 2
- Subscription Identifiers (MQTT 5.0 §3.8.2.1.2) — phase 3
- Server-side message expiry enforcement (MQTT 5.0 §3.3.2.3.3) — phase 2
- Replacing the Kafka binary protocol for Kafka-native clients

---

## 3. Architecture Overview

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                              Kafka Broker                                    │
│                                                                              │
│  Port 1883 (MQTT)              Port 9092 (PLAINTEXT — unchanged)             │
│  Port 8883 (MQTTS)                                                           │
│  ┌────────────────────────┐    ┌──────────────────────────────────────┐      │
│  │     MqttAcceptor       │    │         DataPlaneAcceptor            │      │
│  │  (Netty ServerBootstrap)    │         (existing NIO Acceptor)      │      │
│  │                        │    │                                      │      │
│  │  ┌──────────────────┐  │    └──────────────────┬───────────────────┘      │
│  │  │ MqttFrameDecoder │  │                       │                          │
│  │  │ MqttPacketDecoder│  │                       │                          │
│  │  │ MqttConnHandler  │  │                       │                          │
│  │  │ MqttReqHandler   │  │                       │                          │
│  │  └────────┬─────────┘  │                       │                          │
│  └───────────┼────────────┘                       │                          │
│              │                                    │                          │
│              └──────────────┬─────────────────────┘                          │
│                             ▼                                                │
│                   ┌──────────────────────┐                                   │
│                   │    RequestChannel    │  ◄── shared, protocol-agnostic    │
│                   └──────────┬───────────┘                                   │
│                              │                                               │
│                   ┌──────────▼───────────┐                                   │
│                   │  KafkaRequestHandler │  ◄── thread pool, unchanged       │
│                   │     thread pool      │                                   │
│                   └──────────┬───────────┘                                   │
│                              │                                               │
│           ┌──────────────────▼─────────────────────────────────┐             │
│           │                    KafkaApis                        │             │
│           │  if (securityProtocol == MQTT/MQTTS):               │             │
│           │    handleMqttPublishRequest()                       │             │
│           │    handleMqttSubscribeRequest()                     │             │
│           │  else:                                              │             │
│           │    handleProduceRequest()   (existing, unchanged)   │             │
│           │    handleFetchRequest()     (existing, unchanged)   │             │
│           └──────┬────────────────────────┬─────────────────────┘             │
│                  │                        │                                   │
│        MetadataCache lookup         is this broker leader?                    │
│                  │              YES ──────────┐                               │
│                  │                            │     NO                        │
│                  │              ┌─────────────▼──┐  ┌──────────────┐         │
│                  │              │  ReplicaManager │  │  Forward     │         │
│                  │              │  appendRecords()│  │  Manager     │         │
│                  │              │  fetchMessages()│  │  (§13)       │         │
│                  │              └─────────────┬──┘  └───────┬──────┘         │
│                  │                            │              │                │
│           ┌──────▼────────────────────────────▼──────────────▼───────────┐   │
│           │                    MqttSessionManager                        │   │
│           │  ┌─────────────┐  ┌──────────────────┐  ┌────────────────┐  │   │
│           │  │ Session     │  │ Subscription     │  │ Retained Msg   │  │   │
│           │  │ Store       │  │ Manager          │  │ Store          │  │   │
│           │  │(__mqtt_     │  │(push delivery,   │  │(__mqtt_        │  │   │
│           │  │ sessions)   │  │ wildcard match)  │  │ retained)      │  │   │
│           │  └─────────────┘  └──────────────────┘  └────────────────┘  │   │
│           └─────────────────────────────────────────────────────────────┘   │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

MQTT requests and binary-protocol requests share the **same `RequestChannel` queue and the
same `KafkaRequestHandler` thread pool**. This mirrors the HTTP design (§3 of http-protocol-design.md)
and avoids a separate handler pool while reusing all existing quota, metrics, and throttling hooks.

**Dispatch model.** Like HTTP, no new `ApiKeys` are introduced. MQTT PUBLISH maps to
`ApiKeys.PRODUCE`, MQTT SUBSCRIBE triggers a push subscription backed by `ApiKeys.FETCH`.
`KafkaApis.handle()` dispatches to **dedicated MQTT handler methods** when
`request.context.securityProtocol == MQTT || MQTTS`.

```scala
// KafkaApis.handle() — dispatch addition:
case ApiKeys.PRODUCE =>
  if (request.context.securityProtocol.isMqtt)
    handleMqttPublishRequest(request)    // MQTT PUBLISH → Kafka append
  else if (request.context.securityProtocol.isHttp)
    handleHttpProduceRequest(request)    // HTTP produce (existing)
  else
    handleProduceRequest(request)        // binary handler (unchanged)
```

**Key difference from HTTP: bidirectional push.** HTTP is request/response — the broker only
sends data when the client asks. MQTT is bidirectional — after SUBSCRIBE, the broker
**pushes** PUBLISH frames to the client whenever new records appear on matching Kafka
partitions. This requires a persistent Netty channel reference and a `SubscriptionManager`
that delivers records asynchronously to connected MQTT clients.

### Thread Model

```
                      ┌─────────────────────────────────────────────┐
  Port 1883           │  Netty Boss Thread (1)                      │
  MQTT connections ──►│  accepts TCP connections                    │
                      └───────────────────┬─────────────────────────┘
                                          │ distributes connections
                      ┌───────────────────▼─────────────────────────┐
                      │  Netty Worker Threads (num.mqtt.network.threads, default=8) │
                      │  • read MQTT frames off the socket          │
                      │  • MqttFrameDecoder splits frames           │
                      │  • MqttPacketDecoder decodes packets        │
                      │  • MqttConnectionHandler validates CONNECT  │
                      │  • MqttRequestHandler routes by packet type │
                      │  • writes MQTT responses/pushes to socket   │
                      └───────────────────┬─────────────────────────┘
                                          │
                  ┌───────────────────────┼───────────────────────────┐
                  │                       │                           │
         PUBLISH path              SUBSCRIBE path              PING/DISCONNECT
         (§6)                      (§7)                        (handled inline
                  │                       │                     on Netty thread)
                  ▼                       ▼
    RequestChannel.tryEnqueue()    MqttSubscriptionManager.register()
                  │                       │
    KafkaRequestHandler pool       On new records (via FlushEvent
    → KafkaApis.handleMqttPublish   or ReplicaManager callback):
    → ReplicaManager.append         → encode PUBLISH frame
      or ProduceForwardManager      → write to client's Netty channel
    → PUBACK/PUBREC via future      (bypasses RequestChannel for
      completion                     push delivery — §7.4)
```

**PINGREQ, DISCONNECT, and CONNACK** are handled directly on the Netty worker thread (no
`RequestChannel` round-trip needed). Only PUBLISH and SUBSCRIBE/UNSUBSCRIBE operations
touch the Kafka internals via `RequestChannel`.

---

## 4. MQTT Protocol Specification

### 4.1 Packet Types

All 15 MQTT control packet types. Phase column indicates when each is implemented.

| Type | Value | Direction | Description | Phase |
|---|---|---|---|---|
| CONNECT | 1 | C → S | Client requests connection | 1 |
| CONNACK | 2 | S → C | Connection acknowledgement | 1 |
| PUBLISH | 3 | C ↔ S | Publish message (bidirectional) | 1 |
| PUBACK | 4 | C ↔ S | QoS 1 acknowledgement | 1 |
| PUBREC | 5 | C ↔ S | QoS 2: Publish received (step 1) | 2 |
| PUBREL | 6 | C ↔ S | QoS 2: Publish release (step 2) | 2 |
| PUBCOMP | 7 | C ↔ S | QoS 2: Publish complete (step 3) | 2 |
| SUBSCRIBE | 8 | C → S | Subscribe to topics | 1 |
| SUBACK | 9 | S → C | Subscribe acknowledgement | 1 |
| UNSUBSCRIBE | 10 | C → S | Unsubscribe from topics | 1 |
| UNSUBACK | 11 | S → C | Unsubscribe acknowledgement | 1 |
| PINGREQ | 12 | C → S | Ping request (keepalive) | 1 |
| PINGRESP | 13 | S → C | Ping response | 1 |
| DISCONNECT | 14 | C ↔ S | Disconnect notification (S → C in MQTT 5.0 only) | 1 |
| AUTH | 15 | C ↔ S | Authentication exchange (MQTT 5.0 only) | 3 |

### 4.2 CONNECT / CONNACK

#### CONNECT wire format

```
Fixed Header: 0x10 + Remaining Length (VBI)
Variable Header:
  Protocol Name:   0x00 0x04 "MQTT"  (UTF-8 string, 2-byte length prefix)
  Protocol Level:  0x04 (v3.1.1) or 0x05 (v5.0)
  Connect Flags:   1 byte
    bit 7: Username Flag
    bit 6: Password Flag
    bit 5: Will Retain
    bit 4-3: Will QoS (0, 1, 2)
    bit 2: Will Flag
    bit 1: Clean Session (v3.1.1) / Clean Start (v5.0)
    bit 0: Reserved (must be 0)
  Keep Alive:      2 bytes (seconds, 0 = no timeout)
  [MQTT 5.0] Properties: VBI length + property bytes
    Session Expiry Interval (0x11, 4 bytes)
    Receive Maximum (0x21, 2 bytes)
    Maximum Packet Size (0x27, 4 bytes)
    Topic Alias Maximum (0x22, 2 bytes)
    Authentication Method (0x15, UTF-8 string)
    Authentication Data (0x16, binary data)
Payload:
  Client Identifier: UTF-8 string (required, may be zero-length)
  [if Will Flag] Will Properties (MQTT 5.0 only): VBI length + bytes
  [if Will Flag] Will Topic: UTF-8 string
  [if Will Flag] Will Payload: 2-byte length + bytes
  [if Username Flag] Username: UTF-8 string
  [if Password Flag] Password: binary data (2-byte length + bytes)
```

#### CONNACK wire format

```
Fixed Header: 0x20 0x02 (v3.1.1) or 0x20 + VBI (v5.0)
Variable Header:
  Connect Acknowledge Flags: 1 byte
    bit 0: Session Present (1 if resuming existing session)
    bits 7-1: Reserved (must be 0)
  Reason Code: 1 byte
  [MQTT 5.0] Properties: VBI length + property bytes
    Session Expiry Interval (0x11)
    Receive Maximum (0x21)
    Maximum QoS (0x24)
    Retain Available (0x25)
    Maximum Packet Size (0x27)
    Assigned Client Identifier (0x12)
    Topic Alias Maximum (0x22)
    Wildcard Subscription Available (0x28)
    Subscription Identifiers Available (0x29)
    Shared Subscription Available (0x2A)
    Server Keep Alive (0x13)
```

#### CONNECT reason codes

| Code | MQTT 3.1.1 Name | MQTT 5.0 Name | Meaning |
|---|---|---|---|
| 0x00 | Connection Accepted | Success | Connection accepted |
| 0x01 | Unacceptable Protocol Version | — | — |
| 0x02 | Identifier Rejected | — | — |
| 0x03 | Server Unavailable | — | — |
| 0x04 | Bad Username or Password | — | — |
| 0x05 | Not Authorized | — | — |
| 0x81 | — | Malformed Packet | Packet structure invalid |
| 0x82 | — | Protocol Error | Protocol violation |
| 0x84 | — | Unsupported Protocol Version | v3.1.1 reason 0x01 equivalent |
| 0x85 | — | Client Identifier not valid | v3.1.1 reason 0x02 equivalent |
| 0x86 | — | Bad User Name or Password | v3.1.1 reason 0x04 equivalent |
| 0x87 | — | Not Authorized | v3.1.1 reason 0x05 equivalent |
| 0x88 | — | Server Unavailable | v3.1.1 reason 0x03 equivalent |
| 0x89 | — | Server Busy | Broker overloaded |
| 0x8A | — | Banned | Client banned |
| 0x95 | — | Packet Too Large | CONNECT exceeds max packet size |
| 0x97 | — | Quota Exceeded | Client quota exceeded |
| 0x99 | — | Payload Format Invalid | Will payload format invalid |

#### CONNECT processing flow

```
MqttConnectionHandler.channelRead0(ctx, MqttConnectData):

1. Validate protocol name == "MQTT"
   ├── Fail → CONNACK(0x01 / 0x84), close
   └── Pass ↓

2. Validate protocol level ∈ {4, 5}
   ├── Fail → CONNACK(0x01 / 0x84), close
   └── Pass ↓

3. Resolve tenant (from TLS SNI hostname or default tenant)
   └── Continue ↓

4. Authenticate: principalBuilder.build(MqttAuthenticationContext)
   ├── Fail → CONNACK(0x04 / 0x86), close
   └── Pass → KafkaPrincipal ↓

5. Generate client ID if empty: "mqtt-" + UUID
   └── Continue ↓

6. Client ID uniqueness check (per tenant):
   ├── Existing connection with same clientId →
   │     a. Send DISCONNECT to old connection (v5.0: reason 0x8E Session Taken Over)
   │     b. Close old channel after flush
   │     c. Capture old session state if cleanSession=false
   └── Continue ↓

7. Session management:
   ├── cleanSession=true (v3.1.1) or cleanStart=true + SEI=0 (v5.0):
   │     a. Discard any existing session
   │     b. Create new session
   │     c. CONNACK with sessionPresent=false
   ├── cleanSession=false (v3.1.1) or SEI>0 (v5.0):
   │     a. Look up existing session
   │     b. If found: restore subscriptions, drain offline queue
   │        CONNACK with sessionPresent=true
   │     c. If not found: create new session
   │        CONNACK with sessionPresent=false
   └── Continue ↓

8. Store will message (if Will Flag set)
   └── Continue ↓

9. Register channel in client registry
10. Start keep-alive timer: 1.5 × keepAlive seconds (per spec §3.1.2.10)
11. Set channel attributes: clientId, mqttVersion, session, principal
12. Send CONNACK (§4.2 wire format)
```

---

### 4.3 PUBLISH / PUBACK / PUBREC / PUBREL / PUBCOMP

#### PUBLISH wire format

```
Fixed Header: 0x30 + flags + Remaining Length (VBI)
  bit 3: DUP flag
  bit 2-1: QoS level (0, 1, 2)
  bit 0: RETAIN flag
Variable Header:
  Topic Name: UTF-8 string (2-byte length prefix, no wildcards allowed)
  [if QoS > 0] Packet Identifier: 2 bytes (1–65535)
  [MQTT 5.0] Properties: VBI length + property bytes
    Payload Format Indicator (0x01, 1 byte)
    Message Expiry Interval (0x02, 4 bytes)
    Topic Alias (0x23, 2 bytes)
    Response Topic (0x08, UTF-8 string)
    Correlation Data (0x09, binary)
    User Property (0x26, UTF-8 string pair, repeatable)
    Content Type (0x03, UTF-8 string)
Payload: application data (remaining bytes)
```

#### QoS acknowledgement flows

**QoS 0 — At Most Once (fire-and-forget)**
```
Client                  Broker
  │  PUBLISH (QoS 0)      │
  │───────────────────────►│
  │                        │ → append to Kafka (acks=0 internally)
  │  (no acknowledgement)  │
```

**QoS 1 — At Least Once**
```
Client                  Broker
  │  PUBLISH (QoS 1)      │
  │───────────────────────►│
  │                        │ → append to Kafka (acks=1 or acks=all)
  │  PUBACK                │ ← after append succeeds
  │◄───────────────────────│
```

**QoS 2 — Exactly Once (4-way handshake)**
```
Client                  Broker
  │  PUBLISH (QoS 2)      │
  │───────────────────────►│
  │                        │ → store packet ID in QoS 2 state machine
  │  PUBREC                │ ← acknowledged receipt
  │◄───────────────────────│
  │  PUBREL                │
  │───────────────────────►│
  │                        │ → append to Kafka (exactly once)
  │  PUBCOMP               │ ← release complete
  │◄───────────────────────│
```

#### PUBACK wire format (QoS 1)
```
Fixed Header: 0x40 0x02
Variable Header:
  Packet Identifier: 2 bytes
  [MQTT 5.0 with remaining > 2] Reason Code: 1 byte
  [MQTT 5.0] Properties: VBI length + bytes
```

#### PUBREC / PUBREL / PUBCOMP wire formats (QoS 2)

| Packet | Fixed Header | Reserved Flags | Payload |
|---|---|---|---|
| PUBREC | 0x50 | 0000 | Packet ID + [v5.0: reason + properties] |
| PUBREL | 0x62 | 0010 (bits 3-0 must be 0010) | Packet ID + [v5.0: reason + properties] |
| PUBCOMP | 0x70 | 0000 | Packet ID + [v5.0: reason + properties] |

---

### 4.4 SUBSCRIBE / SUBACK

#### SUBSCRIBE wire format

```
Fixed Header: 0x82 + Remaining Length (VBI)
  (bits 3-0 must be 0010)
Variable Header:
  Packet Identifier: 2 bytes
  [MQTT 5.0] Properties: VBI length + property bytes
    Subscription Identifier (0x0B, VBI, 1–268435455)
    User Property (0x26, UTF-8 string pair, repeatable)
Payload: one or more subscription entries:
  Topic Filter: UTF-8 string (2-byte length prefix)
  Subscription Options: 1 byte
    bits 1-0: Maximum QoS (0, 1, 2)
    [MQTT 5.0] bit 2: No Local (don't receive own publishes)
    [MQTT 5.0] bit 3: Retain As Published (preserve original retain flag)
    [MQTT 5.0] bits 5-4: Retain Handling (0=send on subscribe, 1=send if new, 2=don't send)
```

#### SUBACK wire format

```
Fixed Header: 0x90 + Remaining Length (VBI)
Variable Header:
  Packet Identifier: 2 bytes
  [MQTT 5.0] Properties: VBI length + property bytes
Payload: one reason code byte per subscription (same order as SUBSCRIBE):
  MQTT 3.1.1: 0x00 (QoS 0), 0x01 (QoS 1), 0x02 (QoS 2), 0x80 (failure)
  MQTT 5.0:   0x00-0x02 (granted QoS), 0x80 (unspecified), 0x83 (impl-specific),
              0x87 (not authorized), 0x8F (topic filter invalid),
              0x91 (packet ID in use), 0x97 (quota exceeded),
              0xA1 (shared sub not supported), 0xA2 (subscription ID not supported)
```

#### Topic Filter Wildcards

```
+   Single-level wildcard (matches exactly one topic level)
#   Multi-level wildcard (matches zero or more levels, must be last segment)
$   System topic prefix (hidden from + and # at first level)
```

Examples:
```
sensors/+/temperature    matches  sensors/room1/temperature
                         matches  sensors/room2/temperature
                    not  matches  sensors/building/room1/temperature

sensors/#                matches  sensors
                         matches  sensors/room1
                         matches  sensors/room1/temperature

$SYS/monitor/clients     matches  $SYS/monitor/clients  (exact)
+/monitor/clients   not  matches  $SYS/monitor/clients  ($ hidden from +)
#                   not  matches  $SYS/monitor/clients  ($ hidden from #)
```

**Topic matching algorithm** (per MQTT 5.0 §4.7):

```java
static boolean matches(String topicFilter, String topicName) {
    // Fast path: no wildcards → exact string comparison
    if (topicFilter.indexOf('+') < 0 && topicFilter.indexOf('#') < 0)
        return topicFilter.equals(topicName);

    // System topic rule: $ topics never match filters starting with + or #
    if (topicName.charAt(0) == '$' &&
        (topicFilter.charAt(0) == '+' || topicFilter.charAt(0) == '#'))
        return false;

    // Split into levels and compare
    String[] filterLevels = topicFilter.split("/", -1);
    String[] topicLevels  = topicName.split("/", -1);

    for (int i = 0; i < filterLevels.length; i++) {
        String f = filterLevels[i];
        if ("#".equals(f)) return true;           // matches all remaining
        if (i >= topicLevels.length) return false; // filter longer than topic
        if (!"+".equals(f) && !f.equals(topicLevels[i])) return false;
    }
    return filterLevels.length == topicLevels.length;
}
```

---

### 4.5 UNSUBSCRIBE / UNSUBACK

#### UNSUBSCRIBE wire format

```
Fixed Header: 0xA2 + Remaining Length (VBI)
  (bits 3-0 must be 0010)
Variable Header:
  Packet Identifier: 2 bytes
  [MQTT 5.0] Properties: VBI length + property bytes
Payload: one or more topic filters:
  Topic Filter: UTF-8 string (2-byte length prefix)
```

#### UNSUBACK wire format

```
Fixed Header: 0xB0 + Remaining Length (VBI)
Variable Header:
  Packet Identifier: 2 bytes
  [MQTT 5.0] Properties: VBI length + property bytes
  [MQTT 5.0] Payload: one reason code byte per topic filter:
    0x00 (Success), 0x11 (No subscription existed), 0x80 (Unspecified error),
    0x83 (Implementation specific error), 0x87 (Not authorized),
    0x8F (Topic Filter invalid)
```

---

### 4.6 PINGREQ / PINGRESP

```
PINGREQ:  0xC0 0x00  (2 bytes, no payload)
PINGRESP: 0xD0 0x00  (2 bytes, no payload)
```

Handled directly on the Netty worker thread — no `RequestChannel` round-trip. The handler
simply writes `PINGRESP` to the channel and resets the keep-alive timer.

---

### 4.7 DISCONNECT

#### DISCONNECT wire format

```
MQTT 3.1.1:
  Fixed Header: 0xE0 0x00  (2 bytes, no payload)

MQTT 5.0:
  Fixed Header: 0xE0 + Remaining Length (VBI)
  Variable Header:
    Reason Code: 1 byte
    Properties: VBI length + property bytes
      Session Expiry Interval (0x11, 4 bytes) — can override CONNECT value
      Reason String (0x1F, UTF-8 string)
      User Property (0x26, UTF-8 string pair, repeatable)
```

#### DISCONNECT reason codes (server → client, MQTT 5.0)

| Code | Name | When sent |
|---|---|---|
| 0x00 | Normal disconnection | Clean shutdown |
| 0x04 | Disconnect with Will Message | Server closing, will should be published |
| 0x81 | Malformed Packet | Protocol violation detected |
| 0x82 | Protocol Error | Unexpected packet type |
| 0x89 | Server Busy | Broker overloaded |
| 0x8B | Server Shutting Down | Graceful shutdown |
| 0x8D | Keep Alive Timeout | Client missed keep-alive deadline |
| 0x8E | Session Taken Over | Another client connected with same clientId |
| 0x93 | Receive Maximum Exceeded | Too many in-flight QoS 1/2 messages |
| 0x95 | Packet Too Large | Packet exceeds negotiated maximum |
| 0x9A | Retain Not Supported | Retained messages not available |
| 0x9B | QoS Not Supported | Requested QoS level not available |
| 0x9C | Use Another Server | Client should reconnect to different server |
| 0x9D | Server Moved | Permanent redirect |
| 0x9E | Shared Subscriptions Not Supported | $share/ not available |
| 0xA0 | Maximum Connect Time | Connection time limit reached |
| 0xA1 | Subscription Identifiers Not Supported | Sub IDs not available |
| 0xA2 | Wildcard Subscriptions Not Supported | + and # not available |

#### DISCONNECT processing

```
Client → Broker (DISCONNECT received):

MQTT 3.1.1:
  1. Clear will message (will NOT be published)
  2. Close connection
  3. If cleanSession=false: session persists with subscriptions

MQTT 5.0:
  1. Parse reason code + properties
  2. If Session Expiry Interval property present:
     a. If was 0 in CONNECT and now > 0 → protocol error (DISCONNECT 0x82)
     b. Otherwise: override session expiry with new value
  3. If reason code == 0x04 (Disconnect with Will Message):
     → Publish will message (honor will delay interval)
  4. If reason code == 0x00 (Normal):
     → Clear will message (will NOT be published)
  5. Close connection
  6. If sessionExpiryInterval > 0: session persists with subscriptions

Broker → Client (server-initiated DISCONNECT, MQTT 5.0 only):
  1. Encode DISCONNECT with reason code + optional properties
  2. Flush and close channel
  (MQTT 3.1.1: just close the connection — no server-initiated DISCONNECT)
```

---

### 4.8 AUTH (MQTT 5.0 — Phase 3)

Enhanced authentication for multi-round-trip auth mechanisms (SCRAM, Kerberos, OAuth2).

```
Fixed Header: 0xF0 + Remaining Length (VBI)
Variable Header:
  Reason Code: 1 byte (0x00=Success, 0x18=Continue Authentication, 0x19=Re-authenticate)
  Properties:
    Authentication Method (0x15, UTF-8 string)
    Authentication Data (0x16, binary)
    Reason String (0x1F, UTF-8 string)
```

Phase 1 uses simple username/password from CONNECT. Phase 3 adds AUTH packet support
for SASL-like multi-step authentication flows, mapping to Kafka's `SaslAuthenticateRequest`
internally.

---

## 5. Topic Mapping: MQTT to Kafka

### 5.1 Naming convention

MQTT uses `/` as a topic level separator. Kafka uses any characters (but conventionally `.`
or `-` or `_`). The broker must translate between the two namespaces.

**Design decision: direct passthrough with configurable prefix.**

```
MQTT topic "sensors/room1/temperature"
  → Kafka topic "sensors/room1/temperature"    (passthrough, default)
  → Kafka topic "mqtt.sensors.room1.temperature"  (with prefix + separator rewrite)
```

Configuration: `mqtt.topic.mapping.strategy`

| Strategy | MQTT topic | Kafka topic | When to use |
|---|---|---|---|
| `passthrough` (default) | `sensors/room1/temp` | `sensors/room1/temp` | Kafka topics already use `/` separator |
| `prefix` | `sensors/room1/temp` | `mqtt.sensors.room1.temp` | Separate MQTT namespace; `/` → `.` rewrite |
| `custom` | any | configurable | User-defined mapping class |

**Passthrough is the default** because Kafka topic names support `/` characters (it is not a
restricted character per `Topic.java`). This gives the simplest mental model: the MQTT topic
IS the Kafka topic.

### 5.2 Topic validation

```java
// MqttTopicMapper.java

String mapToKafka(String mqttTopic, TopicMappingStrategy strategy) {
    // 1. Reject wildcards in PUBLISH topics (MQTT spec §3.3.2.1)
    if (mqttTopic.contains("+") || mqttTopic.contains("#"))
        throw new MqttProtocolException("Wildcards not allowed in PUBLISH topic");

    // 2. Reject empty or too-long topics
    if (mqttTopic.isEmpty() || mqttTopic.length() > 65535)
        throw new MqttProtocolException("Topic name invalid length");

    // 3. Apply mapping strategy
    String kafkaTopic = switch (strategy) {
        case PASSTHROUGH -> mqttTopic;
        case PREFIX -> config.mqttTopicPrefix() + mqttTopic.replace('/', '.');
        case CUSTOM -> customMapper.map(mqttTopic);
    };

    // 4. Validate result as a legal Kafka topic name
    Topic.validate(kafkaTopic);
    return kafkaTopic;
}
```

### 5.3 Wildcard subscription mapping

MQTT wildcard subscriptions (`+`, `#`) do not map to a single Kafka topic. The broker
handles this by:

1. **Exact topic subscriptions** (no wildcards): Register a push subscription on the
   specific Kafka topic's partitions via `SubscriptionManager`.

2. **Wildcard subscriptions**: Register with `SubscriptionManager.subscribeGlobal()` —
   the manager delivers records from ALL partitions. The MQTT handler filters by topic
   name match in the delivery callback:

```java
// In MqttSubscriptionBridge — wildcard subscription callback:
subscriptionManager.subscribeGlobal(subscriberId, entries -> {
    for (SegmentEntry entry : entries) {
        String kafkaTopic = entry.topicName();
        String mqttTopic = topicMapper.mapToMqtt(kafkaTopic);
        if (MqttTopicMatcher.matches(topicFilter, mqttTopic)) {
            deliverToClient(channel, mqttTopic, entry.value(), effectiveQoS);
        }
    }
});
```

This means wildcard subscriptions see **all** broker traffic and filter in the callback.
For brokers with many topics, this is O(topics). Phase 2 optimization: build an inverted
index of topic levels → subscriptions to reduce the matching cost (§20.3).

### 5.4 Partition routing for MQTT PUBLISH

MQTT does not have a concept of partitions. The broker assigns a partition using:

```
partitionCount = MetadataCache.getTopicMetadata(kafkaTopic).partitionCount

if MQTT PUBLISH has a user property "kafka.partition" (MQTT 5.0):
    partition = parseInt(propertyValue)           // explicit partition
else if MQTT PUBLISH has a user property "kafka.key":
    partition = murmur2(key) % partitionCount     // keyed routing
else:
    partition = stickyPartitioner(clientId)        // round-robin per client
```

**Default behavior (no properties):** All messages from a single client are sticky to one
partition per topic (round-robin across reconnects). This gives ordering per client, which
is the expected semantic for MQTT — messages from one device arrive in order.

**MQTT 5.0 user properties** allow advanced clients to control partition routing without
breaking the MQTT protocol. This is opt-in and transparent to MQTT 3.1.1 clients.

---

## 6. Publish Path (Client → Broker → Kafka)

### 6.1 End-to-end: QoS 1 Happy Path

Client publishes to MQTT topic `sensors/room1/temperature`. Kafka topic has 3 partitions.
Client is connected to **Broker 2**. Broker 1 is leader for P0 (where the message will land).

```
MQTT Client          Broker 2                                    Broker 1
     │                  │                                            │
     │  PUBLISH (QoS 1) │                                           │
     │  topic: sensors/room1/temperature                             │
     │  payload: {"temp": 22.5}                                      │
     │  packetId: 42                                                 │
     │──────────────────►│                                           │
     │                   │                                           │
     │             ┌─────▼──────────────────────────────────┐       │
     │             │  MqttRequestHandler                     │       │
     │             │                                         │       │
     │             │  1. Map topic: sensors/room1/temperature│       │
     │             │     → Kafka topic (passthrough)         │       │
     │             │  2. Resolve partition:                   │       │
     │             │     stickyPartitioner(clientId) → P0    │       │
     │             │  3. MetadataCache: P0 leader = Broker 1 │       │
     │             │     (remote — must forward)             │       │
     │             └─────┬──────────────────────────────────┘       │
     │                   │                                           │
     │             ┌─────▼────────────────────────┐                 │
     │             │  ProduceForwardManager        │                 │
     │             │  forward(leaderId=1, {P0:rec})│                 │
     │             └─────┬────────────────────────┘                 │
     │                   │                                           │
     │                   │  binary ProduceRequest                    │
     │                   │  { P0: [record] }                         │
     │                   │  acks=all                                  │
     │                   │──────────────────────────────────────────►│
     │                   │                                           │
     │                   │                             ReplicaManager │
     │                   │                             appendRecords()│
     │                   │                             ISR ack        │
     │                   │                                           │
     │                   │  ProduceResponse                          │
     │                   │  { P0: offset=15042 }                     │
     │                   │◄──────────────────────────────────────────│
     │                   │                                           │
     │             future.complete({P0: offset=15042})               │
     │                   │                                           │
     │  PUBACK            │                                          │
     │  packetId: 42      │                                          │
     │◄──────────────────│                                           │
```

### 6.2 End-to-end: QoS 0 (fire-and-forget)

```
MQTT Client          Broker 2                          Broker 1
     │                  │                                  │
     │  PUBLISH (QoS 0) │                                  │
     │  topic: events/click                                │
     │  payload: {"btn": "buy"}                            │
     │──────────────────►│                                 │
     │                   │                                 │
     │  (no ack)         │  ProduceForwardManager          │
     │                   │  forward(leaderId=1, {P2:rec})  │
     │                   │  acks=0 (fire-and-forget)       │
     │                   │─────────────────────────────────►
     │                   │                                 │
     │                   │  (no response waited)           │
```

For QoS 0, the broker does not wait for the Kafka append to complete. The
`ProduceForwardThread` enqueues the request and the MQTT handler returns immediately.
No PUBACK is sent.

### 6.3 ACK semantics mapping

| MQTT QoS | Kafka `required_acks` | Behavior |
|---|---|---|
| QoS 0 | 0 (fire-and-forget) | No PUBACK; message may be lost |
| QoS 1 | `mqtt.qos1.acks` config (default: `-1` = all) | PUBACK after ISR ack |
| QoS 2 | `mqtt.qos2.acks` config (default: `-1` = all) | PUBCOMP after ISR ack + QoS 2 handshake |

Configurable: operators can set `mqtt.qos1.acks=1` (leader-only) for lower latency if
they accept the risk of data loss on leader failure.

### 6.4 Retained message handling on PUBLISH

```
if (publishData.retain()) {
    if (publishData.payload().length == 0) {
        // Delete retained message for this topic
        retainedMessageStore.delete(mqttTopic);
    } else {
        // Store/replace retained message
        retainedMessageStore.store(mqttTopic, publishData.payload(), publishData.qos());
    }
}
// Continue with normal Kafka append regardless
```

See §11 for the retained message store design.

### 6.5 Edge cases

#### A. Request validation (before touching Kafka)

| Condition | MQTT action | Notes |
|---|---|---|
| Topic contains wildcards (+, #) | DISCONNECT (v5.0: 0x82 Protocol Error) | MQTT spec §3.3.2.1 |
| Topic is empty | DISCONNECT (v5.0: 0x82 Protocol Error) | |
| Topic exceeds 65535 bytes | DISCONNECT (v5.0: 0x82 Protocol Error) | UTF-8 encoded string limit |
| Payload exceeds `max.message.bytes` | DISCONNECT (v5.0: 0x95 Packet Too Large) | |
| QoS 2 but phase 1 (not yet implemented) | PUBREC with reason 0x80 (v3.1.1: close) | Downgrade or reject |
| QoS 1/2 with packet ID 0 | DISCONNECT (v5.0: 0x82 Protocol Error) | Spec requires 1–65535 |

#### B. Kafka errors from append

| Kafka error | MQTT QoS 1 action | MQTT 5.0 reason code |
|---|---|---|
| `NONE` | PUBACK (success) | 0x00 |
| `UNKNOWN_TOPIC_OR_PARTITION` | PUBACK with error (v5.0 only) | 0x83 (Implementation specific error) |
| `TOPIC_AUTHORIZATION_FAILED` | PUBACK with error | 0x87 (Not authorized) |
| `MESSAGE_TOO_LARGE` | DISCONNECT | 0x95 (Packet too large) |
| `NOT_ENOUGH_REPLICAS` | No PUBACK (retry internally) | Internal retry, then 0x80 |
| `REQUEST_TIMED_OUT` | No PUBACK (client will retry) | 0x80 (Unspecified error) |
| `KAFKA_STORAGE_ERROR` | DISCONNECT | 0x89 (Server busy) |

**MQTT 3.1.1 limitation:** There are no reason codes on PUBACK in v3.1.1. On error, the
broker simply does not send PUBACK (client will retry with DUP=1) or closes the connection
for fatal errors.

#### C. Forwarding-specific edge cases

Same as HTTP forwarding (§5.5.C of http-protocol-design.md) but with MQTT-specific responses:

| Condition | MQTT action |
|---|---|
| Leader unknown in MetadataCache | No PUBACK / DISCONNECT (v5.0: 0x89 Server Busy) |
| Forward connection failure | No PUBACK (client retries) |
| Leader changed mid-forward | Retry once internally (same budget as HTTP §5.5.C) |
| Forward timeout | No PUBACK (client retries after keep-alive timeout) |

---

## 7. Subscribe Path (Kafka → Broker → Client)

### 7.1 Subscribe processing flow

```
MqttSubscribeHandler.channelRead0(ctx, MqttSubscribeData):

1. Validate packet:
   ├── Packet ID must be 1–65535
   ├── At least one topic filter
   └── Each filter must be a valid MQTT topic filter

2. For each subscription entry (topicFilter, requestedQoS):
   a. Authorization check: principal has READ on topic
      ├── Fail → SUBACK return code 0x80 (failure) / 0x87 (v5.0: Not Authorized)
      └── Pass ↓

   b. Check subscription limits:
      ├── Total subscriptions > mqtt.max.subscriptions.per.client →
      │   SUBACK return code 0x80 for all
      ├── Wildcard subscriptions > mqtt.max.wildcard.subscriptions.per.client →
      │   SUBACK return code 0x80 for wildcards exceeding limit
      └── Pass ↓

   c. QoS downgrade:
      effectiveQoS = min(requestedQoS, mqtt.max.qos)
      (default mqtt.max.qos = 1 in phase 1, 2 in phase 2)

   d. Register subscription in session:
      session.addSubscription(topicFilter, effectiveQoS)

   e. Register push subscription with Kafka:
      ├── Exact topic (no wildcards):
      │   MqttSubscriptionBridge.subscribeTopic(kafkaTopic, callback)
      │   → SubscriptionManager.subscribe(partitionId, subscriberId, callback)
      │     for ALL partitions of that topic
      └── Wildcard topic (+ or #):
          MqttSubscriptionBridge.subscribeWildcard(topicFilter, callback)
          → SubscriptionManager.subscribeGlobal(subscriberId, callback)
          (callback filters by topic match — §5.3)

   f. Deliver retained messages (if retain handling allows):
      retainedMessageStore.getMatching(topicFilter).forEach(msg ->
          deliverPublish(channel, msg.topic(), msg.payload(), msg.qos()))

3. Send SUBACK with return codes
```

### 7.2 Push delivery: how Kafka records become MQTT PUBLISH frames

After a successful SUBSCRIBE, the broker pushes new records to the client as they are
written to Kafka. The delivery chain:

```
WriteWorker.commitBatch(partitionId, entries)
    │
    ▼
SubscriptionManager.broadcast(partitionId, entries)
    │
    ├── For each registered subscriber callback:
    │   ├── Kafka-native subscriber → enqueue FetchResponse
    │   ├── MQTT subscriber → MqttSubscriptionBridge.deliver()
    │   └── HTTP subscriber → (not applicable — HTTP is pull-only)
    │
    ▼
MqttSubscriptionBridge.deliver(entries):
    │
    for entry in entries:
    │   String kafkaTopic = entry.topicName()
    │   String mqttTopic = topicMapper.mapToMqtt(kafkaTopic)
    │
    │   // For wildcard subscriptions: check match
    │   if (isWildcard && !MqttTopicMatcher.matches(topicFilter, mqttTopic))
    │       continue
    │
    │   // QoS: effective = min(subscription QoS, original publish QoS if available)
    │   // Since Kafka records don't carry MQTT QoS, use subscription QoS
    │   MqttQoS effectiveQoS = subscription.qos()
    │
    │   // Encode PUBLISH frame
    │   ByteBuf frame = MqttPublishCodec.encode(
    │       mqttTopic, entry.value(), effectiveQoS, packetId, retain=false)
    │
    │   // Write to client's Netty channel
    │   if (channel.isActive() && channel.isWritable()) {
    │       channel.writeAndFlush(frame)
    │       if (effectiveQoS > 0) {
    │           // Track in-flight packet for ACK
    │           session.trackInflight(packetId, entry.offset())
    │       }
    │   } else if (!channel.isActive()) {
    │       // Client disconnected — queue for offline delivery
    │       session.enqueueOffline(mqttTopic, entry.value(), effectiveQoS)
    │   }
```

### 7.3 End-to-end: Subscribe with push delivery

Client subscribes to `sensors/+/temperature`. Another client publishes to
`sensors/room1/temperature`. Both connected to the same broker.

```
MQTT Subscriber      Broker 2                     MQTT Publisher
     │                  │                               │
     │  SUBSCRIBE       │                               │
     │  packetId: 7     │                               │
     │  topicFilter: sensors/+/temperature              │
     │  QoS: 1          │                               │
     │──────────────────►│                              │
     │                   │                               │
     │             Register global subscription          │
     │             (wildcard → subscribeGlobal)           │
     │             Deliver retained messages (if any)     │
     │                   │                               │
     │  SUBACK           │                               │
     │  packetId: 7      │                               │
     │  returnCode: 0x01 (granted QoS 1)                 │
     │◄──────────────────│                              │
     │                   │                               │
     │                   │          ... time passes ...  │
     │                   │                               │
     │                   │  PUBLISH (QoS 1)              │
     │                   │  topic: sensors/room1/temperature
     │                   │  payload: {"temp": 22.5}      │
     │                   │◄──────────────────────────────│
     │                   │                               │
     │                   │  Append to Kafka (P0)         │
     │                   │  PUBACK → publisher           │
     │                   │──────────────────────────────►│
     │                   │                               │
     │             SubscriptionManager.broadcast(P0, entries)
     │             MqttSubscriptionBridge:
     │               matches("sensors/+/temperature",
     │                       "sensors/room1/temperature") → true
     │               effectiveQoS = 1
     │               encode PUBLISH frame
     │                   │
     │  PUBLISH (QoS 1)  │
     │  topic: sensors/room1/temperature
     │  payload: {"temp": 22.5}
     │  packetId: 1 (broker-assigned)
     │◄──────────────────│
     │                   │
     │  PUBACK            │
     │  packetId: 1       │
     │──────────────────►│
     │                   │
     │             session.ackInflight(1) → advance offset
```

### 7.4 Push delivery bypasses RequestChannel

Push delivery from Kafka to MQTT clients does **not** go through `RequestChannel`.
The delivery path is:

```
WriteWorker (committed to Kafka log)
    → SubscriptionManager.broadcast() (same thread or flush event thread)
    → MqttSubscriptionBridge.deliver() (encodes PUBLISH frame)
    → channel.writeAndFlush() (writes directly to Netty channel)
```

This is intentional: `RequestChannel` is designed for request/response pairs, not for
server-initiated push. The existing binary `FetchRequest` path pulls data; MQTT push
is event-driven. Using the same `WriteWorker` → `SubscriptionManager` callback path
that the ivy-ref project uses ensures minimal latency (no queue hop) and reuses the
broadcast infrastructure.

**Thread safety:** `channel.writeAndFlush()` is thread-safe in Netty — it can be called
from any thread and the write is serialized on the channel's event loop. The
`MqttSubscriptionBridge` runs on the `WriteWorker` thread or the `FlushEventDispatcher`
thread, which is a different thread than the Netty worker — Netty handles this correctly.

### 7.5 Offline message queuing

When a client with a persistent session disconnects, new messages for its subscriptions
are queued:

```java
// MqttSubscriptionBridge — when channel is not active:
if (!channel.isActive() && session.isPersistent()) {
    OfflineMessage msg = new OfflineMessage(
        mqttTopic, payload, qos, System.currentTimeMillis());
    session.enqueueOffline(msg);  // bounded queue, drops oldest on overflow
}
```

On reconnect with `cleanSession=false` / `cleanStart=false`:
```
CONNECT (cleanSession=false)
  → session found with offline messages
  → CONNACK (sessionPresent=true)
  → drain offline queue: send PUBLISH for each queued message
  → re-register push subscriptions with SubscriptionManager
```

---

## 8. Quality of Service (QoS)

### 8.1 QoS level support matrix

| QoS | Phase | MQTT behavior | Kafka mapping | Notes |
|---|---|---|---|---|
| 0 | 1 | Fire-and-forget | `acks=0` | No acknowledgement, message may be lost |
| 1 | 1 | At-least-once | `acks=all` (configurable) | PUBACK after Kafka ISR ack; duplicates possible on retry |
| 2 | 2 | Exactly-once | `acks=all` + packet ID state machine | 4-way handshake ensures no duplicates |

### 8.2 QoS downgrading

The broker may downgrade the effective QoS based on configuration and capabilities:

```
effectiveQoS = min(publishQoS, subscriptionQoS, mqtt.max.qos)
```

In phase 1, `mqtt.max.qos = 1`, so QoS 2 requests are downgraded to QoS 1 in SUBACK
(return code 0x01 instead of 0x02). In phase 2, QoS 2 is fully supported.

**MQTT 5.0 CONNACK property:** The broker advertises its maximum QoS in CONNACK via the
`Maximum QoS` property (0x24). Compliant v5.0 clients will not publish at a higher QoS.

### 8.3 QoS 2 state machine (Phase 2)

Per-session, per-packet-ID state machine:

```
States: EMPTY → PUBREC_SENT → PUBCOMP_SENT

Client → Broker (inbound PUBLISH QoS 2):

1. PUBLISH received (new packet ID):
   state[packetId] = PUBREC_SENT
   Store message (do NOT append to Kafka yet)
   Send PUBREC

2. PUBREL received:
   if state[packetId] == PUBREC_SENT:
     Append to Kafka (acks=all)
     Wait for Kafka ack
     state[packetId] = PUBCOMP_SENT
     Send PUBCOMP
   else:
     Protocol error

3. (Client retransmits PUBLISH with DUP=1):
   if state[packetId] == PUBREC_SENT:
     Resend PUBREC (idempotent)

Broker → Client (outbound PUBLISH QoS 2):

1. New record for subscribed topic:
   Assign packetId
   outState[packetId] = PUBLISH_SENT
   Send PUBLISH (QoS 2)

2. PUBREC received from client:
   outState[packetId] = PUBREL_SENT
   Send PUBREL

3. PUBCOMP received from client:
   outState[packetId] = EMPTY
   Release packet ID
```

**Storage:** QoS 2 pending messages are stored in the session's in-memory state. On session
persistence (broker restart), pending QoS 2 messages are replayed from the session store.
This is bounded: `mqtt.max.inflight.qos2 = 10` (default) limits concurrent QoS 2 exchanges.

### 8.4 Packet ID management

```java
// MqttSession — packet ID allocator:
private final AtomicInteger nextPacketId = new AtomicInteger(1);

public int allocatePacketId() {
    int id = nextPacketId.getAndIncrement();
    if (id > 65535) {
        nextPacketId.compareAndSet(id + 1, 1);
        id = nextPacketId.getAndIncrement();
    }
    return id;
}
```

Packet IDs are 16-bit unsigned integers (1–65535, 0 is reserved). The allocator wraps
around at 65535. In-flight packet IDs (awaiting PUBACK/PUBCOMP) are tracked in a
`ConcurrentHashMap<Integer, InflightMessage>` on the session. If all 65535 IDs are in use
(should never happen with reasonable `mqtt.max.inflight = 65535`), the broker sends
DISCONNECT (v5.0: 0x93 Receive Maximum Exceeded).

---

## 9. Session Management

### 9.1 Session lifecycle

```
┌─────────────┐     CONNECT         ┌──────────────┐
│  No Session  │────(cleanSession=   │    Active     │
│              │     true)──────────►│   Session     │
└──────┬───────┘                     │  (connected)  │
       │                             └──────┬────────┘
       │                                    │
       │  CONNECT                      DISCONNECT or
       │  (cleanSession=false,         connection lost
       │   no prior session)                │
       │                             ┌──────▼────────┐
       └────────────────────────────►│  Persistent   │
                                     │   Session     │
                CONNECT              │ (disconnected)│
          (cleanSession=false,       └──────┬────────┘
           prior session exists)            │
                    │                       │ Session Expiry
                    │  resume               │ Interval fires
                    └───────────────────────▼───────────────┐
                                     ┌──────────────┐       │
                                     │  Session     │       │
                                     │  Expired     │◄──────┘
                                     │  (deleted)   │
                                     └──────────────┘
```

### 9.2 Session state

Each MQTT session stores:

```java
public final class MqttSession {
    private final String clientId;
    private final TenantId tenantId;           // multi-tenant isolation
    private final MqttVersion version;

    // Connection state
    private volatile Channel channel;           // null when disconnected
    private volatile boolean connected;
    private volatile long connectedAtMs;
    private volatile long lastActivityMs;

    // Session configuration
    private final boolean persistent;           // cleanSession=false or SEI>0
    private final long sessionExpiryIntervalSec; // MQTT 5.0 (0 = transient, 0xFFFFFFFF = indefinite)

    // Subscriptions (persisted)
    private final ConcurrentHashMap<String, MqttSubscription> subscriptions;

    // Packet ID allocation (per session)
    private final AtomicInteger nextPacketId = new AtomicInteger(1);

    // QoS 1 in-flight tracking (outbound: broker → client)
    private final ConcurrentHashMap<Integer, InflightMessage> outboundInflight;

    // QoS 2 state machines (inbound and outbound)
    private final ConcurrentHashMap<Integer, QoS2InboundState> inboundQoS2;
    private final ConcurrentHashMap<Integer, QoS2OutboundState> outboundQoS2;

    // Offline message queue (bounded)
    private final ConcurrentLinkedQueue<OfflineMessage> offlineQueue;
    private final AtomicInteger offlineQueueSize = new AtomicInteger(0);

    // Will message
    private volatile MqttWillData willData;
}
```

### 9.3 Session persistence: `__mqtt_sessions` internal topic

Session state is persisted to a compacted internal Kafka topic `__mqtt_sessions`, similar
to `__consumer_offsets`:

```
Topic: __mqtt_sessions
  Partitions: mqtt.session.topic.partitions (default 50)
  Replication factor: mqtt.session.topic.replication.factor (default 3)
  Cleanup policy: compact
  Key: UTF-8("{tenantId}/{clientId}")
  Value: JSON-encoded session state
```

**Session record format:**

```json
{
  "version": 1,
  "clientId": "device-001",
  "tenantId": "default",
  "mqttVersion": 5,
  "persistent": true,
  "sessionExpiryIntervalSec": 3600,
  "connectedBrokerId": 2,
  "connectedAtMs": 1713260400000,
  "subscriptions": [
    { "topicFilter": "sensors/+/temperature", "qos": 1 },
    { "topicFilter": "commands/device-001", "qos": 1 }
  ],
  "willData": {
    "topic": "status/device-001",
    "payload": "b2ZmbGluZQ==",
    "qos": 1,
    "retain": true,
    "willDelayIntervalSec": 30,
    "properties": null
  }
}
```

**Tombstone (session deletion):** Key = `{tenantId}/{clientId}`, Value = null.

**Write-through pattern:**
- On CONNECT (session created/resumed): write session record
- On SUBSCRIBE/UNSUBSCRIBE: write updated session record
- On DISCONNECT: write updated session record (connectedBrokerId = -1)
- On session expiry: write tombstone

**Read-on-startup:**
- `MqttSessionManager` reads all records from `__mqtt_sessions` at broker startup
- Builds in-memory session index: `ConcurrentHashMap<String, MqttSession>`
- Partitions sessions by key hash — the broker that is leader for the session's partition
  is authoritative for that session

### 9.4 Session expiry

```java
// MqttSessionManager — periodic expiry sweep (every 60s):
void removeExpiredSessions() {
    long now = System.currentTimeMillis();
    sessions.forEach((key, session) -> {
        if (!session.isConnected() && session.isPersistent()) {
            long expiryMs = session.lastActivityMs()
                + (session.sessionExpiryIntervalSec() * 1000L);
            if (now > expiryMs) {
                // 1. Publish will message if still pending (§10.2)
                if (session.willData() != null) {
                    publishWill(session);
                }
                // 2. Delete session from in-memory map
                sessions.remove(key);
                // 3. Write tombstone to __mqtt_sessions
                sessionStore.delete(session.tenantId(), session.clientId());
            }
        }
    });
}
```

**MQTT 3.1.1 sessions:** `sessionExpiryIntervalSec = Long.MAX_VALUE` (sessions persist
indefinitely). Operators can set `mqtt.session.max.lifetime.sec` as a safety cap.

**MQTT 5.0 sessions:** `sessionExpiryIntervalSec` from CONNECT properties (default 0 =
transient). Can be overridden in DISCONNECT properties.

### 9.5 Session affinity and migration

Sessions are not pinned to a specific broker. Any broker can serve any client. On CONNECT:

1. Read session from `__mqtt_sessions` (may be on a different broker's partition)
2. If session's `connectedBrokerId` != this broker and `connected = true`:
   - Forward a "disconnect old connection" signal to the old broker (via internal Kafka
     protocol or direct broker-to-broker RPC)
   - This is the **client takeover** flow (§4.2, step 6)
3. Update session's `connectedBrokerId` to this broker
4. Re-register push subscriptions on this broker's `SubscriptionManager`

This means clients can reconnect to any broker in the cluster without session loss.

---

## 10. Will Messages

### 10.1 Will message lifecycle

```
CONNECT (willFlag=true)
    │
    ├── Store will in session: topic, payload, QoS, retain, willDelayInterval
    │
    ▼
Connection active (will NOT be published while connected)
    │
    ├── Normal DISCONNECT received:
    │   ├── MQTT 3.1.1: clear will (never published)
    │   └── MQTT 5.0, reason 0x00: clear will
    │       MQTT 5.0, reason 0x04: publish will (with delay)
    │
    ├── Abnormal disconnect (connection lost, keep-alive timeout):
    │   ├── willDelayInterval == 0: publish immediately
    │   └── willDelayInterval > 0: schedule publication after delay
    │       If client reconnects before delay expires: cancel publication
    │
    └── Session expiry fires:
        └── If will still pending: publish now (regardless of delay)
```

### 10.2 Will delay interval (MQTT 5.0)

```java
// MqttConnectionHandler — on abnormal disconnect:
void onChannelInactive(MqttSession session) {
    if (session.willData() == null) return;

    int delaySeconds = session.willData().willDelayIntervalSec();
    if (delaySeconds <= 0) {
        publishWill(session);
    } else {
        // Schedule delayed will publication
        ScheduledFuture<?> future = willScheduler.schedule(
            () -> {
                if (!session.isConnected()) {  // still disconnected?
                    publishWill(session);
                }
            },
            delaySeconds, TimeUnit.SECONDS);
        session.setPendingWillFuture(future);
    }
}

// On reconnect (CONNECT with same clientId):
void cancelPendingWill(MqttSession session) {
    ScheduledFuture<?> future = session.getPendingWillFuture();
    if (future != null) {
        future.cancel(false);
        session.setPendingWillFuture(null);
    }
}
```

### 10.3 Will message publication

Will messages are published as normal Kafka records. They go through the same publish path
as client PUBLISH messages (§6), including partition routing and forwarding:

```java
void publishWill(MqttSession session) {
    MqttWillData will = session.willData();
    if (will == null) return;

    String kafkaTopic = topicMapper.mapToKafka(will.topic());
    // Route to correct partition leader, append via ReplicaManager or forward
    ProduceRequest willRecord = buildProduceRequest(kafkaTopic, will.payload(),
        will.qos(), will.retain());

    // If retain flag set, also update retained message store
    if (will.retain()) {
        retainedMessageStore.store(will.topic(), will.payload(), will.qos());
    }

    // Clear will from session
    session.clearWill();
    sessionStore.update(session);
}
```

---

## 11. Retained Messages

### 11.1 Retained message semantics

- **One retained message per topic.** Publishing a new retained message replaces the old one.
- **Empty payload deletes.** PUBLISH with retain=true and empty payload removes the retained
  message for that topic.
- **Delivered on SUBSCRIBE.** When a client subscribes to a topic filter, the broker delivers
  all matching retained messages before any new messages.
- **Retain flag on delivery.** When delivering a retained message to a new subscriber, the
  RETAIN flag is set in the PUBLISH. For messages delivered from ongoing push delivery, the
  RETAIN flag is NOT set (unless MQTT 5.0 `Retain As Published` subscription option is set).

### 11.2 Retained message store: `__mqtt_retained` internal topic

```
Topic: __mqtt_retained
  Partitions: mqtt.retained.topic.partitions (default 50)
  Replication factor: mqtt.retained.topic.replication.factor (default 3)
  Cleanup policy: compact
  Key: UTF-8(mqttTopic)
  Value: retained message payload (raw bytes) + headers for QoS and metadata
```

**In-memory cache:** The broker maintains a `ConcurrentHashMap<String, RetainedMessage>`
for fast lookup on SUBSCRIBE. The cache is populated from the compacted topic at startup.

```java
public final class MqttRetainedMessageStore {
    private final ConcurrentHashMap<String, RetainedMessage> cache;
    private final KafkaProducer<byte[], byte[]> producer;  // writes to __mqtt_retained

    public void store(String topic, byte[] payload, MqttQoS qos) {
        if (payload.length == 0) {
            cache.remove(topic);
            producer.send(new ProducerRecord<>("__mqtt_retained",
                topic.getBytes(UTF_8), null));  // tombstone
        } else {
            RetainedMessage msg = new RetainedMessage(topic, payload, qos);
            cache.put(topic, msg);
            producer.send(new ProducerRecord<>("__mqtt_retained",
                topic.getBytes(UTF_8), encodeRetainedMessage(msg)));
        }
    }

    public List<RetainedMessage> getMatching(String topicFilter) {
        return cache.values().stream()
            .filter(msg -> MqttTopicMatcher.matches(topicFilter, msg.topic()))
            .toList();
    }
}
```

### 11.3 Retained message limits

| Config | Default | Description |
|---|---|---|
| `mqtt.retained.messages.max` | `100000` | Max retained messages per broker (LRU eviction) |
| `mqtt.retained.message.max.bytes` | `65536` | Max payload size for retained messages |

When `mqtt.retained.messages.max` is exceeded, the oldest retained message (by insertion
time) is evicted. The eviction writes a tombstone to `__mqtt_retained`.

---

## 12. Shared Subscriptions (Consumer Groups)

### 12.1 Shared subscription syntax

MQTT 5.0 defines shared subscriptions as a special topic filter prefix:

```
$share/{shareName}/{topicFilter}

Example: $share/workers/tasks/+/execute
  shareName  = "workers"
  topicFilter = "tasks/+/execute"
```

Multiple clients subscribing to the same `$share/{shareName}/{topicFilter}` form a
consumer group. Each message matching the topic filter is delivered to exactly one client
in the group (round-robin).

### 12.2 Mapping to Kafka consumer groups

Shared subscriptions map naturally to Kafka consumer groups:

| MQTT concept | Kafka concept |
|---|---|
| `$share/{shareName}` | Consumer group ID: `mqtt-share-{shareName}` |
| Topic filter | Subscribed topics (with wildcard matching) |
| Group members | MQTT clients subscribed to the same $share filter |
| Delivery | Round-robin (MQTT) vs partition-based (Kafka) |

**Key difference:** Kafka consumer groups assign entire partitions to consumers. MQTT shared
subscriptions do per-message round-robin. The broker bridges this gap:

```
Strategy: Each shared subscription group acts as a single Kafka consumer
that distributes records to group members at the MQTT level.

1. On first $share/{shareName}/{filter} subscription:
   Create internal Kafka consumer with groupId = "mqtt-share-{shareName}"
   Subscribe to matching Kafka topics (respecting wildcard filter)

2. On Kafka records received by the internal consumer:
   Select next group member (round-robin: AtomicInteger counter)
   Encode PUBLISH frame with effective QoS
   Write to selected member's channel

3. On member disconnect:
   Remove from round-robin pool
   If last member: pause internal Kafka consumer (don't commit offsets)

4. On member reconnect or new member joins:
   Add to round-robin pool
   Resume internal consumer if paused
```

### 12.3 End-to-end: Shared subscription delivery

```
Client A (sub: $share/workers/tasks/#, QoS 1)     Broker      Client B (same sub)
     │                                                │              │
     │  Message 1 arrives on tasks/upload             │              │
     │◄──────────── PUBLISH (msg 1) ──────────────────│              │
     │                                                │              │
     │  Message 2 arrives on tasks/process            │              │
     │                                                │──── PUBLISH (msg 2) ────►
     │                                                │              │
     │  Message 3 arrives on tasks/upload             │              │
     │◄──────────── PUBLISH (msg 3) ──────────────────│              │
     │                                                │              │
     │  (round-robin: A, B, A, B, ...)                │              │
```

### 12.4 QoS and acknowledgement for shared subscriptions

When a shared subscription member receives a QoS 1 message:
- The member must send PUBACK
- On PUBACK, the broker commits the Kafka offset for that record
- If no PUBACK within timeout: redeliver to another group member (rebalance)

This maps to Kafka's `auto.commit = false` with manual offset management.

---

## 13. Broker-to-Broker Forwarding

MQTT PUBLISH messages must be appended to the Kafka partition leader. If the receiving
broker is not the leader, the message must be forwarded. The forwarding mechanism is
**identical** to the HTTP protocol's forwarding design (§7 of http-protocol-design.md):

### 13.1 Reusing HTTP forwarding infrastructure

The `ProduceForwardThread` and `ProduceForwardManager` classes from the `http-server` module
are reused for MQTT forwarding. Both HTTP and MQTT protocols need the same operation:
forward a `ProduceRequest` to the correct partition leader and receive a `ProduceResponse`.

```
mqtt-server module
    │
    │ depends on
    ▼
forwarding-common module (extracted from http-server)
    ├── ProduceForwardThread.java
    ├── ProduceForwardManager.java
    ├── FetchForwardThread.java        (not needed for MQTT — push model)
    └── FetchForwardManager.java       (not needed for MQTT — push model)
```

**Key difference from HTTP:** MQTT does not need `FetchForwardManager`. HTTP is pull-based
(client sends fetch request → broker forwards to leader). MQTT is push-based (records are
pushed to clients via `SubscriptionManager` callbacks). The push subscription is registered
on the local broker; when a record is committed to a local partition, the local
`SubscriptionManager` broadcasts to all registered MQTT subscribers.

**Cross-broker subscriptions:** If a client on Broker 2 subscribes to a topic whose
partitions are led by Broker 1 and Broker 3, the broker must receive records from those
partitions. This is handled by:

1. For each subscribed partition not led by this broker: start an internal
   `FetchForwardThread` that tail-fetches from the leader
2. On new records received: route through `SubscriptionManager` → MQTT push delivery

This is essentially a "subscription fan-out" that ensures the subscribing broker has access
to all partition data for the subscribed topics.

### 13.2 Forwarding decision matrix

```
MQTT PUBLISH arrives at broker B for topic T:

1. Map MQTT topic T → Kafka topic K
2. Resolve partition P = stickyPartitioner(clientId)
3. Is B the leader for P?
   ├── YES → ReplicaManager.appendRecords() directly
   └── NO  → ProduceForwardManager.forward(leaderId, {P: record})
             → binary ProduceRequest to leader
             → on response: send PUBACK/PUBREC to MQTT client
```

```
MQTT SUBSCRIBE for topic filter F:

1. Resolve all Kafka topics matching F
2. For each matching topic T with partitions P0..Pn:
   For each partition Pi:
     Is B the leader for Pi?
     ├── YES → SubscriptionManager.subscribe(Pi, callback)
     │         (records delivered directly from local log)
     └── NO  → Start internal FetchForwardThread for (leaderId, Pi)
               → tail-fetch from leader, route through local SubscriptionManager
               → callback delivers PUBLISH to MQTT client
```

### 13.3 Internal fetch for subscription fan-out

```java
// MqttSubscriptionFanout — manages cross-broker subscriptions:
public class MqttSubscriptionFanout {

    // One fetch thread per remote broker (shared across all MQTT subscribers)
    private final ConcurrentHashMap<Integer, FetchForwardThread> fetchThreads;

    // Tracks which partitions are being fetched for MQTT subscriptions
    private final ConcurrentHashMap<TopicPartition, Set<String>> partitionSubscribers;

    public void startFetching(int leaderId, TopicPartition partition, String subscriberId) {
        partitionSubscribers.computeIfAbsent(partition, k -> ConcurrentHashMap.newKeySet())
            .add(subscriberId);

        FetchForwardThread thread = fetchThreads.computeIfAbsent(leaderId, id -> {
            // Create fetch thread for this remote broker (same as HTTP fetch forwarding)
            FetchForwardThread t = new FetchForwardThread(...);
            t.start();
            return t;
        });

        // Register callback: on new records from this partition, deliver to MQTT subscribers
        thread.registerCallback(partition, entries -> {
            subscriptionManager.broadcast(partition.toPartitionId(), entries);
        });
    }

    public void stopFetching(TopicPartition partition, String subscriberId) {
        Set<String> subscribers = partitionSubscribers.get(partition);
        if (subscribers != null) {
            subscribers.remove(subscriberId);
            if (subscribers.isEmpty()) {
                partitionSubscribers.remove(partition);
                // Stop fetching this partition if no subscribers remain
            }
        }
    }
}
```

---

## 14. Integration with Existing Kafka Infrastructure

### 14.1 New security protocol: `MQTT`

Add `MQTT` and `MQTTS` to `SecurityProtocol` enum (ids 6 and 7):

```java
// clients/src/main/java/org/apache/kafka/common/security/auth/SecurityProtocol.java
// Existing: PLAINTEXT(0), SSL(1), SASL_PLAINTEXT(2), SASL_SSL(3), HTTP(4), HTTPS(5)
MQTT(6, "MQTT"),
MQTTS(7, "MQTTS");
```

Add helper:
```java
public boolean isMqtt() { return this == MQTT || this == MQTTS; }
```

`listener.security.protocol.map` example:
```properties
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,SSL:SSL,HTTP:HTTP,MQTT:MQTT,MQTTS:MQTTS
listeners=PLAINTEXT://0.0.0.0:9092,HTTP://0.0.0.0:9094,MQTT://0.0.0.0:1883,MQTTS://0.0.0.0:8883
advertised.listeners=PLAINTEXT://broker1:9092,MQTT://broker1:1883
```

### 14.2 MqttAcceptor — plugging into SocketServer

```scala
// core/src/main/scala/kafka/network/SocketServer.scala

def createDataPlaneAcceptorAndProcessors(endpoint: Endpoint): Unit = {
  endpoint.securityProtocol match {
    case SecurityProtocol.MQTT | SecurityProtocol.MQTTS =>
      val mqttAcceptor = new MqttAcceptor(this, endpoint, config, ...)
      mqttAcceptors.put(endpoint, mqttAcceptor)
    case SecurityProtocol.HTTP | SecurityProtocol.HTTPS =>
      val httpAcceptor = new HttpAcceptor(this, endpoint, config, ...)
      httpAcceptors.put(endpoint, httpAcceptor)
    case _ =>
      val dataPlaneAcceptor = new DataPlaneAcceptor(this, endpoint, config, ...)
      dataPlaneAcceptors.put(endpoint, dataPlaneAcceptor)
  }
}
```

### 14.3 MqttAcceptor — Netty-based server

```scala
class MqttAcceptor(
  socketServer:   SocketServer,
  endpoint:       Endpoint,
  config:         KafkaConfig,
  requestChannel: RequestChannel,
  sessionManager: MqttSessionManager,
  ...
) extends Closeable {

  private val bossGroup:   EventLoopGroup = new NioEventLoopGroup(1)
  private val workerGroup: EventLoopGroup = new NioEventLoopGroup(config.numMqttNetworkThreads)

  def startup(): Unit = {
    val bootstrap = new ServerBootstrap()
    bootstrap
      .group(bossGroup, workerGroup)
      .channel(classOf[NioServerSocketChannel])
      .childOption(ChannelOption.TCP_NODELAY, Boolean.box(true))     // low latency
      .childOption(ChannelOption.SO_KEEPALIVE, Boolean.box(true))
      .childHandler(new MqttChannelInitializer(config, requestChannel, sessionManager, ...))
    channel = bootstrap.bind(endpoint.host, endpoint.port).sync().channel()
  }
}
```

### 14.4 MqttChannelInitializer — Netty pipeline

```scala
class MqttChannelInitializer(config: KafkaConfig, requestChannel: RequestChannel, ...)
    extends ChannelInitializer[SocketChannel] {

  override def initChannel(ch: SocketChannel): Unit = {
    val pipeline = ch.pipeline()

    // Optional TLS (for MQTTS)
    if (endpoint.securityProtocol == SecurityProtocol.MQTTS)
      pipeline.addLast("ssl", sslContext.newHandler(ch.alloc()))

    // MQTT frame decoder: splits TCP stream into complete MQTT packets
    pipeline.addLast("mqtt-frame-decoder",
      new MqttFrameDecoder(config.mqttMaxPacketSize))

    // MQTT packet decoder: parses frame bytes into typed packet objects
    pipeline.addLast("mqtt-packet-decoder",
      new MqttPacketDecoder())

    // Connection handler: CONNECT/CONNACK, client registry, session management
    pipeline.addLast("mqtt-connection",
      new MqttConnectionHandler(config, sessionManager, principalBuilder, clientRegistry))

    // Keep-alive handler: monitors client ping interval
    pipeline.addLast("mqtt-keepalive",
      new MqttKeepAliveHandler())

    // Request handler: routes PUBLISH, SUBSCRIBE, UNSUBSCRIBE, PING, DISCONNECT
    pipeline.addLast("mqtt-request",
      new MqttRequestHandler(config, requestChannel, sessionManager,
          subscriptionBridge, retainedMessageStore, produceForwardManager))

    // MQTT encoder: encodes outbound MQTT packets to bytes
    pipeline.addLast("mqtt-encoder",
      new MqttPacketEncoder())

    // Exception handler: catch-all for protocol errors
    pipeline.addLast("mqtt-exception",
      new MqttExceptionHandler())
  }
}
```

### 14.5 MQTT frame decoder

The MQTT fixed header uses a Variable Byte Integer (VBI) encoding for the remaining length:

```java
public class MqttFrameDecoder extends ByteToMessageDecoder {

    private final int maxPacketSize;

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        if (in.readableBytes() < 2) return;  // need at least fixed header

        in.markReaderIndex();
        in.readByte();  // packet type + flags

        // Decode VBI (1-4 bytes)
        int remainingLength = 0;
        int multiplier = 1;
        int vbiBytes = 0;
        byte b;
        do {
            if (in.readableBytes() < 1) {
                in.resetReaderIndex();
                return;  // incomplete VBI
            }
            b = in.readByte();
            remainingLength += (b & 0x7F) * multiplier;
            multiplier *= 128;
            vbiBytes++;
            if (vbiBytes > 4) {
                throw new TooLongFrameException("Malformed VBI: more than 4 bytes");
            }
        } while ((b & 0x80) != 0);

        // Check max packet size
        int totalLength = 1 + vbiBytes + remainingLength;
        if (totalLength > maxPacketSize) {
            throw new TooLongFrameException("Packet size " + totalLength
                + " exceeds max " + maxPacketSize);
        }

        // Wait for complete frame
        if (in.readableBytes() < remainingLength) {
            in.resetReaderIndex();
            return;  // incomplete frame
        }

        // Emit complete frame
        in.resetReaderIndex();
        out.add(in.readRetainedSlice(totalLength));
    }
}
```

### 14.6 MQTT protocol detection (for shared-port mode)

If `mqtt.shared.port = true`, the MQTT acceptor shares a port with other protocols (like
the ivy-ref project's multi-protocol detection). Detection is based on the first byte:

```java
// MqttProtocolDetector — magic byte check:
static boolean isMqttConnect(ByteBuf buf) {
    if (buf.readableBytes() < 2) return false;
    byte firstByte = buf.getByte(buf.readerIndex());
    // CONNECT packet type is 0x10 (type=1, flags=0)
    // But we should also check for other valid MQTT packet types
    int packetType = (firstByte & 0xF0) >> 4;
    return packetType >= 1 && packetType <= 15 && packetType != 0;
}
```

Default mode: dedicated ports (1883/8883), no detection needed.

### 14.7 MqttProcessor — response routing bridge

Like `HttpProcessor` (§8.6 of http-protocol-design.md), `MqttProcessor` bridges responses
from `RequestChannel` back to the correct Netty channel:

```java
public class MqttProcessor {

    private final int id;  // registered with RequestChannel.addProcessor()
    private final ConcurrentHashMap<String, MqttResponseContext> pendingResponses;

    // Called by MqttRequestHandler when a PUBLISH is enqueued
    public void registerPending(String connectionId, int packetId,
                                 MqttQoS qos, ChannelHandlerContext ctx) {
        pendingResponses.put(connectionId + ":" + packetId,
            new MqttResponseContext(ctx, packetId, qos));
    }

    // Called by RequestChannel.sendResponse()
    public void enqueueResponse(RequestChannel.Response response) {
        responseQueue.add(response);
    }

    // Polled by response-drainer thread
    public void processResponses() {
        List<RequestChannel.Response> batch = new ArrayList<>();
        responseQueue.drainTo(batch);

        for (RequestChannel.Response response : batch) {
            if (response instanceof RequestChannel.SendResponse sendResp) {
                String connectionId = response.request().context().connectionId();
                // Find pending MQTT packet context
                MqttResponseContext mqttCtx = findPendingContext(connectionId);
                if (mqttCtx != null && mqttCtx.ctx().channel().isActive()) {
                    // Extract Kafka ProduceResponse → determine success/failure
                    ProduceResponse produceResp = (ProduceResponse) sendResp.response();
                    // Send PUBACK or PUBREC based on QoS
                    sendMqttAck(mqttCtx, produceResp);
                }
            }
        }
    }
}
```

---

## 15. New Module: `mqtt-server`

```
kafka/
└── mqtt-server/
    ├── build.gradle
    └── src/
        ├── main/
        │   ├── java/kafka/server/mqtt/
        │   │   ├── codec/
        │   │   │   ├── MqttFrameDecoder.java
        │   │   │   ├── MqttPacketDecoder.java
        │   │   │   ├── MqttPacketEncoder.java
        │   │   │   ├── MqttPacketType.java
        │   │   │   ├── MqttVersion.java
        │   │   │   ├── MqttQoS.java
        │   │   │   ├── MqttConnectCodec.java
        │   │   │   ├── MqttPublishCodec.java
        │   │   │   ├── MqttSubscribeCodec.java
        │   │   │   └── MqttSimpleCodec.java          (PING, DISCONNECT, QoS2 acks)
        │   │   ├── handler/
        │   │   │   ├── MqttConnectionHandler.java
        │   │   │   ├── MqttKeepAliveHandler.java
        │   │   │   ├── MqttRequestHandler.java
        │   │   │   ├── MqttPublishHandler.java
        │   │   │   ├── MqttSubscribeHandler.java
        │   │   │   ├── MqttExceptionHandler.java
        │   │   │   └── MqttAuthenticationContext.java
        │   │   ├── session/
        │   │   │   ├── MqttSession.java
        │   │   │   ├── MqttSessionManager.java
        │   │   │   ├── MqttSessionStore.java          (persistence to __mqtt_sessions)
        │   │   │   └── MqttWillData.java
        │   │   ├── subscription/
        │   │   │   ├── MqttSubscription.java
        │   │   │   ├── MqttSubscriptionBridge.java
        │   │   │   ├── MqttSubscriptionFanout.java
        │   │   │   ├── MqttTopicMatcher.java
        │   │   │   ├── MqttTopicMapper.java
        │   │   │   └── MqttGroupAdapter.java          (shared subscription → consumer group)
        │   │   ├── retained/
        │   │   │   └── MqttRetainedMessageStore.java
        │   │   ├── MqttProcessor.java
        │   │   └── MqttClientRegistry.java
        │   └── scala/kafka/network/
        │       ├── MqttAcceptor.scala
        │       └── MqttChannelInitializer.scala
        └── test/
            └── java/kafka/server/mqtt/
                ├── codec/
                │   ├── MqttFrameDecoderTest.java
                │   ├── MqttConnectCodecTest.java
                │   ├── MqttPublishCodecTest.java
                │   └── MqttSubscribeCodecTest.java
                ├── handler/
                │   ├── MqttConnectionHandlerTest.java
                │   ├── MqttPublishHandlerTest.java
                │   └── MqttSubscribeHandlerTest.java
                ├── session/
                │   ├── MqttSessionManagerTest.java
                │   └── MqttSessionStoreTest.java
                ├── subscription/
                │   ├── MqttTopicMatcherTest.java
                │   ├── MqttSubscriptionBridgeTest.java
                │   └── MqttGroupAdapterTest.java
                ├── MqttRetainedMessageStoreTest.java
                ├── MqttProduceIntegrationTest.java
                ├── MqttSubscribeIntegrationTest.java
                ├── MqttQoS2IntegrationTest.java
                ├── MqttSessionPersistenceIntegrationTest.java
                └── MqttSharedSubscriptionIntegrationTest.java
```

### 15.1 `mqtt-server/build.gradle` dependencies

```groovy
dependencies {
  implementation project(':core')
  implementation project(':clients')
  implementation project(':server-common')
  implementation project(':http-server')           // reuses ProduceForwardManager
  implementation "io.netty:netty-all:${versions.netty}"

  testImplementation project(':core').sourceSets.test.output
  testImplementation "org.apache.kafka:kafka-clients:${version}:test"
  testImplementation "org.eclipse.paho:org.eclipse.paho.mqttv5.client:1.2.5"  // MQTT test client
  testImplementation "org.eclipse.paho:org.eclipse.paho.client.mqttv3:1.2.5"  // MQTT 3.1.1 test client
}
```

---

## 16. Configuration

New properties added to `KafkaConfig`:

### 16.1 Listener and network

| Property | Default | Description |
|---|---|---|
| `mqtt.enabled` | `false` | Master switch (also triggered by adding `MQTT://` to `listeners`) |
| `num.mqtt.network.threads` | `8` | Netty worker thread count. Higher than HTTP (4) because MQTT connections are long-lived and concurrent, while HTTP is short-lived request/response. |
| `mqtt.max.connections` | `100000` | Max concurrent MQTT connections per broker |
| `mqtt.max.packet.size` | `2097152` (2 MB) | Max MQTT packet size (frame decoder limit) |
| `mqtt.connection.idle.timeout.ms` | `0` (disabled) | Idle connection timeout. MQTT has its own keep-alive mechanism (§4.2), so Netty-level idle timeout is disabled by default. |

### 16.2 Session management

| Property | Default | Description |
|---|---|---|
| `mqtt.session.topic.partitions` | `50` | Partitions for `__mqtt_sessions` internal topic |
| `mqtt.session.topic.replication.factor` | `3` | Replication factor for `__mqtt_sessions` |
| `mqtt.session.max.lifetime.sec` | `604800` (7 days) | Max session lifetime for v3.1.1 sessions (which persist indefinitely by spec). Safety cap to prevent session leak. |
| `mqtt.session.expiry.sweep.interval.sec` | `60` | How often to sweep for expired sessions |
| `mqtt.max.sessions.per.broker` | `500000` | Max total sessions (connected + disconnected) per broker |

### 16.3 Subscription management

| Property | Default | Description |
|---|---|---|
| `mqtt.max.subscriptions.per.client` | `100` | Max subscriptions per MQTT client |
| `mqtt.max.wildcard.subscriptions.per.client` | `10` | Max wildcard subscriptions (expensive — global delivery) |
| `mqtt.max.qos` | `1` | Maximum QoS level supported (1 in phase 1, 2 in phase 2) |
| `mqtt.max.inflight` | `65535` | Max in-flight QoS 1/2 messages per client (MQTT 5.0 Receive Maximum) |
| `mqtt.max.inflight.qos2` | `10` | Max concurrent QoS 2 exchanges per client |

### 16.4 Publish and delivery

| Property | Default | Description |
|---|---|---|
| `mqtt.qos1.acks` | `all` | Kafka acks for QoS 1 publish (`all`, `leader`, `none`) |
| `mqtt.qos2.acks` | `all` | Kafka acks for QoS 2 publish |
| `mqtt.offline.queue.max` | `1000` | Max offline messages per persistent session |
| `mqtt.offline.message.expiry.sec` | `86400` (24h) | Offline messages older than this are dropped on drain |

### 16.5 Retained messages

| Property | Default | Description |
|---|---|---|
| `mqtt.retained.messages.max` | `100000` | Max retained messages per broker |
| `mqtt.retained.message.max.bytes` | `65536` | Max retained message payload size |
| `mqtt.retained.topic.partitions` | `50` | Partitions for `__mqtt_retained` internal topic |
| `mqtt.retained.topic.replication.factor` | `3` | Replication factor for `__mqtt_retained` |

### 16.6 Topic mapping

| Property | Default | Description |
|---|---|---|
| `mqtt.topic.mapping.strategy` | `passthrough` | Topic mapping strategy: `passthrough`, `prefix`, `custom` |
| `mqtt.topic.prefix` | `mqtt.` | Prefix for `prefix` strategy |
| `mqtt.topic.separator` | `.` | Separator replacement for `prefix` strategy (replaces `/`) |
| `mqtt.topic.auto.create` | `true` | Auto-create Kafka topics on first MQTT PUBLISH |

### 16.7 Forwarding

| Property | Default | Description |
|---|---|---|
| `mqtt.internal.forwarding.timeout.ms` | `10000` | Timeout for broker-to-broker forwarding |
| `mqtt.internal.forwarding.retries` | `1` | Max retries per forwarded group |
| `mqtt.internal.forwarding.queue.size` | `10000` | Bounded queue capacity per forward thread |

### 16.8 Shutdown

| Property | Default | Description |
|---|---|---|
| `mqtt.shutdown.drain.ms` | `5000` | Drain window during graceful shutdown (longer than HTTP because MQTT clients expect will messages and clean disconnect) |

Listener registration (standard mechanism):
```properties
listeners=PLAINTEXT://0.0.0.0:9092,MQTT://0.0.0.0:1883,MQTTS://0.0.0.0:8883
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,MQTT:MQTT,MQTTS:MQTTS
advertised.listeners=PLAINTEXT://broker1:9092,MQTT://broker1:1883
```

---

## 17. Error Handling

### 17.1 MQTT 5.0 reason code mapping from Kafka errors

| Kafka Error | MQTT 5.0 Reason Code | Packet | Notes |
|---|---|---|---|
| `NONE` | 0x00 (Success) | PUBACK/CONNACK/SUBACK | |
| `UNKNOWN_TOPIC_OR_PARTITION` | 0x83 (Implementation specific error) | PUBACK | Topic does not exist and auto-create disabled |
| `TOPIC_AUTHORIZATION_FAILED` | 0x87 (Not authorized) | PUBACK/SUBACK | Principal lacks WRITE/READ |
| `CLUSTER_AUTHORIZATION_FAILED` | 0x87 (Not authorized) | PUBACK | |
| `MESSAGE_TOO_LARGE` | 0x95 (Packet too large) | DISCONNECT | Record exceeds broker limit |
| `NOT_ENOUGH_REPLICAS` | 0x80 (Unspecified error) | — | Internal retry, no immediate MQTT response |
| `REQUEST_TIMED_OUT` | 0x80 (Unspecified error) | — | Client retries (no PUBACK sent) |
| `KAFKA_STORAGE_ERROR` | 0x89 (Server busy) | DISCONNECT | Fatal — close connection |
| `LEADER_NOT_AVAILABLE` | 0x89 (Server busy) | DISCONNECT | Transient, client reconnects |
| `THROTTLING_QUOTA_EXCEEDED` | 0x97 (Quota exceeded) | DISCONNECT | §18.3 |

### 17.2 MQTT 3.1.1 error handling

MQTT 3.1.1 has very limited error signaling — most packet types have no error fields.
The broker's options are:

1. **CONNACK:** Only packet with reason codes (0x00–0x05)
2. **SUBACK:** Return code 0x80 (failure) per subscription
3. **Everything else:** Close the connection (the only way to signal an error)

For retriable errors (timeout, ISR issues), the broker simply withholds the ACK. The MQTT
3.1.1 client will retry the PUBLISH with DUP=1 after its own timeout.

### 17.3 Protocol violation handling

| Violation | Action |
|---|---|
| First packet is not CONNECT | Close immediately (v5.0: CONNACK 0x82) |
| Second CONNECT on same connection | Close (v5.0: DISCONNECT 0x82 Protocol Error) |
| Packet type 0 or > 15 | Close (v5.0: DISCONNECT 0x81 Malformed Packet) |
| Reserved flags incorrect | Close (v5.0: DISCONNECT 0x81 Malformed Packet) |
| PUBREL flags not 0010 | Close (v5.0: DISCONNECT 0x81 Malformed Packet) |
| UTF-8 string validation failure | Close (v5.0: DISCONNECT 0x81 Malformed Packet) |
| VBI encoding > 4 bytes | Close (`TooLongFrameException` → DISCONNECT 0x81) |
| Wildcard in PUBLISH topic | Close (v5.0: DISCONNECT 0x82 Protocol Error) |

---

## 18. Security

### 18.1 Authentication via `KafkaPrincipalBuilder`

Same pattern as HTTP (§12.1 of http-protocol-design.md). `MqttAuthenticationContext` carries
credentials from the CONNECT packet:

| Auth method | `MqttAuthenticationContext` carries | Builder behavior |
|---|---|---|
| **mTLS** (`MQTTS` listener) | `X509Certificate[]` from Netty's `SslHandler` | `DefaultKafkaPrincipalBuilder` extracts CN/SAN |
| **Username/Password** | CONNECT username + password fields | Custom builder validates against configured credentials store |
| **No auth** (`MQTT` listener) | Client ID + remote IP | Returns `ANONYMOUS` principal |
| **Enhanced Auth** (phase 3) | AUTH packet `Authentication Method` + `Authentication Data` | Custom builder handles SCRAM/OAuth/Kerberos exchange |

```java
public class MqttAuthenticationContext implements AuthenticationContext {
    private final String username;           // from CONNECT
    private final byte[] password;           // from CONNECT
    private final String clientId;
    private final InetAddress clientAddress;
    private final SSLSession sslSession;     // null if plaintext

    @Override
    public SecurityProtocol securityProtocol() {
        return sslSession != null ? SecurityProtocol.MQTTS : SecurityProtocol.MQTT;
    }
}
```

### 18.2 Authorization

MQTT operations map to Kafka ACL checks:

| MQTT operation | Kafka resource | Kafka operation | Notes |
|---|---|---|---|
| PUBLISH to topic T | Topic(T) | WRITE | T after topic mapping |
| SUBSCRIBE to filter F | Topic(T) for each matching T | READ | Checked per matching topic |
| CONNECT | Cluster | CREATE (if auto-create) | Only if `mqtt.topic.auto.create=true` |
| Will message publish | Topic(willTopic) | WRITE | Checked at CONNECT time |

### 18.3 Quota enforcement

MQTT clients are identified by `clientId` from the CONNECT packet. The quota key is
`(clientId, principal.getName())`, same as binary protocol clients.

**Throttle behavior for MQTT:**

Unlike HTTP (429 + Retry-After), MQTT has no standard throttle mechanism. The broker uses
two strategies:

1. **Publish throttle:** Delay PUBACK by `throttleTimeMs` before sending. The MQTT client
   waits for PUBACK before sending more QoS 1 messages (assuming `max.inflight = 1`).
   For QoS 0 (no ack), the broker drops messages that exceed the quota.

2. **Subscribe throttle:** Slow down push delivery by rate-limiting the number of PUBLISH
   frames sent per second to the client. Use Netty's `ChannelTrafficShapingHandler` with
   write limit derived from the client's quota.

3. **MQTT 5.0 DISCONNECT on quota exceeded:** Send DISCONNECT with reason 0x97
   (Quota Exceeded) for severe quota violations.

---

## 19. Implementation Plan

### Phase 1 — Core MQTT 3.1.1 + 5.0 (QoS 0 and 1)

1. Add `MQTT(6)` / `MQTTS(7)` to `SecurityProtocol` enum with `isMqtt()` helper
2. Create `mqtt-server` Gradle submodule with Netty dependency
3. Implement MQTT frame decoder (`MqttFrameDecoder`) — VBI parsing, frame boundary detection
4. Implement MQTT packet decoder/encoder (`MqttPacketDecoder`, `MqttPacketEncoder`)
5. Implement MQTT codecs: `MqttConnectCodec`, `MqttPublishCodec`, `MqttSubscribeCodec`, `MqttSimpleCodec`
6. Implement `MqttConnectionHandler` — CONNECT/CONNACK, protocol version detection, client ID handling
7. Implement `MqttSession` — per-client state, subscription tracking, packet ID allocation
8. Implement `MqttSessionManager` — session lifecycle, in-memory storage, LRU eviction
9. Implement `MqttTopicMapper` — MQTT → Kafka topic mapping (passthrough strategy)
10. Implement `MqttTopicMatcher` — wildcard matching algorithm (+, #, $ rules)
11. Implement `MqttPublishHandler` — PUBLISH → Kafka append → PUBACK (QoS 0 and 1)
12. Implement `MqttSubscribeHandler` — SUBSCRIBE/UNSUBSCRIBE → subscription registration → SUBACK/UNSUBACK
13. Implement `MqttSubscriptionBridge` — push delivery from Kafka to MQTT clients
14. Implement `MqttKeepAliveHandler` — PINGREQ/PINGRESP, timeout monitoring
15. Wire `MqttAcceptor` into `SocketServer.createDataPlaneAcceptorAndProcessors()`
16. Add dispatch branch in `KafkaApis.handle()` for `securityProtocol.isMqtt`
17. Implement `MqttProcessor` — response routing bridge (RequestChannel → Netty)
18. Implement `MqttClientRegistry` — client ID uniqueness, takeover
19. Implement `MqttAuthenticationContext` (§18.1)
20. Add `MqttAcceptor` → `RequestChannel` integration with `tryEnqueue()` (non-blocking)
21. Add new config properties to `KafkaConfig` (§16)
22. Integration test: CONNECT/DISCONNECT lifecycle
23. Integration test: single-topic PUBLISH/SUBSCRIBE (QoS 0 and 1)
24. Integration test: wildcard subscription matching

### Phase 2 — QoS 2, sessions, retained messages, will messages

25. Implement QoS 2 state machine (`QoS2InboundState`, `QoS2OutboundState`)
26. Implement QoS 2 packet handling: PUBREC, PUBREL, PUBCOMP
27. Implement `MqttSessionStore` — persistence to `__mqtt_sessions` compacted topic
28. Implement session recovery on broker restart (read `__mqtt_sessions` at startup)
29. Implement offline message queuing and drain on reconnect
30. Implement `MqttRetainedMessageStore` — persistence to `__mqtt_retained` compacted topic
31. Implement retained message delivery on SUBSCRIBE
32. Implement will message handling: store, publish on abnormal disconnect, will delay interval
33. Implement will message persistence in `__mqtt_sessions`
34. Implement MQTT 5.0 properties in CONNACK (Maximum QoS, Retain Available, Maximum Packet Size, etc.)
35. Implement broker-to-broker forwarding for MQTT PUBLISH (reuse `ProduceForwardManager`)
36. Implement subscription fan-out for cross-broker subscriptions (`MqttSubscriptionFanout`)
37. Implement MQTT 5.0 session expiry interval (CONNECT + DISCONNECT override)
38. Integration tests: QoS 2, session persistence, retained messages, will messages

### Phase 3 — Shared subscriptions, enhanced auth, advanced features

39. Implement shared subscriptions: `$share/{name}/{filter}` → Kafka consumer group
40. Implement `MqttGroupAdapter` — round-robin delivery within shared group
41. Implement MQTT 5.0 enhanced authentication (AUTH packet, multi-round-trip)
42. Implement topic alias support (MQTT 5.0 §3.3.2.3.4)
43. Implement Receive Maximum flow control (MQTT 5.0 §3.2.2.3.3)
44. Implement subscription identifiers (MQTT 5.0 §3.8.2.1.2)
45. Implement message expiry interval enforcement (MQTT 5.0 §3.3.2.3.3)
46. MQTT over WebSocket support (HTTP upgrade → WebSocket → MQTT frames)
47. Metrics: `mqtt.connect.rate`, `mqtt.publish.rate`, `mqtt.subscribe.rate`, `mqtt.active.connections`, `mqtt.sessions.total`, `mqtt.offline.queue.size`
48. Graceful shutdown: DISCONNECT to all connected clients (v5.0: reason 0x8B Server Shutting Down)
49. Integration tests: shared subscriptions, auth, WebSocket

### Phase 4 — Performance, observability, advanced features

50. Performance benchmarking: 100K concurrent connections, throughput, latency
51. Wildcard subscription index optimization (inverted topic-level index — §20.3)
52. Request/Response pattern (MQTT 5.0 §4.10)
53. MQTT 5.0 server redirection (reason codes 0x9C, 0x9D) for load balancing
54. `$SYS` system topics (§20.5)
55. MQTT bridge mode (connect to external MQTT brokers)

---

## 20. Implementation Concerns

### 20.1 CRITICAL — Push delivery must not block Netty worker threads

**Problem.** `MqttSubscriptionBridge.deliver()` runs on the `WriteWorker` thread (or
`FlushEventDispatcher` thread). If it calls `channel.writeAndFlush()` synchronously and the
channel's write buffer is full (slow client), the call blocks until the buffer drains — this
stalls the `WriteWorker` for ALL partitions, not just the one delivering to the slow client.

**Solution: check `channel.isWritable()` before writing.**

```java
void deliver(Channel channel, String topic, byte[] payload, MqttQoS qos, int packetId) {
    if (!channel.isActive()) {
        enqueueOffline(topic, payload, qos);
        return;
    }
    if (!channel.isWritable()) {
        // Channel write buffer full — drop message for QoS 0, queue for QoS 1/2
        if (qos == MqttQoS.AT_MOST_ONCE) {
            mqttDroppedMessagesCounter.increment();
            return;
        }
        enqueueOffline(topic, payload, qos);
        return;
    }
    ByteBuf frame = MqttPublishCodec.encode(topic, payload, qos, packetId, false);
    channel.writeAndFlush(frame);
}
```

**`channel.isWritable()`** is a non-blocking check that returns false when the channel's
`ChannelOutboundBuffer` exceeds the high-water mark (default 64 KB). Set a reasonable
write buffer water mark:

```java
// In MqttChannelInitializer:
ch.config().setWriteBufferWaterMark(
    new WriteBufferWaterMark(32 * 1024, 64 * 1024));  // 32 KB low, 64 KB high
```

---

### 20.2 CRITICAL — Session store read at startup must be bounded

**Problem.** `MqttSessionManager` reads all records from `__mqtt_sessions` at broker startup.
With 500K sessions × ~1 KB each = 500 MB of data to read and deserialize before the broker
can accept MQTT connections.

**Solution: parallel partition reads with a startup deadline.**

```java
// MqttSessionStore.loadSessions() — called at broker startup:
void loadSessions(Duration deadline) {
    KafkaConsumer<byte[], byte[]> consumer = createInternalConsumer("__mqtt_sessions");
    consumer.assign(allPartitions);
    consumer.seekToBeginning(allPartitions);

    long deadlineMs = System.currentTimeMillis() + deadline.toMillis();
    int loaded = 0;

    while (System.currentTimeMillis() < deadlineMs) {
        ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(100));
        if (records.isEmpty()) break;  // reached end of all partitions

        for (ConsumerRecord<byte[], byte[]> record : records) {
            if (record.value() == null) {
                // Tombstone — delete session
                sessionMap.remove(keyToSessionId(record.key()));
            } else {
                MqttSession session = deserialize(record.value());
                sessionMap.put(session.sessionId(), session);
                loaded++;
            }
        }
    }
    log.info("Loaded {} MQTT sessions from __mqtt_sessions in {}ms",
        loaded, System.currentTimeMillis() - (deadlineMs - deadline.toMillis()));
}
```

**Startup deadline:** `mqtt.session.load.timeout.ms` (default 30000). If the deadline fires
before all records are read, the broker starts accepting connections with a partial session
view. Missing sessions will be treated as new (clients reconnect with clean session if their
session is not found).

---

### 20.3 HIGH — Wildcard subscription matching performance

**Problem.** Global wildcard subscriptions (`subscribeGlobal()`) see ALL records on ALL
partitions and filter by topic match in the callback. With 1000 topics × 100 wildcard
subscribers, every record triggers 100 `MqttTopicMatcher.matches()` calls.

**Phase 1 solution (simple):** O(subscribers) matching per record. Acceptable for ≤100
wildcard subscribers.

**Phase 2 optimization (inverted index):**

```java
// MqttTopicIndex — O(1) lookup by topic level:
public class MqttTopicIndex {
    // Trie of topic levels → set of subscriberIds
    private final TrieNode root = new TrieNode();

    void addSubscription(String topicFilter, String subscriberId) {
        String[] levels = topicFilter.split("/", -1);
        TrieNode node = root;
        for (String level : levels) {
            node = node.children.computeIfAbsent(level, k -> new TrieNode());
        }
        node.subscribers.add(subscriberId);
    }

    Set<String> getMatchingSubscribers(String topicName) {
        String[] levels = topicName.split("/", -1);
        Set<String> result = new HashSet<>();
        matchRecursive(root, levels, 0, result);
        return result;
    }

    private void matchRecursive(TrieNode node, String[] levels, int depth, Set<String> result) {
        if (depth == levels.length) {
            result.addAll(node.subscribers);
            // Also check for trailing # at this depth
            TrieNode hashNode = node.children.get("#");
            if (hashNode != null) result.addAll(hashNode.subscribers);
            return;
        }
        // Exact match
        TrieNode exact = node.children.get(levels[depth]);
        if (exact != null) matchRecursive(exact, levels, depth + 1, result);
        // + wildcard
        TrieNode plus = node.children.get("+");
        if (plus != null) matchRecursive(plus, levels, depth + 1, result);
        // # wildcard — matches all remaining levels
        TrieNode hash = node.children.get("#");
        if (hash != null) result.addAll(hash.subscribers);
    }
}
```

This reduces matching from O(subscribers) to O(topic_levels × fan_out) — typically O(3–5)
for most MQTT topic hierarchies.

---

### 20.4 HIGH — Keep-alive timeout precision

**Problem.** MQTT spec §3.1.2.10 requires the broker to disconnect a client that does not
send any packet (including PINGREQ) within 1.5 × Keep Alive seconds. With 100K connections,
maintaining per-connection timers is expensive.

**Solution: use Netty's `IdleStateHandler` per connection.**

```java
// MqttConnectionHandler — after successful CONNECT:
int keepAliveSec = connectData.keepAlive();
if (keepAliveSec > 0) {
    // Per spec: broker must wait 1.5× the keep alive value
    long timeoutMs = (long)(keepAliveSec * 1500);
    ctx.pipeline().addBefore("mqtt-request", "mqtt-idle",
        new IdleStateHandler(timeoutMs, 0, 0, MILLISECONDS));
}
```

Netty's `IdleStateHandler` uses a hashed-wheel timer internally (shared across all channels
on the same event loop), which is O(1) per tick regardless of connection count. This is
efficient for 100K+ connections.

---

### 20.5 MEDIUM — `$SYS` system topics

MQTT convention (not spec-mandated) defines `$SYS/` prefix for broker metrics:

| Topic | Value |
|---|---|
| `$SYS/broker/clients/connected` | Number of connected clients |
| `$SYS/broker/clients/total` | Number of total sessions |
| `$SYS/broker/messages/received` | Total messages received |
| `$SYS/broker/messages/sent` | Total messages sent |
| `$SYS/broker/uptime` | Broker uptime in seconds |

Published periodically (every `mqtt.sys.topic.interval.sec`, default 60) as retained
messages. These are virtual — they don't go through Kafka, just the retained message store.

---

### 20.6 MEDIUM — Metrics

MQTT-specific metrics exposed via `KafkaMetricsGroup`:

| Metric | Type | Tags | Description |
|---|---|---|---|
| `mqtt.connections.active` | Gauge | — | Currently connected MQTT clients |
| `mqtt.sessions.total` | Gauge | `state={connected,disconnected}` | Total sessions |
| `mqtt.connect.rate` | Meter | — | CONNECT packets per second |
| `mqtt.disconnect.rate` | Meter | `reason={normal,keepalive,error,takeover}` | DISCONNECT rate |
| `mqtt.publish.inbound.rate` | Meter | `qos={0,1,2}` | Client → broker PUBLISH rate |
| `mqtt.publish.outbound.rate` | Meter | `qos={0,1,2}` | Broker → client PUBLISH rate |
| `mqtt.subscribe.rate` | Meter | — | SUBSCRIBE packets per second |
| `mqtt.puback.latency.ms` | Histogram | — | Time from PUBLISH to PUBACK |
| `mqtt.forward.rate` | Meter | — | Forwarded PUBLISH rate |
| `mqtt.forward.error.rate` | Meter | — | Forwarding failures |
| `mqtt.offline.queue.size` | Gauge | — | Total offline messages across all sessions |
| `mqtt.retained.messages.count` | Gauge | — | Retained messages in store |
| `mqtt.dropped.messages.rate` | Meter | `reason={qos0_backpressure,offline_full}` | Dropped messages |

---

### 20.7 MEDIUM — Graceful shutdown

```scala
// MqttAcceptor — graceful shutdown:
def beginDrain(): Unit = {
  accepting.set(false)

  // Send DISCONNECT to all connected MQTT 5.0 clients
  clientRegistry.forEachConnected { (clientId, channel) =>
    if (channel.attr(MQTT_VERSION_KEY).get() == MqttVersion.V5_0) {
      ByteBuf disconnect = MqttSimpleCodec.encodeDisconnect(
        0x8B,  // Server Shutting Down
        MqttVersion.V5_0)
      channel.writeAndFlush(disconnect)
    }
    // MQTT 3.1.1: just close (no server-initiated DISCONNECT)
  }
}

def awaitDrain(timeoutMs: Long): Unit = {
  val deadline = System.currentTimeMillis() + timeoutMs
  // Wait for in-flight PUBACK/PUBCOMP to complete
  while (mqttProcessor.pendingCount() > 0 && System.currentTimeMillis() < deadline)
    Thread.sleep(50)
}
```

---

### 20.8 LOW — Multi-tenant isolation

All MQTT operations are scoped by `TenantId`. In single-tenant mode (default), all clients
share `TenantId.DEFAULT`. In multi-tenant mode, `TenantId` is resolved from:

1. TLS SNI hostname (MQTTS): `tenant1.mqtt.example.com` → `TenantId("tenant1")`
2. MQTT 5.0 user property: `tenant=tenant1` in CONNECT properties
3. Username prefix: `tenant1/username` → `TenantId("tenant1")`

Per-tenant isolation applies to:
- Session namespace: `{tenantId}/{clientId}` is the unique key
- Subscription visibility: clients only see records for their tenant
- Retained messages: scoped by tenant
- Quotas: per-tenant quota allocation

---

## 21. MQTT 3.1.1 vs 5.0 Compatibility Matrix

| Feature | MQTT 3.1.1 | MQTT 5.0 | Implementation notes |
|---|---|---|---|
| **Protocol level** | 4 | 5 | Detected in CONNECT |
| **Clean Session** | `cleanSession` flag | `cleanStart` flag + Session Expiry Interval | v5.0 separates "start clean" from "persist after disconnect" |
| **Session persistence** | Indefinite if cleanSession=false | Configurable via SEI (0 = transient, 0xFFFFFFFF = indefinite) | v3.1.1: cap via `mqtt.session.max.lifetime.sec` |
| **Reason codes** | Limited (CONNACK only) | All packets | v3.1.1: close connection on error |
| **Server DISCONNECT** | Not supported | Supported with reason code | v3.1.1: just close |
| **Properties** | None | All packets | v5.0 properties stored/forwarded as raw bytes |
| **Will Delay** | Not supported | 0x18 property | v3.1.1: immediate will on abnormal disconnect |
| **Session Expiry Override** | Not supported | In DISCONNECT properties | v3.1.1: N/A |
| **Maximum Packet Size** | Not negotiated | Negotiated in CONNACK (0x27) | v3.1.1: use configured max |
| **Shared subscriptions** | Not standard | `$share/` prefix | v3.1.1: not available |
| **Topic alias** | Not supported | 0x23 property (phase 3) | v3.1.1: N/A |
| **Subscription ID** | Not supported | 0x0B property (phase 3) | v3.1.1: N/A |
| **User properties** | Not supported | 0x26 property (repeatable) | v3.1.1: N/A |
| **Enhanced auth** | Not supported | AUTH packet (phase 3) | v3.1.1: username/password only |
| **Retain Available** | Implied (always yes) | Advertised in CONNACK (0x25) | v3.1.1: assume available |
| **Maximum QoS** | Implied (QoS 2) | Advertised in CONNACK (0x24) | v3.1.1: downgrade in SUBACK |
| **UNSUBACK payload** | No payload (just packet ID) | Per-filter reason codes | v3.1.1: bare UNSUBACK |

---

## 22. Comparison with Existing MQTT-Kafka Bridges

### 22.1 External bridges

| Solution | Architecture | Limitations |
|---|---|---|
| **Confluent MQTT Proxy** | Separate process, Kafka producer/consumer internally | Single point of failure, no session persistence, no QoS 2, no retained messages |
| **HiveMQ Extension for Kafka** | HiveMQ plugin, Kafka producer/consumer | Requires HiveMQ license, separate cluster to manage |
| **Eclipse Streamsheets** | Separate process, Kafka Streams | Complex deployment, limited MQTT feature support |
| **Custom Kafka Connect MQTT Source/Sink** | Kafka Connect framework | No bidirectional messaging, no session management |

### 22.2 What this design offers

| Capability | External bridge | Native broker (this design) |
|---|---|---|
| Zero additional processes | No | Yes |
| Single-hop latency | No (2 hops) | Yes |
| Session persistence | Manual (external DB) | Automatic (`__mqtt_sessions`) |
| Retained messages | Manual (external store) | Automatic (`__mqtt_retained`) |
| Will messages | Limited | Full spec compliance |
| QoS 2 exactly-once | Rare | Yes (phase 2) |
| Shared subscriptions | No | Yes (phase 3) |
| Replication & HA | Separate HA for bridge | Inherits Kafka ISR |
| Auth integration | Separate auth stack | Same `KafkaPrincipalBuilder` + `Authorizer` |
| Quota enforcement | Not available | Same `ClientQuotaManager` |
| Metrics | Separate monitoring | Same JMX/Prometheus endpoint |
| Cross-protocol pub/sub | Limited | Native (Kafka producer → MQTT subscriber) |

---

*Document version: 0.1 — 2026-04-16*
*Branch: feature/http-protocol*
