# Multi-Tenant Architecture for Apache Kafka — Design Document

## Table of Contents

1. [Overview](#1-overview)
2. [Goals & Non-Goals](#2-goals--non-goals)
3. [Architecture Overview](#3-architecture-overview)
   - 3.1 8-Layer Security Pipeline
   - 3.2 Module Ownership
   - 3.3 Thread Model
4. [Tenant Context & Propagation](#4-tenant-context--propagation)
   - 4.1 TenantId
   - 4.2 TenantContext Record
   - 4.3 ScopedValue Propagation
   - 4.4 Netty Channel Attribute Fallback
   - 4.5 TenantContextPropagator
5. [Tenant Resolution via SNI](#5-tenant-resolution-via-sni)
   - 5.1 SNI Hostname Extraction
   - 5.2 Tenant ID Derivation
   - 5.3 Auto-Provisioning
6. [Tenant Lifecycle Management](#6-tenant-lifecycle-management)
   - 6.1 TenantStatus State Machine
   - 6.2 Create Tenant
   - 6.3 Suspend Tenant (8-Step Flow)
   - 6.4 Activate Tenant (4-Step Flow)
   - 6.5 Delete Tenant (10-Step Flow)
7. [Network Pipeline](#7-network-pipeline)
   - 7.1 TLS / SNI Pipeline
   - 7.2 Protocol Detection
   - 7.3 Tenant Context Handler
   - 7.4 Codec Context Integration
8. [Storage Isolation](#8-storage-isolation)
   - 8.1 Segment-Level Tenant ID
   - 8.2 Metadata Store Isolation
   - 8.3 Internal Topics
9. [Write Path Isolation](#9-write-path-isolation)
   - 9.1 WriteAccumulator Tenant Checks
   - 9.2 PendingWrite Tenant Propagation
   - 9.3 Tenant Suspension in Write Path
10. [Read Path Isolation](#10-read-path-isolation)
    - 10.1 Fetch Filtering by Tenant
    - 10.2 Subscription Manager Isolation
    - 10.3 Consumer Group & Offset Isolation
11. [Namespace Strategies](#11-namespace-strategies)
    - 11.1 PREFIX Strategy
    - 11.2 NATIVE Strategy (AMQP)
    - 11.3 SCHEMA Strategy (SQL)
    - 11.4 SESSION Strategy
12. [Protocol-Specific Multi-Tenancy](#12-protocol-specific-multi-tenancy)
    - 12.1 Kafka Protocol
    - 12.2 HTTP Protocol
    - 12.3 AMQP 0-9-1
    - 12.4 AMQP 1.0
    - 12.5 MQTT 5.0 / 3.1.1
    - 12.6 Other Protocols (STOMP, NATS, OpenWire, PgWire, MySQL)
13. [Per-Tenant Configuration](#13-per-tenant-configuration)
    - 13.1 TenantConfig
    - 13.2 TLS Configuration
    - 13.3 Authentication Configuration
    - 13.4 Quota Configuration
14. [Authentication & Authorization](#14-authentication--authorization)
    - 14.1 Protocol-Scoped ACLs
    - 14.2 Credential Storage
    - 14.3 Delegation Tokens
15. [Quota & Rate Limiting](#15-quota--rate-limiting)
16. [Error Handling](#16-error-handling)
17. [Configuration](#17-configuration)
18. [Implementation Plan](#18-implementation-plan)
19. [Implementation Concerns](#19-implementation-concerns)
20. [Comparison with Alternative Approaches](#20-comparison-with-alternative-approaches)

---

## 1. Overview

This document describes the design for adding native multi-tenant isolation to Apache Kafka
brokers. The feature allows multiple independent tenants to share the same broker cluster
while guaranteeing complete data isolation, independent namespaces, per-tenant quotas, and
per-tenant authentication — all without running separate broker instances per tenant.

### Key Properties

| Property | Behavior |
|---|---|
| **Tenant Resolution** | Client connects via TLS with SNI hostname → broker extracts subdomain → deterministic UUID tenant ID |
| **Data Isolation** | Every record carries `tenant_id` in segment trailer metadata. All SQL queries include `WHERE tenant_id = ?` (defense-in-depth) |
| **Namespace Isolation** | Same topic/queue/exchange name on different tenants → separate data stores. No cross-tenant visibility |
| **Context Propagation** | `ScopedValue<TenantContext>` (JEP 506) for virtual threads; Netty channel attribute for event loop code |
| **Lifecycle** | ACTIVE → SUSPENDED → DELETED state machine with graceful drain and async data purge |
| **Protocol Support** | All protocols (Kafka, HTTP, AMQP 0-9-1, AMQP 1.0, MQTT, STOMP, NATS, etc.) share the same tenant isolation layer |

### Motivation

- Enable SaaS deployments where a single Kafka cluster serves hundreds of independent customers
- Eliminate operational overhead of managing separate clusters per tenant
- Provide protocol-native multi-tenancy (AMQP vhost mapping, MQTT client-id scoping, Kafka topic prefixing) without application-level workarounds
- Defense-in-depth isolation: even if one layer fails, tenant_id filtering at every other layer prevents cross-contamination

### Reference Implementation

This design is derived from the Ivy reference implementation (`references/ivy-ref/`), which
provides a production-grade multi-tenant message broker supporting 13+ protocols. The Ivy
architecture has been adapted for integration with Apache Kafka's existing infrastructure
(`RequestChannel`, `KafkaApis`, `ReplicaManager`, `Authorizer`).

---

## 2. Goals & Non-Goals

### Goals

- SNI-based tenant resolution: tenant identity derived from TLS ServerName Indication hostname
- Complete data isolation: every stored record tagged with tenant_id at the segment level
- Per-tenant namespace: same resource names (topics, queues, exchanges) can exist independently per tenant
- Per-tenant configuration: TLS certificates, authentication methods, quotas configurable per tenant
- Per-tenant lifecycle management: create, suspend, activate, delete tenants with graceful drain
- Per-tenant ACLs: authorization rules scoped to (tenant, protocol, resource) triples
- Per-tenant quotas: connection limits, produce/consume rate limits, storage quotas
- Protocol-agnostic core: tenant isolation layer shared across all protocols (Kafka, HTTP, AMQP, MQTT, etc.)
- Auto-provisioning: first connection from unknown SNI hostname automatically creates tenant
- Backward compatibility: single-tenant deployments work unchanged (multi-tenancy is opt-in)

### Non-Goals

- Cross-tenant data sharing (tenants are fully isolated; sharing requires explicit replication)
- Per-tenant broker process isolation (tenants share the same JVM; isolation is logical, not physical)
- Tenant-level resource reservations (quotas cap usage but do not guarantee minimums)
- Multi-cluster federation (this design covers a single cluster; cross-cluster replication is out of scope)
- Custom per-tenant plugins (all tenants share the same broker code; customization is via configuration only)

---

## 3. Architecture Overview

### 3.1 8-Layer Security Pipeline

Every connection passes through an 8-layer pipeline before reaching the message handler.
Each layer has a single responsibility and a clear owner module.

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                           8-Layer Security Pipeline                              │
│                                                                                  │
│  Layer 0: Connection Metadata                              [ivy-router]          │
│  ┌──────────────────────────────────────────────────────────────────────┐        │
│  │  Collect: remote IP, local port, timestamp, connection ID (UUIDv7)  │        │
│  │  Optional: PROXY Protocol v2 (real client IP behind load balancer)  │        │
│  └──────────────────────────────────┬───────────────────────────────────┘        │
│                                     ▼                                            │
│  Layer 1: Tenant Resolution (SNI)                          [ivy-router]          │
│  ┌──────────────────────────────────────────────────────────────────────┐        │
│  │  Extract SNI hostname from TLS ClientHello                          │        │
│  │  Resolve: "tenant-a.broker.example.com" → TenantId(UUID)           │        │
│  │  Validate: tenant exists AND status == ACTIVE                       │        │
│  │  Reject: unknown tenant → close connection (no protocol error yet)  │        │
│  └──────────────────────────────────┬───────────────────────────────────┘        │
│                                     ▼                                            │
│  Layer 2: TLS Termination                                  [ivy-router]          │
│  ┌──────────────────────────────────────────────────────────────────────┐        │
│  │  Per-tenant SslContext (separate KeyStore + TrustStore per tenant)   │        │
│  │  TLS 1.2+ only (1.0/1.1 rejected)                                  │        │
│  │  Cipher: TLS_AES_256_GCM_SHA384, TLS_AES_128_GCM_SHA256            │        │
│  │  mTLS: optional per-tenant (client certificate validation)          │        │
│  └──────────────────────────────────┬───────────────────────────────────┘        │
│                                     ▼                                            │
│  Layer 3: Protocol Authentication                          [ivy-codec]           │
│  ┌──────────────────────────────────────────────────────────────────────┐        │
│  │  Kafka: SASL/SCRAM-SHA-512, SASL/PLAIN, SASL/OAUTHBEARER           │        │
│  │  AMQP 0-9-1: SASL PLAIN in Connection.StartOk                      │        │
│  │  AMQP 1.0: SASL PLAIN/EXTERNAL in SASL Init                        │        │
│  │  MQTT: CONNECT username/password                                    │        │
│  │  HTTP: Authorization header (Bearer/Basic)                          │        │
│  │  → Produces: AuthenticatedPrincipal(tenantId, username, groups)     │        │
│  └──────────────────────────────────┬───────────────────────────────────┘        │
│                                     ▼                                            │
│  Layer 4: Identity Mapping                                 [ivy-auth]            │
│  ┌──────────────────────────────────────────────────────────────────────┐        │
│  │  Map protocol-specific identity → canonical principal               │        │
│  │  LDAP / external identity provider integration (optional)           │        │
│  │  Default: username from auth step → KafkaPrincipal(USER, name)      │        │
│  └──────────────────────────────────┬───────────────────────────────────┘        │
│                                     ▼                                            │
│  Layer 5: ACL Authorization                                [ivy-auth]            │
│  ┌──────────────────────────────────────────────────────────────────────┐        │
│  │  Check: (tenant_id, protocol, principal, resource, operation)       │        │
│  │  Protocol-scoped: Kafka ACLs don't affect AMQP clients              │        │
│  │  Wildcard: protocol=* applies to all protocols                      │        │
│  │  Default: allow-when-empty (no ACL entries → permit all)            │        │
│  └──────────────────────────────────┬───────────────────────────────────┘        │
│                                     ▼                                            │
│  Layer 6: Quota Enforcement                                [ivy-auth]            │
│  ┌──────────────────────────────────────────────────────────────────────┐        │
│  │  Per-tenant token bucket rate limiting                              │        │
│  │  Connection limits, produce rate, consume rate, storage quota       │        │
│  │  Exceeds → protocol-appropriate throttle response                    │        │
│  └──────────────────────────────────┬───────────────────────────────────┘        │
│                                     ▼                                            │
│  Layer 7: Protocol Handler                                 [ivy-broker]          │
│  ┌──────────────────────────────────────────────────────────────────────┐        │
│  │  ZERO auth awareness — pure messaging logic                         │        │
│  │  BrokerEngine.write() / BrokerEngine.read()                         │        │
│  │  TenantContext available via ScopedValue.current()                   │        │
│  └──────────────────────────────────┬───────────────────────────────────┘        │
│                                     ▼                                            │
│  Layer 8: Audit Logging                                    [ivy-router]          │
│  ┌──────────────────────────────────────────────────────────────────────┐        │
│  │  Structured JSON: tenantId, principal, protocol, operation, outcome │        │
│  │  All security events: auth success/failure, ACL deny, quota exceed  │        │
│  └──────────────────────────────────────────────────────────────────────┘        │
└──────────────────────────────────────────────────────────────────────────────────┘
```

### 3.2 Module Ownership

| Module | Responsibility | Multi-Tenant Role |
|---|---|---|
| **ivy-router** | TLS termination, SNI resolution, connection metadata, audit | Layers 0, 1, 2, 8 |
| **ivy-auth** | Identity mapping, ACL authorization, quota enforcement | Layers 4, 5, 6 |
| **ivy-codec** | Protocol-native auth framing (SASL, CONNECT, etc.) | Layer 3 |
| **ivy-broker** | Message handling — ZERO auth awareness | Layer 7 |
| **ivy-storage** | Persistent metadata stores with tenant_id columns | Storage isolation |
| **ivy-common** | `TenantContext`, `TenantId`, `AuthEngine`, sealed types | Cross-cutting types |

**Critical design principle:** `ivy-broker` has **zero auth awareness**. It receives requests
that have already passed all security layers. Tenant context is available via `ScopedValue`
but the broker never makes authorization decisions — that responsibility belongs exclusively
to layers 1–6.

### 3.3 Thread Model

```
TCP Connection → SNI Tenant Resolution
     │
     ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  Netty Boss Thread (1)                                                   │
│  accepts TCP connections, fires SslHandshakeCompletionEvent              │
└───────────────────────────────┬──────────────────────────────────────────┘
                                │ distributes connections
┌───────────────────────────────▼──────────────────────────────────────────┐
│  Netty Worker Threads (num.network.threads)                              │
│  • TLS handshake (SNI extraction happens here)                           │
│  • Protocol detection (magic byte matching)                              │
│  • Protocol codec (decode/encode frames)                                 │
│  • TenantContextHandler sets channel attribute                           │
│  • CodecContextTenantUpdater propagates to codec                         │
│  • Protocol handler reads frames, wraps task for virtual thread          │
│                                                                          │
│  CRITICAL: Netty workers are NON-BLOCKING (Rule §2.1)                    │
│  All blocking I/O offloaded to virtual threads                           │
└───────────────────────────────┬──────────────────────────────────────────┘
                                │ TenantContextPropagator.wrap(channel, task)
┌───────────────────────────────▼──────────────────────────────────────────┐
│  Virtual Threads (per-request, JEP 444)                                  │
│  • TenantContext.runScoped(ctx, action) — binds ScopedValue              │
│  • Authentication (SASL, CONNECT, Bearer token)                          │
│  • ACL check                                                             │
│  • Quota check                                                           │
│  • BrokerEngine.write() / BrokerEngine.read()                            │
│  • WriteAccumulator.submit(PendingWrite)                                 │
│                                                                          │
│  TenantContext.current() works in any virtual thread                      │
└───────────────────────────────┬──────────────────────────────────────────┘
                                │
┌───────────────────────────────▼──────────────────────────────────────────┐
│  Write-Worker Threads (platform threads, num.write.workers)              │
│  • Drain WriteAccumulator batches                                        │
│  • Write to segment files + PostgreSQL                                   │
│  • tenant_id embedded in every TrailerMetadata record                    │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## 4. Tenant Context & Propagation

### 4.1 TenantId

A value record wrapping a UUID, used as the tenant identifier everywhere in the system.

```java
// ivy-common/src/main/java/com/ivy/common/types/TenantId.java

public value record TenantId(UUID id) {
    public TenantId {
        if (id == null) throw new NullPointerException("TenantId.id must not be null");
    }

    @Override
    public String toString() {
        return id.toString();
    }
}
```

**Design decisions:**
- `value record` (JEP 401) — zero allocation on hot paths, stack-scalarized by JIT
- UUID-based — deterministic derivation from SNI hostname via `UUID.nameUUIDFromBytes()`
- Same hostname always yields same UUID — no central ID generator needed

### 4.2 TenantContext Record

Per-connection context carrying tenant identity and metadata.

```java
// ivy-common/src/main/java/com/ivy/common/tenant/TenantContext.java

public record TenantContext(
    TenantId tenantId,          // resolved from SNI hostname
    String tenantName,          // human-readable (SNI hostname)
    ProtocolId protocol,        // detected protocol (nullable until detection)
    UUID connectionId           // unique connection ID (UUIDv7) for tracing
) {
    // ── ScopedValue (JEP 506, final JDK 25) ──
    private static final ScopedValue<TenantContext> SCOPED = ScopedValue.newInstance();

    public static void runScoped(TenantContext ctx, Runnable action) {
        ScopedValue.where(SCOPED, ctx).run(action);
    }

    public static <T> T callScoped(TenantContext ctx, Callable<T> callable) throws Exception {
        return ScopedValue.where(SCOPED, ctx).call(callable);
    }

    public static TenantContext current() {
        return SCOPED.orElseThrow();
    }

    public static Optional<TenantContext> currentOptional() {
        return SCOPED.isBound() ? Optional.of(SCOPED.get()) : Optional.empty();
    }

    public static boolean isBound() {
        return SCOPED.isBound();
    }

    // ── Netty channel attribute fallback ──
    public static final AttributeKey<TenantContext> TENANT_ATTR =
        AttributeKey.valueOf("tenantContext");
}
```

### 4.3 ScopedValue Propagation

`ScopedValue` (JEP 506) is the **primary** mechanism for tenant context propagation. It
replaces `ThreadLocal` which is unsafe with virtual threads (Rule §6.5 — memory leaks).

```
┌──────────────────────────────────────────────────────────────┐
│  Netty Worker Thread (event loop)                            │
│                                                              │
│  channelRead(ctx, msg):                                      │
│    TenantContext tc = ctx.channel().attr(TENANT_ATTR).get()  │
│    Runnable task = () -> handleRequest(msg)                   │
│    Runnable wrapped = TenantContextPropagator.wrap(tc, task) │
│    Thread.ofVirtual().start(wrapped)                         │
│                                                              │
└──────────────────────────────────────────────────────────────┘
                          │
                          ▼
┌──────────────────────────────────────────────────────────────┐
│  Virtual Thread                                              │
│                                                              │
│  TenantContext.runScoped(tc, () -> {                          │
│    // ScopedValue now bound for this virtual thread           │
│    TenantContext ctx = TenantContext.current();  // works ✓   │
│    brokerEngine.write(ctx.tenantId(), message);               │
│  });                                                         │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

**Why ScopedValue, not ThreadLocal:**

| Concern | ThreadLocal | ScopedValue |
|---|---|---|
| Virtual thread memory | Accumulates — never GC'd until VT ends | Scoped — freed when `runScoped` returns |
| Inheritance | Copies to child threads (expensive) | Inherits automatically (zero-copy) |
| Accidental leaks | Easy to forget `.remove()` | Impossible — scope is lexical |
| Performance | HashMap lookup per access | JIT-optimized, single indirection |

### 4.4 Netty Channel Attribute Fallback

ScopedValue cannot be used directly in Netty event loop code (no virtual thread boundary).
The channel attribute serves as a bridge:

```java
// Set by TenantContextHandler in channelActive():
ctx.channel().attr(TenantContext.TENANT_ATTR).set(tenantContext);

// Read by protocol handlers before offloading to virtual threads:
TenantContext tc = ctx.channel().attr(TenantContext.TENANT_ATTR).get();
```

**Lifecycle:** Set once when tenant is resolved (after TLS handshake). Cleared in
`channelInactive()`. Immutable within the connection lifetime.

### 4.5 TenantContextPropagator

Handles the wrap/unwrap at the boundary between Netty event loop and virtual threads.

```java
// ivy-common/src/main/java/com/ivy/common/tenant/TenantContextPropagator.java

public final class TenantContextPropagator {

    public static Runnable wrap(Runnable task) {
        if (TenantContext.isBound()) {
            TenantContext ctx = TenantContext.current();
            return () -> TenantContext.runScoped(ctx, task);
        }
        return task;
    }

    public static Runnable wrap(Channel channel, Runnable task) {
        // Primary: ScopedValue (if already bound)
        if (TenantContext.isBound()) {
            return wrap(task);
        }
        // Fallback: channel attribute (Netty event loop → virtual thread boundary)
        TenantContext ctx = channel.attr(TenantContext.TENANT_ATTR).get();
        if (ctx != null) {
            return () -> TenantContext.runScoped(ctx, task);
        }
        return task;
    }

    public static Executor wrapExecutor(Executor delegate) {
        return command -> delegate.execute(wrap(command));
    }

    public static <T> Callable<T> wrapCallable(Callable<T> callable) {
        if (TenantContext.isBound()) {
            TenantContext ctx = TenantContext.current();
            return () -> TenantContext.callScoped(ctx, callable);
        }
        return callable;
    }
}
```

**Pattern:** Every Netty handler that offloads work to a virtual thread MUST use
`TenantContextPropagator.wrap(channel, task)` before calling
`Thread.ofVirtual().start(wrapped)`.

---

## 5. Tenant Resolution via SNI

### 5.1 SNI Hostname Extraction

Tenant identity is derived from the TLS ServerName Indication (SNI) extension in the
ClientHello message. This is the first byte of every TLS connection and is available
before the TLS handshake completes.

```
Client                          Broker
  │                                │
  │  ClientHello                   │
  │  SNI: "tenant-a.broker.example.com"
  │──────────────────────────────►│
  │                                │
  │                          SniHandler extracts hostname
  │                          SniTenantResolver: "tenant-a" → UUID
  │                          TenantRegistry.resolve("tenant-a.broker.example.com")
  │                                │
  │  ServerHello + Certificate     │
  │  (per-tenant TLS context)      │
  │◄──────────────────────────────│
  │                                │
  │  TLS Handshake Complete        │
  │◄─────────────────────────────►│
  │                                │
  │  Protocol Detection begins     │
  │  (Kafka / AMQP / MQTT / ...)  │
```

**Netty pipeline position:**

```
pipeline:
  SniHandler                    ← extracts SNI hostname BEFORE TLS handshake
  SslHandler                    ← per-tenant SslContext (selected by SNI)
  TenantResolverHandler         ← resolves hostname → TenantId
  TenantContextHandler          ← validates tenant ACTIVE, sets channel attribute
  CodecContextTenantUpdater     ← propagates TenantId to codec context
  ProtocolDetector              ← detects Kafka / AMQP / MQTT / ...
  [Protocol-specific handlers]
```

### 5.2 Tenant ID Derivation

```java
// ivy-router/src/main/java/com/ivy/router/tenant/SniTenantResolver.java

public class SniTenantResolver {

    public Optional<TenantId> resolve(String sniHostname) {
        if (sniHostname == null || sniHostname.isEmpty()) return Optional.empty();

        // Extract first subdomain: "tenant-a.broker.example.com" → "tenant-a"
        int dot = sniHostname.indexOf('.');
        if (dot <= 0) return Optional.empty();  // bare domain, no subdomain

        String subdomain = sniHostname.substring(0, dot);

        // Deterministic UUID from subdomain — same hostname always yields same UUID
        UUID tenantUuid = UUID.nameUUIDFromBytes(
            subdomain.getBytes(StandardCharsets.UTF_8));

        return Optional.of(new TenantId(tenantUuid));
    }

    public TenantId resolveOrThrow(String sniHostname) {
        return resolve(sniHostname)
            .orElseThrow(() -> new UnknownTenantException(
                "Cannot resolve tenant from hostname: " + sniHostname));
    }
}
```

**Why deterministic UUID:**
- No central ID generator — any broker can resolve the same hostname to the same ID independently
- `UUID.nameUUIDFromBytes()` produces a v3 UUID (MD5-based namespace UUID)
- Collision risk is negligible for typical subdomain naming (billions of tenants before concern)
- Same subdomain on different base domains (e.g., `acme.us.broker.com` vs `acme.eu.broker.com`) can be handled by using the full hostname instead of subdomain

### 5.3 Auto-Provisioning

When a connection arrives with an unknown SNI hostname, the broker can automatically
create the tenant rather than rejecting the connection.

```java
// ivy-router/src/main/java/com/ivy/router/tenant/TenantLifecycleManager.java

public Optional<TenantRecord> autoProvision(String sniHostname) {
    TenantId tenantId = sniTenantResolver.resolveOrThrow(sniHostname);

    // Check in-memory cache first (fast path)
    Optional<TenantRecord> cached = tenantRegistry.get(tenantId);
    if (cached.isPresent()) return cached;

    // Check persistent store (race-safe)
    Optional<TenantRecord> persisted = pgTenantStore.getTenant(tenantId);
    if (persisted.isPresent()) {
        tenantRegistry.register(persisted.get());
        return persisted;
    }

    // Create new tenant with default config
    TenantRecord newTenant = TenantRecord.forAutoProvision(
        tenantId, sniHostname, Instant.now());
    pgTenantStore.upsertTenant(newTenant);
    tenantRegistry.register(newTenant);
    tenantStore.putTenant(newTenant);  // write to __tenants internal topic

    return Optional.of(newTenant);
}
```

**Configuration:** Auto-provisioning is controlled by `tenant.auto.provision.enabled`
(default `true`). When disabled, only pre-registered tenants can connect.

---

## 6. Tenant Lifecycle Management

### 6.1 TenantStatus State Machine

```
                  ┌──────────┐
         create → │  ACTIVE  │ ← activate
                  └────┬─────┘
                       │ suspend
                       ▼
                  ┌──────────┐
                  │ SUSPENDED│ → activate → ACTIVE
                  └────┬─────┘
                       │ delete
                       ▼
                  ┌──────────┐
                  │ DELETED  │  (terminal — never reverts)
                  └──────────┘
```

```java
// ivy-common/src/main/java/com/ivy/common/tenant/TenantStatus.java

public enum TenantStatus {
    ACTIVE,     // operational — all operations allowed
    SUSPENDED,  // exists but all operations rejected
    DELETED;    // terminal — data scheduled for purge

    public boolean canTransitionTo(TenantStatus target) {
        return switch (this) {
            case ACTIVE    -> target == SUSPENDED || target == DELETED;
            case SUSPENDED -> target == ACTIVE || target == DELETED;
            case DELETED   -> false;  // terminal
        };
    }
}
```

### 6.2 Create Tenant

```
Input: TenantDefinition { sniHostname, config (optional) }

1. Derive TenantId from sniHostname
2. Check TenantRegistry — reject if already exists
3. Write to PgTenantStore (INSERT)
4. Write to __tenants internal topic
5. Register in TenantRegistry (both id and hostname indexes)
6. Return TenantRecord
```

### 6.3 Suspend Tenant (8-Step Flow)

Suspension is a graceful operation that drains existing connections before rejecting all
new operations. This prevents data loss from in-flight writes.

```
suspendTenant(TenantId) → SuspendResult

Step 1: Mark SUSPENDED in persistence + in-memory cache
  └─ PgTenantStore.update(tenantId, status=SUSPENDED)
  └─ TenantRegistry.update(tenantId, status=SUSPENDED)
  └─ TenantStore.putTenant(updated record)  → __tenants topic

Step 2: TenantContextHandler auto-rejects new connections
  └─ channelActive() checks registry → status != ACTIVE → close

Step 3: WriteAccumulator.suspendTenant(tenantId)
  └─ Adds tenantId to suspendedTenantIds set
  └─ Future submit() calls for this tenant → TenantSuspendedException

Step 4: Drain existing connections (30s deadline)
  └─ TenantChannelRegistry.drainTenant(tenantId, deadline)
  └─ Sends protocol-appropriate close to all active connections:
       Kafka: CLUSTER_AUTHORIZATION_FAILED
       AMQP: Connection.Close(403, "tenant suspended")
       MQTT: DISCONNECT with reason code 0x98
       HTTP: 503 Service Unavailable

Step 5: Flush WriteAccumulator batches
  └─ Drain any remaining PendingWrite entries for this tenant

Step 6: Flush segments to persistent storage
  └─ StorageFlusher.flush() — force segment fsync

Step 7: Clean sealed segments
  └─ SegmentCleaner background pass

Step 8: Flush metadata segments
  └─ Ensure __tenants, __acl_entries, etc. are persisted

Return: SuspendResult { connectionsDrained, timeElapsedMs }
```

### 6.4 Activate Tenant (4-Step Flow)

```
activateTenant(TenantId):

Step 1: Mark ACTIVE in persistence + cache
Step 2: TenantContextHandler accepts new connections (lazy — next connect)
Step 3: Backfill is lazy (BackfillScheduler triggers on first read miss)
Step 4: WriteAccumulator.unsuspendTenant(tenantId) — producers resume
```

### 6.5 Delete Tenant (10-Step Flow)

```
deleteTenant(TenantId) → DeleteResult

Steps 1–8: Full suspension (if not already SUSPENDED)
Step 9: Mark DELETED, unregister from TenantRegistry
Step 10: Schedule async data purge (7-day retention default)
  └─ ScheduledExecutorService.schedule(() -> purgeData(tenantId), 7, DAYS)
```

**Data purge is deferred** to allow recovery from accidental deletion. The 7-day retention
is configurable via `tenant.delete.retention.days`.

---

## 7. Network Pipeline

### 7.1 TLS / SNI Pipeline

```
TCP Connection
  │
  ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  SniHandler (Netty built-in)                                             │
│  • Reads ClientHello SNI extension without completing handshake          │
│  • Selects per-tenant SslContext from TlsContextRegistry                 │
│  • Replaces self with SslHandler using selected context                  │
│  • Sets SNI_HOSTNAME_KEY attribute on channel                            │
└────────────────────────────────┬─────────────────────────────────────────┘
                                 ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  SslHandler (Netty, per-tenant SslContext)                                │
│  • Completes TLS handshake with tenant-specific certificate              │
│  • Extracts client certificate (for mTLS) if configured                  │
│  • Fires SslHandshakeCompletionEvent on success                          │
└────────────────────────────────┬─────────────────────────────────────────┘
                                 ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  TenantResolverHandler                                                   │
│  • Reads SNI_HOSTNAME_KEY attribute                                      │
│  • Calls TenantRegistry.resolve(sniHostname)                             │
│  • Auto-provisions if enabled and tenant unknown                         │
│  • Sets TENANT_KEY attribute (TenantId) on channel                       │
│  • Rejects if tenant not found or not ACTIVE                             │
└────────────────────────────────┬─────────────────────────────────────────┘
                                 ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  TenantContextHandler                                                    │
│  • Reads TENANT_KEY attribute                                            │
│  • Creates TenantContext(tenantId, sniHostname, null, connectionId)      │
│  • Sets TenantContext.TENANT_ATTR on channel                             │
│  • Validates tenant status == ACTIVE (defense-in-depth)                  │
│  • Closes connection if tenant not ACTIVE                                │
└────────────────────────────────┬─────────────────────────────────────────┘
                                 ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  CodecContextTenantUpdater (@Sharable, stateless)                        │
│  • Reads TenantContext from channel attribute                            │
│  • Updates CodecContext.tenantId for codec operations                    │
│  • Ensures codec always has correct tenant context                       │
└────────────────────────────────┬─────────────────────────────────────────┘
                                 ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  ProtocolDetector                                                        │
│  • Magic byte matching (first N bytes):                                  │
│      AMQP\x00\x00\x09\x01  → AMQP 0-9-1                                │
│      AMQP\x03\x01\x00\x00  → AMQP 1.0 (SASL)                           │
│      AMQP\x00\x01\x00\x00  → AMQP 1.0 (no SASL)                        │
│      \x00\x00\x00\x00...   → Kafka (4-byte length prefix)               │
│      MQTT CONNECT (0x10)    → MQTT                                       │
│      STOMP\n / CONNECT\n    → STOMP                                      │
│      INFO/CONNECT           → NATS                                       │
│      HTTP/1.1               → HTTP                                       │
│  • Timeout: 5s (configurable) — closes connection if no protocol         │
│  • Removes self from pipeline after detection                            │
│  • Updates TenantContext.protocol field                                   │
└────────────────────────────────┬─────────────────────────────────────────┘
                                 ▼
         [Protocol-Specific Codec + Handler Pipeline]
```

### 7.2 Protocol Detection

The broker supports multiplexing multiple protocols on a single TLS port. After TLS
termination and tenant resolution, the `ProtocolDetector` identifies the protocol from
the first bytes of the application-layer data.

```java
// ivy-router/src/main/java/com/ivy/router/ProtocolDetector.java

public class ProtocolDetector extends ByteToMessageDecoder {

    private static final int DETECTION_TIMEOUT_MS = 5000;

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        if (in.readableBytes() < 8) return;  // need at least 8 bytes

        ProtocolId detected = MagicByteRegistry.detect(in);
        if (detected == null && in.readableBytes() >= 16) {
            // Unknown protocol after 16 bytes — close connection
            ctx.close();
            return;
        }
        if (detected == null) return;  // need more bytes

        // Update TenantContext with detected protocol
        TenantContext tc = ctx.channel().attr(TenantContext.TENANT_ATTR).get();
        if (tc != null) {
            TenantContext updated = new TenantContext(
                tc.tenantId(), tc.tenantName(), detected, tc.connectionId());
            ctx.channel().attr(TenantContext.TENANT_ATTR).set(updated);
        }

        // Install protocol-specific pipeline and remove self
        protocolPipelineFactory.install(ctx.pipeline(), detected);
        ctx.pipeline().remove(this);
    }
}
```

### 7.3 Tenant Context Handler

Defense-in-depth validation at the connection level.

```java
// ivy-router/src/main/java/com/ivy/router/tenant/TenantContextHandler.java

public class TenantContextHandler extends ChannelInboundHandlerAdapter {

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        TenantId tenantId = ctx.channel().attr(TenantResolverHandler.TENANT_KEY).get();
        if (tenantId == null) {
            ctx.close();
            return;
        }

        Optional<TenantRecord> record = tenantRegistry.get(tenantId);
        if (record.isEmpty() || record.get().status() != TenantStatus.ACTIVE) {
            ctx.close();
            return;
        }

        String sniHostname = ctx.channel().attr(SNI_HOSTNAME_KEY).get();
        TenantContext context = new TenantContext(
            tenantId, sniHostname, null, UuidCreator.getTimeOrderedEpoch());

        ctx.channel().attr(TenantContext.TENANT_ATTR).set(context);
        tenantChannelRegistry.register(tenantId, ctx.channel());

        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        TenantContext tc = ctx.channel().attr(TenantContext.TENANT_ATTR).get();
        if (tc != null) {
            tenantChannelRegistry.unregister(tc.tenantId(), ctx.channel());
        }
        super.channelInactive(ctx);
    }
}
```

### 7.4 Codec Context Integration

The `CodecContextTenantUpdater` is a `@Sharable` (stateless) handler that bridges
the tenant context from the channel attribute into the codec's per-connection context.

```java
// ivy-router/src/main/java/com/ivy/router/tenant/CodecContextTenantUpdater.java

@ChannelHandler.Sharable
public class CodecContextTenantUpdater extends ChannelInboundHandlerAdapter {

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        updateCodecContext(ctx);
        super.channelActive(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        // Deferred update for TLS mode (TENANT_KEY may not be set until after handshake)
        updateCodecContext(ctx);
        super.channelRead(ctx, msg);
    }

    private void updateCodecContext(ChannelHandlerContext ctx) {
        TenantId tenantId = ctx.channel().attr(TenantResolverHandler.TENANT_KEY).get();
        if (tenantId != null) {
            CodecContext codecCtx = ctx.channel().attr(CodecContext.CODEC_CTX_KEY).get();
            if (codecCtx != null && codecCtx.tenantId() == null) {
                codecCtx.setTenantId(tenantId);
            }
        }
    }
}
```

---

## 8. Storage Isolation

### 8.1 Segment-Level Tenant ID

Every record written to a segment file includes the tenant ID in its trailer metadata.
This provides defense-in-depth: even if a read query bypasses higher-level tenant filtering,
the storage layer can still filter by tenant.

```java
// ivy-storage/src/main/java/com/ivy/storage/segment/TrailerMetadata.java

public value record TrailerMetadata(
    UUID destId,           // destination partition UUID
    UUID tenantId,         // TENANT ID — Rule §10.2: in EVERY record
    int protocol,          // protocol identifier (ProtocolId ordinal)
    long producerId,       // Kafka producer ID (for idempotency)
    short epoch,           // producer epoch
    int sequence,          // sequence number
    long timestampMs,      // record timestamp
    byte flags             // transactional, control, null-value, null-key
) {
    // Fixed size: 59 bytes per record
    public static final int SERIALIZED_SIZE = 16 + 16 + 4 + 8 + 2 + 4 + 8 + 1;
}
```

**Segment record layout:**

```
┌─────────────────────────────────────────┐
│  Entry Header (8 bytes)                  │
│  ├─ dataLen (4 bytes)                    │
│  └─ crc32  (4 bytes)                     │
├─────────────────────────────────────────┤
│  Key (variable length)                   │
├─────────────────────────────────────────┤
│  Value (variable length)                 │
├─────────────────────────────────────────┤
│  TrailerMetadata (59 bytes)              │
│  ├─ destId    (16 bytes, UUID)           │
│  ├─ tenantId  (16 bytes, UUID)  ◄───── defense-in-depth isolation
│  ├─ protocol  (4 bytes)                  │
│  ├─ producerId (8 bytes)                 │
│  ├─ epoch     (2 bytes)                  │
│  ├─ sequence  (4 bytes)                  │
│  ├─ timestamp (8 bytes)                  │
│  └─ flags     (1 byte)                   │
└─────────────────────────────────────────┘
```

### 8.2 Metadata Store Isolation

All PostgreSQL metadata stores use `tenant_id` as part of their primary key or WHERE clause.
This is Rule §10.2: **"tenant_id in SQL WHERE clauses even when partition_id should suffice
(defense-in-depth)"**.

| Store | Primary Key | Tenant Isolation |
|---|---|---|
| `PgTenantStore` | `(tenant_id)` | Tenant records themselves |
| `PgAclStore` | `(tenant_id, resource_type, resource_name, principal, operation)` | Per-tenant ACL entries |
| `PgAmqpBindingStore` | `(tenant_id, vhost, exchange_name, queue_name, routing_key)` | Per-tenant AMQP bindings |
| `PgAmqp10UnsettledStore` | `(tenant_id, link_name, delivery_id)` | Per-tenant AMQP 1.0 unsettled deliveries |
| `PgConsumerGroupStore` | `(tenant_id, group_id, topic_id)` | Per-tenant Kafka consumer groups |
| `PgConsumerOffsetStore` | `(tenant_id, group_id, topic_partition)` | Per-tenant committed offsets |
| `PgSubscriptionStore` | `(tenant_id, vhost, subscriber_id)` | Per-tenant subscriptions |
| `PgQuotaStore` | `(tenant_id, ...)` | Per-tenant quota configurations |
| `PgCredentialStore` | `(tenant_id, ...)` | Per-tenant credentials |

**Example — PgAmqpBindingStore:**

```sql
CREATE TABLE amqp_bindings (
    tenant_id     UUID    NOT NULL,
    vhost         TEXT    NOT NULL,
    exchange_name TEXT    NOT NULL,
    queue_name    TEXT    NOT NULL,
    routing_key   TEXT    NOT NULL DEFAULT '',
    arguments     JSONB   DEFAULT '{}',
    created_at    TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (tenant_id, vhost, exchange_name, queue_name, routing_key)
);

-- All queries ALWAYS include tenant_id:
SELECT * FROM amqp_bindings
WHERE tenant_id = ? AND vhost = ? AND exchange_name = ?;
```

### 8.3 Internal Topics

Tenant metadata is stored in log-compacted internal topics for durability and replication:

| Internal Topic | Key | Value | Purpose |
|---|---|---|---|
| `__tenants` | `tenantId.toString()` | Binary-encoded `TenantEntry` | Tenant registry |
| `__acl_entries` | `tenantId:resourceType:resource:principal:operation` | Binary-encoded `AclEntry` | ACL rules |
| `__credentials` | `tenantId:username` | Hashed credential | Authentication |
| `__quotas` | `tenantId:quotaType` | Quota limits | Rate limiting |
| `__delegation_tokens` | `tokenId` | Token metadata | Delegation tokens |

**Replay on startup:** All internal topics are replayed into in-memory caches (e.g.,
`TenantStore`, `AclStore`) at broker startup. This provides fast lookups without
PostgreSQL round-trips on the hot path.

---

## 9. Write Path Isolation

### 9.1 WriteAccumulator Tenant Checks

The `WriteAccumulator` is the gateway for all writes. It enforces tenant suspension
as the first check before accepting a write.

```java
// ivy-broker/src/main/java/com/ivy/broker/write/WriteAccumulator.java

public class WriteAccumulator {

    private final Set<UUID> suspendedTenantIds = ConcurrentHashMap.newKeySet();

    public void submit(PendingWrite write) {
        UUID tenantId = write.trailer().tenantId();

        // First check: is tenant suspended?
        if (suspendedTenantIds.contains(tenantId)) {
            throw new TenantSuspendedException(tenantId);
        }

        // Normal write path: add to per-partition batch
        // Batch flushes on: 1000 messages OR 1 MB OR 5ms linger
        PartitionBatch batch = getOrCreateBatch(write.partitionId());
        batch.add(write);
    }

    public void suspendTenant(TenantId tenantId) {
        suspendedTenantIds.add(tenantId.id());
    }

    public void unsuspendTenant(TenantId tenantId) {
        suspendedTenantIds.remove(tenantId.id());
    }
}
```

### 9.2 PendingWrite Tenant Propagation

Every `PendingWrite` carries the tenant ID in its `TrailerMetadata`. This ensures the
tenant ID is persisted with every record at the storage level.

```java
// ivy-broker/src/main/java/com/ivy/broker/write/PendingWrite.java

public value class PendingWrite {
    private final PartitionId partitionId;
    private final ByteBuf key;
    private final ByteBuf value;
    private final TrailerMetadata trailer;  // ← contains tenantId
    private final CompletableFuture<WriteResult> future;

    // trailer.tenantId() is set by the protocol handler from TenantContext
}
```

### 9.3 Tenant Suspension in Write Path

```
Protocol Handler → WriteAccumulator.submit(pendingWrite)
                          │
                    ┌─────▼──────────────────────┐
                    │ suspendedTenantIds.contains? │
                    └─────┬──────────┬────────────┘
                       NO │          │ YES
                          │          │
                    ┌─────▼────┐   ┌─▼──────────────────────────┐
                    │  Normal  │   │ throw TenantSuspendedException │
                    │  batch   │   │ → protocol-appropriate error    │
                    │  path    │   │   Kafka: CLUSTER_AUTH_FAILED    │
                    └──────────┘   │   AMQP: Channel.Close(403)     │
                                   │   MQTT: PUBACK reason=0x87      │
                                   │   HTTP: 503 Service Unavailable │
                                   └────────────────────────────────┘
```

---

## 10. Read Path Isolation

### 10.1 Fetch Filtering by Tenant

All read operations filter by tenant ID. The `PartitionId` already encodes the tenant
(partitions are tenant-scoped), but `tenant_id` is also checked in the segment trailer
as defense-in-depth.

```
Consumer Fetch Request (Kafka / HTTP / AMQP / ...)
  │
  ├─ Resolve: topicName → PartitionId (tenant-scoped hash)
  │   partitionId = hash(tenantId + logicalTopicName)
  │   ← Different tenants with same topic name → different PartitionIds
  │
  ├─ Read from OffsetIndex → segment file position
  │
  ├─ Read from segment → parse entries
  │   for each entry:
  │     assert entry.trailer.tenantId == requestTenantId  ← defense-in-depth
  │
  └─ Return only entries matching tenant
```

### 10.2 Subscription Manager Isolation

Push-based subscriptions (AMQP, MQTT, STOMP) are isolated per tenant via the
`SubscriptionManager`, which includes `tenantId` in all persistence operations.

```java
// ivy-broker/src/main/java/com/ivy/broker/subscription/SubscriptionManager.java

public void subscribe(PartitionId partitionId, Subscription sub,
                      InetSocketAddress source, ProtocolId protocol) {
    // In-memory: per-partition subscription map
    subscriptions.computeIfAbsent(partitionId, k -> new ConcurrentHashMap<>())
        .put(sub.subscriberId(), sub);

    // Persistent: PG write-through on virtual thread (Rule 2.1)
    if (pgStore != null) {
        Thread.ofVirtual().start(() -> {
            try (var conn = dataSource.getConnection()) {
                pgStore.upsert(conn, tenantId, vhost, sub.subscriberId(),
                    source, protocol, sub.filter(), sub.qos());
            }
        });
    }
}
```

### 10.3 Consumer Group & Offset Isolation

Kafka consumer groups and committed offsets are scoped per tenant:

```sql
-- PgConsumerGroupStore
SELECT * FROM consumer_groups
WHERE tenant_id = ? AND group_id = ?;

-- PgConsumerOffsetStore
SELECT * FROM consumer_offsets
WHERE tenant_id = ? AND group_id = ? AND topic_partition = ?;
```

Same `group_id` on different tenants → completely independent offset tracking.

---

## 11. Namespace Strategies

Different protocols have different native namespace mechanisms. The namespace strategy
translates between protocol-native names and tenant-scoped internal names.

### 11.1 PREFIX Strategy

Used by: **Kafka**, **NATS**, **Kinesis**, **Pub/Sub**

```
Client sends: topic "orders"
Internal:     topic "tenantUuid__orders"  (prefix with tenant UUID + separator)

namespaceResource(tenantId, "orders")   → "550e8400__orders"
denamespaceResource(tenantId, "550e8400__orders") → "orders"
belongsToTenant(tenantId, "550e8400__orders")     → true
belongsToTenant(tenantId, "other__orders")        → false
```

**Separator:** `__` (double underscore) — chosen because single underscore appears in
natural topic names but double underscore is rare.

### 11.2 NATIVE Strategy (AMQP)

Used by: **AMQP 0-9-1**, **AMQP 1.0**

AMQP has a native namespace mechanism: **virtual hosts (vhosts)**. The broker forces
the connection into a tenant-specific vhost regardless of what the client requests.

```java
// ivy-codec/src/main/java/com/ivy/codec/namespace/AmqpNativeStrategy.java

public class AmqpNativeStrategy implements NamespaceStrategy {

    @Override
    public String namespaceResource(TenantId tenantId, String resourceName) {
        return resourceName;  // NO modification — isolation via vhost
    }

    @Override
    public String denamespaceResource(TenantId tenantId, String resourceName) {
        return resourceName;  // NO modification
    }

    @Override
    public boolean belongsToTenant(TenantId tenantId, String resourceName) {
        return true;  // all resources on this connection belong to the tenant
    }

    @Override
    public String connectionNamespaceCommand(TenantId tenantId) {
        return "/" + tenantId.id();  // FORCED vhost override
    }
}
```

**CRITICAL: Vhost override.** When an AMQP 0-9-1 client sends `Connection.Open` with
vhost `/`, the broker **ignores the client's vhost** and forces the connection into
`/tenantUUID`. This prevents a malicious client from accessing another tenant's vhost.

```
Client sends:   Connection.Open(vhost="/")
Broker applies: Connection.Open(vhost="/550e8400-e29b-41d4-a716-446655440001")
Broker responds: Connection.OpenOk

Client is now scoped to tenant's vhost — all exchanges, queues, bindings
are isolated within this vhost.
```

### 11.3 SCHEMA Strategy (SQL)

Used by: **PgWire**, **MySQL Wire**

SQL protocols use schema/database as the namespace. Each tenant gets an implicit
schema prefix.

```
Client sends:   SELECT * FROM orders
Internal:       SELECT * FROM "tenant_550e8400".orders
```

### 11.4 SESSION Strategy

Used by: **MQTT**

MQTT uses client-id uniqueness per tenant. Topic names are prefixed.

```
Client connects: clientId="sensor-1"
Internal key:    tenantId + "sensor-1"

Client publishes: topic "sensors/temp"
Internal:         topic "550e8400__sensors/temp"
```

---

## 12. Protocol-Specific Multi-Tenancy

### 12.1 Kafka Protocol

**Tenant binding:** PREFIX namespace strategy (tenant UUID prefix on topic names).

**Request flow:**

```
Kafka Client → TLS/SNI → Tenant Resolution → SASL Auth
  │
  ├─ ProduceRequest(topic="orders")
  │   Broker prefixes: "550e8400__orders"
  │   Write to tenant-scoped partition
  │
  ├─ FetchRequest(topic="orders")
  │   Broker prefixes: "550e8400__orders"
  │   Read from tenant-scoped partition
  │
  └─ MetadataRequest(topics=["orders"])
      Broker returns metadata for "550e8400__orders"
      Response de-prefixes: client sees "orders"
```

**Consumer group isolation:** Group ID is prefixed: `550e8400__my-consumer-group`.
Same group name on different tenants → independent coordinator state.

### 12.2 HTTP Protocol

**Tenant binding:** PREFIX namespace strategy (same as Kafka — HTTP shares the Kafka topic namespace).

**Request flow:**

```
HTTP Client → TLS/SNI → Tenant Resolution → Bearer/Basic Auth
  │
  ├─ POST /v1/topics/orders/records
  │   Broker prefixes: "550e8400__orders"
  │   Proceeds through normal HTTP produce path (§5 of http-protocol-design.md)
  │
  └─ POST /v1/topics/orders/records:fetch
      Broker prefixes: "550e8400__orders"
      Proceeds through normal HTTP consume path (§6 of http-protocol-design.md)
```

### 12.3 AMQP 0-9-1

**Tenant binding:** NATIVE namespace strategy — vhost override.

**Connection handshake:**

```
AMQP Client                      Broker
     │                              │
     │  Protocol Header             │
     │  AMQP\x00\x00\x09\x01       │
     │────────────────────────────►│
     │                              │
     │  Connection.Start            │
     │  (mechanisms: PLAIN)         │
     │◄────────────────────────────│
     │                              │
     │  Connection.StartOk          │
     │  (mechanism=PLAIN,           │
     │   response=\0user\0pass)     │
     │────────────────────────────►│
     │                              │
     │  Authenticate via            │
     │  Amqp091Authenticator        │
     │  (no TenantId param —        │
     │   tenant from vhost override)│
     │                              │
     │  Connection.Tune             │
     │  (channel-max, frame-max,    │
     │   heartbeat)                 │
     │◄────────────────────────────│
     │                              │
     │  Connection.TuneOk           │
     │────────────────────────────►│
     │                              │
     │  Connection.Open             │
     │  (vhost="/")  ← client's    │
     │────────────────────────────►│
     │                              │
     │  Broker OVERRIDES vhost:     │
     │  vhost = "/" + tenantUUID    │
     │  (AmqpNativeStrategy         │
     │   .connectionNamespaceCmd)   │
     │                              │
     │  Connection.OpenOk           │
     │◄────────────────────────────│
     │                              │
     │  All exchanges, queues,      │
     │  bindings now scoped to      │
     │  tenant's vhost namespace    │
```

**Exchange isolation:** In-memory `ConcurrentHashMap` per connection. Each connection
is scoped to a single tenant's vhost, so exchange declarations are inherently isolated.

**Binding persistence:** `PgAmqpBindingStore` uses composite key
`(tenant_id, vhost, exchange_name, queue_name, routing_key)`.

**Queue isolation:** Same queue name on different tenants → different storage partitions.
The `PartitionId` is computed as `hash(tenantId + queueName)`.

**E2E isolation test pattern:**

```java
// Tenant A: declare exchange + queue + binding, publish 5 messages
try (var connA = amqpConn(TENANT_A_HOST, port); var chA = connA.createChannel()) {
    chA.exchangeDeclare("orders", BuiltinExchangeType.DIRECT, true);
    chA.queueDeclare("order-queue", true, false, false, null);
    chA.queueBind("order-queue", "orders", "new-order");
    for (int i = 0; i < 5; i++) {
        chA.basicPublish("orders", "new-order", null, ("a-order-" + i).getBytes());
    }
}

// Tenant B: SAME names, publish 3 messages
try (var connB = amqpConn(TENANT_B_HOST, port); var chB = connB.createChannel()) {
    chB.exchangeDeclare("orders", BuiltinExchangeType.DIRECT, true);
    chB.queueDeclare("order-queue", true, false, false, null);
    chB.queueBind("order-queue", "orders", "new-order");
    for (int i = 0; i < 3; i++) {
        chB.basicPublish("orders", "new-order", null, ("b-order-" + i).getBytes());
    }
}

// Assertion: Tenant A sees 5, Tenant B sees 3. ZERO cross-contamination.
```

### 12.4 AMQP 1.0

**Tenant binding:** NATIVE namespace strategy — tenant resolved from SNI, passed to authenticator.

**Connection handshake:**

```
AMQP 1.0 Client                  Broker
     │                              │
     │  SASL Protocol Header        │
     │  AMQP\x03\x01\x00\x00       │
     │────────────────────────────►│
     │                              │
     │  SaslMechanisms             │
     │  (PLAIN, EXTERNAL)           │
     │◄────────────────────────────│
     │                              │
     │  SaslInit(mechanism=PLAIN,   │
     │   response=\0user\0pass)     │
     │────────────────────────────►│
     │                              │
     │  Authenticate via            │
     │  Amqp10Authenticator         │
     │  .authenticate(user, pass,   │
     │    tenantId)  ← FROM SNI     │
     │                              │
     │  SaslOutcome(code=ok)        │
     │◄────────────────────────────│
     │                              │
     │  AMQP Protocol Header        │
     │  AMQP\x00\x01\x00\x00       │
     │────────────────────────────►│
     │                              │
     │  Open(containerId, hostname) │
     │────────────────────────────►│
     │                              │
     │  Open(containerId)           │
     │◄────────────────────────────│
     │                              │
     │  All sessions, links, and    │
     │  addresses now scoped to     │
     │  tenant from SNI resolution  │
```

**Key difference from AMQP 0-9-1:** The `Amqp10Authenticator` receives the `TenantId`
as an explicit parameter (resolved from the channel attribute set by `TenantResolverHandler`).
AMQP 1.0 does not have vhosts — isolation is enforced entirely through the SNI-resolved
tenant context.

**Session/link scoping:** All sessions within a connection inherit the connection's tenant
context. Links (senders/receivers) attach to addresses that are resolved within the tenant's
namespace. Handle keys are `(channel, handle)` composites — per-session, per-tenant.

**Unsettled delivery isolation:** `PgAmqp10UnsettledStore` uses
`(tenant_id, link_name, delivery_id)` as primary key.

### 12.5 MQTT 5.0 / 3.1.1

**Tenant binding:** SESSION namespace strategy — client-id uniqueness per tenant,
topic prefix.

**Isolation mechanisms:**
- Client ID: uniqueness enforced per `(tenantId, clientId)` — same clientId on different tenants is allowed
- Topics: prefixed with tenant UUID (`550e8400__sensors/temp`)
- Retained messages: scoped per tenant
- Will messages: delivered only within the tenant's namespace
- Shared subscriptions: `$share/group/topic` → group scoped per tenant

### 12.6 Other Protocols

| Protocol | Namespace Strategy | Isolation Mechanism |
|---|---|---|
| **STOMP** | PREFIX | Destination prefix |
| **NATS** | PREFIX | Subject prefix |
| **OpenWire** | PREFIX | JMS destination prefix, message selector per tenant |
| **PgWire** | SCHEMA | Schema prefix per tenant |
| **MySQL Wire** | SCHEMA | Database prefix per tenant |
| **Redis Streams** | PREFIX | Key prefix per tenant |
| **Kinesis** | PREFIX | Stream name prefix |
| **Pub/Sub** | PREFIX | Topic prefix |

---

## 13. Per-Tenant Configuration

### 13.1 TenantConfig

Each tenant has an optional configuration that overrides broker defaults.

```java
// ivy-router/src/main/java/com/ivy/router/tenant/TenantConfig.java

public record TenantConfig(
    TlsTenantConfig tls,           // per-tenant TLS settings
    AuthTenantConfig auth,         // per-tenant authentication
    String namespaceStrategy,      // PREFIX, SCHEMA, NATIVE, SESSION
    QuotaTenantConfig quotas       // per-tenant rate limits
) {
    // All fields nullable — null means "use broker default"
}
```

### 13.2 TLS Configuration

```java
public record TlsTenantConfig(
    String keystorePath,           // per-tenant keystore
    String keystorePassword,       // per-tenant keystore password
    String truststorePath,         // per-tenant truststore (for mTLS)
    String truststorePassword,     // per-tenant truststore password
    boolean mtlsRequired           // require client certificate?
) {}
```

**Per-tenant SslContext:** The `TlsContextRegistry` maintains a `ConcurrentHashMap<TenantId, SslContext>`.
When a new tenant connects, the `SniHandler` looks up the tenant's `SslContext` and uses it
for the TLS handshake. If no per-tenant context exists, the broker's default `SslContext` is used.

### 13.3 Authentication Configuration

```java
public record AuthTenantConfig(
    List<String> allowedMechanisms,   // e.g., ["SCRAM-SHA-512", "OAUTHBEARER"]
    String oauthIssuerUri,            // per-tenant OAuth issuer
    String oauthAudience,             // per-tenant OAuth audience
    boolean anonymousAllowed          // allow unauthenticated connections?
) {}
```

### 13.4 Quota Configuration

```java
public record QuotaTenantConfig(
    int maxConnections,            // max simultaneous connections
    long produceRateBytesPerSec,   // produce throughput limit
    long consumeRateBytesPerSec,   // consume throughput limit
    long storageQuotaBytes,        // total storage limit
    int maxTopics                  // max topics/queues/exchanges
) {}
```

---

## 14. Authentication & Authorization

### 14.1 Protocol-Scoped ACLs

ACLs are scoped to `(tenant, protocol, resource, principal, operation)`. This means Kafka
ACLs do not affect AMQP clients, and vice versa. A wildcard `protocol=*` applies to all protocols.

```java
// ivy-auth ACL entry
public record AclEntry(
    TenantId tenantId,
    ProtocolId protocol,       // KAFKA, AMQP_091, AMQP_10, MQTT, HTTP, *
    String resourceType,       // TOPIC, GROUP, EXCHANGE, QUEUE, CLUSTER
    String resourceName,       // specific name or "*" for wildcard
    String principal,          // "User:admin" or "*"
    AclOperation operation,    // READ, WRITE, CREATE, DELETE, ALTER, DESCRIBE
    AclPermission permission   // ALLOW, DENY
) {}
```

**Storage:**

```sql
CREATE TABLE acl_entries (
    tenant_id      UUID    NOT NULL,
    protocol       TEXT    NOT NULL DEFAULT '*',
    resource_type  TEXT    NOT NULL,
    resource_name  TEXT    NOT NULL,
    principal      TEXT    NOT NULL,
    operation      TEXT    NOT NULL,
    permission     TEXT    NOT NULL DEFAULT 'ALLOW',
    PRIMARY KEY (tenant_id, protocol, resource_type, resource_name, principal, operation)
);
```

**Default policy:** Allow-when-empty. If no ACL entries exist for a tenant, all operations
are permitted. This matches standard message broker behavior and simplifies initial setup.

### 14.2 Credential Storage

Credentials are stored per-tenant in the `__credentials` internal topic and `PgCredentialStore`.

```sql
CREATE TABLE credentials (
    tenant_id    UUID    NOT NULL,
    username     TEXT    NOT NULL,
    mechanism    TEXT    NOT NULL,  -- 'SCRAM-SHA-512', 'PLAIN', etc.
    credential   BYTEA   NOT NULL, -- Argon2id hash (passwords) or SCRAM salted hash
    created_at   TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (tenant_id, username, mechanism)
);
```

**Sealed credential hierarchy:**

```java
public sealed interface Credential
    permits Password, Token, Certificate, SaslCredential {
}

public record Password(char[] value) implements Credential {
    // char[] zeroed after use — Rule §10.4
    // toString() always returns "****" — never exposes password
}
```

### 14.3 Delegation Tokens

Per-tenant delegation tokens for Kafka protocol compatibility. Tokens are HMAC-SHA-256
signed and include the tenant ID in the token payload.

---

## 15. Quota & Rate Limiting

Per-tenant quota enforcement using token bucket rate limiting.

```
Request arrives → Layer 6: Quota Enforcement
  │
  ├─ Connection count check:
  │   if (activeConnections[tenantId] >= maxConnections)
  │     → reject with protocol-appropriate error
  │
  ├─ Produce rate check:
  │   if (produceTokenBucket[tenantId].tryConsume(recordBytes) == false)
  │     → throttle:
  │       Kafka: THROTTLING_QUOTA_EXCEEDED (throttleTimeMs in response)
  │       HTTP:  429 Too Many Requests + Retry-After header
  │       AMQP:  Channel.Flow(active=false)
  │       MQTT:  PUBACK with reason code 0x97 (Quota exceeded)
  │
  ├─ Consume rate check:
  │   if (consumeTokenBucket[tenantId].tryConsume(fetchBytes) == false)
  │     → throttle with protocol-appropriate signal
  │
  └─ Storage quota check:
      if (tenantStorageUsage[tenantId] >= storageQuotaBytes)
        → reject produce with TOPIC_AUTHORIZATION_FAILED (storage full)
```

**Metrics:** All quota enforcement produces metrics labeled by `(tenantId, protocol)`:
- `security.connections.active` — gauge by tenant and protocol
- `quota.produce.throttle.rate` — throttle events per second
- `quota.consume.throttle.rate` — throttle events per second
- `quota.storage.usage.bytes` — current storage usage per tenant

---

## 16. Error Handling

### 16.1 Tenant-specific error responses per protocol

| Error Condition | Kafka | HTTP | AMQP 0-9-1 | AMQP 1.0 | MQTT |
|---|---|---|---|---|---|
| Unknown tenant (SNI) | Connection closed | Connection closed | Connection closed | Connection closed | Connection closed |
| Tenant suspended | `CLUSTER_AUTHORIZATION_FAILED` | 503 + `Retry-After` | Connection.Close(403) | Close(amqp:unauthorized-access) | DISCONNECT(0x98) |
| Auth failure | `SASL_AUTHENTICATION_FAILED` | 401 Unauthorized | Connection.Close(530) | SaslOutcome(auth) | CONNACK(0x86) |
| ACL denied | `TOPIC_AUTHORIZATION_FAILED` | 403 Forbidden | Channel.Close(403) | Detach(amqp:unauthorized-access) | PUBACK(0x87) |
| Quota exceeded | `THROTTLING_QUOTA_EXCEEDED` | 429 + `Retry-After` | Channel.Flow(false) | Flow(linkCredit=0) | PUBACK(0x97) |
| Storage full | `TOPIC_AUTHORIZATION_FAILED` | 507 Insufficient Storage | Channel.Close(403) | Detach(amqp:resource-limit-exceeded) | PUBACK(0x97) |

### 16.2 Error context

All error responses include tenant context for debugging:

```java
// Structured error logging
logger.warn("Tenant operation denied: tenantId={}, protocol={}, principal={}, " +
    "resource={}, operation={}, reason={}",
    tenantId, protocol, principal, resource, operation, reason);
```

---

## 17. Configuration

New properties added for multi-tenancy:

| Property | Default | Description |
|---|---|---|
| `multi.tenant.enabled` | `false` | Master switch for multi-tenancy. When false, broker operates in single-tenant mode. |
| `tenant.auto.provision.enabled` | `true` | Auto-create tenants on first SNI connection |
| `tenant.auto.provision.default.config` | `{}` | Default TenantConfig JSON for auto-provisioned tenants |
| `tenant.sni.base.domain` | `""` | Base domain for SNI resolution (e.g., `broker.example.com`) |
| `tenant.delete.retention.days` | `7` | Days to retain data after tenant deletion before purge |
| `tenant.suspend.drain.timeout.ms` | `30000` | Max time to drain connections during suspension |
| `tenant.max.tenants` | `10000` | Max number of tenants per broker (prevents unbounded growth) |
| `tenant.tls.context.cache.size` | `1000` | Max cached per-tenant SslContexts |
| `tenant.namespace.strategy.default` | `PREFIX` | Default namespace strategy for new tenants |
| `tenant.acl.default.policy` | `ALLOW_WHEN_EMPTY` | Default ACL policy when no entries exist |
| `tenant.quota.default.max.connections` | `1000` | Default max connections per tenant |
| `tenant.quota.default.produce.rate.bytes` | `104857600` (100 MB/s) | Default produce rate limit |
| `tenant.quota.default.consume.rate.bytes` | `209715200` (200 MB/s) | Default consume rate limit |
| `tenant.quota.default.storage.bytes` | `107374182400` (100 GB) | Default storage quota per tenant |

**Single-tenant mode:** When `multi.tenant.enabled=false`, no SNI resolution occurs,
no tenant context is propagated, and all connections share a single implicit tenant.
Existing Kafka deployments are completely unaffected.

---

## 18. Implementation Plan

### Phase 1 — Core tenant infrastructure

1. Define `TenantId` value record in `ivy-common`
2. Define `TenantContext` record with ScopedValue propagation
3. Implement `TenantContextPropagator` (wrap/unwrap at event loop ↔ virtual thread boundary)
4. Implement `TenantStatus` enum with state machine transitions
5. Implement `TenantRecord` with factory methods and immutable builders
6. Implement `TenantConfig`, `TlsTenantConfig`, `AuthTenantConfig`, `QuotaTenantConfig` records
7. Implement `SniTenantResolver` — SNI hostname → deterministic UUID
8. Implement `TenantRegistry` — in-memory cache with dual indexes (id, hostname)
9. Implement `PgTenantStore` — PostgreSQL persistence for tenant records
10. Implement `TenantStore` — in-memory cache backed by `__tenants` internal topic
11. Add multi-tenant configuration properties to `KafkaConfig`
12. Unit tests for all core types

### Phase 2 — Network pipeline

13. Implement `TenantResolverHandler` — Netty handler for SNI → TenantId resolution
14. Implement `TenantContextHandler` — validates tenant ACTIVE, sets channel attribute
15. Implement `CodecContextTenantUpdater` — propagates tenant to codec context
16. Implement per-tenant `SslContext` selection in `SniHandler`
17. Implement `TlsContextRegistry` — per-tenant TLS context cache
18. Implement `TenantChannelRegistry` — tracks active channels per tenant (for drain)
19. Wire tenant handlers into Netty pipeline (after TLS, before protocol detection)
20. Integration tests: tenant resolution, connection rejection for unknown/suspended tenants

### Phase 3 — Storage isolation

21. Add `tenantId` field to `TrailerMetadata` (59-byte per-record overhead)
22. Add `tenant_id` columns to all PG metadata stores
23. Create `__tenants` internal topic with log compaction
24. Create `__acl_entries` internal topic with log compaction
25. Create `__credentials` internal topic with log compaction
26. Implement startup replay for all internal topics → in-memory caches
27. Implement tenant-scoped `PartitionId` derivation: `hash(tenantId + logicalName)`
28. Add defense-in-depth tenant filtering in segment reader
29. Unit tests for storage isolation

### Phase 4 — Write & read path isolation

30. Add `suspendedTenantIds` set to `WriteAccumulator`
31. Add tenant suspension check in `WriteAccumulator.submit()`
32. Implement `TenantSuspendedException` with protocol-specific error mapping
33. Add tenant filtering in fetch/read path
34. Implement `SubscriptionManager` tenant-scoped persistence
35. Implement consumer group/offset tenant isolation
36. Integration tests: cross-tenant write/read isolation

### Phase 5 — Namespace strategies

37. Define `NamespaceStrategy` interface
38. Implement `PrefixNamespaceStrategy` (Kafka, NATS, Kinesis, Pub/Sub)
39. Implement `AmqpNativeStrategy` (AMQP 0-9-1, AMQP 1.0 — vhost override)
40. Implement `SchemaNamespaceStrategy` (PgWire, MySQL)
41. Implement `SessionNamespaceStrategy` (MQTT)
42. Wire namespace strategies into protocol handlers
43. Integration tests: namespace isolation per protocol

### Phase 6 — Tenant lifecycle management

44. Implement `TenantLifecycleManager` — create/suspend/activate/delete
45. Implement 8-step suspension flow with drain deadline
46. Implement 4-step activation flow
47. Implement 10-step deletion flow with async data purge
48. Implement auto-provisioning on first SNI connection
49. Add tenant management API (internal, admin-only)
50. Integration tests: full lifecycle transitions

### Phase 7 — Authentication & authorization

51. Implement protocol-scoped `AclEntry` with `ProtocolId` field
52. Implement `PgAclStore` with tenant-scoped composite keys
53. Implement per-tenant credential storage (`PgCredentialStore`)
54. Implement SASL integration for Kafka, AMQP (PLAIN, SCRAM-SHA-512)
55. Implement Bearer/Basic auth for HTTP
56. Implement MQTT CONNECT auth integration
57. Default allow-when-empty ACL policy
58. Integration tests: cross-protocol ACL isolation

### Phase 8 — Quota & rate limiting

59. Implement per-tenant token bucket rate limiter
60. Implement connection count limits per tenant
61. Implement produce/consume rate limiting per tenant
62. Implement storage quota enforcement per tenant
63. Implement protocol-specific throttle responses (§16.1 error table)
64. Add quota metrics by (tenantId, protocol)
65. Integration tests: quota enforcement and throttle behavior

### Phase 9 — E2E multi-tenant tests

66. Implement `MultiTenantClusterTestBase` — 3-broker cluster with TLS + SNI
67. `MultiTenantKafkaIsolationIT` — topic/consumer group isolation
68. `MultiTenantHttpIsolationIT` — HTTP produce/consume isolation
69. `MultiTenantAmqpExchangeIT` — AMQP 0-9-1 exchange/queue/binding isolation
70. `MultiTenantAmqp10IsolationIT` — AMQP 1.0 address/session isolation
71. `MultiTenantMqtt5IsolationIT` — MQTT topic/client-id isolation
72. Cross-protocol multi-tenant tests — produce via Kafka, consume via AMQP (same tenant)
73. Negative tests — verify cross-tenant operations are rejected
74. Benchmark: per-tenant throughput/latency under multi-tenant load

---

## 19. Implementation Concerns

### 19.1 CRITICAL — ScopedValue vs ThreadLocal for tenant context

**Problem.** `ThreadLocal` causes memory leaks with virtual threads (Rule §6.5). Virtual
threads reuse carrier threads, but `ThreadLocal` values accumulate on the virtual thread
until it terminates — which may be never for long-lived connections.

**Solution:** Use `ScopedValue` (JEP 506, final JDK 25) exclusively for tenant context
propagation. ScopedValue is lexically scoped — the value is automatically freed when
`runScoped()` returns. It inherits into child virtual threads without copying.

**Enforcement:** Static analysis / code review: any new `ThreadLocal<TenantContext>` usage
must be rejected. Only `ScopedValue<TenantContext>` is permitted for tenant scoping.

---

### 19.2 CRITICAL — Defense-in-depth: tenant_id in every query

**Problem.** A bug in namespace resolution could cause a tenant's request to read another
tenant's data if the only isolation is at the namespace layer.

**Solution:** Every SQL query, every segment read, and every subscription lookup includes
`tenant_id` in the WHERE clause — even when other columns (like `partition_id`) would
uniquely identify the data. This means a namespace bug results in "not found" rather than
"cross-tenant data leak".

**Enforcement:** Code review rule: any new PG query touching tenant-scoped data MUST
include `WHERE tenant_id = ?` as the first predicate.

---

### 19.3 CRITICAL — AMQP vhost override must be enforced server-side

**Problem.** A malicious AMQP 0-9-1 client could send `Connection.Open(vhost="/admin")`
to access another tenant's vhost.

**Solution:** The broker **ignores** the client-requested vhost in `Connection.Open` and
forces the connection into `"/" + tenantUUID`. The `AmqpNativeStrategy.connectionNamespaceCommand()`
method computes the forced vhost, and the `Amqp091ConnectionHandler.handleOpen()` applies it
before sending `Connection.OpenOk`.

**This is non-negotiable.** The client's vhost request is treated as untrusted input.

---

### 19.4 HIGH — Deterministic TenantId derivation must be consistent across brokers

**Problem.** `UUID.nameUUIDFromBytes()` uses MD5 internally (UUID v3). If different
brokers use different implementations or encodings, the same hostname could produce
different UUIDs, breaking tenant resolution.

**Solution:** Always use `hostname.getBytes(StandardCharsets.UTF_8)` — never platform
default charset. The `UUID.nameUUIDFromBytes()` implementation is specified by RFC 4122
and is consistent across all JVMs.

**Test:** Unit test that verifies specific hostname → UUID mappings across multiple runs.

---

### 19.5 HIGH — Per-tenant SslContext cache must be bounded

**Problem.** If the broker serves 10,000 tenants, each with a unique SslContext, the
JCA crypto state per context is ~50 KB → 500 MB of cache.

**Solution:** `TlsContextRegistry` uses an LRU cache bounded by
`tenant.tls.context.cache.size` (default 1000). Evicted contexts are recreated on next
connection. The default SslContext (used when no per-tenant config exists) is never evicted.

---

### 19.6 HIGH — Tenant suspension must drain WriteAccumulator before closing connections

**Problem.** If connections are closed before the WriteAccumulator is drained, in-flight
writes may be lost — the client has already sent the data but the broker hasn't persisted it.

**Solution:** The suspension flow (§6.3) has a strict ordering:
1. First: `WriteAccumulator.suspendTenant()` — reject NEW writes
2. Second: drain existing connections (30s deadline)
3. Third: flush WriteAccumulator batches — persist any remaining in-flight data
4. Fourth: flush segments to storage

Steps 1–3 ensure all data that was accepted before suspension is persisted.

---

### 19.7 MEDIUM — Auto-provisioning race condition

**Problem.** Two simultaneous connections from the same unknown hostname could both
attempt to create the tenant, causing a duplicate insert.

**Solution:** `PgTenantStore.upsertTenant()` uses `INSERT ... ON CONFLICT (tenant_id) DO UPDATE`.
The `TenantRegistry.register()` method uses `ConcurrentHashMap.putIfAbsent()`. Both operations
are idempotent — the second connection gets the existing tenant record.

---

### 19.8 MEDIUM — Namespace strategy must be immutable after tenant creation

**Problem.** Changing a tenant's namespace strategy (e.g., from PREFIX to NATIVE) after
data has been written would make existing data inaccessible under the new naming scheme.

**Solution:** `namespaceStrategy` in `TenantConfig` is set at tenant creation and cannot
be changed. Attempts to update it are rejected by `TenantLifecycleManager.updateTenant()`.

---

### 19.9 MEDIUM — Cross-protocol produce/consume within the same tenant

**Problem.** A message produced via Kafka (topic "orders") must be consumable via AMQP
(queue "orders") for the same tenant. Different namespace strategies (PREFIX for Kafka,
NATIVE for AMQP) could produce different internal names.

**Solution:** All namespace strategies resolve to the same `PartitionId` for a given
`(tenantId, logicalName)` pair. The `PartitionId = hash(tenantId + logicalName)` formula
is shared across all strategies. The namespace strategy only affects how the logical name
is derived from the protocol-native resource name.

```
Kafka:  topic "orders"  → logicalName = "orders" → PartitionId = hash(tenant + "orders")
AMQP:   queue "orders"  → logicalName = "orders" → PartitionId = hash(tenant + "orders")
MQTT:   topic "orders"  → logicalName = "orders" → PartitionId = hash(tenant + "orders")

All resolve to the SAME PartitionId → same data.
```

---

### 19.10 LOW — Tenant metrics cardinality

**Problem.** Per-tenant metrics with `tenantId` as a label can produce high cardinality
(10,000 tenants × N metrics = 100K+ time series), overwhelming monitoring systems.

**Solution:** Use tenant name (short subdomain) as the label, not the full UUID. Cap
the number of distinct tenant metric labels at `tenant.metrics.max.cardinality` (default
100). Tenants beyond the cap share a `_overflow` label. Critical tenants can be pinned
via `tenant.metrics.pinned.tenants` config.

---

## 20. Comparison with Alternative Approaches

### 20.1 Separate clusters per tenant

| Concern | Separate Clusters | Shared Cluster (this design) |
|---|---|---|
| Isolation guarantee | Physical (strongest) | Logical (enforced at every layer) |
| Operational cost | O(N) clusters to manage | O(1) cluster, N tenants in config |
| Resource efficiency | Low — each cluster has fixed overhead | High — tenants share brokers, storage |
| Onboarding latency | Minutes (provision infra) | Milliseconds (auto-provision on first connection) |
| Cross-tenant risk | Zero | Defense-in-depth (§19.2) makes it negligible |
| Per-tenant customization | Full (separate configs) | Limited to TenantConfig fields |

**Recommendation:** Use shared multi-tenant clusters for SaaS platforms with many small
tenants. Use separate clusters for enterprise customers with strict regulatory requirements
or very high throughput needs.

### 20.2 Topic-prefix convention (application-level)

Many Kafka deployments use a naming convention (e.g., `tenant1.orders`, `tenant2.orders`)
to separate tenant data. This is an application-level convention — the broker has no
awareness of tenants.

| Concern | Topic prefix convention | Native multi-tenancy (this design) |
|---|---|---|
| Enforcement | None — any client can access any topic | Broker-enforced — SNI → tenant → scoped access |
| Namespace collision | Possible (typo → wrong tenant) | Impossible (namespace derived from SNI) |
| ACL management | Per-topic ACLs, O(topics × tenants) | Per-tenant ACLs, O(tenants) |
| Quota enforcement | Not per-tenant (only per-client) | Per-tenant (connections, throughput, storage) |
| Lifecycle management | Manual (delete all topics starting with prefix) | Single API call (delete tenant → scheduled purge) |
| Cross-protocol support | Kafka only | All protocols (AMQP, MQTT, HTTP, etc.) |

### 20.3 Confluent Platform multi-tenancy (topic-level ACLs + prefix convention)

Confluent recommends topic-level ACLs with prefix conventions for multi-tenancy. This
design extends that model with:

- **Broker-enforced isolation** (not just convention)
- **Per-tenant TLS contexts** (separate certificates per tenant)
- **Per-tenant quotas** (not just per-client quotas)
- **Cross-protocol namespace isolation** (AMQP vhost, MQTT client-id, etc.)
- **Tenant lifecycle management** (suspend, delete with data purge)
- **Auto-provisioning** (zero-touch tenant onboarding)

---

*Document version: 0.1 — 2026-04-16*
*Branch: feature/http-protocol*
*Reference: ivy-ref multi-tenant implementation (MASTERPLAN v9, SECURITY-AND-MULTI-TENANT-ARCHITECTURE v7)*
