# Schema Registry for Apache Kafka — Design Document

## Table of Contents

1. [Overview](#1-overview)
2. [Goals & Non-Goals](#2-goals--non-goals)
3. [Architecture Overview](#3-architecture-overview)
4. [HTTP API Specification](#4-http-api-specification)
   - 4.1 Schema Operations
   - 4.2 Subject Operations
   - 4.3 Compatibility Operations
   - 4.4 Configuration Operations
   - 4.5 Mode Operations
5. [Schema Type System](#5-schema-type-system)
6. [Schema Parsing & Validation](#6-schema-parsing--validation)
7. [Compatibility Engine](#7-compatibility-engine)
8. [Storage Layer](#8-storage-layer)
9. [Integration with Existing Kafka Infrastructure](#9-integration-with-existing-kafka-infrastructure)
10. [New Module: `schema-registry`](#10-new-module-schema-registry)
11. [Configuration](#11-configuration)
12. [Error Handling](#12-error-handling)
13. [Security](#13-security)
14. [Implementation Plan](#14-implementation-plan)
15. [Implementation Concerns](#15-implementation-concerns)
16. [Comparison with Confluent Schema Registry](#16-comparison-with-confluent-schema-registry)

---

## 1. Overview

This document describes the design for adding a native, embedded schema registry to
Apache Kafka brokers. The feature allows clients to **register**, **retrieve**, and
**validate** schemas (Avro, Protobuf, JSON Schema) over the HTTP listener without a
separate Confluent Schema Registry process.

### Key Properties

| Property | Behavior |
|---|---|
| **Register** | Client sends HTTP POST with schema definition → broker validates, checks compatibility, allocates global ID → returns schema ID |
| **Retrieve** | Client sends HTTP GET by schema ID or subject+version → broker returns schema definition |
| **Compatibility** | Before registering, broker validates new schema against previous versions using configurable compatibility level |
| **Protocol** | HTTP/1.1 with JSON body; reuses the existing `http-server` module's Netty infrastructure |
| **Integration** | Plugs into the existing `HttpAcceptor → HttpRouter → HttpRequestHandler` pipeline (§9 of HTTP protocol design) |
| **Storage** | Internal Kafka topic `__schemas` (log-compacted) with in-memory materialized view for O(1) reads |
| **Wire Format** | Confluent-compatible: magic byte `0x00` + 4-byte schema ID prefix on record values |

### Motivation

- Eliminate the need for a separate Confluent Schema Registry deployment — zero additional infrastructure
- Confluent-compatible API surface: existing `kafka-avro-serializer`, `kafka-protobuf-serializer`, and `kafka-json-schema-serializer` clients work by changing only `schema.registry.url`
- Embedded in the broker: leverages existing auth (mTLS, SASL via HTTP listener §12.1), metrics, and cluster metadata
- Schema validation at produce time: broker can reject records with unknown schema IDs before appending to the log

---

## 2. Goals & Non-Goals

### Goals

- Full Confluent Schema Registry v1 API compatibility (subjects, versions, schemas, config, compatibility, mode)
- Support Avro, Protobuf, and JSON Schema formats with pluggable parser architecture
- Seven compatibility levels: NONE, BACKWARD, BACKWARD_TRANSITIVE, FORWARD, FORWARD_TRANSITIVE, FULL, FULL_TRANSITIVE
- Idempotent schema registration: identical schema under same subject returns same ID without version bump
- Soft-delete and hard-delete semantics for subjects and individual versions
- Schema mode enforcement: READWRITE (normal), READONLY (reject registrations), IMPORT (bypass compatibility for migration)
- Global and per-subject configuration (compatibility level, normalize flag, mode)
- Wire-format validation on the produce path: reject records with unknown schema IDs at the broker
- Cluster-aware: schema writes are durable via internal Kafka topic; reads are served from in-memory cache on any broker

### Non-Goals

- Schema references / cross-schema `$ref` resolution (phase 2 — stored but not resolved)
- Schema rules / migration rules (Confluent enterprise feature — out of scope)
- DEK registry / field-level encryption (out of scope)
- Multi-tenant isolation (out of scope — single-tenant Kafka deployment; multi-tenant can be layered later)
- Schema normalization / canonicalization beyond what the parsers provide (phase 2)
- Confluent Cloud-specific API extensions (`/exporters`, `/clusters`, `/contexts`)
- Admin UI for schema browsing (out of scope — use existing tools like Schema Registry UI)

---

## 3. Architecture Overview

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                           Kafka Broker                                       │
│                                                                              │
│  Port 9094 (HTTP)                                                            │
│  ┌───────────────────────────────────────────────────────────────────────┐   │
│  │     HttpAcceptor (existing, from http-server module)                   │   │
│  │                                                                       │   │
│  │  ┌─────────────────────────────────────────────────────────────────┐  │   │
│  │  │                    HttpRouter                                    │  │   │
│  │  │                                                                 │  │   │
│  │  │   /v1/topics/*        → existing produce/consume handlers       │  │   │
│  │  │   /schemas/*          → SchemaRegistryRequestHandler ◄──────┐  │  │   │
│  │  │   /subjects/*         → SchemaRegistryRequestHandler        │  │  │   │
│  │  │   /config/*           → SchemaRegistryRequestHandler        │  │  │   │
│  │  │   /compatibility/*    → SchemaRegistryRequestHandler        │  │  │   │
│  │  │   /mode/*             → SchemaRegistryRequestHandler        │  │  │   │
│  │  └──────────────────────────────────────────────────┬──────────┘  │  │   │
│  └──────────────────────────────────────────────────────┼────────────┘  │   │
│                                                          │                   │
│  ┌──────────────────────────────────────────────────────▼────────────────┐  │
│  │                SchemaRegistryRequestHandler                           │  │
│  │                                                                       │  │
│  │  • Parses HTTP request (method, path, body)                           │  │
│  │  • Validates content-type: application/vnd.schemaregistry.v1+json     │  │
│  │  • Delegates to SchemaRegistry interface                              │  │
│  │  • Serializes response as JSON                                        │  │
│  │  • Returns FullHttpResponse directly (no RequestChannel round-trip)   │  │
│  └──────────────────────────┬────────────────────────────────────────────┘  │
│                              │                                               │
│  ┌──────────────────────────▼────────────────────────────────────────────┐  │
│  │                    SchemaRegistry (interface)                          │  │
│  │                                                                       │  │
│  │  Read operations ──► SchemaStore (in-memory, O(1))                    │  │
│  │    getById(schemaId)                                                  │  │
│  │    getVersion(subject, version)                                       │  │
│  │    getLatestVersion(subject)                                          │  │
│  │    listSubjects()                                                     │  │
│  │    listVersions(subject)                                              │  │
│  │    lookupSchema(subject, schema)                                      │  │
│  │                                                                       │  │
│  │  Write operations ──► Validate + append to __schemas topic            │  │
│  │    registerSchema(subject, parsedSchema)                              │  │
│  │    deleteSubject(subject, permanent)                                  │  │
│  │    deleteVersion(subject, version, permanent)                         │  │
│  │    putConfig(subject, compatibilityConfig)                            │  │
│  │    putMode(subject, mode)                                             │  │
│  │                                                                       │  │
│  │  Validation ──► CompatibilityEngine                                   │  │
│  │    testCompatibility(subject, newSchema)                              │  │
│  └──────────────────────────┬────────────────────────────────────────────┘  │
│                              │                                               │
│  ┌──────────────────────────▼────────────────────────────────────────────┐  │
│  │                         Storage Layer                                  │  │
│  │                                                                       │  │
│  │  ┌─────────────────┐  ┌───────────────────┐  ┌────────────────────┐  │  │
│  │  │  SchemaStore     │  │ SchemaConfigStore │  │  SchemaModeStore   │  │  │
│  │  │  (versions + IDs)│  │ (compat levels)   │  │  (RW/RO/IMPORT)   │  │  │
│  │  │                  │  │                   │  │                    │  │  │
│  │  │  versionCache:   │  │  configCache:     │  │  modeCache:       │  │  │
│  │  │  subject:ver → SV│  │  subject → Config │  │  subject → Mode   │  │  │
│  │  │                  │  │                   │  │                    │  │  │
│  │  │  idCache:        │  │  __global → def.  │  │  __global → def.  │  │  │
│  │  │  schemaId → SV   │  │                   │  │                    │  │  │
│  │  └────────┬─────────┘  └─────────┬─────────┘  └─────────┬────────┘  │  │
│  │           │                      │                       │           │  │
│  │           └──────────────────────┴───────────────────────┘           │  │
│  │                              │                                       │  │
│  │                   ┌──────────▼───────────┐                           │  │
│  │                   │   __schemas topic    │  log-compacted             │  │
│  │                   │   (internal)         │  replication.factor=3      │  │
│  │                   │                      │  min.insync.replicas=2     │  │
│  │                   └──────────────────────┘                           │  │
│  └──────────────────────────────────────────────────────────────────────┘  │
│                                                                              │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                    Schema Parsing & Compatibility                     │   │
│  │                                                                       │   │
│  │  SchemaParserRegistry ──► AvroSchemaParser                            │   │
│  │                       ──► JsonSchemaParser                            │   │
│  │                       ──► ProtobufSchemaParser                        │   │
│  │                                                                       │   │
│  │  CompatibilityEngine  ──► AvroCompatibilityChecker (Apache Avro lib)  │   │
│  │                       ──► JsonCompatibilityChecker (required fields)   │   │
│  │                       ──► ProtobufCompatibilityChecker (field maps)    │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────────────────┘
```

### Request flow — schema registration

```
HTTP Client                Broker (any)                              __schemas topic
     │                         │                                          │
     │  POST /subjects/orders-value/versions                              │
     │  { "schema": "{...}", "schemaType": "AVRO" }                      │
     │────────────────────────►│                                          │
     │                         │                                          │
     │                   ┌─────▼──────────────────────────────┐          │
     │                   │  SchemaRegistryRequestHandler       │          │
     │                   │                                     │          │
     │                   │  1. Parse JSON body                 │          │
     │                   │  2. SchemaParserRegistry.parse()    │          │
     │                   │     → validates schema syntax        │          │
     │                   │  3. SchemaRegistry.registerSchema()  │          │
     │                   │     a. Check idempotency (exact match│          │
     │                   │        → return existing ID)         │          │
     │                   │     b. Check mode != READONLY        │          │
     │                   │     c. Get previous versions         │          │
     │                   │     d. CompatibilityEngine.check()   │          │
     │                   │     e. Allocate SchemaId (monotonic) │          │
     │                   │     f. Allocate version (per-subject)│          │
     │                   │     g. Append to __schemas topic     │──────────►
     │                   │     h. Update in-memory caches       │          │
     │                   │  4. Return { "id": 42 }              │          │
     │                   └─────────────────────────────────────┘          │
     │                         │                                          │
     │  HTTP 200               │                                          │
     │  { "id": 42 }           │                                          │
     │◄────────────────────────│                                          │
```

### Thread Model

Schema registry requests are **handled directly on the Netty worker thread** — they
do not enter `RequestChannel` or the `KafkaRequestHandler` thread pool. This is a
deliberate design choice:

```
                      ┌─────────────────────────────────────────────┐
  Port 9094           │  Netty Boss Thread (1)                      │
  HTTP requests ─────►│  accepts TCP connections                    │
                      └───────────────────┬─────────────────────────┘
                                          │ distributes connections
                      ┌───────────────────▼─────────────────────────┐
                      │  Netty Worker Threads (num.http.network.threads)│
                      │                                              │
                      │  Route: /v1/topics/*                         │
                      │    → RequestChannel → KafkaRequestHandler    │
                      │    → KafkaApis (existing produce/consume)    │
                      │                                              │
                      │  Route: /schemas/*, /subjects/*, /config/*,  │
                      │         /compatibility/*, /mode/*             │
                      │    → SchemaRegistryRequestHandler (in-line)  │
                      │    → SchemaRegistry.xxx() (in-memory read    │
                      │       or topic append for writes)            │
                      │    → JSON response written directly          │
                      └──────────────────────────────────────────────┘
```

**Why schema registry bypasses RequestChannel.** Schema registry read operations are
pure in-memory lookups (O(1) ConcurrentHashMap access) — adding a RequestChannel queue
hop and handler thread context switch adds 50–100 µs of latency for zero benefit. Write
operations produce to the `__schemas` internal topic using the broker's own `ReplicaManager`
(same thread can call `appendRecords()` since Netty worker threads are not blocked by
the KafkaRequestHandler pool). The `appendRecords()` callback completes the HTTP response
asynchronously via `CompletableFuture`.

---

## 4. HTTP API Specification

Content-Type: `application/vnd.schemaregistry.v1+json` (accepted and returned).
Also accepts `application/json` for convenience. All error responses use the same
content type.

### 4.1 Schema Operations

---

#### 4.1.1 List Supported Types — `GET /schemas/types`

```
GET /schemas/types
```

```json
["AVRO", "PROTOBUF", "JSON"]
```

Hardcoded list of supported schema formats. No authorization required.

---

#### 4.1.2 Get Schema by Global ID — `GET /schemas/ids/{id}`

```
GET /schemas/ids/42
```

```json
{
  "schema": "{\"type\":\"record\",\"name\":\"Order\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"}]}",
  "schemaType": "AVRO",
  "references": []
}
```

**Implementation:** `SchemaStore.getById(schemaId)` — O(1) `idCache` lookup. Returns
the schema in its canonical form.

Used by consumers to deserialize records. The Confluent `kafka-avro-deserializer` calls
this endpoint on every new schema ID it encounters, then caches the result client-side.

**Error:** 404 with error code `40403` if schema ID does not exist.

---

#### 4.1.3 Get Subjects for Schema ID — `GET /schemas/ids/{id}/subjects`

```
GET /schemas/ids/42/subjects
```

```json
["orders-value"]
```

Returns all subject names that contain the given schema ID.

---

#### 4.1.4 Get Versions for Schema ID — `GET /schemas/ids/{id}/versions`

```
GET /schemas/ids/42/versions
```

```json
[
  { "subject": "orders-value", "version": 3 }
]
```

Returns all subject+version pairs that reference the given schema ID.

---

### 4.2 Subject Operations

---

#### 4.2.1 List Subjects — `GET /subjects`

```
GET /subjects
```

```json
["orders-value", "orders-key", "payments-value"]
```

Returns all registered subject names. Soft-deleted subjects are excluded unless
`?deleted=true` is passed.

---

#### 4.2.2 List Versions Under Subject — `GET /subjects/{subject}/versions`

```
GET /subjects/orders-value/versions
```

```json
[1, 2, 3]
```

Returns version numbers for the subject. Soft-deleted versions excluded unless
`?deleted=true`.

**Error:** 404 with error code `40401` if subject has no registered versions.

---

#### 4.2.3 Get Schema by Subject + Version — `GET /subjects/{subject}/versions/{version}`

```
GET /subjects/orders-value/versions/3
GET /subjects/orders-value/versions/latest
```

`{version}` may be an integer or the literal `latest`.

```json
{
  "subject": "orders-value",
  "id": 42,
  "version": 3,
  "schemaType": "AVRO",
  "schema": "{\"type\":\"record\",\"name\":\"Order\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"}]}",
  "references": []
}
```

**Implementation:** `SchemaStore.getVersion(subject, version)` — O(1) `versionCache`
lookup.

**Errors:**
- 404 / `40401` — subject not found
- 404 / `40402` — version not found

---

#### 4.2.4 Register Schema — `POST /subjects/{subject}/versions`

```
POST /subjects/orders-value/versions
```

```json
{
  "schema": "{\"type\":\"record\",\"name\":\"Order\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"amount\",\"type\":\"double\"}]}",
  "schemaType": "AVRO",
  "references": []
}
```

| Field | Type | Default | Description |
|---|---|---|---|
| `schema` | string | required | Schema definition (JSON string for Avro/JSON Schema; `.proto` text or base64 for Protobuf) |
| `schemaType` | string | `"AVRO"` | One of `AVRO`, `PROTOBUF`, `JSON` |
| `references` | array | `[]` | Schema references (stored but not resolved in phase 1) |

#### Response (200 OK)

```json
{
  "id": 42
}
```

**Idempotency:** If the exact same schema string (or its canonical form) is already
registered under this subject, the existing schema ID is returned. No new version is
created.

**Registration flow:**

```
1. Parse schema string → ParsedSchema (validates syntax)
2. Check mode != READONLY (error 42201 if so)
3. Exact-match against existing versions under this subject
   → if match found, return existing SchemaId (idempotent)
4. If mode == IMPORT:
   → skip compatibility check, use explicit ID/version from request body
5. Else (mode == READWRITE):
   → get previous versions for this subject
   → CompatibilityEngine.isCompatible(newSchema, previousVersions, schemaType, level)
   → if incompatible, return 409 with error messages
6. Allocate new SchemaId (global monotonic counter)
7. Allocate new version number (per-subject: max(existing) + 1)
8. Create SchemaVersion record
9. Append to __schemas topic (key = subject:version, value = serialized SchemaVersion)
10. Update in-memory caches (versionCache, idCache)
11. Return { "id": newSchemaId }
```

**Errors:**
- 409 / `40801` — schema is incompatible with previous version(s)
- 422 / `42201` — invalid schema (parse failure) or mode is READONLY
- 422 / `42201` — schema exceeds max size (64 KB)

---

#### 4.2.5 Lookup Schema Under Subject — `POST /subjects/{subject}`

```
POST /subjects/orders-value
```

```json
{
  "schema": "{\"type\":\"record\",\"name\":\"Order\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"}]}",
  "schemaType": "AVRO"
}
```

Returns the schema version if the exact schema is already registered under this subject.
This is **not** registration — it is a content-addressed lookup.

#### Response (200 OK)

```json
{
  "subject": "orders-value",
  "id": 42,
  "version": 3,
  "schema": "{...}",
  "schemaType": "AVRO"
}
```

**Error:** 404 / `40403` — schema not found under this subject.

---

#### 4.2.6 Delete Subject — `DELETE /subjects/{subject}`

```
DELETE /subjects/orders-value
DELETE /subjects/orders-value?permanent=true
```

**Soft delete (default):** Marks all versions as deleted. Schemas remain retrievable by
global ID (consumers can still deserialize). Subject is hidden from `GET /subjects`.

**Hard delete (`?permanent=true`):** Removes all versions from both caches. Requires
prior soft delete (error `40404` otherwise). Schema IDs become unresolvable.

#### Response (200 OK)

```json
[1, 2, 3]
```

Returns the list of deleted version numbers.

---

#### 4.2.7 Delete Version — `DELETE /subjects/{subject}/versions/{version}`

```
DELETE /subjects/orders-value/versions/3
DELETE /subjects/orders-value/versions/3?permanent=true
```

Same soft/hard delete semantics as subject deletion, but for a single version.

#### Response (200 OK)

```json
3
```

Returns the deleted version number.

---

### 4.3 Compatibility Operations

---

#### 4.3.1 Test Compatibility — `POST /compatibility/subjects/{subject}/versions/{version}`

```
POST /compatibility/subjects/orders-value/versions/latest
```

```json
{
  "schema": "{\"type\":\"record\",\"name\":\"Order\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"amount\",\"type\":\"double\"}]}",
  "schemaType": "AVRO"
}
```

Tests whether the provided schema is compatible with the specified version (or `latest`)
under the subject's configured compatibility level. Does **not** register the schema.

#### Response (200 OK)

```json
{
  "is_compatible": true
}
```

#### Response (200 OK — incompatible)

```json
{
  "is_compatible": false,
  "messages": [
    "Incompatibility{type:READER_FIELD_MISSING_DEFAULT_VALUE, location:/fields/1, message:...}"
  ]
}
```

---

### 4.4 Configuration Operations

---

#### 4.4.1 Get Global Config — `GET /config`

```json
{
  "compatibilityLevel": "BACKWARD"
}
```

---

#### 4.4.2 Set Global Config — `PUT /config`

```json
{
  "compatibility": "FULL_TRANSITIVE"
}
```

#### Response (200 OK)

```json
{
  "compatibility": "FULL_TRANSITIVE"
}
```

---

#### 4.4.3 Get Subject Config — `GET /config/{subject}`

```json
{
  "compatibilityLevel": "FORWARD"
}
```

Falls back to global config if no subject-specific config is set.

---

#### 4.4.4 Set Subject Config — `PUT /config/{subject}`

```json
{
  "compatibility": "NONE"
}
```

---

### 4.5 Mode Operations

---

#### 4.5.1 Get Mode — `GET /mode[/{subject}]`

```json
{
  "mode": "READWRITE"
}
```

Falls back: subject-specific → global → `READWRITE` (default).

---

#### 4.5.2 Set Mode — `PUT /mode[/{subject}]`

```json
{
  "mode": "READONLY"
}
```

| Mode | Behavior |
|---|---|
| `READWRITE` | Normal operation (default) |
| `READONLY` | Reject all schema registrations with error `42201` |
| `IMPORT` | Bypass compatibility checks; accept explicit ID/version (for migration) |

---

## 5. Schema Type System

### 5.1 Core Types

```java
// schema-registry/src/main/java/org/apache/kafka/schemaregistry/SchemaType.java

public enum SchemaType {
    AVRO("AVRO"),
    PROTOBUF("PROTOBUF"),
    JSON("JSON");

    private final String wireName;

    public static SchemaType fromWireName(String name) {
        // Normalize: "JSON_SCHEMA" → JSON (Confluent client compat)
        if ("JSON_SCHEMA".equalsIgnoreCase(name)) return JSON;
        return valueOf(name.toUpperCase());
    }
}
```

### 5.2 ParsedSchema — Sealed Interface

```java
// schema-registry/src/main/java/org/apache/kafka/schemaregistry/ParsedSchema.java

public sealed interface ParsedSchema
    permits AvroSchema, ProtobufSchema, JsonSchema {

    SchemaType schemaType();

    /** Original schema string as provided by the client. */
    String rawSchema();

    /** Normalized/canonical form for storage and comparison. */
    String canonicalString();
}
```

### 5.3 Concrete Schema Types

**AvroSchema** — wraps Apache Avro's `org.apache.avro.Schema`:
```java
public record AvroSchema(Schema avroSchema, String rawSchema) implements ParsedSchema {
    @Override public SchemaType schemaType() { return SchemaType.AVRO; }
    @Override public String canonicalString() { return avroSchema.toString(); }
}
```

**ProtobufSchema** — stores raw `.proto` text or base64-encoded `FileDescriptorProto`:
```java
public record ProtobufSchema(String rawSchema, String canonical, int messageCount)
    implements ParsedSchema {
    @Override public SchemaType schemaType() { return SchemaType.PROTOBUF; }
    @Override public String canonicalString() { return canonical; }
}
```

**JsonSchema** — stores JSON schema definition:
```java
public record JsonSchema(String rawSchema, String canonical) implements ParsedSchema {
    @Override public SchemaType schemaType() { return SchemaType.JSON; }
    @Override public String canonicalString() { return canonical; }
}
```

### 5.4 Value Types

```java
// SchemaId — globally unique, monotonically allocated
public record SchemaId(int id) {
    public static final int FIRST_ID = 1;
}

// SubjectName — wraps subject string, provides key/value helpers
public record SubjectName(String value) {
    public boolean isKeySubject()   { return value.endsWith("-key"); }
    public boolean isValueSubject() { return value.endsWith("-value"); }
}

// SchemaVersion — the full versioned record stored in __schemas
public record SchemaVersion(
    SubjectName subject,
    int version,                    // 1, 2, 3 ... per subject
    SchemaId id,                    // global unique ID
    SchemaType schemaType,
    String schemaString,            // canonical form
    List<SchemaReference> references,
    long timestampMs,
    boolean deleted                 // soft-delete flag
) {}

// SchemaReference — pointer from one schema to another (stored, not resolved in phase 1)
public record SchemaReference(String name, SubjectName subject, int version) {}

// CompatibilityConfig — per-subject or global
public record CompatibilityConfig(CompatibilityLevel level, boolean normalize) {
    public static final CompatibilityConfig DEFAULT =
        new CompatibilityConfig(CompatibilityLevel.BACKWARD, false);
}

// CompatibilityLevel
public enum CompatibilityLevel {
    NONE,
    BACKWARD,
    BACKWARD_TRANSITIVE,
    FORWARD,
    FORWARD_TRANSITIVE,
    FULL,
    FULL_TRANSITIVE
}

// SchemaMode
public enum SchemaMode {
    READWRITE,
    READONLY,
    IMPORT
}
```

---

## 6. Schema Parsing & Validation

### 6.1 Parser Interface

```java
public interface SchemaParser {
    ParsedSchema parse(String schemaString) throws InvalidSchemaException;
    SchemaType type();
}
```

### 6.2 Parser Registry

```java
public final class SchemaParserRegistry {
    private static final AvroSchemaParser AVRO_PARSER = new AvroSchemaParser();
    private static final JsonSchemaParser JSON_PARSER = new JsonSchemaParser();
    private static final ProtobufSchemaParser PROTO_PARSER = new ProtobufSchemaParser();

    public static ParsedSchema parse(SchemaType type, String schemaString)
            throws InvalidSchemaException {
        return switch (type) {
            case AVRO -> AVRO_PARSER.parse(schemaString);
            case JSON -> JSON_PARSER.parse(schemaString);
            case PROTOBUF -> PROTO_PARSER.parse(schemaString);
        };
    }
}
```

### 6.3 Parser Implementations

All parsers enforce a **64 KB max schema size** to prevent OOM from adversarial input.

**AvroSchemaParser:**
- Uses Apache Avro's `Schema.Parser.parse()` (battle-tested, handles all Avro types)
- Catches `SchemaParseException` → wraps in `InvalidSchemaException`
- Canonical form: Avro's compact JSON representation (`schema.toString()`)

**JsonSchemaParser:**
- Uses Jackson `ObjectMapper` to parse and validate JSON syntax
- Canonical form: re-serialized JSON (consistent key ordering via Jackson)
- Does NOT validate against JSON Schema meta-schema (that would require `json-schema-validator`)
- Phase 2: add `networknt/json-schema-validator` for full JSON Schema spec validation

**ProtobufSchemaParser:**
- Supports two input formats:
  1. **Text format (`.proto`):** regex-based message counting, validates ≥1 message declaration
  2. **Binary format (base64-encoded `FileDescriptorProto`):** base64 decode + protobuf wire format parsing
- Canonical form: stored as-is (protobuf text format is already canonical enough)
- Detection heuristic: if input matches `^[A-Za-z0-9+/=]+$`, treat as base64 binary

### 6.4 Validation Error Response

```json
{
  "error_code": 42201,
  "message": "Invalid AVRO schema: Undefined name: \"BadType\" at line 5"
}
```

---

## 7. Compatibility Engine

### 7.1 Interface

```java
public interface CompatibilityEngine {
    /**
     * Check if newSchema is compatible with previousVersions.
     *
     * @return empty list if compatible; non-empty list of error messages if incompatible
     */
    List<String> isCompatible(
        String newSchema,
        List<String> previousVersions,
        SchemaType type,
        CompatibilityLevel level
    );
}
```

### 7.2 Compatibility Level Semantics

| Level | Direction | Scope | Meaning |
|---|---|---|---|
| `NONE` | — | — | No validation; any schema accepted |
| `BACKWARD` | new reads old | Latest only | New consumers can read data written by previous schema |
| `BACKWARD_TRANSITIVE` | new reads old | All versions | New consumers can read data written by ANY previous schema |
| `FORWARD` | old reads new | Latest only | Previous consumers can read data written by new schema |
| `FORWARD_TRANSITIVE` | old reads new | All versions | ALL previous consumers can read data written by new schema |
| `FULL` | both directions | Latest only | Both BACKWARD and FORWARD against latest |
| `FULL_TRANSITIVE` | both directions | All versions | Both BACKWARD and FORWARD against ALL versions |

### 7.3 Dispatch Logic

```java
public class DefaultCompatibilityEngine implements CompatibilityEngine {

    @Override
    public List<String> isCompatible(String newSchema, List<String> previousVersions,
                                      SchemaType type, CompatibilityLevel level) {
        if (level == CompatibilityLevel.NONE || previousVersions.isEmpty()) {
            return List.of();  // short-circuit
        }

        return switch (type) {
            case AVRO -> avroChecker.check(newSchema, previousVersions, level);
            case JSON -> jsonChecker.check(newSchema, previousVersions, level);
            case PROTOBUF -> protobufChecker.check(newSchema, previousVersions, level);
        };
    }
}
```

### 7.4 Per-Type Compatibility Checkers

**AvroCompatibilityChecker** — uses Apache Avro's `SchemaCompatibility` library:

```java
// For BACKWARD: reader=new, writer=old  (can new schema read old data?)
// For FORWARD:  reader=old, writer=new  (can old schema read new data?)
// For FULL:     both directions

SchemaCompatibility.SchemaPairCompatibility result =
    SchemaCompatibility.checkReaderWriterCompatibility(readerSchema, writerSchema);

if (result.getType() == INCOMPATIBLE) {
    errors.add(result.getDescription());
}
```

- Non-transitive levels: check only against the latest previous schema
- Transitive levels: check against ALL previous schemas (oldest first)

**JsonCompatibilityChecker** (MVP):

Checks two rules:
1. **BACKWARD:** new schema must NOT add required properties (old data may not have them)
2. **FORWARD:** new schema must NOT remove required properties (old consumers expect them)
3. **Both:** property type changes are always incompatible

Limitations: does not resolve `$ref`, does not check nested object schemas. Phase 2 will
add full JSON Schema compatibility per the Confluent specification.

**ProtobufCompatibilityChecker** (MVP):

Regex-based field extraction:
```
(?:optional|required|repeated)?\s*(?:[a-zA-Z_][\w.]*\s+)([a-zA-Z_]\w*)\s*=\s*(\d+)\s*;
```

Rules:
1. Field number → field name mapping must be stable (renaming a field number is breaking)
2. Field name → field number mapping must be stable (renumbering a field name is breaking)
3. Adding or removing fields is allowed

Limitations: does not validate nested messages, `oneof` blocks, or `map` fields.

### 7.5 Compatibility check during registration

```
registerSchema(subject, parsedSchema):
    previousVersions = getVersionsForSubject(subject)  // non-deleted, chronological order

    if level is transitive:
        // Check against ALL previous versions
        schemasToCheck = previousVersions.map(v -> v.schemaString())
    else:
        // Check against latest only
        schemasToCheck = [getLatestVersion(subject).schemaString()]

    errors = compatibilityEngine.isCompatible(
        parsedSchema.canonicalString(), schemasToCheck, schemaType, level)

    if errors is not empty:
        throw new IncompatibleSchemaException(errors)
```

---

## 8. Storage Layer

### 8.1 Internal Topic: `__schemas`

Schema registry metadata is stored in a log-compacted internal Kafka topic.

| Property | Value |
|---|---|
| Topic name | `__schemas` |
| Partitions | 1 (single-partition for total ordering of schema operations) |
| Replication factor | `min(3, cluster size)` |
| `cleanup.policy` | `compact` |
| `min.insync.replicas` | `min(2, replication factor)` |
| `retention.ms` | `-1` (infinite) |

**Key format:** UTF-8 string

| Record type | Key pattern | Example |
|---|---|---|
| Schema version | `schema:{subject}:{version}` | `schema:orders-value:3` |
| Subject config | `config:{subject}` | `config:orders-value` |
| Global config | `config:__global` | `config:__global` |
| Subject mode | `mode:{subject}` | `mode:orders-value` |
| Global mode | `mode:__global` | `mode:__global` |

**Value format:** JSON-serialized (human-readable, easy to debug with console consumer).

Schema version value:
```json
{
  "subject": "orders-value",
  "version": 3,
  "id": 42,
  "schemaType": "AVRO",
  "schema": "{...canonical form...}",
  "references": [],
  "timestampMs": 1713260400000,
  "deleted": false
}
```

**Tombstone (hard delete):** null value with the same key triggers log compaction removal.

### 8.2 SchemaStore — In-Memory Materialized View

```java
public class SchemaStore {

    // Dual-index for O(1) lookups
    private final ConcurrentHashMap<String, SchemaVersion> versionCache;  // "subject:version" → SV
    private final ConcurrentHashMap<Integer, SchemaVersion> idCache;       // schemaId → SV

    // Monotonic ID allocator
    private final SchemaIdAllocator idAllocator;

    // Soft-delete tracking
    private final ConcurrentHashMap<String, Boolean> softDeletedSubjects;

    /**
     * Register a new schema version. Allocates ID and version, writes to topic,
     * updates caches.
     */
    public SchemaVersion register(SubjectName subject, ParsedSchema schema,
                                   List<SchemaReference> references) { ... }

    /** O(1) lookup by global ID. Used by consumers for deserialization. */
    public SchemaVersion getById(SchemaId id) { ... }

    /** O(1) lookup by subject + version. */
    public SchemaVersion getVersion(SubjectName subject, int version) { ... }

    /** Returns latest non-deleted version for subject. */
    public SchemaVersion getLatestVersion(SubjectName subject) { ... }

    /** Returns all registered (non-deleted) subject names. */
    public List<String> listSubjects() { ... }

    /** Returns all non-deleted version numbers for a subject. */
    public List<Integer> listVersions(SubjectName subject) { ... }

    /**
     * Replay records from __schemas topic on startup.
     * Rebuilds versionCache, idCache, and idAllocator from topic contents.
     */
    public void replay(ConsumerRecords<String, byte[]> records) { ... }
}
```

### 8.3 SchemaIdAllocator

```java
public class SchemaIdAllocator {
    private final AtomicInteger counter = new AtomicInteger(SchemaId.FIRST_ID);

    public SchemaId next() {
        return new SchemaId(counter.getAndIncrement());
    }

    /** Called during replay to seed the counter past any existing IDs. */
    public void seedFrom(int maxKnownId) {
        counter.updateAndGet(current -> Math.max(current, maxKnownId + 1));
    }
}
```

### 8.4 SchemaConfigStore

```java
public class SchemaConfigStore {
    private final ConcurrentHashMap<String, CompatibilityConfig> configs;
    // key: subject name, or "__global" for default

    public CompatibilityConfig get(SubjectName subject) {
        CompatibilityConfig subjectConfig = configs.get(subject.value());
        if (subjectConfig != null) return subjectConfig;
        CompatibilityConfig globalConfig = configs.get("__global");
        if (globalConfig != null) return globalConfig;
        return CompatibilityConfig.DEFAULT;  // BACKWARD
    }
}
```

### 8.5 SchemaModeStore

```java
public class SchemaModeStore {
    private final ConcurrentHashMap<String, SchemaMode> modes;
    // key: subject name, or "__global" for default

    public SchemaMode get(SubjectName subject) {
        SchemaMode subjectMode = modes.get(subject.value());
        if (subjectMode != null) return subjectMode;
        SchemaMode globalMode = modes.get("__global");
        if (globalMode != null) return globalMode;
        return SchemaMode.READWRITE;
    }
}
```

### 8.6 Startup Replay — Topic Materialization

On broker startup, the schema registry consumer reads `__schemas` from offset 0 to
the end, replaying all records to rebuild the in-memory caches:

```
Broker starts
  │
  ├─ Create internal __schemas topic if it doesn't exist
  │   (auto-create with configured replication factor)
  │
  ├─ Create a consumer (assign partition 0, seek to beginning)
  │
  ├─ Read all records to end:
  │   for each record:
  │     if record.value == null:
  │       → tombstone: remove from caches (hard delete)
  │     else:
  │       key = record.key()
  │       if key starts with "schema:":
  │         → deserialize SchemaVersion, put in versionCache + idCache
  │         → seedFrom(schemaVersion.id())
  │       if key starts with "config:":
  │         → deserialize CompatibilityConfig, put in configCache
  │       if key starts with "mode:":
  │         → deserialize SchemaMode, put in modeCache
  │
  ├─ Mark schema registry as READY
  │   (reject all requests with 503 until replay completes)
  │
  └─ Begin accepting schema registry HTTP requests
```

### 8.7 Write Path — Topic Append

Write operations (register, delete, config change, mode change) produce to `__schemas`
using the broker's `ReplicaManager.appendRecords()`:

```java
// In SchemaStore.register():
byte[] value = JsonSerde.serialize(schemaVersion);
MemoryRecords records = MemoryRecords.withRecords(
    CompressionType.NONE,
    new SimpleRecord(key.getBytes(UTF_8), value));

CompletableFuture<Long> future = new CompletableFuture<>();
replicaManager.appendRecords(
    timeout = 5000,
    requiredAcks = -1,  // all ISR
    internalTopicsAllowed = true,
    origin = AppendOrigin.COORDINATOR,
    entriesPerPartition = Map.of(topicPartition, records),
    responseCallback = result -> {
        if (result.error == Errors.NONE) {
            // Update in-memory cache AFTER successful append
            versionCache.put(cacheKey, schemaVersion);
            idCache.put(schemaVersion.id().id(), schemaVersion);
            future.complete(result.baseOffset);
        } else {
            future.completeExceptionally(
                new SchemaRegistryStoreException(result.error));
        }
    }
);
return future;
```

### 8.8 Consistency Model

**Read-your-writes on the registering broker.** The in-memory cache is updated in the
`appendRecords` callback — after the record is durably committed (acks=all). Subsequent
reads on the same broker immediately see the new schema.

**Eventual consistency across brokers.** Other brokers must consume the `__schemas` topic
to see new registrations. A background consumer thread on each broker tails `__schemas`
and applies updates to the local cache. Typical propagation delay: 10–100 ms (comparable
to metadata propagation).

**Schema ID allocation.** In a multi-broker cluster, two concurrent registrations could
race for the same ID. The single-partition `__schemas` topic provides total ordering —
the first write wins. The second broker detects the conflict during replay and re-allocates:

```
Broker A: allocates ID=42, appends to __schemas
Broker B: allocates ID=42, appends to __schemas
  → __schemas topic ordering: Broker A's record first (offset 100), Broker B's record second (offset 101)
  → Broker B sees Broker A's ID=42 during tail consumption
  → Broker B's registration callback detects conflict, re-allocates ID=43, appends again
```

In practice, the race window is extremely small because:
1. Schema registration is low-frequency (deployment events, not per-message)
2. The append + callback cycle is < 10 ms on a healthy cluster
3. `SchemaIdAllocator.seedFrom()` in the tail consumer adjusts the counter upward

---

## 9. Integration with Existing Kafka Infrastructure

### 9.1 HTTP Router Integration

The `HttpRouter` (from the existing `http-server` module) gains new route registrations:

```java
// In HttpRouter — add schema registry routes:

// Schema operations
router.get("/schemas/types",              schemaHandler::handleGetTypes);
router.get("/schemas/ids/:id",            schemaHandler::handleGetSchemaById);
router.get("/schemas/ids/:id/subjects",   schemaHandler::handleGetSubjectsForId);
router.get("/schemas/ids/:id/versions",   schemaHandler::handleGetVersionsForId);

// Subject operations
router.get("/subjects",                           schemaHandler::handleListSubjects);
router.get("/subjects/:subject/versions",          schemaHandler::handleListVersions);
router.get("/subjects/:subject/versions/:version", schemaHandler::handleGetVersion);
router.post("/subjects/:subject/versions",         schemaHandler::handleRegister);
router.post("/subjects/:subject",                  schemaHandler::handleLookup);
router.delete("/subjects/:subject",                schemaHandler::handleDeleteSubject);
router.delete("/subjects/:subject/versions/:version", schemaHandler::handleDeleteVersion);

// Compatibility
router.post("/compatibility/subjects/:subject/versions/:version",
            schemaHandler::handleTestCompatibility);

// Config
router.get("/config",                    schemaHandler::handleGetGlobalConfig);
router.put("/config",                    schemaHandler::handlePutGlobalConfig);
router.get("/config/:subject",           schemaHandler::handleGetSubjectConfig);
router.put("/config/:subject",           schemaHandler::handlePutSubjectConfig);

// Mode
router.get("/mode",                      schemaHandler::handleGetGlobalMode);
router.put("/mode",                      schemaHandler::handlePutGlobalMode);
router.get("/mode/:subject",             schemaHandler::handleGetSubjectMode);
router.put("/mode/:subject",             schemaHandler::handlePutSubjectMode);
```

### 9.2 Wire Format Validation on Produce Path

Kafka producers using `kafka-avro-serializer` prepend a 5-byte header to record values:

```
┌──────────┬────────────────────┬────────────────────────┐
│ 0x00     │ Schema ID (4 bytes)│ Serialized data        │
│ magic    │ big-endian int     │ (Avro binary, etc.)    │
└──────────┴────────────────────┴────────────────────────┘
```

The HTTP produce handler (from the HTTP protocol design) can optionally validate schema
IDs at produce time:

```java
// In KafkaApis.handleHttpProduceRequest() — optional validation step:
if (config.schemaRegistryValidateOnProduce) {
    for (Record record : batch) {
        byte[] value = record.value();
        if (value != null && value.length >= 5 && value[0] == 0x00) {
            int schemaId = ByteBuffer.wrap(value, 1, 4).getInt();
            if (!schemaRegistry.existsById(new SchemaId(schemaId))) {
                // Reject with 422: unknown schema ID
            }
        }
    }
}
```

`existsById()` is O(1) — a `ConcurrentHashMap.containsKey()` call. Safe for the hot
produce path.

### 9.3 Lifecycle Integration

Schema registry startup and shutdown integrate with `BrokerServer`:

```java
// In BrokerServer.startup():
if (config.schemaRegistryEnabled) {
    schemaRegistry = new DefaultSchemaRegistry(
        replicaManager, config, time, metrics);
    schemaRegistry.startup();  // creates topic, runs replay, starts tail consumer
    httpRouter.registerSchemaRegistryRoutes(
        new SchemaRegistryRequestHandler(schemaRegistry));
}

// In BrokerServer.shutdown():
if (schemaRegistry != null) {
    schemaRegistry.shutdown();  // stops tail consumer, flushes pending writes
}
```

### 9.4 Metrics Integration

Schema registry exposes Micrometer-compatible metrics:

| Metric | Type | Description |
|---|---|---|
| `kafka.schema.registry.registered.total` | Counter | New schema registrations (excludes idempotent re-registers) |
| `kafka.schema.registry.compat.passed` | Counter | Compatibility checks that passed |
| `kafka.schema.registry.compat.failed` | Counter | Compatibility checks that failed |
| `kafka.schema.registry.cache.hits` | Counter | `getById` cache hits |
| `kafka.schema.registry.cache.misses` | Counter | `getById` cache misses (should approach zero after replay) |
| `kafka.schema.registry.request.rate` | Meter | Request rate by operation (register, get, delete, etc.) |
| `kafka.schema.registry.request.latency` | Histogram | Request latency by operation |

These metrics are tagged with `operation` (register, get-by-id, get-version, list-subjects,
etc.) and exposed via the existing Kafka JMX / Prometheus metrics endpoint.

---

## 10. New Module: `schema-registry`

```
kafka/
└── schema-registry/
    ├── build.gradle
    └── src/
        ├── main/
        │   └── java/org/apache/kafka/schemaregistry/
        │       ├── SchemaType.java
        │       ├── ParsedSchema.java               (sealed interface)
        │       ├── AvroSchema.java
        │       ├── ProtobufSchema.java
        │       ├── JsonSchema.java
        │       ├── SchemaId.java
        │       ├── SubjectName.java
        │       ├── SchemaVersion.java
        │       ├── SchemaReference.java
        │       ├── CompatibilityLevel.java
        │       ├── CompatibilityConfig.java
        │       ├── SchemaMode.java
        │       ├── InvalidSchemaException.java
        │       ├── SchemaNotFoundException.java
        │       ├── IncompatibleSchemaException.java
        │       ├── SchemaRegistryStoreException.java
        │       │
        │       ├── parser/
        │       │   ├── SchemaParser.java            (interface)
        │       │   ├── SchemaParserRegistry.java    (static dispatcher)
        │       │   ├── AvroSchemaParser.java
        │       │   ├── JsonSchemaParser.java
        │       │   └── ProtobufSchemaParser.java
        │       │
        │       ├── compat/
        │       │   ├── CompatibilityEngine.java     (interface)
        │       │   ├── DefaultCompatibilityEngine.java
        │       │   ├── AvroCompatibilityChecker.java
        │       │   ├── JsonCompatibilityChecker.java
        │       │   └── ProtobufCompatibilityChecker.java
        │       │
        │       ├── store/
        │       │   ├── SchemaStore.java
        │       │   ├── SchemaConfigStore.java
        │       │   ├── SchemaModeStore.java
        │       │   ├── SchemaIdAllocator.java
        │       │   └── SchemaTopicManager.java      (topic creation + tail consumer)
        │       │
        │       ├── SchemaRegistry.java              (interface)
        │       ├── DefaultSchemaRegistry.java       (implementation)
        │       ├── SchemaRegistryMetrics.java
        │       │
        │       └── handler/
        │           ├── SchemaRegistryRequestHandler.java
        │           └── SchemaRegistryJsonSerde.java  (request/response JSON serialization)
        │
        └── test/
            └── java/org/apache/kafka/schemaregistry/
                ├── parser/
                │   ├── AvroSchemaParserTest.java
                │   ├── JsonSchemaParserTest.java
                │   └── ProtobufSchemaParserTest.java
                ├── compat/
                │   ├── AvroCompatibilityCheckerTest.java
                │   ├── JsonCompatibilityCheckerTest.java
                │   └── ProtobufCompatibilityCheckerTest.java
                ├── store/
                │   ├── SchemaStoreTest.java
                │   ├── SchemaIdAllocatorTest.java
                │   └── SchemaConfigStoreTest.java
                ├── DefaultSchemaRegistryTest.java
                ├── SchemaRegistryRequestHandlerTest.java
                └── SchemaRegistryIntegrationTest.java
```

### 10.1 `schema-registry/build.gradle` Dependencies

```groovy
dependencies {
    implementation project(':core')
    implementation project(':http-server')
    implementation project(':server-common')
    implementation project(':clients')

    // Schema parsing
    implementation "org.apache.avro:avro:${versions.avro}"
    implementation "com.google.protobuf:protobuf-java:${versions.protobuf}"
    implementation "com.networknt:json-schema-validator:${versions.jsonSchemaValidator}"  // phase 2

    // JSON serialization (shared with http-server)
    implementation "com.fasterxml.jackson.core:jackson-databind:${versions.jackson}"

    // Test
    testImplementation project(':core').sourceSets.test.output
    testImplementation "org.junit.jupiter:junit-jupiter:${versions.junit}"
    testImplementation "org.mockito:mockito-core:${versions.mockito}"
}
```

**Avro dependency note:** Apache Avro bundles old Jackson versions. The `avro` dependency
must exclude `jackson-core` and `jackson-mapper` to avoid classpath conflicts:

```groovy
implementation("org.apache.avro:avro:${versions.avro}") {
    exclude group: 'org.codehaus.jackson'  // old Jackson 1.x
    exclude group: 'com.fasterxml.jackson.core'  // let Kafka's version win
}
```

---

## 11. Configuration

New properties added to `KafkaConfig`:

| Property | Default | Description |
|---|---|---|
| `schema.registry.enabled` | `false` | Master switch. When true, creates `__schemas` topic and registers HTTP routes. Requires HTTP listener to be configured. |
| `schema.registry.topic` | `__schemas` | Internal topic name for schema metadata storage. |
| `schema.registry.topic.replication.factor` | `3` | Replication factor for `__schemas` topic. Clamped to cluster size. |
| `schema.registry.compatibility.level` | `BACKWARD` | Default global compatibility level. Can be overridden per-subject via `PUT /config/{subject}`. |
| `schema.registry.max.schema.size.bytes` | `65536` (64 KB) | Max schema definition size. Schemas larger than this are rejected before parsing. |
| `schema.registry.validate.on.produce` | `false` | When true, HTTP produce requests validate the Confluent wire-format schema ID against the registry. Binary protocol produces are not affected. |
| `schema.registry.request.max.body.bytes` | `1048576` (1 MB) | Max HTTP request body size for schema registry endpoints. |
| `schema.registry.tail.consumer.poll.ms` | `100` | Poll interval for the background consumer tailing `__schemas` for cross-broker cache updates. |

No new listener configuration needed — schema registry endpoints are served on the
existing HTTP listener (port 9094 or as configured).

---

## 12. Error Handling

### 12.1 Error Code Mapping

Schema registry uses Confluent-compatible numeric error codes:

| Error Code | HTTP Status | Condition |
|---|---|---|
| `40401` | 404 Not Found | Subject not found (no versions registered) |
| `40402` | 404 Not Found | Specific version not found |
| `40403` | 404 Not Found | Schema ID not found |
| `40404` | 404 Not Found | Permanent delete without prior soft delete |
| `40801` | 409 Conflict | Schema incompatible with previous version(s) |
| `42201` | 422 Unprocessable Entity | Invalid schema (parse failure) |
| `42202` | 422 Unprocessable Entity | Mode is READONLY — registration rejected |
| `50001` | 503 Service Unavailable | Internal topic write failure (broker unavailable) |
| `50002` | 503 Service Unavailable | Schema registry not ready (replay in progress) |
| `50003` | 500 Internal Server Error | Unexpected internal error |

### 12.2 Error Response Format

```json
{
  "error_code": 40401,
  "message": "Subject 'orders-value' not found."
}
```

All error responses use the same JSON structure. The `error_code` is a Confluent-compatible
integer (not a Kafka `Errors` enum value). The `message` is human-readable.

### 12.3 HTTP Status Derivation

The HTTP status code is derived from the error code prefix:

```java
static int httpStatusForErrorCode(int errorCode) {
    int prefix = errorCode / 100;
    return switch (prefix) {
        case 400 -> 400;    // Bad Request
        case 401 -> 401;    // Unauthorized
        case 404 -> 404;    // Not Found
        case 408 -> 409;    // Conflict (incompatible)
        case 422 -> 422;    // Unprocessable Entity
        case 500 -> errorCode == 50001 || errorCode == 50002 ? 503 : 500;
        default  -> 500;
    };
}
```

### 12.4 Schema Registry Readiness

During startup replay (§8.6), all schema registry HTTP requests receive:

```
HTTP 503 Service Unavailable
Retry-After: 1

{
  "error_code": 50002,
  "message": "Schema registry is initializing, please retry"
}
```

The `SchemaRegistryRequestHandler` checks `schemaRegistry.isReady()` before dispatching
any request.

---

## 13. Security

### 13.1 Authentication

Schema registry endpoints share the HTTP listener's authentication mechanism (§12.1 of
the HTTP protocol design). The `KafkaPrincipal` from the HTTP request context flows into
every schema registry operation.

### 13.2 Authorization

Schema registry operations map to Kafka ACL resources. The broker's existing `Authorizer`
is used unchanged.

| Operation | Resource Type | Resource Name | Permission |
|---|---|---|---|
| Register schema | `TOPIC` | subject name (e.g., `orders-value`) | `WRITE` |
| Get schema by ID | — | — | No auth (schema IDs are considered public within the cluster) |
| Get schema by subject | `TOPIC` | subject name | `DESCRIBE` |
| List subjects | `TOPIC` | `*` (all topics) | `DESCRIBE` (filtered) |
| Delete subject/version | `TOPIC` | subject name | `DELETE` |
| Get/set config | `CLUSTER` | cluster ID | `ALTER` |
| Get/set mode | `CLUSTER` | cluster ID | `ALTER` |
| Test compatibility | `TOPIC` | subject name | `DESCRIBE` |

**Subject → Topic mapping.** By convention, subject names follow the pattern
`{topic-name}-key` or `{topic-name}-value`. The authorizer uses the subject name directly
as the topic resource name. For subjects that don't follow this convention, the full
subject string is used as the resource name.

### 13.3 Quota Enforcement

Schema registry requests are counted toward the HTTP client's quota (shared with
produce/consume HTTP quota via `ClientQuotaManager`). Schema registration is low-frequency
enough that a dedicated quota is not needed.

---

## 14. Implementation Plan

### Phase 1 — Core Schema Registry

1. Create `schema-registry` Gradle submodule with Avro + Protobuf dependencies
2. Implement `SchemaType`, `ParsedSchema` sealed interface, `AvroSchema`, `ProtobufSchema`, `JsonSchema`
3. Implement `SchemaParser` interface + `AvroSchemaParser`, `JsonSchemaParser`, `ProtobufSchemaParser`
4. Implement `CompatibilityEngine` interface + `AvroCompatibilityChecker` (using Apache Avro lib)
5. Implement `JsonCompatibilityChecker` (MVP: required fields + type changes)
6. Implement `ProtobufCompatibilityChecker` (MVP: field number/name stability)
7. Implement value types: `SchemaId`, `SubjectName`, `SchemaVersion`, `SchemaReference`, `CompatibilityConfig`, `CompatibilityLevel`, `SchemaMode`
8. Implement `SchemaIdAllocator` (AtomicInteger, CAS-based)
9. Implement `SchemaStore` with dual-index caches (versionCache + idCache)
10. Implement `SchemaConfigStore` and `SchemaModeStore`
11. Implement `SchemaRegistry` interface + `DefaultSchemaRegistry`
12. Unit tests for all parsers, compatibility checkers, store operations

### Phase 2 — HTTP API + Storage

13. Implement `SchemaRegistryRequestHandler` — HTTP request parsing and routing
14. Implement `SchemaRegistryJsonSerde` — request/response JSON serialization
15. Register schema registry routes in `HttpRouter`
16. Implement `SchemaTopicManager` — internal `__schemas` topic creation and management
17. Implement startup replay (§8.6) — read `__schemas` to rebuild in-memory caches
18. Implement write path (§8.7) — produce to `__schemas` via `ReplicaManager.appendRecords()`
19. Add `schema.registry.enabled` config property and lifecycle integration with `BrokerServer`
20. Implement readiness check (503 during replay)
21. Integration tests: register → get → compatibility check on single broker

### Phase 3 — Multi-Broker + Delete + Polish

22. Implement background tail consumer for cross-broker cache propagation
23. Implement soft-delete and hard-delete for subjects and versions
24. Implement IMPORT mode (bypass compatibility, explicit ID/version)
25. Implement schema lookup (`POST /subjects/{subject}` — content-addressed match)
26. Implement `GET /schemas/ids/{id}/subjects` and `GET /schemas/ids/{id}/versions`
27. Add `SchemaRegistryMetrics` (register count, compat pass/fail, cache hit/miss, request rate/latency)
28. Add ACL integration (§13.2) — authorization checks per operation
29. Integration tests: multi-broker cluster, register on Broker A → read from Broker B

### Phase 4 — Wire Format Validation + Advanced Features

30. Implement wire-format schema ID validation on HTTP produce path (§9.2, opt-in via config)
31. Schema references — store and return `references` array (resolve in future phase)
32. Schema normalization (canonicalize before comparison for improved idempotency)
33. OpenAPI 3.0 spec for schema registry endpoints
34. Performance benchmarking: registration throughput, read latency under load
35. Confluent client compatibility testing: `kafka-avro-serializer` end-to-end

---

## 15. Implementation Concerns

### 15.1 CRITICAL — Single-Partition `__schemas` Topic and ID Allocation

**Problem.** Schema ID allocation must be globally unique across all brokers. Two brokers
registering schemas concurrently could allocate the same ID.

**Solution.** The `__schemas` topic has a single partition. All writes go through this
partition, providing total ordering. The registering broker:

1. Allocates a candidate ID from `SchemaIdAllocator` (local AtomicInteger)
2. Appends the record to `__schemas` (acks=all)
3. The tail consumer on all brokers (including the writer) processes records in offset order
4. If a conflict is detected (another broker's ID was committed first), the local
   allocator advances past the conflict via `seedFrom()`

**Why not use a KRaft metadata log?** Schema metadata is application-level data, not
cluster metadata. Using the KRaft controller for schema storage would couple schema
registry availability to the controller quorum and require changes to the metadata log
format. An internal topic is the established pattern for application-level metadata in
Kafka (`__consumer_offsets`, `__transaction_state`, `_schemas` in Confluent Schema Registry).

**Why single partition is sufficient.** Schema registration is a low-frequency operation
(tens per hour in typical deployments, not thousands per second). The single partition is
not a throughput bottleneck. Read operations are served entirely from in-memory caches
and do not touch the topic at all.

---

### 15.2 CRITICAL — Startup Replay Must Complete Before Serving Requests

**Problem.** If the schema registry starts serving requests before replay completes:
- `getById()` returns 404 for schemas that exist but haven't been replayed yet
- `registerSchema()` could allocate a duplicate ID (allocator not yet seeded)
- Consumers using `kafka-avro-deserializer` fail to deserialize records

**Solution.** The `SchemaRegistryRequestHandler` checks `schemaRegistry.isReady()` on
every request. During startup, `isReady()` returns false until replay reaches the end
of the `__schemas` topic. All requests during this window receive HTTP 503 with
`Retry-After: 1`.

```java
// In SchemaTopicManager.startup():
Consumer<String, byte[]> consumer = createConsumer();
consumer.assign(List.of(new TopicPartition(topicName, 0)));
consumer.seekToBeginning(List.of(new TopicPartition(topicName, 0)));

Map<TopicPartition, Long> endOffsets = consumer.endOffsets(
    List.of(new TopicPartition(topicName, 0)));
long targetOffset = endOffsets.values().iterator().next();

while (consumer.position(tp) < targetOffset) {
    ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(100));
    schemaStore.replay(records);
}

ready.set(true);  // AtomicBoolean — now serve requests
// Switch consumer to tail mode (background thread)
```

**Expected replay time.** A registry with 10,000 schemas produces ~10,000 records in
`__schemas`. At ~1 MB total, replay completes in < 1 second. For large registries
(100K+ schemas), replay may take 5–10 seconds.

---

### 15.3 HIGH — Idempotent Registration: Canonical vs Raw Match

**Problem.** Confluent clients may submit the same schema with different whitespace,
field ordering, or formatting. The registry must recognize these as identical to avoid
version proliferation.

**Solution.** During registration, the schema is checked against existing versions using
**both** raw string equality and canonical string equality:

```java
for (SchemaVersion existing : getVersionsForSubject(subject)) {
    if (existing.schemaString().equals(parsedSchema.canonicalString())
        || existing.schemaString().equals(parsedSchema.rawSchema())) {
        return existing.id();  // idempotent: return existing ID
    }
}
```

Schemas are stored in **canonical form** (`parsedSchema.canonicalString()`), which provides
consistent comparison for future registrations.

---

### 15.4 HIGH — Thread Safety for In-Memory Caches

**Problem.** Schema registry reads and writes happen concurrently on different threads
(Netty workers for reads, append callbacks for writes, tail consumer for cross-broker
updates). All cache operations must be thread-safe.

**Solution.** All caches use `ConcurrentHashMap`. The `SchemaIdAllocator` uses `AtomicInteger`
with CAS operations. No locks are needed because:

1. **Reads** are pure `get()` calls on `ConcurrentHashMap` (lock-free)
2. **Writes** (registration) are linearized by the single-partition `__schemas` topic —
   even if two brokers write concurrently, the topic ordering determines which version
   number is assigned
3. **Cache updates** from the tail consumer are `put()` calls on `ConcurrentHashMap`
   (atomic per-entry)
4. **ID allocation** uses `AtomicInteger.getAndIncrement()` (CAS loop, lock-free)

---

### 15.5 HIGH — Body Size Validation

**Problem.** Schema registry endpoints accept POST/PUT bodies. Without size limits, an
adversarial client could submit a 1 GB schema definition, causing OOM.

**Solution.** Two-layer defense:

1. **Netty layer:** `HttpObjectAggregator` rejects bodies > `schema.registry.request.max.body.bytes`
   (default 1 MB) with HTTP 413 before the handler is invoked
2. **Parser layer:** `SchemaParserRegistry` rejects schema strings > `schema.registry.max.schema.size.bytes`
   (default 64 KB) with error code `42201`

The gap between 1 MB (body) and 64 KB (schema) accounts for JSON envelope overhead and
the `references` array.

---

### 15.6 MEDIUM — Content-Type Negotiation

**Problem.** Confluent clients send `Content-Type: application/vnd.schemaregistry.v1+json`.
Generic HTTP clients send `application/json`. The handler must accept both.

**Solution:**

```java
String contentType = request.headers().get(CONTENT_TYPE);
if (contentType != null
    && !contentType.contains("application/json")
    && !contentType.contains("application/vnd.schemaregistry.v1+json")) {
    return errorResponse(415, "Unsupported content type");
}
```

Response content-type is always `application/vnd.schemaregistry.v1+json` for Confluent
client compatibility.

---

### 15.7 MEDIUM — `latest` Version Resolution

**Problem.** Multiple endpoints accept `latest` as the version parameter. This must
resolve to the highest non-deleted version number at the time of the request.

**Solution:**

```java
int resolveVersion(SubjectName subject, String versionParam) {
    if ("latest".equals(versionParam)) {
        SchemaVersion latest = schemaStore.getLatestVersion(subject);
        if (latest == null) throw new SchemaNotFoundException(40401);
        return latest.version();
    }
    try {
        int version = Integer.parseInt(versionParam);
        if (version < 1) throw new InvalidRequestException("Version must be >= 1");
        return version;
    } catch (NumberFormatException e) {
        throw new InvalidRequestException("Invalid version: " + versionParam);
    }
}
```

---

### 15.8 LOW — Schema Registry Availability During Broker Rolling Restart

**Problem.** During rolling restarts, each broker must replay `__schemas` before serving
schema registry requests. If all brokers restart simultaneously, there is a window where
no broker can serve schema registry requests.

**Mitigation.** Schema registry readiness is independent per-broker — as soon as one
broker completes replay, it can serve reads. Rolling restarts (one broker at a time)
ensure continuous availability. The `GET /v1/health` endpoint (from the HTTP protocol
design) can be extended with schema registry readiness:

```json
{
  "status": "RUNNING",
  "brokerId": 3,
  "clusterId": "abc123",
  "schemaRegistry": "READY"
}
```

Load balancers can use this to route schema registry traffic only to brokers that have
completed replay.

---

## 16. Comparison with Confluent Schema Registry

### 16.1 Architecture Comparison

| Aspect | Confluent Schema Registry | This Design |
|---|---|---|
| **Deployment** | Separate process (requires own JVM, port, monitoring) | Embedded in Kafka broker (zero additional infrastructure) |
| **Storage** | Internal `_schemas` Kafka topic | Internal `__schemas` Kafka topic (same pattern) |
| **HA** | Leader election via `_schemas` consumer group | All brokers serve reads; writes linearized by topic |
| **Port** | Dedicated port (default 8081) | Shared HTTP listener (default 9094) |
| **API** | Proprietary REST API | Same API — Confluent client-compatible |
| **Auth** | Separate auth config (basic auth, SSL) | Reuses Kafka's `KafkaPrincipalBuilder` + `Authorizer` |
| **Metrics** | JMX (separate process) | Integrated with Kafka's metrics (JMX + Prometheus) |
| **Multi-tenant** | Requires multiple instances | Single instance (multi-tenant out of scope) |

### 16.2 API Compatibility

This design implements the Confluent Schema Registry v1 API surface. Existing Confluent
serializers/deserializers (`kafka-avro-serializer`, `kafka-protobuf-serializer`,
`kafka-json-schema-serializer`) work by changing only:

```properties
schema.registry.url=http://broker1:9094
```

### 16.3 Features Not Implemented

| Confluent Feature | Status | Rationale |
|---|---|---|
| Schema references (`$ref` resolution) | Stored, not resolved | Complexity vs. usage; most Avro schemas don't use references |
| Schema rules / migration rules | Out of scope | Enterprise feature |
| DEK registry | Out of scope | Enterprise feature |
| Exporters | Out of scope | Enterprise feature |
| Multi-datacenter replication | Out of scope | Use MirrorMaker for `__schemas` topic |
| Cluster linking | Out of scope | Enterprise feature |
| Context support | Out of scope | Multi-tenant feature |

### 16.4 Advantages of Embedded Approach

1. **Zero ops overhead:** No separate process to deploy, monitor, or upgrade
2. **Unified auth:** Same `KafkaPrincipalBuilder` and `Authorizer` as Kafka itself
3. **No port conflicts:** Schema registry shares the HTTP listener
4. **Built-in HA:** Every broker can serve schema reads after replay; no separate leader election
5. **Faster local reads:** In-memory cache on every broker; no network hop to a separate registry
6. **Consistent lifecycle:** Schema registry starts and stops with the broker

---

*Document version: 1.0 — 2026-04-16*
*Branch: feature/http-protocol*
