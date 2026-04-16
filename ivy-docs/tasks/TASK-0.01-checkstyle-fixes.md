# TASK-0.01: Fix Checkstyle Violations (13 errors)

## Prerequisites

| Task | What it provides |
|------|-----------------|
| None | This is Phase 0 -- must be done first before any other build-fix tasks |

---

## Context

The `http-server` module has 13 checkstyle violations that block CI. There are two categories:

1. **LeftCurly violations (12 errors):** Kafka's checkstyle enforces `LeftCurly` at the default `eol` option for method/class definitions, but the inner-class one-liner accessor methods in `HttpRequestTranslator.java` and `HttpRouter.java` place `{` on the same line as the method signature in a single-line format. The Kafka checkstyle `LeftCurly` rule requires that `{` for **class/interface/record** definitions be on its own line (or at least that the body not be on a single line). For the inner `static final class` declarations and their one-liner accessor methods, the checkstyle rule flags them because the `{` is on the same line as the accessor method and the entire body is crammed into one line.

2. **ImportControl violation (1 error):** `HttpSslContextBuilder.java` imports `javax.net.ssl.SSLException`, but the `checkstyle/import-control-http-server.xml` does not allow the `javax.net.ssl` package.

### Error List

| # | File | Line | Rule | Description |
|---|------|------|------|-------------|
| 1 | `HttpRequestTranslator.java` | 133 | LeftCurly | `public ApiKeys apiKey() { return apiKey; }` -- one-liner accessor |
| 2 | `HttpRequestTranslator.java` | 134 | LeftCurly | `public short apiVersion() { return apiVersion; }` -- one-liner accessor |
| 3 | `HttpRequestTranslator.java` | 135 | LeftCurly | `public ByteBuffer serializedRequest() { return serializedRequest; }` -- one-liner accessor |
| 4 | `HttpRequestTranslator.java` | 625 | LeftCurly | `public ApiKeys apiKey() { return apiKey; }` -- one-liner accessor in OffsetCommitTranslationResult |
| 5 | `HttpRequestTranslator.java` | 626 | LeftCurly | `public OffsetCommitRequest.Builder builder() { return builder; }` -- one-liner accessor |
| 6 | `HttpRequestTranslator.java` | 641 | LeftCurly | `public ApiKeys apiKey() { return apiKey; }` -- one-liner accessor in OffsetFetchTranslationResult |
| 7 | `HttpRequestTranslator.java` | 642 | LeftCurly | `public OffsetFetchRequest.Builder builder() { return builder; }` -- one-liner accessor |
| 8 | `HttpRouter.java` | 88 | LeftCurly | `public HandlerType handlerType() { return handlerType; }` -- one-liner accessor |
| 9 | `HttpRouter.java` | 89 | LeftCurly | `public String topicName() { return topicName; }` -- one-liner accessor |
| 10 | `HttpRouter.java` | 90 | LeftCurly | `public Integer partition() { return partition; }` -- one-liner accessor |
| 11 | `HttpRouter.java` | 91 | LeftCurly | `public String consumerGroup() { return consumerGroup; }` -- one-liner accessor |
| 12 | `HttpRouter.java` | 92 | LeftCurly | `public Map<String, String> queryParams() { return queryParams; }` -- one-liner accessor |
| 13 | `HttpSslContextBuilder.java` | 26 | ImportControl | `import javax.net.ssl.SSLException` not allowed |

---

## Specification

### Fix 1: Expand one-liner accessor methods to multi-line

Kafka's `LeftCurly` checkstyle rule does not allow one-liner method bodies like `public Foo bar() { return baz; }`. Each one-liner accessor must be expanded to a 3-line format:

```java
public Foo bar() {
    return baz;
}
```

This applies to all 12 LeftCurly violations across `HttpRequestTranslator.java` and `HttpRouter.java`.

### Fix 2: Add `javax.net.ssl` to import-control-http-server.xml

Add `<allow pkg="javax.net.ssl" />` to the allowed packages in `checkstyle/import-control-http-server.xml`. This package is needed for `SSLException` in `HttpSslContextBuilder.java` and is a legitimate dependency for an HTTP server module that supports TLS.

---

## Implementation Details

### File 1: `http-server/src/main/java/kafka/server/http/HttpRequestTranslator.java`

**TranslationResult inner class (lines 133-135):**

BEFORE:
```java
        public ApiKeys apiKey() { return apiKey; }
        public short apiVersion() { return apiVersion; }
        public ByteBuffer serializedRequest() { return serializedRequest; }
```

AFTER:
```java
        public ApiKeys apiKey() {
            return apiKey;
        }

        public short apiVersion() {
            return apiVersion;
        }

        public ByteBuffer serializedRequest() {
            return serializedRequest;
        }
```

**OffsetCommitTranslationResult inner class (lines 625-626):**

BEFORE:
```java
        public ApiKeys apiKey() { return apiKey; }
        public OffsetCommitRequest.Builder builder() { return builder; }
```

AFTER:
```java
        public ApiKeys apiKey() {
            return apiKey;
        }

        public OffsetCommitRequest.Builder builder() {
            return builder;
        }
```

**OffsetFetchTranslationResult inner class (lines 641-642):**

BEFORE:
```java
        public ApiKeys apiKey() { return apiKey; }
        public OffsetFetchRequest.Builder builder() { return builder; }
```

AFTER:
```java
        public ApiKeys apiKey() {
            return apiKey;
        }

        public OffsetFetchRequest.Builder builder() {
            return builder;
        }
```

### File 2: `http-server/src/main/java/kafka/server/http/HttpRouter.java`

**RouteResult inner class (lines 88-92):**

BEFORE:
```java
        public HandlerType handlerType() { return handlerType; }
        public String topicName() { return topicName; }
        public Integer partition() { return partition; }
        public String consumerGroup() { return consumerGroup; }
        public Map<String, String> queryParams() { return queryParams; }
```

AFTER:
```java
        public HandlerType handlerType() {
            return handlerType;
        }

        public String topicName() {
            return topicName;
        }

        public Integer partition() {
            return partition;
        }

        public String consumerGroup() {
            return consumerGroup;
        }

        public Map<String, String> queryParams() {
            return queryParams;
        }
```

### File 3: `checkstyle/import-control-http-server.xml`

BEFORE (line 31):
```xml
  <allow pkg="java.security" />
```

AFTER:
```xml
  <allow pkg="java.security" />
  <allow pkg="javax.net.ssl" />
```

---

## Skeleton Code

See Implementation Details above -- the before/after blocks are the complete fix.

---

## Tests

### Verification Command

```bash
./gradlew :http-server:checkstyleMain
```

This must complete with 0 errors. No new tests are needed -- this is a pure style/config fix.

### Pre-existing Tests

All existing `http-server` tests that reference `TranslationResult`, `OffsetCommitTranslationResult`, `OffsetFetchTranslationResult`, and `RouteResult` must still compile and pass. Expanding one-liners to multi-line does not change behavior.

---

## Rules

- Do NOT change any method signatures, return types, or behavior -- only formatting.
- The expanded accessor methods must use 4-space indentation (Kafka standard for Java files).
- The `javax.net.ssl` allow rule must be placed near the other `java.*` / `javax.*` allows for readability.
- Run `./gradlew :http-server:checkstyleMain` and confirm 0 violations before marking complete.

---

## Learning

(empty)

## Limitations

(empty)

## Field Notes

(empty)

---

## Acceptance Criteria

- [x] All 12 LeftCurly violations are fixed by expanding one-liner accessors to multi-line format
- [x] ImportControl violation is fixed by adding `javax.net.ssl` to `checkstyle/import-control-http-server.xml`
- [x] `./gradlew :http-server:checkstyleMain` passes with 0 errors
- [x] No behavioral changes -- only formatting and checkstyle config

---

## File Manifest

| File | Action | Description |
|------|--------|-------------|
| `http-server/src/main/java/kafka/server/http/HttpRequestTranslator.java` | Modified | Expanded 7 one-liner accessor methods to multi-line format (TranslationResult, OffsetCommitTranslationResult, OffsetFetchTranslationResult) |
| `http-server/src/main/java/kafka/server/http/HttpRouter.java` | Modified | Expanded 5 one-liner accessor methods to multi-line format (RouteResult) |
| `checkstyle/import-control-http-server.xml` | Modified | Added `<allow pkg="javax.net.ssl" />` for SSLException import |
