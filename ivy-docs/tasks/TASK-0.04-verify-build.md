# TASK-0.04: Verify Full Build After Fixes

## Prerequisites

| Task | What it provides |
|------|-----------------|
| TASK-0.01 | Checkstyle violations fixed (13 errors resolved) |
| TASK-0.02 | Duplicate classes removed (3 stubs deleted, SocketServer updated) |
| TASK-0.03 | Broken tests fixed (2 restored, 3 deleted) |

ALL three prerequisite tasks must be complete before this task runs. This is the final verification gate.

---

## Context

After the three build-fix tasks, the codebase must be verified end-to-end to confirm:
1. Checkstyle passes with 0 errors
2. All http-server unit tests compile and pass
3. The core module still compiles (no regressions from deleted stubs or updated SocketServer)
4. No remaining `.broken` files
5. No remaining duplicate class conflicts

This task is purely a verification/validation task -- it makes no code changes.

---

## Specification

### Verification Steps (in order)

#### Step 1: Checkstyle

```bash
./gradlew :http-server:checkstyleMain
```

**Expected:** BUILD SUCCESSFUL, 0 checkstyle violations.

**If it fails:** Go back to TASK-0.01 and fix the remaining violations. The error output will show which file/line/rule is still failing.

#### Step 2: http-server compilation (Java + Scala)

```bash
./gradlew :http-server:compileJava :http-server:compileScala
```

**Expected:** BUILD SUCCESSFUL. Both Java and Scala sources compile without errors.

**If it fails:** Check for:
- References to deleted `HttpRequestHandler.java` stub
- Import ambiguity between Java and Scala `HttpResponseSerializer`
- Missing dependencies in `http-server/build.gradle`

#### Step 3: http-server test compilation

```bash
./gradlew :http-server:compileTestJava :http-server:compileTestScala
```

**Expected:** BUILD SUCCESSFUL. All test sources compile.

**If it fails:** Check for:
- Restored `.broken` tests referencing methods that changed in TASK-0.02
- Missing test dependencies (Mockito, JUnit, Netty test utilities)

#### Step 4: http-server unit tests

```bash
./gradlew :http-server:test
```

**Expected:** BUILD SUCCESSFUL, all tests pass.

**Key test classes to watch for:**
- `kafka.network.HttpAcceptorDrainTest` -- restored in TASK-0.03
- `kafka.network.HttpGracefulShutdownTest` -- restored in TASK-0.03
- `kafka.network.HttpAcceptorTest` -- existing test
- `kafka.network.HttpRequestHandlerTest` -- existing test
- `kafka.server.http.HttpRouterTest` -- existing test
- `kafka.server.http.HttpRequestTranslatorTest` -- existing test
- `kafka.server.http.HttpProcessorTest` -- existing test
- `kafka.server.http.HttpResponseSerializerTest` -- existing test
- `kafka.server.http.HttpErrorMapperTest` -- existing test
- `kafka.server.http.HttpMetricsTest` -- existing test

**If tests fail:**
- Port binding failures in `HttpAcceptorDrainTest`: ensure tests use port 0 (ephemeral) and `close()` in `@AfterEach`
- `ClassCastException` in `HttpGracefulShutdownTest`: expected for non-drain path (EmbeddedChannel uses EmbeddedSocketAddress, not InetSocketAddress)
- `NoSuchMethodError`: indicates TASK-0.02 deduplication is incomplete -- a stub class is still on the classpath

#### Step 5: core module compilation

```bash
./gradlew :core:compileScala
```

**Expected:** BUILD SUCCESSFUL. `SocketServer.scala` compiles with the updated `HttpAcceptor` constructor call.

**If it fails:** Check for:
- `SocketServer.scala` still referencing the deleted core `HttpAcceptor` constructor
- Missing compile dependency on `http-server` module in `core/build.gradle`
- Type mismatch: `ConcurrentHashMap[Endpoint, HttpAcceptor]` should resolve to the http-server version

#### Step 6: Verify no .broken files remain

```bash
find http-server/ -name '*.broken' -type f
```

**Expected:** No output (0 files found).

#### Step 7: Verify no duplicate classes

```bash
# Check that HttpAcceptor only exists in http-server
find . -path '*/main/scala/kafka/network/HttpAcceptor.scala' -o -path '*/main/java/kafka/network/HttpAcceptor.java' | grep -v build/

# Check that HttpProcessor stub is gone from core
find core/ -name 'HttpProcessor.scala' -path '*/main/*' | grep -v build/

# Check that HttpRequestHandler.java stub is gone from http-server
find http-server/ -name 'HttpRequestHandler.java' -path '*/main/*' | grep -v build/
```

**Expected:**
- Only `http-server/src/main/scala/kafka/network/HttpAcceptor.scala` for HttpAcceptor
- No output for core HttpProcessor
- No output for http-server HttpRequestHandler.java

---

## Implementation Details

This task makes no code changes. It is a verification-only task.

If any step fails, the fix must be applied in the appropriate prerequisite task (0.01, 0.02, or 0.03) and this task re-run.

---

## Skeleton Code

N/A -- verification only.

---

## Tests

The verification commands above ARE the tests for this task. Each must pass.

### Summary of all commands to run:

```bash
# Step 1: Checkstyle
./gradlew :http-server:checkstyleMain

# Step 2: Main compilation
./gradlew :http-server:compileJava :http-server:compileScala

# Step 3: Test compilation
./gradlew :http-server:compileTestJava :http-server:compileTestScala

# Step 4: Unit tests
./gradlew :http-server:test

# Step 5: Core compilation (regression check)
./gradlew :core:compileScala

# Step 6: No .broken files
find http-server/ -name '*.broken' -type f

# Step 7: No duplicate classes
find . -path '*/main/scala/kafka/network/HttpAcceptor.scala' -o -path '*/main/java/kafka/network/HttpAcceptor.java' | grep -v build/
find core/ -name 'HttpProcessor.scala' -path '*/main/*' | grep -v build/
find http-server/ -name 'HttpRequestHandler.java' -path '*/main/*' | grep -v build/
```

### One-shot combined command (after individual steps pass):

```bash
./gradlew :http-server:checkstyleMain :http-server:test :core:compileScala
```

---

## Rules

- Do NOT make code changes in this task. If something fails, go back to the prerequisite task.
- Run tests with `--tests` filters for targeted test classes if the full `:http-server:test` is too slow. But the final verification must run the full test suite.
- Record the exact error message and stack trace for any failure before going back to fix it.
- Do not skip any verification step -- each catches a different class of failure.

---

## Learning

(empty)

## Limitations

(empty)

## Field Notes

(empty)

---

## Acceptance Criteria

- [ ] `./gradlew :http-server:checkstyleMain` passes with 0 violations
- [ ] `./gradlew :http-server:compileJava :http-server:compileScala` passes
- [ ] `./gradlew :http-server:compileTestJava :http-server:compileTestScala` passes
- [ ] `./gradlew :http-server:test` passes -- all tests green
- [ ] `./gradlew :core:compileScala` passes -- no regressions
- [ ] Zero `.broken` files remain in the repository
- [ ] Zero duplicate class files remain (HttpAcceptor, HttpProcessor, HttpRequestHandler)
- [ ] Build is clean and ready for CI

---

## File Manifest

(empty -- no files modified in this task)
