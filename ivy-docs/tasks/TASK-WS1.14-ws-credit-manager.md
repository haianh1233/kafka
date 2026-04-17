# TASK-WS1.14: WsCreditManager — Per-Subscription Credit-Based Flow Control

## Prerequisites

| Task | What it delivers | Why this task needs it |
|------|------------------|-----------------------|
| None | — | Standalone flow control component with no upstream dependencies |

---

## Context

WebSocket push delivery requires **credit-based flow control** to prevent the broker from overwhelming slow consumers. Without flow control, a slow WebSocket client (e.g., a browser on a bad connection) causes the Netty write buffer to grow unboundedly until OOM.

From **design doc §21.2**:

> Credits provide explicit, client-controlled flow control:
> 1. Client sets initial credits on `subscribe` (default 100)
> 2. Each `deliver` frame consumes 1 credit
> 3. When credits = 0, the fetch loop pauses (no more deliveries)
> 4. Client sends `credits` frame to replenish
> 5. Netty `ChannelOption.WRITE_BUFFER_WATER_MARK` acts as a safety net

The `WsCreditManager` is per-subscription. The `WsConsumerFetchLoop` (TASK-WS1.15) calls `awaitCredits()` which blocks until credits > 0 or timeout. The fetch loop runs on `wsConsumerExecutor` (not Netty), so blocking is safe.

The `awaitCredits` implementation uses `LockSupport.parkNanos` with 10ms polling as specified in §21.2.

---

## Specification

```java
package kafka.server.http.ws;

import io.netty.channel.Channel;

/**
 * Per-subscription credit tracking for WebSocket push flow control.
 *
 * Thread-safe: grant() called from Netty event loop, consume()/awaitCredits()
 * called from wsConsumerExecutor.
 */
public final class WsCreditManager {

    /**
     * @param initialCredits starting credit count
     * @param channel        the Netty channel (for writability check)
     */
    public WsCreditManager(int initialCredits, Channel channel);

    /**
     * Grants additional credits. Called when client sends a credits frame.
     *
     * @param additional number of credits to add (must be > 0)
     * @throws IllegalArgumentException if additional <= 0
     */
    public void grant(int additional);

    /**
     * Consumes one credit. Returns true if a credit was available, false if already at 0.
     */
    public boolean consume();

    /**
     * Blocks until credits > 0 and channel is writable, or timeout expires.
     * Uses LockSupport.parkNanos with 10ms polling.
     *
     * @param timeoutMs maximum time to wait in milliseconds
     * @return current credit count (0 if timed out or channel not writable)
     */
    public int awaitCredits(long timeoutMs);

    /**
     * Returns the current credit count.
     */
    public int available();

    /**
     * Resets credits to zero. Called on unsubscribe.
     */
    public void reset();
}
```

---

## Implementation Details

**Module:** `http-server`

**Files to study:**

| File | Why |
|------|-----|
| `ivy-docs/http-protocol-extend-design.md` §21.2 | awaitCredits pattern with LockSupport.parkNanos |
| `ivy-docs/http-protocol-extend-design.md` §21.3 | Memory bounds — credits cap pending acks |

```java
// From design doc §21.2 — WsCreditManager pattern:
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

**Files to create:**

| File | What it does |
|------|--------------|
| `http-server/src/main/java/kafka/server/http/ws/WsCreditManager.java` | Per-subscription credit tracking with park/unpark |
| `http-server/src/test/java/kafka/server/http/ws/WsCreditManagerTest.java` | Unit tests |

**Files to modify:**

None.

> **CRITICAL:** `awaitCredits` must also check `channel.isWritable()`. Per design doc §21.2: "if the write buffer exceeds the high water mark, `channel.isWritable()` returns false and the fetch loop pauses regardless of credit count." Treat not-writable channel as 0 credits.

> **CRITICAL:** `consume()` must use `compareAndSet` or `decrementAndGet` carefully to avoid going below 0.

**Implementation order:**
1. Create `WsCreditManager` with `AtomicInteger` credits and `Channel` reference
2. Implement `grant()` with addAndGet
3. Implement `consume()` with getAndUpdate to floor at 0
4. Implement `awaitCredits()` with LockSupport.parkNanos polling loop
5. Implement `available()` and `reset()`
6. Write tests

---

## Skeleton Code

### Production class

```java
package kafka.server.http.ws;

import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

/**
 * Per-subscription credit-based flow control for WebSocket push delivery.
 *
 * // Time: Created - TASK-WS1.14
 */
public final class WsCreditManager {

    private static final Logger log = LoggerFactory.getLogger(WsCreditManager.class);
    private static final long POLL_INTERVAL_NANOS = 10_000_000L; // 10ms

    private final AtomicInteger credits;
    private final Channel channel;

    /**
     * @param initialCredits starting credit count (must be >= 0)
     * @param channel        the Netty channel (for writability check)
     */
    public WsCreditManager(int initialCredits, Channel channel) {
        if (initialCredits < 0) throw new IllegalArgumentException("initialCredits must be >= 0");
        this.credits = new AtomicInteger(initialCredits);
        this.channel = Objects.requireNonNull(channel, "channel");
    }

    /**
     * Grants additional credits. Called when client sends a credits frame.
     */
    public void grant(int additional) {
        if (additional <= 0) throw new IllegalArgumentException("additional must be > 0");
        int newValue = credits.addAndGet(additional);
        if (log.isDebugEnabled()) {
            log.debug("Credits granted: +{} → {}", additional, newValue);
        }
    }

    /**
     * Consumes one credit. Returns true if a credit was available.
     */
    public boolean consume() {
        int prev = credits.getAndUpdate(c -> c > 0 ? c - 1 : 0);
        return prev > 0;
    }

    /**
     * Blocks until credits > 0 and channel is writable, or timeout expires.
     */
    public int awaitCredits(long timeoutMs) {
        long deadline = System.nanoTime() + timeoutMs * 1_000_000L;
        while (true) {
            int current = credits.get();
            if (current > 0 && channel.isWritable()) {
                return current;
            }
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) return 0;
            LockSupport.parkNanos(Math.min(remaining, POLL_INTERVAL_NANOS));
        }
    }

    /**
     * Returns the current credit count.
     */
    public int available() {
        return credits.get();
    }

    /**
     * Resets credits to zero.
     */
    public void reset() {
        credits.set(0);
    }
}
```

### Test class

```java
package kafka.server.http.ws;

import io.netty.channel.Channel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * // Time: Created - TASK-WS1.14
 */
class WsCreditManagerTest {

    private Channel channel;
    private WsCreditManager manager;

    @BeforeEach
    void setUp() {
        channel = mock(Channel.class);
        when(channel.isWritable()).thenReturn(true);
        manager = new WsCreditManager(10, channel);
    }

    @Test
    void initialCredits_availableImmediately() {
        assertEquals(10, manager.available());
    }

    @Test
    void consume_decrementsCredits() {
        assertTrue(manager.consume());
        assertEquals(9, manager.available());
    }

    @Test
    void consume_atZero_returnsFalse() {
        manager = new WsCreditManager(0, channel);
        assertFalse(manager.consume());
        assertEquals(0, manager.available());
    }

    @Test
    void consume_neverGoesBelowZero() {
        manager = new WsCreditManager(1, channel);
        assertTrue(manager.consume());
        assertFalse(manager.consume());
        assertEquals(0, manager.available());
    }

    @Test
    void grant_addsCredits() {
        manager.grant(5);
        assertEquals(15, manager.available());
    }

    @Test
    void grant_zeroOrNegative_throwsIAE() {
        assertThrows(IllegalArgumentException.class, () -> manager.grant(0));
        assertThrows(IllegalArgumentException.class, () -> manager.grant(-1));
    }

    @Test
    void awaitCredits_returnsImmediately_whenAvailable() {
        int credits = manager.awaitCredits(1000);
        assertEquals(10, credits);
    }

    @Test
    void awaitCredits_returnsZero_onTimeout() {
        manager = new WsCreditManager(0, channel);
        long start = System.nanoTime();
        int credits = manager.awaitCredits(50);
        long elapsed = (System.nanoTime() - start) / 1_000_000;
        assertEquals(0, credits);
        assertTrue(elapsed >= 40, "Should have waited ~50ms, got " + elapsed);
    }

    @Test
    void awaitCredits_returnsZero_whenChannelNotWritable() {
        when(channel.isWritable()).thenReturn(false);
        int credits = manager.awaitCredits(50);
        assertEquals(0, credits);
    }

    @Test
    void awaitCredits_unblocksOnGrant() throws InterruptedException {
        manager = new WsCreditManager(0, channel);
        AtomicInteger result = new AtomicInteger(-1);
        CountDownLatch latch = new CountDownLatch(1);

        Thread waiter = new Thread(() -> {
            result.set(manager.awaitCredits(5000));
            latch.countDown();
        });
        waiter.start();

        Thread.sleep(50); // let waiter start polling
        manager.grant(5);

        assertTrue(latch.await(1, java.util.concurrent.TimeUnit.SECONDS));
        assertTrue(result.get() > 0);
    }

    @Test
    void reset_setsCreditsToZero() {
        manager.reset();
        assertEquals(0, manager.available());
    }

    @Test
    void constructor_nullChannel_throwsNPE() {
        assertThrows(NullPointerException.class, () -> new WsCreditManager(10, null));
    }

    @Test
    void constructor_negativeCredits_throwsIAE() {
        assertThrows(IllegalArgumentException.class, () -> new WsCreditManager(-1, channel));
    }
}
```

### Existing pattern reference

```java
// From design doc §21.2 — the complete awaitCredits pattern:
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
```

---

## Tests

**Test class:** `http-server/src/test/java/kafka/server/http/ws/WsCreditManagerTest.java`

| Test method | What it verifies |
|-------------|-----------------|
| `initialCredits_availableImmediately` | Constructor sets initial count |
| `consume_decrementsCredits` | Single consume decrements |
| `consume_atZero_returnsFalse` | Cannot consume when empty |
| `consume_neverGoesBelowZero` | Floor at 0 |
| `grant_addsCredits` | Grant increases count |
| `grant_zeroOrNegative_throwsIAE` | Input validation |
| `awaitCredits_returnsImmediately_whenAvailable` | No wait when credits > 0 |
| `awaitCredits_returnsZero_onTimeout` | Timeout returns 0 |
| `awaitCredits_returnsZero_whenChannelNotWritable` | Channel writability check |
| `awaitCredits_unblocksOnGrant` | Cross-thread grant unblocks waiter |
| `reset_setsCreditsToZero` | Reset zeroes credits |
| `constructor_nullChannel_throwsNPE` | Null rejection |
| `constructor_negativeCredits_throwsIAE` | Negative rejection |

**Run command:**
```bash
cd /home/anh/kafka && ./gradlew :http-server:test --tests 'kafka.server.http.ws.WsCreditManagerTest'
```

---

## Rules

- No blocking on Netty event loop — awaitCredits is called from wsConsumerExecutor only (§21.1)
- LockSupport.parkNanos with 10ms polling per §21.2
- Channel writability as additional backpressure signal (§21.2)
- Credits naturally bound pending acks memory (§21.3)

---

## Learning

- **`getAndUpdate` is the idiomatic floor-at-zero decrement.** `decrementAndGet` can go negative under concurrent consume before a reader reads, requiring a retry loop. `getAndUpdate(c -> c > 0 ? c - 1 : 0)` does it atomically in one CAS cycle and the return value (prev) tells us whether a credit was actually available.
- **`LockSupport.parkNanos` is used as a sleep, not for handoff.** There is no `unpark` when credits are granted — the 10ms polling interval is what guarantees bounded wakeup latency. This keeps the API lock-free on the grant side (`grant()` is only an `addAndGet`, safe from the Netty event loop) at the cost of up to 10ms latency on cross-thread wakeup. The concurrent tests confirmed the 10ms window is fast enough to meet a 2s latch.
- **`channel.isWritable()` is treated as a gate equivalent to "credits == 0."** Even if the client has granted credits, pushing data into a full Netty write buffer would defeat the backpressure. By blending both signals into a single `awaitCredits` return value, the fetch loop doesn't need two separate checks.
- **Deadline-based timeout, not remaining decrement.** Computing `deadline = nanoTime() + timeoutNanos` once at entry and comparing against `nanoTime()` each loop prevents drift from the 10ms polling interval accumulating across iterations.
- **Zero-credits is a valid initial state.** The task spec permits `initialCredits = 0` (consumer must grant before receiving anything). Only strictly negative is rejected.

---

## Limitations

- **Polling instead of explicit unpark.** `grant()` could `LockSupport.unpark(waiterThread)` for instantaneous wakeup, but that requires tracking the waiter thread reference with additional synchronization, complicating the API. 10ms latency is acceptable for WebSocket delivery pacing.
- **Single-waiter assumption.** The design doc makes this per-subscription, so only one fetch loop ever calls `awaitCredits`. If multiple threads called it simultaneously they would all wake on the same credit increment and race in `consume()`. Correctness still holds (consume is atomic floor-at-zero), but efficiency would degrade.
- **No bounded cap on credits.** The spec doesn't enforce a maximum, so a malicious/buggy client could grant `Integer.MAX_VALUE` and cause overflow on `addAndGet`. A future `ws.max.credits` config (referenced in TASK-WS1.01) would add this bound — out of scope here.
- **`reset()` does not unblock a waiter on another thread.** After reset, the waiter keeps polling until timeout. This is intentional: reset is called on close and the caller is responsible for also interrupting the fetch loop. Adding wakeup would require tracking a parked thread.
- **Pre-existing branch compilation issue (not caused by this task).** `http-server/src/main/scala/kafka/network/HttpRequestHandler.scala` references Java symbols that don't exist on the current Java API (`HttpRouter.validateClientId`, `HttpRouter.HandlerType`, `HttpResponseSerializer.serialize`, `HttpRequestTranslator.translateCommitOffsets`, etc.) — and `http-server/src/main/scala/kafka/server/http/HttpRouter.scala` defines a Scala object that shadows the Java `HttpRouter` class in the same package. `compileScala` fails with 23 errors out-of-the-box on branch HEAD. Tests were run with `-x compileScala -x compileTestScala` to bypass; the Java side (WsCreditManager + test) compiles cleanly on its own.

---

## Field Notes

- **TDD cycle:** test file written first (22 methods compiled against nothing and failed with "cannot find symbol"); production class written second; tests ran green on first attempt.
- **Concurrent test tuned for invariants, not exact counts.** The test runs 4 grant threads × 1000 grants and 4 consume threads × 1000 consume attempts (with no blocking). Since consumers can race ahead and observe zero credits, the exact invariant `granted == consumed + remaining` is verified, plus `remaining >= 0` and `consumed <= granted`. No flaky time-based assertions.
- **`awaitCredits_unblocksWhenChannelBecomesWritable`** mirrors the grant-unblock test and ensures the writability gate is symmetric with the credits gate.
- **Mockito for Channel.** The Netty `Channel` interface has many methods; only `isWritable()` is needed.
- Test runs in ~200ms including the concurrent test; well below any reasonable CI budget.
- Run command requires Scala excludes on this branch: `./gradlew :http-server:test --tests 'kafka.server.http.ws.WsCreditManagerTest' -x compileScala -x compileTestScala`.

---

## Acceptance Criteria

- [ ] `cd /home/anh/kafka && ./gradlew :http-server:test --tests 'kafka.server.http.ws.WsCreditManagerTest'` exits 0
- [ ] `grep -r "WsCreditManager" http-server/src/main/java/` returns at least 1 hit
- [ ] `grep -r "awaitCredits" http-server/src/main/java/` returns at least 1 hit
- [ ] Cross-thread grant unblock test passes reliably
- [ ] Learning section filled with at least one entry
- [ ] Limitations section filled (use "None" if truly none)
- [ ] File Manifest section updated after commit

---

## File Manifest

> Filled by the executing agent after each commit.
> Run: `git diff --name-status HEAD~1 HEAD -- '*.java' '*.xml' '*.json' '*.yaml' '*.yml'`

<!-- ### YYYY-MM-DD — <short description> (commit <hash>)
Created:
  - path/to/NewFile.java — <what it does>
Modified:
  - path/to/Existing.java — <what changed>
-->

### 2026-04-17 — WS1.14 WsCreditManager (commit 364438b49c)

Created:
  - http-server/src/main/java/kafka/server/http/ws/WsCreditManager.java — per-subscription credit counter with `grant`/`consume`/`awaitCredits`/`available`/`reset`; lock-free via `AtomicInteger`, blocking via `LockSupport.parkNanos` (10ms polling); additionally gates on Netty `Channel.isWritable()`
  - http-server/src/test/java/kafka/server/http/ws/WsCreditManagerTest.java — 22 unit tests covering basic state, consume floor-at-zero, grant validation, awaitCredits (immediate/zero-timeout/timeout/writability gate/cross-thread unblock-on-grant/cross-thread unblock-on-writable), reset, constructor validation, 8-thread concurrent grant/consume invariant
