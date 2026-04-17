# TASK-WS3.07: Graceful Shutdown Drain for WebSocket Connections

## Prerequisites

| Task | What it delivers | Why this task needs it |
|------|------------------|-----------------------|
| TASK-WS1.01 | WsUpgradeOrHttpHandler | Has draining flag to reject new upgrades |
| TASK-WS1.03 | WsConnectionContext | Per-connection state with channel reference |
| TASK-WS1.12 | WsConsumerFetchLoop | Fetch loops must finish current iteration |
| TASK-WS1.14 | WsAckHandler | Pending ACK commits must flush |

---

## Context

The design doc §21.5 specifies graceful shutdown for WebSocket connections. When the broker is shutting down, it must drain all WebSocket connections cleanly:

1. Stop accepting new WebSocket upgrades (check `draining` flag)
2. Send close frame (1001, "Broker shutting down") to ALL WS connections
3. Wait up to `ws.shutdown.drain.ms` (default 5000) for:
   a. Consumer fetch loops to finish current iteration
   b. Pending ACK commits to flush
   c. Publisher confirm futures to resolve
4. Force-close remaining connections

This integrates with the existing `HttpAcceptor.beginDrain()` / `awaitDrain()` lifecycle. WebSocket drain is an additional step in that shutdown sequence.

---

## Specification

### WsConnectionDrainer

```java
package kafka.server.http.ws;

public final class WsConnectionDrainer {

    /** Begin drain: send close frames to all connections, stop fetch loops. */
    public void beginDrain();

    /** Wait for drain to complete within timeout. Returns true if all drained. */
    public boolean awaitDrain(long timeoutMs);

    /** Force-close any remaining connections after drain timeout. */
    public void forceCloseAll();

    /** Get count of connections still in drain. */
    public int drainingCount();
}
```

---

## Implementation Details

**Module:** `http-server`

**Files to create:**

| File | What it does |
|------|--------------|
| `http-server/src/main/java/kafka/server/http/ws/WsConnectionDrainer.java` | Graceful WS connection drain |

**Files to modify:**

| File | What changes |
|------|--------------|
| `http-server/src/main/java/kafka/server/http/HttpAcceptor.java` | Call WsConnectionDrainer.beginDrain() in beginDrain(), awaitDrain() in awaitDrain() |
| `http-server/src/main/java/kafka/server/http/ws/WsUpgradeOrHttpHandler.java` | Check draining flag, reject upgrades during drain |

> **CRITICAL:** Close frame code must be 1001 (Going Away), not 1000 (Normal Closure). This signals to clients that the server is shutting down.

> **CRITICAL:** During drain, the fetch loop must complete its current iteration (finish in-flight fetch, deliver pending records, flush ACK commits) before the connection is closed.

> **CRITICAL:** `awaitDrain()` must have a hard timeout (`ws.shutdown.drain.ms`). After timeout, force-close all remaining connections to prevent shutdown hang.

**Implementation order:**
1. Create WsConnectionDrainer with beginDrain/awaitDrain/forceCloseAll
2. Integrate with HttpAcceptor shutdown lifecycle
3. Modify WsUpgradeOrHttpHandler to reject upgrades during drain
4. Add unit tests

---

## Skeleton Code

```java
package kafka.server.http.ws;

import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Graceful drain for WebSocket connections during broker shutdown.
 *
 * // Time: Created - TASK-WS3.07
 */
public final class WsConnectionDrainer {

    private static final int CLOSE_CODE_GOING_AWAY = 1001;
    private static final String CLOSE_REASON = "Broker shutting down";

    private final AtomicBoolean draining = new AtomicBoolean(false);
    // TODO: WsConnectionRegistry dependency for listing all connections

    public void beginDrain() {
        if (!draining.compareAndSet(false, true)) return;
        // TODO: Send close frame 1001 to all connections
        // TODO: Signal all fetch loops to stop after current iteration
        // TODO: Flush pending ACK commits
    }

    public boolean awaitDrain(long timeoutMs) {
        // TODO: Wait for all connections to close within timeout
        // TODO: Return true if all drained, false if timeout expired
        throw new UnsupportedOperationException("Not yet implemented");
    }

    public void forceCloseAll() {
        // TODO: Force-close any remaining connections
    }

    public boolean isDraining() {
        return draining.get();
    }

    public int drainingCount() {
        // TODO: Return count of still-open connections
        return 0;
    }
}
```

---

## Tests

**Test class:** `http-server/src/test/java/kafka/server/http/ws/WsConnectionDrainerTest.java`

| Test method | What it verifies |
|-------------|-----------------|
| `beginDrain_sendsCloseFrame1001` | All connections receive close frame with code 1001 |
| `beginDrain_idempotent` | Calling beginDrain twice is safe |
| `awaitDrain_returnsTrue_whenAllClosed` | Returns true when connections close before timeout |
| `awaitDrain_returnsFalse_onTimeout` | Returns false when timeout expires |
| `forceCloseAll_closesRemainingConnections` | Remaining connections force-closed |
| `isDraining_correctState` | Reports draining state accurately |
| `newUpgrade_duringDrain_rejected` | WS upgrade requests rejected during drain |

**Run command:**
```bash
timeout 300 ./gradlew :http-server:test --tests 'kafka.server.http.ws.WsConnectionDrainerTest' -x spotlessCheck
```

---

## Rules

- Close frame code: 1001 (Going Away), reason: "Broker shutting down".
- Drain timeout: `ws.shutdown.drain.ms` (default 5000).
- Fetch loops finish current iteration before stopping.
- Pending ACK commits must flush during drain.
- Force-close after timeout — do not block shutdown indefinitely.
- New WS upgrades rejected during drain.

---

## Learning

_To be filled by the executing agent._

---

## Limitations

_To be filled by the executing agent._

---

## Field Notes

_To be filled by the executing agent._

---

## Acceptance Criteria

- [ ] `timeout 300 ./gradlew :http-server:test --tests 'kafka.server.http.ws.WsConnectionDrainerTest' -x spotlessCheck` exits 0
- [ ] `grep -r "WsConnectionDrainer" http-server/src/main/java/` returns at least 2 hits
- [ ] Close frame uses code 1001
- [ ] Learning section filled with at least one entry

---

## File Manifest

_To be filled by the executing agent._
