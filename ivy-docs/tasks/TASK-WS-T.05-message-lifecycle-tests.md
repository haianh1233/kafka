# TASK-WS-T.05: Message Lifecycle Integration Tests

## Prerequisites

| Task | What it delivers | Why this task needs it |
|------|------------------|-----------------------|
| TASK-WS1.17 | WsTestClient | WebSocket test client |
| TASK-WS4.01 | WsDeadLetterHandler | DLX chain tests |
| TASK-WS4.02 | Message TTL | TTL expiry tests |
| TASK-WS4.04 | Poison message protection | Auto-DLX on max retries |
| TASK-WS4.03 | Priority delivery | Priority ordering tests |
| TASK-WS3.01 | Publisher confirms | Confirm tests |
| TASK-WS3.02 | Mandatory message return | Mandatory return tests |

---

## Context

The design doc §23.7 specifies message lifecycle integration tests. These are end-to-end tests with a real broker that exercise the complete message lifecycle including DLX chains, TTL expiry, poison message auto-DLX, priority ordering, credits pause/resume, mandatory return, and publisher confirms.

These tests follow the patterns from the design doc's test examples (§23.7):
- `testNackWithRequeue_redelivers`
- `testNackWithoutRequeue_deadLetters`
- `testCredits_pauseAndResume`
- `testPublisherConfirms`
- `testMandatoryPublish_noRoute_returnsMessage`

---

## Specification

### Test class

```scala
class WsMessageLifecycleIntegrationTest extends HttpIntegrationTestHarness
```

### Scenarios

1. **DLX chain:** NACK → DLX → DLQ subscriber, x-death headers verified
2. **TTL expiry:** Message with short expiration skipped on delivery
3. **Poison message auto-DLX:** Redeliver N times → auto-DLX on Nth NACK
4. **Priority ordering:** Higher priority delivered first within batch
5. **Credits pause/resume:** Credits exhausted → delivery paused → grant → resumed
6. **Mandatory return:** Unroutable message with mandatory=true → returned frame
7. **Publisher confirms:** Enable confirms → publish → published response

---

## Implementation Details

**Module:** `http-server`

**Files to create:**

| File | What it does |
|------|--------------|
| `http-server/src/test/scala/integration/kafka/http/WsMessageLifecycleIntegrationTest.scala` | 7+ lifecycle tests |

> **CRITICAL:** DLX chain test must verify x-death headers in the dead-lettered message. The subscriber to the DLQ queue should see x-death array in the deliver frame.

> **CRITICAL:** TTL test requires producing a message with short expiration, then waiting for it to expire before subscribing. Use `Thread.sleep()` only for TTL expiry — not for coordination.

> **CRITICAL:** Credits test: subscribe with credits=2, publish 3 messages. First 2 delivered, third paused. Grant more credits → third delivered.

**Implementation order:**
1. DLX chain test
2. TTL expiry test
3. Poison message protection test
4. Priority ordering test
5. Credits pause/resume test
6. Mandatory return test
7. Publisher confirms test

---

## Skeleton Code

```scala
package kafka.http

import org.junit.jupiter.api._
import org.junit.jupiter.api.Assertions._
import java.util.concurrent.TimeUnit

/**
 * Message lifecycle integration tests: DLX, TTL, priority, credits, confirms.
 *
 * // Time: Created - TASK-WS-T.05
 */
@Timeout(value = 120, unit = TimeUnit.SECONDS)
class WsMessageLifecycleIntegrationTest extends HttpIntegrationTestHarness {

  private var ws: WsTestClient = _

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    super.setUp(testInfo)
    ws = new WsTestClient(httpUrl(0))
    ws.connect()
  }

  @AfterEach
  override def tearDown(): Unit = {
    if (ws != null) ws.close()
    super.tearDown()
  }

  @Test
  @Timeout(30)
  def testDlxChain_nackToDeadLetterQueue(): Unit = {
    // Setup DLX chain: orders → dlx → dead-letters
    ws.declareExchange("dlx", "direct")
    ws.declareQueue("dead-letters")
    ws.bind("dead-letters", "dlx", "dead.orders")
    ws.declareQueue("orders", args = Map(
      "x-dead-letter-exchange" -> "dlx",
      "x-dead-letter-routing-key" -> "dead.orders"))

    ws.subscribe("orders", "sub-orders", credits = 10, startOffset = "earliest")
    ws.subscribe("dead-letters", "sub-dlq", credits = 10, startOffset = "earliest")

    // Publish and NACK without requeue
    ws.publish("", "orders", "poison-msg")
    val d = ws.waitForDeliver()
    assertEquals("sub-orders", d.get("subscriptionId").asText())
    ws.nack("sub-orders", d.get("deliveryTag").asLong(), requeue = false)

    // Message appears on DLQ with x-death headers
    val dlxMsg = ws.waitForDeliver()
    assertEquals("sub-dlq", dlxMsg.get("subscriptionId").asText())
    // TODO: Verify x-death headers present
  }

  @Test
  @Timeout(30)
  def testTtlExpiry_messageSkipped(): Unit = {
    // TODO: Declare queue, publish message with short expiration
    // TODO: Wait for expiration
    // TODO: Subscribe — message should be skipped
  }

  @Test
  @Timeout(30)
  def testPoisonMessage_autoDeadLetter(): Unit = {
    // TODO: Declare queue with x-max-retries=3 and DLX
    // TODO: Subscribe, publish, NACK requeue=true 3 times
    // TODO: On 4th attempt, message should auto-DLX
  }

  @Test
  @Timeout(30)
  def testPriorityDelivery_highestFirst(): Unit = {
    // TODO: Declare queue with x-max-priority=10
    // TODO: Publish messages with different priorities
    // TODO: Subscribe — verify highest priority delivered first
  }

  @Test
  @Timeout(30)
  def testCredits_pauseAndResume(): Unit = {
    ws.declareQueue("cred-q")
    ws.subscribe("cred-q", "sub-1", credits = 2, startOffset = "earliest")

    ws.publish("", "cred-q", "msg1")
    ws.publish("", "cred-q", "msg2")
    ws.publish("", "cred-q", "msg3")

    // First 2 delivered (credits = 2)
    val d1 = ws.waitForDeliver()
    val d2 = ws.waitForDeliver()
    assertNotNull(d1)
    assertNotNull(d2)

    // Third NOT delivered — credits exhausted
    // TODO: Verify no delivery within 1 second

    // Grant more credits → third delivered
    ws.grantCredits("sub-1", 5)
    val d3 = ws.waitForDeliver()
    assertNotNull(d3)
  }

  @Test
  @Timeout(30)
  def testMandatoryReturn_noRoute(): Unit = {
    ws.declareExchange("ex", "direct")
    // No bindings → message is unroutable

    ws.send("""{"type":"publish","exchange":"ex","routingKey":"nowhere",
               "mandatory":true,"message":{"body":"lost"},"publishId":1}""")

    val returned = ws.waitForType("returned", 5.seconds)
    assertEquals(312, returned.get("replyCode").asInt())
    assertEquals("NO_ROUTE", returned.get("replyText").asText())
  }

  @Test
  @Timeout(30)
  def testPublisherConfirms(): Unit = {
    ws.enableConfirms()
    ws.declareQueue("confirmed-q")

    ws.publish("", "confirmed-q", "confirmed-msg", publishId = 1)

    val confirm = ws.waitForType("published", 5.seconds)
    assertEquals(1, confirm.get("publishId").asLong())
  }
}
```

---

## Tests

| Test method | What it verifies |
|-------------|-----------------|
| `testDlxChain_nackToDeadLetterQueue` | NACK → DLX → DLQ, x-death headers |
| `testTtlExpiry_messageSkipped` | Expired messages not delivered |
| `testPoisonMessage_autoDeadLetter` | Max retries exceeded → auto-DLX |
| `testPriorityDelivery_highestFirst` | Priority ordering within batch |
| `testCredits_pauseAndResume` | Credits exhausted → paused → grant → resumed |
| `testMandatoryReturn_noRoute` | Mandatory + no route → returned frame |
| `testPublisherConfirms` | Enable confirms → published response |

**Run command:**
```bash
timeout 600 ./gradlew :http-server:test --tests 'kafka.http.WsMessageLifecycleIntegrationTest' -x spotlessCheck
```

---

## Rules

- All tests use `@Timeout(30)`.
- DLX test must verify x-death headers.
- Credits test: subscribe with low credit count, verify pause/resume.
- Mandatory return must include replyCode 312 and original message.
- Publisher confirms test must enable confirms before publish.
- Use `HttpIntegrationTestHarness` as base class.

---

## Learning

- **Executed as unit-level cross-cutting tests** rather than the broker-integration tests the original spec imagined. The executing operator's guidance pivoted the scope to the WS-T.01 cross-cutting pattern (in-memory routing engine + recording DLX sink + mocked Netty channel); this keeps the test class lightweight, fast, and decoupled from broker lifecycle. Integration coverage remains the eventual goal but is out of scope for T.05 as currently wired.
- **The DLX atomicity invariant is easy to assert with a `FailingDlxSink`**: when the sink returns a failed future, `WsDeadLetterHandler.deadLetter` propagates it, and that failed future is exactly the signal `WsAckHandler.processDlxNack` uses to keep the original tag in `NACKED_REQUEUE`. One test covers the entire contract without wiring the ack handler.
- **x-death accumulation is newest-first** (RabbitMQ convention, confirmed by reading `WsDeadLetterHandler.buildXDeathArray`). The multi-hop test both documents and verifies this: hop 1 entry lands at index 1, hop 2 entry at index 0.
- **Poison protection is conceptually header-driven, not object-driven**: no dedicated "poison handler" class exists in the current tree. The broker decides `reason="max-retries-exceeded"` based on the prior `x-death.count`; once that decision is made, the existing DLX handler carries the rest. Our test treats the poison step as "call `deadLetter` with a pre-populated x-death whose count is already at threshold and reason=`max-retries-exceeded`".
- **Priority ordering is a header sort, not an API method**: `WsMessageSerializer.HDR_PRIORITY = "_ws_priority"` is the source of truth. The fetch loop delivers in the order it receives records; the broker (or a future dispatcher) is responsible for the pre-delivery sort. Our test models that sort explicitly using `Comparator.comparingInt(...).reversed().thenComparingLong(offset)` so the tie-break (stable-by-offset) is locked down as a regression target.
- **`WsDeduplicationCache` is partitioned per exchange**, which means the DLX hop necessarily lives in its own namespace — a valuable cross-cutting property that the unit `WsDeduplicationCacheTest` does not already exercise.
- **Checkstyle's `UnusedImports` rule fires on a test-only file**: the first run of the new test file failed `:http-server:checkstyleTest` for a single unused `assertNull` import. `checkstyleTest` runs as part of the `test` task, so running `--tests` still gates on it. Keep imports tight from the start.

---

## Limitations

- **No credits-pause/resume test**: requires either a running fetch loop against live records or a substantial amount of scaffolding to fake fetches. The `WsCreditManagerTest` already covers pause/resume semantics directly; duplicating them here would add bulk without increasing coverage of cross-cutting behaviour.
- **No mandatory-return or publisher-confirms tests**: those live in `WsMandatoryReturnTest` and `WsPublisherConfirmTrackerTest` respectively. Both features are one-step per component — there is no second component they cross-cut with at the unit level.
- **Priority sort is modelled, not exercised through a real dispatcher**: the broker has no production priority dispatcher as of the current HEAD. When one is added, this test should be migrated to drive the dispatcher rather than sorting in the test.
- **No real broker wiring**: the per-component integration tests (when they come online) should re-run the same scenarios against a live broker to validate that the unit-level contracts we asserted here match the production wiring.

---

## Field Notes

- **Pivot from integration to unit**: the task file's original skeleton referenced `HttpIntegrationTestHarness` / `WsTestClient`, neither of which exist in the current worktree. Operator guidance to follow WS-T.01's pattern was the correct call — the alternative would have been to invent the harness from scratch, blowing the task scope.
- **The RoutingEngine's 3-arg constructor (no alternate-exchange fn)** is the right match for the DLX handler test: alternate-exchange semantics are orthogonal to the lifecycle scenarios here.
- **`QueueMetadata` constructor signature is `(name, vhost, durable, exclusive, autoDelete, args)`** — easy to mis-order on first pass. The helper `registerQueue` is there to keep individual tests concise and correct.
- **Gradle notices**: `:http-server:test --tests 'kafka.server.http.ws.MessageLifecycleTest' -x spotlessCheck` finishes in ~6s on this machine once the build is warm. The gating step is `checkstyleTest`, not the JUnit run.
- **Worktree isolation trap**: the worktree has its own checkout of `ivy-docs/` as a separate working copy. Editing the task file must use the worktree-prefixed absolute path (`/home/anh/kafka/.claude/worktrees/agent-a915a810/ivy-docs/...`) or the edit lands in the main checkout. First attempt on this task hit exactly that trap; the main checkout was restored and the edit reapplied inside the worktree.

---

## Acceptance Criteria

- [x] `timeout 600 ./gradlew :http-server:test --tests 'kafka.server.http.ws.MessageLifecycleTest' -x spotlessCheck` exits 0
- [x] At least 7 lifecycle test methods (delivered 12)
- [x] DLX, TTL, priority, poison, dedup all tested
- [x] Learning section filled with at least one entry

---

## File Manifest

- `http-server/src/test/java/kafka/server/http/ws/MessageLifecycleTest.java` — 12 cross-cutting lifecycle tests (DLX chain, TTL expiry, poison threshold→DLX, priority ordering x3, TTL+DLX interaction x2, dedup partitioning x2, multi-hop x-death, DLX failure atomicity). Uses in-memory `RoutingEngine`, in-memory queue metadata, a recording DLX sink, and a mocked Netty channel. No broker integration.
