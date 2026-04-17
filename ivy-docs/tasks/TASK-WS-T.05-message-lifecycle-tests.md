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

_To be filled by the executing agent._

---

## Limitations

_To be filled by the executing agent._

---

## Field Notes

_To be filled by the executing agent._

---

## Acceptance Criteria

- [ ] `timeout 600 ./gradlew :http-server:test --tests 'kafka.http.WsMessageLifecycleIntegrationTest' -x spotlessCheck` exits 0
- [ ] At least 7 lifecycle test methods
- [ ] DLX, TTL, priority, credits, mandatory, confirms all tested
- [ ] Learning section filled with at least one entry

---

## File Manifest

_To be filled by the executing agent._
