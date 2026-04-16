---
name: generate-tasks
description: Generate self-sufficient task files from high-level requirements. Each task is a complete work package — an outsourced freelancer with zero project context can execute it. Reads design docs, existing code, and CLAUDE.md invariants to produce TASK-*.md files in ivy-docs/tasks/.
trigger: /generate-tasks
---

# Task Generation

## Overview

Generate task files that a developer with **zero project knowledge** can execute autonomously. Think outsourcing: every task is a sealed envelope containing everything needed — context, exact file paths, code snippets, contracts to satisfy, test plans, and acceptance criteria. The executor should never need to read other tasks, design docs, or wander the codebase to understand what to do.

## When to Use

- User says "generate tasks for X", "create tasks from this design doc", "break this into tasks"
- User provides a high-level goal, feature request, or design document and wants executable task files
- User points to a design doc in `docs/` and wants it decomposed into tasks

## Inputs

The user provides one or more of:
1. **A high-level goal** — e.g., "implement ACL enforcement for MQTT"
2. **A design document** — e.g., `docs/ACL-DESIGN.md`
3. **A list of changes** — e.g., "add TenantId to all storage queries"
4. **A phase code** — which phase these tasks belong to (see Phase Ordering below)

If the user does not specify a phase, ask them.

## Phase Ordering

Tasks are ordered by phase letter then number within phase:

```
0 → A → B → C → D → E → T → CG → L → MT → M1 → M2 → R → F
```

File naming: `TASK-{PHASE}.{NUMBER}-{slug}.md` (e.g., `TASK-MT.03-tenant-cache-isolation.md`)

## The Self-Sufficiency Principle

**The cardinal rule: every task file must be executable by someone who has never seen this project.**

This means each task MUST contain:

### 1. Inlined Context (not just references)
- Do NOT write "see ACL-DESIGN.md for details" — instead, extract and inline the specific sections relevant to this task
- Do NOT write "follows the same pattern as KafkaHandler" — instead, show the actual pattern with a code snippet
- Design doc references are allowed as **supplementary reading**, never as the primary source of truth

### 2. Concrete Code Contracts
- Exact interface/class signatures the implementation must satisfy
- Exact method signatures with parameter types and return types
- Example input → output pairs where applicable
- If the task creates a new class that other tasks will use, define the full public API inline

### 3. Relevant Invariants (Cherry-Picked)
- Do NOT write "see CLAUDE.md § Multi-Tenancy" — instead, inline the specific rules that apply:
  > "Every SQL query must include `tenant_id` in its WHERE clause. Use both `SET LOCAL ivy.tenant_id` (RLS) AND `WHERE tenant_id = ?` — defense-in-depth."
- Only include invariants that the executor will actually encounter. A codec task doesn't need storage rules.

### 4. Existing Code Snapshots
- For files the task modifies, include the **relevant excerpt** (not the whole file) so the executor knows what they're working with
- For patterns to follow, include a **concrete example** from the codebase (copy-paste the actual code, trimmed to the relevant part)

### 5. Dependency Boundaries
- If this task depends on another task's output, describe **exactly what that output looks like** (class name, method signatures, package) — don't say "uses the result of TASK-A.02"
- If another task depends on this task's output, specify the **exact public API contract** this task must produce

## Step-by-Step Generation Process

### Step 1 — Understand the Scope

Read the input (design doc, goal, or change list) completely. Identify:
- Which **modules** are affected (use `ls` / `find` to verify paths exist)
- Which **existing files** will be modified or studied
- Which **new files** will be created
- What the **dependency chain** is between the pieces of work

### Step 2 — Decompose into Tasks

Split the work into tasks where each task:
- Has a **single clear outcome** (one module, one feature slice, one migration)
- Is **testable in isolation** (can run `mvn test -pl <module>` and get a pass/fail)
- Takes **2-8 hours** for a competent developer (not 30 minutes, not 3 days)
- Has **minimal overlap** with other tasks (no two tasks modify the same file if avoidable)

When overlap is unavoidable, make one task depend on the other — never parallel-modify the same file.

### Step 3 — Research Each Task

For EACH task, before writing the file:

1. **Read the existing files** that will be modified — extract relevant excerpts
2. **Find the pattern** to follow — grep for similar implementations, copy a concrete example
3. **Identify the exact invariants** from CLAUDE.md that apply to this task
4. **Verify file paths** — every path in the task file must exist (or be clearly marked as "to be created")
5. **Draft the test plan** — specific test class, specific test methods, specific assertions

### Step 4 — Write Task Files

Write each task file to `ivy-docs/tasks/TASK-{PHASE}.{NUMBER}-{slug}.md` using the template from `.claude/skills/execute-task/TASK-TEMPLATE.md`.

**Section-by-section guidance:**

#### Prerequisites
- List exact TASK-IDs that must complete first, with a one-line reason WHY
- If the prerequisite produces an artifact this task needs, describe the artifact
- "None" is valid for tasks that can start immediately

#### Context
- 2-4 paragraphs explaining WHY this task exists
- Inline the relevant design doc sections (not just a link)
- Explain the problem being solved in terms the executor can understand without project history
- If there's a design decision or tradeoff, explain it here so the executor understands the constraints

#### Specification
- **Exact class/interface names** with full package paths
- **Exact method signatures** with Javadoc-level descriptions
- **Behavioral contracts** — what must be true before/after each operation
- **Data flow** — where input comes from, where output goes
- Use code blocks for signatures:
  ```java
  public sealed interface AclDecision permits Allowed, Denied {
      boolean isAllowed();
      String reason();
  }
  ```

#### Implementation Details
- **Module:** exact Maven module name (verified it exists)
- **Files to study:** with path AND why — what pattern to learn from each file
  - Include a **code snippet** of the pattern (5-20 lines, the relevant part)
- **Files to create/modify:** with path AND what changes
- **CRITICAL/GOTCHA/EDGE CASE** callouts for non-obvious things the executor might miss
- **Step-by-step implementation order** if the sequence matters

#### Skeleton Code

**This section is mandatory for every task that creates new files.** The executor should be able to copy this skeleton, fill in the TODOs, and have a compiling class. This is the most important section for self-sufficiency — it eliminates guesswork about structure, imports, naming, and patterns.

**What to include:**

1. **Production class skeleton** — full compilable shell:
   - Correct `package` statement (verify the package exists or will be created)
   - ALL import statements the executor will need (don't make them guess)
   - Class declaration with correct modifiers (`sealed`, `final`, `record`, etc.)
   - All constants — magic bytes, API keys, version numbers, opcodes, error codes, wire format values as literal hex/int/byte values:
     ```java
     private static final int MAGIC = 0x4B614651;           // "KafQ" in ASCII
     private static final short API_KEY_PRODUCE = 0;
     private static final short API_KEY_FETCH = 1;
     private static final byte FRAME_TYPE_REQUEST = 0x01;
     private static final int MAX_MESSAGE_SIZE = 1_048_576;  // 1 MB
     private static final int HEADER_SIZE = 4 + 2 + 2 + 4;  // length + apiKey + version + correlationId
     ```
   - Constructor with validation (using the project's `Validation` utility)
   - Method stubs with full signatures, Javadoc, parameter names, return types — body is `throw new UnsupportedOperationException("Not yet implemented")`
   - `// TODO:` comments inside method bodies explaining what the implementation must do

2. **Test class skeleton** — matching test shell:
   - Correct package and imports (JUnit 5, assertion imports, test utilities)
   - Test methods with descriptive names matching the Tests section table
   - Arrange/Act/Assert structure with TODO comments
   - At least one happy-path and one negative/edge-case test

3. **Existing pattern to follow** — copy-paste the ACTUAL code (5-30 lines) from an existing file that demonstrates the pattern. Include file path and line numbers. This is not pseudocode — it's real code from the repo that the executor uses as a reference.

**Why all the constants and magic numbers inline?**

The executor has no context. If they need to write a Kafka protocol handler, they need to know that `API_KEY_PRODUCE = 0`, `HEADER_SIZE = 14 bytes`, and the magic byte is `0x4B614651`. If they're writing an MQTT codec, they need `CONNECT = 0x10`, `CONNACK = 0x20`, `PUBLISH = 0x30`. If it's an AMQP frame, they need `FRAME_METHOD = 1`, `FRAME_HEADER = 2`, `FRAME_BODY = 3`, `FRAME_END = 0xCE`.

Don't make the executor grep for these — put them in the skeleton.

**Example of a good skeleton:**

```java
package com.ivy.server.handler.mqtt;

import com.ivy.broker.acl.AclAction;
import com.ivy.broker.acl.AclResource;
import com.ivy.broker.acl.AclService;
import com.ivy.common.types.SecurityContext;
import com.ivy.common.types.TenantId;
import com.ivy.common.types.TopicName;
import com.ivy.codec.mqtt.MqttPacketType;
import io.netty.channel.ChannelHandlerContext;

/**
 * Enforces ACL checks on MQTT PUBLISH and SUBSCRIBE packets.
 * Rejects unauthorized operations with MQTT 5.0 reason code 0x87 (Not Authorized).
 *
 * // Time: Created - TASK-MT.05
 */
public final class MqttAclHandler {

    // --- MQTT 5.0 Reason Codes (spec §3.4.2.1) ---
    private static final byte REASON_SUCCESS = 0x00;
    private static final byte REASON_NOT_AUTHORIZED = (byte) 0x87;
    private static final byte REASON_TOPIC_INVALID = (byte) 0x90;

    // --- MQTT Packet Type Nibbles (spec §2.1.2) ---
    private static final byte PUBLISH  = 0x30;  // 0011 0000
    private static final byte SUBSCRIBE = (byte) 0x82;  // 1000 0010

    private final AclService aclService;

    public MqttAclHandler(AclService aclService) {
        this.aclService = java.util.Objects.requireNonNull(aclService, "aclService");
    }

    /**
     * Check ACL for a publish operation. Called before message is written to storage.
     *
     * @param ctx       the security context of the authenticated MQTT client
     * @param topicName the topic the client is publishing to
     * @return REASON_SUCCESS (0x00) if allowed, REASON_NOT_AUTHORIZED (0x87) if denied
     */
    public byte checkPublish(SecurityContext ctx, TopicName topicName) {
        // TODO: build AclResource from topicName
        // TODO: call aclService.check(ctx, AclAction.WRITE, resource)
        // TODO: return REASON_SUCCESS or REASON_NOT_AUTHORIZED
        throw new UnsupportedOperationException("Not yet implemented");
    }

    /**
     * Check ACL for a subscribe operation. Called before subscription is registered.
     *
     * @param ctx         the security context of the authenticated MQTT client
     * @param topicFilter the topic filter the client is subscribing to (may contain wildcards: +, #)
     * @return REASON_SUCCESS (0x00) if allowed, REASON_NOT_AUTHORIZED (0x87) if denied
     */
    public byte checkSubscribe(SecurityContext ctx, TopicName topicFilter) {
        // TODO: build AclResource from topicFilter
        // TODO: call aclService.check(ctx, AclAction.READ, resource)
        // TODO: return REASON_SUCCESS or REASON_NOT_AUTHORIZED
        throw new UnsupportedOperationException("Not yet implemented");
    }
}
```

```java
package com.ivy.server.handler.mqtt;

import com.ivy.broker.acl.AclDecision;
import com.ivy.broker.acl.AclService;
import com.ivy.common.types.SecurityContext;
import com.ivy.common.types.TenantId;
import com.ivy.common.types.TopicName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * // Time: Created - TASK-MT.05
 */
class MqttAclHandlerTest {

    private MqttAclHandler handler;

    @BeforeEach
    void setUp() {
        // TODO: create mock or stub AclService
        // TODO: handler = new MqttAclHandler(aclService);
    }

    @Test
    void publishAllowed_returnsSuccess() {
        // TODO: configure aclService to allow WRITE on "sensor/temp"
        // byte result = handler.checkPublish(ctx, TopicName.of("sensor/temp"));
        // assertEquals(0x00, result);
    }

    @Test
    void publishDenied_returnsNotAuthorized() {
        // TODO: configure aclService to deny WRITE on "admin/config"
        // byte result = handler.checkPublish(ctx, TopicName.of("admin/config"));
        // assertEquals((byte) 0x87, result);
    }

    @Test
    void subscribeDenied_returnsNotAuthorized() {
        // TODO: configure aclService to deny READ on "secret/#"
        // byte result = handler.checkSubscribe(ctx, TopicName.of("secret/#"));
        // assertEquals((byte) 0x87, result);
    }

    @Test
    void nullAclService_throwsNPE() {
        assertThrows(NullPointerException.class, () -> new MqttAclHandler(null));
    }
}
```

```java
// From ivy-server/.../KafkaProduceHandler.java lines 112-125 — existing ACL pattern to follow:
var resource = AclResource.topic(securityContext.tenantId(), topicName);
var decision = aclService.check(securityContext, AclAction.WRITE, resource);
if (!decision.isAllowed()) {
    log.warn("ACL denied: tenant={} action=WRITE topic={} reason={}",
             securityContext.tenantId(), topicName, decision.reason());
    return new ProduceResponse(ErrorCode.TOPIC_AUTHORIZATION_FAILED, decision.reason());
}
// ... proceed with write
```

#### Tests
- **Test class path** — exact location
- **Test method table** — method name + what it verifies
- **Test data** — example inputs and expected outputs
- **Run command** — always with `timeout 300` and `-pl <module>` and `-T 1C`
- **Negative tests** — what should fail and how

#### Rules
- Cherry-pick only the CLAUDE.md invariants that apply to THIS task
- **Inline the rule text** — don't just reference the section
- Add task-specific constraints (e.g., "this class must be thread-safe because X")

#### Learning / Limitations / Field Notes
- Always leave as `_To be filled by the executing agent._`
- Never pre-fill these — they capture what actually happened, not what you predicted

#### Acceptance Criteria
- Every criterion must be **mechanically verifiable** (a command you can run, a grep you can check)
- Include the build/test command with exact module and timeout
- Include "no remaining references to X" checks where applicable
- Include "Learning section filled" and "Limitations section filled" — always

#### File Manifest
- Leave empty with the comment template — filled during execution

### Step 5 — Present the Task Graph

After generating all task files, show the user:

```
## Generated Tasks

### Dependency Graph
TASK-A.01 (independent)
TASK-A.02 (independent)
TASK-A.03 → depends on TASK-A.01
TASK-A.04 → depends on TASK-A.02, TASK-A.03

### Parallel Batches
Batch 1: TASK-A.01, TASK-A.02 (independent — different modules)
Batch 2: TASK-A.03 (depends on A.01)
Batch 3: TASK-A.04 (depends on A.02, A.03)

### Files Created
- ivy-docs/tasks/TASK-A.01-slug.md
- ivy-docs/tasks/TASK-A.02-slug.md
- ...
```

## Quality Checklist

Before finalizing each task file, verify:

- [ ] **Could a stranger execute this?** Read the task pretending you know nothing about the project. Are there gaps?
- [ ] **Are all file paths real?** Every path mentioned exists in the repo (or is clearly marked "to be created")
- [ ] **Are code snippets included?** Not just "follows the pattern in X.java" — the actual pattern is shown
- [ ] **Are invariants inlined?** Not "see CLAUDE.md §X" — the actual rule text is quoted
- [ ] **Is the test plan concrete?** Specific test class, specific methods, specific assertions — not "add appropriate tests"
- [ ] **Are dependencies explicit?** If task B needs task A's output, task B describes exactly what that output looks like
- [ ] **Is the scope right?** Not too small (trivial), not too large (multi-day). 2-8 hours of focused work.
- [ ] **Is the acceptance criteria mechanically verifiable?** Commands to run, greps to check — not subjective judgments

## Anti-Patterns — Do NOT Generate These

### The Lazy Reference
```markdown
## Context
See docs/ACL-DESIGN.md for the full design.
```
**Fix:** Inline the 2-4 paragraphs from the design doc that are relevant to THIS task.

### The Vague Specification
```markdown
## Specification
Implement ACL enforcement for MQTT handlers following the existing pattern.
```
**Fix:** Show the exact pattern with a code snippet. Name the exact classes, methods, signatures.

### The Missing Pattern
```markdown
## Implementation Details
**Files to study:**
- `ivy-server/.../KafkaProduceHandler.java` — study how it enforces ACLs
```
**Fix:** Include the 10-20 line excerpt showing the ACL enforcement pattern:
```java
// From KafkaProduceHandler.java lines 45-58 — ACL enforcement pattern:
var decision = aclService.check(securityContext, AclAction.WRITE, resource);
if (!decision.isAllowed()) {
    return ProtocolResponse.error(ErrorCode.AUTHORIZATION_FAILED, decision.reason());
}
```

### The Invisible Dependency
```markdown
## Prerequisites
- TASK-A.01 must be complete
```
**Fix:** Explain what TASK-A.01 produces that this task needs:
```markdown
## Prerequisites
- TASK-A.01 must be complete — it creates `AclService` interface at
  `ivy-broker/src/main/java/com/ivy/broker/acl/AclService.java` with method
  `AclDecision check(SecurityContext ctx, AclAction action, AclResource resource)`
  that this task calls from the MQTT handler.
```

### The Generic Acceptance
```markdown
## Acceptance Criteria
- [ ] Implementation is correct
- [ ] Tests pass
```
**Fix:** Be specific:
```markdown
## Acceptance Criteria
- [ ] `MqttAclEnforcementTest` passes all 6 test methods
- [ ] `timeout 300 mvn test -pl ivy-server -T 1C -Dtest=MqttAclEnforcementTest` exits 0
- [ ] `grep -r "AclService" ivy-server/src/main/java/.../mqtt/` returns at least 2 hits
- [ ] No handler in `mqtt/` package accepts a publish/subscribe without ACL check
```

## Red Flags — STOP and Fix

- A task's Context section is less than 2 sentences → too thin, the executor won't understand WHY
- A task's Specification has no code blocks → too vague, the executor will guess wrong
- A task's Implementation Details has no code snippets from existing files → the executor will invent a pattern instead of following the existing one
- Two tasks modify the same file without a dependency between them → merge conflict guaranteed
- A task has no negative test cases → only the happy path is tested
- Acceptance criteria use subjective language ("implementation is clean", "code is well-structured") → not verifiable
