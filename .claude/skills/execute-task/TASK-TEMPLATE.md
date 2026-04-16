# TASK-{PHASE}.{NUMBER}: {Title}

## Prerequisites

> List exact TASK-IDs that must complete first, with a one-line reason WHY.
> If the prerequisite produces an artifact this task needs, describe it exactly.
> "None" is valid.

- None

---

## Context

> 2–4 paragraphs explaining WHY this task exists.
> Inline the relevant design doc sections (not just a link).
> Explain the problem being solved so the executor understands without project history.
> If there's a design decision or tradeoff, explain it here.

_To be filled during generation._

---

## Specification

> Exact class/interface names with full package paths.
> Exact method signatures with Javadoc-level descriptions.
> Behavioral contracts — what must be true before/after each operation.
> Data flow — where input comes from, where output goes.

```java
// Example interface or class signature
```

---

## Implementation Details

**Module:** `<maven-module-name>`

**Files to study:**

| File | Why |
|------|-----|
| `path/to/File.java` | Pattern to follow — include snippet below |

```java
// Snippet from File.java lines XX-YY — the pattern to follow
```

**Files to create:**

| File | What it does |
|------|--------------|
| `path/to/NewFile.java` | Description |

**Files to modify:**

| File | What changes |
|------|--------------|
| `path/to/Existing.java` | Description of change |

> **CRITICAL:** Any non-obvious gotcha, edge case, or trap the executor might miss.

**Implementation order** (if sequence matters):
1. Step one
2. Step two

---

## Skeleton Code

> Mandatory for every task that creates new files.
> The executor copies this, fills in the TODOs, and has a compiling class.
> Include ALL imports, ALL constants (magic bytes, API keys, opcodes as literal values), and method stubs.

### Production class

```java
package com.ivy...;

// imports

/**
 * One-line class description.
 *
 * // Time: Created - TASK-{PHASE}.{NUMBER}
 */
public final class MyClass {

    // --- Constants ---
    private static final int MAGIC = 0x00; // describe what this is

    private final Dependency dep;

    public MyClass(Dependency dep) {
        this.dep = java.util.Objects.requireNonNull(dep, "dep");
    }

    /**
     * What this method does.
     *
     * @param param description
     * @return description
     */
    public ReturnType doSomething(ParamType param) {
        // TODO: implement
        throw new UnsupportedOperationException("Not yet implemented");
    }
}
```

### Test class

```java
package com.ivy...;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 * // Time: Created - TASK-{PHASE}.{NUMBER}
 */
class MyClassTest {

    private MyClass subject;

    @BeforeEach
    void setUp() {
        // TODO: initialize subject
    }

    @Test
    void happyPath_doesExpectedThing() {
        // Arrange
        // Act
        // Assert
    }

    @Test
    void nullInput_throwsNPE() {
        assertThrows(NullPointerException.class, () -> new MyClass(null));
    }
}
```

### Existing pattern reference

```java
// From path/to/ExistingHandler.java lines XX-YY — pattern to follow:
// (copy-paste the actual code here)
```

---

## Tests

**Test class:** `path/to/MyClassTest.java`

| Test method | What it verifies |
|-------------|-----------------|
| `happyPath_doesExpectedThing` | ... |
| `edgeCase_handledCorrectly` | ... |
| `nullInput_throwsNPE` | Constructor rejects null |

**Run command:**
```bash
timeout 300 mvn test -pl <module> -T 1C -Dtest=MyClassTest
```

---

## Rules

> Inline only the CLAUDE.md invariants that apply to THIS task.
> Do not reference sections — quote the actual rule text.

- Rule 1 (from CLAUDE.md §X): _exact rule text_
- Task-specific constraint: _reason_

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

> Every criterion must be mechanically verifiable — a command to run or a grep to check.

- [ ] `timeout 300 mvn test -pl <module> -T 1C -Dtest=MyClassTest` exits 0
- [ ] `grep -r "MyClass" <module>/src/main/java/` returns at least N hits
- [ ] No usage of `<forbidden-pattern>` in new code: `grep -r "<pattern>" <path>` returns 0
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
