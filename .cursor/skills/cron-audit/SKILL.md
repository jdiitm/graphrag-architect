---
name: cron-audit
description: Periodic full-system audit for graphrag-architect. Performs a deep, wide, context-complete audit of the entire codebase, tests, documentation, and infrastructure against the PRD and architecture specs. Produces a structured report. Trigger every 30 minutes, or when asked to audit, health-check, or assess system state.
---

# System Audit

Full-depth, full-width audit of the graphrag-architect system. Produces a single structured report covering every requirement, every file, every test, every doc.

## FSM Position

```
**AUDIT** → DOC_SYNC → [RED] → TDD → REVIEW → (FIX loop) → AUDIT → ...
                       [YELLOW/GREEN] → wait → AUDIT
```

You are in the **AUDIT** state. You are the entry point of every cycle.
Your only exit: HALT and emit `→ DOC_SYNC`. Always. The verdict is written to `audit-report.md`; `DOC_SYNC` reads it and decides what happens next.

## Model Requirement

This skill runs on **Claude Opus 4 (Thinking)**. When opening a new chat to trigger this skill, select `claude-4-opus-thinking` from the model picker.

## Isolation Protocol

This skill MUST run in a **fresh conversation** with no prior context from any other skill.
You are an independent auditor. You trust ONLY what you read from disk and what test output tells you.

If you have any memory of writing, reviewing, fixing, or syncing docs in this session, STOP — you are contaminated.
Tell the user: "This skill must run in a new conversation to maintain isolation."

## Precondition Gate

Before doing ANY work, verify these conditions:

```bash
# 1. Must be on main with clean working tree
git branch --show-current   # must be "main"
git status --porcelain      # must be empty
```

**If not on main or dirty tree:** HALT. Tell the user: "Working tree is not clean on main. Resolve before auditing."

If there are open PRs, that is fine — the audit still runs. It will note the open PR in the report.

## Schedule

This audit should run every ~30 minutes during active development. The user triggers it manually. The report replaces any prior audit report.

## Step 1: Read the Specification (Ground Truth)

Read these files in full — they define what the system SHOULD be:

```bash
# Requirements spec
docs/prd/02_SYSTEM_REQUIREMENTS.md

# Architecture spec
docs/architecture/01_SYSTEM_DESIGN.md
docs/architecture/02_DATA_DICTIONARY.md

# Coding invariants
CLAUDE.md
```

Build a mental checklist of every FR-N (FR-1 through FR-8) and every NFR-N. These are your audit criteria.

## Step 2: Read the Entire Codebase

Read every source file. Do not skip any. Do not skim.

**Python orchestrator:**

```
orchestrator/app/*.py          — every module
orchestrator/tests/test_*.py   — every test file
orchestrator/requirements.txt  — dependencies
```

**Go workers:**

```
workers/ingestion/internal/**/*.go   — every source and test file
workers/ingestion/go.mod             — dependencies
```

**Infrastructure:**

```
infrastructure/docker-compose.yml
```

**Configuration:**

```
pyproject.toml
.github/workflows/*.yml
```

For each file, note: what it implements, what it tests, what it imports, what it exports.

## Step 3: Read All Documentation

Read every documentation file — these are what the audit checks for staleness:

```
README.md
architecture_state.md
claude-progress.txt
docs/prd/01_VISION_AND_SCOPE.md
docs/prd/02_SYSTEM_REQUIREMENTS.md
docs/architecture/01_SYSTEM_DESIGN.md
docs/architecture/02_DATA_DICTIONARY.md
```

## Step 4: Run All Quality Gates

Execute and capture full raw output:

```bash
cd /home/j/side/graphrag-architect
source .venv/bin/activate

# Gate 1: Pylint
pylint orchestrator/

# Gate 2: Python tests
python -m pytest orchestrator/tests/ -v

# Gate 3: Go tests
cd /home/j/side/graphrag-architect/workers/ingestion && go test ./... -v -count=1 -timeout 30s
```

Record exact counts: `Pylint: X/10, Python: A/B passed, Go: C/D passed`.

## Step 5: Audit Dimensions

Evaluate each dimension systematically. For every finding, record severity and evidence.

### 5a. Requirement Coverage (FR-1 through FR-8)

For each FR-N, determine:

| Status | Meaning |
|--------|---------|
| **Implemented** | Code exists, tests exist, tests pass |
| **Partial** | Code exists but incomplete, or tests missing |
| **Stub** | Function/node exists but is a no-op or placeholder |
| **Not Started** | No code at all |

Check every node in the LangGraph DAG (`graph_builder.py`) — is it a real implementation or a stub?

### 5b. Test Coverage Gaps

For every public function in `orchestrator/app/*.py` and every exported function in `workers/ingestion/internal/**/*.go`:

- Is there at least one test?
- Does the test cover happy path, error path, and edge cases?
- Are mocks testing contracts (not just confirming mock calls)?

List any untested public functions.

### 5c. Integrity Violations

Search the entire codebase for:

```bash
# Inline suppressions
rg -n 'pylint.*disable|noqa|nolint' orchestrator/ workers/

# Skipped tests
rg -n 'skip|xfail|expected_failure|Skip\(' orchestrator/tests/ workers/

# Swallowed exceptions
rg -n 'except.*:.*pass$|except:' orchestrator/ workers/

# Timing workarounds
rg -n 'time\.sleep|asyncio\.sleep' orchestrator/tests/ workers/
```

Flag every match. Investigate whether each is a genuine violation.

### 5d. Documentation Accuracy

Cross-reference each doc against the code:

| Check | How |
|-------|-----|
| README project structure tree | Compare against actual `find` output |
| README test counts | Compare against actual test runner output |
| README file descriptions | Compare against actual file contents |
| architecture_state.md component status | Compare against actual implementation status |
| claude-progress.txt latest entry | Compare against actual git log and test counts |
| Architecture doc DAG diagram | Compare against actual `graph_builder.py` nodes and edges |
| Architecture doc node status table | Compare against actual implementation |

List every discrepancy.

### 5e. Code Quality

- All Python functions have complete type annotations?
- No inline comments (per CLAUDE.md)?
- Frozen dataclass config pattern followed?
- Pydantic models for all data contracts?
- Go: context propagation, channel safety, defer cleanup?

### 5f. Security

- No hardcoded secrets, API keys, or passwords in code?
- Parameterized Cypher queries only (no string interpolation)?
- Error messages don't leak internal state?
- No overly permissive file reads (path traversal)?

### 5g. Stale Code

- Any dead imports?
- Any functions defined but never called?
- Any TODO/FIXME/HACK comments?
- Any files that exist but aren't referenced?

```bash
rg -n 'TODO|FIXME|HACK|XXX' orchestrator/ workers/
```

### 5h. Dependency Health

- Are all imports in `requirements.txt` actually used?
- Are all Go imports in `go.mod` actually used?
- Any known vulnerable versions?

## Step 6: Generate Report

Produce the report in this exact format. Write it to `audit-report.md` in the repo root:

```markdown
# System Audit Report

**Generated:** <timestamp>
**Auditor:** cron-audit (automated)
**Commit:** <current HEAD sha>

## Executive Summary

- Quality Gates: Pylint X/10, Python A/B, Go C/D
- Requirement Coverage: N/8 FRs implemented, M/8 partial, P/8 not started
- Findings: X CRITICAL, Y HIGH, Z LOW

## Requirement Coverage Matrix

| FR | Status | Implementation | Tests | Notes |
|----|--------|---------------|-------|-------|
| FR-1 | ... | ... | ... | ... |
| FR-2 | ... | ... | ... | ... |
| ... | ... | ... | ... | ... |

## CRITICAL Findings

1. **[CRITICAL-001]** <file:line> — <description>

## HIGH Findings

1. **[HIGH-001]** <file:line> — <description>

## LOW Findings

1. **[LOW-001]** <file:line> — <description>

## Documentation Discrepancies

| Document | Section | Says | Reality |
|----------|---------|------|---------|
| ... | ... | ... | ... |

## Test Coverage Gaps

| Module | Function | Test Status |
|--------|----------|-------------|
| ... | ... | Missing / Partial |

## Stale Code

| File | Item | Issue |
|------|------|-------|
| ... | ... | Dead import / Unused function / TODO |

## Recommendations (Priority Order)

1. ...
2. ...
3. ...

## Verdict

**Status:** RED / YELLOW / GREEN
**Action:** Trigger `@tdd-feature-cycle` / No action needed
```

Also present the full report content in your response to the user.

## Step 7: HALT — Always Emit → DOC_SYNC

The audit always transitions to `@cron-doc-sync`. The verdict (RED/YELLOW/GREEN) is persisted in `audit-report.md`. The doc-sync skill reads it and decides whether to trigger `@tdd-feature-cycle` or stop.

**HALT. Your job is done. Do NOT continue. Do NOT trigger `@tdd-feature-cycle` directly.**

Tell the user exactly this:

> Audit complete. Report written to `audit-report.md`. **Verdict: RED / YELLOW / GREEN.**
> **Next:** Open a new chat **(model: claude-4-opus-thinking)** and trigger `@cron-doc-sync`.

Then STOP. Do not write another word or call another tool.
