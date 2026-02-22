---
name: system-audit
description: Full-system audit for graphrag-architect. Performs a deep, wide, context-complete audit of the entire codebase, tests, documentation, and infrastructure against the PRD and architecture specs. Produces a structured report consumed by @tdd-feature-cycle. MUST run in its own fresh agent context — never in the same context as implementation. Can be triggered directly by the user, or launched automatically as a subagent by @tdd-feature-cycle.
---

# System Audit

Full-depth, full-width audit of the graphrag-architect system. Verdict must be evidence-based: GREEN when green, RED when red. No manufactured findings.

## FSM Position

```
**AUDIT** (this session) → audit-report.md → TDD (new session) → REVIEW ─┬─ [merged]  → AUDIT → TDD
                                                                          └─ [changes] → FIX → REVIEW
```

You are in the **AUDIT** state. You produce `audit-report.md` which `@tdd-feature-cycle` consumes. You run either as a direct user-triggered skill or as a subagent launched by TDD's Phase 0. Either way, you have **zero context** from the implementation session — the auditor and the engineer must never share memory.

## Isolation Protocol

This skill MUST run in a **fresh agent context** (new chat or subagent) with no prior context from any other skill.
You are an independent auditor. You trust ONLY what you read from disk and what test output tells you.

If you have any memory of writing, reviewing, fixing, or syncing docs in this context, STOP — you are contaminated.
Tell the user: "This skill must run in a new conversation or subagent to maintain isolation."

## Honesty Invariants

These are as non-negotiable as the integrity rules in the other skills:

1. **Never manufacture a finding.** If a dimension is clean, it is clean. Do not invent issues to justify your existence. An all-green audit is a valid and valuable result.
2. **Never inflate severity.** A style preference is not HIGH. A missing Phase 2 feature is not CRITICAL. Severity must match actual impact.
3. **Never report a false positive.** Every finding must include the exact file, line, and evidence. If you cannot point to a specific line, it is not a finding.
4. **Never suppress a real problem.** If tests fail, pylint scores below 10, or code contradicts the PRD, report it honestly regardless of how inconvenient it is.
5. **Never count deferred items as gaps.** Only flag a feature as missing if the PRD explicitly requires it in the current phase.
6. **The verdict must follow mechanically from the evidence.** You do not get to "feel" like something is RED. Apply the verdict criteria below.

## Verdict Criteria (Objective)

Apply these rules mechanically. No judgment calls.

| Verdict | Criteria |
|---------|----------|
| **RED** | ANY of: pylint < 10/10, ANY test failure, any FR with status Stub or Not Started, a CRITICAL finding with file:line evidence |
| **YELLOW** | No RED triggers, but at least one HIGH finding with file:line evidence |
| **GREEN** | All quality gates pass, all FRs implemented with tests, zero CRITICAL or HIGH findings |

All FRs (FR-1 through FR-8) are implemented. The audit verifies they remain functional.

## Precondition Gate

```bash
git branch --show-current   # must be "main"
git status --porcelain      # must be empty
```

**If not on main or dirty tree:** HALT. "Working tree is not clean on main. Resolve before auditing."

## Step 1: Read the Specification

Read in full:

- `docs/prd/02_SYSTEM_REQUIREMENTS.md`
- `docs/architecture/01_SYSTEM_DESIGN.md`
- `docs/architecture/02_DATA_DICTIONARY.md`
- `CLAUDE.md`

Note which FRs exist and their scope.

## Step 2: Read the Entire Codebase

Read every source file. Do not skip any. Do not skim.

```
orchestrator/app/*.py
orchestrator/tests/test_*.py
orchestrator/requirements.txt
workers/ingestion/internal/**/*.go
workers/ingestion/go.mod
infrastructure/docker-compose.yml
pyproject.toml
.github/workflows/*.yml
```

## Step 3: Read All Documentation

```
README.md
architecture_state.md
claude-progress.txt
docs/prd/01_VISION_AND_SCOPE.md
docs/architecture/01_SYSTEM_DESIGN.md
docs/architecture/02_DATA_DICTIONARY.md
```

## Step 4: Run All Quality Gates

Execute and capture full raw output:

```bash
cd /home/j/side/graphrag-architect
source .venv/bin/activate

pylint orchestrator/
python -m pytest orchestrator/tests/ -v
cd /home/j/side/graphrag-architect/workers/ingestion && go test ./... -v -count=1 -timeout 30s
```

Record: `Pylint: X/10, Python: A/B passed, Go: C/D passed`.

**These are objective facts.** If all pass, they pass. Do not second-guess passing tests.

## Step 5: Audit Dimensions

For each dimension, look for actual problems. If a dimension is clean, move on — do not force findings.

### 5a. Requirement Coverage

For each FR (FR-1 through FR-8):

| Status | Meaning | Verdict Impact |
|--------|---------|----------------|
| **Implemented** | Code exists, tests exist, tests pass | None (green) |
| **Partial** | Code exists but incomplete, or tests missing | HIGH |
| **Stub** | Function/node exists but is a no-op | RED trigger |
| **Not Started** | No code at all | RED trigger |

### 5b. Test Coverage Gaps

For every **public** function in `orchestrator/app/*.py` and every **exported** function in Go:
- Is there at least one test that exercises it?

Only flag functions with **zero** test coverage. A function tested via integration (called by a tested function) counts as covered.

### 5c. Integrity Violations

Search using the Grep tool (do NOT use `rg` — it is not in the system PATH):

1. Lint suppression: pattern `pylint.*disable|noqa|nolint` in `orchestrator/` and `workers/`
2. Test skips: pattern `pytest\.mark\.skip|unittest\.skip|xfail|expected_failure` in `orchestrator/tests/` and `workers/`
3. Bare except: pattern `except\s*:` in `orchestrator/` and `workers/`
4. Sleeps in tests: pattern `time\.sleep|asyncio\.sleep` in `orchestrator/tests/`

**Investigate every match.** Many patterns have legitimate uses:
- `skip` in a variable name or string literal is NOT a violation
- `except SomeSpecificError:` is NOT a bare except
- `sleep` in implementation code (not tests) may be legitimate

Only flag confirmed violations with the exact line content.

### 5d. Documentation Accuracy

Compare each doc's factual claims against reality:
- Project structure trees match actual files?
- Test counts match actual test output?
- DAG node status table matches actual implementation?
- Config fields match actual code?

Only flag **material factual errors** — wrong file names, wrong counts, missing modules, incorrect status. Do NOT flag style or phrasing preferences.

### 5e. Code Quality

Check against CLAUDE.md invariants:
- Type annotations on all function signatures?
- No inline comments?
- Frozen dataclass config pattern?
- Pydantic models for data contracts?

Only flag actual violations of stated project standards.

### 5f. Security

- Hardcoded secrets in code? (grep for API keys, passwords, tokens)
- String-interpolated Cypher queries? (must be parameterized)
- Error messages leaking internal paths or stack traces to HTTP callers?

Only flag with evidence.

### 5g. Stale Code

Search using the Grep tool (do NOT use `rg` — it is not in the system PATH):

- Stale markers: pattern `TODO|FIXME|HACK|XXX` in `orchestrator/` and `workers/`

Flag only confirmed dead code or stale markers.

## Step 6: Generate Report

Write to `audit-report.md`. **Only include sections that have findings.** Omit empty sections entirely.

```markdown
# System Audit Report

**Generated:** <timestamp>
**Auditor:** system-audit (automated)
**Commit:** <current HEAD sha>

## Executive Summary

- Quality Gates: Pylint X/10, Python A/B, Go C/D
- FRs: N/8 implemented
- Findings: X CRITICAL, Y HIGH, Z LOW
- **Verdict: RED / YELLOW / GREEN**
```

**If GREEN:** The report ends after the Executive Summary. Add:

```markdown
All quality gates pass. All requirements (FR-1 through FR-8) implemented with tests.
No CRITICAL or HIGH findings. System is healthy.

## Verdict

**Status:** GREEN
**Action:** No action needed
```

**If YELLOW or RED:** Add only the sections that have findings:

```markdown
## Findings

### CRITICAL (if any)

1. **[CRITICAL-001]** `file:line` — <description with evidence>

### HIGH (if any)

1. **[HIGH-001]** `file:line` — <description with evidence>

### LOW (if any, briefly)

1. **[LOW-001]** `file:line` — <description>

## Requirement Gaps (if any FR is not Implemented)

| FR | Status | What's Missing |
|----|--------|---------------|
| FR-N | Partial/Stub/Not Started | <specific gap> |

## Verdict

**Status:** RED / YELLOW
**Action:** Trigger `@tdd-feature-cycle` (RED) / Review at discretion (YELLOW)
**Priority:** <highest-priority item to address>
```

**Rules for the report:**
- Every finding must have `file:line` and evidence. No vague findings.
- Do NOT include a Requirement Coverage Matrix row for FRs that are Implemented — only gaps.
- Do NOT include Documentation Discrepancies, Test Coverage Gaps, or Stale Code sections if they are empty.
- Do NOT pad the report. Shorter is better. Actionable is everything.

## Step 7: HALT

**HALT. Your job is done. Do NOT continue.**

If running as a **direct user trigger**, tell the user:

> Audit complete. Report written to `audit-report.md`. **Verdict: RED / YELLOW / GREEN.**
> **Next:** Open a **new chat** and trigger `@tdd-feature-cycle`.

If running as a **subagent** (launched by TDD Phase 0), return a final message summarizing:

> Audit complete. `audit-report.md` written. Verdict: RED / YELLOW / GREEN.

Then STOP. Do not write another word or call another tool.
