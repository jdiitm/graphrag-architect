---
name: system-audit
description: Full-system audit for graphrag-architect. Uses decomposed sub-audits with targeted file scoping, defeat-test methodology, and cross-component interaction analysis. Produces a structured report consumed by @tdd-feature-cycle. MUST run in its own fresh agent context — never in the same context as implementation.
---

# System Audit

Full-depth, full-width audit of the graphrag-architect system. Verdict must be evidence-based: GREEN when green, RED when red. No manufactured findings. Uses focused sub-audits to preserve reasoning depth.

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

These are non-negotiable:

1. **Never manufacture a finding.** If a dimension is clean, it is clean. Do not invent issues to justify your existence. An all-green audit is a valid and valuable result.
2. **Never inflate severity.** A style preference is not HIGH. A missing Phase 2 feature is not CRITICAL. Severity must match actual impact.
3. **Never dismiss on uncertainty.** If you suspect a problem but are unsure, construct a concrete defeat test (see methodology below). If the defeat test confirms the problem, report it. If the mechanism holds, dismiss it. Never dismiss without testing.
4. **Never suppress a real problem.** If tests fail, pylint scores below 10, or code contradicts the PRD, report it honestly regardless of how inconvenient it is.
5. **Never count deferred items as gaps.** Only flag a feature as missing if the PRD explicitly requires it in the current phase.
6. **The verdict must follow mechanically from the evidence.** You do not get to "feel" like something is RED. Apply the verdict criteria below.

## Defeat Test Methodology

When evaluating any security mechanism, validation function, data pipeline step, or infrastructure configuration:

1. **Identify the claim**: What does this code/config guarantee?
2. **Construct a concrete bypass/failure**: Write a specific input, scenario, or configuration state designed to break the guarantee.
3. **Trace it through the code**: Follow the constructed scenario step-by-step through the actual implementation.
4. **Verdict**: If the scenario causes the feared outcome, it is a finding. If the mechanism holds, it is not a finding.

Do NOT evaluate mechanisms by asking "does this exist?" — evaluate by asking "can this be defeated?"

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

Note which FRs exist, their scope, and their acceptance criteria. These are claims to verify.

## Step 2: Quality Gates

Execute and capture full raw output:

```bash
source .venv/bin/activate

pylint orchestrator/
python -m pytest orchestrator/tests/ -v
cd workers/ingestion && go test ./... -v -count=1 -timeout 30s
```

Record: `Pylint: X/10, Python: A/B passed, Go: C/D passed`.

**These are objective facts.** If all pass, they pass. Do not second-guess passing tests.

## Step 3: File Discovery

Enumerate all files:

```bash
find . -type f \
  ! -path './.git/*' \
  ! -path './.venv/*' \
  ! -path '*__pycache__*' \
  ! -path '*.pyc' \
  ! -path './node_modules/*' \
  ! -path './.pytest_cache/*' \
  ! -path './.cursor/skills/*' \
  ! -path './.cursor/rules/*' \
  | sort
```

Review the file list. Ensure every file is covered by at least one sub-audit below. Files not fitting any sub-audit scope are read during Sub-Audit C as a catch-all.

## Step 4: Focused Sub-Audits

Execute each sub-audit **sequentially**. For each one:
1. Read ONLY the files listed in its scope
2. Perform the analysis immediately while those files are fresh
3. Record intermediate findings before moving to the next sub-audit

**Critical rule**: Do NOT read all files upfront. Read each sub-audit's files immediately before analyzing that domain.

---

### Sub-Audit A: Security & Requirement Coverage

**Scope — read these files now:**
```
orchestrator/app/access_control.py
orchestrator/app/cypher_validator.py
orchestrator/app/main.py
orchestrator/app/manifest_parser.py
orchestrator/app/extraction_models.py
orchestrator/app/neo4j_client.py
orchestrator/app/query_engine.py
orchestrator/tests/test_access_control.py
orchestrator/tests/test_cypher_validator.py
```

**A1. Requirement Coverage:**

For each FR (FR-1 through FR-8):

| Status | Meaning | Verdict Impact |
|--------|---------|----------------|
| **Implemented** | Code exists, tests exist, tests pass | None (green) |
| **Partial** | Code exists but incomplete, or tests missing | HIGH |
| **Stub** | Function/node exists but is a no-op | RED trigger |
| **Not Started** | No code at all | RED trigger |

**A2. Security — defeat tests required:**

Trace the Cypher query path from user input to Neo4j execution. For every validation step, construct a bypass attempt:
- A nested subquery: `CALL { MATCH (n) WHERE n.name = 'x' RETURN n } RETURN n`
- A string literal containing keywords: `MATCH (n) WHERE n.desc = "DELETE WHERE RETURN" RETURN n`
- A mutation query: `CREATE (n:Evil) RETURN n`
Trace each through the validation code. Record what happens.

Trace the auth path. Construct these scenarios:
- Request with no auth token
- Request when `AUTH_TOKEN_SECRET` is empty/unset
If missing secrets cause verification to be skipped, that is a CRITICAL finding.

**A3. ACL propagation — absence detection:**

For every Pydantic node type with `team_owner` or `namespace_acl` fields, verify the parser actually populates them. Build a table:

| Node type | Field | Default | Parser sets it? | Evidence |
|-----------|-------|---------|----------------|----------|

Fields that default to None/empty and are never populated are findings.

**A4. Secrets scan:**

Search the entire repo using the Grep tool for:
1. Case-insensitive: `password`, `secret`, `api.key`, `token`, `credential`
2. Hardcoded key prefixes: `BEGIN.*KEY`, `AKIA`, `sk-`, `ghp_`, `ghs_`

Only flag confirmed hardcoded secrets, not environment variable name references.

**Record Sub-Audit A findings before proceeding.**

---

### Sub-Audit B: Data Pipeline & Infrastructure

**Scope — read these files now:**
```
workers/ingestion/internal/consumer/consumer.go
workers/ingestion/internal/dispatcher/dispatcher.go
workers/ingestion/internal/dlq/handler.go
workers/ingestion/internal/processor/forwarding.go
workers/ingestion/cmd/dlq_sink.go
workers/ingestion/cmd/kafka.go
workers/ingestion/cmd/main.go
orchestrator/app/circuit_breaker.py
orchestrator/app/graph_builder.py
orchestrator/app/workspace_loader.py
infrastructure/k8s/kafka-statefulset.yaml
infrastructure/k8s/neo4j-statefulset.yaml
infrastructure/k8s/network-policies.yaml
infrastructure/k8s/neo4j-schema-job.yaml
infrastructure/k8s/orchestrator-deployment.yaml
infrastructure/k8s/ingestion-worker-deployment.yaml
infrastructure/k8s/hpa.yaml
infrastructure/k8s/alerting.yaml
infrastructure/docker-compose.yml
.github/workflows/ci.yml
```

**B1. Message lifecycle — trace end-to-end:**

Follow a single message: Kafka poll → consumer → dispatcher → HTTP forward → Python ingest → Neo4j write → offset commit. At every step:
- If this step fails, is the message retried, DLQ'd, or lost?
- Is the offset committed before or after downstream success?

**B2. DLQ durability — defeat test required:**

Does the DLQ write to Kafka or just log? Construct a scenario where the DLQ producer itself fails. Is the original message lost?

**B3. Memory exhaustion:**

Trace `workspace_loader.py` → `load_directory()` for a large directory (5,000 files). Does it accumulate all contents in memory? A chunked function whose caller collects all chunks is NOT streaming — check the actual call site.

**B4. Go consumer blocking — defeat test required:**

Trace this scenario: batch of 10 messages, message 3 enters a 15-second retry loop. Does the consumer stop polling? Will this exceed `session.timeout.ms` and trigger a Kafka rebalance?

**B5. Kafka operational completeness:**

Verify the Kafka StatefulSet has:
- [ ] `KAFKA_ADVERTISED_LISTENERS` — must resolve from external pods (not just `:9092`)
- [ ] `KAFKA_LISTENER_SECURITY_PROTOCOL_MAP`
- [ ] KRaft cluster configuration

If `KAFKA_ADVERTISED_LISTENERS` uses `$(HOSTNAME)`, verify this is a K8s downward-API reference, not a shell variable.

**B6. NetworkPolicy cross-reference matrix:**

If a default-deny policy exists, build a complete matrix:

| Workload | Pod labels | Needs egress to | Needs ingress from | Allow policy? |
|----------|-----------|-----------------|-------------------|--------------|

For EVERY required traffic path, verify a matching NetworkPolicy rule exists. A missing allow rule blocks traffic.

**B7. CI/CD completeness:**

Cross-reference `.github/workflows/ci.yml` against CLAUDE.md quality gates. Flag any gate CLAUDE.md requires but CI does not enforce.

**Record Sub-Audit B findings before proceeding.**

---

### Sub-Audit C: Code Quality, Tests & Documentation

**Scope — read these files now:**

Read ALL `orchestrator/app/*.py` and `orchestrator/tests/test_*.py` files not already read, plus:
```
orchestrator/tests/conftest.py
workers/ingestion/internal/telemetry/telemetry.go
workers/ingestion/internal/metrics/metrics.go
workers/ingestion/internal/metrics/observer.go
pyproject.toml
README.md
architecture_state.md
claude-progress.txt
```

Also read any files from Step 3 discovery not covered by Sub-Audits A-B.

**C1. Integrity violations:**

Search using the Grep tool:
1. Lint suppression: pattern `pylint.*disable|noqa|nolint` in `orchestrator/` and `workers/`
2. Test skips: pattern `pytest\.mark\.skip|unittest\.skip|xfail|expected_failure` in `orchestrator/tests/` and `workers/`
3. Bare except: pattern `except\s*:` in `orchestrator/` and `workers/`
4. Sleeps in tests: pattern `time\.sleep|asyncio\.sleep` in `orchestrator/tests/`

Investigate every match in context:
- `skip` in a variable name or string literal is NOT a violation
- `except SomeSpecificError:` is NOT a bare except
- `sleep` in implementation code (not tests) may be legitimate

Only flag confirmed violations.

**C2. Test effectiveness:**

For every **public** function in `orchestrator/app/*.py` and every **exported** function in Go:
- Is there at least one test that exercises it? (Integration coverage counts)

For the tests themselves:
- Do assertions test behavior or just confirm mocks were called?
- Are error paths tested?
- **Critical**: Does any test validate a security anti-pattern as correct? (e.g., asserting that missing secrets should skip auth verification)
- Are tests deterministic?

**C3. Documentation accuracy:**

Compare factual claims against reality:
- Project structure trees match actual files?
- Test counts match actual test output?
- DAG node status table matches actual implementation?
- Config fields match actual code?

Only flag **material factual errors** — wrong file names, wrong counts, missing modules, incorrect status. Do NOT flag style or phrasing preferences.

**C4. Code quality (CLAUDE.md invariants):**
- Type annotations on all function signatures?
- No inline comments in Python source?
- Frozen dataclass config pattern?
- Pydantic models for data contracts?

**C5. Stale code:**

Search using the Grep tool:
- Pattern `TODO|FIXME|HACK|XXX` in `orchestrator/` and `workers/`

Flag only confirmed dead code or stale markers.

**Record Sub-Audit C findings before proceeding.**

---

## Step 5: Generate Report

Merge all sub-audit findings. Write to `audit-report.md`. **Only include sections that have findings.** Omit empty sections entirely.

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

**If YELLOW or RED:** Add only sections with findings:

```markdown
## Findings

### CRITICAL (if any)

1. **[CRITICAL-001]** `file:line` — <description with evidence and defeat test result>

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

**Report rules:**
- Every CRITICAL or HIGH finding MUST include a defeat test showing the concrete bypass/failure scenario.
- Every finding must have `file:line` and evidence. No vague findings.
- Do NOT include a Requirement Coverage row for FRs that are Implemented — only gaps.
- Do NOT include empty sections.
- Do NOT pad the report. Shorter is better. Actionable is everything.

## Step 6: HALT

**HALT. Your job is done. Do NOT continue.**

If running as a **direct user trigger**, tell the user:

> Audit complete. Report written to `audit-report.md`. **Verdict: RED / YELLOW / GREEN.**
> **Next:** Open a **new chat** and trigger `@tdd-feature-cycle`.

If running as a **subagent** (launched by TDD Phase 0), return a final message summarizing:

> Audit complete. `audit-report.md` written. Verdict: RED / YELLOW / GREEN.

Then STOP. Do not write another word or call another tool.
