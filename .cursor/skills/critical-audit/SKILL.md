---
name: critical-audit
description: Deep, adversarial, external-quality audit of the entire graphrag-architect repository. Uses decomposed sub-audits with targeted file scoping, concrete defeat-test methodology, and cross-component interaction analysis. Produces a structured report modeled on a professional penetration-test / architecture-review engagement.
---

# Critical Audit — Deep Technical System Review

You are an external auditor engaged to perform a full-depth, adversarial review of the graphrag-architect system. You have never seen this codebase before. You have no relationship with the engineers who wrote it. Your reputation depends on finding real problems and not manufacturing fake ones.

## Isolation Protocol

This skill MUST run in a **fresh conversation** with zero prior context from any other skill or session.
You are an independent third-party auditor. You trust ONLY what you read from disk and what test/lint output tells you.

If you have any memory of writing, reviewing, fixing, or auditing code in this session, STOP — you are contaminated.
Tell the user: "This skill must run in a new conversation to maintain auditor independence."

## Honesty Invariants

These are non-negotiable. Violating any of them invalidates the entire audit.

1. **Never manufacture a finding.** If something is correct, it is correct. A clean system is a valid result. Do not invent issues to justify your engagement.
2. **Never inflate severity.** A style preference is not HIGH. A missing Phase 2 feature is not CRITICAL. A theoretical concern without a concrete exploit path is not a Blocker. Severity must match actual, demonstrable impact.
3. **Never dismiss on uncertainty.** If you suspect a vulnerability but are unsure, you MUST construct a concrete defeat test (see methodology below). If the defeat test shows the mechanism can be bypassed, report it. If the defeat test shows the mechanism holds, dismiss it. Never dismiss without testing.
4. **Never suppress a real problem.** If tests fail, code has injection vectors, infrastructure is misconfigured, or documentation contradicts reality — report it honestly. No favoritism.
5. **Never count deferred/planned items as gaps.** Only flag something as missing if the PRD explicitly requires it in the current phase AND the code is absent or stubbed.
6. **The verdict must follow mechanically from the evidence.** You do not get to "feel" like something is problematic. Every finding must have a concrete, reproducible risk.
7. **Never double-count.** If a single root cause manifests in multiple places, it is ONE finding with multiple locations, not N findings.

## Defeat Test Methodology

This is the core analytical technique for this audit. Apply it to every security mechanism, validation function, and infrastructure configuration you evaluate.

1. **Identify the claim**: What does this code/config claim to protect against or guarantee?
2. **Construct a concrete bypass**: Write a specific input, query, request, configuration state, or failure scenario designed to defeat the mechanism. Be creative and adversarial.
3. **Trace the bypass through the code**: Follow the constructed input step-by-step through the actual implementation. At every branch and decision point, record what the code does with your crafted input.
4. **Verdict**: If the bypass reaches the protected resource or causes the feared outcome, it is a finding. If the mechanism correctly blocks it at every attempt, it is not a finding.

Do NOT evaluate mechanisms by asking "does this exist?" — evaluate by asking "can this be defeated?"

## Verdict Criteria (Mechanical)

| Verdict | Criteria |
|---------|----------|
| **CRITICAL FAILING GRADE** | ANY Blocker-level finding with file:line evidence AND a concrete exploit/failure path |
| **SIGNIFICANT CONCERNS** | No Blockers, but multiple HIGH findings with file:line evidence |
| **MINOR CONCERNS** | No Blockers, no HIGH. Only moderate or low findings |
| **CLEAN** | All quality gates pass, no findings above informational |

## Precondition Gate

```bash
git branch --show-current
git status --porcelain
git log --oneline -1
```

Record the current branch, commit SHA, and whether the tree is clean. Proceed regardless of branch — you audit whatever state the code is in.

---

## Phase 1: Specification Intake

Read the entire specification stack. These define what the system SHOULD be.

```
CLAUDE.md
docs/prd/01_VISION_AND_SCOPE.md
docs/prd/02_SYSTEM_REQUIREMENTS.md
docs/architecture/01_SYSTEM_DESIGN.md
docs/architecture/02_DATA_DICTIONARY.md
```

Extract and internalize:
- Every FR (functional requirement) and its acceptance criteria
- Every stated architectural invariant (from CLAUDE.md)
- The intended data flow (Kafka → Go worker → Python orchestrator → Neo4j)
- The graph schema (node types, edge types, required properties)
- Security model (RBAC, tenant isolation, read-only query paths)
- Stated deployment topology (K8s, Docker, namespaces)

Do NOT take these documents at face value. They are claims to be verified against reality.

---

## Phase 2: Quality Gates

Run all gates and record raw output verbatim:

```bash
source .venv/bin/activate

# Gate 1: Pylint
pylint orchestrator/

# Gate 2: Python tests
python -m pytest orchestrator/tests/ -v

# Gate 3: Go tests
cd workers/ingestion && go test ./... -v -count=1 -timeout 30s
```

Record: `Pylint: X/10, Python: A/B passed, Go: C/D passed`

If any gate FAILS, that is an automatic Blocker finding. Record the exact failure output.

---

## Phase 3: File Discovery

Enumerate all files to ensure no sub-audit misses a component:

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

Review the file list. If you see files not covered by any sub-audit scope below, read them during Sub-Audit E (Code Quality) as a catch-all.

---

## Phase 4: Focused Sub-Audits

Execute each sub-audit **sequentially**. For each one:
1. Read ONLY the files listed in its scope
2. Perform the specified analysis immediately while those files are fresh
3. Record intermediate findings in a scratchpad before moving to the next sub-audit

**Critical rule**: Do NOT read all files upfront. Read each sub-audit's files immediately before analyzing that domain. This preserves reasoning capacity for deep analysis.

---

### Sub-Audit A: Security & Access Control

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
infrastructure/k8s/secrets.yaml
```

**A1. Cypher injection — defeat tests required:**

Trace the full path: user natural-language query → LLM → generated Cypher → validation → Neo4j execution.

For every validation/sanitization step in the chain, construct ALL of these specific bypass attempts and trace each through the code:
- A Cypher query with a nested subquery: `CALL { MATCH (n) WHERE n.name = 'x' RETURN n } RETURN n`
- A Cypher query with a CASE expression: `MATCH (n) RETURN CASE WHEN n.name = 'test' THEN 1 ELSE 0 END`
- A Cypher query with a string literal containing keywords: `MATCH (n) WHERE n.desc = "DELETE all WHERE RETURN" RETURN n`
- A Cypher query with mutation verbs: `CREATE (n:Evil {name: 'injected'}) RETURN n`
- A multi-statement query: `MATCH (n) RETURN n; DROP CONSTRAINT unique_service_name`

For each, trace character-by-character through the validation. Record what happens.

Verify ALL query-path Neo4j transactions use `execute_read`. Search for any `execute_write`, `session.write_transaction`, or `session.run` calls in query paths (not ingestion paths).

**A2. RBAC bypass — defeat tests required:**

Trace: HTTP request → header extraction → SecurityPrincipal → Cypher ACL filter injection.

Construct these specific scenarios:
- Request with no Authorization header at all
- Request with a syntactically valid but unsigned JWT
- Request when `AUTH_TOKEN_SECRET` environment variable is empty string, None, or not set at all
- Request from a non-admin tenant attempting to access another tenant's data

For each, trace the full code path. Does the request succeed, fail safely, or fail open?

**Important**: If any test in the test suite asserts that missing/empty secrets should cause verification to be SKIPPED, that test is validating a fail-open vulnerability. Flag both the code path AND the test as a finding.

**A3. ACL propagation — absence detection:**

For every node type in `extraction_models.py` that has `team_owner` or `namespace_acl` fields:
1. List the field name and its default value
2. Search the parser/extractor code for where that field is explicitly populated
3. If a field defaults to None/empty and no parser code ever sets it to a real value, that is a finding — the field exists but is never populated

Build a table:

| Node type | Field | Default | Parser sets it? | Evidence |
|-----------|-------|---------|----------------|----------|

**A4. Secrets scan:**

Search the entire repo (all `*.py`, `*.go`, `*.yaml`, `*.yml` files) using the Grep tool for:
1. Case-insensitive: `password`, `secret`, `api.key`, `token`, `credential`
2. Hardcoded key prefixes: `BEGIN.*KEY`, `AKIA`, `sk-`, `ghp_`, `ghs_`

Investigate every match in context. Only flag confirmed hardcoded secrets, not references to environment variable names.

**Record Sub-Audit A findings before proceeding.**

---

### Sub-Audit B: Data Pipeline Integrity

**Scope — read these files now:**
```
workers/ingestion/internal/consumer/consumer.go
workers/ingestion/internal/dispatcher/dispatcher.go
workers/ingestion/internal/dlq/handler.go
workers/ingestion/internal/processor/forwarding.go
workers/ingestion/internal/processor/processor.go
workers/ingestion/internal/domain/job.go
workers/ingestion/cmd/dlq_sink.go
workers/ingestion/cmd/kafka.go
workers/ingestion/cmd/main.go
orchestrator/app/main.py (re-read — focus on /ingest endpoint)
orchestrator/app/graph_builder.py
orchestrator/app/neo4j_client.py (re-read — focus on write transactions)
orchestrator/app/circuit_breaker.py
```

**B1. Message loss — trace the full lifecycle:**

Follow a single message through the entire pipeline: Kafka poll → consumer dispatch → HTTP forward to Python → Python ingest → Neo4j write → offset commit.

At EVERY step, answer these three questions:
- If this step fails, is the message retried, DLQ'd, or lost forever?
- Is the Kafka offset committed BEFORE or AFTER the downstream operation confirms success?
- What specific HTTP status codes or error types trigger each behavior (retry vs DLQ vs drop)?

Draw the state machine:
```
Message → [consumer] → [dispatcher] → [processor/forwarding] → [HTTP to Python] → [graph_builder] → [neo4j_client] → [commit offset]
                                                                     ↓ (on error)
                                                              [DLQ handler]
```

For every arrow, document the failure mode.

**B2. DLQ durability — defeat test required:**

- Read `dlq/handler.go` and `cmd/dlq_sink.go`. Does the DLQ actually write to a Kafka topic, or does it only log?
- Construct this scenario: the DLQ Kafka producer itself fails (e.g., broker unreachable). Trace what happens to the original message. Is it lost?
- Does the DLQ record preserve enough metadata for replay? Check for: original topic, partition, offset, error reason, timestamp, original payload.

**B3. Transaction atomicity:**

- In `neo4j_client.py`, trace the write path. Are all entities written in a single transaction, or can a partial set be committed?
- Is there a transaction timeout configured on the Neo4j driver? What happens on timeout?
- Check for MERGE vs CREATE. Can a retried ingest create duplicate nodes?

**B4. Circuit breaker interaction — defeat test required:**

- When the circuit breaker in `circuit_breaker.py` opens, what HTTP status code does the Python `/ingest` endpoint return?
- Read the Go `forwarding.go` — what does it do with that specific status code? Retry? DLQ? Drop?
- Construct a scenario: circuit breaker opens after 5 consecutive Neo4j failures. The next 10 ingest requests arrive. Trace each one through both Python and Go. How many messages are lost vs DLQ'd vs retried?

**Record Sub-Audit B findings before proceeding.**

---

### Sub-Audit C: Infrastructure Correctness

**Scope — read these files now:**
```
infrastructure/k8s/kafka-statefulset.yaml
infrastructure/k8s/neo4j-statefulset.yaml
infrastructure/k8s/orchestrator-deployment.yaml
infrastructure/k8s/ingestion-worker-deployment.yaml
infrastructure/k8s/neo4j-schema-job.yaml
infrastructure/k8s/network-policies.yaml
infrastructure/k8s/hpa.yaml
infrastructure/k8s/alerting.yaml
infrastructure/k8s/configmap.yaml
infrastructure/k8s/secrets.yaml
infrastructure/k8s/namespace.yaml
infrastructure/k8s/ingress.yaml
infrastructure/docker-compose.yml
.github/workflows/ci.yml
```

**C1. Kafka operational completeness:**

Verify the Kafka StatefulSet has ALL required environment variables for KRaft mode:
- [ ] `KAFKA_ADVERTISED_LISTENERS` — MUST resolve to a hostname reachable from other pods. If it uses `$(HOSTNAME)`, verify this is a Kubernetes downward-API env var reference, NOT a shell variable (shell variables do not expand in K8s `env:` blocks).
- [ ] `KAFKA_LISTENER_SECURITY_PROTOCOL_MAP`
- [ ] `KAFKA_CONTROLLER_QUORUM_VOTERS` or KRaft equivalent
- [ ] `CLUSTER_ID` or KRaft cluster identification

For each missing or incorrectly-configured variable, explain the concrete runtime failure it causes.

**C2. NetworkPolicy cross-reference matrix:**

First, determine whether a default-deny policy exists (a NetworkPolicy selecting all pods with empty ingress/egress rules, or a policy with `policyTypes: [Ingress, Egress]` and no rules).

If default-deny exists, build this complete matrix. Read the label selectors from every Deployment, StatefulSet, and Job manifest:

| Workload | Pod labels | Needs egress to (service:port) | Needs ingress from (service) | Allow policy exists? | Policy name |
|----------|-----------|-------------------------------|-----------------------------|--------------------|-------------|

For EVERY cell marked "needs", verify a specific NetworkPolicy rule matches:
- The `podSelector` matches the workload's labels
- The `ingress.from` or `egress.to` selector matches the peer's labels
- The port number matches

A missing allow rule when default-deny is active means traffic is BLOCKED. This is a Blocker finding if it affects a required data path.

**C3. Probes and ports:**
- For every workload, verify liveness/readiness probe ports match container port declarations
- Verify probe HTTP paths (e.g., `/health`) actually exist as endpoints in the application code

**C4. HPA correctness:**
- Verify every metric name referenced in HPA manifests is actually exported by the target workload
- Check metric types: `Pods` type metrics must be per-pod averages, not cluster-wide totals
- Verify `targetAverageValue` or `averageUtilization` thresholds are reasonable

**C5. Alerting rule consistency:**
- Verify every PromQL metric name in alerting rules is actually exported by the application or infrastructure
- Verify label selectors in alerting rules match the labels that Prometheus scraping actually produces
- Check for alerting blind spots: are there critical failure modes (DLQ overflow, Neo4j connection failure, circuit breaker open) that have no alerting rule?

**C6. CI/CD completeness:**
- Read `.github/workflows/ci.yml`
- Cross-reference against CLAUDE.md quality gates. Every gate in CLAUDE.md must appear in CI.
- Flag any gate that CLAUDE.md requires but CI does not enforce.

**Record Sub-Audit C findings before proceeding.**

---

### Sub-Audit D: Performance & Scalability

**Scope — read these files now:**
```
orchestrator/app/workspace_loader.py
orchestrator/app/graph_builder.py
orchestrator/app/llm_extraction.py
orchestrator/app/ingest_models.py
workers/ingestion/internal/consumer/consumer.go
workers/ingestion/internal/dispatcher/dispatcher.go
```

**D1. Memory exhaustion — trace the data volume:**

In `workspace_loader.py`, trace `load_directory()` for a directory with 5,000 files (each up to 1MB):
- Does the function accumulate all file contents in a single in-memory list?
- Is that list passed into `IngestionState` and carried through the entire LangGraph DAG?
- Calculate worst-case memory: if 5,000 files * 500KB average = 2.5GB, does this exceed pod limits?
- A function named `load_directory_chunked()` that yields chunks is NOT streaming if the caller collects all chunks into a single list. Check the actual call site.

**D2. Blocking calls in async context:**
- Search for synchronous blocking calls inside `async def` functions
- Are LLM API calls (likely slow, CPU-bound for response parsing) executed with `asyncio.to_thread` or `loop.run_in_executor`?
- Are file I/O operations in async handlers using async file libraries or blocking the event loop?

**D3. Go consumer throughput — defeat test required:**

In `consumer.go`, trace this specific scenario:
1. Consumer polls a batch of 10 messages
2. Messages 1-2 are dispatched and ack'd quickly (100ms each)
3. Message 3 enters a retry loop in the processor — each retry takes 5 seconds, max 3 retries = 15 seconds
4. Messages 4-10 are waiting in the channel

Answer:
- Does the consumer block polling during step 3? For how long?
- What is `session.timeout.ms`? (Check the Kafka config in `cmd/kafka.go` or `cmd/main.go`)
- If the consumer stops polling for >session.timeout.ms, Kafka triggers a rebalance. Does this scenario cause a rebalance?
- What happens to messages 4-10 during the rebalance?

**D4. Unbounded collections:**
- In Python: search for `append()` or `extend()` calls inside loops with no size cap
- In Go: search for sends to unbuffered or large-buffered channels without backpressure

**Record Sub-Audit D findings before proceeding.**

---

### Sub-Audit E: Code Quality & Test Integrity

**Scope — read these files now:**

Read ALL `orchestrator/app/*.py` source files not already read in previous sub-audits, plus:
```
orchestrator/tests/conftest.py
orchestrator/tests/test_*.py (all test files)
workers/ingestion/internal/telemetry/telemetry.go
workers/ingestion/internal/metrics/metrics.go
workers/ingestion/internal/metrics/observer.go
pyproject.toml
```

Also read any files from the Phase 3 discovery that were not covered by Sub-Audits A-D.

**E1. CLAUDE.md invariant compliance:**

Verify each invariant across the entire codebase:
- Type annotations on ALL function signatures (Python)
- No inline comments in Python source (self-documenting names only)
- Frozen dataclass config pattern with `from_env()` classmethod
- Pydantic models for data contracts
- Idiomatic Go (context propagation, channel safety, defer cleanup)
- No bare `except:` handlers
- No swallowed exceptions

Search for violations using the Grep tool:
1. Bare except handlers: pattern `except\s*:` in `orchestrator/`
2. Swallowed exceptions: pattern `except\s+\w+` in `orchestrator/` — read surrounding lines to check if the exception is logged/re-raised or silently consumed with `pass`
3. Lint suppression: pattern `pylint.*disable|noqa|nolint` in `orchestrator/` and `workers/`
4. Stale markers: pattern `TODO|FIXME|HACK|XXX` in `orchestrator/` and `workers/`
5. Test skips: pattern `pytest\.mark\.skip|unittest\.skip|xfail|expected_failure` in `orchestrator/tests/` and `workers/`
6. Sleeps in tests: pattern `time\.sleep|asyncio\.sleep` in `orchestrator/tests/`

Investigate every match in context. Many patterns have legitimate uses — only flag confirmed violations.

**E2. Test effectiveness audit:**

For every public function/method in source code, verify at least one test exercises it. Then examine the tests themselves:
- Are assertions testing actual behavior or just confirming mocks were called with specific args?
- Are error paths tested, not just happy paths?
- **Critical**: Does any test validate a security anti-pattern as correct? For example, a test asserting "when AUTH_TOKEN_SECRET is missing, token verification should be skipped" is validating fail-open behavior. Flag these — the test is the evidence, not the defense.
- Are tests deterministic (no timing dependencies, no ordering assumptions, no shared mutable state)?
- Do integration tests actually test component interactions, or do they mock everything and test in isolation?

**E3. Architectural drift:**

Compare factual claims in architecture documents against actual code:
- Does every component described in `01_SYSTEM_DESIGN.md` exist in code?
- Does every file in the repo appear in the documented project structure?
- Does the DAG topology in `graph_builder.py` match the documented DAG?
- Do the `IngestionState` fields match what the docs claim?
- Do node/edge types in `extraction_models.py` match `02_DATA_DICTIONARY.md`?
- Are all documented properties present in the Pydantic models?

**E4. Observability completeness:**
- Are Prometheus metrics exported from both Go and Python?
- Are metric names consistent between code and alerting rules?
- Is distributed tracing (OpenTelemetry) propagated end-to-end (Go → Python → Neo4j)?
- Are trace context headers forwarded in HTTP calls?
- Are there blind spots where failures occur silently (no metric, no log, no trace)?

**Record Sub-Audit E findings before proceeding.**

---

## Phase 5: Report Generation

Merge all sub-audit findings into a single report. Write to `critical-audit-report.md`:

```markdown
# GraphRAG Architect — Deep Technical System Audit Report

**Auditor:** critical-audit (independent, automated)
**Date:** <timestamp>
**Commit:** <HEAD sha>
**Branch:** <current branch>

## Executive Verdict: <VERDICT>

<2-3 sentence summary of overall system health>

Quality Gates: Pylint X/10, Python A/B, Go C/D

---
```

Then, ONLY if there are findings, organize them by severity tier:

```markdown
## 1. Critical Security & Data Loss Vectors (Blockers)

### 1.N <Finding Title>

**Location:** `file/path.ext` → `function_name` (line N)
**Issue:** <Precise description of what is wrong>
**Defeat test:**
    <The specific input/scenario you constructed and what happened when you traced it through the code>
**Evidence:**
    ```
    <verbatim code snippet from the file>
    ```
**Risk:** <Concrete, specific impact — not vague hand-waving>
**Remediation:** <Specific, actionable fix>

---

## 2. Infrastructure & Operational Risks (Blockers)

### 2.N ...

---

## 3. High Severity / Performance & Resilience

### 3.N ...

---

## 4. Minor Code Smells & Documentation Drift

### 4.N ...
```

**Report rules:**
- OMIT any severity tier that has zero findings. Do not include empty sections.
- Every finding MUST have: Location (file:line), Issue, Defeat test or Evidence (verbatim snippet), Risk, Remediation.
- Blocker and HIGH findings MUST include a defeat test showing the concrete bypass/failure scenario.
- If the system is clean, the report ends after the Executive Verdict with: "No findings above informational level. System is healthy."
- Findings are numbered per-section: 1.1, 1.2, 2.1, 3.1, 3.2, etc.
- Do NOT pad the report. Do NOT include background, methodology, or scope narrative. Findings only.
- The report must be self-contained. A reader with no context should be able to understand and act on every finding.

---

## Phase 6: HALT

**HALT. Your job is done. Do NOT continue.**

Tell the user exactly this:

> Critical audit complete. Report written to `critical-audit-report.md`.
> **Verdict: <VERDICT>**
> **Findings:** X Blocker, Y High, Z Minor
>
> To act on findings, open a new chat and trigger `@tdd-feature-cycle`.

Then STOP. Do not write another word or call another tool.
