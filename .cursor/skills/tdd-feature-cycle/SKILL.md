---
name: tdd-feature-cycle
description: Autonomous end-to-end feature development cycle for graphrag-architect. Discovers the next missing PRD requirement, implements it with strict TDD (red-green-refactor), performs a staff-level self-review, and raises a Pull Request. Use when asked to implement the next feature, build the next component, continue development, or pick up the next task.
---

# TDD Feature Cycle

Autonomous workflow: discover the next missing feature from the PRD, implement it via strict TDD, self-review, and raise a PR. Halt after PR creation -- never merge your own PRs.

## FSM Position

```
AUDIT → DOC_SYNC → [RED] → **TDD** → REVIEW → (FIX loop) → AUDIT → ...
                   [YELLOW/GREEN] → wait → AUDIT
```

You are in the **TDD** state. You were triggered because the last audit verdict was RED.
Your only exit: HALT and emit `→ REVIEW`.

## Model Requirement

This skill runs on **Claude Opus 4 (Thinking)**. When opening a new chat to trigger this skill, select `claude-4-opus-thinking` from the model picker.

## Isolation Protocol

This skill MUST run in a **fresh conversation** with no prior context from `@pr-review`, `@pr-fix`, `@cron-audit`, or `@cron-doc-sync`.
You are Engineer 1. You have never seen the review feedback, fix commits, or audit reports from any other skill.

If you have any memory of reviewing, fixing, or auditing code in this session, STOP — you are contaminated.
Tell the user: "This skill must run in a new conversation to maintain isolation."

## Precondition Gate

Before doing ANY work, verify these conditions. If any fail, HALT immediately.

```bash
# 1. Must be on main
git branch --show-current  # must output "main"

# 2. No open PRs (would mean a review/fix cycle is in progress)
gh pr list --state open --limit 1  # must be empty

# 3. Audit verdict must be RED (this skill only runs when there's work to do)
cat audit-report.md  # read the Verdict section
```

**If not on main:** HALT. Tell the user: "Not on main branch. Resolve the current branch first."
**If open PRs exist:** HALT. Tell the user: "Open PR detected. Complete the review/fix cycle first via `@pr-review`."
**If audit-report.md is missing or verdict is not RED:** HALT. Tell the user: "Audit verdict is not RED. No high-priority work to do. Run `@cron-audit` to re-assess."

## Integrity Invariants (Non-Negotiable)

These rules are absolute. Violating any of them is a **showstopper** — stop, undo, and fix.

1. **Never weaken a test to make it pass.** If a test fails, fix the implementation. The only reason to modify a test is if the test itself has a genuine specification error — and you must explain why in the commit message.
2. **Never skip, disable, or ignore a test.** No `pytest.mark.skip`, `@unittest.skip`, `t.Skip()`, `xfail`, `expected_failure`, or equivalent. No commenting out test cases.
3. **Never add `pylint: disable`, `noqa`, `nolint`, or any inline suppression** to bypass a lint rule. Fix the code or update `pyproject.toml` if the rule is genuinely inapplicable project-wide.
4. **Never add sleeps, retries, or timing workarounds** to mask flaky behavior. Tests must be deterministic. If a test is flaky, find and fix the root cause.
5. **Never claim tests passed without raw output.** Every test run must include the verbatim terminal output in your response. If you did not run tests, say "I did not run tests."
6. **Never fabricate, truncate, or paraphrase test output.** The user must see the real result.
7. **Never reduce assertion specificity.** Do not replace exact equality checks with weaker containment checks, existence checks, or `assertTrue(True)` to make a test pass.
8. **Documentation and tests are first-class artifacts.** They are never "nice to have." PRD/architecture docs define the specification. Tests verify the specification. Code implements the specification. This order of priority is non-negotiable.
9. **Never swallow exceptions silently.** No bare `except:`, no `except Exception: pass`. Every error path must be explicit and tested.
10. **Never merge with failing tests.** Zero tolerance. The full suite must be green before any push.

## Phase 1: Discovery & Branching

1. Read these files in parallel:
   - `docs/prd/02_SYSTEM_REQUIREMENTS.md`
   - `docs/architecture/01_SYSTEM_DESIGN.md`
   - `claude-progress.txt`
2. Cross-reference requirements (FR-1 through FR-8) against implemented code to identify the **highest-priority missing feature**.
3. Explore the codebase to confirm what exists vs. what is stubbed.
4. Formulate a concrete plan: new files, modified files, test plan, Cypher/query templates if applicable.
5. Create a branch:

```bash
git checkout main && git pull origin main
git checkout -b feat/<feature-name>
```

6. Log the plan in `claude-progress.txt` before writing any code.

## Phase 2: Strict TDD (Red-Green-Refactor)

### Red: Write Failing Tests First

- Create the test file (e.g., `orchestrator/tests/test_<module>.py` or `workers/ingestion/internal/<pkg>/<pkg>_test.go`).
- Write tests covering: happy path, error path, edge cases, integration with the LangGraph DAG if applicable.
- Run the tests to **prove they fail**:

```bash
# Python
source .venv/bin/activate && python -m pytest orchestrator/tests/test_<module>.py -v

# Go
cd workers/ingestion && go test ./... -v -count=1 -timeout 30s
```

- Do NOT write implementation code during this step.

### Green: Minimal Implementation

- Write the minimal code to make all tests pass.
- Run tests again. If any fail, **fix the implementation, not the tests**. The tests encode the specification. The code must conform to them.
- Do not proceed until all tests are green.
- Include the full raw test output in your response.

### Refactor: Staff-Level Self-Review

Audit your code against these enterprise standards before committing:

**Python checklist:**
- All functions have complete type annotations
- Pydantic models used for data contracts
- Async functions use `async with` for resource management
- No inline comments (per CLAUDE.md -- self-documenting names only)
- Pylint 10/10: `pylint orchestrator/`

**Go checklist:**
- `context.Context` propagated to all external calls
- Channels properly closed; no goroutine leaks
- `sync.WaitGroup` used for graceful shutdown
- `defer` for cleanup

**Cross-cutting:**
- No secrets or credentials in code
- Error handling is explicit (no swallowed exceptions)
- Existing tests still pass (zero regressions)

Fix any issues found, then re-run tests to confirm green.

## Phase 3: Quality Gates (Mandatory Before Push)

All three gates must pass. Run sequentially:

```bash
# Gate 1: Python lint
source .venv/bin/activate && pylint orchestrator/

# Gate 2: Python tests
python -m pytest orchestrator/tests/ -v

# Gate 3: Go tests
cd workers/ingestion && go test ./... -v -count=1 -timeout 30s
```

If ANY gate fails: fix, then re-run ALL gates from the top.

Report results as: `Pylint: X/10, Python tests: A/B passed, Go tests: C/D passed`

## Phase 4: Git-Ops & PR

1. Update `claude-progress.txt` with completed work, test counts, architecture decisions, and next steps.
2. Stage, commit, push:

```bash
git add .
git commit -m "feat: implement <feature-name> with TDD"
git push -u origin feat/<feature-name>
```

3. Create PR with `gh pr create`, linking back to the specific FR-N requirement:

```bash
gh pr create \
  --title "feat: <feature-name>" \
  --body "$(cat <<'EOF'
## Summary
Implementation of FR-N from docs/prd/02_SYSTEM_REQUIREMENTS.md.

### Changes
- [bullet list of new/modified files]

### Design Decisions
- [key architectural choices]

## Test Plan
- [x] Tests written first (TDD red phase)
- [x] All new tests passing
- [x] All existing tests passing (zero regressions)
- [x] Pylint 10/10
EOF
)"
```

4. **HALT. Your job is done. Do NOT continue.**

Do NOT merge your own PR. Do NOT review your own PR. Do NOT trigger any other skill.

Tell the user exactly this:

> PR #N created on branch `<branch>`.
> **Next:** Open a new chat **(model: claude-4-opus-thinking)** and trigger `@pr-review`.

Then STOP. Do not write another word or call another tool.

## Conventions

| Item | Convention |
|------|-----------|
| Branch naming | `feat/<kebab-case-feature>` |
| Commit messages | Conventional commits: `feat:`, `fix:`, `refactor:`, `test:` |
| Progress log | Prepend new entries to top of `claude-progress.txt` |
| Test file naming | Python: `test_<module>.py` in `orchestrator/tests/`; Go: `<pkg>_test.go` alongside source |
| Config pattern | Frozen dataclass with `from_env()` classmethod |
| Error handling | Catch specific exceptions, never bare `except:` |
