---
name: tdd-feature-cycle
description: Autonomous end-to-end feature development cycle for graphrag-architect. Discovers the next missing PRD requirement, implements it with strict TDD (red-green-refactor), performs a staff-level self-review, and raises a Pull Request. Use when asked to implement the next feature, build the next component, continue development, or pick up the next task.
---

# TDD Feature Cycle

Autonomous workflow: discover the next missing feature from the PRD, implement it via strict TDD, self-review, and raise a PR. Halt after PR creation -- never merge your own PRs.

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
- Run tests again. If any fail, fix the implementation -- do not modify tests to make them pass.
- Do not proceed until all tests are green.

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

4. **HALT.** Do NOT merge your own PR. Do NOT self-review.

Tell the user: "PR #N created. Trigger `@pr-review` for independent review, or review manually."

## Conventions

| Item | Convention |
|------|-----------|
| Branch naming | `feat/<kebab-case-feature>` |
| Commit messages | Conventional commits: `feat:`, `fix:`, `refactor:`, `test:` |
| Progress log | Prepend new entries to top of `claude-progress.txt` |
| Test file naming | Python: `test_<module>.py` in `orchestrator/tests/`; Go: `<pkg>_test.go` alongside source |
| Config pattern | Frozen dataclass with `from_env()` classmethod |
| Error handling | Catch specific exceptions, never bare `except:` |
