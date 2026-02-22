---
name: pr-fix
description: Fix changes requested in a PR review for graphrag-architect. Reads review comments, checks out the feature branch, applies TDD-driven fixes, runs quality gates, and pushes. Use when a PR review requested changes, when asked to fix PR feedback, address review comments, or after pr-review requests changes.
---

# PR Fix

Address review feedback on an open PR using the same TDD discipline as feature development. After pushing fixes, the `pr-review` skill should be re-triggered for a fresh independent review.

## FSM Position

```
... → TDD → REVIEW → **FIX** → REVIEW → ┬─ [merged]  → AUDIT → ...
                        ↑          |      └─ [changes] → FIX
                        └──────────┘
```

You are in the **FIX** state. A PR has changes requested by the reviewer.
Your only exit: HALT and emit `→ REVIEW`.

## Isolation Protocol

This skill MUST run in a **fresh conversation** with no prior context from `@tdd-feature-cycle`, `@pr-review`, `@system-audit`, or `@doc-sync`.
You are Engineer 3 — a fixer. You have never seen the code being written or the review being conducted.
You trust ONLY: the review comments on the PR, the codebase on disk, and test output you run yourself.

If you have any memory of writing the original code, conducting the review, or auditing in this session, STOP — you are contaminated.
Tell the user: "This skill must run in a new conversation to maintain isolation."

## Integrity Invariants (Non-Negotiable)

When fixing review findings, these rules are absolute:

1. **Never weaken an existing test** to make a review finding "go away." If a reviewer says the code is wrong, fix the code, not the test.
2. **Never skip, disable, or mark tests as expected-failure** to bypass a failing gate.
3. **Never add inline lint suppression** (`pylint: disable`, `noqa`, `nolint`) to silence a reviewer's lint concern. Fix the code.
4. **Never add sleeps or retries** to work around a flaky test. Fix the non-determinism.
5. **Never reduce assertion specificity.** If a reviewer says an assertion is wrong, make it *more* precise, not less.
6. **Every fix must be verified.** Run the relevant test after each fix and include the raw output. Do not claim a fix works without evidence.
7. **If a fix requires a new test**, write the failing test first (red), then fix the code (green). Maintain TDD discipline even for one-line fixes.
8. **If you disagree with a review finding**, do not silently ignore it. Reply to the comment explaining your reasoning and let the reviewer decide during re-review.

## Step 1: Read Review Feedback

```bash
gh pr list --state open --limit 5
gh pr view <number> --json reviews,comments
gh api repos/{owner}/{repo}/pulls/<number>/reviews
gh api repos/{owner}/{repo}/pulls/<number>/comments
```

Parse all CRITICAL and HIGH findings. Create a TODO list from them.

## Step 2: Checkout the Branch

```bash
git fetch origin
git checkout <branch-name>
git pull origin <branch-name>
```

## Step 3: Fix Each Finding (TDD)

For each CRITICAL/HIGH finding:

1. **If the fix needs a new test**: write the failing test first, then fix the code. Never skip the red phase.
2. **If an existing test has a genuine specification error**: fix the test to match the correct spec as documented in the PRD/architecture docs, then fix the code. You must justify why the test was wrong in the commit message.
3. **If the fix is code-only** (refactor, security, type annotation): make the change, verify existing tests still pass.

**NEVER** weaken an assertion, remove a test, or reduce test coverage to make a fix "work." The tests are the specification. If the tests and the code disagree, the code is wrong unless the test contradicts the PRD.

After each fix, run the relevant test file and include raw output:

```bash
source .venv/bin/activate && python -m pytest orchestrator/tests/<file> -v
```

## Step 4: Quality Gates

After all fixes, run the full gate suite:

```bash
# Gate 1: Python lint
source .venv/bin/activate && pylint orchestrator/

# Gate 2: Python tests
python -m pytest orchestrator/tests/ -v

# Gate 3: Go tests
cd workers/ingestion && go test ./... -v -count=1 -timeout 30s
```

ALL must pass. If any fail, fix and re-run from the top.

Report: `Pylint: X/10, Python tests: A/B passed, Go tests: C/D passed`

## Step 5: Push Fixes

```bash
git add .
git commit -m "fix: address PR review feedback

- [bullet list of what was fixed, referencing review findings]"
git push
```

## Step 6: Respond to Review Comments

After pushing, reply to each review comment that raised a finding you fixed. This creates an audit trail showing what was done for each issue.

For each issue comment on the PR (from `gh pr view <number> --json comments`):

```bash
gh api repos/{owner}/{repo}/issues/<number>/comments \
  -f body="$(cat <<'EOF'
**Addressed.** [Explain what was changed and how it resolves the finding.]

- Commit: `<short-sha>`
- [Brief description of the fix]
EOF
)"
```

For each inline review comment (from `gh api repos/{owner}/{repo}/pulls/<number>/comments`):

```bash
gh api repos/{owner}/{repo}/pulls/<number>/comments/<comment_id>/replies \
  -f body="Fixed in <short-sha>. [Brief explanation of the fix.]"
```

Reply to every CRITICAL and HIGH finding. LOW findings: reply only if you addressed them.

## Step 7: Hand Off to Re-Review

Switch back to main for clean handoff:

```bash
git checkout main
```

**HALT. Your job is done. Do NOT continue.**

Do NOT self-review. Do NOT merge.

Tell the user exactly this:

> Fixes pushed to branch `<branch>` for PR #N.
> **Next:** Open a new chat and trigger `@pr-review`.

Then STOP. Do not write another word or call another tool.
