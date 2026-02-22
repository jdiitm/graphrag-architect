---
name: pr-fix
description: Fix changes requested in a PR review for graphrag-architect. Reads review comments, checks out the feature branch, applies TDD-driven fixes, runs quality gates, and pushes. Use when a PR review requested changes, when asked to fix PR feedback, address review comments, or after pr-review requests changes.
---

# PR Fix

Address review feedback on an open PR using the same TDD discipline as feature development. After pushing fixes, the `pr-review` skill should be re-triggered for a fresh independent review.

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

1. **If the fix needs a new test**: write the failing test first, then fix the code.
2. **If an existing test is wrong**: fix the test to match the correct spec, then fix the code.
3. **If the fix is code-only** (refactor, security, type annotation): make the change, verify existing tests still pass.

After each fix, run the relevant test file to confirm green:

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

After pushing and responding to comments, tell the user:

"Fixes pushed to `<branch>`. Trigger `@pr-review` for a fresh independent re-review."

Do NOT self-review. Do NOT merge. The `pr-review` skill handles the merge decision.
