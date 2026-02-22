---
name: cron-doc-sync
description: Periodic documentation truth-sync for graphrag-architect. Reads the entire repository (every source file, test, config) and updates all mutable documentation to reflect the actual state of the code. Code is truth — docs must match. Trigger every 30 minutes, or when asked to sync docs, update docs, fix stale docs, or reconcile documentation.
---

# Documentation Truth Sync

Read the entire repo. Compare every mutable doc against reality. Update docs to match what the code actually does. Code is the single source of truth — documentation is its reflection.

## FSM Position

```
AUDIT → **DOC_SYNC** → [RED] → TDD → REVIEW → (FIX loop) → AUDIT → ...
                       [YELLOW/GREEN] → wait → AUDIT
```

You are in the **DOC_SYNC** state. You always run immediately after `@cron-audit`.
Your exits depend on the audit verdict in `audit-report.md`:
- RED → HALT and emit `→ TDD`
- YELLOW/GREEN → HALT and emit `→ wait for next @cron-audit`

## Model Requirement

This skill runs on **Claude Opus 4.6 Thinking**. When opening a new chat to trigger this skill, select `claude-4.6-opus-thinking` from the model picker.

## Isolation Protocol

This skill MUST run in a **fresh conversation** with no prior context from any other skill.
You are an independent technical writer. You trust ONLY what you read from disk and what test output tells you.

If you have any memory of auditing, writing, reviewing, or fixing code in this session, STOP — you are contaminated.
Tell the user: "This skill must run in a new conversation to maintain isolation."

## Precondition Gate

Before doing ANY work:

```bash
# 1. Must be on main with clean working tree
git branch --show-current   # must be "main"
git status --porcelain      # must be empty

# 2. audit-report.md must exist (this skill always runs after @cron-audit)
cat audit-report.md | head -5
```

**If not on main or dirty tree:** HALT. Tell the user: "Working tree is not clean on main."
**If audit-report.md is missing:** HALT. Tell the user: "No audit report found. Run `@cron-audit` first."

Read the `## Verdict` section of `audit-report.md` and note the status (RED/YELLOW/GREEN). You will use this in Step 8.

## Principle: Code is Truth

- The PRD (`docs/prd/`) defines what the system SHOULD do — do NOT modify PRD files.
- The architecture spec (`docs/architecture/`) defines the target design — update it ONLY when implementation has materially diverged (new components, removed components, changed interfaces).
- All other docs (`README.md`, `architecture_state.md`, `claude-progress.txt`) MUST reflect reality.

## Step 1: Read the Entire Codebase

Read every file. No exceptions, no skimming.

**Source files:**

```
orchestrator/app/*.py
orchestrator/tests/test_*.py
orchestrator/requirements.txt
workers/ingestion/internal/**/*.go
workers/ingestion/go.mod
infrastructure/docker-compose.yml
pyproject.toml
.github/workflows/*.yml
.cursor/skills/*/SKILL.md
```

For each source file, extract:
- Module purpose (from structure and naming)
- Public functions/classes and their signatures
- What it imports and exports
- Test count per test file

**Get actual counts:**

```bash
cd /home/j/side/graphrag-architect
source .venv/bin/activate

# Python test count
python -m pytest orchestrator/tests/ -v --co -q 2>/dev/null | tail -1

# Go test count
cd /home/j/side/graphrag-architect/workers/ingestion && go test ./... -v -count=1 -timeout 30s 2>&1 | grep -c '^--- PASS'

# File tree
cd /home/j/side/graphrag-architect && find . -type f \
  ! -path './.git/*' \
  ! -path './.venv/*' \
  ! -path '*__pycache__*' \
  ! -path '*.pyc' \
  | sort
```

## Step 2: Read All Mutable Documentation

Read each of these files in full:

```
README.md
architecture_state.md
claude-progress.txt
docs/architecture/01_SYSTEM_DESIGN.md
docs/architecture/02_DATA_DICTIONARY.md
```

## Step 3: Identify Discrepancies

Build a comparison table for each mutable doc. Check every factual claim against what you read in Step 1.

### README.md checks

| Check | Source of Truth |
|-------|----------------|
| Project structure tree | Actual `find` output |
| File descriptions | Actual file contents |
| Test counts in "Run tests" section | Actual pytest/go test output |
| Test file list | Actual `orchestrator/tests/` contents |
| Tech stack table | Actual imports in requirements.txt and go.mod |
| Graph schema section | Actual `extraction_models.py` fields |
| Quickstart instructions | Actual `requirements.txt`, actual env vars in `config.py` |
| Prerequisites | Actual toolchain requirements |

### architecture_state.md checks

| Check | Source of Truth |
|-------|----------------|
| Component map (ASCII diagram) | Actual modules and their connections |
| Component descriptions | Actual code in each module |
| Data flow description | Actual `graph_builder.py` DAG and `main.py` endpoints |
| Graph schema (nodes/edges) | Actual `extraction_models.py` |
| File listings per component | Actual files on disk |

### claude-progress.txt checks

| Check | Source of Truth |
|-------|----------------|
| Latest entry's "Completed" section | Actual files and git log |
| Test counts | Actual test runner output |
| "Unresolved" items | Actual codebase (are they still unresolved?) |
| "Next Step" suggestions | Still relevant given current state? |
| Quality gate numbers | Actual pylint/pytest/go output |

### docs/architecture/01_SYSTEM_DESIGN.md checks

| Check | Source of Truth |
|-------|----------------|
| File tree listings per component | Actual files on disk |
| DAG node status table (Implemented/Stub) | Actual `graph_builder.py` and referenced modules |
| IngestionState fields | Actual TypedDict in `graph_builder.py` |
| Config fields and defaults | Actual `config.py` |
| Test counts per component | Actual test output |

## Step 4: Apply Updates

For each discrepancy found, update the doc to match reality. Follow these rules:

1. **Be surgical.** Change only the specific lines that are wrong. Do not rewrite entire sections unless the structure itself is broken.
2. **Preserve doc style.** Match the existing formatting, heading levels, table style, and tone.
3. **Never inflate.** If something isn't implemented, say so. Never describe aspirational state as current.
4. **Never deflate.** If something IS implemented, make sure the doc reflects it.
5. **Update counts.** Test counts, file counts, and line counts must be exact.
6. **Update trees.** Project structure trees must match `find` output exactly.
7. **Do NOT modify PRD files** (`docs/prd/*`). These are the spec, not the implementation record.

### Specific update targets

**README.md:**
- Project structure tree
- Test counts in quickstart section
- File descriptions if files were added/removed/renamed
- Graph schema if models changed

**architecture_state.md:**
- Component descriptions if new modules were added
- ASCII diagram if data flow changed
- File listings if files were added/removed

**claude-progress.txt:**
- Do NOT rewrite history. Only update the topmost entry's "Unresolved" section if items have been resolved since it was written.
- If the topmost entry is stale (refers to work 2+ PRs ago), add a brief sync entry at the top:

```markdown
## <date>: Documentation Sync

### State of the System
- FR-N: <status for each>
- Quality Gates: Pylint X/10, Python A/B, Go C/D
- Total tests: N
- Open PRs: <list or "none">

### Unresolved
- <current list of unimplemented items>

### Next Step
- <most impactful next task>
```

**docs/architecture/01_SYSTEM_DESIGN.md:**
- DAG node status table (Implemented/Stub column)
- File tree listings per component
- IngestionState fields if they changed
- Only update if there's a material factual error (not style preferences)

## Step 5: Run Quality Gates

After making changes, verify nothing is broken:

```bash
cd /home/j/side/graphrag-architect
source .venv/bin/activate
pylint orchestrator/
python -m pytest orchestrator/tests/ -v
cd /home/j/side/graphrag-architect/workers/ingestion && go test ./... -v -count=1 -timeout 30s
```

## Step 6: Commit

If any docs were changed:

```bash
cd /home/j/side/graphrag-architect
git add README.md architecture_state.md claude-progress.txt docs/architecture/
git commit -m "$(cat <<'EOF'
docs: sync documentation with codebase reality

Automated truth-sync by cron-doc-sync skill.
- [list specific changes: updated test counts, fixed project tree, etc.]
EOF
)"
git push
```

If no docs needed changes, skip the commit and report that docs are in sync.

## Step 7: Report

Present a summary to the user:

```markdown
## Documentation Sync Report

**Timestamp:** <now>
**Commit:** <HEAD sha>

### Changes Made
- <file>: <what changed and why>
- ...

### Already Accurate (No Changes)
- <file>: verified, no discrepancies

### Skipped (Immutable)
- docs/prd/*: specification files, not modified
```

## Step 8: HALT — Gate Based on Audit Verdict

Read the `## Verdict` section from `audit-report.md` (you noted this in the Precondition Gate).

### If audit verdict = RED:

**HALT. Your job is done. Do NOT continue.**

Tell the user exactly this:

> Documentation sync complete. N files updated, M files already accurate.
> Audit verdict is **RED** — there is high-priority work to do.
> **Next:** Open a new chat **(model: claude-4.6-opus-thinking)** and trigger `@tdd-feature-cycle`.

Then STOP. Do not write another word or call another tool.

### If audit verdict = YELLOW or GREEN:

**HALT. Your job is done. Do NOT continue.**

Tell the user exactly this:

> Documentation sync complete. N files updated, M files already accurate.
> Audit verdict is **YELLOW/GREEN** — no high-priority work. System is healthy.
> **Do NOT trigger `@tdd-feature-cycle`.**
> **Next:** Trigger `@cron-audit` again in ~30 minutes **(model: claude-4.6-opus-thinking)**.

Then STOP. Do not write another word or call another tool.
