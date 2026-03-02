---
name: doc-sync
description: Documentation truth-sync for graphrag-architect. Reads the entire repository (every source file, test, config) and updates all mutable documentation to reflect the actual state of the code. Standalone utility — trigger when asked to sync docs, update docs, fix stale docs, or reconcile documentation.
---

# Documentation Truth Sync

Read the entire repo. Compare every mutable doc against reality. Update docs to match what the code actually does. Code is the single source of truth — documentation is its reflection.

## FSM Position

```
AUDIT → TDD → REVIEW ─┬─ [merged]  → AUDIT → TDD → ...
                       └─ [changes] → FIX → REVIEW

**DOC_SYNC** is a standalone utility — trigger on demand, not part of the mandatory cycle.
```

You are in the **DOC_SYNC** state. You are a standalone utility for keeping documentation truthful.
You can be triggered at any time — after a merge, before a release, or whenever docs drift from reality.

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
```

**If not on main or dirty tree:** HALT. Tell the user: "Working tree is not clean on main."

## Principle: Code is Truth

- `docs/SPEC.md` is the canonical system specification. Update it when implementation status, counts, file maps, or schema DDL have materially diverged from code.
- `CLAUDE.md` defines AI agent invariants — update only when the tech stack, entity types, or edge types change.
- `README.md` is the user-facing entry point — keep it accurate (counts, quickstart, project structure).
- `.cursor/skills/*/SKILL.md` reference specification files — update when canonical file paths change.

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
source .venv/bin/activate

# Python test count
python -m pytest orchestrator/tests/ -v --co -q 2>/dev/null | tail -1

# Go test count
cd workers/ingestion && go test ./... -v -count=1 -timeout 30s 2>&1 | grep -c '^--- PASS'

# File tree
cd "$(git rev-parse --show-toplevel)" && find . -type f \
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
CLAUDE.md
docs/SPEC.md
```

## Step 3: Identify Discrepancies

Build a comparison table for each mutable doc. Check every factual claim against what you read in Step 1.

### README.md checks

| Check | Source of Truth |
|-------|----------------|
| Project structure tree | Actual `find` output |
| Module/test counts | Actual pytest --collect-only / go test output |
| Tech stack table | Actual imports in requirements.txt and go.mod |
| Docker-compose services | Actual `infrastructure/docker-compose.yml` |
| Quickstart instructions | Actual `requirements.txt`, actual env vars in `config.py` |

### CLAUDE.md checks

| Check | Source of Truth |
|-------|----------------|
| Tech stack | Actual imports (LangGraph vs LlamaIndex, etc.) |
| Entity types | Actual `extraction_models.py` class names |
| Edge types | Actual relationship names in `extraction_models.py` |

### docs/SPEC.md checks

| Check | Source of Truth |
|-------|----------------|
| Section 4.3 module/test counts | Actual file counts and pytest/go test output |
| Section 5.5 schema DDL | Actual `schema_init.cypher` |
| Section 13.2 API endpoints | Actual `@app.get`/`@app.post` decorators in `main.py` |
| Section 16 CI pipeline | Actual `.github/workflows/ci.yml` |
| Section 19.1 production checklist | Actual code (verify Implemented items still hold) |
| Appendix A file maps | Actual files on disk |
| All numeric claims | Actual counts, defaults in `config.py` |

## Step 4: Apply Updates

For each discrepancy found, update the doc to match reality. Follow these rules:

1. **Be surgical.** Change only the specific lines that are wrong. Do not rewrite entire sections unless the structure itself is broken.
2. **Preserve doc style.** Match the existing formatting, heading levels, table style, and tone.
3. **Never inflate.** If something isn't implemented, say so. Never describe aspirational state as current.
4. **Never deflate.** If something IS implemented, make sure the doc reflects it.
5. **Update counts.** Test counts, file counts, and line counts must be exact.
6. **Update trees.** Project structure trees must match `find` output exactly.
7. **Preserve spec intent.** Update factual claims (counts, file paths, DDL) but do not change architectural decisions or requirement definitions in `docs/SPEC.md`.

### Specific update targets

**README.md:**
- Project structure tree and module/test counts
- Docker-compose service list
- Tech stack table

**CLAUDE.md:**
- Tech stack line (ensure no stale framework references)
- Entity types and edge types (must match extraction_models.py)

**docs/SPEC.md:**
- Section 4.3: module/test counts
- Section 5.5: schema DDL (must match schema_init.cypher verbatim)
- Section 13.2: API endpoints (implemented vs planned)
- Section 16: test counts and CI pipeline status
- Section 19.1: production checklist statuses
- Appendix A: file maps (add new files, remove deleted files)

## Step 5: Run Quality Gates

After making changes, verify nothing is broken:

```bash
source .venv/bin/activate
pylint orchestrator/
python -m pytest orchestrator/tests/ -v
cd workers/ingestion && go test ./... -v -count=1 -timeout 30s
```

## Step 6: Commit

If any docs were changed:

```bash
git add README.md CLAUDE.md docs/SPEC.md
git commit -m "$(cat <<'EOF'
docs: sync documentation with codebase reality

Automated truth-sync by doc-sync skill.
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

### Skipped
- .cursor/skills/*: skill files (update only when canonical file paths change)
- .cursor/rules/*: rule files (update only when quality gate commands change)
```

## Step 8: HALT

**HALT. Your job is done. Do NOT continue.**

Tell the user exactly this:

> Documentation sync complete. N files updated, M files already accurate.
> **Next:** Open a new chat and trigger `@tdd-feature-cycle` to continue the development cycle.

Then STOP. Do not write another word or call another tool.
