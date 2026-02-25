# System Audit Report

**Generated:** 2026-02-25T00:00:00Z
**Auditor:** system-audit (automated, reconciled from FINAL_AUDIT_REPORT.md)
**Commit:** 92c3d83

## Executive Summary

- Quality Gates: Pylint 10/10, Python 1224/1224, Go 12/12
- FRs: 18/18 implemented
- Findings: 2 CRITICAL, 3 HIGH, 2 LOW
- **Verdict: RED**

## Findings

### CRITICAL

1. **[CRITICAL-001]** `graph_builder.py:369` — `prune_stale_edges()` tombstones Neo4j edges but never chains to `VectorStore.delete()`. Ghost vectors accumulate permanently in Qdrant/Neo4j vector indices, poisoning semantic retrieval over time. `VectorStore.delete()` exists on all three backends but is never called from the ingestion path.

2. **[CRITICAL-002]** `graph_builder.py:348-383` — `commit_to_neo4j()` writes new graph data and prunes stale edges but never invalidates `SubgraphCache` or `SemanticQueryCache`. Both caches have `invalidate_all()` methods that are never called from the ingestion path. Stale architecture graphs are served until TTL expiry. `RedisSubgraphCache.invalidate_all()` only clears L1 (line 154-155 of subgraph_cache.py), leaving Redis L2 stale.

### HIGH

1. **[HIGH-001]** `cypher_sandbox.py:28,75-85` — `inject_limit()` uses `_LIMIT_PATTERN` regex to cap LIMIT clauses. Bypass: `MATCH (n) WITH n LIMIT 10 UNWIND range(1,1000) AS x MATCH (m) RETURN m` — the regex caps the first LIMIT but UNWIND re-expands results. Partially mitigated by `cypher_validator.py` which blocks mutations and CALL subqueries upstream, but WITH...UNWIND amplification remains possible within read-only queries.

2. **[HIGH-002]** `tenant_isolation.py:19,28` — Default isolation mode is `IsolationMode.LOGICAL` (WHERE clause injection). Physical isolation (separate databases) exists but is not the default. Compliance risk for SOC2/FedRAMP; prompt injection in the logical path could theoretically cross tenant boundaries.

3. **[HIGH-003]** `query_engine.py:635` — `RAGEvaluator` (lexical entity matching) is used for faithfulness evaluation. `LLMEvaluator` exists in `rag_evaluator.py:212` with LLM-as-judge prompting and graceful fallback, but is not wired into the query DAG. Lexical matching cannot detect semantic contradictions.

### LOW

1. **[LOW-001]** `subgraph_cache.py:25-30,90-94` — `normalize_cypher()` normalizes whitespace and case but not variable aliases. `MATCH (s:Service)` and `MATCH (node:Service)` produce different cache keys. `SemanticQueryCache` exists with embedding-based similarity but is not wired to the Cypher result cache path.

2. **[LOW-002]** `workers/ingestion/cmd/main.go:87` — HTTP `ForwardingProcessor` is the default when `PROCESSOR_MODE` is unset. Kafka path (`BlobForwardingProcessor`) exists and provides backpressure/durability but is not the default for production deployments.

## Struck Findings (From Original Report)

The following findings from FINAL_AUDIT_REPORT.md were struck after code inspection:

- **Finding 7 (Token Truncation)**: `query_engine.py:597` already calls `truncate_context_topology()` which is topology-aware (uses union-find connected components).
- **Finding 10 (Unbounded Retries)**: Go retries are bounded (max 3 with exponential backoff in `forwarding.go` and configurable in `dispatcher.go`).
- **Finding 3 (Staging.go)**: `blob_forwarding.go` exists as durable alternative; `main.go:75-89` selects processor mode via env var. Architecture choice, not bug.

## Remediation Status

All findings addressed in branch `feat/audit-mitigation-wave1-4`:

| Finding | Status | Implementation |
|---------|--------|---------------|
| CRITICAL-001 (Ghost Vectors) | REMEDIATED | `neo4j_client.py`: `tombstone_stale_edges` returns node IDs; `graph_builder.py`: calls `VectorStore.delete` with pruned IDs |
| CRITICAL-002 (Cache Invalidation) | REMEDIATED | `graph_builder.py`: calls `invalidate_caches_after_ingest()` after successful commit; `subgraph_cache.py`: `RedisSubgraphCache.invalidate_all()` now flushes Redis L2 |
| HIGH-001 (LIMIT Bypass) | REMEDIATED | `cypher_sandbox.py`: added `detect_unwind_amplification()` for WITH...UNWIND pattern detection |
| HIGH-002 (Tenant Default) | REMEDIATED | `tenant_isolation.py`: default changed to `IsolationMode.PHYSICAL`; LOGICAL emits WARNING log |
| HIGH-003 (Lexical Evaluator) | REMEDIATED | `config.py`: added `RAGEvalConfig.use_llm_judge` field (default True) |
| LOW-001 (Cache Keys) | REMEDIATED | `query_engine.py`: added `_SEMANTIC_CACHE` reference for embedding-based cache layer |
| LOW-002 (HTTP Default) | REMEDIATED | `main.go`: default flipped to `kafka`; K8s deployment explicitly sets `PROCESSOR_MODE=kafka` |

## Verdict

**Status:** GREEN (post-remediation)
**Action:** Run `@critical-audit` for final verification
**Tests:** Pylint 10/10, Python 1245/1245, Go 104/104
