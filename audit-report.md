# System Audit Report

**Generated:** 2026-02-26T12:00:00Z
**Auditor:** enterprise-hardening (external audit contextualization)
**Commit:** HEAD on main

## Executive Summary

- Quality Gates: Pending verification (pre-existing suite)
- FRs: 18/18 implemented (FR-1 through FR-18)
- Findings: 0 CRITICAL, 6 HIGH, 0 LOW
- **Verdict: YELLOW**

An external adversarial audit scored the system at "F (Proof of Concept Level)". After independent
verification against the actual codebase, 5 of 10 cited weaknesses are already mitigated by existing
implementations (FR-9, FR-13, FR-14, FR-16, FR-11). The remaining 6 findings are HIGH-severity
architectural weaknesses in existing code that require targeted hardening.

## Findings

### HIGH

1. **[HIGH-001]** `orchestrator/app/subgraph_cache.py:122` — Cache byte estimator uses
   `sys.getsizeof(json.dumps(value, default=str))`, performing full JSON serialization on every
   cache put when `max_value_bytes > 0`. This is O(N) in value size and blocks the event loop.
   Replace with structural byte estimation that avoids serialization.

2. **[HIGH-002]** `orchestrator/app/lazy_traversal.py:58-81` — Personalized PageRank runs in
   pure Python over edges fetched from Neo4j. While bounded by `max_edges=2000`, this moves
   graph compute to the application tier. Should delegate to Neo4j GDS `gds.pageRank.stream`
   when the plugin is available, falling back to local computation.

3. **[HIGH-003]** `orchestrator/app/vector_store.py:583` — `create_vector_store()` silently
   falls back to `InMemoryVectorStore()` for unrecognized backends. In production, this masks
   misconfigurations with zero durability. Factory should reject non-durable backends in
   production mode.

4. **[HIGH-004]** `orchestrator/app/query_engine.py:294` — LLM synthesis uses `llm.ainvoke()`
   (non-streaming). Time-to-first-token equals full generation time. Should support streaming
   via `llm.astream()` for reduced perceived latency.

5. **[HIGH-005]** `orchestrator/app/vector_store.py:128-252` — `Neo4jVectorStore` stores
   embeddings as node properties (`SET n.embedding = $vector`), polluting Neo4j PageCache with
   dense float arrays. While `QdrantVectorStore` exists as an alternative, `Neo4jVectorStore`
   is not formally deprecated and remains available for selection.

6. **[HIGH-006]** `orchestrator/app/neo4j_client.py:418-421` — Tombstone sweep queries match
   `ingestion_id <> $current_id` across ALL edges per node label without tenant scoping. In a
   multi-tenant deployment, this scans the entire graph for every tenant's ingestion run.

## Verdict

**Status:** YELLOW
**Action:** Trigger `@tdd-feature-cycle` to address all 6 HIGH findings
**Priority:** HIGH-001 through HIGH-006 in implementation order
