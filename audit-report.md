# System Audit Report

**Generated:** 2026-02-26
**Auditor:** External Principal Distributed Systems Engineer (RED mode)
**Commit:** 4c03517

## Executive Summary

- Quality Gates: Deferred (external audit — code-based red-team review)
- FRs: All FR-1 through FR-18 implemented
- Findings: 7 CRITICAL
- **Verdict: RED**

## Findings

### CRITICAL

1. **[CRITICAL-001]** `workers/ingestion/internal/blobstore/memory.go:1` — InMemoryBlobStore is the sole BlobStore implementation. Pod restarts lose all >256KB documents currently in the Kafka pipeline. Blob keys become dangling pointers causing hard 404s downstream.

2. **[CRITICAL-002]** `workers/ingestion/internal/dedup/dedup.go:10` — LRUStore is single-process in-memory. main.go wires NoopStore by default, meaning deduplication is completely inactive. Kafka partition rebalances route duplicates to new workers, amplifying Neo4j transaction load.

3. **[CRITICAL-003]** `orchestrator/app/graph_builder.py:93` — invalidate_caches_after_ingest() calls advance_generation() globally on both subgraph and semantic caches. Every ingest wipes all tenants' caches, creating a thundering herd to Neo4j.

4. **[CRITICAL-004]** `orchestrator/app/main.py:261` — POST /ingest blocks HTTP thread on ingestion_graph.ainvoke(), causing ALB 504 Gateway Timeouts on large payloads.

5. **[CRITICAL-005]** `orchestrator/app/query_engine.py:273` — _raw_llm_synthesize injects graph context without XML boundary demarcation. Malicious manifests can hijack the system instruction via prompt injection.

6. **[CRITICAL-006]** `orchestrator/app/neo4j_client.py:273` — DEFAULT_WRITE_CONCURRENCY=4 with no entity sorting. Concurrent MERGE statements acquire locks in arbitrary order, guaranteeing deadlocks at scale.

7. **[CRITICAL-007]** `orchestrator/app/query_engine.py:390` — single_hop_retrieve uses blunt LIMIT $hop_limit truncation. In dense graphs, this randomly truncates hub nodes, destroying graph semantics for the LLM.

## Verdict

**Status:** RED
**Action:** Trigger `@tdd-feature-cycle` — implement CRITICAL-001 first (S3 BlobStore)
**Priority:** CRITICAL-001 (volatile distributed state)
