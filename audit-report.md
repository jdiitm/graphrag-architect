# System Audit Report

**Generated:** 2026-03-04
**Auditor:** system-audit (adversarial, code-grounded)
**Commit:** 24ce8a7
**Branch:** main

## Executive Summary

- Quality Gates: Pylint 10.00/10, Python 4306/4306 passed, Go 203/203 passed, Go lint: clean
- FRs: 18/18 implemented
- Findings: 4 CRITICAL, 5 HIGH, 1 LOW
- **Verdict: RED**

## Findings

### CRITICAL

1. **[CRITICAL-001]** `orchestrator/app/blob_fetcher.py:87` — `BlobFetcher.fetch_content(ref)` calls `self._store.get(ref.key)` with zero tenant_id validation. Cross-tenant blob access is possible if a Kafka message payload contains another tenant's blob key. `resolve_file_content()` (line 68) and `upload_files_to_blob()` (line 53) also lack tenant scoping. Defeat test: tenant B constructs `BlobReference(key="tenant-A/repo/main.py")` and calls `fetch_content` — current code returns tenant A's source code.

2. **[CRITICAL-002]** `orchestrator/app/tenant_admin_api.py:273` — `register_webhook` handler calls `_resolve_principal(authorization)` but does NOT call `_require_admin(principal)`. Any authenticated user (including `role=reader`) can register webhooks for their tenant's event stream. Compare with `get_tenant` (line 134), `apply_migrations` (line 208), and `rollback_migration` (line 239) which all enforce `_require_admin`. The webhook handler is the sole exception.

3. **[CRITICAL-003]** `orchestrator/app/tenant_admin_api.py` — `get_tenant` handler uses `driver.session()` instead of `TenantConnectionWrapper`. The query is parameterized (`WHERE n.tenant_id = $tenant_id`) but the session is not tenant-bound, bypassing the `TenantEnforcingDriver.validate_query_params` enforcement layer. Other admin handlers exhibit the same pattern.

4. **[CRITICAL-004]** Five security modules are implemented with tests but never wired into the request handling chain:
   - `orchestrator/app/request_signing.py` — HMAC request integrity with nonce replay protection. Not imported in `main.py` or any handler.
   - `orchestrator/app/call_isolation.py` — CALL subquery ACL enforcement. Not imported in `query_engine.py`.
   - `orchestrator/app/data_residency.py` — Regional routing enforcement. Only self-referencing.
   - `orchestrator/app/vault_provider.py` — Secrets from HashiCorp Vault. `AuthConfig` uses env vars instead.
   - `TenantSecurityProvider.validate_query` — Used in `agentic_traversal.py` (lines 481, 570, 667) but NOT in `query_engine.py` template or dynamic Cypher execution paths.

### HIGH

1. **[HIGH-001]** `orchestrator/app/vector_sync_outbox.py:939` — `DurableOutboxDrainer._latest_version_by_key: Dict[str, int] = {}` grows monotonically with one entry per `(tenant_id, fence_key)` pair. No eviction, no TTL, no max size. At 10M documents across 1,000 tenants with 4 entity types, this dictionary accumulates 40M+ entries (~4GB), is lost on pod restart, and re-populated from Kafka replay.

2. **[HIGH-002]** `orchestrator/app/prompt_sanitizer.py:501` — `HMACDelimiter.__init__` generates a random key per instance. In a multi-replica deployment, context blocks signed by replica A fail HMAC validation on replica B. Breaks any form of request routing that crosses replicas.

3. **[HIGH-003]** `orchestrator/app/ingestion_resume.py:50` — `InMemoryStatusStore` is the only implementation of `IngestionStatusStore`. No Redis-backed or Neo4j-backed variant exists. Pod restart loses all ingestion progress tracking. The `list_resumable()` method returns empty after restart.

4. **[HIGH-004]** `orchestrator/app/cypher_validator.py` — `ALLOWED_PROCEDURES` includes `dbms.queryJmx`. This procedure exposes JMX MBean attributes including memory usage, thread counts, and internal Neo4j configuration. Information disclosure risk.

5. **[HIGH-005]** `orchestrator/app/main.py` — Both `/ingest` and `/query` endpoints share the same asyncio event loop. LLM extraction during ingestion (via `asyncio.Semaphore(max_concurrency)`) competes with query handlers for event loop time. Under high ingestion load, query P99 will degrade beyond the 3s SLO (NFR-2). SPEC Section 4.3 documents Phase 3 separation but it is not implemented.

### LOW

1. **[LOW-001]** `orchestrator/app/query_classifier.py` — 32 lines of regex patterns for query classification. Ambiguous queries (e.g., "What happens to payments when auth-service is slow?") may be classified as ENTITY_LOOKUP instead of MULTI_HOP. No LLM fallback for borderline cases.

## Requirement Gaps

All FRs (FR-1 through FR-18) are implemented with tests. No requirement gaps.

## Verdict

**Status:** RED
**Action:** Trigger `@tdd-feature-cycle`
**Priority:** CRITICAL-001 through CRITICAL-003 (tenant boundary hardening cluster) — BlobFetcher tenant validation, webhook admin check, admin API tenant-bound sessions. These form a coherent security fix for a single PR.
