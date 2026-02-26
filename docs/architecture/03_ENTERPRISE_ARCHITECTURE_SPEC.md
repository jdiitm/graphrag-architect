# GraphRAG Deterministic Enterprise Architecture Specification

> **Status:** Finalized Spec | **Version:** 1.0.0 | **Tier:** Enterprise-Grade / FAANG-Ready

---

## 1. Product Specification Reconstruction

The following serves as the canonical source of truth for the system, resolving ambiguities present in the earlier evolutionary RFC/PRD documents and redefining the system boundary as a strict, strict, deterministic machine.

### 1.1 Core Mission & Identity
GraphRAG-Architect is not a simple internal tool; it is a **Tier-1 Infrastructure Topology Intelligence Platform**. Its core strategic moat is **Hybrid VectorCypher Retrieval**, marrying deterministic graph traversals (Neo4j) with probabilistic semantic search (Vector Embeddings) and continuous Kafka-scale asynchronous ingestion.

### 1.2 System Boundary & Abstractions
The system boundary is strictly segregated into four control planes:

1. **Data Plane (Ingestion):** Kafka-based async pipeline. Go-based workers guarantee at-least-once delivery with DLQ-driven fault tolerance. Rate-limited, ordered by repository hash.
2. **Control Plane (Query & Orchestration):** Python FastAPI orchestrator. Handles dynamic query classification, routing (Vector/Cypher/Hybrid), and synthesis. Strict API boundaries with parameter validation.
3. **Storage Plane (Persistence):** 
    - **Graph:** Neo4j (Target: Enterprise Multi-Region Clustering).
    - **Vector:** Co-located in Neo4j HNSW index.
    - **Blob:** S3-compatible blob storage (removing code blobs from Neo4j properties).
4. **Observability Plane (Telemetry):** OpenTelemetry (OTEL) injecting trace contexts (`traceparent`, Baggage) across Python/Go/Kafka boundaries.

### 1.3 Tenant Isolation Constraints (Non-Negotiable)
- **Standard Tier:** Logical Isolation (mandatory `tenant_id` property on all nodes AND edges).
- **Enterprise Tier:** Physical Isolation (Neo4j Composite Databases, dedicated Kafka topics, dedicated Vault KMS DEKs per batch).
- **Enforcement:** Zero-trust Cypher. All Cypher queries must use parameterized templates. Regex-based Cypher injection is explicitly banned and considered a security vulnerability.

---

## 2. Enterprise Gap Analysis (FAANG-Level Standards)

The transition from prototype to an Enterprise-grade platform requires strict adherence to Service Level Objectives (SLOs) and Non-Functional Requirements (NFRs).

### 2.1 Measurable Service Level Objectives (SLOs)
| Control | Target SLA | Error Budget | Enforcement Mechanism |
|---|---|---|---|
| **Query Availability** | `99.95%` | 21.6 mins / mo | Prometheus Burn Rate Alerting (5m/1h/6h) |
| **Ingestion Availability** | `99.90%` | 43.2 mins / mo | API Gateway (Envoy/Kong) |
| **Query Latency (Vector)** | `P99 < 500ms` | 0.5% permitted | OpenTelemetry span duration (`query.vector_retrieve`) |
| **Query Latency (Graph)** | `P99 < 3,000ms`| 1.0% permitted | Circuit Breaker Timeouts + EXPLAIN Cost limits |
| **Ingestion Freshness** | `P99 < 10s` | 1.0% permitted | End-to-end trace correlation |
| **DLQ Fault Tolerance** | `< 0.01%` | ~4 msgs / 40K | Sink interface guarantees |

### 2.2 Security & Compliance Imperatives
- **IAM / Auth:** HMAC-SHA256 tokens with fail-closed validation. Empty `AUTH_TOKEN_SECRET` mandates immediate 401 Unauthorized rejection.
- **RBAC / ABAC:** Role-based (`admin`, `writer`, `reader`) combined with Attribute-based (`team_owner`, `namespace_acl`).
- **OWASP LLM Top 10 Defenses:** 
  - **Prompt Injection:** Input sanitization + sandboxed extraction.
  - **Secret Spillage:** Pre-extraction regex scanning (gitleaks) + log redaction.
  - **Unbounded Consumption:** Query complexity EXPLAIN checks -> Token bucket rate limiting (Header: `X-RateLimit-Remaining`).
- **Encryption:** TLS 1.3 in-transit, mTLS service-mesh, AES-256-GCM envelope encryption via KMS for data-at-rest.

---

## 3. Deterministic Expansion Requirements

### 3.1 Async Ingestion State Machine
Ingestion is broken down into a strict, verifiable DAG state machine:

1. **`Kafka Poll`:** Consume from `raw-documents` -> extract composite partition key (`hash(tenant_id + repo_hash)`).
2. **`Semantic Deduplication`:** Check content-hash against `Cache L2 (Redis)`. If unaltered, skip to `Ack`.
3. **`LLM Extraction`:** 
    - Concurrency bound by asyncio Semaphores (Target: 15-20 concurrent).
    - Output strictly constrained to Pydantic schemas. Wait max timeout (default: 8s per batch).
4. **`Schema Validation`:** Validate `ServiceNode`, `KafkaTopicNode`, etc. If validation fails > `validation_retries` (default: 3), route to DLQ.
5. **`Neo4j Write Batching`:** 
    - Buffer valid entities until batch size `N=100` or idle timeout `T=1s`.
    - Execute `UNWIND $batch AS row MERGE ...` wrapped in explicit transaction.
6. **`Commit & Ack`:** Async commit acknowledgment back to Kafka.

### 3.2 Cache Tiering Hierarchy
- **L1 (In-Process LRU):** FastAPI Memory. Caches `query_classifier`, `schema_metadata`. TTL: 5-30m.
- **L2 (Distributed Redis):** Hash-slot sharded by `{tenant_id}:`. Caches `query_result` (TTL 60s-600s), `embedding_lookup` (TTL 3600s).
- **L3 (Neo4j Page Cache):** Heap-resident optimized for highly connected traversal pages.

---

## 4. Graph Intelligence Hardening

### 4.1 Schema Strictness
Nodes and edges cannot be arbitrarily created by the LLM. They must conform to the defined enterprise schema.

**Nodes:** `Service`, `Database`, `KafkaTopic`, `K8sDeployment`
**Edges:** `CALLS`, `PRODUCES`, `CONSUMES`, `DEPLOYED_IN`

**Properties:**
- Mandatory: `id`, `tenant_id`, `name`
- Source Code storage is explicitly BANNED in graph properties. Moving forward, full code text is stored in S3, and the graph node holds an `s3_uri_ref`.

### 4.2 Query Complexity Bounding (The O(d^k) Problem)
Graph traversals (`CALLS*1..5`) risk combinatorial explosions spanning millions of paths.

**Hard Limits:**
- Dynamic relationship depths maxed at `5` (`*1..5`). 
- Queries executing `EXPLAIN` with estimated hits > `100,000` are preemptively killed (HTTP 422).
- At 100x scale (>50,000 nodes), Leiden Community Detection algorithms are pre-computed offline to cluster sub-graphs, avoiding full-graph unconstrained searches.

---

## 5. Search Pipeline & Agent Architecture

### 5.1 Hybrid VectorCypher Retrieval
The Orchestrator's retrieval pipeline:

1. **`classify_query`:** LLM dynamically classifies query intent:
    - *Navigational/Exact* -> Cypher Match.
    - *Semantic/Fuzzy* -> Vector Search.
    - *Blast Radius/Multi-hop* -> Agentic Traversal.
2. **`check_semantic_cache`:** Query text is embedded; if cos-sim > 0.98 to a cached query hit for the same `tenant_id`, return immediately.
3. **`cypher_retrieve` (Agentic Traversal):**
    - Identifies `start_node_id` via preliminary Vector HNSW lookup.
    - Uses pre-parameterized Cypher routing.
    - Employs bounded BFS (Breadth-First Search) outward.
4. **`synthesize`:** Injects raw Cypher graph output into the final LLM prompt with strict limits on context window size to fulfill token budgets.

---

## 6. Failure Modes Analysis & Circuit Breakers

### 6.1 Distributed State & Split Brain
- **Race Conditions:** Multiple concurrent ingest workflows creating duplicate nodes. 
  - *Fix:* Enforce Neo4j `CREATE CONSTRAINT ... IS UNIQUE` combined with `MERGE`.
- **Kafka Consumer Contention:** Go consumer blocking on synchronous acknowledgments.
  - *Fix:* Must implement asynchronous offset commit processing (non-blocking channels) to avoid consumer rebalancing storms during high-load ingestion.
  
### 6.2 Bulkheads & Circuit Breakers
- **Neo4j Pool Exhaustion:** Limit connection pool size matching `num_uvicorn_workers * extraction_concurrency`.
- **Circuit Breaker Trip:** If Neo4j failures > `3` in `10s`, breaker opens. System fails gracefully by serving requests from Redis L2 cache if available, or returning explicit HTTP 503 (Dependency Failure), preventing cascading timeouts back up to the API Gateway.
- **LLM Rate Limits:** Immediate 429 propagation using Tenacity exponential backoff with jitter on `GoogleGenAI` calls.

---

## 7. Strategic Scorecard & Final Audit Conclusion

### 7.1 FAANG Readiness Scorecard
- **System Boundaries:** `A-` (Well-defined planes, needs implementation of S3 blob storage).
- **Security & RBAC:** `B` (Current Cypher injection risks must be refactored to static templates).
- **Scalability (Compute):** `A` (Kafka/Go data plane is highly scalable).
- **Scalability (Database):** `B+` (Requires composite DB/Infinigraph at 1000x scale).
- **Observability:** `A` (OTEL and Google SRE error budgeting is best-in-class).

### 7.2 Crucial Gaps & Unanswered Questions
1. **Neo4j Page Cache vs. Heap:** Optimal memory tuning remains empirical; must be load tested in production.
2. **Third-Party Plugin Sandboxing:** Untrusted custom extractors (`RFC-005`) present a severe remote code execution vulnerability unless strongly jailed in WebAssembly (WASM).
3. **LLM Economics:** Scaling to 1000x makes commercial LLM APIs cost-prohibitive. Migration path to an internally fine-tuned LLaMA-3/Mistral open-source model is mandatory for positive gross margins.

### 7.3 Competitive Edge
GraphRAG-Architect is structurally superior to simple LangChain wrappers. The strict ingestion DAG, asynchronous Go data plane, and deterministic Cypher template routing give it the high-availability characteristics of a Tier-1 infrastructure service, perfectly positioned to capture the Enterprise market.
