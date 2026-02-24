# 02 — System Requirements

> **Status:** Approved | **Version:** 1.0 | **Last Updated:** 2026-02-22

---

## 1. Functional Requirements

### FR-1: Async Document Ingestion

The system SHALL ingest raw infrastructure artifacts (source code files, Kubernetes manifests, Kafka topic configurations) via an asynchronous pipeline.

| Property | Specification |
|---|---|
| **Transport** | Apache Kafka, topic `raw-documents` |
| **Consumer** | Go worker using franz-go (`github.com/twmb/franz-go/pkg/kgo`) |
| **Processing** | Dispatcher fans out to N goroutines from a shared buffered channel |
| **Forwarding** | `ForwardingProcessor` HTTP POSTs deserialized payloads to the Python orchestrator |
| **Delivery Semantics** | At-least-once. Manual offset commit after successful forwarding. |
| **Partition Strategy** | Keyed by source repository hash for per-repo ordering guarantees |
| **Implementation** | `workers/ingestion/internal/dispatcher/dispatcher.go`, `workers/ingestion/internal/processor/processor.go` |

---

### FR-2: LLM Entity Extraction

The system SHALL extract typed entities and relationships from raw infrastructure artifacts using LLM-powered structured output.

| Property | Specification |
|---|---|
| **LLM Provider** | Google Gemini via `langchain-google-genai` (default: `gemini-2.0-flash`, overridable to `gemini-2.5-pro`) |
| **Input Filtering** | Static filter on file extensions (`.go`, `.py` for source; `.yaml`, `.yml` for manifests) |
| **Batching** | Greedy bin-packing by estimated token budget (`len(content) // 4`) |
| **Concurrency** | Semaphore-gated async extraction (`asyncio.Semaphore`) |
| **Output Schema** | Pydantic models: `ServiceNode`, `DatabaseNode`, `KafkaTopicNode`, `K8sDeploymentNode`, `CallsEdge`, `ProducesEdge`, `ConsumesEdge`, `DeployedInEdge` |
| **Deduplication** | By `ServiceNode.id` across batches |
| **Retry** | Exponential backoff via `tenacity` on `google.api_core.exceptions.ResourceExhausted` and `ServiceUnavailable` |
| **Implementation** | `orchestrator/app/llm_extraction.py`, `orchestrator/app/extraction_models.py` |

---

### FR-3: Knowledge Graph Persistence

The system SHALL persist extracted entities and relationships to Neo4j via idempotent Cypher operations.

| Property | Specification |
|---|---|
| **Write Strategy** | Cypher `MERGE` on unique key properties (e.g., `Service.id`, `KafkaTopic.name`) |
| **Idempotency** | Guaranteed by unique constraints. Repeated ingestion of the same artifact produces identical graph state. |
| **Schema Enforcement** | 5 uniqueness constraints, 2 secondary indexes (see `orchestrator/app/schema_init.cypher`) |
| **Driver** | `neo4j` Python driver v6.1 with async session support |
| **Transaction Scope** | One transaction per ingestion batch. Rollback on partial failure. |
| **Implementation** | `commit_to_neo4j` node in `orchestrator/app/graph_builder.py`, `orchestrator/app/neo4j_client.py` |

---

### FR-4: Hybrid Query Routing

The system SHALL classify incoming queries by complexity and route them to the optimal retrieval strategy.

| Property | Specification |
|---|---|
| **Router** | LangGraph `StateGraph` with conditional edges classifying query type |
| **Vector Path** | Entity disambiguation and simple lookups via Neo4j vector index (ANN search, Lucene-backed). Targets: `Service.name`, `KafkaTopic.name` embeddings. |
| **Graph Path** | Multi-hop structural queries via agentic Cypher generation. LLM generates Cypher, executes against Neo4j, iterates if result set is insufficient. Planner-executor loop with max 3 iterations. |
| **Hybrid Path** | DRIFT-inspired: vector pre-filter narrows candidate nodes, then Cypher traversal operates on the filtered subgraph for complex structural reasoning. |
| **Synthesis** | LLM produces natural-language answer grounded in retrieved subgraph context. |
| **Implementation** | `orchestrator/app/query_engine.py`, `orchestrator/app/query_classifier.py`, `orchestrator/app/query_models.py` |

**Routing Classification:**

| Query Type | Route | Example |
|---|---|---|
| Entity lookup | Vector | "What language is the auth-service written in?" |
| Single-hop relationship | Vector + 1-hop Cypher | "What topics does order-service produce to?" |
| Multi-hop traversal | Graph (agentic Cypher) | "If auth-service fails, what is the full downstream blast radius?" |
| Aggregate/analytical | Hybrid (DRIFT) | "What are the most critical services by transitive dependency count?" |

---

### FR-5: Schema Validation with Correction Loop

The system SHALL validate all extracted entities against the Pydantic schema before graph persistence, with automatic correction for recoverable errors.

| Property | Specification |
|---|---|
| **Validation** | Post-extraction Pydantic model validation on all node and edge types |
| **Error Routing** | `route_validation` conditional edge in LangGraph DAG |
| **Correction** | `fix_extraction_errors` node re-invokes LLM with error context for targeted re-extraction |
| **Max Iterations** | 3 correction cycles before routing to DLQ |
| **Implementation** | `orchestrator/app/graph_builder.py`: `validate_extracted_schema` -> conditional -> `fix_extraction_errors` -> `validate_extracted_schema` (loop) |

---

### FR-6: Dead Letter Queue Fault Tolerance

The system SHALL guarantee zero message loss by routing permanently failed jobs to a Dead Letter Queue with full error metadata.

| Property | Specification |
|---|---|
| **Trigger** | Job fails after `MaxRetries` attempts (configurable, default 3) |
| **DLQ Topic** | `raw-documents.dlq` |
| **Error Metadata** | Original message key/value, error string, attempt count, timestamp, source partition/offset in Kafka headers |
| **Handler** | Dedicated goroutine draining a buffered `Result` channel to a `DeadLetterSink` interface |
| **Offset Commit** | Kafka offset committed even on DLQ routing to prevent consumer stall |
| **Implementation** | `workers/ingestion/internal/dlq/handler.go`, `workers/ingestion/internal/dispatcher/dispatcher.go` |

---

### FR-7: Access Control (Phase 2)

The system SHALL enforce permission-aware query results, ensuring users only see graph entities they are authorized to access.

| Property | Specification |
|---|---|
| **Model** | Zanzibar-inspired relationship-based access control |
| **Ingestion-Time** | Permission metadata (team ownership, namespace ACLs) indexed as properties on graph nodes during extraction |
| **Query-Time** | Security principal resolved from request context. Cypher queries extended with `WHERE` clauses filtering by permission metadata. |
| **Graph-Aware RBAC** | Edge traversal authorization: a user may view `Service A` but not traverse `CALLS` edges to `Service B` if `B` is in a restricted namespace |
| **Implementation** | `orchestrator/app/access_control.py` |

---

### FR-8: Observability

The system SHALL emit distributed traces, metrics, and structured logs across the full ingestion and query pipeline using OpenTelemetry.

| Property | Specification |
|---|---|
| **Python** | `opentelemetry-instrumentation-fastapi` auto-instrumentation on all HTTP endpoints. Manual spans on LangGraph node transitions and Neo4j transactions. |
| **Go** | Manual span creation via `go.opentelemetry.io/otel/trace`. Spans on Kafka consume, dispatch, process, and DLQ routing. |
| **Correlation** | Trace context propagated via Kafka message headers (`traceparent`). Spans linked across Go -> HTTP -> Python boundary. |
| **Export** | `BatchSpanProcessor` for production. OTLP exporter to configurable backend (Jaeger, Grafana Tempo). |
| **Metrics** | Kafka consumer lag, ingestion batch duration, Neo4j transaction latency, LLM extraction duration, DLQ routing rate. |
| **Implementation** | `orchestrator/app/observability.py`, `workers/ingestion/internal/telemetry/telemetry.go` |

---

### FR-9: Physical Tenant Isolation

The system SHALL enforce mandatory tenant isolation at the data layer, preventing cross-tenant data access regardless of query construction.

| Property | Specification |
|---|---|
| **Mandatory Field** | `tenant_id: str` required on all 4 node types (`ServiceNode`, `DatabaseNode`, `KafkaTopicNode`, `K8sDeploymentNode`). Pydantic `Field(..., min_length=1)` — no default, no empty string. |
| **Database Constraint** | Neo4j `IS NOT NULL` constraint on `tenant_id` for every node label. Writes without `tenant_id` are rejected at the database level. |
| **Isolation Tiers** | Standard (logical: `WHERE tenant_id = $tid`), Enterprise (physical: dedicated Neo4j composite database per tenant), Sovereign (dedicated Neo4j cluster per tenant). |
| **Driver Routing** | `TenantAwareDriverPool.get_driver(tenant_id)` returns a tenant-specific driver for physical isolation or the shared driver with tenant-scoped sessions for logical isolation. |
| **Request Context** | `TenantContext` dataclass threaded through all request handlers. Resolved from JWT `tenant_id` claim. All downstream operations receive tenant context. |
| **Implementation** | `orchestrator/app/tenant_isolation.py`, `orchestrator/app/extraction_models.py`, `orchestrator/app/neo4j_pool.py`, `orchestrator/app/main.py` |

---

### FR-10: Parameterized Query Templates

The system SHALL use pre-compiled, parameterized Cypher templates for all standard query patterns, eliminating runtime LLM Cypher generation for common operations.

| Property | Specification |
|---|---|
| **Template Catalog** | `vector_search`, `single_hop` (per relationship type), `multi_hop` (bounded depth), `aggregate` (count/rank patterns). All templates include `$tenant_id` as mandatory parameter. |
| **ACL Enforcement** | ACL conditions (`$is_admin`, `$team`, `$namespaces`) baked into every template. No runtime Cypher string manipulation for ACL injection. |
| **Relationship Allowlist** | Template-based queries validate relationship types against `{"CALLS", "PRODUCES", "CONSUMES", "DEPLOYED_IN"}`. Invalid types raise `ValueError`. |
| **Depth Bounds** | Variable-length path templates enforce `min(int(max_depth), 5)` with integer casting. |
| **Dynamic Fallback** | LLM Cypher generation available as fallback behind `ALLOW_DYNAMIC_CYPHER=false` feature flag (disabled by default in production). |
| **Routing Change** | ENTITY_LOOKUP and SINGLE_HOP use templates directly (zero LLM calls). MULTI_HOP uses templates with LLM selecting parameters. AGGREGATE uses pre-compiled aggregation templates. |
| **Implementation** | `orchestrator/app/query_templates.py`, `orchestrator/app/query_engine.py`, `orchestrator/app/access_control.py` |

---

### FR-11: Context Token Budget and Reranking

The system SHALL bound the context injected into the synthesis LLM and rerank retrieved candidates before synthesis.

| Property | Specification |
|---|---|
| **Token Budget** | `max_context_tokens: int = 32_000` configurable. Context exceeding the budget is truncated by relevance score after reranking. |
| **Token Estimation** | `len(text) // 4` (consistent with ingestion batching in `llm_extraction.py`). |
| **Reranking** | `RerankerProtocol` interface with implementations: `CrossEncoderReranker` (production), `BM25Reranker` (fast fallback), `NoopReranker` (testing). |
| **Pipeline** | `_do_synthesize()` flow: candidates -> rerank -> token budget truncation -> LLM synthesis. |
| **Max Results** | Hard cap of 50 candidates post-reranking, before token truncation. |
| **Implementation** | `orchestrator/app/context_manager.py`, `orchestrator/app/reranker.py`, `orchestrator/app/query_engine.py` |

---

### FR-12: Faithfulness-Based RAG Evaluation

The system SHALL evaluate synthesized answers for faithfulness, groundedness, and hallucination — not merely context relevance.

| Property | Specification |
|---|---|
| **Context Relevance** | Cosine similarity between query embedding and retrieved context (post-reranker). Measures retrieval quality. |
| **Answer Faithfulness** | LLM-as-judge verifies every claim in the synthesized answer is supported by a specific entity or relationship in the retrieved context. Returns citation coverage ratio (0.0–1.0). |
| **Answer Groundedness** | For each cited graph entity in the answer, verify it exists via a targeted Cypher query. Returns grounding ratio (0.0–1.0). |
| **Hallucination Detection** | Flag claims referencing entities not present in the retrieved context. Returns list of ungrounded claims. |
| **Output Model** | `EvaluationResult` Pydantic model with per-metric scores and `ungrounded_claims: List[str]`. |
| **Implementation** | `orchestrator/app/rag_evaluator.py`, `orchestrator/app/query_engine.py`, `orchestrator/app/query_models.py` |

---

### FR-13: Decoupled Vector Store

The system SHALL support a dedicated external vector store (Qdrant) for embedding storage and similarity search, keeping Neo4j strictly for relational graph traversal.

| Property | Specification |
|---|---|
| **Protocol** | `VectorStore` protocol with `search(query_embedding, top_k, tenant_id) -> List[ScoredCandidate]` and `upsert(node_id, embedding, metadata)`. |
| **Backends** | `neo4j` (default for <50K nodes, uses native vector index), `qdrant` (for scale, dedicated Qdrant cluster). Selectable via `VECTOR_STORE_BACKEND` env var. |
| **Linking** | Vector store entries keyed by `{node_id, label, tenant_id}`. Neo4j stores topology only (no embedding properties at scale). |
| **Query Path** | Vector search returns candidate node IDs -> Cypher uses `WHERE n.id IN $candidate_ids AND n.tenant_id = $tid` for graph traversal on candidates. |
| **Ingestion Path** | `commit_to_neo4j` also upserts embeddings to the configured vector store. |
| **Implementation** | `orchestrator/app/vector_store.py`, `orchestrator/app/neo4j_client.py`, `orchestrator/app/query_engine.py` |

---

### FR-14: Streaming Ingestion Pipeline

The system SHALL process ingestion data in bounded-memory streaming chunks rather than loading entire workspaces into memory.

| Property | Specification |
|---|---|
| **Chunk Processing** | `graph_builder.py` processes one chunk at a time through the DAG. Memory footprint: O(chunk_size) not O(total_files). |
| **Streaming Pipeline** | `StreamingIngestionPipeline` reads chunks from `load_directory_chunked()` as a generator. Each chunk: AST parse -> LLM extract -> validate -> commit. |
| **Progress Tracking** | `ExtractionCheckpoint` tracks per-chunk progress for resumable ingestion. |
| **Go AST Extraction** | CPU-bound AST parsing delegated to Go workers via `/extract-ast` endpoint, avoiding Python GIL limitations. |
| **Memory Bound** | `WORKSPACE_MAX_BYTES` (100MB) and `MAX_FILE_SIZE_BYTES` (1MB) enforced per-chunk, not accumulated. |
| **Implementation** | `orchestrator/app/graph_builder.py`, `orchestrator/app/workspace_loader.py`, `workers/ingestion/internal/processor/ast_processor.go` |

---

### FR-15: Agentic Graph Traversal

The system SHALL support incremental, LLM-guided graph traversal for multi-hop queries instead of generating monolithic Cypher queries.

| Property | Specification |
|---|---|
| **Traversal Agent** | LangGraph sub-graph with state: `visited_nodes`, `frontier`, `accumulated_context`, `remaining_hops`. |
| **Step Logic** | Each step: LLM decides which frontier node to expand -> execute 1-hop parameterized template query -> add results to context -> decide to continue or synthesize. |
| **Hard Limits** | `max_hops=5`, `max_visited=50`, `token_budget=32_000` (from FR-11). |
| **Template Usage** | Each hop uses parameterized templates from FR-10 (no dynamic Cypher). |
| **Routing** | Wired as the primary retrieval strategy for MULTI_HOP queries, replacing LLM Cypher generation. |
| **Implementation** | `orchestrator/app/agentic_traversal.py`, `orchestrator/app/query_engine.py` |

---

### FR-16: Tombstone-Based Edge Cleanup

The system SHALL use a tombstone-and-reap pattern for stale edge cleanup instead of synchronous bulk deletion.

| Property | Specification |
|---|---|
| **Tombstoning** | On ingestion, edges not seen in the current run are marked `tombstoned_at = <timestamp>` rather than deleted. |
| **Query Filtering** | All read queries include `WHERE r.tombstoned_at IS NULL` to exclude tombstoned edges. |
| **Async Reaper** | Background async worker processes tombstones in batches (100 at a time) with configurable delay. |
| **TTL** | Tombstoned edges older than `TOMBSTONE_TTL_DAYS` (default 7) are permanently deleted in small batches. |
| **Lock Avoidance** | No large write locks on the graph. Reaper batches are small, independent transactions. |
| **Implementation** | `orchestrator/app/neo4j_client.py`, `orchestrator/app/node_sink.py`, `orchestrator/app/graph_builder.py` |

---

### FR-17: Graph-Native Structural Embeddings

The system SHALL generate structural graph embeddings (Node2Vec) that encode topological position, supplementing text-based embeddings.

| Property | Specification |
|---|---|
| **Algorithm** | Biased random walks on the Neo4j graph (Node2Vec). Walk parameters: `walk_length=80`, `num_walks=10`, `p=1.0`, `q=0.5`. |
| **Output** | 128-dimensional structural embedding per node, stored in the vector store alongside text embeddings. |
| **Hybrid Retrieval** | Query retrieval combines text similarity (existing) + structural similarity (new) via weighted sum. |
| **Trigger** | Runs as a background job after ingestion commits. Re-computed incrementally when graph topology changes. |
| **Implementation** | `orchestrator/app/graph_embeddings.py` |

---

### FR-18: Semantic Graph Partitioning

The system SHALL partition the knowledge graph into semantically meaningful bounded contexts using community detection.

| Property | Specification |
|---|---|
| **Algorithm** | Leiden community detection applied to the topology graph. Hierarchical communities with configurable resolution. |
| **Output** | `community_id: str` property assigned to each node. Community hierarchy stored as metadata. |
| **Query Routing** | Multi-hop queries scoped to community first, then drill into specific nodes. Reduces traversal search space. |
| **Physical Mapping** | Communities can map to Neo4j composite database shards for physical isolation at scale. |
| **Implementation** | `orchestrator/app/semantic_partitioner.py` |

---

## 2. Non-Functional Requirements

### NFR-1: Ingestion Latency

| Metric | Target |
|---|---|
| End-to-end (Kafka produce -> Neo4j commit), p50 | < 500 ms per document batch |
| End-to-end, p99 | < 2 s per document batch |
| Kafka transport segment, p99 | < 5 ms (Confluent benchmark baseline at 200 MB/s throughput) |
| LLM extraction per batch, p50 | < 3 s |
| LLM extraction per batch, p99 | < 8 s |

**Measurement:** OpenTelemetry trace duration from Kafka consumer `poll()` to Neo4j transaction commit.

---

### NFR-2: Query Response Time

| Metric | Target |
|---|---|
| Vector-only entity lookup, p50 | < 200 ms |
| Vector-only entity lookup, p99 | < 500 ms |
| Multi-hop Cypher traversal (no LLM), p50 | < 150 ms |
| Multi-hop Cypher traversal (no LLM), p99 | < 500 ms |
| Full agentic query (Cypher + LLM synthesis), p50 | < 1 s |
| Full agentic query (Cypher + LLM synthesis), p99 | < 3 s |

**Measurement:** OpenTelemetry trace duration from FastAPI request receipt to response write.

---

### NFR-3: Throughput

| Metric | Target |
|---|---|
| Sustained ingestion throughput | >= 10,000 documents/minute |
| Concurrent query capacity with SLA compliance | >= 100 concurrent requests |
| Kafka consumer throughput per partition | >= 12,000 messages/second (franz-go baseline) |

**Scaling:** Throughput targets assume a single replica set. Horizontal scaling (NFR-4) extends capacity linearly.

---

### NFR-4: Horizontal Scaling Triggers

| Trigger | Threshold | Action |
|---|---|---|
| Kafka consumer lag | > 1,000 messages sustained for 60 s | Scale Go worker replicas +1 (up to max 10) |
| LangGraph DAG queue depth | > 50 pending jobs for 30 s | Scale Python orchestrator replicas +1 (up to max 5) |
| Neo4j read latency p99 | > 500 ms sustained for 120 s | Add Neo4j secondary read replica |
| CPU utilization (Go workers) | > 80% sustained for 60 s | Scale Go worker replicas +1 |

**Mechanism:** Kubernetes HPA (Horizontal Pod Autoscaler) with custom metrics via Prometheus adapter. Kafka consumer lag exposed as a Prometheus metric by the Go worker.

---

### NFR-5: Availability

| Metric | Target |
|---|---|
| Uptime SLA | 99.9% (43.8 min/month downtime budget) |
| Recovery Time Objective (RTO) | < 5 min |
| Recovery Point Objective (RPO) | 0 (zero message loss via DLQ + at-least-once delivery) |

**Architecture:**
- No single point of failure. All components run as Kubernetes Deployments with `replicas >= 2`.
- Neo4j cluster: N/2+1 primary quorum via Raft consensus. Fault tolerance formula: `M = 2F + 1` where F = tolerated primary failures.
- Kafka: KRaft consensus (no ZooKeeper dependency). Replication factor >= 3 for production topics.

---

### NFR-6: Fault Tolerance

| Failure Mode | Handling |
|---|---|
| Go worker crash mid-processing | Kafka offset not committed. Message redelivered to another consumer in the group. At-least-once guarantee preserved. |
| Neo4j write failure | Circuit breaker (half-open after 30 s). Failed batch routed to DLQ with Neo4j error context. |
| LLM provider rate limit (`ResourceExhausted`) | Exponential backoff with jitter via `tenacity`. Max 5 retries before DLQ routing. |
| LLM provider outage (`ServiceUnavailable`) | Same retry policy. After exhaustion, batch routed to DLQ for manual reprocessing. |
| Kafka broker failure | KRaft consensus elects new leader. Producers retry with idempotent delivery (`enable.idempotence=true`). |
| Python orchestrator OOM | Kubernetes restarts pod. In-flight LangGraph state lost; Kafka message redelivered. |
| Graceful shutdown (SIGTERM) | Go workers: context cancellation + `sync.WaitGroup` drain of in-flight jobs. No message loss. |

---

### NFR-7: Data Consistency

| Aspect | Model |
|---|---|
| Ingestion pipeline | Eventual consistency. Kafka -> Go -> Python -> Neo4j is asynchronous. Graph state converges after all pending messages are processed. |
| Graph reads | Strong consistency within a single Neo4j primary via causal consistency bookmarks. |
| Cross-replica reads | Eventual consistency. Secondary replicas lag primary by replication delay (typically < 1 s). |
| Idempotency | Guaranteed by Cypher `MERGE` on unique-constrained properties. Duplicate ingestion produces identical graph state. |

---

### NFR-8: Observability SLAs

| Metric | Target |
|---|---|
| Mean Time to Detection (MTTD) | < 5 min for ingestion pipeline failures |
| Mean Time to Recovery (MTTR) | < 15 min for automated recovery scenarios |
| Trace sampling (errors) | 100% — every failed request is fully traced |
| Trace sampling (success, production) | 10% — sufficient for latency profiling without storage overhead |
| Log retention | 30 days (structured JSON, exported via OTLP) |
| Metric retention | 90 days (Prometheus TSDB or Grafana Mimir) |
| Alerting channels | PagerDuty (P1: DLQ rate > 10/min), Slack (P2: latency SLA breach) |
