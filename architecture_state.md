# GraphRAG Architect — System Architecture

## Overview

GraphRAG Architect is a Glean-class enterprise knowledge engine that builds a
knowledge graph from distributed system artifacts (source code, Kubernetes
manifests, Kafka topologies) and answers multi-hop architectural questions via
hybrid Vector + Cypher retrieval over Neo4j.

## Component Map

```
                ┌──────────────────────────────────┐
                │        Kafka (Event Bus)         │
                │  ┌──────────┐  ┌──────────────┐  │
                │  │raw-docs  │  │raw-docs.dlq  │  │
                │  └────┬─────┘  └──────▲───────┘  │
                └───────┼───────────────┼──────────┘
                        │               │
                        ▼               │
              ┌─────────────────────────┼──┐
              │   Go Ingestion Workers     │
              │                            │
              │  KafkaConsumer             │
              │       │                    │
              │  Dispatcher (worker pool)  │
              │       │          │         │
              │  Processor    DLQ Handler──┘
              │   │      │
              └───┼──────┼───────────────────┘
                  │      │ PROCESSOR_MODE=kafka
                  │      ▼
                  │  ┌──────────────┐
                  │  │parsed-docs   │ (Kafka topic)
                  │  └──────┬───────┘
                  │         │
                  │ HTTP    │ Consume
                  ▼  POST  ▼
              ┌────────────────────────────┐
              │   Python Orchestrator      │
              │   (FastAPI + LangGraph)    │
              │                            │
              │  load_workspace            │
              │  parse_source_ast          │
              │  enrich_with_llm (Gemini)  │
              │  parse_manifests           │
              │  validate_schema           │
              │  commit_graph              │
              └────────────┬───────────────┘
                           │ Cypher
                           ▼
              ┌────────────────────────────┐
              │        Neo4j               │
              │   (Knowledge Graph)        │
              │                            │
              │  :Service :Database         │
              │  :KafkaTopic :K8sDeployment│
              │  :CALLS :PRODUCES :CONSUMES│
              └────────────────────────────┘
```

## Components

### 1. Go Ingestion Workers (`workers/ingestion/`)

High-throughput Kafka consumer with a concurrent worker pool.

- **Consumer**: Reads from `raw-documents` topic via franz-go. Ack timeouts are non-fatal (consumer stays alive).
- **Dispatcher**: Manages N goroutines, channels, and retry logic.
- **DocumentProcessor**: Interface with five implementations: `ForwardingProcessor` (HTTP POST), `KafkaForwardingProcessor` (writes to parsed-documents topic), `BlobForwardingProcessor` (blob store + event), `ASTProcessor` (Go AST parsing), and `StageAndEmitProcessor` (staging + event). Controlled by `PROCESSOR_MODE` env var.
- **DLQ Handler**: Routes permanently failed jobs to `raw-documents.dlq` with file fallback.
- **Blobstore**: Blob storage abstraction with in-memory implementation for testing.
- **Dedup**: Content-hash deduplication to prevent redundant processing.
- **Healthz**: Liveness/readiness probe checker.
- **Outbox**: Transactional outbox pattern for reliable event publishing.
- **Rate Limiter**: Token-bucket rate limiting for downstream calls.
- **Shutdown**: Graceful via `context.Context` + `sync.WaitGroup`. No message loss.

### 2. Python Orchestrator (`orchestrator/`)

LangGraph-based extraction pipeline.

- **ServiceExtractor**: Async LLM extraction (Gemini) of ServiceNode and CallsEdge from .go/.py files.
- **Graph Builder**: LangGraph ingestion DAG: load → parse_source_ast → enrich_with_llm → parse_manifests → validate → fix_errors → commit. Streaming chunked loading with configurable max bytes.
- **Entity Resolver**: Fuzzy cross-repo name matching (Levenshtein-based) to deduplicate services across repositories before commit.
- **Query Engine**: LangGraph query DAG: classify → route → [vector|single_hop|cypher|hybrid] → synthesize. Agentic Cypher iteration (max 3), DRIFT-inspired hybrid retrieval. Async job-based execution via `POST /query?async_mode=true` + `GET /query/{job_id}`.
- **Query Cost Estimation**: Pre-execution Cypher complexity analysis — rejects unbounded variable-length paths, enforces max depth.
- **Access Control**: Fail-closed authentication (token verification mandatory when token provided). AST-based ACL injection into ALL MATCH clauses (including subqueries and unions). Post-injection `validate_acl_coverage` confirms every MATCH has ACL.
- **Tenant Isolation**: `TenantAwareDriverPool` with LOGICAL (WHERE clause) and PHYSICAL (separate Neo4j database per tenant) modes. `GraphRepository` passes tenant database to session creation.
- **Circuit Breaker**: Supports `InMemoryStateStore` (dev) and `RedisStateStore` (production) for cross-pod state sharing.
- **Caching**: `RedisSubgraphCache` with L1 (process-local) / L2 (Redis) tiering. `SemanticQueryCache` for embedding similarity.
- **Node Sink**: `IncrementalNodeSink` with namespace-partitioned parallel writes for reduced lock contention.
- **Query Templates**: 9 parameterized Cypher templates (blast_radius, dependency_count, service_neighbors, topic_consumers, topic_producers, service_deployments, namespace_services, service_databases, cross_team_dependencies).
- **Observability**: TracerProvider with OTLP gRPC exporter, FastAPI auto-instrumentation, 4 metric histograms.
- **Config**: ExtractionConfig, Neo4jConfig, RedisConfig, RateLimitConfig, VectorStoreConfig, EmbeddingConfig.

### 3. Infrastructure (`infrastructure/`)

- **Neo4j 5.26**: Knowledge graph store (ports 7474/7687). Multi-database support for physical tenant isolation.
- **Apache Kafka 3.9**: Event bus, KRaft mode (port 9092). Topics: `raw-documents`, `raw-documents.dlq`, `graphrag.parsed`.
- **Redis**: Distributed state store for circuit breaker, subgraph cache, and rate limiting (optional; in-memory fallback in dev).
- **K8s**: StatefulSets for Kafka (Downward API for `POD_NAME`), Deployments for orchestrator (HPA-ready), NetworkPolicies for ingress/egress control.

## Data Flow

1. Raw documents (source files, manifests) are published to `raw-documents` Kafka topic.
2. Go worker pool consumes, deserializes, and forwards via HTTP POST or Kafka `parsed-documents` topic (configurable).
3. Orchestrator runs LLM extraction → entity resolution → Pydantic models → namespace-partitioned Cypher → Neo4j.
4. Failed ingestions route to DLQ (`raw-documents.dlq`) with file-based fallback.

## Graph Schema

**Nodes:** Service, Database, KafkaTopic, K8sDeployment (all carry `team_owner`, `namespace_acl`)
**Edges:** CALLS, PRODUCES, CONSUMES, DEPLOYED_IN (all carry `ingestion_id`, `last_seen_at`)

## Feature Requirements Status

| FR | Component | Status | Module |
|---|---|---|---|
| FR-9 | Physical Tenant Isolation | Implemented | `tenant_isolation.py`, `extraction_models.py` |
| FR-10 | Parameterized Query Templates | Implemented | `query_templates.py`, `query_engine.py` |
| FR-11 | Context Token Budget + Reranking | Implemented | `context_manager.py`, `reranker.py` |
| FR-12 | Faithfulness RAG Evaluation | Implemented | `rag_evaluator.py` |
| FR-13 | Decoupled Vector Store (Qdrant) | Implemented | `vector_store.py` |
| FR-14 | Streaming Ingestion Pipeline | Implemented | `graph_builder.py` |
| FR-15 | Agentic Graph Traversal | Implemented | `agentic_traversal.py` |
| FR-16 | Tombstone Edge Cleanup | Implemented | `neo4j_client.py`, `node_sink.py` |
| FR-17 | Graph-Native Embeddings (Node2Vec) | Implemented | `graph_embeddings.py` |
| FR-18 | Semantic Graph Partitioning | Implemented | `semantic_partitioner.py` |

## Test Coverage

- **Python**: 1542 tests (unit + integration)
- **Go**: 152 tests across 13 packages
- **Quality gates**: Pylint 10.00/10, all Python tests, all Go tests
