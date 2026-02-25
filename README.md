# GraphRAG Architect

A production-grade GraphRAG system that analyzes distributed systems by building a knowledge graph from source code, infrastructure manifests, and message broker topologies. It answers multi-hop architectural questions using hybrid Vector + Cypher retrieval over Neo4j.

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                      LangGraph Orchestrator                   │
│                                                               │
│  load_workspace ─► parse_source_ast ─► enrich_with_llm       │
│                                             │                 │
│                                       parse_manifests         │
│                                             │                 │
│                                       validate_schema         │
│                                         ╱         ╲           │
│                                 fix_errors    commit_graph    │
└─────────────────────────────────────┬────────────────────────┘
                                      │
                       ┌──────────────┼──────────────┐
                       ▼              ▼              ▼
                    Neo4j          Kafka          Gemini
                 (Knowledge      (Event Bus)     (LLM Entity
                  Graph)                        Extraction)
```

**Extraction pipeline** — LLM-powered structured extraction from `.go` and `.py` source files into typed Pydantic models (`ServiceNode`, `CallsEdge`, etc.), committed as Neo4j nodes and relationships.

**Retrieval** — Hybrid VectorCypher approach: keyword classifier routes entity lookups to vector search, single-hop to vector + 1-hop Cypher, multi-hop to agentic iterative Cypher generation, and aggregate queries to DRIFT-inspired hybrid retrieval. LLM synthesizes natural-language answers from graph context.

**Access Control** — Zanzibar-inspired permission filtering: `SecurityPrincipal` resolved from request headers, `CypherPermissionFilter` injects ACL `WHERE` clauses into all Cypher queries at query-time. Permission metadata (`team_owner`, `namespace_acl`) persisted on graph nodes at ingestion-time.

**Observability** — OpenTelemetry distributed tracing across the full pipeline: FastAPI auto-instrumentation, manual spans on all 14 LangGraph DAG nodes, Go spans on Kafka poll/dispatch/process/DLQ/commit, trace context propagation via `traceparent` headers across Go-HTTP-Python boundary.

## Tech Stack

| Component | Technology |
|---|---|
| Orchestration | Python, LangGraph, FastAPI |
| Knowledge Graph | Neo4j 5.26 |
| Event Bus | Apache Kafka 3.9 (KRaft) |
| LLM Extraction | Gemini (via LangChain) |
| Data Models | Pydantic v2 |
| Ingestion Workers | Go (worker pool + DLQ) |

## Project Structure

```
graphrag-architect/
├── orchestrator/                        # Python LLM extraction pipeline
│   ├── app/                             # 62 Python modules
│   │   ├── access_control.py            # SecurityPrincipal, CypherPermissionFilter (FR-7)
│   │   ├── agentic_traversal.py         # Incremental 1-hop LLM-guided traversal (FR-15)
│   │   ├── ast_extraction.py            # Go/Python AST parsing for code extraction
│   │   ├── circuit_breaker.py           # Three-state circuit breaker (NFR-6)
│   │   ├── config.py                    # ExtractionConfig, Neo4jConfig, RedisConfig
│   │   ├── context_manager.py           # Token budget management for synthesis (FR-11)
│   │   ├── cypher_sandbox.py            # Cypher query sandboxing
│   │   ├── cypher_validator.py          # Read-only Cypher query validation
│   │   ├── entity_resolver.py           # Fuzzy cross-repo name deduplication
│   │   ├── extraction_models.py         # Pydantic schemas (ServiceNode, CallsEdge, etc.)
│   │   ├── graph_builder.py             # LangGraph ingestion DAG (7 nodes)
│   │   ├── graph_embeddings.py          # Node2Vec structural embeddings (FR-17)
│   │   ├── ingest_models.py             # IngestRequest/Response Pydantic models
│   │   ├── llm_extraction.py            # ServiceExtractor (filter, batch, extract)
│   │   ├── main.py                      # FastAPI endpoints (/health, /ingest, /query)
│   │   ├── manifest_parser.py           # K8s + Kafka YAML parsing
│   │   ├── neo4j_client.py              # Cypher MERGE operations
│   │   ├── neo4j_pool.py               # Connection pooling for Neo4j
│   │   ├── node_sink.py                # IncrementalNodeSink with namespace partitioning
│   │   ├── observability.py             # OpenTelemetry TracerProvider + metrics (FR-8)
│   │   ├── query_classifier.py          # Keyword-based query complexity classifier (FR-4)
│   │   ├── query_engine.py              # LangGraph query DAG (7 nodes) (FR-4)
│   │   ├── query_models.py              # QueryRequest/Response/State models (FR-4)
│   │   ├── query_templates.py           # Parameterized Cypher template catalog (FR-10)
│   │   ├── rag_evaluator.py             # Faithfulness/groundedness evaluation (FR-12)
│   │   ├── reranker.py                  # Cross-encoder/BM25 reranking (FR-11)
│   │   ├── schema_validation.py         # Pydantic validation + correction loop
│   │   ├── semantic_partitioner.py      # Leiden community detection partitioning (FR-18)
│   │   ├── tenant_isolation.py          # TenantAwareDriverPool (FR-9)
│   │   ├── vector_store.py              # VectorStore protocol + Neo4j/Qdrant (FR-13)
│   │   ├── workspace_loader.py          # Filesystem workspace scanner
│   │   └── schema_init.cypher           # Neo4j constraints and indexes
│   │   # ... plus 30 additional modules (guardrails, caching, audit, etc.)
│   ├── tests/                           # 117 test files, 1542 tests
│   │   ├── conftest.py                  # Shared fixtures (query state, mock helpers)
│   │   ├── test_access_control.py
│   │   ├── test_agentic_traversal.py
│   │   ├── test_circuit_breaker.py
│   │   ├── test_graph_builder.py
│   │   ├── test_graph_embeddings.py
│   │   ├── test_ingest_api.py
│   │   ├── test_integration.py
│   │   ├── test_manifest_parser.py
│   │   ├── test_neo4j_client.py
│   │   ├── test_observability.py
│   │   ├── test_query_api.py
│   │   ├── test_query_classifier.py
│   │   ├── test_query_engine.py
│   │   ├── test_query_templates.py
│   │   ├── test_rag_evaluator.py
│   │   ├── test_schema_validation.py
│   │   ├── test_semantic_partitioner.py
│   │   ├── test_service_extractor.py
│   │   ├── test_tenant_isolation.py
│   │   ├── test_vector_store.py
│   │   ├── test_workspace_loader.py
│   │   └── ... (96 more test files)
│   └── requirements.txt
├── workers/                             # Go high-throughput ingestion
│   └── ingestion/
│       ├── cmd/                          # Entry point + Kafka wiring
│       │   ├── main.go
│       │   ├── kafka.go
│       │   └── dlq_sink.go
│       ├── internal/
│       │   ├── blobstore/               # Blob storage abstraction
│       │   ├── consumer/consumer.go      # JobSource interface + Consumer loop
│       │   ├── dedup/                   # Content-hash deduplication
│       │   ├── dispatcher/              # Worker pool (dispatcher + tests)
│       │   ├── dlq/                     # Dead Letter Queue (handler + file sink)
│       │   ├── domain/job.go            # Job, Result value types
│       │   ├── healthz/                 # Liveness/readiness probes
│       │   ├── metrics/                 # Prometheus metrics (consumer lag, batch duration, DLQ, jobs)
│       │   ├── outbox/                  # Transactional outbox pattern
│       │   ├── parser/                  # Go/Python file AST parsing
│       │   ├── processor/              # DocumentProcessor interface (5 implementations)
│       │   ├── ratelimit/              # Token-bucket rate limiting
│       │   └── telemetry/              # OpenTelemetry TracerProvider + span helpers (FR-8)
│       └── go.mod
├── infrastructure/
│   ├── docker-compose.yml               # Neo4j + Kafka
│   ├── helm/graphrag/                   # Helm chart for production deployment
│   │   ├── Chart.yaml
│   │   ├── values.yaml
│   │   └── templates/
│   └── k8s/                             # Kubernetes deployment manifests
│       ├── namespace.yaml
│       ├── configmap.yaml
│       ├── secrets.yaml
│       ├── orchestrator-deployment.yaml
│       ├── ingestion-worker-deployment.yaml
│       ├── neo4j-statefulset.yaml
│       ├── kafka-statefulset.yaml
│       ├── hpa.yaml
│       ├── ingress.yaml
│       ├── network-policies.yaml
│       ├── alerting.yaml
│       └── neo4j-schema-job.yaml
├── architecture_state.md                # System design document
└── CLAUDE.md                            # AI agent invariants
```

## Prerequisites

- Python 3.12+
- Go 1.24+
- Docker and Docker Compose
- A Google API key with Gemini access

## Quickstart

### 1. Start infrastructure

```bash
cd infrastructure
docker compose up -d
```

This starts Neo4j (ports 7474/7687) and Kafka (port 9092).

### 2. Set up Python environment

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r orchestrator/requirements.txt
pip install pytest pytest-asyncio
```

### 3. Configure environment

```bash
export GOOGLE_API_KEY="your-gemini-api-key"

# Optional overrides
export EXTRACTION_MODEL="gemini-2.5-pro"           # default: gemini-2.0-flash
export EXTRACTION_MAX_CONCURRENCY="5"              # default: 5
export EXTRACTION_TOKEN_BUDGET="200000"            # default: 200000
export EXTRACTION_MAX_RETRIES="5"                  # default: 5
export EXTRACTION_RETRY_MIN_WAIT="1.0"             # default: 1.0s
export EXTRACTION_RETRY_MAX_WAIT="60.0"            # default: 60.0s
```

### 4. Run tests

```bash
# Python tests
python -m pytest orchestrator/tests/ -v

# Go tests
cd workers/ingestion && go test ./... -v
```

## Graph Schema

**Nodes:**
- `Service` — id, name, language, framework, opentelemetry_enabled, team_owner, namespace_acl
- `Database` — id, type, team_owner, namespace_acl
- `KafkaTopic` — name, partitions, retention_ms, team_owner, namespace_acl
- `K8sDeployment` — id, namespace, replicas, team_owner, namespace_acl

**Edges:**
- `CALLS` — source_service_id → target_service_id (protocol)
- `PRODUCES` — service_id → topic_name (event_schema)
- `CONSUMES` — service_id → topic_name (consumer_group)
- `DEPLOYED_IN` — service_id → deployment_id

## Development

This project follows strict TDD (Red-Green-Refactor). See `CLAUDE.md` for the agentic execution loop and coding invariants.
