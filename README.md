# GraphRAG Architect

A production-grade GraphRAG system that analyzes distributed systems by building a knowledge graph from source code, infrastructure manifests, and message broker topologies. It answers multi-hop architectural questions using hybrid Vector + Cypher retrieval over Neo4j.

For the full system specification — data model, functional requirements, security architecture, SLOs, and roadmap — see [docs/SPEC.md](docs/SPEC.md).

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
                    Neo4j          Kafka        Gemini / Claude
                 (Knowledge      (Event Bus)     (LLM Entity
                  Graph)                       Extraction +
                                               Synthesis)
```

**Extraction pipeline** — LLM-powered structured extraction from `.go` and `.py` source files into typed Pydantic models (`ServiceNode`, `CallsEdge`, etc.), committed as Neo4j nodes and relationships. Secret scanning and prompt injection defense integrated into the pipeline.

**Retrieval** — Hybrid VectorCypher approach: keyword classifier routes entity lookups to vector search, single-hop to vector + 1-hop Cypher, multi-hop to agentic iterative Cypher generation, and aggregate queries to DRIFT-inspired hybrid retrieval. LLM synthesizes natural-language answers from graph context.

**Access Control** — Zanzibar-inspired permission filtering: `SecurityPrincipal` resolved from request headers, `CypherPermissionFilter` injects ACL `WHERE` clauses into all Cypher queries at query-time. HMAC request signing between Go workers and Python orchestrator. Dual-key rotation with grace period.

**Observability** — OpenTelemetry distributed tracing across the full pipeline: FastAPI auto-instrumentation, manual spans on all 14 LangGraph DAG nodes, Go spans on Kafka poll/dispatch/process/DLQ/commit, trace context propagation via `traceparent` headers across Go-HTTP-Python boundary. SLO-based error budgets with Prometheus recording rules and 8 Grafana dashboards.

## Tech Stack

| Component | Technology |
|---|---|
| Orchestration | Python 3.12, LangGraph, FastAPI |
| Knowledge Graph | Neo4j 5.26 Enterprise (3+2 cluster) |
| Event Bus | Apache Kafka 3.9 (KRaft, SASL_SSL) |
| Schema Registry | Confluent Schema Registry (Avro) |
| LLM Providers | Gemini (extraction), Claude (synthesis) |
| Data Models | Pydantic v2 |
| Ingestion Workers | Go 1.24 (franz-go, worker pool + DLQ) |
| Vector Search | Neo4j native / Qdrant |
| Caching | Redis |
| Secret Management | HashiCorp Vault (Agent sidecar) |
| Certificates | cert-manager (mTLS auto-rotation) |
| Deployments | Argo Rollouts (canary), Chaos Mesh |

## Project Structure

```
graphrag-architect/
├── orchestrator/                  # Python orchestrator (100 modules, 4,140 tests)
│   ├── app/                      # LangGraph DAGs, FastAPI, Neo4j, LLM extraction
│   │   ├── main.py               # FastAPI app: /ingest, /query, /health, /metrics
│   │   ├── graph_builder.py      # LangGraph ingestion DAG + streaming pipeline
│   │   ├── query_engine.py       # Hybrid VectorCypher retrieval
│   │   ├── access_control.py     # JWT auth, ACL filtering, dual-key rotation
│   │   ├── secret_scanner.py     # Pattern-based secret detection in extraction
│   │   ├── slo_rules.py          # SLO definitions + Prometheus recording rules
│   │   ├── vault_provider.py     # HashiCorp Vault secret fetching
│   │   ├── gdpr.py               # GDPR data export + erasure endpoints
│   │   ├── data_residency.py     # Tenant region routing + data residency
│   │   └── plugins/base.py       # Extensible plugin architecture
│   ├── tests/                    # 318 test files (unit, e2e, contract, security)
│   │   ├── e2e/                  # Testcontainers-based E2E (Kafka → Go → Python → Neo4j)
│   │   └── contract/             # HTTP API contract verification (Go ↔ Python)
│   └── requirements.txt
├── workers/ingestion/             # Go Kafka consumers (14 packages, 28 test files)
│   ├── cmd/                      # Entry point, Kafka wiring, DLQ sink
│   └── internal/                 # consumer, dispatcher, processor, dlq, dedup, metrics
├── infrastructure/
│   ├── docker-compose.yml        # Neo4j, Kafka, Redis, Postgres, Schema Registry
│   ├── helm/graphrag/            # Helm chart for production deployment
│   ├── k8s/                      # 20 Kubernetes manifests
│   ├── grafana/                  # 8 Grafana dashboard JSON models
│   ├── chaos/                    # 4 Chaos Mesh resilience experiments
│   ├── schema-registry/          # Avro schemas (ingestion-message.avsc)
│   └── terraform/kafka/          # AWS MSK Terraform module
├── tests/
│   ├── load/                     # k6 load tests (ingestion, query, soak)
│   └── benchmarks/               # Go + Python performance benchmarks
├── docs/
│   ├── SPEC.md                   # Canonical system specification
│   ├── runbooks/                 # 5 operational runbooks (neo4j, kafka, orchestrator, workers, migration)
│   ├── adr/                      # 5 Architecture Decision Records
│   ├── incident-response.md      # Severity levels, escalation, post-mortem template
│   ├── schema-migration.md       # Migration procedures + rollback
│   ├── capacity-planning.md      # 1x/10x/100x resource calculations
│   ├── multi-region-architecture.md
│   ├── compliance/soc2-gap-analysis.md
│   └── legal/dpa.md              # Data Processing Agreement template
├── sdks/                         # OpenAPI generator config for client SDKs
├── .github/workflows/ci.yml     # 10-job CI pipeline
├── .gitleaks.toml                # Secret scanning configuration
└── CLAUDE.md                     # AI agent invariants and TDD protocol
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

This starts Neo4j (ports 7474/7687), Kafka (port 9092), Redis (port 6379), Postgres (port 5432), and Schema Registry (port 8081).

### 2. Set up Python environment

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r orchestrator/requirements.txt
```

### 3. Configure environment

```bash
export GOOGLE_API_KEY="your-gemini-api-key"

# Optional orchestrator overrides
export NEO4J_URI="bolt://localhost:7687"
export REDIS_URL="redis://localhost:6379"
export EXTRACTION_MODEL="gemini-2.5-pro"
export EXTRACTION_MAX_CONCURRENCY="5"
export EXTRACTION_TOKEN_BUDGET="200000"

# Optional Go worker overrides
export KAFKA_BROKERS="localhost:9092"
export ORCHESTRATOR_URL="http://localhost:8000"
export PROCESSOR_MODE="kafka"                     # kafka | ast | http
```

### 4. Run tests

```bash
# Python tests (4,140 tests)
source .venv/bin/activate
python -m pytest orchestrator/tests/ -v

# Go tests (14 packages)
cd workers/ingestion && go test ./... -v -race
```

## CI Pipeline

The GitHub Actions CI runs 10 jobs on every push and PR:

| Gate | Tool | Scope |
|---|---|---|
| Python lint | pylint | `orchestrator/` |
| Python type check | mypy --strict | `orchestrator/app/` |
| Python lint (style) | ruff | `orchestrator/` |
| Python tests | pytest | 4,140 tests |
| Go tests | go test -race | 14 packages |
| Go lint | golangci-lint | `workers/ingestion/` |
| Secret scanning | gitleaks | Full repo history |
| Security scanning | Trivy + SBOM | Filesystem + dependencies |
| Docker build + scan | Trivy image scan | orchestrator + ingestion-worker images |
| Integration tests | pytest + services | Kafka + Neo4j (Testcontainers) |

## Development

This project follows strict TDD (Red-Green-Refactor). See `CLAUDE.md` for the agentic execution loop and coding invariants.

### Quality Gates (pre-push)

All four gates must pass before any push. Defined in `.cursor/rules/pre-push-tests.mdc`:

```bash
source .venv/bin/activate

pylint orchestrator/                                              # Must score 10/10
python -m pytest orchestrator/tests/ -v                           # All tests green
cd workers/ingestion && go test ./... -v -count=1 -timeout 30s    # All packages pass
cd workers/ingestion && golangci-lint run ./...                    # Zero issues
```

## Documentation

| Document | Purpose |
|---|---|
| [SPEC.md](docs/SPEC.md) | Canonical system specification (architecture, data model, security, SLOs, roadmap) |
| [Runbooks](docs/runbooks/) | Operational procedures for Neo4j, Kafka, orchestrator, ingestion workers, Kafka migration |
| [ADRs](docs/adr/) | Architecture Decision Records (graph DB, event-driven ingestion, hybrid retrieval, multi-LLM, K8s) |
| [Incident Response](docs/incident-response.md) | Severity levels, escalation matrix, post-mortem template |
| [Schema Migration](docs/schema-migration.md) | Neo4j schema migration procedures and rollback |
| [Capacity Planning](docs/capacity-planning.md) | 1x / 10x / 100x resource calculations and cost estimates |
| [Multi-Region](docs/multi-region-architecture.md) | Cross-region replication, failover, tenant-affinity routing |
| [SOC2 Gap Analysis](docs/compliance/soc2-gap-analysis.md) | Trust Services Criteria mapping and control evidence |
| [Chaos Experiments](infrastructure/chaos/README.md) | Chaos Mesh experiments for resilience validation |
