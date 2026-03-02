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

**Extraction pipeline** — LLM-powered structured extraction from `.go` and `.py` source files into typed Pydantic models (`ServiceNode`, `CallsEdge`, etc.), committed as Neo4j nodes and relationships.

**Retrieval** — Hybrid VectorCypher approach: keyword classifier routes entity lookups to vector search, single-hop to vector + 1-hop Cypher, multi-hop to agentic iterative Cypher generation, and aggregate queries to DRIFT-inspired hybrid retrieval. LLM synthesizes natural-language answers from graph context.

**Access Control** — Zanzibar-inspired permission filtering: `SecurityPrincipal` resolved from request headers, `CypherPermissionFilter` injects ACL `WHERE` clauses into all Cypher queries at query-time. Permission metadata (`team_owner`, `namespace_acl`) persisted on graph nodes at ingestion-time.

**Observability** — OpenTelemetry distributed tracing across the full pipeline: FastAPI auto-instrumentation, manual spans on all 14 LangGraph DAG nodes, Go spans on Kafka poll/dispatch/process/DLQ/commit, trace context propagation via `traceparent` headers across Go-HTTP-Python boundary.

## Tech Stack

| Component | Technology |
|---|---|
| Orchestration | Python 3.12, LangGraph, FastAPI |
| Knowledge Graph | Neo4j 5.26 |
| Event Bus | Apache Kafka 3.9 (KRaft) |
| LLM Providers | Gemini (extraction, via LangChain), Claude (complex synthesis) |
| Data Models | Pydantic v2 |
| Ingestion Workers | Go 1.24 (franz-go, worker pool + DLQ) |
| Vector Search | Neo4j native / Qdrant |
| Caching | Redis |

## Project Structure

```
graphrag-architect/
├── orchestrator/               # Python orchestrator (84 modules, 3,299 tests)
│   ├── app/                    # LangGraph DAGs, FastAPI, Neo4j client, LLM extraction
│   ├── tests/                  # 260 test files (unit, integration, security defeat)
│   └── requirements.txt
├── workers/ingestion/          # Go high-throughput Kafka ingestion workers
│   ├── cmd/                    # Entry point, Kafka wiring, DLQ sink
│   └── internal/               # consumer, dispatcher, processor, dlq, blobstore, dedup, metrics
├── infrastructure/
│   ├── docker-compose.yml      # Neo4j, Kafka, Redis, Postgres (local dev)
│   ├── helm/graphrag/          # Helm chart for production deployment
│   └── k8s/                    # Raw Kubernetes manifests (11 files)
├── docs/SPEC.md                # Consolidated system specification
└── CLAUDE.md                   # AI agent invariants and TDD protocol
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

This starts Neo4j (ports 7474/7687), Kafka (port 9092), Redis (port 6379), and Postgres (port 5432).

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

# Optional orchestrator overrides
export NEO4J_URI="bolt://localhost:7687"              # default: bolt://localhost:7687
export REDIS_URL="redis://localhost:6379"              # default: redis://localhost:6379
export EXTRACTION_MODEL="gemini-2.5-pro"               # default: gemini-2.0-flash
export EXTRACTION_MAX_CONCURRENCY="5"                  # default: 5
export EXTRACTION_TOKEN_BUDGET="200000"                # default: 200000

# Optional Go worker overrides
export KAFKA_BROKERS="localhost:9092"                   # default: localhost:9092
export ORCHESTRATOR_URL="http://localhost:8000"         # default: http://localhost:8000
export PROCESSOR_MODE="kafka"                           # kafka | ast | http
```

### 4. Run tests

```bash
# Python tests
python -m pytest orchestrator/tests/ -v

# Go tests
cd workers/ingestion && go test ./... -v
```

## Development

This project follows strict TDD (Red-Green-Refactor). See `CLAUDE.md` for the agentic execution loop and coding invariants. Quality gates (pylint, pytest, go test, golangci-lint) are enforced before every push via `.cursor/rules/pre-push-tests.mdc`.
