# GraphRAG Architect

A production-grade GraphRAG system that analyzes distributed systems by building a knowledge graph from source code, infrastructure manifests, and message broker topologies. It answers multi-hop architectural questions using hybrid Vector + Cypher retrieval over Neo4j.

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                   LangGraph Orchestrator             │
│                                                      │
│  load_workspace ─► parse_services ─► parse_manifests │
│                                          │           │
│                                    validate_schema   │
│                                      ╱         ╲     │
│                              fix_errors    commit_graph
└──────────────────────────────────┬───────────────────┘
                                   │
                    ┌──────────────┼──────────────┐
                    ▼              ▼              ▼
                 Neo4j          Kafka          Gemini
              (Knowledge      (Event Bus)     (LLM Entity
               Graph)                        Extraction)
```

**Extraction pipeline** — LLM-powered structured extraction from `.go` and `.py` source files into typed Pydantic models (`ServiceNode`, `CallsEdge`, etc.), committed as Neo4j nodes and relationships.

**Retrieval** (planned) — Hybrid VectorCypher approach: simple lookups via vector search, complex structural queries via agentic Cypher graph traversals.

## Tech Stack

| Component | Technology |
|---|---|
| Orchestration | Python, LangGraph, FastAPI |
| Knowledge Graph | Neo4j 5.15 |
| Event Bus | Apache Kafka 3.9 (KRaft) |
| LLM Extraction | Gemini (via LangChain) |
| Data Models | Pydantic v2 |
| Ingestion Workers | Go (planned) |

## Project Structure

```
graphrag-architect/
├── orchestrator/
│   ├── app/
│   │   ├── config.py               # ExtractionConfig (model, concurrency, token budget)
│   │   ├── extraction_models.py     # Pydantic schemas (ServiceNode, CallsEdge, etc.)
│   │   ├── graph_builder.py         # LangGraph DAG definition
│   │   ├── llm_extraction.py        # ServiceExtractor (filter, batch, extract, deduplicate)
│   │   └── schema_init.cypher       # Neo4j constraints and indexes
│   ├── tests/
│   │   └── test_service_extractor.py
│   └── requirements.txt
├── infrastructure/
│   └── docker-compose.yml           # Neo4j + Kafka
├── CLAUDE.md                        # AI agent invariants
└── claude-progress.txt              # Development progress log
```

## Prerequisites

- Python 3.12+
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
export EXTRACTION_MODEL="gemini-2.5-pro"        # default: gemini-2.0-flash
export EXTRACTION_MAX_CONCURRENCY="5"            # default: 5
export EXTRACTION_TOKEN_BUDGET="200000"           # default: 200000
```

### 4. Run tests

```bash
python -m pytest orchestrator/tests/ -v
```

## Graph Schema

**Nodes:**
- `Service` — id, name, language, framework, opentelemetry_enabled
- `Database` — id, type
- `KafkaTopic` — name, partitions, retention_ms
- `K8sDeployment` — id, namespace, replicas

**Edges:**
- `CALLS` — source_service_id → target_service_id (protocol)
- `PRODUCES` — service_id → topic_name (event_schema)
- `CONSUMES` — service_id → topic_name (consumer_group)
- `DEPLOYED_IN` — service_id → deployment_id

## Development

This project follows strict TDD (Red-Green-Refactor). See `CLAUDE.md` for the agentic execution loop and coding invariants. Progress is tracked in `claude-progress.txt`.
