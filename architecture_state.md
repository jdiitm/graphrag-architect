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
              │       │
              └───────┼────────────────────┘
                      │ HTTP POST
                      ▼
              ┌────────────────────────────┐
              │   Python Orchestrator      │
              │   (FastAPI + LangGraph)    │
              │                            │
              │  load_workspace            │
              │  parse_services  (Gemini)  │
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

- **Consumer**: Reads from `raw-documents` topic via franz-go.
- **Dispatcher**: Manages N goroutines, channels, and retry logic.
- **DocumentProcessor**: Interface for processing each job. `ForwardingProcessor` HTTP POSTs base64-encoded payloads to the Python orchestrator's `/ingest` endpoint.
- **DLQ Handler**: Routes permanently failed jobs to `raw-documents.dlq`.
- **Shutdown**: Graceful via `context.Context` + `sync.WaitGroup`. No message loss.

### 2. Python Orchestrator (`orchestrator/`)

LangGraph-based extraction pipeline.

- **ServiceExtractor**: Async LLM extraction (Gemini) of ServiceNode and CallsEdge from .go/.py files.
- **Graph Builder**: LangGraph ingestion DAG: load → parse_services → parse_manifests → validate → commit. All nodes instrumented with OpenTelemetry spans.
- **Query Engine**: LangGraph query DAG: classify → route → [vector|single_hop|cypher|hybrid] → synthesize. Agentic Cypher iteration (max 3), DRIFT-inspired hybrid retrieval.
- **Access Control**: SecurityPrincipal resolution from Authorization header. CypherPermissionFilter injects ACL WHERE clauses into Cypher queries.
- **Observability**: TracerProvider with OTLP gRPC exporter, FastAPI auto-instrumentation, 4 metric histograms.
- **Config**: ExtractionConfig (model, concurrency, token budget, retry), Neo4jConfig.

### 3. Infrastructure (`infrastructure/`)

- **Neo4j 5.26**: Knowledge graph store (ports 7474/7687).
- **Apache Kafka 3.9**: Event bus, KRaft mode (port 9092).

## Data Flow

1. Raw documents (source files, manifests) are published to `raw-documents` Kafka topic.
2. Go worker pool consumes, deserializes, and forwards to Python orchestrator.
3. Orchestrator runs LLM extraction → Pydantic models → Cypher → Neo4j.
4. Failed ingestions route to DLQ (`raw-documents.dlq`) for inspection.

## Graph Schema

**Nodes:** Service, Database, KafkaTopic, K8sDeployment
**Edges:** CALLS, PRODUCES, CONSUMES, DEPLOYED_IN
