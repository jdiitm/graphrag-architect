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
