# 01 — Vision and Scope

> **Status:** Approved | **Version:** 1.0 | **Last Updated:** 2026-02-22

---

## 1. Executive Summary

**graphrag-architect** is an enterprise-grade knowledge engine that builds a queryable knowledge graph from distributed systems artifacts — source code, Kubernetes manifests, Kafka topic schemas, and database configurations. It answers multi-hop architectural questions through hybrid VectorCypher retrieval: simple entity lookups resolve via Neo4j vector indexes (ANN/Lucene), while complex structural queries trigger agentic Cypher graph traversals with LLM synthesis.

The system follows a three-stage pipeline adapted from the GraphRAG paradigm:

1. **Graph-Based Indexing** — Async ingestion via Kafka, Go worker pools, and LLM-powered entity extraction into a typed property graph.
2. **Graph-Guided Retrieval** — Hybrid routing that selects between vector similarity search and multi-hop Cypher traversals based on query complexity classification.
3. **Graph-Enhanced Generation** — LLM synthesis over retrieved subgraphs, producing natural-language answers grounded in the knowledge graph topology.

This is not a general-purpose enterprise search engine. It is purpose-built for infrastructure topology reasoning — failure propagation, blast radius estimation, dependency auditing, and backpressure analysis across distributed systems at scale.

---

## 2. Target Audience

| Persona | Need |
|---|---|
| **Platform Engineering** | Topology visibility across 50+ microservices; automated dependency mapping from code and manifests |
| **Site Reliability Engineering** | Incident root-cause analysis via multi-hop failure propagation queries; blast radius estimation before deployments |
| **Architecture Review Boards** | Change impact assessment; protocol and schema auditing across service boundaries |
| **Security Engineering** | Cross-service data flow auditing; identification of unauthenticated communication paths |
| **Developer Experience** | Natural-language queries against infrastructure topology without requiring Cypher or graph expertise |

---

## 3. Scope

### 3.1 In-Scope (Phase 1)

| Capability | Implementation Status |
|---|---|
| Async document ingestion via Kafka -> Go worker pool -> Python orchestrator | Dispatcher + DLQ handler implemented (`workers/ingestion/internal/`) |
| LLM-powered entity extraction: 4 node types (`Service`, `Database`, `KafkaTopic`, `K8sDeployment`), 4 edge types (`CALLS`, `PRODUCES`, `CONSUMES`, `DEPLOYED_IN`) | `ServiceExtractor` + Pydantic models implemented (`orchestrator/app/extraction_models.py`) |
| Knowledge graph storage in Neo4j with unique constraints and secondary indexes | Cypher schema implemented (`orchestrator/app/schema_init.cypher`) |
| LangGraph ingestion DAG with schema validation and correction loop | `StateGraph` compiled (`orchestrator/app/graph_builder.py`) |
| Dead Letter Queue fault tolerance with zero message loss | DLQ handler implemented (`workers/ingestion/internal/dlq/handler.go`) |
| Hybrid query routing: vector search for entity disambiguation, Cypher traversals for multi-hop reasoning | Implemented (`orchestrator/app/query_engine.py`, `query_classifier.py`) |
| OpenTelemetry instrumentation across Go and Python components | Implemented (`orchestrator/app/observability.py`, `workers/ingestion/internal/telemetry/`) |
| FastAPI HTTP interface accepting ingestion payloads and query requests | Implemented (`orchestrator/app/main.py`: `/health`, `/ingest`, `/query`) |

### 3.2 Out-of-Scope

These are explicit exclusions. They will not be accepted into the backlog without a formal scope change request.

| Exclusion | Rationale |
|---|---|
| Runtime APM / live metrics collection | Solved by Datadog, Grafana, Prometheus. This system operates on static artifacts, not live telemetry. |
| CI/CD pipeline execution or deployment automation | Orthogonal concern. Integrate via API consumers, not native features. |
| Cost optimization / FinOps analysis | Different data model and query patterns. |
| User-facing dashboards or visualization | Phase 1 is API-only. Visualization is a consumer of the API, not a core capability. |
| Real-time streaming topology updates | Phase 1 is batch ingestion. Streaming CDC from Neo4j is a Phase 2 consideration. |
| Multi-tenant isolation | Single-tenant in Phase 1. Tenant-scoped graph partitioning requires Neo4j Fabric or Infinigraph sharding. |

---

## 4. Core Use Cases

### UC-1: Cross-Service Failure Propagation Analysis

**Trigger:** "If the `auth-service` fails, what downstream services are impacted?"

**Mechanism:** Traverses `CALLS` edges via variable-length Cypher patterns. Uses bidirectional breadth-first search for shortest failure paths. Returns the ordered cascade of affected services with hop distance.

**Graph Pattern:**
```cypher
MATCH path = (root:Service {name: $name})-[:CALLS*1..5]->(downstream:Service)
RETURN downstream.name, length(path) AS hops
ORDER BY hops
```

**Value:** Incident responders get a pre-computed blast radius within seconds instead of manually tracing service dependencies during an outage.

---

### UC-2: Automated Topology Mapping

**Trigger:** Ingestion of a new codebase, Kubernetes namespace, or Kafka cluster configuration.

**Mechanism:** The LangGraph ingestion DAG processes raw files through `ServiceExtractor` (Gemini), producing a complete `SystemTopology` — all 4 node types and 4 edge types — committed to Neo4j via idempotent MERGE operations.

**Value:** Eliminates manual architecture diagramming. The knowledge graph is always consistent with the actual codebase and infrastructure configuration.

---

### UC-3: Kafka Backpressure Impact Analysis

**Trigger:** "If topic `order-events` experiences consumer lag, which services are affected?"

**Mechanism:** Traverses `PRODUCES` and `CONSUMES` edges from the target `KafkaTopic` node. Identifies all producer and consumer services, their consumer groups, and transitive dependencies via `CALLS`.

**Graph Pattern:**
```cypher
MATCH (producer:Service)-[:PRODUCES]->(t:KafkaTopic {name: $topic})<-[:CONSUMES]-(consumer:Service)
OPTIONAL MATCH (consumer)-[:CALLS*1..3]->(transitive:Service)
RETURN producer.name, consumer.name, collect(DISTINCT transitive.name) AS transitive_impact
```

**Value:** Kafka operators can assess the full downstream impact of topic-level issues before they cascade.

---

### UC-4: Kubernetes Blast Radius Estimation

**Trigger:** "What is the blast radius of redeploying the `payments` namespace?"

**Mechanism:** Identifies all services in the target namespace via `DEPLOYED_IN` edges from `K8sDeployment`, then fans out via `CALLS` edges to determine transitive service impact.

**Graph Pattern:**
```cypher
MATCH (d:K8sDeployment {namespace: $ns})<-[:DEPLOYED_IN]-(s:Service)
OPTIONAL MATCH (s)-[:CALLS*1..3]->(impacted:Service)
WHERE NOT (impacted)-[:DEPLOYED_IN]->(d)
RETURN s.name AS directly_affected, collect(DISTINCT impacted.name) AS transitive_impact
```

**Value:** Platform teams quantify deployment risk before rolling out changes to a namespace.

---

### UC-5: Service Protocol Audit

**Trigger:** "Which services communicate via gRPC vs REST/HTTP?"

**Mechanism:** Filters `CALLS` edges by the `protocol` property. Aggregates by protocol type and returns service pairs.

**Graph Pattern:**
```cypher
MATCH (a:Service)-[c:CALLS]->(b:Service)
RETURN c.protocol, count(*) AS edge_count, collect([a.name, b.name]) AS pairs
ORDER BY edge_count DESC
```

**Value:** Architecture review boards enforce protocol standards and identify migration candidates.

---

### UC-6: Database Dependency Mapping

**Trigger:** "Which services depend on the `users-db` PostgreSQL instance, and what events do they produce?"

**Mechanism:** Multi-hop traversal from `Database` through connected `Service` nodes to `KafkaTopic` nodes via `PRODUCES` edges. Surfaces the full data flow from database to event bus.

**Graph Pattern:**
```cypher
MATCH (db:Database {id: $db_id})<-[:WRITES_TO]-(s:Service)
OPTIONAL MATCH (s)-[p:PRODUCES]->(t:KafkaTopic)
RETURN s.name, s.language, t.name AS topic, p.event_schema
```

**Value:** Database teams understand the full downstream impact of schema migrations or failovers.
