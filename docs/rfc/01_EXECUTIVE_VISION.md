# RFC-001: Executive Vision and Strategic Positioning

> **Status:** Draft | **Authors:** Architecture Team | **Reviewers:** Engineering Leadership, CTO
> **Created:** 2026-02-23 | **Last Updated:** 2026-02-23

---

## Abstract

This document defines the long-term mission, strategic differentiation, competitive positioning, and multi-year roadmap for **graphrag-architect** — an infrastructure topology intelligence platform that transforms static distributed systems artifacts into a queryable knowledge graph. It establishes the product's strategic moat, evaluates build-vs-buy tradeoffs, and charts the transition from single-tenant prototype to globally deployable enterprise SaaS.

---

## 1. Mission Statement

**graphrag-architect** exists to make distributed systems legible.

Modern infrastructure sprawls across hundreds of microservices, dozens of Kafka topics, multiple Kubernetes namespaces, and heterogeneous data stores. The relationships between these components — who calls whom, which services produce to which topics, what the blast radius of a namespace redeployment is — live in the heads of senior engineers or scattered across stale Confluence pages.

graphrag-architect builds a living, queryable knowledge graph from the artifacts that define these systems: source code, Kubernetes manifests, Kafka topic configurations, and database schemas. It answers multi-hop architectural questions that no existing tool can answer without manual investigation.

**The mission:** Reduce mean-time-to-understanding (MTTU) for distributed system topology from hours of manual trace-following to seconds of natural-language querying.

---

## 2. Strategic Moat

The platform's defensibility rests on three interlocking advantages:

### 2.1 Hybrid VectorCypher Retrieval

Unlike pure-vector RAG systems that retrieve disconnected chunks, graphrag-architect combines vector similarity search (for entity disambiguation) with typed Cypher graph traversals (for structural reasoning). This hybrid approach is the only architecture capable of answering questions like "If the auth-service fails, which Kafka topics experience backpressure and what consumer groups are affected?" — questions that require multi-hop relational reasoning over a typed property graph.

Academic validation: Practical GraphRAG (arXiv:2507.03226, 2025) demonstrates 4-15% improvement over vector-only baselines on multi-hop retrieval tasks. HetaRAG (arXiv:2509.21336, 2025) confirms that fusing heterogeneous data stores (vector + graph + full-text) outperforms any single retrieval modality.

### 2.2 Domain-Specific Graph Schema

The graph schema is purpose-built for infrastructure topology: `Service`, `Database`, `KafkaTopic`, `K8sDeployment` as first-class node types with typed relationships (`CALLS`, `PRODUCES`, `CONSUMES`, `DEPLOYED_IN`). This domain specificity enables:

- Deterministic extraction: LLM extraction prompts are tuned for infrastructure artifacts, not general-purpose text.
- Typed traversals: Query routing can exploit schema semantics (e.g., "blast radius" always means variable-length `CALLS` traversal).
- Permission modeling: ACLs map naturally to `team_owner` and `namespace_acl` properties on graph nodes.

Generic knowledge graph tools (Neo4j Bloom, AWS Neptune) require customers to design their own ontology. graphrag-architect ships with one.

### 2.3 Async Ingestion at Kafka Scale

The Go worker pool + Kafka pipeline processes infrastructure artifacts at 10,000+ documents/minute with at-least-once delivery semantics and zero-loss DLQ fault tolerance. This is not a batch-import tool that runs weekly — it is a continuous ingestion pipeline that keeps the knowledge graph synchronized with the evolving codebase.

---

## 3. Competitive Differentiation

| Dimension | graphrag-architect | Microsoft GraphRAG | Neo4j Bloom | ServiceNow CMDB | Backstage | Cortex |
|---|---|---|---|---|---|---|
| **Purpose** | Infrastructure topology intelligence | General-purpose document RAG | Graph visualization | IT asset management | Developer portal / catalog | Internal developer portal |
| **Graph Construction** | Automated LLM extraction from code + manifests | Automated from documents (general) | Manual or import | Manual entry / discovery agents | Manual YAML registration | Manual YAML + API |
| **Query Model** | Hybrid VectorCypher with agentic Cypher generation | Hierarchical community search (Leiden) | Visual exploration | CMDB queries (ServiceNow QL) | Catalog search | Catalog search |
| **Multi-Hop Reasoning** | Native (Cypher traversal with LLM synthesis) | Community-level summarization | Manual traversal | Limited (flat relationships) | None | None |
| **Ingestion Model** | Continuous Kafka pipeline (at-least-once) | Batch indexing | N/A | Agent-based discovery | Git push hooks | Git push hooks |
| **Infrastructure Awareness** | Native (K8s, Kafka, service mesh) | None (domain-agnostic) | None (generic graph) | Partial (CMDB schema) | Service catalog only | Service catalog only |
| **Access Control** | Graph-aware RBAC (ACL on nodes + edges) | Document-level ACL | Neo4j native roles | ServiceNow ACL | None (catalog is public) | Team-based visibility |
| **Pricing Model** | Self-hosted or managed SaaS | Azure consumption | Neo4j Enterprise license | ServiceNow platform license | Open-source + plugins | SaaS per-service |

### Key Differentiators

1. **vs. Microsoft GraphRAG:** Microsoft GraphRAG uses Leiden community detection for hierarchical summarization of general documents. graphrag-architect uses a typed infrastructure schema with deterministic Cypher traversals. Microsoft's approach excels at "summarize this corpus" queries; ours excels at "trace the failure propagation path from service A to service Z through Kafka topics."

2. **vs. ServiceNow CMDB:** CMDBs require manual data entry or brittle discovery agents. graphrag-architect extracts topology directly from source code and infrastructure-as-code, making the graph automatically consistent with the actual system state.

3. **vs. Backstage/Cortex:** Developer portals catalog services but do not model relationships. They cannot answer multi-hop topology questions because they store flat metadata, not a graph.

---

## 4. Platform vs. Tool Strategy

### Phase 1 (Current): Purpose-Built Tool

graphrag-architect is currently a vertical tool: it ingests infrastructure artifacts and answers topology questions. This is the correct starting point — it validates the core value proposition without premature abstraction.

### Phase 2-3: Extensible Platform

The tool becomes a platform through three extension axes:

1. **Custom Extractors:** Third parties register `EntityExtractor` implementations for domain-specific artifact types (Terraform resources, Docker Compose services, Helm charts, Pulumi programs, CloudFormation templates). The extraction interface is stable; the entity types are extensible.

2. **Custom Query Strategies:** New LangGraph nodes can be registered as query routing paths. A financial services customer might add a "compliance traversal" path that checks regulatory metadata on every edge in a dependency chain.

3. **Custom Sinks and Sources:** Beyond Kafka, ingestion can accept artifacts from GitHub webhooks, GitLab CI pipelines, ArgoCD sync events, or Terraform Cloud run outputs. The `DocumentProcessor` interface in Go and `IngestDocument` model in Python are the stable contracts.

### Phase 4-5: Ecosystem and Standard

At maturity, graphrag-architect becomes the standard format for infrastructure topology intelligence:

- **Marketplace:** Community-contributed extractors, query strategies, and integrations.
- **Export Format:** The graph schema becomes a portable standard (analogous to OpenTelemetry for observability or CycloneDX for SBOMs).
- **Federation:** Multiple graphrag-architect instances across organizations can federate queries across organizational boundaries (e.g., a platform team querying the topology of a partner's API surface).

---

## 5. Three-to-Five Year Roadmap

### Year 1: Foundation to Production (2026)

| Quarter | Milestone | Key Deliverables |
|---|---|---|
| Q1 | Audit Remediation + Beta | Fix 7 audit findings, vector embedding pipeline, basic SLOs |
| Q2 | Production Launch | Multi-tenant logical isolation, API versioning, load testing, Grafana dashboards |
| Q3 | Enterprise Hardening | Physical tenant isolation, SDK (Python + Go + TypeScript), plugin architecture |
| Q4 | SOC2 + Scale | SOC2 Type II audit, Neo4j Infinigraph evaluation, multi-region planning |

### Year 2: Enterprise SaaS (2027)

| Quarter | Milestone | Key Deliverables |
|---|---|---|
| Q1 | Multi-Region GA | Active-passive deployment, cross-region replication, tenant affinity routing |
| Q2 | Real-Time CDC | Streaming topology updates via Neo4j CDC, WebSocket subscriptions |
| Q3 | Marketplace Launch | Community extractor registry, integration marketplace, developer documentation portal |
| Q4 | Federation Protocol | Cross-organization graph federation, privacy-preserving query routing |

### Year 3-5: Industry Standard (2028-2030)

- Open-source the graph schema specification as an industry standard for infrastructure topology representation.
- Contribute to CNCF as a sandbox project for infrastructure intelligence.
- Build partnerships with major cloud providers for native integration (AWS, GCP, Azure marketplace listings).
- Expand beyond infrastructure: application dependency graphs, data lineage graphs, security posture graphs — all sharing the same hybrid VectorCypher retrieval engine.

---

## 6. Build vs. Buy Analysis

| Component | Current Choice | Alternative | Decision | Rationale |
|---|---|---|---|---|
| **Graph Database** | Neo4j (self-managed) | Amazon Neptune, TigerGraph, Neo4j AuraDB (managed) | **Build (self-managed Neo4j)** | Full control over schema, indexing, and clustering. AuraDB lacks Infinigraph property sharding. Neptune uses Gremlin, not Cypher. Migrate to AuraDB when Infinigraph support lands. |
| **Event Bus** | Apache Kafka (self-managed) | Amazon MSK, Confluent Cloud, Apache Pulsar | **Buy (managed Kafka) for production** | Self-managed Kafka in dev for cost. Production deployments should use MSK or Confluent to eliminate operational burden of KRaft consensus, partition rebalancing, and broker maintenance. |
| **LLM Provider** | Google Gemini | OpenAI GPT-4, Anthropic Claude, self-hosted (Llama) | **Build (multi-provider)** | Gemini for extraction (fast structured output), Claude for complex synthesis (superior reasoning). No single-provider lock-in. Self-hosted Llama for air-gapped enterprise deployments. |
| **Observability** | OpenTelemetry + Prometheus | Datadog, Grafana Cloud, New Relic | **Build (OTEL) + Buy (Grafana Cloud) for visualization** | OTEL is the open standard — zero vendor lock-in for instrumentation. Grafana Cloud for dashboards, alerting, and long-term metric storage. |
| **Container Orchestration** | Kubernetes (self-managed manifests) | EKS/GKE (managed K8s), AWS ECS, Nomad | **Buy (managed K8s)** | EKS or GKE eliminates control plane management. Current K8s manifests are portable across managed providers. |
| **Secret Management** | K8s Secrets (plaintext base64) | HashiCorp Vault, AWS Secrets Manager, External Secrets Operator | **Buy (Vault or AWS SM)** | K8s Secrets are not encrypted at rest by default. Production requires a proper secret management solution with rotation, audit trails, and access policies. |
| **Vector Search** | Neo4j native vector index (planned) | Pinecone, Weaviate, Qdrant, pgvector | **Build (Neo4j native)** | Keeping vectors co-located with the graph eliminates a network hop on hybrid queries and simplifies consistency. Neo4j 5.x vector indexes use HNSW with configurable similarity functions. |

---

## 7. Market Positioning

### 7.1 Category: Infrastructure Intelligence as a Service (IIaaS)

graphrag-architect creates a new category. Existing categories address adjacent problems:

- **Observability** (Datadog, Grafana): Answers "what is happening right now?" from live telemetry.
- **Developer Portals** (Backstage, Cortex): Answers "what services exist?" from manual registration.
- **CMDBs** (ServiceNow): Answers "what assets do we own?" from manual or agent-based discovery.

graphrag-architect answers "how is everything connected, and what happens if something breaks?" from the actual artifacts that define the system. This is a fundamentally different question that none of the above can answer.

### 7.2 Target Market Segments

| Segment | Company Size | Pain Point | Willingness to Pay |
|---|---|---|---|
| **Platform Engineering Teams** | 200-5000 engineers | Cannot visualize or query cross-service dependencies at scale | High — directly reduces incident MTTR |
| **Enterprise Architecture** | 5000+ engineers | Architecture documentation is perpetually stale | High — automated topology mapping replaces manual diagramming |
| **Regulated Industries** | Any (finance, healthcare) | Compliance requires demonstrable understanding of data flows | Very High — regulatory pressure creates urgency |
| **Cloud Migration Programs** | Any | Need to understand legacy system topology before migration | High — time-bounded, high-urgency use case |

### 7.3 Pricing Model (Future SaaS)

| Tier | Price | Includes |
|---|---|---|
| **Community** | Free (self-hosted) | Core platform, community support, single tenant |
| **Team** | $500/mo per namespace | Managed SaaS, 5 namespaces, logical tenant isolation, email support |
| **Enterprise** | Custom pricing | Physical tenant isolation, SSO/SAML, SLA guarantees, dedicated support, SOC2 report |
| **Platform** | Custom pricing | Multi-region, federation, custom extractors, premium support, on-premises deployment option |

---

## 8. Success Metrics

| Metric | Year 1 Target | Year 3 Target |
|---|---|---|
| **Ingested Repositories** | 50 (beta customers) | 10,000 (SaaS) |
| **Graph Nodes Under Management** | 50,000 | 50,000,000 |
| **Monthly Active Queries** | 10,000 | 10,000,000 |
| **Query Accuracy (human-evaluated)** | 85% | 95% |
| **Mean Time to Understanding (MTTU)** | < 30 seconds | < 10 seconds |
| **Uptime SLA** | 99.9% | 99.99% |
| **Customer NPS** | > 40 | > 60 |
| **SOC2 Compliance** | Type I | Type II (annual) |

---

## 9. Open Questions

1. **LLM Cost at Scale:** At 1M+ documents, LLM extraction costs could reach $50K+/month. Should we invest in fine-tuned open-source models (Llama, Mistral) for extraction to reduce per-document cost by 10-50x?

2. **Graph Schema Extensibility vs. Consistency:** Allowing custom node/edge types (via plugin extractors) risks schema fragmentation. How do we maintain query correctness when the schema is user-extensible?

3. **Federation Privacy Model:** Cross-organization graph federation requires answering topology queries without revealing the full graph to the querying party. What privacy-preserving query protocols (differential privacy, secure multi-party computation) are practical at graph-query latencies?

4. **Neo4j vs. Multi-Model:** Should we migrate to a multi-model database (e.g., TigerGraph, SurrealDB) that natively combines graph + vector + document storage, or continue with Neo4j + external vector index?

5. **Open-Source Strategy:** When (if ever) should the core platform be open-sourced? What is the right balance between community adoption and commercial viability?
