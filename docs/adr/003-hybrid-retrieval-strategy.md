# ADR-003: Hybrid Vector-Cypher Retrieval Strategy

## Status

Accepted

## Context

The system must answer both simple factual queries ("What port does the auth service use?")
and complex structural queries ("If the auth service fails, which downstream services are
affected?"). Pure vector similarity search cannot answer graph-structural questions, and
pure graph traversals miss semantic relevance.

Evaluated options: vector-only RAG, graph-only Cypher queries, hybrid approach.

## Decision

Implement a Hybrid VectorCypher retrieval strategy with intelligent query routing.

- Simple entity lookups route to Qdrant vector search
- Complex multi-hop structural queries trigger an agentic Cypher generation loop
- Query complexity scoring determines routing (cost estimation)
- LangGraph state machine orchestrates the retrieval pipeline

## Consequences

- Higher system complexity with two retrieval paths
- Query router must accurately classify intent (risk of misrouting)
- Requires maintaining both vector embeddings (Qdrant) and graph schema (Neo4j) in sync
- Agentic Cypher loop may have higher latency for complex queries
- MAX_QUERY_COST configuration needed to bound expensive graph traversals
