# ADR-001: Graph Database Selection

## Status

Accepted

## Context

The GraphRAG system requires a graph database to store and query a knowledge graph of
distributed system infrastructure. Key requirements: native graph traversals for multi-hop
queries, full-text search, Cypher query language support, enterprise clustering, and GDS
(Graph Data Science) algorithms for community detection and path analysis.

Evaluated options: Neo4j Enterprise, Amazon Neptune, JanusGraph, ArangoDB.

## Decision

Use Neo4j Enterprise Edition as the primary knowledge graph store.

Rationale:
- Native labeled property graph with mature Cypher query language
- Graph Data Science (GDS) library for PageRank, community detection, similarity
- Enterprise edition provides multi-database for tenant isolation
- Bolt protocol with official drivers for Python and Go
- Proven at scale with indexing, constraints, and APOC procedures

## Consequences

- Requires Neo4j Enterprise license for multi-database tenant isolation
- Operational complexity of managing a 3-node cluster (StatefulSet)
- Team must maintain Cypher expertise for schema evolution
- Vendor lock-in on Cypher; mitigated by abstracting queries behind repository pattern
