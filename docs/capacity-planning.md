# Capacity Planning Guide

Resource calculations for the GraphRAG system at 1x, 10x, and 100x scale.

## Baseline Assumptions (1x Scale)

| Metric | Value |
|--------|-------|
| Documents ingested per day | 10,000 |
| Average document size | 50 KB |
| Queries per second (QPS) | 10 |
| Neo4j nodes | 500,000 |
| Neo4j relationships | 2,000,000 |
| Kafka messages per second | 50 |
| Concurrent tenants | 10 |

## Component Resource Matrix

### Orchestrator (FastAPI + LangGraph)

| Scale | Replicas | CPU Request | CPU Limit | Memory Request | Memory Limit |
|-------|----------|-------------|-----------|----------------|--------------|
| 1x | 2 | 500m | 2 | 512Mi | 2Gi |
| 10x | 5 | 1 | 4 | 1Gi | 4Gi |
| 100x | 20 | 2 | 8 | 2Gi | 8Gi |

Key scaling factors:
- CPU scales linearly with QPS due to LLM orchestration overhead
- Memory grows with concurrent graph traversal depth and context window sizes
- HPA targets: CPU 70%, custom metric `query_latency_p99 < 3s`

### Ingestion Workers (Go)

| Scale | Replicas | CPU Request | CPU Limit | Memory Request | Memory Limit |
|-------|----------|-------------|-----------|----------------|--------------|
| 1x | 3 | 200m | 1 | 256Mi | 512Mi |
| 10x | 10 | 500m | 2 | 512Mi | 1Gi |
| 100x | 30 | 1 | 4 | 1Gi | 2Gi |

Key scaling factors:
- Scales with Kafka partition count (1 consumer per partition)
- Memory depends on message batch size and AST parsing buffer
- At 100x, consider consumer groups with partition reassignment

### Neo4j Cluster

| Scale | Primary | Secondary | CPU/Node | Memory/Node | Heap | Page Cache | Storage |
|-------|---------|-----------|----------|-------------|------|------------|---------|
| 1x | 3 | 2 | 1-4 | 8-16Gi | 4g | 8g | 50Gi |
| 10x | 3 | 5 | 4-8 | 32-64Gi | 16g | 32g | 500Gi |
| 100x | 5 | 10 | 8-16 | 64-128Gi | 32g | 64g | 2Ti |

Key scaling factors:
- Page cache should fit the entire graph for optimal read performance
- Heap scales with concurrent transaction count
- Secondary replicas absorb read traffic; primaries handle writes
- At 100x, consider sharding by tenant or domain

### Kafka Cluster

| Scale | Brokers | CPU/Broker | Memory/Broker | Storage/Broker | Partitions/Topic |
|-------|---------|------------|---------------|----------------|------------------|
| 1x | 3 | 500m-2 | 1-2Gi | 50Gi | 6 |
| 10x | 5 | 2-4 | 4-8Gi | 200Gi | 30 |
| 100x | 10 | 4-8 | 8-16Gi | 1Ti | 100 |

Key scaling factors:
- Partition count determines max consumer parallelism
- Replication factor 3 maintained at all scales (min.insync.replicas=2)
- Storage retention: 7 days at 1x, consider tiered storage at 100x

### Qdrant (Vector Store)

| Scale | Replicas | CPU Request | CPU Limit | Memory Request | Memory Limit | Storage |
|-------|----------|-------------|-----------|----------------|--------------|---------|
| 1x | 1 | 500m | 2 | 1Gi | 4Gi | 20Gi |
| 10x | 3 | 2 | 4 | 4Gi | 16Gi | 200Gi |
| 100x | 6 | 4 | 8 | 16Gi | 64Gi | 2Ti |

Key scaling factors:
- Memory must hold HNSW index for acceptable query latency
- At 10x+, enable sharding across replicas
- Quantization (scalar/product) reduces memory at cost of recall

### Redis (Cache + Circuit Breaker State)

| Scale | Mode | CPU | Memory | Max Memory |
|-------|------|-----|--------|------------|
| 1x | Standalone | 200m | 512Mi | 256mb |
| 10x | Sentinel (3) | 500m | 2Gi | 1gb |
| 100x | Cluster (6) | 1 | 4Gi | 3gb |

## Total Cluster Resources

| Scale | Total CPU (request) | Total Memory (request) | Total Storage |
|-------|--------------------|-----------------------|---------------|
| 1x | ~12 cores | ~30 Gi | ~250 Gi |
| 10x | ~60 cores | ~200 Gi | ~2 Ti |
| 100x | ~300 cores | ~1.2 Ti | ~15 Ti |

## Scaling Triggers

| Metric | Threshold | Action |
|--------|-----------|--------|
| Query p99 latency | > 3s for 5 min | Scale orchestrator replicas |
| Kafka consumer lag | > 10,000 messages | Scale ingestion workers |
| Neo4j query time | > 500ms p95 | Add secondary replicas |
| Qdrant search latency | > 200ms p95 | Add Qdrant shards |
| Redis hit rate | < 80% | Increase max memory |
| CPU utilization | > 70% sustained | HPA triggers (orchestrator, workers) |

## Cost Estimation (AWS EKS, us-east-1)

| Scale | Node Type | Node Count | Monthly Estimate |
|-------|-----------|------------|------------------|
| 1x | m6i.xlarge (4c/16G) | 3 | ~$400 |
| 10x | m6i.2xlarge (8c/32G) | 8 | ~$2,500 |
| 100x | m6i.4xlarge (16c/64G) | 20 | ~$15,000 |

Costs exclude LLM API spend, which dominates at scale:
- 1x: ~$500/mo (10 QPS, mixed Claude/Gemini)
- 10x: ~$5,000/mo (semantic cache reduces by ~40%)
- 100x: ~$30,000/mo (aggressive caching + prompt compression)
