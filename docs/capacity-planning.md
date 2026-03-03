# Capacity Planning

Resource calculations for the GraphRAG system at 1x, 10x, and 100x scale.

## Baseline Assumptions (1x)

| Parameter | Value |
|---|---|
| Ingestion rate | 100 documents/min |
| Concurrent queries | 10 |
| Knowledge graph nodes | 50,000 |
| Knowledge graph edges | 200,000 |
| Average document size | 50 KB |
| Kafka retention | 7 days |

## Component Resource Requirements

### Neo4j Cluster

| Scale | Replicas | CPU (req/limit) | Memory (req/limit) | Storage | Heap | Pagecache |
|---|---|---|---|---|---|---|
| 1x | 3 primary + 2 secondary | 1/4 | 8Gi/16Gi | 50Gi | 4g | 8g |
| 10x | 3 primary + 4 secondary | 4/8 | 16Gi/32Gi | 200Gi | 8g | 16g |
| 100x | 5 primary + 8 secondary | 8/16 | 32Gi/64Gi | 1Ti | 16g | 32g |

**Scaling rationale**: Neo4j pagecache should hold the active working set.
At 100x (5M nodes, 20M edges), the graph footprint approaches ~80GB,
requiring proportional pagecache and heap increases. Secondary replicas
handle read scaling for concurrent queries.

### Kafka Cluster

| Scale | Brokers | CPU (req/limit) | Memory (req/limit) | Storage | Partitions per topic |
|---|---|---|---|---|---|
| 1x | 3 | 500m/2 | 1Gi/2Gi | 50Gi | 6 |
| 10x | 5 | 2/4 | 4Gi/8Gi | 200Gi | 12 |
| 100x | 9 | 4/8 | 8Gi/16Gi | 500Gi | 24 |

**Scaling rationale**: At 1x, 3 brokers with `min.insync.replicas=2` and
`replication.factor=3` provide fault tolerance for 1 broker failure. At
100x (10,000 docs/min), partitions increase to 24 for consumer parallelism
and storage scales for 7-day retention of ~700GB raw data.

### Orchestrator (FastAPI)

| Scale | Replicas | CPU (req/limit) | Memory (req/limit) | HPA target |
|---|---|---|---|---|
| 1x | 2 | 500m/2 | 512Mi/2Gi | 70% CPU |
| 10x | 6 | 1/4 | 1Gi/4Gi | 70% CPU, p99 < 500ms |
| 100x | 20 | 2/8 | 2Gi/8Gi | 70% CPU, p99 < 500ms |

**Scaling rationale**: The orchestrator is stateless and horizontally
scalable. At 100x, 100 concurrent queries with LLM calls and graph
traversals require sufficient memory for in-flight context windows
(~100KB per request * 100 concurrent = 10MB minimum, plus LLM
response buffering).

### Ingestion Workers (Go)

| Scale | Replicas | CPU (req/limit) | Memory (req/limit) | Consumer group partitions |
|---|---|---|---|---|
| 1x | 3 | 250m/1 | 256Mi/512Mi | 6 |
| 10x | 6 | 500m/2 | 512Mi/1Gi | 12 |
| 100x | 12 | 1/4 | 1Gi/2Gi | 24 |

**Scaling rationale**: Each Go worker processes one partition. Workers
scale 1:2 with Kafka partitions (2 partitions per worker for headroom
during rebalances). Memory scales with document size and parser buffers.

### Redis (Caching + Circuit Breaker State)

| Scale | Replicas | CPU (req/limit) | Memory (req/limit) | maxmemory |
|---|---|---|---|---|
| 1x | 1 primary + 1 replica | 250m/1 | 512Mi/1Gi | 512MB |
| 10x | 1 primary + 2 replicas | 500m/2 | 2Gi/4Gi | 2GB |
| 100x | 3 primary (cluster) + 3 replicas | 1/4 | 4Gi/8Gi | 4GB per shard |

**Scaling rationale**: At 1x, Redis caches query results and stores circuit
breaker state (~1KB per breaker * 1000 tenants). At 100x, the cache working
set grows to ~2GB and Redis Cluster provides sharded writes.

### Qdrant (Vector Store)

| Scale | Replicas | CPU (req/limit) | Memory (req/limit) | Storage |
|---|---|---|---|---|
| 1x | 1 | 500m/2 | 1Gi/2Gi | 20Gi |
| 10x | 3 | 2/4 | 4Gi/8Gi | 100Gi |
| 100x | 6 | 4/8 | 8Gi/16Gi | 500Gi |

**Scaling rationale**: Vector index size grows linearly with node count.
At 100x (5M nodes * 768-dim embeddings * 4 bytes = ~15GB raw vectors),
HNSW index overhead adds ~50%, requiring ~25GB active memory.

## Total Cluster Resources

| Scale | Total CPU (requests) | Total Memory (requests) | Total Storage |
|---|---|---|---|
| 1x | 12 vCPU | 30 Gi | 270 Gi |
| 10x | 45 vCPU | 100 Gi | 1.1 Ti |
| 100x | 160 vCPU | 320 Gi | 5.5 Ti |

## Node Pool Recommendations

| Scale | Node type (AWS) | Nodes | Notes |
|---|---|---|---|
| 1x | m6i.2xlarge (8 vCPU, 32GB) | 3 | Mixed workloads |
| 10x | r6i.4xlarge (16 vCPU, 128GB) | 4 | Memory-optimized for Neo4j + Qdrant |
| 100x | r6i.8xlarge (32 vCPU, 256GB) | 8 | Dedicated node groups per component |

## Cost Estimates (AWS us-east-1, on-demand)

| Scale | Monthly compute | Monthly storage | Total |
|---|---|---|---|
| 1x | ~$950 | ~$70 | ~$1,020 |
| 10x | ~$4,500 | ~$280 | ~$4,780 |
| 100x | ~$18,000 | ~$1,400 | ~$19,400 |

*Estimates use on-demand pricing. Reserved instances or savings plans
reduce costs by 30-50%.*

## Bottleneck Analysis

| Bottleneck | Symptom | Mitigation |
|---|---|---|
| Neo4j write throughput | Write latency > 100ms p99 | Batch Cypher UNWIND, add primary nodes |
| Kafka consumer lag | Lag > 10k messages | Add ingestion workers, increase partitions |
| Query latency | p99 > 500ms | Add orchestrator replicas, tune cache TTL |
| Neo4j page cache misses | Disk I/O spikes | Increase pagecache size, use NVMe storage |
| Qdrant recall degradation | Recall < 0.95 | Increase ef_construct, add replicas |
| Redis cache eviction | Hit rate < 80% | Increase memory, move to cluster mode |

## Monitoring Thresholds

| Metric | Warning | Critical | Action |
|---|---|---|---|
| Neo4j heap utilization | > 70% | > 85% | Increase heap or add read replicas |
| Kafka consumer lag | > 1000 msgs | > 10000 msgs | Scale ingestion workers |
| Orchestrator p99 latency | > 2s | > 5s | Scale replicas or optimize queries |
| Redis memory utilization | > 70% | > 85% | Increase maxmemory or eviction policy |
| Qdrant search latency | > 100ms | > 500ms | Add replicas or tune HNSW params |
