# Multi-Region Architecture

## Overview

GraphRAG supports multi-region deployments for low-latency access, regulatory
compliance (data residency), and disaster recovery. Each tenant is assigned a
**home region** that determines where its data is persisted and where write
operations are routed. Read replicas may exist in additional allowed regions.

---

## Replication Strategy

### Neo4j

- **Primary–secondary** replication within the home region cluster (3-node
  Raft consensus).
- **Cross-region read replicas** deployed via Neo4j Causal Clustering. Each
  secondary region runs at least one read replica that tails the transaction
  log from the primary cluster.
- Replication lag target: < 500 ms p99 under normal conditions.

### Kafka (MirrorMaker 2)

- Each region runs its own Kafka cluster.
- **MirrorMaker 2** continuously mirrors topics from the primary region to
  secondary regions with topic-level filtering.
- Consumer group offsets are translated automatically so consumers in failover
  regions resume from the correct position.
- Configuration lives in `infrastructure/k8s/multi-region/mirrormaker.yaml`.

### Vector Store

- Embeddings are replicated asynchronously from the primary vector store to
  region-local replicas. The outbox drainer publishes upserts to a dedicated
  `vector-sync` Kafka topic that MirrorMaker mirrors cross-region.

---

## Failover

### Automated Regional Failover

1. **Health probes**: Each region endpoint exposes `/healthz` and `/readyz`.
   An external health-check service (Route 53 health checks or equivalent)
   monitors all region endpoints.
2. **DNS failover**: When the primary region fails health checks for 3
   consecutive intervals (30 s each), DNS is updated to route traffic to the
   highest-priority healthy secondary.
3. **Kafka consumer rebalance**: Consumers in the failover region begin
   processing from translated offsets.
4. **Neo4j promotion**: A read replica in the failover region is promoted to
   primary via the Neo4j cluster management API.

### Recovery (Failback)

- Once the original primary region recovers, an operator-initiated runbook
  re-synchronises data and switches DNS back.
- MirrorMaker offset translation ensures no duplicate processing.

---

## Data Routing

### Tenant-Affinity Routing

Every API request includes a tenant identifier (from the JWT or API key).
The **TenantRegionRouter** resolves the tenant's home region and routes the
request accordingly:

```
Request → API Gateway → TenantRegionRouter.route(tenant_id)
                        │
                        ├─ home region  → direct to local cluster
                        └─ other region → proxy to home region endpoint
```

### Allowed Regions

Each tenant's `DataResidencyConfig` specifies:

| Field             | Description                                    |
|-------------------|------------------------------------------------|
| `home_region`     | Region where writes are persisted              |
| `allowed_regions` | Regions permitted to serve reads for the tenant |
| `consistency`     | `eventual` (default) or `strong`               |

If `allowed_regions` is empty, only the home region may serve requests.

### Cross-Region Service

The `cross-region-service.yaml` manifest deploys a lightweight proxy that
forwards write requests arriving at a non-home region to the tenant's home
region cluster, ensuring data residency constraints are never violated.

---

## Deployment Topology

```
Region A (us-east-1)           Region B (eu-west-1)
┌──────────────────┐           ┌──────────────────┐
│  Neo4j Primary   │──repl────▶│  Neo4j Replica   │
│  Kafka Cluster   │──MM2─────▶│  Kafka Cluster   │
│  Orchestrator    │           │  Orchestrator    │
│  Vector Store    │           │  Vector Store    │
└──────────────────┘           └──────────────────┘
```

---

## References

- `infrastructure/k8s/multi-region/` — K8s manifests for cross-region infra
- `orchestrator/app/data_residency.py` — Tenant region routing logic
