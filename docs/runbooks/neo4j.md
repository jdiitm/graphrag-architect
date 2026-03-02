# Neo4j Runbook

## Overview

Neo4j is the primary knowledge graph store for GraphRAG. It runs as a 3-node Raft cluster
(Enterprise Edition 5.26) deployed via `StatefulSet` in the `graphrag` namespace. Each node
exposes Bolt (7687), HTTP (7474), discovery (5000), transaction (6000), and routing (7688) ports.

**K8s resource:** `neo4j-statefulset.yaml` — 3 replicas, 8Gi page cache, 4Gi max heap.

**Schema initialization:** The `neo4j-schema-init` Job (`neo4j-schema-job.yaml`) runs
`schema_init.cypher` to create constraints (tenant-scoped NODE KEYs) and indexes on startup.

---

## Common Issues

### 1. Connection Pool Exhaustion

**Symptoms:** `ClientException: Unable to acquire connection from pool` in orchestrator
logs. `neo4j_connection_pool_active` gauge saturated at pool max.

**Root cause:** Long-running Cypher queries hold connections, or a burst of concurrent
ingestion batches exceeds the pool size configured in `neo4j_pool.py`.

### 2. Cluster Leader Election Failure

**Symptoms:** Write transactions fail with `NotALeader`. Multiple pods restart.
`neo4j_cluster_leader` metric flaps between 0 and 1.

**Root cause:** Network partition between pods, or a pod evicted during GC pause. The
Raft quorum requires 2-of-3 nodes to elect a leader.

### 3. Memory Pressure (Page Cache vs Heap)

**Symptoms:** Queries slow down, `neo4j_page_cache_hit_ratio` drops below 0.95.
OOMKilled events on the pod.

**Root cause:** Graph grew beyond page cache allocation (8Gi), or heap was consumed by
large result sets from unbounded MATCH queries.

### 4. Index Corruption After Ungraceful Shutdown

**Symptoms:** Queries return empty results despite data existing. `IndexNotFoundError`
in Neo4j logs after a node restart.

**Root cause:** Pod killed during index rebuild or compaction.

### 5. Schema Drift Between Environments

**Symptoms:** Constraint violation errors on ingestion. `schema_init.cypher` applied
in staging but not production.

**Root cause:** `neo4j-schema-init` Job failed silently, or a migration was applied
out of order.

---

## Troubleshooting Steps

### Connection Pool Exhaustion

1. Check current pool utilization:
   ```bash
   kubectl exec -n graphrag deploy/orchestrator -- curl -s localhost:8000/metrics | grep neo4j_connection_pool
   ```
2. Identify long-running transactions:
   ```cypher
   CALL dbms.listTransactions() YIELD transactionId, elapsedTime, currentQuery
   WHERE elapsedTime > duration('PT10S')
   RETURN transactionId, elapsedTime, currentQuery
   ```
3. Kill problematic transactions:
   ```cypher
   CALL dbms.killTransaction('neo4j-transaction-XXX')
   ```
4. If pool size is the bottleneck, increase `max_connection_pool_size` in ConfigMap
   `graphrag-config` and restart the orchestrator.

### Leader Election Failure

1. Check cluster status from any Neo4j pod:
   ```bash
   kubectl exec -n graphrag neo4j-0 -- cypher-shell -u neo4j -p "$NEO4J_PASSWORD" \
     "CALL dbms.cluster.overview()"
   ```
2. Verify all 3 pods are running and ready:
   ```bash
   kubectl get pods -n graphrag -l app=neo4j -o wide
   ```
3. Check network policies are not blocking discovery port 5000:
   ```bash
   kubectl get networkpolicy -n graphrag -o yaml | grep -A5 neo4j
   ```
4. If a single node is stuck, delete the pod to trigger re-election:
   ```bash
   kubectl delete pod -n graphrag neo4j-2
   ```

### Memory Pressure

1. Check current memory allocation:
   ```bash
   kubectl exec -n graphrag neo4j-0 -- cypher-shell -u neo4j -p "$NEO4J_PASSWORD" \
     "CALL dbms.listConfig() YIELD name, value WHERE name CONTAINS 'memory' RETURN name, value"
   ```
2. Check page cache hit ratio:
   ```cypher
   CALL dbms.queryJmx('org.neo4j:instance=kernel#0,name=Page cache')
   YIELD attributes
   RETURN attributes.HitRatio.value AS hitRatio
   ```
3. If page cache is undersized, increase `NEO4J_dbms_memory_pagecache_size` in the
   StatefulSet env and perform a rolling restart.

---

## Recovery Procedures

### Full Cluster Recovery (All Nodes Down)

1. Scale the StatefulSet to 0:
   ```bash
   kubectl scale statefulset neo4j -n graphrag --replicas=0
   ```
2. Wait for all pods to terminate.
3. Scale back to 3:
   ```bash
   kubectl scale statefulset neo4j -n graphrag --replicas=3
   ```
4. Verify cluster formation:
   ```bash
   kubectl exec -n graphrag neo4j-0 -- cypher-shell -u neo4j -p "$NEO4J_PASSWORD" \
     "CALL dbms.cluster.overview()"
   ```

### Restore from Backup

1. Scale down the StatefulSet to 0.
2. Copy backup to the first node's PVC:
   ```bash
   kubectl cp backup.dump graphrag/neo4j-0:/data/backup.dump
   ```
3. Restore using `neo4j-admin`:
   ```bash
   kubectl exec -n graphrag neo4j-0 -- neo4j-admin database load --from-path=/data/backup.dump neo4j --overwrite-destination
   ```
4. Scale back up to 3 and verify data integrity.

### Re-initialize Schema

If constraints or indexes are missing:
```bash
kubectl delete job neo4j-schema-init -n graphrag --ignore-not-found
kubectl apply -f infrastructure/k8s/neo4j-schema-job.yaml
kubectl wait --for=condition=complete job/neo4j-schema-init -n graphrag --timeout=120s
```

---

## Monitoring Queries

### Cluster Health

```promql
# Leader availability (should always be 1)
neo4j_cluster_leader{namespace="graphrag"}

# Raft log append latency
rate(neo4j_raft_append_index[5m])
```

### Performance

```promql
# Page cache hit ratio (target: > 0.95)
neo4j_page_cache_hit_ratio{namespace="graphrag"}

# Transaction commit rate
rate(neo4j_transaction_committed_total[5m])

# Connection pool utilization
neo4j_connection_pool_active / neo4j_connection_pool_max
```

### Storage

```promql
# Disk usage (alert at 80%)
kubelet_volume_stats_used_bytes{namespace="graphrag", persistentvolumeclaim=~"data-neo4j-.*"}
/
kubelet_volume_stats_capacity_bytes{namespace="graphrag", persistentvolumeclaim=~"data-neo4j-.*"}

# Node and relationship counts
neo4j_ids_in_use_nodes{namespace="graphrag"}
neo4j_ids_in_use_relationships{namespace="graphrag"}
```

### Key Alerts (from `alerting.yaml`)

| Alert | Trigger | Action |
|---|---|---|
| `Neo4jDown` | `up{job="neo4j"} == 0` for 1m | Page on-call, check pod status |
| Page cache < 0.90 | `neo4j_page_cache_hit_ratio < 0.90` | Increase `pagecache_size` |
| Pool exhaustion | `neo4j_connection_pool_active == neo4j_connection_pool_max` | Kill long transactions, scale pool |
