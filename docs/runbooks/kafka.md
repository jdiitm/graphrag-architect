# Kafka Runbook

## Overview

Apache Kafka 3.9.0 serves as the async ingestion event bus for GraphRAG. It runs in KRaft
mode (no ZooKeeper) as a 3-broker `StatefulSet` in the `graphrag` namespace with
SASL_SSL authentication and SCRAM-SHA-512. Each broker serves both broker and controller roles.

**K8s resource:** `kafka-statefulset.yaml` — 3 replicas, 50Gi PVCs, JMX exporter sidecar
on port 5556.

**Key topics:**
- `document-ingestion` — primary ingestion pipeline (replication factor 3, min.insync 2)
- `document-ingestion-dlq` — dead letter queue for failed processing

**Wire security:** SASL_SSL between brokers and clients, PLAINTEXT for controller-only
listener. TLS keystores/truststores from `kafka-tls-credentials` Secret.

---

## Common Issues

### 1. Consumer Lag Spike

**Symptoms:** `HighConsumerLag` alert fires. `kafka_consumer_lag_messages` exceeds 1000
for 60s. Ingested documents appear delayed in the knowledge graph.

**Root cause:** Ingestion workers processing slower than production rate (large documents,
LLM extraction latency), or workers crashed and are rebalancing.

### 2. Partition Reassignment Storm

**Symptoms:** Consumer group repeatedly rebalancing. `kafka_consumer_rebalance_total`
counter spikes. Throughput drops to zero during rebalance windows.

**Root cause:** Workers failing liveness probes, pod evictions, or `max.poll.interval.ms`
exceeded because batch processing took too long.

### 3. Broker Failure

**Symptoms:** `KafkaBrokerDown` alert fires (`< 2 brokers healthy`). Under-replicated
partitions increase. Producers get `NotEnoughReplicasException`.

**Root cause:** Pod OOMKilled, disk full, or node failure. With `min.insync.replicas=2`
and `replication.factor=3`, losing 2 brokers halts writes.

### 4. Topic Compaction Issues

**Symptoms:** Topic storage grows unbounded. Old messages not being cleaned up.

**Root cause:** `cleanup.policy` set to `delete` but retention too long, or compaction
thread stuck on a corrupted segment.

### 5. KRaft Metadata Quorum Loss

**Symptoms:** All brokers report `ERROR` in logs about metadata quorum. No leader for
`__cluster_metadata`. New topics cannot be created.

**Root cause:** 2-of-3 controller nodes lost simultaneously. KRaft requires majority
quorum (2/3) for metadata operations.

---

## Troubleshooting Steps

### Consumer Lag

1. Check current lag per partition:
   ```bash
   kubectl exec -n graphrag kafka-0 -- /opt/kafka/bin/kafka-consumer-groups.sh \
     --bootstrap-server localhost:9092 \
     --describe --group graphrag-ingestion
   ```
2. Check ingestion worker health:
   ```bash
   kubectl get pods -n graphrag -l app=ingestion-worker
   kubectl logs -n graphrag -l app=ingestion-worker --tail=50
   ```
3. If workers are healthy but slow, check HPA status:
   ```bash
   kubectl get hpa ingestion-worker-hpa -n graphrag
   ```
4. Manual scale-up if HPA is not reacting fast enough:
   ```bash
   kubectl scale deployment ingestion-worker -n graphrag --replicas=5
   ```

### Partition Reassignment

1. Check consumer group state:
   ```bash
   kubectl exec -n graphrag kafka-0 -- /opt/kafka/bin/kafka-consumer-groups.sh \
     --bootstrap-server localhost:9092 \
     --describe --group graphrag-ingestion --state
   ```
2. If group is stuck in `PreparingRebalance`, check for dead members:
   ```bash
   kubectl exec -n graphrag kafka-0 -- /opt/kafka/bin/kafka-consumer-groups.sh \
     --bootstrap-server localhost:9092 \
     --describe --group graphrag-ingestion --members
   ```
3. Increase `session.timeout.ms` and `max.poll.interval.ms` if processing time legitimately
   exceeds defaults. Update via Go worker env vars `KAFKA_SESSION_TIMEOUT_MS` and
   `KAFKA_MAX_POLL_INTERVAL_MS`.

### Broker Recovery

1. Check broker status:
   ```bash
   kubectl get pods -n graphrag -l app=kafka -o wide
   kubectl describe pod -n graphrag kafka-<N>
   ```
2. Check under-replicated partitions:
   ```bash
   kubectl exec -n graphrag kafka-0 -- /opt/kafka/bin/kafka-topics.sh \
     --bootstrap-server localhost:9092 \
     --describe --under-replicated-partitions
   ```
3. If broker disk is full, expand the PVC or purge old segments:
   ```bash
   kubectl exec -n graphrag kafka-<N> -- du -sh /var/kafka-logs/
   ```

---

## Recovery Procedures

### Single Broker Recovery

1. Delete the failed pod (StatefulSet will recreate with same PVC):
   ```bash
   kubectl delete pod -n graphrag kafka-<N>
   ```
2. Wait for ISR catch-up:
   ```bash
   kubectl exec -n graphrag kafka-0 -- /opt/kafka/bin/kafka-topics.sh \
     --bootstrap-server localhost:9092 \
     --describe --topic document-ingestion
   ```
3. Verify all partitions have full ISR (should show 3 replicas in-sync).

### Full Cluster Recovery (KRaft Metadata Loss)

1. Scale StatefulSet to 0:
   ```bash
   kubectl scale statefulset kafka -n graphrag --replicas=0
   ```
2. Wait for all pods to terminate.
3. If metadata is corrupted, format the log directories on each PVC:
   ```bash
   for i in 0 1 2; do
     kubectl run kafka-format-$i --rm -it --restart=Never \
       --image=apache/kafka:3.9.0 \
       --overrides='{"spec":{"volumes":[{"name":"data","persistentVolumeClaim":{"claimName":"data-kafka-'$i'"}}],"containers":[{"name":"format","image":"apache/kafka:3.9.0","command":["/opt/kafka/bin/kafka-storage.sh","format","-t","MkU3OEVBNTcwNTJENDM2Qk","-c","/opt/kafka/config/kraft/server.properties"],"volumeMounts":[{"name":"data","mountPath":"/var/kafka-logs"}]}]}}'
   done
   ```
4. Scale back to 3 and recreate topics.

### DLQ Drain

When the DLQ has accumulated messages that need reprocessing:
```bash
kubectl exec -n graphrag kafka-0 -- /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic document-ingestion-dlq \
  --from-beginning --max-messages 100 > /tmp/dlq-dump.json
```
Review messages, fix root cause, then republish to the main topic.

---

## Monitoring Queries

### Consumer Lag

```promql
# Per-partition lag (alert threshold: 1000)
kafka_consumer_lag_messages{topic="document-ingestion", consumer_group="graphrag-ingestion"}

# Lag rate of change (positive = falling behind)
deriv(kafka_consumer_lag_messages{topic="document-ingestion"}[5m])
```

### Broker Health

```promql
# Active brokers (should be 3)
count(up{app="kafka"} == 1)

# Under-replicated partitions (should be 0)
kafka_server_replica_UnderReplicatedPartitions{namespace="graphrag"}

# Active controller count (should be 1)
kafka_controller_ActiveControllerCount{namespace="graphrag"}
```

### Throughput

```promql
# Messages in per second
rate(kafka_server_MessagesInPerSec_Count{namespace="graphrag"}[5m])

# Bytes in/out per second
rate(kafka_server_BytesInPerSec_Count{namespace="graphrag"}[5m])
rate(kafka_server_BytesOutPerSec_Count{namespace="graphrag"}[5m])
```

### DLQ Monitoring

```promql
# DLQ ingestion rate (should be near 0)
rate(ingestion_dlq_routed_total[5m])

# DLQ sink failures (must be 0 — messages at risk of loss)
rate(ingestion_dlq_sink_error_total[5m])
```

### Key Alerts (from `alerting.yaml`)

| Alert | Trigger | Action |
|---|---|---|
| `HighDLQRate` | DLQ rate > 10/min for 1m | Check worker logs, investigate extraction failures |
| `DLQSinkFailure` | DLQ sink error > 0 for 1m | Immediate — messages being lost |
| `HighConsumerLag` | Lag > 1000 for 60s | Scale workers, check processing time |
| `KafkaBrokerDown` | < 2 brokers healthy for 2m | Page on-call, check pod status |
