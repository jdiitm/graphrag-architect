# Ingestion Workers Runbook

## Overview

The Go ingestion workers are high-throughput Kafka consumers that process infrastructure
documents (Kubernetes manifests, source code, configuration files) and forward extracted
entities to the Python orchestrator for graph ingestion. They run as a `Deployment` with
HPA scaling based on consumer lag.

**K8s resource:** `ingestion-worker-deployment.yaml` — 2-10 replicas, HPA on
`ingestion_consumer_lag` (target: 1000) and CPU (80%). Exposes `/healthz` on port 9090
and Prometheus metrics on `/metrics`.

**Key features:**
- Channel-based async ack pattern (`consumer.go`) with configurable `maxInflight` backpressure
- Content-hash deduplication (LRU, Redis, or Noop stores via `internal/dedup/`)
- DLQ routing for messages that fail processing 3 times

---

## Common Issues

### 1. Stalled Consumer (Healthz Probe Failure)

**Symptoms:** Pod marked NotReady. Liveness probe fails on `/healthz` after 3 consecutive
failures (30s intervals, starting at 10s). Consumer stops processing messages.

**Root cause:** Goroutine blocked on a synchronous HTTP call to the orchestrator, or the
Kafka poll loop is stuck waiting for a rebalance. The `/healthz` endpoint reports unhealthy
when the consumer loop has not polled within the configured timeout.

### 2. DLQ Overflow

**Symptoms:** `HighDLQRate` alert fires. `ingestion_dlq_routed_total` counter spikes.
`DLQSinkFailure` alert means DLQ publish itself is failing — messages are being lost.

**Root cause:** Extraction failures (malformed documents, LLM errors), orchestrator
unavailable, or schema validation failures. Each message gets 3 retry attempts before
DLQ routing.

### 3. Kafka Rebalance Storm

**Symptoms:** Consumer group repeatedly enters `PreparingRebalance`. Throughput drops to
zero during each rebalance window. Logs show `Revoke` and `Assign` in rapid succession.

**Root cause:** Workers failing health checks and being replaced, or `max.poll.interval.ms`
exceeded because a batch took too long to process. HPA rapid scale-up/down can also trigger
frequent rebalances.

### 4. Goroutine Leak

**Symptoms:** `go_goroutines` metric steadily increases. Memory usage grows over time.
Worker eventually OOMKilled.

**Root cause:** HTTP client connections to the orchestrator not being closed, or channel
sends blocking indefinitely on a full channel without timeout/context cancellation.

### 5. Blob Store Connectivity Issues

**Symptoms:** Workers cannot fetch large documents referenced by ingestion messages.
Errors like `connection refused` or `timeout` when accessing the blob store.

**Root cause:** Network policy blocking egress, blob store credentials expired, or the
blob store service is down.

---

## Troubleshooting Steps

### Stalled Consumer Detection

1. Check pod readiness:
   ```bash
   kubectl get pods -n graphrag -l app=ingestion-worker -o wide
   ```
2. Hit the healthz endpoint directly:
   ```bash
   kubectl exec -n graphrag <pod-name> -- wget -qO- http://localhost:9090/healthz
   ```
3. Check goroutine dump for blocked goroutines:
   ```bash
   kubectl exec -n graphrag <pod-name> -- wget -qO- http://localhost:9090/debug/pprof/goroutine?debug=2 | head -100
   ```
4. If the consumer is stuck, delete the pod:
   ```bash
   kubectl delete pod -n graphrag <pod-name>
   ```

### DLQ Investigation

1. Check DLQ message count:
   ```bash
   kubectl exec -n graphrag kafka-0 -- /opt/kafka/bin/kafka-consumer-groups.sh \
     --bootstrap-server localhost:9092 \
     --describe --group graphrag-ingestion
   ```
2. Sample DLQ messages to identify the failure pattern:
   ```bash
   kubectl exec -n graphrag kafka-0 -- /opt/kafka/bin/kafka-console-consumer.sh \
     --bootstrap-server localhost:9092 \
     --topic document-ingestion-dlq \
     --from-beginning --max-messages 5
   ```
3. Check worker logs for the error that triggered DLQ routing:
   ```bash
   kubectl logs -n graphrag -l app=ingestion-worker --tail=200 | grep -i "dlq\|error\|failed"
   ```

### Rebalance Storm

1. Check consumer group state:
   ```bash
   kubectl exec -n graphrag kafka-0 -- /opt/kafka/bin/kafka-consumer-groups.sh \
     --bootstrap-server localhost:9092 \
     --describe --group graphrag-ingestion --state
   ```
2. Check HPA for rapid scaling:
   ```bash
   kubectl describe hpa ingestion-worker-hpa -n graphrag
   ```
3. Stabilize by setting fixed replicas temporarily:
   ```bash
   kubectl scale deployment ingestion-worker -n graphrag --replicas=4
   kubectl patch hpa ingestion-worker-hpa -n graphrag -p '{"spec":{"minReplicas":4,"maxReplicas":4}}'
   ```
4. Increase `max.poll.interval.ms` if processing time legitimately exceeds the default.

### Goroutine Leak Detection

1. Check current goroutine count:
   ```bash
   kubectl exec -n graphrag <pod-name> -- wget -qO- http://localhost:9090/metrics | grep go_goroutines
   ```
2. Get a goroutine profile:
   ```bash
   kubectl exec -n graphrag <pod-name> -- wget -qO- http://localhost:9090/debug/pprof/goroutine?debug=1 > goroutines.txt
   ```
3. Look for goroutines stuck on channel operations or HTTP calls.
4. If the leak is confirmed, restart the affected pod and file a bug.

---

## Recovery Procedures

### Rolling Restart

```bash
kubectl rollout restart deployment/ingestion-worker -n graphrag
kubectl rollout status deployment/ingestion-worker -n graphrag --timeout=120s
```

### Clear Consumer Group Offsets (Reset to Latest)

Use only when DLQ messages have been triaged and you want to skip a bad batch:
```bash
kubectl scale deployment ingestion-worker -n graphrag --replicas=0
kubectl exec -n graphrag kafka-0 -- /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group graphrag-ingestion \
  --topic document-ingestion \
  --reset-offsets --to-latest --execute
kubectl scale deployment ingestion-worker -n graphrag --replicas=2
```

### Emergency Scale-Up

When consumer lag is growing faster than the HPA can react:
```bash
kubectl scale deployment ingestion-worker -n graphrag --replicas=10
```

### Flush Dedup Cache (Redis)

If the Redis dedup store has stale entries preventing reprocessing:
```bash
kubectl exec -n graphrag deploy/orchestrator -- python -c "
import redis
r = redis.from_url('redis://redis:6379')
keys = r.keys('graphrag:dedup:*')
if keys: r.delete(*keys)
print(f'Cleared {len(keys)} dedup keys')
"
```

---

## Monitoring Queries

### Consumer Performance

```promql
# Consumer lag per partition (alert threshold: 1000)
kafka_consumer_lag_messages{consumer_group="graphrag-ingestion"}

# Messages processed per second
rate(ingestion_messages_processed_total[5m])

# Batch processing duration (p99, SLA: < 2s)
histogram_quantile(0.99, rate(ingestion_batch_duration_seconds_bucket[5m]))
```

### DLQ Rates

```promql
# DLQ routing rate (should be near 0)
rate(ingestion_dlq_routed_total[5m])

# DLQ sink errors (must be 0)
rate(ingestion_dlq_sink_error_total[5m])
```

### Resource Utilization

```promql
# Goroutine count (watch for steady increase)
go_goroutines{app="ingestion-worker"}

# Memory usage
container_memory_usage_bytes{namespace="graphrag", container="ingestion-worker"}

# CPU utilization
rate(container_cpu_usage_seconds_total{namespace="graphrag", container="ingestion-worker"}[5m])
```

### HPA Status

```promql
# Current vs desired replicas
kube_horizontalpodautoscaler_status_current_replicas{horizontalpodautoscaler="ingestion-worker-hpa"}
kube_horizontalpodautoscaler_spec_max_replicas{horizontalpodautoscaler="ingestion-worker-hpa"}
```

### Key Alerts (from `alerting.yaml`)

| Alert | Trigger | Action |
|---|---|---|
| `HighDLQRate` | DLQ rate > 10/min for 1m | Check worker logs, extraction errors |
| `DLQSinkFailure` | DLQ sink error > 0 for 1m | Immediate — messages being lost |
| `HighConsumerLag` | Lag > 1000 for 60s | Scale workers, check processing time |
| `SlowBatchProcessing` | p99 > 2s for 5m | Check orchestrator latency, LLM provider |
