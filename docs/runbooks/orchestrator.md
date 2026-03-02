# Orchestrator Runbook

## Overview

The Python orchestrator is the central API service for GraphRAG. It handles document
ingestion (via `/ingest`), graph queries (via `/query`), and health checks (via `/health`).
Built with FastAPI and LangGraph, it coordinates LLM-based entity extraction, Cypher
generation, and hybrid VectorCypher retrieval.

**K8s resource:** `orchestrator-deployment.yaml` — 2-5 replicas, HPA on request queue
depth and CPU (80% target). Exposes port 8000 with Prometheus scrape on `/metrics`.

**Key dependencies:** Neo4j (graph store), Kafka (async ingestion), Redis (caching, rate
limiting, circuit breaker state), LLM providers (Gemini, Claude).

---

## Common Issues

### 1. Circuit Breaker Open State

**Symptoms:** Queries return 503 with `CircuitOpenError`. `circuit_breaker_state` metric
shows value 2 (open) for a dependency.

**Root cause:** Downstream dependency (Neo4j, LLM provider) exceeded the failure threshold
(default: 3 failures). The breaker opens for `recovery_timeout` seconds (default: 30s)
with jitter.

**Relevant code:** `circuit_breaker.py` — `CircuitBreaker`, `GlobalProviderBreaker`,
`TenantCircuitBreakerRegistry`.

### 2. LLM Provider Failover Events

**Symptoms:** `LLMProviderDown` alert or elevated `llm_request_errors_total`. Extraction
quality may degrade if falling back from Gemini to Claude or vice versa.

**Root cause:** Provider rate limits (429), API outages, or quota exhaustion. The
`FallbackChain` in `llm_provider.py` rotates through providers with per-provider circuit
breakers.

### 3. Rate Limiting Tenant Rejection

**Symptoms:** Tenant gets 429 responses. `rate_limit_rejected_total` counter increments
for that tenant.

**Root cause:** Tenant exceeded their token bucket allocation. Configured via
`AdaptiveTokenBucket` in `token_bucket.py` with Redis-backed state.

### 4. Memory Pressure from Large Ingestions

**Symptoms:** Pod OOMKilled. `container_memory_usage_bytes` approaches the 2Gi limit.

**Root cause:** Large documents (>10MB) loaded entirely into memory during extraction.
The `StreamingIngestionPipeline` in `graph_builder.py` should prevent this, but malformed
payloads can bypass streaming.

### 5. Query Timeout

**Symptoms:** `SlowQueryResponse` alert fires. Queries return 504 or take > 3s (NFR-2 SLA).

**Root cause:** Complex multi-hop Cypher traversals hitting supernodes, missing indexes,
or Neo4j under memory pressure. The `cypher_validator.py` cost estimator should reject
expensive queries, but estimates can be inaccurate.

---

## Troubleshooting Steps

### Circuit Breaker Open

1. Identify which breaker is open:
   ```bash
   kubectl exec -n graphrag deploy/orchestrator -- curl -s localhost:8000/metrics | grep circuit_breaker_state
   ```
2. Check the downstream dependency health:
   - Neo4j: `kubectl exec -n graphrag neo4j-0 -- cypher-shell "RETURN 1"`
   - Redis: `kubectl exec -n graphrag deploy/orchestrator -- python -c "import redis; r=redis.from_url('redis://redis:6379'); print(r.ping())"`
3. The breaker will auto-recover after `recovery_timeout` (30s + jitter). To force
   a reset, restart the orchestrator pod:
   ```bash
   kubectl rollout restart deployment/orchestrator -n graphrag
   ```

### LLM Provider Failover

1. Check provider-specific error rates:
   ```bash
   kubectl exec -n graphrag deploy/orchestrator -- curl -s localhost:8000/metrics | grep llm_request_errors_total
   ```
2. Check provider status pages:
   - Gemini: https://status.cloud.google.com
   - Claude: https://status.anthropic.com
3. If a provider is down, the `FallbackChain` handles automatic failover. Monitor
   `llm_request_duration_seconds` to ensure the fallback provider is not also degraded.

### Rate Limit Investigation

1. Check a specific tenant's rate limit state:
   ```bash
   kubectl exec -n graphrag deploy/orchestrator -- python -c "
   import redis, json
   r = redis.from_url('redis://redis:6379')
   keys = r.keys('graphrag:ratelimit:*')
   for k in keys: print(k.decode(), r.hgetall(k))
   "
   ```
2. If a tenant legitimately needs higher limits, update the tier configuration
   in `token_bucket.py` and redeploy.

### Memory Pressure

1. Check current memory usage:
   ```bash
   kubectl top pods -n graphrag -l app=orchestrator
   ```
2. Check for large in-flight ingestions:
   ```bash
   kubectl logs -n graphrag deploy/orchestrator --tail=100 | grep -i "payload.*bytes"
   ```
3. The `MAX_INGEST_PAYLOAD_BYTES` limit in `main.py` should reject oversized payloads.
   Verify it is configured correctly in the ConfigMap.

---

## Recovery Procedures

### Rolling Restart (Stateless)

The orchestrator is stateless — all state is in Neo4j, Redis, and Kafka:
```bash
kubectl rollout restart deployment/orchestrator -n graphrag
kubectl rollout status deployment/orchestrator -n graphrag --timeout=120s
```

### Recover from Mass Circuit Breaker Trip

If all breakers are open due to a transient downstream failure:
1. Verify the downstream is healthy again.
2. Restart all orchestrator pods to reset in-memory breaker state:
   ```bash
   kubectl rollout restart deployment/orchestrator -n graphrag
   ```
3. If using Redis-backed circuit breaker state (`RedisStateStore`), flush breaker keys:
   ```bash
   kubectl exec -n graphrag deploy/orchestrator -- python -c "
   import redis
   r = redis.from_url('redis://redis:6379')
   keys = r.keys('graphrag:cb:*')
   if keys: r.delete(*keys)
   print(f'Cleared {len(keys)} breaker keys')
   "
   ```

### Scale for Traffic Burst

```bash
kubectl scale deployment/orchestrator -n graphrag --replicas=5
kubectl get hpa orchestrator-hpa -n graphrag
```

---

## Monitoring Queries

### Request Rate and Errors

```promql
# Request rate by endpoint
rate(http_server_request_duration_seconds_count{service="orchestrator"}[5m])

# Error rate (5xx)
sum(rate(http_server_request_duration_seconds_count{service="orchestrator", status_code=~"5.."}[5m]))
/
sum(rate(http_server_request_duration_seconds_count{service="orchestrator"}[5m]))

# p99 query latency (SLA: < 3s)
histogram_quantile(0.99, rate(query_duration_ms_bucket[5m]))
```

### Circuit Breaker State

```promql
# Breaker states (0=closed, 1=half-open, 2=open)
circuit_breaker_state{service="orchestrator"}

# Time spent in open state
changes(circuit_breaker_state{service="orchestrator"}[1h])
```

### LLM Provider Health

```promql
# Per-provider error rate
rate(llm_request_errors_total[5m])

# Per-provider latency
histogram_quantile(0.99, rate(llm_request_duration_seconds_bucket[5m]))

# Token usage (cost tracking)
rate(llm_request_tokens_total[5m])
```

### Resource Utilization

```promql
# Memory usage vs limit
container_memory_usage_bytes{namespace="graphrag", container="orchestrator"}
/
container_spec_memory_limit_bytes{namespace="graphrag", container="orchestrator"}

# CPU utilization
rate(container_cpu_usage_seconds_total{namespace="graphrag", container="orchestrator"}[5m])
```

### Key Alerts (from `alerting.yaml`)

| Alert | Trigger | Action |
|---|---|---|
| `OrchestratorHighErrorRate` | 5xx rate > 5% for 5m | Check logs, downstream health |
| `SlowQueryResponse` | p99 > 3000ms for 5m | Check Neo4j, LLM latency |
| `CircuitBreakerOpen` | Any breaker OPEN > 2m | Check dependency health |
| `QueryAvailabilityBurnRateHigh` | Success rate < 99.5% over 5m | SLO burn, investigate errors |
