# Chaos Engineering Experiments

Chaos Mesh experiments for validating GraphRAG system resilience under
failure conditions. Each experiment follows a steady-state / inject / verify
pattern per the Principles of Chaos Engineering.

## Prerequisites

1. **Chaos Mesh** installed in the cluster:

```bash
helm repo add chaos-mesh https://charts.chaos-mesh.org
helm install chaos-mesh chaos-mesh/chaos-mesh \
  --namespace chaos-testing --create-namespace \
  --set chaosDaemon.runtime=containerd \
  --set chaosDaemon.socketPath=/run/containerd/containerd.sock
```

2. **RBAC**: The Chaos Mesh service account needs permissions in the
   `graphrag` namespace. Apply the provided RBAC or use cluster-admin
   for testing environments only.

3. **GraphRAG namespace** fully deployed with all workloads healthy.

## Experiments

| File | Target | Fault Type | Validates |
|---|---|---|---|
| `neo4j-leader-failure.yaml` | Neo4j leader (neo4j-0) | PodChaos pod-kill | Cluster re-elects leader within 300s |
| `kafka-broker-death.yaml` | Kafka broker (kafka-1) | PodChaos pod-kill | ISR recovers to >= 2 replicas within 200s |
| `orchestrator-pod-kill.yaml` | Orchestrator pod | PodChaos pod-kill | K8s restarts pod, /health returns 200 within 150s |
| `network-partition.yaml` | Orchestrator <-> Neo4j | NetworkChaos partition | Circuit breaker activates; recovery after heal |

## Running Experiments

### Single Experiment

```bash
kubectl apply -f infrastructure/chaos/neo4j-leader-failure.yaml
kubectl -n graphrag get workflow neo4j-leader-failure -w
```

### All Experiments (sequential)

```bash
for f in infrastructure/chaos/*.yaml; do
  [ "$(basename "$f")" = "README.md" ] && continue
  echo "--- Running: $f ---"
  kubectl apply -f "$f"
  kubectl -n graphrag wait --for=condition=Accomplished \
    workflow/$(basename "$f" .yaml) --timeout=600s
  kubectl delete -f "$f"
done
```

### Via Chaos Dashboard

```bash
kubectl port-forward -n chaos-testing svc/chaos-dashboard 2333:2333
# Open http://localhost:2333, import YAML files via the UI
```

## Interpreting Results

Each workflow has three phases:

1. **steady-state-check** — Confirms the system is healthy before injection.
   If this fails, the system was already degraded.
2. **inject-\*** — Applies the fault. Duration is bounded by `deadline`.
3. **verify-recovery** — Polls until the system returns to steady state
   or times out (reported as failure).

Check workflow status:

```bash
kubectl -n graphrag describe workflow <experiment-name>
kubectl -n graphrag logs -l experiment=<experiment-name> --tail=50
```

## Safety

- All experiments are namespace-scoped to `graphrag`.
- Injection durations are bounded (60-120s).
- Recovery verification has finite retry counts with timeouts.
- Run in staging/dev environments before production.
- Ensure PDB (PodDisruptionBudget) policies are in place before
  running in production.
