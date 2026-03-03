# Chaos Engineering Experiments

Chaos Mesh experiments for validating GraphRAG system resilience under failure conditions.

## Prerequisites

- Kubernetes cluster with [Chaos Mesh](https://chaos-mesh.org/) v2.6+ installed
- `kubectl` configured for the `graphrag` namespace
- All workloads healthy (`kubectl get pods -n graphrag`)

## Experiments

| File | Target | Failure Mode | Expected Recovery |
|------|--------|--------------|-------------------|
| `neo4j-leader-failure.yaml` | Neo4j primary (neo4j-0) | Pod kill | Cluster re-elects leader within 60s |
| `kafka-broker-death.yaml` | Kafka broker (kafka-1) | Pod kill | ISR catches up within 120s |
| `orchestrator-pod-kill.yaml` | Orchestrator pod | Pod kill | Deployment restores replica within 30s |
| `network-partition.yaml` | Orchestrator / Neo4j | Network partition | Circuit breaker trips; reconnects after partition heals |

## Running an Experiment

```bash
kubectl apply -f infrastructure/chaos/neo4j-leader-failure.yaml
kubectl get workflow -n graphrag -w
```

## Cleanup

```bash
kubectl delete workflows --all -n graphrag
```

## Circuit Breaker Integration

The network partition experiment validates the orchestrator circuit breaker
(`orchestrator/app/circuit_breaker.py`). During partition the breaker transitions
CLOSED -> OPEN after consecutive Neo4j failures. After partition heals it
transitions OPEN -> HALF_OPEN -> CLOSED.

## Safety

- Experiments target the `graphrag` namespace only
- Pod kill experiments use `mode: one` to limit blast radius
- All experiments have `deadline` limits to auto-revert
- Run in staging environments first
