# ADR-005: Kubernetes Deployment Model

## Status

Accepted

## Context

The system comprises multiple components (orchestrator, ingestion workers, Neo4j, Kafka,
Redis, Qdrant) that must be deployed, scaled, and managed independently. Need consistent
deployment across development, staging, and production with infrastructure-as-code.

Evaluated options: Docker Compose only, raw K8s manifests, Helm charts, Kustomize.

## Decision

Use a layered Kubernetes deployment model:
- Docker Compose for local development
- Raw K8s manifests for base infrastructure (namespace, configmaps, secrets, statefulsets)
- Helm chart for application deployments (orchestrator, ingestion workers) with values-based configuration

## Consequences

- Dual maintenance of Docker Compose and K8s manifests (mitigated by CI validation)
- Helm chart provides templating for environment-specific configuration
- Raw manifests allow fine-grained control of infrastructure components
- HPA configured for auto-scaling orchestrator (CPU) and ingestion workers (consumer lag)
- Network policies enforce zero-trust pod-to-pod communication
- cert-manager integration for automated TLS certificate rotation
