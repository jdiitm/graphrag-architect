# SOC2 Type II Gap Analysis — GraphRAG System

## Overview

This document maps SOC2 Trust Services Criteria (TSC) to existing and planned controls
in the GraphRAG system. Each criterion is assessed with current implementation status,
evidence sources, and remediation actions.

## Trust Services Categories

### Security

| Criterion | Description | Control Mapping | Status | Evidence |
|-----------|-------------|-----------------|--------|----------|
| CC1.1 | COSO principle: integrity and ethical values | Code review policy, branch protection | Implemented | GitHub branch rules, PR reviews |
| CC1.2 | Board oversight of internal controls | Architecture Decision Records (ADRs) | Implemented | docs/adr/ |
| CC2.1 | Information and communication objectives | OpenAPI spec, runbooks | Implemented | docs/openapi.json, docs/runbooks/ |
| CC3.1 | Risk assessment objectives | Trivy scanning, gitleaks, SBOM | Implemented | .github/workflows/ci.yml |
| CC5.1 | Control activities over technology | Network policies, RBAC, Vault | Implemented | infrastructure/k8s/network-policies.yaml |
| CC6.1 | Logical and physical access controls | JWT auth, tenant isolation, mTLS | Implemented | orchestrator/app/access_control.py |
| CC6.2 | System authentication | Token-based auth with rotation | Implemented | orchestrator/app/auth.py |
| CC6.3 | Authorization of access | Role-based access, tenant boundaries | Implemented | orchestrator/app/tenant_isolation.py |
| CC7.1 | System monitoring and detection | Prometheus alerting, Grafana dashboards | Implemented | infrastructure/grafana/, alerting.yaml |
| CC7.2 | Incident response procedures | Incident response runbook | Implemented | docs/incident-response.md |
| CC7.3 | Remediation of identified vulnerabilities | Trivy, Dependabot, security scanning | Partial | CI pipeline, needs dependency auto-update |
| CC8.1 | Change management processes | Git-based workflow, CI/CD gates | Implemented | .github/workflows/ci.yml |
| CC9.1 | Risk mitigation activities | DLQ handling, circuit breakers, retries | Implemented | orchestrator/app/circuit_breaker.py |

### Availability

| Criterion | Description | Control Mapping | Status | Evidence |
|-----------|-------------|-----------------|--------|----------|
| A1.1 | Processing capacity and availability | HPA, resource limits, replicas | Implemented | infrastructure/k8s/hpa.yaml |
| A1.2 | Recovery procedures | StatefulSet persistence, backup strategy | Partial | neo4j-statefulset.yaml, needs backup automation |
| A1.3 | System component recovery testing | Health probes, readiness checks | Implemented | deployment manifests |

### Confidentiality

| Criterion | Description | Control Mapping | Status | Evidence |
|-----------|-------------|-----------------|--------|----------|
| C1.1 | Confidential information identification | Secrets management via Vault, K8s secrets | Implemented | vault-agent-config.yaml |
| C1.2 | Confidential information disposal | TTL-based dedup, log redaction | Partial | Needs explicit data retention policy |

## Gap Summary

### Critical Gaps (Remediation Required)
1. **CC7.3** — Automated dependency updates not fully configured
2. **A1.2** — Database backup automation not implemented
3. **C1.2** — Formal data retention and disposal policy missing

### Recommended Actions
1. Enable Dependabot or Renovate for automated dependency PRs
2. Implement Neo4j backup CronJob with offsite storage
3. Document data retention policy and implement TTL-based cleanup

## Evidence Inventory

| Evidence Type | Location | Description |
|--------------|----------|-------------|
| CI Pipeline | .github/workflows/ci.yml | 8-job pipeline with lint, test, security, integration |
| Network Policies | infrastructure/k8s/network-policies.yaml | deny-all + allowlist policies |
| Alerting Rules | infrastructure/k8s/alerting.yaml | SLO recording rules and alerts |
| Incident Response | docs/incident-response.md | Severity levels, escalation, post-mortem |
| Secret Scanning | .gitleaks.toml + CI gitleaks job | Full-history secret detection |
| Container Scanning | CI docker-build job | Trivy image scan + CycloneDX SBOM |
| Access Control | orchestrator/app/access_control.py | Token auth, tenant isolation |
| Encryption | infrastructure/k8s/cert-manager-issuer.yaml | mTLS with cert-manager auto-rotation |
