# Kafka Migration Runbook: KRaft Self-Managed → AWS MSK

## Overview

This runbook guides the migration from the current self-managed KRaft-based
Kafka deployment (running on Kubernetes) to AWS Managed Streaming for Apache
Kafka (MSK). The migration uses a dual-write, dual-read strategy to achieve
zero downtime.

---

## Prerequisites

Before starting the migration, ensure all of the following are in place:

1. **AWS Account & Permissions** — IAM role with `kafka:*`, `ec2:Describe*`,
   `kms:CreateGrant`, and `logs:CreateLogGroup` permissions.
2. **Terraform State** — Remote state backend (S3 + DynamoDB) configured.
3. **Networking** — VPC peering or Transit Gateway between the EKS cluster
   and the MSK VPC subnets. Security groups allow broker ports (9092, 9094).
4. **KMS Key** — A CMK for at-rest encryption provisioned.
5. **Monitoring** — Prometheus scrape targets configured for the new cluster.
6. **Current Kafka Baseline** — Record current topic list, partition counts,
   consumer group offsets, and throughput metrics.

---

## Migration Phases

### Phase 1: Provision MSK Cluster

```bash
cd infrastructure/terraform/kafka
terraform init
terraform plan -var-file=production.tfvars
terraform apply -var-file=production.tfvars
```

Validate the cluster is healthy:

```bash
aws kafka describe-cluster --cluster-arn <ARN> \
  --query 'ClusterInfo.State'
```

Expected output: `"ACTIVE"`.

### Phase 2: Topic Replication (MirrorMaker 2)

Deploy MirrorMaker 2 to replicate all `graphrag.*` topics from the
self-managed cluster to MSK:

```bash
kubectl apply -f infrastructure/k8s/multi-region/mirrormaker.yaml
```

Monitor replication lag:

```bash
kubectl logs -l app=graphrag,component=mirrormaker -f
```

### Phase 3: Dual-Read Validation

Update ingestion workers to consume from **both** clusters in parallel.
Compare message counts and checksums over a 24-hour window.

### Phase 4: Cutover

1. Redirect producers to MSK bootstrap servers.
2. Wait for self-managed consumer lag to reach zero.
3. Switch consumer configuration to MSK.
4. Disable MirrorMaker replication.

### Phase 5: Decommission Self-Managed Cluster

After 7 days of stable operation on MSK:

```bash
kubectl delete statefulset kafka -n graphrag
kubectl delete pvc -l app=kafka -n graphrag
```

---

## Rollback Procedure

If issues arise during or after cutover:

1. **Re-enable MirrorMaker** in reverse direction (MSK → self-managed).
2. **Switch producers** back to the self-managed bootstrap servers.
3. **Switch consumers** back to self-managed.
4. **Investigate** root cause before re-attempting migration.

The self-managed cluster must remain running for at least 7 days post-cutover
to serve as a rollback target.

---

## Validation Checklist

| Check                              | Command / Action                        |
|------------------------------------|-----------------------------------------|
| MSK cluster ACTIVE                 | `aws kafka describe-cluster`            |
| All topics replicated              | Compare `kafka-topics.sh --list` output |
| Consumer offsets translated        | MirrorMaker checkpoint connector logs   |
| Producer throughput matches        | Grafana dashboard comparison            |
| No consumer lag after cutover      | `kafka-consumer-groups.sh --describe`   |
| Alerts firing correctly            | Trigger test alert                      |

---

## References

- `infrastructure/terraform/kafka/` — Terraform MSK module
- `infrastructure/k8s/multi-region/mirrormaker.yaml` — MirrorMaker 2 config
- `docs/multi-region-architecture.md` — Multi-region architecture overview
