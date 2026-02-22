# Deep Technical System Audit: GraphRAG Architect

## Verdict: RED

Seven findings require immediate remediation. Five are CRITICAL/HIGH severity.

## Requirement Gaps

| ID   | Severity | Finding | Location |
|------|----------|---------|----------|
| 1.1  | CRITICAL | Cypher ACL injection bypass via regex WHERE detection | `orchestrator/app/access_control.py` |
| 1.2  | HIGH     | Manifest parser silently drops team_owner/namespace_acl | `orchestrator/app/manifest_parser.py` |
| 2.1  | CRITICAL | Kafka StatefulSet missing KAFKA_ADVERTISED_LISTENERS | `infrastructure/k8s/kafka-statefulset.yaml` |
| 2.2  | CRITICAL | Schema init job blocked by NetworkPolicy (no egress/ingress) | `infrastructure/k8s/network-policies.yaml` |
| 3.1  | HIGH     | Memory exhaustion via synchronous workspace loading | `orchestrator/app/workspace_loader.py` |
| 3.2  | MEDIUM   | Go consumer thread contention causes Kafka rebalances | `workers/ingestion/internal/consumer/consumer.go` |
| 4.0  | HIGH     | Fail-open token verification when AUTH_TOKEN_SECRET missing | `orchestrator/app/access_control.py`, `orchestrator/app/main.py` |

## Detailed Findings

### 1.1 Cypher ACL Injection Bypass (CRITICAL)

`CypherPermissionFilter.inject_into_cypher()` uses `re.search(r"\bWHERE\b", ...)` to locate
WHERE clauses. This regex matches keywords inside nested subqueries (`CALL { MATCH ... WHERE ... }`),
CASE expressions, and string literals. The ACL clause is injected at the wrong position, causing
syntax errors or complete ACL bypass for advanced multi-hop Cypher queries.

### 1.2 Manifest Parser Silently Drops ACLs (HIGH)

`_extract_deployment()` and `_extract_kafka_topic()` ignore `metadata.labels` and
`metadata.annotations` that carry ownership information. `K8sDeploymentNode` and `KafkaTopicNode`
are inserted with `team_owner=None` and `namespace_acl=[]`. Non-admin users querying these
entities receive empty results.

### 2.1 Kafka Advertised Listeners Missing (CRITICAL)

`KAFKA_ADVERTISED_LISTENERS` is omitted from the Kafka StatefulSet env vars. Brokers advertise
`PLAINTEXT://:9092` which is unresolvable from external pods. Go ingestion workers connect to
the bootstrap server but fail partition fetching.

### 2.2 Schema Init Job Blocked by NetworkPolicy (CRITICAL)

The `deny-all` policy blocks all egress. `allow-neo4j-ingress` only permits ingress from
`app: orchestrator`. The `neo4j-schema-init` Job has no egress policy and is not in Neo4j's
ingress whitelist. The schema job hangs indefinitely.

### 3.1 Memory Exhaustion via Synchronous Workspace Loading (HIGH)

`load_directory()` reads all files into a single list in memory. In large monorepos (thousands
of files, even with 1MB cap), this exceeds the 2Gi pod memory limit. The entire list is held
in `IngestionState` throughout the LangGraph DAG.

### 3.2 Go Consumer Thread Contention (MEDIUM)

`Consumer.Run()` blocks on `c.acks` for every job in a batch before polling again. A single
stuck retry blocks the entire consumer, exceeding `session.timeout.ms` and triggering
Kafka consumer group rebalances.

### 4.0 Fail-Open Token Verification (HIGH)

When `AUTH_TOKEN_SECRET` is empty (Helm chart misconfiguration), token verification is skipped
entirely. The service silently accepts forged tokens. `test_no_secret_skips_verification`
explicitly validates this fail-open behavior.
