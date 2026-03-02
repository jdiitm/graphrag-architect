# Schema Migration Procedures

## Prerequisites

Before running any schema migration:

1. **Backup the Neo4j database.** Take a full online backup or snapshot the PVCs:
   ```bash
   kubectl exec -n graphrag neo4j-0 -- neo4j-admin database dump neo4j --to-path=/data/backups/
   kubectl cp graphrag/neo4j-0:/data/backups/neo4j.dump ./neo4j-backup-$(date +%Y%m%d).dump
   ```

2. **Schedule a maintenance window.** Migrations acquire schema locks that block writes.
   Coordinate with the team via `#graphrag-incidents` Slack channel.

3. **Verify current schema version:**
   ```cypher
   MATCH (p:_SchemaPointer)
   RETURN p.major AS major, p.minor AS minor, p.patch AS patch
   ```
   If no `_SchemaPointer` node exists, the database is at version 0.0.0 (initial state).

4. **Review the migration plan.** Each migration has an `up` function (apply) and an
   optional `down` function (rollback). Read the migration descriptions before applying.

5. **Test on staging first.** Never apply migrations directly to production without
   validating on a staging environment with representative data.

---

## Migration Procedure

GraphRAG uses the `MigrationRegistry` from `orchestrator/app/schema_evolution.py` to manage
schema versions. The registry tracks migrations as `SchemaVersion` objects (MAJOR.MINOR.PATCH)
and stores the current version via a `VersionStore` backend.

### Architecture

```
MigrationRegistry
├── register(migration)     # Add a migration to the registry
├── pending()               # List migrations not yet applied
├── apply_all()             # Apply all pending migrations in order
├── rollback_last()         # Rollback the most recent migration
└── current_version()       # Query the current schema version

VersionStore (Protocol)
├── InMemoryVersionStore    # For testing
├── Neo4jVersionStore       # Production — stores _SchemaPointer node
└── RedisVersionStore       # Alternative — stores in Redis hash
```

### Defining a Migration

```python
from orchestrator.app.schema_evolution import (
    Migration,
    MigrationRegistry,
    SchemaVersion,
    create_version_store,
)

def up_add_pagerank_index():
    # Execute via Neo4j driver
    session.run(
        "CREATE INDEX service_pagerank_idx IF NOT EXISTS "
        "FOR (s:Service) ON (s.tenant_id, s.pagerank)"
    )

def down_remove_pagerank_index():
    session.run("DROP INDEX service_pagerank_idx IF EXISTS")

migration = Migration(
    version=SchemaVersion(1, 1, 0),
    description="Add pagerank index for Service nodes",
    up=up_add_pagerank_index,
    down=down_remove_pagerank_index,
)
```

### Applying Migrations

```python
store = create_version_store("neo4j", driver=neo4j_driver)
registry = MigrationRegistry(store)

# Register all known migrations
registry.register(migration_1_0_0)
registry.register(migration_1_1_0)
registry.register(migration_1_2_0)

# Check what will be applied
pending = registry.pending()
print(f"Current version: {registry.current_version()}")
print(f"Pending migrations: {len(pending)}")
for m in pending:
    print(f"  {m.version}: {m.description}")

# Apply all pending
applied = registry.apply_all()
print(f"Applied {applied} migrations. New version: {registry.current_version()}")
```

### Step-by-Step Production Procedure

1. **Scale down consumers** to prevent writes during migration:
   ```bash
   kubectl scale deployment ingestion-worker -n graphrag --replicas=0
   ```

2. **Verify no in-flight transactions:**
   ```cypher
   CALL dbms.listTransactions() YIELD transactionId, elapsedTime
   WHERE elapsedTime > duration('PT5S')
   RETURN count(*) AS active_transactions
   ```

3. **Run the migration** (from a Job or local script):
   ```bash
   kubectl run schema-migrate --rm -it --restart=Never \
     --image=graphrag-architect/orchestrator:latest \
     --env-from=configmap/graphrag-config \
     --env-from=secret/graphrag-secrets \
     -- python -m orchestrator.migrate
   ```

4. **Verify the new version:**
   ```cypher
   MATCH (p:_SchemaPointer)
   RETURN p.major, p.minor, p.patch
   ```

5. **Scale consumers back up:**
   ```bash
   kubectl scale deployment ingestion-worker -n graphrag --replicas=2
   ```

---

## Rollback Steps

If a migration fails or causes issues:

### Automatic Rollback (Migration Failure)

The `MigrationRegistry.apply_all()` method stops on the first failure and records
`MigrationStatus.FAILED` in the VersionStore. The schema version pointer remains at the
last successfully applied version.

### Manual Rollback

1. **Identify the current version and the target rollback version:**
   ```cypher
   MATCH (p:_SchemaPointer)
   RETURN p.major, p.minor, p.patch
   ```

2. **Execute the rollback:**
   ```python
   store = create_version_store("neo4j", driver=neo4j_driver)
   registry = MigrationRegistry(store)
   # Register all migrations (including the one to roll back)
   registry.register(migration_1_0_0)
   registry.register(migration_1_1_0)

   success = registry.rollback_last()
   if success:
       print(f"Rolled back. Now at version: {registry.current_version()}")
   else:
       print("Rollback failed — no down() defined or no version to roll back")
   ```

3. **Verify data integrity** after rollback (see Verification Queries below).

### Emergency Rollback (Restore from Backup)

If the `down()` function is not defined or rollback fails:

1. Scale all services to 0:
   ```bash
   kubectl scale deployment orchestrator ingestion-worker -n graphrag --replicas=0
   ```
2. Restore the backup taken in Prerequisites:
   ```bash
   kubectl cp ./neo4j-backup-YYYYMMDD.dump graphrag/neo4j-0:/data/backups/neo4j.dump
   kubectl exec -n graphrag neo4j-0 -- neo4j-admin database load \
     --from-path=/data/backups/neo4j.dump neo4j --overwrite-destination
   ```
3. Restart Neo4j:
   ```bash
   kubectl rollout restart statefulset neo4j -n graphrag
   ```
4. Scale services back up.

---

## Verification Queries

After any migration, run these queries to verify schema integrity:

### Constraint Verification

```cypher
SHOW CONSTRAINTS
YIELD name, type, entityType, labelsOrTypes, properties, ownedIndex
RETURN name, type, entityType, labelsOrTypes, properties
ORDER BY name
```

Expected constraints (from `schema_init.cypher`):
- `service_tenant_id` — `(Service) REQUIRE (tenant_id, id) IS NODE KEY`
- `database_tenant_id` — `(Database) REQUIRE (tenant_id, id) IS NODE KEY`
- `topic_tenant_name` — `(KafkaTopic) REQUIRE (tenant_id, name) IS NODE KEY`
- `k8s_deploy_tenant_id` — `(K8sDeployment) REQUIRE (tenant_id, id) IS NODE KEY`
- `k8s_pod_tenant_id` — `(K8sPod) REQUIRE (tenant_id, id) IS NODE KEY`

### Index Verification

```cypher
SHOW INDEXES
YIELD name, type, entityType, labelsOrTypes, properties, state
WHERE state <> 'ONLINE'
RETURN name, state
```

All indexes should be in `ONLINE` state. If any are `POPULATING`, wait for them to
complete before resuming traffic.

### Data Integrity Checks

```cypher
// Verify all nodes have tenant_id (mandatory invariant)
MATCH (n)
WHERE n.tenant_id IS NULL
  AND NOT n:_SchemaPointer
  AND NOT n:_SchemaMigration
RETURN labels(n) AS labels, count(*) AS orphaned_nodes

// Verify no duplicate services within a tenant
MATCH (s:Service)
WITH s.tenant_id AS tid, s.id AS sid, count(*) AS cnt
WHERE cnt > 1
RETURN tid, sid, cnt

// Verify relationship integrity
MATCH (s:Service)-[r:DEPLOYED_IN]->(k:K8sDeployment)
WHERE s.tenant_id <> k.tenant_id
RETURN s.id, k.id, s.tenant_id, k.tenant_id
```

### Schema Version History

```cypher
MATCH (m:_SchemaMigration)
RETURN m.major, m.minor, m.patch, m.status, m.recorded_at
ORDER BY m.recorded_at DESC
LIMIT 20
```

---

## Version Store Backends

The `create_version_store()` factory supports three backends:

| Backend | Use Case | Storage |
|---|---|---|
| `memory` | Unit tests | In-process dictionary |
| `neo4j` | Production | `_SchemaPointer` node + `_SchemaMigration` history nodes |
| `redis` | Alternative | `graphrag:schema:current_version` key + `graphrag:schema:migration_history` list |

For production, always use the `neo4j` backend so the schema version is co-located with the
data it describes.
