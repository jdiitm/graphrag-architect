from __future__ import annotations

import asyncio
import logging
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from neo4j import AsyncDriver, AsyncManagedTransaction, READ_ACCESS, WRITE_ACCESS

from orchestrator.app.circuit_breaker import CircuitBreaker, CircuitBreakerConfig
from orchestrator.app.extraction_models import (
    CallsEdge,
    ConsumesEdge,
    compute_content_hash,
    DatabaseNode,
    DeployedInEdge,
    K8sDeploymentNode,
    KafkaTopicNode,
    ProducesEdge,
    ServiceNode,
)

logger = logging.getLogger(__name__)

DEFAULT_BATCH_SIZE = 100

_VALID_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validate_cypher_identifier(value: str, label: str = "identifier") -> str:
    if not _VALID_IDENTIFIER.match(value):
        raise ValueError(
            f"Invalid Cypher identifier for {label}: {value!r}"
        )
    return value


CypherOp = Tuple[str, Dict[str, Any]]

_NODE_TYPES = (ServiceNode, DatabaseNode, KafkaTopicNode, K8sDeploymentNode)
_EDGE_TYPES = (CallsEdge, ProducesEdge, ConsumesEdge, DeployedInEdge)


def _service_cypher(entity: ServiceNode) -> CypherOp:
    query = (
        "MERGE (n:Service {id: $id}) "
        "SET n.name = $name, n.language = $language, "
        "n.framework = $framework, n.opentelemetry_enabled = $opentelemetry_enabled, "
        "n.tenant_id = $tenant_id, "
        "n.team_owner = $team_owner, n.namespace_acl = $namespace_acl, "
        "n.confidence = $confidence"
    )
    return query, entity.model_dump()


def _database_cypher(entity: DatabaseNode) -> CypherOp:
    query = (
        "MERGE (n:Database {id: $id}) "
        "SET n.type = $type, "
        "n.tenant_id = $tenant_id, "
        "n.team_owner = $team_owner, n.namespace_acl = $namespace_acl"
    )
    return query, entity.model_dump()


def _kafka_topic_cypher(entity: KafkaTopicNode) -> CypherOp:
    query = (
        "MERGE (n:KafkaTopic {name: $name}) "
        "SET n.partitions = $partitions, n.retention_ms = $retention_ms, "
        "n.tenant_id = $tenant_id, "
        "n.team_owner = $team_owner, n.namespace_acl = $namespace_acl"
    )
    return query, entity.model_dump()


def _k8s_deployment_cypher(entity: K8sDeploymentNode) -> CypherOp:
    query = (
        "MERGE (n:K8sDeployment {id: $id}) "
        "SET n.namespace = $namespace, n.replicas = $replicas, "
        "n.tenant_id = $tenant_id, "
        "n.team_owner = $team_owner, n.namespace_acl = $namespace_acl"
    )
    return query, entity.model_dump()


def _calls_cypher(entity: CallsEdge) -> CypherOp:
    query = (
        "MATCH (a:Service {id: $source_service_id}), "
        "(b:Service {id: $target_service_id}) "
        "MERGE (a)-[r:CALLS]->(b) SET r.protocol = $protocol, "
        "r.confidence = $confidence, "
        "r.ingestion_id = $ingestion_id, "
        "r.last_seen_at = $last_seen_at"
    )
    return query, entity.model_dump()


def _produces_cypher(entity: ProducesEdge) -> CypherOp:
    query = (
        "MATCH (s:Service {id: $service_id}), "
        "(t:KafkaTopic {name: $topic_name}) "
        "MERGE (s)-[r:PRODUCES]->(t) SET r.event_schema = $event_schema, "
        "r.ingestion_id = $ingestion_id, "
        "r.last_seen_at = $last_seen_at"
    )
    return query, entity.model_dump()


def _consumes_cypher(entity: ConsumesEdge) -> CypherOp:
    query = (
        "MATCH (s:Service {id: $service_id}), "
        "(t:KafkaTopic {name: $topic_name}) "
        "MERGE (s)-[r:CONSUMES]->(t) SET r.consumer_group = $consumer_group, "
        "r.ingestion_id = $ingestion_id, "
        "r.last_seen_at = $last_seen_at"
    )
    return query, entity.model_dump()


def _deployed_in_cypher(entity: DeployedInEdge) -> CypherOp:
    query = (
        "MATCH (s:Service {id: $service_id}), "
        "(k:K8sDeployment {id: $deployment_id}) "
        "MERGE (s)-[r:DEPLOYED_IN]->(k) "
        "SET r.ingestion_id = $ingestion_id, "
        "r.last_seen_at = $last_seen_at"
    )
    return query, entity.model_dump()


_CYPHER_DISPATCH = {
    ServiceNode: _service_cypher,
    DatabaseNode: _database_cypher,
    KafkaTopicNode: _kafka_topic_cypher,
    K8sDeploymentNode: _k8s_deployment_cypher,
    CallsEdge: _calls_cypher,
    ProducesEdge: _produces_cypher,
    ConsumesEdge: _consumes_cypher,
    DeployedInEdge: _deployed_in_cypher,
}


def compute_hashes(entities: List[Any]) -> List[Any]:
    for entity in entities:
        fields = getattr(type(entity), "model_fields", None)
        if fields is not None and "content_hash" in fields:
            setattr(
                entity,
                "content_hash",
                compute_content_hash(entity),
            )
    return entities


def cypher_op_for_entity(entity: Any) -> CypherOp:
    generator = _CYPHER_DISPATCH.get(type(entity))
    if generator is None:
        raise TypeError(f"Unsupported entity type: {type(entity).__name__}")
    return generator(entity)


def _partition_entities(
    entities: List[Any],
) -> Tuple[List[Any], List[Any]]:
    nodes: List[Any] = []
    edges: List[Any] = []
    for entity in entities:
        if isinstance(entity, _NODE_TYPES):
            nodes.append(entity)
        elif isinstance(entity, _EDGE_TYPES):
            edges.append(entity)
        else:
            raise TypeError(f"Unsupported entity type: {type(entity).__name__}")
    return nodes, edges


def _group_by_type(
    entities: List[Any],
) -> Dict[type, List[Dict[str, Any]]]:
    groups: Dict[type, List[Dict[str, Any]]] = {}
    for entity in entities:
        entity_type = type(entity)
        if entity_type not in groups:
            groups[entity_type] = []
        groups[entity_type].append(entity.model_dump())
    return groups


_UNWIND_QUERIES: Dict[type, str] = {
    ServiceNode: (
        "UNWIND $batch AS row "
        "MERGE (n:Service {id: row.id}) "
        "SET n.name = row.name, n.language = row.language, "
        "n.framework = row.framework, "
        "n.opentelemetry_enabled = row.opentelemetry_enabled, "
        "n.tenant_id = row.tenant_id, "
        "n.team_owner = row.team_owner, "
        "n.namespace_acl = row.namespace_acl, "
        "n.confidence = row.confidence"
    ),
    DatabaseNode: (
        "UNWIND $batch AS row "
        "MERGE (n:Database {id: row.id}) "
        "SET n.type = row.type, "
        "n.tenant_id = row.tenant_id, "
        "n.team_owner = row.team_owner, "
        "n.namespace_acl = row.namespace_acl"
    ),
    KafkaTopicNode: (
        "UNWIND $batch AS row "
        "MERGE (n:KafkaTopic {name: row.name}) "
        "SET n.partitions = row.partitions, "
        "n.retention_ms = row.retention_ms, "
        "n.tenant_id = row.tenant_id, "
        "n.team_owner = row.team_owner, "
        "n.namespace_acl = row.namespace_acl"
    ),
    K8sDeploymentNode: (
        "UNWIND $batch AS row "
        "MERGE (n:K8sDeployment {id: row.id}) "
        "SET n.namespace = row.namespace, "
        "n.replicas = row.replicas, "
        "n.tenant_id = row.tenant_id, "
        "n.team_owner = row.team_owner, "
        "n.namespace_acl = row.namespace_acl"
    ),
    CallsEdge: (
        "UNWIND $batch AS row "
        "MATCH (a:Service {id: row.source_service_id}), "
        "(b:Service {id: row.target_service_id}) "
        "MERGE (a)-[r:CALLS]->(b) "
        "SET r.protocol = row.protocol, "
        "r.confidence = row.confidence, "
        "r.ingestion_id = row.ingestion_id, "
        "r.last_seen_at = row.last_seen_at"
    ),
    ProducesEdge: (
        "UNWIND $batch AS row "
        "MATCH (s:Service {id: row.service_id}), "
        "(t:KafkaTopic {name: row.topic_name}) "
        "MERGE (s)-[r:PRODUCES]->(t) "
        "SET r.event_schema = row.event_schema, "
        "r.ingestion_id = row.ingestion_id, "
        "r.last_seen_at = row.last_seen_at"
    ),
    ConsumesEdge: (
        "UNWIND $batch AS row "
        "MATCH (s:Service {id: row.service_id}), "
        "(t:KafkaTopic {name: row.topic_name}) "
        "MERGE (s)-[r:CONSUMES]->(t) "
        "SET r.consumer_group = row.consumer_group, "
        "r.ingestion_id = row.ingestion_id, "
        "r.last_seen_at = row.last_seen_at"
    ),
    DeployedInEdge: (
        "UNWIND $batch AS row "
        "MATCH (s:Service {id: row.service_id}), "
        "(k:K8sDeployment {id: row.deployment_id}) "
        "MERGE (s)-[r:DEPLOYED_IN]->(k) "
        "SET r.ingestion_id = row.ingestion_id, "
        "r.last_seen_at = row.last_seen_at"
    ),
}


def _chunk_list(
    items: List[Any], size: int,
) -> List[List[Any]]:
    return [items[i:i + size] for i in range(0, len(items), size)]


DEFAULT_WRITE_CONCURRENCY = 4


class GraphRepository:
    def __init__(
        self,
        driver: AsyncDriver,
        circuit_breaker: Optional[CircuitBreaker] = None,
        batch_size: int = DEFAULT_BATCH_SIZE,
        database: Optional[str] = None,
        write_concurrency: Optional[int] = None,
    ) -> None:
        self._driver = driver
        self._cb = circuit_breaker or CircuitBreaker(CircuitBreakerConfig())
        self._batch_size = batch_size
        self._database = database
        resolved = (
            write_concurrency
            if write_concurrency is not None
            else int(os.environ.get(
                "WRITE_CONCURRENCY", str(DEFAULT_WRITE_CONCURRENCY),
            ))
        )
        self._write_concurrency = max(1, resolved)

    def _session(self, access_mode: Optional[str] = None) -> Any:
        kwargs: Dict[str, Any] = {}
        if self._database:
            kwargs["database"] = self._database
        if access_mode is not None:
            kwargs["default_access_mode"] = access_mode
        return self._driver.session(**kwargs)

    def _read_session(self) -> Any:
        return self._session(access_mode=READ_ACCESS)

    def _write_session(self) -> Any:
        return self._session(access_mode=WRITE_ACCESS)

    async def read_topology(self, label: str = "Service") -> List[Dict[str, Any]]:
        _validate_cypher_identifier(label)
        cypher = f"MATCH (n:{label}) RETURN n"

        async def _tx(tx: AsyncManagedTransaction) -> list:
            result = await tx.run(cypher)
            return await result.data()

        async with self._read_session() as session:
            return await session.execute_read(_tx)

    async def commit_topology(self, entities: List[Any]) -> None:
        if not entities:
            return

        entities = compute_hashes(entities)
        nodes, edges = _partition_entities(entities)
        await self._cb.call(self._execute_batched_commit, nodes, edges)

    async def _execute_batched_commit(
        self, nodes: List[Any], edges: List[Any],
    ) -> None:
        semaphore = asyncio.Semaphore(self._write_concurrency)

        async def _guarded_write(
            entity_type: type, records: List[Dict[str, Any]],
        ) -> None:
            async with semaphore:
                await self._write_batches(entity_type, records)

        node_groups = _group_by_type(nodes)
        if node_groups:
            await asyncio.gather(*(
                _guarded_write(et, recs)
                for et, recs in node_groups.items()
            ))

        edge_groups = _group_by_type(edges)
        if edge_groups:
            await asyncio.gather(*(
                _guarded_write(et, recs)
                for et, recs in edge_groups.items()
            ))

    async def _write_batches(
        self,
        entity_type: type,
        records: List[Dict[str, Any]],
    ) -> None:
        unwind_query = _UNWIND_QUERIES.get(entity_type)
        if unwind_query is None:
            raise TypeError(
                f"No UNWIND query registered for {entity_type.__name__}; "
                f"{len(records)} records would be dropped"
            )

        for chunk in _chunk_list(records, self._batch_size):
            async with self._write_session() as session:
                await session.execute_write(
                    self._run_unwind, query=unwind_query, batch=chunk,
                )

    @staticmethod
    async def _run_unwind(
        tx: AsyncManagedTransaction,
        query: str,
        batch: List[Dict[str, Any]],
    ) -> None:
        await tx.run(query, batch=batch)

    @staticmethod
    async def _run_unwind_with_tenant(
        tx: AsyncManagedTransaction,
        query: str,
        batch: List[Dict[str, Any]],
        tenant_id: str,
    ) -> None:
        await tx.run(query, batch=batch, tenant_id=tenant_id)

    async def prune_stale_edges(
        self,
        current_ingestion_id: str,
        max_age_hours: int = 24,
    ) -> tuple[int, list[str]]:
        return await self.tombstone_stale_edges(current_ingestion_id)

    _TOMBSTONE_NODE_LABELS = ("Service", "Database", "KafkaTopic", "K8sDeployment")

    async def tombstone_stale_edges(
        self,
        current_ingestion_id: str,
    ) -> tuple[int, list[str]]:
        timestamp = datetime.now(timezone.utc).isoformat()
        total = 0
        affected_node_ids: list[str] = []
        async with self._write_session() as session:
            for label in self._TOMBSTONE_NODE_LABELS:
                query = (
                    f"MATCH (n:{label})-[r]->() "
                    "WHERE r.ingestion_id IS NOT NULL "
                    "AND r.ingestion_id <> $current_id "
                    "AND r.tombstoned_at IS NULL "
                    "SET r.tombstoned_at = $timestamp "
                    "RETURN count(r) AS tombstoned, "
                    "collect(DISTINCT n.id) AS node_ids"
                )
                result = await session.execute_write(
                    self._run_tombstone_with_ids, query=query,
                    current_id=current_ingestion_id, timestamp=timestamp,
                )
                count, ids = result
                total += count
                affected_node_ids.extend(ids)
        return total, affected_node_ids

    @staticmethod
    async def _run_tombstone_with_ids(
        tx: AsyncManagedTransaction,
        query: str,
        current_id: str,
        timestamp: str,
    ) -> tuple[int, list[str]]:
        result = await tx.run(
            query, current_id=current_id, timestamp=timestamp,
        )
        record = await result.single()
        if record is None:
            return 0, []
        return record["tombstoned"], list(record.get("node_ids", []))

    async def reap_tombstoned_edges(
        self,
        ttl_days: int = 7,
        batch_size: int = 100,
    ) -> int:
        cutoff = (
            datetime.now(timezone.utc) - timedelta(days=ttl_days)
        ).isoformat()
        total_reaped = 0
        while True:
            query = (
                "MATCH ()-[r]->() "
                "WHERE r.tombstoned_at IS NOT NULL "
                "AND r.tombstoned_at < $cutoff "
                "WITH r LIMIT $batch_size "
                "DELETE r RETURN count(r) AS reaped"
            )
            async with self._session() as session:
                reaped = await session.execute_write(
                    self._run_reap, query=query,
                    cutoff=cutoff, batch_size=batch_size,
                )
                total_reaped += reaped
                if reaped < batch_size:
                    break
        return total_reaped

    @staticmethod
    async def _run_reap(
        tx: AsyncManagedTransaction,
        query: str,
        cutoff: str,
        batch_size: int,
    ) -> int:
        result = await tx.run(
            query, cutoff=cutoff, batch_size=batch_size,
        )
        record = await result.single()
        if record is None:
            return 0
        return record["reaped"]

    async def create_vector_index(
        self,
        index_name: str = "service_embedding_index",
        label: str = "Service",
        property_name: str = "embedding",
        dimensions: int = 1536,
    ) -> None:
        _validate_cypher_identifier(index_name, "index_name")
        _validate_cypher_identifier(label, "label")
        _validate_cypher_identifier(property_name, "property_name")
        cypher = (
            f"CREATE VECTOR INDEX {index_name} IF NOT EXISTS "
            f"FOR (n:{label}) ON (n.{property_name}) "
            f"OPTIONS {{indexConfig: {{"
            f"`vector.dimensions`: {dimensions}, "
            f"`vector.similarity_function`: 'cosine'"
            f"}}}}"
        )
        async with self._session() as session:
            await session.run(cypher)

    async def upsert_embeddings(
        self,
        label: str,
        id_field: str,
        embeddings: List[Dict[str, Any]],
        tenant_id: Optional[str] = None,
    ) -> None:
        if not embeddings:
            return
        _validate_cypher_identifier(label, "label")
        _validate_cypher_identifier(id_field, "id_field")
        if tenant_id:
            cypher = (
                f"UNWIND $batch AS item "
                f"MATCH (n:{label} {{{id_field}: item.id, tenant_id: $tenant_id}}) "
                f"SET n.embedding = item.embedding"
            )
        else:
            cypher = (
                f"UNWIND $batch AS item "
                f"MATCH (n:{label} {{{id_field}: item.id}}) "
                f"SET n.embedding = item.embedding"
            )
        async with self._session() as session:
            if tenant_id:
                await session.execute_write(
                    self._run_unwind_with_tenant,
                    query=cypher, batch=embeddings, tenant_id=tenant_id,
                )
            else:
                await session.execute_write(
                    self._run_unwind, query=cypher, batch=embeddings,
                )
