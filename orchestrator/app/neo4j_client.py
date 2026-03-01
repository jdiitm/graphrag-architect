from __future__ import annotations

import asyncio
import logging
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from neo4j import AsyncDriver, AsyncManagedTransaction, READ_ACCESS, WRITE_ACCESS

from orchestrator.app.circuit_breaker import CircuitBreaker, CircuitBreakerConfig
from orchestrator.app.config import HotTargetConfig
from orchestrator.app.ontology import (
    Ontology,
    build_default_ontology,
    generate_edge_merge_cypher,
    generate_edge_unwind_cypher,
    generate_merge_cypher,
    generate_unwind_cypher,
)
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

_MODEL_TO_NODE_LABEL: Dict[type, str] = {
    ServiceNode: "Service",
    DatabaseNode: "Database",
    KafkaTopicNode: "KafkaTopic",
    K8sDeploymentNode: "K8sDeployment",
}

_MODEL_TO_EDGE_TYPE: Dict[type, str] = {
    CallsEdge: "CALLS",
    ProducesEdge: "PRODUCES",
    ConsumesEdge: "CONSUMES",
    DeployedInEdge: "DEPLOYED_IN",
}


def _make_node_cypher_fn(
    label: str, ontology: Ontology,
) -> Callable[[Any], CypherOp]:
    node_def = ontology.get_node_type(label)
    if node_def is None:
        raise ValueError(f"Ontology has no node type: {label!r}")
    query = generate_merge_cypher(label, node_def)

    def _cypher_fn(entity: Any) -> CypherOp:
        return query, entity.model_dump()

    return _cypher_fn


def _make_edge_cypher_fn(
    rel_type: str, ontology: Ontology,
) -> Callable[[Any], CypherOp]:
    edge_def = ontology.get_edge_type(rel_type)
    if edge_def is None:
        raise ValueError(f"Ontology has no edge type: {rel_type!r}")
    query = generate_edge_merge_cypher(rel_type, edge_def)

    def _cypher_fn(entity: Any) -> CypherOp:
        return query, entity.model_dump()

    return _cypher_fn


def build_cypher_dispatch(
    ontology: Optional[Ontology] = None,
) -> Dict[type, Callable[[Any], CypherOp]]:
    resolved = ontology or build_default_ontology()
    dispatch: Dict[type, Callable[[Any], CypherOp]] = {}
    for model_type, label in _MODEL_TO_NODE_LABEL.items():
        if resolved.get_node_type(label) is not None:
            dispatch[model_type] = _make_node_cypher_fn(label, resolved)
    for model_type, rel_type in _MODEL_TO_EDGE_TYPE.items():
        if resolved.get_edge_type(rel_type) is not None:
            dispatch[model_type] = _make_edge_cypher_fn(rel_type, resolved)
    return dispatch


_CYPHER_DISPATCH: Dict[type, Callable[[Any], CypherOp]] = build_cypher_dispatch()

_service_cypher = _CYPHER_DISPATCH[ServiceNode]
_database_cypher = _CYPHER_DISPATCH[DatabaseNode]
_kafka_topic_cypher = _CYPHER_DISPATCH[KafkaTopicNode]
_k8s_deployment_cypher = _CYPHER_DISPATCH[K8sDeploymentNode]
_calls_cypher = _CYPHER_DISPATCH[CallsEdge]
_produces_cypher = _CYPHER_DISPATCH[ProducesEdge]
_consumes_cypher = _CYPHER_DISPATCH[ConsumesEdge]
_deployed_in_cypher = _CYPHER_DISPATCH[DeployedInEdge]


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


def _entity_sort_key(entity: Any) -> Tuple[str, str]:
    type_name = type(entity).__name__
    identity = (
        getattr(entity, "id", "")
        or getattr(entity, "name", "")
        or getattr(entity, "source_service_id", "")
        or getattr(entity, "service_id", "")
    )
    return (type_name, str(identity))


def _sort_entities_for_write(entities: List[Any]) -> List[Any]:
    return sorted(entities, key=_entity_sort_key)


_HOT_TARGET_DEFAULT_THRESHOLD = 10


def _edge_target_id(edge: Any) -> str:
    return (
        getattr(edge, "target_service_id", "")
        or getattr(edge, "topic_name", "")
        or getattr(edge, "deployment_id", "")
    )


def detect_hot_targets(
    edges: List[Any],
    threshold: int = _HOT_TARGET_DEFAULT_THRESHOLD,
) -> Set[str]:
    target_counts: Dict[str, int] = {}
    for edge in edges:
        target = _edge_target_id(edge)
        if target:
            target_counts[target] = target_counts.get(target, 0) + 1
    return {tid for tid, count in target_counts.items() if count >= threshold}


def split_hot_targets(
    edges: List[Any],
    threshold: int = _HOT_TARGET_DEFAULT_THRESHOLD,
) -> Tuple[List[Any], List[Any]]:
    hot_ids = detect_hot_targets(edges, threshold=threshold)
    if not hot_ids:
        return edges, []
    regular: List[Any] = []
    hot: List[Any] = []
    for edge in edges:
        target = _edge_target_id(edge)
        if target in hot_ids:
            hot.append(edge)
        else:
            regular.append(edge)
    return regular, hot


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


def build_unwind_queries(
    ontology: Optional[Ontology] = None,
) -> Dict[type, str]:
    resolved = ontology or build_default_ontology()
    queries: Dict[type, str] = {}
    for model_type, label in _MODEL_TO_NODE_LABEL.items():
        node_def = resolved.get_node_type(label)
        if node_def is not None:
            queries[model_type] = generate_unwind_cypher(label, node_def)
    for model_type, rel_type in _MODEL_TO_EDGE_TYPE.items():
        edge_def = resolved.get_edge_type(rel_type)
        if edge_def is not None:
            queries[model_type] = generate_edge_unwind_cypher(rel_type, edge_def)
    return queries


_UNWIND_QUERIES = build_unwind_queries()


def _chunk_list(
    items: List[Any], size: int,
) -> List[List[Any]]:
    return [items[i:i + size] for i in range(0, len(items), size)]


DEFAULT_WRITE_CONCURRENCY = 4

_IDENTITY_ATTRS = (
    "id", "name", "source_service_id", "target_service_id",
    "service_id", "deployment_id", "topic_name",
)


def _collect_affected_node_ids(entities: List[Any]) -> List[str]:
    ids: set[str] = set()
    for entity in entities:
        for attr in _IDENTITY_ATTRS:
            val = getattr(entity, attr, None)
            if val:
                ids.add(val)
    return sorted(ids)


class GraphRepository:
    def __init__(
        self,
        driver: AsyncDriver,
        circuit_breaker: Optional[CircuitBreaker] = None,
        batch_size: int = DEFAULT_BATCH_SIZE,
        database: Optional[str] = None,
        write_concurrency: Optional[int] = None,
        outbox_store: Optional[Any] = None,
        ontology: Optional[Ontology] = None,
        hot_target_config: Optional[HotTargetConfig] = None,
    ) -> None:
        self._driver = driver
        self._cb = circuit_breaker or CircuitBreaker(CircuitBreakerConfig())
        self._batch_size = batch_size
        self._database = database
        self._outbox_store = outbox_store
        self._ontology = ontology or build_default_ontology()
        self._unwind_queries = build_unwind_queries(ontology=self._ontology)
        self._hot_target_config = hot_target_config or HotTargetConfig()
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

        entities = _sort_entities_for_write(entities)
        entities = compute_hashes(entities)
        nodes, edges = _partition_entities(entities)
        await self._cb.call(self._execute_batched_commit, nodes, edges)

    async def commit_topology_with_affected_ids(
        self, entities: List[Any],
    ) -> List[str]:
        if not entities:
            return []

        entities = _sort_entities_for_write(entities)
        entities = compute_hashes(entities)
        nodes, edges = _partition_entities(entities)
        affected_ids = _collect_affected_node_ids(entities)
        await self._cb.call(self._execute_batched_commit, nodes, edges)
        return affected_ids

    async def commit_topology_with_outbox(
        self,
        entities: List[Any],
        outbox_events: Optional[List[Any]] = None,
    ) -> None:
        if not entities and not outbox_events:
            return

        if entities:
            entities = _sort_entities_for_write(entities)
            entities = compute_hashes(entities)

        nodes, edges = _partition_entities(entities) if entities else ([], [])

        instance_unwind = self._unwind_queries
        outbox_store = self._outbox_store
        resolved_events = outbox_events or []

        async def _topology_write() -> None:
            async def _topology_commit(
                tx: AsyncManagedTransaction,
            ) -> None:
                node_groups = _group_by_type(nodes)
                for entity_type, records in node_groups.items():
                    unwind_query = instance_unwind.get(entity_type)
                    if unwind_query is None:
                        raise TypeError(
                            f"No UNWIND query for {entity_type.__name__}"
                        )
                    for chunk in _chunk_list(records, self._batch_size):
                        await tx.run(unwind_query, batch=chunk)

                edge_groups = _group_by_type(edges)
                for entity_type, records in edge_groups.items():
                    unwind_query = instance_unwind.get(entity_type)
                    if unwind_query is None:
                        raise TypeError(
                            f"No UNWIND query for {entity_type.__name__}"
                        )
                    for chunk in _chunk_list(records, self._batch_size):
                        await tx.run(unwind_query, batch=chunk)

                if outbox_store and resolved_events:
                    for event in resolved_events:
                        await outbox_store.write_in_tx(tx, event=event)

            async with self._write_session() as session:
                await session.execute_write(_topology_commit)

        await self._cb.call(_topology_write)

    async def refresh_degree_for_ids(
        self, affected_ids: List[str],
    ) -> None:
        await self._refresh_degree_property(affected_ids)

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

        regular_edges, hot_edges = split_hot_targets(
            edges,
            threshold=self._hot_target_config.hot_target_threshold,
        )

        regular_edge_groups = _group_by_type(regular_edges)
        if regular_edge_groups:
            await asyncio.gather(*(
                _guarded_write(et, recs)
                for et, recs in regular_edge_groups.items()
            ))

        if hot_edges:
            await self._write_hot_edges_serialized(hot_edges)

    async def _write_hot_edges_serialized(
        self, hot_edges: List[Any],
    ) -> None:
        hot_groups = _group_by_type(hot_edges)

        async def _batched_hot_commit(
            tx: AsyncManagedTransaction,
        ) -> None:
            for entity_type, records in hot_groups.items():
                unwind_query = self._unwind_queries.get(entity_type)
                if unwind_query is None:
                    raise TypeError(
                        f"No UNWIND query registered for "
                        f"{entity_type.__name__}"
                    )
                for chunk in _chunk_list(records, self._batch_size):
                    await tx.run(unwind_query, batch=chunk)

        async with self._write_session() as session:
            await session.execute_write(_batched_hot_commit)

    async def _refresh_degree_property(
        self, affected_ids: List[str],
    ) -> None:
        if not affected_ids:
            return
        degree_cypher = (
            "MATCH (n) WHERE (n:Service OR n:Database "
            "OR n:KafkaTopic OR n:K8sDeployment) "
            "AND (n.id IN $ids OR n.name IN $ids) "
            "SET n.degree = size((n)--())"
        )

        async def _tx(tx: AsyncManagedTransaction) -> None:
            await tx.run(degree_cypher, ids=affected_ids)

        async with self._write_session() as session:
            await session.execute_write(_tx)

    async def _write_batches(
        self,
        entity_type: type,
        records: List[Dict[str, Any]],
    ) -> None:
        unwind_query = self._unwind_queries.get(entity_type)
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
        tenant_id: str = "",
    ) -> tuple[int, list[str]]:
        return await self.tombstone_stale_edges(
            current_ingestion_id, tenant_id=tenant_id,
        )

    _TOMBSTONE_NODE_LABELS = ("Service", "Database", "KafkaTopic", "K8sDeployment")

    async def tombstone_stale_edges(
        self,
        current_ingestion_id: str,
        tenant_id: str = "",
    ) -> tuple[int, list[str]]:
        timestamp = datetime.now(timezone.utc).isoformat()
        total = 0
        affected_node_ids: list[str] = []
        async with self._write_session() as session:
            for label in self._TOMBSTONE_NODE_LABELS:
                tenant_clause = (
                    "AND n.tenant_id = $tenant_id "
                    if tenant_id else ""
                )
                query = (
                    f"MATCH (n:{label})-[r]->() "
                    "WHERE r.ingestion_id IS NOT NULL "
                    "AND r.ingestion_id <> $current_id "
                    "AND r.tombstoned_at IS NULL "
                    f"{tenant_clause}"
                    "SET r.tombstoned_at = $timestamp "
                    "RETURN count(r) AS tombstoned, "
                    "collect(DISTINCT n.id) AS node_ids"
                )
                result = await session.execute_write(
                    self._run_tombstone_with_ids, query=query,
                    current_id=current_ingestion_id, timestamp=timestamp,
                    tenant_id=tenant_id,
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
        tenant_id: str = "",
    ) -> tuple[int, list[str]]:
        params: Dict[str, Any] = {
            "current_id": current_id,
            "timestamp": timestamp,
        }
        if tenant_id:
            params["tenant_id"] = tenant_id
        result = await tx.run(query, **params)
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

    async def ensure_tombstone_index(self) -> None:
        cypher = (
            "CREATE RANGE INDEX tombstone_calls_idx IF NOT EXISTS "
            "FOR ()-[r:CALLS]-() ON (r.tombstoned_at)"
        )
        async with self._session() as session:
            await session.run(cypher)

        for rel_type in ("PRODUCES", "CONSUMES", "DEPLOYED_IN"):
            idx_cypher = (
                f"CREATE RANGE INDEX tombstone_{rel_type.lower()}_idx IF NOT EXISTS "
                f"FOR ()-[r:{rel_type}]-() ON (r.tombstoned_at)"
            )
            async with self._session() as session:
                await session.run(idx_cypher)

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
