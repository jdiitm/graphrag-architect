from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple, Union

from neo4j import AsyncDriver, AsyncManagedTransaction

from orchestrator.app.circuit_breaker import CircuitBreaker, CircuitBreakerConfig
from orchestrator.app.extraction_models import (
    CallsEdge,
    ConsumesEdge,
    DatabaseNode,
    DeployedInEdge,
    K8sDeploymentNode,
    KafkaTopicNode,
    ProducesEdge,
    ServiceNode,
)

NodeEntity = Union[ServiceNode, DatabaseNode, KafkaTopicNode, K8sDeploymentNode]
EdgeEntity = Union[CallsEdge, ProducesEdge, ConsumesEdge, DeployedInEdge]
CypherOp = Tuple[str, Dict[str, Any]]

_NODE_TYPES = (ServiceNode, DatabaseNode, KafkaTopicNode, K8sDeploymentNode)
_EDGE_TYPES = (CallsEdge, ProducesEdge, ConsumesEdge, DeployedInEdge)


def _service_cypher(entity: ServiceNode) -> CypherOp:
    query = (
        "MERGE (n:Service {id: $id}) "
        "SET n.name = $name, n.language = $language, "
        "n.framework = $framework, n.opentelemetry_enabled = $opentelemetry_enabled, "
        "n.team_owner = $team_owner, n.namespace_acl = $namespace_acl"
    )
    return query, entity.model_dump()


def _database_cypher(entity: DatabaseNode) -> CypherOp:
    query = (
        "MERGE (n:Database {id: $id}) "
        "SET n.type = $type, "
        "n.team_owner = $team_owner, n.namespace_acl = $namespace_acl"
    )
    return query, entity.model_dump()


def _kafka_topic_cypher(entity: KafkaTopicNode) -> CypherOp:
    query = (
        "MERGE (n:KafkaTopic {name: $name}) "
        "SET n.partitions = $partitions, n.retention_ms = $retention_ms, "
        "n.team_owner = $team_owner, n.namespace_acl = $namespace_acl"
    )
    return query, entity.model_dump()


def _k8s_deployment_cypher(entity: K8sDeploymentNode) -> CypherOp:
    query = (
        "MERGE (n:K8sDeployment {id: $id}) "
        "SET n.namespace = $namespace, n.replicas = $replicas, "
        "n.team_owner = $team_owner, n.namespace_acl = $namespace_acl"
    )
    return query, entity.model_dump()


def _calls_cypher(entity: CallsEdge) -> CypherOp:
    query = (
        "MATCH (a:Service {id: $source_service_id}), "
        "(b:Service {id: $target_service_id}) "
        "MERGE (a)-[r:CALLS]->(b) SET r.protocol = $protocol"
    )
    return query, entity.model_dump()


def _produces_cypher(entity: ProducesEdge) -> CypherOp:
    query = (
        "MATCH (s:Service {id: $service_id}), "
        "(t:KafkaTopic {name: $topic_name}) "
        "MERGE (s)-[r:PRODUCES]->(t) SET r.event_schema = $event_schema"
    )
    return query, entity.model_dump()


def _consumes_cypher(entity: ConsumesEdge) -> CypherOp:
    query = (
        "MATCH (s:Service {id: $service_id}), "
        "(t:KafkaTopic {name: $topic_name}) "
        "MERGE (s)-[r:CONSUMES]->(t) SET r.consumer_group = $consumer_group"
    )
    return query, entity.model_dump()


def _deployed_in_cypher(entity: DeployedInEdge) -> CypherOp:
    query = (
        "MATCH (s:Service {id: $service_id}), "
        "(k:K8sDeployment {id: $deployment_id}) "
        "MERGE (s)-[r:DEPLOYED_IN]->(k)"
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


class GraphRepository:
    def __init__(
        self,
        driver: AsyncDriver,
        circuit_breaker: Optional[CircuitBreaker] = None,
    ) -> None:
        self._driver = driver
        self._cb = circuit_breaker or CircuitBreaker(CircuitBreakerConfig())

    async def commit_topology(self, entities: List[Any]) -> None:
        if not entities:
            return

        nodes, edges = _partition_entities(entities)
        await self._cb.call(self._execute_commit, nodes, edges)

    async def _execute_commit(
        self, nodes: List[Any], edges: List[Any]
    ) -> None:
        async with self._driver.session() as session:
            await session.execute_write(
                self._merge_all, nodes=nodes, edges=edges
            )

    @staticmethod
    async def _merge_all(
        tx: AsyncManagedTransaction,
        nodes: List[Any],
        edges: List[Any],
    ) -> None:
        for entity in nodes:
            query, params = cypher_op_for_entity(entity)
            await tx.run(query, **params)

        for entity in edges:
            query, params = cypher_op_for_entity(entity)
            await tx.run(query, **params)
