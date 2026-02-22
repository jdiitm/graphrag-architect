from __future__ import annotations

from typing import Any, List, Set

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

_NODE_TYPES = (ServiceNode, DatabaseNode, KafkaTopicNode, K8sDeploymentNode)
_EDGE_TYPES = (CallsEdge, ProducesEdge, ConsumesEdge, DeployedInEdge)
_ALL_TYPES = _NODE_TYPES + _EDGE_TYPES


def _build_ref_indexes(
    entities: List[Any],
) -> tuple[Set[str], Set[str], Set[str]]:
    service_ids: Set[str] = set()
    topic_names: Set[str] = set()
    deployment_ids: Set[str] = set()

    for entity in entities:
        if isinstance(entity, ServiceNode):
            service_ids.add(entity.id)
        elif isinstance(entity, KafkaTopicNode):
            topic_names.add(entity.name)
        elif isinstance(entity, K8sDeploymentNode):
            deployment_ids.add(entity.id)

    return service_ids, topic_names, deployment_ids


def _validate_calls(
    edge: CallsEdge, service_ids: Set[str],
) -> List[str]:
    errors: List[str] = []
    if edge.source_service_id not in service_ids:
        errors.append(
            f"CallsEdge references unknown source service: {edge.source_service_id}"
        )
    if edge.target_service_id not in service_ids:
        errors.append(
            f"CallsEdge references unknown target service: {edge.target_service_id}"
        )
    return errors


def _validate_produces(
    edge: ProducesEdge,
    service_ids: Set[str],
    topic_names: Set[str],
) -> List[str]:
    errors: List[str] = []
    if edge.service_id not in service_ids:
        errors.append(
            f"ProducesEdge references unknown service: {edge.service_id}"
        )
    if edge.topic_name not in topic_names:
        errors.append(
            f"ProducesEdge references unknown topic: {edge.topic_name}"
        )
    return errors


def _validate_consumes(
    edge: ConsumesEdge,
    service_ids: Set[str],
    topic_names: Set[str],
) -> List[str]:
    errors: List[str] = []
    if edge.service_id not in service_ids:
        errors.append(
            f"ConsumesEdge references unknown service: {edge.service_id}"
        )
    if edge.topic_name not in topic_names:
        errors.append(
            f"ConsumesEdge references unknown topic: {edge.topic_name}"
        )
    return errors


def _validate_deployed_in(
    edge: DeployedInEdge,
    service_ids: Set[str],
    deployment_ids: Set[str],
) -> List[str]:
    errors: List[str] = []
    if edge.service_id not in service_ids:
        errors.append(
            f"DeployedInEdge references unknown service: {edge.service_id}"
        )
    if edge.deployment_id not in deployment_ids:
        errors.append(
            f"DeployedInEdge references unknown deployment: {edge.deployment_id}"
        )
    return errors


def validate_topology(entities: List[Any]) -> List[str]:
    errors: List[str] = []

    for entity in entities:
        if not isinstance(entity, _ALL_TYPES):
            errors.append(
                f"Unknown entity type: {type(entity).__name__}"
            )

    service_ids, topic_names, deployment_ids = _build_ref_indexes(entities)

    for entity in entities:
        if isinstance(entity, CallsEdge):
            errors.extend(_validate_calls(entity, service_ids))
        elif isinstance(entity, ProducesEdge):
            errors.extend(
                _validate_produces(entity, service_ids, topic_names)
            )
        elif isinstance(entity, ConsumesEdge):
            errors.extend(
                _validate_consumes(entity, service_ids, topic_names)
            )
        elif isinstance(entity, DeployedInEdge):
            errors.extend(
                _validate_deployed_in(entity, service_ids, deployment_ids)
            )

    return errors
