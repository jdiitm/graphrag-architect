import logging
import os
from typing import Any, Dict, List, Optional, Union

import yaml

from orchestrator.app.extraction_models import K8sDeploymentNode, KafkaTopicNode

logger = logging.getLogger(__name__)


YAML_EXTENSIONS: frozenset[str] = frozenset({".yaml", ".yml"})

DEFAULT_NAMESPACE = "default"
DEFAULT_REPLICAS = 1
DEFAULT_PARTITIONS = 1
DEFAULT_RETENTION_MS = 604800000

TEAM_OWNER_LABEL = "graphrag.io/team-owner"
NAMESPACE_ACL_ANNOTATION = "graphrag.io/namespace-acl"

_TEAM_OWNER_FALLBACK_LABELS = (
    TEAM_OWNER_LABEL,
    "team",
    "owner",
    "app.kubernetes.io/managed-by",
)


def _safe_load_all(content: str) -> List[Dict[str, Any]]:
    if not content or not content.strip():
        return []
    try:
        documents = list(yaml.safe_load_all(content))
    except yaml.YAMLError as exc:
        logger.warning("Failed to parse YAML: %s", exc)
        return []
    return [doc for doc in documents if isinstance(doc, dict)]


def _extract_team_owner(metadata: Dict[str, Any]) -> Optional[str]:
    labels: Dict[str, Any] = metadata.get("labels", {})
    if not isinstance(labels, dict):
        return None
    for label_key in _TEAM_OWNER_FALLBACK_LABELS:
        owner = labels.get(label_key)
        if owner is not None:
            return str(owner)
    return None


def _extract_namespace_acl(
    metadata: Dict[str, Any], manifest_namespace: str = "",
) -> List[str]:
    annotations: Dict[str, Any] = metadata.get("annotations", {})
    acl_namespaces: List[str] = []
    if isinstance(annotations, dict):
        raw = annotations.get(NAMESPACE_ACL_ANNOTATION)
        if raw:
            acl_namespaces = [ns.strip() for ns in str(raw).split(",") if ns.strip()]
    if not acl_namespaces and manifest_namespace:
        acl_namespaces = [manifest_namespace]
    return acl_namespaces


def _extract_deployment(
    doc: Dict[str, Any], tenant_id: str = "default",
) -> Optional[K8sDeploymentNode]:
    if doc.get("kind") != "Deployment":
        return None
    metadata: Dict[str, Any] = doc.get("metadata", {})
    if not isinstance(metadata, dict):
        return None
    name = metadata.get("name")
    if not name:
        return None
    namespace = str(metadata.get("namespace", DEFAULT_NAMESPACE))
    spec: Dict[str, Any] = doc.get("spec", {})
    if not isinstance(spec, dict):
        spec = {}
    replicas = spec.get("replicas", DEFAULT_REPLICAS)
    team_owner = _extract_team_owner(metadata)
    namespace_acl = _extract_namespace_acl(metadata, manifest_namespace=namespace)
    if team_owner is None:
        logger.warning(
            "Deployment %s/%s missing label %s — will be invisible "
            "to non-admin users under default-deny ACL",
            namespace, name, TEAM_OWNER_LABEL,
        )
    if not namespace_acl:
        logger.warning(
            "Deployment %s/%s missing annotation %s — will be invisible "
            "to namespace-scoped users under default-deny ACL",
            namespace, name, NAMESPACE_ACL_ANNOTATION,
        )
    return K8sDeploymentNode(
        id=str(name),
        namespace=str(namespace),
        replicas=int(replicas),
        tenant_id=tenant_id,
        team_owner=team_owner,
        namespace_acl=namespace_acl,
    )


def _extract_kafka_topic(
    doc: Dict[str, Any], tenant_id: str = "default",
) -> Optional[KafkaTopicNode]:
    if doc.get("kind") != "KafkaTopic":
        return None
    metadata: Dict[str, Any] = doc.get("metadata", {})
    if not isinstance(metadata, dict):
        return None
    name = metadata.get("name")
    if not name:
        return None
    spec: Dict[str, Any] = doc.get("spec", {})
    if not isinstance(spec, dict):
        spec = {}
    partitions = spec.get("partitions", DEFAULT_PARTITIONS)
    config: Dict[str, Any] = spec.get("config", {})
    if not isinstance(config, dict):
        config = {}
    retention_raw: Union[str, int] = config.get("retention.ms", DEFAULT_RETENTION_MS)
    topic_namespace = str(metadata.get("namespace", DEFAULT_NAMESPACE))
    team_owner = _extract_team_owner(metadata)
    namespace_acl = _extract_namespace_acl(metadata, manifest_namespace=topic_namespace)
    if team_owner is None:
        logger.warning(
            "KafkaTopic %s missing label %s — will be invisible "
            "to non-admin users under default-deny ACL",
            name, TEAM_OWNER_LABEL,
        )
    if not namespace_acl:
        logger.warning(
            "KafkaTopic %s missing annotation %s — will be invisible "
            "to namespace-scoped users under default-deny ACL",
            name, NAMESPACE_ACL_ANNOTATION,
        )
    return KafkaTopicNode(
        name=str(name),
        partitions=int(partitions),
        retention_ms=int(retention_raw),
        tenant_id=tenant_id,
        team_owner=team_owner,
        namespace_acl=namespace_acl,
    )


def parse_k8s_manifests(content: str, tenant_id: str = "default") -> List[K8sDeploymentNode]:
    results: List[K8sDeploymentNode] = []
    for doc in _safe_load_all(content):
        node = _extract_deployment(doc, tenant_id=tenant_id)
        if node is not None:
            results.append(node)
    return results


def parse_kafka_topics(content: str, tenant_id: str = "default") -> List[KafkaTopicNode]:
    results: List[KafkaTopicNode] = []
    for doc in _safe_load_all(content):
        topic = _extract_kafka_topic(doc, tenant_id=tenant_id)
        if topic is not None:
            results.append(topic)
    return results


def parse_all_manifests(
    files: List[Dict[str, str]], tenant_id: str = "default",
) -> List[Any]:
    entities: List[Any] = []
    for file_entry in files:
        path = file_entry.get("path", "")
        _, ext = os.path.splitext(path)
        if ext not in YAML_EXTENSIONS:
            continue
        content = file_entry.get("content", "")
        entities.extend(parse_k8s_manifests(content, tenant_id=tenant_id))
        entities.extend(parse_kafka_topics(content, tenant_id=tenant_id))
    return entities
