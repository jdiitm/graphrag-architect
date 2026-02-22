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


def _safe_load_all(content: str) -> List[Dict[str, Any]]:
    if not content or not content.strip():
        return []
    try:
        documents = list(yaml.safe_load_all(content))
    except yaml.YAMLError as exc:
        logger.warning("Failed to parse YAML: %s", exc)
        return []
    return [doc for doc in documents if isinstance(doc, dict)]


def _extract_deployment(doc: Dict[str, Any]) -> Optional[K8sDeploymentNode]:
    if doc.get("kind") != "Deployment":
        return None
    metadata: Dict[str, Any] = doc.get("metadata", {})
    if not isinstance(metadata, dict):
        return None
    name = metadata.get("name")
    if not name:
        return None
    namespace = metadata.get("namespace", DEFAULT_NAMESPACE)
    spec: Dict[str, Any] = doc.get("spec", {})
    if not isinstance(spec, dict):
        spec = {}
    replicas = spec.get("replicas", DEFAULT_REPLICAS)
    return K8sDeploymentNode(id=str(name), namespace=str(namespace), replicas=int(replicas))


def _extract_kafka_topic(doc: Dict[str, Any]) -> Optional[KafkaTopicNode]:
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
    return KafkaTopicNode(
        name=str(name),
        partitions=int(partitions),
        retention_ms=int(retention_raw),
    )


def parse_k8s_manifests(content: str) -> List[K8sDeploymentNode]:
    results: List[K8sDeploymentNode] = []
    for doc in _safe_load_all(content):
        node = _extract_deployment(doc)
        if node is not None:
            results.append(node)
    return results


def parse_kafka_topics(content: str) -> List[KafkaTopicNode]:
    results: List[KafkaTopicNode] = []
    for doc in _safe_load_all(content):
        topic = _extract_kafka_topic(doc)
        if topic is not None:
            results.append(topic)
    return results


def parse_all_manifests(files: List[Dict[str, str]]) -> List[Any]:
    entities: List[Any] = []
    for file_entry in files:
        path = file_entry.get("path", "")
        _, ext = os.path.splitext(path)
        if ext not in YAML_EXTENSIONS:
            continue
        content = file_entry.get("content", "")
        entities.extend(parse_k8s_manifests(content))
        entities.extend(parse_kafka_topics(content))
    return entities
