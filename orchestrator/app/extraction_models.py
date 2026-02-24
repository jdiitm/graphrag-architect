import hashlib
import json
from typing import List, Optional

from pydantic import BaseModel, Field


def compute_content_hash(entity: BaseModel) -> str:
    data = entity.model_dump(exclude={"content_hash"})
    json_str = json.dumps(data, sort_keys=True)
    return hashlib.sha256(json_str.encode()).hexdigest()


class ServiceNode(BaseModel):
    id: str
    name: str
    language: str
    framework: str
    opentelemetry_enabled: bool
    team_owner: Optional[str] = None
    namespace_acl: List[str] = Field(default_factory=list)
    confidence: float = Field(default=1.0, ge=0.0, le=1.0)
    content_hash: str = ""


class DatabaseNode(BaseModel):
    id: str
    type: str
    team_owner: Optional[str] = None
    namespace_acl: List[str] = Field(default_factory=list)
    content_hash: str = ""


class KafkaTopicNode(BaseModel):
    name: str
    partitions: int
    retention_ms: int
    team_owner: Optional[str] = None
    namespace_acl: List[str] = Field(default_factory=list)
    content_hash: str = ""


class K8sDeploymentNode(BaseModel):
    id: str
    namespace: str
    replicas: int
    team_owner: Optional[str] = None
    namespace_acl: List[str] = Field(default_factory=list)
    content_hash: str = ""

class CallsEdge(BaseModel):
    source_service_id: str
    target_service_id: str
    protocol: str
    confidence: float = Field(default=1.0, ge=0.0, le=1.0)
    ingestion_id: str = ""
    last_seen_at: str = ""

class ProducesEdge(BaseModel):
    service_id: str
    topic_name: str
    event_schema: str
    ingestion_id: str = ""
    last_seen_at: str = ""

class ConsumesEdge(BaseModel):
    service_id: str
    topic_name: str
    consumer_group: str
    ingestion_id: str = ""
    last_seen_at: str = ""

class DeployedInEdge(BaseModel):
    service_id: str
    deployment_id: str
    ingestion_id: str = ""
    last_seen_at: str = ""

class ServiceExtractionResult(BaseModel):
    services: List[ServiceNode]
    calls: List[CallsEdge]
