from typing import List, Optional

from pydantic import BaseModel, Field


class ServiceNode(BaseModel):
    id: str
    name: str
    language: str
    framework: str
    opentelemetry_enabled: bool
    team_owner: Optional[str] = None
    namespace_acl: List[str] = Field(default_factory=list)
    confidence: float = 1.0

class DatabaseNode(BaseModel):
    id: str
    type: str
    team_owner: Optional[str] = None
    namespace_acl: List[str] = Field(default_factory=list)

class KafkaTopicNode(BaseModel):
    name: str
    partitions: int
    retention_ms: int
    team_owner: Optional[str] = None
    namespace_acl: List[str] = Field(default_factory=list)

class K8sDeploymentNode(BaseModel):
    id: str
    namespace: str
    replicas: int
    team_owner: Optional[str] = None
    namespace_acl: List[str] = Field(default_factory=list)

class CallsEdge(BaseModel):
    source_service_id: str
    target_service_id: str
    protocol: str
    confidence: float = 1.0

class ProducesEdge(BaseModel):
    service_id: str
    topic_name: str
    event_schema: str

class ConsumesEdge(BaseModel):
    service_id: str
    topic_name: str
    consumer_group: str

class DeployedInEdge(BaseModel):
    service_id: str
    deployment_id: str

class ServiceExtractionResult(BaseModel):
    services: List[ServiceNode]
    calls: List[CallsEdge]
