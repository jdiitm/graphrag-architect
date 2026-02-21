from typing import List

from pydantic import BaseModel

class ServiceNode(BaseModel):
    id: str
    name: str
    language: str
    framework: str
    opentelemetry_enabled: bool

class DatabaseNode(BaseModel):
    id: str
    type: str

class KafkaTopicNode(BaseModel):
    name: str
    partitions: int
    retention_ms: int

class K8sDeploymentNode(BaseModel):
    id: str
    namespace: str
    replicas: int

class CallsEdge(BaseModel):
    source_service_id: str
    target_service_id: str
    protocol: str

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


class SystemTopology(BaseModel):
    services: List[ServiceNode]
    databases: List[DatabaseNode]
    topics: List[KafkaTopicNode]
    deployments: List[K8sDeploymentNode]
    calls: List[CallsEdge]
    produces: List[ProducesEdge]
    consumes: List[ConsumesEdge]
    deployed_in: List[DeployedInEdge]
