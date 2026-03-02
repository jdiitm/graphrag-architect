import hashlib
import json
import re
from typing import List, Optional

from pydantic import BaseModel, Field, computed_field, field_validator

_SAFE_ENTITY_NAME = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9._-]{0,252}$")
_CYPHER_INJECTION_CHARS = re.compile(r"['\"{};\\`\x00]")
_MAX_EDGE_REF_LENGTH = 512

_LABEL_UNSAFE = re.compile(r"[^a-zA-Z0-9_]")


def _sanitize_label_suffix(value: str) -> str:
    return _LABEL_UNSAFE.sub("_", value)


def validate_entity_identifier(value: str) -> str:
    if not _SAFE_ENTITY_NAME.match(value):
        raise ValueError(
            f"Entity identifier {value!r} contains disallowed characters "
            f"or exceeds 253 chars. Must match: [a-zA-Z0-9][a-zA-Z0-9._-]{{0,252}}"
        )
    return value


def validate_edge_reference(value: str) -> str:
    if not value or len(value) > _MAX_EDGE_REF_LENGTH:
        raise ValueError(
            f"Edge reference {value!r} must be non-empty and "
            f"at most {_MAX_EDGE_REF_LENGTH} characters"
        )
    if _CYPHER_INJECTION_CHARS.search(value):
        raise ValueError(
            f"Edge reference {value!r} contains disallowed characters "
            f"(quotes, braces, semicolons, backslashes, backticks, or null bytes)"
        )
    return value


def compute_content_hash(entity: BaseModel) -> str:
    data = entity.model_dump(exclude={"content_hash", "rbac_labels"})
    json_str = json.dumps(data, sort_keys=True)
    return hashlib.sha256(json_str.encode()).hexdigest()


def _build_rbac_labels(
    team_owner: Optional[str],
    namespace_acl: List[str],
    read_roles: List[str],
) -> List[str]:
    labels: List[str] = []
    if team_owner:
        labels.append(f"Team_{_sanitize_label_suffix(team_owner)}")
    for ns in namespace_acl:
        labels.append(f"Ns_{_sanitize_label_suffix(ns)}")
    for role in read_roles:
        labels.append(f"Role_{_sanitize_label_suffix(role)}")
    return labels


class ServiceNode(BaseModel):
    id: str
    name: str
    language: str
    framework: str
    opentelemetry_enabled: bool
    tenant_id: str = Field(..., min_length=1)
    team_owner: Optional[str] = None
    namespace_acl: List[str] = Field(default_factory=list)
    read_roles: List[str] = Field(default_factory=list)
    confidence: float = Field(default=1.0, ge=0.0, le=1.0)
    content_hash: str = ""

    @computed_field  # type: ignore[prop-decorator]
    @property
    def rbac_labels(self) -> List[str]:
        return _build_rbac_labels(
            self.team_owner, self.namespace_acl, self.read_roles,
        )

    @field_validator("id", "name")
    @classmethod
    def _validate_safe_identifier(cls, v: str) -> str:
        return validate_entity_identifier(v)


class DatabaseNode(BaseModel):
    id: str
    type: str
    tenant_id: str = Field(..., min_length=1)
    team_owner: Optional[str] = None
    namespace_acl: List[str] = Field(default_factory=list)
    read_roles: List[str] = Field(default_factory=list)
    content_hash: str = ""

    @computed_field  # type: ignore[prop-decorator]
    @property
    def rbac_labels(self) -> List[str]:
        return _build_rbac_labels(
            self.team_owner, self.namespace_acl, self.read_roles,
        )

    @field_validator("id")
    @classmethod
    def _validate_safe_identifier(cls, v: str) -> str:
        return validate_entity_identifier(v)


class KafkaTopicNode(BaseModel):
    name: str
    partitions: int
    retention_ms: int
    tenant_id: str = Field(..., min_length=1)
    team_owner: Optional[str] = None
    namespace_acl: List[str] = Field(default_factory=list)
    read_roles: List[str] = Field(default_factory=list)
    content_hash: str = ""

    @computed_field  # type: ignore[prop-decorator]
    @property
    def rbac_labels(self) -> List[str]:
        return _build_rbac_labels(
            self.team_owner, self.namespace_acl, self.read_roles,
        )

    @field_validator("name")
    @classmethod
    def _validate_safe_identifier(cls, v: str) -> str:
        return validate_entity_identifier(v)


class K8sDeploymentNode(BaseModel):
    id: str
    namespace: str
    replicas: int
    tenant_id: str = Field(..., min_length=1)
    team_owner: Optional[str] = None
    namespace_acl: List[str] = Field(default_factory=list)
    read_roles: List[str] = Field(default_factory=list)
    content_hash: str = ""

    @computed_field  # type: ignore[prop-decorator]
    @property
    def rbac_labels(self) -> List[str]:
        return _build_rbac_labels(
            self.team_owner, self.namespace_acl, self.read_roles,
        )

    @field_validator("id")
    @classmethod
    def _validate_safe_identifier(cls, v: str) -> str:
        return validate_entity_identifier(v)

class CallsEdge(BaseModel):
    source_service_id: str
    target_service_id: str
    protocol: str
    tenant_id: str = Field(..., min_length=1)
    confidence: float = Field(default=1.0, ge=0.0, le=1.0)
    ingestion_id: str = ""
    last_seen_at: str = ""

    @field_validator("source_service_id", "target_service_id")
    @classmethod
    def _validate_edge_ref(cls, v: str) -> str:
        return validate_edge_reference(v)


class ProducesEdge(BaseModel):
    service_id: str
    topic_name: str
    event_schema: str
    tenant_id: str = Field(..., min_length=1)
    ingestion_id: str = ""
    last_seen_at: str = ""

    @field_validator("service_id", "topic_name")
    @classmethod
    def _validate_edge_ref(cls, v: str) -> str:
        return validate_edge_reference(v)


class ConsumesEdge(BaseModel):
    service_id: str
    topic_name: str
    consumer_group: str
    tenant_id: str = Field(..., min_length=1)
    ingestion_id: str = ""
    last_seen_at: str = ""

    @field_validator("service_id", "topic_name", "consumer_group")
    @classmethod
    def _validate_edge_ref(cls, v: str) -> str:
        return validate_edge_reference(v)


class DeployedInEdge(BaseModel):
    service_id: str
    deployment_id: str
    tenant_id: str = Field(..., min_length=1)
    ingestion_id: str = ""
    last_seen_at: str = ""

    @field_validator("service_id", "deployment_id")
    @classmethod
    def _validate_edge_ref(cls, v: str) -> str:
        return validate_edge_reference(v)

class ServiceExtractionResult(BaseModel):
    services: List[ServiceNode]
    calls: List[CallsEdge]
