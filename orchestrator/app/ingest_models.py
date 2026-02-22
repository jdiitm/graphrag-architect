from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, Field


class SourceType(str, Enum):
    SOURCE_CODE = "source_code"
    K8S_MANIFEST = "k8s_manifest"
    KAFKA_SCHEMA = "kafka_schema"


class IngestDocument(BaseModel):
    file_path: str
    content: str
    source_type: SourceType
    repository: Optional[str] = None
    commit_sha: Optional[str] = None


class IngestRequest(BaseModel):
    documents: List[IngestDocument] = Field(..., min_length=1)


class IngestResponse(BaseModel):
    status: str
    entities_extracted: int
    errors: List[str]
