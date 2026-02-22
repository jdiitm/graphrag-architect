from enum import Enum
from typing import Any, Dict, List

from pydantic import BaseModel, Field
from typing_extensions import TypedDict


class QueryComplexity(str, Enum):
    ENTITY_LOOKUP = "entity_lookup"
    SINGLE_HOP = "single_hop"
    MULTI_HOP = "multi_hop"
    AGGREGATE = "aggregate"


class QueryRequest(BaseModel):
    query: str = Field(..., min_length=1)
    max_results: int = Field(default=10, ge=1, le=100)


class QueryResponse(BaseModel):
    answer: str
    sources: List[Dict[str, Any]]
    complexity: QueryComplexity
    retrieval_path: str


class QueryState(TypedDict):
    query: str
    max_results: int
    complexity: QueryComplexity
    retrieval_path: str
    candidates: List[Dict[str, Any]]
    cypher_query: str
    cypher_results: List[Dict[str, Any]]
    iteration_count: int
    answer: str
    sources: List[Dict[str, Any]]
