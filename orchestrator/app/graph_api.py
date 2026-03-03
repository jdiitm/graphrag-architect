from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Header, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from orchestrator.app.access_control import (
    InvalidTokenError,
    SecurityPrincipal,
)
from orchestrator.app.audit_log import AuditAction, AuditEvent, SecurityAuditLogger
from orchestrator.app.config import AuthConfig
from orchestrator.app.cypher_validator import CypherValidationError, validate_cypher_readonly
from orchestrator.app.neo4j_pool import get_driver

logger = logging.getLogger(__name__)
_audit = SecurityAuditLogger()

router = APIRouter(prefix="/v1/graph", tags=["graph"])

_KNOWN_NODE_LABELS = ("Service", "Database", "KafkaTopic", "K8sDeployment")
_KNOWN_EDGE_TYPES = ("CALLS", "PRODUCES", "CONSUMES", "DEPLOYED_IN")
_MAX_HOPS = 5


class EntitySummary(BaseModel):
    id: str = ""
    name: str = ""
    labels: List[str] = Field(default_factory=list)
    properties: Dict[str, Any] = Field(default_factory=dict)


class EntityListResponse(BaseModel):
    entities: List[EntitySummary] = Field(default_factory=list)
    total: int = 0
    limit: int = 50
    offset: int = 0


class RelationshipDetail(BaseModel):
    type: str = ""
    target_id: str = ""
    target_name: str = ""
    properties: Dict[str, Any] = Field(default_factory=dict)


class EntityDetailResponse(BaseModel):
    id: str = ""
    name: str = ""
    labels: List[str] = Field(default_factory=list)
    properties: Dict[str, Any] = Field(default_factory=dict)
    relationships: List[RelationshipDetail] = Field(default_factory=list)


class NeighborEntry(BaseModel):
    id: str = ""
    name: str = ""
    labels: List[str] = Field(default_factory=list)
    rel_type: str = ""
    hops: int = 1


class NeighborResponse(BaseModel):
    entity_id: str = ""
    neighbors: List[NeighborEntry] = Field(default_factory=list)
    hops: int = 1


class SchemaResponse(BaseModel):
    node_types: List[str] = Field(default_factory=list)
    edge_types: List[str] = Field(default_factory=list)
    constraints: List[str] = Field(default_factory=list)


class TypeCount(BaseModel):
    label: str = ""
    count: int = 0


class StatsResponse(BaseModel):
    node_counts: List[TypeCount] = Field(default_factory=list)
    edge_counts: List[TypeCount] = Field(default_factory=list)
    total_nodes: int = 0
    total_edges: int = 0


class CypherRequest(BaseModel):
    query: str


class CypherResponse(BaseModel):
    results: List[Dict[str, Any]] = Field(default_factory=list)
    columns: List[str] = Field(default_factory=list)


def _resolve_principal(
    authorization: Optional[str],
) -> SecurityPrincipal:
    auth = AuthConfig.from_env()
    if not authorization or not authorization.strip():
        if auth.require_tokens:
            raise HTTPException(status_code=401, detail="authorization header required")
        return SecurityPrincipal(team="*", namespace="*", role="anonymous")
    if not auth.token_secret:
        if auth.require_tokens:
            raise HTTPException(
                status_code=503,
                detail="server misconfigured: token verification required but no secret set",
            )
        return SecurityPrincipal(team="*", namespace="*", role="anonymous")
    try:
        return SecurityPrincipal.from_header(authorization, token_secret=auth.token_secret)
    except InvalidTokenError as exc:
        raise HTTPException(status_code=401, detail=str(exc)) from exc


def _require_admin(principal: SecurityPrincipal) -> None:
    if not principal.is_admin:
        raise HTTPException(status_code=403, detail="admin role required")


def _tenant_clause(alias: str = "n") -> str:
    return f"{alias}.tenant_id = $tenant_id"


@router.get(
    "/entities",
    response_model=EntityListResponse,
    summary="List graph entities",
    description="Paginated list of graph entities, optionally filtered by node type.",
)
async def list_entities(
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    node_type: Optional[str] = Query(default=None, alias="type"),
    authorization: Optional[str] = Header(default=None),
) -> JSONResponse:
    principal = _resolve_principal(authorization)
    tenant_id = principal.team

    label_filter = ""
    if node_type and node_type in _KNOWN_NODE_LABELS:
        label_filter = f":{node_type}"

    cypher = (
        f"MATCH (n{label_filter}) "
        f"WHERE {_tenant_clause()} "
        f"RETURN n, labels(n) AS labels "
        f"ORDER BY n.name "
        f"SKIP $offset LIMIT $limit"
    )

    driver = get_driver()
    async with driver.session() as session:
        result = await session.execute_read(
            _run_read_query, cypher,
            {"tenant_id": tenant_id, "offset": offset, "limit": limit},
        )

    entities = []
    for record in result:
        node = record.get("n", {})
        node_data = dict(node) if hasattr(node, "items") else node
        entities.append(EntitySummary(
            id=node_data.get("id", ""),
            name=node_data.get("name", ""),
            labels=record.get("labels", []),
            properties={k: v for k, v in node_data.items() if k not in ("id", "name")},
        ))

    body = EntityListResponse(
        entities=entities,
        total=len(entities),
        limit=limit,
        offset=offset,
    )
    return JSONResponse(content=body.model_dump(), status_code=200)


@router.get(
    "/entities/{entity_id}",
    response_model=EntityDetailResponse,
    summary="Get entity by ID",
    description="Returns entity details including its relationships.",
)
async def get_entity(
    entity_id: str,
    authorization: Optional[str] = Header(default=None),
) -> JSONResponse:
    principal = _resolve_principal(authorization)
    tenant_id = principal.team

    cypher = (
        "MATCH (n) WHERE n.id = $entity_id AND n.tenant_id = $tenant_id "
        "OPTIONAL MATCH (n)-[r]->(m) "
        "WHERE m.tenant_id = $tenant_id "
        "RETURN n, labels(n) AS labels, "
        "collect({type: type(r), target_id: m.id, target_name: m.name}) AS rels"
    )

    driver = get_driver()
    async with driver.session() as session:
        result = await session.execute_read(
            _run_read_query, cypher,
            {"entity_id": entity_id, "tenant_id": tenant_id},
        )

    if not result:
        raise HTTPException(status_code=404, detail="Entity not found")

    record = result[0]
    node = record.get("n", {})
    node_data = dict(node) if hasattr(node, "items") else node
    rels_raw = record.get("rels", [])

    relationships = []
    for rel in rels_raw:
        if rel.get("type"):
            relationships.append(RelationshipDetail(
                type=rel.get("type", ""),
                target_id=rel.get("target_id", ""),
                target_name=rel.get("target_name", ""),
            ))

    body = EntityDetailResponse(
        id=node_data.get("id", entity_id),
        name=node_data.get("name", ""),
        labels=record.get("labels", []),
        properties={k: v for k, v in node_data.items() if k not in ("id", "name")},
        relationships=relationships,
    )
    return JSONResponse(content=body.model_dump(), status_code=200)


@router.get(
    "/entities/{entity_id}/neighbors",
    response_model=NeighborResponse,
    summary="Get N-hop neighbors",
    description="Returns neighbors within N hops (default 1, max 5).",
)
async def get_neighbors(
    entity_id: str,
    hops: int = Query(default=1, ge=1, le=_MAX_HOPS),
    authorization: Optional[str] = Header(default=None),
) -> JSONResponse:
    principal = _resolve_principal(authorization)
    tenant_id = principal.team

    cypher = (
        f"MATCH (n {{id: $entity_id, tenant_id: $tenant_id}})"
        f"-[r*1..{min(hops, _MAX_HOPS)}]-(m) "
        f"WHERE m.tenant_id = $tenant_id "
        f"UNWIND r AS rel "
        f"RETURN DISTINCT m.id AS id, m.name AS name, labels(m) AS labels, "
        f"type(rel) AS rel_type"
    )

    driver = get_driver()
    async with driver.session() as session:
        result = await session.execute_read(
            _run_read_query, cypher,
            {"entity_id": entity_id, "tenant_id": tenant_id},
        )

    neighbors = [
        NeighborEntry(
            id=r.get("id", ""),
            name=r.get("name", ""),
            labels=r.get("labels", []),
            rel_type=r.get("rel_type", ""),
            hops=hops,
        )
        for r in result
    ]

    body = NeighborResponse(
        entity_id=entity_id,
        neighbors=neighbors,
        hops=hops,
    )
    return JSONResponse(content=body.model_dump(), status_code=200)


@router.get(
    "/schema",
    response_model=SchemaResponse,
    summary="Get graph schema",
    description="Returns current graph schema: node types, edge types, constraints.",
)
async def get_schema(
    authorization: Optional[str] = Header(default=None),
) -> JSONResponse:
    _resolve_principal(authorization)

    node_cypher = (
        "CALL db.labels() YIELD label RETURN collect(label) AS labels"
    )
    edge_cypher = (
        "CALL db.relationshipTypes() YIELD relationshipType "
        "RETURN collect(relationshipType) AS types"
    )
    constraint_cypher = (
        "SHOW CONSTRAINTS YIELD name RETURN collect(name) AS constraints"
    )

    driver = get_driver()
    async with driver.session() as session:
        node_result = await session.execute_read(
            _run_read_query, node_cypher, {},
        )
        edge_result = await session.execute_read(
            _run_read_query, edge_cypher, {},
        )
        try:
            constraint_result = await session.execute_read(
                _run_read_query, constraint_cypher, {},
            )
        except Exception:
            constraint_result = []

    node_types = node_result[0].get("labels", []) if node_result else []
    edge_types = edge_result[0].get("types", []) if edge_result else []
    constraints = constraint_result[0].get("constraints", []) if constraint_result else []

    body = SchemaResponse(
        node_types=node_types,
        edge_types=edge_types,
        constraints=constraints,
    )
    return JSONResponse(content=body.model_dump(), status_code=200)


@router.get(
    "/stats",
    response_model=StatsResponse,
    summary="Get graph statistics",
    description="Returns node and edge counts per type.",
)
async def get_stats(
    authorization: Optional[str] = Header(default=None),
) -> JSONResponse:
    principal = _resolve_principal(authorization)
    tenant_id = principal.team

    node_count_cypher = (
        "UNWIND $labels AS label "
        "CALL { WITH label "
        "  MATCH (n) WHERE label IN labels(n) AND n.tenant_id = $tenant_id "
        "  RETURN count(n) AS cnt "
        "} RETURN label, cnt"
    )
    edge_count_cypher = (
        "UNWIND $rel_types AS rel_type "
        "CALL { WITH rel_type "
        "  MATCH (src)-[r]->() WHERE type(r) = rel_type "
        "  AND src.tenant_id = $tenant_id "
        "  RETURN count(r) AS cnt "
        "} RETURN rel_type, cnt"
    )

    driver = get_driver()
    async with driver.session() as session:
        node_result = await session.execute_read(
            _run_read_query, node_count_cypher,
            {"labels": list(_KNOWN_NODE_LABELS), "tenant_id": tenant_id},
        )
        edge_result = await session.execute_read(
            _run_read_query, edge_count_cypher,
            {"rel_types": list(_KNOWN_EDGE_TYPES), "tenant_id": tenant_id},
        )

    node_counts = [
        TypeCount(label=r.get("label", ""), count=r.get("cnt", 0))
        for r in node_result
    ]
    edge_counts = [
        TypeCount(label=r.get("rel_type", ""), count=r.get("cnt", 0))
        for r in edge_result
    ]

    total_nodes = sum(tc.count for tc in node_counts)
    total_edges = sum(tc.count for tc in edge_counts)

    body = StatsResponse(
        node_counts=node_counts,
        edge_counts=edge_counts,
        total_nodes=total_nodes,
        total_edges=total_edges,
    )
    return JSONResponse(content=body.model_dump(), status_code=200)


@router.post(
    "/cypher",
    response_model=CypherResponse,
    summary="Execute raw Cypher (admin, read-only)",
    description="Admin-only endpoint for executing read-only Cypher queries. Audit-logged.",
)
async def execute_cypher(
    request: CypherRequest,
    authorization: Optional[str] = Header(default=None),
) -> JSONResponse:
    principal = _resolve_principal(authorization)
    _require_admin(principal)

    try:
        validate_cypher_readonly(request.query)
    except CypherValidationError as exc:
        raise HTTPException(
            status_code=400, detail=f"Write queries are not allowed: {exc}",
        ) from exc

    tenant_id = principal.team

    _audit.log(AuditEvent(
        action=AuditAction.QUERY_EXECUTE,
        tenant_id=tenant_id,
        principal=f"{principal.team}/{principal.role}",
        resource="cypher:direct",
        detail=request.query[:200],
    ))

    driver = get_driver()
    async with driver.session() as session:
        result = await session.execute_read(
            _run_read_query, request.query,
            {"tenant_id": tenant_id},
        )

    columns = list(result[0].keys()) if result else []

    body = CypherResponse(results=result, columns=columns)
    return JSONResponse(content=body.model_dump(), status_code=200)


async def _run_read_query(
    tx: Any,
    query: str,
    params: Dict[str, Any],
) -> List[Dict[str, Any]]:
    result = await tx.run(query, **params)
    return [dict(record) async for record in result]
