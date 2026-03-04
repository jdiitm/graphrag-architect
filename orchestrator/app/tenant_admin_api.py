from __future__ import annotations

import logging
import uuid
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Dict, List, Optional

from fastapi import APIRouter, Header, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from orchestrator.app.access_control import (
    InvalidTokenError,
    SecurityPrincipal,
)
from orchestrator.app.audit_log import AuditAction, AuditEvent, SecurityAuditLogger
from orchestrator.app.config import AuthConfig
from orchestrator.app.neo4j_pool import get_driver
from orchestrator.app.schema_evolution import (
    InMemoryVersionStore,
    MigrationRegistry,
)

logger = logging.getLogger(__name__)
_audit = SecurityAuditLogger()

tenant_router = APIRouter(prefix="/v1/tenants", tags=["tenants"])
admin_router = APIRouter(prefix="/v1/admin", tags=["admin"])
webhook_router = APIRouter(prefix="/v1", tags=["webhooks"])

_version_store = InMemoryVersionStore()
_migration_registry = MigrationRegistry(_version_store)
_webhook_store: Dict[str, Dict[str, Any]] = {}


class TenantMetadataResponse(BaseModel):
    tenant_id: str
    database_name: str = "neo4j"
    isolation_mode: str = "physical"
    node_count: int = 0


class RepositoryEntry(BaseModel):
    repo_url: str = ""
    ingested_at: str = ""


class RepositoryListResponse(BaseModel):
    tenant_id: str
    repositories: List[RepositoryEntry] = Field(default_factory=list)


class SchemaVersionEntry(BaseModel):
    version: str
    status: str


class SchemaVersionsResponse(BaseModel):
    current_version: Optional[str] = None
    versions: List[SchemaVersionEntry] = Field(default_factory=list)


class MigrateRequest(BaseModel):
    target_version: str = ""


class MigrateResponse(BaseModel):
    status: str
    applied: int = 0
    current_version: Optional[str] = None


class WebhookRequest(BaseModel):
    url: str
    events: List[str] = Field(default_factory=list)
    secret: Optional[str] = None


class WebhookResponse(BaseModel):
    webhook_id: str
    url: str
    events: List[str] = Field(default_factory=list)


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


def _require_tenant_access(
    principal: SecurityPrincipal, tenant_id: str,
) -> None:
    if principal.is_admin:
        return
    if principal.team != tenant_id:
        raise HTTPException(
            status_code=403,
            detail=f"access denied: principal tenant {principal.team!r} "
            f"cannot access tenant {tenant_id!r}",
        )


_DEFAULT_DATABASE = "neo4j"


@asynccontextmanager
async def _tenant_scoped_session(
    driver: Any, *, tenant_id: str, database: str = _DEFAULT_DATABASE,
) -> AsyncIterator[Any]:
    if not tenant_id:
        raise ValueError("tenant_id is required for tenant-scoped session")
    async with driver.session(database=database) as session:
        yield session


@tenant_router.get(
    "/{tenant_id}",
    response_model=TenantMetadataResponse,
    summary="Get tenant metadata",
    description="Admin-only endpoint returning tenant configuration.",
)
async def get_tenant(
    tenant_id: str,
    authorization: Optional[str] = Header(default=None),
) -> JSONResponse:
    principal = _resolve_principal(authorization)
    _require_admin(principal)

    _audit.log(AuditEvent(
        action=AuditAction.TENANT_ACCESS,
        tenant_id=tenant_id,
        principal=f"{principal.team}/{principal.role}",
        resource=f"tenant:{tenant_id}",
    ))

    driver = get_driver()
    async with _tenant_scoped_session(driver, tenant_id=tenant_id) as session:
        result = await session.execute_read(
            _run_read_query,
            "MATCH (n) WHERE n.tenant_id = $tenant_id "
            "RETURN count(n) AS node_count",
            {"tenant_id": tenant_id},
        )

    node_count = result[0].get("node_count", 0) if result else 0

    body = TenantMetadataResponse(
        tenant_id=tenant_id,
        database_name="neo4j",
        isolation_mode="physical",
        node_count=node_count,
    )
    return JSONResponse(content=body.model_dump(), status_code=200)


@tenant_router.get(
    "/{tenant_id}/repositories",
    response_model=RepositoryListResponse,
    summary="Get ingested repositories",
    description="Returns the list of repositories ingested for a tenant.",
)
async def get_tenant_repositories(
    tenant_id: str,
    authorization: Optional[str] = Header(default=None),
) -> JSONResponse:
    principal = _resolve_principal(authorization)
    _require_tenant_access(principal, tenant_id)

    driver = get_driver()
    async with _tenant_scoped_session(driver, tenant_id=tenant_id) as session:
        result = await session.execute_read(
            _run_read_query,
            "MATCH (r:Repository) WHERE r.tenant_id = $tenant_id "
            "RETURN r.url AS repo_url, r.ingested_at AS ingested_at "
            "ORDER BY r.ingested_at DESC",
            {"tenant_id": tenant_id},
        )

    repos = [
        RepositoryEntry(
            repo_url=r.get("repo_url", ""),
            ingested_at=r.get("ingested_at", ""),
        )
        for r in result
    ]

    body = RepositoryListResponse(tenant_id=tenant_id, repositories=repos)
    return JSONResponse(content=body.model_dump(), status_code=200)


@admin_router.get(
    "/schema/versions",
    response_model=SchemaVersionsResponse,
    summary="Get schema versions",
    description="Admin-only endpoint listing applied schema migrations.",
)
async def get_schema_versions(
    authorization: Optional[str] = Header(default=None),
) -> JSONResponse:
    principal = _resolve_principal(authorization)
    _require_admin(principal)

    current = _migration_registry.current_version()
    migrations = _migration_registry.all_migrations()

    versions = [
        SchemaVersionEntry(
            version=str(m.version),
            status=m.status.value,
        )
        for m in migrations
    ]

    body = SchemaVersionsResponse(
        current_version=str(current) if current else None,
        versions=versions,
    )
    return JSONResponse(content=body.model_dump(), status_code=200)


@admin_router.post(
    "/schema/migrate",
    response_model=MigrateResponse,
    summary="Trigger schema migration",
    description="Admin-only endpoint to apply pending schema migrations.",
)
async def trigger_migration(
    request: MigrateRequest,
    authorization: Optional[str] = Header(default=None),
) -> JSONResponse:
    principal = _resolve_principal(authorization)
    _require_admin(principal)

    _audit.log(AuditEvent(
        action=AuditAction.ADMIN_ACTION,
        tenant_id=principal.team,
        principal=f"{principal.team}/{principal.role}",
        resource="schema:migrate",
        detail=f"target_version={request.target_version}",
    ))

    try:
        applied = _migration_registry.apply_all()
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Migration failed: {exc}",
        ) from exc

    current = _migration_registry.current_version()
    body = MigrateResponse(
        status="completed",
        applied=applied,
        current_version=str(current) if current else None,
    )
    return JSONResponse(content=body.model_dump(), status_code=200)


@webhook_router.post(
    "/webhooks",
    response_model=WebhookResponse,
    summary="Register webhook",
    description="Register a webhook callback for system events.",
    status_code=201,
)
async def register_webhook(
    request: WebhookRequest,
    authorization: Optional[str] = Header(default=None),
) -> JSONResponse:
    principal = _resolve_principal(authorization)
    _require_admin(principal)

    webhook_id = str(uuid.uuid4())
    _webhook_store[webhook_id] = {
        "url": request.url,
        "events": request.events,
        "tenant_id": principal.team,
    }

    _audit.log(AuditEvent(
        action=AuditAction.CONFIG_CHANGE,
        tenant_id=principal.team,
        principal=f"{principal.team}/{principal.role}",
        resource=f"webhook:{webhook_id}",
        detail=f"registered for events: {request.events}",
    ))

    body = WebhookResponse(
        webhook_id=webhook_id,
        url=request.url,
        events=request.events,
    )
    return JSONResponse(content=body.model_dump(), status_code=201)


async def _run_read_query(
    tx: Any,
    query: str,
    params: Dict[str, Any],
) -> List[Dict[str, Any]]:
    result = await tx.run(query, **params)
    return [dict(record) async for record in result]
