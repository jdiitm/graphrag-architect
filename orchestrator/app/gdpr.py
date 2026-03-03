from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Protocol

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

logger = logging.getLogger(__name__)
_audit = SecurityAuditLogger()

gdpr_router = APIRouter(prefix="/v1/tenants", tags=["gdpr"])


class TenantDataStore(Protocol):
    async def find_by_tenant(self, tenant_id: str) -> List[Dict[str, Any]]: ...
    async def delete_by_tenant(self, tenant_id: str) -> int: ...
    async def delete_by_subject(
        self, tenant_id: str, subject_id: str,
    ) -> int: ...


@dataclass(frozen=True)
class DataSubjectRequest:
    request_type: str
    tenant_id: str
    subject_id: str
    requested_at: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DataExportResult:
    tenant_id: str
    subject_id: str
    records: List[Dict[str, Any]]
    exported_at: float = field(default_factory=time.time)

    @property
    def record_count(self) -> int:
        return len(self.records)

    def to_portable_format(self) -> Dict[str, Any]:
        return {
            "data_subject": self.subject_id,
            "tenant": self.tenant_id,
            "exported_at": self.exported_at,
            "record_count": self.record_count,
            "records": self.records,
        }


@dataclass
class ErasureResult:
    tenant_id: str
    subject_id: str
    records_deleted: int
    erased_at: float = field(default_factory=time.time)
    verified: bool = False


class GDPRService:
    def __init__(self) -> None:
        self._request_log: List[DataSubjectRequest] = []

    def _log_request(self, request: DataSubjectRequest) -> None:
        self._request_log.append(request)
        logger.info(
            "GDPR %s request: tenant=%s subject=%s",
            request.request_type,
            request.tenant_id,
            request.subject_id,
        )

    async def export_data(
        self,
        tenant_id: str,
        subject_id: str,
        store: TenantDataStore,
    ) -> DataExportResult:
        request = DataSubjectRequest(
            request_type="export",
            tenant_id=tenant_id,
            subject_id=subject_id,
        )
        self._log_request(request)

        records = await store.find_by_tenant(tenant_id)
        subject_records = [
            r for r in records
            if r.get("subject_id") == subject_id
            or r.get("owner") == subject_id
            or r.get("team_owner") == subject_id
        ]

        return DataExportResult(
            tenant_id=tenant_id,
            subject_id=subject_id,
            records=subject_records,
        )

    async def erase_data(
        self,
        tenant_id: str,
        subject_id: str,
        store: TenantDataStore,
    ) -> ErasureResult:
        request = DataSubjectRequest(
            request_type="erasure",
            tenant_id=tenant_id,
            subject_id=subject_id,
        )
        self._log_request(request)

        deleted = await store.delete_by_subject(tenant_id, subject_id)

        verification = await store.find_by_tenant(tenant_id)
        remaining = [
            r for r in verification
            if r.get("subject_id") == subject_id
            or r.get("owner") == subject_id
        ]

        return ErasureResult(
            tenant_id=tenant_id,
            subject_id=subject_id,
            records_deleted=deleted,
            verified=len(remaining) == 0,
        )

    def request_log(self) -> List[DataSubjectRequest]:
        return list(self._request_log)


class TenantExportRecord(BaseModel):
    node_id: str = ""
    name: str = ""
    node_type: str = ""
    tenant_id: str = ""
    properties: Dict[str, Any] = Field(default_factory=dict)


class TenantDataExportResponse(BaseModel):
    tenant_id: str
    record_count: int = 0
    records: List[Dict[str, Any]] = Field(default_factory=list)
    exported_at: float = 0.0


class TenantErasureResponse(BaseModel):
    tenant_id: str
    records_deleted: int = 0
    verified: bool = False
    erased_at: float = 0.0


class GDPRExportService:
    async def export_tenant_data(
        self, tenant_id: str,
    ) -> TenantDataExportResponse:
        driver = get_driver()
        async with driver.session() as session:
            records = await session.execute_read(
                _run_read_query,
                "MATCH (n) WHERE n.tenant_id = $tenant_id "
                "RETURN n.node_id AS node_id, n.name AS name, "
                "labels(n)[0] AS node_type, n.tenant_id AS tenant_id",
                {"tenant_id": tenant_id},
            )

        return TenantDataExportResponse(
            tenant_id=tenant_id,
            record_count=len(records),
            records=records,
            exported_at=time.time(),
        )


class GDPRErasureService:
    async def erase_tenant(
        self, tenant_id: str,
    ) -> TenantErasureResponse:
        driver = get_driver()
        async with driver.session() as session:
            result = await session.execute_write(
                _run_write_query,
                "MATCH (n {tenant_id: $tenant_id}) DETACH DELETE n "
                "RETURN count(n) AS deleted_count",
                {"tenant_id": tenant_id},
            )

        deleted_count = result[0].get("deleted_count", 0) if result else 0

        async with driver.session() as session:
            remaining = await session.execute_read(
                _run_read_query,
                "MATCH (n) WHERE n.tenant_id = $tenant_id "
                "RETURN count(n) AS remaining",
                {"tenant_id": tenant_id},
            )

        remaining_count = remaining[0].get("remaining", 0) if remaining else 0

        return TenantErasureResponse(
            tenant_id=tenant_id,
            records_deleted=deleted_count,
            verified=remaining_count == 0,
            erased_at=time.time(),
        )


_export_service = GDPRExportService()
_erasure_service = GDPRErasureService()


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


@gdpr_router.get(
    "/{tenant_id}/data-export",
    response_model=TenantDataExportResponse,
    summary="Export all tenant data (GDPR Art.15)",
)
async def export_tenant_data(
    tenant_id: str,
    authorization: Optional[str] = Header(default=None),
) -> JSONResponse:
    principal = _resolve_principal(authorization)
    _require_admin(principal)

    _audit.log(AuditEvent(
        action=AuditAction.ADMIN_ACTION,
        tenant_id=tenant_id,
        principal=f"{principal.team}/{principal.role}",
        resource=f"gdpr:export:{tenant_id}",
        detail="GDPR Art.15 data export requested",
    ))

    result = await _export_service.export_tenant_data(tenant_id)
    return JSONResponse(content=result.model_dump(), status_code=200)


@gdpr_router.delete(
    "/{tenant_id}",
    response_model=TenantErasureResponse,
    summary="Erase all tenant data (GDPR Art.17)",
)
async def erase_tenant_data(
    tenant_id: str,
    authorization: Optional[str] = Header(default=None),
) -> JSONResponse:
    principal = _resolve_principal(authorization)
    _require_admin(principal)

    _audit.log(AuditEvent(
        action=AuditAction.ADMIN_ACTION,
        tenant_id=tenant_id,
        principal=f"{principal.team}/{principal.role}",
        resource=f"gdpr:erasure:{tenant_id}",
        detail="GDPR Art.17 data erasure requested",
    ))

    result = await _erasure_service.erase_tenant(tenant_id)
    return JSONResponse(content=result.model_dump(), status_code=200)


async def _run_read_query(
    tx: Any,
    query: str,
    params: Dict[str, Any],
) -> List[Dict[str, Any]]:
    result = await tx.run(query, **params)
    return [dict(record) async for record in result]


async def _run_write_query(
    tx: Any,
    query: str,
    params: Dict[str, Any],
) -> List[Dict[str, Any]]:
    result = await tx.run(query, **params)
    return [dict(record) async for record in result]
