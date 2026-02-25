from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Protocol

logger = logging.getLogger(__name__)


class TenantDataStore(Protocol):
    async def find_by_tenant(self, tenant_id: str) -> List[Dict[str, Any]]: ...
    async def delete_by_tenant(self, tenant_id: str) -> int: ...


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

        deleted = await store.delete_by_tenant(tenant_id)

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
