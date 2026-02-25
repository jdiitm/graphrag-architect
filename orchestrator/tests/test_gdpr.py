from __future__ import annotations

from typing import Any, Dict, List

import pytest

from orchestrator.app.gdpr import (
    DataExportResult,
    DataSubjectRequest,
    ErasureResult,
    GDPRService,
)


class MockStore:
    def __init__(self, records: List[Dict[str, Any]]) -> None:
        self._records = list(records)
        self._deleted = 0

    async def find_by_tenant(self, tenant_id: str) -> List[Dict[str, Any]]:
        return [r for r in self._records if r.get("tenant_id") == tenant_id]

    async def delete_by_tenant(self, tenant_id: str) -> int:
        before = len(self._records)
        self._records = [r for r in self._records if r.get("tenant_id") != tenant_id]
        deleted = before - len(self._records)
        self._deleted += deleted
        return deleted


class TestDataExport:

    @pytest.mark.asyncio
    async def test_export_returns_subject_records(self) -> None:
        store = MockStore([
            {"tenant_id": "t1", "subject_id": "user@a.com", "data": "x"},
            {"tenant_id": "t1", "subject_id": "other@a.com", "data": "y"},
            {"tenant_id": "t2", "subject_id": "user@a.com", "data": "z"},
        ])
        service = GDPRService()
        result = await service.export_data("t1", "user@a.com", store)
        assert result.record_count == 1
        assert result.records[0]["data"] == "x"

    @pytest.mark.asyncio
    async def test_export_returns_empty_for_unknown_subject(self) -> None:
        store = MockStore([
            {"tenant_id": "t1", "subject_id": "other@a.com", "data": "x"},
        ])
        service = GDPRService()
        result = await service.export_data("t1", "nobody@a.com", store)
        assert result.record_count == 0

    @pytest.mark.asyncio
    async def test_export_portable_format(self) -> None:
        store = MockStore([
            {"tenant_id": "t1", "subject_id": "u1", "data": "x"},
        ])
        service = GDPRService()
        result = await service.export_data("t1", "u1", store)
        portable = result.to_portable_format()
        assert portable["data_subject"] == "u1"
        assert portable["record_count"] == 1
        assert "exported_at" in portable


class TestDataErasure:

    @pytest.mark.asyncio
    async def test_erase_deletes_tenant_records(self) -> None:
        store = MockStore([
            {"tenant_id": "t1", "subject_id": "u1", "data": "x"},
            {"tenant_id": "t2", "subject_id": "u2", "data": "y"},
        ])
        service = GDPRService()
        result = await service.erase_data("t1", "u1", store)
        assert result.records_deleted == 1
        assert result.verified is True

    @pytest.mark.asyncio
    async def test_erase_verifies_deletion(self) -> None:
        store = MockStore([
            {"tenant_id": "t1", "subject_id": "u1", "data": "x"},
        ])
        service = GDPRService()
        result = await service.erase_data("t1", "u1", store)
        assert result.verified is True
        assert result.records_deleted == 1


class TestGDPRRequestLog:

    @pytest.mark.asyncio
    async def test_export_logged(self) -> None:
        store = MockStore([])
        service = GDPRService()
        await service.export_data("t1", "u1", store)
        log = service.request_log()
        assert len(log) == 1
        assert log[0].request_type == "export"
        assert log[0].tenant_id == "t1"

    @pytest.mark.asyncio
    async def test_erasure_logged(self) -> None:
        store = MockStore([])
        service = GDPRService()
        await service.erase_data("t1", "u1", store)
        log = service.request_log()
        assert len(log) == 1
        assert log[0].request_type == "erasure"

    @pytest.mark.asyncio
    async def test_multiple_requests_accumulated(self) -> None:
        store = MockStore([])
        service = GDPRService()
        await service.export_data("t1", "u1", store)
        await service.erase_data("t1", "u1", store)
        assert len(service.request_log()) == 2


class TestDataSubjectRequest:

    def test_has_timestamp(self) -> None:
        req = DataSubjectRequest(
            request_type="export",
            tenant_id="t1",
            subject_id="u1",
        )
        assert req.requested_at > 0
