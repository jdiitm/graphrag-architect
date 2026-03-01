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

    async def delete_by_subject(
        self, tenant_id: str, subject_id: str,
    ) -> int:
        before = len(self._records)
        self._records = [
            r for r in self._records
            if not (
                r.get("tenant_id") == tenant_id
                and (
                    r.get("subject_id") == subject_id
                    or r.get("owner") == subject_id
                )
            )
        ]
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


class TestSubjectScopedErasure:

    @pytest.mark.asyncio
    async def test_erase_only_subject_records(self) -> None:
        store = MockStore([
            {"tenant_id": "t1", "subject_id": "alice", "data": "a"},
            {"tenant_id": "t1", "subject_id": "bob", "data": "b"},
            {"tenant_id": "t1", "subject_id": "charlie", "data": "c"},
        ])
        service = GDPRService()
        result = await service.erase_data("t1", "alice", store)
        assert result.records_deleted == 1
        assert result.verified is True
        remaining = await store.find_by_tenant("t1")
        assert len(remaining) == 2
        remaining_subjects = {r["subject_id"] for r in remaining}
        assert remaining_subjects == {"bob", "charlie"}

    @pytest.mark.asyncio
    async def test_erase_unknown_subject_returns_zero(self) -> None:
        store = MockStore([
            {"tenant_id": "t1", "subject_id": "alice", "data": "a"},
        ])
        service = GDPRService()
        result = await service.erase_data("t1", "nobody", store)
        assert result.records_deleted == 0
        assert result.verified is True

    @pytest.mark.asyncio
    async def test_erase_does_not_cascade_to_other_tenants(self) -> None:
        store = MockStore([
            {"tenant_id": "t1", "subject_id": "alice", "data": "a1"},
            {"tenant_id": "t2", "subject_id": "alice", "data": "a2"},
        ])
        service = GDPRService()
        result = await service.erase_data("t1", "alice", store)
        assert result.records_deleted == 1
        assert result.verified is True
        t2_records = await store.find_by_tenant("t2")
        assert len(t2_records) == 1
        assert t2_records[0]["data"] == "a2"


class TestDataSubjectRequest:

    def test_has_timestamp(self) -> None:
        req = DataSubjectRequest(
            request_type="export",
            tenant_id="t1",
            subject_id="u1",
        )
        assert req.requested_at > 0
