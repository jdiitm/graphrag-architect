from __future__ import annotations

import os
from contextlib import asynccontextmanager, contextmanager
from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from starlette.testclient import TestClient

from orchestrator.app.access_control import sign_token


_TOKEN_SECRET = "test-secret-key-for-gdpr-api-xxxx"

_ADMIN_TOKEN = sign_token(
    {"team": "platform", "namespace": "infra", "role": "admin"},
    _TOKEN_SECRET,
)
_VIEWER_TOKEN = sign_token(
    {"team": "team-a", "namespace": "ns-a", "role": "viewer"},
    _TOKEN_SECRET,
)

_ENV = {
    "NEO4J_PASSWORD": "test",
    "NEO4J_URI": "bolt://localhost:7687",
    "GOOGLE_API_KEY": "fake",
    "AUTH_TOKEN_SECRET": _TOKEN_SECRET,
    "AUTH_REQUIRE_TOKENS": "true",
}

_SAMPLE_TENANT_NODES = [
    {
        "node_id": "svc-1",
        "name": "auth-service",
        "node_type": "Service",
        "tenant_id": "acme-corp",
    },
    {
        "node_id": "topic-1",
        "name": "user-events",
        "node_type": "KafkaTopic",
        "tenant_id": "acme-corp",
    },
    {
        "node_id": "db-1",
        "name": "users-db",
        "node_type": "Database",
        "tenant_id": "acme-corp",
    },
    {
        "node_id": "dep-1",
        "name": "auth-deployment",
        "node_type": "K8sDeployment",
        "tenant_id": "acme-corp",
    },
]


def _make_mock_driver(
    read_results: List[Dict[str, Any]] | None = None,
    write_results: List[Dict[str, Any]] | None = None,
):
    records = read_results if read_results is not None else []
    mock_driver = MagicMock()
    mock_session = AsyncMock()
    _call_count = {"execute_read": 0}

    async def _execute_read(fn, *args, **kwargs):
        idx = _call_count["execute_read"]
        _call_count["execute_read"] += 1
        if isinstance(records, list) and len(records) > 0 and isinstance(records[0], list):
            return records[idx] if idx < len(records) else []
        return records

    async def _execute_write(fn, *args, **kwargs):
        return write_results if write_results is not None else []

    mock_session.execute_read = AsyncMock(side_effect=_execute_read)
    mock_session.execute_write = AsyncMock(side_effect=_execute_write)
    mock_session.run = AsyncMock()

    @asynccontextmanager
    async def _session(**kwargs):
        yield mock_session

    mock_driver.session = _session
    mock_driver.close = AsyncMock()
    return mock_driver, mock_session


@contextmanager
def _patched_client(mock_driver):
    @asynccontextmanager
    async def _lifespan(_app):
        yield

    from orchestrator.app.main import app

    original_lifespan = app.router.lifespan_context
    app.router.lifespan_context = _lifespan
    try:
        with (
            patch.dict(os.environ, _ENV, clear=False),
            patch("orchestrator.app.gdpr.get_driver", return_value=mock_driver),
        ):
            yield TestClient(app, raise_server_exceptions=False)
    finally:
        app.router.lifespan_context = original_lifespan


class TestDataExportEndpoint:
    def test_admin_exports_tenant_data(self):
        mock_driver, _ = _make_mock_driver(read_results=_SAMPLE_TENANT_NODES)
        with _patched_client(mock_driver) as client:
            resp = client.get(
                "/v1/tenants/acme-corp/data-export",
                headers={"Authorization": f"Bearer {_ADMIN_TOKEN}"},
            )
        assert resp.status_code == 200
        body = resp.json()
        assert body["tenant_id"] == "acme-corp"
        assert len(body["records"]) == len(_SAMPLE_TENANT_NODES)

    def test_export_includes_all_node_types(self):
        mock_driver, _ = _make_mock_driver(read_results=_SAMPLE_TENANT_NODES)
        with _patched_client(mock_driver) as client:
            resp = client.get(
                "/v1/tenants/acme-corp/data-export",
                headers={"Authorization": f"Bearer {_ADMIN_TOKEN}"},
            )
        body = resp.json()
        node_types = {r["node_type"] for r in body["records"]}
        assert "Service" in node_types
        assert "KafkaTopic" in node_types
        assert "Database" in node_types
        assert "K8sDeployment" in node_types

    def test_export_includes_record_count(self):
        mock_driver, _ = _make_mock_driver(read_results=_SAMPLE_TENANT_NODES)
        with _patched_client(mock_driver) as client:
            resp = client.get(
                "/v1/tenants/acme-corp/data-export",
                headers={"Authorization": f"Bearer {_ADMIN_TOKEN}"},
            )
        body = resp.json()
        assert body["record_count"] == len(_SAMPLE_TENANT_NODES)

    def test_export_requires_admin(self):
        mock_driver, _ = _make_mock_driver()
        with _patched_client(mock_driver) as client:
            resp = client.get(
                "/v1/tenants/acme-corp/data-export",
                headers={"Authorization": f"Bearer {_VIEWER_TOKEN}"},
            )
        assert resp.status_code == 403

    def test_export_requires_auth(self):
        mock_driver, _ = _make_mock_driver()
        with _patched_client(mock_driver) as client:
            resp = client.get("/v1/tenants/acme-corp/data-export")
        assert resp.status_code == 401


class TestTenantErasureEndpoint:
    def test_admin_erases_tenant_data(self):
        delete_result = [{"deleted_count": 4}]
        verification = []
        mock_driver, _ = _make_mock_driver(
            read_results=[verification],
            write_results=delete_result,
        )
        with _patched_client(mock_driver) as client:
            resp = client.delete(
                "/v1/tenants/acme-corp",
                headers={"Authorization": f"Bearer {_ADMIN_TOKEN}"},
            )
        assert resp.status_code == 200
        body = resp.json()
        assert body["tenant_id"] == "acme-corp"
        assert body["records_deleted"] == 4
        assert body["verified"] is True

    def test_erasure_verifies_no_data_remains(self):
        delete_result = [{"deleted_count": 2}]
        remaining_data = [{"remaining": 1}]
        mock_driver, _ = _make_mock_driver(
            read_results=[remaining_data],
            write_results=delete_result,
        )
        with _patched_client(mock_driver) as client:
            resp = client.delete(
                "/v1/tenants/acme-corp",
                headers={"Authorization": f"Bearer {_ADMIN_TOKEN}"},
            )
        body = resp.json()
        assert body["verified"] is False

    def test_erasure_requires_admin(self):
        mock_driver, _ = _make_mock_driver()
        with _patched_client(mock_driver) as client:
            resp = client.delete(
                "/v1/tenants/acme-corp",
                headers={"Authorization": f"Bearer {_VIEWER_TOKEN}"},
            )
        assert resp.status_code == 403

    def test_erasure_requires_auth(self):
        mock_driver, _ = _make_mock_driver()
        with _patched_client(mock_driver) as client:
            resp = client.delete("/v1/tenants/acme-corp")
        assert resp.status_code == 401

    def test_erasure_cascades_across_graph(self):
        delete_result = [{"deleted_count": 10}]
        mock_driver, mock_session = _make_mock_driver(
            read_results=[[]],
            write_results=delete_result,
        )
        with _patched_client(mock_driver) as client:
            resp = client.delete(
                "/v1/tenants/acme-corp",
                headers={"Authorization": f"Bearer {_ADMIN_TOKEN}"},
            )
        assert resp.status_code == 200
        mock_session.execute_write.assert_called_once()
