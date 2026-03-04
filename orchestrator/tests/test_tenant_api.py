from __future__ import annotations

import os
from contextlib import asynccontextmanager, contextmanager
from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from starlette.testclient import TestClient

from orchestrator.app.access_control import sign_token

_TOKEN_SECRET = "test-secret-key-for-tenant-api-xx"

_ADMIN_TOKEN = sign_token(
    {"team": "platform", "namespace": "infra", "role": "admin"},
    _TOKEN_SECRET,
)
_VIEWER_TOKEN = sign_token(
    {"team": "team-a", "namespace": "ns-a", "role": "viewer"},
    _TOKEN_SECRET,
)
_TENANT_B_TOKEN = sign_token(
    {"team": "team-b", "namespace": "ns-b", "role": "viewer"},
    _TOKEN_SECRET,
)

_ENV = {
    "NEO4J_PASSWORD": "test",
    "NEO4J_URI": "bolt://localhost:7687",
    "GOOGLE_API_KEY": "fake",
    "AUTH_TOKEN_SECRET": _TOKEN_SECRET,
    "AUTH_REQUIRE_TOKENS": "true",
}


def _make_mock_driver(read_results: List[Dict[str, Any]] | None = None):
    records = read_results if read_results is not None else []
    mock_driver = MagicMock()
    mock_session = AsyncMock()

    async def _execute_read(fn, *args, **kwargs):
        return records

    mock_session.execute_read = AsyncMock(side_effect=_execute_read)
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
            patch("orchestrator.app.tenant_admin_api.get_driver", return_value=mock_driver),
        ):
            yield TestClient(app, raise_server_exceptions=False)
    finally:
        app.router.lifespan_context = original_lifespan


class TestGetTenantMetadata:
    def test_admin_gets_tenant_metadata(self):
        tenant_data = [
            {"tenant_id": "platform", "database_name": "neo4j", "isolation_mode": "physical"},
        ]
        mock_driver, _ = _make_mock_driver(tenant_data)
        with _patched_client(mock_driver) as client:
            resp = client.get(
                "/v1/tenants/platform",
                headers={"Authorization": f"Bearer {_ADMIN_TOKEN}"},
            )
        assert resp.status_code == 200
        body = resp.json()
        assert body["tenant_id"] == "platform"

    def test_non_admin_gets_403(self):
        mock_driver, _ = _make_mock_driver()
        with _patched_client(mock_driver) as client:
            resp = client.get(
                "/v1/tenants/platform",
                headers={"Authorization": f"Bearer {_VIEWER_TOKEN}"},
            )
        assert resp.status_code == 403


class TestGetTenantRepositories:
    def test_returns_ingested_repos(self):
        repos = [
            {"repo_url": "github.com/org/service-a", "ingested_at": "2026-01-01T00:00:00Z"},
            {"repo_url": "github.com/org/service-b", "ingested_at": "2026-01-02T00:00:00Z"},
        ]
        mock_driver, _ = _make_mock_driver(repos)
        with _patched_client(mock_driver) as client:
            resp = client.get(
                "/v1/tenants/team-a/repositories",
                headers={"Authorization": f"Bearer {_VIEWER_TOKEN}"},
            )
        assert resp.status_code == 200
        body = resp.json()
        assert "repositories" in body

    def test_tenant_isolation_enforced(self):
        mock_driver, _ = _make_mock_driver([])
        with _patched_client(mock_driver) as client:
            resp = client.get(
                "/v1/tenants/team-b/repositories",
                headers={"Authorization": f"Bearer {_VIEWER_TOKEN}"},
            )
        assert resp.status_code == 403


class TestSchemaVersions:
    def test_admin_gets_schema_versions(self):
        versions = [
            {"version": "1.0.0", "status": "applied"},
            {"version": "1.1.0", "status": "applied"},
        ]
        mock_driver, _ = _make_mock_driver(versions)
        with _patched_client(mock_driver) as client:
            resp = client.get(
                "/v1/admin/schema/versions",
                headers={"Authorization": f"Bearer {_ADMIN_TOKEN}"},
            )
        assert resp.status_code == 200
        body = resp.json()
        assert "versions" in body

    def test_non_admin_gets_403(self):
        mock_driver, _ = _make_mock_driver()
        with _patched_client(mock_driver) as client:
            resp = client.get(
                "/v1/admin/schema/versions",
                headers={"Authorization": f"Bearer {_VIEWER_TOKEN}"},
            )
        assert resp.status_code == 403


class TestSchemaMigrate:
    def test_admin_triggers_migration(self):
        mock_driver, _ = _make_mock_driver()
        with _patched_client(mock_driver) as client:
            resp = client.post(
                "/v1/admin/schema/migrate",
                json={"target_version": "1.2.0"},
                headers={"Authorization": f"Bearer {_ADMIN_TOKEN}"},
            )
        assert resp.status_code == 200
        body = resp.json()
        assert "applied" in body or "status" in body

    def test_non_admin_gets_403(self):
        mock_driver, _ = _make_mock_driver()
        with _patched_client(mock_driver) as client:
            resp = client.post(
                "/v1/admin/schema/migrate",
                json={"target_version": "1.2.0"},
                headers={"Authorization": f"Bearer {_VIEWER_TOKEN}"},
            )
        assert resp.status_code == 403


class TestWebhookRegistration:
    def test_register_webhook(self):
        mock_driver, _ = _make_mock_driver()
        with _patched_client(mock_driver) as client:
            resp = client.post(
                "/v1/webhooks",
                json={
                    "url": "https://example.com/hook",
                    "events": ["ingestion.completed"],
                },
                headers={"Authorization": f"Bearer {_ADMIN_TOKEN}"},
            )
        assert resp.status_code == 201
        body = resp.json()
        assert "webhook_id" in body


class TestAuthRequired:
    @pytest.mark.parametrize("method_path_body", [
        ("GET", "/v1/tenants/test-tenant", None),
        ("GET", "/v1/tenants/test-tenant/repositories", None),
        ("GET", "/v1/admin/schema/versions", None),
        ("POST", "/v1/admin/schema/migrate", {"target_version": "1.0.0"}),
        ("POST", "/v1/webhooks", {"url": "https://example.com", "events": ["test"]}),
    ])
    def test_all_endpoints_require_auth(self, method_path_body):
        method, path, body = method_path_body
        mock_driver, _ = _make_mock_driver([])
        with _patched_client(mock_driver) as client:
            if method == "GET":
                resp = client.get(path)
            else:
                resp = client.post(path, json=body)
        assert resp.status_code == 401, f"{method} {path} should require auth"


class TestWebhookAdminRequired:

    def test_viewer_cannot_register_webhook(self):
        mock_driver, _ = _make_mock_driver()
        with _patched_client(mock_driver) as client:
            resp = client.post(
                "/v1/webhooks",
                json={
                    "url": "https://example.com/hook",
                    "events": ["ingestion.completed"],
                },
                headers={"Authorization": f"Bearer {_VIEWER_TOKEN}"},
            )
        assert resp.status_code == 403

    def test_admin_can_register_webhook(self):
        mock_driver, _ = _make_mock_driver()
        with _patched_client(mock_driver) as client:
            resp = client.post(
                "/v1/webhooks",
                json={
                    "url": "https://example.com/hook",
                    "events": ["ingestion.completed"],
                },
                headers={"Authorization": f"Bearer {_ADMIN_TOKEN}"},
            )
        assert resp.status_code == 201


class TestTenantScopedSessions:

    def test_get_tenant_opens_session_with_database(self):
        session_kwargs_log: List[Dict[str, Any]] = []
        mock_driver, _ = _make_mock_driver([{"node_count": 5}])
        original_session = mock_driver.session

        @asynccontextmanager
        async def _tracking_session(**kwargs):
            session_kwargs_log.append(kwargs)
            async with original_session(**kwargs) as s:
                yield s

        mock_driver.session = _tracking_session

        with _patched_client(mock_driver) as client:
            resp = client.get(
                "/v1/tenants/platform",
                headers={"Authorization": f"Bearer {_ADMIN_TOKEN}"},
            )
        assert resp.status_code == 200
        assert len(session_kwargs_log) == 1
        assert "database" in session_kwargs_log[0]

    def test_get_repositories_opens_session_with_database(self):
        session_kwargs_log: List[Dict[str, Any]] = []
        mock_driver, _ = _make_mock_driver([])
        original_session = mock_driver.session

        @asynccontextmanager
        async def _tracking_session(**kwargs):
            session_kwargs_log.append(kwargs)
            async with original_session(**kwargs) as s:
                yield s

        mock_driver.session = _tracking_session

        with _patched_client(mock_driver) as client:
            resp = client.get(
                "/v1/tenants/team-a/repositories",
                headers={"Authorization": f"Bearer {_VIEWER_TOKEN}"},
            )
        assert resp.status_code == 200
        assert len(session_kwargs_log) == 1
        assert "database" in session_kwargs_log[0]


class TestTenantIsolation:
    def test_viewer_can_only_see_own_tenant(self):
        mock_driver, _ = _make_mock_driver([])
        with _patched_client(mock_driver) as client:
            resp_own = client.get(
                "/v1/tenants/team-a/repositories",
                headers={"Authorization": f"Bearer {_VIEWER_TOKEN}"},
            )
            assert resp_own.status_code == 200

            resp_other = client.get(
                "/v1/tenants/team-b/repositories",
                headers={"Authorization": f"Bearer {_VIEWER_TOKEN}"},
            )
            assert resp_other.status_code == 403
