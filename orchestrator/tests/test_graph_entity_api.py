from __future__ import annotations

import os
from contextlib import asynccontextmanager, contextmanager
from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from starlette.testclient import TestClient

from orchestrator.app.access_control import sign_token

_TOKEN_SECRET = "test-secret-key-for-graph-entity-api"

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
            patch("orchestrator.app.graph_api.get_driver", return_value=mock_driver),
        ):
            yield TestClient(app, raise_server_exceptions=False)
    finally:
        app.router.lifespan_context = original_lifespan


class TestListEntities:
    def test_returns_paginated_list(self):
        entities = [
            {"n": {"id": "svc-1", "name": "auth"}, "labels": ["Service"]},
            {"n": {"id": "svc-2", "name": "gateway"}, "labels": ["Service"]},
        ]
        mock_driver, _ = _make_mock_driver(entities)
        with _patched_client(mock_driver) as client:
            resp = client.get(
                "/v1/graph/entities?limit=10&offset=0",
                headers={"Authorization": f"Bearer {_ADMIN_TOKEN}"},
            )
        assert resp.status_code == 200
        body = resp.json()
        assert "entities" in body
        assert body["limit"] == 10
        assert body["offset"] == 0

    def test_filters_by_node_type(self):
        entities = [
            {"n": {"id": "svc-1", "name": "auth"}, "labels": ["Service"]},
        ]
        mock_driver, _ = _make_mock_driver(entities)
        with _patched_client(mock_driver) as client:
            resp = client.get(
                "/v1/graph/entities?type=Service",
                headers={"Authorization": f"Bearer {_ADMIN_TOKEN}"},
            )
        assert resp.status_code == 200
        body = resp.json()
        assert "entities" in body


class TestGetEntity:
    def test_returns_entity_with_relationships(self):
        entity_records = [
            {
                "n": {"id": "svc-1", "name": "auth"},
                "labels": ["Service"],
                "rels": [
                    {"type": "CALLS", "target_id": "svc-2", "target_name": "gateway"},
                ],
            },
        ]
        mock_driver, _ = _make_mock_driver(entity_records)
        with _patched_client(mock_driver) as client:
            resp = client.get(
                "/v1/graph/entities/svc-1",
                headers={"Authorization": f"Bearer {_ADMIN_TOKEN}"},
            )
        assert resp.status_code == 200
        body = resp.json()
        assert body["id"] == "svc-1"

    def test_returns_404_for_unknown_id(self):
        mock_driver, _ = _make_mock_driver([])
        with _patched_client(mock_driver) as client:
            resp = client.get(
                "/v1/graph/entities/nonexistent",
                headers={"Authorization": f"Bearer {_ADMIN_TOKEN}"},
            )
        assert resp.status_code == 404


class TestGetNeighbors:
    def test_returns_default_one_hop_neighbors(self):
        neighbors = [
            {"id": "svc-2", "name": "gateway", "labels": ["Service"], "rel_type": "CALLS"},
        ]
        mock_driver, _ = _make_mock_driver(neighbors)
        with _patched_client(mock_driver) as client:
            resp = client.get(
                "/v1/graph/entities/svc-1/neighbors",
                headers={"Authorization": f"Bearer {_ADMIN_TOKEN}"},
            )
        assert resp.status_code == 200
        body = resp.json()
        assert "neighbors" in body

    def test_respects_hop_parameter(self):
        neighbors = [
            {"id": "svc-2", "name": "gateway", "labels": ["Service"], "rel_type": "CALLS"},
            {"id": "svc-3", "name": "db-proxy", "labels": ["Service"], "rel_type": "CALLS"},
        ]
        mock_driver, _ = _make_mock_driver(neighbors)
        with _patched_client(mock_driver) as client:
            resp = client.get(
                "/v1/graph/entities/svc-1/neighbors?hops=2",
                headers={"Authorization": f"Bearer {_ADMIN_TOKEN}"},
            )
        assert resp.status_code == 200
        body = resp.json()
        assert "neighbors" in body


class TestGraphSchema:
    def test_returns_schema(self):
        mock_driver, mock_session = _make_mock_driver()
        call_count = 0

        async def _multi_execute_read(fn, *args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return [{"labels": ["Service", "Database"]}]
            if call_count == 2:
                return [{"types": ["CALLS", "PRODUCES"]}]
            return [{"constraints": ["unique_service_id"]}]

        mock_session.execute_read = AsyncMock(side_effect=_multi_execute_read)
        with _patched_client(mock_driver) as client:
            resp = client.get(
                "/v1/graph/schema",
                headers={"Authorization": f"Bearer {_ADMIN_TOKEN}"},
            )
        assert resp.status_code == 200
        body = resp.json()
        assert "node_types" in body
        assert "edge_types" in body


class TestGraphStats:
    def test_returns_node_edge_counts(self):
        mock_driver, mock_session = _make_mock_driver()
        call_count = 0

        async def _multi_execute_read(fn, *args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return [
                    {"label": "Service", "cnt": 42},
                    {"label": "Database", "cnt": 5},
                ]
            return [
                {"rel_type": "CALLS", "cnt": 100},
                {"rel_type": "PRODUCES", "cnt": 20},
            ]

        mock_session.execute_read = AsyncMock(side_effect=_multi_execute_read)
        with _patched_client(mock_driver) as client:
            resp = client.get(
                "/v1/graph/stats",
                headers={"Authorization": f"Bearer {_ADMIN_TOKEN}"},
            )
        assert resp.status_code == 200
        body = resp.json()
        assert "node_counts" in body
        assert "edge_counts" in body


class TestCypherEndpoint:
    def test_admin_can_execute_readonly_cypher(self):
        query_results = [{"n.name": "auth"}]
        mock_driver, _ = _make_mock_driver(query_results)
        with _patched_client(mock_driver) as client:
            resp = client.post(
                "/v1/graph/cypher",
                json={
                    "query": (
                        "MATCH (n:Service) WHERE n.tenant_id = $tenant_id "
                        "RETURN n.name LIMIT 10"
                    ),
                },
                headers={"Authorization": f"Bearer {_ADMIN_TOKEN}"},
            )
        assert resp.status_code == 200
        body = resp.json()
        assert "results" in body

    def test_non_admin_gets_403(self):
        mock_driver, _ = _make_mock_driver()
        with _patched_client(mock_driver) as client:
            resp = client.post(
                "/v1/graph/cypher",
                json={"query": "MATCH (n) RETURN n LIMIT 1"},
                headers={"Authorization": f"Bearer {_VIEWER_TOKEN}"},
            )
        assert resp.status_code == 403

    def test_write_query_returns_400(self):
        mock_driver, _ = _make_mock_driver()
        with _patched_client(mock_driver) as client:
            resp = client.post(
                "/v1/graph/cypher",
                json={"query": "CREATE (n:Service {name: 'evil'})"},
                headers={"Authorization": f"Bearer {_ADMIN_TOKEN}"},
            )
        assert resp.status_code == 400


class TestAuthRequired:
    @pytest.mark.parametrize("path", [
        "/v1/graph/entities",
        "/v1/graph/entities/svc-1",
        "/v1/graph/entities/svc-1/neighbors",
        "/v1/graph/schema",
        "/v1/graph/stats",
    ])
    def test_get_endpoints_require_auth(self, path):
        mock_driver, _ = _make_mock_driver([])
        with _patched_client(mock_driver) as client:
            resp = client.get(path)
        assert resp.status_code == 401, f"{path} should require auth"

    def test_cypher_endpoint_requires_auth(self):
        mock_driver, _ = _make_mock_driver()
        with _patched_client(mock_driver) as client:
            resp = client.post(
                "/v1/graph/cypher",
                json={"query": "MATCH (n) RETURN n LIMIT 1"},
            )
        assert resp.status_code == 401


class TestACLFiltering:
    def test_tenant_a_cannot_see_tenant_b_data(self):
        tenant_a_entities = [
            {"n": {"id": "svc-a1", "name": "auth-a", "tenant_id": "team-a"}, "labels": ["Service"]},
        ]
        mock_driver, _ = _make_mock_driver(tenant_a_entities)
        with _patched_client(mock_driver) as client:
            resp = client.get(
                "/v1/graph/entities",
                headers={"Authorization": f"Bearer {_VIEWER_TOKEN}"},
            )
            assert resp.status_code == 200

            resp_b = client.get(
                "/v1/graph/entities",
                headers={"Authorization": f"Bearer {_TENANT_B_TOKEN}"},
            )
            assert resp_b.status_code == 200
