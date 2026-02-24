from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import Request
from starlette.testclient import TestClient

from orchestrator.app.access_control import sign_token
from orchestrator.app.main import app
from orchestrator.app.query_models import QueryComplexity
from orchestrator.tests.conftest import mock_neo4j_driver_with_session

_TEST_SECRET = "rbac-integration-test-secret-key-32b"


def _jwt_header(claims: dict) -> str:
    token = sign_token(claims, _TEST_SECRET)
    return f"Bearer {token}"


def _make_state(base_query_state, **overrides):
    return {**base_query_state, **overrides}


def _make_neo4j_session(run_return=None, run_side_effect=None):
    mock_session = AsyncMock()
    if run_side_effect:
        mock_session.execute_read = AsyncMock(side_effect=run_side_effect)
    else:
        mock_session.execute_read = AsyncMock(return_value=run_return or [])
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=False)
    return mock_session


@pytest.fixture(name="client")
def fixture_client():
    return TestClient(app, raise_server_exceptions=False)


class TestQueryEndpointAuthorizationHeader:
    def test_passes_auth_header_into_state(self, client):
        mock_result = {
            "query": "auth-service",
            "max_results": 10,
            "complexity": QueryComplexity.ENTITY_LOOKUP,
            "retrieval_path": "vector",
            "candidates": [],
            "cypher_query": "",
            "cypher_results": [],
            "iteration_count": 0,
            "answer": "auth-service is Go.",
            "sources": [],
            "authorization": "Bearer team=platform,namespace=production,role=admin",
        }
        mock_graph = MagicMock()
        mock_graph.ainvoke = AsyncMock(return_value=mock_result)

        with patch("orchestrator.app.main.query_graph", mock_graph):
            response = client.post(
                "/query",
                json={"query": "What is auth-service?"},
                headers={"Authorization": "Bearer team=platform,namespace=production,role=admin"},
            )

        assert response.status_code == 200
        call_state = mock_graph.ainvoke.call_args[0][0]
        assert call_state["authorization"] == "Bearer team=platform,namespace=production,role=admin"

    def test_empty_auth_header_passes_empty_string(self, client):
        mock_result = {
            "query": "auth",
            "max_results": 10,
            "complexity": QueryComplexity.ENTITY_LOOKUP,
            "retrieval_path": "vector",
            "candidates": [],
            "cypher_query": "",
            "cypher_results": [],
            "iteration_count": 0,
            "answer": "result",
            "sources": [],
            "authorization": "",
        }
        mock_graph = MagicMock()
        mock_graph.ainvoke = AsyncMock(return_value=mock_result)

        with patch("orchestrator.app.main.query_graph", mock_graph):
            response = client.post(
                "/query",
                json={"query": "What is auth?"},
            )

        assert response.status_code == 200
        call_state = mock_graph.ainvoke.call_args[0][0]
        assert call_state["authorization"] == ""


class TestVectorRetrieveAppliesACL:
    @pytest.mark.asyncio
    @patch.dict("os.environ", {"AUTH_TOKEN_SECRET": _TEST_SECRET})
    async def test_team_scoped_filter_injected(self, base_query_state):
        from orchestrator.app.query_engine import vector_retrieve

        mock_records = [{"name": "auth-service", "score": 0.95}]
        mock_session = _make_neo4j_session(run_return=mock_records)
        mock_driver = mock_neo4j_driver_with_session(mock_session)

        state = _make_state(
            base_query_state,
            query="auth-service",
            authorization=_jwt_header({"team": "platform", "namespace": "production", "role": "viewer"}),
        )

        with patch(
            "orchestrator.app.query_engine._get_neo4j_driver",
            return_value=mock_driver,
        ):
            result = await vector_retrieve(state)

        tx_func = mock_session.execute_read.call_args[0][0]
        mock_tx = AsyncMock()
        mock_run_result = AsyncMock()
        mock_run_result.data = AsyncMock(return_value=[])
        mock_tx.run = AsyncMock(return_value=mock_run_result)
        await tx_func(mock_tx)

        called_cypher = mock_tx.run.call_args[0][0]
        called_kwargs = mock_tx.run.call_args[1]
        assert "WHERE" in called_cypher
        assert "acl_team" in called_kwargs
        assert called_kwargs["acl_team"] == "platform"

    @pytest.mark.asyncio
    @patch.dict("os.environ", {"AUTH_TOKEN_SECRET": _TEST_SECRET})
    async def test_admin_bypasses_acl(self, base_query_state):
        from orchestrator.app.query_engine import vector_retrieve

        mock_records = [{"name": "svc", "score": 0.9}]
        mock_session = _make_neo4j_session(run_return=mock_records)
        mock_driver = mock_neo4j_driver_with_session(mock_session)

        state = _make_state(
            base_query_state,
            query="svc",
            authorization=_jwt_header({"team": "ops", "namespace": "all", "role": "admin"}),
        )

        with patch(
            "orchestrator.app.query_engine._get_neo4j_driver",
            return_value=mock_driver,
        ):
            result = await vector_retrieve(state)

        tx_func = mock_session.execute_read.call_args[0][0]
        mock_tx = AsyncMock()
        mock_run_result = AsyncMock()
        mock_run_result.data = AsyncMock(return_value=[])
        mock_tx.run = AsyncMock(return_value=mock_run_result)
        await tx_func(mock_tx)

        called_kwargs = mock_tx.run.call_args[1]
        assert "acl_team" not in called_kwargs


class TestCypherRetrieveAppliesACL:
    @pytest.mark.asyncio
    @patch.dict("os.environ", {"AUTH_TOKEN_SECRET": _TEST_SECRET})
    async def test_acl_injected_into_generated_cypher(self, base_query_state):
        from orchestrator.app.query_engine import cypher_retrieve

        mock_session = _make_neo4j_session(
            run_return=[{"source": "auth", "target": "user"}]
        )
        mock_driver = mock_neo4j_driver_with_session(mock_session)

        state = _make_state(
            base_query_state,
            query="blast radius of auth",
            authorization=_jwt_header({"team": "data", "namespace": "staging", "role": "editor"}),
        )

        with patch(
            "orchestrator.app.query_engine._get_neo4j_driver",
            return_value=mock_driver,
        ), patch(
            "orchestrator.app.query_engine._generate_cypher",
            new_callable=AsyncMock,
            return_value="MATCH (n:Service)-[:CALLS*]->(t) RETURN n, t",
        ):
            result = await cypher_retrieve(state)

        tx_func = mock_session.execute_read.call_args[0][0]
        mock_tx = AsyncMock()
        mock_run_result = AsyncMock()
        mock_run_result.data = AsyncMock(return_value=[])
        mock_tx.run = AsyncMock(return_value=mock_run_result)
        await tx_func(mock_tx)

        called_cypher = mock_tx.run.call_args[0][0]
        assert "WHERE" in called_cypher or "acl_team" in str(mock_tx.run.call_args)


class TestSingleHopRetrieveAppliesACL:
    @pytest.mark.asyncio
    @patch.dict("os.environ", {"AUTH_TOKEN_SECRET": _TEST_SECRET})
    async def test_acl_applied_to_hop_query(self, base_query_state):
        from orchestrator.app.query_engine import single_hop_retrieve

        call_count = [0]
        captured_cyphers = []

        async def fake_execute_read(tx_func, **kwargs):
            call_count[0] += 1
            mock_tx = AsyncMock()
            mock_run_result = AsyncMock()
            if call_count[0] == 1:
                mock_run_result.data = AsyncMock(
                    return_value=[{"name": "svc-a", "score": 0.9}]
                )
            else:
                mock_run_result.data = AsyncMock(
                    return_value=[{"source": "svc-a", "rel": "CALLS", "target": "svc-b"}]
                )
            mock_tx.run = AsyncMock(return_value=mock_run_result)
            result = await tx_func(mock_tx)
            if mock_tx.run.called:
                captured_cyphers.append(mock_tx.run.call_args)
            return result

        mock_session = AsyncMock()
        mock_session.execute_read = AsyncMock(side_effect=fake_execute_read)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)
        mock_driver = mock_neo4j_driver_with_session(mock_session)

        state = _make_state(
            base_query_state,
            query="what does svc-a call?",
            authorization=_jwt_header({"team": "platform", "namespace": "production", "role": "viewer"}),
        )

        with patch(
            "orchestrator.app.query_engine._get_neo4j_driver",
            return_value=mock_driver,
        ):
            result = await single_hop_retrieve(state)

        assert len(captured_cyphers) >= 2
        hop_cypher_call = captured_cyphers[1]
        hop_cypher = hop_cypher_call[0][0]
        hop_kwargs = hop_cypher_call[1]
        assert "acl_team" in hop_kwargs
        assert hop_kwargs["acl_team"] == "platform"


class TestHybridRetrieveAppliesACL:
    @pytest.mark.asyncio
    @patch.dict("os.environ", {"AUTH_TOKEN_SECRET": _TEST_SECRET})
    async def test_acl_injected_into_aggregation_cypher(self, base_query_state):
        from orchestrator.app.query_engine import hybrid_retrieve

        mock_vector_session = AsyncMock()
        mock_vector_session.execute_read = AsyncMock(
            return_value=[{"name": "auth-service", "score": 0.9}]
        )
        mock_vector_session.__aenter__ = AsyncMock(return_value=mock_vector_session)
        mock_vector_session.__aexit__ = AsyncMock(return_value=False)

        mock_agg_session = AsyncMock()
        mock_agg_session.execute_read = AsyncMock(
            return_value=[{"service": "auth-service", "dep_count": 5}]
        )
        mock_agg_session.__aenter__ = AsyncMock(return_value=mock_agg_session)
        mock_agg_session.__aexit__ = AsyncMock(return_value=False)

        mock_driver = MagicMock()
        mock_driver.session = MagicMock(
            side_effect=[mock_vector_session, mock_agg_session]
        )
        mock_driver.close = AsyncMock()

        state = _make_state(
            base_query_state,
            query="most critical services",
            authorization=_jwt_header({"team": "data", "namespace": "staging", "role": "viewer"}),
        )

        with patch(
            "orchestrator.app.query_engine._get_neo4j_driver",
            return_value=mock_driver,
        ), patch(
            "orchestrator.app.query_engine._generate_cypher",
            new_callable=AsyncMock,
            return_value="MATCH (n:Service)-[:CALLS*]->(t) RETURN n.name, count(t)",
        ):
            result = await hybrid_retrieve(state)

        agg_tx_func = mock_agg_session.execute_read.call_args[0][0]
        mock_tx = AsyncMock()
        mock_run_result = AsyncMock()
        mock_run_result.data = AsyncMock(return_value=[])
        mock_tx.run = AsyncMock(return_value=mock_run_result)
        await agg_tx_func(mock_tx)

        called_cypher = mock_tx.run.call_args[0][0]
        assert "WHERE" in called_cypher
