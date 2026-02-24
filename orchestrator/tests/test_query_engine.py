from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.app.cypher_validator import CypherValidationError
from orchestrator.app.query_models import QueryComplexity
from orchestrator.tests.conftest import mock_neo4j_driver_with_session


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


class TestClassifyNode:
    def test_sets_entity_lookup(self, base_query_state):
        from orchestrator.app.query_engine import classify_query_node

        state = _make_state(base_query_state, query="What language is auth-service?")
        result = classify_query_node(state)
        assert result["complexity"] == QueryComplexity.ENTITY_LOOKUP
        assert result["retrieval_path"] == "vector"

    def test_sets_multi_hop(self, base_query_state):
        from orchestrator.app.query_engine import classify_query_node

        state = _make_state(base_query_state, query="What is the blast radius if auth fails?")
        result = classify_query_node(state)
        assert result["complexity"] == QueryComplexity.MULTI_HOP
        assert result["retrieval_path"] == "cypher"


class TestRouteQuery:
    def test_routes_entity_lookup_to_vector(self, base_query_state):
        from orchestrator.app.query_engine import route_query

        state = _make_state(base_query_state, retrieval_path="vector")
        assert route_query(state) == "vector_retrieve"

    def test_routes_single_hop_to_single_hop(self, base_query_state):
        from orchestrator.app.query_engine import route_query

        state = _make_state(base_query_state, retrieval_path="single_hop")
        assert route_query(state) == "single_hop_retrieve"

    def test_routes_multi_hop_to_cypher(self, base_query_state):
        from orchestrator.app.query_engine import route_query

        state = _make_state(base_query_state, retrieval_path="cypher")
        assert route_query(state) == "cypher_retrieve"

    def test_routes_aggregate_to_hybrid(self, base_query_state):
        from orchestrator.app.query_engine import route_query

        state = _make_state(base_query_state, retrieval_path="hybrid")
        assert route_query(state) == "hybrid_retrieve"


class TestVectorRetrieve:
    @pytest.mark.asyncio
    async def test_returns_candidates_from_neo4j(self, base_query_state):
        from orchestrator.app.query_engine import vector_retrieve

        mock_records = [
            {"name": "auth-service", "language": "go", "score": 0.95},
            {"name": "user-service", "language": "python", "score": 0.82},
        ]
        mock_session = _make_neo4j_session(run_return=mock_records)
        mock_driver = mock_neo4j_driver_with_session(mock_session)

        state = _make_state(
            base_query_state,
            query="What language is auth-service?",
            retrieval_path="vector",
        )

        with patch(
            "orchestrator.app.query_engine._get_neo4j_driver",
            return_value=mock_driver,
        ):
            result = await vector_retrieve(state)

        assert len(result["candidates"]) == 2
        assert result["candidates"][0]["name"] == "auth-service"

    @pytest.mark.asyncio
    async def test_respects_max_results(self, base_query_state):
        from orchestrator.app.query_engine import vector_retrieve

        mock_session = _make_neo4j_session(run_return=[{"name": "svc", "score": 0.9}])
        mock_driver = mock_neo4j_driver_with_session(mock_session)

        state = _make_state(
            base_query_state,
            query="auth-service",
            max_results=5,
            retrieval_path="vector",
        )

        with patch(
            "orchestrator.app.query_engine._get_neo4j_driver",
            return_value=mock_driver,
        ):
            await vector_retrieve(state)

        mock_session.execute_read.assert_called_once()


class TestSingleHopRetrieve:
    @pytest.mark.asyncio
    async def test_returns_vector_and_one_hop_neighbors(self, base_query_state):
        from orchestrator.app.query_engine import single_hop_retrieve

        mock_session = _make_neo4j_session(
            run_side_effect=[
                [{"name": "order-service", "score": 0.95}],
                [{"source": "order-service", "rel": "PRODUCES", "target": "orders-topic"}],
            ]
        )
        mock_driver = mock_neo4j_driver_with_session(mock_session)

        state = _make_state(
            base_query_state,
            query="What topics does order-service produce to?",
            retrieval_path="single_hop",
        )

        with patch(
            "orchestrator.app.query_engine._get_neo4j_driver",
            return_value=mock_driver,
        ):
            result = await single_hop_retrieve(state)

        assert len(result["candidates"]) == 1
        assert result["candidates"][0]["name"] == "order-service"
        assert len(result["cypher_results"]) == 1
        assert result["cypher_results"][0]["target"] == "orders-topic"
        assert mock_session.execute_read.call_count == 2

    @pytest.mark.asyncio
    async def test_hop_query_uses_vector_names(self, base_query_state):
        from orchestrator.app.query_engine import single_hop_retrieve

        mock_session = _make_neo4j_session(
            run_side_effect=[
                [{"name": "svc-a", "score": 0.9}, {"name": "svc-b", "score": 0.8}],
                [{"source": "svc-a", "rel": "CALLS", "target": "svc-c"}],
            ]
        )
        mock_driver = mock_neo4j_driver_with_session(mock_session)

        state = _make_state(
            base_query_state,
            query="What does svc-a call?",
            retrieval_path="single_hop",
        )

        with patch(
            "orchestrator.app.query_engine._get_neo4j_driver",
            return_value=mock_driver,
        ):
            result = await single_hop_retrieve(state)

        assert mock_session.execute_read.call_count == 2
        assert result["cypher_results"] == [
            {"source": "svc-a", "rel": "CALLS", "target": "svc-c"}
        ]


class TestCypherRetrieve:
    @pytest.mark.asyncio
    async def test_generates_and_executes_cypher(self, base_query_state):
        from orchestrator.app.query_engine import cypher_retrieve

        mock_session = _make_neo4j_session(
            run_return=[{"source": "auth", "target": "user", "rel": "CALLS"}]
        )
        mock_driver = mock_neo4j_driver_with_session(mock_session)

        state = _make_state(
            base_query_state,
            query="What is the blast radius if auth fails?",
            retrieval_path="cypher",
        )

        with patch(
            "orchestrator.app.query_engine._get_neo4j_driver",
            return_value=mock_driver,
        ), patch(
            "orchestrator.app.query_engine._generate_cypher",
            new_callable=AsyncMock,
            return_value="MATCH (s:Service)-[:CALLS*]->(t) RETURN s, t",
        ), patch(
            "orchestrator.app.query_engine._try_template_match",
            new_callable=AsyncMock,
            return_value=None,
        ), patch(
            "orchestrator.app.query_engine.dynamic_cypher_allowed",
            return_value=True,
        ):
            result = await cypher_retrieve(state)

        assert len(result["cypher_results"]) == 1
        assert result["iteration_count"] == 1
        assert result["cypher_query"] != ""
        mock_session.execute_read.assert_called_once()

    @pytest.mark.asyncio
    async def test_iterates_on_empty_results_up_to_max(self, base_query_state):
        from orchestrator.app.query_engine import (
            MAX_CYPHER_ITERATIONS,
            cypher_retrieve,
        )

        mock_session = _make_neo4j_session(run_return=[])
        mock_driver = mock_neo4j_driver_with_session(mock_session)
        generate_mock = AsyncMock(return_value="MATCH (n) RETURN n")

        state = _make_state(
            base_query_state,
            query="blast radius if auth fails?",
            retrieval_path="cypher",
        )

        with patch(
            "orchestrator.app.query_engine._get_neo4j_driver",
            return_value=mock_driver,
        ), patch(
            "orchestrator.app.query_engine._generate_cypher",
            generate_mock,
        ), patch(
            "orchestrator.app.query_engine.dynamic_cypher_allowed",
            return_value=True,
        ):
            result = await cypher_retrieve(state)

        assert generate_mock.call_count == MAX_CYPHER_ITERATIONS
        assert result["iteration_count"] == MAX_CYPHER_ITERATIONS
        assert result["cypher_results"] == []

    @pytest.mark.asyncio
    async def test_stops_iterating_when_results_found(self, base_query_state):
        from orchestrator.app.query_engine import cypher_retrieve

        mock_session = _make_neo4j_session(
            run_side_effect=[[], [{"node": "auth"}]]
        )
        mock_driver = mock_neo4j_driver_with_session(mock_session)

        cyphers = iter([
            "MATCH (n) RETURN n",
            "MATCH (s:Service)-[:CALLS]->(t) RETURN s, t",
        ])
        generate_mock = AsyncMock(side_effect=lambda *a, **kw: next(cyphers))

        state = _make_state(
            base_query_state,
            query="blast radius if auth fails?",
            retrieval_path="cypher",
        )

        with patch(
            "orchestrator.app.query_engine._get_neo4j_driver",
            return_value=mock_driver,
        ), patch(
            "orchestrator.app.query_engine._generate_cypher",
            generate_mock,
        ), patch(
            "orchestrator.app.query_engine._try_template_match",
            new_callable=AsyncMock,
            return_value=None,
        ), patch(
            "orchestrator.app.query_engine.dynamic_cypher_allowed",
            return_value=True,
        ):
            result = await cypher_retrieve(state)

        assert generate_mock.call_count == 2
        assert result["iteration_count"] == 2
        assert len(result["cypher_results"]) == 1


class TestHybridRetrieve:
    @pytest.mark.asyncio
    async def test_prefilters_then_aggregates(self, base_query_state):
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
            query="Most critical services by dependency count",
            retrieval_path="hybrid",
        )

        with patch(
            "orchestrator.app.query_engine._get_neo4j_driver",
            return_value=mock_driver,
        ), patch(
            "orchestrator.app.query_engine._generate_cypher",
            new_callable=AsyncMock,
            return_value="MATCH (s:Service)-[:CALLS*]->(t) RETURN s.name, count(t)",
        ):
            result = await hybrid_retrieve(state)

        assert len(result["candidates"]) > 0
        assert len(result["cypher_results"]) > 0

    @pytest.mark.asyncio
    async def test_llm_call_outside_session_scope(self, base_query_state):
        from orchestrator.app.query_engine import hybrid_retrieve

        session_entry_order: list = []

        mock_vector_session = AsyncMock()
        mock_vector_session.execute_read = AsyncMock(
            return_value=[{"name": "svc-a", "score": 0.8}]
        )
        mock_vector_session.__aenter__ = AsyncMock(return_value=mock_vector_session)

        async def vector_exit(*_args):
            session_entry_order.append("vector_session_closed")
            return False

        mock_vector_session.__aexit__ = vector_exit

        mock_agg_session = AsyncMock()
        mock_agg_session.execute_read = AsyncMock(return_value=[{"total": 3}])
        mock_agg_session.__aenter__ = AsyncMock(return_value=mock_agg_session)
        mock_agg_session.__aexit__ = AsyncMock(return_value=False)

        mock_driver = MagicMock()
        mock_driver.session = MagicMock(
            side_effect=[mock_vector_session, mock_agg_session]
        )
        mock_driver.close = AsyncMock()

        async def fake_generate_cypher(*_args, **_kwargs):
            session_entry_order.append("llm_called")
            return "MATCH (n) RETURN n"

        state = _make_state(
            base_query_state,
            query="aggregate query",
            retrieval_path="hybrid",
        )

        with patch(
            "orchestrator.app.query_engine._get_neo4j_driver",
            return_value=mock_driver,
        ), patch(
            "orchestrator.app.query_engine._generate_cypher",
            side_effect=fake_generate_cypher,
        ):
            await hybrid_retrieve(state)

        assert session_entry_order.index("vector_session_closed") < \
            session_entry_order.index("llm_called")


class TestSynthesize:
    @pytest.mark.asyncio
    async def test_produces_answer_from_candidates(self, base_query_state):
        from orchestrator.app.query_engine import synthesize_answer

        state = _make_state(
            base_query_state,
            query="What language is auth-service?",
            retrieval_path="vector",
            candidates=[{"name": "auth-service", "language": "go", "score": 0.95}],
        )

        with patch(
            "orchestrator.app.query_engine._llm_synthesize",
            new_callable=AsyncMock,
            return_value="auth-service is written in Go.",
        ):
            result = await synthesize_answer(state)

        assert result["answer"] != ""
        assert len(result["sources"]) > 0

    @pytest.mark.asyncio
    async def test_returns_fallback_when_context_empty(self, base_query_state):
        from orchestrator.app.query_engine import synthesize_answer

        state = _make_state(
            base_query_state,
            query="What port does ghost-service listen on?",
            retrieval_path="vector",
        )
        result = await synthesize_answer(state)
        assert result["answer"] == "No relevant information found in the knowledge graph."
        assert result["sources"] == []

    @pytest.mark.asyncio
    async def test_includes_cypher_results_as_sources(self, base_query_state):
        from orchestrator.app.query_engine import synthesize_answer

        state = _make_state(
            base_query_state,
            query="blast radius of auth failure",
            retrieval_path="cypher",
            cypher_query="MATCH ...",
            cypher_results=[
                {"source": "auth", "target": "user"},
                {"source": "auth", "target": "order"},
            ],
            iteration_count=1,
        )

        with patch(
            "orchestrator.app.query_engine._llm_synthesize",
            new_callable=AsyncMock,
            return_value="If auth fails, user and order services are affected.",
        ):
            result = await synthesize_answer(state)

        assert result["answer"] != ""
        assert len(result["sources"]) == 2


class TestReadTransactions:
    @pytest.mark.asyncio
    async def test_vector_retrieve_uses_execute_read(self, base_query_state):
        from orchestrator.app.query_engine import vector_retrieve

        mock_session = AsyncMock()
        mock_read_result = MagicMock()
        mock_read_result.data.return_value = [{"name": "svc", "score": 0.9}]
        mock_session.execute_read = AsyncMock(return_value=mock_read_result.data())
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)
        mock_driver = mock_neo4j_driver_with_session(mock_session)

        state = _make_state(base_query_state, query="auth-service")

        with patch(
            "orchestrator.app.query_engine._get_neo4j_driver",
            return_value=mock_driver,
        ):
            await vector_retrieve(state)

        mock_session.execute_read.assert_called_once()
        assert not mock_session.run.called

    @pytest.mark.asyncio
    async def test_single_hop_uses_execute_read(self, base_query_state):
        from orchestrator.app.query_engine import single_hop_retrieve

        mock_session = AsyncMock()
        mock_session.execute_read = AsyncMock(
            side_effect=[
                [{"name": "svc", "score": 0.9}],
                [{"source": "svc", "rel": "CALLS", "target": "other"}],
            ]
        )
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)
        mock_driver = mock_neo4j_driver_with_session(mock_session)

        state = _make_state(
            base_query_state, query="what does svc call?"
        )

        with patch(
            "orchestrator.app.query_engine._get_neo4j_driver",
            return_value=mock_driver,
        ):
            await single_hop_retrieve(state)

        assert mock_session.execute_read.call_count == 2
        assert not mock_session.run.called

    @pytest.mark.asyncio
    async def test_cypher_retrieve_uses_execute_read(self, base_query_state):
        from orchestrator.app.query_engine import cypher_retrieve

        mock_session = AsyncMock()
        mock_session.execute_read = AsyncMock(
            return_value=[{"node": "auth"}]
        )
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)
        mock_driver = mock_neo4j_driver_with_session(mock_session)

        state = _make_state(
            base_query_state, query="blast radius if auth fails?"
        )

        with patch(
            "orchestrator.app.query_engine._get_neo4j_driver",
            return_value=mock_driver,
        ), patch(
            "orchestrator.app.query_engine._generate_cypher",
            new_callable=AsyncMock,
            return_value="MATCH (s:Service)-[:CALLS*]->(t) RETURN s, t",
        ):
            await cypher_retrieve(state)

        mock_session.execute_read.assert_called()
        assert not mock_session.run.called

    @pytest.mark.asyncio
    async def test_hybrid_retrieve_uses_execute_read(self, base_query_state):
        from orchestrator.app.query_engine import hybrid_retrieve

        mock_session = AsyncMock()
        call_count = [0]

        async def fake_execute_read(tx_func, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return [{"name": "svc", "score": 0.9}]
            return [{"total": 3}]

        mock_session.execute_read = AsyncMock(side_effect=fake_execute_read)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)
        mock_driver = mock_neo4j_driver_with_session(mock_session)

        state = _make_state(
            base_query_state, query="most critical services"
        )

        with patch(
            "orchestrator.app.query_engine._get_neo4j_driver",
            return_value=mock_driver,
        ), patch(
            "orchestrator.app.query_engine._generate_cypher",
            new_callable=AsyncMock,
            return_value="MATCH (n) RETURN count(n)",
        ):
            await hybrid_retrieve(state)

        assert mock_session.execute_read.call_count >= 2
        assert not mock_session.run.called


class TestCypherValidation:
    @pytest.mark.asyncio
    async def test_cypher_retrieve_rejects_write_query(self, base_query_state):
        from orchestrator.app.query_engine import cypher_retrieve

        mock_session = AsyncMock()
        mock_session.execute_read = AsyncMock(return_value=[])
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)
        mock_driver = mock_neo4j_driver_with_session(mock_session)

        state = _make_state(
            base_query_state, query="delete everything"
        )

        with patch(
            "orchestrator.app.query_engine._get_neo4j_driver",
            return_value=mock_driver,
        ), patch(
            "orchestrator.app.query_engine._generate_cypher",
            new_callable=AsyncMock,
            return_value="MATCH (n) DETACH DELETE n",
        ):
            result = await cypher_retrieve(state)

        assert result["cypher_results"] == []
        assert not mock_session.execute_read.called

    @pytest.mark.asyncio
    async def test_hybrid_retrieve_rejects_write_query(self, base_query_state):
        from orchestrator.app.query_engine import hybrid_retrieve

        mock_session = AsyncMock()
        mock_session.execute_read = AsyncMock(
            return_value=[{"name": "svc", "score": 0.9}]
        )
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)
        mock_driver = mock_neo4j_driver_with_session(mock_session)

        state = _make_state(
            base_query_state, query="most critical services"
        )

        with patch(
            "orchestrator.app.query_engine._get_neo4j_driver",
            return_value=mock_driver,
        ), patch(
            "orchestrator.app.query_engine._generate_cypher",
            new_callable=AsyncMock,
            return_value="MATCH (n) DETACH DELETE n",
        ):
            result = await hybrid_retrieve(state)

        assert result["cypher_results"] == []


class TestBuildAclFilterRequireTokens:
    def test_raises_when_require_tokens_true_and_secret_empty(self, base_query_state):
        from orchestrator.app.access_control import AuthConfigurationError
        from orchestrator.app.query_engine import _build_acl_filter

        state = {**base_query_state, "authorization": "Bearer some.token"}
        fake_auth = MagicMock()
        fake_auth.require_tokens = True
        fake_auth.token_secret = ""

        with patch(
            "orchestrator.app.query_engine.AuthConfig.from_env",
            return_value=fake_auth,
        ), pytest.raises(AuthConfigurationError):
            _build_acl_filter(state)

    def test_no_error_when_require_tokens_false_and_secret_empty(self, base_query_state):
        from orchestrator.app.query_engine import _build_acl_filter

        state = {**base_query_state, "authorization": ""}
        fake_auth = MagicMock()
        fake_auth.require_tokens = False
        fake_auth.token_secret = ""

        with patch(
            "orchestrator.app.query_engine.AuthConfig.from_env",
            return_value=fake_auth,
        ):
            result = _build_acl_filter(state)

        assert result is not None

    def test_no_error_when_require_tokens_true_and_secret_set(self, base_query_state):
        from orchestrator.app.access_control import sign_token
        from orchestrator.app.query_engine import _build_acl_filter

        secret = "test-secret-key"
        token = sign_token({"team": "infra", "namespace": "default", "role": "viewer"}, secret)
        state = {**base_query_state, "authorization": f"Bearer {token}"}
        fake_auth = MagicMock()
        fake_auth.require_tokens = True
        fake_auth.token_secret = secret

        with patch(
            "orchestrator.app.query_engine.AuthConfig.from_env",
            return_value=fake_auth,
        ):
            result = _build_acl_filter(state)

        assert result is not None


class TestCypherSandboxWiring:
    @pytest.mark.asyncio
    async def test_cypher_retrieve_injects_limit(self, base_query_state):
        from orchestrator.app.query_engine import cypher_retrieve

        mock_session = _make_neo4j_session(
            run_return=[{"node": "auth"}]
        )
        mock_driver = mock_neo4j_driver_with_session(mock_session)

        executed_queries: list = []
        original_execute_read = mock_session.execute_read

        async def capture_execute_read(tx_func, **kwargs):
            class FakeTx:
                async def run(self, query, **params):
                    executed_queries.append(query)
                    mock_result = AsyncMock()
                    mock_result.data = AsyncMock(
                        return_value=[{"node": "auth"}]
                    )
                    return mock_result
            return await tx_func(FakeTx())

        mock_session.execute_read = AsyncMock(side_effect=capture_execute_read)

        state = _make_state(
            base_query_state,
            query="What is the blast radius if auth fails?",
            retrieval_path="cypher",
        )

        with patch(
            "orchestrator.app.query_engine._get_neo4j_driver",
            return_value=mock_driver,
        ), patch(
            "orchestrator.app.query_engine._generate_cypher",
            new_callable=AsyncMock,
            return_value="MATCH (s:Service)-[:CALLS*]->(t) RETURN s, t",
        ):
            result = await cypher_retrieve(state)

        assert len(executed_queries) >= 1
        assert "LIMIT" in executed_queries[0].upper()

    @pytest.mark.asyncio
    async def test_hybrid_retrieve_injects_limit_on_aggregation(
        self, base_query_state
    ):
        from orchestrator.app.query_engine import hybrid_retrieve

        mock_vector_session = AsyncMock()
        mock_vector_session.execute_read = AsyncMock(
            return_value=[{"name": "auth-service", "score": 0.9}]
        )
        mock_vector_session.__aenter__ = AsyncMock(
            return_value=mock_vector_session
        )
        mock_vector_session.__aexit__ = AsyncMock(return_value=False)

        executed_agg_queries: list = []

        async def capture_agg_execute_read(tx_func, **kwargs):
            class FakeTx:
                async def run(self, query, **params):
                    executed_agg_queries.append(query)
                    mock_result = AsyncMock()
                    mock_result.data = AsyncMock(
                        return_value=[{"total": 3}]
                    )
                    return mock_result
            return await tx_func(FakeTx())

        mock_agg_session = AsyncMock()
        mock_agg_session.execute_read = AsyncMock(
            side_effect=capture_agg_execute_read
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
            query="Most critical services by dependency count",
            retrieval_path="hybrid",
        )

        with patch(
            "orchestrator.app.query_engine._get_neo4j_driver",
            return_value=mock_driver,
        ), patch(
            "orchestrator.app.query_engine._generate_cypher",
            new_callable=AsyncMock,
            return_value="MATCH (s:Service)-[:CALLS*]->(t) RETURN s.name, count(t)",
        ):
            await hybrid_retrieve(state)

        assert len(executed_agg_queries) >= 1
        assert "LIMIT" in executed_agg_queries[0].upper()


class TestVectorStoreWiring:
    @pytest.mark.asyncio
    async def test_fetch_candidates_uses_vector_store(self, base_query_state):
        from orchestrator.app.query_engine import _fetch_candidates

        fake_embedding = [0.1] * 10
        mock_store = AsyncMock()
        mock_store.search = AsyncMock(return_value=[
            MagicMock(id="n1", score=0.95, metadata={"name": "auth-svc", "language": "go"}),
        ])

        mock_driver = MagicMock()
        state = _make_state(base_query_state, query="auth-svc info")

        with patch(
            "orchestrator.app.query_engine._VECTOR_STORE", mock_store,
        ), patch(
            "orchestrator.app.query_engine._embed_query",
            new_callable=AsyncMock, return_value=fake_embedding,
        ):
            result = await _fetch_candidates(mock_driver, state)

        mock_store.search.assert_called_once()
        assert result[0]["name"] == "auth-svc"
        assert result[0]["score"] == 0.95

    @pytest.mark.asyncio
    async def test_fetch_candidates_tenant_scoped_search(self, base_query_state):
        from orchestrator.app.query_engine import _fetch_candidates

        fake_embedding = [0.1] * 10
        mock_store = AsyncMock()
        mock_store.search_with_tenant = AsyncMock(return_value=[
            MagicMock(id="n1", score=0.9, metadata={"name": "svc-a"}),
        ])

        mock_driver = MagicMock()
        state = _make_state(base_query_state, query="svc-a", tenant_id="acme")

        with patch(
            "orchestrator.app.query_engine._VECTOR_STORE", mock_store,
        ), patch(
            "orchestrator.app.query_engine._embed_query",
            new_callable=AsyncMock, return_value=fake_embedding,
        ):
            result = await _fetch_candidates(mock_driver, state)

        mock_store.search_with_tenant.assert_called_once()

    @pytest.mark.asyncio
    async def test_fetch_candidates_falls_back_to_neo4j_fulltext(self, base_query_state):
        from orchestrator.app.query_engine import _fetch_candidates

        mock_session = _make_neo4j_session(run_return=[{"name": "svc", "score": 0.8}])
        mock_driver = mock_neo4j_driver_with_session(mock_session)
        state = _make_state(base_query_state, query="auth-service")

        with patch(
            "orchestrator.app.query_engine._embed_query",
            new_callable=AsyncMock, return_value=None,
        ):
            result = await _fetch_candidates(mock_driver, state)

        mock_session.execute_read.assert_called_once()
        assert result[0]["name"] == "svc"


class TestHopEdgeLimit:
    @pytest.mark.asyncio
    async def test_hop_cypher_contains_limit(self, base_query_state):
        from orchestrator.app.query_engine import single_hop_retrieve

        executed_queries: list = []

        async def capture_read(tx_func, **kwargs):
            class FakeTx:
                async def run(self, query, **params):
                    executed_queries.append(query)
                    mock_result = AsyncMock()
                    mock_result.data = AsyncMock(return_value=[])
                    return mock_result
            return await tx_func(FakeTx())

        mock_session = AsyncMock()
        mock_session.execute_read = AsyncMock(side_effect=[
            [{"name": "svc-a", "score": 0.9}],
            capture_read,
        ])

        async def side_effect_fn(tx_func_or_result, **kwargs):
            call_count = mock_session.execute_read.call_count
            if call_count == 1:
                return [{"name": "svc-a", "score": 0.9}]
            return await capture_read(tx_func_or_result, **kwargs)

        mock_session.execute_read = AsyncMock(side_effect=side_effect_fn)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)
        mock_driver = mock_neo4j_driver_with_session(mock_session)

        state = _make_state(base_query_state, query="what does svc-a call?")

        with patch(
            "orchestrator.app.query_engine._get_neo4j_driver",
            return_value=mock_driver,
        ):
            await single_hop_retrieve(state)

        hop_queries = [q for q in executed_queries if "MATCH" in q and "n.name" in q]
        assert hop_queries, "No hop query was captured"
        assert "LIMIT" in hop_queries[0].upper()

    def test_hop_limit_configurable_via_env(self):
        from orchestrator.app.query_engine import _get_hop_edge_limit

        with patch.dict("os.environ", {"HOP_EDGE_LIMIT": "100"}):
            assert _get_hop_edge_limit() == 100

    def test_hop_limit_defaults_to_500(self):
        from orchestrator.app.query_engine import _get_hop_edge_limit

        with patch.dict("os.environ", {}, clear=True):
            assert _get_hop_edge_limit() == 500


class TestAsyncEvaluation:
    @pytest.mark.asyncio
    async def test_evaluate_response_returns_immediately(self, base_query_state):
        from orchestrator.app.query_engine import evaluate_response

        eval_started = False

        original_evaluate = None

        def slow_evaluate(**kwargs):
            nonlocal eval_started
            eval_started = True
            import time
            time.sleep(0)
            from orchestrator.app.rag_evaluator import RelevanceScore
            return RelevanceScore(query="q", score=0.5, context_count=1, retrieval_path="vector")

        state = _make_state(
            base_query_state,
            query="test query",
            answer="test answer",
            sources=[{"name": "svc"}],
        )

        with patch(
            "orchestrator.app.query_engine.RAGEvalConfig.from_env",
            return_value=MagicMock(enable_evaluation=True, low_relevance_threshold=0.3),
        ), patch(
            "orchestrator.app.query_engine._embed_query",
            new_callable=AsyncMock,
            return_value=[0.1] * 10,
        ):
            result = await evaluate_response(state)

        assert "evaluation_score" in result

    @pytest.mark.asyncio
    async def test_evaluate_does_not_block_on_failure(self, base_query_state):
        from orchestrator.app.query_engine import evaluate_response

        state = _make_state(
            base_query_state,
            query="test query",
            answer="answer",
            sources=[],
        )

        with patch(
            "orchestrator.app.query_engine.RAGEvalConfig.from_env",
            return_value=MagicMock(enable_evaluation=True, low_relevance_threshold=0.3),
        ), patch(
            "orchestrator.app.query_engine._embed_query",
            new_callable=AsyncMock,
            side_effect=RuntimeError("embedding failed"),
        ):
            result = await evaluate_response(state)

        assert result["evaluation_score"] == -1.0


class TestNeo4jDriverTimeout:
    def test_pool_configured_with_timeout(self):
        from orchestrator.app import neo4j_pool
        from orchestrator.app.config import Neo4jConfig

        neo4j_pool._state["driver"] = None
        fake_config = Neo4jConfig(
            uri="bolt://test:7687",
            username="neo4j",
            password="test",
            query_timeout=42.0,
        )

        with patch(
            "orchestrator.app.neo4j_pool.Neo4jConfig.from_env",
            return_value=fake_config,
        ), patch(
            "orchestrator.app.neo4j_pool.AsyncGraphDatabase.driver",
            return_value=MagicMock(),
        ) as mock_driver_ctor:
            neo4j_pool.init_driver()

        call_kwargs = mock_driver_ctor.call_args.kwargs
        assert call_kwargs["max_transaction_retry_time"] == 42.0
        neo4j_pool._state["driver"] = None

    @pytest.mark.asyncio
    async def test_execute_read_receives_query_timeout(self, base_query_state):
        from orchestrator.app.query_engine import vector_retrieve

        mock_session = _make_neo4j_session(run_return=[{"name": "svc"}])
        mock_driver = mock_neo4j_driver_with_session(mock_session)
        state = _make_state(base_query_state)

        with patch(
            "orchestrator.app.query_engine._get_neo4j_driver",
            return_value=mock_driver,
        ), patch(
            "orchestrator.app.query_engine._embed_query",
            new_callable=AsyncMock,
            return_value=None,
        ), patch(
            "orchestrator.app.query_engine._get_query_timeout",
            return_value=25.0,
        ):
            await vector_retrieve(state)

        mock_session.execute_read.assert_awaited_once()
        call_kwargs = mock_session.execute_read.call_args.kwargs
        assert call_kwargs.get("timeout") == 25.0

    @pytest.mark.asyncio
    async def test_sandboxed_read_passes_timeout(self, base_query_state):
        from orchestrator.app.query_engine import _execute_sandboxed_read

        mock_session = AsyncMock()
        mock_session.execute_read = AsyncMock(return_value=[{"x": 1}])
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)
        mock_driver = MagicMock()
        mock_driver.session = MagicMock(return_value=mock_session)

        with patch(
            "orchestrator.app.query_engine._get_query_timeout",
            return_value=17.5,
        ):
            await _execute_sandboxed_read(
                mock_driver, "MATCH (n) RETURN n", {},
            )

        call_kwargs = mock_session.execute_read.call_args.kwargs
        assert call_kwargs.get("timeout") == 17.5
