from unittest.mock import AsyncMock, MagicMock, patch

import pytest

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


class TestPersonalizedPageRank:
    def test_hub_nodes_ranked_higher_than_leaves(self):
        from orchestrator.app.lazy_traversal import personalized_pagerank

        edges = [
            {"source": "hub", "rel": "CALLS", "target": "leaf-1"},
            {"source": "hub", "rel": "CALLS", "target": "leaf-2"},
            {"source": "hub", "rel": "CALLS", "target": "leaf-3"},
            {"source": "hub", "rel": "CALLS", "target": "leaf-4"},
            {"source": "hub", "rel": "CALLS", "target": "leaf-5"},
            {"source": "other", "rel": "CALLS", "target": "hub"},
            {"source": "seed", "rel": "CALLS", "target": "hub"},
            {"source": "seed", "rel": "CALLS", "target": "isolated"},
        ]
        ranked = personalized_pagerank(
            edges, seed_nodes=["seed"], iterations=30, damping=0.85, top_n=10,
        )
        names = [node for node, _score in ranked]
        hub_idx = names.index("hub")
        for leaf in ("leaf-1", "leaf-2", "leaf-3", "leaf-4", "leaf-5"):
            if leaf in names:
                assert hub_idx < names.index(leaf), (
                    f"Hub must rank higher than {leaf}"
                )

    def test_empty_graph_returns_empty(self):
        from orchestrator.app.lazy_traversal import personalized_pagerank

        result = personalized_pagerank([], seed_nodes=["x"], top_n=10)
        assert result == []

    def test_single_node_returns_that_node(self):
        from orchestrator.app.lazy_traversal import personalized_pagerank

        edges = [{"source": "A", "rel": "CALLS", "target": "B"}]
        result = personalized_pagerank(edges, seed_nodes=["A"], top_n=10)
        names = [node for node, _score in result]
        assert "A" in names

    def test_scores_sum_to_approximately_one(self):
        from orchestrator.app.lazy_traversal import personalized_pagerank

        edges = [
            {"source": "A", "rel": "CALLS", "target": "B"},
            {"source": "B", "rel": "CALLS", "target": "C"},
            {"source": "C", "rel": "CALLS", "target": "A"},
            {"source": "A", "rel": "CALLS", "target": "D"},
        ]
        ranked = personalized_pagerank(
            edges, seed_nodes=["A"], iterations=50, top_n=100,
        )
        total = sum(score for _node, score in ranked)
        assert abs(total - 1.0) < 0.05, f"Scores sum to {total}, expected ~1.0"


class TestSingleHopPPRIntegration:
    @pytest.mark.asyncio
    async def test_single_hop_uses_ppr_to_select_results(self, base_query_state):
        from orchestrator.app.query_engine import single_hop_retrieve

        hub_edges = [
            {"source": "svc-a", "rel": "CALLS", "target": f"dep-{i}"}
            for i in range(20)
        ]
        hub_edges.append(
            {"source": "svc-a", "rel": "CALLS", "target": "hub-node"},
        )
        for i in range(10):
            hub_edges.append(
                {"source": "hub-node", "rel": "CALLS", "target": f"hub-dep-{i}"},
            )

        mock_session = _make_neo4j_session(
            run_side_effect=[
                [{"name": "svc-a", "score": 0.95}],
                hub_edges,
            ]
        )
        mock_driver = mock_neo4j_driver_with_session(mock_session)

        state = _make_state(
            base_query_state,
            query="What does svc-a depend on?",
            retrieval_path="single_hop",
        )

        with patch(
            "orchestrator.app.query_engine._get_neo4j_driver",
            return_value=mock_driver,
        ):
            result = await single_hop_retrieve(state)

        returned_targets = {
            r["target"] for r in result["cypher_results"]
        }
        assert "hub-node" in returned_targets, (
            "PPR should keep hub-node since it's topologically important"
        )

    @pytest.mark.asyncio
    async def test_hop_edge_limit_still_caps_neo4j_query(self, base_query_state):
        from orchestrator.app.query_engine import single_hop_retrieve

        executed_queries: list = []

        async def capture_read(tx_func, **kwargs):
            class FakeTx:
                async def run(self, query, **params):
                    executed_queries.append((query, params))
                    mock_result = AsyncMock()
                    mock_result.data = AsyncMock(return_value=[])
                    return mock_result
            return await tx_func(FakeTx())

        call_count = 0

        async def side_effect_fn(tx_func_or_result, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return [{"name": "svc-a", "score": 0.9}]
            return await capture_read(tx_func_or_result, **kwargs)

        mock_session = AsyncMock()
        mock_session.execute_read = AsyncMock(side_effect=side_effect_fn)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)
        mock_driver = mock_neo4j_driver_with_session(mock_session)

        state = _make_state(base_query_state, query="what does svc-a call?")

        with patch(
            "orchestrator.app.query_engine._get_neo4j_driver",
            return_value=mock_driver,
        ), patch(
            "orchestrator.app.query_engine._get_hop_edge_limit",
            return_value=200,
        ):
            await single_hop_retrieve(state)

        hop_queries = [
            (q, p) for q, p in executed_queries
            if "MATCH" in q and "n.name" in q
        ]
        assert hop_queries, "No hop query was captured"
        query_text, params = hop_queries[0]
        assert "LIMIT" in query_text.upper(), (
            "HOP_EDGE_LIMIT must remain as LIMIT in the Cypher query"
        )
        assert params.get("hop_limit") == 200, (
            "hop_limit param must match HOP_EDGE_LIMIT value"
        )
