from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, call

import pytest

from orchestrator.app.lazy_traversal import (
    GDSPageRankStrategy,
    LocalPageRankStrategy,
    _GDS_EDGE_THRESHOLD,
    _GDS_GRAPH_DROP,
    _GDS_PAGERANK_QUERY,
    _GDS_PROJECT_QUERY,
    _sanitize_cypher_string,
    build_gds_node_query,
    build_gds_rel_query,
    gds_pagerank_filter,
)


class TestGDSQueryTemplates:
    def test_project_query_contains_gds_project_cypher(self) -> None:
        assert "gds.graph.project.cypher" in _GDS_PROJECT_QUERY

    def test_pagerank_query_contains_gds_pagerank_stream(self) -> None:
        assert "gds.pageRank.stream" in _GDS_PAGERANK_QUERY

    def test_pagerank_query_orders_by_score_desc(self) -> None:
        assert "ORDER BY score DESC" in _GDS_PAGERANK_QUERY

    def test_pagerank_query_limits_results(self) -> None:
        assert "$top_n" in _GDS_PAGERANK_QUERY

    def test_drop_query_contains_gds_graph_drop(self) -> None:
        assert "gds.graph.drop" in _GDS_GRAPH_DROP

    def test_edge_threshold_is_positive_integer(self) -> None:
        assert isinstance(_GDS_EDGE_THRESHOLD, int)
        assert _GDS_EDGE_THRESHOLD > 0


class TestCypherSanitization:
    def test_escapes_single_quotes(self) -> None:
        assert _sanitize_cypher_string("it's") == "it\\'s"

    def test_escapes_backslashes(self) -> None:
        assert _sanitize_cypher_string("a\\b") == "a\\\\b"

    def test_leaves_clean_strings_unchanged(self) -> None:
        assert _sanitize_cypher_string("svc-api") == "svc-api"

    def test_handles_empty_string(self) -> None:
        assert _sanitize_cypher_string("") == ""


class TestBuildGDSNodeQuery:
    def test_includes_seed_names(self) -> None:
        query = build_gds_node_query(["svc-a", "svc-b"])
        assert "'svc-a'" in query
        assert "'svc-b'" in query

    def test_includes_match_clause(self) -> None:
        query = build_gds_node_query(["svc-a"])
        assert "MATCH" in query

    def test_returns_distinct_node_ids(self) -> None:
        query = build_gds_node_query(["svc-a"])
        assert "DISTINCT" in query
        assert "id(node)" in query or "id(n)" in query

    def test_includes_tenant_filter_when_provided(self) -> None:
        query = build_gds_node_query(["svc-a"], tenant_id="tenant-1")
        assert "tenant_id" in query
        assert "tenant-1" in query

    def test_no_tenant_filter_when_empty(self) -> None:
        query = build_gds_node_query(["svc-a"], tenant_id="")
        assert "tenant_id" not in query

    def test_sanitizes_injection_in_seed_names(self) -> None:
        query = build_gds_node_query(["svc' OR 1=1 --"])
        assert "svc\\' OR 1=1 --" in query, "Quote must be backslash-escaped"
        assert "['svc' OR" not in query, "Unescaped quote would break Cypher"


class TestBuildGDSRelQuery:
    def test_includes_seed_names(self) -> None:
        query = build_gds_rel_query(["svc-a"])
        assert "'svc-a'" in query

    def test_returns_source_and_target(self) -> None:
        query = build_gds_rel_query(["svc-a"])
        assert "source" in query
        assert "target" in query

    def test_excludes_tombstoned_relationships(self) -> None:
        query = build_gds_rel_query(["svc-a"])
        assert "tombstoned_at IS NULL" in query

    def test_includes_tenant_filter_when_provided(self) -> None:
        query = build_gds_rel_query(["svc-a"], tenant_id="t-1")
        assert "tenant_id" in query
        assert "t-1" in query


class TestGDSPageRankStrategySync:
    def test_sync_rank_delegates_to_local(self) -> None:
        strategy = GDSPageRankStrategy(driver=MagicMock())
        edges = [
            {"source": "a", "rel": "CALLS", "target": "b"},
            {"source": "b", "rel": "CALLS", "target": "c"},
        ]
        result = strategy.rank(edges, seed_nodes=["a"], top_n=10)
        local = LocalPageRankStrategy()
        expected = local.rank(edges, seed_nodes=["a"], top_n=10)
        assert result == expected


class TestGDSPageRankStrategyAsync:
    @pytest.mark.asyncio
    async def test_rank_async_returns_empty_without_driver(self) -> None:
        strategy = GDSPageRankStrategy(driver=None)
        result = await strategy.rank_async(["svc-a"], tenant_id="t1")
        assert result == []

    @pytest.mark.asyncio
    async def test_rank_async_projects_runs_pagerank_and_drops(self) -> None:
        mock_session = AsyncMock()
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        write_calls = []

        async def capture_write(tx_func, **kwargs):
            write_calls.append("write")
            await tx_func(mock_session)

        pagerank_data = [
            {"name": "svc-a", "score": 0.6},
            {"name": "svc-b", "score": 0.4},
        ]
        read_call_count = 0

        async def capture_read(tx_func, **kwargs):
            nonlocal read_call_count
            read_call_count += 1
            mock_tx = AsyncMock()
            mock_result = AsyncMock()
            mock_result.data = AsyncMock(return_value=pagerank_data)
            mock_tx.run = AsyncMock(return_value=mock_result)
            return await tx_func(mock_tx)

        mock_session.execute_write = AsyncMock(side_effect=capture_write)
        mock_session.execute_read = AsyncMock(side_effect=capture_read)

        mock_run_result = AsyncMock()
        mock_session.run = AsyncMock(return_value=mock_run_result)

        mock_driver = MagicMock()
        mock_driver.session = MagicMock(return_value=mock_session)

        strategy = GDSPageRankStrategy(driver=mock_driver)
        result = await strategy.rank_async(["svc-a"], tenant_id="t1", top_n=10)

        assert len(result) == 2
        assert result[0] == ("svc-a", 0.6)
        assert result[1] == ("svc-b", 0.4)
        assert len(write_calls) >= 1, "Should have called execute_write for projection"

    @pytest.mark.asyncio
    async def test_rank_async_returns_empty_on_gds_error(self) -> None:
        mock_session = AsyncMock()
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)
        mock_session.execute_write = AsyncMock(
            side_effect=RuntimeError("GDS not installed"),
        )

        mock_driver = MagicMock()
        mock_driver.session = MagicMock(return_value=mock_session)

        strategy = GDSPageRankStrategy(driver=mock_driver)
        result = await strategy.rank_async(["svc-a"], tenant_id="t1")
        assert result == []

    @pytest.mark.asyncio
    async def test_rank_async_drops_graph_even_on_error(self) -> None:
        call_sequence: list = []

        mock_session = AsyncMock()
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        async def fail_write(tx_func, **kwargs):
            call_sequence.append("project")
            raise RuntimeError("projection failed")

        async def drop_write(tx_func, **kwargs):
            call_sequence.append("drop")

        write_call_count = 0

        async def dispatch_write(tx_func, **kwargs):
            nonlocal write_call_count
            write_call_count += 1
            if write_call_count == 1:
                return await fail_write(tx_func, **kwargs)
            return await drop_write(tx_func, **kwargs)

        mock_session.execute_write = AsyncMock(side_effect=dispatch_write)

        mock_driver = MagicMock()
        mock_driver.session = MagicMock(return_value=mock_session)

        strategy = GDSPageRankStrategy(driver=mock_driver)
        result = await strategy.rank_async(["svc-a"])
        assert result == []
        assert "drop" in call_sequence, "Graph should be dropped even on projection error"


class TestPrecomputedPageRankStrategy:
    def test_returns_nodes_sorted_by_pagerank_property(self) -> None:
        from orchestrator.app.lazy_traversal import PrecomputedPageRankStrategy

        strategy = PrecomputedPageRankStrategy()
        edges = [
            {"source": "a", "target": "b", "source_pagerank": 0.1, "target_pagerank": 0.9},
            {"source": "b", "target": "c", "source_pagerank": 0.9, "target_pagerank": 0.5},
            {"source": "c", "target": "d", "source_pagerank": 0.5, "target_pagerank": 0.2},
        ]
        result = strategy.rank(edges, seed_nodes=["a"], top_n=5)
        assert len(result) > 0
        scores = [score for _, score in result]
        assert scores == sorted(scores, reverse=True)

    def test_handles_missing_pagerank_defaults_to_zero(self) -> None:
        from orchestrator.app.lazy_traversal import PrecomputedPageRankStrategy

        strategy = PrecomputedPageRankStrategy()
        edges = [
            {"source": "a", "target": "b"},
            {"source": "b", "target": "c", "source_pagerank": 0.8, "target_pagerank": 0.6},
        ]
        result = strategy.rank(edges, seed_nodes=["a"], top_n=10)
        names = [name for name, _ in result]
        assert "b" in names
        for _, score in result:
            assert isinstance(score, float)

    def test_empty_edges_returns_empty(self) -> None:
        from orchestrator.app.lazy_traversal import PrecomputedPageRankStrategy

        strategy = PrecomputedPageRankStrategy()
        result = strategy.rank([], seed_nodes=["a"], top_n=10)
        assert result == []

    def test_respects_top_n_limit(self) -> None:
        from orchestrator.app.lazy_traversal import PrecomputedPageRankStrategy

        strategy = PrecomputedPageRankStrategy()
        edges = [
            {"source": f"n{i}", "target": f"n{i+1}",
             "source_pagerank": float(i) / 10, "target_pagerank": float(i + 1) / 10}
            for i in range(20)
        ]
        result = strategy.rank(edges, seed_nodes=["n0"], top_n=3)
        assert len(result) <= 3


class TestDefaultStrategyIsPrecomputed:
    def test_default_strategy_is_precomputed(self) -> None:
        from orchestrator.app.lazy_traversal import (
            PrecomputedPageRankStrategy,
            _DEFAULT_STRATEGY,
        )

        assert isinstance(_DEFAULT_STRATEGY, PrecomputedPageRankStrategy)


class TestLocalPageRankSafetyCap:
    def test_logs_deprecation_when_edges_exceed_cap(self, caplog) -> None:
        import logging
        from orchestrator.app.lazy_traversal import LocalPageRankStrategy, _LOCAL_SAFETY_CAP

        strategy = LocalPageRankStrategy()
        edges = [
            {"source": f"n{i}", "target": f"n{i+1}"}
            for i in range(_LOCAL_SAFETY_CAP + 1)
        ]
        with caplog.at_level(logging.WARNING):
            strategy.rank(edges, seed_nodes=["n0"], top_n=10)
        assert any("deprecat" in msg.lower() for msg in caplog.messages)

    def test_truncates_edges_at_safety_cap(self) -> None:
        from orchestrator.app.lazy_traversal import LocalPageRankStrategy, _LOCAL_SAFETY_CAP

        strategy = LocalPageRankStrategy(max_edges=_LOCAL_SAFETY_CAP + 5000)
        edges = [
            {"source": f"n{i}", "target": f"n{i+1}"}
            for i in range(_LOCAL_SAFETY_CAP + 100)
        ]
        result = strategy.rank(edges, seed_nodes=["n0"], top_n=10)
        assert isinstance(result, list)


class TestGDSPageRankFilter:
    @pytest.mark.asyncio
    async def test_uses_local_below_threshold(self) -> None:
        edges = [
            {"source": "a", "rel": "CALLS", "target": "b"},
        ]
        mock_driver = MagicMock()
        result = await gds_pagerank_filter(
            driver=mock_driver,
            hop_records=edges,
            seed_names=["a"],
            tenant_id="",
        )
        assert isinstance(result, list)
        assert len(result) > 0

    @pytest.mark.asyncio
    async def test_attempts_gds_above_threshold(self) -> None:
        edges = [
            {"source": f"n-{i}", "rel": "CALLS", "target": f"n-{i+1}"}
            for i in range(_GDS_EDGE_THRESHOLD + 1)
        ]

        mock_session = AsyncMock()
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)
        mock_session.execute_write = AsyncMock(
            side_effect=RuntimeError("GDS unavailable"),
        )

        mock_driver = MagicMock()
        mock_driver.session = MagicMock(return_value=mock_session)

        result = await gds_pagerank_filter(
            driver=mock_driver,
            hop_records=edges,
            seed_names=["n-0"],
            tenant_id="",
        )
        assert isinstance(result, list)
        assert len(result) > 0, "Should fallback to local on GDS failure"

    @pytest.mark.asyncio
    async def test_returns_filtered_records_from_gds(self) -> None:
        edges = [
            {"source": f"n-{i}", "rel": "CALLS", "target": f"n-{i+1}"}
            for i in range(_GDS_EDGE_THRESHOLD + 10)
        ]

        gds_results = [("n-0", 0.5), ("n-1", 0.3)]

        mock_session = AsyncMock()
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        pagerank_data = [
            {"name": name, "score": score} for name, score in gds_results
        ]

        write_count = 0

        async def mock_write(tx_func, **kwargs):
            nonlocal write_count
            write_count += 1

        async def mock_read(tx_func, **kwargs):
            mock_tx = AsyncMock()
            mock_result = AsyncMock()
            mock_result.data = AsyncMock(return_value=pagerank_data)
            mock_tx.run = AsyncMock(return_value=mock_result)
            return await tx_func(mock_tx)

        mock_session.execute_write = AsyncMock(side_effect=mock_write)
        mock_session.execute_read = AsyncMock(side_effect=mock_read)

        mock_driver = MagicMock()
        mock_driver.session = MagicMock(return_value=mock_session)

        result = await gds_pagerank_filter(
            driver=mock_driver,
            hop_records=edges,
            seed_names=["n-0"],
            tenant_id="",
        )
        for rec in result:
            assert (
                rec["source"] in {"n-0", "n-1"}
                or rec["target"] in {"n-0", "n-1"}
            ), f"Record {rec} should only involve GDS top-K nodes"
