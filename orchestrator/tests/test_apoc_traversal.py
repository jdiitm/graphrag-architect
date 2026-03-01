from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from neo4j.exceptions import ClientError

from orchestrator.app.agentic_traversal import (
    TraversalConfig,
    TraversalStrategy,
    run_traversal,
)


_DEFAULT_ACL: dict = {"is_admin": True, "acl_team": "", "acl_namespaces": []}


class _AsyncRecordIterator:
    def __init__(self, records: list) -> None:
        self._records = list(records)
        self._index = 0

    def __aiter__(self) -> _AsyncRecordIterator:
        return self

    async def __anext__(self) -> dict:
        if self._index >= len(self._records):
            raise StopAsyncIteration
        record = self._records[self._index]
        self._index += 1
        return record


def _mock_session_with_records(records: list) -> AsyncMock:
    session = AsyncMock()
    session.run = AsyncMock(return_value=_AsyncRecordIterator(records))
    return session


def _mock_async_session_driver() -> tuple[MagicMock, AsyncMock]:
    driver = MagicMock()
    session = AsyncMock()
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=False)
    driver.session.return_value = session
    return driver, session


class TestApocPathExpansionCypher:
    @pytest.mark.asyncio
    async def test_sends_correct_cypher_with_apoc_expand_config(self) -> None:
        from orchestrator.app.agentic_traversal import apoc_path_expansion

        session = _mock_session_with_records([])
        await apoc_path_expansion(
            session, start_id="4:abc:0", max_hops=3, max_visited=500,
        )

        session.run.assert_called_once()
        query = session.run.call_args[0][0]
        assert "apoc.path.expandConfig" in query
        assert "$start_id" in query
        assert "$max_hops" in query
        assert "$max_visited" in query

        kwargs = session.run.call_args[1]
        assert kwargs["start_id"] == "4:abc:0"
        assert kwargs["max_hops"] == 3
        assert kwargs["max_visited"] == 500


class TestApocResultRecordStepCompat:
    @pytest.mark.asyncio
    async def test_returns_nodes_and_edges_for_record_step(self) -> None:
        from orchestrator.app.agentic_traversal import apoc_path_expansion

        records = [
            {
                "node_id": "4:xxx:1",
                "labels": ["Service"],
                "rel_id": "5:xxx:0",
                "rel_type": "DEPENDS_ON",
                "src": "4:xxx:0",
                "tgt": "4:xxx:1",
            },
        ]
        session = _mock_session_with_records(records)
        result = await apoc_path_expansion(session, start_id="4:xxx:0")

        assert "nodes" in result
        assert "edges" in result
        assert "4:xxx:1" in result["nodes"]
        assert result["nodes"]["4:xxx:1"] == ["Service"]
        assert len(result["edges"]) == 1
        assert result["edges"][0] == ("4:xxx:0", "4:xxx:1", "DEPENDS_ON")


class TestTraversalStrategyApocEnum:
    def test_apoc_enum_value_exists(self) -> None:
        assert TraversalStrategy.APOC.value == "apoc"
        assert TraversalStrategy("apoc") == TraversalStrategy.APOC


class TestApocStrategyRouting:
    @pytest.mark.asyncio
    async def test_run_traversal_routes_to_apoc_expansion(self) -> None:
        config = TraversalConfig(strategy=TraversalStrategy.APOC)
        driver, _ = _mock_async_session_driver()
        apoc_raw = {
            "nodes": {"4:n:1": ["Service"]},
            "edges": [("4:n:0", "4:n:1", "CALLS")],
        }

        with patch(
            "orchestrator.app.agentic_traversal.apoc_path_expansion",
            new_callable=AsyncMock,
            return_value=apoc_raw,
        ) as mock_apoc:
            result = await run_traversal(
                driver=driver,
                start_node_id="svc-a",
                tenant_id="t1",
                acl_params=_DEFAULT_ACL,
                config=config,
            )
            mock_apoc.assert_called_once()
            assert isinstance(result, list)


class TestAdaptiveApocFallback:
    @pytest.mark.asyncio
    async def test_adaptive_tries_apoc_first_falls_back_on_client_error(
        self,
    ) -> None:
        config = TraversalConfig(strategy=TraversalStrategy.ADAPTIVE)
        driver, _ = _mock_async_session_driver()
        bfs_result = [{"target_id": "svc-fallback"}]

        with (
            patch(
                "orchestrator.app.agentic_traversal._try_apoc_expansion",
                new_callable=AsyncMock,
                side_effect=ClientError("Procedure not found"),
            ) as mock_apoc,
            patch(
                "orchestrator.app.agentic_traversal._batched_bfs",
                new_callable=AsyncMock,
                return_value=bfs_result,
            ) as mock_bfs,
        ):
            result = await run_traversal(
                driver=driver,
                start_node_id="svc-a",
                tenant_id="t1",
                acl_params=_DEFAULT_ACL,
                config=config,
            )
            mock_apoc.assert_called_once()
            mock_bfs.assert_called_once()
            assert result == bfs_result


class TestApocEmptyResult:
    @pytest.mark.asyncio
    async def test_empty_start_returns_empty_path_set(self) -> None:
        from orchestrator.app.agentic_traversal import apoc_path_expansion

        session = _mock_session_with_records([])
        result = await apoc_path_expansion(
            session, start_id="4:nonexistent:0",
        )

        assert result["nodes"] == {}
        assert result["edges"] == []


class TestApocResultNodeIdsAndRelTypes:
    @pytest.mark.asyncio
    async def test_result_includes_node_ids_and_relationship_types(self) -> None:
        from orchestrator.app.agentic_traversal import apoc_path_expansion

        records = [
            {
                "node_id": "4:a:0",
                "labels": ["Pod"],
                "rel_id": "5:r:0",
                "rel_type": "RUNS_ON",
                "src": "4:a:0",
                "tgt": "4:b:0",
            },
            {
                "node_id": "4:b:0",
                "labels": ["Node"],
                "rel_id": "5:r:1",
                "rel_type": "HOSTS",
                "src": "4:b:0",
                "tgt": "4:c:0",
            },
            {
                "node_id": "4:c:0",
                "labels": ["Cluster"],
                "rel_id": None,
                "rel_type": None,
                "src": None,
                "tgt": None,
            },
        ]
        session = _mock_session_with_records(records)
        result = await apoc_path_expansion(session, start_id="4:a:0")

        assert "4:a:0" in result["nodes"]
        assert "4:b:0" in result["nodes"]
        assert "4:c:0" in result["nodes"]
        assert ("4:a:0", "4:b:0", "RUNS_ON") in result["edges"]
        assert ("4:b:0", "4:c:0", "HOSTS") in result["edges"]
        assert len(result["edges"]) == 2


class TestExistingStrategiesUnaffected:
    @pytest.mark.asyncio
    async def test_bounded_cypher_does_not_invoke_apoc(self) -> None:
        config = TraversalConfig(strategy=TraversalStrategy.BOUNDED_CYPHER)
        mock_driver = MagicMock()

        with (
            patch(
                "orchestrator.app.agentic_traversal.bounded_path_expansion",
                new_callable=AsyncMock,
                return_value=[],
            ),
            patch(
                "orchestrator.app.agentic_traversal.apoc_path_expansion",
                new_callable=AsyncMock,
            ) as mock_apoc,
        ):
            await run_traversal(
                driver=mock_driver,
                start_node_id="svc-a",
                tenant_id="t1",
                acl_params=_DEFAULT_ACL,
                config=config,
            )
            mock_apoc.assert_not_called()

    @pytest.mark.asyncio
    async def test_batched_bfs_does_not_invoke_apoc(self) -> None:
        config = TraversalConfig(strategy=TraversalStrategy.BATCHED_BFS)
        mock_driver = MagicMock()

        with (
            patch(
                "orchestrator.app.agentic_traversal._batched_bfs",
                new_callable=AsyncMock,
                return_value=[],
            ),
            patch(
                "orchestrator.app.agentic_traversal.apoc_path_expansion",
                new_callable=AsyncMock,
            ) as mock_apoc,
        ):
            await run_traversal(
                driver=mock_driver,
                start_node_id="svc-a",
                tenant_id="t1",
                acl_params=_DEFAULT_ACL,
                config=config,
            )
            mock_apoc.assert_not_called()


class TestTraversalStrategyEnvConfig:
    def test_apoc_env_var_selects_apoc_strategy(self) -> None:
        with patch.dict("os.environ", {"TRAVERSAL_STRATEGY": "apoc"}):
            config = TraversalConfig.from_env()
        assert config.strategy == TraversalStrategy.APOC
