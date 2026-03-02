from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.app.config import ConfigurationError


class TestInjectionHardBlockProductionEnforcement:

    def test_hard_block_enabled_returns_true_by_default(self) -> None:
        with patch.dict("os.environ", {}, clear=True):
            from orchestrator.app.query_engine import _injection_hard_block_enabled
            assert _injection_hard_block_enabled() is True

    def test_hard_block_can_be_disabled_in_dev(self) -> None:
        env = {
            "DEPLOYMENT_MODE": "dev",
            "INJECTION_HARD_BLOCK_ENABLED": "false",
        }
        with patch.dict("os.environ", env, clear=True):
            from orchestrator.app.query_engine import _injection_hard_block_enabled
            assert _injection_hard_block_enabled() is False

    def test_hard_block_cannot_be_disabled_in_production(self) -> None:
        env = {
            "DEPLOYMENT_MODE": "production",
            "INJECTION_HARD_BLOCK_ENABLED": "false",
        }
        with patch.dict("os.environ", env, clear=True):
            from orchestrator.app.query_engine import _injection_hard_block_enabled
            assert _injection_hard_block_enabled() is True, (
                "In production mode, prompt injection hard-block must always "
                "be enabled regardless of INJECTION_HARD_BLOCK_ENABLED env var. "
                "Allowing bypass enables tenant data exfiltration."
            )

    def test_hard_block_forced_in_production_even_with_zero(self) -> None:
        env = {
            "DEPLOYMENT_MODE": "production",
            "INJECTION_HARD_BLOCK_ENABLED": "0",
        }
        with patch.dict("os.environ", env, clear=True):
            from orchestrator.app.query_engine import _injection_hard_block_enabled
            assert _injection_hard_block_enabled() is True

    def test_hard_block_respects_env_in_non_production(self) -> None:
        env = {
            "DEPLOYMENT_MODE": "dev",
            "INJECTION_HARD_BLOCK_ENABLED": "true",
        }
        with patch.dict("os.environ", env, clear=True):
            from orchestrator.app.query_engine import _injection_hard_block_enabled
            assert _injection_hard_block_enabled() is True


class TestLocalASTProductionBlock:

    def test_local_ast_in_production_raises_configuration_error(self) -> None:
        env = {
            "DEPLOYMENT_MODE": "production",
            "AST_EXTRACTION_MODE": "local",
        }
        with patch.dict("os.environ", env, clear=True):
            from orchestrator.app.graph_builder import warn_if_local_ast_in_production
            with pytest.raises(ConfigurationError, match="CPU starvation"):
                warn_if_local_ast_in_production()

    def test_local_ast_in_dev_does_not_raise(self) -> None:
        env = {
            "DEPLOYMENT_MODE": "dev",
            "AST_EXTRACTION_MODE": "local",
        }
        with patch.dict("os.environ", env, clear=True):
            from orchestrator.app.graph_builder import warn_if_local_ast_in_production
            warn_if_local_ast_in_production()

    def test_go_ast_in_production_does_not_raise(self) -> None:
        env = {
            "DEPLOYMENT_MODE": "production",
            "AST_EXTRACTION_MODE": "go",
        }
        with patch.dict("os.environ", env, clear=True):
            from orchestrator.app.graph_builder import warn_if_local_ast_in_production
            warn_if_local_ast_in_production()

    def test_default_ast_mode_in_production_does_not_raise(self) -> None:
        env = {"DEPLOYMENT_MODE": "production"}
        with patch.dict("os.environ", env, clear=True):
            from orchestrator.app.graph_builder import warn_if_local_ast_in_production
            warn_if_local_ast_in_production()

    def test_local_ast_implicit_in_production_raises(self) -> None:
        env = {
            "DEPLOYMENT_MODE": "production",
            "AST_EXTRACTION_MODE": "local",
        }
        with patch.dict("os.environ", env, clear=True):
            from orchestrator.app.graph_builder import warn_if_local_ast_in_production
            with pytest.raises(ConfigurationError):
                warn_if_local_ast_in_production()


class TestQdrantSearchTimeout:

    @pytest.mark.asyncio
    async def test_qdrant_search_raises_on_timeout(self) -> None:
        from orchestrator.app.vector_store import QdrantVectorStore, VectorDegradedError

        mock_client = AsyncMock()

        async def slow_search(**kwargs):
            await asyncio.sleep(10.0)
            return []

        mock_client.search = slow_search

        store = QdrantVectorStore(
            url="http://localhost:6333",
            search_timeout_seconds=0.05,
        )
        store._client = mock_client

        with pytest.raises(VectorDegradedError, match="timed out"):
            await store.search("coll", [1.0, 0.0], limit=5)

    @pytest.mark.asyncio
    async def test_qdrant_search_with_tenant_raises_on_timeout(self) -> None:
        from orchestrator.app.vector_store import QdrantVectorStore, VectorDegradedError

        mock_client = AsyncMock()

        async def slow_search(**kwargs):
            await asyncio.sleep(10.0)
            return []

        mock_client.search = slow_search

        store = QdrantVectorStore(
            url="http://localhost:6333",
            search_timeout_seconds=0.05,
        )
        store._client = mock_client

        with pytest.raises(VectorDegradedError, match="timed out"):
            await store.search_with_tenant(
                "coll", [1.0, 0.0], tenant_id="t1", limit=5,
            )

    @pytest.mark.asyncio
    async def test_qdrant_search_completes_within_timeout(self) -> None:
        from orchestrator.app.vector_store import QdrantVectorStore

        mock_client = AsyncMock()
        mock_hit = MagicMock()
        mock_hit.id = "result-1"
        mock_hit.score = 0.95
        mock_hit.payload = {"name": "svc"}
        mock_client.search = AsyncMock(return_value=[mock_hit])

        store = QdrantVectorStore(
            url="http://localhost:6333",
            search_timeout_seconds=5.0,
        )
        store._client = mock_client

        results = await store.search("coll", [1.0, 0.0], limit=5)
        assert len(results) == 1
        assert results[0].id == "result-1"


class TestVectorDegradedErrorExists:

    def test_vector_degraded_error_importable(self) -> None:
        from orchestrator.app.vector_store import VectorDegradedError
        assert issubclass(VectorDegradedError, Exception)

    def test_vector_degraded_error_message(self) -> None:
        from orchestrator.app.vector_store import VectorDegradedError
        err = VectorDegradedError("search timed out after 5.0s")
        assert "timed out" in str(err)


class TestQueryEngineVectorFallback:

    @pytest.mark.asyncio
    async def test_vector_retrieve_falls_back_on_circuit_open(
        self, base_query_state,
    ) -> None:
        from orchestrator.app.circuit_breaker import CircuitOpenError
        from orchestrator.app.query_engine import vector_retrieve

        state = {**base_query_state, "query": "What is auth-service?"}

        with patch(
            "orchestrator.app.query_engine._embed_query",
            side_effect=CircuitOpenError("qdrant"),
        ), patch(
            "orchestrator.app.query_engine._neo4j_session",
        ) as mock_session_ctx:
            mock_driver = MagicMock()
            mock_session_ctx.return_value.__aenter__ = AsyncMock(
                return_value=mock_driver,
            )
            mock_session_ctx.return_value.__aexit__ = AsyncMock(return_value=False)

            result = await vector_retrieve(state)

        assert result.get("retrieval_degraded") is True
        assert "candidates" in result

    @pytest.mark.asyncio
    async def test_vector_retrieve_falls_back_on_vector_degraded(
        self, base_query_state,
    ) -> None:
        from orchestrator.app.query_engine import vector_retrieve
        from orchestrator.app.vector_store import VectorDegradedError

        state = {**base_query_state, "query": "What is auth-service?"}

        with patch(
            "orchestrator.app.query_engine._VECTOR_STORE",
        ) as mock_vs, patch(
            "orchestrator.app.query_engine._embed_query",
            return_value=[1.0, 0.0],
        ), patch(
            "orchestrator.app.query_engine._neo4j_session",
        ) as mock_session_ctx:
            mock_vs.search_with_tenant = AsyncMock(
                side_effect=VectorDegradedError("timed out"),
            )
            mock_vs.search = AsyncMock(
                side_effect=VectorDegradedError("timed out"),
            )
            mock_driver = MagicMock()
            mock_session_ctx.return_value.__aenter__ = AsyncMock(
                return_value=mock_driver,
            )
            mock_session_ctx.return_value.__aexit__ = AsyncMock(return_value=False)

            result = await vector_retrieve(state)

        assert result.get("retrieval_degraded") is True
        assert "candidates" in result
