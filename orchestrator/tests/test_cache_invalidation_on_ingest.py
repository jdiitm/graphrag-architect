from __future__ import annotations

from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.app.extraction_models import ServiceNode


_ENV_VARS = {
    "NEO4J_PASSWORD": "test",
    "NEO4J_URI": "bolt://localhost:7687",
    "GOOGLE_API_KEY": "test-key",
}


def _make_services(count: int) -> List[ServiceNode]:
    return [
        ServiceNode(
            id=f"svc-{i}", name=f"service-{i}", language="go",
            framework="gin", opentelemetry_enabled=False, confidence=1.0,
            tenant_id="test-tenant",
        )
        for i in range(count)
    ]


class TestCacheInvalidationOnCommit:

    @pytest.mark.asyncio
    async def test_successful_commit_invalidates_subgraph_cache(self) -> None:
        from orchestrator.app.graph_builder import commit_to_neo4j

        invalidation_calls: List[str] = []

        async def _tracking_commit(self_repo, entities):
            pass

        async def _tracking_prune(self_repo, current_ingestion_id="", max_age_hours=24):
            return 0, []

        mock_driver = MagicMock()
        mock_driver.close = AsyncMock()

        mock_vector_store = AsyncMock()
        mock_vector_store.delete = AsyncMock(return_value=0)

        original_invalidate = None

        def _track_invalidate():
            invalidation_calls.append("subgraph_cache")

        with (
            patch.dict("os.environ", _ENV_VARS),
            patch(
                "orchestrator.app.graph_builder.get_driver",
                return_value=mock_driver,
            ),
            patch(
                "orchestrator.app.neo4j_client.GraphRepository.commit_topology",
                _tracking_commit,
            ),
            patch(
                "orchestrator.app.neo4j_client.GraphRepository.prune_stale_edges",
                _tracking_prune,
            ),
            patch(
                "orchestrator.app.graph_builder.get_vector_store",
                return_value=mock_vector_store,
            ),
            patch(
                "orchestrator.app.graph_builder.invalidate_caches_after_ingest",
                new_callable=AsyncMock,
            ) as mock_invalidate,
        ):
            result = await commit_to_neo4j({"extracted_nodes": _make_services(2)})

        assert result["commit_status"] == "success"
        mock_invalidate.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_failed_commit_does_not_invalidate_caches(self) -> None:
        from orchestrator.app.graph_builder import commit_to_neo4j
        from neo4j.exceptions import Neo4jError

        async def _failing_commit(self_repo, entities):
            raise Neo4jError("Connection lost")

        mock_driver = MagicMock()
        mock_driver.close = AsyncMock()

        mock_vector_store = AsyncMock()
        mock_vector_store.delete = AsyncMock(return_value=0)

        with (
            patch.dict("os.environ", _ENV_VARS),
            patch(
                "orchestrator.app.graph_builder.get_driver",
                return_value=mock_driver,
            ),
            patch(
                "orchestrator.app.neo4j_client.GraphRepository.commit_topology",
                _failing_commit,
            ),
            patch(
                "orchestrator.app.graph_builder.get_vector_store",
                return_value=mock_vector_store,
            ),
            patch(
                "orchestrator.app.graph_builder.invalidate_caches_after_ingest",
                new_callable=AsyncMock,
            ) as mock_invalidate,
        ):
            result = await commit_to_neo4j({"extracted_nodes": _make_services(1)})

        assert result["commit_status"] == "failed"
        mock_invalidate.assert_not_awaited()


class TestRedisSubgraphCacheInvalidateAll:

    @pytest.mark.asyncio
    async def test_invalidate_all_removes_prefixed_keys_from_redis_l2(self) -> None:
        from orchestrator.app.subgraph_cache import RedisSubgraphCache

        mock_redis = AsyncMock()
        prefix = "graphrag:sgcache:"
        matching_keys = [f"{prefix}key1".encode()]
        mock_redis.scan = AsyncMock(return_value=(0, matching_keys))
        mock_redis.delete = AsyncMock()

        cache = RedisSubgraphCache.__new__(RedisSubgraphCache)
        from orchestrator.app.subgraph_cache import SubgraphCache
        cache._l1 = SubgraphCache(maxsize=10)
        cache._redis = mock_redis
        cache._ttl = 300
        cache._prefix = prefix
        cache._l1.put("key1", [{"data": "old"}])

        await cache.invalidate_all()

        assert cache._l1.stats().size == 0
        mock_redis.scan.assert_called()
        mock_redis.delete.assert_awaited_once_with(*matching_keys)
        mock_redis.flushdb.assert_not_called()
