from __future__ import annotations

from typing import Any, Dict, List, Set
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.app.extraction_models import ServiceNode
from orchestrator.app.subgraph_cache import SubgraphCache


def _make_services(count: int, tenant_id: str = "test-tenant") -> List[ServiceNode]:
    return [
        ServiceNode(
            id=f"svc-{i}", name=f"service-{i}", language="go",
            framework="gin", opentelemetry_enabled=False, confidence=1.0,
            tenant_id=tenant_id,
        )
        for i in range(count)
    ]


_ENV_VARS = {
    "NEO4J_PASSWORD": "test",
    "NEO4J_URI": "bolt://localhost:7687",
    "GOOGLE_API_KEY": "test-key",
}


class TestInvalidateByNodesEvictsTaggedEntries:

    def test_invalidate_by_nodes_evicts_tagged_entries(self) -> None:
        cache = SubgraphCache(maxsize=256)
        cache.put("q1", [{"svc": "auth"}], node_ids={"node-A", "node-B"})
        cache.put("q2", [{"svc": "billing"}], node_ids={"node-C"})

        evicted = cache.invalidate_by_nodes({"node-A"})

        assert evicted == 1
        assert cache.get("q1") is None
        assert cache.get("q2") == [{"svc": "billing"}]


class TestInvalidateByNodesPreservesUnrelatedEntries:

    def test_invalidate_by_nodes_preserves_unrelated_entries(self) -> None:
        cache = SubgraphCache(maxsize=256)
        cache.put("q1", [{"svc": "auth"}], node_ids={"node-A"})
        cache.put("q2", [{"svc": "billing"}], node_ids={"node-B"})
        cache.put("q3", [{"svc": "gateway"}], node_ids={"node-C"})

        cache.invalidate_by_nodes({"node-A"})

        assert cache.get("q2") == [{"svc": "billing"}]
        assert cache.get("q3") == [{"svc": "gateway"}]


class TestInvalidateByNodesWithOverlappingTags:

    def test_overlapping_tags_evicts_entry(self) -> None:
        cache = SubgraphCache(maxsize=256)
        cache.put("q1", [{"svc": "combo"}], node_ids={"node-A", "node-B"})

        evicted = cache.invalidate_by_nodes({"node-A"})

        assert evicted == 1
        assert cache.get("q1") is None

    def test_multiple_entries_share_node(self) -> None:
        cache = SubgraphCache(maxsize=256)
        cache.put("q1", [{"svc": "auth"}], node_ids={"node-A", "node-B"})
        cache.put("q2", [{"svc": "billing"}], node_ids={"node-B", "node-C"})
        cache.put("q3", [{"svc": "gateway"}], node_ids={"node-D"})

        evicted = cache.invalidate_by_nodes({"node-B"})

        assert evicted == 2
        assert cache.get("q1") is None
        assert cache.get("q2") is None
        assert cache.get("q3") == [{"svc": "gateway"}]


class TestNodeTagsCleanedOnLruEviction:

    def test_node_tags_cleaned_on_lru_eviction(self) -> None:
        cache = SubgraphCache(maxsize=2)
        cache.put("q1", [{"svc": "old"}], node_ids={"node-X"})
        cache.put("q2", [{"svc": "mid"}], node_ids={"node-Y"})
        cache.put("q3", [{"svc": "new"}], node_ids={"node-Z"})

        assert "node-X" not in cache._node_tags or not cache._node_tags["node-X"]

        evicted = cache.invalidate_by_nodes({"node-X"})
        assert evicted == 0


class TestNodeTagsCleanedOnExplicitInvalidate:

    def test_invalidate_cleans_node_tags(self) -> None:
        cache = SubgraphCache(maxsize=256)
        cache.put("q1", [{"svc": "auth"}], node_ids={"node-A", "node-B"})

        cache.invalidate("q1")

        assert "node-A" not in cache._node_tags or not cache._node_tags["node-A"]
        assert "node-B" not in cache._node_tags or not cache._node_tags["node-B"]

    def test_invalidate_tenant_cleans_node_tags(self) -> None:
        cache = SubgraphCache(maxsize=256)
        cache.put("tenant_a:q1", [{"svc": "auth"}], node_ids={"node-A"})
        cache.put("tenant_a:q2", [{"svc": "billing"}], node_ids={"node-B"})
        cache.put("tenant_b:q1", [{"svc": "gateway"}], node_ids={"node-C"})

        cache.invalidate_tenant("tenant_a")

        assert "node-A" not in cache._node_tags or not cache._node_tags["node-A"]
        assert "node-B" not in cache._node_tags or not cache._node_tags["node-B"]
        assert cache._node_tags.get("node-C") == {"tenant_b:q1"}


class TestPutWithoutNodeIdsBackwardCompatible:

    def test_put_without_node_ids_works(self) -> None:
        cache = SubgraphCache(maxsize=256)
        cache.put("q1", [{"svc": "auth"}])

        assert cache.get("q1") == [{"svc": "auth"}]

    def test_untagged_entries_not_evicted_by_node_invalidation(self) -> None:
        cache = SubgraphCache(maxsize=256)
        cache.put("q1", [{"svc": "auth"}])
        cache.put("q2", [{"svc": "billing"}], node_ids={"node-A"})

        evicted = cache.invalidate_by_nodes({"node-A"})

        assert evicted == 1
        assert cache.get("q1") == [{"svc": "auth"}]


class TestInvalidateByNodesEmptySetNoOp:

    def test_empty_set_invalidates_nothing(self) -> None:
        cache = SubgraphCache(maxsize=256)
        cache.put("q1", [{"svc": "auth"}], node_ids={"node-A"})
        cache.put("q2", [{"svc": "billing"}], node_ids={"node-B"})

        evicted = cache.invalidate_by_nodes(set())

        assert evicted == 0
        assert cache.get("q1") == [{"svc": "auth"}]
        assert cache.get("q2") == [{"svc": "billing"}]


class TestInvalidateTenantStillWorks:

    def test_tenant_invalidation_fallback(self) -> None:
        cache = SubgraphCache(maxsize=256)
        cache.put("tenant_a:q1", [{"svc": "auth"}], node_ids={"node-A"})
        cache.put("tenant_a:q2", [{"svc": "billing"}])
        cache.put("tenant_b:q1", [{"svc": "gateway"}], node_ids={"node-C"})

        removed = cache.invalidate_tenant("tenant_a")

        assert removed == 2
        assert cache.get("tenant_a:q1") is None
        assert cache.get("tenant_a:q2") is None
        assert cache.get("tenant_b:q1") == [{"svc": "gateway"}]


class TestGraphBuilderPassesNodeIds:

    @pytest.mark.asyncio
    async def test_commit_extracts_and_passes_node_ids(self) -> None:
        from orchestrator.app.graph_builder import commit_to_neo4j

        captured_kwargs: Dict[str, Any] = {}

        async def _tracking_invalidate(**kwargs: Any) -> None:
            captured_kwargs.update(kwargs)

        async def _tracking_commit(self_repo: Any, entities: Any) -> None:
            pass

        async def _tracking_prune(
            self_repo: Any, current_ingestion_id: str = "", max_age_hours: int = 24,
        ) -> tuple:
            return 0, []

        mock_driver = MagicMock()
        mock_driver.close = AsyncMock()

        services = _make_services(3)

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
                "orchestrator.app.graph_builder.invalidate_caches_after_ingest",
                side_effect=_tracking_invalidate,
            ),
        ):
            result = await commit_to_neo4j(
                {"extracted_nodes": services, "tenant_id": "test-tenant"},
            )

        assert result["commit_status"] == "success"
        assert "node_ids" in captured_kwargs
        assert captured_kwargs["node_ids"] == {"svc-0", "svc-1", "svc-2"}


class TestRedisSubgraphCacheInvalidateByNodes:

    def test_redis_cache_delegates_invalidate_by_nodes_to_l1(self) -> None:
        from orchestrator.app.subgraph_cache import RedisSubgraphCache

        mock_redis = AsyncMock()
        with patch("redis.asyncio.from_url", return_value=mock_redis):
            cache = RedisSubgraphCache(redis_url="redis://localhost:6379")

        cache._l1.put("q1", [{"svc": "auth"}], node_ids={"node-A"})
        cache._l1.put("q2", [{"svc": "billing"}], node_ids={"node-B"})

        evicted = cache.invalidate_by_nodes({"node-A"})

        assert evicted == 1
        assert cache._l1.get("q1") is None
        assert cache._l1.get("q2") == [{"svc": "billing"}]


class TestInvalidateCachesWithNodeIds:

    @pytest.mark.asyncio
    async def test_invalidate_uses_node_ids_when_provided(self) -> None:
        from orchestrator.app.graph_builder import invalidate_caches_after_ingest

        subgraph_cache = MagicMock()
        subgraph_cache.invalidate_by_nodes = MagicMock(return_value=2)
        subgraph_cache.invalidate_tenant = MagicMock(return_value=0)

        semantic_cache = MagicMock()
        semantic_cache.invalidate_tenant = MagicMock(return_value=1)

        with (
            patch(
                "orchestrator.app.query_engine._SUBGRAPH_CACHE",
                new=subgraph_cache,
            ),
            patch(
                "orchestrator.app.query_engine._SEMANTIC_CACHE",
                new=semantic_cache,
            ),
        ):
            await invalidate_caches_after_ingest(
                tenant_id="team-alpha", node_ids={"node-1", "node-2"},
            )

        subgraph_cache.invalidate_by_nodes.assert_called_once_with(
            {"node-1", "node-2"},
        )
        subgraph_cache.invalidate_tenant.assert_not_called()

    @pytest.mark.asyncio
    async def test_invalidate_falls_back_to_tenant_when_no_node_ids(self) -> None:
        from orchestrator.app.graph_builder import invalidate_caches_after_ingest

        subgraph_cache = MagicMock()
        subgraph_cache.invalidate_tenant = MagicMock(return_value=2)

        semantic_cache = MagicMock()
        semantic_cache.invalidate_tenant = MagicMock(return_value=1)

        with (
            patch(
                "orchestrator.app.query_engine._SUBGRAPH_CACHE",
                new=subgraph_cache,
            ),
            patch(
                "orchestrator.app.query_engine._SEMANTIC_CACHE",
                new=semantic_cache,
            ),
        ):
            await invalidate_caches_after_ingest(tenant_id="team-alpha")

        subgraph_cache.invalidate_tenant.assert_called_once_with("team-alpha")
