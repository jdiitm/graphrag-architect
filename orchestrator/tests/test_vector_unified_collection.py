from __future__ import annotations

import pytest

from orchestrator.app.vector_store import (
    InMemoryVectorStore,
    VectorRecord,
)


@pytest.mark.asyncio
class TestUnifiedPayloadFiltering:

    async def test_production_uses_payload_filter_not_separate_collection(
        self,
    ) -> None:
        store = InMemoryVectorStore()
        await store.upsert(
            "services",
            [
                VectorRecord(
                    id="a",
                    vector=[1.0, 0.0],
                    metadata={"tenant_id": "acme"},
                ),
                VectorRecord(
                    id="b",
                    vector=[0.9, 0.1],
                    metadata={"tenant_id": "globex"},
                ),
            ],
        )

        acme_results = await store.search_with_tenant(
            "services",
            [1.0, 0.0],
            "acme",
            limit=10,
            deployment_mode="production",
        )
        assert len(acme_results) == 1
        assert acme_results[0].id == "a"

    async def test_production_filters_cross_tenant_vectors(self) -> None:
        store = InMemoryVectorStore()
        await store.upsert(
            "services",
            [
                VectorRecord(
                    id="own",
                    vector=[1.0, 0.0],
                    metadata={"tenant_id": "acme"},
                ),
                VectorRecord(
                    id="foreign",
                    vector=[1.0, 0.0],
                    metadata={"tenant_id": "globex"},
                ),
            ],
        )

        results = await store.search_with_tenant(
            "services",
            [1.0, 0.0],
            "acme",
            limit=10,
            deployment_mode="production",
        )
        result_ids = {r.id for r in results}
        assert "own" in result_ids
        assert "foreign" not in result_ids

    async def test_dev_mode_also_uses_payload_filtering(self) -> None:
        store = InMemoryVectorStore()
        await store.upsert(
            "services",
            [
                VectorRecord(
                    id="a",
                    vector=[1.0, 0.0],
                    metadata={"tenant_id": "acme"},
                ),
                VectorRecord(
                    id="b",
                    vector=[0.9, 0.1],
                    metadata={"tenant_id": "globex"},
                ),
            ],
        )

        results = await store.search_with_tenant(
            "services", [1.0, 0.0], "acme", limit=10,
        )
        assert len(results) == 1
        assert results[0].id == "a"

    async def test_empty_tenant_returns_all(self) -> None:
        store = InMemoryVectorStore()
        await store.upsert(
            "services",
            [
                VectorRecord(
                    id="a",
                    vector=[1.0, 0.0],
                    metadata={"tenant_id": "acme"},
                ),
                VectorRecord(
                    id="b",
                    vector=[0.9, 0.1],
                    metadata={"tenant_id": "globex"},
                ),
            ],
        )

        results = await store.search_with_tenant(
            "services", [1.0, 0.0], "", limit=10,
        )
        assert len(results) == 2


class TestResolveCollectionNameRemoved:

    def test_resolve_collection_name_no_longer_exported(self) -> None:
        from orchestrator.app import vector_store
        assert not hasattr(vector_store, "resolve_collection_name"), (
            "resolve_collection_name must be removed — per-tenant Qdrant "
            "collections cause OOM at scale. Use payload filtering."
        )


class TestGraphBuilderUnifiedCollection:

    def test_resolve_vector_collection_always_returns_base(self) -> None:
        from orchestrator.app.graph_builder import resolve_vector_collection
        assert resolve_vector_collection("acme") == "services"
        assert resolve_vector_collection("") == "services"
        assert resolve_vector_collection(None) == "services"
