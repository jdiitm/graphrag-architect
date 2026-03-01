from __future__ import annotations

import pytest

from orchestrator.app.vector_store import (
    InMemoryVectorStore,
    QdrantVectorStore,
    VectorRecord,
    create_vector_store,
    resolve_collection_name,
)


class TestQdrantProductionUsesPhysicalIsolation:

    def test_qdrant_store_accepts_deployment_mode(self) -> None:
        store = QdrantVectorStore(
            url="http://localhost:6333",
            deployment_mode="production",
        )
        assert store._deployment_mode == "production"

    def test_qdrant_store_defaults_to_dev_mode(self) -> None:
        store = QdrantVectorStore(url="http://localhost:6333")
        assert store._deployment_mode == "dev"

    def test_factory_passes_deployment_mode_to_qdrant(self) -> None:
        store = create_vector_store(
            backend="qdrant",
            url="http://localhost:6333",
            deployment_mode="production",
        )
        assert isinstance(store, QdrantVectorStore)
        assert store._deployment_mode == "production"


@pytest.mark.asyncio
class TestInMemoryPhysicalIsolation:

    async def test_production_mode_routes_to_tenant_collection(self) -> None:
        store = InMemoryVectorStore()
        await store.upsert(
            resolve_collection_name("services", "acme"),
            [VectorRecord(id="a", vector=[1.0, 0.0], metadata={"tenant_id": "acme"})],
        )
        await store.upsert(
            resolve_collection_name("services", "globex"),
            [VectorRecord(id="b", vector=[1.0, 0.0], metadata={"tenant_id": "globex"})],
        )

        acme_results = await store.search_with_tenant(
            "services", [1.0, 0.0], "acme", limit=10,
            deployment_mode="production",
        )
        assert len(acme_results) == 1
        assert acme_results[0].id == "a"

        globex_results = await store.search_with_tenant(
            "services", [1.0, 0.0], "globex", limit=10,
            deployment_mode="production",
        )
        assert len(globex_results) == 1
        assert globex_results[0].id == "b"

    async def test_production_mode_prevents_cross_tenant_leakage(self) -> None:
        store = InMemoryVectorStore()
        await store.upsert(
            resolve_collection_name("services", "acme"),
            [VectorRecord(id="a", vector=[1.0, 0.0], metadata={"tenant_id": "acme"})],
        )
        await store.upsert(
            "services",
            [VectorRecord(id="leak", vector=[1.0, 0.0], metadata={"tenant_id": "globex"})],
        )

        acme_results = await store.search_with_tenant(
            "services", [1.0, 0.0], "acme", limit=10,
            deployment_mode="production",
        )
        result_ids = {r.id for r in acme_results}
        assert "leak" not in result_ids

    async def test_dev_mode_uses_logical_filtering(self) -> None:
        store = InMemoryVectorStore()
        await store.upsert(
            "services",
            [
                VectorRecord(id="a", vector=[1.0, 0.0], metadata={"tenant_id": "acme"}),
                VectorRecord(id="b", vector=[0.9, 0.1], metadata={"tenant_id": "globex"}),
            ],
        )

        results = await store.search_with_tenant(
            "services", [1.0, 0.0], "acme", limit=10,
        )
        assert len(results) == 1
        assert results[0].id == "a"


class TestVectorIsolationValidation:

    def test_validate_rejects_logical_vector_isolation_in_production(self) -> None:
        from orchestrator.app.tenant_isolation import (
            LogicalIsolationInProductionError,
            validate_vector_isolation,
        )
        with pytest.raises(LogicalIsolationInProductionError):
            validate_vector_isolation(
                deployment_mode="production",
                isolation_mode="logical",
            )

    def test_validate_allows_physical_vector_isolation_in_production(self) -> None:
        from orchestrator.app.tenant_isolation import validate_vector_isolation
        validate_vector_isolation(
            deployment_mode="production",
            isolation_mode="physical",
        )

    def test_validate_allows_logical_in_dev(self) -> None:
        from orchestrator.app.tenant_isolation import validate_vector_isolation
        validate_vector_isolation(
            deployment_mode="dev",
            isolation_mode="logical",
        )

    def test_validate_allows_physical_in_dev(self) -> None:
        from orchestrator.app.tenant_isolation import validate_vector_isolation
        validate_vector_isolation(
            deployment_mode="dev",
            isolation_mode="physical",
        )
