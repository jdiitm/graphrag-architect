from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from orchestrator.app.vector_store import (
    QdrantVectorStore,
    VectorRecord,
)


@pytest.mark.asyncio
class TestNativeClientErrorHandling:

    async def test_upsert_propagates_connection_error(self) -> None:
        mock_client = AsyncMock()
        mock_client.upsert = AsyncMock(side_effect=ConnectionError("broken"))
        store = QdrantVectorStore(url="http://localhost:6333")
        store._client = mock_client

        with pytest.raises(ConnectionError, match="broken"):
            await store.upsert(
                "coll",
                [VectorRecord(id="1", vector=[0.1], metadata={})],
            )

    async def test_search_propagates_connection_error(self) -> None:
        mock_client = AsyncMock()
        mock_client.search = AsyncMock(side_effect=ConnectionError("broken"))
        store = QdrantVectorStore(url="http://localhost:6333")
        store._client = mock_client

        with pytest.raises(ConnectionError, match="broken"):
            await store.search("coll", [0.1], limit=5)

    async def test_delete_propagates_connection_error(self) -> None:
        mock_client = AsyncMock()
        mock_client.delete = AsyncMock(side_effect=ConnectionError("broken"))
        store = QdrantVectorStore(url="http://localhost:6333")
        store._client = mock_client

        with pytest.raises(ConnectionError, match="broken"):
            await store.delete("coll", ["id1"])

    async def test_search_with_tenant_propagates_connection_error(self) -> None:
        mock_client = AsyncMock()
        mock_client.search = AsyncMock(side_effect=ConnectionError("broken"))
        store = QdrantVectorStore(url="http://localhost:6333")
        store._client = mock_client

        with pytest.raises(ConnectionError, match="broken"):
            await store.search_with_tenant("coll", [0.1], "tenant-1", limit=5)


@pytest.mark.asyncio
class TestNativeClientSuccessPath:

    async def test_upsert_returns_count_on_success(self) -> None:
        mock_client = AsyncMock()
        mock_client.upsert = AsyncMock()
        store = QdrantVectorStore(url="http://localhost:6333")
        store._client = mock_client

        count = await store.upsert(
            "coll",
            [VectorRecord(id="1", vector=[0.1], metadata={})],
        )
        assert count == 1

    async def test_search_returns_results_on_success(self) -> None:
        hit = MagicMock()
        hit.id = "a"
        hit.score = 0.95
        hit.payload = {"name": "svc"}
        mock_client = AsyncMock()
        mock_client.search = AsyncMock(return_value=[hit])
        store = QdrantVectorStore(url="http://localhost:6333")
        store._client = mock_client

        results = await store.search("coll", [0.1], limit=5)
        assert len(results) == 1
        assert results[0].id == "a"

    async def test_delete_returns_count_on_success(self) -> None:
        mock_client = AsyncMock()
        mock_client.delete = AsyncMock()
        store = QdrantVectorStore(url="http://localhost:6333")
        store._client = mock_client

        count = await store.delete("coll", ["a", "b"])
        assert count == 2

    async def test_search_with_tenant_returns_filtered_results(self) -> None:
        hit = MagicMock()
        hit.id = "a"
        hit.score = 0.9
        hit.payload = {"tenant_id": "t1"}
        mock_client = AsyncMock()
        mock_client.search = AsyncMock(return_value=[hit])
        store = QdrantVectorStore(url="http://localhost:6333")
        store._client = mock_client

        results = await store.search_with_tenant("coll", [0.1], "t1", limit=5)
        assert len(results) == 1
        assert results[0].id == "a"


@pytest.mark.asyncio
class TestNativeClientDeleteWithTenant:

    async def test_delete_with_tenant_uses_compound_filter(self) -> None:
        mock_client = AsyncMock()
        count_result = MagicMock()
        count_result.count = 1
        mock_client.count = AsyncMock(return_value=count_result)
        mock_client.delete = AsyncMock()
        store = QdrantVectorStore(url="http://localhost:6333")
        store._client = mock_client

        count = await store.delete("coll", ["a"], tenant_id="t1")
        assert count == 1
        mock_client.count.assert_awaited_once()
        mock_client.delete.assert_awaited_once()

    async def test_delete_without_tenant_uses_point_ids(self) -> None:
        mock_client = AsyncMock()
        mock_client.delete = AsyncMock()
        store = QdrantVectorStore(url="http://localhost:6333")
        store._client = mock_client

        count = await store.delete("coll", ["a", "b"])
        assert count == 2
        mock_client.delete.assert_awaited_once()
        mock_client.count.assert_not_called()


@pytest.mark.asyncio
class TestNativeClientGrpcConfig:

    async def test_default_store_uses_grpc(self) -> None:
        store = QdrantVectorStore(url="http://localhost:6333")
        assert store._prefer_grpc is True

    async def test_store_respects_grpc_false(self) -> None:
        store = QdrantVectorStore(url="http://localhost:6333", prefer_grpc=False)
        assert store._prefer_grpc is False

    async def test_lazy_client_init(self) -> None:
        store = QdrantVectorStore(url="http://localhost:6333")
        assert store._client is None
