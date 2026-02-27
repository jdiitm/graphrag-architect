from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.app.vector_store import (
    QdrantVectorStore,
    VectorRecord,
    create_vector_store,
)


class TestQdrantVectorStoreGrpcInit:

    def test_prefer_grpc_defaults_to_true(self) -> None:
        store = QdrantVectorStore(url="http://localhost:6333")
        assert store._prefer_grpc is True

    def test_prefer_grpc_can_be_disabled(self) -> None:
        store = QdrantVectorStore(url="http://localhost:6333", prefer_grpc=False)
        assert store._prefer_grpc is False

    def test_client_created_with_grpc_flag(self) -> None:
        with patch(
            "qdrant_client.AsyncQdrantClient",
            return_value=MagicMock(),
        ) as mock_cls:
            store = QdrantVectorStore(
                url="http://localhost:6333", prefer_grpc=True,
            )
            store._client = None
            _ = store._get_client()
            mock_cls.assert_called_once_with(
                url="http://localhost:6333",
                api_key=None,
                prefer_grpc=True,
            )

    def test_client_created_without_grpc_when_disabled(self) -> None:
        with patch(
            "qdrant_client.AsyncQdrantClient",
            return_value=MagicMock(),
        ) as mock_cls:
            store = QdrantVectorStore(
                url="http://localhost:6333", prefer_grpc=False,
            )
            store._client = None
            _ = store._get_client()
            mock_cls.assert_called_once_with(
                url="http://localhost:6333",
                api_key=None,
                prefer_grpc=False,
            )


class TestCreateVectorStoreReturnsNativeQdrant:

    def test_qdrant_backend_returns_qdrant_vector_store(self) -> None:
        store = create_vector_store(backend="qdrant", url="http://localhost:6333")
        assert isinstance(store, QdrantVectorStore)

    def test_qdrant_backend_does_not_return_pooled_type(self) -> None:
        store = create_vector_store(backend="qdrant", url="http://localhost:6333")
        assert not hasattr(store, "_pool")


class TestQdrantVectorStoreNoPoolLifecycle:

    def test_no_acquire_method(self) -> None:
        store = QdrantVectorStore(url="http://localhost:6333")
        assert not hasattr(store, "acquire")

    def test_no_release_method(self) -> None:
        store = QdrantVectorStore(url="http://localhost:6333")
        assert not hasattr(store, "release")

    def test_no_sweep_task(self) -> None:
        store = QdrantVectorStore(url="http://localhost:6333")
        assert not hasattr(store, "_sweep_task")

    def test_no_pool_attribute(self) -> None:
        store = QdrantVectorStore(url="http://localhost:6333")
        assert not hasattr(store, "_pool")


@pytest.mark.asyncio
class TestQdrantVectorStoreOperations:

    async def test_upsert_delegates_to_client(self) -> None:
        mock_client = AsyncMock()
        mock_client.upsert = AsyncMock()
        store = QdrantVectorStore(url="http://localhost:6333")
        store._client = mock_client

        records = [
            VectorRecord(id="a", vector=[0.1, 0.2], metadata={"name": "svc"}),
        ]
        count = await store.upsert("coll", records)
        assert count == 1
        mock_client.upsert.assert_awaited_once()

    async def test_search_delegates_to_client(self) -> None:
        hit = MagicMock()
        hit.id = "a"
        hit.score = 0.95
        hit.payload = {"name": "svc"}
        mock_client = AsyncMock()
        mock_client.search = AsyncMock(return_value=[hit])
        store = QdrantVectorStore(url="http://localhost:6333")
        store._client = mock_client

        results = await store.search("coll", [0.1, 0.2], limit=5)
        assert len(results) == 1
        assert results[0].id == "a"
        assert results[0].score == 0.95

    async def test_delete_delegates_to_client(self) -> None:
        mock_client = AsyncMock()
        mock_client.delete = AsyncMock()
        store = QdrantVectorStore(url="http://localhost:6333")
        store._client = mock_client

        count = await store.delete("coll", ["a", "b"])
        assert count == 2

    async def test_search_with_tenant_uses_filter(self) -> None:
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
        call_kwargs = mock_client.search.call_args
        assert call_kwargs.kwargs.get("query_filter") is not None

    async def test_error_propagates_without_pool_lifecycle(self) -> None:
        mock_client = AsyncMock()
        mock_client.upsert = AsyncMock(side_effect=ConnectionError("broken"))
        store = QdrantVectorStore(url="http://localhost:6333")
        store._client = mock_client

        with pytest.raises(ConnectionError, match="broken"):
            await store.upsert(
                "coll",
                [VectorRecord(id="x", vector=[0.1], metadata={})],
            )

    async def test_delete_with_tenant_uses_compound_filter(self) -> None:
        mock_client = AsyncMock()
        count_result = MagicMock()
        count_result.count = 2
        mock_client.count = AsyncMock(return_value=count_result)
        mock_client.delete = AsyncMock()
        store = QdrantVectorStore(url="http://localhost:6333")
        store._client = mock_client

        count = await store.delete("coll", ["a", "b"], tenant_id="t1")
        assert count == 2
        mock_client.count.assert_awaited_once()
        mock_client.delete.assert_awaited_once()


class TestPooledQdrantVectorStoreRemoved:

    def test_pooled_qdrant_not_in_public_api(self) -> None:
        import orchestrator.app.vector_store as vs
        assert not hasattr(vs, "PooledQdrantVectorStore")

    def test_qdrant_client_pool_not_in_public_api(self) -> None:
        import orchestrator.app.vector_store as vs
        assert not hasattr(vs, "QdrantClientPool")
