from __future__ import annotations

from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.app.config import VectorStoreConfig
from orchestrator.app.vector_store import (
    QdrantVectorStore,
    VectorRecord,
    create_vector_store,
)


class TestVectorStoreConfigSharding:

    def test_shard_by_tenant_defaults_to_false(self) -> None:
        config = VectorStoreConfig()
        assert config.shard_by_tenant is False

    def test_shard_by_tenant_from_env(self) -> None:
        with patch.dict(
            "os.environ",
            {"QDRANT_SHARD_BY_TENANT": "true"},
            clear=False,
        ):
            config = VectorStoreConfig.from_env()
            assert config.shard_by_tenant is True

    def test_shard_by_tenant_env_false(self) -> None:
        with patch.dict(
            "os.environ",
            {"QDRANT_SHARD_BY_TENANT": "false"},
            clear=False,
        ):
            config = VectorStoreConfig.from_env()
            assert config.shard_by_tenant is False


class TestQdrantShardKeyRouting:

    def test_qdrant_store_accepts_shard_by_tenant(self) -> None:
        store = QdrantVectorStore(
            url="http://localhost:6333",
            shard_by_tenant=True,
        )
        assert store._shard_by_tenant is True

    def test_qdrant_store_shard_by_tenant_defaults_false(self) -> None:
        store = QdrantVectorStore(url="http://localhost:6333")
        assert store._shard_by_tenant is False

    @pytest.mark.asyncio
    async def test_upsert_passes_shard_key_when_enabled(self) -> None:
        store = QdrantVectorStore(
            url="http://localhost:6333",
            shard_by_tenant=True,
        )
        mock_client = AsyncMock()
        store._client = mock_client

        vectors = [
            VectorRecord(
                id="v1",
                vector=[0.1, 0.2],
                metadata={"tenant_id": "t1"},
            ),
        ]
        await store.upsert("test_collection", vectors, tenant_id="t1")

        mock_client.upsert.assert_awaited_once()
        call_kwargs = mock_client.upsert.call_args
        assert call_kwargs.kwargs.get("shard_key_selector") == "t1"

    @pytest.mark.asyncio
    async def test_upsert_no_shard_key_when_disabled(self) -> None:
        store = QdrantVectorStore(
            url="http://localhost:6333",
            shard_by_tenant=False,
        )
        mock_client = AsyncMock()
        store._client = mock_client

        vectors = [
            VectorRecord(
                id="v1",
                vector=[0.1, 0.2],
                metadata={"tenant_id": "t1"},
            ),
        ]
        await store.upsert("test_collection", vectors)

        mock_client.upsert.assert_awaited_once()
        call_kwargs = mock_client.upsert.call_args
        assert "shard_key_selector" not in (call_kwargs.kwargs or {})

    @pytest.mark.asyncio
    async def test_search_with_tenant_uses_shard_key(self) -> None:
        store = QdrantVectorStore(
            url="http://localhost:6333",
            shard_by_tenant=True,
        )
        mock_client = AsyncMock()
        mock_client.search = AsyncMock(return_value=[])
        store._client = mock_client
        from orchestrator.app.circuit_breaker import (
            CircuitBreaker,
            CircuitBreakerConfig,
        )
        store._circuit_breaker = CircuitBreaker(
            config=CircuitBreakerConfig(failure_threshold=100, recovery_timeout=1.0),
            name="test",
        )

        await store.search_with_tenant(
            collection="test_collection",
            query_vector=[0.1, 0.2],
            tenant_id="t1",
            limit=5,
        )

        mock_client.search.assert_awaited_once()
        call_kwargs = mock_client.search.call_args
        assert call_kwargs.kwargs.get("shard_key_selector") == "t1"

    @pytest.mark.asyncio
    async def test_search_with_tenant_no_shard_key_when_disabled(self) -> None:
        store = QdrantVectorStore(
            url="http://localhost:6333",
            shard_by_tenant=False,
        )
        mock_client = AsyncMock()
        mock_client.search = AsyncMock(return_value=[])
        store._client = mock_client
        from orchestrator.app.circuit_breaker import (
            CircuitBreaker,
            CircuitBreakerConfig,
        )
        store._circuit_breaker = CircuitBreaker(
            config=CircuitBreakerConfig(failure_threshold=100, recovery_timeout=1.0),
            name="test",
        )

        await store.search_with_tenant(
            collection="test_collection",
            query_vector=[0.1, 0.2],
            tenant_id="t1",
            limit=5,
        )

        mock_client.search.assert_awaited_once()
        call_kwargs = mock_client.search.call_args
        assert "shard_key_selector" not in (call_kwargs.kwargs or {})

    @pytest.mark.asyncio
    async def test_delete_uses_shard_key_when_enabled(self) -> None:
        store = QdrantVectorStore(
            url="http://localhost:6333",
            shard_by_tenant=True,
        )
        mock_client = AsyncMock()
        mock_client.count = AsyncMock(
            return_value=MagicMock(count=1),
        )
        store._client = mock_client

        await store.delete(
            collection="test_collection",
            ids=["v1"],
            tenant_id="t1",
        )

        mock_client.delete.assert_awaited_once()
        call_kwargs = mock_client.delete.call_args
        assert call_kwargs.kwargs.get("shard_key_selector") == "t1"


class TestVectorCollectionManager:

    @pytest.mark.asyncio
    async def test_ensure_collection_creates_with_custom_sharding(self) -> None:
        from orchestrator.app.vector_collection_manager import (
            VectorCollectionManager,
        )
        mock_client = AsyncMock()
        mock_client.collection_exists = AsyncMock(return_value=False)
        manager = VectorCollectionManager(client=mock_client)

        await manager.ensure_collection(
            collection_name="test_collection",
            vector_size=768,
        )

        mock_client.create_collection.assert_awaited_once()
        call_kwargs = mock_client.create_collection.call_args
        assert call_kwargs.kwargs.get("collection_name") == "test_collection"
        assert call_kwargs.kwargs.get("sharding_method") == "custom"
        assert call_kwargs.kwargs.get("shard_number") == 1

    @pytest.mark.asyncio
    async def test_ensure_collection_skips_if_exists(self) -> None:
        from orchestrator.app.vector_collection_manager import (
            VectorCollectionManager,
        )
        mock_client = AsyncMock()
        mock_client.collection_exists = AsyncMock(return_value=True)
        manager = VectorCollectionManager(client=mock_client)

        await manager.ensure_collection(
            collection_name="test_collection",
            vector_size=768,
        )

        mock_client.create_collection.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_ensure_shard_key_creates_key(self) -> None:
        from orchestrator.app.vector_collection_manager import (
            VectorCollectionManager,
        )
        mock_client = AsyncMock()
        manager = VectorCollectionManager(client=mock_client)

        await manager.ensure_shard_key(
            collection_name="test_collection",
            shard_key="t1",
        )

        mock_client.create_shard_key.assert_awaited_once()
        call_kwargs = mock_client.create_shard_key.call_args
        assert call_kwargs.kwargs.get("collection_name") == "test_collection"
        assert call_kwargs.kwargs.get("shard_key") == "t1"

    @pytest.mark.asyncio
    async def test_ensure_shard_key_cached_idempotent(self) -> None:
        from orchestrator.app.vector_collection_manager import (
            VectorCollectionManager,
        )
        mock_client = AsyncMock()
        manager = VectorCollectionManager(client=mock_client)

        await manager.ensure_shard_key(
            collection_name="test_collection",
            shard_key="t1",
        )
        await manager.ensure_shard_key(
            collection_name="test_collection",
            shard_key="t1",
        )

        assert mock_client.create_shard_key.await_count == 1

    @pytest.mark.asyncio
    async def test_ensure_shard_key_reraises_non_duplicate_errors(self) -> None:
        from orchestrator.app.vector_collection_manager import (
            VectorCollectionManager,
        )
        mock_client = AsyncMock()
        mock_client.create_shard_key = AsyncMock(
            side_effect=ConnectionError("connection refused"),
        )
        manager = VectorCollectionManager(client=mock_client)

        with pytest.raises(ConnectionError, match="connection refused"):
            await manager.ensure_shard_key(
                collection_name="test_collection",
                shard_key="t1",
            )

    @pytest.mark.asyncio
    async def test_ensure_shard_key_tolerates_duplicate_error(self) -> None:
        from orchestrator.app.vector_collection_manager import (
            VectorCollectionManager,
        )
        mock_client = AsyncMock()
        mock_client.create_shard_key = AsyncMock(
            side_effect=RuntimeError("Shard key already exists"),
        )
        manager = VectorCollectionManager(client=mock_client)

        await manager.ensure_shard_key(
            collection_name="test_collection",
            shard_key="t1",
        )


class TestFactoryPassesShardConfig:

    def test_create_vector_store_passes_shard_by_tenant(self) -> None:
        store = create_vector_store(
            backend="qdrant",
            url="http://localhost:6333",
            shard_by_tenant=True,
        )
        assert isinstance(store, QdrantVectorStore)
        assert store._shard_by_tenant is True

    def test_create_vector_store_default_no_sharding(self) -> None:
        store = create_vector_store(
            backend="qdrant",
            url="http://localhost:6333",
        )
        assert isinstance(store, QdrantVectorStore)
        assert store._shard_by_tenant is False


class TestQdrantCollectionManagerIntegration:

    @pytest.mark.asyncio
    async def test_ensure_tenant_shard_calls_manager(self) -> None:
        store = QdrantVectorStore(
            url="http://localhost:6333",
            shard_by_tenant=True,
        )
        mock_manager = AsyncMock()
        store._collection_manager = mock_manager

        await store.ensure_tenant_shard("test_collection", "t1")

        mock_manager.ensure_shard_key.assert_awaited_once_with(
            collection_name="test_collection", shard_key="t1",
        )

    @pytest.mark.asyncio
    async def test_ensure_tenant_shard_noop_when_disabled(self) -> None:
        store = QdrantVectorStore(
            url="http://localhost:6333",
            shard_by_tenant=False,
        )
        mock_manager = AsyncMock()
        store._collection_manager = mock_manager

        await store.ensure_tenant_shard("test_collection", "t1")

        mock_manager.ensure_shard_key.assert_not_awaited()


class TestBackwardCompatibility:

    @pytest.mark.asyncio
    async def test_upsert_without_tenant_id_still_works(self) -> None:
        store = QdrantVectorStore(
            url="http://localhost:6333",
            shard_by_tenant=True,
        )
        mock_client = AsyncMock()
        store._client = mock_client

        vectors = [
            VectorRecord(
                id="v1",
                vector=[0.1, 0.2],
                metadata={"tenant_id": "t1"},
            ),
        ]
        await store.upsert("test_collection", vectors)

        mock_client.upsert.assert_awaited_once()
        call_kwargs = mock_client.upsert.call_args
        assert "shard_key_selector" not in (call_kwargs.kwargs or {})

    @pytest.mark.asyncio
    async def test_inmemory_store_unaffected(self) -> None:
        from orchestrator.app.vector_store import InMemoryVectorStore
        store = InMemoryVectorStore()
        vectors = [
            VectorRecord(
                id="v1",
                vector=[0.1, 0.2, 0.3],
                metadata={"tenant_id": "t1"},
            ),
        ]
        count = await store.upsert("test", vectors)
        assert count == 1

        results = await store.search_with_tenant(
            collection="test",
            query_vector=[0.1, 0.2, 0.3],
            tenant_id="t1",
        )
        assert len(results) == 1
        assert results[0].id == "v1"
