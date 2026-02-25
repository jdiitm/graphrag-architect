from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.app.vector_store import (
    InMemoryVectorStore,
    VectorRecord,
    create_vector_store,
)


class TestNeo4jWriteSideTruth:
    def test_create_vector_store_default_is_memory(self) -> None:
        store = create_vector_store(backend="memory")
        assert isinstance(store, InMemoryVectorStore)

    def test_create_neo4j_backend(self) -> None:
        from orchestrator.app.vector_store import Neo4jVectorStore

        store = create_vector_store(backend="neo4j")
        assert isinstance(store, Neo4jVectorStore)


class TestNeo4jVectorStore:
    @pytest.mark.asyncio
    async def test_upsert_stores_vectors(self) -> None:
        from orchestrator.app.vector_store import Neo4jVectorStore

        mock_session = AsyncMock()
        mock_session.execute_write = AsyncMock(return_value=1)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)
        mock_driver = MagicMock()
        mock_driver.session.return_value = mock_session

        store = Neo4jVectorStore(driver=mock_driver)
        records = [
            VectorRecord(id="n1", vector=[0.1, 0.2], metadata={"name": "svc"}),
        ]
        count = await store.upsert("collection", records)
        assert count == 1
        mock_session.execute_write.assert_called_once()

    @pytest.mark.asyncio
    async def test_search_returns_results(self) -> None:
        from orchestrator.app.vector_store import Neo4jVectorStore

        mock_session = AsyncMock()
        mock_session.execute_read = AsyncMock(return_value=[
            {"id": "n1", "score": 0.95, "name": "svc-a"},
        ])
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)
        mock_driver = MagicMock()
        mock_driver.session.return_value = mock_session

        store = Neo4jVectorStore(driver=mock_driver)
        results = await store.search("collection", [0.1, 0.2], limit=5)
        assert len(results) == 1
        assert results[0].id == "n1"

    @pytest.mark.asyncio
    async def test_delete_removes_vectors(self) -> None:
        from orchestrator.app.vector_store import Neo4jVectorStore

        mock_session = AsyncMock()
        mock_session.execute_write = AsyncMock(return_value=1)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)
        mock_driver = MagicMock()
        mock_driver.session.return_value = mock_session

        store = Neo4jVectorStore(driver=mock_driver)
        count = await store.delete("collection", ["n1"])
        assert count == 1


class TestNeo4jGhostNodePrevention:

    @pytest.mark.asyncio
    async def test_delete_marks_embedding_removed(self) -> None:
        from orchestrator.app.vector_store import Neo4jVectorStore

        executed_queries: list[str] = []

        async def _capture_write(func, **kwargs):
            result_mock = AsyncMock()
            result_mock.data = AsyncMock(return_value=[{"removed": 1}])

            class FakeTx:
                async def run(self, query, **params):
                    executed_queries.append(query)
                    return result_mock
            return await func(FakeTx())

        mock_session = AsyncMock()
        mock_session.execute_write = _capture_write
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)
        mock_driver = MagicMock()
        mock_driver.session.return_value = mock_session

        store = Neo4jVectorStore(driver=mock_driver)
        await store.delete("collection", ["n1"])

        assert len(executed_queries) >= 1, "Delete must execute at least one query"
        combined = " ".join(executed_queries)
        assert "embedding_removed" in combined, (
            "Delete must set embedding_removed marker on nodes to prevent "
            f"ghost nodes. Queries executed: {executed_queries}"
        )

    @pytest.mark.asyncio
    async def test_delete_sets_timestamp(self) -> None:
        from orchestrator.app.vector_store import Neo4jVectorStore

        executed_queries: list[str] = []

        async def _capture_write(func, **kwargs):
            result_mock = AsyncMock()
            result_mock.data = AsyncMock(return_value=[{"removed": 1}])

            class FakeTx:
                async def run(self, query, **params):
                    executed_queries.append(query)
                    return result_mock
            return await func(FakeTx())

        mock_session = AsyncMock()
        mock_session.execute_write = _capture_write
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)
        mock_driver = MagicMock()
        mock_driver.session.return_value = mock_session

        store = Neo4jVectorStore(driver=mock_driver)
        await store.delete("collection", ["n1"])

        combined = " ".join(executed_queries)
        assert "embedding_removed_at" in combined, (
            "Delete must set embedding_removed_at timestamp for reaper scheduling. "
            f"Queries executed: {executed_queries}"
        )


class TestQdrantReadOnly:
    def test_qdrant_store_marked_as_read_replica(self) -> None:
        from orchestrator.app.vector_store import QdrantVectorStore

        store = QdrantVectorStore()
        assert store.is_read_replica is True

    def test_neo4j_store_is_not_read_replica(self) -> None:
        from orchestrator.app.vector_store import Neo4jVectorStore

        store = Neo4jVectorStore(driver=MagicMock())
        assert store.is_read_replica is False
