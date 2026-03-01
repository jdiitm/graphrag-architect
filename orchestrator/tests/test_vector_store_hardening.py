from __future__ import annotations

import pytest

from orchestrator.app.vector_store import (
    InMemoryVectorStore,
    QdrantVectorStore,
    create_vector_store,
)


class TestNeo4jVectorStoreRemoved:

    def test_neo4j_backend_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="[Nn]eo4j.*removed|[Dd]eprecated.*removed"):
            create_vector_store(backend="neo4j")

    def test_neo4j_backend_raises_in_production(self) -> None:
        with pytest.raises(ValueError):
            create_vector_store(backend="neo4j", deployment_mode="production")

    def test_neo4j_backend_raises_in_dev(self) -> None:
        with pytest.raises(ValueError):
            create_vector_store(backend="neo4j", deployment_mode="dev")

    def test_neo4j_vector_store_class_not_importable(self) -> None:
        from orchestrator.app import vector_store

        assert not hasattr(vector_store, "Neo4jVectorStore"), (
            "Neo4jVectorStore class must be removed entirely, "
            "not merely deprecated"
        )


class TestRemainingBackendsWork:

    def test_memory_backend_still_works(self) -> None:
        store = create_vector_store(backend="memory")
        assert isinstance(store, InMemoryVectorStore)

    def test_qdrant_backend_still_works(self) -> None:
        store = create_vector_store(
            backend="qdrant", url="http://localhost:6333",
        )
        assert isinstance(store, QdrantVectorStore)

    def test_production_still_rejects_memory(self) -> None:
        with pytest.raises(ValueError, match="[Pp]roduction"):
            create_vector_store(backend="memory", deployment_mode="production")

    def test_production_accepts_qdrant(self) -> None:
        store = create_vector_store(
            backend="qdrant",
            url="http://localhost:6333",
            deployment_mode="production",
        )
        assert isinstance(store, QdrantVectorStore)
