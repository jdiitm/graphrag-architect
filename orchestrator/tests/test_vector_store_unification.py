from __future__ import annotations

import pytest

from orchestrator.app.vector_store import (
    InMemoryVectorStore,
    VectorRecord,
    create_vector_store,
)


class TestCreateVectorStoreDefaults:
    def test_create_vector_store_default_is_memory(self) -> None:
        store = create_vector_store(backend="memory")
        assert isinstance(store, InMemoryVectorStore)

    def test_neo4j_backend_rejected(self) -> None:
        with pytest.raises(ValueError, match="removed"):
            create_vector_store(backend="neo4j")


class TestQdrantReadOnly:
    def test_qdrant_store_marked_as_read_replica(self) -> None:
        from orchestrator.app.vector_store import QdrantVectorStore

        store = QdrantVectorStore()
        assert store.is_read_replica is True
