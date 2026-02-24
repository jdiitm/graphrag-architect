from __future__ import annotations

from unittest.mock import patch

import pytest

from orchestrator.app.config import VectorStoreConfig
from orchestrator.app.vector_store import (
    InMemoryVectorStore,
    SearchResult,
    VectorRecord,
    _cosine_similarity,
)


class TestCosineSimilarity:
    def test_identical_vectors_return_one(self) -> None:
        vec = [1.0, 2.0, 3.0]
        assert abs(_cosine_similarity(vec, vec) - 1.0) < 1e-9

    def test_orthogonal_vectors_return_zero(self) -> None:
        a = [1.0, 0.0, 0.0]
        b = [0.0, 1.0, 0.0]
        assert abs(_cosine_similarity(a, b)) < 1e-9

    def test_opposite_vectors_return_minus_one(self) -> None:
        a = [1.0, 0.0, 0.0]
        b = [-1.0, 0.0, 0.0]
        assert abs(_cosine_similarity(a, b) - (-1.0)) < 1e-9

    def test_scaled_vectors_same_direction_return_one(self) -> None:
        a = [1.0, 2.0, 3.0]
        b = [2.0, 4.0, 6.0]
        assert abs(_cosine_similarity(a, b) - 1.0) < 1e-9


@pytest.mark.asyncio
class TestInMemoryVectorStoreUpsert:
    async def test_upsert_adds_records(self) -> None:
        store = InMemoryVectorStore()
        records = [
            VectorRecord(id="a", vector=[1.0, 0.0], metadata={"x": 1}),
            VectorRecord(id="b", vector=[0.0, 1.0], metadata={"y": 2}),
        ]
        count = await store.upsert("coll", records)
        assert count == 2
        results = await store.search("coll", [1.0, 0.0], limit=2)
        assert len(results) == 2
        assert results[0].id == "a"
        assert results[0].score == 1.0

    async def test_upsert_updates_existing_record(self) -> None:
        store = InMemoryVectorStore()
        await store.upsert(
            "coll",
            [VectorRecord(id="a", vector=[1.0, 0.0], metadata={"v": 1})],
        )
        await store.upsert(
            "coll",
            [VectorRecord(id="a", vector=[0.0, 1.0], metadata={"v": 2})],
        )
        results = await store.search("coll", [0.0, 1.0], limit=1)
        assert len(results) == 1
        assert results[0].id == "a"
        assert results[0].score == 1.0
        assert results[0].metadata["v"] == 2


@pytest.mark.asyncio
class TestInMemoryVectorStoreSearch:
    async def test_search_returns_results_sorted_by_similarity_score(
        self,
    ) -> None:
        store = InMemoryVectorStore()
        await store.upsert(
            "coll",
            [
                VectorRecord(id="low", vector=[0.0, 1.0], metadata={}),
                VectorRecord(id="high", vector=[1.0, 0.0], metadata={}),
                VectorRecord(id="mid", vector=[0.7, 0.7], metadata={}),
            ],
        )
        results = await store.search("coll", [1.0, 0.0], limit=3)
        assert [r.id for r in results] == ["high", "mid", "low"]
        assert results[0].score >= results[1].score >= results[2].score

    async def test_search_with_limit_truncates_results(self) -> None:
        store = InMemoryVectorStore()
        await store.upsert(
            "coll",
            [
                VectorRecord(id="best", vector=[1.0, 0.0], metadata={}),
                VectorRecord(id="mid", vector=[0.5, 0.5], metadata={}),
                VectorRecord(id="worst", vector=[0.0, 1.0], metadata={}),
            ],
        )
        results = await store.search("coll", [1.0, 0.0], limit=2)
        assert len(results) == 2
        assert results[0].id == "best"
        assert results[1].id == "mid"

    async def test_search_on_empty_collection_returns_empty_list(self) -> None:
        store = InMemoryVectorStore()
        results = await store.search("nonexistent", [1.0, 0.0])
        assert results == []


@pytest.mark.asyncio
class TestInMemoryVectorStoreDelete:
    async def test_delete_removes_records(self) -> None:
        store = InMemoryVectorStore()
        await store.upsert(
            "coll",
            [
                VectorRecord(id="a", vector=[1.0, 0.0], metadata={}),
                VectorRecord(id="b", vector=[0.0, 1.0], metadata={}),
            ],
        )
        removed = await store.delete("coll", ["a"])
        assert removed == 1
        results = await store.search("coll", [1.0, 0.0], limit=10)
        ids = [r.id for r in results]
        assert "a" not in ids
        assert "b" in ids


class TestVectorStoreConfig:
    def test_from_env_defaults(self) -> None:
        with patch.dict("os.environ", {}, clear=True):
            cfg = VectorStoreConfig.from_env()
            assert cfg.backend == "memory"
            assert cfg.qdrant_url == ""
            assert cfg.qdrant_api_key == ""

    def test_from_env_reads_env_vars(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "VECTOR_STORE_BACKEND": "qdrant",
                "QDRANT_URL": "http://localhost:6333",
                "QDRANT_API_KEY": "secret",
            },
            clear=True,
        ):
            cfg = VectorStoreConfig.from_env()
            assert cfg.backend == "qdrant"
            assert cfg.qdrant_url == "http://localhost:6333"
            assert cfg.qdrant_api_key == "secret"
