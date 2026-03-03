from __future__ import annotations

from unittest.mock import patch

import pytest

from orchestrator.app.config import VectorStoreConfig


def test_safe_create_subgraph_cache_returns_noop_on_failure() -> None:
    from orchestrator.app.query_engine import _safe_create_subgraph_cache

    with patch(
        "orchestrator.app.query_engine.create_subgraph_cache",
        side_effect=RuntimeError("boom"),
    ):
        cache = _safe_create_subgraph_cache()
    assert cache is not None
    assert cache.get("k") is None


def test_safe_create_vector_store_raises_in_production_on_init_failure() -> None:
    from orchestrator.app.query_engine import _safe_create_vector_store

    cfg = VectorStoreConfig(
        backend="qdrant",
        qdrant_url="http://localhost:6333",
        deployment_mode="production",
        shard_by_tenant=True,
    )
    with (
        patch("orchestrator.app.query_engine.VectorStoreConfig.from_env", return_value=cfg),
        patch("orchestrator.app.query_engine.create_vector_store", side_effect=RuntimeError("qdrant down")),
    ):
        with pytest.raises(RuntimeError):
            _safe_create_vector_store()
