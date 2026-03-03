from __future__ import annotations

import pytest

from orchestrator.app.vector_store import resolve_collection_name


def test_resolve_collection_name_defaults_to_base() -> None:
    assert resolve_collection_name("service_embeddings", "tenant-a", False) == "service_embeddings"


def test_resolve_collection_name_appends_sanitized_tenant() -> None:
    resolved = resolve_collection_name(
        "service_embeddings",
        "tenant/a space",
        True,
    )
    assert resolved == "service_embeddings_tenant_a_space"


def test_graph_builder_resolve_vector_collection_uses_env_toggle(monkeypatch: pytest.MonkeyPatch) -> None:
    from orchestrator.app.graph_builder import resolve_vector_collection

    monkeypatch.setenv("QDRANT_PER_TENANT_COLLECTION", "true")
    assert resolve_vector_collection("t1") == "service_embeddings_t1"

