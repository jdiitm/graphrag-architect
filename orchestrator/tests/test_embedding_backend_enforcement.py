import logging

import pytest

from orchestrator.app.graph_embeddings import (
    ConfigurationError,
    LocalEmbeddingBackend,
    create_embedding_backend,
)


def test_production_rejects_local_backend(monkeypatch):
    monkeypatch.setenv("GRAPHRAG_ENV", "production")
    monkeypatch.setenv("EMBEDDING_BACKEND", "local")
    with pytest.raises(ConfigurationError, match="not supported in production"):
        create_embedding_backend(backend_type="local")


def test_dev_allows_local_backend(monkeypatch):
    monkeypatch.setenv("GRAPHRAG_ENV", "development")
    monkeypatch.setenv("EMBEDDING_BACKEND", "local")
    backend = create_embedding_backend(backend_type="local")
    assert isinstance(backend, LocalEmbeddingBackend)


def test_factory_creates_correct_backend(monkeypatch):
    monkeypatch.delenv("GRAPHRAG_ENV", raising=False)
    monkeypatch.delenv("EMBEDDING_BACKEND", raising=False)
    backend = create_embedding_backend(backend_type="local")
    assert isinstance(backend, LocalEmbeddingBackend)


def test_local_backend_emits_deprecation_warning(monkeypatch, caplog):
    monkeypatch.setenv("GRAPHRAG_ENV", "development")
    monkeypatch.setenv("EMBEDDING_BACKEND", "local")
    with caplog.at_level(logging.WARNING, logger="orchestrator.app.graph_embeddings"):
        create_embedding_backend(backend_type="local")
    assert any("deprecated" in record.message.lower() for record in caplog.records)
