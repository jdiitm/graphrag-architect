from __future__ import annotations

import pytest

from orchestrator.app.tenant_isolation import (
    QdrantShardingRequiredInProductionError,
    validate_vector_shard_enforcement,
)
from orchestrator.app.vector_store import QdrantVectorStore, create_vector_store


class TestVectorShardEnforcement:
    def test_validate_raises_in_production_without_sharding(self) -> None:
        with pytest.raises(QdrantShardingRequiredInProductionError):
            validate_vector_shard_enforcement(
                deployment_mode="production",
                backend="qdrant",
                shard_by_tenant=False,
            )

    def test_validate_allows_in_production_with_sharding(self) -> None:
        validate_vector_shard_enforcement(
            deployment_mode="production",
            backend="qdrant",
            shard_by_tenant=True,
        )

    def test_create_vector_store_rejects_production_without_sharding(self) -> None:
        with pytest.raises(QdrantShardingRequiredInProductionError):
            create_vector_store(
                backend="qdrant",
                url="http://localhost:6333",
                deployment_mode="production",
                shard_by_tenant=False,
            )

    def test_create_vector_store_allows_production_with_sharding(self) -> None:
        store = create_vector_store(
            backend="qdrant",
            url="http://localhost:6333",
            deployment_mode="production",
            shard_by_tenant=True,
        )
        assert isinstance(store, QdrantVectorStore)
