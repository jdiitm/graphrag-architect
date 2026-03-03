import pytest

from orchestrator.app.config import ConfigurationError, ProductionConfigValidator


class TestConfigurationError:

    def test_inherits_from_exception(self) -> None:
        assert issubclass(ConfigurationError, Exception)

    def test_carries_message(self) -> None:
        error = ConfigurationError("broken config")
        assert str(error) == "broken config"


class TestProductionConfigValidatorFromEnv:

    def test_defaults_to_dev_mode(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("DEPLOYMENT_MODE", raising=False)
        monkeypatch.delenv("REDIS_URL", raising=False)
        monkeypatch.delenv("NEO4J_PASSWORD", raising=False)
        monkeypatch.delenv("VECTOR_SYNC_BACKEND", raising=False)
        validator = ProductionConfigValidator.from_env()
        assert validator.deployment_mode == "dev"

    def test_resolves_redis_ast_dlq_backend(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("REDIS_URL", "redis://localhost:6379")
        validator = ProductionConfigValidator.from_env()
        assert validator.ast_dlq_backend == "redis"

    def test_resolves_memory_ast_dlq_when_no_redis(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.delenv("REDIS_URL", raising=False)
        validator = ProductionConfigValidator.from_env()
        assert validator.ast_dlq_backend == "memory"

    def test_resolves_neo4j_outbox_backend(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("NEO4J_PASSWORD", "secret")
        monkeypatch.delenv("REDIS_URL", raising=False)
        validator = ProductionConfigValidator.from_env()
        assert validator.outbox_backend == "neo4j"

    def test_resolves_redis_outbox_when_no_neo4j(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.delenv("NEO4J_PASSWORD", raising=False)
        monkeypatch.setenv("REDIS_URL", "redis://localhost:6379")
        validator = ProductionConfigValidator.from_env()
        assert validator.outbox_backend == "redis"

    def test_resolves_memory_outbox_when_nothing_configured(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.delenv("NEO4J_PASSWORD", raising=False)
        monkeypatch.delenv("REDIS_URL", raising=False)
        validator = ProductionConfigValidator.from_env()
        assert validator.outbox_backend == "memory"

    def test_resolves_vector_sync_backend_from_env(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("VECTOR_SYNC_BACKEND", "kafka")
        validator = ProductionConfigValidator.from_env()
        assert validator.vector_sync_backend == "kafka"

    def test_vector_sync_defaults_to_memory(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.delenv("VECTOR_SYNC_BACKEND", raising=False)
        validator = ProductionConfigValidator.from_env()
        assert validator.vector_sync_backend == "memory"


class TestProductionInvariantValidation:

    def test_dev_mode_allows_all_memory_backends(self) -> None:
        validator = ProductionConfigValidator(
            deployment_mode="dev",
            ast_dlq_backend="memory",
            outbox_backend="memory",
            vector_sync_backend="memory",
        )
        validator.validate_production_invariants()

    def test_production_rejects_memory_ast_dlq(self) -> None:
        validator = ProductionConfigValidator(
            deployment_mode="production",
            ast_dlq_backend="memory",
            outbox_backend="neo4j",
            vector_sync_backend="kafka",
        )
        with pytest.raises(ConfigurationError, match="AST_DLQ_BACKEND"):
            validator.validate_production_invariants()

    def test_production_rejects_memory_outbox(self) -> None:
        validator = ProductionConfigValidator(
            deployment_mode="production",
            ast_dlq_backend="redis",
            outbox_backend="memory",
            vector_sync_backend="kafka",
        )
        with pytest.raises(ConfigurationError, match="OUTBOX_BACKEND"):
            validator.validate_production_invariants()

    def test_production_rejects_memory_vector_sync(self) -> None:
        validator = ProductionConfigValidator(
            deployment_mode="production",
            ast_dlq_backend="redis",
            outbox_backend="neo4j",
            vector_sync_backend="memory",
        )
        with pytest.raises(ConfigurationError, match="VECTOR_SYNC_BACKEND"):
            validator.validate_production_invariants()

    def test_production_collects_all_violations(self) -> None:
        validator = ProductionConfigValidator(
            deployment_mode="production",
            ast_dlq_backend="memory",
            outbox_backend="memory",
            vector_sync_backend="memory",
        )
        with pytest.raises(ConfigurationError) as exc_info:
            validator.validate_production_invariants()
        message = str(exc_info.value)
        assert "AST_DLQ_BACKEND" in message
        assert "OUTBOX_BACKEND" in message
        assert "VECTOR_SYNC_BACKEND" in message

    def test_production_with_redis_satisfies_dlq_and_outbox(self) -> None:
        validator = ProductionConfigValidator(
            deployment_mode="production",
            ast_dlq_backend="redis",
            outbox_backend="redis",
            vector_sync_backend="kafka",
        )
        validator.validate_production_invariants()

    def test_production_with_neo4j_outbox_and_redis_dlq(self) -> None:
        validator = ProductionConfigValidator(
            deployment_mode="production",
            ast_dlq_backend="redis",
            outbox_backend="neo4j",
            vector_sync_backend="redis",
        )
        validator.validate_production_invariants()

    def test_production_neo4j_outbox_still_rejects_memory_dlq(self) -> None:
        validator = ProductionConfigValidator(
            deployment_mode="production",
            ast_dlq_backend="memory",
            outbox_backend="neo4j",
            vector_sync_backend="kafka",
        )
        with pytest.raises(ConfigurationError) as exc_info:
            validator.validate_production_invariants()
        message = str(exc_info.value)
        assert "AST_DLQ_BACKEND" in message
        assert "OUTBOX_BACKEND" not in message

    def test_all_durable_production_passes(self) -> None:
        validator = ProductionConfigValidator(
            deployment_mode="production",
            ast_dlq_backend="redis",
            outbox_backend="neo4j",
            vector_sync_backend="kafka",
        )
        validator.validate_production_invariants()

    def test_frozen_dataclass(self) -> None:
        validator = ProductionConfigValidator(
            deployment_mode="dev",
            ast_dlq_backend="memory",
            outbox_backend="memory",
            vector_sync_backend="memory",
        )
        with pytest.raises(AttributeError):
            validator.deployment_mode = "production"  # type: ignore[misc]


class TestProductionConfigEndToEnd:

    def test_production_all_memory_via_env_raises(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("DEPLOYMENT_MODE", "production")
        monkeypatch.delenv("REDIS_URL", raising=False)
        monkeypatch.delenv("NEO4J_PASSWORD", raising=False)
        monkeypatch.setenv("VECTOR_SYNC_BACKEND", "memory")
        with pytest.raises(ConfigurationError):
            ProductionConfigValidator.from_env().validate_production_invariants()

    def test_production_fully_configured_via_env_passes(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("DEPLOYMENT_MODE", "production")
        monkeypatch.setenv("REDIS_URL", "redis://localhost:6379")
        monkeypatch.setenv("NEO4J_PASSWORD", "secret")
        monkeypatch.setenv("VECTOR_SYNC_BACKEND", "kafka")
        monkeypatch.setenv("VECTOR_STORE_BACKEND", "qdrant")
        monkeypatch.setenv("QDRANT_SHARD_BY_TENANT", "true")
        ProductionConfigValidator.from_env().validate_production_invariants()

    def test_dev_all_memory_via_env_passes(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("DEPLOYMENT_MODE", "dev")
        monkeypatch.delenv("REDIS_URL", raising=False)
        monkeypatch.delenv("NEO4J_PASSWORD", raising=False)
        monkeypatch.setenv("VECTOR_SYNC_BACKEND", "memory")
        ProductionConfigValidator.from_env().validate_production_invariants()

    def test_production_qdrant_without_sharding_raises(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("DEPLOYMENT_MODE", "production")
        monkeypatch.setenv("REDIS_URL", "redis://localhost:6379")
        monkeypatch.setenv("NEO4J_PASSWORD", "secret")
        monkeypatch.setenv("VECTOR_SYNC_BACKEND", "kafka")
        monkeypatch.setenv("VECTOR_STORE_BACKEND", "qdrant")
        monkeypatch.setenv("QDRANT_SHARD_BY_TENANT", "false")
        with pytest.raises(ConfigurationError, match="QDRANT_SHARD_BY_TENANT"):
            ProductionConfigValidator.from_env().validate_production_invariants()
