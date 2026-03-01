from unittest.mock import MagicMock

import pytest

from orchestrator.app.graph_builder import create_outbox_drainer
from orchestrator.app.vector_sync_outbox import DurableOutboxDrainer, OutboxDrainer


class TestOutboxProductionGuard:

    def test_production_no_durable_store_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DEPLOYMENT_MODE", "production")
        with pytest.raises(SystemExit):
            create_outbox_drainer(
                redis_conn=None, vector_store=MagicMock(), neo4j_driver=None,
            )

    def test_production_with_redis_returns_durable(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("DEPLOYMENT_MODE", "production")
        drainer = create_outbox_drainer(
            redis_conn=MagicMock(), vector_store=MagicMock(), neo4j_driver=None,
        )
        assert isinstance(drainer, DurableOutboxDrainer)

    def test_production_with_neo4j_returns_durable(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("DEPLOYMENT_MODE", "production")
        drainer = create_outbox_drainer(
            redis_conn=None, vector_store=MagicMock(), neo4j_driver=MagicMock(),
        )
        assert isinstance(drainer, DurableOutboxDrainer)

    def test_dev_no_durable_store_returns_in_memory(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("DEPLOYMENT_MODE", "dev")
        drainer = create_outbox_drainer(
            redis_conn=None, vector_store=MagicMock(), neo4j_driver=None,
        )
        assert isinstance(drainer, OutboxDrainer)
        assert not isinstance(drainer, DurableOutboxDrainer)

    def test_unset_mode_allows_in_memory(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("DEPLOYMENT_MODE", raising=False)
        drainer = create_outbox_drainer(
            redis_conn=None, vector_store=MagicMock(), neo4j_driver=None,
        )
        assert isinstance(drainer, OutboxDrainer)
