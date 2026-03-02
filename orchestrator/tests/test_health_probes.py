from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from starlette.testclient import TestClient

from orchestrator.app.main import app


@pytest.fixture(name="client")
def fixture_client():
    return TestClient(app, raise_server_exceptions=False)


class TestLivenessProbe:
    def test_returns_200(self, client):
        response = client.get("/v1/health/live")
        assert response.status_code == 200

    def test_returns_alive_status(self, client):
        response = client.get("/v1/health/live")
        body = response.json()
        assert body["status"] == "alive"


class TestReadinessProbeAllHealthy:
    def test_returns_200_when_deps_healthy(self, client):
        mock_driver = MagicMock()
        mock_session = AsyncMock()
        mock_session.run = AsyncMock(return_value=MagicMock())
        mock_driver.session.return_value.__aenter__ = AsyncMock(
            return_value=mock_session,
        )
        mock_driver.session.return_value.__aexit__ = AsyncMock(
            return_value=False,
        )

        with patch(
            "orchestrator.app.main.get_driver", return_value=mock_driver,
        ), patch(
            "orchestrator.app.main._kafka_consumer_enabled", return_value=False,
        ):
            response = client.get("/v1/health/ready")

        assert response.status_code == 200

    def test_returns_ready_status(self, client):
        mock_driver = MagicMock()
        mock_session = AsyncMock()
        mock_session.run = AsyncMock(return_value=MagicMock())
        mock_driver.session.return_value.__aenter__ = AsyncMock(
            return_value=mock_session,
        )
        mock_driver.session.return_value.__aexit__ = AsyncMock(
            return_value=False,
        )

        with patch(
            "orchestrator.app.main.get_driver", return_value=mock_driver,
        ), patch(
            "orchestrator.app.main._kafka_consumer_enabled", return_value=False,
        ):
            response = client.get("/v1/health/ready")

        body = response.json()
        assert body["status"] == "ready"

    def test_includes_neo4j_check(self, client):
        mock_driver = MagicMock()
        mock_session = AsyncMock()
        mock_session.run = AsyncMock(return_value=MagicMock())
        mock_driver.session.return_value.__aenter__ = AsyncMock(
            return_value=mock_session,
        )
        mock_driver.session.return_value.__aexit__ = AsyncMock(
            return_value=False,
        )

        with patch(
            "orchestrator.app.main.get_driver", return_value=mock_driver,
        ), patch(
            "orchestrator.app.main._kafka_consumer_enabled", return_value=False,
        ):
            response = client.get("/v1/health/ready")

        body = response.json()
        assert body["checks"]["neo4j"] == "ok"


class TestReadinessProbeNeo4jDown:
    def test_returns_503_when_neo4j_unavailable(self, client):
        with patch(
            "orchestrator.app.main.get_driver",
            side_effect=RuntimeError("Neo4j driver not initialized"),
        ), patch(
            "orchestrator.app.main._kafka_consumer_enabled", return_value=False,
        ):
            response = client.get("/v1/health/ready")

        assert response.status_code == 503

    def test_returns_degraded_status(self, client):
        with patch(
            "orchestrator.app.main.get_driver",
            side_effect=RuntimeError("Neo4j driver not initialized"),
        ), patch(
            "orchestrator.app.main._kafka_consumer_enabled", return_value=False,
        ):
            response = client.get("/v1/health/ready")

        body = response.json()
        assert body["status"] == "degraded"

    def test_neo4j_check_shows_unavailable(self, client):
        with patch(
            "orchestrator.app.main.get_driver",
            side_effect=RuntimeError("Neo4j driver not initialized"),
        ), patch(
            "orchestrator.app.main._kafka_consumer_enabled", return_value=False,
        ):
            response = client.get("/v1/health/ready")

        body = response.json()
        assert body["checks"]["neo4j"] == "unavailable"


class TestReadinessProbeKafkaDown:
    def test_returns_503_when_kafka_unavailable(self, client):
        mock_driver = MagicMock()
        mock_session = AsyncMock()
        mock_session.run = AsyncMock(return_value=MagicMock())
        mock_driver.session.return_value.__aenter__ = AsyncMock(
            return_value=mock_session,
        )
        mock_driver.session.return_value.__aexit__ = AsyncMock(
            return_value=False,
        )

        with patch(
            "orchestrator.app.main.get_driver", return_value=mock_driver,
        ), patch(
            "orchestrator.app.main._kafka_consumer_enabled", return_value=True,
        ), patch.dict(
            "orchestrator.app.main._STATE",
            {"kafka_consumer": None, "kafka_task": None, "semaphore": None},
        ):
            response = client.get("/v1/health/ready")

        assert response.status_code == 503

    def test_kafka_check_shows_unavailable(self, client):
        mock_driver = MagicMock()
        mock_session = AsyncMock()
        mock_session.run = AsyncMock(return_value=MagicMock())
        mock_driver.session.return_value.__aenter__ = AsyncMock(
            return_value=mock_session,
        )
        mock_driver.session.return_value.__aexit__ = AsyncMock(
            return_value=False,
        )

        with patch(
            "orchestrator.app.main.get_driver", return_value=mock_driver,
        ), patch(
            "orchestrator.app.main._kafka_consumer_enabled", return_value=True,
        ), patch.dict(
            "orchestrator.app.main._STATE",
            {"kafka_consumer": None, "kafka_task": None, "semaphore": None},
        ):
            response = client.get("/v1/health/ready")

        body = response.json()
        assert body["checks"]["kafka"] == "unavailable"


class TestReadinessProbeKafkaDisabled:
    def test_kafka_disabled_still_returns_200(self, client):
        mock_driver = MagicMock()
        mock_session = AsyncMock()
        mock_session.run = AsyncMock(return_value=MagicMock())
        mock_driver.session.return_value.__aenter__ = AsyncMock(
            return_value=mock_session,
        )
        mock_driver.session.return_value.__aexit__ = AsyncMock(
            return_value=False,
        )

        with patch(
            "orchestrator.app.main.get_driver", return_value=mock_driver,
        ), patch(
            "orchestrator.app.main._kafka_consumer_enabled", return_value=False,
        ):
            response = client.get("/v1/health/ready")

        assert response.status_code == 200
        body = response.json()
        assert body["checks"]["kafka"] == "disabled"
