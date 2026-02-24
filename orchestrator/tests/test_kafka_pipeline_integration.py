from __future__ import annotations

import asyncio
import json
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.app.config import KafkaConsumerConfig


_ENV_VARS = {
    "NEO4J_PASSWORD": "test",
    "NEO4J_URI": "bolt://localhost:7687",
    "GOOGLE_API_KEY": "test-key",
    "KAFKA_CONSUMER_ENABLED": "true",
    "KAFKA_BROKERS": "localhost:9092",
    "KAFKA_TOPIC": "graphrag.parsed",
}


class FakeMessage:
    def __init__(self, key: Optional[bytes], value: bytes, headers=None):
        self.key = key
        self.value = value
        self.headers = headers or []


class TestKafkaConsumerLifecycleWiring:

    @pytest.mark.asyncio
    async def test_lifespan_starts_kafka_consumer_when_enabled(self) -> None:
        from orchestrator.app.main import app

        started = {"called": False}

        original_start = None

        async def _mock_start(self_consumer) -> None:
            started["called"] = True

        with (
            patch.dict("os.environ", _ENV_VARS),
            patch(
                "orchestrator.app.main.init_driver",
            ),
            patch(
                "orchestrator.app.main.close_driver",
                new_callable=AsyncMock,
            ),
            patch(
                "orchestrator.app.main.init_checkpointer",
            ),
            patch(
                "orchestrator.app.main.close_checkpointer",
            ),
            patch(
                "orchestrator.app.main.configure_telemetry",
            ),
            patch(
                "orchestrator.app.main.configure_metrics",
            ),
            patch(
                "orchestrator.app.main.shutdown_pool",
            ),
            patch(
                "orchestrator.app.kafka_consumer.AsyncKafkaConsumer.start",
                _mock_start,
            ),
        ):
            async with app.router.lifespan_context(app):
                await asyncio.sleep(0.05)

        assert started["called"], (
            "AsyncKafkaConsumer.start() was never called during lifespan. "
            "The consumer must be started as a background task when "
            "KAFKA_CONSUMER_ENABLED=true."
        )

    @pytest.mark.asyncio
    async def test_lifespan_does_not_start_consumer_when_disabled(self) -> None:
        from orchestrator.app.main import app

        started = {"called": False}

        async def _mock_start(self_consumer) -> None:
            started["called"] = True

        env = {**_ENV_VARS, "KAFKA_CONSUMER_ENABLED": "false"}
        with (
            patch.dict("os.environ", env),
            patch("orchestrator.app.main.init_driver"),
            patch("orchestrator.app.main.close_driver", new_callable=AsyncMock),
            patch("orchestrator.app.main.init_checkpointer"),
            patch("orchestrator.app.main.close_checkpointer"),
            patch("orchestrator.app.main.configure_telemetry"),
            patch("orchestrator.app.main.configure_metrics"),
            patch("orchestrator.app.main.shutdown_pool"),
            patch(
                "orchestrator.app.kafka_consumer.AsyncKafkaConsumer.start",
                _mock_start,
            ),
        ):
            async with app.router.lifespan_context(app):
                await asyncio.sleep(0.05)

        assert not started["called"], (
            "Consumer should NOT start when KAFKA_CONSUMER_ENABLED=false"
        )


class TestKafkaConsumerIngestIntegration:

    @pytest.mark.asyncio
    async def test_parsed_message_triggers_ingestion_graph(self) -> None:
        from orchestrator.app.kafka_consumer import AsyncKafkaConsumer

        invoked_states: List[Dict[str, Any]] = []

        async def _fake_ingest(raw_files):
            invoked_states.append(raw_files)
            return {"commit_status": "success", "entities_extracted": 1}

        payload = json.dumps({
            "file_path": "k8s/deployment.yaml",
            "content": "apiVersion: apps/v1\nkind: Deployment",
            "source_type": "k8s_manifest",
        }).encode()

        config = KafkaConsumerConfig(topic="graphrag.parsed")
        consumer = AsyncKafkaConsumer(config, _fake_ingest)
        result = await consumer.process_parsed_message(payload)

        assert result["commit_status"] == "success"
        assert len(invoked_states) == 1
        assert invoked_states[0][0]["path"] == "k8s/deployment.yaml"


class TestIngestEndpointDeprecation:

    @pytest.mark.asyncio
    async def test_ingest_endpoint_includes_deprecation_header(self) -> None:
        from httpx import ASGITransport, AsyncClient
        from orchestrator.app.main import app

        fake_result = {
            "commit_status": "success",
            "extracted_nodes": [],
            "extraction_errors": [],
        }

        with (
            patch.dict("os.environ", _ENV_VARS),
            patch("orchestrator.app.main.init_driver"),
            patch("orchestrator.app.main.close_driver", new_callable=AsyncMock),
            patch("orchestrator.app.main.init_checkpointer"),
            patch("orchestrator.app.main.close_checkpointer"),
            patch("orchestrator.app.main.configure_telemetry"),
            patch("orchestrator.app.main.configure_metrics"),
            patch("orchestrator.app.main.shutdown_pool"),
            patch(
                "orchestrator.app.main.ingestion_graph",
                **{"ainvoke": AsyncMock(return_value=fake_result)},
            ),
        ):
            import base64
            doc_content = base64.b64encode(b"package main").decode()
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                response = await client.post(
                    "/ingest",
                    json={"documents": [{
                        "file_path": "main.go",
                        "content": doc_content,
                        "source_type": "source_code",
                    }]},
                )

        assert response.status_code == 200
        deprecation = response.headers.get("Deprecation")
        assert deprecation is not None, (
            "/ingest endpoint must return a Deprecation header. "
            "The Kafka pipeline is the primary ingestion path."
        )
