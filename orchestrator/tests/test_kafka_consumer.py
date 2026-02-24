import base64
import json
import os

import pytest

from orchestrator.app.config import KafkaConsumerConfig
from orchestrator.app.kafka_consumer import AsyncKafkaConsumer


class TestKafkaConsumerConfig:
    def test_from_env_uses_defaults_when_unset(self):
        for key in ("KAFKA_BROKERS", "KAFKA_TOPIC", "KAFKA_CONSUMER_GROUP"):
            os.environ.pop(key, None)
        config = KafkaConsumerConfig.from_env()
        assert config.brokers == "localhost:9092"
        assert config.topic == "raw-documents"
        assert config.group_id == "orchestrator-consumers"

    def test_from_env_uses_env_vars_when_set(self):
        os.environ["KAFKA_BROKERS"] = "kafka:9093"
        os.environ["KAFKA_TOPIC"] = "custom-topic"
        os.environ["KAFKA_CONSUMER_GROUP"] = "my-group"
        try:
            config = KafkaConsumerConfig.from_env()
            assert config.brokers == "kafka:9093"
            assert config.topic == "custom-topic"
            assert config.group_id == "my-group"
        finally:
            for key in ("KAFKA_BROKERS", "KAFKA_TOPIC", "KAFKA_CONSUMER_GROUP"):
                os.environ.pop(key, None)


class TestAsyncKafkaConsumer:
    @pytest.mark.asyncio
    async def test_process_message_decodes_base64_and_invokes_ingest(self):
        calls = []

        async def mock_ingest(raw_files):
            calls.append(raw_files)
            return {"status": "success", "entities_extracted": 1}

        content = "package main\nfunc main() {}"
        value = base64.b64encode(content.encode("utf-8"))
        headers = [("file_path", "cmd/main.go".encode("utf-8"))]

        config = KafkaConsumerConfig()
        consumer = AsyncKafkaConsumer(config, mock_ingest)
        result = await consumer.process_message(None, value, headers)

        assert result["status"] == "success"
        assert len(calls) == 1
        assert calls[0][0]["path"] == "cmd/main.go"
        assert calls[0][0]["content"] == content

    @pytest.mark.asyncio
    async def test_process_message_uses_key_as_file_path_when_headers_missing(self):
        calls = []

        async def mock_ingest(raw_files):
            calls.append(raw_files)
            return {"status": "ok"}

        value = base64.b64encode(b"content")
        key = b"pkg/file.go"

        config = KafkaConsumerConfig()
        consumer = AsyncKafkaConsumer(config, mock_ingest)
        await consumer.process_message(key, value, None)

        assert calls[0][0]["path"] == "pkg/file.go"
        assert calls[0][0]["content"] == "content"

    @pytest.mark.asyncio
    async def test_stop_sets_shutdown_flag(self):
        async def mock_ingest(raw_files):
            return {"status": "ok"}

        config = KafkaConsumerConfig()
        consumer = AsyncKafkaConsumer(config, mock_ingest)
        assert consumer._shutdown is False
        await consumer.stop()
        assert consumer._shutdown is True

    @pytest.mark.asyncio
    async def test_process_message_handles_decode_errors_gracefully(self):
        async def mock_ingest(raw_files):
            return {"status": "ok"}

        config = KafkaConsumerConfig()
        consumer = AsyncKafkaConsumer(config, mock_ingest)

        result = await consumer.process_message(None, b"not-valid-base64!!", None)
        assert result["status"] == "failed"
        assert "decode error" in result["error"].lower()
        assert "ok" not in str(result)

    @pytest.mark.asyncio
    async def test_process_message_handles_empty_value(self):
        async def mock_ingest(raw_files):
            return {"status": "ok"}

        config = KafkaConsumerConfig()
        consumer = AsyncKafkaConsumer(config, mock_ingest)

        result = await consumer.process_message(None, None, None)
        assert result["status"] == "failed"
        assert "empty" in result["error"].lower()


class TestAsyncKafkaConsumerParsedMessages:
    @pytest.mark.asyncio
    async def test_process_parsed_message_decodes_json_payload(self):
        calls = []

        async def mock_ingest(raw_files):
            calls.append(raw_files)
            return {"status": "success", "entities_extracted": 2}

        payload = json.dumps({
            "file_path": "k8s/auth-svc.yaml",
            "content": "apiVersion: v1\nkind: Service",
            "source_type": "k8s_manifest",
        }).encode()

        config = KafkaConsumerConfig()
        consumer = AsyncKafkaConsumer(config, mock_ingest)
        result = await consumer.process_parsed_message(payload)

        assert result["status"] == "success"
        assert len(calls) == 1
        assert calls[0][0]["path"] == "k8s/auth-svc.yaml"
        assert calls[0][0]["content"] == "apiVersion: v1\nkind: Service"

    @pytest.mark.asyncio
    async def test_process_parsed_message_rejects_none_value(self):
        async def mock_ingest(raw_files):
            return {"status": "ok"}

        config = KafkaConsumerConfig()
        consumer = AsyncKafkaConsumer(config, mock_ingest)
        result = await consumer.process_parsed_message(None)
        assert result["status"] == "failed"
        assert "empty" in result["error"]

    @pytest.mark.asyncio
    async def test_process_parsed_message_rejects_invalid_json(self):
        async def mock_ingest(raw_files):
            return {"status": "ok"}

        config = KafkaConsumerConfig()
        consumer = AsyncKafkaConsumer(config, mock_ingest)
        result = await consumer.process_parsed_message(b"not-json{{{")
        assert result["status"] == "failed"
        assert "json decode error" in result["error"]

    @pytest.mark.asyncio
    async def test_process_parsed_message_rejects_missing_content(self):
        async def mock_ingest(raw_files):
            return {"status": "ok"}

        payload = json.dumps({"file_path": "main.go"}).encode()
        config = KafkaConsumerConfig()
        consumer = AsyncKafkaConsumer(config, mock_ingest)
        result = await consumer.process_parsed_message(payload)
        assert result["status"] == "failed"
        assert "missing content" in result["error"]

    @pytest.mark.asyncio
    async def test_process_parsed_message_defaults_file_path(self):
        calls = []

        async def mock_ingest(raw_files):
            calls.append(raw_files)
            return {"status": "ok"}

        payload = json.dumps({"content": "some data"}).encode()
        config = KafkaConsumerConfig()
        consumer = AsyncKafkaConsumer(config, mock_ingest)
        result = await consumer.process_parsed_message(payload)

        assert result["status"] == "ok"
        assert calls[0][0]["path"] == "unknown"
