from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
SCHEMA_PATH = REPO_ROOT / "infrastructure" / "schema-registry" / "ingestion-message.avsc"
COMPOSE_PATH = REPO_ROOT / "infrastructure" / "docker-compose.yml"

REQUIRED_FIELDS = {"file_path", "content", "source_type", "tenant_id", "commit_sha"}

VALID_MESSAGE: Dict[str, Any] = {
    "file_path": "src/main.go",
    "content": "package main",
    "source_type": "git",
    "tenant_id": "tenant-1",
    "commit_sha": "abc123def456",
}


class TestAvroSchemaFileExists:

    def test_schema_file_exists(self) -> None:
        assert SCHEMA_PATH.exists(), f"Avro schema not found at {SCHEMA_PATH}"

    def test_schema_is_valid_avro_record(self) -> None:
        schema = json.loads(SCHEMA_PATH.read_text())
        assert schema["type"] == "record"
        assert schema["name"] == "IngestionMessage"


class TestAvroSchemaFields:

    def test_has_all_required_fields(self) -> None:
        schema = json.loads(SCHEMA_PATH.read_text())
        field_names = {f["name"] for f in schema["fields"]}
        assert REQUIRED_FIELDS.issubset(field_names)

    def test_commit_sha_nullable_for_backward_compat(self) -> None:
        schema = json.loads(SCHEMA_PATH.read_text())
        commit_sha_field = next(
            f for f in schema["fields"] if f["name"] == "commit_sha"
        )
        field_type = commit_sha_field["type"]
        assert isinstance(field_type, list)
        assert "null" in field_type


class TestSchemaCompatibilityMode:

    def test_compatibility_mode_is_backward(self) -> None:
        from orchestrator.app.schema_registry_client import SchemaRegistryClient

        client = SchemaRegistryClient()
        assert client.compatibility_mode == "BACKWARD"


class TestDockerComposeSchemaRegistry:

    def test_schema_registry_service_in_compose(self) -> None:
        compose = yaml.safe_load(COMPOSE_PATH.read_text())
        assert "schema-registry" in compose["services"]

    def test_schema_registry_depends_on_kafka(self) -> None:
        compose = yaml.safe_load(COMPOSE_PATH.read_text())
        sr_service = compose["services"]["schema-registry"]
        assert "kafka" in sr_service.get("depends_on", [])


class TestSchemaRegistryClientValidation:

    def test_validate_message_method_exists(self) -> None:
        from orchestrator.app.schema_registry_client import SchemaRegistryClient

        client = SchemaRegistryClient()
        assert hasattr(client, "validate_message")
        assert callable(client.validate_message)

    def test_valid_message_accepted(self) -> None:
        from orchestrator.app.schema_registry_client import SchemaRegistryClient

        client = SchemaRegistryClient()
        assert client.validate_message(VALID_MESSAGE) is True

    def test_valid_message_with_null_commit_sha(self) -> None:
        from orchestrator.app.schema_registry_client import SchemaRegistryClient

        client = SchemaRegistryClient()
        msg = {**VALID_MESSAGE, "commit_sha": None}
        assert client.validate_message(msg) is True


class TestInvalidMessageRejection:

    def test_missing_file_path_rejected(self) -> None:
        from orchestrator.app.schema_registry_client import (
            SchemaRegistryClient,
            SchemaValidationError,
        )

        client = SchemaRegistryClient()
        msg = {k: v for k, v in VALID_MESSAGE.items() if k != "file_path"}
        with pytest.raises(SchemaValidationError):
            client.validate_message(msg)

    def test_missing_content_rejected(self) -> None:
        from orchestrator.app.schema_registry_client import (
            SchemaRegistryClient,
            SchemaValidationError,
        )

        client = SchemaRegistryClient()
        msg = {k: v for k, v in VALID_MESSAGE.items() if k != "content"}
        with pytest.raises(SchemaValidationError):
            client.validate_message(msg)

    def test_wrong_type_rejected(self) -> None:
        from orchestrator.app.schema_registry_client import (
            SchemaRegistryClient,
            SchemaValidationError,
        )

        client = SchemaRegistryClient()
        msg = {**VALID_MESSAGE, "file_path": 12345}
        with pytest.raises(SchemaValidationError):
            client.validate_message(msg)

    def test_empty_message_rejected(self) -> None:
        from orchestrator.app.schema_registry_client import (
            SchemaRegistryClient,
            SchemaValidationError,
        )

        client = SchemaRegistryClient()
        with pytest.raises(SchemaValidationError):
            client.validate_message({})
