from __future__ import annotations

from pathlib import Path

import yaml
import pytest


INFRA_DIR = Path(__file__).resolve().parents[2] / "infrastructure"
CONFIGMAP_PATH = INFRA_DIR / "k8s" / "configmap.yaml"
DEPLOYMENT_PATH = INFRA_DIR / "k8s" / "ingestion-worker-deployment.yaml"


@pytest.fixture(name="configmap_data")
def _configmap_data() -> dict[str, str]:
    content = CONFIGMAP_PATH.read_text(encoding="utf-8")
    doc = yaml.safe_load(content)
    return doc.get("data", {})


def _load_deployment_docs() -> list[dict]:
    content = DEPLOYMENT_PATH.read_text(encoding="utf-8")
    return list(yaml.safe_load_all(content))


def _find_deployment(docs: list[dict]) -> dict:
    for doc in docs:
        if doc and doc.get("kind") == "Deployment":
            return doc
    raise AssertionError("No Deployment document found")


class TestConfigMapProductionEnvVars:

    def test_max_inflight_present_and_positive(
        self, configmap_data: dict[str, str],
    ) -> None:
        assert "MAX_INFLIGHT" in configmap_data, (
            "MAX_INFLIGHT missing from ConfigMap; "
            "main.go validateInflightForProduction requires > 0 in production"
        )
        value = int(configmap_data["MAX_INFLIGHT"])
        assert value > 0, (
            f"MAX_INFLIGHT={value} but must be > 0 in production mode"
        )

    def test_blob_store_type_is_s3(
        self, configmap_data: dict[str, str],
    ) -> None:
        assert "BLOB_STORE_TYPE" in configmap_data, (
            "BLOB_STORE_TYPE missing from ConfigMap; "
            "main.go validateBlobStoreForProduction requires 's3' in production"
        )
        assert configmap_data["BLOB_STORE_TYPE"] == "s3", (
            f"BLOB_STORE_TYPE={configmap_data['BLOB_STORE_TYPE']!r} "
            f"but must be 's3' in production mode"
        )

    def test_dedup_store_type_not_noop(
        self, configmap_data: dict[str, str],
    ) -> None:
        assert "DEDUP_STORE_TYPE" in configmap_data, (
            "DEDUP_STORE_TYPE missing from ConfigMap; "
            "main.go validateDedupStoreForProduction rejects 'noop' in production"
        )
        assert configmap_data["DEDUP_STORE_TYPE"] != "noop", (
            "DEDUP_STORE_TYPE='noop' is unsafe for production mode"
        )

    def test_dlq_topic_present(
        self, configmap_data: dict[str, str],
    ) -> None:
        assert "DLQ_TOPIC" in configmap_data, (
            "DLQ_TOPIC missing from ConfigMap; "
            "required for Kafka DLQ sink in production"
        )
        assert configmap_data["DLQ_TOPIC"].strip(), (
            "DLQ_TOPIC must be a non-empty topic name"
        )

    def test_kafka_parsed_topic_present(
        self, configmap_data: dict[str, str],
    ) -> None:
        assert "KAFKA_PARSED_TOPIC" in configmap_data, (
            "KAFKA_PARSED_TOPIC missing from ConfigMap; "
            "required when PROCESSOR_MODE=kafka"
        )

    def test_blob_bucket_present(
        self, configmap_data: dict[str, str],
    ) -> None:
        assert "BLOB_BUCKET" in configmap_data, (
            "BLOB_BUCKET missing from ConfigMap; "
            "required when BLOB_STORE_TYPE=s3"
        )

    def test_dlq_fallback_path_absent(
        self, configmap_data: dict[str, str],
    ) -> None:
        assert "DLQ_FALLBACK_PATH" not in configmap_data, (
            "DLQ_FALLBACK_PATH must not be set in production ConfigMap; "
            "main.go validateDLQFallbackForProduction rejects it"
        )

    def test_deployment_mode_is_production(
        self, configmap_data: dict[str, str],
    ) -> None:
        assert configmap_data.get("DEPLOYMENT_MODE") == "production"


class TestIngestionWorkerDeploymentConfig:

    def test_deployment_references_configmap(self) -> None:
        deployment = _find_deployment(_load_deployment_docs())
        containers = deployment["spec"]["template"]["spec"]["containers"]
        env_from = containers[0].get("envFrom", [])
        configmap_refs = [
            ref["configMapRef"]["name"]
            for ref in env_from
            if "configMapRef" in ref
        ]
        assert "graphrag-config" in configmap_refs

    def test_processor_mode_is_kafka(self) -> None:
        deployment = _find_deployment(_load_deployment_docs())
        containers = deployment["spec"]["template"]["spec"]["containers"]
        env_list = containers[0].get("env", [])
        mode_entries = [
            e["value"] for e in env_list if e["name"] == "PROCESSOR_MODE"
        ]
        assert mode_entries == ["kafka"], (
            "PROCESSOR_MODE must be 'kafka' for production ingestion workers"
        )
