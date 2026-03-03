from __future__ import annotations

import pathlib

import pytest
import yaml

INFRA_DIR = pathlib.Path(__file__).resolve().parents[2] / "infrastructure" / "k8s"


class TestVaultAgentConfigExists:

    def test_vault_agent_config_file_exists(self) -> None:
        config_path = INFRA_DIR / "vault-agent-config.yaml"
        assert config_path.exists(), "vault-agent-config.yaml not found"

    def test_vault_agent_config_is_valid_yaml(self) -> None:
        config_path = INFRA_DIR / "vault-agent-config.yaml"
        docs = list(yaml.safe_load_all(config_path.read_text()))
        assert len(docs) >= 1

    def test_vault_agent_config_has_configmap(self) -> None:
        config_path = INFRA_DIR / "vault-agent-config.yaml"
        docs = list(yaml.safe_load_all(config_path.read_text()))
        kinds = [d.get("kind") for d in docs if d]
        assert "ConfigMap" in kinds

    def test_vault_agent_config_in_graphrag_namespace(self) -> None:
        config_path = INFRA_DIR / "vault-agent-config.yaml"
        docs = list(yaml.safe_load_all(config_path.read_text()))
        for doc in docs:
            if doc and doc.get("kind") == "ConfigMap":
                assert doc["metadata"]["namespace"] == "graphrag"


class TestOrchestratorVaultAnnotations:

    @pytest.fixture()
    def orchestrator_deployment(self) -> dict:
        deploy_path = INFRA_DIR / "orchestrator-deployment.yaml"
        docs = list(yaml.safe_load_all(deploy_path.read_text()))
        for doc in docs:
            if doc and doc.get("kind") == "Deployment":
                return doc
        pytest.fail("No Deployment found in orchestrator-deployment.yaml")

    def test_vault_agent_inject_annotation(
        self, orchestrator_deployment: dict,
    ) -> None:
        annotations = orchestrator_deployment["spec"]["template"]["metadata"]["annotations"]
        assert annotations.get("vault.hashicorp.com/agent-inject") == "true"

    def test_vault_role_annotation(
        self, orchestrator_deployment: dict,
    ) -> None:
        annotations = orchestrator_deployment["spec"]["template"]["metadata"]["annotations"]
        assert "vault.hashicorp.com/role" in annotations

    def test_vault_agent_inject_secret_annotation(
        self, orchestrator_deployment: dict,
    ) -> None:
        annotations = orchestrator_deployment["spec"]["template"]["metadata"]["annotations"]
        secret_keys = [
            k for k in annotations
            if k.startswith("vault.hashicorp.com/agent-inject-secret-")
        ]
        assert len(secret_keys) >= 1


class TestIngestionWorkerVaultAnnotations:

    @pytest.fixture()
    def ingestion_deployment(self) -> dict:
        deploy_path = INFRA_DIR / "ingestion-worker-deployment.yaml"
        docs = list(yaml.safe_load_all(deploy_path.read_text()))
        for doc in docs:
            if doc and doc.get("kind") == "Deployment":
                return doc
        pytest.fail("No Deployment found in ingestion-worker-deployment.yaml")

    def test_vault_agent_inject_annotation(
        self, ingestion_deployment: dict,
    ) -> None:
        annotations = ingestion_deployment["spec"]["template"]["metadata"]["annotations"]
        assert annotations.get("vault.hashicorp.com/agent-inject") == "true"

    def test_vault_role_annotation(
        self, ingestion_deployment: dict,
    ) -> None:
        annotations = ingestion_deployment["spec"]["template"]["metadata"]["annotations"]
        assert "vault.hashicorp.com/role" in annotations

    def test_vault_agent_inject_secret_annotation(
        self, ingestion_deployment: dict,
    ) -> None:
        annotations = ingestion_deployment["spec"]["template"]["metadata"]["annotations"]
        secret_keys = [
            k for k in annotations
            if k.startswith("vault.hashicorp.com/agent-inject-secret-")
        ]
        assert len(secret_keys) >= 1
