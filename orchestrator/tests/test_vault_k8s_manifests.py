from __future__ import annotations

from pathlib import Path

import pytest
import yaml

INFRA_DIR = Path(__file__).resolve().parents[2] / "infrastructure"
VAULT_AGENT_CONFIG_PATH = INFRA_DIR / "k8s" / "vault-agent-config.yaml"
ORCHESTRATOR_DEPLOYMENT_PATH = INFRA_DIR / "k8s" / "orchestrator-deployment.yaml"
INGESTION_WORKER_DEPLOYMENT_PATH = INFRA_DIR / "k8s" / "ingestion-worker-deployment.yaml"


class TestVaultAgentConfigExists:

    def test_vault_agent_config_file_exists(self) -> None:
        assert VAULT_AGENT_CONFIG_PATH.exists(), (
            "infrastructure/k8s/vault-agent-config.yaml must exist"
        )

    def test_vault_agent_config_is_valid_yaml(self) -> None:
        content = VAULT_AGENT_CONFIG_PATH.read_text(encoding="utf-8")
        doc = yaml.safe_load(content)
        assert doc is not None

    def test_vault_agent_config_is_configmap(self) -> None:
        content = VAULT_AGENT_CONFIG_PATH.read_text(encoding="utf-8")
        doc = yaml.safe_load(content)
        assert doc["kind"] == "ConfigMap"
        assert doc["metadata"]["name"] == "vault-agent-config"

    def test_vault_agent_config_has_hcl_template(self) -> None:
        content = VAULT_AGENT_CONFIG_PATH.read_text(encoding="utf-8")
        doc = yaml.safe_load(content)
        hcl = doc["data"]["vault-agent-config.hcl"]
        assert "auto_auth" in hcl
        assert "kubernetes" in hcl
        assert "template" in hcl

    def test_vault_agent_renders_required_secrets(self) -> None:
        content = VAULT_AGENT_CONFIG_PATH.read_text(encoding="utf-8")
        doc = yaml.safe_load(content)
        hcl = doc["data"]["vault-agent-config.hcl"]
        required_keys = [
            "NEO4J_PASSWORD",
            "GOOGLE_API_KEY",
            "AUTH_TOKEN_SECRET",
            "ANTHROPIC_API_KEY",
        ]
        for key in required_keys:
            assert key in hcl, (
                f"Vault agent template must render {key}"
            )


def _load_deployment(path: Path) -> dict:
    docs = list(yaml.safe_load_all(path.read_text(encoding="utf-8")))
    for doc in docs:
        if doc and doc.get("kind") == "Deployment":
            return doc
    pytest.fail(f"No Deployment found in {path.name}")


def _get_pod_annotations(deployment: dict) -> dict:
    return (
        deployment.get("spec", {})
        .get("template", {})
        .get("metadata", {})
        .get("annotations", {})
    )


class TestOrchestratorVaultAnnotations:

    @pytest.fixture(name="deployment")
    def _deployment(self) -> dict:
        return _load_deployment(ORCHESTRATOR_DEPLOYMENT_PATH)

    def test_vault_agent_inject_annotation(self, deployment: dict) -> None:
        annotations = _get_pod_annotations(deployment)
        assert annotations.get("vault.hashicorp.com/agent-inject") == "true", (
            "Orchestrator must have vault.hashicorp.com/agent-inject: 'true'"
        )

    def test_vault_role_annotation(self, deployment: dict) -> None:
        annotations = _get_pod_annotations(deployment)
        assert annotations.get("vault.hashicorp.com/role") == "graphrag", (
            "Orchestrator must have vault.hashicorp.com/role: 'graphrag'"
        )

    def test_vault_agent_configmap_annotation(self, deployment: dict) -> None:
        annotations = _get_pod_annotations(deployment)
        assert annotations.get(
            "vault.hashicorp.com/agent-configmap"
        ) == "vault-agent-config", (
            "Orchestrator must reference vault-agent-config ConfigMap"
        )

    def test_vault_secret_template_annotation(self, deployment: dict) -> None:
        annotations = _get_pod_annotations(deployment)
        secret_path_key = "vault.hashicorp.com/agent-inject-secret-graphrag.env"
        assert secret_path_key in annotations, (
            "Orchestrator must have vault agent-inject-secret annotation "
            "for graphrag.env"
        )
        assert annotations[secret_path_key] == "secret/data/graphrag"


class TestIngestionWorkerVaultAnnotations:

    @pytest.fixture(name="deployment")
    def _deployment(self) -> dict:
        return _load_deployment(INGESTION_WORKER_DEPLOYMENT_PATH)

    def test_vault_agent_inject_annotation(self, deployment: dict) -> None:
        annotations = _get_pod_annotations(deployment)
        assert annotations.get("vault.hashicorp.com/agent-inject") == "true", (
            "Ingestion worker must have vault.hashicorp.com/agent-inject: 'true'"
        )

    def test_vault_role_annotation(self, deployment: dict) -> None:
        annotations = _get_pod_annotations(deployment)
        assert annotations.get("vault.hashicorp.com/role") == "graphrag", (
            "Ingestion worker must have vault.hashicorp.com/role: 'graphrag'"
        )

    def test_vault_agent_configmap_annotation(self, deployment: dict) -> None:
        annotations = _get_pod_annotations(deployment)
        assert annotations.get(
            "vault.hashicorp.com/agent-configmap"
        ) == "vault-agent-config", (
            "Ingestion worker must reference vault-agent-config ConfigMap"
        )

    def test_vault_secret_template_annotation(self, deployment: dict) -> None:
        annotations = _get_pod_annotations(deployment)
        secret_path_key = "vault.hashicorp.com/agent-inject-secret-graphrag.env"
        assert secret_path_key in annotations, (
            "Ingestion worker must have vault agent-inject-secret annotation "
            "for graphrag.env"
        )
        assert annotations[secret_path_key] == "secret/data/graphrag"
