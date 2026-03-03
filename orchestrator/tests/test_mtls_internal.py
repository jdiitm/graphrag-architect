"""Tests for SEC-09: mTLS internal communication with cert-manager."""

from __future__ import annotations

from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
INFRA_DIR = REPO_ROOT / "infrastructure"
K8S_DIR = INFRA_DIR / "k8s"
HELM_DIR = INFRA_DIR / "helm" / "graphrag"
HELM_TEMPLATES = HELM_DIR / "templates"


def _load_yaml(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _load_all_yaml(path: Path) -> list[dict]:
    return list(yaml.safe_load_all(path.read_text(encoding="utf-8")))


class TestCertManagerIssuer:
    def _find_by_kind(self, kind: str) -> dict | None:
        docs = _load_all_yaml(K8S_DIR / "cert-manager-issuer.yaml")
        for doc in docs:
            if doc and doc.get("kind") == kind:
                return doc
        return None

    def test_cluster_issuer_yaml_exists(self) -> None:
        path = K8S_DIR / "cert-manager-issuer.yaml"
        assert path.exists(), "cert-manager-issuer.yaml must exist"

    def test_cluster_issuer_kind(self) -> None:
        doc = self._find_by_kind("ClusterIssuer")
        assert doc is not None, "ClusterIssuer document must exist"

    def test_cluster_issuer_api_version(self) -> None:
        doc = self._find_by_kind("ClusterIssuer")
        assert doc is not None
        assert doc["apiVersion"] == "cert-manager.io/v1"

    def test_cluster_issuer_is_self_signed_ca(self) -> None:
        doc = self._find_by_kind("ClusterIssuer")
        assert doc is not None
        assert "selfSigned" in doc.get("spec", {}) or "ca" in doc.get("spec", {})


class TestOrchestratorTLSMount:
    def _load_deployment(self) -> dict:
        return _load_yaml(HELM_TEMPLATES / "orchestrator-deployment.yaml")

    def test_orchestrator_has_tls_volume(self) -> None:
        raw = (HELM_TEMPLATES / "orchestrator-deployment.yaml").read_text()
        assert "orchestrator-tls" in raw

    def test_orchestrator_has_cert_volume_mount(self) -> None:
        raw = (HELM_TEMPLATES / "orchestrator-deployment.yaml").read_text()
        assert "/etc/tls" in raw


class TestIngestionTLSMount:
    def test_ingestion_has_tls_volume(self) -> None:
        raw = (HELM_TEMPLATES / "ingestion-deployment.yaml").read_text()
        assert "ingestion-tls" in raw

    def test_ingestion_has_cert_volume_mount(self) -> None:
        raw = (HELM_TEMPLATES / "ingestion-deployment.yaml").read_text()
        assert "/etc/tls" in raw


class TestGoTLSConfig:
    def test_tls_config_file_exists(self) -> None:
        tls_path = (
            REPO_ROOT / "workers" / "ingestion" / "internal" / "tlsconfig" / "tlsconfig.go"
        )
        assert tls_path.exists(), "internal/tlsconfig/tlsconfig.go must exist"

    def test_tls_config_uses_tls13(self) -> None:
        tls_path = (
            REPO_ROOT / "workers" / "ingestion" / "internal" / "tlsconfig" / "tlsconfig.go"
        )
        content = tls_path.read_text()
        assert "tls.VersionTLS13" in content

    def test_tls_config_loads_cert_and_key(self) -> None:
        tls_path = (
            REPO_ROOT / "workers" / "ingestion" / "internal" / "tlsconfig" / "tlsconfig.go"
        )
        content = tls_path.read_text()
        assert "tls.LoadX509KeyPair" in content or "LoadX509KeyPair" in content


class TestHelmTLSValues:
    def test_tls_section_in_values(self) -> None:
        values = _load_yaml(HELM_DIR / "values.yaml")
        assert "tls" in values

    def test_cert_manager_section_in_values(self) -> None:
        values = _load_yaml(HELM_DIR / "values.yaml")
        assert values["tls"]["certManager"]["enabled"] is not None

    def test_cert_manager_issuer_name_in_values(self) -> None:
        values = _load_yaml(HELM_DIR / "values.yaml")
        assert "issuerName" in values["tls"]["certManager"]
