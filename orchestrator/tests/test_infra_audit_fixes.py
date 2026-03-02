from __future__ import annotations

from pathlib import Path

import pytest
import yaml

INFRA_K8S_DIR = Path(__file__).resolve().parents[2] / "infrastructure" / "k8s"


def _load_yaml_documents(filename: str) -> list[dict]:
    path = INFRA_K8S_DIR / filename
    assert path.exists(), f"Missing file: {path}"
    raw = path.read_text(encoding="utf-8")
    return [doc for doc in yaml.safe_load_all(raw) if doc is not None]


class TestIngestionWorkerProbes:
    @pytest.fixture()
    def deployment(self) -> dict:
        docs = _load_yaml_documents("ingestion-worker-deployment.yaml")
        deployments = [
            d for d in docs if d.get("kind") == "Deployment"
        ]
        assert len(deployments) == 1
        return deployments[0]

    def _container(self, deployment: dict) -> dict:
        containers = deployment["spec"]["template"]["spec"]["containers"]
        matches = [c for c in containers if c["name"] == "ingestion-worker"]
        assert len(matches) == 1
        return matches[0]

    def test_readiness_probe_uses_healthz(self, deployment: dict) -> None:
        container = self._container(deployment)
        probe = container.get("readinessProbe", {})
        http_get = probe.get("httpGet", {})
        assert http_get.get("path") == "/healthz", (
            f"readinessProbe path is '{http_get.get('path')}', expected '/healthz'"
        )

    def test_liveness_probe_uses_healthz(self, deployment: dict) -> None:
        container = self._container(deployment)
        probe = container.get("livenessProbe", {})
        http_get = probe.get("httpGet", {})
        assert http_get.get("path") == "/healthz", (
            f"livenessProbe path is '{http_get.get('path')}', expected '/healthz'"
        )


class TestKafkaSASLSSL:
    @pytest.fixture()
    def statefulset(self) -> dict:
        docs = _load_yaml_documents("kafka-statefulset.yaml")
        statefulsets = [
            d for d in docs if d.get("kind") == "StatefulSet"
        ]
        assert len(statefulsets) == 1
        return statefulsets[0]

    def _kafka_env(self, statefulset: dict) -> dict[str, str]:
        containers = statefulset["spec"]["template"]["spec"]["containers"]
        kafka_containers = [c for c in containers if c["name"] == "kafka"]
        assert len(kafka_containers) == 1
        env_list = kafka_containers[0].get("env", [])
        return {e["name"]: e.get("value", "") for e in env_list}

    def test_listeners_use_sasl_ssl(self, statefulset: dict) -> None:
        env = self._kafka_env(statefulset)
        listeners = env.get("KAFKA_LISTENERS", "")
        assert "SASL_SSL" in listeners, (
            f"KAFKA_LISTENERS does not contain SASL_SSL: '{listeners}'"
        )
        assert "PLAINTEXT://:9092" not in listeners, (
            "KAFKA_LISTENERS still contains PLAINTEXT://:9092"
        )

    def test_security_protocol_map_has_sasl_ssl(self, statefulset: dict) -> None:
        env = self._kafka_env(statefulset)
        protocol_map = env.get("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "")
        assert "SASL_SSL" in protocol_map, (
            f"Protocol map missing SASL_SSL: '{protocol_map}'"
        )

    def test_inter_broker_uses_sasl_ssl(self, statefulset: dict) -> None:
        env = self._kafka_env(statefulset)
        inter_broker = env.get("KAFKA_INTER_BROKER_LISTENER_NAME", "")
        assert inter_broker == "SASL_SSL", (
            f"Inter-broker listener is '{inter_broker}', expected 'SASL_SSL'"
        )

    def test_scram_sha_512_mechanism_configured(self, statefulset: dict) -> None:
        env = self._kafka_env(statefulset)
        mechanism = env.get("KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL", "")
        assert mechanism == "SCRAM-SHA-512", (
            f"SASL mechanism is '{mechanism}', expected 'SCRAM-SHA-512'"
        )


class TestIngressTLS:
    @pytest.fixture()
    def ingress(self) -> dict:
        docs = _load_yaml_documents("ingress.yaml")
        ingresses = [
            d for d in docs if d.get("kind") == "Ingress"
        ]
        assert len(ingresses) == 1
        return ingresses[0]

    def test_tls_section_exists(self, ingress: dict) -> None:
        tls_block = ingress.get("spec", {}).get("tls")
        assert tls_block is not None, "Ingress spec is missing 'tls' section"
        assert isinstance(tls_block, list)
        assert len(tls_block) > 0

    def test_tls_has_secret_name(self, ingress: dict) -> None:
        tls_entries = ingress["spec"]["tls"]
        for entry in tls_entries:
            assert "secretName" in entry, "TLS entry missing 'secretName'"
            assert len(entry["secretName"]) > 0

    def test_tls_has_hosts(self, ingress: dict) -> None:
        tls_entries = ingress["spec"]["tls"]
        for entry in tls_entries:
            assert "hosts" in entry, "TLS entry missing 'hosts'"
            assert len(entry["hosts"]) > 0

    def test_cert_manager_annotation_present(self, ingress: dict) -> None:
        annotations = ingress.get("metadata", {}).get("annotations", {})
        assert "cert-manager.io/cluster-issuer" in annotations, (
            "Missing cert-manager.io/cluster-issuer annotation"
        )
