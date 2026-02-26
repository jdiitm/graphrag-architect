from __future__ import annotations

from pathlib import Path

import yaml
import pytest

from orchestrator.app.query_templates import TemplateCatalog


INFRA_DIR = Path(__file__).resolve().parents[2] / "infrastructure"
NETWORK_POLICIES_PATH = INFRA_DIR / "k8s" / "network-policies.yaml"


def _load_network_policies() -> list[dict]:
    content = NETWORK_POLICIES_PATH.read_text(encoding="utf-8")
    return list(yaml.safe_load_all(content))


def _find_network_policy(policies: list[dict], name: str) -> dict | None:
    for policy in policies:
        if policy and policy.get("metadata", {}).get("name") == name:
            return policy
    return None


def _egress_allows_port_to_app(
    policy: dict, app_label: str, port: int,
) -> bool:
    for rule in policy.get("spec", {}).get("egress", []):
        for to_entry in rule.get("to", []):
            pod_sel = to_entry.get("podSelector", {})
            if pod_sel.get("matchLabels", {}).get("app") == app_label:
                for port_entry in rule.get("ports", []):
                    if port_entry.get("port") == port:
                        return True
    return False


@pytest.fixture(name="network_policies")
def _network_policies() -> list[dict]:
    return _load_network_policies()


class TestServiceNeighborsSupernodeProtection:

    def test_service_neighbors_template_has_limit_clause(self) -> None:
        catalog = TemplateCatalog()
        template = catalog.get("service_neighbors")
        assert template is not None
        assert "LIMIT" in template.cypher.upper(), (
            "service_neighbors template must include a LIMIT clause to "
            "prevent unbounded neighbor expansion on supernodes. Without "
            "LIMIT, querying a hub node with 500K edges causes Neo4j OOM."
        )

    def test_service_neighbors_template_uses_limit_parameter(self) -> None:
        catalog = TemplateCatalog()
        template = catalog.get("service_neighbors")
        assert template is not None
        assert "limit" in template.parameters, (
            "service_neighbors template must declare 'limit' in its "
            "parameters tuple so callers can control result set size."
        )
        assert "$limit" in template.cypher, (
            "service_neighbors template must use the $limit parameter "
            "in its Cypher query, not a hardcoded value."
        )

    def test_all_traversal_templates_have_bounded_results(self) -> None:
        catalog = TemplateCatalog()
        unbounded: list[str] = []
        for name, template in catalog.all_templates().items():
            if name.startswith("acl_"):
                continue
            cypher_upper = template.cypher.upper()
            has_limit = "LIMIT" in cypher_upper
            has_bounded_path = "*1.." in template.cypher
            if not has_limit and not has_bounded_path:
                unbounded.append(name)
        assert not unbounded, (
            f"Templates without result bounds (LIMIT or bounded path): "
            f"{unbounded}. Every template must prevent unbounded expansion."
        )


class TestOrchestratorEgressNetworkPolicy:

    def test_orchestrator_egress_allows_redis(
        self, network_policies: list[dict],
    ) -> None:
        policy = _find_network_policy(
            network_policies, "allow-orchestrator-egress",
        )
        assert policy is not None
        assert _egress_allows_port_to_app(policy, "redis", 6379), (
            "Orchestrator egress must allow traffic to app: redis on "
            "port 6379 for SemanticCache, SubgraphCache, and "
            "EvaluationStore. With default-deny active, Redis "
            "connections are silently dropped without this rule."
        )

    def test_orchestrator_egress_allows_qdrant(
        self, network_policies: list[dict],
    ) -> None:
        policy = _find_network_policy(
            network_policies, "allow-orchestrator-egress",
        )
        assert policy is not None
        assert _egress_allows_port_to_app(policy, "qdrant", 6333), (
            "Orchestrator egress must allow traffic to app: qdrant on "
            "port 6333 for QdrantVectorStore gRPC API. With default-deny "
            "active, Qdrant connections are silently dropped."
        )

    def test_orchestrator_egress_allows_qdrant_http(
        self, network_policies: list[dict],
    ) -> None:
        policy = _find_network_policy(
            network_policies, "allow-orchestrator-egress",
        )
        assert policy is not None
        assert _egress_allows_port_to_app(policy, "qdrant", 6334), (
            "Orchestrator egress must allow traffic to app: qdrant on "
            "port 6334 for QdrantVectorStore HTTP API. With default-deny "
            "active, Qdrant HTTP connections are silently dropped."
        )

    def test_orchestrator_egress_allows_kafka(
        self, network_policies: list[dict],
    ) -> None:
        policy = _find_network_policy(
            network_policies, "allow-orchestrator-egress",
        )
        assert policy is not None
        assert _egress_allows_port_to_app(policy, "kafka", 9092), (
            "Orchestrator egress must allow traffic to app: kafka on "
            "port 9092 for KafkaConsumer integration. With default-deny "
            "active, Kafka connections are silently dropped."
        )
