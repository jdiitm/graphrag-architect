from __future__ import annotations

from pathlib import Path

import yaml
import pytest


INFRA_DIR = Path(__file__).resolve().parents[2] / "infrastructure"
NETWORK_POLICIES_PATH = INFRA_DIR / "k8s" / "network-policies.yaml"
KAFKA_STATEFULSET_PATH = INFRA_DIR / "k8s" / "kafka-statefulset.yaml"
DOCKER_COMPOSE_PATH = INFRA_DIR / "docker-compose.yml"


def _load_network_policies() -> list[dict]:
    content = NETWORK_POLICIES_PATH.read_text(encoding="utf-8")
    return list(yaml.safe_load_all(content))


def _find_network_policy(policies: list[dict], name: str) -> dict | None:
    for policy in policies:
        if policy and policy.get("metadata", {}).get("name") == name:
            return policy
    return None


@pytest.fixture(name="network_policies")
def _network_policies() -> list[dict]:
    return _load_network_policies()


@pytest.fixture(name="compose_config")
def _compose_config() -> dict:
    content = DOCKER_COMPOSE_PATH.read_text(encoding="utf-8")
    return yaml.safe_load(content)


class TestNetworkPolicyNamespaceRestriction:

    def test_orchestrator_ingress_policy_exists(
        self, network_policies: list[dict]
    ) -> None:
        policy = _find_network_policy(
            network_policies, "allow-orchestrator-ingress"
        )
        assert policy is not None, (
            "Expected allow-orchestrator-ingress NetworkPolicy"
        )

    def test_ingestion_worker_rule_restricts_to_graphrag_namespace(
        self, network_policies: list[dict]
    ) -> None:
        policy = _find_network_policy(
            network_policies, "allow-orchestrator-ingress"
        )
        assert policy is not None

        ingress_rules = policy["spec"]["ingress"]
        ingestion_worker_rule = None
        for rule in ingress_rules:
            for from_entry in rule.get("from", []):
                pod_sel = from_entry.get("podSelector", {})
                labels = pod_sel.get("matchLabels", {})
                if labels.get("app") == "ingestion-worker":
                    ingestion_worker_rule = from_entry
                    break

        assert ingestion_worker_rule is not None, (
            "Expected an ingress rule for ingestion-worker pods"
        )

        ns_selector = ingestion_worker_rule.get("namespaceSelector", {})
        ns_labels = ns_selector.get("matchLabels", {})
        assert ns_labels.get("kubernetes.io/metadata.name") == "graphrag", (
            "Ingestion-worker ingress rule must restrict to the graphrag "
            "namespace via namespaceSelector.matchLabels"
            "[kubernetes.io/metadata.name]=graphrag, "
            f"but got namespaceSelector: {ns_selector}"
        )

    def test_no_wildcard_namespace_selectors_for_app_pods(
        self, network_policies: list[dict]
    ) -> None:
        for policy in network_policies:
            if not policy:
                continue
            policy_name = policy.get("metadata", {}).get("name", "unknown")
            for rule in policy.get("spec", {}).get("ingress", []):
                for from_entry in rule.get("from", []):
                    pod_sel = from_entry.get("podSelector", {})
                    ns_sel = from_entry.get("namespaceSelector")
                    has_app_label = "app" in pod_sel.get("matchLabels", {})
                    if has_app_label and ns_sel is not None:
                        assert ns_sel != {}, (
                            f"NetworkPolicy '{policy_name}' uses "
                            f"namespaceSelector: {{}} (wildcard) with an app "
                            f"pod selector. This allows matching pods from "
                            f"ANY namespace. Use an explicit namespace label."
                        )


class TestDeploymentSecurityContext:

    @pytest.fixture(name="orchestrator_deployment")
    def _orchestrator_deployment(self) -> dict:
        path = INFRA_DIR / "k8s" / "orchestrator-deployment.yaml"
        docs = list(yaml.safe_load_all(path.read_text(encoding="utf-8")))
        for doc in docs:
            if doc and doc.get("kind") == "Deployment":
                return doc
        pytest.fail("No Deployment found in orchestrator-deployment.yaml")

    @pytest.fixture(name="ingestion_worker_deployment")
    def _ingestion_worker_deployment(self) -> dict:
        path = INFRA_DIR / "k8s" / "ingestion-worker-deployment.yaml"
        docs = list(yaml.safe_load_all(path.read_text(encoding="utf-8")))
        for doc in docs:
            if doc and doc.get("kind") == "Deployment":
                return doc
        pytest.fail("No Deployment found in ingestion-worker-deployment.yaml")

    def _get_container_security_context(self, deployment: dict) -> dict:
        containers = (
            deployment["spec"]["template"]["spec"]["containers"]
        )
        assert len(containers) > 0
        return containers[0].get("securityContext", {})

    def test_orchestrator_runs_as_non_root(
        self, orchestrator_deployment: dict
    ) -> None:
        ctx = self._get_container_security_context(orchestrator_deployment)
        assert ctx.get("runAsNonRoot") is True, (
            "Orchestrator container must set runAsNonRoot: true"
        )

    def test_orchestrator_read_only_root_filesystem(
        self, orchestrator_deployment: dict
    ) -> None:
        ctx = self._get_container_security_context(orchestrator_deployment)
        assert ctx.get("readOnlyRootFilesystem") is True, (
            "Orchestrator container must set readOnlyRootFilesystem: true"
        )

    def test_orchestrator_no_privilege_escalation(
        self, orchestrator_deployment: dict
    ) -> None:
        ctx = self._get_container_security_context(orchestrator_deployment)
        assert ctx.get("allowPrivilegeEscalation") is False, (
            "Orchestrator container must set allowPrivilegeEscalation: false"
        )

    def test_ingestion_worker_runs_as_non_root(
        self, ingestion_worker_deployment: dict
    ) -> None:
        ctx = self._get_container_security_context(
            ingestion_worker_deployment
        )
        assert ctx.get("runAsNonRoot") is True, (
            "Ingestion worker container must set runAsNonRoot: true"
        )

    def test_ingestion_worker_read_only_root_filesystem(
        self, ingestion_worker_deployment: dict
    ) -> None:
        ctx = self._get_container_security_context(
            ingestion_worker_deployment
        )
        assert ctx.get("readOnlyRootFilesystem") is True, (
            "Ingestion worker container must set readOnlyRootFilesystem: true"
        )

    def test_ingestion_worker_no_privilege_escalation(
        self, ingestion_worker_deployment: dict
    ) -> None:
        ctx = self._get_container_security_context(
            ingestion_worker_deployment
        )
        assert ctx.get("allowPrivilegeEscalation") is False, (
            "Ingestion worker container must set "
            "allowPrivilegeEscalation: false"
        )


class TestKafkaAdvertisedListeners:

    @pytest.fixture(name="kafka_statefulset")
    def _kafka_statefulset(self) -> dict:
        docs = list(yaml.safe_load_all(
            KAFKA_STATEFULSET_PATH.read_text(encoding="utf-8")
        ))
        for doc in docs:
            if doc and doc.get("kind") == "StatefulSet":
                return doc
        pytest.fail("No StatefulSet found in kafka-statefulset.yaml")

    def _get_kafka_env(self, statefulset: dict) -> list[dict]:
        containers = statefulset["spec"]["template"]["spec"]["containers"]
        for container in containers:
            if container["name"] == "kafka":
                return container.get("env", [])
        pytest.fail("No kafka container found")

    def test_advertised_listeners_env_var_present(
        self, kafka_statefulset: dict
    ) -> None:
        env_vars = self._get_kafka_env(kafka_statefulset)
        names = [e["name"] for e in env_vars]
        assert "KAFKA_ADVERTISED_LISTENERS" in names, (
            "Kafka StatefulSet must define KAFKA_ADVERTISED_LISTENERS "
            "to ensure external pods can resolve broker addresses"
        )

    def test_advertised_listeners_uses_statefulset_dns(
        self, kafka_statefulset: dict
    ) -> None:
        env_vars = self._get_kafka_env(kafka_statefulset)
        adv_listener = None
        for var in env_vars:
            if var["name"] == "KAFKA_ADVERTISED_LISTENERS":
                adv_listener = var["value"]
                break
        assert adv_listener is not None
        assert "kafka-headless" in adv_listener, (
            "Advertised listeners must reference the headless service DNS"
        )
        assert "9092" in adv_listener


class TestSchemaInitNetworkPolicy:

    def test_neo4j_ingress_allows_schema_init(
        self, network_policies: list[dict]
    ) -> None:
        policy = _find_network_policy(
            network_policies, "allow-neo4j-ingress"
        )
        assert policy is not None

        allowed_apps = set()
        for rule in policy["spec"].get("ingress", []):
            for from_entry in rule.get("from", []):
                pod_sel = from_entry.get("podSelector", {})
                app_label = pod_sel.get("matchLabels", {}).get("app")
                if app_label:
                    allowed_apps.add(app_label)

        assert "neo4j-schema-init" in allowed_apps, (
            "allow-neo4j-ingress must permit ingress from "
            "app: neo4j-schema-init on port 7687"
        )

    def test_schema_init_egress_policy_exists(
        self, network_policies: list[dict]
    ) -> None:
        policy = _find_network_policy(
            network_policies, "allow-schema-init-egress"
        )
        assert policy is not None, (
            "A NetworkPolicy allowing neo4j-schema-init egress "
            "to Neo4j must exist"
        )

    def test_schema_init_egress_targets_neo4j_on_7687(
        self, network_policies: list[dict]
    ) -> None:
        policy = _find_network_policy(
            network_policies, "allow-schema-init-egress"
        )
        assert policy is not None

        egress_rules = policy["spec"].get("egress", [])
        targets_neo4j = False
        for rule in egress_rules:
            for to_entry in rule.get("to", []):
                pod_sel = to_entry.get("podSelector", {})
                if pod_sel.get("matchLabels", {}).get("app") == "neo4j":
                    for port_entry in rule.get("ports", []):
                        if port_entry.get("port") == 7687:
                            targets_neo4j = True
        assert targets_neo4j, (
            "Schema init egress policy must allow traffic "
            "to app: neo4j on port 7687"
        )


class TestDockerComposeNoApocFileAccess:

    def test_neo4j_service_exists(self, compose_config: dict) -> None:
        assert "neo4j" in compose_config.get("services", {}), (
            "Expected neo4j service in docker-compose.yml"
        )

    def test_no_apoc_export_file_enabled(self, compose_config: dict) -> None:
        neo4j_env = compose_config["services"]["neo4j"].get(
            "environment", []
        )
        apoc_export_vars = [
            e for e in neo4j_env
            if "apoc_export_file_enabled" in str(e).lower()
        ]
        assert len(apoc_export_vars) == 0, (
            f"Neo4j should not enable APOC file export in dev environment. "
            f"Found: {apoc_export_vars}"
        )

    def test_no_apoc_import_file_enabled(self, compose_config: dict) -> None:
        neo4j_env = compose_config["services"]["neo4j"].get(
            "environment", []
        )
        apoc_import_vars = [
            e for e in neo4j_env
            if "apoc_import_file_enabled" in str(e).lower()
        ]
        assert len(apoc_import_vars) == 0, (
            f"Neo4j should not enable APOC file import in dev environment. "
            f"Found: {apoc_import_vars}"
        )

    def test_no_apoc_import_file_use_neo4j_config(
        self, compose_config: dict
    ) -> None:
        neo4j_env = compose_config["services"]["neo4j"].get(
            "environment", []
        )
        apoc_neo4j_config_vars = [
            e for e in neo4j_env
            if "apoc_import_file_use" in str(e).lower()
        ]
        assert len(apoc_neo4j_config_vars) == 0, (
            f"Neo4j should not enable APOC file use_neo4j_config in dev "
            f"environment. Found: {apoc_neo4j_config_vars}"
        )

    def test_neo4j_retains_auth_config(self, compose_config: dict) -> None:
        neo4j_env = compose_config["services"]["neo4j"].get(
            "environment", []
        )
        auth_vars = [e for e in neo4j_env if "NEO4J_AUTH" in str(e)]
        assert len(auth_vars) == 1, (
            "Neo4j must retain its NEO4J_AUTH environment variable"
        )
