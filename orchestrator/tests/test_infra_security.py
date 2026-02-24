from __future__ import annotations

from pathlib import Path

import yaml
import pytest


INFRA_DIR = Path(__file__).resolve().parents[2] / "infrastructure"
NETWORK_POLICIES_PATH = INFRA_DIR / "k8s" / "network-policies.yaml"
KAFKA_STATEFULSET_PATH = INFRA_DIR / "k8s" / "kafka-statefulset.yaml"
SCHEMA_JOB_PATH = INFRA_DIR / "k8s" / "neo4j-schema-job.yaml"
SECRETS_PATH = INFRA_DIR / "k8s" / "secrets.yaml"
DOCKER_COMPOSE_PATH = INFRA_DIR / "docker-compose.yml"


def _load_network_policies() -> list[dict]:
    content = NETWORK_POLICIES_PATH.read_text(encoding="utf-8")
    return list(yaml.safe_load_all(content))


def _find_network_policy(policies: list[dict], name: str) -> dict | None:
    for policy in policies:
        if policy and policy.get("metadata", {}).get("name") == name:
            return policy
    return None


def _egress_allows_port_to_app(
    policy: dict, app_label: str, port: int
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


class TestSchemaInitJobPodLabels:

    @pytest.fixture(name="schema_job")
    def _schema_job(self) -> dict:
        docs = list(yaml.safe_load_all(
            SCHEMA_JOB_PATH.read_text(encoding="utf-8")
        ))
        for doc in docs:
            if doc and doc.get("kind") == "Job":
                return doc
        pytest.fail("No Job found in neo4j-schema-job.yaml")

    def test_pod_template_has_labels(self, schema_job: dict) -> None:
        pod_template = schema_job["spec"]["template"]
        pod_labels = pod_template.get("metadata", {}).get("labels", {})
        assert pod_labels, (
            "Schema init Job pod template must have metadata.labels. "
            "Kubernetes does not propagate Job.metadata.labels to pods. "
            "Without explicit pod labels, NetworkPolicies cannot match "
            "the schema init pods and traffic is blocked by deny-all."
        )

    def test_pod_template_label_matches_network_policy(
        self, schema_job: dict, network_policies: list[dict]
    ) -> None:
        pod_labels = (
            schema_job["spec"]["template"]
            .get("metadata", {}).get("labels", {})
        )
        pod_app_label = pod_labels.get("app")
        assert pod_app_label == "neo4j-schema-init", (
            f"Schema init pod template must have app: neo4j-schema-init "
            f"to match NetworkPolicy selectors, got app: {pod_app_label}"
        )

        egress_policy = _find_network_policy(
            network_policies, "allow-schema-init-egress"
        )
        assert egress_policy is not None
        egress_selector = (
            egress_policy["spec"]["podSelector"]
            .get("matchLabels", {}).get("app")
        )
        assert pod_app_label == egress_selector, (
            f"Pod template label app={pod_app_label} must match "
            f"allow-schema-init-egress podSelector app={egress_selector}"
        )


class TestKafkaEgressNetworkPolicy:

    def test_kafka_egress_policy_exists(
        self, network_policies: list[dict]
    ) -> None:
        policy = _find_network_policy(
            network_policies, "allow-kafka-egress"
        )
        assert policy is not None, (
            "A NetworkPolicy named 'allow-kafka-egress' must exist. "
            "Without it, the deny-all policy blocks all inter-broker "
            "communication, preventing KRaft quorum formation."
        )

    def test_kafka_egress_selects_kafka_pods(
        self, network_policies: list[dict]
    ) -> None:
        policy = _find_network_policy(
            network_policies, "allow-kafka-egress"
        )
        assert policy is not None
        pod_selector = policy["spec"]["podSelector"]
        assert pod_selector.get("matchLabels", {}).get("app") == "kafka", (
            "Kafka egress policy must select pods with app: kafka"
        )

    def test_kafka_egress_allows_inter_broker_replication(
        self, network_policies: list[dict]
    ) -> None:
        policy = _find_network_policy(
            network_policies, "allow-kafka-egress"
        )
        assert policy is not None
        assert _egress_allows_port_to_app(policy, "kafka", 9092), (
            "Kafka egress must allow traffic to app: kafka on port 9092 "
            "for inter-broker replication"
        )

    def test_kafka_egress_allows_controller_quorum(
        self, network_policies: list[dict]
    ) -> None:
        policy = _find_network_policy(
            network_policies, "allow-kafka-egress"
        )
        assert policy is not None
        assert _egress_allows_port_to_app(policy, "kafka", 9093), (
            "Kafka egress must allow traffic to app: kafka on port 9093 "
            "for KRaft controller quorum communication"
        )

    def test_kafka_egress_allows_dns(
        self, network_policies: list[dict]
    ) -> None:
        policy = _find_network_policy(
            network_policies, "allow-kafka-egress"
        )
        assert policy is not None
        egress_rules = policy["spec"].get("egress", [])
        dns_udp = False
        dns_tcp = False
        for rule in egress_rules:
            for port_entry in rule.get("ports", []):
                if port_entry.get("port") == 53:
                    if port_entry.get("protocol") == "UDP":
                        dns_udp = True
                    elif port_entry.get("protocol") == "TCP":
                        dns_tcp = True
        assert dns_udp, (
            "Kafka egress must allow DNS resolution on port 53/UDP"
        )
        assert dns_tcp, (
            "Kafka egress must allow DNS resolution on port 53/TCP"
        )


class TestKafkaClusterId:

    @pytest.fixture(name="kafka_statefulset")
    def _kafka_statefulset(self) -> dict:
        docs = list(yaml.safe_load_all(
            KAFKA_STATEFULSET_PATH.read_text(encoding="utf-8")
        ))
        for doc in docs:
            if doc and doc.get("kind") == "StatefulSet":
                return doc
        pytest.fail("No StatefulSet found in kafka-statefulset.yaml")

    @pytest.fixture(name="kafka_env_vars")
    def _kafka_env_vars(self, kafka_statefulset: dict) -> list[dict]:
        containers = kafka_statefulset["spec"]["template"]["spec"]["containers"]
        for container in containers:
            if container["name"] == "kafka":
                return container.get("env", [])
        pytest.fail("No kafka container found")

    def test_cluster_id_env_var_present(
        self, kafka_env_vars: list[dict]
    ) -> None:
        names = [e["name"] for e in kafka_env_vars]
        assert "CLUSTER_ID" in names, (
            "Kafka StatefulSet must define CLUSTER_ID env var. "
            "Without it, each KRaft broker auto-generates its own "
            "cluster ID, forming separate single-node clusters."
        )

    def test_cluster_id_is_non_empty(
        self, kafka_env_vars: list[dict]
    ) -> None:
        cluster_id_value = None
        for var in kafka_env_vars:
            if var["name"] == "CLUSTER_ID":
                cluster_id_value = var.get("value", "")
                break
        assert cluster_id_value is not None, "CLUSTER_ID env var not found"
        assert len(cluster_id_value.strip()) > 0, (
            "CLUSTER_ID must be a non-empty string"
        )

    def test_cluster_id_matches_docker_compose(
        self, kafka_env_vars: list[dict], compose_config: dict
    ) -> None:
        k8s_cluster_id = None
        for var in kafka_env_vars:
            if var["name"] == "CLUSTER_ID":
                k8s_cluster_id = var.get("value")
                break
        assert k8s_cluster_id is not None, "CLUSTER_ID env var not found"

        compose_kafka_env = compose_config["services"]["kafka"].get(
            "environment", []
        )
        compose_cluster_id = None
        for entry in compose_kafka_env:
            if isinstance(entry, str) and entry.startswith("CLUSTER_ID="):
                compose_cluster_id = entry.split("=", 1)[1]
                break
        assert compose_cluster_id is not None, (
            "docker-compose.yml must define CLUSTER_ID for kafka"
        )
        assert k8s_cluster_id == compose_cluster_id, (
            f"K8s CLUSTER_ID ({k8s_cluster_id}) must match "
            f"docker-compose CLUSTER_ID ({compose_cluster_id}) "
            f"to ensure consistent cluster identity across environments"
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


class TestKafkaListenerSecurityProtocolMap:

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

    def test_listener_security_protocol_map_present(
        self, kafka_statefulset: dict
    ) -> None:
        env_vars = self._get_kafka_env(kafka_statefulset)
        names = [e["name"] for e in env_vars]
        assert "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP" in names, (
            "Kafka StatefulSet must define "
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP. Without it, Kafka "
            "3.9.0 cannot resolve protocol for the custom CONTROLLER "
            "listener name and may fail to start."
        )

    def test_listener_security_protocol_map_includes_controller(
        self, kafka_statefulset: dict
    ) -> None:
        env_vars = self._get_kafka_env(kafka_statefulset)
        protocol_map = None
        for var in env_vars:
            if var["name"] == "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP":
                protocol_map = var.get("value", "")
                break
        assert protocol_map is not None, (
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP not found"
        )
        assert "CONTROLLER:PLAINTEXT" in protocol_map, (
            "Protocol map must include CONTROLLER:PLAINTEXT "
            f"but got: {protocol_map}"
        )
        assert "PLAINTEXT:PLAINTEXT" in protocol_map, (
            "Protocol map must include PLAINTEXT:PLAINTEXT "
            f"but got: {protocol_map}"
        )

    def test_listener_security_protocol_map_matches_docker_compose(
        self, kafka_statefulset: dict, compose_config: dict
    ) -> None:
        k8s_env = self._get_kafka_env(kafka_statefulset)
        k8s_value = None
        for var in k8s_env:
            if var["name"] == "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP":
                k8s_value = var.get("value", "")
                break
        assert k8s_value is not None, (
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP not in K8s manifest"
        )

        compose_kafka_env = compose_config["services"]["kafka"].get(
            "environment", []
        )
        compose_value = None
        for entry in compose_kafka_env:
            if isinstance(entry, str) and entry.startswith(
                "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP="
            ):
                compose_value = entry.split("=", 1)[1]
                break
        assert compose_value is not None, (
            "docker-compose.yml must define "
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP for kafka"
        )
        assert k8s_value == compose_value, (
            f"K8s protocol map ({k8s_value}) must match "
            f"docker-compose ({compose_value})"
        )


class TestSecretsTemplate:

    @pytest.fixture(name="secrets")
    def _secrets(self) -> dict:
        content = SECRETS_PATH.read_text(encoding="utf-8")
        return yaml.safe_load(content)

    def test_auth_token_secret_present(self, secrets: dict) -> None:
        string_data = secrets.get("stringData", {})
        assert "AUTH_TOKEN_SECRET" in string_data, (
            "secrets.yaml must include AUTH_TOKEN_SECRET placeholder. "
            "Without it, fresh deployments with AUTH_REQUIRE_TOKENS=true "
            "will return 503 until an operator manually adds the secret."
        )

    def test_auth_token_secret_is_placeholder(self, secrets: dict) -> None:
        string_data = secrets.get("stringData", {})
        value = string_data.get("AUTH_TOKEN_SECRET", "")
        assert value == "REPLACE_ME", (
            "AUTH_TOKEN_SECRET should be a placeholder value 'REPLACE_ME', "
            f"got: {value}"
        )
