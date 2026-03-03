import os
import glob

import yaml
import pytest


_CHAOS_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
    "infrastructure", "chaos",
)

REQUIRED_EXPERIMENTS = [
    "neo4j-leader-failure.yaml",
    "kafka-broker-death.yaml",
    "orchestrator-pod-kill.yaml",
    "network-partition.yaml",
]

REQUIRED_PHASES = {"steady-state", "inject", "verify"}


def _load_experiment(filename: str) -> dict:
    filepath = os.path.join(_CHAOS_DIR, filename)
    with open(filepath, encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def _template_names(doc: dict) -> list[str]:
    return [t["name"] for t in doc.get("spec", {}).get("templates", [])]


class TestChaosDirectoryStructure:
    def test_chaos_directory_exists(self):
        assert os.path.isdir(_CHAOS_DIR), (
            f"infrastructure/chaos/ directory missing: {_CHAOS_DIR}"
        )

    def test_at_least_four_experiment_files(self):
        yaml_files = glob.glob(os.path.join(_CHAOS_DIR, "*.yaml"))
        assert len(yaml_files) >= 4, (
            f"Expected at least 4 chaos experiment YAML files, found {len(yaml_files)}"
        )

    def test_readme_exists(self):
        readme = os.path.join(_CHAOS_DIR, "README.md")
        assert os.path.isfile(readme), "infrastructure/chaos/README.md missing"

    @pytest.mark.parametrize("filename", REQUIRED_EXPERIMENTS)
    def test_required_experiment_exists(self, filename):
        filepath = os.path.join(_CHAOS_DIR, filename)
        assert os.path.isfile(filepath), f"Missing chaos experiment: {filename}"


class TestChaosExperimentStructure:
    @pytest.mark.parametrize("filename", REQUIRED_EXPERIMENTS)
    def test_is_valid_chaos_mesh_workflow(self, filename):
        doc = _load_experiment(filename)
        assert doc["apiVersion"] == "chaos-mesh.org/v1alpha1"
        assert doc["kind"] == "Workflow"

    @pytest.mark.parametrize("filename", REQUIRED_EXPERIMENTS)
    def test_targets_graphrag_namespace(self, filename):
        doc = _load_experiment(filename)
        assert doc["metadata"]["namespace"] == "graphrag"

    @pytest.mark.parametrize("filename", REQUIRED_EXPERIMENTS)
    def test_has_entry_point(self, filename):
        doc = _load_experiment(filename)
        entry = doc["spec"]["entry"]
        names = _template_names(doc)
        assert entry in names, f"Entry '{entry}' not found in templates: {names}"

    @pytest.mark.parametrize("filename", REQUIRED_EXPERIMENTS)
    def test_has_required_phases(self, filename):
        doc = _load_experiment(filename)
        names = _template_names(doc)
        joined = " ".join(names)
        for phase in REQUIRED_PHASES:
            assert phase in joined, (
                f"Experiment {filename} missing phase '{phase}' in template names: {names}"
            )

    @pytest.mark.parametrize("filename", REQUIRED_EXPERIMENTS)
    def test_has_experiment_labels(self, filename):
        doc = _load_experiment(filename)
        labels = doc["metadata"].get("labels", {})
        assert "experiment" in labels, f"{filename} missing 'experiment' label"
        assert "component" in labels, f"{filename} missing 'component' label"


class TestNeo4jLeaderFailure:
    def test_targets_neo4j_pod(self):
        doc = _load_experiment("neo4j-leader-failure.yaml")
        templates = doc["spec"]["templates"]
        pod_chaos = [t for t in templates if t.get("templateType") == "PodChaos"]
        assert pod_chaos, "No PodChaos template found"
        selector = pod_chaos[0]["podChaos"]["selector"]
        assert selector["labelSelectors"]["app"] == "neo4j"

    def test_action_is_pod_kill(self):
        doc = _load_experiment("neo4j-leader-failure.yaml")
        templates = doc["spec"]["templates"]
        pod_chaos = [t for t in templates if t.get("templateType") == "PodChaos"]
        assert pod_chaos[0]["podChaos"]["action"] == "pod-kill"


class TestKafkaBrokerDeath:
    def test_targets_kafka_pod(self):
        doc = _load_experiment("kafka-broker-death.yaml")
        templates = doc["spec"]["templates"]
        pod_chaos = [t for t in templates if t.get("templateType") == "PodChaos"]
        assert pod_chaos, "No PodChaos template found"
        selector = pod_chaos[0]["podChaos"]["selector"]
        assert selector["labelSelectors"]["app"] == "kafka"

    def test_action_is_pod_kill(self):
        doc = _load_experiment("kafka-broker-death.yaml")
        templates = doc["spec"]["templates"]
        pod_chaos = [t for t in templates if t.get("templateType") == "PodChaos"]
        assert pod_chaos[0]["podChaos"]["action"] == "pod-kill"


class TestOrchestratorPodKill:
    def test_targets_orchestrator_pod(self):
        doc = _load_experiment("orchestrator-pod-kill.yaml")
        templates = doc["spec"]["templates"]
        pod_chaos = [t for t in templates if t.get("templateType") == "PodChaos"]
        assert pod_chaos, "No PodChaos template found"
        selector = pod_chaos[0]["podChaos"]["selector"]
        assert selector["labelSelectors"]["app"] == "orchestrator"


class TestNetworkPartition:
    def test_uses_network_chaos(self):
        doc = _load_experiment("network-partition.yaml")
        templates = doc["spec"]["templates"]
        net_chaos = [t for t in templates if t.get("templateType") == "NetworkChaos"]
        assert net_chaos, "No NetworkChaos template found"

    def test_partition_action(self):
        doc = _load_experiment("network-partition.yaml")
        templates = doc["spec"]["templates"]
        net_chaos = [t for t in templates if t.get("templateType") == "NetworkChaos"]
        assert net_chaos[0]["networkChaos"]["action"] == "partition"

    def test_partition_between_orchestrator_and_neo4j(self):
        doc = _load_experiment("network-partition.yaml")
        templates = doc["spec"]["templates"]
        net_chaos = [t for t in templates if t.get("templateType") == "NetworkChaos"]
        chaos_spec = net_chaos[0]["networkChaos"]
        source_labels = chaos_spec["selector"]["labelSelectors"]
        target_labels = chaos_spec["target"]["selector"]["labelSelectors"]
        assert source_labels["app"] == "orchestrator"
        assert target_labels["app"] == "neo4j"

    def test_partition_is_bidirectional(self):
        doc = _load_experiment("network-partition.yaml")
        templates = doc["spec"]["templates"]
        net_chaos = [t for t in templates if t.get("templateType") == "NetworkChaos"]
        assert net_chaos[0]["networkChaos"]["direction"] == "both"
