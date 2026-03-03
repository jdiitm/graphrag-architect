from __future__ import annotations

from pathlib import Path

import pytest
import yaml

INFRA_DIR = Path(__file__).resolve().parents[2] / "infrastructure"
CHAOS_DIR = INFRA_DIR / "chaos"

REQUIRED_EXPERIMENTS = [
    "neo4j-leader-failure",
    "kafka-broker-death",
    "orchestrator-pod-kill",
    "network-partition",
]

REQUIRED_PHASES = ["steady-state", "inject", "verify"]


def _load_experiment(name: str) -> dict:
    path = CHAOS_DIR / f"{name}.yaml"
    content = path.read_text(encoding="utf-8")
    return yaml.safe_load(content)


def _get_template_names(experiment: dict) -> list[str]:
    return [t["name"] for t in experiment.get("spec", {}).get("templates", [])]


class TestChaosDirectoryStructure:

    def test_chaos_directory_exists(self) -> None:
        assert CHAOS_DIR.is_dir(), (
            f"Expected infrastructure/chaos/ directory at {CHAOS_DIR}"
        )

    def test_at_least_four_experiment_files(self) -> None:
        yaml_files = list(CHAOS_DIR.glob("*.yaml"))
        assert len(yaml_files) >= 4, (
            f"Expected at least 4 chaos experiment YAML files, "
            f"found {len(yaml_files)}: {[f.name for f in yaml_files]}"
        )

    def test_readme_exists(self) -> None:
        readme = CHAOS_DIR / "README.md"
        assert readme.is_file(), "Expected README.md in infrastructure/chaos/"

    @pytest.mark.parametrize("experiment_name", REQUIRED_EXPERIMENTS)
    def test_experiment_file_exists(self, experiment_name: str) -> None:
        path = CHAOS_DIR / f"{experiment_name}.yaml"
        assert path.is_file(), (
            f"Expected chaos experiment file: {experiment_name}.yaml"
        )


class TestExperimentStructure:

    @pytest.mark.parametrize("experiment_name", REQUIRED_EXPERIMENTS)
    def test_experiment_is_valid_yaml(self, experiment_name: str) -> None:
        exp = _load_experiment(experiment_name)
        assert isinstance(exp, dict), (
            f"{experiment_name}.yaml must be a valid YAML document"
        )

    @pytest.mark.parametrize("experiment_name", REQUIRED_EXPERIMENTS)
    def test_experiment_has_chaos_mesh_api(
        self, experiment_name: str
    ) -> None:
        exp = _load_experiment(experiment_name)
        api_version = exp.get("apiVersion", "")
        assert api_version.startswith("chaos-mesh.org/"), (
            f"{experiment_name} must use chaos-mesh.org API, "
            f"got: {api_version}"
        )

    @pytest.mark.parametrize("experiment_name", REQUIRED_EXPERIMENTS)
    def test_experiment_targets_graphrag_namespace(
        self, experiment_name: str
    ) -> None:
        exp = _load_experiment(experiment_name)
        ns = exp.get("metadata", {}).get("namespace")
        assert ns == "graphrag", (
            f"{experiment_name} must target graphrag namespace, got: {ns}"
        )

    @pytest.mark.parametrize("experiment_name", REQUIRED_EXPERIMENTS)
    def test_experiment_has_labels(self, experiment_name: str) -> None:
        exp = _load_experiment(experiment_name)
        labels = exp.get("metadata", {}).get("labels", {})
        assert "experiment" in labels, (
            f"{experiment_name} must have an 'experiment' label"
        )
        assert "component" in labels, (
            f"{experiment_name} must have a 'component' label"
        )


class TestExperimentPhases:

    @pytest.mark.parametrize("experiment_name", REQUIRED_EXPERIMENTS)
    def test_experiment_has_three_phases(
        self, experiment_name: str
    ) -> None:
        exp = _load_experiment(experiment_name)
        template_names = _get_template_names(exp)
        for phase_keyword in REQUIRED_PHASES:
            matching = [
                n for n in template_names if phase_keyword in n
            ]
            assert len(matching) >= 1, (
                f"{experiment_name} must have a template containing "
                f"'{phase_keyword}' in its name. "
                f"Found templates: {template_names}"
            )

    @pytest.mark.parametrize("experiment_name", REQUIRED_EXPERIMENTS)
    def test_workflow_entry_point_exists(
        self, experiment_name: str
    ) -> None:
        exp = _load_experiment(experiment_name)
        entry = exp.get("spec", {}).get("entry")
        assert entry is not None, (
            f"{experiment_name} must define spec.entry"
        )
        template_names = _get_template_names(exp)
        assert entry in template_names, (
            f"{experiment_name} entry '{entry}' not found in templates: "
            f"{template_names}"
        )


class TestNeo4jLeaderFailure:

    @pytest.fixture(name="experiment")
    def _experiment(self) -> dict:
        return _load_experiment("neo4j-leader-failure")

    def test_targets_neo4j_pods(self, experiment: dict) -> None:
        templates = experiment["spec"]["templates"]
        pod_chaos = [
            t for t in templates if t.get("templateType") == "PodChaos"
        ]
        assert len(pod_chaos) >= 1, "Must have at least one PodChaos template"
        selector = pod_chaos[0]["podChaos"]["selector"]
        labels = selector.get("labelSelectors", {})
        assert labels.get("app") == "neo4j"

    def test_uses_pod_kill_action(self, experiment: dict) -> None:
        templates = experiment["spec"]["templates"]
        pod_chaos = [
            t for t in templates if t.get("templateType") == "PodChaos"
        ]
        assert pod_chaos[0]["podChaos"]["action"] == "pod-kill"


class TestKafkaBrokerDeath:

    @pytest.fixture(name="experiment")
    def _experiment(self) -> dict:
        return _load_experiment("kafka-broker-death")

    def test_targets_kafka_pods(self, experiment: dict) -> None:
        templates = experiment["spec"]["templates"]
        pod_chaos = [
            t for t in templates if t.get("templateType") == "PodChaos"
        ]
        assert len(pod_chaos) >= 1
        selector = pod_chaos[0]["podChaos"]["selector"]
        labels = selector.get("labelSelectors", {})
        assert labels.get("app") == "kafka"

    def test_kills_single_broker(self, experiment: dict) -> None:
        templates = experiment["spec"]["templates"]
        pod_chaos = [
            t for t in templates if t.get("templateType") == "PodChaos"
        ]
        assert pod_chaos[0]["podChaos"]["mode"] == "one"


class TestOrchestratorPodKill:

    @pytest.fixture(name="experiment")
    def _experiment(self) -> dict:
        return _load_experiment("orchestrator-pod-kill")

    def test_targets_orchestrator_pods(self, experiment: dict) -> None:
        templates = experiment["spec"]["templates"]
        pod_chaos = [
            t for t in templates if t.get("templateType") == "PodChaos"
        ]
        assert len(pod_chaos) >= 1
        selector = pod_chaos[0]["podChaos"]["selector"]
        labels = selector.get("labelSelectors", {})
        assert labels.get("app") == "orchestrator"

    def test_verifies_health_endpoint(self, experiment: dict) -> None:
        templates = experiment["spec"]["templates"]
        verify_tasks = [
            t for t in templates
            if "verify" in t["name"] and t.get("templateType") == "Task"
        ]
        assert len(verify_tasks) >= 1
        command = str(verify_tasks[0]["task"]["container"]["command"])
        assert "/health" in command


class TestNetworkPartition:

    @pytest.fixture(name="experiment")
    def _experiment(self) -> dict:
        return _load_experiment("network-partition")

    def test_uses_network_chaos_type(self, experiment: dict) -> None:
        templates = experiment["spec"]["templates"]
        net_chaos = [
            t for t in templates
            if t.get("templateType") == "NetworkChaos"
        ]
        assert len(net_chaos) >= 1, (
            "Must have at least one NetworkChaos template"
        )

    def test_partitions_orchestrator_from_neo4j(
        self, experiment: dict
    ) -> None:
        templates = experiment["spec"]["templates"]
        net_chaos = [
            t for t in templates
            if t.get("templateType") == "NetworkChaos"
        ]
        chaos_spec = net_chaos[0]["networkChaos"]

        source_labels = chaos_spec["selector"]["labelSelectors"]
        assert source_labels.get("app") == "orchestrator"

        target_labels = chaos_spec["target"]["selector"]["labelSelectors"]
        assert target_labels.get("app") == "neo4j"

    def test_partition_action(self, experiment: dict) -> None:
        templates = experiment["spec"]["templates"]
        net_chaos = [
            t for t in templates
            if t.get("templateType") == "NetworkChaos"
        ]
        assert net_chaos[0]["networkChaos"]["action"] == "partition"
