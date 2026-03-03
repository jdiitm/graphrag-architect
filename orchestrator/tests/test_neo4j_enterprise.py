import pathlib

import yaml
import pytest

from orchestrator.app.neo4j_pool import ReplicaAwarePool


REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
HELM_VALUES = REPO_ROOT / "infrastructure" / "helm" / "graphrag" / "values.yaml"
STATEFULSET = REPO_ROOT / "infrastructure" / "k8s" / "neo4j-statefulset.yaml"
BACKUP_CRONJOB = REPO_ROOT / "infrastructure" / "k8s" / "neo4j-backup-cronjob.yaml"


def _load_yaml_docs(path: pathlib.Path) -> list[dict]:
    text = path.read_text()
    return [doc for doc in yaml.safe_load_all(text) if doc is not None]


class TestHelmEdition:
    def test_neo4j_edition_is_enterprise(self):
        values = yaml.safe_load(HELM_VALUES.read_text())
        assert values["neo4j"]["neo4j"]["edition"] == "enterprise"

    def test_neo4j_cluster_core_count_configured(self):
        values = yaml.safe_load(HELM_VALUES.read_text())
        cluster = values["neo4j"].get("cluster", {})
        assert cluster.get("core_count") == 3, (
            "Helm values must specify neo4j.cluster.core_count = 3"
        )

    def test_neo4j_cluster_read_replica_count_configured(self):
        values = yaml.safe_load(HELM_VALUES.read_text())
        cluster = values["neo4j"].get("cluster", {})
        assert cluster.get("read_replica_count") == 2, (
            "Helm values must specify neo4j.cluster.read_replica_count = 2"
        )


class TestStatefulSetTopology:
    def test_primary_statefulset_has_3_replicas(self):
        docs = _load_yaml_docs(STATEFULSET)
        sts = next(
            d for d in docs
            if d.get("kind") == "StatefulSet"
            and d["metadata"]["name"] == "neo4j"
        )
        assert sts["spec"]["replicas"] == 3

    def test_secondary_statefulset_exists_with_2_replicas(self):
        docs = _load_yaml_docs(STATEFULSET)
        secondary = next(
            (
                d for d in docs
                if d.get("kind") == "StatefulSet"
                and d["metadata"]["name"] == "neo4j-secondary"
            ),
            None,
        )
        assert secondary is not None, (
            "neo4j-secondary StatefulSet must exist in neo4j-statefulset.yaml"
        )
        assert secondary["spec"]["replicas"] == 2

    def test_secondary_server_mode_is_secondary(self):
        docs = _load_yaml_docs(STATEFULSET)
        secondary = next(
            d for d in docs
            if d.get("kind") == "StatefulSet"
            and d["metadata"]["name"] == "neo4j-secondary"
        )
        containers = secondary["spec"]["template"]["spec"]["containers"]
        envs = {
            e["name"]: e["value"]
            for e in containers[0].get("env", [])
            if "value" in e
        }
        assert envs.get("NEO4J_initial_server_mode__constraint") == "SECONDARY"


class TestBackupCronJob:
    def test_backup_cronjob_file_exists(self):
        assert BACKUP_CRONJOB.exists(), (
            "neo4j-backup-cronjob.yaml must exist"
        )

    def test_backup_cronjob_kind(self):
        docs = _load_yaml_docs(BACKUP_CRONJOB)
        cj = next(
            (d for d in docs if d.get("kind") == "CronJob"),
            None,
        )
        assert cj is not None, "CronJob resource must be present"

    def test_backup_schedule_is_nightly(self):
        docs = _load_yaml_docs(BACKUP_CRONJOB)
        cj = next(d for d in docs if d.get("kind") == "CronJob")
        schedule = cj["spec"]["schedule"]
        assert schedule.startswith("0 "), (
            "Backup must run at minute 0 (nightly)"
        )

    def test_backup_uses_neo4j_admin_image(self):
        docs = _load_yaml_docs(BACKUP_CRONJOB)
        cj = next(d for d in docs if d.get("kind") == "CronJob")
        containers = (
            cj["spec"]["jobTemplate"]["spec"]["template"]["spec"]["containers"]
        )
        assert "neo4j" in containers[0]["image"]


class TestReplicaAwarePoolRouting:
    def test_read_driver_round_robins_across_replicas(self):
        primary = object()
        r1, r2 = object(), object()
        pool = ReplicaAwarePool(
            primary_driver=primary,
            replica_drivers=(r1, r2),
        )
        reads = [pool.get_read_driver() for _ in range(4)]
        assert reads == [r1, r2, r1, r2]

    def test_write_always_returns_primary(self):
        primary = object()
        pool = ReplicaAwarePool(
            primary_driver=primary,
            replica_drivers=(object(), object()),
        )
        assert pool.get_write_driver() is primary

    def test_read_falls_back_to_primary_when_no_replicas(self):
        primary = object()
        pool = ReplicaAwarePool(primary_driver=primary)
        assert pool.get_read_driver() is primary


class TestCompositeDatabase:
    def test_helm_composite_databases_section_exists(self):
        values = yaml.safe_load(HELM_VALUES.read_text())
        composites = values["neo4j"].get("composite_databases", [])
        assert len(composites) > 0, (
            "Helm values must declare at least one composite database"
        )

    def test_composite_database_has_name_and_constituents(self):
        values = yaml.safe_load(HELM_VALUES.read_text())
        composites = values["neo4j"]["composite_databases"]
        entry = composites[0]
        assert "name" in entry
        assert "constituents" in entry
        assert len(entry["constituents"]) >= 1
