from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
TERRAFORM_KAFKA_DIR = REPO_ROOT / "infrastructure" / "terraform" / "kafka"
DOCS_DIR = REPO_ROOT / "docs"
RUNBOOKS_DIR = DOCS_DIR / "runbooks"


class TestTerraformKafkaDirectory:

    def test_terraform_kafka_directory_exists(self) -> None:
        assert TERRAFORM_KAFKA_DIR.is_dir(), (
            f"Expected infrastructure/terraform/kafka/ at {TERRAFORM_KAFKA_DIR}"
        )

    def test_main_tf_exists(self) -> None:
        assert (TERRAFORM_KAFKA_DIR / "main.tf").is_file(), (
            "Expected main.tf in terraform/kafka/"
        )

    def test_variables_tf_exists(self) -> None:
        assert (TERRAFORM_KAFKA_DIR / "variables.tf").is_file(), (
            "Expected variables.tf in terraform/kafka/"
        )

    def test_outputs_tf_exists(self) -> None:
        assert (TERRAFORM_KAFKA_DIR / "outputs.tf").is_file(), (
            "Expected outputs.tf in terraform/kafka/"
        )


class TestMSKClusterResource:

    def test_main_tf_defines_msk_cluster(self) -> None:
        content = (TERRAFORM_KAFKA_DIR / "main.tf").read_text(encoding="utf-8")
        assert "aws_msk_cluster" in content, (
            "main.tf must define an aws_msk_cluster resource"
        )

    def test_main_tf_has_broker_node_config(self) -> None:
        content = (TERRAFORM_KAFKA_DIR / "main.tf").read_text(encoding="utf-8")
        assert "broker_node_group_info" in content, (
            "MSK cluster must configure broker_node_group_info"
        )

    def test_main_tf_has_encryption(self) -> None:
        content = (TERRAFORM_KAFKA_DIR / "main.tf").read_text(encoding="utf-8")
        assert "encryption_info" in content, (
            "MSK cluster must configure encryption"
        )

    def test_variables_has_cluster_name(self) -> None:
        content = (TERRAFORM_KAFKA_DIR / "variables.tf").read_text(
            encoding="utf-8",
        )
        assert "cluster_name" in content, (
            "variables.tf must define cluster_name variable"
        )

    def test_variables_has_instance_type(self) -> None:
        content = (TERRAFORM_KAFKA_DIR / "variables.tf").read_text(
            encoding="utf-8",
        )
        assert "instance_type" in content, (
            "variables.tf must define instance_type variable"
        )

    def test_outputs_has_bootstrap_servers(self) -> None:
        content = (TERRAFORM_KAFKA_DIR / "outputs.tf").read_text(
            encoding="utf-8",
        )
        assert "bootstrap" in content.lower(), (
            "outputs.tf must expose bootstrap servers"
        )


class TestMigrationRunbook:

    def test_runbook_directory_exists(self) -> None:
        assert RUNBOOKS_DIR.is_dir(), (
            f"Expected docs/runbooks/ directory at {RUNBOOKS_DIR}"
        )

    def test_kafka_migration_runbook_exists(self) -> None:
        path = RUNBOOKS_DIR / "kafka-migration.md"
        assert path.is_file(), (
            "Expected docs/runbooks/kafka-migration.md"
        )

    def test_runbook_covers_prerequisites(self) -> None:
        content = (RUNBOOKS_DIR / "kafka-migration.md").read_text(
            encoding="utf-8",
        ).lower()
        assert "prerequisite" in content, (
            "Migration runbook must cover prerequisites"
        )

    def test_runbook_covers_rollback(self) -> None:
        content = (RUNBOOKS_DIR / "kafka-migration.md").read_text(
            encoding="utf-8",
        ).lower()
        assert "rollback" in content, (
            "Migration runbook must cover rollback procedures"
        )

    def test_runbook_has_minimum_depth(self) -> None:
        content = (RUNBOOKS_DIR / "kafka-migration.md").read_text(
            encoding="utf-8",
        )
        assert len(content) >= 400, (
            "Migration runbook should be substantive (>= 400 chars)"
        )


class TestKafkaConfigFlexibility:

    def test_variables_has_kafka_version(self) -> None:
        content = (TERRAFORM_KAFKA_DIR / "variables.tf").read_text(
            encoding="utf-8",
        )
        assert "kafka_version" in content, (
            "variables.tf should allow configuring kafka_version"
        )

    def test_variables_has_number_of_brokers(self) -> None:
        content = (TERRAFORM_KAFKA_DIR / "variables.tf").read_text(
            encoding="utf-8",
        )
        assert re.search(r"number_of_broker|broker_count|num_brokers", content), (
            "variables.tf should allow configuring broker count"
        )

    def test_main_tf_has_monitoring(self) -> None:
        content = (TERRAFORM_KAFKA_DIR / "main.tf").read_text(encoding="utf-8")
        assert re.search(r"open_monitoring|cloudwatch|logging", content.lower()), (
            "MSK cluster should configure monitoring/logging"
        )
