"""Tests that operational documentation exists and contains required sections."""

from __future__ import annotations

import pathlib

import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
DOCS_DIR = REPO_ROOT / "docs"
RUNBOOKS_DIR = DOCS_DIR / "runbooks"

REQUIRED_RUNBOOKS = [
    "neo4j.md",
    "kafka.md",
    "orchestrator.md",
    "ingestion-workers.md",
]

RUNBOOK_REQUIRED_SECTIONS = [
    "Overview",
    "Common Issues",
    "Troubleshooting Steps",
    "Recovery Procedures",
    "Monitoring Queries",
]

INCIDENT_RESPONSE_SECTIONS = [
    "Severity Levels",
    "Escalation Matrix",
    "Communication Templates",
    "Post-Mortem Template",
]

SCHEMA_MIGRATION_SECTIONS = [
    "Prerequisites",
    "Migration Procedure",
    "Rollback Steps",
    "Verification Queries",
]


class TestRunbooksDirectoryExists:
    def test_runbooks_directory_exists(self) -> None:
        assert RUNBOOKS_DIR.is_dir(), f"Expected directory: {RUNBOOKS_DIR}"


class TestRunbookFilesExist:
    @pytest.mark.parametrize("filename", REQUIRED_RUNBOOKS)
    def test_runbook_file_exists(self, filename: str) -> None:
        path = RUNBOOKS_DIR / filename
        assert path.is_file(), f"Missing runbook: {path}"


class TestRunbookSections:
    @pytest.mark.parametrize("filename", REQUIRED_RUNBOOKS)
    @pytest.mark.parametrize("section", RUNBOOK_REQUIRED_SECTIONS)
    def test_runbook_has_required_section(
        self, filename: str, section: str,
    ) -> None:
        path = RUNBOOKS_DIR / filename
        if not path.is_file():
            pytest.skip(f"{path} does not exist yet")
        content = path.read_text()
        assert section in content, (
            f"{filename} missing required section: {section!r}"
        )


class TestRunbookSubstantive:
    @pytest.mark.parametrize("filename", REQUIRED_RUNBOOKS)
    def test_runbook_is_not_trivial(self, filename: str) -> None:
        path = RUNBOOKS_DIR / filename
        if not path.is_file():
            pytest.skip(f"{path} does not exist yet")
        content = path.read_text()
        line_count = len([l for l in content.splitlines() if l.strip()])
        assert line_count >= 30, (
            f"{filename} has only {line_count} non-empty lines; expected >= 30"
        )


class TestIncidentResponse:
    def test_incident_response_file_exists(self) -> None:
        path = DOCS_DIR / "incident-response.md"
        assert path.is_file(), f"Missing: {path}"

    @pytest.mark.parametrize("section", INCIDENT_RESPONSE_SECTIONS)
    def test_incident_response_has_section(self, section: str) -> None:
        path = DOCS_DIR / "incident-response.md"
        if not path.is_file():
            pytest.skip("incident-response.md does not exist yet")
        content = path.read_text()
        assert section in content, (
            f"incident-response.md missing section: {section!r}"
        )

    def test_incident_response_defines_severity_p0_through_p4(self) -> None:
        path = DOCS_DIR / "incident-response.md"
        if not path.is_file():
            pytest.skip("incident-response.md does not exist yet")
        content = path.read_text()
        for level in ("P0", "P1", "P2", "P3", "P4"):
            assert level in content, (
                f"incident-response.md missing severity level: {level}"
            )


class TestSchemaMigration:
    def test_schema_migration_file_exists(self) -> None:
        path = DOCS_DIR / "schema-migration.md"
        assert path.is_file(), f"Missing: {path}"

    @pytest.mark.parametrize("section", SCHEMA_MIGRATION_SECTIONS)
    def test_schema_migration_has_section(self, section: str) -> None:
        path = DOCS_DIR / "schema-migration.md"
        if not path.is_file():
            pytest.skip("schema-migration.md does not exist yet")
        content = path.read_text()
        assert section in content, (
            f"schema-migration.md missing section: {section!r}"
        )

    def test_schema_migration_references_migration_runner(self) -> None:
        path = DOCS_DIR / "schema-migration.md"
        if not path.is_file():
            pytest.skip("schema-migration.md does not exist yet")
        content = path.read_text()
        assert "MigrationRegistry" in content, (
            "schema-migration.md should reference MigrationRegistry"
        )

    def test_schema_migration_references_version_store(self) -> None:
        path = DOCS_DIR / "schema-migration.md"
        if not path.is_file():
            pytest.skip("schema-migration.md does not exist yet")
        content = path.read_text()
        assert "VersionStore" in content or "version_store" in content, (
            "schema-migration.md should reference VersionStore"
        )
