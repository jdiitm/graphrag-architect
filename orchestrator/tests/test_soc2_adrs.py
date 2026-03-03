"""Tests for CMP-01 (SOC2 gap analysis) and DOC-03 (ADRs)."""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
COMPLIANCE_DIR = REPO_ROOT / "docs" / "compliance"
ADR_DIR = REPO_ROOT / "docs" / "adr"
SOC2_PATH = COMPLIANCE_DIR / "soc2-gap-analysis.md"


class TestSOC2GapAnalysis:
    def test_soc2_file_exists(self) -> None:
        assert SOC2_PATH.exists(), "soc2-gap-analysis.md must exist"

    def test_has_cc_criteria_section(self) -> None:
        content = SOC2_PATH.read_text()
        assert "CC" in content, "Must reference Common Criteria (CC) identifiers"

    def test_has_control_mapping(self) -> None:
        content = SOC2_PATH.read_text().lower()
        assert "control" in content and "mapping" in content

    def test_has_evidence_section(self) -> None:
        content = SOC2_PATH.read_text().lower()
        assert "evidence" in content

    def test_covers_trust_services_categories(self) -> None:
        content = SOC2_PATH.read_text()
        required = ["Security", "Availability", "Confidentiality"]
        for category in required:
            assert category in content, f"Must cover TSC: {category}"


class TestADRDirectory:
    def test_adr_directory_exists(self) -> None:
        assert ADR_DIR.exists(), "docs/adr/ directory must exist"

    def test_at_least_five_adrs(self) -> None:
        adrs = list(ADR_DIR.glob("*.md"))
        assert len(adrs) >= 5, f"Expected >=5 ADRs, found {len(adrs)}"


class TestADRFormat:
    def _adr_files(self) -> list[Path]:
        return sorted(ADR_DIR.glob("*.md"))

    def test_each_adr_has_status(self) -> None:
        for adr in self._adr_files():
            content = adr.read_text()
            assert re.search(r"(?i)status", content), (
                f"{adr.name} missing Status section"
            )

    def test_each_adr_has_context(self) -> None:
        for adr in self._adr_files():
            content = adr.read_text()
            assert re.search(r"(?i)context", content), (
                f"{adr.name} missing Context section"
            )

    def test_each_adr_has_decision(self) -> None:
        for adr in self._adr_files():
            content = adr.read_text()
            assert re.search(r"(?i)decision", content), (
                f"{adr.name} missing Decision section"
            )

    def test_each_adr_has_consequences(self) -> None:
        for adr in self._adr_files():
            content = adr.read_text()
            assert re.search(r"(?i)consequences", content), (
                f"{adr.name} missing Consequences section"
            )

    def test_adrs_have_numbered_filenames(self) -> None:
        for adr in self._adr_files():
            assert re.match(r"\d{3}-", adr.name), (
                f"{adr.name} must start with NNN- prefix"
            )
