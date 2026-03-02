"""Tests verifying CI security scanning configuration completeness.

Validates SEC-16 (gitleaks) and Gate 3 (Trivy, SBOM) from SPEC Section 16.2.
"""

from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
CI_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "ci.yml"
GITLEAKS_CONFIG = REPO_ROOT / ".gitleaks.toml"


def _load_ci() -> dict:
    ci = yaml.safe_load(CI_WORKFLOW.read_text())
    if True in ci and "on" not in ci:
        ci["on"] = ci.pop(True)
    return ci


class TestGitleaksJob:
    def test_gitleaks_job_exists(self) -> None:
        ci = _load_ci()
        jobs = ci.get("jobs", {})
        assert "gitleaks" in jobs, "CI must define a 'gitleaks' job"

    def test_gitleaks_uses_official_action(self) -> None:
        ci = _load_ci()
        gitleaks = ci["jobs"]["gitleaks"]
        step_uses = [s.get("uses", "") for s in gitleaks["steps"]]
        assert any(
            "gitleaks/gitleaks-action" in u for u in step_uses
        ), "gitleaks job must use gitleaks/gitleaks-action"

    def test_gitleaks_full_history_clone(self) -> None:
        ci = _load_ci()
        gitleaks = ci["jobs"]["gitleaks"]
        checkout = next(
            s for s in gitleaks["steps"] if "actions/checkout" in s.get("uses", "")
        )
        assert checkout.get("with", {}).get("fetch-depth") == 0, (
            "gitleaks needs fetch-depth: 0 for full history scan"
        )


class TestGitleaksConfig:
    def test_gitleaks_toml_exists(self) -> None:
        assert GITLEAKS_CONFIG.exists(), ".gitleaks.toml must exist at repo root"

    def test_gitleaks_toml_has_allowlist(self) -> None:
        content = GITLEAKS_CONFIG.read_text()
        assert "[allowlist]" in content, ".gitleaks.toml must have an [allowlist] section"

    def test_allowlist_covers_docker_compose(self) -> None:
        content = GITLEAKS_CONFIG.read_text()
        assert "docker-compose" in content, (
            "allowlist must cover infrastructure/docker-compose.yml dev passwords"
        )


class TestSecurityScanJob:
    def test_security_scan_job_exists(self) -> None:
        ci = _load_ci()
        jobs = ci.get("jobs", {})
        assert "security-scan" in jobs, "CI must define a 'security-scan' job"

    def test_trivy_step_present(self) -> None:
        ci = _load_ci()
        scan_job = ci["jobs"]["security-scan"]
        step_uses = [s.get("uses", "") for s in scan_job["steps"]]
        assert any(
            "trivy-action" in u for u in step_uses
        ), "security-scan must include Trivy filesystem scan"

    def test_trivy_blocks_on_critical_high(self) -> None:
        ci = _load_ci()
        scan_job = ci["jobs"]["security-scan"]
        trivy_step = next(
            s for s in scan_job["steps"] if "trivy-action" in s.get("uses", "")
        )
        trivy_with = trivy_step.get("with", {})
        assert trivy_with.get("exit-code") == "1", (
            "Trivy must exit non-zero on findings (exit-code: '1')"
        )
        severity = trivy_with.get("severity", "")
        assert "CRITICAL" in severity and "HIGH" in severity, (
            "Trivy severity must include CRITICAL and HIGH"
        )

    def test_sbom_generation_step(self) -> None:
        ci = _load_ci()
        scan_job = ci["jobs"]["security-scan"]
        step_uses = [s.get("uses", "") for s in scan_job["steps"]]
        has_sbom = any("sbom-action" in u or "cyclonedx" in u for u in step_uses)
        step_names = [s.get("name", "") for s in scan_job["steps"]]
        has_sbom_name = any("sbom" in n.lower() for n in step_names)
        assert has_sbom or has_sbom_name, (
            "security-scan must include SBOM generation (sbom-action or cyclonedx)"
        )

    def test_sbom_artifact_uploaded(self) -> None:
        ci = _load_ci()
        scan_job = ci["jobs"]["security-scan"]
        step_uses = [s.get("uses", "") for s in scan_job["steps"]]
        assert any(
            "upload-artifact" in u for u in step_uses
        ), "SBOM must be uploaded as a build artifact"


class TestCITriggers:
    def test_workflow_triggers_on_push_to_main(self) -> None:
        ci = _load_ci()
        push_branches = ci.get("on", {}).get("push", {}).get("branches", [])
        assert "main" in push_branches

    def test_workflow_triggers_on_pr_to_main(self) -> None:
        ci = _load_ci()
        pr_branches = ci.get("on", {}).get("pull_request", {}).get("branches", [])
        assert "main" in pr_branches
