"""Tests for SEC-10: SBOM generation and container image scanning."""

from __future__ import annotations

import re
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
ORCHESTRATOR_DOCKERFILE = REPO_ROOT / "orchestrator" / "Dockerfile"
WORKER_DOCKERFILE = REPO_ROOT / "workers" / "ingestion" / "Dockerfile"
CI_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "ci.yml"


def _load_ci() -> dict:
    ci = yaml.safe_load(CI_WORKFLOW.read_text())
    if True in ci and "on" not in ci:
        ci["on"] = ci.pop(True)
    return ci


class TestOrchestratorDockerfile:
    def test_dockerfile_exists(self) -> None:
        assert ORCHESTRATOR_DOCKERFILE.exists()

    def test_multi_stage_build(self) -> None:
        content = ORCHESTRATOR_DOCKERFILE.read_text()
        assert content.count("FROM ") >= 2, "Must be multi-stage"

    def test_non_root_user(self) -> None:
        content = ORCHESTRATOR_DOCKERFILE.read_text()
        assert "USER" in content
        assert "root" not in content.split("USER")[-1].split("\n")[0]

    def test_no_latest_base_tag(self) -> None:
        content = ORCHESTRATOR_DOCKERFILE.read_text()
        from_lines = [l for l in content.splitlines() if l.startswith("FROM ")]
        for line in from_lines:
            image = line.split()[1]
            if "AS" not in line.upper() or ":" in image:
                assert ":latest" not in image, f"Avoid :latest tag: {line}"


class TestWorkerDockerfile:
    def test_dockerfile_exists(self) -> None:
        assert WORKER_DOCKERFILE.exists()

    def test_multi_stage_build(self) -> None:
        content = WORKER_DOCKERFILE.read_text()
        assert content.count("FROM ") >= 2, "Must be multi-stage"

    def test_non_root_user(self) -> None:
        content = WORKER_DOCKERFILE.read_text()
        assert "USER" in content
        assert "root" not in content.split("USER")[-1].split("\n")[0]


class TestCIContainerScanning:
    def test_docker_build_job_exists(self) -> None:
        ci = _load_ci()
        assert "docker-build" in ci["jobs"]

    def test_trivy_image_scan_step(self) -> None:
        ci = _load_ci()
        docker_job = ci["jobs"]["docker-build"]
        step_texts = []
        for step in docker_job.get("steps", []):
            step_texts.append(step.get("uses", "") + step.get("name", ""))
        combined = " ".join(step_texts)
        assert "trivy" in combined.lower() or "scan" in combined.lower(), (
            "docker-build must include a Trivy image scan step"
        )

    def test_sbom_artifact_step(self) -> None:
        ci = _load_ci()
        docker_job = ci["jobs"]["docker-build"]
        step_texts = []
        for step in docker_job.get("steps", []):
            step_texts.append(
                step.get("uses", "") + " " + step.get("name", "")
                + " " + str(step.get("with", {}))
            )
        combined = " ".join(step_texts).lower()
        assert "sbom" in combined, "docker-build must produce SBOM artifact"
