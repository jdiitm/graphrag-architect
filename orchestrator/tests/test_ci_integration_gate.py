"""Tests verifying CI Gate 4: integration tests with service containers.

Validates SPEC Section 16.2 — Gate 4 (Integration Tests) is present in
.github/workflows/ci.yml with Neo4j + Kafka service containers, E2E test
execution, and correct job dependencies.
"""

from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
CI_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "ci.yml"


def _load_ci() -> dict:
    ci = yaml.safe_load(CI_WORKFLOW.read_text())
    if True in ci and "on" not in ci:
        ci["on"] = ci.pop(True)
    return ci


class TestIntegrationTestJobExists:
    def test_integration_test_job_defined(self) -> None:
        ci = _load_ci()
        jobs = ci.get("jobs", {})
        assert "integration-test" in jobs, (
            "CI must define an 'integration-test' job (Gate 4)"
        )


class TestServiceContainers:
    def test_neo4j_service_container(self) -> None:
        ci = _load_ci()
        job = ci["jobs"]["integration-test"]
        services = job.get("services", {})
        assert "neo4j" in services, (
            "integration-test must have a neo4j service container"
        )
        neo4j = services["neo4j"]
        assert "neo4j" in neo4j.get("image", ""), (
            "neo4j service must use an official neo4j image"
        )

    def test_neo4j_image_version(self) -> None:
        ci = _load_ci()
        job = ci["jobs"]["integration-test"]
        neo4j = job["services"]["neo4j"]
        image = neo4j.get("image", "")
        version = image.split(":")[1] if ":" in image else ""
        assert version.startswith("5.") or version == "5", (
            f"neo4j service should use 5.x community image, got '{image}'"
        )

    def test_neo4j_ports_exposed(self) -> None:
        ci = _load_ci()
        job = ci["jobs"]["integration-test"]
        neo4j = job["services"]["neo4j"]
        ports = [str(p) for p in neo4j.get("ports", [])]
        bolt_mapped = any("7687" in p for p in ports)
        assert bolt_mapped, "neo4j service must expose bolt port 7687"

    def test_kafka_service_container(self) -> None:
        ci = _load_ci()
        job = ci["jobs"]["integration-test"]
        services = job.get("services", {})
        assert "kafka" in services, (
            "integration-test must have a kafka service container"
        )


class TestIntegrationTestSteps:
    def test_runs_contract_tests(self) -> None:
        ci = _load_ci()
        job = ci["jobs"]["integration-test"]
        step_runs = [s.get("run", "") for s in job.get("steps", [])]
        step_names = [s.get("name", "") for s in job.get("steps", [])]
        has_contract = any("contract" in r.lower() for r in step_runs) or any(
            "contract" in n.lower() for n in step_names
        )
        assert has_contract, (
            "integration-test must run contract tests"
        )

    def test_runs_e2e_tests(self) -> None:
        ci = _load_ci()
        job = ci["jobs"]["integration-test"]
        step_runs = [s.get("run", "") for s in job.get("steps", [])]
        step_names = [s.get("name", "") for s in job.get("steps", [])]
        has_e2e = any("e2e" in r.lower() for r in step_runs) or any(
            "e2e" in n.lower() for n in step_names
        )
        assert has_e2e, "integration-test must run E2E tests"


class TestJobDependencies:
    def test_depends_on_python_test(self) -> None:
        ci = _load_ci()
        job = ci["jobs"]["integration-test"]
        needs = job.get("needs", [])
        if isinstance(needs, str):
            needs = [needs]
        assert "python-test" in needs, (
            "integration-test must depend on python-test"
        )

    def test_depends_on_go_test(self) -> None:
        ci = _load_ci()
        job = ci["jobs"]["integration-test"]
        needs = job.get("needs", [])
        if isinstance(needs, str):
            needs = [needs]
        assert "go-test" in needs, (
            "integration-test must depend on go-test"
        )


class TestTimeoutConfig:
    def test_has_timeout(self) -> None:
        ci = _load_ci()
        job = ci["jobs"]["integration-test"]
        assert "timeout-minutes" in job, (
            "integration-test must have a timeout-minutes setting"
        )

    def test_timeout_reasonable(self) -> None:
        ci = _load_ci()
        job = ci["jobs"]["integration-test"]
        timeout = job.get("timeout-minutes", 0)
        assert 10 <= timeout <= 30, (
            f"timeout-minutes should be 10-30, got {timeout}"
        )
