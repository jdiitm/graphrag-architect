from __future__ import annotations

import pathlib
import subprocess
import sys
import tomllib

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
PYPROJECT = REPO_ROOT / "pyproject.toml"
CI_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "ci.yml"
ORCHESTRATOR_APP = REPO_ROOT / "orchestrator" / "app"


def _load_pyproject() -> dict:
    return tomllib.loads(PYPROJECT.read_text())


def _load_ci_yaml() -> str:
    return CI_WORKFLOW.read_text()


class TestMypyConfiguration:
    def test_pyproject_has_mypy_section(self):
        cfg = _load_pyproject()
        assert "mypy" in cfg.get("tool", {}), (
            "pyproject.toml must contain a [tool.mypy] section"
        )

    def test_mypy_strict_enabled(self):
        cfg = _load_pyproject()
        mypy = cfg["tool"]["mypy"]
        assert mypy.get("strict") is True, (
            "[tool.mypy] must set strict = true"
        )

    def test_mypy_python_version(self):
        cfg = _load_pyproject()
        mypy = cfg["tool"]["mypy"]
        assert mypy.get("python_version") == "3.12", (
            "[tool.mypy] must target python_version = '3.12'"
        )

    def test_mypy_show_error_codes(self):
        cfg = _load_pyproject()
        mypy = cfg["tool"]["mypy"]
        assert mypy.get("show_error_codes") is True

    def test_mypy_test_override_relaxes_strict(self):
        cfg = _load_pyproject()
        overrides = cfg["tool"]["mypy"].get("overrides", [])
        test_override = [
            o for o in overrides
            if "orchestrator.tests.*" in (
                o.get("module", []) if isinstance(o.get("module"), list)
                else [o.get("module", "")]
            )
        ]
        assert test_override, (
            "[tool.mypy] must have an override relaxing strict for orchestrator.tests.*"
        )


class TestRuffConfiguration:
    def test_pyproject_has_ruff_section(self):
        cfg = _load_pyproject()
        assert "ruff" in cfg.get("tool", {}), (
            "pyproject.toml must contain a [tool.ruff] section"
        )

    def test_ruff_target_version(self):
        cfg = _load_pyproject()
        ruff = cfg["tool"]["ruff"]
        assert ruff.get("target-version") == "py312"

    def test_ruff_lint_rules_configured(self):
        cfg = _load_pyproject()
        lint = cfg["tool"]["ruff"].get("lint", {})
        select = lint.get("select", [])
        required = {"E", "W", "F", "I", "UP", "B"}
        assert required.issubset(set(select)), (
            f"ruff lint.select must include at minimum {required}, got {select}"
        )


class TestCIWorkflow:
    def test_ci_has_mypy_job(self):
        content = _load_ci_yaml()
        assert "python-mypy" in content, (
            "ci.yml must contain a python-mypy job"
        )

    def test_ci_mypy_runs_strict(self):
        content = _load_ci_yaml()
        assert "mypy" in content and "--strict" in content, (
            "ci.yml mypy job must run with --strict flag"
        )

    def test_ci_has_ruff_job(self):
        content = _load_ci_yaml()
        assert "python-ruff" in content, (
            "ci.yml must contain a python-ruff job"
        )

    def test_ci_ruff_runs_check(self):
        content = _load_ci_yaml()
        assert "ruff check" in content, (
            "ci.yml ruff job must run 'ruff check'"
        )


class TestMypyPassesOnApp:
    def test_mypy_clean_on_orchestrator_app(self):
        result = subprocess.run(
            [
                sys.executable, "-m", "mypy",
                "--strict",
                "--config-file", str(PYPROJECT),
                str(ORCHESTRATOR_APP),
            ],
            capture_output=True,
            text=True,
            cwd=str(REPO_ROOT),
            timeout=120,
        )
        assert result.returncode == 0, (
            f"mypy --strict failed on orchestrator/app/:\n"
            f"{result.stdout}\n{result.stderr}"
        )


class TestPyTypedMarker:
    def test_py_typed_exists(self):
        marker = ORCHESTRATOR_APP / "py.typed"
        assert marker.exists(), (
            "orchestrator/app/py.typed must exist for PEP 561 compliance"
        )
