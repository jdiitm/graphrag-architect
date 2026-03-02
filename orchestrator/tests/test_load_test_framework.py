from __future__ import annotations

import json
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
LOAD_TEST_DIR = REPO_ROOT / "tests" / "load"


class TestLoadTestDirectoryExists:

    def test_load_test_directory_exists(self) -> None:
        assert LOAD_TEST_DIR.is_dir(), (
            "tests/load/ directory must exist at repository root"
        )


class TestRequiredK6ScriptsExist:

    def test_at_least_three_k6_scripts(self) -> None:
        scripts = list(LOAD_TEST_DIR.glob("*.js"))
        assert len(scripts) >= 3, (
            f"Expected at least 3 k6 scripts in tests/load/, "
            f"found {len(scripts)}: {[s.name for s in scripts]}"
        )

    def test_ingestion_script_exists(self) -> None:
        assert (LOAD_TEST_DIR / "ingestion.js").is_file(), (
            "tests/load/ingestion.js must exist"
        )

    def test_query_script_exists(self) -> None:
        assert (LOAD_TEST_DIR / "query.js").is_file(), (
            "tests/load/query.js must exist"
        )

    def test_soak_script_exists(self) -> None:
        assert (LOAD_TEST_DIR / "soak.js").is_file(), (
            "tests/load/soak.js must exist"
        )


class TestIngestionScriptThresholds:

    @pytest.fixture(name="ingestion_content")
    def _ingestion_content(self) -> str:
        return (LOAD_TEST_DIR / "ingestion.js").read_text(encoding="utf-8")

    def test_has_thresholds_block(self, ingestion_content: str) -> None:
        assert "thresholds" in ingestion_content, (
            "ingestion.js must define k6 thresholds"
        )

    def test_p50_threshold_matches_spec(self, ingestion_content: str) -> None:
        assert "p(50)" in ingestion_content and "500" in ingestion_content, (
            "ingestion.js must define p50 < 500ms threshold (SPEC NFR-1)"
        )

    def test_p99_threshold_matches_spec(self, ingestion_content: str) -> None:
        assert "p(99)" in ingestion_content and "2000" in ingestion_content, (
            "ingestion.js must define p99 < 2s threshold (SPEC NFR-1)"
        )

    def test_has_ramp_stages(self, ingestion_content: str) -> None:
        assert "stages" in ingestion_content, (
            "ingestion.js must define ramp-up stages"
        )

    def test_posts_to_ingest_endpoint(self, ingestion_content: str) -> None:
        assert "/ingest" in ingestion_content, (
            "ingestion.js must target POST /ingest"
        )


class TestQueryScriptThresholds:

    @pytest.fixture(name="query_content")
    def _query_content(self) -> str:
        return (LOAD_TEST_DIR / "query.js").read_text(encoding="utf-8")

    def test_has_thresholds_block(self, query_content: str) -> None:
        assert "thresholds" in query_content, (
            "query.js must define k6 thresholds"
        )

    def test_vector_p99_threshold(self, query_content: str) -> None:
        assert "p(99)" in query_content and "500" in query_content, (
            "query.js must define vector p99 < 500ms threshold (SPEC NFR-2)"
        )

    def test_graph_p99_threshold(self, query_content: str) -> None:
        assert "p(99)" in query_content and "3000" in query_content, (
            "query.js must define graph p99 < 3s threshold (SPEC NFR-2)"
        )

    def test_posts_to_query_endpoint(self, query_content: str) -> None:
        assert "/query" in query_content, (
            "query.js must target POST /query"
        )


class TestSoakScriptProfile:

    @pytest.fixture(name="soak_content")
    def _soak_content(self) -> str:
        return (LOAD_TEST_DIR / "soak.js").read_text(encoding="utf-8")

    def test_has_extended_duration(self, soak_content: str) -> None:
        assert "24h" in soak_content or "86400" in soak_content, (
            "soak.js must define a 24h test duration"
        )

    def test_has_concurrent_vus(self, soak_content: str) -> None:
        assert "100" in soak_content, (
            "soak.js must target 100 concurrent VUs"
        )

    def test_has_memory_check(self, soak_content: str) -> None:
        assert "memory" in soak_content.lower() or "heap" in soak_content.lower(), (
            "soak.js must include memory stability assertion"
        )


class TestConfigurationFile:

    def test_config_file_exists(self) -> None:
        assert (LOAD_TEST_DIR / "config.json").is_file(), (
            "tests/load/config.json must exist"
        )

    def test_config_is_valid_json(self) -> None:
        content = (LOAD_TEST_DIR / "config.json").read_text(encoding="utf-8")
        config = json.loads(content)
        assert isinstance(config, dict)

    def test_config_has_environment_settings(self) -> None:
        content = (LOAD_TEST_DIR / "config.json").read_text(encoding="utf-8")
        config = json.loads(content)
        for env in ("dev", "staging", "prod"):
            assert env in config, (
                f"config.json must contain '{env}' environment settings"
            )

    def test_each_environment_has_base_url(self) -> None:
        content = (LOAD_TEST_DIR / "config.json").read_text(encoding="utf-8")
        config = json.loads(content)
        for env in ("dev", "staging", "prod"):
            env_config = config.get(env, {})
            assert "base_url" in env_config, (
                f"config.json '{env}' must define base_url"
            )
