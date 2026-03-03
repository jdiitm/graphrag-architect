from __future__ import annotations

import abc
import inspect
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
APP_DIR = REPO_ROOT / "orchestrator" / "app"
PLUGINS_DIR = APP_DIR / "plugins"
DOCS_DIR = REPO_ROOT / "docs"
SDKS_DIR = REPO_ROOT / "sdks"


class TestPluginDirectory:

    def test_plugins_directory_exists(self) -> None:
        assert PLUGINS_DIR.is_dir(), (
            f"Expected orchestrator/app/plugins/ at {PLUGINS_DIR}"
        )

    def test_plugins_init_exists(self) -> None:
        assert (PLUGINS_DIR / "__init__.py").is_file(), (
            "Expected __init__.py in plugins directory"
        )

    def test_plugins_base_module_exists(self) -> None:
        assert (PLUGINS_DIR / "base.py").is_file(), (
            "Expected base.py in plugins directory"
        )


class TestEntityExtractorABC:

    def test_entity_extractor_importable(self) -> None:
        from orchestrator.app.plugins.base import EntityExtractor
        assert EntityExtractor is not None

    def test_entity_extractor_is_abstract(self) -> None:
        from orchestrator.app.plugins.base import EntityExtractor
        assert inspect.isabstract(EntityExtractor)

    def test_entity_extractor_has_extract_method(self) -> None:
        from orchestrator.app.plugins.base import EntityExtractor
        assert hasattr(EntityExtractor, "extract")
        assert "extract" in EntityExtractor.__abstractmethods__

    def test_entity_extractor_has_supported_types(self) -> None:
        from orchestrator.app.plugins.base import EntityExtractor
        assert hasattr(EntityExtractor, "supported_entity_types")
        assert "supported_entity_types" in EntityExtractor.__abstractmethods__

    def test_entity_extractor_cannot_instantiate_directly(self) -> None:
        from orchestrator.app.plugins.base import EntityExtractor
        with pytest.raises(TypeError):
            EntityExtractor()


class TestQueryStrategyABC:

    def test_query_strategy_importable(self) -> None:
        from orchestrator.app.plugins.base import QueryStrategy
        assert QueryStrategy is not None

    def test_query_strategy_is_abstract(self) -> None:
        from orchestrator.app.plugins.base import QueryStrategy
        assert inspect.isabstract(QueryStrategy)

    def test_query_strategy_has_execute(self) -> None:
        from orchestrator.app.plugins.base import QueryStrategy
        assert hasattr(QueryStrategy, "execute")
        assert "execute" in QueryStrategy.__abstractmethods__

    def test_query_strategy_has_name_property(self) -> None:
        from orchestrator.app.plugins.base import QueryStrategy
        assert hasattr(QueryStrategy, "name")
        assert "name" in QueryStrategy.__abstractmethods__

    def test_query_strategy_cannot_instantiate_directly(self) -> None:
        from orchestrator.app.plugins.base import QueryStrategy
        with pytest.raises(TypeError):
            QueryStrategy()


class TestDPA:

    def test_dpa_exists(self) -> None:
        path = DOCS_DIR / "legal" / "dpa.md"
        assert path.is_file(), "Expected docs/legal/dpa.md"

    def test_dpa_covers_data_processing(self) -> None:
        content = (DOCS_DIR / "legal" / "dpa.md").read_text(
            encoding="utf-8",
        ).lower()
        assert "data processing" in content, (
            "DPA must reference data processing"
        )

    def test_dpa_covers_subprocessors(self) -> None:
        content = (DOCS_DIR / "legal" / "dpa.md").read_text(
            encoding="utf-8",
        ).lower()
        assert "sub-processor" in content or "subprocessor" in content, (
            "DPA must address sub-processors"
        )

    def test_dpa_has_minimum_depth(self) -> None:
        content = (DOCS_DIR / "legal" / "dpa.md").read_text(encoding="utf-8")
        assert len(content) >= 500, (
            "DPA should be substantive (>= 500 chars)"
        )


class TestSDKGenerationConfig:

    def test_sdks_directory_exists(self) -> None:
        assert SDKS_DIR.is_dir(), f"Expected sdks/ directory at {SDKS_DIR}"

    def test_openapi_generator_config_exists(self) -> None:
        path = SDKS_DIR / "openapi-generator-config.yaml"
        assert path.is_file(), (
            "Expected sdks/openapi-generator-config.yaml"
        )

    def test_config_is_valid_yaml(self) -> None:
        content = yaml.safe_load(
            (SDKS_DIR / "openapi-generator-config.yaml").read_text(
                encoding="utf-8",
            )
        )
        assert isinstance(content, dict)

    def test_config_specifies_generator(self) -> None:
        content = yaml.safe_load(
            (SDKS_DIR / "openapi-generator-config.yaml").read_text(
                encoding="utf-8",
            )
        )
        assert "generators" in content or "generatorName" in content, (
            "SDK config must specify generator(s)"
        )
