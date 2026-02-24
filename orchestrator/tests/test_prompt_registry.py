from __future__ import annotations

import os
import tempfile

import pytest

from orchestrator.app.prompt_registry import (
    PromptNotFoundError,
    PromptRegistry,
    PromptTemplate,
)


SAMPLE_PROMPT_YAML = """
name: extraction
version: v2
system: "You are an entity extraction expert."
human: "Extract entities from: {file_contents}"
"""


class TestPromptTemplate:
    def test_from_yaml(self) -> None:
        tpl = PromptTemplate.from_yaml_string(SAMPLE_PROMPT_YAML)
        assert tpl.name == "extraction"
        assert tpl.version == "v2"
        assert "entity extraction" in tpl.system
        assert "{file_contents}" in tpl.human

    def test_format_human(self) -> None:
        tpl = PromptTemplate.from_yaml_string(SAMPLE_PROMPT_YAML)
        result = tpl.format_human(file_contents="def main(): pass")
        assert "def main(): pass" in result

    def test_key(self) -> None:
        tpl = PromptTemplate.from_yaml_string(SAMPLE_PROMPT_YAML)
        assert tpl.key == "extraction:v2"


class TestPromptRegistry:
    def test_register_and_get(self) -> None:
        registry = PromptRegistry()
        tpl = PromptTemplate.from_yaml_string(SAMPLE_PROMPT_YAML)
        registry.register(tpl)
        result = registry.get("extraction", "v2")
        assert result.system == tpl.system

    def test_get_nonexistent_raises(self) -> None:
        registry = PromptRegistry()
        with pytest.raises(PromptNotFoundError):
            registry.get("unknown", "v1")

    def test_get_latest_version(self) -> None:
        registry = PromptRegistry()
        v1 = PromptTemplate(
            name="extraction", version="v1",
            system="old system", human="old {file_contents}",
        )
        v2 = PromptTemplate(
            name="extraction", version="v2",
            system="new system", human="new {file_contents}",
        )
        registry.register(v1)
        registry.register(v2)
        latest = registry.get_latest("extraction")
        assert latest.version == "v2"

    def test_load_from_directory(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "extraction_v2.yaml")
            with open(path, "w", encoding="utf-8") as fh:
                fh.write(SAMPLE_PROMPT_YAML)
            registry = PromptRegistry.from_directory(tmpdir)
        result = registry.get("extraction", "v2")
        assert result.name == "extraction"

    def test_list_names(self) -> None:
        registry = PromptRegistry()
        tpl = PromptTemplate.from_yaml_string(SAMPLE_PROMPT_YAML)
        registry.register(tpl)
        assert "extraction" in registry.list_names()

    def test_active_version_from_env(self) -> None:
        registry = PromptRegistry()
        v1 = PromptTemplate(
            name="extraction", version="v1",
            system="v1 system", human="{file_contents}",
        )
        v2 = PromptTemplate(
            name="extraction", version="v2",
            system="v2 system", human="{file_contents}",
        )
        registry.register(v1)
        registry.register(v2)
        result = registry.get_active("extraction", env_version="v1")
        assert result.version == "v1"

    def test_active_version_defaults_to_latest(self) -> None:
        registry = PromptRegistry()
        v1 = PromptTemplate(
            name="extraction", version="v1",
            system="v1 system", human="{file_contents}",
        )
        v2 = PromptTemplate(
            name="extraction", version="v2",
            system="v2 system", human="{file_contents}",
        )
        registry.register(v1)
        registry.register(v2)
        result = registry.get_active("extraction", env_version="")
        assert result.version == "v2"
