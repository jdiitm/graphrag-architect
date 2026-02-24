from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Dict, List, Optional

import yaml


class PromptNotFoundError(Exception):
    pass


@dataclass(frozen=True)
class PromptTemplate:
    name: str
    version: str
    system: str
    human: str

    @property
    def key(self) -> str:
        return f"{self.name}:{self.version}"

    def format_human(self, **kwargs: str) -> str:
        return self.human.format(**kwargs)

    @classmethod
    def from_yaml_string(cls, content: str) -> PromptTemplate:
        data = yaml.safe_load(content)
        return cls(
            name=data["name"],
            version=data["version"],
            system=data["system"],
            human=data["human"],
        )

    @classmethod
    def from_file(cls, path: str) -> PromptTemplate:
        with open(path, encoding="utf-8") as fh:
            return cls.from_yaml_string(fh.read())


class PromptRegistry:
    def __init__(self) -> None:
        self._templates: Dict[str, PromptTemplate] = {}
        self._by_name: Dict[str, List[str]] = {}

    def register(self, template: PromptTemplate) -> None:
        self._templates[template.key] = template
        versions = self._by_name.setdefault(template.name, [])
        if template.version not in versions:
            versions.append(template.version)

    def get(self, name: str, version: str) -> PromptTemplate:
        key = f"{name}:{version}"
        if key not in self._templates:
            raise PromptNotFoundError(
                f"Prompt {name!r} version {version!r} not found"
            )
        return self._templates[key]

    def get_latest(self, name: str) -> PromptTemplate:
        versions = self._by_name.get(name, [])
        if not versions:
            raise PromptNotFoundError(f"No versions for prompt {name!r}")
        return self.get(name, versions[-1])

    def get_active(
        self, name: str, env_version: Optional[str] = None,
    ) -> PromptTemplate:
        if env_version:
            return self.get(name, env_version)
        return self.get_latest(name)

    def list_names(self) -> List[str]:
        return list(self._by_name.keys())

    @classmethod
    def from_directory(cls, directory: str) -> PromptRegistry:
        registry = cls()
        for entry in sorted(os.listdir(directory)):
            if entry.endswith((".yaml", ".yml")):
                path = os.path.join(directory, entry)
                template = PromptTemplate.from_file(path)
                registry.register(template)
        return registry
