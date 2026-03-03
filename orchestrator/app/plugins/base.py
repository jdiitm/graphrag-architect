from __future__ import annotations

import abc
from typing import Any, Dict, List, Sequence


class EntityExtractor(abc.ABC):

    @abc.abstractmethod
    def extract(self, text: str) -> List[Dict[str, Any]]:
        ...

    @abc.abstractmethod
    def supported_entity_types(self) -> Sequence[str]:
        ...


class QueryStrategy(abc.ABC):

    @abc.abstractmethod
    def execute(self, query: str, context: Dict[str, Any]) -> Dict[str, Any]:
        ...

    @property
    @abc.abstractmethod
    def name(self) -> str:
        ...
