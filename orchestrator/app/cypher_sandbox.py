from __future__ import annotations

import hashlib
import logging
import re
from dataclasses import dataclass
from typing import Any, Dict, FrozenSet, List, Optional

logger = logging.getLogger(__name__)


class CypherWhitelistError(Exception):
    pass


@dataclass(frozen=True)
class CypherSandboxConfig:
    max_results: int = 1000
    query_timeout_seconds: float = 30.0


_LIMIT_PATTERN = re.compile(r"\bLIMIT\s+(\d+)", re.IGNORECASE)


def _normalize_cypher(cypher: str) -> str:
    return " ".join(cypher.split())


def _hash_cypher(cypher: str) -> str:
    normalized = _normalize_cypher(cypher)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


class TemplateHashRegistry:
    def __init__(self, catalog: Any) -> None:
        hashes: set[str] = set()
        for template in catalog.all_templates().values():
            hashes.add(_hash_cypher(template.cypher))
        self._hashes: FrozenSet[str] = frozenset(hashes)

    @property
    def registered_hashes(self) -> FrozenSet[str]:
        return self._hashes

    def is_allowed(self, cypher: str) -> bool:
        return _hash_cypher(cypher) in self._hashes


class SandboxedQueryExecutor:
    def __init__(
        self,
        config: Optional[CypherSandboxConfig] = None,
        registry: Optional[TemplateHashRegistry] = None,
    ) -> None:
        self._config = config or CypherSandboxConfig()
        self._registry = registry

    @property
    def config(self) -> CypherSandboxConfig:
        return self._config

    def inject_limit(self, cypher: str) -> str:
        def _cap_limit(match: re.Match) -> str:
            value = int(match.group(1))
            if value > self._config.max_results:
                return f"LIMIT {self._config.max_results}"
            return match.group(0)

        if _LIMIT_PATTERN.search(cypher):
            return _LIMIT_PATTERN.sub(_cap_limit, cypher)
        return f"{cypher.rstrip().rstrip(';')} LIMIT {self._config.max_results}"

    def validate(self, cypher: str) -> None:
        if self._registry is not None and not self._registry.is_allowed(cypher):
            raise CypherWhitelistError(
                f"Cypher query not in registered template whitelist: "
                f"{_hash_cypher(cypher)[:16]}..."
            )

    async def execute_read(
        self,
        session: Any,
        cypher: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        self.validate(cypher)
        sandboxed = self.inject_limit(cypher)

        async def _tx(tx: Any) -> List[Dict[str, Any]]:
            result = await tx.run(
                sandboxed, **(params or {}),
                timeout=self._config.query_timeout_seconds,
            )
            return await result.data()

        return await session.execute_read(_tx)
