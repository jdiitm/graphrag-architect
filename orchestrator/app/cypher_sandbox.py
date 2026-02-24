from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class QueryTooExpensiveError(Exception):
    pass


@dataclass(frozen=True)
class CypherSandboxConfig:
    max_results: int = 1000
    max_estimated_rows: int = 100_000
    query_timeout_seconds: float = 30.0


_LIMIT_PATTERN = re.compile(r"\bLIMIT\s+(\d+)", re.IGNORECASE)


class SandboxedQueryExecutor:
    def __init__(self, config: Optional[CypherSandboxConfig] = None) -> None:
        self._config = config or CypherSandboxConfig()

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

    async def explain_check(self, session: Any, cypher: str) -> None:
        explain_query = f"EXPLAIN {cypher}"
        result = await session.run(explain_query)
        rows = await result.data()
        if not rows:
            return
        plan = rows[0].get("Plan", {})
        estimated = plan.get("estimatedRows", 0)
        if estimated > self._config.max_estimated_rows:
            raise QueryTooExpensiveError(
                f"Query estimated {estimated} rows, exceeds limit of "
                f"{self._config.max_estimated_rows}"
            )

    async def execute_read(
        self,
        session: Any,
        cypher: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        sandboxed = self.inject_limit(cypher)

        async def _tx(tx: Any) -> List[Dict[str, Any]]:
            result = await tx.run(
                sandboxed, **(params or {}),
                timeout=self._config.query_timeout_seconds,
            )
            return await result.data()

        return await session.execute_read(_tx)
