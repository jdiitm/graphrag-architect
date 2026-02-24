from __future__ import annotations

import asyncio
import json
from typing import Any, Dict, List, Protocol


class HopRunner(Protocol):
    async def run_hop(self, names: List[str]) -> List[Dict[str, Any]]: ...


def cap_candidates(
    candidates: List[Dict[str, Any]], limit: int = 50,
) -> List[Dict[str, Any]]:
    return candidates[:limit]


def partition_names(
    names: List[str], batch_size: int = 50,
) -> List[List[str]]:
    if not names:
        return []
    return [
        names[i:i + batch_size]
        for i in range(0, len(names), batch_size)
    ]


class BatchedHopExecutor:
    def __init__(
        self,
        runner: HopRunner,
        candidate_limit: int = 50,
        batch_size: int = 50,
    ) -> None:
        self._runner = runner
        self._candidate_limit = candidate_limit
        self._batch_size = batch_size

    async def execute(
        self, candidates: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        capped = cap_candidates(candidates, self._candidate_limit)
        if not capped:
            return []

        names = [
            c.get("name", c.get("result", {}).get("name", ""))
            for c in capped
        ]
        names = [n for n in names if n]

        batches = partition_names(names, self._batch_size)
        tasks = [self._runner.run_hop(batch) for batch in batches]
        batch_results = await asyncio.gather(*tasks)

        seen: set[str] = set()
        deduped: List[Dict[str, Any]] = []
        for batch_result in batch_results:
            for record in batch_result:
                key = json.dumps(record, sort_keys=True, default=str)
                if key not in seen:
                    seen.add(key)
                    deduped.append(record)

        return deduped
