from __future__ import annotations

import asyncio
import json
import logging
import os
from dataclasses import dataclass
from typing import Any, Callable, Coroutine, Dict, List, Optional

from orchestrator.app.prompt_sanitizer import sanitize_source_content

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ExtractionEvent:
    staging_path: str
    headers: Dict[str, str]

    @classmethod
    def from_json(cls, raw: bytes) -> ExtractionEvent:
        data = json.loads(raw)
        return cls(
            staging_path=data["staging_path"],
            headers=data.get("headers", {}),
        )


@dataclass(frozen=True)
class ExtractionWorkerConfig:
    kafka_brokers: str = "localhost:9092"
    topic: str = "extraction-pending"
    consumer_group: str = "extraction-workers"
    max_concurrent: int = 5
    staging_dir: str = "/tmp/graphrag-staging"


IngestCallback = Callable[[List[Dict[str, str]]], Coroutine[Any, Any, Dict[str, Any]]]


class ExtractionWorker:
    def __init__(
        self,
        config: ExtractionWorkerConfig,
        ingest_callback: IngestCallback,
    ) -> None:
        self._config = config
        self._ingest = ingest_callback
        self._semaphore = asyncio.Semaphore(config.max_concurrent)

    async def process_event(self, event: ExtractionEvent) -> Dict[str, Any]:
        async with self._semaphore:
            if not self._is_safe_staging_path(event.staging_path):
                return {
                    "status": "failed",
                    "error": "path traversal detected",
                }

            content = self._read_staged_file(event.staging_path)
            if content is None:
                return {"status": "failed", "error": "staging file not found"}

            file_path = event.headers.get("file_path", event.staging_path)
            content = sanitize_source_content(content, file_path)

            raw_files = [{
                "path": file_path,
                "content": content,
            }]
            return await self._ingest(raw_files)

    def _is_safe_staging_path(self, path: str) -> bool:
        staging_root = os.path.realpath(self._config.staging_dir)
        resolved = os.path.realpath(path)
        return resolved.startswith(staging_root + os.sep)

    @staticmethod
    def _read_staged_file(path: str) -> Optional[str]:
        try:
            with open(path, encoding="utf-8") as f:
                return f.read()
        except (OSError, UnicodeDecodeError) as exc:
            logger.error("Failed to read staged file %s: %s", path, exc)
            return None

    async def run(self, events: List[ExtractionEvent]) -> List[Dict[str, Any]]:
        tasks = [self.process_event(event) for event in events]
        return await asyncio.gather(*tasks)
