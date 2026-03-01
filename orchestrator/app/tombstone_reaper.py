from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Protocol

logger = logging.getLogger(__name__)


_DEFAULT_TTL_DAYS = 7
_DEFAULT_BATCH_SIZE = 100
_DEFAULT_REAP_INTERVAL_SECONDS = 3600
_DEFAULT_MAX_BATCH_SIZE = 2000


class TombstoneStore(Protocol):
    async def reap_tombstone_batch(
        self,
        *,
        batch_size: int,
        cutoff: str,
        tenant_id: str,
    ) -> int: ...

    async def count_pending_tombstones(
        self,
        *,
        tenant_id: str,
        cutoff: str,
    ) -> int: ...


@dataclass(frozen=True)
class TombstoneReaperConfig:
    ttl_days: int = _DEFAULT_TTL_DAYS
    batch_size: int = _DEFAULT_BATCH_SIZE
    reap_interval_seconds: float = _DEFAULT_REAP_INTERVAL_SECONDS
    max_batch_size: int = _DEFAULT_MAX_BATCH_SIZE

    @classmethod
    def from_env(cls) -> TombstoneReaperConfig:
        raw_ttl = os.environ.get("TOMBSTONE_TTL_DAYS", "")
        raw_batch = os.environ.get("TOMBSTONE_BATCH_SIZE", "")
        raw_interval = os.environ.get("TOMBSTONE_REAP_INTERVAL_SECONDS", "")
        raw_max_batch = os.environ.get("TOMBSTONE_MAX_BATCH_SIZE", "")
        return cls(
            ttl_days=int(raw_ttl) if raw_ttl else _DEFAULT_TTL_DAYS,
            batch_size=int(raw_batch) if raw_batch else _DEFAULT_BATCH_SIZE,
            reap_interval_seconds=(
                float(raw_interval) if raw_interval else _DEFAULT_REAP_INTERVAL_SECONDS
            ),
            max_batch_size=(
                int(raw_max_batch) if raw_max_batch else _DEFAULT_MAX_BATCH_SIZE
            ),
        )


class TombstoneReaper:
    def __init__(
        self,
        client: Any,
        config: TombstoneReaperConfig,
        tenant_id: str = "",
    ) -> None:
        self._client = client
        self._config = config
        self._tenant_id = tenant_id
        self._task: Optional[asyncio.Task[None]] = None
        self._reaped_total = 0
        self._pending = 0
        self._last_effective_batch = 0

    @property
    def running(self) -> bool:
        return self._task is not None and not self._task.done()

    @property
    def metrics(self) -> Dict[str, int]:
        return {
            "reaped_total": self._reaped_total,
            "pending": self._pending,
            "last_effective_batch": self._last_effective_batch,
        }

    def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        self._task = asyncio.create_task(self._reap_loop())

    async def stop(self) -> None:
        if self._task is None or self._task.done():
            self._task = None
            return
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        self._task = None

    async def reap_once(self) -> int:
        cutoff = (
            datetime.now(timezone.utc) - timedelta(days=self._config.ttl_days)
        ).isoformat()

        total_reaped = 0
        effective_batch = self._config.batch_size
        while True:
            reaped = await self._client.reap_tombstone_batch(
                batch_size=effective_batch,
                cutoff=cutoff,
                tenant_id=self._tenant_id,
            )
            total_reaped += reaped
            if reaped < effective_batch:
                break
            effective_batch = min(effective_batch * 2, self._config.max_batch_size)

        self._last_effective_batch = effective_batch
        self._reaped_total += total_reaped

        self._pending = await self._client.count_pending_tombstones(
            tenant_id=self._tenant_id,
            cutoff=cutoff,
        )

        if total_reaped > 0:
            logger.info(
                "Reaped %d tombstoned edges (tenant=%s, pending=%d, last_batch=%d)",
                total_reaped, self._tenant_id or "all", self._pending,
                self._last_effective_batch,
            )

        return total_reaped

    async def _reap_loop(self) -> None:
        while True:
            try:
                await self.reap_once()
            except asyncio.CancelledError:
                return
            except Exception:
                logger.exception("Tombstone reaper cycle failed")
            await asyncio.sleep(self._config.reap_interval_seconds)
