from __future__ import annotations

import hashlib
import logging
import os
from dataclasses import dataclass, field
from typing import List

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Migration:
    version: int
    name: str
    cypher: str
    checksum: str = ""

    def compute_checksum(self) -> str:
        return hashlib.sha256(self.cypher.encode()).hexdigest()[:16]


@dataclass
class SchemaState:
    current_version: int = 0
    applied_migrations: List[str] = field(default_factory=list)


_SCHEMA_PATH = os.path.join(
    os.path.dirname(__file__), "schema_init.cypher",
)


def load_canonical_schema() -> str:
    with open(_SCHEMA_PATH, encoding="utf-8") as fh:
        return fh.read()


def parse_migrations(schema: str) -> List[Migration]:
    migrations: List[Migration] = []
    version = 0
    for line in schema.strip().splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("--"):
            continue
        version += 1
        migrations.append(Migration(
            version=version,
            name=stripped[:60],
            cypher=stripped,
        ))
    return migrations


class SchemaVersionTracker:
    def __init__(self) -> None:
        self._state = SchemaState()

    @property
    def current_version(self) -> int:
        return self._state.current_version

    @property
    def applied_migrations(self) -> List[str]:
        return list(self._state.applied_migrations)

    def record_applied(self, migration: Migration) -> None:
        self._state.current_version = migration.version
        self._state.applied_migrations.append(migration.name)

    def pending_migrations(
        self, all_migrations: List[Migration],
    ) -> List[Migration]:
        return [
            m for m in all_migrations
            if m.version > self._state.current_version
        ]

    def is_up_to_date(self, all_migrations: List[Migration]) -> bool:
        return len(self.pending_migrations(all_migrations)) == 0
