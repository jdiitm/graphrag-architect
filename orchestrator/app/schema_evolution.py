from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Protocol

logger = logging.getLogger(__name__)


class MigrationStatus(Enum):
    PENDING = "pending"
    APPLIED = "applied"
    FAILED = "failed"
    ROLLED_BACK = "rolled_back"


@dataclass(frozen=True)
class SchemaVersion:
    major: int
    minor: int
    patch: int

    def __str__(self) -> str:
        return f"{self.major}.{self.minor}.{self.patch}"

    def __lt__(self, other: SchemaVersion) -> bool:
        return (self.major, self.minor, self.patch) < (
            other.major, other.minor, other.patch,
        )

    def __le__(self, other: SchemaVersion) -> bool:
        return self == other or self < other

    @classmethod
    def parse(cls, version_str: str) -> SchemaVersion:
        parts = version_str.strip().split(".")
        if len(parts) != 3:
            raise ValueError(
                f"Invalid version format: {version_str!r}, expected MAJOR.MINOR.PATCH"
            )
        return cls(
            major=int(parts[0]),
            minor=int(parts[1]),
            patch=int(parts[2]),
        )


MigrationFn = Callable[[], None]


@dataclass
class Migration:
    version: SchemaVersion
    description: str
    up: MigrationFn
    down: Optional[MigrationFn] = None
    status: MigrationStatus = MigrationStatus.PENDING


class VersionStore(Protocol):
    def get_current_version(self) -> Optional[SchemaVersion]: ...
    def set_current_version(self, version: SchemaVersion) -> None: ...
    def record_migration(
        self, version: SchemaVersion, status: MigrationStatus,
    ) -> None: ...


class InMemoryVersionStore:
    def __init__(self) -> None:
        self._current: Optional[SchemaVersion] = None
        self._history: List[Dict[str, Any]] = []

    def get_current_version(self) -> Optional[SchemaVersion]:
        return self._current

    def set_current_version(self, version: SchemaVersion) -> None:
        self._current = version

    def record_migration(
        self, version: SchemaVersion, status: MigrationStatus,
    ) -> None:
        self._history.append({
            "version": str(version),
            "status": status.value,
        })

    @property
    def history(self) -> List[Dict[str, Any]]:
        return list(self._history)


class Neo4jVersionStore:
    _POINTER_LABEL = "_SchemaPointer"
    _MIGRATION_LABEL = "_SchemaMigration"

    def __init__(self, driver: Any) -> None:
        self._driver = driver

    def get_current_version(self) -> Optional[SchemaVersion]:
        with self._driver.session() as session:
            result = session.run(
                "MATCH (p:_SchemaPointer) "
                "RETURN p.major AS major, p.minor AS minor, p.patch AS patch",
            )
            record = result.single()
            if record is None:
                return None
            return SchemaVersion(
                major=record["major"],
                minor=record["minor"],
                patch=record["patch"],
            )

    def set_current_version(self, version: SchemaVersion) -> None:
        with self._driver.session() as session:
            session.run(
                "MERGE (p:_SchemaPointer) "
                "SET p.major = $major, p.minor = $minor, p.patch = $patch",
                major=version.major,
                minor=version.minor,
                patch=version.patch,
            )

    def record_migration(
        self, version: SchemaVersion, status: MigrationStatus,
    ) -> None:
        with self._driver.session() as session:
            session.run(
                "CREATE (m:_SchemaMigration {"
                "major: $major, minor: $minor, patch: $patch, "
                "status: $status, recorded_at: datetime()})",
                major=version.major,
                minor=version.minor,
                patch=version.patch,
                status=status.value,
            )


class RedisVersionStore:
    _POINTER_KEY = "graphrag:schema:current_version"
    _HISTORY_KEY = "graphrag:schema:migration_history"

    def __init__(self, client: Any) -> None:
        self._client = client

    def get_current_version(self) -> Optional[SchemaVersion]:
        raw = self._client.get(self._POINTER_KEY)
        if raw is None:
            return None
        decoded = raw.decode() if isinstance(raw, bytes) else raw
        return SchemaVersion.parse(decoded)

    def set_current_version(self, version: SchemaVersion) -> None:
        self._client.set(self._POINTER_KEY, str(version))

    def record_migration(
        self, version: SchemaVersion, status: MigrationStatus,
    ) -> None:
        self._client.rpush(
            self._HISTORY_KEY, f"{version}:{status.value}",
        )


_KNOWN_BACKENDS = {"memory", "neo4j", "redis"}


def create_version_store(backend: str = "memory", **kwargs: Any) -> VersionStore:
    if backend == "memory":
        return InMemoryVersionStore()
    if backend == "neo4j":
        return Neo4jVersionStore(driver=kwargs["driver"])
    if backend == "redis":
        return RedisVersionStore(client=kwargs["client"])
    raise ValueError(
        f"Unknown version store backend: {backend!r}. "
        f"Valid backends: {', '.join(sorted(_KNOWN_BACKENDS))}"
    )


class MigrationRegistry:
    def __init__(self, store: VersionStore) -> None:
        self._store = store
        self._migrations: List[Migration] = []

    def register(self, migration: Migration) -> None:
        for existing in self._migrations:
            if existing.version == migration.version:
                raise ValueError(
                    f"Duplicate migration version: {migration.version}"
                )
        self._migrations.append(migration)
        self._migrations.sort(key=lambda m: m.version)

    def pending(self) -> List[Migration]:
        current = self._store.get_current_version()
        return [
            m for m in self._migrations
            if current is None or current < m.version
        ]

    def apply_all(self) -> int:
        applied_count = 0
        for migration in self.pending():
            try:
                migration.up()
                migration.status = MigrationStatus.APPLIED
                self._store.set_current_version(migration.version)
                self._store.record_migration(
                    migration.version, MigrationStatus.APPLIED,
                )
                applied_count += 1
                logger.info(
                    "Applied migration %s: %s",
                    migration.version, migration.description,
                )
            except Exception:
                migration.status = MigrationStatus.FAILED
                self._store.record_migration(
                    migration.version, MigrationStatus.FAILED,
                )
                logger.error(
                    "Migration %s failed", migration.version, exc_info=True,
                )
                raise
        return applied_count

    def rollback_last(self) -> bool:
        current = self._store.get_current_version()
        if current is None:
            return False

        target = None
        for m in self._migrations:
            if m.version == current:
                target = m
                break

        if target is None or target.down is None:
            return False

        try:
            target.down()
            target.status = MigrationStatus.ROLLED_BACK
            self._store.record_migration(
                target.version, MigrationStatus.ROLLED_BACK,
            )
            previous = self._find_previous_version(current)
            if previous is not None:
                self._store.set_current_version(previous)
            else:
                self._store.set_current_version(SchemaVersion(0, 0, 0))
            return True
        except Exception:
            logger.error(
                "Rollback of %s failed", target.version, exc_info=True,
            )
            raise

    def current_version(self) -> Optional[SchemaVersion]:
        return self._store.get_current_version()

    def all_migrations(self) -> List[Migration]:
        return list(self._migrations)

    def _find_previous_version(
        self, current: SchemaVersion,
    ) -> Optional[SchemaVersion]:
        previous = None
        for m in self._migrations:
            if m.version < current:
                previous = m.version
        return previous
