from __future__ import annotations

import os
from unittest.mock import MagicMock

import pytest

from orchestrator.app.config import SchemaStoreConfig
from orchestrator.app.schema_evolution import (
    InMemoryVersionStore,
    MigrationStatus,
    Neo4jVersionStore,
    RedisVersionStore,
    SchemaVersion,
    create_version_store,
)


def _stateful_neo4j_driver() -> tuple[MagicMock, MagicMock]:
    db: dict[str, object] = {}

    session = MagicMock()
    ctx = MagicMock()
    ctx.__enter__ = MagicMock(return_value=session)
    ctx.__exit__ = MagicMock(return_value=False)

    driver = MagicMock()
    driver.session.return_value = ctx

    def _run(
        query: str, parameters: dict[str, object] | None = None, **kw: object,
    ) -> MagicMock:
        params = {**(parameters or {}), **kw}
        result = MagicMock()

        if "MERGE" in query and "_SchemaPointer" in query:
            db["pointer"] = {
                "major": params["major"],
                "minor": params["minor"],
                "patch": params["patch"],
            }
        elif "MATCH" in query and "_SchemaPointer" in query:
            ptr = db.get("pointer")
            if ptr is None:
                result.single.return_value = None
            else:
                result.single.return_value = dict(ptr)
        elif "CREATE" in query and "_SchemaMigration" in query:
            migrations: list[dict[str, object]] = db.setdefault("migrations", [])
            migrations.append(dict(params))

        return result

    session.run.side_effect = _run
    return driver, session


def _stateful_redis_client() -> MagicMock:
    store: dict[str, object] = {}
    client = MagicMock()

    def _get(key: str) -> bytes | None:
        val = store.get(key)
        if val is None:
            return None
        return val.encode() if isinstance(val, str) else val

    def _set(key: str, value: str | bytes) -> None:
        store[key] = value if isinstance(value, str) else value.decode()

    def _rpush(key: str, *values: str) -> None:
        lst: list[str] = store.setdefault(key, [])
        lst.extend(values)

    client.get.side_effect = _get
    client.set.side_effect = _set
    client.rpush.side_effect = _rpush
    return client


class TestNeo4jVersionStoreProtocol:

    def test_implements_version_store_methods(self) -> None:
        store = Neo4jVersionStore(driver=MagicMock())
        assert callable(getattr(store, "get_current_version", None))
        assert callable(getattr(store, "set_current_version", None))
        assert callable(getattr(store, "record_migration", None))

    def test_empty_database_returns_none(self) -> None:
        driver, _ = _stateful_neo4j_driver()
        store = Neo4jVersionStore(driver=driver)
        assert store.get_current_version() is None

    def test_set_then_get_round_trip(self) -> None:
        driver, _ = _stateful_neo4j_driver()
        store = Neo4jVersionStore(driver=driver)
        version = SchemaVersion(1, 0, 0)
        store.set_current_version(version)
        assert store.get_current_version() == version

    def test_sequential_set_returns_latest(self) -> None:
        driver, _ = _stateful_neo4j_driver()
        store = Neo4jVersionStore(driver=driver)
        store.set_current_version(SchemaVersion(1, 0, 0))
        store.set_current_version(SchemaVersion(2, 0, 0))
        assert store.get_current_version() == SchemaVersion(2, 0, 0)

    def test_record_migration_writes_to_database(self) -> None:
        driver, session = _stateful_neo4j_driver()
        store = Neo4jVersionStore(driver=driver)
        store.record_migration(SchemaVersion(1, 0, 0), MigrationStatus.APPLIED)
        assert session.run.call_count >= 1

    def test_rollback_reflects_previous_version(self) -> None:
        driver, _ = _stateful_neo4j_driver()
        store = Neo4jVersionStore(driver=driver)
        store.set_current_version(SchemaVersion(2, 0, 0))
        store.record_migration(
            SchemaVersion(2, 0, 0), MigrationStatus.ROLLED_BACK,
        )
        store.set_current_version(SchemaVersion(1, 0, 0))
        assert store.get_current_version() == SchemaVersion(1, 0, 0)


class TestRedisVersionStoreProtocol:

    def test_implements_version_store_methods(self) -> None:
        store = RedisVersionStore(client=MagicMock())
        assert callable(getattr(store, "get_current_version", None))
        assert callable(getattr(store, "set_current_version", None))
        assert callable(getattr(store, "record_migration", None))

    def test_empty_client_returns_none(self) -> None:
        client = _stateful_redis_client()
        store = RedisVersionStore(client=client)
        assert store.get_current_version() is None

    def test_set_then_get_round_trip(self) -> None:
        client = _stateful_redis_client()
        store = RedisVersionStore(client=client)
        version = SchemaVersion(1, 2, 3)
        store.set_current_version(version)
        assert store.get_current_version() == version

    def test_record_migration_calls_client(self) -> None:
        client = _stateful_redis_client()
        store = RedisVersionStore(client=client)
        store.record_migration(SchemaVersion(1, 0, 0), MigrationStatus.APPLIED)
        assert client.rpush.call_count >= 1

    def test_rollback_reflects_previous_version(self) -> None:
        client = _stateful_redis_client()
        store = RedisVersionStore(client=client)
        store.set_current_version(SchemaVersion(2, 0, 0))
        store.record_migration(
            SchemaVersion(2, 0, 0), MigrationStatus.ROLLED_BACK,
        )
        store.set_current_version(SchemaVersion(1, 0, 0))
        assert store.get_current_version() == SchemaVersion(1, 0, 0)


class TestCreateVersionStore:

    def test_memory_returns_in_memory_store(self) -> None:
        store = create_version_store("memory")
        assert isinstance(store, InMemoryVersionStore)

    def test_neo4j_returns_neo4j_store(self) -> None:
        store = create_version_store("neo4j", driver=MagicMock())
        assert isinstance(store, Neo4jVersionStore)

    def test_redis_returns_redis_store(self) -> None:
        store = create_version_store("redis", client=MagicMock())
        assert isinstance(store, RedisVersionStore)

    def test_unknown_backend_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="Unknown.*backend"):
            create_version_store("postgres")


class TestSchemaStoreConfig:

    def test_default_backend_is_memory(self) -> None:
        with pytest.MonkeyPatch.context() as mp:
            mp.delenv("SCHEMA_STORE_BACKEND", raising=False)
            config = SchemaStoreConfig.from_env()
        assert config.backend == "memory"

    def test_reads_schema_store_backend_env(self) -> None:
        with pytest.MonkeyPatch.context() as mp:
            mp.setenv("SCHEMA_STORE_BACKEND", "neo4j")
            config = SchemaStoreConfig.from_env()
        assert config.backend == "neo4j"
