from __future__ import annotations

import pytest

from orchestrator.app.schema_evolution import (
    InMemoryVersionStore,
    Migration,
    MigrationRegistry,
    MigrationStatus,
    SchemaVersion,
)


class TestSchemaVersion:

    def test_parse_valid(self) -> None:
        v = SchemaVersion.parse("1.2.3")
        assert v.major == 1
        assert v.minor == 2
        assert v.patch == 3

    def test_parse_invalid(self) -> None:
        with pytest.raises(ValueError, match="Invalid version format"):
            SchemaVersion.parse("1.2")

    def test_ordering(self) -> None:
        v1 = SchemaVersion(1, 0, 0)
        v2 = SchemaVersion(1, 1, 0)
        v3 = SchemaVersion(2, 0, 0)
        assert v1 < v2 < v3
        assert v1 <= v1

    def test_str(self) -> None:
        assert str(SchemaVersion(1, 2, 3)) == "1.2.3"


class TestInMemoryVersionStore:

    def test_initial_version_is_none(self) -> None:
        store = InMemoryVersionStore()
        assert store.get_current_version() is None

    def test_set_and_get(self) -> None:
        store = InMemoryVersionStore()
        v = SchemaVersion(1, 0, 0)
        store.set_current_version(v)
        assert store.get_current_version() == v

    def test_records_history(self) -> None:
        store = InMemoryVersionStore()
        v = SchemaVersion(1, 0, 0)
        store.record_migration(v, MigrationStatus.APPLIED)
        assert len(store.history) == 1
        assert store.history[0]["version"] == "1.0.0"
        assert store.history[0]["status"] == "applied"


class TestMigrationRegistry:

    @staticmethod
    def _noop() -> None:
        pass

    def test_register_and_list(self) -> None:
        store = InMemoryVersionStore()
        registry = MigrationRegistry(store)
        m1 = Migration(SchemaVersion(1, 0, 0), "init", self._noop)
        m2 = Migration(SchemaVersion(1, 1, 0), "add index", self._noop)
        registry.register(m2)
        registry.register(m1)
        assert [m.version for m in registry.all_migrations()] == [
            SchemaVersion(1, 0, 0), SchemaVersion(1, 1, 0),
        ]

    def test_duplicate_version_rejected(self) -> None:
        store = InMemoryVersionStore()
        registry = MigrationRegistry(store)
        m1 = Migration(SchemaVersion(1, 0, 0), "init", self._noop)
        registry.register(m1)
        with pytest.raises(ValueError, match="Duplicate"):
            registry.register(
                Migration(SchemaVersion(1, 0, 0), "dup", self._noop),
            )

    def test_pending_returns_unapplied(self) -> None:
        store = InMemoryVersionStore()
        store.set_current_version(SchemaVersion(1, 0, 0))
        registry = MigrationRegistry(store)
        registry.register(
            Migration(SchemaVersion(1, 0, 0), "v1", self._noop),
        )
        registry.register(
            Migration(SchemaVersion(1, 1, 0), "v1.1", self._noop),
        )
        pending = registry.pending()
        assert len(pending) == 1
        assert pending[0].version == SchemaVersion(1, 1, 0)


class TestMigrationApply:

    def test_apply_all_executes_migrations(self) -> None:
        store = InMemoryVersionStore()
        registry = MigrationRegistry(store)
        executed: list[str] = []

        registry.register(Migration(
            SchemaVersion(1, 0, 0), "init",
            up=lambda: executed.append("1.0.0"),
        ))
        registry.register(Migration(
            SchemaVersion(1, 1, 0), "add index",
            up=lambda: executed.append("1.1.0"),
        ))

        count = registry.apply_all()
        assert count == 2
        assert executed == ["1.0.0", "1.1.0"]
        assert registry.current_version() == SchemaVersion(1, 1, 0)

    def test_apply_stops_on_failure(self) -> None:
        store = InMemoryVersionStore()
        registry = MigrationRegistry(store)
        executed: list[str] = []

        def _fail() -> None:
            raise RuntimeError("migration failed")

        registry.register(Migration(
            SchemaVersion(1, 0, 0), "ok",
            up=lambda: executed.append("1.0.0"),
        ))
        registry.register(Migration(
            SchemaVersion(1, 1, 0), "fail", up=_fail,
        ))
        registry.register(Migration(
            SchemaVersion(1, 2, 0), "never",
            up=lambda: executed.append("1.2.0"),
        ))

        with pytest.raises(RuntimeError, match="migration failed"):
            registry.apply_all()

        assert executed == ["1.0.0"]
        assert registry.current_version() == SchemaVersion(1, 0, 0)

    def test_apply_is_idempotent(self) -> None:
        store = InMemoryVersionStore()
        registry = MigrationRegistry(store)
        counter = {"n": 0}

        registry.register(Migration(
            SchemaVersion(1, 0, 0), "init",
            up=lambda: counter.__setitem__("n", counter["n"] + 1),
        ))

        registry.apply_all()
        registry.apply_all()
        assert counter["n"] == 1


class TestMigrationRollback:

    def test_rollback_last(self) -> None:
        store = InMemoryVersionStore()
        registry = MigrationRegistry(store)
        state: dict[str, bool] = {"index_exists": False}

        registry.register(Migration(
            SchemaVersion(1, 0, 0), "init",
            up=lambda: state.__setitem__("index_exists", True),
            down=lambda: state.__setitem__("index_exists", False),
        ))

        registry.apply_all()
        assert state["index_exists"] is True

        rolled_back = registry.rollback_last()
        assert rolled_back is True
        assert state["index_exists"] is False
        assert registry.current_version() == SchemaVersion(0, 0, 0)

    def test_rollback_without_down_returns_false(self) -> None:
        store = InMemoryVersionStore()
        registry = MigrationRegistry(store)
        registry.register(Migration(
            SchemaVersion(1, 0, 0), "no rollback",
            up=lambda: None,
        ))
        registry.apply_all()
        assert registry.rollback_last() is False

    def test_rollback_empty_returns_false(self) -> None:
        store = InMemoryVersionStore()
        registry = MigrationRegistry(store)
        assert registry.rollback_last() is False
