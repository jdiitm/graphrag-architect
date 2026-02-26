from __future__ import annotations

import pytest

from orchestrator.app.schema_version import (
    Migration,
    SchemaVersionTracker,
    load_canonical_schema,
    parse_migrations,
)


class TestMigration:

    def test_checksum_deterministic(self) -> None:
        m = Migration(version=1, name="test", cypher="CREATE INDEX x")
        assert m.compute_checksum() == m.compute_checksum()

    def test_different_cypher_different_checksum(self) -> None:
        m1 = Migration(version=1, name="a", cypher="CREATE INDEX x")
        m2 = Migration(version=2, name="b", cypher="CREATE INDEX y")
        assert m1.compute_checksum() != m2.compute_checksum()


class TestParseCanonicalSchema:

    def test_loads_canonical_schema(self) -> None:
        schema = load_canonical_schema()
        assert "CREATE CONSTRAINT" in schema
        assert "IS NODE KEY" in schema

    def test_parse_migrations_non_empty(self) -> None:
        schema = load_canonical_schema()
        migrations = parse_migrations(schema)
        assert len(migrations) > 0

    def test_parse_skips_comments_and_blanks(self) -> None:
        schema = "-- comment\n\nCREATE INDEX x;\nCREATE INDEX y;"
        migrations = parse_migrations(schema)
        assert len(migrations) == 2

    def test_migrations_have_sequential_versions(self) -> None:
        schema = load_canonical_schema()
        migrations = parse_migrations(schema)
        versions = [m.version for m in migrations]
        assert versions == sorted(versions)


class TestSchemaVersionTracker:

    def test_initial_version_is_zero(self) -> None:
        tracker = SchemaVersionTracker()
        assert tracker.current_version == 0

    def test_record_applied_updates_version(self) -> None:
        tracker = SchemaVersionTracker()
        m = Migration(version=1, name="init", cypher="CREATE INDEX x")
        tracker.record_applied(m)
        assert tracker.current_version == 1
        assert "init" in tracker.applied_migrations

    def test_pending_migrations(self) -> None:
        tracker = SchemaVersionTracker()
        all_migrations = [
            Migration(version=1, name="m1", cypher="a"),
            Migration(version=2, name="m2", cypher="b"),
            Migration(version=3, name="m3", cypher="c"),
        ]
        tracker.record_applied(all_migrations[0])
        pending = tracker.pending_migrations(all_migrations)
        assert len(pending) == 2
        assert pending[0].name == "m2"

    def test_is_up_to_date_when_all_applied(self) -> None:
        tracker = SchemaVersionTracker()
        all_migrations = [
            Migration(version=1, name="m1", cypher="a"),
            Migration(version=2, name="m2", cypher="b"),
        ]
        for m in all_migrations:
            tracker.record_applied(m)
        assert tracker.is_up_to_date(all_migrations) is True

    def test_not_up_to_date_when_pending(self) -> None:
        tracker = SchemaVersionTracker()
        all_migrations = [
            Migration(version=1, name="m1", cypher="a"),
            Migration(version=2, name="m2", cypher="b"),
        ]
        tracker.record_applied(all_migrations[0])
        assert tracker.is_up_to_date(all_migrations) is False

    def test_canonical_schema_produces_valid_migrations(self) -> None:
        schema = load_canonical_schema()
        migrations = parse_migrations(schema)
        tracker = SchemaVersionTracker()
        for m in migrations:
            tracker.record_applied(m)
        assert tracker.is_up_to_date(migrations)
        assert tracker.current_version == len(migrations)
