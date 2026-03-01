from __future__ import annotations

import json
import time
from typing import Any, Dict, List
from unittest.mock import AsyncMock

import pytest

from orchestrator.app.vector_sync_outbox import (
    ClaimableOutboxStore,
    Neo4jOutboxStore,
    VectorSyncEvent,
)


class FakeRecord:
    def __init__(self, data: Dict[str, Any]) -> None:
        self._data = data

    def data(self) -> Dict[str, Any]:
        return self._data


class FakeResult:
    def __init__(self, records: List[FakeRecord]) -> None:
        self._records = records
        self._index = 0

    def __aiter__(self) -> FakeResult:
        return self

    async def __anext__(self) -> FakeRecord:
        if self._index >= len(self._records):
            raise StopAsyncIteration
        record = self._records[self._index]
        self._index += 1
        return record

    async def data(self) -> List[Dict[str, Any]]:
        return [r.data() for r in self._records]


class FakeTx:
    def __init__(self) -> None:
        self.queries: List[tuple] = []
        self._results: List[FakeResult] = []
        self._result_index = 0

    def add_result(self, result: FakeResult) -> None:
        self._results.append(result)

    async def run(self, query: str, **kwargs: Any) -> FakeResult:
        self.queries.append((query, kwargs))
        if self._result_index < len(self._results):
            result = self._results[self._result_index]
            self._result_index += 1
            return result
        return FakeResult([])


class FakeSession:
    def __init__(self) -> None:
        self.tx = FakeTx()

    async def execute_write(self, fn: Any, **kwargs: Any) -> Any:
        return await fn(self.tx, **kwargs)

    async def execute_read(self, fn: Any, **kwargs: Any) -> Any:
        return await fn(self.tx, **kwargs)

    async def __aenter__(self) -> FakeSession:
        return self

    async def __aexit__(self, *args: Any) -> None:
        pass


class FakeDriver:
    def __init__(self) -> None:
        self._session = FakeSession()

    def session(self, **kwargs: Any) -> FakeSession:
        return self._session


class TestNeo4jOutboxStoreImplementsClaimable:
    def test_satisfies_claimable_protocol(self) -> None:
        driver = FakeDriver()
        store = Neo4jOutboxStore(driver)
        assert isinstance(store, ClaimableOutboxStore)


class TestNeo4jClaimPending:
    @pytest.mark.asyncio
    async def test_claim_returns_events_and_marks_claimed(self) -> None:
        driver = FakeDriver()
        store = Neo4jOutboxStore(driver)
        tx = driver._session.tx
        tx.add_result(FakeResult([
            FakeRecord({
                "event_id": "e1",
                "collection": "svc",
                "operation": "delete",
                "pruned_ids": json.dumps(["id1"]),
                "vectors": "[]",
                "status": "pending",
                "retry_count": 0,
            }),
        ]))
        events = await store.claim_pending("w1", limit=10, lease_seconds=60.0)
        assert len(events) == 1
        assert events[0].event_id == "e1"
        assert events[0].status == "claimed"
        claim_queries = [q for q, _ in tx.queries if "claimed_by" in q]
        assert len(claim_queries) >= 1

    @pytest.mark.asyncio
    async def test_claim_skips_already_claimed_unexpired(self) -> None:
        driver = FakeDriver()
        store = Neo4jOutboxStore(driver)
        tx = driver._session.tx
        tx.add_result(FakeResult([]))
        events = await store.claim_pending("w2", limit=10, lease_seconds=60.0)
        assert len(events) == 0


class TestNeo4jMarkCompleted:
    @pytest.mark.asyncio
    async def test_mark_completed_deletes_event(self) -> None:
        driver = FakeDriver()
        store = Neo4jOutboxStore(driver)
        await store.mark_completed("e1")
        tx = driver._session.tx
        delete_queries = [q for q, _ in tx.queries if "DELETE" in q]
        assert len(delete_queries) >= 1


class TestNeo4jReleaseClaim:
    @pytest.mark.asyncio
    async def test_release_sets_status_pending(self) -> None:
        driver = FakeDriver()
        store = Neo4jOutboxStore(driver)
        await store.release_claim("e1")
        tx = driver._session.tx
        release_queries = [
            q for q, _ in tx.queries if "pending" in q.lower() or "status" in q.lower()
        ]
        assert len(release_queries) >= 1


class TestNeo4jReleaseExpiredClaims:
    @pytest.mark.asyncio
    async def test_release_expired_returns_count(self) -> None:
        driver = FakeDriver()
        store = Neo4jOutboxStore(driver)
        tx = driver._session.tx
        tx.add_result(FakeResult([
            FakeRecord({"released": 2}),
        ]))
        count = await store.release_expired_claims()
        assert isinstance(count, int)
