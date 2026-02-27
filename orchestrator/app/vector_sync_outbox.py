from __future__ import annotations

import dataclasses
import json
import logging
import time
import uuid
from collections import OrderedDict
from typing import Any, List, Protocol, runtime_checkable

from pydantic import BaseModel, Field, field_validator

from orchestrator.app.vector_store import VectorRecord

logger = logging.getLogger(__name__)



class VectorSyncEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    collection: str
    operation: str = "delete"
    pruned_ids: List[str] = Field(default_factory=list)
    vectors: List[Any] = Field(default_factory=list)
    status: str = "pending"
    retry_count: int = 0

    @field_validator("pruned_ids")
    @classmethod
    def _require_nonempty_ids_for_delete(cls, v: List[str], info: Any) -> List[str]:
        if info.data.get("operation", "delete") == "delete" and not v:
            raise ValueError("pruned_ids must contain at least one ID for delete operations")
        return v


@runtime_checkable
class VectorDeleter(Protocol):
    async def delete(self, collection: str, ids: List[str]) -> int: ...


@runtime_checkable
class OutboxStore(Protocol):
    async def write_event(self, event: VectorSyncEvent) -> None: ...
    async def load_pending(self) -> List[VectorSyncEvent]: ...
    async def delete_event(self, event_id: str) -> None: ...
    async def update_retry_count(
        self, event_id: str, retry_count: int,
    ) -> None: ...


@runtime_checkable
class ClaimableOutboxStore(OutboxStore, Protocol):
    async def claim_pending(
        self, worker_id: str, limit: int, lease_seconds: float,
    ) -> List[VectorSyncEvent]: ...

    async def mark_completed(self, event_id: str) -> None: ...

    async def release_claim(self, event_id: str) -> None: ...

    async def release_expired_claims(self) -> int: ...


class VectorSyncOutbox:
    def __init__(self) -> None:
        self._pending: OrderedDict[str, VectorSyncEvent] = OrderedDict()

    @property
    def pending_count(self) -> int:
        return len(self._pending)

    def enqueue(self, event: VectorSyncEvent) -> None:
        self._pending[event.event_id] = event

    def drain_pending(self) -> List[VectorSyncEvent]:
        return list(self._pending.values())

    def mark_emitted(self, event_id: str) -> None:
        self._pending.pop(event_id, None)


class OutboxDrainer:
    def __init__(
        self,
        outbox: VectorSyncOutbox,
        vector_store: Any,
    ) -> None:
        self._outbox = outbox
        self._vector_store = vector_store

    async def process_once(self) -> int:
        pending = self._outbox.drain_pending()
        if not pending:
            return 0

        processed = 0
        for event in pending:
            try:
                if event.operation == "upsert":
                    await self._vector_store.upsert(
                        event.collection, event.vectors,
                    )
                else:
                    await self._vector_store.delete(
                        event.collection, event.pruned_ids,
                    )
                self._outbox.mark_emitted(event.event_id)
                processed += 1
            except Exception as exc:
                logger.warning(
                    "Vector sync failed for event %s: %s",
                    event.event_id, exc,
                )
        return processed


class RedisOutboxStore:
    """Redis-backed outbox store with lease-based claiming.

    Limitation: ``claim_pending`` uses non-atomic read-check-set across
    multiple Redis round-trips.  In a concurrent async context, two
    coroutines can interleave at ``await`` boundaries and both claim the
    same event.  For single-worker deployments this is acceptable.
    Production multi-worker deployments should replace the claim logic
    with a Redis Lua script (``EVALSHA``) or ``WATCH``/``MULTI``
    transaction to make the read-check-claim atomic per event.
    """

    _KEY_PREFIX = "graphrag:vecoutbox:"
    _INDEX_KEY = "graphrag:vecoutbox:pending"

    def __init__(self, redis_conn: Any) -> None:
        self._redis = redis_conn

    def _event_key(self, event_id: str) -> str:
        return f"{self._KEY_PREFIX}{event_id}"

    @staticmethod
    def _serialize_vectors(vectors: List[Any]) -> str:
        return json.dumps([
            dataclasses.asdict(v)
            if dataclasses.is_dataclass(v) and not isinstance(v, type)
            else v
            for v in vectors
        ])

    async def write_event(self, event: VectorSyncEvent) -> None:
        mapping = {
            "event_id": event.event_id,
            "collection": event.collection,
            "operation": event.operation,
            "pruned_ids": json.dumps(event.pruned_ids),
            "vectors": self._serialize_vectors(event.vectors),
            "status": event.status,
            "retry_count": str(event.retry_count),
        }
        await self._redis.hset(
            self._event_key(event.event_id), mapping=mapping,
        )
        await self._redis.sadd(self._INDEX_KEY, event.event_id)

    async def load_pending(self) -> List[VectorSyncEvent]:
        event_ids = await self._redis.smembers(self._INDEX_KEY)
        events: List[VectorSyncEvent] = []
        for eid in event_ids:
            data = await self._redis.hgetall(self._event_key(eid))
            if not data:
                await self._redis.srem(self._INDEX_KEY, eid)
                continue
            events.append(VectorSyncEvent(
                event_id=data["event_id"],
                collection=data["collection"],
                operation=data.get("operation", "delete"),
                pruned_ids=json.loads(data["pruned_ids"]),
                vectors=[
                    VectorRecord(**v)
                    for v in json.loads(data.get("vectors", "[]"))
                ],
                status=data.get("status", "pending"),
                retry_count=int(data.get("retry_count", "0")),
            ))
        return events

    async def delete_event(self, event_id: str) -> None:
        await self._redis.delete(self._event_key(event_id))
        await self._redis.srem(self._INDEX_KEY, event_id)

    async def update_retry_count(
        self, event_id: str, retry_count: int,
    ) -> None:
        await self._redis.hset(
            self._event_key(event_id),
            key="retry_count",
            value=str(retry_count),
        )

    async def claim_pending(
        self, worker_id: str, limit: int, lease_seconds: float,
    ) -> List[VectorSyncEvent]:
        event_ids = await self._redis.smembers(self._INDEX_KEY)
        claimed: List[VectorSyncEvent] = []
        now = time.time()
        for eid in event_ids:
            if len(claimed) >= limit:
                break
            data = await self._redis.hgetall(self._event_key(eid))
            if not data:
                await self._redis.srem(self._INDEX_KEY, eid)
                continue
            status = data.get("status", "pending")
            if status in ("completed", "claimed"):
                if status == "claimed":
                    expires = float(data.get("lease_expires_at", "0"))
                    if expires > now:
                        continue
                else:
                    continue
            await self._redis.hset(
                self._event_key(eid), key="status", value="claimed",
            )
            await self._redis.hset(
                self._event_key(eid), key="claimed_by", value=worker_id,
            )
            await self._redis.hset(
                self._event_key(eid),
                key="lease_expires_at",
                value=str(now + lease_seconds),
            )
            claimed.append(VectorSyncEvent(
                event_id=data["event_id"],
                collection=data["collection"],
                operation=data.get("operation", "delete"),
                pruned_ids=json.loads(data["pruned_ids"]),
                vectors=[
                    VectorRecord(**v)
                    for v in json.loads(data.get("vectors", "[]"))
                ],
                status="claimed",
                retry_count=int(data.get("retry_count", "0")),
            ))
        return claimed

    async def mark_completed(self, event_id: str) -> None:
        await self._redis.hset(
            self._event_key(event_id), key="status", value="completed",
        )

    async def release_claim(self, event_id: str) -> None:
        await self._redis.hset(
            self._event_key(event_id), key="status", value="pending",
        )
        await self._redis.hset(
            self._event_key(event_id), key="lease_expires_at", value="0",
        )

    async def release_expired_claims(self) -> int:
        event_ids = await self._redis.smembers(self._INDEX_KEY)
        released = 0
        now = time.time()
        for eid in event_ids:
            data = await self._redis.hgetall(self._event_key(eid))
            if not data:
                continue
            if data.get("status") != "claimed":
                continue
            expires = float(data.get("lease_expires_at", "0"))
            if expires <= now:
                await self._redis.hset(
                    self._event_key(eid), key="status", value="pending",
                )
                released += 1
        return released


class Neo4jOutboxStore:
    _LOAD_BATCH_LIMIT = 100

    _CREATE_QUERY = (
        "CREATE (e:OutboxEvent {"
        "  event_id: $event_id,"
        "  collection: $collection,"
        "  operation: $operation,"
        "  pruned_ids: $pruned_ids,"
        "  vectors: $vectors,"
        "  status: $status,"
        "  retry_count: $retry_count"
        "})"
    )
    _LOAD_QUERY = (
        "MATCH (e:OutboxEvent {status: 'pending'}) "
        "RETURN e.event_id AS event_id, e.collection AS collection, "
        "e.operation AS operation, e.pruned_ids AS pruned_ids, "
        "e.vectors AS vectors, e.status AS status, "
        "e.retry_count AS retry_count "
        f"ORDER BY e.retry_count ASC LIMIT {_LOAD_BATCH_LIMIT}"
    )
    _DELETE_QUERY = (
        "MATCH (e:OutboxEvent {event_id: $event_id}) "
        "DELETE e"
    )
    _UPDATE_RETRY_QUERY = (
        "MATCH (e:OutboxEvent {event_id: $event_id}) "
        "SET e.retry_count = $retry_count"
    )

    def __init__(self, driver: Any) -> None:
        self._driver = driver

    def _event_params(self, event: VectorSyncEvent) -> dict:
        return {
            "event_id": event.event_id,
            "collection": event.collection,
            "operation": event.operation,
            "pruned_ids": json.dumps(event.pruned_ids),
            "vectors": json.dumps([
                dataclasses.asdict(v)
                if dataclasses.is_dataclass(v) and not isinstance(v, type)
                else v
                for v in event.vectors
            ]),
            "status": event.status,
            "retry_count": event.retry_count,
        }

    async def write_in_tx(self, tx: Any, event: VectorSyncEvent) -> None:
        await tx.run(self._CREATE_QUERY, **self._event_params(event))

    async def write_event(self, event: VectorSyncEvent) -> None:
        async with self._driver.session(default_access_mode="WRITE") as session:
            await session.execute_write(self.write_in_tx, event=event)

    async def load_pending(self) -> List[VectorSyncEvent]:
        async with self._driver.session(default_access_mode="READ") as session:
            records = await session.execute_read(self._read_pending)
        events: List[VectorSyncEvent] = []
        for data in records:
            events.append(VectorSyncEvent(
                event_id=data["event_id"],
                collection=data["collection"],
                operation=data.get("operation", "delete"),
                pruned_ids=json.loads(data["pruned_ids"]),
                vectors=[
                    VectorRecord(**v)
                    for v in json.loads(data.get("vectors", "[]"))
                ],
                status=data.get("status", "pending"),
                retry_count=int(data.get("retry_count", 0)),
            ))
        return events

    @staticmethod
    async def _read_pending(tx: Any) -> list:
        result = await tx.run(Neo4jOutboxStore._LOAD_QUERY)
        return [record.data() async for record in result]

    async def delete_event(self, event_id: str) -> None:
        async with self._driver.session(default_access_mode="WRITE") as session:
            await session.execute_write(self._delete_node, event_id=event_id)

    @staticmethod
    async def _delete_node(tx: Any, event_id: str) -> None:
        await tx.run(Neo4jOutboxStore._DELETE_QUERY, event_id=event_id)

    async def update_retry_count(
        self, event_id: str, retry_count: int,
    ) -> None:
        async with self._driver.session(default_access_mode="WRITE") as session:
            await session.execute_write(
                self._set_retry, event_id=event_id, retry_count=retry_count,
            )

    @staticmethod
    async def _set_retry(tx: Any, event_id: str, retry_count: int) -> None:
        await tx.run(
            Neo4jOutboxStore._UPDATE_RETRY_QUERY,
            event_id=event_id, retry_count=retry_count,
        )


DEFAULT_MAX_RETRIES = 5


_DEFAULT_LEASE_SECONDS = 60.0
_DEFAULT_CLAIM_LIMIT = 50


class DurableOutboxDrainer:
    def __init__(
        self,
        store: OutboxStore,
        vector_store: Any,
        max_retries: int = DEFAULT_MAX_RETRIES,
        worker_id: str = "",
        lease_seconds: float = _DEFAULT_LEASE_SECONDS,
    ) -> None:
        self._store = store
        self._vector_store = vector_store
        self._max_retries = max_retries
        self._worker_id = worker_id or str(uuid.uuid4())
        self._lease_seconds = lease_seconds

    async def _load_events(self) -> List[VectorSyncEvent]:
        if isinstance(self._store, ClaimableOutboxStore):
            await self._store.release_expired_claims()
            return await self._store.claim_pending(
                self._worker_id,
                limit=_DEFAULT_CLAIM_LIMIT,
                lease_seconds=self._lease_seconds,
            )
        return await self._store.load_pending()

    async def _finalize_event(self, event_id: str) -> None:
        if isinstance(self._store, ClaimableOutboxStore):
            await self._store.mark_completed(event_id)
        await self._store.delete_event(event_id)

    async def process_once(self) -> int:
        pending = await self._load_events()
        if not pending:
            return 0

        processed = 0
        for event in pending:
            try:
                if event.operation == "upsert":
                    await self._vector_store.upsert(
                        event.collection, event.vectors,
                    )
                else:
                    await self._vector_store.delete(
                        event.collection, event.pruned_ids,
                    )
                await self._finalize_event(event.event_id)
                processed += 1
            except Exception as exc:
                new_count = event.retry_count + 1
                if new_count >= self._max_retries:
                    logger.error(
                        "Vector sync event %s exceeded max retries (%d), "
                        "discarding: %s",
                        event.event_id, self._max_retries, exc,
                    )
                    await self._store.delete_event(event.event_id)
                else:
                    logger.warning(
                        "Vector sync failed for event %s (retry %d/%d): %s",
                        event.event_id, new_count, self._max_retries, exc,
                    )
                    await self._store.update_retry_count(
                        event.event_id, new_count,
                    )
                    if isinstance(self._store, ClaimableOutboxStore):
                        await self._store.release_claim(event.event_id)
        return processed
