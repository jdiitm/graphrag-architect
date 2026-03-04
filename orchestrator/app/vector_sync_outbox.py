from __future__ import annotations

import asyncio
import dataclasses
import json
import logging
import os
import time
import uuid
from collections import OrderedDict
from typing import Any, Dict, List, Protocol, Tuple, runtime_checkable

from pydantic import BaseModel, Field, field_validator

from orchestrator.app.config import ConfigurationError

logger = logging.getLogger(__name__)


def validate_production_sync_mode() -> None:
    deployment = os.environ.get("DEPLOYMENT_MODE", "dev").lower()
    if deployment != "production":
        return
    sync_mode = os.environ.get("VECTOR_SYNC_MODE", "memory").lower()
    if sync_mode == "memory":
        raise ConfigurationError(
            "VECTOR_SYNC_MODE='memory' is not safe for production. "
            "In-memory sync means pod crash = lost sync events = permanent "
            "vector drift. Set VECTOR_SYNC_MODE to 'kafka' or 'durable'."
        )

_DEFAULT_VECTOR_SYNC_TOPIC = "graphrag.vector-sync"


async def _serialize_event_payload(
    pruned_ids: List[str],
    vectors: List[Any],
) -> Tuple[str, str]:
    def _do_serialize() -> Tuple[str, str]:
        pruned_json = json.dumps(pruned_ids)
        vectors_json = json.dumps([
            dataclasses.asdict(v)
            if dataclasses.is_dataclass(v) and not isinstance(v, type)
            else v
            for v in vectors
        ])
        return pruned_json, vectors_json
    return await asyncio.to_thread(_do_serialize)


async def _deserialize_event_payload(
    pruned_ids_raw: str,
    vectors_raw: str,
) -> Tuple[List[str], List[Any]]:
    from orchestrator.app.vector_store import VectorRecord

    def _do_deserialize() -> Tuple[List[str], List[Any]]:
        pruned_ids = json.loads(pruned_ids_raw)
        raw_vectors = json.loads(vectors_raw)
        return pruned_ids, [VectorRecord(**v) for v in raw_vectors]
    return await asyncio.to_thread(_do_deserialize)


class OutboxOverflowError(RuntimeError):
    pass


@runtime_checkable
class KafkaPublisher(Protocol):
    async def publish(self, event: VectorSyncEvent) -> None: ...


class VectorSyncRouter:
    def __init__(
        self,
        mode: str = "memory",
        kafka_publisher: Any = None,
        memory_outbox: Any = None,
        overflow_strategy: str = "buffer",
    ) -> None:
        self._mode = mode
        self._kafka_publisher = kafka_publisher
        self._memory_outbox = memory_outbox
        self._overflow_strategy = overflow_strategy

    @property
    def mode(self) -> str:
        return self._mode

    @property
    def overflow_strategy(self) -> str:
        return self._overflow_strategy

    async def route(self, event: VectorSyncEvent) -> None:
        if self._mode == "kafka" and self._kafka_publisher is not None:
            try:
                await self._kafka_publisher.publish(event)
                return
            except Exception as exc:
                if self._overflow_strategy == "reject":
                    raise OutboxOverflowError(
                        f"Kafka publish failed and overflow_strategy=reject: {exc}"
                    ) from exc
                logger.warning(
                    "Kafka publish failed, fallback to in-memory outbox: %s",
                    exc,
                )
        if self._memory_outbox is not None:
            self._memory_outbox.enqueue(event)

    @classmethod
    def from_env(
        cls,
        memory_outbox: Any = None,
        kafka_publisher: Any = None,
    ) -> VectorSyncRouter:
        mode = os.environ.get("VECTOR_SYNC_MODE", "memory").lower()
        overflow = os.environ.get(
            "VECTOR_SYNC_OVERFLOW_STRATEGY", "buffer",
        ).lower()
        return cls(
            mode=mode,
            kafka_publisher=kafka_publisher,
            memory_outbox=memory_outbox,
            overflow_strategy=overflow,
        )



class VectorSyncEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    collection: str
    operation: str = "delete"
    pruned_ids: List[str] = Field(default_factory=list)
    vectors: List[Any] = Field(default_factory=list)
    status: str = "pending"
    retry_count: int = 0
    version: int = Field(default_factory=time.time_ns)
    tenant_id: str = ""

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


@runtime_checkable
class VersionFenceStore(Protocol):
    async def get_version_fence(self, fence_key: str, tenant_id: str = "") -> int: ...
    async def update_version_fence(
        self, fence_key: str, version: int, tenant_id: str = "",
    ) -> None: ...


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


_DEFAULT_COALESCE_WINDOW: float = 5.0


_EntryMap = Dict[
    Tuple[str, Tuple[str, ...]], Tuple["VectorSyncEvent", float]
]


class CoalescingOutbox:
    """In-memory outbox that deduplicates events within a time window.

    Events targeting the same ``(collection, node_ids)`` key are
    coalesced: only the latest event survives.  ``drain_pending``
    returns events whose age exceeds ``window_seconds`` (default 5 s).

    When ``max_entries`` is set and the buffer exceeds that limit after
    an enqueue, the oldest entries are evicted and forwarded to
    ``spillover_fn`` so they can be persisted to a durable store
    instead of being lost on pod restart.
    """

    def __init__(
        self,
        window_seconds: float = _DEFAULT_COALESCE_WINDOW,
        max_entries: int = 0,
        spillover_fn: Any = None,
    ) -> None:
        self._window_seconds = window_seconds
        self._max_entries = max_entries
        self._spillover_fn = spillover_fn
        self._entries: _EntryMap = {}

    @staticmethod
    def _dedup_key(
        event: VectorSyncEvent,
    ) -> Tuple[str, Tuple[str, ...]]:
        if event.operation == "upsert" and event.vectors:
            ids = tuple(sorted(
                getattr(v, "id", "") for v in event.vectors
            ))
        else:
            ids = tuple(sorted(event.pruned_ids))
        return (event.collection, ids)

    @property
    def pending_count(self) -> int:
        return len(self._entries)

    def enqueue(self, event: VectorSyncEvent) -> None:
        key = self._dedup_key(event)
        self._entries[key] = (event, time.monotonic())
        self._enforce_cap()

    def _enforce_cap(self) -> None:
        if self._max_entries <= 0 or self._spillover_fn is None:
            return
        if len(self._entries) <= self._max_entries:
            return
        sorted_keys = sorted(
            self._entries.keys(),
            key=lambda k: self._entries[k][1],
        )
        excess = len(self._entries) - self._max_entries
        spilled: List[VectorSyncEvent] = []
        for key in sorted_keys[:excess]:
            spilled.append(self._entries.pop(key)[0])
        if spilled:
            self._spillover_fn(spilled)

    def flush(self) -> List[VectorSyncEvent]:
        events = [entry[0] for entry in self._entries.values()]
        self._entries.clear()
        return events

    def drain_pending(self) -> List[VectorSyncEvent]:
        now = time.monotonic()
        ready: List[VectorSyncEvent] = []
        remaining: _EntryMap = {}
        for key, (event, enqueued_at) in self._entries.items():
            if now - enqueued_at >= self._window_seconds:
                ready.append(event)
            else:
                remaining[key] = (event, enqueued_at)
        self._entries = remaining
        return ready

    def drain_batch(
        self,
        batch_size: int = 100,
    ) -> List[VectorSyncEvent]:
        now = time.monotonic()
        ready: List[VectorSyncEvent] = []
        ready_keys: List[Any] = []
        for key, (event, enqueued_at) in self._entries.items():
            if now - enqueued_at >= self._window_seconds:
                ready_keys.append(key)
                ready.append(event)
                if len(ready) >= batch_size:
                    break
        for key in ready_keys:
            del self._entries[key]
        return ready


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
                    if event.tenant_id:
                        await self._vector_store.upsert(
                            event.collection, event.vectors, tenant_id=event.tenant_id,
                        )
                    else:
                        await self._vector_store.upsert(
                            event.collection, event.vectors,
                        )
                else:
                    if event.tenant_id:
                        await self._vector_store.delete(
                            event.collection, event.pruned_ids, tenant_id=event.tenant_id,
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


_DEDUP_TTL_DEFAULT = 3600

MAX_CLAIM_SCAN = 1000


class RedisOutboxStore:
    """Redis-backed outbox store with atomic lease-based claiming.

    ``claim_pending`` uses a Lua script executed via ``EVAL`` to
    atomically read, check status, and claim events in a single
    Redis round-trip.  This prevents duplicate claims across
    concurrent workers.

    ``write_event`` uses a Lua script to atomically check for an
    existing dedup key, replace the prior event if present, and
    write the new event with a TTL on the dedup key — preventing
    both TOCTOU races and unbounded key growth.
    """

    _KEY_PREFIX = "graphrag:vecoutbox:"
    _INDEX_KEY = "graphrag:vecoutbox:pending"
    _DEDUP_PREFIX = "graphrag:vecoutbox:dedup:"
    _FENCE_PREFIX = "graphrag:vecoutbox:fence:"

    _CLAIM_LUA_SCRIPT = """
local index_key = KEYS[1]
local worker_id = ARGV[1]
local limit = tonumber(ARGV[2])
local lease_seconds = tonumber(ARGV[3])
local now = tonumber(ARGV[4])
local prefix = ARGV[5]
local max_scan = tonumber(ARGV[6])

local event_ids = redis.call('SRANDMEMBER', index_key, max_scan)
if event_ids == false then event_ids = {} end
local claimed = {}

for _, eid in ipairs(event_ids) do
    if #claimed >= limit then break end
    local key = prefix .. eid
    local status = redis.call('HGET', key, 'status')
    if status == false then
        redis.call('SREM', index_key, eid)
    elseif status == 'pending' then
        redis.call('HSET', key, 'status', 'claimed')
        redis.call('HSET', key, 'claimed_by', worker_id)
        redis.call('HSET', key, 'lease_expires_at', tostring(now + lease_seconds))
        table.insert(claimed, eid)
    elseif status == 'claimed' then
        local raw_expires = redis.call('HGET', key, 'lease_expires_at')
        local expires = tonumber(raw_expires or '0')
        if expires <= now then
            redis.call('HSET', key, 'status', 'claimed')
            redis.call('HSET', key, 'claimed_by', worker_id)
            redis.call('HSET', key, 'lease_expires_at', tostring(now + lease_seconds))
            table.insert(claimed, eid)
        end
    end
end

return claimed
"""

    _WRITE_DEDUP_LUA_SCRIPT = """
local dedup_key = KEYS[1]
local index_key = KEYS[2]
local new_event_key = KEYS[3]
local key_prefix = ARGV[1]
local new_event_id = ARGV[2]
local dedup_ttl = tonumber(ARGV[3])

local existing_eid = redis.call('GET', dedup_key)
if existing_eid then
    redis.call('DEL', key_prefix .. existing_eid)
    redis.call('SREM', index_key, existing_eid)
end

for i = 4, #ARGV, 2 do
    redis.call('HSET', new_event_key, ARGV[i], ARGV[i + 1])
end

redis.call('SADD', index_key, new_event_id)
redis.call('SET', dedup_key, new_event_id, 'EX', dedup_ttl)

return 1
"""

    def __init__(
        self, redis_conn: Any,
        dedup_ttl: int = _DEDUP_TTL_DEFAULT,
    ) -> None:
        self._redis = redis_conn
        self._dedup_ttl = dedup_ttl

    def _event_key(self, event_id: str) -> str:
        return f"{self._KEY_PREFIX}{event_id}"

    @staticmethod
    def _dedup_key_for(event: VectorSyncEvent) -> str:
        if event.operation == "upsert" and event.vectors:
            ids = sorted(getattr(v, "id", "") for v in event.vectors)
        else:
            ids = sorted(event.pruned_ids)
        return f"{event.collection}:{','.join(ids)}"

    def _dedup_redis_key(self, event: VectorSyncEvent) -> str:
        return f"{self._DEDUP_PREFIX}{self._dedup_key_for(event)}"

    async def write_event(self, event: VectorSyncEvent) -> None:
        dedup_rkey = self._dedup_redis_key(event)
        pruned_json, vectors_json = await _serialize_event_payload(
            event.pruned_ids, event.vectors,
        )
        mapping = {
            "event_id": event.event_id,
            "collection": event.collection,
            "operation": event.operation,
            "pruned_ids": pruned_json,
            "vectors": vectors_json,
            "status": event.status,
            "retry_count": str(event.retry_count),
            "version": str(event.version),
            "tenant_id": event.tenant_id,
            "_dedup_rkey": dedup_rkey,
        }
        flat_args: list[str] = []
        for field, value in mapping.items():
            flat_args.extend([field, value])
        await self._redis.eval(
            self._WRITE_DEDUP_LUA_SCRIPT,
            3,
            dedup_rkey,
            self._INDEX_KEY,
            self._event_key(event.event_id),
            self._KEY_PREFIX,
            event.event_id,
            str(self._dedup_ttl),
            *flat_args,
        )

    async def load_pending(self) -> List[VectorSyncEvent]:
        event_ids = await self._redis.smembers(self._INDEX_KEY)
        events: List[VectorSyncEvent] = []
        for eid in event_ids:
            data = await self._redis.hgetall(self._event_key(eid))
            if not data:
                await self._redis.srem(self._INDEX_KEY, eid)
                continue
            pruned_ids, vectors = await _deserialize_event_payload(
                data["pruned_ids"], data.get("vectors", "[]"),
            )
            events.append(VectorSyncEvent(
                event_id=data["event_id"],
                collection=data["collection"],
                operation=data.get("operation", "delete"),
                pruned_ids=pruned_ids,
                vectors=vectors,
                status=data.get("status", "pending"),
                retry_count=int(data.get("retry_count", "0")),
                version=int(data.get("version", "0")) or time.time_ns(),
                tenant_id=data.get("tenant_id", ""),
            ))
        return events

    async def delete_event(self, event_id: str) -> None:
        event_key = self._event_key(event_id)
        data = await self._redis.hgetall(event_key)
        dedup_rkey = data.get("_dedup_rkey", "") if data else ""
        await self._redis.delete(event_key)
        await self._redis.srem(self._INDEX_KEY, event_id)
        if dedup_rkey:
            current = await self._redis.get(dedup_rkey)
            if current is not None:
                current_str = (
                    current.decode()
                    if isinstance(current, bytes)
                    else str(current)
                )
                if current_str == event_id:
                    await self._redis.delete(dedup_rkey)

    async def update_retry_count(
        self, event_id: str, retry_count: int,
    ) -> None:
        await self._redis.hset(
            self._event_key(event_id),
            key="retry_count",
            value=str(retry_count),
        )

    async def claim_pending(
        self,
        worker_id: str,
        limit: int,
        lease_seconds: float,
        max_scan: int = MAX_CLAIM_SCAN,
    ) -> List[VectorSyncEvent]:
        now = time.time()
        claimed_ids = await self._redis.eval(
            self._CLAIM_LUA_SCRIPT,
            1,
            self._INDEX_KEY,
            worker_id,
            str(limit),
            str(lease_seconds),
            str(now),
            self._KEY_PREFIX,
            str(max_scan),
        )
        if not claimed_ids:
            return []
        claimed: List[VectorSyncEvent] = []
        for eid in claimed_ids:
            eid_str = eid.decode() if isinstance(eid, bytes) else str(eid)
            data = await self._redis.hgetall(self._event_key(eid_str))
            if not data:
                continue
            pruned_ids, vectors = await _deserialize_event_payload(
                data["pruned_ids"], data.get("vectors", "[]"),
            )
            claimed.append(VectorSyncEvent(
                event_id=data["event_id"],
                collection=data["collection"],
                operation=data.get("operation", "delete"),
                pruned_ids=pruned_ids,
                vectors=vectors,
                status="claimed",
                retry_count=int(data.get("retry_count", "0")),
                version=int(data.get("version", "0")) or time.time_ns(),
                tenant_id=data.get("tenant_id", ""),
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

    async def get_version_fence(self, fence_key: str, tenant_id: str = "") -> int:
        redis_key = f"{self._FENCE_PREFIX}{tenant_id}:{fence_key}"
        raw = await self._redis.get(redis_key)
        if raw is None:
            return -1
        if isinstance(raw, bytes):
            return int(raw.decode())
        return int(raw)

    async def update_version_fence(
        self, fence_key: str, version: int, tenant_id: str = "",
    ) -> None:
        redis_key = f"{self._FENCE_PREFIX}{tenant_id}:{fence_key}"
        current = await self.get_version_fence(fence_key, tenant_id=tenant_id)
        if version > current:
            await self._redis.set(redis_key, str(version))


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
        "  retry_count: $retry_count,"
        "  version: $version,"
        "  tenant_id: $tenant_id"
        "})"
    )
    _LOAD_QUERY = (
        "MATCH (e:OutboxEvent {status: 'pending'}) "
        "RETURN e.event_id AS event_id, e.collection AS collection, "
        "e.operation AS operation, e.pruned_ids AS pruned_ids, "
        "e.vectors AS vectors, e.status AS status, "
        "e.retry_count AS retry_count, e.version AS version, "
        "e.tenant_id AS tenant_id "
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
    _CLAIM_QUERY = (
        "MATCH (e:OutboxEvent) "
        "WHERE e.status = 'pending' "
        "   OR (e.status = 'claimed' AND e.lease_expires_at < $now) "
        "WITH e ORDER BY e.retry_count ASC LIMIT $limit "
        "SET e.status = 'claimed', "
        "    e.claimed_by = $worker_id, "
        "    e.lease_expires_at = $lease_expires_at "
        "RETURN e.event_id AS event_id, e.collection AS collection, "
        "e.operation AS operation, e.pruned_ids AS pruned_ids, "
        "e.vectors AS vectors, e.status AS status, "
        "e.retry_count AS retry_count, e.version AS version, "
        "e.tenant_id AS tenant_id"
    )
    _RELEASE_CLAIM_QUERY = (
        "MATCH (e:OutboxEvent {event_id: $event_id}) "
        "SET e.status = 'pending', e.claimed_by = NULL, "
        "    e.lease_expires_at = NULL"
    )
    _MARK_COMPLETED_QUERY = (
        "MATCH (e:OutboxEvent {event_id: $event_id}) "
        "DELETE e"
    )
    _RELEASE_EXPIRED_QUERY = (
        "MATCH (e:OutboxEvent {status: 'claimed'}) "
        "WHERE e.lease_expires_at < $now "
        "SET e.status = 'pending', e.claimed_by = NULL, "
        "    e.lease_expires_at = NULL "
        "RETURN count(e) AS released"
    )
    _GET_FENCE_QUERY = (
        "MATCH (f:OutboxVersionFence {fence_key: $fence_key, tenant_id: $tenant_id}) "
        "RETURN f.version AS version"
    )
    _UPSERT_FENCE_QUERY = (
        "MERGE (f:OutboxVersionFence {fence_key: $fence_key, tenant_id: $tenant_id}) "
        "ON CREATE SET f.version = $version "
        "ON MATCH SET f.version = CASE WHEN f.version < $version THEN $version ELSE f.version END"
    )

    def __init__(self, driver: Any) -> None:
        self._driver = driver

    async def _event_params_async(self, event: VectorSyncEvent) -> dict[str, Any]:
        pruned_json, vectors_json = await _serialize_event_payload(
            event.pruned_ids, event.vectors,
        )
        return {
            "event_id": event.event_id,
            "collection": event.collection,
            "operation": event.operation,
            "pruned_ids": pruned_json,
            "vectors": vectors_json,
            "status": event.status,
            "retry_count": event.retry_count,
            "version": event.version,
            "tenant_id": event.tenant_id,
        }

    async def write_in_tx(self, tx: Any, event: VectorSyncEvent) -> None:
        params = await self._event_params_async(event)
        await tx.run(self._CREATE_QUERY, **params)

    async def write_after_tx(self, events: List[VectorSyncEvent]) -> None:
        if not events:
            return
        create_query = self._CREATE_QUERY
        pre_serialized = [
            await self._event_params_async(e) for e in events
        ]
        async with self._driver.session(default_access_mode="WRITE") as session:

            async def _batch_create(tx: Any) -> None:
                for params in pre_serialized:
                    await tx.run(create_query, **params)

            await session.execute_write(_batch_create)

    async def write_event(self, event: VectorSyncEvent) -> None:
        async with self._driver.session(default_access_mode="WRITE") as session:
            await session.execute_write(self.write_in_tx, event=event)

    async def load_pending(self) -> List[VectorSyncEvent]:
        async with self._driver.session(default_access_mode="READ") as session:
            records = await session.execute_read(self._read_pending)
        events: List[VectorSyncEvent] = []
        for data in records:
            pruned_ids, vectors = await _deserialize_event_payload(
                data["pruned_ids"], data.get("vectors", "[]"),
            )
            events.append(VectorSyncEvent(
                event_id=data["event_id"],
                collection=data["collection"],
                operation=data.get("operation", "delete"),
                pruned_ids=pruned_ids,
                vectors=vectors,
                status=data.get("status", "pending"),
                retry_count=int(data.get("retry_count", 0)),
                version=int(data.get("version", 0)) or time.time_ns(),
                tenant_id=str(data.get("tenant_id") or ""),
            ))
        return events

    @staticmethod
    async def _read_pending(tx: Any) -> list[Any]:
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

    async def claim_pending(
        self, worker_id: str, limit: int, lease_seconds: float,
    ) -> List[VectorSyncEvent]:
        now = time.time()
        lease_expires_at = now + lease_seconds
        async with self._driver.session(default_access_mode="WRITE") as session:
            records = await session.execute_write(
                self._claim_tx,
                worker_id=worker_id,
                limit=limit,
                now=now,
                lease_expires_at=lease_expires_at,
            )
        events: List[VectorSyncEvent] = []
        for data in records:
            pruned_ids, vectors = await _deserialize_event_payload(
                data["pruned_ids"], data.get("vectors", "[]"),
            )
            events.append(VectorSyncEvent(
                event_id=data["event_id"],
                collection=data["collection"],
                operation=data.get("operation", "delete"),
                pruned_ids=pruned_ids,
                vectors=vectors,
                status="claimed",
                retry_count=int(data.get("retry_count", 0)),
                version=int(data.get("version", 0)) or time.time_ns(),
                tenant_id=str(data.get("tenant_id") or ""),
            ))
        return events

    @staticmethod
    async def _claim_tx(
        tx: Any,
        worker_id: str,
        limit: int,
        now: float,
        lease_expires_at: float,
    ) -> list[Any]:
        result = await tx.run(
            Neo4jOutboxStore._CLAIM_QUERY,
            worker_id=worker_id,
            limit=limit,
            now=now,
            lease_expires_at=lease_expires_at,
        )
        return [record.data() async for record in result]

    async def mark_completed(self, event_id: str) -> None:
        async with self._driver.session(default_access_mode="WRITE") as session:
            await session.execute_write(
                self._mark_completed_tx, event_id=event_id,
            )

    @staticmethod
    async def _mark_completed_tx(tx: Any, event_id: str) -> None:
        await tx.run(
            Neo4jOutboxStore._MARK_COMPLETED_QUERY, event_id=event_id,
        )

    async def release_claim(self, event_id: str) -> None:
        async with self._driver.session(default_access_mode="WRITE") as session:
            await session.execute_write(
                self._release_claim_tx, event_id=event_id,
            )

    @staticmethod
    async def _release_claim_tx(tx: Any, event_id: str) -> None:
        await tx.run(
            Neo4jOutboxStore._RELEASE_CLAIM_QUERY, event_id=event_id,
        )

    async def release_expired_claims(self) -> int:
        now = time.time()
        async with self._driver.session(default_access_mode="WRITE") as session:
            records = await session.execute_write(
                self._release_expired_tx, now=now,
            )
        if records:
            return int(records[0].get("released", 0))
        return 0

    async def get_version_fence(self, fence_key: str, tenant_id: str = "") -> int:
        async with self._driver.session(default_access_mode="READ") as session:
            records = await session.execute_read(
                self._get_fence_tx, fence_key=fence_key, tenant_id=tenant_id,
            )
        if records:
            return int(records[0].get("version", -1))
        return -1

    @staticmethod
    async def _get_fence_tx(tx: Any, fence_key: str, tenant_id: str) -> list[Any]:
        result = await tx.run(
            Neo4jOutboxStore._GET_FENCE_QUERY,
            fence_key=fence_key,
            tenant_id=tenant_id,
        )
        return [record.data() async for record in result]

    async def update_version_fence(
        self, fence_key: str, version: int, tenant_id: str = "",
    ) -> None:
        async with self._driver.session(default_access_mode="WRITE") as session:
            await session.execute_write(
                self._upsert_fence_tx,
                fence_key=fence_key,
                version=version,
                tenant_id=tenant_id,
            )

    @staticmethod
    async def _upsert_fence_tx(
        tx: Any, fence_key: str, version: int, tenant_id: str,
    ) -> None:
        await tx.run(
            Neo4jOutboxStore._UPSERT_FENCE_QUERY,
            fence_key=fence_key,
            version=version,
            tenant_id=tenant_id,
        )

    @staticmethod
    async def _release_expired_tx(tx: Any, now: float) -> list[Any]:
        result = await tx.run(
            Neo4jOutboxStore._RELEASE_EXPIRED_QUERY, now=now,
        )
        return [record.data() async for record in result]


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
        self._max_tracked_fences = int(os.environ.get("OUTBOX_MAX_TRACKED_FENCES", "100000"))
        self._latest_version_by_key: OrderedDict[str, int] = OrderedDict()

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

    def _fence_lookup(self, scoped_key: str) -> int:
        version = self._latest_version_by_key.get(scoped_key, -1)
        if version != -1:
            self._latest_version_by_key.move_to_end(scoped_key)
        return version

    def _fence_record(self, scoped_key: str, version: int) -> None:
        self._latest_version_by_key[scoped_key] = version
        if len(self._latest_version_by_key) > self._max_tracked_fences:
            self._latest_version_by_key.popitem(last=False)

    async def process_once(self) -> int:
        pending = await self._load_events()
        if not pending:
            return 0

        processed = 0
        for event in pending:
            fence_key = self._event_fence_key(event)
            tenant_id = self._event_tenant_id(event)
            scoped_fence_key = f"{tenant_id}:{fence_key}"
            latest = self._fence_lookup(scoped_fence_key)
            if isinstance(self._store, VersionFenceStore):
                persisted = await self._store.get_version_fence(
                    fence_key, tenant_id=tenant_id,
                )
                latest = max(latest, persisted)
            if event.version < latest:
                await self._finalize_event(event.event_id)
                processed += 1
                continue
            try:
                if event.operation == "upsert":
                    if event.tenant_id:
                        await self._vector_store.upsert(
                            event.collection, event.vectors, tenant_id=event.tenant_id,
                        )
                    else:
                        await self._vector_store.upsert(
                            event.collection, event.vectors,
                        )
                else:
                    if event.tenant_id:
                        await self._vector_store.delete(
                            event.collection, event.pruned_ids, tenant_id=event.tenant_id,
                        )
                    else:
                        await self._vector_store.delete(
                            event.collection, event.pruned_ids,
                        )
                self._fence_record(scoped_fence_key, event.version)
                if isinstance(self._store, VersionFenceStore):
                    await self._store.update_version_fence(
                        fence_key, event.version, tenant_id=tenant_id,
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

    @staticmethod
    def _event_fence_key(event: VectorSyncEvent) -> str:
        if event.operation == "upsert" and event.vectors:
            vector_ids = sorted(str(getattr(v, "id", "")) for v in event.vectors)
            return f"{event.collection}:upsert:{','.join(vector_ids)}"
        return f"{event.collection}:{event.operation}:{','.join(sorted(event.pruned_ids))}"

    @staticmethod
    def _event_tenant_id(event: VectorSyncEvent) -> str:
        if event.tenant_id:
            return event.tenant_id
        if event.operation == "upsert" and event.vectors:
            first = event.vectors[0]
            metadata = getattr(first, "metadata", {})
            if isinstance(metadata, dict):
                return str(metadata.get("tenant_id", ""))
        base = "service_embeddings_"
        if event.collection.startswith(base):
            return event.collection[len(base):]
        return ""
