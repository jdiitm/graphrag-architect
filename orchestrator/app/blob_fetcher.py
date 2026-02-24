from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Optional, Protocol

logger = logging.getLogger(__name__)


class BlobStore(Protocol):
    async def get(self, key: str) -> bytes: ...
    async def put(self, key: str, data: bytes) -> str: ...
    async def exists(self, key: str) -> bool: ...


@dataclass(frozen=True)
class BlobReference:
    bucket: str
    key: str
    content_type: str = "application/octet-stream"
    size_bytes: int = 0

    @classmethod
    def from_kafka_message(cls, payload: dict) -> Optional[BlobReference]:
        blob_key = payload.get("blob_key")
        if not blob_key:
            return None
        return cls(
            bucket=payload.get("bucket", "graphrag-ingestion"),
            key=blob_key,
            content_type=payload.get("content_type", "application/octet-stream"),
            size_bytes=payload.get("size_bytes", 0),
        )


class InMemoryBlobStore:
    def __init__(self) -> None:
        self._store: dict[str, bytes] = {}

    async def get(self, key: str) -> bytes:
        if key not in self._store:
            raise KeyError(f"Blob not found: {key}")
        return self._store[key]

    async def put(self, key: str, data: bytes) -> str:
        self._store[key] = data
        return key

    async def exists(self, key: str) -> bool:
        return key in self._store


class BlobFetcher:
    def __init__(self, store: BlobStore) -> None:
        self._store = store

    async def fetch_content(self, ref: BlobReference) -> str:
        data = await self._store.get(ref.key)
        return data.decode("utf-8")

    async def fetch_if_blob(self, payload: dict) -> Optional[str]:
        ref = BlobReference.from_kafka_message(payload)
        if ref is None:
            return None
        return await self.fetch_content(ref)
