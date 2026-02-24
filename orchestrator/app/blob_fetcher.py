from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional, Protocol

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


async def upload_files_to_blob(
    store: BlobStore,
    files: list[dict[str, str]],
    prefix: str = "",
) -> list[dict[str, str]]:
    refs: list[dict[str, str]] = []
    for entry in files:
        path = entry["path"]
        content = entry.get("content", "")
        blob_key = f"{prefix}/{path}" if prefix else path
        await store.put(blob_key, content.encode("utf-8"))
        refs.append({"path": path, "blob_key": blob_key})
    return refs


async def resolve_file_content(
    store: BlobStore,
    file_refs: list[dict[str, str]],
) -> list[dict[str, str]]:
    resolved: list[dict[str, str]] = []
    for ref in file_refs:
        if "content" in ref:
            resolved.append(ref)
            continue
        blob_key = ref.get("blob_key", "")
        data = await store.get(blob_key)
        resolved.append({"path": ref["path"], "content": data.decode("utf-8")})
    return resolved


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
