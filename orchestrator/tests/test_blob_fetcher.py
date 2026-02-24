from __future__ import annotations

import pytest

from orchestrator.app.blob_fetcher import (
    BlobFetcher,
    BlobReference,
    InMemoryBlobStore,
)


class TestBlobReference:

    def test_from_kafka_message_with_blob_key(self) -> None:
        payload = {
            "blob_key": "ingestion/run-123/main.go",
            "bucket": "graphrag-ingestion",
            "content_type": "text/plain",
            "size_bytes": 1024,
        }
        ref = BlobReference.from_kafka_message(payload)
        assert ref is not None
        assert ref.key == "ingestion/run-123/main.go"
        assert ref.bucket == "graphrag-ingestion"

    def test_from_kafka_message_without_blob_key(self) -> None:
        payload = {"file_path": "main.go", "content": "package main"}
        ref = BlobReference.from_kafka_message(payload)
        assert ref is None


class TestInMemoryBlobStore:

    @pytest.mark.asyncio
    async def test_put_and_get_roundtrip(self) -> None:
        store = InMemoryBlobStore()
        await store.put("key1", b"hello world")
        data = await store.get("key1")
        assert data == b"hello world"

    @pytest.mark.asyncio
    async def test_get_missing_key_raises(self) -> None:
        store = InMemoryBlobStore()
        with pytest.raises(KeyError):
            await store.get("nonexistent")

    @pytest.mark.asyncio
    async def test_exists(self) -> None:
        store = InMemoryBlobStore()
        assert not await store.exists("key1")
        await store.put("key1", b"data")
        assert await store.exists("key1")


class TestBlobFetcher:

    @pytest.mark.asyncio
    async def test_fetch_content_decodes_utf8(self) -> None:
        store = InMemoryBlobStore()
        await store.put("file.go", b"package main\nfunc main() {}")
        fetcher = BlobFetcher(store)
        ref = BlobReference(bucket="test", key="file.go")
        content = await fetcher.fetch_content(ref)
        assert content == "package main\nfunc main() {}"

    @pytest.mark.asyncio
    async def test_fetch_if_blob_returns_content(self) -> None:
        store = InMemoryBlobStore()
        await store.put("ingestion/main.go", b"package main")
        fetcher = BlobFetcher(store)
        payload = {"blob_key": "ingestion/main.go"}
        content = await fetcher.fetch_if_blob(payload)
        assert content == "package main"

    @pytest.mark.asyncio
    async def test_fetch_if_blob_returns_none_for_inline(self) -> None:
        store = InMemoryBlobStore()
        fetcher = BlobFetcher(store)
        payload = {"content": "package main"}
        content = await fetcher.fetch_if_blob(payload)
        assert content is None


class TestUploadFilesToBlob:

    @pytest.mark.asyncio
    async def test_uploads_and_returns_refs(self) -> None:
        from orchestrator.app.blob_fetcher import upload_files_to_blob

        store = InMemoryBlobStore()
        files = [
            {"path": "main.go", "content": "package main"},
            {"path": "utils.py", "content": "def foo(): pass"},
        ]
        refs = await upload_files_to_blob(store, files, prefix="run-1")
        assert len(refs) == 2
        assert refs[0]["path"] == "main.go"
        assert "blob_key" in refs[0]
        assert "content" not in refs[0]
        stored = await store.get(refs[0]["blob_key"])
        assert stored == b"package main"

    @pytest.mark.asyncio
    async def test_empty_files_returns_empty(self) -> None:
        from orchestrator.app.blob_fetcher import upload_files_to_blob

        store = InMemoryBlobStore()
        refs = await upload_files_to_blob(store, [], prefix="run-2")
        assert refs == []


class TestResolveFileContent:

    @pytest.mark.asyncio
    async def test_resolves_blob_ref_to_content(self) -> None:
        from orchestrator.app.blob_fetcher import (
            resolve_file_content,
            upload_files_to_blob,
        )

        store = InMemoryBlobStore()
        files = [{"path": "app.py", "content": "import os"}]
        refs = await upload_files_to_blob(store, files, prefix="run-3")
        resolved = await resolve_file_content(store, refs)
        assert len(resolved) == 1
        assert resolved[0]["path"] == "app.py"
        assert resolved[0]["content"] == "import os"

    @pytest.mark.asyncio
    async def test_passthrough_inline_content(self) -> None:
        from orchestrator.app.blob_fetcher import resolve_file_content

        store = InMemoryBlobStore()
        files = [{"path": "inline.go", "content": "package main"}]
        resolved = await resolve_file_content(store, files)
        assert resolved[0]["content"] == "package main"
