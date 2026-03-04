from __future__ import annotations

import pytest

from orchestrator.app.blob_fetcher import (
    BlobFetcher,
    BlobReference,
    InMemoryBlobStore,
    TenantBlobAccessError,
    _validate_tenant_key,
    resolve_file_content,
    upload_files_to_blob,
)


class TestValidateTenantKeyEmptyGuard:

    def test_empty_tenant_id_raises(self) -> None:
        with pytest.raises(TenantBlobAccessError):
            _validate_tenant_key("/anything", tenant_id="")

    def test_empty_tenant_id_raises_even_for_normal_key(self) -> None:
        with pytest.raises(TenantBlobAccessError):
            _validate_tenant_key("some/key", tenant_id="")


class TestBlobFetcherTenantValidation:

    @pytest.mark.asyncio
    async def test_fetch_content_rejects_cross_tenant_key(self) -> None:
        store = InMemoryBlobStore()
        await store.put("tenant-a/repo/main.py", b"secret code")
        fetcher = BlobFetcher(store)
        ref = BlobReference(bucket="test", key="tenant-a/repo/main.py")
        with pytest.raises(TenantBlobAccessError):
            await fetcher.fetch_content(ref, tenant_id="tenant-b")

    @pytest.mark.asyncio
    async def test_fetch_content_allows_own_tenant_key(self) -> None:
        store = InMemoryBlobStore()
        await store.put("tenant-a/repo/main.py", b"my code")
        fetcher = BlobFetcher(store)
        ref = BlobReference(bucket="test", key="tenant-a/repo/main.py")
        content = await fetcher.fetch_content(ref, tenant_id="tenant-a")
        assert content == "my code"

    @pytest.mark.asyncio
    async def test_fetch_if_blob_rejects_cross_tenant(self) -> None:
        store = InMemoryBlobStore()
        await store.put("tenant-a/data.json", b"{}")
        fetcher = BlobFetcher(store)
        payload = {"blob_key": "tenant-a/data.json"}
        with pytest.raises(TenantBlobAccessError):
            await fetcher.fetch_if_blob(payload, tenant_id="tenant-b")

    @pytest.mark.asyncio
    async def test_fetch_if_blob_allows_own_tenant(self) -> None:
        store = InMemoryBlobStore()
        await store.put("tenant-a/data.json", b"{}")
        fetcher = BlobFetcher(store)
        payload = {"blob_key": "tenant-a/data.json"}
        content = await fetcher.fetch_if_blob(payload, tenant_id="tenant-a")
        assert content == "{}"

    @pytest.mark.asyncio
    async def test_fetch_content_rejects_empty_tenant_id(self) -> None:
        store = InMemoryBlobStore()
        await store.put("/evil-data", b"stolen")
        fetcher = BlobFetcher(store)
        ref = BlobReference(bucket="test", key="/evil-data")
        with pytest.raises(TenantBlobAccessError):
            await fetcher.fetch_content(ref, tenant_id="")

    @pytest.mark.asyncio
    async def test_fetch_content_rejects_key_without_tenant_prefix(self) -> None:
        store = InMemoryBlobStore()
        await store.put("bare-key.py", b"data")
        fetcher = BlobFetcher(store)
        ref = BlobReference(bucket="test", key="bare-key.py")
        with pytest.raises(TenantBlobAccessError):
            await fetcher.fetch_content(ref, tenant_id="tenant-a")


class TestResolveFileContentTenantValidation:

    @pytest.mark.asyncio
    async def test_rejects_cross_tenant_blob_key(self) -> None:
        store = InMemoryBlobStore()
        await store.put("tenant-a/app.py", b"import os")
        refs = [{"path": "app.py", "blob_key": "tenant-a/app.py"}]
        with pytest.raises(TenantBlobAccessError):
            await resolve_file_content(store, refs, tenant_id="tenant-b")

    @pytest.mark.asyncio
    async def test_allows_own_tenant_blob_key(self) -> None:
        store = InMemoryBlobStore()
        await store.put("tenant-a/app.py", b"import os")
        refs = [{"path": "app.py", "blob_key": "tenant-a/app.py"}]
        resolved = await resolve_file_content(store, refs, tenant_id="tenant-a")
        assert resolved[0]["content"] == "import os"

    @pytest.mark.asyncio
    async def test_rejects_empty_tenant_id(self) -> None:
        store = InMemoryBlobStore()
        await store.put("/evil-data", b"stolen")
        refs = [{"path": "evil.py", "blob_key": "/evil-data"}]
        with pytest.raises(TenantBlobAccessError):
            await resolve_file_content(store, refs, tenant_id="")

    @pytest.mark.asyncio
    async def test_passthrough_inline_content_skips_tenant_check(self) -> None:
        store = InMemoryBlobStore()
        files = [{"path": "inline.go", "content": "package main"}]
        resolved = await resolve_file_content(store, files, tenant_id="any-tenant")
        assert resolved[0]["content"] == "package main"


class TestUploadFilesToBlobTenantScoping:

    @pytest.mark.asyncio
    async def test_prefixes_keys_with_tenant_id(self) -> None:
        store = InMemoryBlobStore()
        files = [{"path": "main.go", "content": "package main"}]
        refs = await upload_files_to_blob(store, files, tenant_id="tenant-x")
        assert refs[0]["blob_key"].startswith("tenant-x/")
        stored = await store.get(refs[0]["blob_key"])
        assert stored == b"package main"

    @pytest.mark.asyncio
    async def test_upload_with_empty_tenant_raises(self) -> None:
        store = InMemoryBlobStore()
        files = [{"path": "main.go", "content": "package main"}]
        with pytest.raises(ValueError):
            await upload_files_to_blob(store, files, tenant_id="")
