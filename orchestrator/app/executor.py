from __future__ import annotations

import asyncio
import os
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from typing import Any, Callable, TypeVar

T = TypeVar("T")

_DEFAULT_MAX_WORKERS = 4
_DEFAULT_THREAD_POOL_WORKERS = 8


class _PoolHolder:
    instance: ProcessPoolExecutor | None = None


class _ThreadPoolHolder:
    instance: ThreadPoolExecutor | None = None


def get_pool() -> ProcessPoolExecutor:
    if _PoolHolder.instance is None:
        max_workers = int(
            os.environ.get("CPU_POOL_MAX_WORKERS", str(_DEFAULT_MAX_WORKERS))
        )
        _PoolHolder.instance = ProcessPoolExecutor(max_workers=max_workers)
    return _PoolHolder.instance


def get_thread_pool() -> ThreadPoolExecutor:
    if _ThreadPoolHolder.instance is None:
        max_workers = int(
            os.environ.get(
                "RERANK_POOL_MAX_WORKERS", str(_DEFAULT_THREAD_POOL_WORKERS),
            )
        )
        _ThreadPoolHolder.instance = ThreadPoolExecutor(max_workers=max_workers)
    return _ThreadPoolHolder.instance


async def run_in_process(func: Callable[..., T], *args: Any) -> T:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(get_pool(), func, *args)


async def run_in_thread(func: Callable[..., T], *args: Any) -> T:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(get_thread_pool(), func, *args)


def shutdown_pool() -> None:
    if _PoolHolder.instance is not None:
        _PoolHolder.instance.shutdown(wait=False)
        _PoolHolder.instance = None


def shutdown_thread_pool() -> None:
    if _ThreadPoolHolder.instance is not None:
        _ThreadPoolHolder.instance.shutdown(wait=False)
        _ThreadPoolHolder.instance = None
