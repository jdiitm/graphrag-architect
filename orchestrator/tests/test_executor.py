import asyncio

import pytest

from orchestrator.app.executor import get_pool, run_in_process, shutdown_pool


def _cpu_bound_add(a: int, b: int) -> int:
    return a + b


class TestRunInProcess:
    @pytest.mark.asyncio
    async def test_executes_function_off_event_loop(self) -> None:
        result = await run_in_process(_cpu_bound_add, 3, 7)
        assert result == 10

    @pytest.mark.asyncio
    async def test_does_not_block_event_loop(self) -> None:
        flag = asyncio.Event()

        async def _setter() -> None:
            flag.set()

        task = asyncio.create_task(_setter())
        result = await run_in_process(_cpu_bound_add, 1, 2)
        await task
        assert result == 3
        assert flag.is_set()

    @pytest.mark.asyncio
    async def test_multiple_concurrent_calls(self) -> None:
        results = await asyncio.gather(
            run_in_process(_cpu_bound_add, 1, 1),
            run_in_process(_cpu_bound_add, 2, 2),
            run_in_process(_cpu_bound_add, 3, 3),
        )
        assert results == [2, 4, 6]


class TestPoolLifecycle:
    def test_get_pool_returns_executor(self) -> None:
        pool = get_pool()
        assert pool is not None

    def test_shutdown_pool_clears_reference(self) -> None:
        _ = get_pool()
        shutdown_pool()
        new_pool = get_pool()
        assert new_pool is not None
        shutdown_pool()
