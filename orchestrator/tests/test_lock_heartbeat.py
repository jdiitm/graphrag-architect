import asyncio
from unittest.mock import AsyncMock

import pytest

from orchestrator.app.distributed_lock import LockHeartbeat


@pytest.mark.asyncio
async def test_heartbeat_extends_lock_periodically():
    mock_lock = AsyncMock()
    mock_lock.extend = AsyncMock(return_value=True)
    ttl = 0.3
    heartbeat = LockHeartbeat(lock=mock_lock, ttl=ttl)
    heartbeat.start()
    await asyncio.sleep(0.5)
    heartbeat.stop()
    await asyncio.sleep(0.05)
    assert mock_lock.extend.call_count >= 2
    mock_lock.extend.assert_called_with(ttl)


@pytest.mark.asyncio
async def test_heartbeat_stops_on_cancel():
    mock_lock = AsyncMock()
    mock_lock.extend = AsyncMock(return_value=True)
    ttl = 0.3
    heartbeat = LockHeartbeat(lock=mock_lock, ttl=ttl)
    heartbeat.start()
    heartbeat.stop()
    await asyncio.sleep(0.15)
    initial_count = mock_lock.extend.call_count
    await asyncio.sleep(0.3)
    assert mock_lock.extend.call_count == initial_count


@pytest.mark.asyncio
async def test_heartbeat_abort_on_extension_failure():
    mock_lock = AsyncMock()
    mock_lock.extend = AsyncMock(side_effect=ConnectionError("Redis down"))
    ttl = 0.3
    heartbeat = LockHeartbeat(lock=mock_lock, ttl=ttl)
    heartbeat.start()
    await asyncio.sleep(0.2)
    assert heartbeat.abort_event.is_set()
    heartbeat.stop()


@pytest.mark.asyncio
async def test_heartbeat_no_task_leak():
    mock_lock = AsyncMock()
    mock_lock.extend = AsyncMock(return_value=True)
    ttl = 0.3
    heartbeat = LockHeartbeat(lock=mock_lock, ttl=ttl)
    heartbeat.start()
    heartbeat.stop()
    await asyncio.sleep(0.15)
    assert heartbeat._task is None or heartbeat._task.done()
