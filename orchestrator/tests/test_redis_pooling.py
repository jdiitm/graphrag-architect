"""Tests for Redis connection pooling configuration."""
from unittest.mock import MagicMock, patch

from orchestrator.app.redis_client import create_async_redis


class TestRedisPoolingConfig:
    def test_explicit_pool_size(self) -> None:
        with patch("orchestrator.app.redis_client.aioredis") as mock_aioredis:
            mock_aioredis.from_url = MagicMock(return_value="conn")
            create_async_redis("redis://localhost", max_connections=50)
            call_kwargs = mock_aioredis.from_url.call_args[1]
            assert call_kwargs["max_connections"] == 50

    def test_default_pool_size(self) -> None:
        with patch("orchestrator.app.redis_client.aioredis") as mock_aioredis:
            mock_aioredis.from_url = MagicMock(return_value="conn")
            create_async_redis("redis://localhost")
            call_kwargs = mock_aioredis.from_url.call_args[1]
            assert "max_connections" in call_kwargs
            assert call_kwargs["max_connections"] == 20

    def test_retry_on_timeout_enabled(self) -> None:
        with patch("orchestrator.app.redis_client.aioredis") as mock_aioredis:
            mock_aioredis.from_url = MagicMock(return_value="conn")
            create_async_redis("redis://localhost")
            call_kwargs = mock_aioredis.from_url.call_args[1]
            assert call_kwargs.get("retry_on_timeout") is True

    def test_socket_timeouts_set(self) -> None:
        with patch("orchestrator.app.redis_client.aioredis") as mock_aioredis:
            mock_aioredis.from_url = MagicMock(return_value="conn")
            create_async_redis("redis://localhost")
            call_kwargs = mock_aioredis.from_url.call_args[1]
            assert "socket_connect_timeout" in call_kwargs
            assert "socket_timeout" in call_kwargs
