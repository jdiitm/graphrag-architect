from __future__ import annotations

from typing import Any

try:
    import redis.asyncio as aioredis
except ImportError:  # pragma: no cover
    aioredis = None  # type: ignore[assignment]


def require_redis(class_name: str) -> None:
    if aioredis is None:
        raise ImportError(f"redis package is required for {class_name}")


def create_async_redis(
    redis_url: str,
    password: str = "",
    db: int = 0,
) -> Any:
    kwargs: dict[str, Any] = {"decode_responses": True, "db": db}
    if password:
        kwargs["password"] = password
    return aioredis.from_url(redis_url, **kwargs)
