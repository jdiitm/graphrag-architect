from __future__ import annotations

import logging
from typing import Any

try:
    import redis.asyncio as aioredis
except ImportError:  # pragma: no cover
    aioredis = None  # type: ignore[assignment]

_logger = logging.getLogger(__name__)


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


async def delete_keys_by_prefix(
    redis_conn: Any,
    prefix: str,
) -> None:
    cursor = None
    pattern = f"{prefix}*"
    while cursor != 0:
        cursor, keys = await redis_conn.scan(
            cursor=cursor or 0, match=pattern, count=100,
        )
        if keys:
            await redis_conn.delete(*keys)
