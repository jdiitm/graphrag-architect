from __future__ import annotations

import logging
from typing import Any, Optional, Set

logger = logging.getLogger(__name__)


class VectorCollectionManager:
    def __init__(
        self,
        client: Any,
        shard_number_per_key: int = 1,
    ) -> None:
        self._client = client
        self._shard_number_per_key = shard_number_per_key
        self._known_shard_keys: Set[str] = set()

    async def ensure_collection(
        self,
        collection_name: str,
        vector_size: int,
        distance: str = "Cosine",
        on_disk: Optional[bool] = None,
    ) -> None:
        exists = await self._client.collection_exists(
            collection_name=collection_name,
        )
        if exists:
            logger.debug(
                "Collection %s already exists, skipping creation",
                collection_name,
            )
            return

        await self._client.create_collection(
            collection_name=collection_name,
            vectors_config={
                "size": vector_size,
                "distance": distance,
            },
            sharding_method="custom",
            shard_number=self._shard_number_per_key,
            on_disk_payload=on_disk,
        )
        logger.info(
            "Created collection %s with custom sharding (vector_size=%d)",
            collection_name,
            vector_size,
        )

    async def ensure_shard_key(
        self,
        collection_name: str,
        shard_key: str,
    ) -> None:
        cache_key = f"{collection_name}:{shard_key}"
        if cache_key in self._known_shard_keys:
            return

        try:
            await self._client.create_shard_key(
                collection_name=collection_name,
                shard_key=shard_key,
            )
            logger.info(
                "Created shard key %s in collection %s",
                shard_key,
                collection_name,
            )
        except Exception:
            logger.debug(
                "Shard key %s may already exist in %s",
                shard_key,
                collection_name,
            )

        self._known_shard_keys.add(cache_key)
