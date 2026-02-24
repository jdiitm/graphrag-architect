from __future__ import annotations

import base64
import binascii
import json
import logging
from typing import Any, Callable, Coroutine, Dict, List, Optional, Protocol, Sequence, Tuple

from orchestrator.app.config import KafkaConsumerConfig

logger = logging.getLogger(__name__)

IngestCallback = Callable[[List[Dict[str, str]]], Coroutine[Any, Any, Dict[str, Any]]]


class KafkaConsumerProtocol(Protocol):
    def __aiter__(self) -> Any:
        ...


ConsumerFactory = Callable[[KafkaConsumerConfig], KafkaConsumerProtocol]


def _headers_to_dict(
    headers: Optional[Sequence[Tuple[str, Optional[bytes]]]],
) -> Dict[str, str]:
    if not headers:
        return {}
    result: Dict[str, str] = {}
    for k, v in headers:
        if v is None:
            continue
        try:
            result[k] = v.decode("utf-8") if isinstance(v, bytes) else str(v)
        except UnicodeDecodeError:
            continue
    return result


class AsyncKafkaConsumer:
    def __init__(
        self,
        config: KafkaConsumerConfig,
        ingest_callback: IngestCallback,
        consumer_factory: Optional[ConsumerFactory] = None,
    ) -> None:
        self._config = config
        self._ingest = ingest_callback
        self._consumer_factory = consumer_factory
        self._shutdown = False
        self._consumer: Optional[KafkaConsumerProtocol] = None

    async def start(self) -> None:
        self._shutdown = False
        if self._consumer_factory is None:
            return
        consumer = self._consumer_factory(self._config)
        self._consumer = consumer
        async for message in consumer:
            if self._shutdown:
                break
            key = getattr(message, "key", None)
            value = getattr(message, "value", None)
            msg_headers = getattr(message, "headers", None) or []
            try:
                await self.process_message(key, value, msg_headers)
            except Exception as exc:
                logger.exception("Failed to process message: %s", exc)

    async def stop(self) -> None:
        self._shutdown = True

    async def process_parsed_message(self, value: Optional[bytes]) -> Dict[str, Any]:
        if value is None:
            return {"status": "failed", "error": "message value is empty"}
        try:
            payload = json.loads(value)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            return {"status": "failed", "error": f"json decode error: {exc}"}
        file_path = payload.get("file_path", "unknown")
        content = payload.get("content")
        if content is None:
            return {"status": "failed", "error": "parsed payload missing content field"}
        raw_files = [{"path": file_path, "content": content}]
        return await self._ingest(raw_files)

    async def process_message(
        self,
        key: Optional[bytes],
        value: Optional[bytes],
        headers: Optional[Sequence[Tuple[str, Optional[bytes]]]] = None,
    ) -> Dict[str, Any]:
        header_dict = _headers_to_dict(headers)
        file_path = header_dict.get("file_path") or header_dict.get("file-path")
        if file_path is None and key is not None:
            try:
                file_path = key.decode("utf-8")
            except UnicodeDecodeError:
                file_path = "unknown"
        if file_path is None:
            file_path = "unknown"
        if value is None:
            return {"status": "failed", "error": "message value is empty"}
        try:
            decoded_bytes = base64.b64decode(value, validate=True)
            decoded_content = decoded_bytes.decode("utf-8")
        except (binascii.Error, UnicodeDecodeError) as exc:
            logger.warning("Decode error for message (key=%r): %s", key, exc)
            return {"status": "failed", "error": f"decode error: {exc}"}
        raw_files = [{"path": file_path, "content": decoded_content}]
        return await self._ingest(raw_files)
