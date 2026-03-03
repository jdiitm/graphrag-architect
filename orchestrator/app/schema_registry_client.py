from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

from fastavro import parse_schema
from fastavro.validation import validate as _avro_validate

_DEFAULT_SCHEMA_PATH = (
    Path(__file__).resolve().parents[2]
    / "infrastructure"
    / "schema-registry"
    / "ingestion-message.avsc"
)


class SchemaValidationError(Exception):
    pass


class SchemaRegistryClient:
    def __init__(
        self,
        schema_path: Path = _DEFAULT_SCHEMA_PATH,
        compatibility_mode: str = "BACKWARD",
    ) -> None:
        self._compatibility_mode = compatibility_mode
        raw_schema = json.loads(schema_path.read_text(encoding="utf-8"))
        self._parsed_schema = parse_schema(raw_schema)

    @property
    def compatibility_mode(self) -> str:
        return self._compatibility_mode

    def validate_message(self, message: Dict[str, Any]) -> bool:
        try:
            _avro_validate(message, self._parsed_schema, raise_errors=True)
        except Exception as exc:
            raise SchemaValidationError(str(exc)) from exc
        return True
