import re
from typing import FrozenSet


_WRITE_KEYWORDS = re.compile(
    r"\b(MERGE|CREATE|DELETE|DETACH\s+DELETE|SET|REMOVE|DROP)\b",
    re.IGNORECASE,
)

_LOAD_CSV = re.compile(
    r"\bLOAD\s+CSV\b", re.IGNORECASE,
)

_CALL_PROCEDURE = re.compile(
    r"\bCALL\s+(?!\{)([\w.]+)", re.IGNORECASE,
)

_DESTRUCTIVE_CALL = re.compile(
    r"\bCALL\s*\{", re.IGNORECASE,
)

ALLOWED_PROCEDURES: FrozenSet[str] = frozenset({
    "db.index.fulltext.queryNodes",
    "db.index.fulltext.queryRelationships",
    "db.labels",
    "db.relationshipTypes",
    "db.propertyKeys",
    "db.schema.visualization",
    "db.schema.nodeTypeProperties",
    "db.schema.relTypeProperties",
    "dbms.components",
    "dbms.queryJmx",
})


class CypherValidationError(ValueError):
    pass


def validate_cypher_readonly(cypher: str) -> str:
    stripped = cypher.strip()
    if _WRITE_KEYWORDS.search(stripped):
        raise CypherValidationError(
            f"Cypher contains write operation: {stripped[:80]}"
        )
    if _DESTRUCTIVE_CALL.search(stripped):
        raise CypherValidationError(
            f"Cypher contains CALL subquery: {stripped[:80]}"
        )
    if _LOAD_CSV.search(stripped):
        raise CypherValidationError(
            f"Cypher contains LOAD CSV: {stripped[:80]}"
        )
    for match in _CALL_PROCEDURE.finditer(stripped):
        procedure_name = match.group(1)
        if procedure_name not in ALLOWED_PROCEDURES:
            raise CypherValidationError(
                f"Cypher calls disallowed procedure '{procedure_name}': "
                f"{stripped[:80]}"
            )
    return stripped
