import re


_WRITE_KEYWORDS = re.compile(
    r"\b(MERGE|CREATE|DELETE|DETACH\s+DELETE|SET|REMOVE|DROP)\b",
    re.IGNORECASE,
)

_DESTRUCTIVE_CALL = re.compile(
    r"\bCALL\s*\{", re.IGNORECASE,
)


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
    return stripped
