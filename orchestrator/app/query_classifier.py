import re

from orchestrator.app.query_models import QueryComplexity

_AGGREGATE_PATTERNS = re.compile(
    r"most critical|top \d|count|how many|ranking|rank\b|highest|"
    r"transitive.*count|by.*count",
    re.IGNORECASE,
)

_MULTI_HOP_PATTERNS = re.compile(
    r"blast radius|downstream|upstream|depends on|dependency chain|"
    r"cascade|if.*fails|impact|indirect|transitive(?!.*count)",
    re.IGNORECASE,
)

_SINGLE_HOP_PATTERNS = re.compile(
    r"produce[sd]?( to)?|consume[sd]?( from)?|calls?\b|"
    r"deployed in|connects? to|communicates? with",
    re.IGNORECASE,
)


def classify_query(query: str) -> QueryComplexity:
    if _AGGREGATE_PATTERNS.search(query):
        return QueryComplexity.AGGREGATE
    if _MULTI_HOP_PATTERNS.search(query):
        return QueryComplexity.MULTI_HOP
    if _SINGLE_HOP_PATTERNS.search(query):
        return QueryComplexity.SINGLE_HOP
    return QueryComplexity.ENTITY_LOOKUP
