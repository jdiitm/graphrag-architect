from __future__ import annotations

from typing import Any, Dict, List, Set

from neo4j import AsyncDriver, AsyncManagedTransaction


TOMBSTONE_CHECK_QUERY = (
    "UNWIND $node_ids AS nid "
    "MATCH (n {id: nid})-[r]->() "
    "WHERE r.tombstoned_at IS NOT NULL "
    "RETURN DISTINCT n.id AS node_id"
)

TOMBSTONE_CHECK_TENANT_QUERY = (
    "UNWIND $node_ids AS nid "
    "MATCH (n {id: nid, tenant_id: $tenant_id})-[r]->() "
    "WHERE r.tombstoned_at IS NOT NULL "
    "RETURN DISTINCT n.id AS node_id"
)


async def check_tombstoned_nodes(
    driver: AsyncDriver,
    node_ids: List[str],
    tenant_id: str = "",
) -> Set[str]:
    if not node_ids:
        return set()

    if tenant_id:
        query = TOMBSTONE_CHECK_TENANT_QUERY
        params: Dict[str, Any] = {"node_ids": node_ids, "tenant_id": tenant_id}
    else:
        query = TOMBSTONE_CHECK_QUERY
        params = {"node_ids": node_ids}

    async def _tx(tx: AsyncManagedTransaction) -> Set[str]:
        result = await tx.run(query, **params)
        data = await result.data()
        return {row["node_id"] for row in data}

    async with driver.session() as session:
        return await session.execute_read(_tx)


async def filter_tombstoned_results(
    driver: AsyncDriver,
    candidates: List[Dict[str, Any]],
    tenant_id: str = "",
) -> List[Dict[str, Any]]:
    if not candidates:
        return []

    node_ids = [c["id"] for c in candidates if "id" in c]
    if not node_ids:
        return candidates

    tombstoned = await check_tombstoned_nodes(driver, node_ids, tenant_id)
    if not tombstoned:
        return candidates

    return [c for c in candidates if c.get("id") not in tombstoned]
