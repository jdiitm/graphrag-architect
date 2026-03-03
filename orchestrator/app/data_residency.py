from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Dict, List

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DataResidencyConfig:
    tenant_id: str
    home_region: str
    allowed_regions: List[str] = field(default_factory=list)
    consistency: str = "eventual"


class TenantRegionRouter:

    def __init__(self, configs: Dict[str, DataResidencyConfig]) -> None:
        self._configs = dict(configs)

    def route(self, tenant_id: str) -> str:
        try:
            return self._configs[tenant_id].home_region
        except KeyError:
            logger.error("No region config for tenant %s", tenant_id)
            raise

    def is_region_allowed(self, tenant_id: str, region: str) -> bool:
        cfg = self._configs[tenant_id]
        effective = cfg.allowed_regions if cfg.allowed_regions else [cfg.home_region]
        return region in effective
