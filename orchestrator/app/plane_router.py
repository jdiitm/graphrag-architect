from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, List, Optional

from fastapi import FastAPI

logger = logging.getLogger(__name__)


class Plane(Enum):
    CONTROL = "control"
    DATA = "data"


@dataclass(frozen=True)
class RouteBinding:
    path: str
    methods: List[str]
    plane: Plane
    handler: Callable[..., Any]
    tags: List[str]


class PlaneRouter:
    def __init__(self) -> None:
        self._bindings: List[RouteBinding] = []

    def register(self, binding: RouteBinding) -> None:
        for existing in self._bindings:
            if existing.path == binding.path and set(existing.methods) & set(binding.methods):
                raise ValueError(
                    f"Duplicate route: {binding.methods} {binding.path}"
                )
        self._bindings.append(binding)

    def control_routes(self) -> List[RouteBinding]:
        return [b for b in self._bindings if b.plane == Plane.CONTROL]

    def data_routes(self) -> List[RouteBinding]:
        return [b for b in self._bindings if b.plane == Plane.DATA]

    def all_routes(self) -> List[RouteBinding]:
        return list(self._bindings)


@dataclass(frozen=True)
class PlaneConfig:
    plane: Plane
    title: str = ""
    version: str = "1.0.0"
    port: int = 8000
    workers: int = 1


def create_plane_app(
    config: PlaneConfig,
    router: PlaneRouter,
    lifespan: Optional[Any] = None,
) -> FastAPI:
    title = config.title or f"GraphRAG {config.plane.value.title()} Plane"
    app = FastAPI(title=title, version=config.version, lifespan=lifespan)

    if config.plane == Plane.CONTROL:
        bindings = router.control_routes()
    elif config.plane == Plane.DATA:
        bindings = router.data_routes()
    else:
        bindings = router.all_routes()

    for binding in bindings:
        for method in binding.methods:
            method_lower = method.lower()
            decorator = getattr(app, method_lower, None)
            if decorator is not None:
                decorator(binding.path, tags=binding.tags)(binding.handler)

    logger.info(
        "Created %s plane app with %d routes",
        config.plane.value, len(bindings),
    )
    return app


def create_unified_app(
    router: PlaneRouter,
    lifespan: Optional[Any] = None,
) -> FastAPI:
    app = FastAPI(
        title="GraphRAG Orchestrator (Unified)",
        version="1.0.0",
        lifespan=lifespan,
    )
    for binding in router.all_routes():
        for method in binding.methods:
            method_lower = method.lower()
            decorator = getattr(app, method_lower, None)
            if decorator is not None:
                decorator(binding.path, tags=binding.tags)(binding.handler)

    logger.info(
        "Created unified app with %d routes", len(router.all_routes()),
    )
    return app
