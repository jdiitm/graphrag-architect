from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from orchestrator.app.plane_router import (
    Plane,
    PlaneConfig,
    PlaneRouter,
    RouteBinding,
    create_plane_app,
    create_unified_app,
)


def _health_handler() -> dict:
    return {"status": "healthy"}


def _metrics_handler() -> dict:
    return {"metrics": []}


def _ingest_handler() -> dict:
    return {"status": "success"}


def _query_handler() -> dict:
    return {"answer": "42"}


def _build_router() -> PlaneRouter:
    router = PlaneRouter()
    router.register(RouteBinding(
        path="/health", methods=["GET"], plane=Plane.CONTROL,
        handler=_health_handler, tags=["control"],
    ))
    router.register(RouteBinding(
        path="/metrics", methods=["GET"], plane=Plane.CONTROL,
        handler=_metrics_handler, tags=["control"],
    ))
    router.register(RouteBinding(
        path="/ingest", methods=["POST"], plane=Plane.DATA,
        handler=_ingest_handler, tags=["data"],
    ))
    router.register(RouteBinding(
        path="/query", methods=["POST"], plane=Plane.DATA,
        handler=_query_handler, tags=["data"],
    ))
    return router


class TestPlaneRouter:

    def test_control_routes(self) -> None:
        router = _build_router()
        control = router.control_routes()
        assert len(control) == 2
        assert all(b.plane == Plane.CONTROL for b in control)

    def test_data_routes(self) -> None:
        router = _build_router()
        data = router.data_routes()
        assert len(data) == 2
        assert all(b.plane == Plane.DATA for b in data)

    def test_all_routes(self) -> None:
        router = _build_router()
        assert len(router.all_routes()) == 4

    def test_duplicate_route_rejected(self) -> None:
        router = PlaneRouter()
        router.register(RouteBinding(
            path="/health", methods=["GET"], plane=Plane.CONTROL,
            handler=_health_handler, tags=["control"],
        ))
        with pytest.raises(ValueError, match="Duplicate route"):
            router.register(RouteBinding(
                path="/health", methods=["GET"], plane=Plane.DATA,
                handler=_health_handler, tags=["data"],
            ))


class TestControlPlaneApp:

    def test_control_plane_has_health(self) -> None:
        router = _build_router()
        config = PlaneConfig(plane=Plane.CONTROL)
        app = create_plane_app(config, router)
        client = TestClient(app)
        resp = client.get("/health")
        assert resp.status_code == 200
        assert resp.json()["status"] == "healthy"

    def test_control_plane_no_data_routes(self) -> None:
        router = _build_router()
        config = PlaneConfig(plane=Plane.CONTROL)
        app = create_plane_app(config, router)
        client = TestClient(app)
        resp = client.post("/ingest")
        assert resp.status_code in (404, 405)

    def test_control_plane_title(self) -> None:
        router = _build_router()
        config = PlaneConfig(plane=Plane.CONTROL, title="Custom Control")
        app = create_plane_app(config, router)
        assert app.title == "Custom Control"


class TestDataPlaneApp:

    def test_data_plane_has_ingest(self) -> None:
        router = _build_router()
        config = PlaneConfig(plane=Plane.DATA)
        app = create_plane_app(config, router)
        client = TestClient(app)
        resp = client.post("/ingest")
        assert resp.status_code == 200
        assert resp.json()["status"] == "success"

    def test_data_plane_has_query(self) -> None:
        router = _build_router()
        config = PlaneConfig(plane=Plane.DATA)
        app = create_plane_app(config, router)
        client = TestClient(app)
        resp = client.post("/query")
        assert resp.status_code == 200
        assert resp.json()["answer"] == "42"

    def test_data_plane_no_control_routes(self) -> None:
        router = _build_router()
        config = PlaneConfig(plane=Plane.DATA)
        app = create_plane_app(config, router)
        client = TestClient(app)
        resp = client.get("/health")
        assert resp.status_code in (404, 405)


class TestUnifiedApp:

    def test_unified_has_all_routes(self) -> None:
        router = _build_router()
        app = create_unified_app(router)
        client = TestClient(app)
        assert client.get("/health").status_code == 200
        assert client.get("/metrics").status_code == 200
        assert client.post("/ingest").status_code == 200
        assert client.post("/query").status_code == 200


class TestPlaneConfig:

    def test_defaults(self) -> None:
        cfg = PlaneConfig(plane=Plane.CONTROL)
        assert cfg.port == 8000
        assert cfg.workers == 1
        assert cfg.version == "1.0.0"

    def test_default_title(self) -> None:
        router = _build_router()
        app = create_plane_app(PlaneConfig(plane=Plane.DATA), router)
        assert app.title == "GraphRAG Data Plane"
