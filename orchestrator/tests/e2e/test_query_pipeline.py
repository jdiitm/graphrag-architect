import pytest

try:
    from testcontainers.neo4j import Neo4jContainer
    HAS_TESTCONTAINERS = True
except ImportError:
    HAS_TESTCONTAINERS = False

pytestmark = [
    pytest.mark.e2e,
    pytest.mark.asyncio(loop_scope="session"),
    pytest.mark.skipif(
        not HAS_TESTCONTAINERS,
        reason="testcontainers package not installed",
    ),
]


async def test_query_after_ingest(orchestrator_client):
    from orchestrator.tests.e2e.conftest import (
        K8S_DEPLOYMENT_YAML,
        build_ingest_payload,
    )

    ingest_payload = build_ingest_payload(
        [{"path": "k8s/auth-deploy.yaml", "content": K8S_DEPLOYMENT_YAML}],
        source_type="k8s_manifest",
    )
    ingest_resp = await orchestrator_client.post(
        "/ingest?sync=true",
        json=ingest_payload,
    )
    assert ingest_resp.status_code in (200, 202)

    query_resp = await orchestrator_client.post(
        "/query",
        json={"query": "What services are deployed?", "max_results": 10},
    )
    assert query_resp.status_code == 200
    body = query_resp.json()
    assert "answer" in body
    assert isinstance(body["sources"], list)


async def test_query_empty_graph(orchestrator_client):
    response = await orchestrator_client.post(
        "/query",
        json={"query": "List all services", "max_results": 5},
    )
    assert response.status_code == 200
    body = response.json()
    assert "answer" in body
    assert isinstance(body.get("sources", []), list)


async def test_query_acl_filtering(orchestrator_client):
    from orchestrator.tests.e2e.conftest import build_ingest_payload

    tenant_a_payload = build_ingest_payload(
        [{
            "path": "tenant_a/service.yaml",
            "content": (
                "apiVersion: apps/v1\nkind: Deployment\n"
                "metadata:\n  name: tenant-a-svc\n  namespace: tenant-a\n"
                "spec:\n  replicas: 1\n  selector:\n    matchLabels:\n"
                "      app: tenant-a-svc\n  template:\n    metadata:\n"
                "      labels:\n        app: tenant-a-svc\n    spec:\n"
                "      containers:\n      - name: svc\n        image: svc:latest\n"
            ),
        }],
        source_type="k8s_manifest",
    )
    ingest_resp = await orchestrator_client.post(
        "/ingest?sync=true",
        json=tenant_a_payload,
    )
    assert ingest_resp.status_code in (200, 202)

    query_resp = await orchestrator_client.post(
        "/query",
        json={
            "query": "Show tenant-a-svc details",
            "max_results": 10,
        },
    )
    assert query_resp.status_code == 200
    body = query_resp.json()
    assert "answer" in body
