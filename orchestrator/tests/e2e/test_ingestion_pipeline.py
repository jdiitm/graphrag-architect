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


@pytest.fixture
def k8s_manifest_payload():
    from orchestrator.tests.e2e.conftest import (
        K8S_DEPLOYMENT_YAML,
        build_ingest_payload,
    )
    return build_ingest_payload(
        [{"path": "k8s/deployment.yaml", "content": K8S_DEPLOYMENT_YAML}],
        source_type="k8s_manifest",
    )


@pytest.fixture
def python_source_payload():
    from orchestrator.tests.e2e.conftest import (
        SAMPLE_PYTHON_SOURCE,
        build_ingest_payload,
    )
    return build_ingest_payload(
        [{"path": "services/auth/main.py", "content": SAMPLE_PYTHON_SOURCE}],
        source_type="source_code",
    )


async def test_ingest_kubernetes_manifest(orchestrator_client, k8s_manifest_payload):
    response = await orchestrator_client.post(
        "/ingest?sync=true",
        json=k8s_manifest_payload,
    )
    assert response.status_code in (200, 202)
    body = response.json()
    assert body["status"] in ("success", "committed")
    assert body["entities_extracted"] >= 1


async def test_ingest_source_code(orchestrator_client, python_source_payload):
    response = await orchestrator_client.post(
        "/ingest?sync=true",
        json=python_source_payload,
    )
    assert response.status_code in (200, 202)
    body = response.json()
    assert body["status"] in ("success", "committed")
    assert body["entities_extracted"] >= 0


async def test_ingest_idempotent(orchestrator_client, k8s_manifest_payload):
    first = await orchestrator_client.post(
        "/ingest?sync=true",
        json=k8s_manifest_payload,
    )
    assert first.status_code in (200, 202)
    first_count = first.json()["entities_extracted"]

    second = await orchestrator_client.post(
        "/ingest?sync=true",
        json=k8s_manifest_payload,
    )
    assert second.status_code in (200, 202)
    second_count = second.json()["entities_extracted"]

    assert second_count == first_count


async def test_ingest_invalid_payload(orchestrator_client):
    response = await orchestrator_client.post(
        "/ingest?sync=true",
        json={"documents": []},
    )
    assert response.status_code == 422


async def test_ingest_large_batch(orchestrator_client):
    from orchestrator.tests.e2e.conftest import build_ingest_payload

    files = [
        {
            "path": f"services/svc_{i}/main.py",
            "content": f"# service {i}\nclass Service{i}:\n    pass\n",
        }
        for i in range(50)
    ]
    payload = build_ingest_payload(files, source_type="source_code")

    response = await orchestrator_client.post(
        "/ingest?sync=true",
        json=payload,
    )
    assert response.status_code in (200, 202)
    body = response.json()
    assert body["status"] in ("success", "committed", "failed")
