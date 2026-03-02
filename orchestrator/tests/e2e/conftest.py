import asyncio
import base64
from typing import Any, AsyncIterator, Dict, Generator, List

import pytest
import pytest_asyncio

try:
    from testcontainers.kafka import KafkaContainer
    from testcontainers.neo4j import Neo4jContainer
    HAS_TESTCONTAINERS = True
except ImportError:
    HAS_TESTCONTAINERS = False

try:
    from httpx import ASGITransport, AsyncClient
    HAS_HTTPX = True
except ImportError:
    HAS_HTTPX = False

pytestmark = [
    pytest.mark.e2e,
    pytest.mark.skipif(
        not HAS_TESTCONTAINERS,
        reason="testcontainers package not installed",
    ),
    pytest.mark.skipif(
        not HAS_HTTPX,
        reason="httpx package not installed",
    ),
]

NEO4J_IMAGE = "neo4j:5.26-community"
NEO4J_TEST_PASSWORD = "e2e-test-password"


@pytest.fixture(scope="session")
def event_loop() -> Generator[asyncio.AbstractEventLoop, None, None]:
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session")
def neo4j_container() -> Generator[Any, None, None]:
    container = Neo4jContainer(
        image=NEO4J_IMAGE,
        password=NEO4J_TEST_PASSWORD,
    ).with_env("NEO4J_PLUGINS", '["apoc"]')
    with container:
        yield container


@pytest.fixture(scope="session")
def kafka_container() -> Generator[Any, None, None]:
    container = KafkaContainer()
    with container:
        yield container


@pytest.fixture(scope="session")
def neo4j_bolt_url(neo4j_container: Any) -> str:
    return neo4j_container.get_connection_url()


@pytest.fixture(scope="session")
def kafka_bootstrap_server(kafka_container: Any) -> str:
    return kafka_container.get_bootstrap_server()


@pytest.fixture(autouse=True)
def e2e_env(
    monkeypatch: pytest.MonkeyPatch,
    neo4j_bolt_url: str,
    kafka_bootstrap_server: str,
) -> None:
    monkeypatch.setenv("NEO4J_URI", neo4j_bolt_url)
    monkeypatch.setenv("NEO4J_PASSWORD", NEO4J_TEST_PASSWORD)
    monkeypatch.setenv("NEO4J_USERNAME", "neo4j")
    monkeypatch.setenv("KAFKA_BROKERS", kafka_bootstrap_server)
    monkeypatch.setenv("KAFKA_CONSUMER_ENABLED", "false")
    monkeypatch.setenv("AUTH_REQUIRE_TOKENS", "false")
    monkeypatch.setenv("DEPLOYMENT_MODE", "dev")
    monkeypatch.setenv("VECTOR_STORE_BACKEND", "in_memory")
    monkeypatch.setenv("AST_EXTRACTION_MODE", "local")
    monkeypatch.setenv("OUTBOX_COALESCING", "false")
    monkeypatch.setenv("RAG_EVAL_ENABLED", "false")
    monkeypatch.setenv("PROMPT_GUARDRAILS_ENABLED", "false")


@pytest_asyncio.fixture
async def orchestrator_client(
    e2e_env: None,
) -> AsyncIterator[AsyncClient]:
    from orchestrator.app.main import app

    transport = ASGITransport(app=app)
    async with AsyncClient(
        transport=transport,
        base_url="http://e2e-test",
        timeout=60.0,
    ) as client:
        yield client


def encode_file_content(content: str) -> str:
    return base64.b64encode(content.encode("utf-8")).decode("ascii")


def build_ingest_payload(
    files: List[Dict[str, str]],
    source_type: str = "source_code",
) -> Dict[str, Any]:
    documents = []
    for file_info in files:
        documents.append({
            "file_path": file_info["path"],
            "content": encode_file_content(file_info["content"]),
            "source_type": source_type,
        })
    return {"documents": documents}


K8S_DEPLOYMENT_YAML = """apiVersion: apps/v1
kind: Deployment
metadata:
  name: auth-service
  namespace: production
spec:
  replicas: 3
  selector:
    matchLabels:
      app: auth-service
  template:
    metadata:
      labels:
        app: auth-service
    spec:
      containers:
      - name: auth-service
        image: auth-service:latest
        ports:
        - containerPort: 8080
"""

SAMPLE_PYTHON_SOURCE = '''from fastapi import FastAPI

app = FastAPI()

class AuthService:
    def authenticate(self, token: str) -> bool:
        return self.validator.validate(token)

    def call_user_service(self):
        return requests.get("http://user-service/api/users")
'''

SAMPLE_GO_SOURCE = '''package main

import (
    "net/http"
    "github.com/gin-gonic/gin"
)

func main() {
    r := gin.Default()
    r.GET("/health", func(c *gin.Context) {
        c.JSON(http.StatusOK, gin.H{"status": "ok"})
    })
    r.Run(":8080")
}
'''
