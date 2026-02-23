from orchestrator.app.ast_extraction import PythonASTExtractor


PYTHON_FASTAPI_APP = '''
from fastapi import FastAPI

app = FastAPI()

@app.get("/health")
def health():
    return {"status": "ok"}
'''

PYTHON_FLASK_APP = '''
from flask import Flask

app = Flask(__name__)

@app.route("/health")
def health():
    return "ok"
'''

PYTHON_GRPC_SERVER = '''
import grpc
from concurrent import futures

server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
server.add_insecure_port("[::]:50051")
server.start()
'''

PYTHON_HTTP_CLIENT = '''
import httpx

def call_auth_service():
    response = httpx.get("http://auth-service:8000/verify")
    return response.json()

def call_order_service():
    response = httpx.post("http://order-service:8000/create")
    return response.json()
'''

PYTHON_REQUESTS_CLIENT = '''
import requests

def fetch_data():
    resp = requests.get("http://data-service:8080/api/data")
    return resp.json()
'''

PYTHON_KAFKA_PRODUCER = '''
from aiokafka import AIOKafkaProducer

async def produce():
    producer = AIOKafkaProducer(bootstrap_servers="kafka:9092")
    await producer.send("user-events", b"data")
'''

PYTHON_WEBSOCKET_SEND = '''
import websocket

def send_message():
    ws = websocket.WebSocket()
    ws.send("user-events")
'''

PYTHON_SOCKET_SEND = '''
import socket

def send_data():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.send("telemetry")
'''

PYTHON_NO_SERVICE = '''
def add(a, b):
    return a + b

class Calculator:
    def multiply(self, a, b):
        return a * b
'''

GO_CODE = '''package main

import "net/http"

func main() {
    http.ListenAndServe(":8080", nil)
}
'''


class TestPythonASTServiceDetection:
    def test_detects_fastapi_app(self):
        extractor = PythonASTExtractor()
        result = extractor.extract("app.py", PYTHON_FASTAPI_APP)
        assert len(result.services) >= 1
        svc = result.services[0]
        assert svc.language == "python"
        assert svc.framework == "fastapi"

    def test_detects_flask_app(self):
        extractor = PythonASTExtractor()
        result = extractor.extract("app.py", PYTHON_FLASK_APP)
        assert len(result.services) >= 1
        svc = result.services[0]
        assert svc.framework == "flask"

    def test_detects_grpc_server(self):
        extractor = PythonASTExtractor()
        result = extractor.extract("server.py", PYTHON_GRPC_SERVER)
        assert len(result.services) >= 1
        svc = result.services[0]
        assert svc.framework == "grpc"

    def test_no_service_for_utility_module(self):
        extractor = PythonASTExtractor()
        result = extractor.extract("utils.py", PYTHON_NO_SERVICE)
        assert len(result.services) == 0

    def test_ignores_go_source(self):
        extractor = PythonASTExtractor()
        result = extractor.extract("main.go", GO_CODE)
        assert len(result.services) == 0


class TestPythonASTCallDetection:
    def test_detects_httpx_calls(self):
        extractor = PythonASTExtractor()
        result = extractor.extract("client.py", PYTHON_HTTP_CLIENT)
        assert len(result.calls) >= 2
        urls = {c.target_hint for c in result.calls}
        assert any("auth-service" in u for u in urls)
        assert any("order-service" in u for u in urls)

    def test_detects_requests_calls(self):
        extractor = PythonASTExtractor()
        result = extractor.extract("client.py", PYTHON_REQUESTS_CLIENT)
        assert len(result.calls) >= 1
        assert result.calls[0].protocol == "http"


class TestPythonASTKafkaDetection:
    def test_detects_kafka_producer_topic(self):
        extractor = PythonASTExtractor()
        result = extractor.extract("producer.py", PYTHON_KAFKA_PRODUCER)
        assert "user-events" in result.topics_produced

    def test_websocket_send_not_detected_as_kafka(self):
        extractor = PythonASTExtractor()
        result = extractor.extract("ws_client.py", PYTHON_WEBSOCKET_SEND)
        assert len(result.topics_produced) == 0

    def test_socket_send_not_detected_as_kafka(self):
        extractor = PythonASTExtractor()
        result = extractor.extract("net_client.py", PYTHON_SOCKET_SEND)
        assert len(result.topics_produced) == 0


class TestPythonASTBatchExtraction:
    def test_extract_all_multiple_files(self):
        extractor = PythonASTExtractor()
        files = [
            {"path": "app.py", "content": PYTHON_FASTAPI_APP},
            {"path": "client.py", "content": PYTHON_HTTP_CLIENT},
            {"path": "utils.py", "content": PYTHON_NO_SERVICE},
        ]
        result = extractor.extract_all(files)
        assert len(result.services) >= 1
        assert len(result.calls) >= 2

    def test_extract_all_filters_non_python(self):
        extractor = PythonASTExtractor()
        files = [
            {"path": "main.go", "content": GO_CODE},
            {"path": "app.py", "content": PYTHON_FASTAPI_APP},
        ]
        result = extractor.extract_all(files)
        python_services = [s for s in result.services if s.language == "python"]
        assert len(python_services) >= 1
