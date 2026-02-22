import textwrap

import pytest

from orchestrator.app.extraction_models import K8sDeploymentNode, KafkaTopicNode
from orchestrator.app.manifest_parser import (
    parse_all_manifests,
    parse_k8s_manifests,
    parse_kafka_topics,
)


class TestParseK8sManifests:
    def test_valid_deployment(self):
        content = textwrap.dedent("""\
            apiVersion: apps/v1
            kind: Deployment
            metadata:
              name: auth-service
              namespace: production
            spec:
              replicas: 3
        """)
        result = parse_k8s_manifests(content)
        assert len(result) == 1
        node = result[0]
        assert isinstance(node, K8sDeploymentNode)
        assert node.id == "auth-service"
        assert node.namespace == "production"
        assert node.replicas == 3

    def test_deployment_default_namespace(self):
        content = textwrap.dedent("""\
            apiVersion: apps/v1
            kind: Deployment
            metadata:
              name: order-service
            spec:
              replicas: 2
        """)
        result = parse_k8s_manifests(content)
        assert len(result) == 1
        assert result[0].namespace == "default"

    def test_deployment_default_replicas(self):
        content = textwrap.dedent("""\
            apiVersion: apps/v1
            kind: Deployment
            metadata:
              name: gateway
              namespace: staging
        """)
        result = parse_k8s_manifests(content)
        assert len(result) == 1
        assert result[0].replicas == 1

    def test_ignores_non_deployment_kind(self):
        content = textwrap.dedent("""\
            apiVersion: v1
            kind: Service
            metadata:
              name: my-service
        """)
        result = parse_k8s_manifests(content)
        assert result == []

    def test_missing_metadata_name_skipped(self):
        content = textwrap.dedent("""\
            apiVersion: apps/v1
            kind: Deployment
            metadata:
              namespace: staging
            spec:
              replicas: 2
        """)
        result = parse_k8s_manifests(content)
        assert result == []

    def test_multi_document_yaml(self):
        content = textwrap.dedent("""\
            apiVersion: apps/v1
            kind: Deployment
            metadata:
              name: service-a
              namespace: ns1
            spec:
              replicas: 2
            ---
            apiVersion: apps/v1
            kind: Deployment
            metadata:
              name: service-b
              namespace: ns2
            spec:
              replicas: 5
        """)
        result = parse_k8s_manifests(content)
        assert len(result) == 2
        names = {node.id for node in result}
        assert names == {"service-a", "service-b"}

    def test_multi_document_mixed_kinds(self):
        content = textwrap.dedent("""\
            apiVersion: v1
            kind: Service
            metadata:
              name: ignored-svc
            ---
            apiVersion: apps/v1
            kind: Deployment
            metadata:
              name: real-deploy
              namespace: prod
            spec:
              replicas: 4
        """)
        result = parse_k8s_manifests(content)
        assert len(result) == 1
        assert result[0].id == "real-deploy"

    def test_malformed_yaml_returns_empty(self):
        result = parse_k8s_manifests("not: valid: yaml: {{{}}}::::")
        assert result == []

    def test_empty_content_returns_empty(self):
        result = parse_k8s_manifests("")
        assert result == []

    def test_null_document_in_multi_doc_skipped(self):
        content = textwrap.dedent("""\
            ---
            ---
            apiVersion: apps/v1
            kind: Deployment
            metadata:
              name: valid-deploy
            spec:
              replicas: 1
        """)
        result = parse_k8s_manifests(content)
        assert len(result) == 1
        assert result[0].id == "valid-deploy"


class TestParseKafkaTopics:
    def test_valid_strimzi_topic(self):
        content = textwrap.dedent("""\
            apiVersion: kafka.strimzi.io/v1beta2
            kind: KafkaTopic
            metadata:
              name: raw-documents
            spec:
              partitions: 6
              config:
                retention.ms: "604800000"
        """)
        result = parse_kafka_topics(content)
        assert len(result) == 1
        topic = result[0]
        assert isinstance(topic, KafkaTopicNode)
        assert topic.name == "raw-documents"
        assert topic.partitions == 6
        assert topic.retention_ms == 604800000

    def test_topic_default_partitions(self):
        content = textwrap.dedent("""\
            apiVersion: kafka.strimzi.io/v1beta2
            kind: KafkaTopic
            metadata:
              name: events
            spec:
              config:
                retention.ms: "86400000"
        """)
        result = parse_kafka_topics(content)
        assert len(result) == 1
        assert result[0].partitions == 1

    def test_topic_default_retention(self):
        content = textwrap.dedent("""\
            apiVersion: kafka.strimzi.io/v1beta2
            kind: KafkaTopic
            metadata:
              name: logs
            spec:
              partitions: 3
        """)
        result = parse_kafka_topics(content)
        assert len(result) == 1
        assert result[0].retention_ms == 604800000

    def test_missing_metadata_name_skipped(self):
        content = textwrap.dedent("""\
            apiVersion: kafka.strimzi.io/v1beta2
            kind: KafkaTopic
            spec:
              partitions: 3
        """)
        result = parse_kafka_topics(content)
        assert result == []

    def test_ignores_non_topic_kind(self):
        content = textwrap.dedent("""\
            apiVersion: kafka.strimzi.io/v1beta2
            kind: Kafka
            metadata:
              name: my-cluster
        """)
        result = parse_kafka_topics(content)
        assert result == []

    def test_multi_document_topics(self):
        content = textwrap.dedent("""\
            apiVersion: kafka.strimzi.io/v1beta2
            kind: KafkaTopic
            metadata:
              name: topic-a
            spec:
              partitions: 3
              config:
                retention.ms: "100000"
            ---
            apiVersion: kafka.strimzi.io/v1beta2
            kind: KafkaTopic
            metadata:
              name: topic-b
            spec:
              partitions: 12
              config:
                retention.ms: "200000"
        """)
        result = parse_kafka_topics(content)
        assert len(result) == 2
        names = {t.name for t in result}
        assert names == {"topic-a", "topic-b"}

    def test_retention_as_integer(self):
        content = textwrap.dedent("""\
            apiVersion: kafka.strimzi.io/v1beta2
            kind: KafkaTopic
            metadata:
              name: int-retention
            spec:
              partitions: 1
              config:
                retention.ms: 3600000
        """)
        result = parse_kafka_topics(content)
        assert len(result) == 1
        assert result[0].retention_ms == 3600000

    def test_malformed_yaml_returns_empty(self):
        result = parse_kafka_topics("{{broken yaml")
        assert result == []

    def test_empty_content_returns_empty(self):
        result = parse_kafka_topics("")
        assert result == []


class TestParseAllManifests:
    def test_filters_yaml_extensions_only(self):
        files = [
            {"path": "deploy.yaml", "content": textwrap.dedent("""\
                apiVersion: apps/v1
                kind: Deployment
                metadata:
                  name: svc-from-yaml
                  namespace: test
                spec:
                  replicas: 1
            """)},
            {"path": "main.go", "content": "package main"},
            {"path": "app.py", "content": "print('hello')"},
            {"path": "config.yml", "content": textwrap.dedent("""\
                apiVersion: kafka.strimzi.io/v1beta2
                kind: KafkaTopic
                metadata:
                  name: topic-from-yml
                spec:
                  partitions: 2
                  config:
                    retention.ms: "86400000"
            """)},
        ]
        result = parse_all_manifests(files)
        assert len(result) == 2
        types = {type(e) for e in result}
        assert types == {K8sDeploymentNode, KafkaTopicNode}

    def test_combines_deployments_and_topics(self):
        files = [
            {"path": "k8s/deploy.yaml", "content": textwrap.dedent("""\
                apiVersion: apps/v1
                kind: Deployment
                metadata:
                  name: auth-svc
                  namespace: prod
                spec:
                  replicas: 3
            """)},
            {"path": "kafka/topics.yaml", "content": textwrap.dedent("""\
                apiVersion: kafka.strimzi.io/v1beta2
                kind: KafkaTopic
                metadata:
                  name: events
                spec:
                  partitions: 6
                  config:
                    retention.ms: "604800000"
            """)},
        ]
        result = parse_all_manifests(files)
        assert len(result) == 2
        deployment = [e for e in result if isinstance(e, K8sDeploymentNode)][0]
        topic = [e for e in result if isinstance(e, KafkaTopicNode)][0]
        assert deployment.id == "auth-svc"
        assert topic.name == "events"

    def test_empty_files_list(self):
        result = parse_all_manifests([])
        assert result == []

    def test_no_yaml_files(self):
        files = [
            {"path": "main.go", "content": "package main"},
            {"path": "app.py", "content": "import os"},
        ]
        result = parse_all_manifests(files)
        assert result == []

    def test_skips_invalid_manifests(self):
        files = [
            {"path": "bad.yaml", "content": "not: valid: yaml: {{{}}}"},
            {"path": "good.yaml", "content": textwrap.dedent("""\
                apiVersion: apps/v1
                kind: Deployment
                metadata:
                  name: good-deploy
                  namespace: prod
                spec:
                  replicas: 2
            """)},
        ]
        result = parse_all_manifests(files)
        assert len(result) == 1
        assert isinstance(result[0], K8sDeploymentNode)
        assert result[0].id == "good-deploy"

    def test_multi_doc_file_with_mixed_entities(self):
        content = textwrap.dedent("""\
            apiVersion: apps/v1
            kind: Deployment
            metadata:
              name: my-deploy
              namespace: staging
            spec:
              replicas: 2
            ---
            apiVersion: kafka.strimzi.io/v1beta2
            kind: KafkaTopic
            metadata:
              name: my-topic
            spec:
              partitions: 4
              config:
                retention.ms: "300000"
        """)
        files = [{"path": "infra.yaml", "content": content}]
        result = parse_all_manifests(files)
        assert len(result) == 2
        types = {type(e) for e in result}
        assert types == {K8sDeploymentNode, KafkaTopicNode}


class TestDagNodeIntegration:
    def test_parse_manifests_appends_to_existing_nodes(self):
        from orchestrator.app.extraction_models import ServiceNode
        from orchestrator.app.graph_builder import parse_k8s_and_kafka_manifests

        existing_service = ServiceNode(
            id="auth-service",
            name="auth-service",
            language="go",
            framework="gin",
            opentelemetry_enabled=False,
        )
        state = {
            "directory_path": "",
            "raw_files": [
                {"path": "deploy.yaml", "content": textwrap.dedent("""\
                    apiVersion: apps/v1
                    kind: Deployment
                    metadata:
                      name: auth-deploy
                      namespace: prod
                    spec:
                      replicas: 3
                """)},
            ],
            "extracted_nodes": [existing_service],
            "extraction_errors": [],
            "validation_retries": 0,
            "commit_status": "",
        }
        result = parse_k8s_and_kafka_manifests(state)
        nodes = result["extracted_nodes"]
        assert len(nodes) == 2
        assert nodes[0] is existing_service
        assert isinstance(nodes[1], K8sDeploymentNode)

    def test_parse_manifests_no_yaml_preserves_existing(self):
        from orchestrator.app.extraction_models import CallsEdge, ServiceNode
        from orchestrator.app.graph_builder import parse_k8s_and_kafka_manifests

        svc = ServiceNode(
            id="svc-a", name="svc-a", language="python",
            framework="fastapi", opentelemetry_enabled=True,
        )
        edge = CallsEdge(
            source_service_id="svc-a", target_service_id="svc-b", protocol="grpc",
        )
        state = {
            "directory_path": "",
            "raw_files": [{"path": "main.go", "content": "package main"}],
            "extracted_nodes": [svc, edge],
            "extraction_errors": [],
            "validation_retries": 0,
            "commit_status": "",
        }
        result = parse_k8s_and_kafka_manifests(state)
        assert result["extracted_nodes"] == [svc, edge]

    def test_parse_manifests_empty_state(self):
        from orchestrator.app.graph_builder import parse_k8s_and_kafka_manifests

        state = {
            "directory_path": "",
            "raw_files": [],
            "extracted_nodes": [],
            "extraction_errors": [],
            "validation_retries": 0,
            "commit_status": "",
        }
        result = parse_k8s_and_kafka_manifests(state)
        assert result["extracted_nodes"] == []


class TestMalformedYamlLogging:
    def test_malformed_yaml_logs_warning(self, caplog):
        import logging

        with caplog.at_level(logging.WARNING, logger="orchestrator.app.manifest_parser"):
            result = parse_k8s_manifests("{{invalid yaml:::")
        assert result == []
        assert any("Failed to parse YAML" in msg for msg in caplog.messages)

    def test_valid_yaml_does_not_log_warning(self, caplog):
        import logging

        content = textwrap.dedent("""\
            apiVersion: apps/v1
            kind: Deployment
            metadata:
              name: test-svc
        """)
        with caplog.at_level(logging.WARNING, logger="orchestrator.app.manifest_parser"):
            parse_k8s_manifests(content)
        assert not any("Failed to parse YAML" in msg for msg in caplog.messages)
