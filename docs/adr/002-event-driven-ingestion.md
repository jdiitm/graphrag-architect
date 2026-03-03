# ADR-002: Event-Driven Ingestion Architecture

## Status

Accepted

## Context

Document ingestion must handle variable load (bursts during CI/CD deployments) without
overwhelming the orchestrator or graph database. Synchronous HTTP ingestion creates
tight coupling and cascading failures under load.

Evaluated options: direct HTTP POST to orchestrator, RabbitMQ message queue, Apache Kafka
event streaming.

## Decision

Use Apache Kafka as the event bus for asynchronous document ingestion with Go-based
consumer workers.

Rationale:
- Kafka provides durable, partitioned, replayable event log
- Consumer groups enable horizontal scaling of ingestion workers
- Backpressure via partition assignment and max-inflight configuration
- Dead Letter Queue (DLQ) topic for poison message isolation
- KRaft mode eliminates ZooKeeper dependency

## Consequences

- Added infrastructure complexity (Kafka cluster management)
- Eventual consistency: documents are not immediately queryable after submission
- Requires idempotent processing (deduplication via Redis/memory store)
- Go workers must handle graceful shutdown with in-flight message draining
- Monitoring required: consumer lag, DLQ depth, processing latency
