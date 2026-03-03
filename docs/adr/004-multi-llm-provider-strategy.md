# ADR-004: Multi-LLM Provider Strategy

## Status

Accepted

## Context

Different stages of the GraphRAG pipeline have different LLM requirements. Entity extraction
benefits from strong instruction-following. Massive context synthesis (e.g., full K8s manifest
analysis) requires large context windows. Query answering needs fast, accurate responses.
Depending on a single LLM provider creates vendor lock-in and availability risk.

Evaluated options: Claude-only, Gemini-only, multi-provider with routing.

## Decision

Use a multi-LLM provider strategy with Claude and Gemini, routed by task type.

- Claude: complex reasoning, entity/relationship extraction, agentic orchestration
- Gemini: multimodal context ingestion, large context window synthesis
- LLM provider is configurable per-stage via environment variables
- Circuit breaker pattern for provider failover

## Consequences

- Must maintain abstraction layer over multiple LLM APIs
- Different prompt engineering per provider
- Cost optimization possible by routing simpler tasks to cheaper models
- Circuit breaker adds resilience but increases code complexity
- API key management for multiple providers (Vault integration)
