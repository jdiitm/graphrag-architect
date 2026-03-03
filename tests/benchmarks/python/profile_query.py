#!/usr/bin/env python3
"""Query engine profiling script for GraphRAG orchestrator."""

import argparse
import cProfile
import io
import pstats
import statistics
import sys
import time


def simulate_vector_search(query: str, top_k: int = 10) -> list[dict]:
    results = []
    for i in range(top_k):
        results.append({
            "id": f"node-{i}",
            "score": 1.0 - (i * 0.05),
            "text": f"Result {i} for query: {query[:50]}",
            "metadata": {"source": f"doc-{i}", "chunk": i},
        })
    return results


def simulate_cypher_traversal(entity: str, depth: int = 3) -> list[dict]:
    nodes = []
    for d in range(depth):
        for i in range(2 ** d):
            nodes.append({
                "id": f"{entity}-d{d}-{i}",
                "type": "Service" if d == 0 else "Dependency",
                "depth": d,
                "properties": {"name": f"dep-{d}-{i}"},
            })
    return nodes


def simulate_reranking(results: list[dict], query: str) -> list[dict]:
    scored = []
    for r in results:
        boost = 0.1 if query.split()[0].lower() in r.get("text", "").lower() else 0.0
        scored.append({**r, "rerank_score": r.get("score", 0.5) + boost})
    return sorted(scored, key=lambda x: x["rerank_score"], reverse=True)


def simulate_query_pipeline(query: str, top_k: int = 10, depth: int = 3) -> dict:
    vector_results = simulate_vector_search(query, top_k)
    entity = query.split()[0] if query.split() else "unknown"
    graph_results = simulate_cypher_traversal(entity, depth)
    combined = vector_results + [
        {"id": n["id"], "score": 0.3, "text": str(n["properties"])}
        for n in graph_results[:5]
    ]
    reranked = simulate_reranking(combined, query)
    return {
        "query": query,
        "vector_count": len(vector_results),
        "graph_count": len(graph_results),
        "final_count": len(reranked),
        "top_score": reranked[0]["rerank_score"] if reranked else 0.0,
    }


def run_latency_benchmark(iterations: int = 100) -> dict:
    queries = [
        "What services depend on the auth service?",
        "Show me all Kafka topics consumed by ingestion workers",
        "Which pods are deployed in the graphrag namespace?",
        "If Neo4j fails, which services are affected?",
        "List all gRPC connections between microservices",
    ]
    latencies = []
    for _ in range(iterations):
        for q in queries:
            start = time.perf_counter()
            simulate_query_pipeline(q)
            elapsed = (time.perf_counter() - start) * 1000
            latencies.append(elapsed)

    return {
        "iterations": iterations * len(queries),
        "p50_ms": round(statistics.median(latencies), 3),
        "p95_ms": round(sorted(latencies)[int(len(latencies) * 0.95)], 3),
        "p99_ms": round(sorted(latencies)[int(len(latencies) * 0.99)], 3),
        "mean_ms": round(statistics.mean(latencies), 3),
        "stdev_ms": round(statistics.stdev(latencies), 3),
    }


def run_cprofile(iterations: int = 50) -> str:
    profiler = cProfile.Profile()
    profiler.enable()
    for _ in range(iterations):
        simulate_query_pipeline("What services depend on auth?")
    profiler.disable()

    stream = io.StringIO()
    stats = pstats.Stats(profiler, stream=stream)
    stats.sort_stats("cumulative")
    stats.print_stats(20)
    return stream.getvalue()


def main():
    parser = argparse.ArgumentParser(description="GraphRAG query engine profiler")
    parser.add_argument("--iterations", type=int, default=100)
    parser.add_argument("--profile", action="store_true")
    args = parser.parse_args()

    print("=" * 60)
    print("GraphRAG Query Engine Benchmark")
    print("=" * 60)

    print("\nRunning latency benchmark...")
    latency = run_latency_benchmark(args.iterations)
    print(f"  Iterations: {latency['iterations']}")
    print(f"  p50:  {latency['p50_ms']} ms")
    print(f"  p95:  {latency['p95_ms']} ms")
    print(f"  p99:  {latency['p99_ms']} ms")
    print(f"  mean: {latency['mean_ms']} ms")
    print(f"  stdev: {latency['stdev_ms']} ms")

    if args.profile:
        print("\ncProfile output (top 20 functions):")
        print(run_cprofile(args.iterations))

    return 0


if __name__ == "__main__":
    sys.exit(main())
