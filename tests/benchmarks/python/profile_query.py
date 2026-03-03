#!/usr/bin/env python3
"""Profile the GraphRAG query engine hot paths.

Usage:
    python tests/benchmarks/python/profile_query.py [--iterations N] [--output FILE]

Profiles entity extraction, Cypher generation, and result formatting
to identify bottlenecks in the query pipeline.
"""
from __future__ import annotations

import argparse
import cProfile
import io
import pstats
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT / "orchestrator"))

from app.manifest_parser import ManifestParser  # noqa: E402
from app.service_extractor import ServiceExtractor  # noqa: E402


def _build_sample_manifests(count: int) -> list[dict]:
    manifests = []
    for i in range(count):
        manifests.append({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {
                "name": f"service-{i}",
                "namespace": "default",
                "labels": {"app": f"service-{i}", "team": f"team-{i % 5}"},
            },
            "spec": {
                "replicas": 2,
                "selector": {"matchLabels": {"app": f"service-{i}"}},
                "template": {
                    "metadata": {"labels": {"app": f"service-{i}"}},
                    "spec": {
                        "containers": [{
                            "name": f"service-{i}",
                            "image": f"registry/service-{i}:latest",
                            "ports": [{"containerPort": 8080}],
                            "env": [
                                {"name": "DATABASE_URL", "value": f"postgres://db-{i}:5432/app"},
                                {"name": "KAFKA_BROKER", "value": "kafka:9092"},
                            ],
                        }],
                    },
                },
            },
        })
    return manifests


def benchmark_manifest_parsing(iterations: int) -> dict:
    parser = ManifestParser()
    manifests = _build_sample_manifests(50)
    timings = []

    for _ in range(iterations):
        start = time.perf_counter()
        for manifest in manifests:
            parser.parse(manifest)
        elapsed = time.perf_counter() - start
        timings.append(elapsed)

    return {
        "operation": "manifest_parsing",
        "manifests_per_batch": len(manifests),
        "iterations": iterations,
        "mean_ms": sum(timings) / len(timings) * 1000,
        "min_ms": min(timings) * 1000,
        "max_ms": max(timings) * 1000,
        "p50_ms": sorted(timings)[len(timings) // 2] * 1000,
    }


def benchmark_service_extraction(iterations: int) -> dict:
    extractor = ServiceExtractor()
    raw_text = "\n".join([
        f"Service auth-service-{i} calls user-service via HTTP on port 8080. "
        f"It produces events to topic user-events-{i} on Kafka. "
        f"Deployed in namespace production with 3 replicas."
        for i in range(20)
    ])
    timings = []

    for _ in range(iterations):
        start = time.perf_counter()
        extractor.extract(raw_text)
        elapsed = time.perf_counter() - start
        timings.append(elapsed)

    return {
        "operation": "service_extraction",
        "text_length": len(raw_text),
        "iterations": iterations,
        "mean_ms": sum(timings) / len(timings) * 1000,
        "min_ms": min(timings) * 1000,
        "max_ms": max(timings) * 1000,
        "p50_ms": sorted(timings)[len(timings) // 2] * 1000,
    }


def run_cprofile(output_path: str | None) -> None:
    profiler = cProfile.Profile()
    profiler.enable()

    benchmark_manifest_parsing(100)
    benchmark_service_extraction(100)

    profiler.disable()

    stream = io.StringIO()
    stats = pstats.Stats(profiler, stream=stream)
    stats.sort_stats("cumulative")
    stats.print_stats(30)

    report = stream.getvalue()
    print(report)

    if output_path:
        profiler.dump_stats(output_path)
        print(f"\nProfile data saved to: {output_path}")


def main() -> None:
    arg_parser = argparse.ArgumentParser(description="Profile GraphRAG query engine")
    arg_parser.add_argument("--iterations", type=int, default=50)
    arg_parser.add_argument("--output", type=str, default=None)
    args = arg_parser.parse_args()

    print("=" * 60)
    print("GraphRAG Query Engine Profiler")
    print("=" * 60)

    results = [
        benchmark_manifest_parsing(args.iterations),
        benchmark_service_extraction(args.iterations),
    ]

    for result in results:
        print(f"\n--- {result['operation']} ---")
        for key, value in result.items():
            if key == "operation":
                continue
            if isinstance(value, float):
                print(f"  {key}: {value:.3f}")
            else:
                print(f"  {key}: {value}")

    print("\n" + "=" * 60)
    print("cProfile output (top 30 functions by cumulative time):")
    print("=" * 60)
    run_cprofile(args.output)


if __name__ == "__main__":
    main()
