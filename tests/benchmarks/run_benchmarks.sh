#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
RESULTS_DIR="${REPO_ROOT}/tests/benchmarks/results"
TIMESTAMP="$(date +%Y%m%d-%H%M%S)"

mkdir -p "$RESULTS_DIR"

echo "============================================================"
echo "  GraphRAG Benchmark Suite — $TIMESTAMP"
echo "============================================================"

echo ""
echo "--- Go Benchmarks (parser + processor) ---"
echo ""

GO_RESULT="${RESULTS_DIR}/go-bench-${TIMESTAMP}.txt"
(
  cd "${REPO_ROOT}/tests/benchmarks/go"
  go test -bench=. -benchmem -count=3 -timeout 120s 2>&1 | tee "$GO_RESULT"
)

echo ""
echo "--- Go Module Benchmarks (parser + processor inside ingestion module) ---"
echo ""

GO_MODULE_RESULT="${RESULTS_DIR}/go-module-bench-${TIMESTAMP}.txt"
(
  cd "${REPO_ROOT}/workers/ingestion"
  go test -bench=. -benchmem -count=1 -run='^$' -timeout 120s ./internal/parser/ 2>&1 | tee "$GO_MODULE_RESULT"
)

echo ""
echo "Go benchmark results saved to: $GO_RESULT"

echo ""
echo "--- Python Profiling (manifest parser + service extractor) ---"
echo ""

PYTHON_RESULT="${RESULTS_DIR}/python-profile-${TIMESTAMP}.prof"
(
  cd "$REPO_ROOT"
  source .venv/bin/activate 2>/dev/null || true
  python tests/benchmarks/python/profile_query.py \
    --iterations 50 \
    --output "$PYTHON_RESULT"
)

echo ""
echo "============================================================"
echo "  Benchmark run complete"
echo "  Results in: $RESULTS_DIR"
echo "============================================================"
