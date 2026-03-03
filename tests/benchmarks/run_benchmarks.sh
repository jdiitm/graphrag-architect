#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
RESULTS_DIR="${REPO_ROOT}/tests/benchmarks/results"
mkdir -p "$RESULTS_DIR"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

echo "============================================"
echo "  GraphRAG Benchmark Suite"
echo "  $(date)"
echo "============================================"

echo ""
echo "--- Go Ingestion Benchmarks ---"
if command -v go &>/dev/null; then
  cd "${REPO_ROOT}/tests/benchmarks/go"
  go test -bench=. -benchmem -count=3 -timeout=120s 2>&1 | tee "${RESULTS_DIR}/go_bench_${TIMESTAMP}.txt"
else
  echo "SKIP: go not found in PATH"
fi

echo ""
echo "--- Python Query Engine Profiling ---"
cd "$REPO_ROOT"
if [ -f ".venv/bin/activate" ]; then
  source .venv/bin/activate
fi
python tests/benchmarks/python/profile_query.py --iterations 100 --profile 2>&1 | tee "${RESULTS_DIR}/python_bench_${TIMESTAMP}.txt"

echo ""
echo "============================================"
echo "  Benchmark results saved to: ${RESULTS_DIR}"
echo "============================================"
