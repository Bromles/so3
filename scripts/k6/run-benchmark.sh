#!/usr/bin/env bash
# run-benchmark.sh – run the so3 S3 benchmark N times and aggregate statistics
#
# Each k6 run writes a JSON summary export; this script collects the per-run
# median, p90, p95, std-dev estimate for PUT / GET / HEAD and computes across
# all runs: mean, variance, std-dev, min, max, and coefficient of variation.
#
# Dependencies: k6, jq, awk (all standard on macOS/Linux).
#
# Usage
# ─────
#   bash scripts/k6/run-benchmark.sh [--runs N] [--outdir DIR] [k6 extra args…]
#
# Examples
#   bash scripts/k6/run-benchmark.sh --runs 30
#   bash scripts/k6/run-benchmark.sh --runs 50 --outdir /tmp/bench \
#       SO3_ADDR=http://10.0.0.1:3000 VUS=20 DURATION=60s

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCHMARK_SCRIPT="$SCRIPT_DIR/s3-benchmark.js"

# ── argument parsing ──────────────────────────────────────────────────────────

RUNS=30
OUT_DIR=""
K6_EXTRA_ARGS=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --runs)   RUNS="$2";    shift 2 ;;
    --outdir) OUT_DIR="$2"; shift 2 ;;
    *)        K6_EXTRA_ARGS+=("$1"); shift ;;
  esac
done

if [[ -z "$OUT_DIR" ]]; then
  OUT_DIR="$(mktemp -d "/tmp/so3-bench.XXXXXX")"
fi
mkdir -p "$OUT_DIR"

echo "so3 S3 benchmark – ${RUNS} runs → ${OUT_DIR}"
echo ""

# ── run k6 N times ────────────────────────────────────────────────────────────

for i in $(seq 1 "$RUNS"); do
  export_file="${OUT_DIR}/run_$(printf '%03d' "$i").json"
  printf "  run %3d/%d … " "$i" "$RUNS"
  k6 run \
    --quiet \
    --no-color \
    --summary-export="$export_file" \
    ${K6_EXTRA_ARGS[@]+"${K6_EXTRA_ARGS[@]}"} \
    "$BENCHMARK_SCRIPT" >/dev/null 2>/dev/null
  echo "done"
done

echo ""

# ── aggregate statistics across runs ─────────────────────────────────────────
# For each operation and stat we extract the per-run value with jq, then
# compute: mean, variance, std-dev, CV (coefficient of variation), min, max
# using a single awk pass.

aggregate() {
  local label="$1"
  local metric_key="$2"
  local stat="$3"

  # Extract the stat from every run JSON, one value per line.
  local values
  values=$(jq -r --arg m "$metric_key" --arg s "$stat" \
    '.metrics[$m][$s] // empty' \
    "${OUT_DIR}"/run_*.json 2>/dev/null)

  if [[ -z "$values" ]]; then
    printf "  %-12s %-6s : no data\n" "$label" "$stat"
    return
  fi

  printf '%s' "$values" | awk -v label="$label" -v stat="$stat" '
  {
    val = $1 + 0
    n++
    sum  += val
    sum2 += val * val
    if (n == 1 || val < mn) mn = val
    if (n == 1 || val > mx) mx = val
  }
  END {
    mean = sum / n
    var  = sum2 / n - mean * mean
    if (var < 0) var = 0        # floating-point guard
    sd   = sqrt(var)
    cv   = (mean > 0) ? sd / mean * 100 : 0
    printf "  %-12s %-6s :  n=%-3d  mean=%8.2f  σ=%8.2f  var=%10.2f  CV=%5.1f%%  min=%8.2f  max=%8.2f  ms\n",
           label, stat, n, mean, sd, var, cv, mn, mx
  }
  '
}

print_header() {
  echo "  $1"
  echo "  $(printf '%0.s─' {1..90})"
}

print_header "PUT"
aggregate "PUT" "s3_put_ms" "med"
aggregate "PUT" "s3_put_ms" "avg"
aggregate "PUT" "s3_put_ms" "p(90)"
aggregate "PUT" "s3_put_ms" "p(95)"
echo ""

print_header "GET"
aggregate "GET" "s3_get_ms" "med"
aggregate "GET" "s3_get_ms" "avg"
aggregate "GET" "s3_get_ms" "p(90)"
aggregate "GET" "s3_get_ms" "p(95)"
echo ""

print_header "HEAD"
aggregate "HEAD" "s3_head_ms" "med"
aggregate "HEAD" "s3_head_ms" "avg"
aggregate "HEAD" "s3_head_ms" "p(90)"
aggregate "HEAD" "s3_head_ms" "p(95)"
echo ""

print_header "DELETE"
aggregate "DELETE" "s3_delete_ms" "med"
aggregate "DELETE" "s3_delete_ms" "avg"
aggregate "DELETE" "s3_delete_ms" "p(90)"
aggregate "DELETE" "s3_delete_ms" "p(95)"
echo ""

echo "  Raw JSON exports: ${OUT_DIR}/run_*.json"
