#!/usr/bin/env bash
# run-benchmark.sh – run the so3 S3 benchmark N times and aggregate statistics
#
# Each k6 run writes a JSON summary export; this script collects per-run
# throughput, error rate, latency, and so3 process CPU/RSS samples, then prints
# cross-run/resource aggregates suitable for docs/results.md.
#
# Dependencies: k6, jq, awk (all standard on macOS/Linux).
#
# Usage
# ─────
#   bash scripts/k6/run-benchmark.sh [--runs N] [--outdir DIR] [k6 extra args...]
#
# Examples
#   bash scripts/k6/run-benchmark.sh --runs 30
#   bash scripts/k6/run-benchmark.sh --runs 50 --outdir /tmp/bench \
#       SO3_ADDR=http://10.0.0.1:3000 VUS=20 DURATION=60s
#
# Resource sampling
#   The script auto-detects the so3 PID from SO3_ADDR's TCP port. Set SO3_PID
#   explicitly when the endpoint cannot be mapped to a local listener.
#
# Release guard
#   By default, the detected process must be target/release/so3. Set
#   SO3_REQUIRE_RELEASE=0 only for local script debugging, never for reported
#   performance numbers.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCHMARK_SCRIPT="$SCRIPT_DIR/s3-benchmark.js"

# ── argument parsing ──────────────────────────────────────────────────────────

RUNS=30
OUT_DIR=""
K6_EXTRA_ARGS=()
SO3_ADDR="${SO3_ADDR:-http://127.0.0.1:3000}"
SO3_PID="${SO3_PID:-}"
SO3_REQUIRE_RELEASE="${SO3_REQUIRE_RELEASE:-1}"
RESOURCE_SAMPLE_INTERVAL_SECS="${RESOURCE_SAMPLE_INTERVAL_SECS:-1}"
RESOURCE_FILE=""
SAMPLER_PID=""

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
RESOURCE_FILE="${OUT_DIR}/resources.tsv"

echo "so3 S3 benchmark - ${RUNS} runs -> ${OUT_DIR}"
echo ""

# ── resource sampling ────────────────────────────────────────────────────────

detect_port_from_addr() {
  local addr_no_scheme hostport scheme

  scheme="${SO3_ADDR%%://*}"
  addr_no_scheme="${SO3_ADDR#*://}"
  hostport="${addr_no_scheme%%/*}"

  if [[ "$hostport" == *:* ]]; then
    printf '%s\n' "${hostport##*:}"
  elif [[ "$scheme" == "https" ]]; then
    printf '443\n'
  else
    printf '80\n'
  fi
}

detect_so3_pid() {
  local port

  if [[ -n "$SO3_PID" ]]; then
    printf '%s\n' "$SO3_PID"
    return
  fi

  port="$(detect_port_from_addr)"
  if command -v lsof >/dev/null 2>&1; then
    lsof -tiTCP:"$port" -sTCP:LISTEN 2>/dev/null | head -n 1 || true
  fi
}

assert_release_server() {
  local pid="$1"
  local command_line

  if [[ "$SO3_REQUIRE_RELEASE" != "1" ]]; then
    return
  fi

  command_line="$(ps -p "$pid" -o command= 2>/dev/null || true)"
  if [[ -z "$command_line" ]]; then
    echo "error: cannot inspect so3 process ${pid}; set SO3_PID or SO3_REQUIRE_RELEASE=0" >&2
    exit 1
  fi

  if [[ "$command_line" != *"target/release/so3"* ]]; then
    echo "error: refusing to benchmark non-release so3 process ${pid}" >&2
    echo "       command: ${command_line}" >&2
    echo "       start target/release/so3, or set SO3_REQUIRE_RELEASE=0 for script debugging only" >&2
    exit 1
  fi
}

start_resource_sampler() {
  local pid="$1"

  if [[ -z "$pid" ]]; then
    echo "warning: so3 PID was not detected; CPU/RSS sampling disabled" >&2
    return
  fi

  assert_release_server "$pid"
  echo "sampling so3 resources: pid=${pid}, interval=${RESOURCE_SAMPLE_INTERVAL_SECS}s -> ${RESOURCE_FILE}"
  : > "$RESOURCE_FILE"

  (
    while kill -0 "$pid" 2>/dev/null; do
      printf '%s ' "$(date +%s)"
      ps -o %cpu= -o rss= -p "$pid" | awk 'NF >= 2 {print $1, $2}'
      sleep "$RESOURCE_SAMPLE_INTERVAL_SECS"
    done
  ) >> "$RESOURCE_FILE" &
  SAMPLER_PID="$!"
}

stop_resource_sampler() {
  if [[ -n "$SAMPLER_PID" ]]; then
    kill "$SAMPLER_PID" 2>/dev/null || true
    wait "$SAMPLER_PID" 2>/dev/null || true
    SAMPLER_PID=""
  fi
}

cleanup() {
  stop_resource_sampler
}

trap cleanup EXIT INT TERM

SO3_SAMPLED_PID="$(detect_so3_pid)"
start_resource_sampler "$SO3_SAMPLED_PID"
echo ""

# ── run k6 N times ────────────────────────────────────────────────────────────

for i in $(seq 1 "$RUNS"); do
  export_file="${OUT_DIR}/run_$(printf '%03d' "$i").json"
  printf "  run %3d/%d ... " "$i" "$RUNS"
  k6 run \
    --quiet \
    --no-color \
    --summary-export="$export_file" \
    ${K6_EXTRA_ARGS[@]+"${K6_EXTRA_ARGS[@]}"} \
    "$BENCHMARK_SCRIPT" >/dev/null 2>/dev/null
  echo "done"
done

stop_resource_sampler
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
    '.metrics[$m][$s] // (if $s == "rate" then .metrics[$m].value else empty end) // empty' \
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

aggregate_metric() {
  local label="$1"
  local metric_key="$2"
  local stat="$3"
  local unit="$4"

  local values
  values=$(jq -r --arg m "$metric_key" --arg s "$stat" \
    '.metrics[$m][$s] // empty' \
    "${OUT_DIR}"/run_*.json 2>/dev/null)

  if [[ -z "$values" ]]; then
    printf "  %-16s %-8s : no data\n" "$label" "$stat"
    return
  fi

  printf '%s' "$values" | awk -v label="$label" -v stat="$stat" -v unit="$unit" '
  {
    val = $1 + 0
    n++
    sum += val
    sum2 += val * val
    if (n == 1 || val < mn) mn = val
    if (n == 1 || val > mx) mx = val
  }
  END {
    mean = sum / n
    var = sum2 / n - mean * mean
    if (var < 0) var = 0
    sd = sqrt(var)
    cv = (mean > 0) ? sd / mean * 100 : 0
    printf "  %-16s %-8s :  n=%-3d  mean=%10.4f  σ=%10.4f  var=%12.4f  CV=%5.1f%%  min=%10.4f  max=%10.4f  %s\n",
           label, stat, n, mean, sd, var, cv, mn, mx, unit
  }
  '
}

aggregate_resources() {
  if [[ ! -s "$RESOURCE_FILE" ]]; then
    echo "  no CPU/RSS samples collected"
    return
  fi

  awk '
  NF >= 3 {
    cpu = $2 + 0
    rss_kib = $3 + 0
    rss_mib = rss_kib / 1024
    n++
    cpu_sum += cpu
    cpu_sum2 += cpu * cpu
    rss_sum += rss_mib
    rss_sum2 += rss_mib * rss_mib
    if (n == 1 || cpu < cpu_min) cpu_min = cpu
    if (n == 1 || cpu > cpu_max) cpu_max = cpu
    if (n == 1 || rss_mib < rss_min) rss_min = rss_mib
    if (n == 1 || rss_mib > rss_max) rss_max = rss_mib
  }
  END {
    if (n == 0) {
      print "  no CPU/RSS samples collected"
      exit
    }
    cpu_mean = cpu_sum / n
    cpu_var = cpu_sum2 / n - cpu_mean * cpu_mean
    if (cpu_var < 0) cpu_var = 0
    cpu_sd = sqrt(cpu_var)

    rss_mean = rss_sum / n
    rss_var = rss_sum2 / n - rss_mean * rss_mean
    if (rss_var < 0) rss_var = 0
    rss_sd = sqrt(rss_var)

    printf "  CPU %%        :  n=%-4d mean=%8.2f  σ=%8.2f  min=%8.2f  max=%8.2f\n",
           n, cpu_mean, cpu_sd, cpu_min, cpu_max
    printf "  RSS MiB      :  n=%-4d mean=%8.2f  σ=%8.2f  min=%8.2f  max=%8.2f\n",
           n, rss_mean, rss_sd, rss_min, rss_max
  }
  ' "$RESOURCE_FILE"
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

print_header "THROUGHPUT"
aggregate_metric "S3 requests" "http_reqs" "rate" "req/s"
aggregate_metric "S3 requests" "http_reqs" "count" "requests/run"
aggregate_metric "S3 errors" "s3_errors" "rate" "ratio"
echo ""

print_header "SO3 RESOURCES"
aggregate_resources
echo ""

echo "  Raw JSON exports: ${OUT_DIR}/run_*.json"
echo "  Resource samples: ${RESOURCE_FILE}"
