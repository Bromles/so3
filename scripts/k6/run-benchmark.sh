#!/usr/bin/env bash
# run-benchmark.sh – run the so3 S3 benchmark N times and aggregate statistics
#
# Each k6 run starts a fresh release so3 process with a fresh data directory,
# writes a JSON summary export, then stops the process and removes the data
# directory. The script collects per-run throughput, error rate, latency, and
# so3 process CPU/RSS samples, then prints cross-run/resource aggregates
# suitable for docs/results.md.
#
# Dependencies: k6, jq, awk, curl (all standard on macOS/Linux).
#
# Usage
# ─────
#   bash scripts/k6/run-benchmark.sh [--runs N] [--outdir DIR] [k6 extra args...]
#
# Examples
#   bash scripts/k6/run-benchmark.sh --runs 30
#   bash scripts/k6/run-benchmark.sh --runs 50 --outdir /tmp/bench VUS=20 DURATION=60s
#
# Resource sampling
#   The script samples the managed so3 process for each run.
#
# Release guard
#   By default, the managed binary must be target/release/so3. Set SO3_BIN to
#   override the path and SO3_REQUIRE_RELEASE=0 only for local script debugging,
#   never for reported performance numbers.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCHMARK_SCRIPT="$SCRIPT_DIR/s3-benchmark.js"

# ── argument parsing ──────────────────────────────────────────────────────────

RUNS=30
OUT_DIR=""
K6_EXTRA_ARGS=()
SO3_OBJECT_ADDR="${SO3_OBJECT_ADDR:-127.0.0.1:3000}"
SO3_ADDR="${SO3_ADDR:-http://${SO3_OBJECT_ADDR}}"
SO3_RPC_ADDR="${SO3_RPC_ADDR:-127.0.0.1:4000}"
export SO3_ADDR
SO3_BIN="${SO3_BIN:-target/release/so3}"
SO3_REQUIRE_RELEASE="${SO3_REQUIRE_RELEASE:-1}"
SO3_START_TIMEOUT_SECS="${SO3_START_TIMEOUT_SECS:-15}"
SO3_STOP_TIMEOUT_SECS="${SO3_STOP_TIMEOUT_SECS:-10}"
SO3_KEEP_RUN_DIRS="${SO3_KEEP_RUN_DIRS:-0}"
RESOURCE_SAMPLE_INTERVAL_SECS="${RESOURCE_SAMPLE_INTERVAL_SECS:-1}"
RESOURCE_FILE=""
SAMPLER_PID=""
SO3_MANAGED_PID=""
RUN_DATA_DIR=""
RUN_LOG_FILE=""

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
: > "$RESOURCE_FILE"

echo "so3 S3 benchmark - ${RUNS} runs -> ${OUT_DIR}"
echo "managed so3: ${SO3_BIN}"
echo ""

# ── managed so3 lifecycle ────────────────────────────────────────────────────

assert_release_binary() {
  if [[ "$SO3_REQUIRE_RELEASE" != "1" ]]; then
    return
  fi

  case "$SO3_BIN" in
    */target/release/so3|target/release/so3|*/target/release/so3.exe|target/release/so3.exe) ;;
    *)
      echo "error: refusing to benchmark non-release so3 binary ${SO3_BIN}" >&2
      echo "       set SO3_BIN=target/release/so3, or set SO3_REQUIRE_RELEASE=0 for script debugging only" >&2
      exit 1
      ;;
  esac
}

wait_for_so3_ready() {
  local deadline="$((SECONDS + SO3_START_TIMEOUT_SECS))"

  while (( SECONDS < deadline )); do
    if ! kill -0 "$SO3_MANAGED_PID" 2>/dev/null; then
      echo "error: so3 exited before becoming ready; see ${RUN_LOG_FILE}" >&2
      return 1
    fi

    if curl -sS --max-time 1 "$SO3_ADDR/" >/dev/null 2>&1; then
      return 0
    fi

    sleep 0.2
  done

  echo "error: so3 did not become ready within ${SO3_START_TIMEOUT_SECS}s; see ${RUN_LOG_FILE}" >&2
  return 1
}

start_so3() {
  local run_index="$1"

  assert_release_binary
  RUN_DATA_DIR="$(mktemp -d "/tmp/so3-k6-run-${run_index}.XXXXXX")"
  RUN_LOG_FILE="${OUT_DIR}/so3_run_$(printf '%03d' "$run_index").log"

  SO3_OBJECT_ADDR="$SO3_OBJECT_ADDR" \
  SO3_RPC_ADDR="$SO3_RPC_ADDR" \
  SO3_DATA_DIR="$RUN_DATA_DIR" \
  "$SO3_BIN" >"$RUN_LOG_FILE" 2>&1 &
  SO3_MANAGED_PID="$!"

  wait_for_so3_ready
}

stop_so3() {
  if [[ -n "$SO3_MANAGED_PID" ]]; then
    local pid="$SO3_MANAGED_PID"

    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true

      local deadline="$((SECONDS + SO3_STOP_TIMEOUT_SECS))"
      while (( SECONDS < deadline )); do
        local state
        state="$(ps -p "$pid" -o stat= 2>/dev/null || true)"
        state="${state//[[:space:]]/}"
        if [[ -z "$state" || "$state" == Z* ]]; then
          break
        fi
        sleep 0.2
      done

      if kill -0 "$pid" 2>/dev/null; then
        local state
        state="$(ps -p "$pid" -o stat= 2>/dev/null || true)"
        state="${state//[[:space:]]/}"
        if [[ -n "$state" && "$state" != Z* ]]; then
          kill -9 "$pid" 2>/dev/null || true
        fi
      fi
    fi

    wait "$pid" 2>/dev/null || true
    SO3_MANAGED_PID=""
  fi
}

cleanup_run_data_dir() {
  if [[ -n "$RUN_DATA_DIR" && "$SO3_KEEP_RUN_DIRS" != "1" ]]; then
    case "$RUN_DATA_DIR" in
      /tmp/so3-k6-run-*) rm -rf "$RUN_DATA_DIR" ;;
      *) echo "warning: refusing to remove unexpected data dir ${RUN_DATA_DIR}" >&2 ;;
    esac
  fi

  RUN_DATA_DIR=""
}

# ── resource sampling ────────────────────────────────────────────────────────

start_resource_sampler() {
  local pid="$1"

  if [[ -z "$pid" ]]; then
    echo "warning: so3 PID was not detected; CPU/RSS sampling disabled" >&2
    return
  fi

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
  stop_so3
  cleanup_run_data_dir
}

trap cleanup EXIT INT TERM

# ── run k6 N times ────────────────────────────────────────────────────────────

for i in $(seq 1 "$RUNS"); do
  export_file="${OUT_DIR}/run_$(printf '%03d' "$i").json"
  printf "  run %3d/%d ... " "$i" "$RUNS"
  start_so3 "$i"
  start_resource_sampler "$SO3_MANAGED_PID"
  k6 run \
    --quiet \
    --no-color \
    --summary-export="$export_file" \
    ${K6_EXTRA_ARGS[@]+"${K6_EXTRA_ARGS[@]}"} \
    "$BENCHMARK_SCRIPT" >/dev/null 2>/dev/null
  stop_resource_sampler
  stop_so3
  cleanup_run_data_dir
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
