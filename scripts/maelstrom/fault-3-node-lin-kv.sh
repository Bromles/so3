#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

WORKLOAD="lin-kv" \
NODE_COUNT="3" \
TIME_LIMIT="${TIME_LIMIT:-30}" \
RATE="${RATE:-20}" \
CONCURRENCY="${CONCURRENCY:-2n}" \
NEMESIS="${NEMESIS:-partition}" \
NEMESIS_INTERVAL="${NEMESIS_INTERVAL:-5}" \
LOG_STDERR="${LOG_STDERR:-1}" \
bash "$script_dir/run-lin-kv.sh"
