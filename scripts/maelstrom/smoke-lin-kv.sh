#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

WORKLOAD="lin-kv" \
NODE_COUNT="1" \
TIME_LIMIT="10" \
RATE="20" \
CONCURRENCY="1n" \
LOG_STDERR="1" \
"$script_dir/run-lin-kv.sh"
