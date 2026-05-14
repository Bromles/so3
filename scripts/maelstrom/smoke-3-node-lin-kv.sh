#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

export NODE_COUNT="3"
export TIME_LIMIT="10"
export RATE="10"
export CONCURRENCY="2n"
export LOG_STDERR="1"

exec bash scripts/maelstrom/run-lin-kv.sh
