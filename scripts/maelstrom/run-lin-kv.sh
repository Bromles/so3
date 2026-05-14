#!/usr/bin/env bash
set -euo pipefail

WORKLOAD="${WORKLOAD:-lin-kv}"
NODE_COUNT="${NODE_COUNT:-1}"
TIME_LIMIT="${TIME_LIMIT:-20}"
RATE="${RATE:-100}"
CONCURRENCY="${CONCURRENCY:-2n}"
LOG_STDERR="${LOG_STDERR:-1}"
LOG_NET_SEND="${LOG_NET_SEND:-0}"
LOG_NET_RECV="${LOG_NET_RECV:-0}"
NEMESIS="${NEMESIS:-}"
NEMESIS_INTERVAL="${NEMESIS_INTERVAL:-}"
LATENCY="${LATENCY:-}"
LATENCY_DIST="${LATENCY_DIST:-}"
AVAILABILITY="${AVAILABILITY:-}"
CONSISTENCY_MODELS="${CONSISTENCY_MODELS:-}"
NO_BUILD="${NO_BUILD:-0}"
MAELSTROM_BIN="${MAELSTROM_BIN:-}"
MAELSTROM_JAR="${MAELSTROM_JAR:-}"
BINARY_PATH="${BINARY_PATH:-}"
SO3_MAELSTROM_DATA_DIR="${SO3_MAELSTROM_DATA_DIR:-}"

repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
default_jar_path="$repo_root/.tools/maelstrom/maelstrom/lib/maelstrom.jar"
cd "$repo_root"

resolve_maelstrom_command() {
  if [[ -n "$MAELSTROM_BIN" ]]; then
    printf '%s\0' "$MAELSTROM_BIN" test
    return
  fi

  if [[ -n "$MAELSTROM_JAR" ]]; then
    printf '%s\0' java -jar "$MAELSTROM_JAR" test
    return
  fi

  if command -v maelstrom >/dev/null 2>&1; then
    printf '%s\0' "$(command -v maelstrom)" test
    return
  fi

  if [[ -n "${MAELSTROM_JAR:-}" ]]; then
    printf '%s\0' java -jar "${MAELSTROM_JAR}" test
    return
  fi

  if [[ -f "$default_jar_path" ]]; then
    printf '%s\0' java -jar "$default_jar_path" test
    return
  fi

  echo "Maelstrom executable not found. Set MAELSTROM_BIN or MAELSTROM_JAR." >&2
  exit 1
}

resolve_adapter_binary() {
  if [[ -n "$BINARY_PATH" ]]; then
    printf '%s\n' "$BINARY_PATH"
    return
  fi

  if [[ -x "target/debug/so3-maelstrom" ]]; then
    printf '%s\n' "$repo_root/target/debug/so3-maelstrom"
    return
  fi

  echo "$repo_root/target/debug/so3-maelstrom"
}

if [[ "$NO_BUILD" != "1" ]]; then
  cargo build -p so3-maelstrom
fi

adapter_binary="$(resolve_adapter_binary)"
if [[ ! -x "$adapter_binary" ]]; then
  echo "Maelstrom adapter binary not found or not executable at $adapter_binary" >&2
  exit 1
fi

if [[ -z "$SO3_MAELSTROM_DATA_DIR" ]]; then
  SO3_MAELSTROM_DATA_DIR="$(mktemp -d "${TMPDIR:-/tmp}/so3-maelstrom.XXXXXX")"
fi
export SO3_MAELSTROM_DATA_DIR

maelstrom_command=()
while IFS= read -r -d '' part; do
  maelstrom_command+=("$part")
done < <(resolve_maelstrom_command)
command=(
  "${maelstrom_command[@]}"
  --workload "$WORKLOAD"
  --bin "$adapter_binary"
  --node-count "$NODE_COUNT"
  --time-limit "$TIME_LIMIT"
  --rate "$RATE"
  --concurrency "$CONCURRENCY"
  --no-ssh
)

if [[ "$LOG_STDERR" == "1" ]]; then
  command+=(--log-stderr)
fi

if [[ "$LOG_NET_SEND" == "1" ]]; then
  command+=(--log-net-send)
fi

if [[ "$LOG_NET_RECV" == "1" ]]; then
  command+=(--log-net-recv)
fi

if [[ -n "$NEMESIS" ]]; then
  command+=(--nemesis "$NEMESIS")
fi

if [[ -n "$NEMESIS_INTERVAL" ]]; then
  command+=(--nemesis-interval "$NEMESIS_INTERVAL")
fi

if [[ -n "$LATENCY" ]]; then
  command+=(--latency "$LATENCY")
fi

if [[ -n "$LATENCY_DIST" ]]; then
  command+=(--latency-dist "$LATENCY_DIST")
fi

if [[ -n "$AVAILABILITY" ]]; then
  command+=(--availability "$AVAILABILITY")
fi

if [[ -n "$CONSISTENCY_MODELS" ]]; then
  command+=(--consistency-models "$CONSISTENCY_MODELS")
fi

printf 'Running:'
for part in "${command[@]}"; do
  printf ' %q' "$part"
done
printf '\n'
printf 'SO3_MAELSTROM_DATA_DIR=%q\n' "$SO3_MAELSTROM_DATA_DIR"

"${command[@]}"
