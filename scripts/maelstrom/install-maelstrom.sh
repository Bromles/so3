#!/usr/bin/env bash
set -euo pipefail

version="${1:-0.2.4}"
script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "$script_dir/../.." && pwd)"
install_root="$repo_root/.tools/maelstrom"
archive_path="$install_root/maelstrom.tar.bz2"
extract_dir="$install_root/maelstrom"
jar_path="$extract_dir/lib/maelstrom.jar"
download_url="https://github.com/jepsen-io/maelstrom/releases/download/v${version}/maelstrom.tar.bz2"

mkdir -p "$install_root"
rm -rf "$extract_dir"
curl -fsSL "$download_url" -o "$archive_path"
tar -xjf "$archive_path" -C "$install_root"

printf 'Installed Maelstrom under %s\n' "$extract_dir"
printf 'Use jar path: %s\n' "$jar_path"
printf 'Set MAELSTROM_JAR to that jar or pass it explicitly.\n'
