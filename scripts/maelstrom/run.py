#!/usr/bin/env python3
"""Run Maelstrom linearizability test against SO3.

Cross-platform replacement for run-lin-kv.sh and run-lin-kv.ps1.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parents[1]
DEFAULT_JAR = REPO_ROOT / ".tools" / "maelstrom" / "maelstrom" / "lib" / "maelstrom.jar"


def resolve_maelstrom_command(explicit_bin: str, explicit_jar: str) -> list[str]:
    if explicit_bin:
        return [explicit_bin, "test"]
    if explicit_jar:
        return ["java", "-jar", explicit_jar, "test"]
    found = shutil.which("maelstrom")
    if found:
        return [found, "test"]
    env_jar = os.environ.get("MAELSTROM_JAR", "")
    if env_jar:
        return ["java", "-jar", env_jar, "test"]
    if DEFAULT_JAR.exists():
        return ["java", "-jar", str(DEFAULT_JAR), "test"]
    print(
        "error: Maelstrom not found. Run install.py or set MAELSTROM_JAR.",
        file=sys.stderr,
    )
    sys.exit(1)


def resolve_adapter_binary(explicit_path: str) -> Path:
    if explicit_path:
        p = Path(explicit_path)
        return p if p.is_absolute() else REPO_ROOT / p

    try:
        result = subprocess.run(
            ["cargo", "metadata", "--no-deps", "--format-version", "1"],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            check=True,
            timeout=30,
        )
        target_dir = Path(json.loads(result.stdout)["target_directory"])
    except Exception:
        target_dir = REPO_ROOT / "target"

    name = "so3-maelstrom.exe" if sys.platform == "win32" else "so3-maelstrom"
    return target_dir / "release" / name


def check_symlink_support() -> bool:
    tmp = Path(tempfile.mkdtemp(prefix="so3-symlink-check-"))
    try:
        target = tmp / "target.txt"
        link = tmp / "link.txt"
        target.write_text("ok")
        link.symlink_to(target)
    except (OSError, NotImplementedError):
        return False
    else:
        return True
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run Maelstrom lin-kv test against SO3.",
        allow_abbrev=False,
    )
    parser.add_argument("--workload", default="lin-kv")
    parser.add_argument("--node-count", type=int, default=1)
    parser.add_argument("--time-limit", type=int, default=20)
    parser.add_argument("--rate", type=int, default=10)
    parser.add_argument("--concurrency", default="2n")
    parser.add_argument("--nemesis", default="")
    parser.add_argument("--nemesis-interval", default="")
    parser.add_argument("--latency", default="")
    parser.add_argument("--latency-dist", default="")
    parser.add_argument("--availability", default="")
    parser.add_argument("--consistency-models", default="")
    parser.add_argument("--log-stderr", action="store_true")
    parser.add_argument("--log-net-send", action="store_true")
    parser.add_argument("--log-net-recv", action="store_true")
    parser.add_argument("--no-build", action="store_true")
    parser.add_argument("--maelstrom-bin", default="", metavar="PATH")
    parser.add_argument("--maelstrom-jar", default="", metavar="PATH")
    parser.add_argument(
        "--binary-path", default="", metavar="PATH", help="path to so3-maelstrom binary"
    )
    parser.add_argument(
        "--data-dir",
        default="",
        metavar="DIR",
        help="SO3 data directory (temp dir if omitted)",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(list(argv) if argv is not None else sys.argv[1:])

    if sys.platform == "win32" and not check_symlink_support():
        print(
            "error: Maelstrom requires symlink creation on Windows. "
            "Run from an elevated shell or enable Developer Mode.",
            file=sys.stderr,
        )
        return 1

    if not args.no_build:
        result = subprocess.run(
            ["cargo", "build", "--release", "-p", "so3-maelstrom"],
            cwd=REPO_ROOT,
            check=False,
        )
        if result.returncode != 0:
            return result.returncode

    binary = resolve_adapter_binary(args.binary_path)
    if not binary.exists():
        print(f"error: adapter binary not found: {binary}", file=sys.stderr)
        return 1

    data_dir = args.data_dir or tempfile.mkdtemp(prefix="so3-maelstrom-")
    Path(data_dir).mkdir(parents=True, exist_ok=True)

    env = os.environ.copy()
    env["SO3_MAELSTROM_DATA_DIR"] = data_dir

    command = [
        *resolve_maelstrom_command(args.maelstrom_bin, args.maelstrom_jar),
        "--workload",
        args.workload,
        "--bin",
        str(binary),
        "--node-count",
        str(args.node_count),
        "--time-limit",
        str(args.time_limit),
        "--rate",
        str(args.rate),
        "--concurrency",
        args.concurrency,
        "--no-ssh",
    ]

    if args.log_stderr:
        command.append("--log-stderr")
    if args.log_net_send:
        command.append("--log-net-send")
    if args.log_net_recv:
        command.append("--log-net-recv")
    if args.nemesis:
        command += ["--nemesis", args.nemesis]
    if args.nemesis_interval:
        command += ["--nemesis-interval", args.nemesis_interval]
    if args.latency:
        command += ["--latency", args.latency]
    if args.latency_dist:
        command += ["--latency-dist", args.latency_dist]
    if args.availability:
        command += ["--availability", args.availability]
    if args.consistency_models:
        command += ["--consistency-models", args.consistency_models]

    print(f"Running: {' '.join(command)}")
    print(f"SO3_MAELSTROM_DATA_DIR={data_dir}")

    return subprocess.run(command, cwd=REPO_ROOT, env=env, check=False).returncode


if __name__ == "__main__":
    raise SystemExit(main())
