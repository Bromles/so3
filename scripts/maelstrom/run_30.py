#!/usr/bin/env python3
"""Run Maelstrom lin-kv test N times and aggregate pass/fail statistics.

Builds once (or uses --no-build to skip), then runs the same Maelstrom
command N times, capturing success/failure per run.  Writes:
  <outdir>/run-NNN/result.json   — per-run outcome
  <outdir>/aggregate.json        — totals and verdict
  <outdir>/report.md             — markdown summary

Default: lin-kv workload, 3 nodes, no nemesis (read-after-write consistency test).
Use --nemesis partition to test fault behavior (weaker consistency during partitions).
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path
from typing import TYPE_CHECKING

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parents[1]
DEFAULT_RESULTS_DIR = REPO_ROOT / "results" / "maelstrom"

sys.path.insert(0, str(SCRIPT_DIR))

from run import main as run_maelstrom  # noqa: E402

if TYPE_CHECKING:
    from collections.abc import Sequence


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run Maelstrom lin-kv test N times and aggregate results.",
        allow_abbrev=False,
    )
    parser.add_argument("--runs", type=int, default=30)
    parser.add_argument(
        "--allow-low-runs",
        action="store_true",
        help="allow --runs below 30 for local debugging",
    )
    parser.add_argument("--outdir", type=Path, default=None)
    parser.add_argument("--node-count", type=int, default=3)
    parser.add_argument("--time-limit", type=int, default=30)
    parser.add_argument("--rate", type=int, default=100)
    parser.add_argument("--concurrency", default="2n")
    parser.add_argument("--nemesis", default="")
    parser.add_argument("--nemesis-interval", default="")
    parser.add_argument("--no-build", action="store_true")
    parser.add_argument("--maelstrom-bin", default="", metavar="PATH")
    parser.add_argument("--maelstrom-jar", default="", metavar="PATH")
    parser.add_argument("--binary-path", default="", metavar="PATH")
    return parser.parse_args(argv)


def run_once(args: argparse.Namespace, run_index: int, run_dir: Path) -> dict:
    run_dir.mkdir(parents=True, exist_ok=True)
    argv = [
        "--node-count",
        str(args.node_count),
        "--time-limit",
        str(args.time_limit),
        "--rate",
        str(args.rate),
        "--concurrency",
        args.concurrency,
        "--data-dir",
        str(run_dir / "maelstrom-data"),
        "--no-build",
    ]
    if args.nemesis:
        argv += ["--nemesis", args.nemesis]
    if args.nemesis_interval:
        argv += ["--nemesis-interval", args.nemesis_interval]
    if args.maelstrom_bin:
        argv += ["--maelstrom-bin", args.maelstrom_bin]
    if args.maelstrom_jar:
        argv += ["--maelstrom-jar", args.maelstrom_jar]
    if args.binary_path:
        argv += ["--binary-path", args.binary_path]

    start = time.monotonic()
    returncode = run_maelstrom(argv)
    elapsed = time.monotonic() - start
    passed = returncode == 0

    result = {
        "run_index": run_index,
        "returncode": returncode,
        "passed": passed,
        "elapsed_secs": round(elapsed, 3),
    }
    (run_dir / "result.json").write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return result


def write_aggregate(outdir: Path, run_results: list[dict]) -> dict:
    total = len(run_results)
    passed = sum(1 for r in run_results if r["passed"])
    failed = total - passed
    pass_rate = passed / total if total else 0.0
    verdict = "no_runs" if total == 0 else ("passed" if failed == 0 else "failed")

    aggregate = {
        "schema_version": 1,
        "runs_total": total,
        "runs_passed": passed,
        "runs_failed": failed,
        "pass_rate": pass_rate,
        "verdict": verdict,
        "run_results": run_results,
    }
    (outdir / "aggregate.json").write_text(
        json.dumps(aggregate, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return aggregate


def write_report(outdir: Path, aggregate: dict) -> Path:
    total = aggregate["runs_total"]
    passed = aggregate["runs_passed"]
    failed = aggregate["runs_failed"]
    pass_rate = aggregate["pass_rate"] * 100.0
    verdict = aggregate["verdict"].upper()

    lines = [
        "# Maelstrom lin-kv Run Report",
        "",
        f"**Verdict: {verdict}**",
        "",
        "| runs | passed | failed | pass rate |",
        "| ---: | ---: | ---: | ---: |",
        f"| {total} | {passed} | {failed} | {pass_rate:.1f}% |",
        "",
        "## Per-run results",
        "",
        "| run | passed | elapsed (s) | returncode |",
        "| ---: | --- | ---: | ---: |",
    ]
    for r in aggregate["run_results"]:
        ok = "yes" if r["passed"] else "**no**"
        lines.append(
            f"| {r['run_index']:03d} | {ok} | {r['elapsed_secs']:.1f} | {r['returncode']} |"  # noqa: E501
        )

    report_path = outdir / "report.md"
    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return report_path


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(list(argv) if argv is not None else sys.argv[1:])

    if args.runs < 30 and not args.allow_low_runs:
        print(
            "error: --runs must be >= 30; use --allow-low-runs for debugging",
            file=sys.stderr,
        )
        return 2

    timestamp = time.strftime("%Y%m%d-%H%M%S")
    outdir = args.outdir or DEFAULT_RESULTS_DIR / f"lin-kv-{timestamp}"
    outdir.mkdir(parents=True, exist_ok=True)
    print(f"results: {outdir}")
    print()

    if not args.no_build:
        result = subprocess.run(
            ["cargo", "build", "--release", "-p", "so3-maelstrom"],
            cwd=REPO_ROOT,
            check=False,
        )
        if result.returncode != 0:
            return result.returncode

    print(
        f"Maelstrom lin-kv x{args.runs} "
        f"({args.node_count} nodes, nemesis={args.nemesis or 'none'})"
    )
    print()

    run_results: list[dict] = []
    try:
        for run_index in range(1, args.runs + 1):
            run_dir = outdir / f"run-{run_index:03d}"
            print(f"run {run_index:03d}/{args.runs} ... ", end="", flush=True)
            result = run_once(args, run_index, run_dir)
            status = "pass" if result["passed"] else "FAIL"
            print(f"{status} ({result['elapsed_secs']:.1f}s)")
            run_results.append(result)
    except KeyboardInterrupt:
        print("interrupted", file=sys.stderr)
        return 130

    aggregate = write_aggregate(outdir, run_results)
    report_path = write_report(outdir, aggregate)

    print()
    print(f"aggregate: {outdir / 'aggregate.json'}")
    print(f"report:    {report_path}")
    print(
        f"verdict:   {aggregate['verdict']}  "
        f"({aggregate['runs_passed']}/{aggregate['runs_total']} passed)"
    )
    return 0 if aggregate["runs_failed"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
