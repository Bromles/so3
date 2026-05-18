#!/usr/bin/env python3
"""Main CLI for SO3 research scenarios.

This first harness implementation supports a numeric k6 mixed-S3 scenario and
produces the result catalog required by docs/research-implementation-plan.md:
manifest, event timeline, raw k6 export, resource samples, per-run summary,
aggregate summary and markdown report.
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Sequence

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parents[1]
VERIFY_DIR = REPO_ROOT / "scripts" / "verify"
sys.path.insert(0, str(SCRIPT_DIR))
sys.path.insert(0, str(VERIFY_DIR))

import manifest  # noqa: E402
import metrics  # noqa: E402
import report  # noqa: E402
import stats  # noqa: E402
from cluster import ResourceSampler, So3Cluster, require_psutil  # noqa: E402
from correctness_driver import CorrectnessDriver, require_boto3  # noqa: E402
from topology import SUPPORTED_NODE_COUNTS, generate_topology  # noqa: E402
from verify_history import verify_history_file  # noqa: E402

DEFAULT_ACCESS_KEY = "so3testkey000000"
DEFAULT_SECRET_KEY = "so3testsecret0000000000000000000"
DEFAULT_RESULTS_DIR = REPO_ROOT / "results" / "research"
CORRECTNESS_SCENARIOS = {"e1-correctness"}
WORKLOAD_SCRIPTS = {
    "k6-mixed": REPO_ROOT / "scripts" / "k6" / "workloads" / "s3_mixed.js",
    "e3-degradation": REPO_ROOT / "scripts" / "k6" / "workloads" / "s3_degradation.js",
    "e4-hot-key": REPO_ROOT / "scripts" / "k6" / "workloads" / "s3_hot_key.js",
    "e5-leaderless": REPO_ROOT / "scripts" / "k6" / "workloads" / "s3_leaderless.js",
    "e6-recovery": REPO_ROOT / "scripts" / "k6" / "workloads" / "s3_recovery.js",
}


def parse_args(argv: Sequence[str]) -> tuple[argparse.Namespace, list[str]]:
    parser = argparse.ArgumentParser(
        description="Run reproducible SO3 research scenarios.",
        allow_abbrev=False,
    )
    parser.add_argument(
        "scenario",
        nargs="?",
        default="k6-mixed",
        choices=tuple([*CORRECTNESS_SCENARIOS, *WORKLOAD_SCRIPTS]),
        help="scenario to run",
    )
    parser.add_argument("--runs", type=int, default=30)
    parser.add_argument(
        "--allow-low-runs",
        action="store_true",
        help="allow --runs below 30 for local debugging",
    )
    parser.add_argument(
        "--node-count",
        type=int,
        default=3,
        choices=SUPPORTED_NODE_COUNTS,
        help="SO3 cluster size",
    )
    parser.add_argument("--outdir", type=Path, default=None)
    parser.add_argument("--so3-bin", type=Path, default=Path("target/release/so3"))
    parser.add_argument(
        "--k6-script",
        type=Path,
        default=None,
        help="override the workload script selected by scenario",
    )
    parser.add_argument("--object-base-port", type=int, default=3000)
    parser.add_argument("--rpc-base-port", type=int, default=4000)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--duration", default=os.environ.get("DURATION", "30s"))
    parser.add_argument("--vus", type=int, default=int(os.environ.get("VUS", "10")))
    parser.add_argument(
        "--object-size", type=int, default=int(os.environ.get("OBJECT_SIZE", "64"))
    )
    parser.add_argument("--bucket", default=os.environ.get("SO3_BUCKET", "bench"))
    parser.add_argument("--seed", type=int, default=int(time.time()))
    parser.add_argument("--start-timeout-secs", type=float, default=20.0)
    parser.add_argument("--stop-timeout-secs", type=float, default=10.0)
    parser.add_argument("--resource-sample-interval-secs", type=float, default=1.0)
    parser.add_argument("--correctness-ops", type=int, default=120)
    parser.add_argument("--correctness-concurrency", type=int, default=12)
    parser.add_argument("--keep-data-dirs", action="store_true")
    parser.add_argument(
        "--debug-k6",
        action="store_true",
        help="stream k6 output instead of writing stdout/stderr files only",
    )
    return parser.parse_known_args(argv)


def resolve_repo_path(path: Path) -> Path:
    return path if path.is_absolute() else REPO_ROOT / path


def default_outdir(scenario: str) -> Path:
    timestamp = time.strftime("%Y%m%d-%H%M%S")
    return DEFAULT_RESULTS_DIR / f"{scenario}-{timestamp}"


def selected_k6_script(args: argparse.Namespace) -> Path:
    if args.scenario not in WORKLOAD_SCRIPTS and args.k6_script is None:
        raise ValueError(f"scenario {args.scenario} does not use a k6 workload")
    script = (
        args.k6_script
        if args.k6_script is not None
        else WORKLOAD_SCRIPTS[args.scenario]
    )
    return resolve_repo_path(script)


def run_k6(
    *,
    k6_script: Path,
    export_file: Path,
    stdout_file: Path,
    stderr_file: Path,
    env: dict[str, str],
    extra_args: list[str],
    debug: bool,
) -> None:
    command = [
        "k6",
        "run",
        "--quiet",
        "--no-color",
        f"--summary-export={export_file}",
        *extra_args,
        str(k6_script),
    ]
    if debug:
        subprocess.run(command, env=env, check=True)
        return

    with stdout_file.open("wb") as stdout, stderr_file.open("wb") as stderr:
        subprocess.run(command, env=env, stdout=stdout, stderr=stderr, check=True)


def scenario_env(
    args: argparse.Namespace, topology_json: dict[str, Any], run_seed: int
) -> dict[str, str]:
    env = os.environ.copy()
    env.setdefault("AWS_ACCESS_KEY_ID", DEFAULT_ACCESS_KEY)
    env.setdefault("AWS_SECRET_ACCESS_KEY", DEFAULT_SECRET_KEY)
    env.setdefault("AWS_REGION", "us-east-1")
    env["SO3_ADDR"] = str(topology_json["entry_url"])
    env["SO3_ENTRY_URLS"] = ",".join(str(url) for url in topology_json["entry_urls"])
    env["SO3_BUCKET"] = args.bucket
    env["OBJECT_SIZE"] = str(args.object_size)
    env["VUS"] = str(args.vus)
    env["DURATION"] = args.duration
    env["RESEARCH_SEED"] = str(run_seed)
    return env


k6_env = scenario_env


def write_failure_summary(
    run_dir: Path,
    *,
    scenario: str,
    run_index: int,
    error: Exception,
) -> None:
    metrics.write_run_summary(
        run_dir / "summary.json",
        scenario=scenario,
        run_index=run_index,
        status="failed",
        error=f"{type(error).__name__}: {error}",
    )


def run_one(
    args: argparse.Namespace, extra_k6_args: list[str], run_index: int, result_dir: Path
) -> None:
    run_seed = args.seed + run_index - 1
    run_dir = result_dir / f"run-{run_index:03d}"
    run_dir.mkdir(parents=True, exist_ok=True)
    data_dir = run_dir / "data"
    events = manifest.EventLog(run_dir / "events.jsonl")
    events.record("run_start", run_index=run_index, seed=run_seed)

    topology = generate_topology(
        args.node_count,
        data_dir,
        host=args.host,
        object_base_port=args.object_base_port,
        rpc_base_port=args.rpc_base_port,
    )
    topology_json = topology.to_json()
    so3_bin = resolve_repo_path(args.so3_bin)
    k6_script = (
        selected_k6_script(args)
        if args.scenario in WORKLOAD_SCRIPTS or args.k6_script
        else None
    )
    workload = {
        "driver": "k6" if k6_script else "boto3",
        "script": (
            str(k6_script.relative_to(REPO_ROOT))
            if k6_script is not None and k6_script.is_relative_to(REPO_ROOT)
            else (str(k6_script) if k6_script is not None else None)
        ),
        "mix": "s3_put_get_head_delete"
        if k6_script
        else "concurrent_s3_object_correctness",
        "bucket": args.bucket,
        "object_size": args.object_size,
        "vus": args.vus,
        "duration": args.duration,
        "correctness_ops": args.correctness_ops if not k6_script else None,
        "correctness_concurrency": args.correctness_concurrency
        if not k6_script
        else None,
    }
    phases = {"baseline": {"duration": args.duration}}
    manifest.write_json(
        run_dir / "manifest.json",
        manifest.build_manifest(
            scenario=args.scenario,
            run_index=run_index,
            seed=run_seed,
            topology=topology_json,
            workload=workload,
            phases=phases,
            binary_path=args.so3_bin,
            repo_root=REPO_ROOT,
        ),
    )

    env = scenario_env(args, topology_json, run_seed)
    cluster = So3Cluster(
        binary=so3_bin,
        topology=topology,
        log_file=run_dir / "cluster.log",
        env=env,
        start_timeout_secs=args.start_timeout_secs,
        stop_timeout_secs=args.stop_timeout_secs,
    )
    sampler: ResourceSampler | None = None
    try:
        events.record("cluster_start")
        cluster.start()
        events.record("cluster_ready", pids=cluster.process_ids())
        sampler = ResourceSampler(
            cluster,
            run_dir / "resources.jsonl",
            interval_secs=args.resource_sample_interval_secs,
        )
        sampler.start()
        events.record("baseline_start")
        if k6_script is not None:
            run_k6(
                k6_script=k6_script,
                export_file=run_dir / "k6-summary.json",
                stdout_file=run_dir / "k6.stdout.log",
                stderr_file=run_dir / "k6.stderr.log",
                env=env,
                extra_args=extra_k6_args,
                debug=args.debug_k6,
            )
            run_metrics = metrics.summary_from_k6_export(run_dir / "k6-summary.json")
            status = "passed"
        else:
            driver = CorrectnessDriver(
                entry_urls=[str(url) for url in topology_json["entry_urls"]],
                history_path=run_dir / "client-history.jsonl",
                bucket=args.bucket,
                seed=run_seed,
                operations=args.correctness_ops,
                concurrency=args.correctness_concurrency,
                object_size=args.object_size,
            )
            run_metrics = driver.run()
            verifier_result = verify_history_file(run_dir / "client-history.jsonl")
            manifest.write_json(run_dir / "verifier-result.json", verifier_result)
            run_metrics["verifier_passed"] = (
                1.0 if verifier_result["verdict"] == "passed" else 0.0
            )
            run_metrics["unsupported_checks"] = float(
                len(verifier_result.get("unsupported", []))
            )
            status = "passed" if verifier_result["verdict"] == "passed" else "failed"
        events.record("baseline_end")
        metrics.write_run_summary(
            run_dir / "summary.json",
            scenario=args.scenario,
            run_index=run_index,
            status=status,
            metrics=run_metrics,
            error=None if status == "passed" else "verifier failed",
        )
    except Exception as error:
        events.record("run_error", error=f"{type(error).__name__}: {error}")
        write_failure_summary(
            run_dir, scenario=args.scenario, run_index=run_index, error=error
        )
    finally:
        if sampler is not None:
            sampler.stop()
        cluster.stop()
        events.record("run_end")
        if not args.keep_data_dirs:
            shutil.rmtree(data_dir, ignore_errors=True)


def main(argv: Sequence[str]) -> int:
    args, extra_k6_args = parse_args(argv)
    if args.runs < 30 and not args.allow_low_runs:
        print(
            "error: research scenarios require --runs >= 30; use --allow-low-runs for debugging",
            file=sys.stderr,
        )
        return 2

    try:
        require_psutil()
        if args.scenario in CORRECTNESS_SCENARIOS:
            require_boto3()
    except RuntimeError as exc:
        print(exc, file=sys.stderr)
        return 2

    result_dir = args.outdir or default_outdir(args.scenario)
    result_dir.mkdir(parents=True, exist_ok=True)

    print(f"SO3 research scenario: {args.scenario}")
    print(f"runs:      {args.runs}")
    print(f"nodes:     {args.node_count}")
    print(f"results:   {result_dir}")
    if args.scenario in WORKLOAD_SCRIPTS or args.k6_script is not None:
        k6_script = selected_k6_script(args)
        print(f"k6 script: {k6_script}")
    else:
        print("driver:    boto3 S3 correctness driver")
    print()

    try:
        for run_index in range(1, args.runs + 1):
            print(f"run {run_index:03d}/{args.runs} ... ", end="", flush=True)
            run_one(args, extra_k6_args, run_index, result_dir)
            print("done")
    except KeyboardInterrupt:
        print("interrupted", file=sys.stderr)
        return 130

    aggregate = stats.write_aggregate_summary(result_dir)
    report_path = report.write_report(result_dir, aggregate)
    print()
    print(f"aggregate: {result_dir / 'aggregate-summary.json'}")
    print(f"report:    {report_path}")
    print(f"verdict:   {aggregate.get('verdict')}")
    return 0 if aggregate.get("runs_failed", 0) == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
