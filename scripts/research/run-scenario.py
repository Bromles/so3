#!/usr/bin/env python3
"""Main CLI for SO3 research scenarios.

Runs a chosen scenario for --runs iterations, writes per-run manifests and
summaries, then produces an aggregate summary and a markdown report.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import time
from pathlib import Path
from typing import Any, Sequence

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parents[1]
sys.path.insert(0, str(SCRIPT_DIR))
sys.path.insert(0, str(REPO_ROOT))

import manifest  # noqa: E402
import metrics  # noqa: E402
import plot  # noqa: E402
import report  # noqa: E402
import stats  # noqa: E402
from cluster import ResourceSampler, So3Cluster  # noqa: E402
from runner import run_k6  # noqa: E402
from scenarios.e2_fault_safety import run_e2_fault_safety  # noqa: E402
from scenarios.e3_node_degradation import run_e3_node_degradation  # noqa: E402
from scenarios.e4_hot_key import run_e4_hot_key  # noqa: E402
from scenarios.e5_leaderless import run_e5_leaderless  # noqa: E402
from scenarios.e6_recovery import run_e6_recovery  # noqa: E402
from topology import SUPPORTED_NODE_COUNTS, generate_topology  # noqa: E402

DEFAULT_ACCESS_KEY = "so3testkey000000"
DEFAULT_SECRET_KEY = "so3testsecret0000000000000000000"
DEFAULT_RESULTS_DIR = REPO_ROOT / "results" / "research"
MAX_K6_ERROR_RATE = 0.01
CORRECTNESS_SCENARIOS = {"e2-fault-safety"}
PHASED_FAULT_SCENARIOS = {"e3-degradation", "e6-recovery"}
# Scenarios where errors are intentional data: hot key contention and node
# failures produce high error rates by design. Do not fail a run based on
# error rate alone; infrastructure failures surface as exceptions instead.
ERROR_RATE_EXEMPT_SCENARIOS = {
    "e3-degradation",
    "e4-hot-key",
    "e5-leaderless",
    "e6-recovery",
}
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
    parser.add_argument(
        "--matrix-node-counts",
        action="store_true",
        help=(
            "run the scenario for each supported node count"
            " (only e3-degradation and e6-recovery)"
        ),
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
    parser.add_argument(
        "--baseline-duration",
        default=None,
        help="override baseline phase duration for phased fault scenarios",
    )
    parser.add_argument(
        "--degraded-duration",
        default=None,
        help="override degraded phase duration for phased fault scenarios",
    )
    parser.add_argument(
        "--recovery-duration",
        default=None,
        help="override recovery phase duration for phased fault scenarios",
    )
    parser.add_argument(
        "--restored-duration",
        default=None,
        help="override restored phase duration for phased fault scenarios",
    )
    parser.add_argument(
        "--fault-node",
        type=int,
        default=1,
        help="1-based node index to crash/restart in phased fault scenarios",
    )
    parser.add_argument(
        "--fault-node-policy",
        choices=("fixed", "round_robin"),
        default="fixed",
        help="fault node selection policy for phased fault scenarios",
    )
    parser.add_argument("--vus", type=int, default=int(os.environ.get("VUS", "10")))
    parser.add_argument(
        "--object-size", type=int, default=int(os.environ.get("OBJECT_SIZE", "64"))
    )
    parser.add_argument("--bucket", default=os.environ.get("SO3_BUCKET", "bench"))
    parser.add_argument("--seed", type=int, default=int(time.time()))
    parser.add_argument("--start-timeout-secs", type=float, default=20.0)
    parser.add_argument("--stop-timeout-secs", type=float, default=10.0)
    parser.add_argument("--resource-sample-interval-secs", type=float, default=1.0)
    parser.add_argument("--correctness-ops", type=int, default=500)
    parser.add_argument("--correctness-concurrency", type=int, default=12)
    parser.add_argument("--correctness-ops-per-sec", type=float, default=2.0)
    parser.add_argument("--correctness-duration-secs", type=float, default=30.0)
    parser.add_argument(
        "--e2-fault-cycles",
        type=int,
        default=None,
        help="number of crash/restart cycles for e2-fault-safety (default: node-count)",
    )
    parser.add_argument(
        "--e2-cycle-interval-secs",
        type=float,
        default=1.0,
        help="seconds between fault cycles in e2-fault-safety",
    )
    parser.add_argument(
        "--e2-crash-duration-secs",
        type=float,
        default=5.0,
        help="seconds a node stays crashed per cycle in e2-fault-safety",
    )
    parser.add_argument(
        "--e6-long-downtime-secs",
        type=float,
        default=0.0,
        help="extra seconds to keep the node down before restarting in e6-recovery",
    )
    parser.add_argument(
        "--recovery-warmup-secs",
        type=float,
        default=0.0,
        help=(
            "seconds to wait after restarting a node before measuring the"
            " restored phase; gives the recovered node time to finish"
            " catch-up synchronization (e3-degradation, e6-recovery)"
        ),
    )
    parser.add_argument(
        "--e6-re-crash",
        action="store_true",
        help="enable re-crash during sync sub-scenario in e6-recovery",
    )
    parser.add_argument(
        "--e6-re-crash-duration",
        default="15s",
        help="duration of the degraded phase after the second crash in e6-recovery (default: 15s)",
    )
    parser.add_argument("--keep-data-dirs", action="store_true")
    parser.add_argument(
        "--debug-k6",
        action="store_true",
        help="stream k6 output instead of writing stdout/stderr files only",
    )
    args, extra_k6_args = parser.parse_known_args(argv)

    # --matrix-node-counts and explicit --node-count are mutually exclusive,
    # but only when the user actually passed --node-count on the command line.
    if args.matrix_node_counts and "--node-count" in argv:
        parser.error("--matrix-node-counts and --node-count are mutually exclusive")

    return args, extra_k6_args


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


def phase_durations(args: argparse.Namespace) -> dict[str, str]:
    return {
        "baseline": args.baseline_duration or args.duration,
        "degraded": args.degraded_duration or args.duration,
        "recovery": args.recovery_duration or args.duration,
        "restored": args.restored_duration or args.duration,
    }


def phased_scenario(args: argparse.Namespace) -> bool:
    return args.scenario in PHASED_FAULT_SCENARIOS


def fault_node_for_run(args: argparse.Namespace, run_index: int) -> int:
    if getattr(args, "fault_node_policy", "fixed") == "round_robin":
        return ((args.fault_node - 1 + run_index - 1) % args.node_count) + 1
    return args.fault_node


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
    env["RESEARCH_SCENARIO"] = args.scenario
    return env


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


def metric_rate(run_metrics: dict[str, Any], category: str, name: str) -> float | None:
    value = run_metrics.get(category, {}).get(name, {}).get("rate")
    if isinstance(value, (int, float)):
        return float(value)
    return None


def status_from_k6_metrics(run_metrics: dict[str, Any]) -> tuple[str, str | None]:
    for metric_name in ("s3_errors", "http_req_failed"):
        rate = metric_rate(run_metrics, "errors", metric_name)
        if rate is not None and rate >= MAX_K6_ERROR_RATE:
            return (
                "failed",
                f"{metric_name} rate {rate:.6g} exceeded limit {MAX_K6_ERROR_RATE}",
            )
    return "passed", None


def run_one(
    args: argparse.Namespace, extra_k6_args: list[str], run_index: int, result_dir: Path
) -> None:
    run_seed = args.seed + run_index - 1
    run_dir = result_dir / f"run-{run_index:03d}"
    run_dir.mkdir(parents=True, exist_ok=True)
    data_dir = run_dir / "data"
    events = manifest.EventLog(run_dir / "events.jsonl")
    events.record("run_start", run_index=run_index, seed=run_seed)

    run_args = argparse.Namespace(**vars(args))
    if phased_scenario(args):
        run_args.fault_node = fault_node_for_run(args, run_index)
        events.record(
            "fault_node_selected",
            node_index=run_args.fault_node,
            policy=args.fault_node_policy,
        )
    args = run_args

    topology = generate_topology(
        args.node_count,
        data_dir,
        host=args.host,
        object_base_port=args.object_base_port,
        rpc_base_port=args.rpc_base_port,
    )
    topology_json = topology.to_json()
    args.entry_urls = topology_json.get("entry_urls", [])
    so3_bin = resolve_repo_path(args.so3_bin)
    k6_script = (
        selected_k6_script(args)
        if args.scenario in WORKLOAD_SCRIPTS or args.k6_script
        else None
    )

    workload = {
        "driver": "k6" if k6_script else "boto3",
        "script": str(
            k6_script.relative_to(REPO_ROOT)
            if k6_script is not None and k6_script.is_relative_to(REPO_ROOT)
            else k6_script
        )
        if k6_script is not None
        else None,
        "mix": "s3_put_get_head_delete"
        if k6_script
        else "concurrent_s3_object_correctness",
        "bucket": args.bucket,
        "object_size": args.object_size,
        "vus": args.vus,
        "duration": args.duration,
        "correctness_ops": args.correctness_ops if not k6_script else None,
        "correctness_ops_per_sec": args.correctness_ops_per_sec
        if not k6_script
        else None,
        "correctness_duration_secs": args.correctness_duration_secs
        if not k6_script
        else None,
        "correctness_concurrency": args.correctness_concurrency
        if not k6_script
        else None,
    }

    if phased_scenario(args):
        phases = {
            phase: {"duration": duration}
            for phase, duration in phase_durations(args).items()
        }
        fault_injection: dict[str, Any] | None = {
            "kind": "crash_restart",
            "node_index": args.fault_node,
            "node_policy": args.fault_node_policy,
        }
        if args.scenario == "e6-recovery":
            fault_injection["long_downtime_secs"] = args.e6_long_downtime_secs
            if getattr(args, "e6_re_crash", False):
                fault_injection["re_crash"] = True
                fault_injection["re_crash_duration"] = args.e6_re_crash_duration
    elif args.scenario == "e2-fault-safety":
        phases = {
            "baseline": {
                "driver": "aioboto3",
                "ops_per_sec": args.correctness_ops_per_sec,
                "duration_secs": args.correctness_duration_secs,
                "concurrency": args.correctness_concurrency,
            }
        }
        fault_injection = {
            "kind": "concurrent_crash_restart",
            "fault_cycles": args.e2_fault_cycles or args.node_count,
            "cycle_interval_secs": args.e2_cycle_interval_secs,
            "crash_duration_secs": args.e2_crash_duration_secs,
        }
    else:
        phases = {"baseline": {"duration": args.duration}}
        fault_injection = None

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
            fault_injection=fault_injection,
        ),
    )

    env = scenario_env(args, topology_json, run_seed)
    cluster = So3Cluster(
        binary=so3_bin,
        topology=topology,
        log_dir=run_dir / "logs",
        env=env,
        start_timeout_secs=args.start_timeout_secs,
        stop_timeout_secs=args.stop_timeout_secs,
    )
    sampler: ResourceSampler | None = None
    status_error: str | None = None
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

        if args.scenario == "e2-fault-safety":
            run_metrics, verifier_result = run_e2_fault_safety(
                args=args,
                cluster=cluster,
                events=events,
                run_dir=run_dir,
                topology_json=topology_json,
                bucket=args.bucket,
                run_seed=run_seed,
            )
            status = "passed" if verifier_result["verdict"] == "passed" else "failed"
            status_error = None if status == "passed" else "verifier failed"
        elif args.scenario == "e3-degradation":
            assert k6_script is not None
            run_metrics = run_e3_node_degradation(
                args=args,
                k6_script=k6_script,
                run_dir=run_dir,
                env=env,
                extra_k6_args=extra_k6_args,
                cluster=cluster,
                events=events,
                phase_durations=phase_durations(args),
            )
            if args.scenario in ERROR_RATE_EXEMPT_SCENARIOS:
                status, status_error = "passed", None
            else:
                status, status_error = status_from_k6_metrics(run_metrics)
        elif args.scenario == "e4-hot-key":
            assert k6_script is not None
            run_metrics = run_e4_hot_key(
                args=args,
                k6_script=k6_script,
                run_dir=run_dir,
                env=env,
                extra_k6_args=extra_k6_args,
                events=events,
            )
            status, status_error = "passed", None  # errors are intentional data
        elif args.scenario == "e5-leaderless":
            assert k6_script is not None
            run_metrics = run_e5_leaderless(
                args=args,
                k6_script=k6_script,
                run_dir=run_dir,
                env=env,
                extra_k6_args=extra_k6_args,
                events=events,
            )
            status, status_error = status_from_k6_metrics(run_metrics)
        elif args.scenario == "e6-recovery":
            assert k6_script is not None
            run_metrics = run_e6_recovery(
                args=args,
                k6_script=k6_script,
                run_dir=run_dir,
                env=env,
                extra_k6_args=extra_k6_args,
                cluster=cluster,
                events=events,
                phase_durations=phase_durations(args),
                run_seed=run_seed,
            )
            status, status_error = "passed", None  # errors are intentional data
        else:
            # k6-mixed: single-phase k6 run
            assert k6_script is not None
            events.record("baseline_start")
            run_k6(
                k6_script=k6_script,
                export_file=run_dir / "k6-summary.json",
                stdout_file=run_dir / "k6.stdout.log",
                stderr_file=run_dir / "k6.stderr.log",
                env=env,
                extra_args=extra_k6_args,
                debug=args.debug_k6,
            )
            events.record("baseline_end")
            run_metrics = metrics.summary_from_k6_export(run_dir / "k6-summary.json")
            status, status_error = status_from_k6_metrics(run_metrics)

        metrics.merge_metrics(
            run_metrics,
            metrics.summary_from_cluster_log(run_dir / "logs"),
        )
        metrics.write_run_summary(
            run_dir / "summary.json",
            scenario=args.scenario,
            run_index=run_index,
            status=status,
            metrics=run_metrics,
            error=status_error,
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


# ---------------------------------------------------------------------------
# Matrix-runs mode: run a phased scenario across multiple node counts
# ---------------------------------------------------------------------------

MATRIX_SCENARIOS = {"e3-degradation", "e6-recovery"}


def _extract_comparison_metric(
    aggregate: dict[str, Any], *paths: str
) -> dict[str, Any]:
    """Extract mean and CI for a dotted metric path from an aggregate."""
    node: Any = aggregate
    for key in paths:
        if not isinstance(node, dict):
            return {"mean": None, "ci": None}
        node = node.get(key)
    if not isinstance(node, dict):
        return {"mean": None, "ci": None}
    mean = node.get("mean")
    ci_lower = node.get("ci_lower")
    ci_upper = node.get("ci_upper")
    if mean is not None and ci_lower is not None and ci_upper is not None:
        return {"mean": mean, "ci": [ci_lower, ci_upper]}
    return {"mean": mean, "ci": None}


def _build_comparison(
    per_node: dict[int, dict[str, Any]],
) -> dict[str, dict[str, dict[str, Any]]]:
    """Build a cross-node comparison of key metrics from per-node aggregates."""
    comparison: dict[str, dict[str, dict[str, float | None]]] = {}

    # Throughput degradation: normalized throughput during degraded phase
    comparison["throughput_degradation"] = {}
    for nc, agg in sorted(per_node.items()):
        rel = agg.get("relative_metrics", {}).get("degraded", {})
        # Look for a throughput-like metric in relative phase metrics
        found = False
        for metric_name, metric_stat in rel.items():
            if "throughput" in metric_name and "total" not in metric_name:
                comparison["throughput_degradation"][str(nc)] = (
                    _extract_comparison_metric({"_": metric_stat}, "_")
                )
                found = True
                break
        if not found:
            comparison["throughput_degradation"][str(nc)] = {"mean": None, "ci": None}

    # Recovery time
    comparison["recovery_time"] = {}
    for nc, agg in sorted(per_node.items()):
        metrics = agg.get("metrics", {})
        found = False
        for metric_name in metrics:
            if "recovery_time" in metric_name:
                comparison["recovery_time"][str(nc)] = _extract_comparison_metric(
                    agg, "metrics", metric_name
                )
                found = True
                break
        if not found:
            comparison["recovery_time"][str(nc)] = {"mean": None, "ci": None}

    # Stabilization time
    comparison["stabilization_time"] = {}
    for nc, agg in sorted(per_node.items()):
        metrics = agg.get("metrics", {})
        found = False
        for metric_name in metrics:
            if "stabilization_time" in metric_name:
                comparison["stabilization_time"][str(nc)] = _extract_comparison_metric(
                    agg, "metrics", metric_name
                )
                found = True
                break
        if not found:
            comparison["stabilization_time"][str(nc)] = {"mean": None, "ci": None}

    # Verifier pass rate (for E6)
    comparison["verifier_pass_rate"] = {}
    for nc, agg in sorted(per_node.items()):
        metrics_map = agg.get("metrics", {})
        found = False
        for metric_name in metrics_map:
            if "verifier" in metric_name and "pass" in metric_name:
                comparison["verifier_pass_rate"][str(nc)] = _extract_comparison_metric(
                    agg, "metrics", metric_name
                )
                found = True
                break
        if not found:
            comparison["verifier_pass_rate"][str(nc)] = {"mean": None, "ci": None}

    return comparison


def write_matrix_summary(
    parent_dir: Path,
    scenario: str,
    per_node: dict[int, dict[str, Any]],
) -> Path:
    """Write matrix-summary.json at the parent result directory."""
    comparison = _build_comparison(per_node)
    payload: dict[str, Any] = {
        "scenario": scenario,
        "node_counts": {str(nc): agg for nc, agg in sorted(per_node.items())},
        "comparison": comparison,
    }
    path = parent_dir / "matrix-summary.json"
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return path


def write_matrix_report(
    parent_dir: Path,
    scenario: str,
    per_node: dict[int, dict[str, Any]],
) -> Path:
    """Write matrix-report.md comparing results across node counts."""
    comparison = _build_comparison(per_node)
    path = parent_dir / "matrix-report.md"

    lines = [
        f"# Matrix report: {scenario}",
        "",
        "Comparison of key metrics across node counts.",
        "",
    ]

    # Summary table
    lines.extend(
        [
            "## Summary",
            "",
            "| node count | runs | passed | failed | verdict |",
            "| --- | ---: | ---: | ---: | --- |",
        ]
    )
    for nc in sorted(per_node):
        agg = per_node[nc]
        lines.append(
            f"| {nc} "
            f"| {agg.get('runs_total', 0)} "
            f"| {agg.get('runs_successful', 0)} "
            f"| {agg.get('runs_failed', 0)} "
            f"| {agg.get('verdict', 'unknown')} |"
        )
    lines.append("")

    # Comparison metrics tables
    for metric_name, by_nc in comparison.items():
        display_name = metric_name.replace("_", " ").title()
        lines.extend([f"## {display_name}", ""])
        lines.extend(
            [
                "| node count | mean | 95% CI |",
                "| --- | ---: | --- |",
            ]
        )
        for nc_key in sorted(by_nc, key=lambda k: int(k)):
            entry = by_nc[nc_key]
            mean = entry.get("mean")
            ci = entry.get("ci")
            mean_str = f"{mean:.6g}" if mean is not None else "—"
            if ci is not None:
                ci_str = f"[{ci[0]:.6g}, {ci[1]:.6g}]"
            else:
                ci_str = "—"
            lines.append(f"| {nc_key} | {mean_str} | {ci_str} |")
        lines.append("")

    # Per-node subdirectory links
    lines.extend(["## Per-node details", ""])
    for nc in sorted(per_node):
        lines.append(f"- [nodes-{nc}](nodes-{nc}/report.md)")
    lines.append("")

    path.write_text("\n".join(lines), encoding="utf-8")
    return path


def run_matrix(
    args: argparse.Namespace,
    extra_k6_args: list[str],
) -> int:
    """Execute the scenario across all supported node counts."""
    scenario = args.scenario

    if scenario not in MATRIX_SCENARIOS:
        print(
            f"error: --matrix-node-counts is only supported for phased fault"
            f" scenarios: {', '.join(sorted(MATRIX_SCENARIOS))}",
            file=sys.stderr,
        )
        return 2

    parent_dir = args.outdir or default_outdir(scenario)
    parent_dir.mkdir(parents=True, exist_ok=True)

    print(f"SO3 matrix research scenario: {scenario}")
    print(f"node counts: {', '.join(str(nc) for nc in SUPPORTED_NODE_COUNTS)}")
    print(f"runs per node count: {args.runs}")
    print(f"results:   {parent_dir}")
    if args.scenario in WORKLOAD_SCRIPTS or args.k6_script is not None:
        k6_script = selected_k6_script(args)
        print(f"k6 script: {k6_script}")
    print()

    per_node: dict[int, dict[str, Any]] = {}
    any_failed = False

    for node_count in SUPPORTED_NODE_COUNTS:
        sub_dir = parent_dir / f"nodes-{node_count}"
        sub_dir.mkdir(parents=True, exist_ok=True)

        print(f"--- nodes: {node_count} ---")
        matrix_args = argparse.Namespace(**vars(args))
        matrix_args.node_count = node_count

        try:
            for run_index in range(1, args.runs + 1):
                print(
                    f"[{node_count}n] run {run_index:03d}/{args.runs} ... ",
                    end="",
                    flush=True,
                )
                run_one(matrix_args, extra_k6_args, run_index, sub_dir)
                print("done")
        except KeyboardInterrupt:
            print("interrupted", file=sys.stderr)
            return 130

        aggregate = stats.write_aggregate_summary(sub_dir)
        per_node[node_count] = aggregate
        if aggregate.get("runs_failed", 0) > 0:
            any_failed = True

        try:
            plot_paths = plot.generate_plots(sub_dir, aggregate)
            plot_error = None
        except Exception as error:
            plot_paths = []
            plot_error = f"{type(error).__name__}: {error}"

        report_path = report.write_report(sub_dir, aggregate)

        print(f"  aggregate: {sub_dir / 'aggregate-summary.json'}")
        if plot_paths:
            print(f"  plots:     {sub_dir / 'plots'}")
        if plot_error:
            print(f"  plots:     skipped ({plot_error})", file=sys.stderr)
        print(f"  report:    {report_path}")
        print(f"  verdict:   {aggregate.get('verdict')}")
        print()

    # Write matrix-level outputs
    summary_path = write_matrix_summary(parent_dir, scenario, per_node)
    report_path = write_matrix_report(parent_dir, scenario, per_node)

    print(f"matrix summary: {summary_path}")
    print(f"matrix report:  {report_path}")
    return 1 if any_failed else 0


def main(argv: Sequence[str]) -> int:
    args, extra_k6_args = parse_args(argv)
    if args.runs < 30 and not args.allow_low_runs:
        print(
            "error: research scenarios require --runs >= 30; use --allow-low-runs for debugging",
            file=sys.stderr,
        )
        return 2
    if phased_scenario(args) and not 1 <= args.fault_node <= args.node_count:
        print(
            f"error: --fault-node must be between 1 and --node-count ({args.node_count})",
            file=sys.stderr,
        )
        return 2

    # Matrix mode: run across all supported node counts
    if args.matrix_node_counts:
        return run_matrix(args, extra_k6_args)

    result_dir = args.outdir or default_outdir(args.scenario)
    result_dir.mkdir(parents=True, exist_ok=True)

    print(f"SO3 research scenario: {args.scenario}")
    print(f"runs:      {args.runs}")
    print(f"nodes:     {args.node_count}")
    if phased_scenario(args):
        print(f"faults:    node {args.fault_node} ({args.fault_node_policy})")
    print(f"results:   {result_dir}")
    if args.scenario in WORKLOAD_SCRIPTS or args.k6_script is not None:
        k6_script = selected_k6_script(args)
        print(f"k6 script: {k6_script}")
    else:
        print("driver:    aioboto3 async S3 correctness driver")
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
    try:
        plot_paths = plot.generate_plots(result_dir, aggregate)
        plot_error = None
    except Exception as error:
        plot_paths = []
        plot_error = f"{type(error).__name__}: {error}"
    report_path = report.write_report(result_dir, aggregate)
    print()
    print(f"aggregate: {result_dir / 'aggregate-summary.json'}")
    if plot_paths:
        print(f"plots:     {result_dir / 'plots'}")
    if plot_error:
        print(f"plots:     skipped ({plot_error})", file=sys.stderr)
    print(f"report:    {report_path}")
    print(f"verdict:   {aggregate.get('verdict')}")
    return 0 if aggregate.get("runs_failed", 0) == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
