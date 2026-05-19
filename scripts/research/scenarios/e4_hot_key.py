"""E4 hot-key conflict behavior: per-key-class latency comparison."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import manifest
import metrics as metrics_module
import metrics_timeseries
from runner import run_k6


def run_e4_hot_key(
    *,
    args: Any,
    k6_script: Path,
    run_dir: Path,
    env: dict[str, str],
    extra_k6_args: list[str],
    events: manifest.EventLog,
) -> dict[str, Any]:
    """Run E4: hot-key workload with key_class-tagged stream metrics.

    Emits hot_vs_independent_p95_ratio and key_class_metrics into run_metrics.
    Returns run_metrics.
    """
    export_file = run_dir / "k6-summary.json"
    stream_file = run_dir / "k6-stream.jsonl"

    events.record("baseline_start")
    run_k6(
        k6_script=k6_script,
        export_file=export_file,
        stdout_file=run_dir / "k6.stdout.log",
        stderr_file=run_dir / "k6.stderr.log",
        env=env,
        extra_args=extra_k6_args,
        debug=args.debug_k6,
        stream_file=stream_file,
    )
    events.record("baseline_end")

    run_metrics = metrics_module.summary_from_k6_export(export_file)

    if stream_file.exists():
        by_class = metrics_timeseries.hot_vs_independent_metrics(stream_file)
        run_metrics["key_class_metrics"] = by_class
        ratio = metrics_timeseries.latency_ratio(by_class, "hot", "independent")
        if ratio is not None:
            run_metrics["hot_vs_independent_p95_ratio"] = ratio

    return run_metrics
