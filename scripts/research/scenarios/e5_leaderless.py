"""E5 leaderless behavior: per-node entry metrics via k6 stream tagging."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import metrics as metrics_module
import metrics_timeseries
from runner import run_k6

if TYPE_CHECKING:
    from pathlib import Path

    import manifest


def run_e5_leaderless(
    *,
    args: Any,
    k6_script: Path,
    run_dir: Path,
    env: dict[str, str],
    extra_k6_args: list[str],
    events: manifest.EventLog,
) -> dict[str, Any]:
    """Run E5: leaderless verification with per-entry-node stream metrics.

    Runs a single k6 baseline phase and records per-node request distribution
    from the JSONL stream. Symmetry-of-failures testing is covered by Maelstrom
    (see scripts/maelstrom/fault_3node.py).
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
        by_node = metrics_timeseries.per_node_entry_metrics(stream_file)
        run_metrics["entry_node_metrics"] = by_node

    return run_metrics
