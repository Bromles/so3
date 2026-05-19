"""E6 recovery scenario: extended downtime with full recovery measurement."""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

import faults
import manifest
import metrics as metrics_module
from runner import run_k6_phase


def run_e6_recovery(
    *,
    args: Any,
    k6_script: Path,
    run_dir: Path,
    env: dict[str, str],
    extra_k6_args: list[str],
    cluster: Any,
    events: manifest.EventLog,
    phase_durations: dict[str, str],
) -> dict[str, Any]:
    """Run E6: extended-downtime recovery test.

    Like E3 but the node stays down for an additional --e6-long-downtime-secs
    after the degraded phase before being restarted. Tests whether the cluster
    recovers correctly after a longer absence.

    Phases: baseline → (crash) → degraded → (wait) → (restart) → recovery → restored.
    """
    long_downtime_secs: float = getattr(args, "e6_long_downtime_secs", 0.0) or 0.0
    phase_exports: dict[str, Path] = {}

    phase_exports["baseline"], _ = run_k6_phase(
        args=args,
        k6_script=k6_script,
        run_dir=run_dir,
        env=env,
        extra_k6_args=extra_k6_args,
        phase="baseline",
        duration=phase_durations["baseline"],
        events=events,
    )

    fail_monotonic = time.monotonic()
    fault = faults.crash_node(cluster, args.fault_node)
    events.record("fail", kind=fault.kind, node_index=fault.node_index)

    phase_exports["degraded"], _ = run_k6_phase(
        args=args,
        k6_script=k6_script,
        run_dir=run_dir,
        env=env,
        extra_k6_args=extra_k6_args,
        phase="degraded",
        duration=phase_durations["degraded"],
        events=events,
    )

    if long_downtime_secs > 0:
        events.record("long_downtime_start", duration_secs=long_downtime_secs)
        time.sleep(long_downtime_secs)
        events.record("long_downtime_end")

    recovery_start = time.monotonic()
    recovery = faults.restart_node(cluster, args.fault_node)
    recovery_seconds = time.monotonic() - recovery_start
    events.record(
        "recover",
        kind=recovery.kind,
        node_index=recovery.node_index,
        recovery_seconds=recovery_seconds,
    )

    phase_exports["recovery"], _ = run_k6_phase(
        args=args,
        k6_script=k6_script,
        run_dir=run_dir,
        env=env,
        extra_k6_args=extra_k6_args,
        phase="recovery",
        duration=phase_durations["recovery"],
        events=events,
    )

    events.record("normal_restored", node_index=args.fault_node)
    phase_exports["restored"], _ = run_k6_phase(
        args=args,
        k6_script=k6_script,
        run_dir=run_dir,
        env=env,
        extra_k6_args=extra_k6_args,
        phase="restored",
        duration=phase_durations["restored"],
        events=events,
    )

    down_total_secs = recovery_start - fail_monotonic
    run_metrics = metrics_module.summary_from_k6_phase_exports(phase_exports)
    run_metrics.setdefault("fault", {})["recovery_seconds"] = recovery_seconds
    run_metrics["fault"]["time_to_degraded_secs"] = down_total_secs
    run_metrics["fault"]["long_downtime_secs"] = long_downtime_secs
    return run_metrics
