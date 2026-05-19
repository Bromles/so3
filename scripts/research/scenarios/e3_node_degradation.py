"""E3 node degradation scenario: phased baseline → fail → degraded → recover → restored."""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

import faults
import manifest
import metrics as metrics_module
from runner import run_k6_phase


def run_e3_node_degradation(
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
    """Run E3: four-phase degradation test for a single node failure.

    Phases: baseline → (crash node) → degraded → (restart node) → recovery → restored.
    Returns run_metrics with phase summaries, relative metrics, and fault timing.
    """
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

    run_metrics = metrics_module.summary_from_k6_phase_exports(phase_exports)
    run_metrics.setdefault("fault", {})["recovery_seconds"] = recovery_seconds
    run_metrics["fault"]["time_to_degraded_secs"] = recovery_start - fail_monotonic
    return run_metrics
