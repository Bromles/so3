"""E3 node degradation scenario:
phased baseline → fail → degraded → recover → restored.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

import faults
import metrics as metrics_module
import metrics_timeseries
from runner import run_k6_phase

if TYPE_CHECKING:
    from pathlib import Path

    import manifest


def _alive_entry_urls(all_urls: list[str], fault_node: int) -> list[str]:
    """Return entry URLs excluding the fault node (1-based index → 0-based URL list)."""
    return [url for i, url in enumerate(all_urls, start=1) if i != fault_node]


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
    all_urls: list[str] = getattr(args, "entry_urls", None) or []

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

    # During degraded phase, only send traffic to alive nodes.
    alive_urls = _alive_entry_urls(all_urls, args.fault_node)
    phase_exports["degraded"], _ = run_k6_phase(
        args=args,
        k6_script=k6_script,
        run_dir=run_dir,
        env=env,
        extra_k6_args=extra_k6_args,
        phase="degraded",
        duration=phase_durations["degraded"],
        events=events,
        entry_urls_override=alive_urls or None,
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

    phase_exports["recovery"], recovery_stream = run_k6_phase(
        args=args,
        k6_script=k6_script,
        run_dir=run_dir,
        env=env,
        extra_k6_args=extra_k6_args,
        phase="recovery",
        duration=phase_durations["recovery"],
        events=events,
        with_stream=True,
    )

    warmup_secs: float = getattr(args, "recovery_warmup_secs", 0.0) or 0.0
    if warmup_secs > 0:
        events.record("restored_warmup_start", duration_secs=warmup_secs)
        time.sleep(warmup_secs)
        events.record("restored_warmup_end")

    events.record("normal_restored", node_index=args.fault_node)
    phase_exports["restored"], _restored_stream = run_k6_phase(
        args=args,
        k6_script=k6_script,
        run_dir=run_dir,
        env=env,
        extra_k6_args=extra_k6_args,
        phase="restored",
        duration=phase_durations["restored"],
        events=events,
        with_stream=True,
    )

    run_metrics = metrics_module.summary_from_k6_phase_exports(phase_exports)
    run_metrics.setdefault("fault", {})["node_index"] = args.fault_node
    run_metrics["fault"]["recovery_seconds"] = recovery_seconds
    run_metrics["fault"]["total_downtime_secs"] = recovery_start - fail_monotonic

    baseline_rate = (
        run_metrics.get("phases", {})
        .get("baseline", {})
        .get("throughput", {})
        .get("http_reqs", {})
        .get("rate")
    )
    if baseline_rate and recovery_stream is not None:
        stab = metrics_timeseries.stabilization_time_secs(
            recovery_stream, baseline_rate=float(baseline_rate)
        )
        run_metrics["fault"]["stabilization_secs"] = stab

    return run_metrics
