"""E6 recovery scenario: extended downtime with full recovery measurement."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import faults
import manifest
import metrics as metrics_module
import metrics_timeseries
from correctness_driver import RecoverySentinel
from runner import run_k6_phase


def _alive_entry_urls(all_urls: list[str], fault_node: int) -> list[str]:
    """Return entry URLs excluding the fault node (1-based index → 0-based URL list)."""
    return [url for i, url in enumerate(all_urls, start=1) if i != fault_node]


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
    run_seed: int = 0,
) -> dict[str, Any]:
    """Run E6: extended-downtime recovery test.

    Like E3 but the node stays down for an additional --e6-long-downtime-secs
    after the degraded phase before being restarted. Tests whether the cluster
    recovers correctly after a longer absence.

    Phases: baseline -> (crash) -> degraded -> (wait) -> (restart) -> recovery -> restored.

    When --e6-re-crash is enabled, the node is crashed AGAIN after the initial
    recovery phase, then restarted a second time:

    baseline -> (crash) -> degraded -> (wait) -> (restart) -> recovery ->
    (re-crash) -> re_crash_degraded -> (restart) -> re_recovery -> re_restored.
    """
    long_downtime_secs: float = getattr(args, "e6_long_downtime_secs", 0.0) or 0.0
    re_crash_enabled: bool = getattr(args, "e6_re_crash", False)
    re_crash_duration: str = getattr(args, "e6_re_crash_duration", "15s") or "15s"
    entry_urls: list[str] = getattr(args, "entry_urls", None) or []
    bucket: str = getattr(args, "bucket", "so3-benchmark")
    phase_exports: dict[str, Path] = {}

    sentinel = (
        RecoverySentinel(
            entry_urls=entry_urls,
            bucket=bucket,
            seed=run_seed,
        )
        if entry_urls
        else None
    )

    confirmed_writes: dict[str, str] = {}
    if sentinel is not None:
        confirmed_writes = sentinel.write()
        events.record("sentinel_write", count=len(confirmed_writes))

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
    alive_urls = _alive_entry_urls(entry_urls, args.fault_node)
    phase_exports["degraded"], _ = run_k6_phase(
        args=args,
        k6_script=k6_script,
        run_dir=run_dir,
        env=env,
        extra_k6_args=extra_k6_args,
        phase="degraded",
        duration=phase_durations["degraded"],
        events=events,
        entry_urls_override=alive_urls if alive_urls else None,
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

    warmup_secs: float = getattr(args, "recovery_warmup_secs", 0.0) or 0.0

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

    if re_crash_enabled:
        # --- Re-crash sub-scenario ---
        re_crash_monotonic = time.monotonic()
        faults.crash_node(cluster, args.fault_node)
        events.record("re_crash", node_index=args.fault_node)

        phase_exports["re_crash_degraded"], _ = run_k6_phase(
            args=args,
            k6_script=k6_script,
            run_dir=run_dir,
            env=env,
            extra_k6_args=extra_k6_args,
            phase="re_crash_degraded",
            duration=re_crash_duration,
            events=events,
            entry_urls_override=alive_urls if alive_urls else None,
        )

        re_recovery_start = time.monotonic()
        re_recovery = faults.restart_node(cluster, args.fault_node)
        re_recovery_seconds = time.monotonic() - re_recovery_start
        events.record(
            "re_recovery",
            kind=re_recovery.kind,
            node_index=re_recovery.node_index,
            recovery_seconds=re_recovery_seconds,
        )

        re_crash_downtime_secs = re_recovery_start - re_crash_monotonic

        phase_exports["re_recovery"], re_recovery_stream = run_k6_phase(
            args=args,
            k6_script=k6_script,
            run_dir=run_dir,
            env=env,
            extra_k6_args=extra_k6_args,
            phase="re_recovery",
            duration=phase_durations["recovery"],
            events=events,
            with_stream=True,
        )

        if warmup_secs > 0:
            events.record("re_restored_warmup_start", duration_secs=warmup_secs)
            time.sleep(warmup_secs)
            events.record("re_restored_warmup_end")

        events.record("normal_re_restored", node_index=args.fault_node)
        phase_exports["re_restored"], _ = run_k6_phase(
            args=args,
            k6_script=k6_script,
            run_dir=run_dir,
            env=env,
            extra_k6_args=extra_k6_args,
            phase="re_restored",
            duration=phase_durations["restored"],
            events=events,
            with_stream=True,
        )
    else:
        re_crash_downtime_secs = 0.0
        re_recovery_seconds = 0.0
        re_recovery_stream = None

        if warmup_secs > 0:
            events.record("restored_warmup_start", duration_secs=warmup_secs)
            time.sleep(warmup_secs)
            events.record("restored_warmup_end")

    # When re-crash is NOT enabled, run the normal restored phase.
    if not re_crash_enabled:
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
            with_stream=True,
        )

    verifier_result: dict[str, Any] | None = None
    if sentinel is not None and confirmed_writes:
        verifier_result = sentinel.verify(confirmed_writes)
        verifier_path = run_dir / "verifier-result.json"
        verifier_path.write_text(
            json.dumps(verifier_result, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        events.record(
            "sentinel_verify",
            verdict=verifier_result.get("verdict"),
            issues=len(verifier_result.get("issues", [])),
        )

    down_total_secs = recovery_start - fail_monotonic
    run_metrics = metrics_module.summary_from_k6_phase_exports(phase_exports)
    run_metrics.setdefault("fault", {})["node_index"] = args.fault_node
    run_metrics["fault"]["recovery_seconds"] = recovery_seconds
    run_metrics["fault"]["total_downtime_secs"] = down_total_secs
    run_metrics["fault"]["long_downtime_secs"] = long_downtime_secs

    if verifier_result is not None:
        run_metrics["fault"]["verifier_passed"] = (
            1.0 if verifier_result.get("verdict") == "passed" else 0.0
        )

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

    # Re-crash metrics
    if re_crash_enabled:
        run_metrics["fault"]["re_crash"] = True
        run_metrics["fault"]["re_crash_downtime_secs"] = re_crash_downtime_secs
        run_metrics["fault"]["re_crash_recovery_seconds"] = re_recovery_seconds
        if baseline_rate and re_recovery_stream is not None:
            re_crash_stab = metrics_timeseries.stabilization_time_secs(
                re_recovery_stream, baseline_rate=float(baseline_rate)
            )
            run_metrics["fault"]["re_crash_stabilization_secs"] = re_crash_stab

    return run_metrics
