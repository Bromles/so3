"""E2 fault safety scenario: concurrent fault injection during correctness workload."""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import faults
import manifest
from cluster import So3Cluster
from correctness_driver import CorrectnessDriver
from scripts.verify.verify_history import verify_history_file


@dataclass
class FaultCycleRecord:
    cycle: int
    node_index: int
    crash_monotonic: float
    restart_monotonic: float | None = None
    ready_monotonic: float | None = None

    @property
    def node_unavailable_secs(self) -> float | None:
        if self.ready_monotonic is None:
            return None
        return self.ready_monotonic - self.crash_monotonic

    def to_json(self) -> dict[str, Any]:
        return {
            "cycle": self.cycle,
            "node_index": self.node_index,
            "crash_monotonic": self.crash_monotonic,
            "restart_monotonic": self.restart_monotonic,
            "ready_monotonic": self.ready_monotonic,
            "node_unavailable_secs": self.node_unavailable_secs,
        }


class ConcurrentFaultInjector:
    """Crash and restart nodes round-robin while the correctness driver runs.

    At most one node is down at any time, preserving quorum for 3/5/7-node
    clusters. Each cycle: wait → crash → wait crash_duration → restart → record.
    """

    def __init__(
        self,
        cluster: So3Cluster,
        events: manifest.EventLog,
        fault_cycles: int,
        cycle_interval_secs: float,
        crash_duration_secs: float,
    ) -> None:
        self.cluster = cluster
        self.events = events
        self.fault_cycles = fault_cycles
        self.cycle_interval_secs = cycle_interval_secs
        self.crash_duration_secs = crash_duration_secs
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self.completed_cycles: list[FaultCycleRecord] = []
        self._lock = threading.Lock()

    def start(self) -> None:
        self._thread.start()

    def stop(self, timeout: float = 120.0) -> None:
        self._stop.set()
        self._thread.join(timeout=timeout)

    def _run(self) -> None:
        node_count = len(self.cluster.topology.nodes)
        for cycle in range(self.fault_cycles):
            if self._stop.wait(self.cycle_interval_secs):
                break

            node_index = (cycle % node_count) + 1
            record = FaultCycleRecord(
                cycle=cycle,
                node_index=node_index,
                crash_monotonic=time.monotonic(),
            )
            self.events.record("fault_crash", cycle=cycle, node_index=node_index)
            faults.crash_node(self.cluster, node_index)

            if self._stop.wait(self.crash_duration_secs):
                record.restart_monotonic = time.monotonic()
                try:
                    faults.restart_node(self.cluster, node_index)
                    record.ready_monotonic = time.monotonic()
                except Exception:
                    pass
                with self._lock:
                    self.completed_cycles.append(record)
                self.events.record("fault_restart", cycle=cycle, node_index=node_index)
                break

            record.restart_monotonic = time.monotonic()
            try:
                faults.restart_node(self.cluster, node_index)
                record.ready_monotonic = time.monotonic()
            except Exception:
                pass

            with self._lock:
                self.completed_cycles.append(record)
            self.events.record(
                "fault_restart",
                cycle=cycle,
                node_index=node_index,
                unavailable_secs=record.node_unavailable_secs,
            )

    def summary(self) -> dict[str, Any]:
        with self._lock:
            cycles = list(self.completed_cycles)
        unavailable = [c.node_unavailable_secs for c in cycles if c.node_unavailable_secs is not None]
        return {
            "fault_cycles_planned": self.fault_cycles,
            "fault_cycles_completed": len(cycles),
            "total_node_unavailable_secs": sum(unavailable),
            "mean_node_unavailable_secs": sum(unavailable) / len(unavailable) if unavailable else 0.0,
            "cycles": [c.to_json() for c in cycles],
        }


def run_e2_fault_safety(
    *,
    args: Any,
    cluster: So3Cluster,
    events: manifest.EventLog,
    run_dir: Path,
    topology_json: dict[str, Any],
    bucket: str,
    run_seed: int,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Run E2: correctness driver + concurrent fault injection.

    Returns (run_metrics, verifier_result).
    """
    history_path = run_dir / "client-history.jsonl"

    driver = CorrectnessDriver(
        entry_urls=topology_json["entry_urls"],
        history_path=history_path,
        bucket=bucket,
        seed=run_seed,
        operations=args.correctness_ops,
        concurrency=args.correctness_concurrency,
        object_size=args.object_size,
    )

    fault_cycles = getattr(args, "e2_fault_cycles", None) or args.node_count
    cycle_interval_secs = getattr(args, "e2_cycle_interval_secs", 10.0)
    crash_duration_secs = getattr(args, "e2_crash_duration_secs", 5.0)

    injector = ConcurrentFaultInjector(
        cluster=cluster,
        events=events,
        fault_cycles=fault_cycles,
        cycle_interval_secs=cycle_interval_secs,
        crash_duration_secs=crash_duration_secs,
    )

    events.record(
        "e2_start",
        fault_cycles=fault_cycles,
        cycle_interval_secs=cycle_interval_secs,
        crash_duration_secs=crash_duration_secs,
    )
    injector.start()
    try:
        run_metrics = driver.run()
    finally:
        injector.stop()
        events.record("e2_driver_done")

    cluster.wait_ready_all()
    events.record("e2_cluster_ready")

    verifier_result = verify_history_file(history_path)
    manifest.write_json(run_dir / "verifier-result.json", verifier_result)

    fault_summary = injector.summary()
    manifest.write_json(run_dir / "fault-cycles.json", fault_summary)

    run_metrics["verifier_passed"] = 1.0 if verifier_result["verdict"] == "passed" else 0.0
    run_metrics["unsupported_checks"] = float(len(verifier_result.get("unsupported", [])))
    run_metrics["fault"] = fault_summary

    events.record("e2_end", verdict=verifier_result["verdict"])
    return run_metrics, verifier_result
