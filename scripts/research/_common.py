"""Shared read-only helpers used by both plot.py and report.py."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def get_nested(payload: dict[str, Any], path: tuple[str, ...]) -> Any:
    current: Any = payload
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def get_number(payload: dict[str, Any], path: tuple[str, ...]) -> float | None:
    value = get_nested(payload, path)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value)


def load_run_summaries(result_dir: Path) -> list[dict[str, Any]]:
    summaries: list[dict[str, Any]] = []
    for path in sorted(result_dir.glob("run-*/summary.json")):
        try:
            with path.open(encoding="utf-8") as f:
                summaries.append(json.load(f))
        except (OSError, json.JSONDecodeError):
            continue
    return summaries


def detect_scenario(summaries: list[dict[str, Any]]) -> str | None:
    for summary in summaries:
        scenario = summary.get("scenario")
        if isinstance(scenario, str):
            return scenario
    return None


def node_total_samples(summary: dict[str, Any], node: str) -> float | None:
    node_metrics = get_nested(summary, ("metrics", "entry_node_metrics", node))
    if not isinstance(node_metrics, dict):
        return None
    total = 0.0
    for metric_stats in node_metrics.values():
        if not isinstance(metric_stats, dict):
            continue
        count = metric_stats.get("n")
        if isinstance(count, (int, float)):
            total += float(count)
    return total if total > 0.0 else None
