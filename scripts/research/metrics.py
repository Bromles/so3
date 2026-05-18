"""Metric normalization from raw k6 exports into per-run summaries."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

K6_LATENCY_STATS = {
    "avg": "avg_ms",
    "med": "median_ms",
    "min": "min_ms",
    "max": "max_ms",
    "p(90)": "p90_ms",
    "p(95)": "p95_ms",
    "p(99)": "p99_ms",
}


def k6_metric_values(k6: dict[str, Any], metric_name: str) -> dict[str, Any]:
    metric = k6.get("metrics", {}).get(metric_name, {})
    if not isinstance(metric, dict):
        return {}
    values = metric.get("values")
    if isinstance(values, dict):
        return values
    return metric


def normalize_trend(k6: dict[str, Any], metric_name: str) -> dict[str, float]:
    values = k6_metric_values(k6, metric_name)
    normalized: dict[str, float] = {}
    for source, target in K6_LATENCY_STATS.items():
        value = values.get(source)
        if isinstance(value, (int, float)):
            normalized[target] = float(value)
    return normalized


def normalize_rate(k6: dict[str, Any], metric_name: str) -> dict[str, float]:
    values = k6_metric_values(k6, metric_name)
    result: dict[str, float] = {}
    rate = values.get("rate", values.get("value"))
    if isinstance(rate, (int, float)):
        result["rate"] = float(rate)
    for key in ("passes", "fails"):
        value = values.get(key)
        if isinstance(value, (int, float)):
            result[key] = float(value)
    return result


def normalize_counter(k6: dict[str, Any], metric_name: str) -> dict[str, float]:
    values = k6_metric_values(k6, metric_name)
    result: dict[str, float] = {}
    for key in ("count", "rate"):
        value = values.get(key)
        if isinstance(value, (int, float)):
            result[key] = float(value)
    return result


def summary_from_k6_export(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as f:
        k6 = json.load(f)

    metrics: dict[str, Any] = {
        "latency": {},
        "throughput": {},
        "errors": {},
    }
    for op, metric_name in (
        ("put", "s3_put_ms"),
        ("get", "s3_get_ms"),
        ("head", "s3_head_ms"),
        ("delete", "s3_delete_ms"),
    ):
        trend = normalize_trend(k6, metric_name)
        if trend:
            metrics["latency"][op] = trend

    http_reqs = normalize_counter(k6, "http_reqs")
    if http_reqs:
        metrics["throughput"]["http_reqs"] = http_reqs

    s3_errors = normalize_rate(k6, "s3_errors")
    if s3_errors:
        metrics["errors"]["s3_errors"] = s3_errors

    s3_timeouts = normalize_rate(k6, "s3_timeouts")
    if s3_timeouts:
        metrics["errors"]["s3_timeouts"] = s3_timeouts

    s3_successes = normalize_rate(k6, "s3_successes")
    if s3_successes:
        metrics["successes"] = {"s3_successes": s3_successes}

    duration_ms = k6.get("state", {}).get("testRunDurationMs")
    if isinstance(duration_ms, (int, float)):
        metrics.setdefault("duration", {})["test_run_seconds"] = (
            float(duration_ms) / 1000.0
        )

    return metrics


def write_run_summary(
    path: Path,
    *,
    scenario: str,
    run_index: int,
    status: str,
    metrics: dict[str, Any] | None = None,
    error: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_version": 1,
        "scenario": scenario,
        "run_index": run_index,
        "status": status,
        "metrics": metrics or {},
    }
    if error:
        payload["error"] = error
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return payload
