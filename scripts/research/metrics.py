"""Metric normalization from raw k6 exports into per-run summaries."""

from __future__ import annotations

import json
import shlex
from pathlib import Path
from typing import Any

CONSENSUS_PATHS = ("fast", "slow", "recovery")

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
    # k6 summary-export formats differ across versions. Some versions store
    # metric values directly under the metric name instead of nesting them in a
    # `values` object, e.g. `{"s3_put_ms": {"avg": ..., "p(95)": ...}}`.
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
    return summary_from_k6_payload(k6)


def summary_from_k6_payload(k6: dict[str, Any]) -> dict[str, Any]:
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


def safe_divide(
    numerator: float | int | None, denominator: float | int | None
) -> float | None:
    if numerator is None or denominator is None:
        return None
    denominator = float(denominator)
    if denominator == 0.0:
        return None
    return float(numerator) / denominator


def _get_number(payload: dict[str, Any], path: tuple[str, ...]) -> float | None:
    current: Any = payload
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    if isinstance(current, bool) or not isinstance(current, (int, float)):
        return None
    return float(current)


def _put_number(
    payload: dict[str, Any], path: tuple[str, ...], value: float | None
) -> None:
    if value is None:
        return
    current = payload
    for key in path[:-1]:
        current = current.setdefault(key, {})
    current[path[-1]] = value


def relative_to_baseline(phases: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """Compute phase-vs-baseline normalized metrics for k6 phase summaries."""

    baseline = phases.get("baseline")
    if not baseline:
        return {}

    relative: dict[str, Any] = {}
    baseline_http_rate = _get_number(baseline, ("throughput", "http_reqs", "rate"))
    baseline_success_rate = _get_number(baseline, ("successes", "s3_successes", "rate"))
    baseline_timeout_rate = _get_number(baseline, ("errors", "s3_timeouts", "rate"))

    for phase, phase_metrics in sorted(phases.items()):
        if phase == "baseline":
            continue
        phase_relative: dict[str, Any] = {}
        _put_number(
            phase_relative,
            ("throughput", "http_reqs_rate_ratio"),
            safe_divide(
                _get_number(phase_metrics, ("throughput", "http_reqs", "rate")),
                baseline_http_rate,
            ),
        )
        _put_number(
            phase_relative,
            ("success", "s3_success_rate_ratio"),
            safe_divide(
                _get_number(phase_metrics, ("successes", "s3_successes", "rate")),
                baseline_success_rate,
            ),
        )
        _put_number(
            phase_relative,
            ("timeout", "s3_timeout_rate_ratio"),
            safe_divide(
                _get_number(phase_metrics, ("errors", "s3_timeouts", "rate")),
                baseline_timeout_rate,
            ),
        )
        for op in ("put", "get", "head", "delete"):
            _put_number(
                phase_relative,
                ("latency", op, "p95_multiplier"),
                safe_divide(
                    _get_number(phase_metrics, ("latency", op, "p95_ms")),
                    _get_number(baseline, ("latency", op, "p95_ms")),
                ),
            )
            _put_number(
                phase_relative,
                ("latency", op, "p99_multiplier"),
                safe_divide(
                    _get_number(phase_metrics, ("latency", op, "p99_ms")),
                    _get_number(baseline, ("latency", op, "p99_ms")),
                ),
            )
        if phase_relative:
            relative[phase] = phase_relative
    return relative


def summary_from_k6_phase_exports(phase_exports: dict[str, Path]) -> dict[str, Any]:
    phases = {
        phase: summary_from_k6_export(path)
        for phase, path in sorted(phase_exports.items())
    }
    return {
        "phases": phases,
        "relative": relative_to_baseline(phases),
    }


def _parse_log_value(value: str) -> str | int | float:
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        return value


def _parse_tracing_fields(line: str) -> dict[str, str | int | float]:
    fields: dict[str, str | int | float] = {}
    try:
        parts = shlex.split(line)
    except ValueError:
        return fields
    for part in parts:
        if "=" not in part:
            continue
        key, value = part.split("=", 1)
        if not key:
            continue
        fields[key] = _parse_log_value(value)
    return fields


def _inc(counter: dict[str, int], key: str) -> None:
    counter[key] = counter.get(key, 0) + 1


def _safe_ratio(numerator: int, denominator: int) -> float | None:
    if denominator == 0:
        return None
    return numerator / denominator


def summary_from_cluster_log(path: Path) -> dict[str, Any]:
    """Aggregate structured consensus coordination events from cluster.log."""
    if not path.exists():
        return {}

    path_counts: dict[str, int] = {}
    operation_counts: dict[str, int] = {}
    node_counts: dict[str, dict[str, int]] = {
        "coordinator_node": {},
        "origin_node": {},
    }
    numeric_buckets: dict[str, list[float]] = {
        "dependency_count": [],
        "dependency_depth": [],
        "pre_accept_failures": [],
        "pre_accept_ok": [],
        "accept_ok": [],
        "quorum": [],
        "participating_replicas": [],
        "pre_accept_ms": [],
        "accept_ms": [],
        "commit_ms": [],
        "apply_ms": [],
        "recover_ms": [],
        "total_ms": [],
        "quorum_wait_ms": [],
        "retry_count": [],
        "commit_attempts": [],
        "commit_ok": [],
        "in_flight_operations": [],
        "recovery_response_count": [],
        "recovery_wait_for_count": [],
        "recovery_superseding_count": [],
    }
    apply_event_counts: dict[str, int] = {}
    apply_operation_counts: dict[str, int] = {}
    apply_node_counts: dict[str, dict[str, int]] = {
        "node": {},
        "origin_node": {},
    }
    apply_numeric_buckets: dict[str, list[float]] = {
        "commit_dependency_count": [],
        "commit_reorder_buffer_size": [],
        "apply_reorder_buffer_size_start": [],
        "apply_reorder_buffer_size_end": [],
        "earlier_blocking_count": [],
        "explicit_dependency_count": [],
        "pending_dependency_count": [],
        "reorder_wait_iterations": [],
        "dependency_wait_iterations": [],
        "reorder_wait_ms": [],
        "dependency_wait_ms": [],
        "journal_apply_ms": [],
        "metadata_apply_ms": [],
        "apply_total_ms": [],
    }

    with path.open(encoding="utf-8", errors="replace") as f:
        for line in f:
            if "coordination_event" not in line:
                continue
            fields = _parse_tracing_fields(line)
            event_name = fields.get("coordination_event")
            if event_name == "consensus_operation":
                path_name = fields.get("consensus_path")
                if isinstance(path_name, str):
                    _inc(path_counts, path_name)
                operation = fields.get("operation")
                if isinstance(operation, str):
                    _inc(operation_counts, operation)
                for name, counter in node_counts.items():
                    value = fields.get(name)
                    if isinstance(value, str):
                        _inc(counter, value)
                for name, bucket in numeric_buckets.items():
                    value = fields.get(name)
                    if isinstance(value, (int, float)):
                        bucket.append(float(value))
            elif event_name == "apply_backlog":
                backlog_event = fields.get("backlog_event")
                if isinstance(backlog_event, str):
                    _inc(apply_event_counts, backlog_event)
                operation = fields.get("operation")
                if isinstance(operation, str):
                    _inc(apply_operation_counts, operation)
                for name, counter in apply_node_counts.items():
                    value = fields.get(name)
                    if isinstance(value, str):
                        _inc(counter, value)
                for name, bucket in apply_numeric_buckets.items():
                    value = fields.get(name)
                    if isinstance(value, (int, float)):
                        bucket.append(float(value))

    result: dict[str, Any] = {"server": {}}

    total = sum(path_counts.values())
    if total > 0:
        path_metrics = {}
        for path_name in sorted({*CONSENSUS_PATHS, *path_counts}):
            count = path_counts.get(path_name, 0)
            path_metrics[path_name] = {
                "count": count,
                "ratio": _safe_ratio(count, total),
            }

        result["server"]["consensus"] = {
            "operations_total": total,
            "path": path_metrics,
            "operation": {
                name: {"count": count, "ratio": _safe_ratio(count, total)}
                for name, count in sorted(operation_counts.items())
            },
            **{
                name: {
                    node: {"count": count, "ratio": _safe_ratio(count, total)}
                    for node, count in sorted(counter.items())
                }
                for name, counter in node_counts.items()
                if counter
            },
            **{
                name: {
                    "mean": sum(values) / len(values),
                    "max": max(values),
                    "total": sum(values),
                }
                for name, values in numeric_buckets.items()
                if values
            },
        }

    apply_total = sum(apply_event_counts.values())
    if apply_total > 0:
        result["server"]["apply"] = {
            "events_total": apply_total,
            "event": {
                name: {"count": count, "ratio": _safe_ratio(count, apply_total)}
                for name, count in sorted(apply_event_counts.items())
            },
            "operation": {
                name: {"count": count, "ratio": _safe_ratio(count, apply_total)}
                for name, count in sorted(apply_operation_counts.items())
            },
            **{
                name: {
                    node: {"count": count, "ratio": _safe_ratio(count, apply_total)}
                    for node, count in sorted(counter.items())
                }
                for name, counter in apply_node_counts.items()
                if counter
            },
            **{
                name: {
                    "mean": sum(values) / len(values),
                    "max": max(values),
                    "total": sum(values),
                }
                for name, values in apply_numeric_buckets.items()
                if values
            },
        }

    return result if result["server"] else {}


def merge_metrics(target: dict[str, Any], source: dict[str, Any]) -> None:
    for key, value in source.items():
        if isinstance(value, dict) and isinstance(target.get(key), dict):
            merge_metrics(target[key], value)
        else:
            target[key] = value


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
