"""Statistical aggregation for SO3 research benchmark runs."""

from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any, Iterable

DEFAULT_PERCENTILES = (10, 25, 50, 75, 90, 95, 99)


def percentile(values: Iterable[float], p: float) -> float | None:
    """Return a linearly interpolated percentile for p in [0, 100]."""

    xs = sorted(float(value) for value in values)
    if not xs:
        return None
    if p <= 0:
        return xs[0]
    if p >= 100:
        return xs[-1]
    rank = (len(xs) - 1) * (p / 100.0)
    lo = math.floor(rank)
    hi = math.ceil(rank)
    if lo == hi:
        return xs[lo]
    weight = rank - lo
    return xs[lo] * (1.0 - weight) + xs[hi] * weight


def descriptive_stats(values: Iterable[float]) -> dict[str, float | int | None]:
    xs = [float(value) for value in values]
    n = len(xs)
    if n == 0:
        return {
            "n": 0,
            "mean": None,
            "median": None,
            "variance": None,
            "stddev": None,
            "min": None,
            "max": None,
            "cv_percent": None,
            "p10": None,
            "p25": None,
            "p75": None,
            "p90": None,
            "p95": None,
            "p99": None,
        }

    mean = sum(xs) / n
    variance = sum((value - mean) ** 2 for value in xs) / n
    stddev = math.sqrt(max(0.0, variance))
    result: dict[str, float | int | None] = {
        "n": n,
        "mean": mean,
        "median": percentile(xs, 50),
        "variance": variance,
        "stddev": stddev,
        "min": min(xs),
        "max": max(xs),
        "cv_percent": stddev / mean * 100.0 if mean != 0 else None,
    }
    for p in DEFAULT_PERCENTILES:
        if p == 50:
            continue
        result[f"p{p}"] = percentile(xs, p)
    return result


def safe_ratio(
    numerator: float | int | None, denominator: float | int | None
) -> float | None:
    if numerator is None or denominator is None:
        return None
    denominator = float(denominator)
    if denominator == 0.0:
        return None
    return float(numerator) / denominator


def flatten_numeric_values(
    payload: dict[str, Any], prefix: str = ""
) -> dict[str, float]:
    """Flatten numeric leaves into dotted metric names."""

    values: dict[str, float] = {}
    for key, value in payload.items():
        metric_name = f"{prefix}.{key}" if prefix else key
        if isinstance(value, bool):
            continue
        if isinstance(value, (int, float)) and math.isfinite(float(value)):
            values[metric_name] = float(value)
        elif isinstance(value, dict):
            values.update(flatten_numeric_values(value, metric_name))
    return values


def nested_numeric_buckets(payload: dict[str, Any]) -> dict[str, list[float]]:
    buckets: dict[str, list[float]] = {}
    for metric_name, value in flatten_numeric_values(payload).items():
        buckets.setdefault(metric_name, []).append(value)
    return buckets


def merge_numeric_buckets(
    target: dict[str, list[float]], source: dict[str, list[float]]
) -> None:
    for metric_name, values in source.items():
        target.setdefault(metric_name, []).extend(values)


def stats_by_bucket(buckets: dict[str, list[float]]) -> dict[str, Any]:
    return {
        metric_name: descriptive_stats(values)
        for metric_name, values in sorted(buckets.items())
    }


def aggregate_run_summaries(summaries: list[dict[str, Any]]) -> dict[str, Any]:
    successful = [summary for summary in summaries if summary.get("status") == "passed"]
    failed = [summary for summary in summaries if summary.get("status") != "passed"]

    by_metric: dict[str, list[float]] = {}
    by_phase: dict[str, dict[str, list[float]]] = {}
    by_relative_phase: dict[str, dict[str, list[float]]] = {}
    for summary in successful:
        summary_metrics = summary.get("metrics", {})
        for metric_name, value in flatten_numeric_values(summary_metrics).items():
            by_metric.setdefault(metric_name, []).append(value)

        phases = summary_metrics.get("phases", {})
        if isinstance(phases, dict):
            for phase, phase_metrics in phases.items():
                if isinstance(phase_metrics, dict):
                    merge_numeric_buckets(
                        by_phase.setdefault(str(phase), {}),
                        nested_numeric_buckets(phase_metrics),
                    )

        relative = summary_metrics.get("relative", {})
        if isinstance(relative, dict):
            for phase, phase_metrics in relative.items():
                if isinstance(phase_metrics, dict):
                    merge_numeric_buckets(
                        by_relative_phase.setdefault(str(phase), {}),
                        nested_numeric_buckets(phase_metrics),
                    )

    failed_reasons: dict[str, int] = {}
    for summary in failed:
        reason = str(summary.get("error") or summary.get("status") or "unknown")
        failed_reasons[reason] = failed_reasons.get(reason, 0) + 1

    return {
        "schema_version": 1,
        "runs_total": len(summaries),
        "runs_successful": len(successful),
        "runs_failed": len(failed),
        "failed_reasons": failed_reasons,
        "verdict": "no_runs" if not summaries else ("passed" if not failed else "failed"),
        "metrics": stats_by_bucket(by_metric),
        "phase_metrics": {
            phase: stats_by_bucket(buckets)
            for phase, buckets in sorted(by_phase.items())
        },
        "relative_metrics": {
            phase: stats_by_bucket(buckets)
            for phase, buckets in sorted(by_relative_phase.items())
        },
    }


def load_run_summaries(result_dir: Path) -> list[dict[str, Any]]:
    summaries: list[dict[str, Any]] = []
    for path in sorted(result_dir.glob("run-*/summary.json")):
        with path.open(encoding="utf-8") as f:
            summaries.append(json.load(f))
    return summaries


def write_aggregate_summary(result_dir: Path) -> dict[str, Any]:
    aggregate = aggregate_run_summaries(load_run_summaries(result_dir))
    (result_dir / "aggregate-summary.json").write_text(
        json.dumps(aggregate, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return aggregate


def markdown_table_for_metrics(metrics: dict[str, Any]) -> str:
    lines = [
        "| metric | n | mean | median | stddev | variance | min | max | p90 | p95 | p99 | cv % |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for name, stat in metrics.items():

        def fmt(key: str) -> str:
            value = stat.get(key)
            if value is None:
                return ""
            if isinstance(value, int):
                return str(value)
            return f"{value:.6g}"

        lines.append(
            "| "
            + " | ".join(
                [
                    name,
                    fmt("n"),
                    fmt("mean"),
                    fmt("median"),
                    fmt("stddev"),
                    fmt("variance"),
                    fmt("min"),
                    fmt("max"),
                    fmt("p90"),
                    fmt("p95"),
                    fmt("p99"),
                    fmt("cv_percent"),
                ]
            )
            + " |"
        )
    return "\n".join(lines)


def markdown_table(aggregate: dict[str, Any]) -> str:
    return markdown_table_for_metrics(aggregate.get("metrics", {}))
