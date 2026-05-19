"""Statistical aggregation for SO3 research benchmark runs."""

from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any, Iterable

import numpy as np
from scipy import stats as scipy_stats

DEFAULT_PERCENTILES = (10, 25, 50, 75, 90, 95, 99)


def descriptive_stats(
    values: Iterable[float],
    *,
    ci_confidence: float = 0.95,
) -> dict[str, float | int | None]:
    xs = np.asarray(list(values), dtype=float)
    n = int(xs.size)
    empty: dict[str, float | int | None] = {
        "n": 0,
        "mean": None,
        "ci_lower": None,
        "ci_upper": None,
        "ci_confidence": ci_confidence,
        "median": None,
        "variance": None,
        "stddev": None,
        "min": None,
        "max": None,
        "cv_percent": None,
        **{f"p{p}": None for p in DEFAULT_PERCENTILES if p != 50},
    }
    if n == 0:
        return empty

    mean = float(np.mean(xs))
    # Population variance/stddev (ddof=0) to stay consistent with prior behaviour.
    variance = float(np.var(xs, ddof=0))
    stddev = float(np.std(xs, ddof=0))

    if n >= 2:
        sem = float(scipy_stats.sem(xs))
        if sem == 0.0:
            ci_lower, ci_upper = mean, mean
        else:
            lo, hi = scipy_stats.t.interval(
                ci_confidence, df=n - 1, loc=mean, scale=sem
            )
            ci_lower, ci_upper = float(lo), float(hi)
            if math.isnan(ci_lower) or math.isnan(ci_upper):
                ci_lower, ci_upper = None, None
    else:
        ci_lower, ci_upper = None, None

    result: dict[str, float | int | None] = {
        "n": n,
        "mean": mean,
        "ci_lower": ci_lower,
        "ci_upper": ci_upper,
        "ci_confidence": ci_confidence,
        "median": float(np.median(xs)),
        "variance": variance,
        "stddev": stddev,
        "min": float(np.min(xs)),
        "max": float(np.max(xs)),
        "cv_percent": stddev / mean * 100.0 if mean != 0 else None,
    }
    for p in DEFAULT_PERCENTILES:
        if p == 50:
            continue
        result[f"p{p}"] = float(np.percentile(xs, p))
    return result


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
    successful = [s for s in summaries if s.get("status") == "passed"]
    failed = [s for s in summaries if s.get("status") != "passed"]

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
        "verdict": "no_runs"
        if not summaries
        else ("passed" if not failed else "failed"),
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
        "| metric | n | mean | 95% CI | median | stddev | cv % | p90 | p95 | p99 | min | max |",
        "| --- | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for name, stat in metrics.items():

        def fmt(key: str) -> str:
            v = stat.get(key)
            if v is None:
                return ""
            return str(v) if isinstance(v, int) else f"{v:.6g}"

        lo, hi = stat.get("ci_lower"), stat.get("ci_upper")
        ci_str = f"[{lo:.6g}, {hi:.6g}]" if lo is not None and hi is not None else ""

        lines.append(
            "| "
            + " | ".join(
                [
                    name,
                    fmt("n"),
                    fmt("mean"),
                    ci_str,
                    fmt("median"),
                    fmt("stddev"),
                    fmt("cv_percent"),
                    fmt("p90"),
                    fmt("p95"),
                    fmt("p99"),
                    fmt("min"),
                    fmt("max"),
                ]
            )
            + " |"
        )
    return "\n".join(lines)


def markdown_table(aggregate: dict[str, Any]) -> str:
    return markdown_table_for_metrics(aggregate.get("metrics", {}))
