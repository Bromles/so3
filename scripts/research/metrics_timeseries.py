"""Parse k6 --out json JSONL stream and aggregate per-tag metrics."""

from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any

import stats as stats_module

K6_LATENCY_METRICS = {"s3_put_ms", "s3_get_ms", "s3_head_ms", "s3_delete_ms"}
K6_RATE_METRICS = {"s3_errors", "s3_timeouts", "s3_successes"}


def _tag_key(record: dict[str, Any], tag_key: str) -> str | None:
    tags = record.get("data", {}).get("tags", {})
    if not isinstance(tags, dict):
        return None
    value = tags.get(tag_key)
    return str(value) if value is not None else None


def parse_k6_stream(
    path: Path,
    *,
    tag_key: str,
    metric_names: set[str] | None = None,
) -> dict[str, dict[str, list[float]]]:
    """Read k6 --out json JSONL and bucket data points by tag value.

    Returns ``{tag_value: {metric_name: [values...]}}``.
    Only ``Point`` records are considered. Skips lines that are not valid JSON.
    """
    buckets: dict[str, dict[str, list[float]]] = {}
    want = metric_names or (K6_LATENCY_METRICS | K6_RATE_METRICS)

    with path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            if record.get("type") != "Point":
                continue
            metric = record.get("metric")
            if metric not in want:
                continue
            value = record.get("data", {}).get("value")
            if not isinstance(value, (int, float)) or not math.isfinite(float(value)):
                continue
            tag_value = _tag_key(record, tag_key)
            if tag_value is None:
                tag_value = "__untagged__"
            buckets.setdefault(tag_value, {}).setdefault(metric, []).append(float(value))

    return buckets


def tag_stats(
    path: Path,
    *,
    tag_key: str,
    metric_names: set[str] | None = None,
) -> dict[str, dict[str, dict[str, Any]]]:
    """Aggregate per-tag descriptive stats from a k6 JSONL stream.

    Returns ``{tag_value: {metric_name: {descriptive stats}}}``.
    """
    buckets = parse_k6_stream(path, tag_key=tag_key, metric_names=metric_names)
    return {
        tag_value: {
            metric: stats_module.descriptive_stats(values)
            for metric, values in sorted(metric_buckets.items())
        }
        for tag_value, metric_buckets in sorted(buckets.items())
    }


def hot_vs_independent_metrics(stream_path: Path) -> dict[str, Any]:
    """Return latency stats for 'hot' and 'independent' key classes.

    Used by E4 to compare hot-key degradation against independent keys.
    Returns ``{key_class: {metric: {stats}}}``.
    """
    return tag_stats(stream_path, tag_key="key_class", metric_names=K6_LATENCY_METRICS)


def per_node_entry_metrics(stream_path: Path) -> dict[str, Any]:
    """Return latency stats per entry_node.

    Used by E5 to verify symmetric load distribution across nodes.
    Returns ``{node_name: {metric: {stats}}}``.
    """
    return tag_stats(stream_path, tag_key="entry_node", metric_names=K6_LATENCY_METRICS)


def latency_ratio(
    tag_stats_result: dict[str, dict[str, dict[str, Any]]],
    numerator_tag: str,
    denominator_tag: str,
    metric: str = "s3_put_ms",
    stat: str = "p95",
) -> float | None:
    """Ratio of a single stat between two tag values (e.g. hot/independent p95)."""
    num = tag_stats_result.get(numerator_tag, {}).get(metric, {}).get(stat)
    den = tag_stats_result.get(denominator_tag, {}).get(metric, {}).get(stat)
    if num is None or den is None or float(den) == 0.0:
        return None
    return float(num) / float(den)
