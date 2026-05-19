"""Markdown report generation for research runs."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import stats

PHASE_ORDER = ("baseline", "degraded", "recovery", "restored")
K6_STREAM_LATENCY_METRICS = ("s3_put_ms", "s3_get_ms", "s3_head_ms", "s3_delete_ms")


def _format_number(value: Any, *, precision: int = 6) -> str:
    if value is None:
        return ""
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return f"{value:.{precision}g}"
    return str(value)


def _stat_mean(metric_stats: dict[str, Any] | None) -> Any:
    if not isinstance(metric_stats, dict):
        return None
    return metric_stats.get("mean")


def _ordered_keys(keys: set[str], preferred: tuple[str, ...]) -> list[str]:
    preferred_present = [key for key in preferred if key in keys]
    remaining = sorted(keys - set(preferred_present))
    return [*preferred_present, *remaining]


def _get_nested(payload: dict[str, Any], path: tuple[str, ...]) -> Any:
    current: Any = payload
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _get_number(payload: dict[str, Any], path: tuple[str, ...]) -> float | None:
    value = _get_nested(payload, path)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value)


def _load_run_summaries(result_dir: Path) -> list[dict[str, Any]]:
    summaries: list[dict[str, Any]] = []
    for path in sorted(result_dir.glob("run-*/summary.json")):
        try:
            with path.open(encoding="utf-8") as f:
                summaries.append(json.load(f))
        except (OSError, json.JSONDecodeError):
            continue
    return summaries


def _detect_scenario(summaries: list[dict[str, Any]]) -> str | None:
    for summary in summaries:
        scenario = summary.get("scenario")
        if isinstance(scenario, str):
            return scenario
    return None


def _descriptive_nested(
    summaries: list[dict[str, Any]], path: tuple[str, ...]
) -> dict[str, Any]:
    values = []
    for summary in summaries:
        value = _get_number(summary, path)
        if value is not None:
            values.append(value)
    return stats.descriptive_stats(values)


def _phase_summary_section(aggregate: dict[str, Any]) -> list[str]:
    phase_metrics = aggregate.get("phase_metrics", {})
    if not isinstance(phase_metrics, dict) or not phase_metrics:
        return []

    phases = _ordered_keys(set(phase_metrics), PHASE_ORDER)
    lines = ["## Scenario phase summary", ""]
    lines.extend(
        [
            "| phase | put p95 ms | put p99 ms | http req/s | success rate | timeout rate |",
            "| --- | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for phase in phases:
        metrics_for_phase = phase_metrics.get(phase, {})
        if not isinstance(metrics_for_phase, dict):
            continue
        lines.append(
            "| "
            + " | ".join(
                [
                    phase,
                    _format_number(
                        _stat_mean(metrics_for_phase.get("latency.put.p95_ms"))
                    ),
                    _format_number(
                        _stat_mean(metrics_for_phase.get("latency.put.p99_ms"))
                    ),
                    _format_number(
                        _stat_mean(metrics_for_phase.get("throughput.http_reqs.rate"))
                    ),
                    _format_number(
                        _stat_mean(metrics_for_phase.get("successes.s3_successes.rate"))
                    ),
                    _format_number(
                        _stat_mean(metrics_for_phase.get("errors.s3_timeouts.rate"))
                    ),
                ]
            )
            + " |"
        )
    lines.append("")

    relative_metrics = aggregate.get("relative_metrics", {})
    if isinstance(relative_metrics, dict) and relative_metrics:
        relative_phases = _ordered_keys(set(relative_metrics), PHASE_ORDER)
        lines.extend(
            [
                "### Normalized phase-vs-baseline summary",
                "",
                "| phase | throughput ratio | success ratio | timeout ratio | put p95 multiplier | get p95 multiplier |",
                "| --- | ---: | ---: | ---: | ---: | ---: |",
            ]
        )
        for phase in relative_phases:
            metrics_for_phase = relative_metrics.get(phase, {})
            if not isinstance(metrics_for_phase, dict):
                continue
            lines.append(
                "| "
                + " | ".join(
                    [
                        phase,
                        _format_number(
                            _stat_mean(
                                metrics_for_phase.get("throughput.http_reqs_rate_ratio")
                            )
                        ),
                        _format_number(
                            _stat_mean(
                                metrics_for_phase.get("success.s3_success_rate_ratio")
                            )
                        ),
                        _format_number(
                            _stat_mean(
                                metrics_for_phase.get("timeout.s3_timeout_rate_ratio")
                            )
                        ),
                        _format_number(
                            _stat_mean(
                                metrics_for_phase.get("latency.put.p95_multiplier")
                            )
                        ),
                        _format_number(
                            _stat_mean(
                                metrics_for_phase.get("latency.get.p95_multiplier")
                            )
                        ),
                    ]
                )
                + " |"
            )
        lines.append("")

    aggregate_metrics = aggregate.get("metrics", {})
    if isinstance(aggregate_metrics, dict):
        fault_rows = []
        for name, label in (
            ("fault.time_to_degraded_secs", "time to degraded / total downtime, s"),
            ("fault.recovery_seconds", "restart recovery command, s"),
            ("fault.long_downtime_secs", "configured long downtime, s"),
        ):
            mean = _stat_mean(aggregate_metrics.get(name))
            if mean is not None:
                fault_rows.append((label, mean))
        if fault_rows:
            lines.extend(
                [
                    "### Fault timing",
                    "",
                    "| metric | mean |",
                    "| --- | ---: |",
                ]
            )
            for label, mean in fault_rows:
                lines.append(f"| {label} | {_format_number(mean)} |")
            lines.append("")

    return lines


def _hot_key_section(summaries: list[dict[str, Any]]) -> list[str]:
    class_names: set[str] = set()
    for summary in summaries:
        key_class_metrics = _get_nested(summary, ("metrics", "key_class_metrics"))
        if isinstance(key_class_metrics, dict):
            class_names.update(str(key) for key in key_class_metrics)
    if not class_names:
        return []

    classes = _ordered_keys(class_names, ("hot", "independent"))
    lines = ["## Hot-key isolation summary", ""]
    lines.extend(
        [
            "| metric | hot p95 | independent p95 | hot / independent p95 |",
            "| --- | ---: | ---: | ---: |",
        ]
    )
    for metric in K6_STREAM_LATENCY_METRICS:
        hot = _descriptive_nested(
            summaries, ("metrics", "key_class_metrics", "hot", metric, "p95")
        ).get("mean")
        independent = _descriptive_nested(
            summaries,
            ("metrics", "key_class_metrics", "independent", metric, "p95"),
        ).get("mean")
        if hot is None and independent is None:
            continue
        ratio = None
        if isinstance(hot, (int, float)) and isinstance(independent, (int, float)):
            if float(independent) != 0.0:
                ratio = float(hot) / float(independent)
        lines.append(
            "| "
            + " | ".join(
                [
                    metric,
                    _format_number(hot),
                    _format_number(independent),
                    _format_number(ratio),
                ]
            )
            + " |"
        )
    lines.append("")

    ratio = _descriptive_nested(summaries, ("metrics", "hot_vs_independent_p95_ratio"))
    if ratio.get("n"):
        ratio_line = (
            "Explicit `s3_put_ms` hot/independent p95 ratio: "
            f"mean `{_format_number(ratio.get('mean'))}`"
        )
        ci_lower, ci_upper = ratio.get("ci_lower"), ratio.get("ci_upper")
        if ci_lower is not None and ci_upper is not None:
            ratio_line += (
                f", 95% CI `[{_format_number(ci_lower)}, {_format_number(ci_upper)}]`"
            )
        lines.extend([f"{ratio_line}.", ""])

    if len(classes) > 2:
        lines.extend(
            [f"Additional key classes observed: {', '.join(classes[2:])}.", ""]
        )
    return lines


def _node_total_samples(summary: dict[str, Any], node: str) -> float | None:
    node_metrics = _get_nested(summary, ("metrics", "entry_node_metrics", node))
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


def _node_distribution_section(summaries: list[dict[str, Any]]) -> list[str]:
    node_names: set[str] = set()
    for summary in summaries:
        node_metrics = _get_nested(summary, ("metrics", "entry_node_metrics"))
        if isinstance(node_metrics, dict):
            node_names.update(str(key) for key in node_metrics)
    if not node_names:
        return []

    node_totals: dict[str, dict[str, Any]] = {}
    for node in sorted(node_names):
        values = [
            value
            for summary in summaries
            if (value := _node_total_samples(summary, node)) is not None
        ]
        node_totals[node] = stats.descriptive_stats(values)
    total_mean = sum(float(stat.get("mean") or 0.0) for stat in node_totals.values())

    lines = ["## Per-node entry distribution", ""]
    lines.extend(
        [
            "| entry node | mean samples/run | share | put p95 ms | get p95 ms |",
            "| --- | ---: | ---: | ---: | ---: |",
        ]
    )
    for node in sorted(node_names):
        mean_samples = node_totals[node].get("mean")
        share = None
        if isinstance(mean_samples, (int, float)) and total_mean > 0.0:
            share = float(mean_samples) / total_mean
        put_p95 = _descriptive_nested(
            summaries, ("metrics", "entry_node_metrics", node, "s3_put_ms", "p95")
        ).get("mean")
        get_p95 = _descriptive_nested(
            summaries, ("metrics", "entry_node_metrics", node, "s3_get_ms", "p95")
        ).get("mean")
        lines.append(
            "| "
            + " | ".join(
                [
                    node,
                    _format_number(mean_samples),
                    f"{share:.2%}" if share is not None else "",
                    _format_number(put_p95),
                    _format_number(get_p95),
                ]
            )
            + " |"
        )
    lines.append("")
    return lines


def _server_consensus_section(aggregate: dict[str, Any]) -> list[str]:
    metrics = aggregate.get("metrics", {})
    if not isinstance(metrics, dict):
        return []
    total = _stat_mean(metrics.get("server.consensus.operations_total"))
    if total is None:
        return []

    lines = ["## Server-side consensus metrics", ""]
    lines.extend(
        [
            "| path | mean count/run | mean ratio |",
            "| --- | ---: | ---: |",
        ]
    )
    for path_name in ("fast", "slow", "recovery"):
        count = _stat_mean(metrics.get(f"server.consensus.path.{path_name}.count"))
        ratio = _stat_mean(metrics.get(f"server.consensus.path.{path_name}.ratio"))
        if count is None and ratio is None:
            continue
        lines.append(
            "| "
            + " | ".join([path_name, _format_number(count), _format_number(ratio)])
            + " |"
        )
    lines.append("")

    detail_rows = []
    for metric_name, label in (
        ("server.consensus.quorum.mean", "quorum mean"),
        ("server.consensus.participating_replicas.mean", "participating replicas mean"),
        ("server.consensus.pre_accept_ms.mean", "pre-accept mean, ms"),
        ("server.consensus.accept_ms.mean", "accept mean, ms"),
        ("server.consensus.commit_ms.mean", "commit mean, ms"),
        ("server.consensus.apply_ms.mean", "apply mean, ms"),
        ("server.consensus.recover_ms.mean", "recover mean, ms"),
        ("server.consensus.total_ms.mean", "operation total mean, ms"),
        ("server.consensus.quorum_wait_ms.mean", "quorum wait mean, ms"),
        ("server.consensus.retry_count.mean", "retry count mean"),
        ("server.consensus.commit_attempts.mean", "commit attempts mean"),
        ("server.consensus.commit_ok.mean", "commit quorum responses mean"),
        ("server.consensus.in_flight_operations.max", "max in-flight operations"),
        ("server.consensus.dependency_count.mean", "dependency count mean"),
        ("server.consensus.dependency_count.max", "dependency count max"),
        ("server.consensus.dependency_depth.mean", "dependency depth mean"),
        ("server.consensus.dependency_depth.max", "dependency depth max"),
        ("server.consensus.pre_accept_failures.total", "pre-accept failures total"),
        ("server.consensus.recovery_response_count.mean", "recovery responses mean"),
        ("server.consensus.recovery_wait_for_count.mean", "recovery wait-for mean"),
        (
            "server.consensus.recovery_superseding_count.mean",
            "recovery superseding mean",
        ),
    ):
        value = _stat_mean(metrics.get(metric_name))
        if value is not None:
            detail_rows.append((label, value))
    if detail_rows:
        lines.extend(
            ["### Consensus details", "", "| metric | mean |", "| --- | ---: |"]
        )
        for label, value in detail_rows:
            lines.append(f"| {label} | {_format_number(value)} |")
        lines.append("")
    return lines


def _server_apply_section(aggregate: dict[str, Any]) -> list[str]:
    metrics = aggregate.get("metrics", {})
    if not isinstance(metrics, dict):
        return []
    total = _stat_mean(metrics.get("server.apply.events_total"))
    if total is None:
        return []

    rows = []
    for metric_name, label in (
        ("server.apply.commit_reorder_buffer_size.max", "max commit reorder buffer"),
        (
            "server.apply.apply_reorder_buffer_size_start.max",
            "max apply reorder buffer at start",
        ),
        (
            "server.apply.apply_reorder_buffer_size_end.max",
            "max apply reorder buffer at end",
        ),
        ("server.apply.earlier_blocking_count.max", "max earlier blocking commands"),
        ("server.apply.explicit_dependency_count.mean", "explicit dependencies mean"),
        ("server.apply.pending_dependency_count.max", "max pending dependencies"),
        ("server.apply.reorder_wait_ms.mean", "reorder wait mean, ms"),
        ("server.apply.dependency_wait_ms.mean", "dependency wait mean, ms"),
        ("server.apply.apply_total_ms.mean", "inbound apply total mean, ms"),
    ):
        value = _stat_mean(metrics.get(metric_name))
        if value is not None:
            rows.append((label, value))
    if not rows:
        return []

    lines = ["## Server-side apply backlog metrics", ""]
    lines.extend(["| metric | mean/max |", "| --- | ---: |"])
    for label, value in rows:
        lines.append(f"| {label} | {_format_number(value)} |")
    lines.append("")
    return lines


def _plot_links_section(result_dir: Path) -> list[str]:
    plots_dir = result_dir / "plots"
    if not plots_dir.exists():
        return []
    plots = sorted(plots_dir.glob("*.png"))
    if not plots:
        return []
    lines = ["## Plots", ""]
    for path in plots:
        rel = path.relative_to(result_dir)
        title = path.stem.replace("_", " ")
        lines.append(f"- [{title}]({rel.as_posix()})")
    lines.append("")
    return lines


def write_report(result_dir: Path, aggregate: dict[str, Any]) -> Path:
    summaries = _load_run_summaries(result_dir)
    scenario = _detect_scenario(summaries)
    report_path = result_dir / "report.md"
    lines = [
        "# SO3 research scenario report",
        "",
        f"Scenario: `{scenario or 'unknown'}`",
        f"Verdict: `{aggregate.get('verdict', 'unknown')}`",
        "",
        "## Runs",
        "",
        f"- total: {aggregate.get('runs_total', 0)}",
        f"- successful: {aggregate.get('runs_successful', 0)}",
        f"- failed: {aggregate.get('runs_failed', 0)}",
        "",
    ]

    failed_reasons = aggregate.get("failed_reasons", {})
    if failed_reasons:
        lines.extend(["## Failed runs", ""])
        for reason, count in sorted(failed_reasons.items()):
            lines.append(f"- `{reason}`: {count}")
        lines.append("")

    if scenario in {"e3-degradation", "e6-recovery"}:
        lines.extend(_phase_summary_section(aggregate))
    elif scenario == "e4-hot-key":
        lines.extend(_hot_key_section(summaries))
    elif scenario == "e5-leaderless":
        lines.extend(_node_distribution_section(summaries))

    lines.extend(_server_consensus_section(aggregate))
    lines.extend(_server_apply_section(aggregate))
    lines.extend(_plot_links_section(result_dir))

    lines.extend(
        ["## Aggregated numeric metrics", "", stats.markdown_table(aggregate), ""]
    )

    phase_metrics = aggregate.get("phase_metrics", {})
    if phase_metrics:
        lines.extend(["## Phase-aware metrics", ""])
        for phase, metrics in sorted(phase_metrics.items()):
            lines.extend(
                [f"### `{phase}`", "", stats.markdown_table_for_metrics(metrics), ""]
            )

    relative_metrics = aggregate.get("relative_metrics", {})
    if relative_metrics:
        lines.extend(["## Normalized phase-vs-baseline metrics", ""])
        for phase, metrics in sorted(relative_metrics.items()):
            lines.extend(
                [f"### `{phase}`", "", stats.markdown_table_for_metrics(metrics), ""]
            )

    report_path.write_text("\n".join(lines), encoding="utf-8")
    return report_path
