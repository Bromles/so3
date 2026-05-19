"""Plot generation for SO3 research scenario outputs."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from _common import (
    detect_scenario as _detect_scenario,
)
from _common import (
    get_nested as _get_nested,
)
from _common import (
    get_number as _get_number,
)
from _common import (
    load_run_summaries as _load_run_summaries,
)
from _common import (
    node_total_samples as _node_total_samples,
)

PHASE_ORDER = (
    "baseline",
    "degraded",
    "recovery",
    "re_crash_degraded",
    "re_recovery",
    "restored",
    "re_restored",
)
K6_STREAM_LATENCY_METRICS = ("s3_put_ms", "s3_get_ms", "s3_head_ms", "s3_delete_ms")
TIMELINE_EVENT_LABELS = {
    "fail": "fail",
    "degraded_start": "degraded",
    "recover": "recover",
    "normal_restored": "restored",
    "restored_start": "restored",
    "re_crash": "re-crash",
    "re_recovery": "re-recover",
    "normal_re_restored": "re-restored",
}
TIMELINE_FALLBACK_POSITIONS = {
    "fail": 0.5,
    "degraded": 1.0,
    "recover": 2.0,
    "restored": 3.0,
    "re_crash": 3.5,
    "re_recovery": 4.5,
    "re_restored": 5.0,
}


def _pyplot() -> Any:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    return plt


def _mean(values: list[float]) -> float | None:
    return sum(values) / len(values) if values else None


def _mean_nested(
    summaries: list[dict[str, Any]], path: tuple[str, ...]
) -> float | None:
    values = []
    for summary in summaries:
        value = _get_number(summary, path)
        if value is not None:
            values.append(value)
    return _mean(values)


def _ordered_phases(phases: set[str], *, include_baseline: bool) -> list[str]:
    preferred = [
        phase for phase in PHASE_ORDER if include_baseline or phase != "baseline"
    ]
    ordered = [phase for phase in preferred if phase in phases]
    ordered.extend(sorted(phases - set(ordered)))
    return ordered


def _non_none_pairs(
    summaries: list[dict[str, Any]], path: tuple[str, ...]
) -> tuple[list[int], list[float]]:
    xs: list[int] = []
    ys: list[float] = []
    for fallback_index, summary in enumerate(summaries, start=1):
        value = _get_number(summary, path)
        if value is None:
            continue
        run_index = summary.get("run_index")
        xs.append(int(run_index) if isinstance(run_index, int) else fallback_index)
        ys.append(value)
    return xs, ys


def _save(fig: Any, path: Path) -> Path:
    fig.tight_layout()
    fig.savefig(path, dpi=160)
    return path


def _plot_repeatability(
    plt: Any, summaries: list[dict[str, Any]], plots_dir: Path, scenario: str | None
) -> Path | None:
    if not summaries:
        return None

    if scenario in {"e3-degradation", "e6-recovery"}:
        latency_path = (
            "metrics",
            "relative",
            "degraded",
            "latency",
            "put",
            "p95_multiplier",
        )
        throughput_path = (
            "metrics",
            "relative",
            "degraded",
            "throughput",
            "http_reqs_rate_ratio",
        )
        latency_label = "degraded put p95 multiplier"
        throughput_label = "degraded throughput ratio"
    else:
        latency_path = ("metrics", "latency", "put", "p95_ms")
        throughput_path = ("metrics", "throughput", "http_reqs", "rate")
        latency_label = "put p95 ms"
        throughput_label = "http req/s"

    latency_x, latency_y = _non_none_pairs(summaries, latency_path)
    throughput_x, throughput_y = _non_none_pairs(summaries, throughput_path)
    if not latency_y and not throughput_y:
        return None

    fig, axes = plt.subplots(2, 1, figsize=(9, 6), sharex=True)
    if latency_y:
        axes[0].plot(latency_x, latency_y, marker="o", linewidth=1.5)
        axes[0].set_ylabel(latency_label)
        axes[0].grid(True, alpha=0.3)
    else:
        axes[0].axis("off")
    if throughput_y:
        axes[1].plot(throughput_x, throughput_y, marker="o", linewidth=1.5)
        axes[1].set_ylabel(throughput_label)
        axes[1].set_xlabel("run")
        axes[1].grid(True, alpha=0.3)
    else:
        axes[1].axis("off")
    fig.suptitle("Repeatability across runs")
    return _save(fig, plots_dir / "repeatability.png")


def _aggregate_mean(
    aggregate: dict[str, Any], section: str, phase: str, metric: str
) -> float | None:
    value = aggregate.get(section, {}).get(phase, {}).get(metric, {}).get("mean")
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value)


def _aggregate_metric_mean(aggregate: dict[str, Any], metric: str) -> float | None:
    value = aggregate.get("metrics", {}).get(metric, {}).get("mean")
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value)


def _load_events(path: Path) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    try:
        with path.open(encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(event, dict):
                    events.append(event)
    except OSError:
        return []
    return events


def _event_time(event: dict[str, Any]) -> float | None:
    value = event.get("monotonic_secs")
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value)


def _interpolate_event_position(
    event_time: float,
    phase_times: list[tuple[float, float]],
) -> float | None:
    if not phase_times:
        return None
    if event_time <= phase_times[0][0]:
        return phase_times[0][1]
    for (left_time, left_x), (right_time, right_x) in zip(
        phase_times, phase_times[1:], strict=False
    ):
        if event_time <= right_time:
            span = right_time - left_time
            if span <= 0.0:
                return right_x
            return left_x + (event_time - left_time) / span * (right_x - left_x)
    return phase_times[-1][1]


def _timeline_event_positions(result_dir: Path, phases: list[str]) -> dict[str, float]:
    by_label: dict[str, list[float]] = {}
    phase_x = {phase: float(index) for index, phase in enumerate(phases)}

    for events_path in sorted(result_dir.glob("run-*/events.jsonl")):
        events = _load_events(events_path)
        phase_times: list[tuple[float, float]] = []
        for event in events:
            event_name = event.get("event")
            if not isinstance(event_name, str) or not event_name.endswith("_start"):
                continue
            phase = event_name[: -len("_start")]
            if phase not in phase_x:
                continue
            event_time = _event_time(event)
            if event_time is not None:
                phase_times.append((event_time, phase_x[phase]))
        phase_times.sort()

        for event in events:
            event_name = event.get("event")
            if not isinstance(event_name, str):
                continue
            label = TIMELINE_EVENT_LABELS.get(event_name)
            if label is None:
                continue
            event_time = _event_time(event)
            position = (
                _interpolate_event_position(event_time, phase_times)
                if event_time is not None
                else None
            )
            if position is not None:
                by_label.setdefault(label, []).append(position)

    return {
        label: mean
        for label, values in by_label.items()
        if (mean := _mean(values)) is not None
    }


def _plot_phases(plt: Any, aggregate: dict[str, Any], plots_dir: Path) -> Path | None:
    relative_metrics = aggregate.get("relative_metrics", {})
    if not isinstance(relative_metrics, dict) or not relative_metrics:
        return None

    phases = _ordered_phases(set(relative_metrics), include_baseline=False)
    throughput = [
        _aggregate_mean(
            aggregate, "relative_metrics", phase, "throughput.http_reqs_rate_ratio"
        )
        for phase in phases
    ]
    put_p95 = [
        _aggregate_mean(
            aggregate, "relative_metrics", phase, "latency.put.p95_multiplier"
        )
        for phase in phases
    ]
    get_p95 = [
        _aggregate_mean(
            aggregate, "relative_metrics", phase, "latency.get.p95_multiplier"
        )
        for phase in phases
    ]
    if not any(value is not None for value in [*throughput, *put_p95, *get_p95]):
        return None

    x = list(range(len(phases)))
    width = 0.25
    fig, ax = plt.subplots(figsize=(9, 4.8))
    ax.bar(
        [value - width for value in x],
        [value or 0.0 for value in throughput],
        width,
        label="throughput ratio",
    )
    ax.bar(x, [value or 0.0 for value in put_p95], width, label="put p95 multiplier")
    ax.bar(
        [value + width for value in x],
        [value or 0.0 for value in get_p95],
        width,
        label="get p95 multiplier",
    )
    ax.axhline(1.0, color="black", linestyle="--", linewidth=1, alpha=0.5)
    ax.set_xticks(x)
    ax.set_xticklabels(phases)
    ax.set_ylabel("normalized to baseline")
    ax.set_title("Phase behavior vs baseline")
    ax.grid(True, axis="y", alpha=0.3)
    ax.legend()
    return _save(fig, plots_dir / "phases.png")


def _plot_timeline(
    plt: Any, aggregate: dict[str, Any], result_dir: Path, plots_dir: Path
) -> Path | None:
    relative_metrics = aggregate.get("relative_metrics", {})
    if not isinstance(relative_metrics, dict) or not relative_metrics:
        return None

    phases = [
        phase
        for phase in PHASE_ORDER
        if phase == "baseline" or phase in relative_metrics
    ]
    if len(phases) <= 1:
        return None

    x = list(range(len(phases)))
    throughput: list[float | None] = []
    put_p95: list[float | None] = []
    put_p99: list[float | None] = []
    for phase in phases:
        if phase == "baseline":
            throughput.append(1.0)
            put_p95.append(1.0)
            put_p99.append(1.0)
            continue
        throughput.append(
            _aggregate_mean(
                aggregate,
                "relative_metrics",
                phase,
                "throughput.http_reqs_rate_ratio",
            )
        )
        put_p95.append(
            _aggregate_mean(
                aggregate,
                "relative_metrics",
                phase,
                "latency.put.p95_multiplier",
            )
        )
        put_p99.append(
            _aggregate_mean(
                aggregate,
                "relative_metrics",
                phase,
                "latency.put.p99_multiplier",
            )
        )

    if not any(value is not None for value in [*throughput, *put_p95, *put_p99]):
        return None

    fig, ax = plt.subplots(figsize=(10, 5.2))
    for values, label, marker in (
        (throughput, "throughput ratio", "o"),
        (put_p95, "put p95 multiplier", "s"),
        (put_p99, "put p99 multiplier", "^"),
    ):
        if any(value is not None for value in values):
            ax.plot(
                x,
                [value if value is not None else float("nan") for value in values],
                marker=marker,
                linewidth=1.8,
                label=label,
            )

    event_positions = {
        **{
            label: position
            for label, position in TIMELINE_FALLBACK_POSITIONS.items()
            if position <= float(len(phases) - 1)
        },
        **_timeline_event_positions(result_dir, phases),
    }
    y_top = ax.get_ylim()[1]
    for label, position in sorted(event_positions.items(), key=lambda item: item[1]):
        ax.axvline(position, linestyle="--", linewidth=1, alpha=0.45)
        ax.text(
            position,
            y_top,
            label,
            rotation=90,
            va="top",
            ha="right",
            fontsize=8,
            alpha=0.8,
        )

    ax.axhline(1.0, color="black", linestyle=":", linewidth=1, alpha=0.6)
    ax.set_xticks(x)
    ax.set_xticklabels(phases)
    ax.set_ylabel("normalized to baseline")
    ax.set_title("Fault timeline")
    ax.grid(True, alpha=0.3)
    ax.legend(loc="best")
    return _save(fig, plots_dir / "timeline.png")


def _plot_accord_paths(
    plt: Any, aggregate: dict[str, Any], plots_dir: Path
) -> Path | None:
    paths = ("fast", "slow", "recovery")
    ratios = [
        _aggregate_metric_mean(aggregate, f"server.consensus.path.{path_name}.ratio")
        for path_name in paths
    ]
    if not any(value is not None for value in ratios):
        return None

    x = list(range(len(paths)))
    fig, ax = plt.subplots(figsize=(7.5, 4.5))
    ax.bar(x, [value or 0.0 for value in ratios])
    ax.set_xticks(x)
    ax.set_xticklabels(paths)
    ax.set_ylim(0.0, 1.05)
    ax.set_ylabel("operation ratio")
    ax.set_title("Consensus path ratios")
    ax.grid(True, axis="y", alpha=0.3)
    return _save(fig, plots_dir / "accord_paths.png")


def _plot_recovery(plt: Any, aggregate: dict[str, Any], plots_dir: Path) -> Path | None:
    phase_metrics = aggregate.get("phase_metrics", {})
    if not isinstance(phase_metrics, dict) or not phase_metrics:
        return None

    phases = [phase for phase in PHASE_ORDER if phase in phase_metrics]
    if len(phases) <= 1:
        return None

    put_p95 = [
        _aggregate_mean(aggregate, "phase_metrics", phase, "latency.put.p95_ms")
        for phase in phases
    ]
    put_p99 = [
        _aggregate_mean(aggregate, "phase_metrics", phase, "latency.put.p99_ms")
        for phase in phases
    ]
    success_ratio = [
        _aggregate_mean(
            aggregate, "phase_metrics", phase, "successes.s3_successes.rate"
        )
        for phase in phases
    ]
    if not any(value is not None for value in [*put_p95, *put_p99, *success_ratio]):
        return None

    x = list(range(len(phases)))
    fig, ax_latency = plt.subplots(figsize=(10, 5.2))
    for values, label, marker in (
        (put_p95, "put p95 latency", "o"),
        (put_p99, "put p99 latency", "s"),
    ):
        if any(value is not None for value in values):
            ax_latency.plot(
                x,
                [value if value is not None else float("nan") for value in values],
                marker=marker,
                linewidth=1.8,
                label=label,
            )
    ax_latency.set_ylabel("latency, ms")
    ax_latency.grid(True, alpha=0.3)

    ax_success = ax_latency.twinx()
    if any(value is not None for value in success_ratio):
        ax_success.plot(
            x,
            [value if value is not None else float("nan") for value in success_ratio],
            marker="^",
            linewidth=1.8,
            color="tab:green",
            label="success ratio",
        )
    ax_success.set_ylabel("success ratio")
    ax_success.set_ylim(0.0, 1.05)

    ax_latency.set_xticks(x)
    ax_latency.set_xticklabels(phases)
    ax_latency.set_title("Recovery behavior")

    latency_handles, latency_labels = ax_latency.get_legend_handles_labels()
    success_handles, success_labels = ax_success.get_legend_handles_labels()
    ax_latency.legend(
        [*latency_handles, *success_handles],
        [*latency_labels, *success_labels],
        loc="best",
    )
    return _save(fig, plots_dir / "recovery.png")


def _plot_symmetry(
    plt: Any, summaries: list[dict[str, Any]], plots_dir: Path
) -> Path | None:
    by_node: dict[int, dict[str, list[float]]] = {}
    for summary in summaries:
        node_index = _get_number(summary, ("metrics", "fault", "node_index"))
        if node_index is None:
            continue
        node = int(node_index)
        bucket = by_node.setdefault(
            node,
            {"put_p95_multiplier": [], "throughput_degradation_factor": []},
        )
        put_p95 = _get_number(
            summary,
            (
                "metrics",
                "relative",
                "degraded",
                "latency",
                "put",
                "p95_multiplier",
            ),
        )
        throughput_ratio = _get_number(
            summary,
            (
                "metrics",
                "relative",
                "degraded",
                "throughput",
                "http_reqs_rate_ratio",
            ),
        )
        if put_p95 is not None:
            bucket["put_p95_multiplier"].append(put_p95)
        if throughput_ratio is not None and throughput_ratio > 0.0:
            bucket["throughput_degradation_factor"].append(1.0 / throughput_ratio)

    if len(by_node) < 2:
        return None

    nodes = sorted(by_node)
    put_p95_values = [
        _mean(by_node[node]["put_p95_multiplier"]) or 0.0 for node in nodes
    ]
    throughput_values = [
        _mean(by_node[node]["throughput_degradation_factor"]) or 0.0 for node in nodes
    ]
    if not any([*put_p95_values, *throughput_values]):
        return None

    x = list(range(len(nodes)))
    width = 0.35
    fig, ax = plt.subplots(figsize=(9, 4.8))
    ax.bar(
        [value - width / 2 for value in x],
        put_p95_values,
        width,
        label="put p95 multiplier",
    )
    ax.bar(
        [value + width / 2 for value in x],
        throughput_values,
        width,
        label="throughput degradation factor",
    )
    ax.axhline(1.0, color="black", linestyle="--", linewidth=1, alpha=0.5)
    ax.set_xticks(x)
    ax.set_xticklabels([f"node{node}" for node in nodes])
    ax.set_ylabel("degradation factor vs baseline")
    ax.set_title("Symmetry of node failures")
    ax.grid(True, axis="y", alpha=0.3)
    ax.legend()
    return _save(fig, plots_dir / "symmetry.png")


def _plot_hot_key(
    plt: Any, summaries: list[dict[str, Any]], plots_dir: Path
) -> Path | None:
    metrics = []
    hot_values = []
    independent_values = []
    for metric in K6_STREAM_LATENCY_METRICS:
        hot = _mean_nested(
            summaries, ("metrics", "key_class_metrics", "hot", metric, "p95")
        )
        independent = _mean_nested(
            summaries,
            ("metrics", "key_class_metrics", "independent", metric, "p95"),
        )
        if hot is None and independent is None:
            continue
        metrics.append(metric.replace("s3_", "").replace("_ms", ""))
        hot_values.append(hot or 0.0)
        independent_values.append(independent or 0.0)
    if not metrics:
        return None

    x = list(range(len(metrics)))
    width = 0.35
    fig, ax = plt.subplots(figsize=(9, 4.8))
    ax.bar([value - width / 2 for value in x], hot_values, width, label="hot")
    ax.bar(
        [value + width / 2 for value in x],
        independent_values,
        width,
        label="independent",
    )
    ax.set_xticks(x)
    ax.set_xticklabels(metrics)
    ax.set_ylabel("p95 latency, ms")
    ax.set_title("Hot-key vs independent-key latency")
    ax.grid(True, axis="y", alpha=0.3)
    ax.legend()
    return _save(fig, plots_dir / "hot_key.png")


def _plot_nodes(
    plt: Any, summaries: list[dict[str, Any]], plots_dir: Path
) -> Path | None:
    node_names: set[str] = set()
    for summary in summaries:
        node_metrics = _get_nested(summary, ("metrics", "entry_node_metrics"))
        if isinstance(node_metrics, dict):
            node_names.update(str(key) for key in node_metrics)
    if not node_names:
        return None

    nodes = sorted(node_names)
    sample_means = []
    put_p95_means = []
    for node in nodes:
        sample_values = [
            value
            for summary in summaries
            if (value := _node_total_samples(summary, node)) is not None
        ]
        sample_means.append(_mean(sample_values) or 0.0)
        put_p95_means.append(
            _mean_nested(
                summaries,
                ("metrics", "entry_node_metrics", node, "s3_put_ms", "p95"),
            )
            or 0.0
        )

    total_samples = sum(sample_means)
    shares = [
        value / total_samples * 100.0 if total_samples else 0.0
        for value in sample_means
    ]

    x = list(range(len(nodes)))
    fig, axes = plt.subplots(2, 1, figsize=(9, 6), sharex=True)
    axes[0].bar(x, shares)
    axes[0].set_ylabel("request share, %")
    axes[0].set_title("Per-node entry distribution")
    axes[0].grid(True, axis="y", alpha=0.3)
    axes[1].bar(x, put_p95_means)
    axes[1].set_ylabel("put p95 latency, ms")
    axes[1].set_xticks(x)
    axes[1].set_xticklabels(nodes)
    axes[1].grid(True, axis="y", alpha=0.3)
    return _save(fig, plots_dir / "nodes.png")


def generate_plots(
    result_dir: Path, aggregate: dict[str, Any] | None = None
) -> list[Path]:
    """Generate applicable PNG plots for a research result directory.

    The function is intentionally best-effort: missing metrics simply skip the
    corresponding chart, while import/runtime errors from matplotlib are allowed
    to propagate to the caller so CLI output can report the plotting failure.
    """
    summaries = _load_run_summaries(result_dir)
    if not summaries:
        return []

    aggregate = aggregate or {}
    scenario = _detect_scenario(summaries)
    plots_dir = result_dir / "plots"
    plots_dir.mkdir(parents=True, exist_ok=True)

    plt = _pyplot()
    generated: list[Path] = []
    try:
        for path in (
            _plot_repeatability(plt, summaries, plots_dir, scenario),
            _plot_accord_paths(plt, aggregate, plots_dir),
            _plot_phases(plt, aggregate, plots_dir)
            if scenario in {"e3-degradation", "e6-recovery"}
            else None,
            _plot_timeline(plt, aggregate, result_dir, plots_dir)
            if scenario in {"e3-degradation", "e6-recovery"}
            else None,
            _plot_symmetry(plt, summaries, plots_dir)
            if scenario in {"e3-degradation", "e6-recovery"}
            else None,
            _plot_recovery(plt, aggregate, plots_dir)
            if scenario == "e6-recovery"
            else None,
            _plot_hot_key(plt, summaries, plots_dir)
            if scenario == "e4-hot-key"
            else None,
            _plot_nodes(plt, summaries, plots_dir)
            if scenario == "e5-leaderless"
            else None,
        ):
            if path is not None:
                generated.append(path)
    finally:
        plt.close("all")
    return generated
