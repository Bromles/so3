"""Markdown report generation for research runs."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import stats


def write_report(result_dir: Path, aggregate: dict[str, Any]) -> Path:
    report_path = result_dir / "report.md"
    lines = [
        "# SO3 research scenario report",
        "",
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
