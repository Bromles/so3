"""Manifest and event timeline helpers for reproducible research runs."""

from __future__ import annotations

import json
import platform
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def git_revision(repo_root: Path) -> str | None:
    try:
        completed = subprocess.run(
            ["git", "--no-pager", "rev-parse", "HEAD"],
            cwd=repo_root,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=5,
        )
    except Exception:
        return None
    revision = completed.stdout.strip()
    return revision or None


def binary_version(binary_path: Path, repo_root: Path) -> dict[str, Any]:
    resolved = binary_path if binary_path.is_absolute() else repo_root / binary_path
    try:
        st = resolved.stat()
        return {"path": str(binary_path), "exists": True, "mtime": st.st_mtime, "size_bytes": st.st_size}
    except OSError:
        return {"path": str(binary_path), "exists": False, "mtime": None, "size_bytes": None}


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


def build_manifest(
    *,
    scenario: str,
    run_index: int,
    seed: int,
    topology: dict[str, Any],
    workload: dict[str, Any],
    phases: dict[str, Any],
    binary_path: Path,
    repo_root: Path,
    fault_injection: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "scenario": scenario,
        "run_index": run_index,
        "seed": seed,
        "created_at": utc_now(),
        "topology": topology,
        "node_count": topology.get("node_count"),
        "addresses": topology.get("entry_urls", []),
        "workload": workload,
        "phases": phases,
        "binary": binary_version(binary_path, repo_root),
        "git_revision": git_revision(repo_root),
        "fault_injection": fault_injection or {},
        "environment": {
            "platform": platform.platform(),
            "python": platform.python_version(),
        },
    }


@dataclass
class EventLog:
    """Append-only JSONL event timeline."""

    path: Path

    def __post_init__(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text("", encoding="utf-8")

    def record(self, event: str, **fields: Any) -> None:
        payload = {
            "ts": utc_now(),
            "monotonic_secs": time.monotonic(),
            "event": event,
            **fields,
        }
        with self.path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(payload, sort_keys=True) + "\n")
