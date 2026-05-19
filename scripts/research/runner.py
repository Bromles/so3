"""Shared k6 runner helpers for SO3 research scenario modules."""

from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any

import manifest


def run_k6(
    *,
    k6_script: Path,
    export_file: Path,
    stdout_file: Path,
    stderr_file: Path,
    env: dict[str, str],
    extra_args: list[str],
    debug: bool,
    stream_file: Path | None = None,
) -> None:
    command = [
        "k6", "run", "--quiet", "--no-color",
        f"--summary-export={export_file}",
    ]
    if stream_file is not None:
        command.append(f"--out=json={stream_file}")
    command.extend(extra_args)
    command.append(str(k6_script))
    if debug:
        subprocess.run(command, env=env, check=True)
        return
    with stdout_file.open("wb") as stdout, stderr_file.open("wb") as stderr:
        subprocess.run(command, env=env, stdout=stdout, stderr=stderr, check=True)


def run_k6_phase(
    *,
    args: Any,
    k6_script: Path,
    run_dir: Path,
    env: dict[str, str],
    extra_k6_args: list[str],
    phase: str,
    duration: str,
    events: manifest.EventLog,
    with_stream: bool = False,
) -> tuple[Path, Path | None]:
    phase_env = env.copy()
    phase_env["RESEARCH_PHASE"] = phase
    phase_env["DURATION"] = duration
    events.record(f"{phase}_start", duration=duration)
    export_file = run_dir / f"k6-summary-{phase}.json"
    stream_file = run_dir / f"k6-stream-{phase}.jsonl" if with_stream else None
    run_k6(
        k6_script=k6_script,
        export_file=export_file,
        stdout_file=run_dir / f"k6-{phase}.stdout.log",
        stderr_file=run_dir / f"k6-{phase}.stderr.log",
        env=phase_env,
        extra_args=extra_k6_args,
        debug=args.debug_k6,
        stream_file=stream_file,
    )
    events.record(f"{phase}_end", duration=duration)
    return export_file, stream_file
