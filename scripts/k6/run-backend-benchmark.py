"""Cross-platform k6 S3 benchmark runner for SO3.

Usage:
  python scripts/k6/run-backend-benchmark.py --backend so3 --runs 30
  python scripts/k6/run-backend-benchmark.py --backend so3-cluster --runs 30 --outdir /tmp/so3-cluster-k6-clean-30-json

Dependencies:
  - k6 is required for this Python runner
  - psutil is required for local process CPU/RSS sampling on all platforms
"""

from __future__ import annotations

import argparse
import importlib
import json
import math
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Sequence

try:
    psutil: Any | None = importlib.import_module("psutil")
except ImportError:  # pragma: no cover - environment-specific failure
    psutil = None

PSUTIL_REQUIRED_MESSAGE = (
    "error: psutil is required for backend resource sampling; "
    "activate scripts/venv or install it with `python -m pip install -r scripts/requirements.txt`"
)


def require_psutil() -> Any:
    if psutil is None:
        raise RuntimeError(PSUTIL_REQUIRED_MESSAGE)
    return psutil


SUPPORTED_BACKENDS = {"so3", "so3-cluster"}

DEFAULT_ACCESS_KEY = "so3testkey000000"
DEFAULT_SECRET_KEY = "so3testsecret0000000000000000000"

SCRIPT_DIR = Path(__file__).resolve().parent
BENCHMARK_SCRIPT = SCRIPT_DIR / "s3-benchmark.js"


@dataclass
class Settings:
    backend: str
    runs: int
    out_dir: Path
    k6_extra_args: list[str]
    env: dict[str, str]

    resource_file: Path
    so3_bucket: str

    managed_processes: list[subprocess.Popen[bytes]] = field(default_factory=list)
    sampler: "ResourceSampler | None" = None
    run_data_dir: Path | None = None
    run_log_file: Path | None = None

    resource_sample_interval_secs: float = 1.0
    backend_start_timeout_secs: float = 20.0
    backend_stop_timeout_secs: float = 10.0
    keep_run_dirs: bool = False

    so3_object_addr: str = "127.0.0.1:3000"
    so3_rpc_addr: str = "127.0.0.1:4000"
    so3_bin: str = "target/release/so3"
    so3_require_release: bool = True

    so3_addr: str = ""


def env_get_bool(env: dict[str, str], name: str, default: bool) -> bool:
    value = env.get(name)
    if value is None:
        return default
    return value == "1" or value.lower() in {"true", "yes", "on"}


def env_get_float(env: dict[str, str], name: str, default: float) -> float:
    value = env.get(name)
    if value is None:
        return default
    return float(value)


def parse_args(argv: Sequence[str]) -> tuple[argparse.Namespace, list[str]]:
    parser = argparse.ArgumentParser(
        description="Run the k6 S3 benchmark against a local SO3 backend.",
        allow_abbrev=False,
    )
    parser.add_argument("--runs", type=int, default=30)
    parser.add_argument("--outdir", default="")
    parser.add_argument("--backend", default=os.environ.get("BACKEND", "so3"))
    return parser.parse_known_args(argv)


def build_settings(argv: Sequence[str]) -> Settings:
    args, extra = parse_args(argv)
    backend = args.backend
    if backend not in SUPPORTED_BACKENDS:
        expected = "|".join(sorted(SUPPORTED_BACKENDS))
        raise SystemExit(
            f"error: unsupported backend {backend!r} (expected {expected})"
        )

    env = os.environ.copy()
    out_dir = (
        Path(args.outdir)
        if args.outdir
        else Path(tempfile.mkdtemp(prefix=f"{backend}-bench."))
    )
    out_dir.mkdir(parents=True, exist_ok=True)

    env.setdefault("AWS_ACCESS_KEY_ID", DEFAULT_ACCESS_KEY)
    env.setdefault("AWS_SECRET_ACCESS_KEY", DEFAULT_SECRET_KEY)
    env.setdefault("AWS_REGION", "us-east-1")
    env.setdefault("SO3_BUCKET", "bench")

    so3_object_addr = env.get("SO3_OBJECT_ADDR", "127.0.0.1:3000")
    so3_rpc_addr = env.get("SO3_RPC_ADDR", "127.0.0.1:4000")
    so3_addr = env.get("SO3_ADDR", f"http://{so3_object_addr}")

    env["SO3_ADDR"] = so3_addr

    so3_bin = env.get("SO3_BIN", "target/release/so3")
    if (
        os.name == "nt"
        and not Path(so3_bin).exists()
        and Path(f"{so3_bin}.exe").exists()
    ):
        so3_bin = f"{so3_bin}.exe"

    settings = Settings(
        backend=backend,
        runs=args.runs,
        out_dir=out_dir,
        k6_extra_args=extra,
        env=env,
        resource_file=out_dir / "resources.tsv",
        so3_bucket=env["SO3_BUCKET"],
        resource_sample_interval_secs=env_get_float(
            env, "RESOURCE_SAMPLE_INTERVAL_SECS", 1.0
        ),
        backend_start_timeout_secs=env_get_float(
            env, "BACKEND_START_TIMEOUT_SECS", 20.0
        ),
        backend_stop_timeout_secs=env_get_float(env, "BACKEND_STOP_TIMEOUT_SECS", 10.0),
        keep_run_dirs=env_get_bool(env, "KEEP_RUN_DIRS", False),
        so3_object_addr=so3_object_addr,
        so3_rpc_addr=so3_rpc_addr,
        so3_bin=so3_bin,
        so3_require_release=env_get_bool(env, "SO3_REQUIRE_RELEASE", True),
        so3_addr=so3_addr,
    )
    settings.resource_file.write_text("")
    return settings


def assert_release_binary(settings: Settings) -> None:
    if not settings.so3_require_release:
        return
    normalized = settings.so3_bin.replace("\\", "/")
    allowed = (
        normalized.endswith("/target/release/so3") or normalized == "target/release/so3"
    )
    allowed = (
        allowed
        or normalized.endswith("/target/release/so3.exe")
        or normalized == "target/release/so3.exe"
    )
    if not allowed:
        raise SystemExit(
            f"error: refusing to benchmark non-release so3 binary {settings.so3_bin}\n"
            "       set SO3_BIN=target/release/so3, or set SO3_REQUIRE_RELEASE=0 for script debugging only"
        )


def http_ready(url: str) -> bool:
    try:
        with urllib.request.urlopen(url, timeout=1):  # noqa: S310 - benchmark-local URL
            return True
    except urllib.error.HTTPError:
        return True
    except Exception:
        return False


def wait_for_ready(settings: Settings, url: str) -> None:
    assert settings.run_log_file is not None
    deadline = time.monotonic() + settings.backend_start_timeout_secs
    while time.monotonic() < deadline:
        for proc in settings.managed_processes:
            if proc.poll() is not None:
                raise RuntimeError(
                    f"backend process exited before becoming ready; see {settings.run_log_file}"
                )

        if http_ready(url):
            return
        time.sleep(0.2)
    raise RuntimeError(
        f"backend did not become ready within {settings.backend_start_timeout_secs}s; see {settings.run_log_file}"
    )


def popen_backend(
    settings: Settings, env_updates: dict[str, str], *, append_log: bool
) -> subprocess.Popen[bytes]:
    assert settings.run_log_file is not None
    env = settings.env.copy()
    env.update(env_updates)
    mode = "ab" if append_log else "wb"
    log = settings.run_log_file.open(mode)
    try:
        proc = subprocess.Popen([settings.so3_bin], env=env, stdout=log, stderr=log)
    finally:
        log.close()
    settings.managed_processes.append(proc)
    return proc


def start_so3_cluster(settings: Settings) -> None:
    assert_release_binary(settings)
    assert settings.run_data_dir is not None
    node1_id = "11111111-1111-1111-1111-111111111111"
    node2_id = "22222222-2222-2222-2222-222222222222"
    node3_id = "33333333-3333-3333-3333-333333333333"
    node1_rpc = "127.0.0.1:4000"
    node2_rpc = "127.0.0.1:4001"
    node3_rpc = "127.0.0.1:4002"

    popen_backend(
        settings,
        {
            "SO3_NODE_ID": node1_id,
            "SO3_OBJECT_ADDR": settings.so3_object_addr,
            "SO3_RPC_ADDR": node1_rpc,
            "SO3_DATA_DIR": str(settings.run_data_dir / "node1"),
            "SO3_CLUSTER_PEERS": f"{node2_id}@{node2_rpc},{node3_id}@{node3_rpc}",
        },
        append_log=False,
    )
    popen_backend(
        settings,
        {
            "SO3_NODE_ID": node2_id,
            "SO3_OBJECT_ADDR": "127.0.0.1:3001",
            "SO3_RPC_ADDR": node2_rpc,
            "SO3_DATA_DIR": str(settings.run_data_dir / "node2"),
            "SO3_CLUSTER_PEERS": f"{node1_id}@{node1_rpc},{node3_id}@{node3_rpc}",
        },
        append_log=True,
    )
    popen_backend(
        settings,
        {
            "SO3_NODE_ID": node3_id,
            "SO3_OBJECT_ADDR": "127.0.0.1:3002",
            "SO3_RPC_ADDR": node3_rpc,
            "SO3_DATA_DIR": str(settings.run_data_dir / "node3"),
            "SO3_CLUSTER_PEERS": f"{node1_id}@{node1_rpc},{node2_id}@{node2_rpc}",
        },
        append_log=True,
    )

    wait_for_ready(settings, f"{settings.so3_addr}/")
    wait_for_ready(settings, "http://127.0.0.1:3001/")
    wait_for_ready(settings, "http://127.0.0.1:3002/")


def start_backend(settings: Settings, run_index: int) -> None:
    settings.run_data_dir = Path(
        tempfile.mkdtemp(
            prefix=f"{settings.backend}-k6-run-{run_index}.", dir=tempfile.gettempdir()
        )
    )
    settings.run_log_file = (
        settings.out_dir / f"{settings.backend}_run_{run_index:03d}.log"
    )
    settings.run_log_file.write_text("")
    settings.managed_processes = []

    if settings.backend == "so3":
        assert_release_binary(settings)
        popen_backend(
            settings,
            {
                "SO3_OBJECT_ADDR": settings.so3_object_addr,
                "SO3_RPC_ADDR": settings.so3_rpc_addr,
                "SO3_DATA_DIR": str(settings.run_data_dir),
            },
            append_log=False,
        )
        wait_for_ready(settings, f"{settings.so3_addr}/")
    elif settings.backend == "so3-cluster":
        start_so3_cluster(settings)


def stop_backend(settings: Settings) -> None:
    for proc in settings.managed_processes:
        if proc.poll() is None:
            try:
                proc.terminate()
            except ProcessLookupError:
                pass
    deadline = time.monotonic() + settings.backend_stop_timeout_secs
    for proc in settings.managed_processes:
        remaining = max(0.0, deadline - time.monotonic())
        try:
            proc.wait(timeout=remaining)
        except subprocess.TimeoutExpired:
            try:
                proc.kill()
            except ProcessLookupError:
                pass
            proc.wait(timeout=5)
    settings.managed_processes = []


def cleanup_run_data_dir(settings: Settings) -> None:
    if settings.run_data_dir and not settings.keep_run_dirs:
        expected_prefix = f"{settings.backend}-k6-run-"
        if settings.run_data_dir.name.startswith(
            expected_prefix
        ) and settings.run_data_dir.parent == Path(tempfile.gettempdir()):
            shutil.rmtree(settings.run_data_dir, ignore_errors=True)
        else:
            print(
                f"warning: refusing to remove unexpected data dir {settings.run_data_dir}",
                file=sys.stderr,
            )
    settings.run_data_dir = None


class ResourceSampler:
    def __init__(self, settings: Settings) -> None:
        self.psutil = require_psutil()
        self.settings = settings
        self.stop_event = threading.Event()
        self.thread = threading.Thread(target=self.run, daemon=True)

    def start(self) -> None:
        self.thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        self.thread.join(timeout=5)

    def append_sample(self, cpu: float, rss_kib: float) -> None:
        with self.settings.resource_file.open("a", encoding="utf-8") as f:
            f.write(f"{int(time.time())} {cpu:.4f} {rss_kib:.4f}\n")

    def run(self) -> None:
        if self.settings.managed_processes:
            self.run_psutil([proc.pid for proc in self.settings.managed_processes])
        else:
            print(
                "warning: backend PID was not detected; CPU/RSS sampling disabled",
                file=sys.stderr,
            )

    def run_psutil(self, pids: list[int]) -> None:
        processes = []
        for pid in pids:
            try:
                proc = self.psutil.Process(pid)
                proc.cpu_percent(None)
                processes.append(proc)
            except Exception:
                pass
        if not processes:
            return
        while not self.stop_event.wait(self.settings.resource_sample_interval_secs):
            cpu_sum = 0.0
            rss_sum = 0.0
            any_running = False
            for proc in processes:
                try:
                    if proc.is_running():
                        any_running = True
                        cpu_sum += proc.cpu_percent(None)
                        rss_sum += proc.memory_info().rss / 1024.0
                except Exception:
                    pass
            if not any_running:
                break
            self.append_sample(cpu_sum, rss_sum)


def run_k6(settings: Settings, export_file: Path) -> None:
    command = [
        "k6",
        "run",
        "--quiet",
        "--no-color",
        f"--summary-export={export_file}",
        *settings.k6_extra_args,
        str(BENCHMARK_SCRIPT),
    ]
    if settings.env.get("DEBUG_ERRORS") == "1":
        subprocess.run(command, env=settings.env, check=True)
    else:
        subprocess.run(
            command,
            env=settings.env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=True,
        )


def cleanup(settings: Settings) -> None:
    if settings.sampler is not None:
        settings.sampler.stop()
        settings.sampler = None
    stop_backend(settings)
    cleanup_run_data_dir(settings)


def metric_value(run: dict, metric_key: str, stat: str) -> float | None:
    metric = run.get("metrics", {}).get(metric_key)
    if not isinstance(metric, dict):
        return None
    if stat in metric:
        return float(metric[stat])
    if stat == "rate" and "value" in metric:
        return float(metric["value"])
    return None


def aggregate_values(
    values: Iterable[float],
) -> tuple[int, float, float, float, float, float, float]:
    xs = list(values)
    n = len(xs)
    if n == 0:
        return 0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0
    mean = sum(xs) / n
    var = sum((x - mean) ** 2 for x in xs) / n
    sd = math.sqrt(max(0.0, var))
    cv = sd / mean * 100 if mean else 0.0
    return n, mean, sd, var, cv, min(xs), max(xs)


def print_header(title: str) -> None:
    print(f"  {title}")
    print(f"  {'─' * 90}")


def aggregate_metric(
    settings: Settings,
    label: str,
    metric_key: str,
    stat: str,
    unit: str,
    decimals: int = 4,
) -> None:
    values: list[float] = []
    for path in sorted(settings.out_dir.glob("run_*.json")):
        with path.open(encoding="utf-8") as f:
            value = metric_value(json.load(f), metric_key, stat)
        if value is not None:
            values.append(value)
    if not values:
        print(f"  {label:<16} {stat:<8} : no data")
        return
    n, mean, sd, var, cv, mn, mx = aggregate_values(values)
    print(
        f"  {label:<16} {stat:<8} :  n={n:<3d}  mean={mean:10.{decimals}f}  "
        f"σ={sd:10.{decimals}f}  var={var:12.{decimals}f}  CV={cv:5.1f}%  "
        f"min={mn:10.{decimals}f}  max={mx:10.{decimals}f}  {unit}"
    )


def aggregate_latency(
    settings: Settings, label: str, metric_key: str, stat: str
) -> None:
    aggregate_metric(settings, label, metric_key, stat, "ms", decimals=2)


def aggregate_resources(settings: Settings) -> None:
    if (
        not settings.resource_file.exists()
        or settings.resource_file.stat().st_size == 0
    ):
        print("  no CPU/RSS samples collected")
        return
    cpu_values: list[float] = []
    rss_values: list[float] = []
    with settings.resource_file.open(encoding="utf-8") as f:
        for line in f:
            parts = line.split()
            if len(parts) == 3:
                cpu_values.append(float(parts[1]))
                rss_values.append(float(parts[2]) / 1024.0)
    if not cpu_values:
        print("  no CPU/RSS samples collected")
        return
    n, mean, sd, _var, _cv, mn, mx = aggregate_values(cpu_values)
    print(
        f"  CPU %        :  n={n:<4d} mean={mean:8.2f}  σ={sd:8.2f}  min={mn:8.2f}  max={mx:8.2f}"
    )
    n, mean, sd, _var, _cv, mn, mx = aggregate_values(rss_values)
    print(
        f"  RSS MiB      :  n={n:<4d} mean={mean:8.2f}  σ={sd:8.2f}  min={mn:8.2f}  max={mx:8.2f}"
    )


def print_summary(settings: Settings) -> None:
    for title, metric in (
        ("PUT", "s3_put_ms"),
        ("GET", "s3_get_ms"),
        ("HEAD", "s3_head_ms"),
        ("DELETE", "s3_delete_ms"),
    ):
        print_header(title)
        for stat in ("med", "avg", "p(90)", "p(95)"):
            aggregate_latency(settings, title, metric, stat)
        print()

    print_header("THROUGHPUT")
    aggregate_metric(settings, "S3 requests", "http_reqs", "rate", "req/s")
    aggregate_metric(settings, "S3 requests", "http_reqs", "count", "requests/run")
    aggregate_metric(settings, "S3 errors", "s3_errors", "rate", "ratio")
    print()

    print_header(f"{settings.backend} RESOURCES")
    aggregate_resources(settings)
    print()
    print(f"  Raw JSON exports: {settings.out_dir}/run_*.json")
    print(f"  Resource samples: {settings.resource_file}")


def main(argv: Sequence[str]) -> int:
    settings = build_settings(argv)
    try:
        require_psutil()
    except RuntimeError as exc:
        print(exc, file=sys.stderr)
        return 2

    print(
        f"{settings.backend} S3 benchmark - {settings.runs} runs -> {settings.out_dir}"
    )
    print(f"endpoint: {settings.so3_addr}")
    print(f"bucket:   {settings.so3_bucket}")
    print(f"region:   {settings.env['AWS_REGION']}")
    print()

    try:
        for i in range(1, settings.runs + 1):
            export_file = settings.out_dir / f"run_{i:03d}.json"
            print(f"  run {i:3d}/{settings.runs} ... ", end="", flush=True)
            try:
                start_backend(settings, i)
                settings.sampler = ResourceSampler(settings)
                settings.sampler.start()
                run_k6(settings, export_file)
            finally:
                cleanup(settings)
            print("done")
        print()
        print_summary(settings)
        return 0
    except KeyboardInterrupt:
        cleanup(settings)
        print("interrupted", file=sys.stderr)
        return 130
    except Exception as exc:
        cleanup(settings)
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
