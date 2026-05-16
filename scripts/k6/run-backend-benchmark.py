"""Cross-platform k6 S3 benchmark runner.

Usage:
  python scripts/k6/run-backend-benchmark.py --backend so3 --runs 30
  python scripts/k6/run-backend-benchmark.py --backend so3-cluster --runs 30 --outdir /tmp/so3-cluster-k6-clean-30-json
  python scripts/k6/run-backend-benchmark.py --backend minio-distributed --runs 30 --outdir /tmp/minio-distributed-k6-clean-30-json
  python scripts/k6/run-backend-benchmark.py --backend garage-consistent --runs 30 --outdir /tmp/garage-consistent-k6-clean-30-json

Dependencies:
  - k6 is required for this Python runner
  - optional: psutil for local process CPU/RSS sampling on all platforms
"""

from __future__ import annotations

import argparse
import base64
import json
import math
import os
import re
import secrets
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
from typing import Iterable, Sequence

try:
    import psutil  # type: ignore[import-not-found]
except ImportError:  # pragma: no cover - optional runtime dependency
    psutil = None

SUPPORTED_BACKENDS = {
    "so3",
    "so3-cluster",
    "minio",
    "minio-distributed",
    "garage",
    "garage-consistent",
    "external",
}

DEFAULT_ACCESS_KEY = "so3testkey000000"
DEFAULT_SECRET_KEY = "so3testsecret0000000000000000000"

SCRIPT_DIR = Path(__file__).resolve().parent
BENCHMARK_SCRIPT = SCRIPT_DIR / "s3-benchmark.js"
CREATE_BUCKET_SCRIPT = SCRIPT_DIR / "s3-create-bucket.js"
COMPOSE_FILE = SCRIPT_DIR / "docker-compose.backends.yml"


@dataclass
class Settings:
    backend: str
    runs: int
    out_dir: Path
    k6_extra_args: list[str]
    env: dict[str, str]

    resource_file: Path
    so3_bucket: str
    aws_region_was_set: bool

    managed_processes: list[subprocess.Popen[bytes]] = field(default_factory=list)
    container_id: str = ""
    container_ids: list[str] = field(default_factory=list)
    compose_project_name: str = ""
    sampler: "ResourceSampler | None" = None
    run_data_dir: Path | None = None
    run_log_file: Path | None = None
    garage_config_file_for_run: Path | None = None
    garage_config_dir: Path | None = None

    resource_sample_interval_secs: float = 1.0
    backend_start_timeout_secs: float = 20.0
    backend_stop_timeout_secs: float = 10.0
    keep_run_dirs: bool = False
    create_bucket_per_run: bool = True

    so3_object_addr: str = "127.0.0.1:3000"
    so3_rpc_addr: str = "127.0.0.1:4000"
    so3_bin: str = "target/release/so3"
    so3_require_release: bool = True

    minio_addr: str = "127.0.0.1:9000"
    minio_console_addr: str = "127.0.0.1:9001"
    garage_s3_addr: str = "127.0.0.1:3900"
    garage_rpc_addr: str = "127.0.0.1:3901"
    garage_web_addr: str = "127.0.0.1:3902"
    garage_admin_addr: str = "127.0.0.1:3903"

    so3_addr: str = ""
    require_version_id: str = "0"
    sign_head: str = "1"


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
        description="Run the k6 S3 benchmark against so3, MinIO, Garage, or an external S3 endpoint.",
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

    aws_region_was_set = "AWS_REGION" in env
    env.setdefault("AWS_ACCESS_KEY_ID", DEFAULT_ACCESS_KEY)
    env.setdefault("AWS_SECRET_ACCESS_KEY", DEFAULT_SECRET_KEY)
    env.setdefault("AWS_REGION", "us-east-1")
    env.setdefault("SO3_BUCKET", "bench")

    minio_addr = env.get("MINIO_ADDR", "127.0.0.1:9000")
    minio_console_addr = env.get("MINIO_CONSOLE_ADDR", "127.0.0.1:9001")
    env.setdefault("MINIO_API_BIND", minio_addr)
    env.setdefault("MINIO_CONSOLE_BIND", minio_console_addr)

    garage_s3_addr = env.get("GARAGE_S3_ADDR", "127.0.0.1:3900")
    garage_rpc_addr = env.get("GARAGE_RPC_ADDR", "127.0.0.1:3901")
    garage_web_addr = env.get("GARAGE_WEB_ADDR", "127.0.0.1:3902")
    garage_admin_addr = env.get("GARAGE_ADMIN_ADDR", "127.0.0.1:3903")
    env.setdefault("GARAGE_S3_BIND", garage_s3_addr)
    env.setdefault("GARAGE_RPC_BIND", garage_rpc_addr)
    env.setdefault("GARAGE_WEB_BIND", garage_web_addr)
    env.setdefault("GARAGE_ADMIN_BIND", garage_admin_addr)

    so3_object_addr = env.get("SO3_OBJECT_ADDR", "127.0.0.1:3000")
    so3_rpc_addr = env.get("SO3_RPC_ADDR", "127.0.0.1:4000")

    if backend in {"so3", "so3-cluster"}:
        so3_addr = env.get("SO3_ADDR", f"http://{so3_object_addr}")
        require_version_id = env.get("REQUIRE_VERSION_ID", "1")
        sign_head = env.get("SIGN_HEAD", "0")
    elif backend in {"minio", "minio-distributed"}:
        so3_addr = env.get("SO3_ADDR", f"http://{minio_addr}")
        require_version_id = env.get("REQUIRE_VERSION_ID", "0")
        sign_head = env.get("SIGN_HEAD", "1")
    elif backend in {"garage", "garage-consistent"}:
        so3_addr = env.get("SO3_ADDR", f"http://{garage_s3_addr}")
        if not aws_region_was_set:
            env["AWS_REGION"] = "garage"
        require_version_id = env.get("REQUIRE_VERSION_ID", "0")
        sign_head = env.get("SIGN_HEAD", "1")
    else:
        so3_addr = env.get("SO3_ADDR", "http://127.0.0.1:3000")
        require_version_id = env.get("REQUIRE_VERSION_ID", "0")
        sign_head = env.get("SIGN_HEAD", "1")

    env["SO3_ADDR"] = so3_addr
    env["REQUIRE_VERSION_ID"] = require_version_id
    env["SIGN_HEAD"] = sign_head

    create_bucket = env.get("CREATE_BUCKET_PER_RUN", "auto")
    if create_bucket == "auto":
        create_bucket_per_run = backend not in {"so3", "so3-cluster"}
    else:
        create_bucket_per_run = create_bucket == "1"

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
        aws_region_was_set=aws_region_was_set,
        resource_sample_interval_secs=env_get_float(
            env, "RESOURCE_SAMPLE_INTERVAL_SECS", 1.0
        ),
        backend_start_timeout_secs=env_get_float(
            env, "BACKEND_START_TIMEOUT_SECS", 20.0
        ),
        backend_stop_timeout_secs=env_get_float(env, "BACKEND_STOP_TIMEOUT_SECS", 10.0),
        keep_run_dirs=env_get_bool(env, "KEEP_RUN_DIRS", False),
        create_bucket_per_run=create_bucket_per_run,
        so3_object_addr=so3_object_addr,
        so3_rpc_addr=so3_rpc_addr,
        so3_bin=so3_bin,
        so3_require_release=env_get_bool(env, "SO3_REQUIRE_RELEASE", True),
        minio_addr=minio_addr,
        minio_console_addr=minio_console_addr,
        garage_s3_addr=garage_s3_addr,
        garage_rpc_addr=garage_rpc_addr,
        garage_web_addr=garage_web_addr,
        garage_admin_addr=garage_admin_addr,
        so3_addr=so3_addr,
        require_version_id=require_version_id,
        sign_head=sign_head,
    )
    settings.resource_file.write_text("")
    return settings


def run_command(
        args: Sequence[str],
        *,
        env: dict[str, str] | None = None,
        stdout=None,
        stderr=None,
        check: bool = True,
        text: bool = True,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        list(args),
        env=env,
        stdout=stdout,
        stderr=stderr,
        check=check,
        text=text,
    )


def command_output(args: Sequence[str], *, env: dict[str, str] | None = None) -> str:
    completed = run_command(
        args, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    return completed.stdout.strip()


def append_command_to_log(
        settings: Settings, args: Sequence[str], *, check: bool = True
) -> subprocess.CompletedProcess[bytes]:
    assert settings.run_log_file is not None
    with settings.run_log_file.open("ab") as log:
        return subprocess.run(
            list(args),
            env=settings.env,
            stdout=log,
            stderr=log,
            check=check,
            text=False,
        )


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


def container_running(container_id: str) -> bool:
    try:
        out = command_output(
            ["docker", "inspect", "-f", "{{.State.Running}}", container_id]
        )
    except Exception:
        return False
    return out == "true"


def wait_for_ready(settings: Settings, url: str) -> None:
    assert settings.run_log_file is not None
    deadline = time.monotonic() + settings.backend_start_timeout_secs
    while time.monotonic() < deadline:
        for proc in settings.managed_processes:
            if proc.poll() is not None:
                raise RuntimeError(
                    f"backend process exited before becoming ready; see {settings.run_log_file}"
                )

        if settings.container_ids:
            for container_id in settings.container_ids:
                if not container_running(container_id):
                    append_command_to_log(
                        settings,
                        [
                            "docker",
                            "compose",
                            "-f",
                            str(COMPOSE_FILE),
                            "-p",
                            settings.compose_project_name,
                            "logs",
                            "--no-color",
                        ],
                        check=False,
                    )
                    raise RuntimeError(
                        f"backend container exited before becoming ready; see {settings.run_log_file}"
                    )

        if http_ready(url):
            return
        time.sleep(0.2)
    raise RuntimeError(
        f"backend did not become ready within {settings.backend_start_timeout_secs}s; see {settings.run_log_file}"
    )


def ensure_docker_compose() -> None:
    try:
        run_command(
            ["docker", "compose", "version"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except Exception as exc:
        raise RuntimeError(
            "Docker Compose v2 is required for this benchmark backend"
        ) from exc


def compose_up(settings: Settings, service: str, run_index: int) -> None:
    assert settings.run_log_file is not None
    assert settings.run_data_dir is not None
    ensure_docker_compose()
    settings.compose_project_name = f"so3-k6-{service}-{run_index:03d}"
    settings.env["RUN_DATA_DIR"] = str(settings.run_data_dir)
    if settings.garage_config_file_for_run is not None:
        settings.env["GARAGE_CONFIG_FILE_FOR_RUN"] = str(
            settings.garage_config_file_for_run
        )
    if settings.garage_config_dir is not None:
        settings.env["GARAGE_CONFIG_DIR"] = str(settings.garage_config_dir)

    append_command_to_log(
        settings,
        [
            "docker",
            "compose",
            "-f",
            str(COMPOSE_FILE),
            "-p",
            settings.compose_project_name,
            "up",
            "-d",
            "--pull",
            "always",
            service,
        ],
    )
    settings.container_id = command_output(
        [
            "docker",
            "compose",
            "-f",
            str(COMPOSE_FILE),
            "-p",
            settings.compose_project_name,
            "ps",
            "-q",
            service,
        ],
        env=settings.env,
    )
    ids = command_output(
        [
            "docker",
            "compose",
            "-f",
            str(COMPOSE_FILE),
            "-p",
            settings.compose_project_name,
            "ps",
            "-q",
        ],
        env=settings.env,
    )
    settings.container_ids = [line.strip() for line in ids.splitlines() if line.strip()]
    if not settings.container_id or not settings.container_ids:
        raise RuntimeError(
            f"could not resolve {service} container id; see {settings.run_log_file}"
        )


def random_hex(nbytes: int) -> str:
    return secrets.token_hex(nbytes)


def random_b64(nbytes: int) -> str:
    return base64.b64encode(secrets.token_bytes(nbytes)).decode("ascii")


def write_garage_config(settings: Settings) -> None:
    assert settings.run_data_dir is not None
    settings.garage_config_file_for_run = settings.run_data_dir / "garage.toml"
    settings.garage_config_file_for_run.write_text(
        f'''metadata_dir = "/var/lib/garage/meta"
data_dir = "/var/lib/garage/data"
db_engine = "sqlite"
replication_factor = 1
metadata_fsync = false
data_fsync = false

rpc_bind_addr = "0.0.0.0:3901"
rpc_public_addr = "127.0.0.1:3901"
rpc_secret = "{random_hex(32)}"

[s3_api]
s3_region = "{settings.env["AWS_REGION"]}"
api_bind_addr = "0.0.0.0:3900"
root_domain = ".s3.garage.localhost"

[s3_web]
bind_addr = "0.0.0.0:3902"
root_domain = ".web.garage.localhost"
index = "index.html"

[admin]
api_bind_addr = "0.0.0.0:3903"
admin_token = "{random_b64(32)}"
metrics_token = "{random_b64(32)}"
'''
    )


def write_garage_consistent_configs(settings: Settings) -> None:
    assert settings.run_data_dir is not None
    settings.garage_config_dir = settings.run_data_dir / "garage-config"
    settings.garage_config_dir.mkdir(parents=True, exist_ok=True)
    rpc_secret = random_hex(32)
    admin_token = random_b64(32)
    metrics_token = random_b64(32)
    for node in (1, 2, 3):
        public_addr = "garage-consistent:3901" if node == 1 else f"garage{node}:3901"
        (settings.garage_config_dir / f"garage{node}.toml").write_text(
            f'''metadata_dir = "/var/lib/garage/meta"
data_dir = "/var/lib/garage/data"
db_engine = "sqlite"
replication_factor = 3
consistency_mode = "consistent"
metadata_fsync = false
data_fsync = false

rpc_bind_addr = "0.0.0.0:3901"
rpc_public_addr = "{public_addr}"
rpc_secret = "{rpc_secret}"

[s3_api]
s3_region = "{settings.env["AWS_REGION"]}"
api_bind_addr = "0.0.0.0:3900"
root_domain = ".s3.garage.localhost"

[s3_web]
bind_addr = "0.0.0.0:3902"
root_domain = ".web.garage.localhost"
index = "index.html"

[admin]
api_bind_addr = "0.0.0.0:3903"
admin_token = "{admin_token}"
metrics_token = "{metrics_token}"
'''
        )


def garage_exec(
        settings: Settings, service: str, args: Sequence[str], *, capture: bool = False
) -> str:
    command = [
        "docker",
        "compose",
        "-f",
        str(COMPOSE_FILE),
        "-p",
        settings.compose_project_name,
        "exec",
        "-T",
        service,
        "/garage",
        *args,
    ]
    if capture:
        completed = subprocess.run(
            command,
            env=settings.env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            check=True,
            text=True,
        )
        return completed.stdout
    append_command_to_log(settings, command)
    return ""


def get_garage_node_id(settings: Settings, service: str) -> str:
    deadline = time.monotonic() + settings.backend_start_timeout_secs
    while time.monotonic() < deadline:
        try:
            out = garage_exec(settings, service, ["node", "id"], capture=True)
        except Exception:
            out = ""
        match = re.search(r"[0-9a-f]{64}", out)
        if match:
            return match.group(0)
        time.sleep(0.5)
    raise RuntimeError(f"could not get Garage node id for {service}")


def init_garage_consistent(settings: Settings) -> None:
    id1 = get_garage_node_id(settings, "garage-consistent")
    id2 = get_garage_node_id(settings, "garage2")
    id3 = get_garage_node_id(settings, "garage3")
    garage_exec(
        settings, "garage-consistent", ["node", "connect", f"{id2}@garage2:3901"]
    )
    garage_exec(
        settings, "garage-consistent", ["node", "connect", f"{id3}@garage3:3901"]
    )
    time.sleep(2)
    garage_exec(
        settings,
        "garage-consistent",
        ["layout", "assign", "-z", "dc1", "-c", "1G", id1],
    )
    garage_exec(
        settings,
        "garage-consistent",
        ["layout", "assign", "-z", "dc2", "-c", "1G", id2],
    )
    garage_exec(
        settings,
        "garage-consistent",
        ["layout", "assign", "-z", "dc3", "-c", "1G", id3],
    )
    garage_exec(settings, "garage-consistent", ["layout", "apply", "--version", "1"])
    garage_exec(
        settings, "garage-consistent", ["bucket", "create", settings.so3_bucket]
    )
    garage_exec(
        settings,
        "garage-consistent",
        [
            "key",
            "import",
            "--yes",
            "-n",
            "benchmark",
            settings.env["AWS_ACCESS_KEY_ID"],
            settings.env["AWS_SECRET_ACCESS_KEY"],
        ],
    )
    garage_exec(
        settings,
        "garage-consistent",
        [
            "bucket",
            "allow",
            "--read",
            "--write",
            "--owner",
            settings.so3_bucket,
            "--key",
            settings.env["AWS_ACCESS_KEY_ID"],
        ],
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
    settings.container_id = ""
    settings.container_ids = []
    settings.compose_project_name = ""
    settings.garage_config_file_for_run = None
    settings.garage_config_dir = None

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
    elif settings.backend == "minio":
        compose_up(settings, "minio", run_index)
        wait_for_ready(settings, f"{settings.so3_addr}/minio/health/ready")
    elif settings.backend == "minio-distributed":
        compose_up(settings, "minio-distributed", run_index)
        wait_for_ready(settings, f"{settings.so3_addr}/minio/health/cluster")
    elif settings.backend == "garage":
        write_garage_config(settings)
        compose_up(settings, "garage", run_index)
        wait_for_ready(settings, f"{settings.so3_addr}/")
    elif settings.backend == "garage-consistent":
        write_garage_consistent_configs(settings)
        compose_up(settings, "garage-consistent", run_index)
        wait_for_ready(settings, f"{settings.so3_addr}/")
        init_garage_consistent(settings)
    elif settings.backend == "external":
        pid = settings.env.get("BACKEND_RESOURCE_PID")
        if pid and psutil is not None:
            try:
                process = psutil.Process(int(pid))
                # Store the psutil PID through a tiny fake object is unnecessary; sampler reads env.
                if not process.is_running():
                    raise RuntimeError(f"BACKEND_RESOURCE_PID={pid} is not running")
            except Exception as exc:
                raise RuntimeError(
                    f"could not inspect BACKEND_RESOURCE_PID={pid}"
                ) from exc
        wait_for_ready(settings, f"{settings.so3_addr}/")


def stop_backend(settings: Settings) -> None:
    if settings.backend == "external":
        settings.managed_processes = []
        settings.container_id = ""
        settings.container_ids = []
        return

    if settings.container_id:
        append_command_to_log(
            settings,
            [
                "docker",
                "compose",
                "-f",
                str(COMPOSE_FILE),
                "-p",
                settings.compose_project_name,
                "logs",
                "--no-color",
            ],
            check=False,
        )
        append_command_to_log(
            settings,
            [
                "docker",
                "compose",
                "-f",
                str(COMPOSE_FILE),
                "-p",
                settings.compose_project_name,
                "down",
                "-v",
                "--remove-orphans",
            ],
            check=False,
        )
        settings.container_id = ""
        settings.container_ids = []
        settings.compose_project_name = ""
        return

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
    settings.garage_config_file_for_run = None
    settings.garage_config_dir = None


class ResourceSampler:
    def __init__(self, settings: Settings) -> None:
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
        if self.settings.container_ids:
            self.run_docker()
            return
        if psutil is None:
            print(
                "warning: psutil is not installed; local process CPU/RSS sampling disabled",
                file=sys.stderr,
            )
            return
        if self.settings.managed_processes:
            self.run_psutil([proc.pid for proc in self.settings.managed_processes])
            return
        external_pid = self.settings.env.get("BACKEND_RESOURCE_PID")
        if external_pid:
            self.run_psutil([int(external_pid)])
        else:
            print(
                "warning: backend PID was not detected; CPU/RSS sampling disabled",
                file=sys.stderr,
            )

    def run_docker(self) -> None:
        while not self.stop_event.is_set():
            if not any(
                    container_running(container_id)
                    for container_id in self.settings.container_ids
            ):
                break
            try:
                completed = subprocess.run(
                    [
                        "docker",
                        "stats",
                        "--no-stream",
                        "--format",
                        "{{.CPUPerc}} {{.MemUsage}}",
                        *self.settings.container_ids,
                    ],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.DEVNULL,
                    text=True,
                    check=False,
                )
                cpu_sum = 0.0
                rss_kib_sum = 0.0
                for line in completed.stdout.splitlines():
                    parsed = parse_docker_stats_line(line)
                    if parsed is not None:
                        cpu, rss_kib = parsed
                        cpu_sum += cpu
                        rss_kib_sum += rss_kib
                if completed.stdout.strip():
                    self.append_sample(cpu_sum, rss_kib_sum)
            except Exception:
                pass
            self.stop_event.wait(self.settings.resource_sample_interval_secs)

    def run_psutil(self, pids: list[int]) -> None:
        processes = []
        for pid in pids:
            try:
                proc = psutil.Process(pid)  # type: ignore[union-attr]
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


def parse_docker_stats_line(line: str) -> tuple[float, float] | None:
    parts = line.split()
    if len(parts) < 2:
        return None
    try:
        cpu = float(parts[0].replace("%", ""))
    except ValueError:
        return None
    mem = parts[1]
    match = re.match(r"([0-9.]+)([A-Za-z]+)", mem)
    if not match:
        return cpu, 0.0
    value = float(match.group(1))
    unit = match.group(2)
    if unit == "KiB":
        rss_kib = value
    elif unit == "MiB":
        rss_kib = value * 1024
    elif unit == "GiB":
        rss_kib = value * 1024 * 1024
    elif unit == "B":
        rss_kib = value / 1024
    else:
        rss_kib = value
    return cpu, rss_kib


def create_bucket_if_needed(settings: Settings) -> None:
    if not settings.create_bucket_per_run:
        return
    assert settings.run_log_file is not None
    deadline = time.monotonic() + settings.backend_start_timeout_secs
    while time.monotonic() < deadline:
        with settings.run_log_file.open("ab") as log:
            result = subprocess.run(
                ["k6", "run", "--quiet", "--no-color", str(CREATE_BUCKET_SCRIPT)],
                env=settings.env,
                stdout=subprocess.DEVNULL,
                stderr=log,
                check=False,
            )
        if result.returncode == 0:
            if settings.backend == "minio-distributed":
                time.sleep(15)
                with settings.run_log_file.open("ab") as log:
                    second = subprocess.run(
                        [
                            "k6",
                            "run",
                            "--quiet",
                            "--no-color",
                            str(CREATE_BUCKET_SCRIPT),
                        ],
                        env=settings.env,
                        stdout=subprocess.DEVNULL,
                        stderr=log,
                        check=False,
                    )
                if second.returncode != 0:
                    time.sleep(2)
                    continue
            return
        time.sleep(0.5)
    raise RuntimeError(
        f"failed to create bucket {settings.so3_bucket}; see {settings.run_log_file}"
    )


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
    print(
        f"{settings.backend} S3 benchmark - {settings.runs} runs -> {settings.out_dir}"
    )
    print(f"endpoint: {settings.so3_addr}")
    print(f"bucket:   {settings.so3_bucket}")
    print(f"region:   {settings.env['AWS_REGION']}")
    print(f"version-id HEAD check required: {settings.require_version_id}")
    print(f"signed HEAD: {settings.sign_head}")
    print()

    try:
        for i in range(1, settings.runs + 1):
            export_file = settings.out_dir / f"run_{i:03d}.json"
            print(f"  run {i:3d}/{settings.runs} ... ", end="", flush=True)
            try:
                start_backend(settings, i)
                create_bucket_if_needed(settings)
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
