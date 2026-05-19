"""SO3 cluster lifecycle management for research scenarios."""

from __future__ import annotations

import json
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import psutil
from topology import NodeSpec, Topology


@dataclass
class ManagedNode:
    spec: NodeSpec
    process: subprocess.Popen[bytes]


@dataclass
class So3Cluster:
    """Manage a local SO3 cluster with persistent per-node data dirs."""

    binary: Path
    topology: Topology
    log_file: Path
    env: dict[str, str] = field(default_factory=lambda: os.environ.copy())
    start_timeout_secs: float = 20.0
    stop_timeout_secs: float = 10.0
    nodes: dict[int, ManagedNode] = field(default_factory=dict)

    def assert_binary(self) -> None:
        if not self.binary.exists():
            raise FileNotFoundError(f"SO3 binary does not exist: {self.binary}")
        if self.binary.is_dir():
            raise IsADirectoryError(f"SO3 binary path is a directory: {self.binary}")

    def start(self) -> None:
        self.assert_binary()
        self.log_file.parent.mkdir(parents=True, exist_ok=True)
        self.log_file.write_text("", encoding="utf-8")
        for node in self.topology.nodes:
            self.start_node(node.index, append_log=node.index != 1)
        self.wait_ready_all()

    def start_node(self, node_index: int, *, append_log: bool = True) -> None:
        node = self.node_spec(node_index)
        node.data_dir.mkdir(parents=True, exist_ok=True)
        env = self.env.copy()
        env.update(node.env(self.topology.nodes))
        mode = "ab" if append_log else "wb"
        log = self.log_file.open(mode)
        try:
            process = subprocess.Popen(
                [str(self.binary)], env=env, stdout=log, stderr=log
            )
        finally:
            log.close()
        self.nodes[node_index] = ManagedNode(spec=node, process=process)

    def stop_node(self, node_index: int) -> None:
        managed = self.nodes.get(node_index)
        if managed is None:
            return
        self._terminate_process(managed.process)
        self.nodes.pop(node_index, None)

    def kill_node(self, node_index: int) -> None:
        managed = self.nodes.get(node_index)
        if managed is None:
            return
        self._kill_process(managed.process)
        self.nodes.pop(node_index, None)

    def restart_node(self, node_index: int) -> None:
        self.stop_node(node_index)
        self.start_node(node_index, append_log=True)
        self.wait_ready(self.node_spec(node_index).url)

    def stop(self) -> None:
        for node_index in list(self.nodes):
            self.stop_node(node_index)

    def kill(self) -> None:
        for node_index in list(self.nodes):
            self.kill_node(node_index)

    def cleanup_data_dirs(self) -> None:
        for node in self.topology.nodes:
            shutil.rmtree(node.data_dir, ignore_errors=True)

    def wait_ready_all(self) -> None:
        for node in self.topology.nodes:
            self.wait_ready(node.url)

    def wait_ready(self, url: str) -> None:
        deadline = time.monotonic() + self.start_timeout_secs
        while time.monotonic() < deadline:
            self.raise_if_any_exited()
            if http_ready(f"{url}/"):
                return
            time.sleep(0.2)
        raise TimeoutError(
            f"SO3 node did not become ready within {self.start_timeout_secs}s: {url}"
        )

    def raise_if_any_exited(self) -> None:
        for managed in self.nodes.values():
            code = managed.process.poll()
            if code is not None:
                raise RuntimeError(
                    f"SO3 {managed.spec.name} exited before cluster became ready with status {code}; see {self.log_file}"
                )

    def node_spec(self, node_index: int) -> NodeSpec:
        for node in self.topology.nodes:
            if node.index == node_index:
                return node
        raise ValueError(f"unknown node index {node_index}")

    def process_ids(self) -> dict[str, int]:
        return {
            managed.spec.name: managed.process.pid for managed in self.nodes.values()
        }

    def _terminate_process(self, process: subprocess.Popen[bytes]) -> None:
        if process.poll() is None:
            try:
                process.terminate()
            except ProcessLookupError:
                return
        try:
            process.wait(timeout=self.stop_timeout_secs)
        except subprocess.TimeoutExpired:
            self._kill_process(process)

    def _kill_process(self, process: subprocess.Popen[bytes]) -> None:
        if process.poll() is None:
            try:
                if os.name == "nt":
                    process.kill()
                else:
                    process.send_signal(signal.SIGKILL)
            except ProcessLookupError:
                return
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            print(
                f"warning: process {process.pid} did not exit after SIGKILL",
                file=sys.stderr,
            )


def http_ready(url: str) -> bool:
    try:
        with urllib.request.urlopen(url, timeout=1):  # noqa: S310 - benchmark-local URL
            return True
    except urllib.error.HTTPError:
        return True
    except Exception:
        return False


class ResourceSampler:
    """Sample CPU and RSS for managed node processes into JSONL."""

    def __init__(
        self, cluster: So3Cluster, output_file: Path, interval_secs: float = 1.0
    ) -> None:
        self.cluster = cluster
        self.output_file = output_file
        self.interval_secs = interval_secs
        self.stop_event = threading.Event()
        self.thread = threading.Thread(target=self.run, daemon=True)

    def start(self) -> None:
        self.output_file.parent.mkdir(parents=True, exist_ok=True)
        self.output_file.write_text("", encoding="utf-8")
        self.thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        self.thread.join(timeout=5)

    def run(self) -> None:
        processes: dict[str, Any] = {}
        for managed in self.cluster.nodes.values():
            try:
                proc = psutil.Process(managed.process.pid)
                proc.cpu_percent(None)
                processes[managed.spec.name] = proc
            except Exception:
                pass

        while processes and not self.stop_event.wait(self.interval_secs):
            sample = {"ts_unix": time.time(), "nodes": {}}
            for node_name, proc in list(processes.items()):
                try:
                    if not proc.is_running():
                        processes.pop(node_name, None)
                        continue
                    sample["nodes"][node_name] = {
                        "cpu_percent": proc.cpu_percent(None),
                        "rss_bytes": proc.memory_info().rss,
                    }
                except Exception:
                    processes.pop(node_name, None)
            if sample["nodes"]:
                with self.output_file.open("a", encoding="utf-8") as f:
                    f.write(json.dumps(sample, sort_keys=True) + "\n")
