"""Fault injection primitives for SO3 research scenarios.

The first implementation exposes node crash/restart operations through the
cluster lifecycle layer. Network partitions are intentionally left as an explicit
unsupported operation until a proxy-based network layer is introduced.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import NoReturn

from cluster import So3Cluster


@dataclass(frozen=True)
class FaultEvent:
    kind: str
    node_index: int | None = None
    details: dict[str, object] | None = None


def crash_node(cluster: So3Cluster, node_index: int) -> FaultEvent:
    cluster.kill_node(node_index)
    return FaultEvent(kind="crash", node_index=node_index)


def restart_node(cluster: So3Cluster, node_index: int) -> FaultEvent:
    cluster.restart_node(node_index)
    return FaultEvent(kind="restart", node_index=node_index)


def unsupported_partition() -> NoReturn:
    raise NotImplementedError(
        "real-cluster network partitions require a proxy-based fault layer; use Maelstrom scenarios for now"
    )
