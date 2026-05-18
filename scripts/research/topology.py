"""Topology generation for SO3 research scenarios."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path

SUPPORTED_NODE_COUNTS = (1, 3, 5, 7)


@dataclass(frozen=True)
class NodeSpec:
    """Runtime configuration for one SO3 node."""

    index: int
    node_id: str
    object_addr: str
    rpc_addr: str
    data_dir: Path

    @property
    def name(self) -> str:
        return f"node{self.index}"

    @property
    def url(self) -> str:
        return f"http://{self.object_addr}"

    @property
    def peer_entry(self) -> str:
        return f"{self.node_id}@{self.rpc_addr}"

    def env(self, peers: list["NodeSpec"]) -> dict[str, str]:
        return {
            "SO3_NODE_ID": self.node_id,
            "SO3_OBJECT_ADDR": self.object_addr,
            "SO3_RPC_ADDR": self.rpc_addr,
            "SO3_DATA_DIR": str(self.data_dir),
            "SO3_CLUSTER_PEERS": ",".join(
                peer.peer_entry for peer in peers if peer.index != self.index
            ),
        }

    def to_json(self) -> dict[str, object]:
        data = asdict(self)
        data["data_dir"] = str(self.data_dir)
        data["url"] = self.url
        return data


@dataclass(frozen=True)
class Topology:
    """Generated SO3 cluster topology."""

    node_count: int
    nodes: list[NodeSpec]

    @property
    def entry_url(self) -> str:
        return self.nodes[0].url

    @property
    def entry_urls(self) -> list[str]:
        return [node.url for node in self.nodes]

    def to_json(self) -> dict[str, object]:
        return {
            "node_count": self.node_count,
            "entry_url": self.entry_url,
            "entry_urls": self.entry_urls,
            "nodes": [node.to_json() for node in self.nodes],
        }


def stable_node_id(index: int) -> str:
    """Return deterministic UUIDs so persisted data dirs are reproducible."""

    if index < 1:
        raise ValueError("node index must be positive")
    return f"00000000-0000-0000-0000-{index:012d}"


def generate_topology(
    node_count: int,
    data_root: Path,
    *,
    host: str = "127.0.0.1",
    object_base_port: int = 3000,
    rpc_base_port: int = 4000,
) -> Topology:
    """Generate a 1-, 3-, 5- or 7-node localhost topology."""

    if node_count not in SUPPORTED_NODE_COUNTS:
        supported = ", ".join(str(value) for value in SUPPORTED_NODE_COUNTS)
        raise ValueError(
            f"unsupported node count {node_count}; expected one of {supported}"
        )

    nodes = [
        NodeSpec(
            index=index,
            node_id=stable_node_id(index),
            object_addr=f"{host}:{object_base_port + index - 1}",
            rpc_addr=f"{host}:{rpc_base_port + index - 1}",
            data_dir=data_root / f"node{index}",
        )
        for index in range(1, node_count + 1)
    ]
    return Topology(node_count=node_count, nodes=nodes)
