use crate::domain::node::NodeId;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LogicalTimestamp {
    pub epoch: u64,
    pub physical_millis: u64,
    pub logical: u64,
    pub node_id: NodeId,
}

impl PartialEq for LogicalTimestamp {
    fn eq(&self, other: &Self) -> bool {
        self.epoch == other.epoch
            && self.physical_millis == other.physical_millis
            && self.logical == other.logical
            && self.node_id.as_ref() == other.node_id.as_ref()
    }
}

impl Eq for LogicalTimestamp {}

impl Ord for LogicalTimestamp {
    fn cmp(&self, other: &Self) -> Ordering {
        self.epoch
            .cmp(&other.epoch)
            .then_with(|| self.physical_millis.cmp(&other.physical_millis))
            .then_with(|| self.logical.cmp(&other.logical))
            .then_with(|| self.node_id.as_ref().cmp(other.node_id.as_ref()))
    }
}

impl PartialOrd for LogicalTimestamp {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone, Debug)]
pub struct HybridLogicalClock {
    node_id: NodeId,
    physical_millis: u64,
    logical_counter: u64,
}

impl HybridLogicalClock {
    #[must_use]
    pub fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            physical_millis: 0,
            logical_counter: 0,
        }
    }

    /// `network_skew_ms` is the expected one-way network delay added to the physical component
    /// of t0 so that, by the time the message arrives at a peer, t0 is ≥ the peer's wall clock.
    /// This reduces the probability of peers bumping the timestamp (slow path).
    /// Pass 0 for no skew (e.g. when observing a remote timestamp).
    pub fn tick(&mut self, epoch: u64, network_skew_ms: u64) -> LogicalTimestamp {
        let wall = physical_millis_now().saturating_add(network_skew_ms);

        if wall > self.physical_millis {
            self.physical_millis = wall;
            self.logical_counter = 0;
        } else {
            self.logical_counter += 1;
        }

        LogicalTimestamp {
            epoch,
            physical_millis: self.physical_millis,
            logical: self.logical_counter,
            node_id: self.node_id.clone(),
        }
    }

    /// Used during Accord `PreAccept`: if the proposed timestamp is strictly ahead of the local
    /// wall clock, accept it as-is so the coordinator can detect fast-path agreement (all
    /// replicas return the same T₀). The HLC advances only when the proposal is also ahead
    /// of the current HLC state; otherwise the HLC stays put (it is already ≥ remote).
    pub fn accept_or_observe(&mut self, epoch: u64, remote: &LogicalTimestamp) -> LogicalTimestamp {
        let wall = physical_millis_now();
        if remote.physical_millis >= wall {
            if remote.physical_millis > self.physical_millis {
                self.physical_millis = remote.physical_millis;
                self.logical_counter = remote.logical;
            }
            remote.clone()
        } else {
            self.observe(epoch, remote)
        }
    }

    pub fn observe(&mut self, epoch: u64, remote: &LogicalTimestamp) -> LogicalTimestamp {
        let wall = physical_millis_now();
        let new_physical = wall.max(self.physical_millis).max(remote.physical_millis);

        let new_logical =
            if new_physical == self.physical_millis && new_physical == remote.physical_millis {
                self.logical_counter.max(remote.logical) + 1
            } else if new_physical == self.physical_millis {
                self.logical_counter + 1
            } else if new_physical == remote.physical_millis {
                remote.logical + 1
            } else {
                0
            };

        self.physical_millis = new_physical;
        self.logical_counter = new_logical;

        LogicalTimestamp {
            epoch,
            physical_millis: self.physical_millis,
            logical: self.logical_counter,
            node_id: self.node_id.clone(),
        }
    }
}

pub fn physical_millis_now() -> u64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_millis();

    u64::try_from(millis).unwrap_or(u64::MAX)
}
