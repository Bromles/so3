use crate::domain::node::NodeId;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LogicalTimestamp {
    pub epoch: u64,
    pub counter: u64,
    pub node_id: NodeId,
}

impl PartialEq for LogicalTimestamp {
    fn eq(&self, other: &Self) -> bool {
        self.epoch == other.epoch
            && self.counter == other.counter
            && self.node_id.as_ref() == other.node_id.as_ref()
    }
}

impl Eq for LogicalTimestamp {}

impl Ord for LogicalTimestamp {
    fn cmp(&self, other: &Self) -> Ordering {
        self.epoch
            .cmp(&other.epoch)
            .then_with(|| self.counter.cmp(&other.counter))
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

    pub fn tick(&mut self, epoch: u64) -> LogicalTimestamp {
        let wall = physical_millis_now();

        if wall > self.physical_millis {
            self.physical_millis = wall;
            self.logical_counter = 0;
        } else {
            self.logical_counter += 1;
        }

        LogicalTimestamp {
            epoch,
            counter: self.logical_counter,
            node_id: self.node_id.clone(),
        }
    }

    pub fn observe(&mut self, epoch: u64, remote: &LogicalTimestamp) -> LogicalTimestamp {
        let wall = physical_millis_now();
        let new_physical = wall.max(self.physical_millis);

        let new_counter = if new_physical == self.physical_millis && remote.epoch == epoch {
            self.logical_counter.max(remote.counter) + 1
        } else if new_physical == self.physical_millis {
            self.logical_counter + 1
        } else if remote.epoch == epoch {
            remote.counter + 1
        } else {
            0
        };

        self.physical_millis = new_physical;
        self.logical_counter = new_counter;

        LogicalTimestamp {
            epoch,
            counter: self.logical_counter,
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
